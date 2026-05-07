/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.convert;

import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.plugin.olap.execution.LuceneFilterConverter;

/**
 * Splits a Calcite {@link RexNode} filter condition into two parts:
 *
 * <ul>
 *   <li>A Lucene-pushable {@link QueryBuilder} that ships via transport and is applied by the
 *       Lucene index on the data node.
 *   <li>A residual {@link RexNode} that stays in the Velox {@code FilterNode}.
 * </ul>
 *
 * <p>This is what lets PPL relevance functions (match, match_phrase, multi_match, query_string,
 * simple_query_string, match_bool_prefix, match_phrase_prefix) run through the Velox path. Velox
 * has no registered relevance scalars, so the splitter peels them out before they reach the Velox
 * plan and ships them as an OpenSearch {@link QueryBuilder} on the fragment request.
 *
 * <p><b>Splitter level (c)</b>: in addition to the AND-friendly shapes, handles OR subtrees that
 * contain relevance calls by translating <i>every</i> leaf of the OR to a {@link QueryBuilder}
 * (relevance leaves via {@link RelevanceQueryBuilderFactory}, non-relevance leaves via {@link
 * LuceneFilterConverter#toQueryBuilder(RexNode, RelDataType)}) and combining with {@code
 * BoolQueryBuilder.SHOULD}. If any leaf can't be translated, the split is ruled out — {@code
 * canVectorize()} then returns false and the SQL plugin's default engine answers the query.
 */
public final class RelevanceSplitter {

  private static final Logger logger = LogManager.getLogger(RelevanceSplitter.class);

  /** Lower-cased operator names for the seven PPL relevance functions. */
  public static final Set<String> RELEVANCE_FUNCTION_NAMES =
      ImmutableSet.of(
          "match",
          "match_phrase",
          "match_phrase_prefix",
          "match_bool_prefix",
          "multi_match",
          "simple_query_string",
          "query_string");

  /** Outcome of a split. */
  public static final class Result {
    /** Null when there's nothing to push. */
    public final QueryBuilder pushdownQuery;

    /** Null when the FilterNode can be dropped entirely (everything pushed). */
    public final RexNode residualCondition;

    /** True when the filter can't be safely split — {@code canVectorize()} must return false. */
    public final boolean ruledOut;

    private Result(QueryBuilder pushdownQuery, RexNode residualCondition, boolean ruledOut) {
      this.pushdownQuery = pushdownQuery;
      this.residualCondition = residualCondition;
      this.ruledOut = ruledOut;
    }

    static Result ruledOut() {
      return new Result(null, null, true);
    }

    static Result noRelevance(RexNode condition) {
      return new Result(null, condition, false);
    }

    static Result split(QueryBuilder pushdown, RexNode residual) {
      return new Result(pushdown, residual, false);
    }
  }

  private final RexBuilder rexBuilder;
  private final RelDataType inputRowType;
  private final LuceneFilterConverter luceneConverter;

  public RelevanceSplitter(RexBuilder rexBuilder, RelDataType inputRowType) {
    this.rexBuilder = rexBuilder;
    this.inputRowType = inputRowType;
    this.luceneConverter = new LuceneFilterConverter();
  }

  /**
   * Split a filter condition. Never throws — translation failures are encoded as {@link
   * Result#ruledOut}.
   */
  public Result split(RexNode condition) {
    if (!containsRelevance(condition)) {
      return Result.noRelevance(condition);
    }
    try {
      return splitInternal(condition);
    } catch (RuntimeException e) {
      logger.debug("Relevance split ruled out: {}", e.getMessage());
      return Result.ruledOut();
    }
  }

  // ---- Core split ----

  private Result splitInternal(RexNode condition) {
    List<QueryBuilder> pushedClauses = new ArrayList<>();
    List<RexNode> residualClauses = new ArrayList<>();

    for (RexNode conjunct : RelOptUtil.conjunctions(condition)) {
      classifyConjunct(conjunct, pushedClauses, residualClauses);
    }

    QueryBuilder pushdown = combineMust(pushedClauses);
    RexNode residual =
        residualClauses.isEmpty()
            ? null
            : RexUtil.composeConjunction(rexBuilder, residualClauses, true);
    return Result.split(pushdown, residual);
  }

  /**
   * Classify a single AND-conjunct. May throw {@link RuleOutException} when the conjunct can't be
   * safely split (the caller in {@link #split} converts that to {@link Result#ruledOut}).
   */
  private void classifyConjunct(
      RexNode conjunct, List<QueryBuilder> pushed, List<RexNode> residual) {
    if (isRelevanceCall(conjunct)) {
      pushed.add(toQueryBuilder((RexCall) conjunct));
      return;
    }
    if (isNotOfRelevance(conjunct)) {
      RexCall inner = (RexCall) ((RexCall) conjunct).getOperands().get(0);
      pushed.add(new BoolQueryBuilder().mustNot(toQueryBuilder(inner)));
      return;
    }
    if (!containsRelevance(conjunct)) {
      residual.add(conjunct);
      return;
    }
    // The conjunct is a mixed expression containing a relevance call somewhere. The only shape
    // we handle at this level is OR: attempt full OR-subtree translation via level (c). Any
    // other nesting (AND deep inside NOT, relevance as an arg to an arithmetic op, …) is a
    // rule-out.
    if (conjunct instanceof RexCall && ((RexCall) conjunct).getKind() == SqlKind.OR) {
      QueryBuilder orQb = translateOrSubtree((RexCall) conjunct);
      pushed.add(orQb);
      return;
    }
    throw new RuleOutException("Relevance call in unsupported position: " + conjunct);
  }

  /**
   * Translate an OR subtree that contains a relevance call into a single {@code BoolQueryBuilder}
   * with SHOULD clauses. Every disjunct must translate — this is level (c)'s contract.
   */
  private QueryBuilder translateOrSubtree(RexCall orCall) {
    BoolQueryBuilder bool = new BoolQueryBuilder();
    for (RexNode disjunct : RelOptUtil.disjunctions(orCall)) {
      QueryBuilder clause = translateAnyLeaf(disjunct);
      if (clause == null) {
        throw new RuleOutException("OR disjunct not translatable: " + disjunct);
      }
      bool.should(clause);
    }
    bool.minimumShouldMatch(1);
    return bool;
  }

  /**
   * Translate an arbitrary RexNode leaf (relevance or non-relevance). Returns null for
   * non-relevance leaves that {@code LuceneFilterConverter} can't express. Relevance shapes never
   * return null — they throw {@link RuleOutException} on factory errors.
   */
  private QueryBuilder translateAnyLeaf(RexNode leaf) {
    if (isRelevanceCall(leaf)) {
      return toQueryBuilder((RexCall) leaf);
    }
    if (isNotOfRelevance(leaf)) {
      RexCall inner = (RexCall) ((RexCall) leaf).getOperands().get(0);
      return new BoolQueryBuilder().mustNot(toQueryBuilder(inner));
    }
    // Nested mixed subtree: if it's an AND/OR we can recurse; if it contains relevance but isn't
    // AND/OR, we can't handle it.
    if (leaf instanceof RexCall) {
      RexCall call = (RexCall) leaf;
      if (call.getKind() == SqlKind.OR) {
        return translateOrSubtree(call);
      }
      if (call.getKind() == SqlKind.AND) {
        if (containsRelevance(call)) {
          BoolQueryBuilder bool = new BoolQueryBuilder();
          for (RexNode child : call.getOperands()) {
            QueryBuilder sub = translateAnyLeaf(child);
            if (sub == null) {
              return null;
            }
            bool.must(sub);
          }
          return bool;
        }
      }
    }
    // Pure non-relevance leaf — delegate to the existing predicate translator.
    return luceneConverter.toQueryBuilder(leaf, inputRowType);
  }

  // ---- Helpers ----

  public static boolean isRelevanceCall(RexNode node) {
    if (!(node instanceof RexCall)) {
      return false;
    }
    RexCall call = (RexCall) node;
    if (call.getOperator().getKind() != SqlKind.OTHER_FUNCTION) {
      return false;
    }
    return RELEVANCE_FUNCTION_NAMES.contains(call.getOperator().getName().toLowerCase(Locale.ROOT));
  }

  private static boolean isNotOfRelevance(RexNode node) {
    if (!(node instanceof RexCall)) {
      return false;
    }
    RexCall call = (RexCall) node;
    if (call.getKind() != SqlKind.NOT) {
      return false;
    }
    return isRelevanceCall(call.getOperands().get(0));
  }

  /** Recursive scan — true when {@code node} (or any descendant) is a relevance RexCall. */
  public static boolean containsRelevance(RexNode node) {
    if (isRelevanceCall(node)) {
      return true;
    }
    if (!(node instanceof RexCall)) {
      return false;
    }
    for (RexNode operand : ((RexCall) node).getOperands()) {
      if (containsRelevance(operand)) {
        return true;
      }
    }
    return false;
  }

  private QueryBuilder toQueryBuilder(RexCall relevanceCall) {
    return RelevanceQueryBuilderFactory.build(relevanceCall, inputRowType);
  }

  private static QueryBuilder combineMust(List<QueryBuilder> clauses) {
    if (clauses.isEmpty()) {
      return null;
    }
    if (clauses.size() == 1) {
      return clauses.get(0);
    }
    BoolQueryBuilder bool = new BoolQueryBuilder();
    for (QueryBuilder c : clauses) {
      bool.must(c);
    }
    return bool;
  }

  /** Thrown internally when a rule-out condition is detected; caught in {@link #split}. */
  private static final class RuleOutException extends RuntimeException {
    RuleOutException(String message) {
      super(message);
    }
  }
}
