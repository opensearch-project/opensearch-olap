/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimestampString;
import org.opensearch.sql.calcite.type.AbstractExprRelDataType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;
import org.opensearch.sql.utils.DateTimeFormatters;

/**
 * Strips PPL's {@code timestamp()} coercion UDF from the plan so Velox can execute the underlying
 * comparison directly.
 *
 * <p>The SQL plugin wraps both sides of a comparison between an {@code EXPR_TIMESTAMP} UDT column
 * (e.g. {@code @timestamp}) and a string literal in {@code timestamp(...)} via {@link
 * org.opensearch.sql.expression.function.CoercionUtils}. Velox has no {@code timestamp(varchar)}
 * scalar, so the query fails when routed through the OLAP plugin.
 *
 * <p>This rewriter replaces:
 *
 * <ul>
 *   <li>{@code timestamp('yyyy-MM-dd HH:mm:ss[...]')} → Calcite {@code TIMESTAMP} literal
 *   <li>{@code timestamp(<TIMESTAMP/DATE/TIME expression>)} → the inner expression (identity in PPL
 *       semantics, since the UDF's {@code (TIMESTAMP) -> TIMESTAMP} signature is a no-op)
 *   <li>{@code timestamp(timestamp(x))} → inner rewritten first by the RexShuttle; outer then
 *       stripped by the identity rule because the inner result is TIMESTAMP-typed
 * </ul>
 *
 * <p>Shapes that can't be simplified at plan time (e.g. {@code timestamp(concat(x, y))}) are left
 * intact and rejected by {@link #isRewritable(org.apache.calcite.rex.RexCall)} — those queries fall
 * back to the default engine via {@code canVectorize()}.
 *
 * <p>After rewrite, both sides of the comparison are Velox {@code TimestampType} (UDT refs map
 * there via {@link org.opensearch.plugin.olap.plan.convert.VeloxTypeConverter}; TIMESTAMP literals
 * become {@code TimestampValue} variants), so the comparison binds to native {@code
 * greaterthanorequal(TIMESTAMP, TIMESTAMP)} etc.
 */
public final class TimestampUdfRewriter {

  private static final String TIMESTAMP_OP = "TIMESTAMP";

  private TimestampUdfRewriter() {}

  public static RelNode rewrite(RelNode root) {
    RexBuilder rexBuilder = root.getCluster().getRexBuilder();
    return root.accept(new RelShuttle(new RexRewriter(rexBuilder)));
  }

  /**
   * Returns true if {@code call} is a {@code timestamp(...)} UDF in a shape this rewriter can
   * simplify (string literal, date/time/timestamp UDT ref, already-TIMESTAMP-typed expression, or a
   * nested rewritable {@code timestamp(...)} that itself reduces to one of those). Used by {@code
   * canVectorize} so plans containing rewritable timestamp calls are not rejected before
   * optimization.
   */
  public static boolean isRewritable(RexCall call) {
    if (!isTimestampUdf(call) || call.getOperands().size() != 1) {
      return false;
    }
    RexNode operand = call.getOperands().get(0);
    // timestamp(<TIMESTAMP/DATE/TIME expression>) is an identity in PPL; the rewriter strips it.
    if (isTimestampCompatibleType(operand.getType())) {
      return true;
    }
    if (operand instanceof RexLiteral) {
      RexLiteral literal = (RexLiteral) operand;
      if (literal.isNull()) {
        return false;
      }
      SqlTypeName typeName = literal.getTypeName();
      if (typeName != SqlTypeName.CHAR && typeName != SqlTypeName.VARCHAR) {
        return false;
      }
      NlsString nls = literal.getValueAs(NlsString.class);
      return nls != null && RexRewriter.parseTimestampMillis(nls.getValue()) != null;
    }
    if (operand instanceof RexCall) {
      // Nested timestamp(...). Only rewritable if the inner call reduces to a TIMESTAMP-typed
      // node, because the outer wrapper can then be stripped as an identity. The inner call's
      // declared return type is TIMESTAMP (ExprTimeStampType UDT), so isTimestampCompatibleType
      // above already covers the common case; this branch handles any non-timestamp nested call.
      return isRewritable((RexCall) operand);
    }
    return false;
  }

  /** Applies a RexShuttle to each RelNode in the tree, recursing into inputs. */
  private static final class RelShuttle extends RelShuttleImpl {
    private final RexShuttle rexShuttle;

    RelShuttle(RexShuttle rexShuttle) {
      this.rexShuttle = rexShuttle;
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
      RelNode input = filter.getInput().accept(this);
      RelNode rewritten = filter.copy(filter.getTraitSet(), List.of(input));
      return rewritten.accept(rexShuttle);
    }

    @Override
    public RelNode visit(LogicalProject project) {
      RelNode input = project.getInput().accept(this);
      RelNode rewritten = project.copy(project.getTraitSet(), List.of(input));
      return rewritten.accept(rexShuttle);
    }

    @Override
    public RelNode visit(LogicalJoin join) {
      RelNode left = join.getLeft().accept(this);
      RelNode right = join.getRight().accept(this);
      RelNode rewritten = join.copy(join.getTraitSet(), List.of(left, right));
      return rewritten.accept(rexShuttle);
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
      RelNode input = aggregate.getInput().accept(this);
      return aggregate.copy(aggregate.getTraitSet(), List.of(input));
    }

    @Override
    public RelNode visit(LogicalSort sort) {
      RelNode input = sort.getInput().accept(this);
      return sort.copy(sort.getTraitSet(), List.of(input));
    }

    @Override
    public RelNode visit(RelNode other) {
      if (other instanceof Sort) {
        RelNode input = ((Sort) other).getInput().accept(this);
        return other.copy(other.getTraitSet(), List.of(input));
      }
      List<RelNode> inputs =
          other.getInputs().stream().map(i -> i.accept(this)).collect(Collectors.toList());
      RelNode rewritten = inputs.isEmpty() ? other : other.copy(other.getTraitSet(), inputs);
      return rewritten.accept(rexShuttle);
    }
  }

  /** Rewrites {@code timestamp(...)} calls in RexNode trees. */
  private static final class RexRewriter extends RexShuttle {
    private final RexBuilder rexBuilder;

    RexRewriter(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      RexCall rewrittenCall = (RexCall) super.visitCall(call);
      if (!isTimestampUdf(rewrittenCall)) {
        return rewrittenCall;
      }
      if (rewrittenCall.getOperands().size() != 1) {
        // Two-arg timestamp(datetime, addTime) has no plan-time shortcut.
        return rewrittenCall;
      }
      RexNode operand = rewrittenCall.getOperands().get(0);
      RexNode simplified = simplify(operand);
      return simplified != null ? simplified : rewrittenCall;
    }

    private RexNode simplify(RexNode operand) {
      // timestamp(<TIMESTAMP/DATE/TIME expression>) is an identity in PPL — strip the wrapper.
      // This covers @timestamp refs (UDT), already-parsed TIMESTAMP literals, and the outer
      // layer of nested timestamp(timestamp(...)) after the inner call is rewritten.
      if (isTimestampCompatibleType(operand.getType())) {
        return operand;
      }
      if (operand instanceof RexLiteral) {
        return simplifyLiteral((RexLiteral) operand);
      }
      return null;
    }

    private RexNode simplifyLiteral(RexLiteral literal) {
      if (literal.isNull()) {
        return null;
      }
      SqlTypeName typeName = literal.getTypeName();
      if (typeName != SqlTypeName.CHAR && typeName != SqlTypeName.VARCHAR) {
        return null;
      }
      NlsString nls = literal.getValueAs(NlsString.class);
      if (nls == null) {
        return null;
      }
      Long millis = parseTimestampMillis(nls.getValue());
      if (millis == null) {
        return null;
      }
      // Emit a TIMESTAMP literal (3 = millisecond precision) so it matches the Velox
      // TimestampType on the other side of the comparison. VeloxExprConverter converts
      // this literal into a TimestampValue variant; LuceneFilterConverter extracts the
      // millis back when pushing the predicate to Lucene.
      return rexBuilder.makeTimestampLiteral(TimestampString.fromMillisSinceEpoch(millis), 3);
    }

    static Long parseTimestampMillis(String text) {
      try {
        LocalDateTime ldt = LocalDateTime.parse(text, DateTimeFormatters.DATE_TIMESTAMP_FORMATTER);
        return ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
      } catch (DateTimeParseException ignored) {
        // Fall through to ISO 8601 attempt.
      }
      try {
        ZonedDateTime zdt = ZonedDateTime.parse(text, DateTimeFormatter.ISO_DATE_TIME);
        return zdt.toInstant().toEpochMilli();
      } catch (DateTimeParseException ignored) {
        return null;
      }
    }
  }

  private static boolean isTimestampUdf(RexCall call) {
    return TIMESTAMP_OP.equals(call.getOperator().getName().toUpperCase(Locale.ROOT));
  }

  private static boolean isDateTimeUdt(RelDataType type) {
    if (!(type instanceof AbstractExprRelDataType<?>)) {
      return false;
    }
    ExprUDT udt = ((AbstractExprRelDataType<?>) type).getUdt();
    return udt == ExprUDT.EXPR_TIMESTAMP || udt == ExprUDT.EXPR_DATE || udt == ExprUDT.EXPR_TIME;
  }

  /**
   * True when {@code type} is already a timestamp-compatible Calcite type — either a PPL datetime
   * UDT or a plain {@code TIMESTAMP}/{@code DATE}/{@code TIME}. Used to decide whether wrapping
   * such a value in {@code timestamp(...)} is an identity that we can strip.
   */
  private static boolean isTimestampCompatibleType(RelDataType type) {
    if (isDateTimeUdt(type)) {
      return true;
    }
    SqlTypeName sqlTypeName = type.getSqlTypeName();
    return sqlTypeName == SqlTypeName.TIMESTAMP
        || sqlTypeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
        || sqlTypeName == SqlTypeName.DATE
        || sqlTypeName == SqlTypeName.TIME;
  }
}
