/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.convert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.index.query.MatchBoolPrefixQueryBuilder;
import org.opensearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.index.query.SimpleQueryStringFlag;
import org.opensearch.index.search.MatchQuery;

/**
 * Builds OpenSearch {@link QueryBuilder}s from PPL relevance-function RexCalls.
 *
 * <p>The SQL plugin lowers {@code match(field, 'x', operator='AND', boost=2.0)} into a RexCall with
 * SqlKind.OTHER_FUNCTION, operator name {@code "MATCH"}, and operands shaped as:
 *
 * <pre>
 *   operand[i] = RexCall(MAP_VALUE_CONSTRUCTOR, aliasLiteral, valueRex)
 * </pre>
 *
 * where {@code aliasLiteral} is a VARCHAR like {@code "field"}, {@code "query"}, or an option name,
 * and {@code valueRex} is the corresponding RexNode (RexInputRef for fields, RexLiteral for
 * everything else). For {@code multi_match}/{@code query_string}/{@code simple_query_string}, the
 * {@code fields} operand's value is itself a nested {@code MAP_VALUE_CONSTRUCTOR} RexCall with
 * interleaved ({@code fieldName}, {@code boost}) literals.
 *
 * <p>Options are applied via a per-function dispatch table modeled after the SQL plugin's {@code
 * FunctionParameterRepository}. Unknown options throw {@link IllegalArgumentException} so the
 * caller ({@link RelevanceSplitter}) can rule the plan out and fall back to the default engine.
 */
public final class RelevanceQueryBuilderFactory {

  private RelevanceQueryBuilderFactory() {}

  // ---- Per-function option dispatch tables ----

  private static final Map<String, BiConsumer<MatchQueryBuilder, String>> MATCH_OPTS =
      new HashMap<>();

  static {
    MATCH_OPTS.put("analyzer", (b, v) -> b.analyzer(v));
    MATCH_OPTS.put(
        "auto_generate_synonyms_phrase_query",
        (b, v) -> b.autoGenerateSynonymsPhraseQuery(Boolean.parseBoolean(v)));
    MATCH_OPTS.put("boost", (b, v) -> b.boost(Float.parseFloat(v)));
    MATCH_OPTS.put("fuzziness", (b, v) -> b.fuzziness(Fuzziness.build(v)));
    MATCH_OPTS.put("fuzzy_rewrite", (b, v) -> b.fuzzyRewrite(v));
    MATCH_OPTS.put(
        "fuzzy_transpositions", (b, v) -> b.fuzzyTranspositions(Boolean.parseBoolean(v)));
    MATCH_OPTS.put("lenient", (b, v) -> b.lenient(Boolean.parseBoolean(v)));
    MATCH_OPTS.put("max_expansions", (b, v) -> b.maxExpansions(Integer.parseInt(v)));
    MATCH_OPTS.put("minimum_should_match", (b, v) -> b.minimumShouldMatch(v));
    MATCH_OPTS.put("operator", (b, v) -> b.operator(parseOperator(v)));
    MATCH_OPTS.put("prefix_length", (b, v) -> b.prefixLength(Integer.parseInt(v)));
    MATCH_OPTS.put("zero_terms_query", (b, v) -> b.zeroTermsQuery(parseZeroTermsQuery(v)));
  }

  private static final Map<String, BiConsumer<MatchPhraseQueryBuilder, String>> MATCH_PHRASE_OPTS =
      new HashMap<>();

  static {
    MATCH_PHRASE_OPTS.put("analyzer", (b, v) -> b.analyzer(v));
    MATCH_PHRASE_OPTS.put("boost", (b, v) -> b.boost(Float.parseFloat(v)));
    MATCH_PHRASE_OPTS.put("slop", (b, v) -> b.slop(Integer.parseInt(v)));
    MATCH_PHRASE_OPTS.put("zero_terms_query", (b, v) -> b.zeroTermsQuery(parseZeroTermsQuery(v)));
  }

  private static final Map<String, BiConsumer<MatchPhrasePrefixQueryBuilder, String>>
      MATCH_PHRASE_PREFIX_OPTS = new HashMap<>();

  static {
    MATCH_PHRASE_PREFIX_OPTS.put("analyzer", (b, v) -> b.analyzer(v));
    MATCH_PHRASE_PREFIX_OPTS.put("boost", (b, v) -> b.boost(Float.parseFloat(v)));
    MATCH_PHRASE_PREFIX_OPTS.put("max_expansions", (b, v) -> b.maxExpansions(Integer.parseInt(v)));
    MATCH_PHRASE_PREFIX_OPTS.put("slop", (b, v) -> b.slop(Integer.parseInt(v)));
    MATCH_PHRASE_PREFIX_OPTS.put(
        "zero_terms_query", (b, v) -> b.zeroTermsQuery(parseZeroTermsQuery(v)));
  }

  private static final Map<String, BiConsumer<MatchBoolPrefixQueryBuilder, String>>
      MATCH_BOOL_PREFIX_OPTS = new HashMap<>();

  static {
    MATCH_BOOL_PREFIX_OPTS.put("analyzer", (b, v) -> b.analyzer(v));
    MATCH_BOOL_PREFIX_OPTS.put("boost", (b, v) -> b.boost(Float.parseFloat(v)));
    MATCH_BOOL_PREFIX_OPTS.put("fuzziness", (b, v) -> b.fuzziness(Fuzziness.build(v)));
    MATCH_BOOL_PREFIX_OPTS.put("fuzzy_rewrite", (b, v) -> b.fuzzyRewrite(v));
    MATCH_BOOL_PREFIX_OPTS.put(
        "fuzzy_transpositions", (b, v) -> b.fuzzyTranspositions(Boolean.parseBoolean(v)));
    MATCH_BOOL_PREFIX_OPTS.put("max_expansions", (b, v) -> b.maxExpansions(Integer.parseInt(v)));
    MATCH_BOOL_PREFIX_OPTS.put("minimum_should_match", (b, v) -> b.minimumShouldMatch(v));
    MATCH_BOOL_PREFIX_OPTS.put("operator", (b, v) -> b.operator(parseOperator(v)));
    MATCH_BOOL_PREFIX_OPTS.put("prefix_length", (b, v) -> b.prefixLength(Integer.parseInt(v)));
  }

  private static final Map<String, BiConsumer<MultiMatchQueryBuilder, String>> MULTI_MATCH_OPTS =
      new HashMap<>();

  static {
    MULTI_MATCH_OPTS.put("analyzer", (b, v) -> b.analyzer(v));
    MULTI_MATCH_OPTS.put(
        "auto_generate_synonyms_phrase_query",
        (b, v) -> b.autoGenerateSynonymsPhraseQuery(Boolean.parseBoolean(v)));
    MULTI_MATCH_OPTS.put("boost", (b, v) -> b.boost(Float.parseFloat(v)));
    MULTI_MATCH_OPTS.put("cutoff_frequency", (b, v) -> b.cutoffFrequency(Float.parseFloat(v)));
    MULTI_MATCH_OPTS.put("fuzziness", (b, v) -> b.fuzziness(Fuzziness.build(v)));
    MULTI_MATCH_OPTS.put(
        "fuzzy_transpositions", (b, v) -> b.fuzzyTranspositions(Boolean.parseBoolean(v)));
    MULTI_MATCH_OPTS.put("lenient", (b, v) -> b.lenient(Boolean.parseBoolean(v)));
    MULTI_MATCH_OPTS.put("max_expansions", (b, v) -> b.maxExpansions(Integer.parseInt(v)));
    MULTI_MATCH_OPTS.put("minimum_should_match", (b, v) -> b.minimumShouldMatch(v));
    MULTI_MATCH_OPTS.put("operator", (b, v) -> b.operator(parseOperator(v)));
    MULTI_MATCH_OPTS.put("prefix_length", (b, v) -> b.prefixLength(Integer.parseInt(v)));
    MULTI_MATCH_OPTS.put("slop", (b, v) -> b.slop(Integer.parseInt(v)));
    MULTI_MATCH_OPTS.put("tie_breaker", (b, v) -> b.tieBreaker(Float.parseFloat(v)));
    MULTI_MATCH_OPTS.put(
        "type",
        (b, v) -> b.type(MultiMatchQueryBuilder.Type.parse(v.toLowerCase(Locale.ROOT), null)));
    MULTI_MATCH_OPTS.put("zero_terms_query", (b, v) -> b.zeroTermsQuery(parseZeroTermsQuery(v)));
  }

  private static final Map<String, BiConsumer<SimpleQueryStringBuilder, String>>
      SIMPLE_QUERY_STRING_OPTS = new HashMap<>();

  static {
    SIMPLE_QUERY_STRING_OPTS.put(
        "analyze_wildcard", (b, v) -> b.analyzeWildcard(Boolean.parseBoolean(v)));
    SIMPLE_QUERY_STRING_OPTS.put("analyzer", (b, v) -> b.analyzer(v));
    SIMPLE_QUERY_STRING_OPTS.put(
        "auto_generate_synonyms_phrase_query",
        (b, v) -> b.autoGenerateSynonymsPhraseQuery(Boolean.parseBoolean(v)));
    SIMPLE_QUERY_STRING_OPTS.put("boost", (b, v) -> b.boost(Float.parseFloat(v)));
    SIMPLE_QUERY_STRING_OPTS.put("default_operator", (b, v) -> b.defaultOperator(parseOperator(v)));
    SIMPLE_QUERY_STRING_OPTS.put("flags", (b, v) -> b.flags(parseSimpleQueryStringFlags(v)));
    SIMPLE_QUERY_STRING_OPTS.put(
        "fuzzy_max_expansions", (b, v) -> b.fuzzyMaxExpansions(Integer.parseInt(v)));
    SIMPLE_QUERY_STRING_OPTS.put(
        "fuzzy_prefix_length", (b, v) -> b.fuzzyPrefixLength(Integer.parseInt(v)));
    SIMPLE_QUERY_STRING_OPTS.put(
        "fuzzy_transpositions", (b, v) -> b.fuzzyTranspositions(Boolean.parseBoolean(v)));
    SIMPLE_QUERY_STRING_OPTS.put("lenient", (b, v) -> b.lenient(Boolean.parseBoolean(v)));
    SIMPLE_QUERY_STRING_OPTS.put("minimum_should_match", (b, v) -> b.minimumShouldMatch(v));
    SIMPLE_QUERY_STRING_OPTS.put("quote_field_suffix", (b, v) -> b.quoteFieldSuffix(v));
  }

  private static final Map<String, BiConsumer<QueryStringQueryBuilder, String>> QUERY_STRING_OPTS =
      new HashMap<>();

  static {
    QUERY_STRING_OPTS.put(
        "allow_leading_wildcard", (b, v) -> b.allowLeadingWildcard(Boolean.parseBoolean(v)));
    QUERY_STRING_OPTS.put("analyze_wildcard", (b, v) -> b.analyzeWildcard(Boolean.parseBoolean(v)));
    QUERY_STRING_OPTS.put("analyzer", (b, v) -> b.analyzer(v));
    QUERY_STRING_OPTS.put(
        "auto_generate_synonyms_phrase_query",
        (b, v) -> b.autoGenerateSynonymsPhraseQuery(Boolean.parseBoolean(v)));
    QUERY_STRING_OPTS.put("boost", (b, v) -> b.boost(Float.parseFloat(v)));
    QUERY_STRING_OPTS.put("default_operator", (b, v) -> b.defaultOperator(parseOperator(v)));
    QUERY_STRING_OPTS.put(
        "enable_position_increments",
        (b, v) -> b.enablePositionIncrements(Boolean.parseBoolean(v)));
    QUERY_STRING_OPTS.put("escape", (b, v) -> b.escape(Boolean.parseBoolean(v)));
    QUERY_STRING_OPTS.put("fuzziness", (b, v) -> b.fuzziness(Fuzziness.build(v)));
    QUERY_STRING_OPTS.put(
        "fuzzy_max_expansions", (b, v) -> b.fuzzyMaxExpansions(Integer.parseInt(v)));
    QUERY_STRING_OPTS.put(
        "fuzzy_prefix_length", (b, v) -> b.fuzzyPrefixLength(Integer.parseInt(v)));
    QUERY_STRING_OPTS.put("fuzzy_rewrite", (b, v) -> b.fuzzyRewrite(v));
    QUERY_STRING_OPTS.put(
        "fuzzy_transpositions", (b, v) -> b.fuzzyTranspositions(Boolean.parseBoolean(v)));
    QUERY_STRING_OPTS.put("lenient", (b, v) -> b.lenient(Boolean.parseBoolean(v)));
    QUERY_STRING_OPTS.put(
        "max_determinized_states", (b, v) -> b.maxDeterminizedStates(Integer.parseInt(v)));
    QUERY_STRING_OPTS.put("minimum_should_match", (b, v) -> b.minimumShouldMatch(v));
    QUERY_STRING_OPTS.put("phrase_slop", (b, v) -> b.phraseSlop(Integer.parseInt(v)));
    QUERY_STRING_OPTS.put("quote_analyzer", (b, v) -> b.quoteAnalyzer(v));
    QUERY_STRING_OPTS.put("quote_field_suffix", (b, v) -> b.quoteFieldSuffix(v));
    QUERY_STRING_OPTS.put("rewrite", (b, v) -> b.rewrite(v));
    QUERY_STRING_OPTS.put("tie_breaker", (b, v) -> b.tieBreaker(Float.parseFloat(v)));
    QUERY_STRING_OPTS.put("time_zone", (b, v) -> b.timeZone(v));
    QUERY_STRING_OPTS.put(
        "type",
        (b, v) -> b.type(MultiMatchQueryBuilder.Type.parse(v.toLowerCase(Locale.ROOT), null)));
  }

  // ---- Entry point ----

  /**
   * Translate a PPL relevance-function RexCall to an OpenSearch {@link QueryBuilder}.
   *
   * @throws IllegalArgumentException on unknown option name, missing required argument, or
   *     malformed operand (the caller treats this as "can't push down, fall back").
   */
  public static QueryBuilder build(RexCall call, RelDataType inputRowType) {
    String funcName = call.getOperator().getName().toLowerCase(Locale.ROOT);
    Map<String, RexNode> args = decodeOperands(call, funcName);
    switch (funcName) {
      case "match":
        return buildSingleField(args, funcName, inputRowType, MatchQueryBuilder::new, MATCH_OPTS);
      case "match_phrase":
        return buildSingleField(
            args, funcName, inputRowType, MatchPhraseQueryBuilder::new, MATCH_PHRASE_OPTS);
      case "match_phrase_prefix":
        return buildSingleField(
            args,
            funcName,
            inputRowType,
            MatchPhrasePrefixQueryBuilder::new,
            MATCH_PHRASE_PREFIX_OPTS);
      case "match_bool_prefix":
        return buildSingleField(
            args, funcName, inputRowType, MatchBoolPrefixQueryBuilder::new, MATCH_BOOL_PREFIX_OPTS);
      case "multi_match":
        return buildMultiMatch(args, inputRowType);
      case "simple_query_string":
        return buildSimpleQueryString(args, inputRowType);
      case "query_string":
        return buildQueryString(args, inputRowType);
      default:
        throw new IllegalArgumentException("Unknown relevance function: " + funcName);
    }
  }

  // ---- Operand decoding ----

  /**
   * Walk a relevance call's operands (each wrapped in {@code MAP_VALUE_CONSTRUCTOR}) and return a
   * map from argument name (lower-cased) to the argument value RexNode.
   */
  private static Map<String, RexNode> decodeOperands(RexCall call, String funcName) {
    Map<String, RexNode> result = new HashMap<>();
    for (RexNode operand : call.getOperands()) {
      if (!(operand instanceof RexCall)
          || ((RexCall) operand).getOperator().getKind() != SqlKind.MAP_VALUE_CONSTRUCTOR) {
        throw new IllegalArgumentException(
            "Unexpected operand shape in "
                + funcName
                + ": expected MAP_VALUE_CONSTRUCTOR, got "
                + operand);
      }
      RexCall mapCall = (RexCall) operand;
      List<RexNode> pair = mapCall.getOperands();
      if (pair.size() != 2 || !(pair.get(0) instanceof RexLiteral)) {
        throw new IllegalArgumentException(
            "Malformed named-argument operand in " + funcName + ": " + mapCall);
      }
      String name = ((RexLiteral) pair.get(0)).getValueAs(String.class);
      if (name == null) {
        throw new IllegalArgumentException(
            "Null argument name in " + funcName + " operand " + mapCall);
      }
      if (result.put(name.toLowerCase(Locale.ROOT), pair.get(1)) != null) {
        throw new IllegalArgumentException(
            "Parameter '" + name + "' specified more than once in " + funcName);
      }
    }
    return result;
  }

  private static String requireStringLiteral(RexNode node, String argName, String funcName) {
    if (!(node instanceof RexLiteral)) {
      throw new IllegalArgumentException(
          argName + " must be a literal in " + funcName + ": " + node);
    }
    String v = ((RexLiteral) node).getValueAs(String.class);
    if (v == null) {
      throw new IllegalArgumentException(argName + " must be non-null in " + funcName);
    }
    return v;
  }

  /** Resolve a field-reference RexNode to its name using the input row type. */
  private static String resolveFieldName(RexNode node, String argName, RelDataType inputRowType) {
    if (node instanceof RexInputRef) {
      return inputRowType.getFieldList().get(((RexInputRef) node).getIndex()).getName();
    }
    // Some shapes encode the field as a VARCHAR literal (e.g., when the SQL plugin's analyzer
    // can resolve it). Accept both.
    if (node instanceof RexLiteral) {
      return ((RexLiteral) node).getValueAs(String.class);
    }
    throw new IllegalArgumentException("Cannot resolve " + argName + ": " + node);
  }

  // ---- Single-field functions ----

  @FunctionalInterface
  private interface SingleFieldCtor<B extends QueryBuilder> {
    B create(String field, Object query);
  }

  private static <B extends QueryBuilder> B buildSingleField(
      Map<String, RexNode> args,
      String funcName,
      RelDataType inputRowType,
      SingleFieldCtor<B> ctor,
      Map<String, BiConsumer<B, String>> options) {
    RexNode fieldNode = args.remove("field");
    if (fieldNode == null) {
      throw new IllegalArgumentException("'field' is required for " + funcName);
    }
    RexNode queryNode = args.remove("query");
    if (queryNode == null) {
      throw new IllegalArgumentException("'query' is required for " + funcName);
    }
    String field = resolveFieldName(fieldNode, "field", inputRowType);
    String query = requireStringLiteral(queryNode, "query", funcName);
    B builder = ctor.create(field, query);
    applyOptions(args, options, builder, funcName);
    return builder;
  }

  // ---- Multi-field functions ----

  private static QueryBuilder buildMultiMatch(Map<String, RexNode> args, RelDataType inputRowType) {
    RexNode queryNode = args.remove("query");
    if (queryNode == null) {
      throw new IllegalArgumentException("'query' is required for multi_match");
    }
    String query = requireStringLiteral(queryNode, "query", "multi_match");
    MultiMatchQueryBuilder builder = new MultiMatchQueryBuilder(query);
    RexNode fieldsNode = args.remove("fields");
    if (fieldsNode != null) {
      for (Map.Entry<String, Float> e : decodeFieldsMap(fieldsNode, "multi_match").entrySet()) {
        builder.field(e.getKey(), e.getValue());
      }
    }
    applyOptions(args, MULTI_MATCH_OPTS, builder, "multi_match");
    return builder;
  }

  private static QueryBuilder buildSimpleQueryString(
      Map<String, RexNode> args, RelDataType inputRowType) {
    RexNode queryNode = args.remove("query");
    if (queryNode == null) {
      throw new IllegalArgumentException("'query' is required for simple_query_string");
    }
    String query = requireStringLiteral(queryNode, "query", "simple_query_string");
    SimpleQueryStringBuilder builder = new SimpleQueryStringBuilder(query);
    RexNode fieldsNode = args.remove("fields");
    if (fieldsNode != null) {
      for (Map.Entry<String, Float> e :
          decodeFieldsMap(fieldsNode, "simple_query_string").entrySet()) {
        builder.field(e.getKey(), e.getValue());
      }
    }
    applyOptions(args, SIMPLE_QUERY_STRING_OPTS, builder, "simple_query_string");
    return builder;
  }

  private static QueryBuilder buildQueryString(
      Map<String, RexNode> args, RelDataType inputRowType) {
    RexNode queryNode = args.remove("query");
    if (queryNode == null) {
      throw new IllegalArgumentException("'query' is required for query_string");
    }
    String query = requireStringLiteral(queryNode, "query", "query_string");
    QueryStringQueryBuilder builder = new QueryStringQueryBuilder(query);
    RexNode fieldsNode = args.remove("fields");
    if (fieldsNode != null) {
      for (Map.Entry<String, Float> e : decodeFieldsMap(fieldsNode, "query_string").entrySet()) {
        builder.field(e.getKey(), e.getValue());
      }
    }
    applyOptions(args, QUERY_STRING_OPTS, builder, "query_string");
    return builder;
  }

  /**
   * Decode the {@code fields} operand for multi-field functions. The SQL plugin's {@code
   * visitRelevanceFieldList} emits a {@code MAP_VALUE_CONSTRUCTOR} RexCall with interleaved
   * (VARCHAR fieldName, DOUBLE boost) literals.
   */
  private static Map<String, Float> decodeFieldsMap(RexNode fieldsNode, String funcName) {
    if (!(fieldsNode instanceof RexCall)
        || ((RexCall) fieldsNode).getOperator().getKind() != SqlKind.MAP_VALUE_CONSTRUCTOR) {
      throw new IllegalArgumentException(
          "Expected MAP_VALUE_CONSTRUCTOR for 'fields' in " + funcName + ": " + fieldsNode);
    }
    List<RexNode> ops = ((RexCall) fieldsNode).getOperands();
    if ((ops.size() % 2) != 0) {
      throw new IllegalArgumentException(
          "'fields' map must have even arity in " + funcName + ": " + fieldsNode);
    }
    Map<String, Float> result = new HashMap<>();
    for (int i = 0; i < ops.size(); i += 2) {
      if (!(ops.get(i) instanceof RexLiteral) || !(ops.get(i + 1) instanceof RexLiteral)) {
        throw new IllegalArgumentException(
            "'fields' entries must be literals in " + funcName + ": " + fieldsNode);
      }
      String name = ((RexLiteral) ops.get(i)).getValueAs(String.class);
      Double boost = ((RexLiteral) ops.get(i + 1)).getValueAs(Double.class);
      result.put(name, boost == null ? 1.0f : boost.floatValue());
    }
    return result;
  }

  // ---- Option application ----

  private static <B extends QueryBuilder> void applyOptions(
      Map<String, RexNode> args,
      Map<String, BiConsumer<B, String>> options,
      B builder,
      String funcName) {
    for (Map.Entry<String, RexNode> e : args.entrySet()) {
      BiConsumer<B, String> setter = options.get(e.getKey());
      if (setter == null) {
        throw new IllegalArgumentException(
            "Unsupported option '" + e.getKey() + "' for " + funcName);
      }
      String value = requireStringLiteral(e.getValue(), e.getKey(), funcName);
      setter.accept(builder, value);
    }
  }

  // ---- Value coercions ----

  private static Operator parseOperator(String v) {
    return Operator.fromString(v);
  }

  private static MatchQuery.ZeroTermsQuery parseZeroTermsQuery(String v) {
    return MatchQuery.ZeroTermsQuery.valueOf(v.toUpperCase(Locale.ROOT));
  }

  private static SimpleQueryStringFlag[] parseSimpleQueryStringFlags(String v) {
    String[] parts = v.split("\\|");
    List<SimpleQueryStringFlag> flags = new ArrayList<>(parts.length);
    for (String part : parts) {
      flags.add(SimpleQueryStringFlag.valueOf(part.trim().toUpperCase(Locale.ROOT)));
    }
    return flags.toArray(new SimpleQueryStringFlag[0]);
  }
}
