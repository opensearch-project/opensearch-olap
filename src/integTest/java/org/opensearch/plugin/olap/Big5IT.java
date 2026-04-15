/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.json.JSONObject;

/**
 * Big5 benchmark integration tests. Migrated from the SQL plugin's PPLBig5IT. Loads the big5 log
 * dataset and runs all 58 benchmark queries.
 *
 * <p>Each test loads a PPL query from a .ppl resource file (same format as the SQL plugin),
 * executes it, and verifies the result is valid. Queries run through Velox when canVectorize
 * returns true, or fall back to the default engine otherwise (e.g. when fields have unsupported
 * types like ANY).
 */
public class Big5IT extends OlapRestTestCase {

  private static final Map<String, Long> timings = new LinkedHashMap<>();

  @Override
  public void setUp() throws Exception {
    super.setUp();
    // Big5 data has nested object fields (cloud, aws, agent, etc.) that the SQL plugin's Calcite
    // schema maps as MAP<VARCHAR, ANY>. Velox can't handle MAP/ANY types, so these queries fall
    // back to the default engine. Disable force_vectorize for big5 tests.
    // TODO: Handle MAP type by flattening nested objects into individual columns.
    setClusterSetting("plugins.velox.force_vectorize", false);
    loadIndex(Index.BIG5);
  }

  @Override
  public void tearDown() throws Exception {
    deleteIndex(Index.BIG5.getName());
    if (!timings.isEmpty()) {
      long total = timings.values().stream().mapToLong(Long::longValue).sum();
      logger.info("Big5 timings:");
      timings.forEach((name, ms) -> logger.info("  {}: {} ms", name, ms));
      logger.info("  Total {} queries, avg {} ms", timings.size(), total / timings.size());
    }
    super.tearDown();
  }

  // ---- Scan / Filter ----

  public void testDefault() throws IOException {
    runQuery("default");
  }

  public void testTerm() throws IOException {
    runQuery("term");
  }

  public void testRange() throws IOException {
    runQuery("range");
  }

  public void testRangeNumeric() throws IOException {
    runQuery("range_numeric");
  }

  public void testScroll() throws IOException {
    runQuery("scroll");
  }

  public void testKeywordInRange() throws IOException {
    runQuery("keyword_in_range");
  }

  public void testRangeFieldConjunctionBigRangeBigTermQuery() throws IOException {
    runQuery("range_field_conjunction_big_range_big_term_query");
  }

  public void testRangeFieldConjunctionSmallRangeBigTermQuery() throws IOException {
    runQuery("range_field_conjunction_small_range_big_term_query");
  }

  public void testRangeFieldConjunctionSmallRangeSmallTermQuery() throws IOException {
    runQuery("range_field_conjunction_small_range_small_term_query");
  }

  public void testRangeFieldDisjunctionBigRangeSmallTermQuery() throws IOException {
    runQuery("range_field_disjunction_big_range_small_term_query");
  }

  // ---- Sort ----

  public void testSortNumericAsc() throws IOException {
    runQuery("sort_numeric_asc");
  }

  public void testSortNumericDesc() throws IOException {
    runQuery("sort_numeric_desc");
  }

  public void testSortNumericAscWithMatch() throws IOException {
    runQuery("sort_numeric_asc_with_match");
  }

  public void testSortNumericDescWithMatch() throws IOException {
    runQuery("sort_numeric_desc_with_match");
  }

  public void testAscSortTimestamp() throws IOException {
    runQuery("asc_sort_timestamp");
  }

  public void testDescSortTimestamp() throws IOException {
    runQuery("desc_sort_timestamp");
  }

  public void testAscSortTimestampCanMatchShortcut() throws IOException {
    runQuery("asc_sort_timestamp_can_match_shortcut");
  }

  public void testAscSortTimestampNoCanMatchShortcut() throws IOException {
    runQuery("asc_sort_timestamp_no_can_match_shortcut");
  }

  public void testDescSortTimestampCanMatchShortcut() throws IOException {
    runQuery("desc_sort_timestamp_can_match_shortcut");
  }

  public void testDescSortTimestampNoCanMatchShortcut() throws IOException {
    runQuery("desc_sort_timestamp_no_can_match_shortcut");
  }

  public void testAscSortWithAfterTimestamp() throws IOException {
    runQuery("asc_sort_with_after_timestamp");
  }

  public void testDescSortWithAfterTimestamp() throws IOException {
    runQuery("desc_sort_with_after_timestamp");
  }

  public void testSortKeywordCanMatchShortcut() throws IOException {
    runQuery("sort_keyword_can_match_shortcut");
  }

  public void testSortKeywordNoCanMatchShortcut() throws IOException {
    runQuery("sort_keyword_no_can_match_shortcut");
  }

  public void testRangeWithAscSort() throws IOException {
    runQuery("range_with_asc_sort");
  }

  public void testRangeWithDescSort() throws IOException {
    runQuery("range_with_desc_sort");
  }

  // ---- Aggregation ----

  public void testKeywordTerms() throws IOException {
    runQuery("keyword_terms");
  }

  public void testKeywordTermsLowCardinality() throws IOException {
    runQuery("keyword_terms_low_cardinality");
  }

  public void testMultiTermsKeyword() throws IOException {
    runQuery("multi_terms_keyword");
  }

  public void testCompositeTerms() throws IOException {
    runQuery("composite_terms");
  }

  public void testCompositeTermsKeyword() throws IOException {
    runQuery("composite_terms_keyword");
  }

  public void testCompositeDateHistogramDaily() throws IOException {
    runQuery("composite_date_histogram_daily");
  }

  public void testDateHistogramHourlyAgg() throws IOException {
    runQuery("date_histogram_hourly_agg");
  }

  public void testDateHistogramMinuteAgg() throws IOException {
    runQuery("date_histogram_minute_agg");
  }

  public void testRangeAgg1() throws IOException {
    runQuery("range_agg_1");
  }

  public void testRangeAgg2() throws IOException {
    runQuery("range_agg_2");
  }

  public void testRangeAutoDateHisto() throws IOException {
    runQuery("range_auto_date_histo");
  }

  public void testRangeAutoDateHistoWithMetrics() throws IOException {
    runQuery("range_auto_date_histo_with_metrics");
  }

  public void testCardinalityAggHigh() throws IOException {
    runQuery("cardinality_agg_high");
  }

  public void testCardinalityAggHigh2() throws IOException {
    runQuery("cardinality_agg_high_2");
  }

  public void testCardinalityAggLow() throws IOException {
    runQuery("cardinality_agg_low");
  }

  public void testTermsSignificant1() throws IOException {
    runQuery("terms_significant_1");
  }

  public void testTermsSignificant2() throws IOException {
    runQuery("terms_significant_2");
  }

  // ---- Full-text / Relevance ----

  public void testQueryStringOnMessage() throws IOException {
    runQuery("query_string_on_message");
  }

  public void testQueryStringOnMessageFiltered() throws IOException {
    runQuery("query_string_on_message_filtered");
  }

  public void testQueryStringOnMessageFilteredSortedNum() throws IOException {
    runQuery("query_string_on_message_filtered_sorted_num");
  }

  // ---- Pattern / Regex / Script ----

  public void testRexRegexTransformation() throws IOException {
    runQuery("rex_regex_transformation");
  }

  public void testScriptEngineLikePatternWithAggregation() throws IOException {
    runQuery("script_engine_like_pattern_with_aggregation");
  }

  public void testScriptEngineLikePatternWithSort() throws IOException {
    runQuery("script_engine_like_pattern_with_sort");
  }

  // ---- Dedup ----

  public void testDedupMetricsSizeField() throws IOException {
    runQuery("dedup_metrics_size_field");
  }

  // ---- Bin / Span ----

  public void testBinBins() throws IOException {
    runQuery("bin_bins");
  }

  public void testBinSpanLog() throws IOException {
    runQuery("bin_span_log");
  }

  public void testBinSpanTime() throws IOException {
    runQuery("bin_span_time");
  }

  // ---- Coalesce / Fields / Table ----

  public void testCoalesceNonexistentFieldFallback() throws IOException {
    runQuery("coalesce_nonexistent_field_fallback");
  }

  public void testFieldsFullWildcard() throws IOException {
    runQuery("fields_full_wildcard");
  }

  public void testFieldsMixedDelimiters() throws IOException {
    runQuery("fields_mixed_delimiters");
  }

  public void testTableFullWildcard() throws IOException {
    runQuery("table_full_wildcard");
  }

  public void testTableMixedDelimiters() throws IOException {
    runQuery("table_mixed_delimiters");
  }

  // ---- Core helpers ----

  private void runQuery(String queryName) throws IOException {
    String ppl = loadPplQuery(queryName);
    long start = System.currentTimeMillis();
    JSONObject response = executePPLQuery(ppl);
    long elapsed = System.currentTimeMillis() - start;
    timings.put(queryName, elapsed);

    assertTrue(
        "Query " + queryName + " should return datarows",
        response.has("datarows") || response.has("total"));
    logger.info("Big5 query [{}]: {} ms", queryName, elapsed);
  }

  /** Load and sanitize a PPL query: strip comments, collapse newlines to spaces. */
  private String loadPplQuery(String queryName) throws IOException {
    String raw = readResource("big5/queries/" + queryName + ".ppl");
    // Strip /* ... */ comment blocks
    raw = raw.replaceAll("(?s)/\\*.*?\\*/", "");
    // Collapse newlines to spaces (PPL parser expects single-line)
    raw = raw.replaceAll("\\r\\n", " ").replaceAll("\\n", " ").trim();
    // Unescape backslash-escaped quotes from .ppl files (e.g. \" → ")
    return raw.replace("\\\"", "\"");
  }
}
