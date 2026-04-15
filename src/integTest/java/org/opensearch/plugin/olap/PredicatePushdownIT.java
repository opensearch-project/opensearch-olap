/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.client.Request;

/**
 * Integration tests for predicate pushdown to Lucene. These queries include WHERE filters that the
 * OLAP plugin should push down to Lucene's inverted index / BKD tree before reading doc values.
 *
 * <p>The Velox FilterNode is kept as a safety net, so correctness is guaranteed regardless of
 * whether pushdown actually activates. These tests verify end-to-end correctness of results when
 * the pushdown path is exercised.
 *
 * <p>Test data (5 rows):
 *
 * <pre>
 *   Alice,   35, Seattle,  120000
 *   Bob,     28, Portland,  95000
 *   Charlie, 42, Seattle,  150000
 *   Diana,   31, Denver,   110000
 *   Eve,     26, Portland,  88000
 * </pre>
 */
public class PredicatePushdownIT extends OlapRestTestCase {

  @Override
  public void setUp() throws Exception {
    super.setUp();
    loadIndex(Index.TEST_OLAP);
  }

  @Override
  public void tearDown() throws Exception {
    deleteIndex(Index.TEST_OLAP.getName());
    super.tearDown();
  }

  // --- Integer comparison pushdown ---

  public void testFilterGreaterThan() throws IOException {
    // age > 30 → Alice(35), Charlie(42), Diana(31)
    JSONObject response = executePPLQuery("source=test_olap | where age > 30 | fields name, age");
    JSONArray rows = getDataRows(response);

    Set<String> names = extractStringColumn(rows, 0);
    assertEquals("Expected 3 rows", 3, rows.length());
    assertTrue("Alice should match", names.contains("Alice"));
    assertTrue("Charlie should match", names.contains("Charlie"));
    assertTrue("Diana should match", names.contains("Diana"));
  }

  public void testFilterGreaterThanOrEqual() throws IOException {
    // age >= 35 → Alice(35), Charlie(42)
    JSONObject response = executePPLQuery("source=test_olap | where age >= 35 | fields name, age");
    JSONArray rows = getDataRows(response);

    Set<String> names = extractStringColumn(rows, 0);
    assertEquals("Expected 2 rows", 2, rows.length());
    assertTrue("Alice should match", names.contains("Alice"));
    assertTrue("Charlie should match", names.contains("Charlie"));
  }

  public void testFilterLessThan() throws IOException {
    // age < 30 → Bob(28), Eve(26)
    JSONObject response = executePPLQuery("source=test_olap | where age < 30 | fields name, age");
    JSONArray rows = getDataRows(response);

    Set<String> names = extractStringColumn(rows, 0);
    assertEquals("Expected 2 rows", 2, rows.length());
    assertTrue("Bob should match", names.contains("Bob"));
    assertTrue("Eve should match", names.contains("Eve"));
  }

  public void testFilterLessThanOrEqual() throws IOException {
    // age <= 28 → Bob(28), Eve(26)
    JSONObject response = executePPLQuery("source=test_olap | where age <= 28 | fields name, age");
    JSONArray rows = getDataRows(response);

    Set<String> names = extractStringColumn(rows, 0);
    assertEquals("Expected 2 rows", 2, rows.length());
    assertTrue("Bob should match", names.contains("Bob"));
    assertTrue("Eve should match", names.contains("Eve"));
  }

  public void testFilterEquality() throws IOException {
    // age = 35 → Alice
    JSONObject response = executePPLQuery("source=test_olap | where age = 35 | fields name, age");
    JSONArray rows = getDataRows(response);

    assertEquals("Expected 1 row", 1, rows.length());
    assertEquals("Alice", rows.getJSONArray(0).getString(0));
    assertEquals(35, rows.getJSONArray(0).getInt(1));
  }

  // --- Keyword equality pushdown ---

  public void testFilterKeywordEquality() throws IOException {
    // city = 'Seattle' → Alice, Charlie
    JSONObject response =
        executePPLQuery("source=test_olap | where city = 'Seattle' | fields name, city");
    JSONArray rows = getDataRows(response);

    Set<String> names = extractStringColumn(rows, 0);
    assertEquals("Expected 2 rows", 2, rows.length());
    assertTrue("Alice should match", names.contains("Alice"));
    assertTrue("Charlie should match", names.contains("Charlie"));
  }

  public void testFilterKeywordSingleMatch() throws IOException {
    // city = 'Denver' → Diana
    JSONObject response = executePPLQuery("source=test_olap | where city = 'Denver' | fields name");
    JSONArray rows = getDataRows(response);

    assertEquals("Expected 1 row", 1, rows.length());
    assertEquals("Diana", rows.getJSONArray(0).getString(0));
  }

  // --- Double comparison pushdown ---

  public void testFilterDoubleGreaterThan() throws IOException {
    // salary > 100000 → Alice(120000), Charlie(150000), Diana(110000)
    JSONObject response =
        executePPLQuery("source=test_olap | where salary > 100000 | fields name, salary");
    JSONArray rows = getDataRows(response);

    Set<String> names = extractStringColumn(rows, 0);
    assertEquals("Expected 3 rows", 3, rows.length());
    assertTrue("Alice should match", names.contains("Alice"));
    assertTrue("Charlie should match", names.contains("Charlie"));
    assertTrue("Diana should match", names.contains("Diana"));
  }

  // --- Compound predicates (AND / OR) ---

  public void testFilterAnd() throws IOException {
    // age > 30 AND salary > 115000 → Alice(35,120000), Charlie(42,150000)
    JSONObject response =
        executePPLQuery(
            "source=test_olap | where age > 30 and salary > 115000 | fields name, age, salary");
    JSONArray rows = getDataRows(response);

    Set<String> names = extractStringColumn(rows, 0);
    assertEquals("Expected 2 rows", 2, rows.length());
    assertTrue("Alice should match", names.contains("Alice"));
    assertTrue("Charlie should match", names.contains("Charlie"));
  }

  public void testFilterOr() throws IOException {
    // city = 'Denver' OR city = 'Seattle' → Alice, Charlie, Diana
    JSONObject response =
        executePPLQuery(
            "source=test_olap | where city = 'Denver' or city = 'Seattle' | fields name, city");
    JSONArray rows = getDataRows(response);

    Set<String> names = extractStringColumn(rows, 0);
    assertEquals("Expected 3 rows", 3, rows.length());
    assertTrue("Alice should match", names.contains("Alice"));
    assertTrue("Charlie should match", names.contains("Charlie"));
    assertTrue("Diana should match", names.contains("Diana"));
  }

  // --- Filter + aggregation (pushdown on PARTIAL stage) ---

  public void testFilterWithAggregation() throws IOException {
    // Filter first, then aggregate. Pushdown should reduce data read before PARTIAL agg.
    // age > 30 → Alice(35), Charlie(42), Diana(31) → count = 3
    JSONObject response = executePPLQuery("source=test_olap | where age > 30 | stats count()");
    JSONArray rows = getDataRows(response);

    assertEquals("Expected 1 row", 1, rows.length());
    assertEquals("count()", 3, rows.getJSONArray(0).getLong(0));
  }

  public void testFilterWithGroupByAggregation() throws IOException {
    // city = 'Seattle' → Alice, Charlie → avg(salary) = 135000
    JSONObject response =
        executePPLQuery("source=test_olap | where city = 'Seattle' | stats avg(salary) by city");
    JSONArray rows = getDataRows(response);

    assertEquals("Expected 1 row", 1, rows.length());
    JSONArray row = rows.getJSONArray(0);
    assertEquals("avg(salary)", 135000.0, row.getDouble(0), 0.01);
    assertEquals("city", "Seattle", row.getString(1));
  }

  public void testFilterWithSumAggregation() throws IOException {
    // salary > 100000 → Alice(120000), Charlie(150000), Diana(110000) → sum = 380000
    JSONObject response =
        executePPLQuery("source=test_olap | where salary > 100000 | stats sum(salary)");
    JSONArray rows = getDataRows(response);

    assertEquals("Expected 1 row", 1, rows.length());
    assertEquals("sum(salary)", 380000.0, rows.getJSONArray(0).getDouble(0), 0.01);
  }

  // --- Filter that matches all rows (pushdown should not break anything) ---

  public void testFilterMatchesAll() throws IOException {
    // age > 0 matches all 5 rows
    JSONObject response = executePPLQuery("source=test_olap | where age > 0 | stats count()");
    JSONArray rows = getDataRows(response);

    assertEquals("Expected 1 row", 1, rows.length());
    assertEquals("count()", 5, rows.getJSONArray(0).getLong(0));
  }

  // --- Filter that matches no rows ---

  public void testFilterMatchesNone() throws IOException {
    // age > 100 matches nobody
    JSONObject response = executePPLQuery("source=test_olap | where age > 100 | fields name");
    JSONArray rows = getDataRows(response);

    assertEquals("Expected 0 rows", 0, rows.length());
  }

  // --- Filter + projection (non-aggregation pushdown) ---

  public void testFilterWithProjection() throws IOException {
    // age > 30 | fields name, salary → only selected columns returned
    JSONObject response =
        executePPLQuery("source=test_olap | where age > 30 | fields name, salary");
    JSONArray rows = getDataRows(response);

    assertEquals("Expected 3 rows", 3, rows.length());
    // Verify each row has exactly 2 columns
    for (int i = 0; i < rows.length(); i++) {
      assertEquals("Each row should have 2 columns", 2, rows.getJSONArray(i).length());
    }
  }

  // --- Filter + sort + limit ---

  public void testFilterWithSortAndLimit() throws IOException {
    // Top 2 oldest people over 30, ordered by age desc
    JSONObject response =
        executePPLQuery(
            "source=test_olap | where age > 30 | sort - age | head 2 | fields name, age");
    JSONArray rows = getDataRows(response);

    assertEquals("Expected 2 rows", 2, rows.length());
    // Should be Charlie(42), Alice(35) in that order
    assertEquals("Charlie", rows.getJSONArray(0).getString(0));
    assertEquals(42, rows.getJSONArray(0).getInt(1));
    assertEquals("Alice", rows.getJSONArray(1).getString(0));
    assertEquals(35, rows.getJSONArray(1).getInt(1));
  }

  // --- Pushdown verification via cluster logs ---

  public void testPushdownAppearsInLogs() throws IOException {
    // Record log line count before query
    long beforeCount = countLogLines("Predicate pushdown enabled");

    // Run a filter query that should trigger pushdown
    executePPLQuery("source=test_olap | where age > 30 | fields name");

    // The cluster log should contain a "Predicate pushdown enabled" entry
    long afterCount = countLogLines("Predicate pushdown enabled");
    assertTrue(
        "Expected 'Predicate pushdown enabled' log entry after filter query",
        afterCount > beforeCount);
  }

  public void testPushdownScansFewerDocsThanTotal() throws IOException {
    // Enable DEBUG logging so the pushdown scan detail is captured
    Request debugLog = new Request("PUT", "/_cluster/settings");
    debugLog.setJsonEntity(
        "{\"transient\":{\"logger.org.opensearch.plugin.olap.execution\":\"DEBUG\"}}");
    client().performRequest(debugLog);

    // Record log state before query
    long beforeCount = countLogLines("Pushdown scan completed");

    // city = 'Denver' matches only 1 of 5 docs — pushdown should read far fewer
    executePPLQuery("source=test_olap | where city = 'Denver' | fields name");

    // Verify the pushdown scan log appeared with a doc count
    java.util.List<String> pushdownLogs = getLogLines("Pushdown scan completed");
    long afterCount = pushdownLogs.size();
    assertTrue(
        "Expected 'Pushdown scan completed' log entry after filter query",
        afterCount > beforeCount);

    // Extract the doc count from the latest log line: "... {} docs matched"
    String latestLog = pushdownLogs.get(pushdownLogs.size() - 1);
    java.util.regex.Matcher m =
        java.util.regex.Pattern.compile("(\\d+) docs matched").matcher(latestLog);
    assertTrue("Log should contain doc count", m.find());
    int docsMatched = Integer.parseInt(m.group(1));
    assertTrue(
        "Pushdown should scan fewer docs than total (5), got " + docsMatched, docsMatched < 5);
  }

  // --- Helper ---

  private Set<String> extractStringColumn(JSONArray rows, int colIndex) {
    Set<String> values = new HashSet<>();
    for (int i = 0; i < rows.length(); i++) {
      values.add(rows.getJSONArray(i).getString(colIndex));
    }
    return values;
  }
}
