/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap;

import java.io.IOException;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Integration tests for TopN optimization (two-stage sort+limit). Verifies that ORDER BY + LIMIT
 * queries produce correct results with the partial sort on data nodes and final sort on
 * coordinator.
 *
 * <p>Test data: 5 rows (Alice/35/Seattle/120000, Bob/28/Portland/95000, Charlie/42/Seattle/150000,
 * Diana/31/Denver/110000, Eve/26/Portland/88000)
 */
public class TopNIT extends OlapRestTestCase {

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

  // ---- Basic TopN ----

  public void testSortAscWithLimit() throws IOException {
    JSONObject response =
        executePPLQuery("source=test_olap | sort age | head 2 | fields name, age");
    JSONArray rows = getDataRows(response);

    assertEquals(2, rows.length());
    // Youngest 2: Eve(26), Bob(28)
    assertEquals("Eve", rows.getJSONArray(0).getString(0));
    assertEquals(26, rows.getJSONArray(0).getInt(1));
    assertEquals("Bob", rows.getJSONArray(1).getString(0));
    assertEquals(28, rows.getJSONArray(1).getInt(1));
  }

  public void testSortDescWithLimit() throws IOException {
    JSONObject response =
        executePPLQuery("source=test_olap | sort - salary | head 3 | fields name, salary");
    JSONArray rows = getDataRows(response);

    assertEquals(3, rows.length());
    // Top 3 salaries: Charlie(150000), Alice(120000), Diana(110000)
    assertEquals("Charlie", rows.getJSONArray(0).getString(0));
    assertEquals(150000.0, rows.getJSONArray(0).getDouble(1), 0.01);
    assertEquals("Alice", rows.getJSONArray(1).getString(0));
    assertEquals(120000.0, rows.getJSONArray(1).getDouble(1), 0.01);
    assertEquals("Diana", rows.getJSONArray(2).getString(0));
    assertEquals(110000.0, rows.getJSONArray(2).getDouble(1), 0.01);
  }

  // ---- TopN with filter ----

  public void testSortWithFilterAndLimit() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source=test_olap | where age > 30 | sort - age | head 2 | fields name, age");
    JSONArray rows = getDataRows(response);

    assertEquals(2, rows.length());
    // Oldest 2 over 30: Charlie(42), Alice(35)
    assertEquals("Charlie", rows.getJSONArray(0).getString(0));
    assertEquals(42, rows.getJSONArray(0).getInt(1));
    assertEquals("Alice", rows.getJSONArray(1).getString(0));
    assertEquals(35, rows.getJSONArray(1).getInt(1));
  }

  // ---- TopN returns all when limit >= rows ----

  public void testSortWithLimitExceedingRowCount() throws IOException {
    JSONObject response =
        executePPLQuery("source=test_olap | sort age | head 100 | fields name, age");
    JSONArray rows = getDataRows(response);

    // Only 5 rows exist, so all 5 returned
    assertEquals(5, rows.length());
    // Should be sorted: Eve(26), Bob(28), Diana(31), Alice(35), Charlie(42)
    assertEquals("Eve", rows.getJSONArray(0).getString(0));
    assertEquals("Charlie", rows.getJSONArray(4).getString(0));
  }

  // ---- Limit only (no sort) ----

  public void testLimitWithoutSort() throws IOException {
    JSONObject response = executePPLQuery("source=test_olap | head 2 | fields name");
    JSONArray rows = getDataRows(response);

    assertEquals(2, rows.length());
  }

  // ---- TopN with projection ----

  public void testSortLimitWithProjection() throws IOException {
    JSONObject response =
        executePPLQuery("source=test_olap | sort salary | head 1 | fields name, city, salary");
    JSONArray rows = getDataRows(response);

    assertEquals(1, rows.length());
    // Lowest salary: Eve(88000, Portland)
    assertEquals("Eve", rows.getJSONArray(0).getString(0));
    assertEquals("Portland", rows.getJSONArray(0).getString(1));
    assertEquals(88000.0, rows.getJSONArray(0).getDouble(2), 0.01);
  }

  // ---- Plan structure verification ----

  public void testSortLimitPlanContainsOrderByAndLimit() throws IOException {
    String plan = explainVeloxPlan("source=test_olap | sort salary | head 3");

    assertTrue("Plan should contain OrderBy node", plan.contains("OrderBy"));
    assertTrue("Plan should contain Limit node", plan.contains("Limit"));
    assertTrue("Plan should have SOURCE fragment", plan.contains("[SOURCE]"));
  }

  public void testSortWithoutExplicitLimitPlanHasOrderBy() throws IOException {
    String plan = explainVeloxPlan("source=test_olap | sort salary | fields name, salary");

    assertTrue("Plan should contain OrderBy node", plan.contains("OrderBy"));
    // Note: SQL plugin adds a SystemLimit (default 10000 rows) to all queries,
    // so a LimitNode may still appear even without explicit `head N`.
  }

  // ---- TopN correctness: same results with and without TopN path ----

  public void testTopNProducesSameResultsAsFullSort() throws IOException {
    // Full sort (no limit) — get all 5 sorted by age
    JSONObject fullResp = executePPLQuery("source=test_olap | sort age | fields name, age");
    JSONArray fullRows = getDataRows(fullResp);

    // TopN (limit 5 = all rows) — should produce identical result
    JSONObject topnResp =
        executePPLQuery("source=test_olap | sort age | head 5 | fields name, age");
    JSONArray topnRows = getDataRows(topnResp);

    assertEquals(fullRows.length(), topnRows.length());
    for (int i = 0; i < fullRows.length(); i++) {
      assertEquals(
          "Row " + i + " name",
          fullRows.getJSONArray(i).getString(0),
          topnRows.getJSONArray(i).getString(0));
      assertEquals(
          "Row " + i + " age",
          fullRows.getJSONArray(i).getInt(1),
          topnRows.getJSONArray(i).getInt(1));
    }
  }
}
