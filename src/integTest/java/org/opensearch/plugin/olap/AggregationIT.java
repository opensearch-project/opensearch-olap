/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Integration tests for distributed aggregation queries executed through the Velox engine. Each
 * test sends a PPL query that the SQL plugin routes to the OLAP plugin via {@code canVectorize()}.
 * The OLAP plugin splits the plan into PARTIAL (data nodes) and FINAL (coordinator) stages.
 *
 * <p>Test data: 5 rows across 2 shards on 2 nodes:
 *
 * <pre>
 *   Alice,   35, Seattle,  120000
 *   Bob,     28, Portland,  95000
 *   Charlie, 42, Seattle,  150000
 *   Diana,   31, Denver,   110000
 *   Eve,     26, Portland,  88000
 * </pre>
 */
public class AggregationIT extends OlapRestTestCase {

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

  public void testCountByCity() throws IOException {
    JSONObject response = executePPLQuery("source=test_olap | stats count() by city");
    JSONArray rows = getDataRows(response);

    Map<String, Long> countByCity = new HashMap<>();
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      countByCity.put(row.getString(1), row.getLong(0));
    }

    assertEquals("Expected 3 cities", 3, countByCity.size());
    assertEquals("Seattle count", Long.valueOf(2), countByCity.get("Seattle"));
    assertEquals("Portland count", Long.valueOf(2), countByCity.get("Portland"));
    assertEquals("Denver count", Long.valueOf(1), countByCity.get("Denver"));
  }

  public void testAvgSalaryByCity() throws IOException {
    JSONObject response = executePPLQuery("source=test_olap | stats avg(salary) by city");
    JSONArray rows = getDataRows(response);

    Map<String, Double> avgByCity = new HashMap<>();
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      avgByCity.put(row.getString(1), row.getDouble(0));
    }

    assertEquals("Expected 3 cities", 3, avgByCity.size());
    assertEquals("Seattle avg", 135000.0, avgByCity.get("Seattle"), 0.01);
    assertEquals("Portland avg", 91500.0, avgByCity.get("Portland"), 0.01);
    assertEquals("Denver avg", 110000.0, avgByCity.get("Denver"), 0.01);
  }

  public void testSumAgeAndCount() throws IOException {
    JSONObject response = executePPLQuery("source=test_olap | stats sum(age), count()");
    JSONArray rows = getDataRows(response);

    assertEquals("Expected 1 row", 1, rows.length());
    JSONArray row = rows.getJSONArray(0);
    // sum(age) = 35 + 28 + 42 + 31 + 26 = 162
    assertEquals("sum(age)", 162, row.getLong(0));
    assertEquals("count()", 5, row.getLong(1));
  }

  public void testSumSalaryByCity() throws IOException {
    JSONObject response = executePPLQuery("source=test_olap | stats sum(salary) by city");
    JSONArray rows = getDataRows(response);

    Map<String, Double> sumByCity = new HashMap<>();
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      sumByCity.put(row.getString(1), row.getDouble(0));
    }

    assertEquals("Expected 3 cities", 3, sumByCity.size());
    assertEquals("Seattle sum", 270000.0, sumByCity.get("Seattle"), 0.01);
    assertEquals("Portland sum", 183000.0, sumByCity.get("Portland"), 0.01);
    assertEquals("Denver sum", 110000.0, sumByCity.get("Denver"), 0.01);
  }

  public void testMinMaxSalaryByCity() throws IOException {
    JSONObject response = executePPLQuery("source=test_olap | stats min(salary) by city");
    JSONArray rows = getDataRows(response);

    Map<String, Double> minByCity = new HashMap<>();
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      minByCity.put(row.getString(1), row.getDouble(0));
    }

    assertEquals("Seattle min", 120000.0, minByCity.get("Seattle"), 0.01);
    assertEquals("Portland min", 88000.0, minByCity.get("Portland"), 0.01);
    assertEquals("Denver min", 110000.0, minByCity.get("Denver"), 0.01);
  }

  public void testMultipleAggregatesWithGroupBy() throws IOException {
    JSONObject response =
        executePPLQuery("source=test_olap | stats count(), avg(salary), sum(age) by city");
    JSONArray rows = getDataRows(response);

    assertEquals("Expected 3 cities", 3, rows.length());
    // Verify at least one city has expected values
    boolean seattleFound = false;
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      if ("Seattle".equals(row.getString(3))) {
        assertEquals("Seattle count", 2, row.getLong(0));
        assertEquals("Seattle avg(salary)", 135000.0, row.getDouble(1), 0.01);
        assertEquals("Seattle sum(age)", 77, row.getLong(2)); // 35 + 42
        seattleFound = true;
      }
    }
    assertTrue("Seattle row not found", seattleFound);
  }

  public void testGlobalCount() throws IOException {
    JSONObject response = executePPLQuery("source=test_olap | stats count()");
    JSONArray rows = getDataRows(response);

    assertEquals("Expected 1 row", 1, rows.length());
    assertEquals("count()", 5, rows.getJSONArray(0).getLong(0));
  }

  // ---- Plan structure verification ----

  public void testAggregationPlanContainsAggregationNode() throws IOException {
    String plan = explainVeloxPlan("source=test_olap | stats count() by city");
    assertTrue("Plan should contain Aggregation node", plan.contains("Aggregation"));
    assertTrue("Plan should have SOURCE fragment", plan.contains("[SOURCE]"));
  }

  public void testAvgAggregationPlanContainsAvgFunction() throws IOException {
    String plan = explainVeloxPlan("source=test_olap | stats avg(salary) by city");
    assertTrue("Plan should contain Aggregation node", plan.contains("Aggregation"));
    assertTrue("Plan should reference avg function", plan.contains("avg"));
  }
}
