/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap;

import java.io.IOException;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Integration tests for window function execution through the Velox engine. PPL's {@code
 * eventstats} command produces window aggregate functions (COUNT, SUM, AVG, MIN, MAX with PARTITION
 * BY) which are converted to Velox WindowNode.
 *
 * <p>Test data: employees (5 rows) with emp_id, name, dept_id, salary.
 */
public class WindowFunctionIT extends OlapRestTestCase {

  @Override
  public void setUp() throws Exception {
    super.setUp();
    loadIndex(Index.EMPLOYEES);
  }

  @Override
  public void tearDown() throws Exception {
    deleteIndex(Index.EMPLOYEES.getName());
    super.tearDown();
  }

  // ---- eventstats count ----

  public void testEventstatsCount() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = "
                + Index.EMPLOYEES.getName()
                + " | eventstats count() as cnt | fields name, cnt");
    JSONArray rows = getDataRows(response);

    assertEquals("All 5 rows should have count", 5, rows.length());
    for (int i = 0; i < rows.length(); i++) {
      assertEquals("Global count should be 5", 5, rows.getJSONArray(i).getLong(1));
    }
  }

  // ---- eventstats with partition by ----

  public void testEventstatsCountByPartition() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = "
                + Index.EMPLOYEES.getName()
                + " | eventstats count() as cnt by dept_id | fields name, dept_id, cnt");
    JSONArray rows = getDataRows(response);

    assertEquals(5, rows.length());
    Map<Long, Long> expectedCounts = Map.of(10L, 2L, 20L, 2L, 30L, 1L);
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      long deptId = row.getLong(1);
      long cnt = row.getLong(2);
      assertEquals("Count for dept " + deptId, expectedCounts.get(deptId).longValue(), cnt);
    }
  }

  // ---- eventstats max ----

  public void testEventstatsMaxByPartition() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = "
                + Index.EMPLOYEES.getName()
                + " | eventstats max(salary) as max_sal by dept_id"
                + " | fields name, dept_id, max_sal");
    JSONArray rows = getDataRows(response);

    assertEquals(5, rows.length());
    // dept 10: max(120000, 150000) = 150000
    // dept 20: max(95000, 88000) = 95000
    // dept 30: max(110000) = 110000
    Map<Long, Double> expectedMax = Map.of(10L, 150000.0, 20L, 95000.0, 30L, 110000.0);
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      long deptId = row.getLong(1);
      double maxSal = row.getDouble(2);
      assertEquals("Max salary for dept " + deptId, expectedMax.get(deptId), maxSal, 0.01);
    }
  }

  // ---- eventstats sum ----

  public void testEventstatsSumGlobal() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = "
                + Index.EMPLOYEES.getName()
                + " | eventstats sum(salary) as total_sal | fields name, total_sal");
    JSONArray rows = getDataRows(response);

    assertEquals(5, rows.length());
    double expectedTotal = 120000 + 95000 + 150000 + 110000 + 88000;
    for (int i = 0; i < rows.length(); i++) {
      assertEquals("Total salary", expectedTotal, rows.getJSONArray(i).getDouble(1), 0.01);
    }
  }

  // ---- Plan structure verification ----

  public void testEventstatsPlanContainsWindowNode() throws IOException {
    String plan =
        explainVeloxPlan("source = " + Index.EMPLOYEES.getName() + " | eventstats count() as cnt");
    assertTrue("Plan should contain Window node", plan.contains("Window"));
  }

  public void testEventstatsWithPartitionPlanContainsPartitionKey() throws IOException {
    String plan =
        explainVeloxPlan(
            "source = "
                + Index.EMPLOYEES.getName()
                + " | eventstats max(salary) as max_sal by dept_id");
    assertTrue("Plan should contain Window node", plan.contains("Window"));
    // The plan should reference the partition key (dept_id)
    assertTrue("Plan should reference partition key dept_id", plan.contains("dept_id"));
  }

  // ---- Ranking window functions (row_number) ----

  /**
   * PPL's {@code dedup} command lowers to {@code row_number() OVER (PARTITION BY ... ORDER BY ...)}
   * with a {@code row_number <= 1} filter. Velox's registered row_number returns INTEGER while
   * Calcite declares BIGINT, so the plan generator narrows the WindowNode output and inserts a
   * CastTypedExpr projection to bridge the width gap. This test locks that path in.
   */
  public void testDedupCompilesToRowNumber() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = " + Index.EMPLOYEES.getName() + " | dedup dept_id | fields dept_id");
    JSONArray rows = getDataRows(response);
    // employees has 3 distinct dept_ids (10, 20, 30) — dedup on dept_id keeps one per group.
    assertEquals("dedup dept_id should keep 3 rows", 3, rows.length());
  }
}
