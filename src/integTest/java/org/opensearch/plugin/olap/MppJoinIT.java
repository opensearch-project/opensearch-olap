/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Integration tests for join and aggregation queries with mpp_enabled=true. Toggles the dynamic
 * setting {@code plugins.velox.mpp_enabled} via cluster settings API before running tests. Both
 * PhysicalJoinRule (SINGLETON) and MppJoinRule (HASH) are registered in the VolcanoPlanner. The
 * planner explores both alternatives and picks the lower-cost one.
 *
 * <p>These tests verify that:
 *
 * <ul>
 *   <li>MPP rules don't cause CannotPlanException
 *   <li>Join queries produce correct results with MPP rules enabled
 *   <li>Aggregation queries work correctly with MPP aggregate rules
 * </ul>
 */
public class MppJoinIT extends OlapRestTestCase {

  @Override
  public void setUp() throws Exception {
    super.setUp();
    setClusterSetting("plugins.velox.mpp_enabled", true);
    loadIndex(Index.EMPLOYEES);
    loadIndex(Index.DEPARTMENTS);
  }

  @Override
  public void tearDown() throws Exception {
    deleteIndex(Index.EMPLOYEES.getName());
    deleteIndex(Index.DEPARTMENTS.getName());
    setClusterSetting("plugins.velox.broadcast_max_shards", "2");
    setClusterSetting("plugins.velox.mpp_enabled", false);
    super.tearDown();
  }

  // ---- Inner Join ----

  public void testMppInnerJoin() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = "
                + Index.EMPLOYEES.getName()
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + Index.DEPARTMENTS.getName()
                + " | fields e.name, d.dept_name");
    JSONArray rows = getDataRows(response);

    assertEquals("Expected 5 rows from inner join", 5, rows.length());

    Map<String, String> empDept = new HashMap<>();
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      empDept.put(row.getString(0), row.getString(1));
    }

    assertEquals("Engineering", empDept.get("Alice"));
    assertEquals("Marketing", empDept.get("Bob"));
    assertEquals("Engineering", empDept.get("Charlie"));
    assertEquals("Sales", empDept.get("Diana"));
    assertEquals("Marketing", empDept.get("Eve"));
  }

  // ---- Left Join ----

  public void testMppLeftJoin() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = "
                + Index.EMPLOYEES.getName()
                + " | left join left=e right=d ON e.dept_id = d.dept_id "
                + Index.DEPARTMENTS.getName()
                + " | fields e.name, d.dept_name");
    JSONArray rows = getDataRows(response);

    assertEquals("Expected 5 rows from left join", 5, rows.length());
  }

  // ---- Join + Filter ----

  public void testMppJoinWithFilter() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = "
                + Index.EMPLOYEES.getName()
                + " | where dept_id = 10"
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + Index.DEPARTMENTS.getName()
                + " | fields e.name, d.dept_name");
    JSONArray rows = getDataRows(response);

    assertEquals("Expected 2 Engineering employees", 2, rows.length());
    Set<String> names = extractStringColumn(rows, 0);
    assertTrue(names.contains("Alice"));
    assertTrue(names.contains("Charlie"));
  }

  // ---- Join + Aggregation ----

  public void testMppJoinWithCountAggregation() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = "
                + Index.EMPLOYEES.getName()
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + Index.DEPARTMENTS.getName()
                + " | stats count() by d.dept_name");
    JSONArray rows = getDataRows(response);

    Map<String, Long> countByDept = new HashMap<>();
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      countByDept.put(row.getString(1), row.getLong(0));
    }

    assertEquals(3, countByDept.size());
    assertEquals(Long.valueOf(2), countByDept.get("Engineering"));
    assertEquals(Long.valueOf(2), countByDept.get("Marketing"));
    assertEquals(Long.valueOf(1), countByDept.get("Sales"));
  }

  public void testMppJoinWithAvgAggregation() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = "
                + Index.EMPLOYEES.getName()
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + Index.DEPARTMENTS.getName()
                + " | stats avg(e.salary) by d.dept_name");
    JSONArray rows = getDataRows(response);

    Map<String, Double> avgByDept = new HashMap<>();
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      avgByDept.put(row.getString(1), row.getDouble(0));
    }

    assertEquals(135000.0, avgByDept.get("Engineering"), 0.01);
    assertEquals(91500.0, avgByDept.get("Marketing"), 0.01);
    assertEquals(110000.0, avgByDept.get("Sales"), 0.01);
  }

  // ---- MPP Aggregate (no join) ----

  public void testMppAggregateCountByGroup() throws IOException {
    JSONObject response =
        executePPLQuery("source = " + Index.EMPLOYEES.getName() + " | stats count() by dept_id");
    JSONArray rows = getDataRows(response);

    Map<Long, Long> countByDept = new HashMap<>();
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      countByDept.put(row.getLong(1), row.getLong(0));
    }

    assertEquals(Long.valueOf(2), countByDept.get(10L));
    assertEquals(Long.valueOf(2), countByDept.get(20L));
    assertEquals(Long.valueOf(1), countByDept.get(30L));
  }

  public void testMppAggregateSumByGroup() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = " + Index.EMPLOYEES.getName() + " | stats sum(salary) by dept_id");
    JSONArray rows = getDataRows(response);

    Map<Long, Double> sumByDept = new HashMap<>();
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      sumByDept.put(row.getLong(1), row.getDouble(0));
    }

    assertEquals(270000.0, sumByDept.get(10L), 0.01);
    assertEquals(183000.0, sumByDept.get(20L), 0.01);
    assertEquals(110000.0, sumByDept.get(30L), 0.01);
  }

  // ---- Join + Limit ----

  public void testMppJoinWithLimit() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = "
                + Index.EMPLOYEES.getName()
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + Index.DEPARTMENTS.getName()
                + " | head 3"
                + " | fields e.name, d.dept_name");
    JSONArray rows = getDataRows(response);

    assertEquals("Expected 3 rows", 3, rows.length());
  }

  // ---- Hash Shuffle Join (broadcast_max_shards=1 with 3-shard indices forces HASH_SHUFFLE) ----

  /**
   * Hash shuffle join tests use broadcast_max_shards=1 with 3-shard indices. CostEstimator sees
   * min(3,3)=3 > 1 → selects HASH_SHUFFLE strategy. The planner produces HASH exchanges (cheaper
   * cost when mpp_enabled=true), and executeShuffleFragments() handles the shuffle dispatch.
   */
  public void testShuffleJoinWithAggregation() throws IOException {
    deleteIndex(Index.EMPLOYEES.getName());
    deleteIndex(Index.DEPARTMENTS.getName());
    loadIndex(Index.EMPLOYEES, 3);
    loadIndex(Index.DEPARTMENTS, 3);
    setClusterSetting("plugins.velox.broadcast_max_shards", "1");
    try {
      JSONObject response =
          executePPLQuery(
              "source = "
                  + Index.EMPLOYEES.getName()
                  + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                  + Index.DEPARTMENTS.getName()
                  + " | stats count() by d.dept_name");
      JSONArray rows = getDataRows(response);

      Map<String, Long> countByDept = new HashMap<>();
      for (int i = 0; i < rows.length(); i++) {
        JSONArray row = rows.getJSONArray(i);
        countByDept.put(row.getString(1), row.getLong(0));
      }

      assertEquals(3, countByDept.size());
      assertEquals(Long.valueOf(2), countByDept.get("Engineering"));
      assertEquals(Long.valueOf(2), countByDept.get("Marketing"));
      assertEquals(Long.valueOf(1), countByDept.get("Sales"));
    } finally {
      setClusterSetting("plugins.velox.broadcast_max_shards", "2");
      deleteIndex(Index.EMPLOYEES.getName());
      deleteIndex(Index.DEPARTMENTS.getName());
      loadIndex(Index.EMPLOYEES);
      loadIndex(Index.DEPARTMENTS);
    }
  }

  public void testShuffleJoinWithAvgAggregation() throws IOException {
    deleteIndex(Index.EMPLOYEES.getName());
    deleteIndex(Index.DEPARTMENTS.getName());
    loadIndex(Index.EMPLOYEES, 3);
    loadIndex(Index.DEPARTMENTS, 3);
    setClusterSetting("plugins.velox.broadcast_max_shards", "1");
    try {
      JSONObject response =
          executePPLQuery(
              "source = "
                  + Index.EMPLOYEES.getName()
                  + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                  + Index.DEPARTMENTS.getName()
                  + " | stats avg(e.salary) by d.dept_name");
      JSONArray rows = getDataRows(response);

      Map<String, Double> avgByDept = new HashMap<>();
      for (int i = 0; i < rows.length(); i++) {
        JSONArray row = rows.getJSONArray(i);
        avgByDept.put(row.getString(1), row.getDouble(0));
      }

      assertEquals(135000.0, avgByDept.get("Engineering"), 0.01);
      assertEquals(91500.0, avgByDept.get("Marketing"), 0.01);
      assertEquals(110000.0, avgByDept.get("Sales"), 0.01);
    } finally {
      setClusterSetting("plugins.velox.broadcast_max_shards", "2");
      deleteIndex(Index.EMPLOYEES.getName());
      deleteIndex(Index.DEPARTMENTS.getName());
      loadIndex(Index.EMPLOYEES);
      loadIndex(Index.DEPARTMENTS);
    }
  }

  public void testShuffleJoinWithFilter() throws IOException {
    deleteIndex(Index.EMPLOYEES.getName());
    deleteIndex(Index.DEPARTMENTS.getName());
    loadIndex(Index.EMPLOYEES, 3);
    loadIndex(Index.DEPARTMENTS, 3);
    setClusterSetting("plugins.velox.broadcast_max_shards", "1");
    try {
      JSONObject response =
          executePPLQuery(
              "source = "
                  + Index.EMPLOYEES.getName()
                  + " | where dept_id = 10"
                  + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                  + Index.DEPARTMENTS.getName()
                  + " | stats count() by d.dept_name");
      JSONArray rows = getDataRows(response);

      assertEquals("Expected 1 department group", 1, rows.length());
      assertEquals("Engineering", rows.getJSONArray(0).getString(1));
      assertEquals(2, rows.getJSONArray(0).getLong(0));
    } finally {
      setClusterSetting("plugins.velox.broadcast_max_shards", "2");
      deleteIndex(Index.EMPLOYEES.getName());
      deleteIndex(Index.DEPARTMENTS.getName());
      loadIndex(Index.EMPLOYEES);
      loadIndex(Index.DEPARTMENTS);
    }
  }

  public void testShuffleAggregateCountByGroup() throws IOException {
    deleteIndex(Index.EMPLOYEES.getName());
    deleteIndex(Index.DEPARTMENTS.getName());
    loadIndex(Index.EMPLOYEES, 3);
    loadIndex(Index.DEPARTMENTS, 3);
    setClusterSetting("plugins.velox.broadcast_max_shards", "1");
    try {
      JSONObject response =
          executePPLQuery("source = " + Index.EMPLOYEES.getName() + " | stats count() by dept_id");
      JSONArray rows = getDataRows(response);

      Map<Long, Long> countByDept = new HashMap<>();
      for (int i = 0; i < rows.length(); i++) {
        JSONArray row = rows.getJSONArray(i);
        countByDept.put(row.getLong(1), row.getLong(0));
      }

      assertEquals(Long.valueOf(2), countByDept.get(10L));
      assertEquals(Long.valueOf(2), countByDept.get(20L));
      assertEquals(Long.valueOf(1), countByDept.get(30L));
    } finally {
      setClusterSetting("plugins.velox.broadcast_max_shards", "2");
      deleteIndex(Index.EMPLOYEES.getName());
      deleteIndex(Index.DEPARTMENTS.getName());
      loadIndex(Index.EMPLOYEES);
      loadIndex(Index.DEPARTMENTS);
    }
  }

  public void testShuffleJoinWithSumAggregation() throws IOException {
    deleteIndex(Index.EMPLOYEES.getName());
    deleteIndex(Index.DEPARTMENTS.getName());
    loadIndex(Index.EMPLOYEES, 3);
    loadIndex(Index.DEPARTMENTS, 3);
    setClusterSetting("plugins.velox.broadcast_max_shards", "1");
    try {
      JSONObject response =
          executePPLQuery(
              "source = "
                  + Index.EMPLOYEES.getName()
                  + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                  + Index.DEPARTMENTS.getName()
                  + " | stats sum(e.salary) by d.dept_name");
      JSONArray rows = getDataRows(response);

      Map<String, Double> sumByDept = new HashMap<>();
      for (int i = 0; i < rows.length(); i++) {
        JSONArray row = rows.getJSONArray(i);
        sumByDept.put(row.getString(1), row.getDouble(0));
      }

      assertEquals(270000.0, sumByDept.get("Engineering"), 0.01);
      assertEquals(183000.0, sumByDept.get("Marketing"), 0.01);
      assertEquals(110000.0, sumByDept.get("Sales"), 0.01);
    } finally {
      setClusterSetting("plugins.velox.broadcast_max_shards", "2");
      deleteIndex(Index.EMPLOYEES.getName());
      deleteIndex(Index.DEPARTMENTS.getName());
      loadIndex(Index.EMPLOYEES);
      loadIndex(Index.DEPARTMENTS);
    }
  }

  // ---- Plan structure verification ----

  public void testMppJoinPlanContainsHashJoinNode() throws IOException {
    String plan =
        explainVeloxPlan(
            "source = "
                + Index.EMPLOYEES.getName()
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + Index.DEPARTMENTS.getName()
                + " | fields e.name, d.dept_name");
    assertTrue("MPP plan should contain HashJoin node", plan.contains("HashJoin"));
    assertTrue("MPP plan should have SOURCE fragment", plan.contains("[SOURCE]"));
    assertTrue("MPP plan should have COORDINATOR fragment", plan.contains("[COORDINATOR]"));
  }

  // ---- Verify MPP is enabled via logs ----

  public void testMppEnabledInLogs() throws IOException {
    executePPLQuery(
        "source = "
            + Index.EMPLOYEES.getName()
            + " | inner join left=e right=d ON e.dept_id = d.dept_id "
            + Index.DEPARTMENTS.getName()
            + " | fields e.name, d.dept_name");

    long mppLogCount = countLogLines("mpp_enabled=true");
    assertTrue("Expected mpp_enabled=true in logs", mppLogCount > 0);
  }

  // ---- Helpers ----

  private Set<String> extractStringColumn(JSONArray rows, int colIndex) {
    Set<String> values = new HashSet<>();
    for (int i = 0; i < rows.length(); i++) {
      values.add(rows.getJSONArray(i).getString(colIndex));
    }
    return values;
  }
}
