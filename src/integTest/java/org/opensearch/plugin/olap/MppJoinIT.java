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
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;

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

  private static final String EMPLOYEES_INDEX = "employees";
  private static final String DEPARTMENTS_INDEX = "departments";

  @Override
  public void setUp() throws Exception {
    super.setUp();
    enableMpp(true);
    createJoinTestIndices();
  }

  @Override
  public void tearDown() throws Exception {
    deleteIndex(EMPLOYEES_INDEX);
    deleteIndex(DEPARTMENTS_INDEX);
    setBroadcastMaxShards(2);
    enableMpp(false);
    super.tearDown();
  }

  private void enableMpp(boolean enabled) throws IOException {
    Request request = new Request("PUT", "/_cluster/settings");
    request.setJsonEntity("{\"persistent\": {\"plugins.velox.mpp_enabled\": " + enabled + "}}");
    Response response = client().performRequest(request);
    assertEquals(200, response.getStatusLine().getStatusCode());
  }

  private void setBroadcastMaxShards(int maxShards) throws IOException {
    Request request = new Request("PUT", "/_cluster/settings");
    request.setJsonEntity(
        "{\"persistent\": {\"plugins.velox.broadcast_max_shards\": " + maxShards + "}}");
    Response response = client().performRequest(request);
    assertEquals(200, response.getStatusLine().getStatusCode());
  }

  // ---- Inner Join ----

  public void testMppInnerJoin() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = "
                + EMPLOYEES_INDEX
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + DEPARTMENTS_INDEX
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
                + EMPLOYEES_INDEX
                + " | left join left=e right=d ON e.dept_id = d.dept_id "
                + DEPARTMENTS_INDEX
                + " | fields e.name, d.dept_name");
    JSONArray rows = getDataRows(response);

    assertEquals("Expected 5 rows from left join", 5, rows.length());
  }

  // ---- Join + Filter ----

  public void testMppJoinWithFilter() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = "
                + EMPLOYEES_INDEX
                + " | where dept_id = 10"
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + DEPARTMENTS_INDEX
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
                + EMPLOYEES_INDEX
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + DEPARTMENTS_INDEX
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
                + EMPLOYEES_INDEX
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + DEPARTMENTS_INDEX
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
        executePPLQuery("source = " + EMPLOYEES_INDEX + " | stats count() by dept_id");
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
        executePPLQuery("source = " + EMPLOYEES_INDEX + " | stats sum(salary) by dept_id");
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
                + EMPLOYEES_INDEX
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + DEPARTMENTS_INDEX
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
    deleteIndex(EMPLOYEES_INDEX);
    deleteIndex(DEPARTMENTS_INDEX);
    createJoinTestIndicesWithShards(3);
    setBroadcastMaxShards(1);
    try {
      JSONObject response =
          executePPLQuery(
              "source = "
                  + EMPLOYEES_INDEX
                  + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                  + DEPARTMENTS_INDEX
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
      setBroadcastMaxShards(2);
      deleteIndex(EMPLOYEES_INDEX);
      deleteIndex(DEPARTMENTS_INDEX);
      createJoinTestIndices();
    }
  }

  public void testShuffleJoinWithAvgAggregation() throws IOException {
    deleteIndex(EMPLOYEES_INDEX);
    deleteIndex(DEPARTMENTS_INDEX);
    createJoinTestIndicesWithShards(3);
    setBroadcastMaxShards(1);
    try {
      JSONObject response =
          executePPLQuery(
              "source = "
                  + EMPLOYEES_INDEX
                  + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                  + DEPARTMENTS_INDEX
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
      setBroadcastMaxShards(2);
      deleteIndex(EMPLOYEES_INDEX);
      deleteIndex(DEPARTMENTS_INDEX);
      createJoinTestIndices();
    }
  }

  public void testShuffleJoinWithFilter() throws IOException {
    deleteIndex(EMPLOYEES_INDEX);
    deleteIndex(DEPARTMENTS_INDEX);
    createJoinTestIndicesWithShards(3);
    setBroadcastMaxShards(1);
    try {
      JSONObject response =
          executePPLQuery(
              "source = "
                  + EMPLOYEES_INDEX
                  + " | where dept_id = 10"
                  + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                  + DEPARTMENTS_INDEX
                  + " | stats count() by d.dept_name");
      JSONArray rows = getDataRows(response);

      assertEquals("Expected 1 department group", 1, rows.length());
      assertEquals("Engineering", rows.getJSONArray(0).getString(1));
      assertEquals(2, rows.getJSONArray(0).getLong(0));
    } finally {
      setBroadcastMaxShards(2);
      deleteIndex(EMPLOYEES_INDEX);
      deleteIndex(DEPARTMENTS_INDEX);
      createJoinTestIndices();
    }
  }

  public void testShuffleAggregateCountByGroup() throws IOException {
    deleteIndex(EMPLOYEES_INDEX);
    deleteIndex(DEPARTMENTS_INDEX);
    createJoinTestIndicesWithShards(3);
    setBroadcastMaxShards(1);
    try {
      JSONObject response =
          executePPLQuery("source = " + EMPLOYEES_INDEX + " | stats count() by dept_id");
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
      setBroadcastMaxShards(2);
      deleteIndex(EMPLOYEES_INDEX);
      deleteIndex(DEPARTMENTS_INDEX);
      createJoinTestIndices();
    }
  }

  public void testShuffleJoinWithSumAggregation() throws IOException {
    deleteIndex(EMPLOYEES_INDEX);
    deleteIndex(DEPARTMENTS_INDEX);
    createJoinTestIndicesWithShards(3);
    setBroadcastMaxShards(1);
    try {
      JSONObject response =
          executePPLQuery(
              "source = "
                  + EMPLOYEES_INDEX
                  + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                  + DEPARTMENTS_INDEX
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
      setBroadcastMaxShards(2);
      deleteIndex(EMPLOYEES_INDEX);
      deleteIndex(DEPARTMENTS_INDEX);
      createJoinTestIndices();
    }
  }

  // ---- Verify MPP is enabled via logs ----

  public void testMppEnabledInLogs() throws IOException {
    executePPLQuery(
        "source = "
            + EMPLOYEES_INDEX
            + " | inner join left=e right=d ON e.dept_id = d.dept_id "
            + DEPARTMENTS_INDEX
            + " | fields e.name, d.dept_name");

    long mppLogCount = countLogLines("mpp_enabled=true");
    assertTrue("Expected mpp_enabled=true in logs", mppLogCount > 0);
  }

  // ---- Helpers ----

  private void createJoinTestIndicesWithShards(int numShards) throws IOException {
    Request createEmployees = new Request("PUT", "/" + EMPLOYEES_INDEX);
    createEmployees.setJsonEntity(
        "{"
            + "\"settings\": {\"number_of_shards\": "
            + numShards
            + ", \"number_of_replicas\": 0},"
            + "\"mappings\": {\"properties\": {"
            + "\"emp_id\": {\"type\": \"integer\"},"
            + "\"name\": {\"type\": \"keyword\"},"
            + "\"dept_id\": {\"type\": \"integer\"},"
            + "\"salary\": {\"type\": \"double\"}"
            + "}}"
            + "}");
    client().performRequest(createEmployees);

    Request createDepts = new Request("PUT", "/" + DEPARTMENTS_INDEX);
    createDepts.setJsonEntity(
        "{"
            + "\"settings\": {\"number_of_shards\": "
            + numShards
            + ", \"number_of_replicas\": 0},"
            + "\"mappings\": {\"properties\": {"
            + "\"dept_id\": {\"type\": \"integer\"},"
            + "\"dept_name\": {\"type\": \"keyword\"}"
            + "}}"
            + "}");
    client().performRequest(createDepts);

    insertJoinTestData();
  }

  private void createJoinTestIndices() throws IOException {
    createJoinTestIndicesWithShards(1);
  }

  private void insertJoinTestData() throws IOException {
    Request bulkEmp = new Request("POST", "/_bulk?refresh=true");
    bulkEmp.setJsonEntity(
        "{\"index\": {\"_index\": \"employees\"}}\n"
            + "{\"emp_id\": 1, \"name\": \"Alice\", \"dept_id\": 10, \"salary\": 120000}\n"
            + "{\"index\": {\"_index\": \"employees\"}}\n"
            + "{\"emp_id\": 2, \"name\": \"Bob\", \"dept_id\": 20, \"salary\": 95000}\n"
            + "{\"index\": {\"_index\": \"employees\"}}\n"
            + "{\"emp_id\": 3, \"name\": \"Charlie\", \"dept_id\": 10, \"salary\": 150000}\n"
            + "{\"index\": {\"_index\": \"employees\"}}\n"
            + "{\"emp_id\": 4, \"name\": \"Diana\", \"dept_id\": 30, \"salary\": 110000}\n"
            + "{\"index\": {\"_index\": \"employees\"}}\n"
            + "{\"emp_id\": 5, \"name\": \"Eve\", \"dept_id\": 20, \"salary\": 88000}\n");
    client().performRequest(bulkEmp);

    Request bulkDept = new Request("POST", "/_bulk?refresh=true");
    bulkDept.setJsonEntity(
        "{\"index\": {\"_index\": \"departments\"}}\n"
            + "{\"dept_id\": 10, \"dept_name\": \"Engineering\"}\n"
            + "{\"index\": {\"_index\": \"departments\"}}\n"
            + "{\"dept_id\": 20, \"dept_name\": \"Marketing\"}\n"
            + "{\"index\": {\"_index\": \"departments\"}}\n"
            + "{\"dept_id\": 30, \"dept_name\": \"Sales\"}\n");
    client().performRequest(bulkDept);
  }

  private void deleteIndex(String indexName) throws IOException {
    try {
      client().performRequest(new Request("DELETE", "/" + indexName));
    } catch (ResponseException e) {
      if (e.getResponse().getStatusLine().getStatusCode() != 404) {
        throw e;
      }
    }
  }

  private Set<String> extractStringColumn(JSONArray rows, int colIndex) {
    Set<String> values = new HashSet<>();
    for (int i = 0; i < rows.length(); i++) {
      values.add(rows.getJSONArray(i).getString(colIndex));
    }
    return values;
  }
}
