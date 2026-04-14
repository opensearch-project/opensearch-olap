/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap;

import java.io.IOException;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;

/**
 * Integration tests for window function execution through the Velox engine. PPL's {@code
 * eventstats} command produces window aggregate functions (COUNT, SUM, AVG, MIN, MAX with PARTITION
 * BY) which are converted to Velox WindowNode.
 *
 * <p>Test data: employees (5 rows) with emp_id, name, dept_id, salary.
 */
public class WindowFunctionIT extends OlapRestTestCase {

  private static final String EMPLOYEES_INDEX = "employees";

  @Override
  public void setUp() throws Exception {
    super.setUp();
    createWindowTestIndex();
  }

  @Override
  public void tearDown() throws Exception {
    deleteIndex(EMPLOYEES_INDEX);
    super.tearDown();
  }

  // ---- eventstats count ----

  public void testEventstatsCount() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = " + EMPLOYEES_INDEX + " | eventstats count() as cnt | fields name, cnt");
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
                + EMPLOYEES_INDEX
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
                + EMPLOYEES_INDEX
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
                + EMPLOYEES_INDEX
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
    String plan = explainVeloxPlan("source = " + EMPLOYEES_INDEX + " | eventstats count() as cnt");
    assertTrue("Plan should contain Window node", plan.contains("Window"));
  }

  public void testEventstatsWithPartitionPlanContainsPartitionKey() throws IOException {
    String plan =
        explainVeloxPlan(
            "source = " + EMPLOYEES_INDEX + " | eventstats max(salary) as max_sal by dept_id");
    assertTrue("Plan should contain Window node", plan.contains("Window"));
    // The plan should reference the partition key (dept_id)
    assertTrue("Plan should reference partition key dept_id", plan.contains("dept_id"));
  }

  // ---- Helpers ----

  private void createWindowTestIndex() throws IOException {
    Request createEmployees = new Request("PUT", "/" + EMPLOYEES_INDEX);
    createEmployees.setJsonEntity(
        "{"
            + "\"settings\": {\"number_of_shards\": 1, \"number_of_replicas\": 0},"
            + "\"mappings\": {\"properties\": {"
            + "\"emp_id\": {\"type\": \"integer\"},"
            + "\"name\": {\"type\": \"keyword\"},"
            + "\"dept_id\": {\"type\": \"integer\"},"
            + "\"salary\": {\"type\": \"double\"}"
            + "}}"
            + "}");
    client().performRequest(createEmployees);

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
}
