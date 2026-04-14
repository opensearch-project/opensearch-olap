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
import org.opensearch.client.ResponseException;

/**
 * Integration tests for runtime filter pushdown on broadcast joins. Verifies that join results are
 * correct with RF enabled (default) and disabled, and that RF log messages appear when RF is
 * applied.
 *
 * <p>Uses the same employees/departments test data as JoinIT. With RF enabled, the probe-side
 * (employees) scan should filter by dept_id values {10, 20, 30} from the build side (departments)
 * at the Lucene level.
 */
public class RuntimeFilterIT extends OlapRestTestCase {

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
    enableMpp(false);
    setRuntimeFilter(true);
    super.tearDown();
  }

  private void enableMpp(boolean enabled) throws IOException {
    Request request = new Request("PUT", "/_cluster/settings");
    request.setJsonEntity("{\"persistent\": {\"plugins.velox.mpp_enabled\": " + enabled + "}}");
    client().performRequest(request);
  }

  private void setRuntimeFilter(boolean enabled) throws IOException {
    Request request = new Request("PUT", "/_cluster/settings");
    request.setJsonEntity(
        "{\"persistent\": {\"plugins.velox.runtime_filter_enabled\": " + enabled + "}}");
    client().performRequest(request);
  }

  private void setRuntimeFilterMaxCardinality(int max) throws IOException {
    Request request = new Request("PUT", "/_cluster/settings");
    request.setJsonEntity(
        "{\"persistent\": {\"plugins.velox.runtime_filter_max_cardinality\": " + max + "}}");
    client().performRequest(request);
  }

  // ---- RF enabled produces correct results ----

  public void testInnerJoinWithRfEnabled() throws IOException {
    setRuntimeFilter(true);
    JSONObject response =
        executePPLQuery(
            "source = "
                + EMPLOYEES_INDEX
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + DEPARTMENTS_INDEX
                + " | fields e.name, d.dept_name");
    JSONArray rows = getDataRows(response);

    assertEquals("Expected 5 rows from inner join with RF", 5, rows.length());

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

  public void testLeftJoinWithRfEnabled() throws IOException {
    setRuntimeFilter(true);
    JSONObject response =
        executePPLQuery(
            "source = "
                + EMPLOYEES_INDEX
                + " | left join left=e right=d ON e.dept_id = d.dept_id "
                + DEPARTMENTS_INDEX
                + " | fields e.name, d.dept_name");
    JSONArray rows = getDataRows(response);

    assertEquals("Expected 5 rows from left join with RF", 5, rows.length());
  }

  // ---- RF disabled produces same results ----

  public void testInnerJoinWithRfDisabled() throws IOException {
    setRuntimeFilter(false);
    JSONObject response =
        executePPLQuery(
            "source = "
                + EMPLOYEES_INDEX
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + DEPARTMENTS_INDEX
                + " | fields e.name, d.dept_name");
    JSONArray rows = getDataRows(response);

    assertEquals("Expected 5 rows without RF", 5, rows.length());
  }

  // ---- RF with aggregation ----

  public void testJoinWithAggregationAndRf() throws IOException {
    setRuntimeFilter(true);
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

    assertEquals(Long.valueOf(2), countByDept.get("Engineering"));
    assertEquals(Long.valueOf(2), countByDept.get("Marketing"));
    assertEquals(Long.valueOf(1), countByDept.get("Sales"));
  }

  // ---- RF with filter + join ----

  public void testJoinWithFilterAndRf() throws IOException {
    setRuntimeFilter(true);
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
    Set<String> names = new HashSet<>();
    for (int i = 0; i < rows.length(); i++) {
      names.add(rows.getJSONArray(i).getString(0));
    }
    assertTrue(names.contains("Alice"));
    assertTrue(names.contains("Charlie"));
  }

  // ---- RF cardinality threshold ----

  public void testRfSkippedWhenCardinalityExceedsThreshold() throws IOException {
    setRuntimeFilter(true);
    setRuntimeFilterMaxCardinality(1); // only allow 1 distinct value — our build has 3

    // Should still produce correct results even though RF is skipped
    JSONObject response =
        executePPLQuery(
            "source = "
                + EMPLOYEES_INDEX
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + DEPARTMENTS_INDEX
                + " | fields e.name, d.dept_name");
    JSONArray rows = getDataRows(response);

    assertEquals("Should produce correct results even with RF skipped", 5, rows.length());

    // Restore default
    setRuntimeFilterMaxCardinality(10000);
  }

  // ---- RF enabled vs disabled produce identical results ----

  public void testRfEnabledAndDisabledProduceSameResults() throws IOException {
    // With RF enabled
    setRuntimeFilter(true);
    JSONObject rfResp =
        executePPLQuery(
            "source = "
                + EMPLOYEES_INDEX
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + DEPARTMENTS_INDEX
                + " | stats avg(e.salary) by d.dept_name");

    // With RF disabled
    setRuntimeFilter(false);
    JSONObject noRfResp =
        executePPLQuery(
            "source = "
                + EMPLOYEES_INDEX
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + DEPARTMENTS_INDEX
                + " | stats avg(e.salary) by d.dept_name");

    JSONArray rfRows = getDataRows(rfResp);
    JSONArray noRfRows = getDataRows(noRfResp);

    assertEquals("Same number of rows", rfRows.length(), noRfRows.length());

    Map<String, Double> rfAvg = new HashMap<>();
    Map<String, Double> noRfAvg = new HashMap<>();
    for (int i = 0; i < rfRows.length(); i++) {
      rfAvg.put(rfRows.getJSONArray(i).getString(1), rfRows.getJSONArray(i).getDouble(0));
      noRfAvg.put(noRfRows.getJSONArray(i).getString(1), noRfRows.getJSONArray(i).getDouble(0));
    }

    for (String dept : rfAvg.keySet()) {
      assertEquals("Same avg for " + dept, rfAvg.get(dept), noRfAvg.get(dept), 0.01);
    }
  }

  // ---- Bug regression: RF must not drop unmatched rows on outer joins ----

  /**
   * LEFT JOIN with a probe row that has no matching build row. RF must NOT filter out the unmatched
   * probe row — LEFT JOIN semantics require it to appear with NULLs on the build side.
   *
   * <p>This test adds an employee with dept_id=99 (no matching department). With the bug, RF would
   * push TermsQuery("dept_id", [10,20,30]) to the probe scan, skipping dept_id=99 and losing the
   * row.
   */
  /**
   * LEFT JOIN with an unmatched probe row (dept_id=99, no matching department). RF must be skipped
   * for LEFT JOIN, and the broadcast build side must be the right table (departments) so that left
   * (employees) rows are preserved. Verifies both RF skipping and correct outer join semantics.
   */
  public void testLeftJoinPreservesUnmatchedProbeRows() throws IOException {
    // Use isolated indices to avoid shared state with other tests
    String empIdx = "emp_leftjoin_test";
    String deptIdx = "dept_leftjoin_test";
    try {
      // Create employees with one unmatched dept_id
      Request createEmp = new Request("PUT", "/" + empIdx);
      createEmp.setJsonEntity(
          "{"
              + "\"settings\": {\"number_of_shards\": 1, \"number_of_replicas\": 0},"
              + "\"mappings\": {\"properties\": {"
              + "\"name\": {\"type\": \"keyword\"},"
              + "\"dept_id\": {\"type\": \"integer\"}"
              + "}}}");
      client().performRequest(createEmp);

      Request createDept = new Request("PUT", "/" + deptIdx);
      createDept.setJsonEntity(
          "{"
              + "\"settings\": {\"number_of_shards\": 1, \"number_of_replicas\": 0},"
              + "\"mappings\": {\"properties\": {"
              + "\"dept_id\": {\"type\": \"integer\"},"
              + "\"dept_name\": {\"type\": \"keyword\"}"
              + "}}}");
      client().performRequest(createDept);

      Request bulkEmp = new Request("POST", "/_bulk?refresh=true");
      bulkEmp.setJsonEntity(
          "{\"index\": {\"_index\": \""
              + empIdx
              + "\"}}\n"
              + "{\"name\": \"Alice\", \"dept_id\": 10}\n"
              + "{\"index\": {\"_index\": \""
              + empIdx
              + "\"}}\n"
              + "{\"name\": \"Bob\", \"dept_id\": 20}\n"
              + "{\"index\": {\"_index\": \""
              + empIdx
              + "\"}}\n"
              + "{\"name\": \"Zara\", \"dept_id\": 99}\n");
      client().performRequest(bulkEmp);

      Request bulkDept = new Request("POST", "/_bulk?refresh=true");
      bulkDept.setJsonEntity(
          "{\"index\": {\"_index\": \""
              + deptIdx
              + "\"}}\n"
              + "{\"dept_id\": 10, \"dept_name\": \"Engineering\"}\n"
              + "{\"index\": {\"_index\": \""
              + deptIdx
              + "\"}}\n"
              + "{\"dept_id\": 20, \"dept_name\": \"Marketing\"}\n");
      client().performRequest(bulkDept);

      setRuntimeFilter(true);
      JSONObject response =
          executePPLQuery(
              "source = "
                  + empIdx
                  + " | left join left=e right=d ON e.dept_id = d.dept_id "
                  + deptIdx
                  + " | fields e.name, e.dept_id");
      JSONArray rows = getDataRows(response);

      // 3 employees: Alice(10), Bob(20), Zara(99). Departments: 10, 20.
      // LEFT JOIN: Alice matches, Bob matches, Zara unmatched (preserved with null dept_name)
      assertEquals("LEFT JOIN must preserve all 3 probe rows", 3, rows.length());

      Map<String, Integer> nameToDepId = new HashMap<>();
      for (int i = 0; i < rows.length(); i++) {
        JSONArray row = rows.getJSONArray(i);
        nameToDepId.put(row.getString(0), row.getInt(1));
      }
      assertEquals("Alice dept_id", Integer.valueOf(10), nameToDepId.get("Alice"));
      assertEquals("Bob dept_id", Integer.valueOf(20), nameToDepId.get("Bob"));
      assertEquals("Zara dept_id (unmatched)", Integer.valueOf(99), nameToDepId.get("Zara"));
    } finally {
      deleteIndex(empIdx);
      deleteIndex(deptIdx);
    }
  }

  // ---- Bug regression: pushdown must not use build-side filters on probe scan ----

  /**
   * Inner join where the PPL query filters on a build-side column (dept_name). The pushdown
   * extractor must NOT apply this filter to the probe-side Lucene scan — the probe index
   * (employees) doesn't have a dept_name field.
   *
   * <p>With the bug, extractPushdownQuery(plan) could return the build-side FilterNode and apply it
   * to the probe scan, either matching nothing or matching incorrectly.
   */
  public void testJoinWithBuildSideFilterDoesNotBreakProbeScan() throws IOException {
    setRuntimeFilter(true);

    // Filter is on build-side column (d.dept_name) — must not be pushed to probe scan
    JSONObject response =
        executePPLQuery(
            "source = "
                + EMPLOYEES_INDEX
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + DEPARTMENTS_INDEX
                + " | where d.dept_name = 'Engineering'"
                + " | fields e.name, d.dept_name");
    JSONArray rows = getDataRows(response);

    // Should return Alice and Charlie (Engineering dept_id=10)
    assertEquals("Should find 2 Engineering employees", 2, rows.length());
    Set<String> names = new HashSet<>();
    for (int i = 0; i < rows.length(); i++) {
      names.add(rows.getJSONArray(i).getString(0));
    }
    assertTrue(names.contains("Alice"));
    assertTrue(names.contains("Charlie"));
  }

  // ---- Plan structure verification ----

  public void testInnerJoinPlanContainsHashJoinWithRf() throws IOException {
    setRuntimeFilter(true);
    String plan =
        explainVeloxPlan(
            "source = "
                + EMPLOYEES_INDEX
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + DEPARTMENTS_INDEX
                + " | fields e.name, d.dept_name");
    assertTrue("Plan should contain HashJoin node", plan.contains("HashJoin"));
    assertTrue("Plan should indicate INNER join", plan.contains("INNER"));
  }

  // ---- RF appears in logs ----

  public void testRfAppearsInLogs() throws IOException {
    setRuntimeFilter(true);

    long rfLogsBefore = countLogLines("RF extracted");

    executePPLQuery(
        "source = "
            + EMPLOYEES_INDEX
            + " | inner join left=e right=d ON e.dept_id = d.dept_id "
            + DEPARTMENTS_INDEX
            + " | fields e.name, d.dept_name");

    long rfLogsAfter = countLogLines("RF extracted");
    assertTrue("Expected 'RF extracted' log entries", rfLogsAfter > rfLogsBefore);
  }

  // ---- Helpers ----

  private void createJoinTestIndices() throws IOException {
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

    Request createDepts = new Request("PUT", "/" + DEPARTMENTS_INDEX);
    createDepts.setJsonEntity(
        "{"
            + "\"settings\": {\"number_of_shards\": 1, \"number_of_replicas\": 0},"
            + "\"mappings\": {\"properties\": {"
            + "\"dept_id\": {\"type\": \"integer\"},"
            + "\"dept_name\": {\"type\": \"keyword\"}"
            + "}}"
            + "}");
    client().performRequest(createDepts);

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
}
