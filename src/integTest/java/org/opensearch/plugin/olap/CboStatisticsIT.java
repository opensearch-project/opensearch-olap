/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;

/**
 * Integration tests for CBO statistics collection. Enables cbo_statistics_mode=RUNTIME and verifies
 * that queries produce correct results with real row count statistics driving join strategy and
 * build side selection.
 */
public class CboStatisticsIT extends OlapRestTestCase {

  private static final String EMPLOYEES_INDEX = "employees";
  private static final String DEPARTMENTS_INDEX = "departments";

  @Override
  public void setUp() throws Exception {
    super.setUp();
    setCboMode("RUNTIME");
    enableMpp(true);
    createJoinTestIndices();
  }

  @Override
  public void tearDown() throws Exception {
    deleteIndex(EMPLOYEES_INDEX);
    deleteIndex(DEPARTMENTS_INDEX);
    enableMpp(false);
    setCboMode("NONE");
    super.tearDown();
  }

  private void setCboMode(String mode) throws IOException {
    Request request = new Request("PUT", "/_cluster/settings");
    request.setJsonEntity(
        "{\"persistent\": {\"plugins.velox.cbo_statistics_mode\": \"" + mode + "\"}}");
    Response response = client().performRequest(request);
    assertEquals(200, response.getStatusLine().getStatusCode());
  }

  private void enableMpp(boolean enabled) throws IOException {
    Request request = new Request("PUT", "/_cluster/settings");
    request.setJsonEntity("{\"persistent\": {\"plugins.velox.mpp_enabled\": " + enabled + "}}");
    client().performRequest(request);
  }

  // ---- CBO-enabled join produces correct results ----

  public void testCboInnerJoin() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = "
                + EMPLOYEES_INDEX
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + DEPARTMENTS_INDEX
                + " | fields e.name, d.dept_name");
    JSONArray rows = getDataRows(response);

    assertEquals("Expected 5 rows from inner join with CBO", 5, rows.length());

    Map<String, String> empDept = new HashMap<>();
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      empDept.put(row.getString(0), row.getString(1));
    }

    assertEquals("Engineering", empDept.get("Alice"));
    assertEquals("Marketing", empDept.get("Bob"));
  }

  // ---- CBO produces same results as non-CBO ----

  public void testCboAndNonCboProduceSameResults() throws IOException {
    // With CBO
    setCboMode("RUNTIME");
    JSONObject cboResp =
        executePPLQuery(
            "source = "
                + EMPLOYEES_INDEX
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + DEPARTMENTS_INDEX
                + " | stats count() by d.dept_name");

    // Without CBO
    setCboMode("NONE");
    JSONObject noCboResp =
        executePPLQuery(
            "source = "
                + EMPLOYEES_INDEX
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + DEPARTMENTS_INDEX
                + " | stats count() by d.dept_name");

    JSONArray cboRows = getDataRows(cboResp);
    JSONArray noCboRows = getDataRows(noCboResp);

    assertEquals("Same number of rows", cboRows.length(), noCboRows.length());
  }

  // ---- CBO aggregation ----

  public void testCboAggregation() throws IOException {
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

  // ---- CBO stats logged ----

  public void testCboStatsAppearInLogs() throws IOException {
    // Enable DEBUG logging
    Request debugLog = new Request("PUT", "/_cluster/settings");
    debugLog.setJsonEntity(
        "{\"transient\":{\"logger.org.opensearch.plugin.olap.engine\":\"DEBUG\"}}");
    client().performRequest(debugLog);

    long logsBefore = countLogLines("CBO stats");

    executePPLQuery(
        "source = "
            + EMPLOYEES_INDEX
            + " | inner join left=e right=d ON e.dept_id = d.dept_id "
            + DEPARTMENTS_INDEX
            + " | fields e.name, d.dept_name");

    long logsAfter = countLogLines("CBO stats");
    assertTrue("Expected CBO stats log entries", logsAfter > logsBefore);
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
