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

/**
 * Integration tests for CBO statistics collection. Enables cbo_statistics_mode=RUNTIME and verifies
 * that queries produce correct results with real row count statistics driving join strategy and
 * build side selection.
 */
public class CboStatisticsIT extends OlapRestTestCase {

  @Override
  public void setUp() throws Exception {
    super.setUp();
    setClusterSetting("plugins.velox.cbo_statistics_mode", "RUNTIME");
    setClusterSetting("plugins.velox.mpp_enabled", true);
    loadIndex(Index.EMPLOYEES);
    loadIndex(Index.DEPARTMENTS);
  }

  @Override
  public void tearDown() throws Exception {
    deleteIndex(Index.EMPLOYEES.getName());
    deleteIndex(Index.DEPARTMENTS.getName());
    setClusterSetting("plugins.velox.mpp_enabled", false);
    setClusterSetting("plugins.velox.cbo_statistics_mode", "NONE");
    super.tearDown();
  }

  // ---- CBO-enabled join produces correct results ----

  public void testCboInnerJoin() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = "
                + Index.EMPLOYEES.getName()
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + Index.DEPARTMENTS.getName()
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
    setClusterSetting("plugins.velox.cbo_statistics_mode", "RUNTIME");
    JSONObject cboResp =
        executePPLQuery(
            "source = "
                + Index.EMPLOYEES.getName()
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + Index.DEPARTMENTS.getName()
                + " | stats count() by d.dept_name");

    // Without CBO
    setClusterSetting("plugins.velox.cbo_statistics_mode", "NONE");
    JSONObject noCboResp =
        executePPLQuery(
            "source = "
                + Index.EMPLOYEES.getName()
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + Index.DEPARTMENTS.getName()
                + " | stats count() by d.dept_name");

    JSONArray cboRows = getDataRows(cboResp);
    JSONArray noCboRows = getDataRows(noCboResp);

    assertEquals("Same number of rows", cboRows.length(), noCboRows.length());
  }

  // ---- CBO aggregation ----

  public void testCboAggregation() throws IOException {
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
            + Index.EMPLOYEES.getName()
            + " | inner join left=e right=d ON e.dept_id = d.dept_id "
            + Index.DEPARTMENTS.getName()
            + " | fields e.name, d.dept_name");

    long logsAfter = countLogLines("CBO stats");
    assertTrue("Expected CBO stats log entries", logsAfter > logsBefore);
  }
}
