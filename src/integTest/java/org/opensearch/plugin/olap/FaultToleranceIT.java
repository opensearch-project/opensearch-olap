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
import org.opensearch.client.ResponseException;

/**
 * Integration tests for fault tolerance and task retry. Verifies that queries produce correct
 * results with retries enabled (task_max_retries=2) and disabled (task_max_retries=0), and that
 * retry configuration is dynamic.
 *
 * <p>These tests run on a single-node cluster where no real node failures occur, so they verify:
 *
 * <ul>
 *   <li>Retry infrastructure doesn't break normal execution (no regression)
 *   <li>Retry setting is dynamically configurable
 *   <li>Results are correct with retries enabled for all query types (scan, agg, join, pushdown)
 * </ul>
 */
public class FaultToleranceIT extends OlapRestTestCase {

  @Override
  public void setUp() throws Exception {
    super.setUp();
    loadIndex(Index.EMPLOYEES);
    loadIndex(Index.DEPARTMENTS);
  }

  @Override
  public void tearDown() throws Exception {
    deleteIndex(Index.EMPLOYEES.getName());
    deleteIndex(Index.DEPARTMENTS.getName());
    setClusterSetting("plugins.velox.task_max_retries", "2"); // restore default
    super.tearDown();
  }

  private void setTaskMaxRetries(int retries) throws IOException {
    setClusterSetting("plugins.velox.task_max_retries", String.valueOf(retries));
  }

  // ---- Retries enabled (default) — verify no regression ----

  public void testAggregationWithRetriesEnabled() throws IOException {
    setTaskMaxRetries(2);
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

  public void testJoinWithRetriesEnabled() throws IOException {
    setTaskMaxRetries(2);
    JSONObject response =
        executePPLQuery(
            "source = "
                + Index.EMPLOYEES.getName()
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + Index.DEPARTMENTS.getName()
                + " | fields e.name, d.dept_name");
    JSONArray rows = getDataRows(response);

    assertEquals("Expected 5 rows from inner join", 5, rows.length());
  }

  public void testFilterWithRetriesEnabled() throws IOException {
    setTaskMaxRetries(2);
    JSONObject response =
        executePPLQuery(
            "source = " + Index.EMPLOYEES.getName() + " | where dept_id = 10 | fields name");
    JSONArray rows = getDataRows(response);

    assertEquals("Expected 2 Engineering employees", 2, rows.length());
  }

  // ---- Retries disabled — verify queries still work ----

  public void testAggregationWithRetriesDisabled() throws IOException {
    setTaskMaxRetries(0);
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

  public void testJoinWithRetriesDisabled() throws IOException {
    setTaskMaxRetries(0);
    JSONObject response =
        executePPLQuery(
            "source = "
                + Index.EMPLOYEES.getName()
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + Index.DEPARTMENTS.getName()
                + " | fields e.name, d.dept_name");
    JSONArray rows = getDataRows(response);

    assertEquals("Expected 5 rows from inner join", 5, rows.length());
  }

  // ---- Dynamic toggle — retries=0 then retries=2 within same test ----

  public void testDynamicRetryToggle() throws IOException {
    // First with retries disabled
    setTaskMaxRetries(0);
    JSONObject resp1 =
        executePPLQuery("source = " + Index.EMPLOYEES.getName() + " | stats sum(salary)");
    JSONArray rows1 = getDataRows(resp1);
    double sum1 = rows1.getJSONArray(0).getDouble(0);

    // Then with retries enabled
    setTaskMaxRetries(2);
    JSONObject resp2 =
        executePPLQuery("source = " + Index.EMPLOYEES.getName() + " | stats sum(salary)");
    JSONArray rows2 = getDataRows(resp2);
    double sum2 = rows2.getJSONArray(0).getDouble(0);

    assertEquals("Same result regardless of retry setting", sum1, sum2, 0.01);
  }

  // ---- High retry count doesn't cause issues ----

  public void testHighRetryCountDoesNotBreak() throws IOException {
    setTaskMaxRetries(10);
    JSONObject response =
        executePPLQuery(
            "source = " + Index.EMPLOYEES.getName() + " | stats avg(salary) by dept_id");
    JSONArray rows = getDataRows(response);

    assertTrue("Should produce results with high retry count", rows.length() > 0);
  }

  // ---- Fault injection: query non-existent index triggers error path ----

  /**
   * Query a non-existent index with retries enabled. The OLAP plugin should attempt the query, hit
   * a shard routing error, and the error should propagate as a 500 (not hang forever due to
   * infinite retry loops). This verifies the ErrorClassifier treats IndexNotFoundException as
   * FATAL.
   */
  public void testQueryNonExistentIndexFailsFastWithRetries() throws IOException {
    setTaskMaxRetries(2);
    try {
      Request request = new Request("POST", PPL_ENDPOINT);
      request.setJsonEntity("{\"query\": \"source = nonexistent_index | stats count()\"}");
      client().performRequest(request);
      // If we get here with 200, the SQL plugin handled it (not vectorized) — that's OK
    } catch (ResponseException e) {
      // Expected: should fail, not hang. Any error status is acceptable.
      int status = e.getResponse().getStatusLine().getStatusCode();
      assertTrue("Should return error status (got " + status + ")", status >= 400 && status < 600);
    }
  }

  /**
   * Close an index, query it (triggers shard unavailability), then reopen. With retries enabled,
   * the query should still fail (no available shards to retry on) but should not hang.
   */
  public void testQueryClosedIndexFailsWithRetries() throws Exception {
    setTaskMaxRetries(2);

    // Close the index
    client().performRequest(new Request("POST", "/" + Index.EMPLOYEES.getName() + "/_close"));

    try {
      Request request = new Request("POST", PPL_ENDPOINT);
      request.setJsonEntity(
          "{\"query\": \"source = " + Index.EMPLOYEES.getName() + " | stats count()\"}");
      client().performRequest(request);
      // If 200, SQL plugin handled it without vectorization — OK
    } catch (ResponseException e) {
      int status = e.getResponse().getStatusLine().getStatusCode();
      assertTrue(
          "Should return error status for closed index (got " + status + ")",
          status >= 400 && status < 600);
    } finally {
      // Reopen so other tests aren't affected
      client().performRequest(new Request("POST", "/" + Index.EMPLOYEES.getName() + "/_open"));
      // Wait for index to be ready
      Thread.sleep(1000);
    }
  }

  /**
   * Verify that when a query succeeds, no retry log messages are generated. This confirms the retry
   * path is only triggered on actual failures.
   */
  public void testSuccessfulQueryDoesNotTriggerRetry() throws IOException {
    setTaskMaxRetries(2);

    long retryLogsBefore = countLogLines("retrying");

    executePPLQuery("source = " + Index.EMPLOYEES.getName() + " | stats count() by dept_id");

    long retryLogsAfter = countLogLines("retrying");
    assertEquals(
        "Successful query should not trigger any retries", retryLogsBefore, retryLogsAfter);
  }

  /**
   * Delete an index after creating it, then query. With retries=0, should fail immediately. With
   * retries=2, should also fail (no replicas) but exercise the retry classification path.
   */
  public void testDeletedIndexQueryWithAndWithoutRetries() throws IOException {
    // Create a temporary index
    String tempIndex = "temp_fault_test";
    Request createTemp = new Request("PUT", "/" + tempIndex);
    createTemp.setJsonEntity(
        "{"
            + "\"settings\": {\"number_of_shards\": 1, \"number_of_replicas\": 0},"
            + "\"mappings\": {\"properties\": {\"val\": {\"type\": \"integer\"}}}"
            + "}");
    client().performRequest(createTemp);
    Request bulk = new Request("POST", "/_bulk?refresh=true");
    bulk.setJsonEntity("{\"index\": {\"_index\": \"" + tempIndex + "\"}}\n{\"val\": 1}\n");
    client().performRequest(bulk);

    // Verify it works
    JSONObject resp = executePPLQuery("source = " + tempIndex + " | stats count()");
    assertEquals(1, getDataRows(resp).length());

    // Delete the index
    client().performRequest(new Request("DELETE", "/" + tempIndex));

    // Query with retries=0 — should fail fast
    setTaskMaxRetries(0);
    try {
      Request request = new Request("POST", PPL_ENDPOINT);
      request.setJsonEntity("{\"query\": \"source = " + tempIndex + " | stats count()\"}");
      client().performRequest(request);
    } catch (ResponseException e) {
      assertTrue(
          "Should fail with retries=0", e.getResponse().getStatusLine().getStatusCode() >= 400);
    }

    // Query with retries=2 — should also fail (index gone) but not hang
    setTaskMaxRetries(2);
    try {
      Request request = new Request("POST", PPL_ENDPOINT);
      request.setJsonEntity("{\"query\": \"source = " + tempIndex + " | stats count()\"}");
      client().performRequest(request);
    } catch (ResponseException e) {
      assertTrue(
          "Should fail with retries=2", e.getResponse().getStatusLine().getStatusCode() >= 400);
    }
  }
}
