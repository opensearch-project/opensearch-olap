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
 * Integration tests for segment-level parallel reads. Verifies that queries produce correct results
 * with both parallel (segment_parallelism=4) and sequential (segment_parallelism=1) reads, and that
 * the results are identical.
 *
 * <p>Uses the standard test_olap index (5 rows, 1 shard). To ensure multiple segments exist, a
 * force merge is NOT performed — the bulk insert + refresh may produce multiple segments.
 */
public class ParallelReadIT extends OlapRestTestCase {

  @Override
  public void setUp() throws Exception {
    super.setUp();
    loadIndex(Index.TEST_OLAP);
  }

  @Override
  public void tearDown() throws Exception {
    deleteIndex(Index.TEST_OLAP.getName());
    setClusterSetting("plugins.velox.segment_parallelism", "4"); // restore default
    super.tearDown();
  }

  private void setSegmentParallelism(int parallelism) throws IOException {
    setClusterSetting("plugins.velox.segment_parallelism", String.valueOf(parallelism));
  }

  // ---- Parallel reads (default) produce correct results ----

  public void testParallelScanProducesCorrectAggregation() throws IOException {
    setSegmentParallelism(4);
    JSONObject response = executePPLQuery("source=test_olap | stats count() by city");
    JSONArray rows = getDataRows(response);

    Map<String, Long> countByCity = new HashMap<>();
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      countByCity.put(row.getString(1), row.getLong(0));
    }

    assertEquals(Long.valueOf(2), countByCity.get("Seattle"));
    assertEquals(Long.valueOf(2), countByCity.get("Portland"));
    assertEquals(Long.valueOf(1), countByCity.get("Denver"));
  }

  public void testParallelScanWithFilter() throws IOException {
    setSegmentParallelism(4);
    JSONObject response = executePPLQuery("source=test_olap | where age > 30 | stats count()");
    JSONArray rows = getDataRows(response);

    assertEquals(1, rows.length());
    assertEquals(3, rows.getJSONArray(0).getLong(0)); // Alice(35), Charlie(42), Diana(31)
  }

  // ---- Sequential reads produce correct results ----

  public void testSequentialScanProducesCorrectAggregation() throws IOException {
    setSegmentParallelism(1);
    JSONObject response = executePPLQuery("source=test_olap | stats count() by city");
    JSONArray rows = getDataRows(response);

    Map<String, Long> countByCity = new HashMap<>();
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      countByCity.put(row.getString(1), row.getLong(0));
    }

    assertEquals(Long.valueOf(2), countByCity.get("Seattle"));
    assertEquals(Long.valueOf(2), countByCity.get("Portland"));
    assertEquals(Long.valueOf(1), countByCity.get("Denver"));
  }

  public void testSequentialScanWithFilter() throws IOException {
    setSegmentParallelism(1);
    JSONObject response = executePPLQuery("source=test_olap | where age > 30 | stats count()");
    JSONArray rows = getDataRows(response);

    assertEquals(1, rows.length());
    assertEquals(3, rows.getJSONArray(0).getLong(0));
  }

  // ---- Parallel and sequential produce identical results ----

  public void testParallelAndSequentialProduceSameAvg() throws IOException {
    // Parallel
    setSegmentParallelism(4);
    JSONObject parallelResp = executePPLQuery("source=test_olap | stats avg(salary) by city");

    // Sequential
    setSegmentParallelism(1);
    JSONObject sequentialResp = executePPLQuery("source=test_olap | stats avg(salary) by city");

    // Compare
    Map<String, Double> parallelAvg = extractAvgByCity(parallelResp);
    Map<String, Double> sequentialAvg = extractAvgByCity(sequentialResp);

    assertEquals("Same number of groups", parallelAvg.size(), sequentialAvg.size());
    for (String city : parallelAvg.keySet()) {
      assertEquals("Same avg for " + city, parallelAvg.get(city), sequentialAvg.get(city), 0.01);
    }
  }

  public void testParallelAndSequentialProduceSameSum() throws IOException {
    setSegmentParallelism(4);
    JSONObject parallelResp = executePPLQuery("source=test_olap | stats sum(salary), count()");

    setSegmentParallelism(1);
    JSONObject sequentialResp = executePPLQuery("source=test_olap | stats sum(salary), count()");

    JSONArray pRows = getDataRows(parallelResp);
    JSONArray sRows = getDataRows(sequentialResp);

    assertEquals(pRows.getJSONArray(0).getDouble(0), sRows.getJSONArray(0).getDouble(0), 0.01);
    assertEquals(pRows.getJSONArray(0).getLong(1), sRows.getJSONArray(0).getLong(1));
  }

  // ---- Helper ----

  private Map<String, Double> extractAvgByCity(JSONObject response) {
    JSONArray rows = getDataRows(response);
    Map<String, Double> result = new HashMap<>();
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      result.put(row.getString(1), row.getDouble(0));
    }
    return result;
  }
}
