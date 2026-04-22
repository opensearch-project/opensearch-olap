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
 * Integration test for the two-stage BLOOM build path (PARTIAL per data node + FINAL bitwise-OR
 * merge at coordinator). The {@code plugins.velox.runtime_filter_bloom_two_stage} toggle must be
 * functionally transparent — both true (two-stage) and false (single-stage rebuild) must produce
 * the same result set as the no-RF baseline, because the hash-join re-verifies keys regardless of
 * whether the bloom is built from local shards or from broadcast data.
 */
public class RuntimeFilterBloomTwoStageIT extends OlapRestTestCase {

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
    setClusterSetting("plugins.velox.mpp_enabled", false);
    setClusterSetting("plugins.velox.runtime_filter_enabled", true);
    setClusterSetting("plugins.velox.runtime_filter_bloom_enabled", true);
    setClusterSetting("plugins.velox.runtime_filter_bloom_two_stage", true);
    setClusterSetting("plugins.velox.runtime_filter_max_cardinality", "10000");
    setClusterSetting("plugins.velox.runtime_filter_bloom_max_cardinality", "10000000");
    super.tearDown();
  }

  /** Force BLOOM by pinning the TERMS cap below actual build cardinality. */
  private void forceBloom(boolean twoStage) throws IOException {
    setClusterSetting("plugins.velox.runtime_filter_enabled", true);
    setClusterSetting("plugins.velox.runtime_filter_bloom_enabled", true);
    setClusterSetting("plugins.velox.runtime_filter_bloom_two_stage", twoStage);
    setClusterSetting("plugins.velox.runtime_filter_max_cardinality", "1");
    setClusterSetting("plugins.velox.runtime_filter_bloom_max_cardinality", "100000");
  }

  private JSONArray runJoin() throws IOException {
    JSONObject resp =
        executePPLQuery(
            "source = "
                + Index.EMPLOYEES.getName()
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + Index.DEPARTMENTS.getName()
                + " | fields e.name, d.dept_name");
    return getDataRows(resp);
  }

  private Map<String, String> rowsToMap(JSONArray rows) {
    Map<String, String> m = new HashMap<>();
    for (int i = 0; i < rows.length(); i++) {
      m.put(rows.getJSONArray(i).getString(0), rows.getJSONArray(i).getString(1));
    }
    return m;
  }

  /**
   * Correctness equivalence: two-stage=true and two-stage=false must yield the same row set as the
   * RF-disabled baseline. False positives are allowed by the bloom but are filtered out by the
   * hash-join's exact key re-verification.
   */
  public void testTwoStageAndSingleStageMatchBaseline() throws IOException {
    // Baseline: RF disabled.
    setClusterSetting("plugins.velox.runtime_filter_enabled", false);
    Map<String, String> baseline = rowsToMap(runJoin());

    // Two-stage path (PARTIAL per data node + OR merge at coordinator).
    forceBloom(true);
    Map<String, String> twoStage = rowsToMap(runJoin());
    assertEquals("two-stage BLOOM must equal baseline", baseline, twoStage);

    // Single-stage path (coordinator rebuilds bloom from broadcast data).
    forceBloom(false);
    Map<String, String> singleStage = rowsToMap(runJoin());
    assertEquals("single-stage BLOOM must equal baseline", baseline, singleStage);
  }
}
