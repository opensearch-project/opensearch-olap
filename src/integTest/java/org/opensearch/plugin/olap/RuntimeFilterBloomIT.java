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
 * Integration tests for the BLOOM variant of runtime filter. BLOOM triggers when build-side
 * cardinality exceeds {@code runtime_filter_max_cardinality} but stays within {@code
 * runtime_filter_bloom_max_cardinality}. We force this by pinning the TERMS cap to 1 while the test
 * data has 3 distinct dept_ids on the build side.
 *
 * <p>Correctness is the invariant: BLOOM with false positives must still return exactly the same
 * result set as the no-RF baseline, because the hash-join itself re-verifies the join key.
 */
public class RuntimeFilterBloomIT extends OlapRestTestCase {

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
    setClusterSetting("plugins.velox.runtime_filter_max_cardinality", "10000");
    setClusterSetting("plugins.velox.runtime_filter_bloom_max_cardinality", "10000000");
    super.tearDown();
  }

  /** Force BLOOM by setting the TERMS cap below the actual cardinality (3 dept_ids). */
  private void forceBloom() throws IOException {
    setClusterSetting("plugins.velox.runtime_filter_enabled", true);
    setClusterSetting("plugins.velox.runtime_filter_bloom_enabled", true);
    setClusterSetting("plugins.velox.runtime_filter_max_cardinality", "1");
    setClusterSetting("plugins.velox.runtime_filter_bloom_max_cardinality", "100000");
  }

  /** Disable RF entirely to produce the correctness baseline. */
  private void disableRf() throws IOException {
    setClusterSetting("plugins.velox.runtime_filter_enabled", false);
  }

  public void testBloomInnerJoinMatchesBaseline() throws IOException {
    // Baseline: RF disabled.
    disableRf();
    JSONObject baselineResp =
        executePPLQuery(
            "source = "
                + Index.EMPLOYEES.getName()
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + Index.DEPARTMENTS.getName()
                + " | fields e.name, d.dept_name");
    JSONArray baselineRows = getDataRows(baselineResp);

    // BLOOM path: TERMS cap = 1, bloom cap high.
    forceBloom();
    JSONObject bloomResp =
        executePPLQuery(
            "source = "
                + Index.EMPLOYEES.getName()
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + Index.DEPARTMENTS.getName()
                + " | fields e.name, d.dept_name");
    JSONArray bloomRows = getDataRows(bloomResp);

    assertEquals(
        "BLOOM must return the same number of rows as baseline",
        baselineRows.length(),
        bloomRows.length());

    Map<String, String> baselineMap = new HashMap<>();
    Map<String, String> bloomMap = new HashMap<>();
    for (int i = 0; i < baselineRows.length(); i++) {
      baselineMap.put(
          baselineRows.getJSONArray(i).getString(0), baselineRows.getJSONArray(i).getString(1));
    }
    for (int i = 0; i < bloomRows.length(); i++) {
      bloomMap.put(bloomRows.getJSONArray(i).getString(0), bloomRows.getJSONArray(i).getString(1));
    }
    assertEquals("BLOOM result set must equal baseline", baselineMap, bloomMap);
  }

  public void testBloomAbandonedAboveBloomCap() throws IOException {
    // Set both caps low enough to force NONE. With 3 dept_ids and bloom cap = 1, extract returns
    // NONE. The join must still succeed (we just lose the RF optimization).
    setClusterSetting("plugins.velox.runtime_filter_enabled", true);
    setClusterSetting("plugins.velox.runtime_filter_bloom_enabled", true);
    setClusterSetting("plugins.velox.runtime_filter_max_cardinality", "1");
    setClusterSetting("plugins.velox.runtime_filter_bloom_max_cardinality", "1");

    JSONObject resp =
        executePPLQuery(
            "source = "
                + Index.EMPLOYEES.getName()
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + Index.DEPARTMENTS.getName()
                + " | fields e.name, d.dept_name");
    JSONArray rows = getDataRows(resp);
    assertEquals("Join must produce 5 rows even when RF is abandoned", 5, rows.length());
  }

  public void testBloomEmitsExpectedLogLine() throws IOException {
    forceBloom();
    long beforeExtract = countLogLines("kind=BLOOM");

    executePPLQuery(
        "source = "
            + Index.EMPLOYEES.getName()
            + " | inner join left=e right=d ON e.dept_id = d.dept_id "
            + Index.DEPARTMENTS.getName()
            + " | fields e.name, d.dept_name");

    long afterExtract = countLogLines("kind=BLOOM");
    assertTrue(
        "Expected a coordinator or data-node log line mentioning kind=BLOOM",
        afterExtract > beforeExtract);
  }

  public void testBloomDisabledFallsBackToNoneAboveTermsCap() throws IOException {
    // With bloom disabled, exceeding the TERMS cap must abandon RF — same behavior as pre-BLOOM.
    setClusterSetting("plugins.velox.runtime_filter_enabled", true);
    setClusterSetting("plugins.velox.runtime_filter_bloom_enabled", false);
    setClusterSetting("plugins.velox.runtime_filter_max_cardinality", "1");

    JSONObject resp =
        executePPLQuery(
            "source = "
                + Index.EMPLOYEES.getName()
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + Index.DEPARTMENTS.getName()
                + " | fields e.name, d.dept_name");
    JSONArray rows = getDataRows(resp);
    assertEquals("Result must be correct with RF abandoned", 5, rows.length());
  }
}
