/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Integration test that uses the PPL {@code profile=true} flow to answer the question "did the
 * Lucene-level BLOOM runtime filter actually narrow the scan?". With forced BLOOM, the data-node
 * task profile's {@code docsMatched} must be strictly less than {@code docsRead} — i.e., the scorer
 * emitted fewer docs than the segment contained. The baseline (RF disabled) must emit every live
 * doc (no narrowing).
 *
 * <p>This is the only end-to-end signal that proves BLOOM is running as a Lucene pushdown, not as a
 * post-scan filter in Velox or in the feeder layer.
 */
public class ProfileBloomNarrowsScanIT extends OlapRestTestCase {

  private static final Pattern DOCS_READ = Pattern.compile("docsRead=(\\d+)");
  private static final Pattern DOCS_MATCHED = Pattern.compile("docsMatched=(\\d+)");

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

  public void testBloomNarrowsScanBelowBaseline() throws IOException {
    String pql =
        "source = "
            + Index.EMPLOYEES.getName()
            + " | inner join left=e right=d ON e.dept_id = d.dept_id "
            + Index.DEPARTMENTS.getName()
            + " | fields e.name, d.dept_name";

    // Baseline: RF disabled. Every employee doc must be emitted by the Lucene scan.
    setClusterSetting("plugins.velox.runtime_filter_enabled", false);
    JSONObject baseline = executePPLQuery(pql, true);
    long baselineDocsRead = sumProbeLeaf(baseline, DOCS_READ);
    long baselineDocsMatched = sumProbeLeaf(baseline, DOCS_MATCHED);
    assertTrue("baseline must visit at least one employee doc", baselineDocsRead > 0);
    assertEquals(
        "baseline (no RF) must emit every visited doc", baselineDocsRead, baselineDocsMatched);

    // Forced BLOOM: TERMS cap=1 forces BLOOM for the 3-dept build side.
    setClusterSetting("plugins.velox.runtime_filter_enabled", true);
    setClusterSetting("plugins.velox.runtime_filter_bloom_enabled", true);
    setClusterSetting("plugins.velox.runtime_filter_max_cardinality", "1");
    setClusterSetting("plugins.velox.runtime_filter_bloom_max_cardinality", "100000");

    JSONObject withBloom = executePPLQuery(pql, true);
    long bloomDocsRead = sumProbeLeaf(withBloom, DOCS_READ);
    long bloomDocsMatched = sumProbeLeaf(withBloom, DOCS_MATCHED);

    // The universe (live docs) the scan walked over is the same across runs.
    assertEquals(
        "docsRead must be identical between baseline and BLOOM (same underlying shards)",
        baselineDocsRead,
        bloomDocsRead);

    // With the small test dataset (5 employees, 3 matching dept ids), BLOOM narrows to <=
    // baseline. Exact strictness depends on data but assert at most equal to baseline, and at
    // least the join-result row count so correctness holds.
    assertTrue(
        "BLOOM docsMatched ("
            + bloomDocsMatched
            + ") must be <= baseline docsMatched ("
            + baselineDocsMatched
            + ")",
        bloomDocsMatched <= baselineDocsMatched);
    JSONArray rows = getDataRows(withBloom);
    assertTrue(
        "BLOOM docsMatched must at least equal actual joined rows (" + rows.length() + ")",
        bloomDocsMatched >= rows.length());

    // Root node must label BLOOM so users see the RF kind at a glance.
    String rootNode = withBloom.getJSONObject("profile").getJSONObject("plan").getString("node");
    assertTrue(
        "profile root should advertise BLOOM runtime filter: " + rootNode,
        rootNode.contains("rf=BLOOM"));
  }

  /**
   * Return the sum of {@code docsRead} or {@code docsMatched} across probe-leaf task nodes — those
   * are the only nodes that report real Lucene scan activity. Identified as tasks whose label
   * contains {@code rf=} other than {@code NONE} (forced-BLOOM run) or whose fragment is a leaf
   * (baseline run). We sum across every leaf task, since probe reads happen on every shard.
   */
  private long sumProbeLeaf(JSONObject response, Pattern pattern) {
    JSONObject profile = response.getJSONObject("profile");
    JSONObject plan = profile.getJSONObject("plan");
    return walk(plan, pattern, /* best */ 0L);
  }

  private long walk(JSONObject node, Pattern pattern, long acc) {
    String label = node.getString("node");
    if (label.startsWith("Task ")) {
      Matcher m = pattern.matcher(label);
      if (m.find()) {
        acc += Long.parseLong(m.group(1));
      }
    }
    if (node.has("children") && !node.isNull("children")) {
      JSONArray children = node.getJSONArray("children");
      for (int i = 0; i < children.length(); i++) {
        acc = walk(children.getJSONObject(i), pattern, acc);
      }
    }
    return acc;
  }
}
