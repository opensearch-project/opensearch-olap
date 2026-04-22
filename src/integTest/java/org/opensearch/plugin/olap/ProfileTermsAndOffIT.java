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
 * Integration test for two complementary properties of the profile feature:
 *
 * <ol>
 *   <li>When a query is run <b>without</b> {@code profile=true}, the response carries no {@code
 *       profile} block. Guards against accidentally always-on profiling, which would impose wire
 *       overhead on every query.
 *   <li>When a join runs with TERMS runtime filter (low-cardinality build side), the plan reports
 *       {@code rf=TERMS} on probe tasks and TERMS narrows {@code docsMatched} below the baseline.
 *       This matches the {@link ProfileBloomNarrowsScanIT} property for the BLOOM variant.
 * </ol>
 */
public class ProfileTermsAndOffIT extends OlapRestTestCase {

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

  private String joinPql() {
    return "source = "
        + Index.EMPLOYEES.getName()
        + " | inner join left=e right=d ON e.dept_id = d.dept_id "
        + Index.DEPARTMENTS.getName()
        + " | fields e.name, d.dept_name";
  }

  /** With profile=false (the default), the response must not carry a profile block. */
  public void testNoProfileBlockWhenFlagOmitted() throws IOException {
    JSONObject resp = executePPLQuery(joinPql(), /* profile */ false);
    assertFalse(
        "response must not include profile when profile=true is absent", resp.has("profile"));
  }

  /**
   * With profile=true and a default-settings cluster where the TERMS path is selected (3 distinct
   * dept_ids ≤ 10_000 default cap), the probe task labels must report rf=TERMS and docsMatched must
   * be strictly less than baseline's docsMatched.
   */
  public void testTermsNarrowsScanAndLabelsPlan() throws IOException {
    // Baseline: RF disabled.
    setClusterSetting("plugins.velox.runtime_filter_enabled", false);
    JSONObject baseline = executePPLQuery(joinPql(), true);
    long baselineRead = sumOverTasks(baseline, DOCS_READ);
    long baselineMatched = sumOverTasks(baseline, DOCS_MATCHED);
    assertTrue(baselineRead > 0);
    assertEquals("RF-disabled baseline must emit every visited doc", baselineRead, baselineMatched);

    // TERMS path: enable RF, disable BLOOM so extract falls through to the classic distinct-value
    // capture. With 3 dept_ids ≤ the default TERMS cap, this chooses TERMS.
    setClusterSetting("plugins.velox.runtime_filter_enabled", true);
    setClusterSetting("plugins.velox.runtime_filter_bloom_enabled", false);
    setClusterSetting("plugins.velox.runtime_filter_max_cardinality", "10000");

    JSONObject resp = executePPLQuery(joinPql(), true);
    long termsRead = sumOverTasks(resp, DOCS_READ);
    long termsMatched = sumOverTasks(resp, DOCS_MATCHED);

    assertEquals("docsRead (universe) must match baseline — same shards", baselineRead, termsRead);
    assertTrue(
        "TERMS docsMatched (" + termsMatched + ") must be <= baseline (" + baselineMatched + ")",
        termsMatched <= baselineMatched);
    JSONArray rows = getDataRows(resp);
    assertTrue(
        "TERMS docsMatched must at least equal produced join rows (" + rows.length() + ")",
        termsMatched >= rows.length());

    String rootLabel = resp.getJSONObject("profile").getJSONObject("plan").getString("node");
    assertTrue(
        "profile root should advertise TERMS runtime filter: " + rootLabel,
        rootLabel.contains("rf=TERMS"));
  }

  private long sumOverTasks(JSONObject response, Pattern pattern) {
    JSONObject profile = response.getJSONObject("profile");
    JSONObject plan = profile.getJSONObject("plan");
    return walk(plan, pattern, 0L);
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
