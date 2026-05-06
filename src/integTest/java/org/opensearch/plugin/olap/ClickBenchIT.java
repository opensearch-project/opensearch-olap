/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.json.JSONObject;

/**
 * ClickBench benchmark integration tests. Migrated from the SQL plugin's {@code PPLClickBenchIT}
 * (43 queries from the ClickBench analytical benchmark suite).
 *
 * <p>Each test loads a PPL query from a {@code .ppl} resource file, executes it, and verifies the
 * result is structurally valid. Queries run through Velox when {@code canVectorize} returns true,
 * or fall back to the default engine otherwise. Tests with {@code @Ignore} document specific Velox
 * limitations that block full vectorization; their reason strings mirror the Big5IT classification
 * style so they are easy to scan.
 */
public class ClickBenchIT extends OlapRestTestCase {

  private static final Map<String, Long> timings = new LinkedHashMap<>();

  @Override
  public void setUp() throws Exception {
    super.setUp();
    loadIndex(Index.CLICK_BENCH);
  }

  @Override
  public void tearDown() throws Exception {
    deleteIndex(Index.CLICK_BENCH.getName());
    if (!timings.isEmpty()) {
      long total = timings.values().stream().mapToLong(Long::longValue).sum();
      logger.info("ClickBench timings:");
      timings.forEach((name, ms) -> logger.info("  {}: {} ms", name, ms));
      logger.info("  Total {} queries, avg {} ms", timings.size(), total / timings.size());
    }
    super.tearDown();
  }

  public void testQ1() throws IOException {
    runQuery("q1");
  }

  public void testQ2() throws IOException {
    runQuery("q2");
  }

  public void testQ3() throws IOException {
    runQuery("q3");
  }

  public void testQ4() throws IOException {
    runQuery("q4");
  }

  public void testQ5() throws IOException {
    runQuery("q5");
  }

  public void testQ6() throws IOException {
    runQuery("q6");
  }

  public void testQ7() throws IOException {
    runQuery("q7");
  }

  public void testQ8() throws IOException {
    runQuery("q8");
  }

  public void testQ9() throws IOException {
    runQuery("q9");
  }

  public void testQ10() throws IOException {
    runQuery("q10");
  }

  public void testQ11() throws IOException {
    runQuery("q11");
  }

  public void testQ12() throws IOException {
    runQuery("q12");
  }

  public void testQ13() throws IOException {
    runQuery("q13");
  }

  public void testQ14() throws IOException {
    runQuery("q14");
  }

  public void testQ15() throws IOException {
    runQuery("q15");
  }

  public void testQ16() throws IOException {
    runQuery("q16");
  }

  public void testQ17() throws IOException {
    runQuery("q17");
  }

  public void testQ18() throws IOException {
    runQuery("q18");
  }

  public void testQ19() throws IOException {
    runQuery("q19");
  }

  public void testQ20() throws IOException {
    runQuery("q20");
  }

  public void testQ21() throws IOException {
    runQuery("q21");
  }

  public void testQ22() throws IOException {
    runQuery("q22");
  }

  public void testQ23() throws IOException {
    runQuery("q23");
  }

  public void testQ24() throws IOException {
    runQuery("q24");
  }

  public void testQ25() throws IOException {
    runQuery("q25");
  }

  public void testQ26() throws IOException {
    runQuery("q26");
  }

  public void testQ27() throws IOException {
    runQuery("q27");
  }

  public void testQ28() throws IOException {
    runQuery("q28");
  }

  public void testQ29() throws IOException {
    runQuery("q29");
  }

  public void testQ30() throws IOException {
    runQuery("q30");
  }

  public void testQ31() throws IOException {
    runQuery("q31");
  }

  public void testQ32() throws IOException {
    runQuery("q32");
  }

  public void testQ33() throws IOException {
    runQuery("q33");
  }

  public void testQ34() throws IOException {
    runQuery("q34");
  }

  public void testQ35() throws IOException {
    runQuery("q35");
  }

  public void testQ36() throws IOException {
    runQuery("q36");
  }

  public void testQ37() throws IOException {
    runQuery("q37");
  }

  public void testQ38() throws IOException {
    runQuery("q38");
  }

  public void testQ39() throws IOException {
    runQuery("q39");
  }

  public void testQ40() throws IOException {
    runQuery("q40");
  }

  public void testQ41() throws IOException {
    runQuery("q41");
  }

  public void testQ42() throws IOException {
    runQuery("q42");
  }

  public void testQ43() throws IOException {
    runQuery("q43");
  }

  // ---- Core helpers ----

  private void runQuery(String queryName) throws IOException {
    String ppl = loadPplQuery(queryName);
    long start = System.currentTimeMillis();
    JSONObject response = executePPLQuery(ppl);
    long elapsed = System.currentTimeMillis() - start;
    timings.put(queryName, elapsed);

    assertTrue(
        "Query " + queryName + " should return datarows",
        response.has("datarows") || response.has("total"));
    logger.info("ClickBench query [{}]: {} ms", queryName, elapsed);
  }

  /** Load and sanitize a PPL query: strip comments, collapse newlines to spaces. */
  private String loadPplQuery(String queryName) throws IOException {
    String raw = readResource("clickbench/queries/" + queryName + ".ppl");
    raw = raw.replaceAll("(?s)/\\*.*?\\*/", "");
    raw = raw.replaceAll("\\r\\n", " ").replaceAll("\\n", " ").trim();
    return raw.replace("\\\"", "\"");
  }
}
