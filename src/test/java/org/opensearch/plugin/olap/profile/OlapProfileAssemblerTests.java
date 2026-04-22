/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.profile;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.opensearch.plugin.olap.transport.ExecuteFragmentResponse;
import org.opensearch.sql.monitor.profile.QueryProfile;
import org.opensearch.test.OpenSearchTestCase;

public class OlapProfileAssemblerTests extends OpenSearchTestCase {

  private ExecuteFragmentResponse responseWithProfile(OlapTaskProfile tp) {
    ExecuteFragmentResponse r = ExecuteFragmentResponse.success(tp.getRowsEmitted(), new byte[0]);
    r.setTaskProfile(tp);
    return r;
  }

  public void testEmptyInputsProduceRootOnly() {
    QueryProfile.PlanNode root = OlapProfileAssembler.buildSnapshot(null, Collections.emptyList());
    assertNotNull(root);
    assertTrue(root.getNode().startsWith("VeloxQuery"));
    assertNull(root.getChildren());
  }

  public void testFragmentsAreGroupedByFragmentId() {
    OlapTaskProfile a = new OlapTaskProfile(0, 0, "n1", 10_000, 1000, 50, 50, "BLOOM", 128);
    OlapTaskProfile b = new OlapTaskProfile(0, 1, "n2", 20_000, 2000, 70, 70, "BLOOM", 128);
    OlapTaskProfile c = new OlapTaskProfile(1, 0, "coord", 5_000, 0, 0, 120, "NONE", 0);

    QueryProfile.PlanNode root =
        OlapProfileAssembler.buildSnapshot(
            "rf=BLOOM bloomBytes=128",
            Arrays.asList(responseWithProfile(a), responseWithProfile(b), responseWithProfile(c)));

    assertTrue(root.getNode().contains("rf=BLOOM bloomBytes=128"));
    assertTrue(root.getNode().contains("docsRead=3000"));
    assertTrue(root.getNode().contains("docsMatched=120"));

    List<QueryProfile.PlanNode> frags = root.getChildren();
    assertEquals("two fragments expected", 2, frags.size());
    QueryProfile.PlanNode frag0 = frags.get(0);
    assertTrue(frag0.getNode().startsWith("Fragment[0]"));
    assertTrue(frag0.getNode().contains("rf=BLOOM"));
    assertTrue(frag0.getNode().contains("docsRead=3000"));
    assertEquals("frag 0 has 2 tasks", 2, frag0.getChildren().size());

    QueryProfile.PlanNode frag1 = frags.get(1);
    assertTrue(frag1.getNode().startsWith("Fragment[1]"));
    assertTrue(frag1.getNode().contains("rf=NONE"));
    assertEquals("frag 1 has 1 task", 1, frag1.getChildren().size());
  }

  public void testResponsesWithoutProfileAreIgnored() {
    // One response has a profile, the other doesn't. Only the profiled one shows up.
    OlapTaskProfile tp = new OlapTaskProfile(0, 0, "n1", 1, 10, 5, 5, "NONE", 0);
    ExecuteFragmentResponse r1 = responseWithProfile(tp);
    ExecuteFragmentResponse r2 = ExecuteFragmentResponse.success(42, new byte[0]);

    QueryProfile.PlanNode root = OlapProfileAssembler.buildSnapshot(null, Arrays.asList(r1, r2));
    List<QueryProfile.PlanNode> frags = root.getChildren();
    assertEquals(1, frags.size());
    assertEquals(1, frags.get(0).getChildren().size());
  }

  public void testRootTimeIsSumOfFragmentTimes() {
    OlapTaskProfile a = new OlapTaskProfile(0, 0, "n", 5_000_000L, 0, 0, 0, "NONE", 0); // 5ms
    OlapTaskProfile b = new OlapTaskProfile(1, 0, "n", 3_000_000L, 0, 0, 0, "NONE", 0); // 3ms

    QueryProfile.PlanNode root =
        OlapProfileAssembler.buildSnapshot(
            null, Arrays.asList(responseWithProfile(a), responseWithProfile(b)));
    // Root time should be 5 + 3 = 8ms. ProfileUtils.roundToMillis rounds to 2dp.
    assertEquals(8.0, root.getTimeMillis(), 0.01);
  }

  /**
   * Fragment-level time must sum across the fragment's tasks. Regression-guard: if the assembler
   * accidentally reports only the last task's time or averages, this fails.
   */
  public void testFragmentTimeIsSumOfTaskTimes() {
    OlapTaskProfile a = new OlapTaskProfile(0, 0, "n1", 4_000_000L, 0, 0, 0, "NONE", 0); // 4ms
    OlapTaskProfile b = new OlapTaskProfile(0, 1, "n2", 6_000_000L, 0, 0, 0, "NONE", 0); // 6ms
    QueryProfile.PlanNode root =
        OlapProfileAssembler.buildSnapshot(
            null, Arrays.asList(responseWithProfile(a), responseWithProfile(b)));
    QueryProfile.PlanNode frag = root.getChildren().get(0);
    assertEquals("fragment time must be 4 + 6 = 10 ms", 10.0, frag.getTimeMillis(), 0.01);
    assertEquals(2, frag.getChildren().size());
    assertEquals(4.0, frag.getChildren().get(0).getTimeMillis(), 0.01);
    assertEquals(6.0, frag.getChildren().get(1).getTimeMillis(), 0.01);
  }

  /**
   * A fragment is labelled with the non-NONE task rfKind when any task on that fragment applied a
   * runtime filter. This lets users see at a glance which fragment ran BLOOM vs which ran without
   * RF, even when a single fragment has mixed tasks (e.g. one shard had the field, another didn't).
   */
  public void testFragmentRfKindPromotesFromNoneToBloom() {
    OlapTaskProfile plainTask = new OlapTaskProfile(0, 0, "n1", 1, 0, 0, 0, "NONE", 0);
    OlapTaskProfile bloomTask = new OlapTaskProfile(0, 1, "n2", 1, 100, 10, 10, "BLOOM", 64);
    QueryProfile.PlanNode root =
        OlapProfileAssembler.buildSnapshot(
            null, Arrays.asList(responseWithProfile(plainTask), responseWithProfile(bloomTask)));
    QueryProfile.PlanNode frag = root.getChildren().get(0);
    assertTrue(
        "fragment label should promote to rf=BLOOM: " + frag.getNode(),
        frag.getNode().contains("rf=BLOOM"));
  }

  /** Empty RF summary is stripped — label starts with VeloxQuery directly. */
  public void testEmptyRfSummaryStripsSuffix() {
    OlapTaskProfile t = new OlapTaskProfile(0, 0, "n", 1, 0, 0, 0, "NONE", 0);
    QueryProfile.PlanNode root =
        OlapProfileAssembler.buildSnapshot("", Collections.singletonList(responseWithProfile(t)));
    assertTrue(root.getNode().startsWith("VeloxQuery docsRead="));
  }

  /** Failure responses (without a task profile) are ignored, not treated as zero-valued tasks. */
  public void testFailureResponsesDoNotPoisonSums() {
    OlapTaskProfile good = new OlapTaskProfile(0, 0, "n", 1_000_000L, 100, 50, 50, "BLOOM", 32);
    ExecuteFragmentResponse failed = ExecuteFragmentResponse.failure("boom");
    QueryProfile.PlanNode root =
        OlapProfileAssembler.buildSnapshot(null, Arrays.asList(responseWithProfile(good), failed));
    assertEquals(1, root.getChildren().size());
    assertTrue(root.getNode().contains("docsRead=100"));
    assertTrue(root.getNode().contains("docsMatched=50"));
  }
}
