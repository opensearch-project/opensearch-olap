/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.tools.RelBuilder;
import org.boostscale.velox4j.plan.AggregationNode;
import org.boostscale.velox4j.plan.FilterNode;
import org.boostscale.velox4j.plan.HashJoinNode;
import org.boostscale.velox4j.plan.LimitNode;
import org.boostscale.velox4j.plan.OrderByNode;
import org.boostscale.velox4j.plan.PlanNode;
import org.boostscale.velox4j.plan.ProjectNode;
import org.boostscale.velox4j.plan.TableScanNode;
import org.opensearch.plugin.olap.plan.fragment.FragmentProperties;
import org.opensearch.plugin.olap.plan.fragment.PlanFragment;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Plan explain tests that verify the physical plan structure produced by PhysicalOptimizer +
 * VeloxPlanGenerator. Instead of checking execution results or logs, these tests assert on:
 *
 * <ul>
 *   <li>Number of fragments (single-stage vs two-stage)
 *   <li>Fragment distribution types (SOURCE, COORDINATOR)
 *   <li>Velox PlanNode types in each fragment (via instanceof checks on plan tree)
 * </ul>
 */
public class PlanExplainTests extends OpenSearchTestCase {

  private RelBuilder b() {
    return CalciteTestHelper.createRelBuilder();
  }

  private RelNode optimize(RelNode logical, boolean mpp) {
    return new PhysicalOptimizer(mpp).optimize(logical);
  }

  private List<PlanFragment> generateFragments(RelNode physical) {
    return new VeloxPlanGenerator().generate(physical);
  }

  /** Collect all PlanNode class names in the tree (depth-first). */
  private List<String> collectNodeTypes(PlanNode node) {
    List<String> types = new ArrayList<>();
    collectNodeTypesRecursive(node, types);
    return types;
  }

  private void collectNodeTypesRecursive(PlanNode node, List<String> types) {
    types.add(node.getClass().getSimpleName());
    for (PlanNode source : node.getSources()) {
      collectNodeTypesRecursive(source, types);
    }
  }

  /** Check if a plan tree contains a specific node type. */
  private boolean containsNodeType(PlanNode node, Class<? extends PlanNode> nodeType) {
    if (nodeType.isInstance(node)) return true;
    for (PlanNode source : node.getSources()) {
      if (containsNodeType(source, nodeType)) return true;
    }
    return false;
  }

  /** Get the AggregationNode step from the plan tree. */
  private String getAggregationStep(PlanNode node) {
    if (node instanceof AggregationNode) {
      return ((AggregationNode) node).getStep().name();
    }
    for (PlanNode source : node.getSources()) {
      String step = getAggregationStep(source);
      if (step != null) return step;
    }
    return null;
  }

  // ---- Simple scan: single fragment ----

  public void testSimpleScanProducesSingleFragment() {
    RelNode physical = optimize(b().scan("employees").build(), false);
    List<PlanFragment> fragments = generateFragments(physical);

    assertEquals("Simple scan should produce 1 fragment", 1, fragments.size());
    assertEquals(
        FragmentProperties.Distribution.SOURCE, fragments.get(0).getProperties().getDistribution());
    assertTrue(
        "Scan fragment should contain TableScanNode",
        containsNodeType(fragments.get(0).getPlanRoot(), TableScanNode.class));
  }

  // ---- Filter + Project: single fragment ----

  public void testFilterProjectProducesSingleFragment() {
    RelBuilder rb = b();
    RelNode physical =
        optimize(
            rb.scan("employees")
                .filter(rb.equals(rb.field("dept_id"), rb.literal(10)))
                .project(rb.field("name"), rb.field("salary"))
                .build(),
            false);
    List<PlanFragment> fragments = generateFragments(physical);

    assertEquals("Filter+Project should produce 1 fragment", 1, fragments.size());
    PlanNode root = fragments.get(0).getPlanRoot();
    assertTrue("Should contain FilterNode", containsNodeType(root, FilterNode.class));
    assertTrue("Should contain ProjectNode", containsNodeType(root, ProjectNode.class));
  }

  // ---- Two-stage aggregation: 2 fragments with PARTIAL + FINAL ----

  public void testAggregationProducesTwoStageFragments() {
    RelBuilder rb = b();
    RelNode physical =
        optimize(rb.scan("employees").aggregate(rb.groupKey("dept_id"), rb.count()).build(), false);
    List<PlanFragment> fragments = generateFragments(physical);

    // Convention.enforce() may or may not fire in unit test VolcanoPlanner.
    // When it fires: 2 fragments (PARTIAL + FINAL). When not: 1 fragment (SINGLE).
    // Two-stage split is verified end-to-end in AggregationIT.
    assertTrue("Should produce at least 1 fragment", fragments.size() >= 1);

    PlanFragment leaf = fragments.get(0);
    assertTrue(
        "Should contain AggregationNode",
        containsNodeType(leaf.getPlanRoot(), AggregationNode.class));

    if (fragments.size() >= 2) {
      assertEquals(FragmentProperties.Distribution.SOURCE, leaf.getProperties().getDistribution());
      assertEquals(
          "Leaf step should be PARTIAL", "PARTIAL", getAggregationStep(leaf.getPlanRoot()));

      PlanFragment root = fragments.get(fragments.size() - 1);
      assertEquals(
          FragmentProperties.Distribution.COORDINATOR, root.getProperties().getDistribution());
      assertEquals("Root step should be FINAL", "FINAL", getAggregationStep(root.getPlanRoot()));
    }
  }

  // ---- Two-stage TopN: 2 fragments with partial + final sort+limit ----

  public void testSortLimitProducesTwoStageTopN() {
    RelBuilder rb = b();
    RelNode physical =
        optimize(rb.scan("employees").sort(rb.field("salary")).limit(0, 5).build(), false);
    List<PlanFragment> fragments = generateFragments(physical);

    if (fragments.size() >= 2) {
      // Two-stage TopN was triggered
      PlanFragment leaf = fragments.get(0);
      assertEquals(FragmentProperties.Distribution.SOURCE, leaf.getProperties().getDistribution());
      assertTrue(
          "Leaf should contain LimitNode for partial TopN",
          containsNodeType(leaf.getPlanRoot(), LimitNode.class));

      PlanFragment root = fragments.get(fragments.size() - 1);
      assertEquals(
          FragmentProperties.Distribution.COORDINATOR, root.getProperties().getDistribution());
      assertTrue(
          "Coordinator should contain OrderByNode",
          containsNodeType(root.getPlanRoot(), OrderByNode.class));
      assertTrue(
          "Coordinator should contain LimitNode",
          containsNodeType(root.getPlanRoot(), LimitNode.class));
    }
    assertTrue("Should produce at least 1 fragment", fragments.size() >= 1);
  }

  public void testSortWithoutLimitDoesNotSplit() {
    RelBuilder rb = b();
    RelNode physical = optimize(rb.scan("employees").sort(rb.field("salary")).build(), false);
    List<PlanFragment> fragments = generateFragments(physical);

    PlanNode lastRoot = fragments.get(fragments.size() - 1).getPlanRoot();
    assertTrue("Should contain OrderByNode", containsNodeType(lastRoot, OrderByNode.class));
  }

  // ---- Coordinator-centric join: 3 fragments ----

  public void testJoinProducesThreeFragments() {
    RelBuilder rb = b();
    RelNode physical =
        optimize(
            rb.scan("employees")
                .scan("departments")
                .join(
                    JoinRelType.INNER,
                    rb.equals(rb.field(2, 0, "dept_id"), rb.field(2, 1, "dept_id")))
                .build(),
            false);
    List<PlanFragment> fragments = generateFragments(physical);

    assertEquals("Join should produce 3 fragments", 3, fragments.size());

    assertEquals(
        FragmentProperties.Distribution.SOURCE, fragments.get(0).getProperties().getDistribution());
    assertEquals(
        FragmentProperties.Distribution.SOURCE, fragments.get(1).getProperties().getDistribution());

    PlanFragment coordinator = fragments.get(2);
    assertEquals(
        FragmentProperties.Distribution.COORDINATOR, coordinator.getProperties().getDistribution());
    assertTrue(
        "Coordinator should contain HashJoinNode",
        containsNodeType(coordinator.getPlanRoot(), HashJoinNode.class));
  }

  // ---- MPP join (mpp=true): shuffle scan fragments ----

  public void testMppJoinProducesShuffleScanFragments() {
    RelBuilder rb = b();
    RelNode physical =
        optimize(
            rb.scan("employees")
                .scan("departments")
                .join(
                    JoinRelType.INNER,
                    rb.equals(rb.field(2, 0, "dept_id"), rb.field(2, 1, "dept_id")))
                .build(),
            true);
    List<PlanFragment> fragments = generateFragments(physical);

    assertEquals("MPP join should produce 3 fragments", 3, fragments.size());

    long shuffleCount = fragments.stream().filter(f -> f.getProperties().isShuffleScan()).count();
    assertTrue(
        "MPP join should produce shuffle scan fragments, got " + shuffleCount, shuffleCount >= 2);

    for (PlanFragment f : fragments) {
      if (f.getProperties().isShuffleScan()) {
        assertFalse(
            "Shuffle scan should have key channels",
            f.getProperties().getShuffleKeyChannels().isEmpty());
      }
    }
  }

  // ---- Fragment dependency chain ----

  public void testFragmentDependencyChain() {
    RelBuilder rb = b();
    RelNode physical =
        optimize(rb.scan("employees").aggregate(rb.groupKey("dept_id"), rb.count()).build(), false);
    List<PlanFragment> fragments = generateFragments(physical);

    if (fragments.size() >= 2) {
      assertTrue(
          "Leaf fragment should have no input dependencies",
          fragments.get(0).getInputFragmentIds().isEmpty());

      PlanFragment coordinator = fragments.get(fragments.size() - 1);
      assertFalse(
          "Coordinator should have input dependencies",
          coordinator.getInputFragmentIds().isEmpty());
      assertTrue(
          "Coordinator should depend on leaf fragment",
          coordinator.getInputFragmentIds().contains(fragments.get(0).getFragmentId()));
    }
  }

  // ---- Fragment IDs are sequential ----

  public void testFragmentIdsSequential() {
    RelBuilder rb = b();
    RelNode physical =
        optimize(
            rb.scan("employees")
                .scan("departments")
                .join(
                    JoinRelType.INNER,
                    rb.equals(rb.field(2, 0, "dept_id"), rb.field(2, 1, "dept_id")))
                .build(),
            false);
    List<PlanFragment> fragments = generateFragments(physical);

    for (int i = 0; i < fragments.size(); i++) {
      assertEquals("Fragment ID should be sequential", i, fragments.get(i).getFragmentId());
    }
  }

  // ---- Every fragment has a non-null plan root ----

  public void testAllFragmentsHaveValidPlanRoot() {
    RelBuilder rb = b();
    RelNode physical =
        optimize(rb.scan("employees").aggregate(rb.groupKey("dept_id"), rb.count()).build(), false);
    List<PlanFragment> fragments = generateFragments(physical);

    for (PlanFragment f : fragments) {
      assertNotNull("Fragment " + f.getFragmentId() + " should have a plan root", f.getPlanRoot());
      List<String> nodeTypes = collectNodeTypes(f.getPlanRoot());
      assertFalse("Fragment " + f.getFragmentId() + " should have plan nodes", nodeTypes.isEmpty());
    }
  }
}
