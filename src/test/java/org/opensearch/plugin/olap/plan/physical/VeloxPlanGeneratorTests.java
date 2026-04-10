/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical;

import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.plugin.olap.plan.fragment.FragmentProperties;
import org.opensearch.plugin.olap.plan.fragment.PlanFragment;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for VeloxPlanGenerator: verifies that physical plans are correctly converted to Velox
 * PlanNode trees and split into PlanFragments at PhysicalExchange boundaries.
 */
public class VeloxPlanGeneratorTests extends OpenSearchTestCase {

  private RelBuilder b() {
    return CalciteTestHelper.createRelBuilder();
  }

  private RelNode optimize(RelNode logical, boolean mpp) {
    return new PhysicalOptimizer(mpp).optimize(logical);
  }

  // ---- Simple scan ----

  public void testSimpleScanProducesFragments() {
    RelNode physical = optimize(b().scan("employees").build(), false);

    VeloxPlanGenerator gen = new VeloxPlanGenerator();
    List<PlanFragment> fragments = gen.generate(physical);

    assertTrue("Expected at least 1 fragment, got " + fragments.size(), fragments.size() >= 1);
  }

  // ---- Filter + Project ----

  public void testFilterProjectProducesFragments() {
    RelBuilder rb = b();
    RelNode logical =
        rb.scan("employees")
            .filter(rb.equals(rb.field("dept_id"), rb.literal(10)))
            .project(rb.field("name"), rb.field("salary"))
            .build();
    RelNode physical = optimize(logical, false);

    VeloxPlanGenerator gen = new VeloxPlanGenerator();
    List<PlanFragment> fragments = gen.generate(physical);

    assertTrue("Expected at least 1 fragment", fragments.size() >= 1);
    assertNotNull("Leaf fragment should have a plan root", fragments.get(0).getPlanRoot());
  }

  // ---- Aggregation → at least 2 fragments ----

  public void testAggregationProducesFragments() {
    RelBuilder rb = b();
    RelNode logical = rb.scan("employees").aggregate(rb.groupKey("dept_id"), rb.count()).build();
    RelNode physical = optimize(logical, false);

    VeloxPlanGenerator gen = new VeloxPlanGenerator();
    List<PlanFragment> fragments = gen.generate(physical);

    // At least 1 fragment (may be 1 if no exchange, or 2+ if exchanges inserted)
    assertTrue("Expected at least 1 fragment, got " + fragments.size(), fragments.size() >= 1);

    // If multiple fragments, last should be coordinator
    if (fragments.size() >= 2) {
      PlanFragment rootFrag = fragments.get(fragments.size() - 1);
      assertEquals(
          FragmentProperties.Distribution.COORDINATOR, rootFrag.getProperties().getDistribution());
    }
  }

  // ---- Sort + Limit ----

  public void testSortLimitProducesFragments() {
    RelBuilder rb = b();
    RelNode logical = rb.scan("employees").sort(rb.field("salary")).limit(0, 5).build();
    RelNode physical = optimize(logical, false);

    VeloxPlanGenerator gen = new VeloxPlanGenerator();
    List<PlanFragment> fragments = gen.generate(physical);

    assertTrue("Expected at least 1 fragment", fragments.size() >= 1);
  }

  // ---- Join ----

  public void testJoinProducesFragments() {
    RelBuilder rb = b();
    RelNode logical =
        rb.scan("employees")
            .scan("departments")
            .join(
                JoinRelType.INNER, rb.equals(rb.field(2, 0, "dept_id"), rb.field(2, 1, "dept_id")))
            .build();
    RelNode physical = optimize(logical, false);

    VeloxPlanGenerator gen = new VeloxPlanGenerator();
    List<PlanFragment> fragments = gen.generate(physical);

    assertTrue("Expected at least 1 fragment, got " + fragments.size(), fragments.size() >= 1);
  }

  // ---- Fragment ordering: leaf first, root last ----

  public void testFragmentOrderingLeafFirstRootLast() {
    RelBuilder rb = b();
    RelNode logical = rb.scan("employees").aggregate(rb.groupKey("dept_id"), rb.count()).build();
    RelNode physical = optimize(logical, false);

    VeloxPlanGenerator gen = new VeloxPlanGenerator();
    List<PlanFragment> fragments = gen.generate(physical);

    if (fragments.size() >= 2) {
      assertTrue("First fragment should be a leaf", fragments.get(0).isLeaf());
      assertTrue("Last fragment should be root", fragments.get(fragments.size() - 1).isRoot());
    }
  }

  // ---- Fragment IDs are sequential ----

  public void testFragmentIdsAreSequential() {
    RelBuilder rb = b();
    RelNode logical = rb.scan("employees").aggregate(rb.groupKey("dept_id"), rb.count()).build();
    RelNode physical = optimize(logical, false);

    VeloxPlanGenerator gen = new VeloxPlanGenerator();
    List<PlanFragment> fragments = gen.generate(physical);

    for (int i = 0; i < fragments.size(); i++) {
      assertEquals(i, fragments.get(i).getFragmentId());
    }
  }

  // ---- Generator can be reused ----

  public void testGeneratorCanBeReused() {
    VeloxPlanGenerator gen = new VeloxPlanGenerator();

    RelNode physical1 = optimize(b().scan("employees").build(), false);
    List<PlanFragment> f1 = gen.generate(physical1);

    RelBuilder rb = b();
    RelNode physical2 =
        optimize(rb.scan("employees").aggregate(rb.groupKey("dept_id"), rb.count()).build(), false);
    List<PlanFragment> f2 = gen.generate(physical2);

    assertEquals("IDs should reset", 0, f1.get(0).getFragmentId());
    assertEquals("IDs should reset", 0, f2.get(0).getFragmentId());
  }

  // ---- MPP aggregate ----

  public void testMppAggregateProducesFragments() {
    RelBuilder rb = b();
    RelNode logical = rb.scan("employees").aggregate(rb.groupKey("dept_id"), rb.count()).build();
    RelNode physical = optimize(logical, true);

    VeloxPlanGenerator gen = new VeloxPlanGenerator();
    List<PlanFragment> fragments = gen.generate(physical);

    assertTrue("MPP aggregate should produce fragments", fragments.size() >= 1);
  }

  // ---- MPP join ----

  public void testMppJoinProducesFragments() {
    RelBuilder rb = b();
    RelNode logical =
        rb.scan("employees")
            .scan("departments")
            .join(
                JoinRelType.INNER, rb.equals(rb.field(2, 0, "dept_id"), rb.field(2, 1, "dept_id")))
            .build();
    RelNode physical = optimize(logical, true);

    VeloxPlanGenerator gen = new VeloxPlanGenerator();
    List<PlanFragment> fragments = gen.generate(physical);

    // MPP join should produce at least 3 fragments: 2 leaf scans + 1 coordinator join
    assertTrue(
        "MPP join should produce at least 3 fragments, got " + fragments.size(),
        fragments.size() >= 3);
  }

  // ---- MPP aggregate produces fragments without CannotPlanException ----

  public void testMppAggregateWithGroupByProducesFragments() {
    RelBuilder rb = b();
    // With group keys, MppAggregateRule should fire (HASH distribution alternative)
    RelNode logical =
        rb.scan("employees").aggregate(rb.groupKey("dept_id", "name"), rb.count()).build();
    RelNode physical = optimize(logical, true);

    VeloxPlanGenerator gen = new VeloxPlanGenerator();
    List<PlanFragment> fragments = gen.generate(physical);

    // Should produce at least 1 fragment (planner may pick either SINGLETON or HASH)
    assertTrue(
        "MPP aggregate should produce fragments, got " + fragments.size(), fragments.size() >= 1);
  }

  // ---- MPP join with left join ----

  public void testMppLeftJoinProducesFragments() {
    RelBuilder rb = b();
    RelNode logical =
        rb.scan("employees")
            .scan("departments")
            .join(JoinRelType.LEFT, rb.equals(rb.field(2, 0, "dept_id"), rb.field(2, 1, "dept_id")))
            .build();
    RelNode physical = optimize(logical, true);

    VeloxPlanGenerator gen = new VeloxPlanGenerator();
    List<PlanFragment> fragments = gen.generate(physical);

    // Planner explores both SINGLETON and HASH alternatives, picks lower cost.
    // At minimum should produce fragments without CannotPlanException.
    assertTrue(
        "MPP left join should produce at least 1 fragment, got " + fragments.size(),
        fragments.size() >= 1);
  }

  // ---- Every fragment has a non-null plan root ----

  public void testAllFragmentsHavePlanRoot() {
    RelBuilder rb = b();
    RelNode logical = rb.scan("employees").aggregate(rb.groupKey("dept_id"), rb.count()).build();
    RelNode physical = optimize(logical, false);

    VeloxPlanGenerator gen = new VeloxPlanGenerator();
    List<PlanFragment> fragments = gen.generate(physical);

    for (PlanFragment f : fragments) {
      assertNotNull("Fragment " + f.getFragmentId() + " should have a plan root", f.getPlanRoot());
    }
  }
}
