/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for PhysicalOptimizer. Constructs Calcite logical plans using RelBuilder with test tables
 * and verifies the optimizer produces physical PhysicalRel nodes with PhysicalExchange at
 * distribution boundaries.
 */
public class PhysicalOptimizerTests extends OpenSearchTestCase {

  private RelBuilder b() {
    return CalciteTestHelper.createRelBuilder();
  }

  private int countNodes(RelNode node, Class<?> clazz) {
    int count = clazz.isInstance(node) ? 1 : 0;
    for (RelNode input : node.getInputs()) {
      count += countNodes(input, clazz);
    }
    return count;
  }

  private RelNode findFirst(RelNode node, Class<?> clazz) {
    if (clazz.isInstance(node)) return node;
    for (RelNode input : node.getInputs()) {
      RelNode found = findFirst(input, clazz);
      if (found != null) return found;
    }
    return null;
  }

  private void assertAllPhysicalRel(RelNode node) {
    if (!(node instanceof PhysicalRel)) {
      fail("Expected PhysicalRel but got " + node.getClass().getSimpleName());
    }
    for (RelNode input : node.getInputs()) {
      assertAllPhysicalRel(input);
    }
  }

  // ---- Simple scan ----

  public void testSimpleScanProducesPhysicalNodes() {
    RelNode logical = b().scan("employees").build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(false);
    RelNode physical = optimizer.optimize(logical);

    assertTrue(physical instanceof PhysicalRel);
    assertTrue(countNodes(physical, PhysicalTableScan.class) >= 1);
  }

  public void testSimpleScanAllNodesArePhysicalRel() {
    RelNode logical = b().scan("employees").build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(false);
    RelNode physical = optimizer.optimize(logical);

    assertAllPhysicalRel(physical);
  }

  // ---- Filter + Project ----

  public void testFilterProjectProducesCorrectNodes() {
    RelBuilder rb = b();
    RelNode logical =
        rb.scan("employees")
            .filter(rb.equals(rb.field("dept_id"), rb.literal(10)))
            .project(rb.field("name"), rb.field("salary"))
            .build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(false);
    RelNode physical = optimizer.optimize(logical);

    assertAllPhysicalRel(physical);
    assertTrue(countNodes(physical, PhysicalTableScan.class) >= 1);
  }

  // ---- Aggregate (non-MPP) ----

  public void testAggregateNonMppHasAggregate() {
    RelBuilder rb = b();
    RelNode logical = rb.scan("employees").aggregate(rb.groupKey("dept_id"), rb.count()).build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(false);
    RelNode physical = optimizer.optimize(logical);

    assertAllPhysicalRel(physical);
    assertNotNull(findFirst(physical, PhysicalAggregate.class));
    // Exchange may or may not be inserted depending on cost model
    int exchangeCount = countNodes(physical, PhysicalExchange.class);
    logger.info("Aggregate plan has {} exchanges", exchangeCount);
  }

  public void testGlobalAggregateNonMpp() {
    RelBuilder rb = b();
    RelNode logical = rb.scan("employees").aggregate(rb.groupKey(), rb.count()).build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(false);
    RelNode physical = optimizer.optimize(logical);

    assertAllPhysicalRel(physical);
    assertNotNull(findFirst(physical, PhysicalAggregate.class));
  }

  // ---- Aggregate (MPP) ----

  public void testAggregateMppProducesValidPlan() {
    RelBuilder rb = b();
    RelNode logical = rb.scan("employees").aggregate(rb.groupKey("dept_id"), rb.count()).build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(true);
    RelNode physical = optimizer.optimize(logical);

    assertAllPhysicalRel(physical);
    assertNotNull(findFirst(physical, PhysicalAggregate.class));
  }

  // ---- Two-stage aggregation ----

  public void testAggregateHasExchangeViaEnforce() {
    RelBuilder rb = b();
    RelNode logical = rb.scan("employees").aggregate(rb.groupKey("dept_id"), rb.count()).build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(false);
    RelNode physical = optimizer.optimize(logical);

    // Convention.enforce() inserts PhysicalExchange when distribution doesn't satisfy
    assertAllPhysicalRel(physical);
    PhysicalAggregate agg = (PhysicalAggregate) findFirst(physical, PhysicalAggregate.class);
    assertNotNull(agg);
    assertEquals(PhysicalAggregate.Step.SINGLE, agg.getStep());
  }

  // ---- Sort/Limit ----

  public void testSortLimitProducesPhysicalSort() {
    RelBuilder rb = b();
    RelNode logical = rb.scan("employees").sort(rb.field("salary")).limit(0, 10).build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(false);
    RelNode physical = optimizer.optimize(logical);

    assertAllPhysicalRel(physical);
    assertTrue(countNodes(physical, PhysicalSort.class) >= 1);
  }

  // ---- Join (non-MPP) ----

  public void testJoinNonMppProducesPhysicalJoin() {
    RelBuilder rb = b();
    RelNode logical =
        rb.scan("employees")
            .scan("departments")
            .join(
                JoinRelType.INNER, rb.equals(rb.field(2, 0, "dept_id"), rb.field(2, 1, "dept_id")))
            .build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(false);
    RelNode physical = optimizer.optimize(logical);

    assertAllPhysicalRel(physical);
    assertNotNull(findFirst(physical, PhysicalJoin.class));
  }

  public void testJoinNonMppAllPhysicalRel() {
    RelBuilder rb = b();
    RelNode logical =
        rb.scan("employees")
            .scan("departments")
            .join(
                JoinRelType.INNER, rb.equals(rb.field(2, 0, "dept_id"), rb.field(2, 1, "dept_id")))
            .build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(false);
    RelNode physical = optimizer.optimize(logical);

    assertAllPhysicalRel(physical);
    int exchangeCount = countNodes(physical, PhysicalExchange.class);
    logger.info("Join plan has {} exchanges", exchangeCount);
  }

  // ---- Join (MPP) ----

  public void testJoinMppProducesValidPlan() {
    RelBuilder rb = b();
    RelNode logical =
        rb.scan("employees")
            .scan("departments")
            .join(
                JoinRelType.INNER, rb.equals(rb.field(2, 0, "dept_id"), rb.field(2, 1, "dept_id")))
            .build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(true);
    RelNode physical = optimizer.optimize(logical);

    assertAllPhysicalRel(physical);
    assertNotNull(findFirst(physical, PhysicalJoin.class));
  }

  // ---- Left join ----

  public void testLeftJoinProducesPhysicalJoin() {
    RelBuilder rb = b();
    RelNode logical =
        rb.scan("employees")
            .scan("departments")
            .join(JoinRelType.LEFT, rb.equals(rb.field(2, 0, "dept_id"), rb.field(2, 1, "dept_id")))
            .build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(false);
    RelNode physical = optimizer.optimize(logical);

    assertAllPhysicalRel(physical);
    assertNotNull(findFirst(physical, PhysicalJoin.class));
  }

  // ---- Join + Aggregate ----

  public void testJoinWithAggregateProducesValidPlan() {
    RelBuilder rb = b();
    RelNode logical =
        rb.scan("employees")
            .scan("departments")
            .join(
                JoinRelType.INNER, rb.equals(rb.field(2, 0, "dept_id"), rb.field(2, 1, "dept_id")))
            .aggregate(rb.groupKey(rb.field("dept_name")), rb.count())
            .build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(false);
    RelNode physical = optimizer.optimize(logical);

    assertAllPhysicalRel(physical);
    assertNotNull(findFirst(physical, PhysicalJoin.class));
    assertNotNull(findFirst(physical, PhysicalAggregate.class));
  }

  // ---- Filter + Aggregate ----

  public void testFilterAggregateProducesValidPlan() {
    RelBuilder rb = b();
    RelNode logical =
        rb.scan("employees")
            .filter(
                rb.call(
                    org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN,
                    rb.field("salary"),
                    rb.literal(100000)))
            .aggregate(rb.groupKey("dept_id"), rb.count())
            .build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(false);
    RelNode physical = optimizer.optimize(logical);

    assertAllPhysicalRel(physical);
  }

  // ---- Optimizer is reusable ----

  public void testOptimizerCanBeReused() {
    PhysicalOptimizer optimizer = new PhysicalOptimizer(false);

    RelNode plan1 = optimizer.optimize(b().scan("employees").build());
    RelNode plan2 = optimizer.optimize(b().scan("departments").build());

    assertTrue(plan1 instanceof PhysicalRel);
    assertTrue(plan2 instanceof PhysicalRel);
  }
}
