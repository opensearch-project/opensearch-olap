/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Regression tests pinning PhysicalExchange insertion behavior for rules that need a real exchange
 * boundary.
 *
 * <p>Context: {@code Convention.enforce()} is wired up in {@link PhysicalConvention} but does not
 * reliably fire at scan→parent boundaries — {@code PhysicalTableScan} declares distribution=ANY,
 * which satisfies every other distribution, so enforce never sees a mismatch there. Any rule that
 * needs a concrete exchange boundary (joins, MPP hash-shuffle, MPP aggregate) therefore calls
 * {@code PhysicalExchange.create()} explicitly on its inputs.
 *
 * <p>A plausible-looking refactor — have rules declare target distribution on their output and rely
 * on enforce — silently drops exchanges. In turn, {@code VeloxPlanGenerator.convertAggregate} and
 * the fragment split logic produce degraded plans: single-stage aggregation instead of
 * PARTIAL+FINAL, a join with no shuffle boundary, etc. These regressions only surface in end-to-end
 * integration tests (where they show up as wrong results or performance cliffs).
 *
 * <p>This test class locks in the invariants at unit-test time: (a) join rules insert per-input
 * exchanges explicitly, (b) cost ordering HASH < SINGLETON so MPP alternatives can win.
 */
public class ExchangeInsertionTests extends OpenSearchTestCase {

  private RelBuilder b() {
    return CalciteTestHelper.createRelBuilder();
  }

  private List<PhysicalExchange> findAllExchanges(RelNode node) {
    List<PhysicalExchange> out = new ArrayList<>();
    collect(node, out);
    return out;
  }

  private void collect(RelNode node, List<PhysicalExchange> out) {
    if (node instanceof PhysicalExchange) {
      out.add((PhysicalExchange) node);
    }
    for (RelNode input : node.getInputs()) {
      collect(input, out);
    }
  }

  // ---- Join rules: explicit exchanges per input ----

  /**
   * PhysicalJoinRule (non-MPP) must insert PhysicalExchange(SINGLETON) explicitly on both inputs.
   * Attempts to move this path to enforce-driven dropped the exchanges; this test pins the concrete
   * count so the same mistake gets caught here rather than in JoinIT/MppJoinIT.
   */
  public void testPhysicalJoin_insertsTwoSingletonExchanges() {
    RelBuilder rb = b();
    RelNode logical =
        rb.scan("employees")
            .scan("departments")
            .join(
                JoinRelType.INNER, rb.equals(rb.field(2, 0, "dept_id"), rb.field(2, 1, "dept_id")))
            .build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(false);
    RelNode physical = optimizer.optimize(logical);

    long singletons =
        findAllExchanges(physical).stream()
            .filter(e -> e.getDistribution().getType() == RelDistribution.Type.SINGLETON)
            .count();
    assertTrue(
        "PhysicalJoinRule must insert at least 2 SINGLETON exchanges (one per input). Got "
            + singletons,
        singletons >= 2);
  }

  /**
   * MppJoinRule must insert PhysicalExchange(HASH(leftKeys)) and PhysicalExchange(HASH(rightKeys))
   * explicitly. Different per-input distributions cannot be expressed as a single parent trait, so
   * enforce is fundamentally not an option here — this invariant must never be refactored out.
   */
  public void testMppJoin_insertsTwoHashExchanges() {
    RelBuilder rb = b();
    RelNode logical =
        rb.scan("employees")
            .scan("departments")
            .join(
                JoinRelType.INNER, rb.equals(rb.field(2, 0, "dept_id"), rb.field(2, 1, "dept_id")))
            .build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(true);
    RelNode physical = optimizer.optimize(logical);

    long hashes =
        findAllExchanges(physical).stream()
            .filter(e -> e.getDistribution().getType() == RelDistribution.Type.HASH_DISTRIBUTED)
            .count();
    assertTrue(
        "MppJoinRule must insert at least 2 HASH exchanges when mpp_enabled=true. Got " + hashes,
        hashes >= 2);
  }

  /** Every plan must end up fully PhysicalRel-converted, never leaking logical nodes. */
  public void testAllRelsArePhysicalAfterOptimization() {
    RelBuilder rb = b();
    RelNode logical =
        rb.scan("employees")
            .scan("departments")
            .join(
                JoinRelType.INNER, rb.equals(rb.field(2, 0, "dept_id"), rb.field(2, 1, "dept_id")))
            .build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(true);
    RelNode physical = optimizer.optimize(logical);

    assertPhysicalRecursive(physical);
  }

  private void assertPhysicalRecursive(RelNode node) {
    assertTrue(
        "Non-physical node leaked: " + node.getClass().getSimpleName() + " -> " + node,
        node instanceof PhysicalRel);
    for (RelNode input : node.getInputs()) {
      assertPhysicalRecursive(input);
    }
  }

  // ---- Cost-factor regression ----

  /**
   * PhysicalExchange.computeSelfCost gives HASH 0.8x SINGLETON so the planner prefers HASH
   * alternatives when both are available. This is the mechanism by which MppJoinRule /
   * MppAggregateRule win over PhysicalJoinRule / PhysicalAggregateRule when mpp_enabled=true on
   * realistic-scale queries. A future cost tweak that inverts this ordering would silently disable
   * MPP even when both rules are registered.
   */
  public void testExchangeCost_hashCheaperThanSingleton() {
    RelBuilder rb = b();
    RelNode scan = rb.scan("employees").build();

    PhysicalExchange singleton = PhysicalExchange.create(scan, RelDistributions.SINGLETON);
    PhysicalExchange hash = PhysicalExchange.create(scan, RelDistributions.hash(List.of(0)));

    RelOptPlanner planner = new VolcanoPlanner();
    RelMetadataQuery mq = RelMetadataQuery.instance();

    RelOptCost singletonCost = singleton.computeSelfCost(planner, mq);
    RelOptCost hashCost = hash.computeSelfCost(planner, mq);

    assertNotNull(singletonCost);
    assertNotNull(hashCost);
    assertTrue(
        "HASH must be strictly cheaper than SINGLETON. Got singleton="
            + singletonCost
            + ", hash="
            + hashCost,
        hashCost.isLt(singletonCost));
  }
}
