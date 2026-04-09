/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelDistributions;
import org.opensearch.plugin.olap.plan.physical.PhysicalAggregate;
import org.opensearch.plugin.olap.plan.physical.PhysicalExchange;

/**
 * Transforms a SINGLE-step aggregate sitting above an Exchange into a two-stage aggregate:
 *
 * <pre>
 * Before:  PhysicalAggregate(SINGLE) -> PhysicalExchange
 * After:   PhysicalAggregate(FINAL)  -> PhysicalExchange(SINGLETON) -> PhysicalAggregate(PARTIAL)
 * </pre>
 *
 * <p>This reduces network transfer by performing partial aggregation on data nodes (below the
 * exchange) before gathering results to the coordinator for final aggregation.
 */
public class TwoStageAggRule extends RelOptRule {

  public static final TwoStageAggRule INSTANCE = new TwoStageAggRule();

  private TwoStageAggRule() {
    super(
        operand(PhysicalAggregate.class, operand(PhysicalExchange.class, any())),
        "TwoStageAggRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    PhysicalAggregate agg = call.rel(0);
    PhysicalExchange exchange = call.rel(1);

    if (agg.getStep() != PhysicalAggregate.Step.SINGLE) {
      return;
    }

    // Create PARTIAL aggregate below the exchange (runs on data nodes)
    PhysicalAggregate partialAgg =
        new PhysicalAggregate(
            agg.getCluster(),
            exchange.getInput().getTraitSet(),
            exchange.getInput(),
            agg.getGroupSet(),
            agg.getGroupSets(),
            agg.getAggCallList(),
            PhysicalAggregate.Step.PARTIAL);

    // Create exchange above partial with SINGLETON target
    PhysicalExchange newExchange = PhysicalExchange.create(partialAgg, RelDistributions.SINGLETON);

    // Create FINAL aggregate above the exchange (runs on coordinator)
    PhysicalAggregate finalAgg =
        new PhysicalAggregate(
            agg.getCluster(),
            agg.getTraitSet(),
            newExchange,
            agg.getGroupSet(),
            agg.getGroupSets(),
            agg.getAggCallList(),
            PhysicalAggregate.Step.FINAL);

    call.transformTo(finalAgg);
  }
}
