/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical.rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.opensearch.plugin.olap.plan.physical.PhysicalAggregate;
import org.opensearch.plugin.olap.plan.physical.PhysicalConvention;

/**
 * Default aggregate rule: requests SINGLETON distribution from input. All data is gathered to the
 * coordinator before aggregation. Used when mpp_enabled=false.
 */
public class PhysicalAggregateRule extends ConverterRule {

  public static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .withConversion(
              LogicalAggregate.class,
              Convention.NONE,
              PhysicalConvention.INSTANCE,
              "PhysicalAggregateRule")
          .withRuleFactory(PhysicalAggregateRule::new);

  protected PhysicalAggregateRule(Config config) {
    super(config);
  }

  @Override
  public RelNode convert(RelNode rel) {
    Aggregate agg = (Aggregate) rel;
    // Convert child to PhysicalConvention only (no distribution requirement).
    // The aggregate itself declares SINGLETON distribution in its traitSet.
    // Convention.enforce() will auto-insert PhysicalExchange when the child's
    // distribution (e.g., RANDOM from scan) doesn't satisfy SINGLETON.
    RelNode input =
        convert(agg.getInput(), agg.getInput().getTraitSet().replace(PhysicalConvention.INSTANCE));
    return new PhysicalAggregate(
        rel.getCluster(),
        rel.getTraitSet().replace(PhysicalConvention.INSTANCE).replace(RelDistributions.SINGLETON),
        input,
        agg.getGroupSet(),
        agg.getGroupSets(),
        agg.getAggCallList(),
        PhysicalAggregate.Step.SINGLE);
  }
}
