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
 * MPP aggregate rule: requests HASH distribution from input when group keys exist. This allows
 * partial aggregation to run distributed (each partition aggregates its own keys). Registered only
 * when mpp_enabled=true. The VolcanoPlanner explores this as an alternative to
 * PhysicalAggregateRule and picks the lower cost.
 */
public class MppAggregateRule extends ConverterRule {

  public static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .withConversion(
              LogicalAggregate.class,
              Convention.NONE,
              PhysicalConvention.INSTANCE,
              "MppAggregateRule")
          .withRuleFactory(MppAggregateRule::new);

  protected MppAggregateRule(Config config) {
    super(config);
  }

  @Override
  public RelNode convert(RelNode rel) {
    Aggregate agg = (Aggregate) rel;

    // MPP only makes sense with group keys — without them, all data must go to one place anyway
    if (agg.getGroupCount() == 0) {
      return null;
    }

    // Request hash distribution by group keys from input
    RelNode input =
        convert(
            agg.getInput(),
            agg.getInput()
                .getTraitSet()
                .replace(PhysicalConvention.INSTANCE)
                .replace(RelDistributions.hash(agg.getGroupSet().asList())));

    return new PhysicalAggregate(
        rel.getCluster(),
        rel.getTraitSet()
            .replace(PhysicalConvention.INSTANCE)
            .replace(RelDistributions.hash(agg.getGroupSet().asList())),
        input,
        agg.getGroupSet(),
        agg.getGroupSets(),
        agg.getAggCallList(),
        PhysicalAggregate.Step.SINGLE);
  }
}
