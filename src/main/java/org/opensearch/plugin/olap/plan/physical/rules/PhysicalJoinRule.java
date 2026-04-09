/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical.rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.opensearch.plugin.olap.plan.physical.PhysicalConvention;
import org.opensearch.plugin.olap.plan.physical.PhysicalExchange;
import org.opensearch.plugin.olap.plan.physical.PhysicalJoin;

/**
 * Default join rule: coordinator-centric. Requests SINGLETON from both inputs. All data is gathered
 * to the coordinator before join execution. Used when mpp_enabled=false.
 */
public class PhysicalJoinRule extends ConverterRule {

  public static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .withConversion(
              LogicalJoin.class,
              Convention.NONE,
              PhysicalConvention.INSTANCE,
              "PhysicalJoinRule")
          .withRuleFactory(PhysicalJoinRule::new);

  protected PhysicalJoinRule(Config config) {
    super(config);
  }

  @Override
  public RelNode convert(RelNode rel) {
    Join join = (Join) rel;
    RelNode left =
        convert(
            join.getLeft(), join.getLeft().getTraitSet().replace(PhysicalConvention.INSTANCE));
    RelNode right =
        convert(
            join.getRight(), join.getRight().getTraitSet().replace(PhysicalConvention.INSTANCE));
    left = PhysicalExchange.create(left, RelDistributions.SINGLETON);
    right = PhysicalExchange.create(right, RelDistributions.SINGLETON);
    return new PhysicalJoin(
        rel.getCluster(),
        rel.getTraitSet().replace(PhysicalConvention.INSTANCE),
        left,
        right,
        join.getCondition(),
        join.getVariablesSet(),
        join.getJoinType());
  }
}
