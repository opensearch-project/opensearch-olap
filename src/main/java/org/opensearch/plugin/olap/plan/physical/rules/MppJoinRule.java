/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical.rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.opensearch.plugin.olap.plan.physical.PhysicalConvention;
import org.opensearch.plugin.olap.plan.physical.PhysicalJoin;

/**
 * MPP join rule: requests HASH distribution from both inputs on their respective join keys. This
 * enables hash shuffle join where matching rows co-locate on the same worker. Registered only when
 * mpp_enabled=true. Falls back to null (letting PhysicalJoinRule handle it) for non-equi joins.
 */
public class MppJoinRule extends ConverterRule {

  public static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .withConversion(
              LogicalJoin.class, Convention.NONE, PhysicalConvention.INSTANCE, "MppJoinRule")
          .withRuleFactory(MppJoinRule::new);

  protected MppJoinRule(Config config) {
    super(config);
  }

  @Override
  public RelNode convert(RelNode rel) {
    Join join = (Join) rel;
    JoinInfo joinInfo = join.analyzeCondition();

    // Hash shuffle only works for equi-joins
    if (joinInfo.leftKeys.isEmpty()) {
      return null;
    }

    RelNode left =
        convert(
            join.getLeft(),
            join.getLeft()
                .getTraitSet()
                .replace(PhysicalConvention.INSTANCE)
                .replace(RelDistributions.hash(joinInfo.leftKeys)));
    RelNode right =
        convert(
            join.getRight(),
            join.getRight()
                .getTraitSet()
                .replace(PhysicalConvention.INSTANCE)
                .replace(RelDistributions.hash(joinInfo.rightKeys)));

    return new PhysicalJoin(
        rel.getCluster(),
        rel.getTraitSet()
            .replace(PhysicalConvention.INSTANCE)
            .replace(RelDistributions.hash(joinInfo.leftKeys)),
        left,
        right,
        join.getCondition(),
        join.getVariablesSet(),
        join.getJoinType());
  }
}
