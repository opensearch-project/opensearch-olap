/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

/**
 * Physical exchange node representing a data redistribution boundary.
 *
 * <p>Inserted automatically by {@link PhysicalConvention#enforce(RelNode, RelTraitSet)} when a
 * parent's distribution requirement isn't satisfied by the child. During plan generation, each
 * PhysicalExchange becomes a fragment boundary in the distributed execution plan.
 *
 * <p>Distribution types:
 *
 * <ul>
 *   <li>SINGLETON -> GATHER: all data flows to coordinator
 *   <li>HASH_DISTRIBUTED -> SHUFFLE: data hash-partitioned by key columns
 *   <li>BROADCAST_DISTRIBUTED -> BROADCAST: data replicated to all nodes
 * </ul>
 */
public class PhysicalExchange extends SingleRel implements PhysicalRel {

  private final RelDistribution distribution;

  protected PhysicalExchange(
      RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelDistribution distribution) {
    super(cluster, traitSet, input);
    this.distribution = distribution;
  }

  public static PhysicalExchange create(RelNode input, RelDistribution distribution) {
    RelOptCluster cluster = input.getCluster();
    RelTraitSet traitSet =
        input.getTraitSet().replace(PhysicalConvention.INSTANCE).replace(distribution);
    return new PhysicalExchange(cluster, traitSet, input, distribution);
  }

  public RelDistribution getDistribution() {
    return distribution;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new PhysicalExchange(getCluster(), traitSet, sole(inputs), distribution);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    double rowCount = mq.getRowCount(getInput());
    // Exchange cost is proportional to data movement (network I/O)
    return planner.getCostFactory().makeCost(rowCount, 0, rowCount);
  }
}
