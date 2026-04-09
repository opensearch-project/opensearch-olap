/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;

/**
 * Physical aggregate with a step indicator (SINGLE, PARTIAL, FINAL).
 *
 * <p>The TwoStageAggRule transforms SINGLE -> PARTIAL + Exchange + FINAL for distributed
 * aggregation. PARTIAL runs on data nodes (below the exchange), FINAL runs on the coordinator.
 */
public class PhysicalAggregate extends Aggregate implements PhysicalRel {

  /** Aggregation step for distributed execution. */
  public enum Step {
    /** No distribution split. Runs as a single-pass aggregation. */
    SINGLE,
    /** First stage: partial aggregation on data nodes, produces intermediate accumulators. */
    PARTIAL,
    /** Second stage: final aggregation on coordinator, merges intermediate results. */
    FINAL
  }

  private final Step step;

  public PhysicalAggregate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls,
      Step step) {
    super(cluster, traitSet, List.of(), input, groupSet, groupSets, aggCalls);
    this.step = step;
  }

  public Step getStep() {
    return step;
  }

  @Override
  public PhysicalAggregate copy(
      RelTraitSet traitSet,
      RelNode input,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    return new PhysicalAggregate(
        getCluster(), traitSet, input, groupSet, groupSets, aggCalls, step);
  }

  /** Create a copy with a different step. */
  public PhysicalAggregate withStep(Step newStep) {
    return new PhysicalAggregate(
        getCluster(),
        getTraitSet(),
        getInput(),
        getGroupSet(),
        getGroupSets(),
        getAggCallList(),
        newStep);
  }
}
