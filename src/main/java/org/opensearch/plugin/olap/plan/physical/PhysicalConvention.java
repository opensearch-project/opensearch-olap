/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical;

import javax.annotation.Nullable;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;

/**
 * Calcite Convention for physical operators.
 *
 * <p>This convention is used by the {@link PhysicalOptimizer} to convert logical RelNodes
 * (Convention.NONE) into physical operators with distribution traits. The VolcanoPlanner explores
 * alternatives and picks the lowest-cost plan.
 *
 * <p>The key method is {@link #enforce(RelNode, RelTraitSet)}: when the planner detects that a
 * child's distribution trait doesn't satisfy the parent's requirement, it calls enforce() which
 * inserts a {@link PhysicalExchange} node — the redistribution boundary that becomes a fragment
 * boundary during plan generation.
 */
public enum PhysicalConvention implements Convention {
  INSTANCE;

  @Override
  public Class<?> getInterface() {
    return PhysicalRel.class;
  }

  @Override
  public String getName() {
    return "PHYSICAL";
  }

  @Override
  public RelTraitDef getTraitDef() {
    return ConventionTraitDef.INSTANCE;
  }

  @Override
  public boolean satisfies(RelTrait trait) {
    return this == trait;
  }

  @Override
  public void register(RelOptPlanner planner) {}

  @Override
  public String toString() {
    return getName();
  }

  /**
   * Called by VolcanoPlanner when a subset's traits don't satisfy the required traits. If the
   * distribution doesn't match, insert a {@link PhysicalExchange} to enforce the required
   * distribution.
   */
  @Override
  public @Nullable RelNode enforce(RelNode input, RelTraitSet required) {
    RelDistribution requiredDist = required.getTrait(RelDistributionTraitDef.INSTANCE);
    RelDistribution inputDist = input.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE);

    if (requiredDist != null && inputDist != null && !inputDist.satisfies(requiredDist)) {
      return PhysicalExchange.create(input, requiredDist);
    }
    return input;
  }

  /**
   * Allow VolcanoPlanner to insert AbstractConverters for distribution trait enforcement. Without
   * this returning true, the planner never tries to enforce distribution and Exchange nodes are
   * never inserted.
   */
  @Override
  public boolean useAbstractConvertersForConversion(RelTraitSet fromTraits, RelTraitSet toTraits) {
    return true;
  }

  @Override
  public boolean canConvertConvention(Convention toConvention) {
    return false;
  }
}
