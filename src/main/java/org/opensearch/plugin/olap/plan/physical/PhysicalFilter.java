/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;

/** Physical filter. Inherits the child's distribution (filtering doesn't redistribute data). */
public class PhysicalFilter extends Filter implements PhysicalRel {

  protected PhysicalFilter(
      RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexNode condition) {
    super(cluster, traitSet, input, condition);
  }

  public static PhysicalFilter create(RelNode input, RexNode condition) {
    RelTraitSet traitSet = input.getTraitSet().replace(PhysicalConvention.INSTANCE);
    return new PhysicalFilter(input.getCluster(), traitSet, input, condition);
  }

  @Override
  public PhysicalFilter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    return new PhysicalFilter(getCluster(), traitSet, input, condition);
  }
}
