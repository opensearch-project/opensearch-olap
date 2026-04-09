/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical;

import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

/**
 * Physical project. Inherits the child's distribution, transformed through the column mapping (if a
 * hash key column is dropped, distribution degrades to ANY).
 */
public class PhysicalProject extends Project implements PhysicalRel {

  protected PhysicalProject(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      List<? extends RexNode> projects,
      RelDataType rowType) {
    super(cluster, traitSet, List.of(), input, projects, rowType, Set.of());
  }

  public static PhysicalProject create(
      RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
    RelTraitSet traitSet = input.getTraitSet().replace(PhysicalConvention.INSTANCE);
    return new PhysicalProject(input.getCluster(), traitSet, input, projects, rowType);
  }

  @Override
  public PhysicalProject copy(
      RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
    return new PhysicalProject(getCluster(), traitSet, input, projects, rowType);
  }
}
