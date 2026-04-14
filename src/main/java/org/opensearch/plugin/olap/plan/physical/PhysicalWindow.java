/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical;

import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;

/** Physical window node for window function execution. */
public class PhysicalWindow extends Window implements PhysicalRel {

  public PhysicalWindow(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      List<RexLiteral> constants,
      RelDataType rowType,
      List<Group> groups) {
    super(cluster, traitSet, Collections.emptyList(), input, constants, rowType, groups);
  }

  @Override
  public Window copy(List<RexLiteral> constants) {
    return new PhysicalWindow(
        getCluster(), getTraitSet(), getInput(), constants, getRowType(), groups);
  }

  @Override
  public PhysicalWindow copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new PhysicalWindow(
        getCluster(), traitSet, sole(inputs), constants, getRowType(), groups);
  }
}
