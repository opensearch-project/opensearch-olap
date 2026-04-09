/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;

/**
 * Physical sort/limit. Requests SINGLETON distribution from its input because global sorting
 * requires all data at one node.
 */
public class PhysicalSort extends Sort implements PhysicalRel {

  public PhysicalSort(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelCollation collation,
      RexNode offset,
      RexNode fetch) {
    super(cluster, traitSet, input, collation, offset, fetch);
  }

  @Override
  public PhysicalSort copy(
      RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
    return new PhysicalSort(getCluster(), traitSet, input, collation, offset, fetch);
  }
}
