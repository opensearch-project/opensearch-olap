/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical;

import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

/**
 * Physical hash join. The distribution of the output depends on the join strategy:
 *
 * <ul>
 *   <li>Coordinator-centric: output is SINGLETON (both inputs gathered)
 *   <li>Hash shuffle: output is HASH_DISTRIBUTED by the left join keys
 * </ul>
 */
public class PhysicalJoin extends Join implements PhysicalRel {

  public PhysicalJoin(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode left,
      RelNode right,
      RexNode condition,
      Set<CorrelationId> variablesSet,
      JoinRelType joinType) {
    super(cluster, traitSet, List.of(), left, right, condition, variablesSet, joinType);
  }

  @Override
  public PhysicalJoin copy(
      RelTraitSet traitSet,
      RexNode conditionExpr,
      RelNode left,
      RelNode right,
      JoinRelType joinType,
      boolean semiJoinDone) {
    return new PhysicalJoin(
        getCluster(), traitSet, left, right, conditionExpr, getVariablesSet(), joinType);
  }
}
