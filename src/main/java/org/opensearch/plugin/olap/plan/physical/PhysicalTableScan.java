/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;

/**
 * Physical table scan. Distribution is RANDOM_DISTRIBUTED. Stores the original scan's derived row
 * type (which may differ from the table's row type due to SQL plugin pushdown).
 */
public class PhysicalTableScan extends TableScan implements PhysicalRel {

  private final RelDataType derivedRowType;

  protected PhysicalTableScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table,
      RelDataType derivedRowType) {
    super(cluster, traitSet, hints, table);
    this.derivedRowType = derivedRowType;
  }

  /** Create from an existing TableScan, preserving its derived row type. */
  public static PhysicalTableScan create(TableScan original) {
    RelTraitSet traitSet =
        original
            .getCluster()
            .traitSet()
            .replace(PhysicalConvention.INSTANCE)
            .replace(RelDistributions.ANY);
    return new PhysicalTableScan(
        original.getCluster(),
        traitSet,
        original.getHints(),
        original.getTable(),
        original.getRowType());
  }

  @Override
  public RelDataType deriveRowType() {
    return derivedRowType;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return new PhysicalTableScan(getCluster(), traitSet, getHints(), getTable(), derivedRowType);
  }
}
