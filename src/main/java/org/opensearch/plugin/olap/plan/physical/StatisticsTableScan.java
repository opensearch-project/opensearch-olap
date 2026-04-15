/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

/**
 * A TableScan that overrides row count estimation with CBO statistics. Used by {@link
 * PhysicalOptimizer} to inject actual table row counts into Calcite's cost model, enabling accurate
 * join reorder decisions in {@code MultiJoinOptimizeBushyRule}.
 *
 * <p>Extends {@code TableScan} (not {@code LogicalTableScan} which is final). The existing {@code
 * PhysicalTableScanRule} matches on {@code TableScan.class}, so this subclass is handled correctly.
 *
 * <p>When CBO statistics are unavailable, falls back to Calcite's default estimate.
 */
public class StatisticsTableScan extends TableScan {

  private final double rowCount;

  public StatisticsTableScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table,
      double rowCount) {
    super(cluster, traitSet, hints, table);
    this.rowCount = rowCount;
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return rowCount > 0 ? rowCount : super.estimateRowCount(mq);
  }
}
