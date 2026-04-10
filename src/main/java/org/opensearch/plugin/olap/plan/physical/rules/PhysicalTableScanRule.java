/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical.rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableScan;
import org.opensearch.plugin.olap.plan.physical.PhysicalConvention;
import org.opensearch.plugin.olap.plan.physical.PhysicalTableScan;

/**
 * Converts a logical TableScan (Convention.NONE) to a PhysicalTableScan. Matches {@code
 * TableScan.class} (not LogicalTableScan) to cover both standard Calcite scans and the SQL plugin's
 * CalciteLogicalIndexScan.
 */
public class PhysicalTableScanRule extends ConverterRule {

  public static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .withConversion(
              TableScan.class,
              Convention.NONE,
              PhysicalConvention.INSTANCE,
              "PhysicalTableScanRule")
          .withRuleFactory(PhysicalTableScanRule::new);

  protected PhysicalTableScanRule(Config config) {
    super(config);
  }

  @Override
  public RelNode convert(RelNode rel) {
    return PhysicalTableScan.create((TableScan) rel);
  }
}
