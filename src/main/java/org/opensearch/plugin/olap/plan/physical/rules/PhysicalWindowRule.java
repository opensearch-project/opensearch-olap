/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical.rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.opensearch.plugin.olap.plan.physical.PhysicalConvention;
import org.opensearch.plugin.olap.plan.physical.PhysicalWindow;

/**
 * Converts LogicalWindow -> PhysicalWindow. Window functions inherit the input's distribution
 * (window operates on all data at a single node after gathering).
 */
public class PhysicalWindowRule extends ConverterRule {

  public static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .withConversion(
              LogicalWindow.class,
              Convention.NONE,
              PhysicalConvention.INSTANCE,
              "PhysicalWindowRule")
          .withRuleFactory(PhysicalWindowRule::new);

  protected PhysicalWindowRule(Config config) {
    super(config);
  }

  @Override
  public RelNode convert(RelNode rel) {
    Window window = (Window) rel;
    RelNode input =
        convert(
            window.getInput(),
            window.getInput().getTraitSet().replace(PhysicalConvention.INSTANCE));
    return new PhysicalWindow(
        rel.getCluster(),
        rel.getTraitSet().replace(PhysicalConvention.INSTANCE),
        input,
        window.getConstants(),
        window.getRowType(),
        window.groups);
  }
}
