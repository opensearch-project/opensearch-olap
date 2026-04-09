/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical.rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.opensearch.plugin.olap.plan.physical.PhysicalConvention;
import org.opensearch.plugin.olap.plan.physical.PhysicalFilter;

/** Converts LogicalFilter -> PhysicalFilter. Filter inherits child's distribution. */
public class PhysicalFilterRule extends ConverterRule {

  public static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .withConversion(
              LogicalFilter.class,
              Convention.NONE,
              PhysicalConvention.INSTANCE,
              "PhysicalFilterRule")
          .withRuleFactory(PhysicalFilterRule::new);

  protected PhysicalFilterRule(Config config) {
    super(config);
  }

  @Override
  public RelNode convert(RelNode rel) {
    LogicalFilter filter = (LogicalFilter) rel;
    RelNode input =
        convert(
            filter.getInput(),
            filter.getInput().getTraitSet().replace(PhysicalConvention.INSTANCE));
    return PhysicalFilter.create(input, filter.getCondition());
  }
}
