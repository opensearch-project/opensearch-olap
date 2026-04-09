/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical.rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Sort;
import org.opensearch.plugin.olap.plan.physical.PhysicalConvention;
import org.opensearch.plugin.olap.plan.physical.PhysicalExchange;
import org.opensearch.plugin.olap.plan.physical.PhysicalSort;

/**
 * Converts Sort -> PhysicalSort. Requests SINGLETON from input because global sorting requires all
 * data at one node.
 */
public class PhysicalSortRule extends ConverterRule {

  public static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .withConversion(
              Sort.class, Convention.NONE, PhysicalConvention.INSTANCE, "PhysicalSortRule")
          .withRuleFactory(PhysicalSortRule::new);

  protected PhysicalSortRule(Config config) {
    super(config);
  }

  @Override
  public RelNode convert(RelNode rel) {
    Sort sort = (Sort) rel;
    RelNode input =
        convert(
            sort.getInput(), sort.getInput().getTraitSet().replace(PhysicalConvention.INSTANCE));
    input = PhysicalExchange.create(input, RelDistributions.SINGLETON);
    return new PhysicalSort(
        rel.getCluster(),
        rel.getTraitSet().replace(PhysicalConvention.INSTANCE),
        input,
        sort.getCollation(),
        sort.offset,
        sort.fetch);
  }
}
