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
import org.opensearch.plugin.olap.plan.physical.PhysicalSort;

/**
 * Converts Sort -> PhysicalSort. When the sort has a LIMIT (fetch), declares SINGLETON distribution
 * so Convention.enforce() auto-inserts PhysicalExchange between scan (RANDOM) and sort (SINGLETON).
 * This enables VeloxPlanGenerator to split into two-stage TopN (partial sort+limit on data nodes,
 * final sort+limit on coordinator).
 *
 * <p>When the sort sits above an aggregate or join that already produced SINGLETON, enforce() is a
 * no-op (SINGLETON satisfies SINGLETON).
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
    // Convert child to PhysicalConvention only (no explicit distribution on input — the planner
    // can't decompose NONE→PHYSICAL + ANY→SINGLETON in one step). The sort declares SINGLETON
    // on its own traitSet; Convention.enforce() inserts PhysicalExchange between the now-PHYSICAL
    // child and this sort when the child's distribution (e.g. RANDOM from scan) doesn't satisfy
    // SINGLETON. When the child already produces SINGLETON (aggregate/join output), no exchange
    // is added.
    RelNode input =
        convert(
            sort.getInput(), sort.getInput().getTraitSet().replace(PhysicalConvention.INSTANCE));
    return new PhysicalSort(
        rel.getCluster(),
        rel.getTraitSet().replace(PhysicalConvention.INSTANCE).replace(RelDistributions.SINGLETON),
        input,
        sort.getCollation(),
        sort.offset,
        sort.fetch);
  }
}
