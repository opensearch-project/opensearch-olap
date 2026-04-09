/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical.rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.opensearch.plugin.olap.plan.physical.PhysicalConvention;
import org.opensearch.plugin.olap.plan.physical.PhysicalProject;

/** Converts LogicalProject -> PhysicalProject. Distribution is inherited/transformed. */
public class PhysicalProjectRule extends ConverterRule {

  public static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .withConversion(
              LogicalProject.class,
              Convention.NONE,
              PhysicalConvention.INSTANCE,
              "PhysicalProjectRule")
          .withRuleFactory(PhysicalProjectRule::new);

  protected PhysicalProjectRule(Config config) {
    super(config);
  }

  @Override
  public RelNode convert(RelNode rel) {
    LogicalProject project = (LogicalProject) rel;
    RelNode input =
        convert(
            project.getInput(),
            project.getInput().getTraitSet().replace(PhysicalConvention.INSTANCE));
    return PhysicalProject.create(input, project.getProjects(), project.getRowType());
  }
}
