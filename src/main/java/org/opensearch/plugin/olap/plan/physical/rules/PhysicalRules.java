/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical.rules;

import java.util.List;
import org.apache.calcite.plan.RelOptRule;

/** Convenience class holding all physical planning rules. */
public final class PhysicalRules {

  private PhysicalRules() {}

  /** Base converter rules (Convention.NONE -> PhysicalConvention). Always registered. */
  public static final List<RelOptRule> BASE_RULES =
      List.of(
          PhysicalTableScanRule.DEFAULT_CONFIG.toRule(),
          PhysicalFilterRule.DEFAULT_CONFIG.toRule(),
          PhysicalProjectRule.DEFAULT_CONFIG.toRule(),
          PhysicalSortRule.DEFAULT_CONFIG.toRule(),
          PhysicalAggregateRule.DEFAULT_CONFIG.toRule(),
          PhysicalJoinRule.DEFAULT_CONFIG.toRule(),
          PhysicalWindowRule.DEFAULT_CONFIG.toRule());

  /** MPP rules: HASH distribution alternatives. Registered when mpp_enabled=true. */
  public static final List<RelOptRule> MPP_RULES =
      List.of(MppAggregateRule.DEFAULT_CONFIG.toRule(), MppJoinRule.DEFAULT_CONFIG.toRule());

  /**
   * Optimization rules. The two-stage aggregation split (SINGLE -> PARTIAL + FINAL) is handled by
   * VeloxPlanGenerator during Velox PlanNode generation, not as a Calcite rule, because the
   * PARTIAL/FINAL output types require Velox-specific intermediate accumulator types that don't map
   * to Calcite's type system.
   */
  public static final List<RelOptRule> OPTIMIZATION_RULES = List.of();
}
