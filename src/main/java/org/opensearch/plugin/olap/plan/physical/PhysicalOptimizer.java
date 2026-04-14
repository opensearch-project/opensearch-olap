/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.plugin.olap.plan.physical.rules.PhysicalRules;

/**
 * Runs Calcite VolcanoPlanner to convert a logical plan (Convention.NONE) into a physical plan
 * (PhysicalConvention) with distribution traits and automatic Exchange insertion.
 *
 * <p>Creates a <b>new VolcanoPlanner + RelOptCluster</b> with {@code RelDistributionTraitDef}
 * registered from the start. The incoming plan is deep-copied into the new cluster via {@link
 * ClusterCopyShuttle}. This approach:
 *
 * <ul>
 *   <li>Avoids "belongs to a different planner" errors (plan is re-rooted in our cluster)
 *   <li>Prevents SQL plugin's pushdown rules from leaking into our planner (the scan's {@code
 *       register()} adds rules to our fresh planner, not the SQL plugin's)
 *   <li>Enables {@code Convention.enforce()} with distribution traits (new cluster has all 3 trait
 *       defs from construction)
 *   <li>Zero SQL plugin changes required
 * </ul>
 */
public class PhysicalOptimizer {

  private static final Logger logger = LogManager.getLogger(PhysicalOptimizer.class);

  private final boolean mppEnabled;

  public PhysicalOptimizer(boolean mppEnabled) {
    this.mppEnabled = mppEnabled;
  }

  public RelNode optimize(RelNode logicalPlan) {
    // Step 1: Create our own VolcanoPlanner with all 3 trait defs
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
    planner.addRelTraitDef(RelDistributionTraitDef.INSTANCE);

    // Step 2: Create a new cluster with our planner (reuse the existing RexBuilder)
    RelOptCluster newCluster =
        RelOptCluster.create(planner, logicalPlan.getCluster().getRexBuilder());

    // Step 3: Deep-copy the logical plan into the new cluster
    RelNode copiedPlan = logicalPlan.accept(new ClusterCopyShuttle(newCluster));

    // Step 3.5: Run HepPlanner for lightweight logical optimization (same as
    // CalciteToolsHelper.optimize() in the SQL plugin). FilterMergeRule merges
    // adjacent filters; this runs before the VolcanoPlanner so the cost-based
    // optimizer sees a cleaner plan.
    HepProgram hepProgram =
        HepProgram.builder()
            .addRuleInstance(FilterMergeRule.Config.DEFAULT.toRule())
            // Decompose LogicalProject(RexOver) → LogicalWindow + LogicalProject
            .addRuleInstance(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW)
            .build();
    HepPlanner hepPlanner = new HepPlanner(hepProgram);
    hepPlanner.setRoot(copiedPlan);
    copiedPlan = hepPlanner.findBestExp();

    // Step 4: Collect rules
    List<RelOptRule> allRules = new ArrayList<>();
    allRules.addAll(PhysicalRules.BASE_RULES);
    if (mppEnabled) {
      allRules.addAll(PhysicalRules.MPP_RULES);
    }
    allRules.addAll(PhysicalRules.OPTIMIZATION_RULES);
    // AbstractConverter.ExpandConversionRule enables Convention.enforce() to fire
    // for distribution trait mismatches (e.g., RANDOM → SINGLETON via PhysicalExchange).
    allRules.add(AbstractConverter.ExpandConversionRule.INSTANCE);

    // Step 5: Register rules and run optimization directly on our planner
    for (RelOptRule rule : allRules) {
      planner.addRule(rule);
    }

    // Only require PhysicalConvention. Distribution (SINGLETON) is enforced explicitly
    // by the ConverterRules (PhysicalAggregateRule, PhysicalJoinRule, PhysicalSortRule) which
    // insert PhysicalExchange nodes. Convention.enforce() doesn't fire for distribution
    // in Calcite's default VolcanoPlanner flow.
    RelTraitSet requiredTraits = newCluster.traitSet().replace(PhysicalConvention.INSTANCE);

    RelNode root = planner.changeTraits(copiedPlan, requiredTraits);
    planner.setRoot(root);
    RelNode optimized = planner.findBestExp();

    logger.info("Physical optimization complete: mpp_enabled={}", mppEnabled);

    return optimized;
  }

  /**
   * Deep-copies a RelNode tree into a new RelOptCluster. Each node is recreated with the target
   * cluster's trait set (which includes RelDistributionTraitDef). TableScan nodes are converted to
   * plain LogicalTableScan to strip SQL plugin's PushDownContext and prevent pushdown rule
   * registration.
   */
  private static class ClusterCopyShuttle extends RelShuttleImpl {
    private final RelOptCluster targetCluster;

    ClusterCopyShuttle(RelOptCluster targetCluster) {
      this.targetCluster = targetCluster;
    }

    private RelTraitSet mapTraits(RelTraitSet original) {
      // Start with the target cluster's empty trait set (has 3 traits)
      // and replace the Convention from the original
      return targetCluster.traitSet().replace(original.getTrait(ConventionTraitDef.INSTANCE));
    }

    @Override
    public RelNode visit(TableScan scan) {
      // Create a plain LogicalTableScan in the new cluster.
      // This strips CalciteLogicalIndexScan's PushDownContext and prevents
      // its register() from adding pushdown rules to our planner.
      return new LogicalTableScan(
          targetCluster, mapTraits(scan.getTraitSet()), scan.getHints(), scan.getTable());
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
      RelNode newInput = filter.getInput().accept(this);
      return LogicalFilter.create(newInput, filter.getCondition());
    }

    @Override
    public RelNode visit(LogicalProject project) {
      RelNode newInput = project.getInput().accept(this);
      return LogicalProject.create(
          newInput, project.getHints(), project.getProjects(), project.getRowType());
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
      RelNode newInput = aggregate.getInput().accept(this);
      return LogicalAggregate.create(
          newInput,
          aggregate.getHints(),
          aggregate.getGroupSet(),
          aggregate.getGroupSets(),
          aggregate.getAggCallList());
    }

    @Override
    public RelNode visit(LogicalJoin join) {
      RelNode newLeft = join.getLeft().accept(this);
      RelNode newRight = join.getRight().accept(this);
      return LogicalJoin.create(
          newLeft,
          newRight,
          join.getHints(),
          join.getCondition(),
          join.getVariablesSet(),
          join.getJoinType());
    }

    @Override
    public RelNode visit(LogicalSort sort) {
      RelNode newInput = sort.getInput().accept(this);
      return LogicalSort.create(newInput, sort.getCollation(), sort.offset, sort.fetch);
    }

    @Override
    public RelNode visit(RelNode other) {
      // Handle Sort subclasses (LogicalSystemLimit extends Sort but doesn't
      // dispatch to visit(LogicalSort)). LogicalSort.create() uses the input's
      // cluster, ensuring the new node belongs to our cluster.
      if (other instanceof Sort) {
        Sort sort = (Sort) other;
        RelNode newInput = sort.getInput().accept(this);
        return LogicalSort.create(newInput, sort.getCollation(), sort.offset, sort.fetch);
      }
      // Generic fallback: visit children, then create new node using targetCluster
      List<RelNode> newInputs =
          other.getInputs().stream().map(input -> input.accept(this)).collect(Collectors.toList());
      if (!newInputs.isEmpty()) {
        return other.copy(mapTraits(other.getTraitSet()), newInputs);
      }
      return other;
    }
  }
}
