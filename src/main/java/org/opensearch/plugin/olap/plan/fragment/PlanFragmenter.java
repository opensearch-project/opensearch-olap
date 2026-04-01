/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.fragment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.boostscale.velox4j.aggregate.Aggregate;
import org.boostscale.velox4j.aggregate.AggregateStep;
import org.boostscale.velox4j.expression.CallTypedExpr;
import org.boostscale.velox4j.expression.FieldAccessTypedExpr;
import org.boostscale.velox4j.expression.TypedExpr;
import org.boostscale.velox4j.plan.AggregationNode;
import org.boostscale.velox4j.plan.FilterNode;
import org.boostscale.velox4j.plan.LimitNode;
import org.boostscale.velox4j.plan.PlanNode;
import org.boostscale.velox4j.plan.ProjectNode;
import org.boostscale.velox4j.type.BigIntType;
import org.boostscale.velox4j.type.DoubleType;
import org.boostscale.velox4j.type.RowType;
import org.boostscale.velox4j.type.Type;

/**
 * Splits a Velox plan tree into distributable fragments.
 *
 * <p>The fragmenter identifies exchange boundaries where data must flow between nodes. Currently it
 * uses a simple two-stage strategy:
 *
 * <ul>
 *   <li>Leaf fragment: Everything from TableScan up to (and including) partial aggregation
 *   <li>Root fragment: Final aggregation and any remaining operators on coordinator
 * </ul>
 *
 * <p>For plans without aggregation, a single fragment is created that runs on data nodes, with
 * results streamed back to the coordinator.
 */
public class PlanFragmenter {

  private final AtomicInteger fragmentIdCounter = new AtomicInteger(0);

  /**
   * Fragment a Velox plan into distributable pieces. Returns a list of fragments ordered bottom-up
   * (leaf fragments first, root last).
   */
  public List<PlanFragment> fragment(PlanNode root, String sourceIndex) {
    fragmentIdCounter.set(0);
    List<PlanFragment> fragments = new ArrayList<>();

    // Check if plan contains an aggregation that should be split into partial + final
    AggregationSplit split = findAndSplitAggregation(root);

    if (split != null) {
      // Leaf fragment: scan + filter + project + partial aggregation
      int leafId = fragmentIdCounter.getAndIncrement();
      PlanFragment leafFragment =
          new PlanFragment(
              leafId,
              split.partialPlan,
              FragmentProperties.source(sourceIndex),
              Collections.emptyList());
      fragments.add(leafFragment);

      // Root fragment: final aggregation + remaining operators
      int rootId = fragmentIdCounter.getAndIncrement();
      PlanFragment rootFragment =
          new PlanFragment(
              rootId, split.finalPlan, FragmentProperties.coordinator(), List.of(leafId));
      fragments.add(rootFragment);
    } else {
      // No aggregation split needed. Single fragment on data nodes,
      // results collected on coordinator.
      int leafId = fragmentIdCounter.getAndIncrement();
      PlanFragment fragment =
          new PlanFragment(
              leafId, root, FragmentProperties.source(sourceIndex), Collections.emptyList());
      fragments.add(fragment);
    }

    return fragments;
  }

  /**
   * Looks for a SINGLE-step AggregationNode in the plan and splits it into PARTIAL (for data nodes)
   * and FINAL (for coordinator).
   */
  private AggregationSplit findAndSplitAggregation(PlanNode node) {
    if (node instanceof AggregationNode) {
      AggregationNode aggNode = (AggregationNode) node;
      if (aggNode.getStep() == AggregateStep.SINGLE) {
        return splitAggregation(aggNode);
      }
    }

    // Walk the plan tree looking for an aggregation node.
    // In a typical plan: Project -> Aggregation -> Filter -> TableScan
    // We need to split at the aggregation boundary.
    List<PlanNode> sources = getNodeSources(node);
    if (sources != null && !sources.isEmpty()) {
      for (PlanNode source : sources) {
        AggregationSplit split = findAndSplitAggregation(source);
        if (split != null) {
          // Replace the source in the parent with the final plan
          PlanNode newRoot = replaceSource(node, source, split.finalPlan);
          return new AggregationSplit(split.partialPlan, newRoot);
        }
      }
    }

    return null;
  }

  private AggregationSplit splitAggregation(AggregationNode aggNode) {
    // Create PARTIAL aggregation (runs on each data node).
    // The PARTIAL aggregate call's return type must be the intermediate accumulator type,
    // not the final result type. Velox uses call->type() to determine the output column
    // type for PARTIAL aggregation. If the type is wrong (e.g., DOUBLE instead of
    // ROW(DOUBLE, BIGINT) for avg), Velox creates the wrong output vector type and
    // crashes in extractAccumulators when casting to RowVector.
    List<Aggregate> partialAggregates = rewriteAggregatesForPartial(aggNode.getAggregates());

    AggregationNode partialAgg =
        new AggregationNode(
            aggNode.getId() + "_partial",
            AggregateStep.PARTIAL,
            aggNode.getGroupingKeys(),
            aggNode.getPreGroupedKeys(),
            aggNode.getAggregateNames(),
            partialAggregates,
            aggNode.isIgnoreNullKeys(),
            aggNode.isNoGroupsSpanBatches(),
            aggNode.getSources(),
            null,
            Collections.emptyList());

    // Create FINAL aggregation (runs on coordinator, reads from ExternalStream)
    // The FINAL aggregate calls must reference the intermediate accumulator columns
    // from the PARTIAL output by name (via FieldAccessTypedExpr). Without this,
    // Velox C++ cannot locate the intermediate data and crashes in
    // addIntermediateResults with a null pointer dereference.
    List<Aggregate> finalAggregates =
        rewriteAggregatesForFinal(aggNode.getAggregates(), aggNode.getAggregateNames());

    AggregationNode finalAgg =
        new AggregationNode(
            aggNode.getId() + "_final",
            AggregateStep.FINAL,
            aggNode.getGroupingKeys(),
            aggNode.getPreGroupedKeys(),
            aggNode.getAggregateNames(),
            finalAggregates,
            aggNode.isIgnoreNullKeys(),
            aggNode.isNoGroupsSpanBatches(),
            Collections.emptyList(), // source will be wired during execution
            null,
            Collections.emptyList());

    return new AggregationSplit(partialAgg, finalAgg);
  }

  /**
   * Rewrite aggregate calls for a PARTIAL aggregation step. The call's return type must be set to
   * the intermediate accumulator type, because Velox uses call->type() to determine the output
   * column type for PARTIAL aggregation (via AggregationNode::outputType()).
   */
  private List<Aggregate> rewriteAggregatesForPartial(List<Aggregate> originalAggregates) {
    List<Aggregate> partialAggregates = new ArrayList<>(originalAggregates.size());
    for (Aggregate orig : originalAggregates) {
      Type intermediateType =
          resolveIntermediateType(orig.getCall().getFunctionName(), orig.getCall().getReturnType());
      CallTypedExpr partialCall =
          new CallTypedExpr(
              intermediateType, orig.getCall().getInputs(), orig.getCall().getFunctionName());

      partialAggregates.add(
          new Aggregate(
              partialCall,
              orig.getRawInputTypes(),
              orig.getMask(),
              orig.getSortingKeys(),
              orig.getSortingOrders(),
              orig.isDistinct()));
    }
    return partialAggregates;
  }

  /**
   * Resolve the intermediate accumulator type for a given aggregate function. Mirrors the
   * intermediate types registered in Velox's aggregate function signatures.
   *
   * @see velox/functions/prestosql/aggregates/AverageAggregate.cpp —
   *     intermediateType("row(double,bigint)")
   * @see velox/functions/prestosql/aggregates/CountAggregate.cpp — intermediateType("bigint")
   */
  private Type resolveIntermediateType(String functionName, Type finalType) {
    switch (functionName) {
      case "avg":
        // avg intermediate is always ROW(DOUBLE, BIGINT) for non-decimal types
        return new RowType(List.of("sum", "count"), List.of(new DoubleType(), new BigIntType()));
      case "count":
        return new BigIntType();
      case "sum":
      case "min":
      case "max":
        // sum/min/max intermediate type is the same as the final result type
        return finalType;
      default:
        // For unknown functions, use the final type as a fallback
        return finalType;
    }
  }

  /**
   * Rewrite aggregate calls for a FINAL aggregation step. Each aggregate's call must have an input
   * FieldAccessTypedExpr that references the corresponding intermediate accumulator column from the
   * PARTIAL output. The intermediate column names match the aggregate output names.
   */
  private List<Aggregate> rewriteAggregatesForFinal(
      List<Aggregate> originalAggregates, List<String> aggregateNames) {
    List<Aggregate> finalAggregates = new ArrayList<>(originalAggregates.size());
    for (int i = 0; i < originalAggregates.size(); i++) {
      Aggregate orig = originalAggregates.get(i);
      String intermediateName = aggregateNames.get(i);

      // The FINAL call references the intermediate column by name.
      // Use the original call's return type — Velox resolves the actual intermediate
      // accumulator type from the function signature and rawInputTypes.
      TypedExpr intermediateRef =
          FieldAccessTypedExpr.create(orig.getCall().getReturnType(), intermediateName);
      CallTypedExpr finalCall =
          new CallTypedExpr(
              orig.getCall().getReturnType(),
              List.of(intermediateRef),
              orig.getCall().getFunctionName());

      finalAggregates.add(
          new Aggregate(
              finalCall,
              orig.getRawInputTypes(),
              orig.getMask(),
              orig.getSortingKeys(),
              orig.getSortingOrders(),
              orig.isDistinct()));
    }
    return finalAggregates;
  }

  /**
   * Creates a copy of the parent node with one source replaced by newSource. Reconstructs the
   * parent node to preserve operators above the aggregation split point.
   */
  private PlanNode replaceSource(PlanNode parent, PlanNode oldSource, PlanNode newSource) {
    if (parent instanceof ProjectNode) {
      ProjectNode proj = (ProjectNode) parent;
      return new ProjectNode(
          proj.getId(), List.of(newSource), proj.getNames(), proj.getProjections());
    } else if (parent instanceof LimitNode) {
      LimitNode limit = (LimitNode) parent;
      return new LimitNode(
          limit.getId(),
          List.of(newSource),
          limit.getOffset(),
          limit.getCount(),
          limit.isPartial());
    } else if (parent instanceof FilterNode) {
      FilterNode filter = (FilterNode) parent;
      return new FilterNode(filter.getId(), List.of(newSource), filter.getFilter());
    } else if (parent instanceof AggregationNode) {
      AggregationNode agg = (AggregationNode) parent;
      return new AggregationNode(
          agg.getId(),
          agg.getStep(),
          agg.getGroupingKeys(),
          agg.getPreGroupedKeys(),
          agg.getAggregateNames(),
          agg.getAggregates(),
          agg.isIgnoreNullKeys(),
          agg.isNoGroupsSpanBatches(),
          List.of(newSource),
          null,
          Collections.emptyList());
    }
    // Fallback: just return newSource (drops the parent — should not happen for known node types)
    return newSource;
  }

  /** Get sources from a PlanNode using reflection (getSources is protected in PlanNode). */
  @SuppressWarnings("unchecked")
  private List<PlanNode> getNodeSources(PlanNode node) {
    if (node instanceof AggregationNode) {
      return ((AggregationNode) node).getSources();
    }
    try {
      java.lang.reflect.Method m = PlanNode.class.getDeclaredMethod("getSources");
      m.setAccessible(true);
      return (List<PlanNode>) m.invoke(node);
    } catch (Exception e) {
      return Collections.emptyList();
    }
  }

  private static class AggregationSplit {
    final PlanNode partialPlan;
    final PlanNode finalPlan;

    AggregationSplit(PlanNode partialPlan, PlanNode finalPlan) {
      this.partialPlan = partialPlan;
      this.finalPlan = finalPlan;
    }
  }
}
