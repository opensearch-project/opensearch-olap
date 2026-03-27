/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.fragment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.boostscale.velox4j.aggregate.AggregateStep;
import org.boostscale.velox4j.plan.AggregationNode;
import org.boostscale.velox4j.plan.FilterNode;
import org.boostscale.velox4j.plan.LimitNode;
import org.boostscale.velox4j.plan.PlanNode;
import org.boostscale.velox4j.plan.ProjectNode;

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
    // Create PARTIAL aggregation (runs on each data node)
    AggregationNode partialAgg =
        new AggregationNode(
            aggNode.getId() + "_partial",
            AggregateStep.PARTIAL,
            aggNode.getGroupingKeys(),
            aggNode.getPreGroupedKeys(),
            aggNode.getAggregateNames(),
            aggNode.getAggregates(),
            aggNode.isIgnoreNullKeys(),
            aggNode.isNoGroupsSpanBatches(),
            aggNode.getSources(),
            null,
            Collections.emptyList());

    // Create FINAL aggregation (runs on coordinator, reads from ExternalStream)
    // The source of the final agg is a TableScanNode that reads from ExternalStream
    // carrying the partial aggregation results. This will be wired during execution.
    AggregationNode finalAgg =
        new AggregationNode(
            aggNode.getId() + "_final",
            AggregateStep.FINAL,
            aggNode.getGroupingKeys(),
            aggNode.getPreGroupedKeys(),
            aggNode.getAggregateNames(),
            aggNode.getAggregates(),
            aggNode.isIgnoreNullKeys(),
            aggNode.isNoGroupsSpanBatches(),
            Collections.emptyList(), // source will be wired during execution
            null,
            Collections.emptyList());

    return new AggregationSplit(partialAgg, finalAgg);
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
