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
import org.boostscale.velox4j.plan.HashJoinNode;
import org.boostscale.velox4j.plan.LimitNode;
import org.boostscale.velox4j.plan.PlanNode;
import org.boostscale.velox4j.plan.ProjectNode;
import org.boostscale.velox4j.plan.TableScanNode;
import org.boostscale.velox4j.type.BigIntType;
import org.boostscale.velox4j.type.DoubleType;
import org.boostscale.velox4j.type.RowType;
import org.boostscale.velox4j.type.Type;
import org.opensearch.plugin.olap.scheduler.JoinStrategy;

/**
 * Splits a Velox plan tree into distributable fragments.
 *
 * <p>The fragmenter identifies exchange boundaries where data must flow between nodes. It supports:
 *
 * <ul>
 *   <li>Aggregation splits: PARTIAL on data nodes, FINAL on coordinator
 *   <li>Join splits: Three strategies — coordinator-centric, broadcast, hash shuffle
 * </ul>
 *
 * <p>For join + aggregation combinations, the aggregation is split into PARTIAL/FINAL with the
 * PARTIAL running alongside the join on data nodes/workers, and FINAL on the coordinator.
 */
public class PlanFragmenter {

  private final AtomicInteger fragmentIdCounter = new AtomicInteger(0);

  /**
   * Fragment a Velox plan for single-table queries. Returns fragments ordered bottom-up (leaf
   * fragments first, root last).
   */
  public List<PlanFragment> fragment(PlanNode root, String sourceIndex) {
    fragmentIdCounter.set(0);
    List<PlanFragment> fragments = new ArrayList<>();

    AggregationSplit split = findAndSplitAggregation(root);

    if (split != null) {
      int leafId = fragmentIdCounter.getAndIncrement();
      PlanFragment leafFragment =
          new PlanFragment(
              leafId,
              split.partialPlan,
              FragmentProperties.source(sourceIndex),
              Collections.emptyList());
      fragments.add(leafFragment);

      int rootId = fragmentIdCounter.getAndIncrement();
      PlanFragment rootFragment =
          new PlanFragment(
              rootId, split.finalPlan, FragmentProperties.coordinator(), List.of(leafId));
      fragments.add(rootFragment);
    } else {
      int leafId = fragmentIdCounter.getAndIncrement();
      PlanFragment fragment =
          new PlanFragment(
              leafId, root, FragmentProperties.source(sourceIndex), Collections.emptyList());
      fragments.add(fragment);
    }

    return fragments;
  }

  /**
   * Fragment a Velox plan containing a join. Returns fragments ordered bottom-up.
   *
   * @param root the full plan tree containing a HashJoinNode
   * @param leftIndex index name for the left (probe) side
   * @param rightIndex index name for the right (build) side
   * @param strategy the join distribution strategy
   * @param partitionCount number of shuffle partitions (for HASH_SHUFFLE only)
   * @return ordered list of plan fragments
   */
  public List<PlanFragment> fragmentJoin(
      PlanNode root,
      String leftIndex,
      String rightIndex,
      JoinStrategy strategy,
      int partitionCount) {
    fragmentIdCounter.set(0);

    switch (strategy) {
      case COORDINATOR_CENTRIC:
        return fragmentJoinCoordinatorCentric(root, leftIndex, rightIndex);
      case BROADCAST:
        return fragmentJoinBroadcast(root, leftIndex, rightIndex);
      case HASH_SHUFFLE:
        return fragmentJoinHashShuffle(root, leftIndex, rightIndex, partitionCount);
      default:
        throw new UnsupportedOperationException("Unsupported join strategy: " + strategy);
    }
  }

  // ---- Coordinator-Centric Join ----
  // Fragment 0 (SOURCE, left): left subtree
  // Fragment 1 (SOURCE, right): right subtree
  // Fragment 2 (COORDINATOR): join + everything above

  private List<PlanFragment> fragmentJoinCoordinatorCentric(
      PlanNode root, String leftIndex, String rightIndex) {
    List<PlanFragment> fragments = new ArrayList<>();

    JoinSplit joinSplit = findAndSplitJoin(root);
    if (joinSplit == null) {
      throw new IllegalStateException("No HashJoinNode found in plan for join fragmentation");
    }

    int leftId = fragmentIdCounter.getAndIncrement();
    fragments.add(
        new PlanFragment(
            leftId,
            joinSplit.leftPlan,
            FragmentProperties.source(leftIndex, "left"),
            Collections.emptyList()));

    int rightId = fragmentIdCounter.getAndIncrement();
    fragments.add(
        new PlanFragment(
            rightId,
            joinSplit.rightPlan,
            FragmentProperties.source(rightIndex, "right"),
            Collections.emptyList()));

    int coordinatorId = fragmentIdCounter.getAndIncrement();
    fragments.add(
        new PlanFragment(
            coordinatorId,
            joinSplit.coordinatorPlan,
            FragmentProperties.coordinator(),
            List.of(leftId, rightId)));

    return fragments;
  }

  // ---- Broadcast Join ----
  // Fragment 0 (SOURCE, build): build-side scan (collected centrally first)
  // Fragment 1 (BROADCAST, probe): probe scan + join with broadcast data
  //   If aggregation exists above join:
  //     Fragment 1 includes partial agg
  //     Fragment 2 (COORDINATOR): final agg + project + limit
  //   If no aggregation:
  //     Fragment 1 is the complete join plan
  //     No coordinator fragment needed (results collected directly)

  private List<PlanFragment> fragmentJoinBroadcast(
      PlanNode root, String leftIndex, String rightIndex) {
    List<PlanFragment> fragments = new ArrayList<>();
    JoinSplit joinSplit = findAndSplitJoin(root);
    if (joinSplit == null) {
      throw new IllegalStateException("No HashJoinNode found in plan for join fragmentation");
    }

    // Determine which side is build (smaller) and which is probe (larger).
    // The caller sets this via the ordering of leftIndex/rightIndex, but
    // within the plan, left=probe and right=build by convention.
    // The build side is collected centrally and broadcast.
    String buildIndex = rightIndex;
    String probeIndex = leftIndex;
    PlanNode buildPlan = joinSplit.rightPlan;

    int buildId = fragmentIdCounter.getAndIncrement();
    fragments.add(
        new PlanFragment(
            buildId,
            buildPlan,
            FragmentProperties.source(buildIndex, "right"),
            Collections.emptyList()));

    // Check if there's an aggregation above the join in the coordinator plan
    AggregationSplit aggSplit = findAndSplitAggregation(joinSplit.coordinatorPlan);

    if (aggSplit != null) {
      // Broadcast fragment: probe scan + join + partial agg
      // Wire the partial agg on top of the join (with probe scan as left source)
      PlanNode probePlusJoinPlusPartialAgg = aggSplit.partialPlan;
      // The partial plan's source chain ends at the join node which has empty sources.
      // We need to wire the probe-side TableScan back into the join's left source.
      probePlusJoinPlusPartialAgg =
          wireLeftSourceIntoJoin(probePlusJoinPlusPartialAgg, joinSplit.leftPlan);

      int probeJoinId = fragmentIdCounter.getAndIncrement();
      fragments.add(
          new PlanFragment(
              probeJoinId,
              probePlusJoinPlusPartialAgg,
              FragmentProperties.broadcast(probeIndex),
              List.of(buildId)));

      // Coordinator fragment: final agg + project + limit
      int coordinatorId = fragmentIdCounter.getAndIncrement();
      fragments.add(
          new PlanFragment(
              coordinatorId,
              aggSplit.finalPlan,
              FragmentProperties.coordinator(),
              List.of(probeJoinId)));
    } else {
      // No aggregation: broadcast fragment has the complete join + above operators
      PlanNode fullPlan = wireLeftSourceIntoJoin(joinSplit.coordinatorPlan, joinSplit.leftPlan);

      int probeJoinId = fragmentIdCounter.getAndIncrement();
      fragments.add(
          new PlanFragment(
              probeJoinId, fullPlan, FragmentProperties.broadcast(probeIndex), List.of(buildId)));
    }

    return fragments;
  }

  // ---- Hash Shuffle Join ----
  // Fragment 0 (SOURCE+shuffle, left): left scan, partitioned by join key
  // Fragment 1 (SOURCE+shuffle, right): right scan, partitioned by join key
  // Fragment 2 (HASH_PARTITIONED): join on workers
  //   If aggregation exists above join:
  //     Fragment 2 includes partial agg
  //     Fragment 3 (COORDINATOR): final agg + project + limit
  //   If no aggregation:
  //     Fragment 2 is the complete join plan
  //     Fragment 3 (COORDINATOR): collect results

  private List<PlanFragment> fragmentJoinHashShuffle(
      PlanNode root, String leftIndex, String rightIndex, int partitionCount) {
    List<PlanFragment> fragments = new ArrayList<>();
    JoinSplit joinSplit = findAndSplitJoin(root);
    if (joinSplit == null) {
      throw new IllegalStateException("No HashJoinNode found in plan for join fragmentation");
    }

    // Compute shuffle key channels from the join keys and scan output types
    List<Integer> leftKeyChannels = computeKeyChannels(joinSplit.leftPlan, joinSplit.leftKeys);
    List<Integer> rightKeyChannels = computeKeyChannels(joinSplit.rightPlan, joinSplit.rightKeys);

    // Left scan fragment (with shuffle config)
    int leftId = fragmentIdCounter.getAndIncrement();
    fragments.add(
        new PlanFragment(
            leftId,
            joinSplit.leftPlan,
            FragmentProperties.shuffleScan(leftIndex, "left", leftKeyChannels, partitionCount),
            Collections.emptyList()));

    // Right scan fragment (with shuffle config)
    int rightId = fragmentIdCounter.getAndIncrement();
    fragments.add(
        new PlanFragment(
            rightId,
            joinSplit.rightPlan,
            FragmentProperties.shuffleScan(rightIndex, "right", rightKeyChannels, partitionCount),
            Collections.emptyList()));

    // Check if there's an aggregation above the join
    AggregationSplit aggSplit = findAndSplitAggregation(joinSplit.coordinatorPlan);

    if (aggSplit != null) {
      // Join workers: join + partial agg
      int joinId = fragmentIdCounter.getAndIncrement();
      fragments.add(
          new PlanFragment(
              joinId,
              aggSplit.partialPlan,
              FragmentProperties.hashPartitioned(partitionCount),
              List.of(leftId, rightId)));

      // Coordinator: final agg
      int coordinatorId = fragmentIdCounter.getAndIncrement();
      fragments.add(
          new PlanFragment(
              coordinatorId,
              aggSplit.finalPlan,
              FragmentProperties.coordinator(),
              List.of(joinId)));
    } else {
      // Join workers: complete join + above operators
      int joinId = fragmentIdCounter.getAndIncrement();
      fragments.add(
          new PlanFragment(
              joinId,
              joinSplit.coordinatorPlan,
              FragmentProperties.hashPartitioned(partitionCount),
              List.of(leftId, rightId)));

      // Coordinator: collect results
      int coordinatorId = fragmentIdCounter.getAndIncrement();
      fragments.add(
          new PlanFragment(
              coordinatorId,
              null, // no plan — just collects results
              FragmentProperties.coordinator(),
              List.of(joinId)));
    }

    return fragments;
  }

  // ---- Join Detection and Splitting ----

  /**
   * Find a HashJoinNode in the plan tree and split it into left subtree, right subtree, and
   * coordinator plan (join + everything above with empty join sources).
   */
  private JoinSplit findAndSplitJoin(PlanNode node) {
    if (node instanceof HashJoinNode) {
      HashJoinNode joinNode = (HashJoinNode) node;
      List<PlanNode> sources = getNodeSources(joinNode);
      PlanNode leftPlan = sources.get(0);
      PlanNode rightPlan = sources.get(1);

      // Create coordinator plan: join with placeholder sources.
      // HashJoinNode uses ImmutableList which rejects null, so we use placeholder
      // TableScanNodes that will be replaced during execution wiring.
      TableScanNode leftPlaceholder =
          new TableScanNode(
              "left_exchange",
              leftPlan instanceof TableScanNode
                  ? ((TableScanNode) leftPlan).getOutputType()
                  : new RowType(List.of("_placeholder"), List.of(new BigIntType())),
              new org.boostscale.velox4j.connector.ExternalStreamTableHandle(
                  "connector-external-stream"),
              Collections.emptyList());
      TableScanNode rightPlaceholder =
          new TableScanNode(
              "right_exchange",
              rightPlan instanceof TableScanNode
                  ? ((TableScanNode) rightPlan).getOutputType()
                  : new RowType(List.of("_placeholder"), List.of(new BigIntType())),
              new org.boostscale.velox4j.connector.ExternalStreamTableHandle(
                  "connector-external-stream"),
              Collections.emptyList());
      HashJoinNode emptyJoin =
          new HashJoinNode(
              joinNode.getId(),
              joinNode.getJoinType(),
              joinNode.getLeftKeys(),
              joinNode.getRightKeys(),
              joinNode.getFilter(),
              leftPlaceholder,
              rightPlaceholder,
              joinNode.getOutputType(),
              false,
              false);

      return new JoinSplit(
          leftPlan, rightPlan, emptyJoin, joinNode.getLeftKeys(), joinNode.getRightKeys());
    }

    // Walk the tree looking for a HashJoinNode in the sources
    List<PlanNode> sources = getNodeSources(node);
    if (sources != null && !sources.isEmpty()) {
      for (PlanNode source : sources) {
        JoinSplit split = findAndSplitJoin(source);
        if (split != null) {
          // Replace the source with the coordinator plan (join with empty sources)
          PlanNode newRoot = replaceSource(node, source, split.coordinatorPlan);
          return new JoinSplit(
              split.leftPlan, split.rightPlan, newRoot, split.leftKeys, split.rightKeys);
        }
      }
    }

    return null;
  }

  /** Wire a left source PlanNode back into the join's left input within a plan tree. */
  private PlanNode wireLeftSourceIntoJoin(PlanNode node, PlanNode leftSource) {
    if (node instanceof HashJoinNode) {
      HashJoinNode join = (HashJoinNode) node;
      return new HashJoinNode(
          join.getId(),
          join.getJoinType(),
          join.getLeftKeys(),
          join.getRightKeys(),
          join.getFilter(),
          leftSource,
          getNodeSources(join).size() > 1 ? getNodeSources(join).get(1) : leftSource,
          join.getOutputType(),
          false,
          false);
    }

    List<PlanNode> sources = getNodeSources(node);
    if (sources != null && !sources.isEmpty()) {
      List<PlanNode> newSources = new ArrayList<>(sources.size());
      boolean changed = false;
      for (PlanNode source : sources) {
        PlanNode wired = wireLeftSourceIntoJoin(source, leftSource);
        newSources.add(wired);
        if (wired != source) changed = true;
      }
      if (changed) {
        return reconstructNode(node, newSources);
      }
    }
    return node;
  }

  /**
   * Compute the column indices in the scan output that correspond to the join keys. Used for hash
   * partitioning during shuffle.
   */
  private List<Integer> computeKeyChannels(PlanNode scanPlan, List<FieldAccessTypedExpr> joinKeys) {
    // Find the output type of the scan plan (the root of the subtree)
    RowType outputType = getOutputType(scanPlan);
    if (outputType == null) {
      return Collections.emptyList();
    }

    List<Integer> channels = new ArrayList<>();
    for (FieldAccessTypedExpr key : joinKeys) {
      int idx = outputType.getNames().indexOf(key.getFieldName());
      if (idx >= 0) {
        channels.add(idx);
      }
    }
    return channels;
  }

  /** Get the output RowType from a plan node. */
  private RowType getOutputType(PlanNode node) {
    if (node instanceof TableScanNode) {
      Type t = ((TableScanNode) node).getOutputType();
      return t instanceof RowType ? (RowType) t : null;
    }
    if (node instanceof ProjectNode) {
      // Project output is defined by its projection names/types
      // For shuffle key resolution, we need the top-level output
      ProjectNode proj = (ProjectNode) node;
      List<String> names = proj.getNames();
      List<Type> types = new ArrayList<>();
      for (TypedExpr expr : proj.getProjections()) {
        types.add(expr.getReturnType());
      }
      return new RowType(names, types);
    }
    if (node instanceof FilterNode) {
      // Filter doesn't change the schema
      List<PlanNode> sources = getNodeSources(node);
      if (sources != null && !sources.isEmpty()) {
        return getOutputType(sources.get(0));
      }
    }
    if (node instanceof AggregationNode) {
      AggregationNode agg = (AggregationNode) node;
      List<String> names = new ArrayList<>();
      List<Type> types = new ArrayList<>();
      for (FieldAccessTypedExpr key : agg.getGroupingKeys()) {
        names.add(key.getFieldName());
        types.add(key.getReturnType());
      }
      for (int i = 0; i < agg.getAggregateNames().size(); i++) {
        names.add(agg.getAggregateNames().get(i));
        types.add(agg.getAggregates().get(i).getCall().getReturnType());
      }
      return new RowType(names, types);
    }
    // Fallback: try to get from the first source
    List<PlanNode> sources = getNodeSources(node);
    if (sources != null && !sources.isEmpty()) {
      return getOutputType(sources.get(0));
    }
    return null;
  }

  /** Check if a plan tree contains a HashJoinNode. */
  public boolean containsJoin(PlanNode node) {
    if (node instanceof HashJoinNode) {
      return true;
    }
    List<PlanNode> sources = getNodeSources(node);
    if (sources != null) {
      for (PlanNode source : sources) {
        if (containsJoin(source)) {
          return true;
        }
      }
    }
    return false;
  }

  // ---- Aggregation Splitting (unchanged logic) ----

  private AggregationSplit findAndSplitAggregation(PlanNode node) {
    if (node instanceof AggregationNode) {
      AggregationNode aggNode = (AggregationNode) node;
      if (aggNode.getStep() == AggregateStep.SINGLE) {
        return splitAggregation(aggNode);
      }
    }

    List<PlanNode> sources = getNodeSources(node);
    if (sources != null && !sources.isEmpty()) {
      for (PlanNode source : sources) {
        AggregationSplit split = findAndSplitAggregation(source);
        if (split != null) {
          PlanNode newRoot = replaceSource(node, source, split.finalPlan);
          return new AggregationSplit(split.partialPlan, newRoot);
        }
      }
    }

    return null;
  }

  private AggregationSplit splitAggregation(AggregationNode aggNode) {
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
            Collections.emptyList(),
            null,
            Collections.emptyList());

    return new AggregationSplit(partialAgg, finalAgg);
  }

  private List<Aggregate> rewriteAggregatesForPartial(List<Aggregate> originalAggregates) {
    List<Aggregate> partialAggregates = new ArrayList<>(originalAggregates.size());
    for (Aggregate orig : originalAggregates) {
      Type intermediateType =
          resolveIntermediateType(
              orig.getCall().getFunctionName(),
              orig.getCall().getReturnType(),
              orig.getRawInputTypes());
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

  private Type resolveIntermediateType(
      String functionName, Type finalType, List<Type> rawInputTypes) {
    switch (functionName) {
      case "avg":
        return new RowType(List.of("sum", "count"), List.of(new DoubleType(), new BigIntType()));
      case "count":
        return new BigIntType();
      case "sum":
        if (!rawInputTypes.isEmpty()) {
          Type inputType = rawInputTypes.get(0);
          if (inputType instanceof org.boostscale.velox4j.type.RealType
              || inputType instanceof DoubleType) {
            return new DoubleType();
          }
        }
        return new BigIntType();
      case "min":
      case "max":
        return finalType;
      default:
        return finalType;
    }
  }

  private List<Aggregate> rewriteAggregatesForFinal(
      List<Aggregate> originalAggregates, List<String> aggregateNames) {
    List<Aggregate> finalAggregates = new ArrayList<>(originalAggregates.size());
    for (int i = 0; i < originalAggregates.size(); i++) {
      Aggregate orig = originalAggregates.get(i);
      String intermediateName = aggregateNames.get(i);
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

  // ---- Node Manipulation Helpers ----

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
    return newSource;
  }

  private PlanNode reconstructNode(PlanNode node, List<PlanNode> newSources) {
    if (node instanceof ProjectNode) {
      ProjectNode p = (ProjectNode) node;
      return new ProjectNode(p.getId(), newSources, p.getNames(), p.getProjections());
    } else if (node instanceof LimitNode) {
      LimitNode l = (LimitNode) node;
      return new LimitNode(l.getId(), newSources, l.getOffset(), l.getCount(), l.isPartial());
    } else if (node instanceof FilterNode) {
      FilterNode f = (FilterNode) node;
      return new FilterNode(f.getId(), newSources, f.getFilter());
    } else if (node instanceof AggregationNode) {
      AggregationNode a = (AggregationNode) node;
      return new AggregationNode(
          a.getId(),
          a.getStep(),
          a.getGroupingKeys(),
          a.getPreGroupedKeys(),
          a.getAggregateNames(),
          a.getAggregates(),
          a.isIgnoreNullKeys(),
          a.isNoGroupsSpanBatches(),
          newSources,
          null,
          Collections.emptyList());
    } else if (node instanceof HashJoinNode) {
      HashJoinNode j = (HashJoinNode) node;
      List<PlanNode> origSources = getNodeSources(j);
      PlanNode left = newSources.size() > 0 ? newSources.get(0) : origSources.get(0);
      PlanNode right = newSources.size() > 1 ? newSources.get(1) : origSources.get(1);
      return new HashJoinNode(
          j.getId(),
          j.getJoinType(),
          j.getLeftKeys(),
          j.getRightKeys(),
          j.getFilter(),
          left,
          right,
          j.getOutputType(),
          false,
          false);
    }
    return node;
  }

  @SuppressWarnings("unchecked")
  private List<PlanNode> getNodeSources(PlanNode node) {
    if (node instanceof AggregationNode) {
      return ((AggregationNode) node).getSources();
    }
    if (node instanceof HashJoinNode) {
      // getSources() is protected in AbstractJoinNode, use reflection
      try {
        java.lang.reflect.Method m = PlanNode.class.getDeclaredMethod("getSources");
        m.setAccessible(true);
        return (List<PlanNode>) m.invoke(node);
      } catch (Exception e) {
        return Collections.emptyList();
      }
    }
    try {
      java.lang.reflect.Method m = PlanNode.class.getDeclaredMethod("getSources");
      m.setAccessible(true);
      return (List<PlanNode>) m.invoke(node);
    } catch (Exception e) {
      return Collections.emptyList();
    }
  }

  // ---- Split Result Classes ----

  private static class AggregationSplit {
    final PlanNode partialPlan;
    final PlanNode finalPlan;

    AggregationSplit(PlanNode partialPlan, PlanNode finalPlan) {
      this.partialPlan = partialPlan;
      this.finalPlan = finalPlan;
    }
  }

  static class JoinSplit {
    final PlanNode leftPlan;
    final PlanNode rightPlan;
    final PlanNode coordinatorPlan;
    final List<FieldAccessTypedExpr> leftKeys;
    final List<FieldAccessTypedExpr> rightKeys;

    JoinSplit(
        PlanNode leftPlan,
        PlanNode rightPlan,
        PlanNode coordinatorPlan,
        List<FieldAccessTypedExpr> leftKeys,
        List<FieldAccessTypedExpr> rightKeys) {
      this.leftPlan = leftPlan;
      this.rightPlan = rightPlan;
      this.coordinatorPlan = coordinatorPlan;
      this.leftKeys = leftKeys;
      this.rightKeys = rightKeys;
    }
  }
}
