/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Window.Group;
import org.apache.calcite.rel.core.Window.RexWinAggCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.boostscale.velox4j.aggregate.Aggregate;
import org.boostscale.velox4j.aggregate.AggregateStep;
import org.boostscale.velox4j.connector.ExternalStreamTableHandle;
import org.boostscale.velox4j.expression.CallTypedExpr;
import org.boostscale.velox4j.expression.FieldAccessTypedExpr;
import org.boostscale.velox4j.expression.TypedExpr;
import org.boostscale.velox4j.join.JoinType;
import org.boostscale.velox4j.plan.AggregationNode;
import org.boostscale.velox4j.plan.FilterNode;
import org.boostscale.velox4j.plan.HashJoinNode;
import org.boostscale.velox4j.plan.LimitNode;
import org.boostscale.velox4j.plan.OrderByNode;
import org.boostscale.velox4j.plan.PlanNode;
import org.boostscale.velox4j.plan.ProjectNode;
import org.boostscale.velox4j.plan.TableScanNode;
import org.boostscale.velox4j.plan.WindowNode;
import org.boostscale.velox4j.sort.SortOrder;
import org.boostscale.velox4j.type.BigIntType;
import org.boostscale.velox4j.type.DoubleType;
import org.boostscale.velox4j.type.RealType;
import org.boostscale.velox4j.type.RowType;
import org.boostscale.velox4j.type.Type;
import org.boostscale.velox4j.window.BoundType;
import org.boostscale.velox4j.window.WindowFrame;
import org.boostscale.velox4j.window.WindowFunction;
import org.boostscale.velox4j.window.WindowType;
import org.opensearch.plugin.olap.plan.convert.PlanIdGenerator;
import org.opensearch.plugin.olap.plan.convert.VeloxAggConverter;
import org.opensearch.plugin.olap.plan.convert.VeloxExprConverter;
import org.opensearch.plugin.olap.plan.convert.VeloxTypeConverter;
import org.opensearch.plugin.olap.plan.fragment.FragmentProperties;
import org.opensearch.plugin.olap.plan.fragment.PlanFragment;

/**
 * Converts a physical Calcite plan (PhysicalConvention with PhysicalExchange nodes) into a list of
 * {@link PlanFragment}s containing velox4j PlanNodes.
 *
 * <p>The generator walks the physical plan tree. At each {@link PhysicalExchange} node, it creates
 * a fragment boundary. Within each fragment, it converts physical RelNodes to Velox PlanNodes using
 * the existing converter utilities ({@link VeloxExprConverter}, {@link VeloxAggConverter}, {@link
 * VeloxTypeConverter}).
 *
 * <p>This replaces both the old {@code VeloxPlanConverter} (for Velox PlanNode generation) and
 * {@code PlanFragmenter} (for fragment splitting).
 */
public class VeloxPlanGenerator {

  private static final Logger logger = LogManager.getLogger(VeloxPlanGenerator.class);
  private static final String CONNECTOR_ID = "connector-external-stream";

  private static final Set<String> METADATA_COLUMNS =
      Set.of("_id", "_index", "_score", "_maxscore", "_sort", "_routing");

  private final PlanIdGenerator idGen = new PlanIdGenerator();
  private final AtomicInteger fragmentId = new AtomicInteger(0);
  private final List<PlanFragment> fragments = new ArrayList<>();

  /**
   * Generate PlanFragments from an optimized physical plan.
   *
   * @param physicalPlan the physical plan from PhysicalOptimizer (PhysicalRel nodes with
   *     PhysicalExchange at boundaries)
   * @return ordered list of PlanFragments (leaf first, root last)
   */
  public List<PlanFragment> generate(RelNode physicalPlan) {
    idGen.reset();
    fragmentId.set(0);
    fragments.clear();

    // Convert the physical plan to Velox PlanNode, creating fragments at Exchange boundaries
    PlanNode rootPlan = toVeloxPlan(physicalPlan);

    // The root fragment is always the coordinator
    if (rootPlan != null) {
      String sourceIndex = extractSourceIndex(physicalPlan);
      // If the root plan IS a simple scan (no exchange), it's a single-fragment plan
      if (fragments.isEmpty()) {
        fragments.add(
            new PlanFragment(
                fragmentId.getAndIncrement(),
                rootPlan,
                FragmentProperties.source(sourceIndex),
                Collections.emptyList()));
      } else {
        // Root fragment is the coordinator, dependent on all leaf fragments
        List<Integer> inputIds = new ArrayList<>();
        for (PlanFragment f : fragments) {
          inputIds.add(f.getFragmentId());
        }
        fragments.add(
            new PlanFragment(
                fragmentId.getAndIncrement(),
                rootPlan,
                FragmentProperties.coordinator(),
                inputIds));
      }
    }

    for (PlanFragment f : fragments) {
      logger.info(
          "Fragment {}: dist={}, inputs={}",
          f.getFragmentId(),
          f.getProperties().getDistribution(),
          f.getInputFragmentIds());
    }
    return new ArrayList<>(fragments);
  }

  // ---- Physical -> Velox conversion ----

  private PlanNode toVeloxPlan(RelNode node) {
    // VolcanoPlanner wraps nodes in RelSubset — unwrap to get the actual physical node
    if (node instanceof RelSubset) {
      RelNode best = ((RelSubset) node).getBest();
      if (best != null) {
        return toVeloxPlan(best);
      }
      throw new IllegalStateException("RelSubset has no best plan: " + node);
    }
    if (node instanceof PhysicalExchange) {
      return handleExchange((PhysicalExchange) node);
    }
    if (node instanceof PhysicalTableScan) {
      return convertTableScan((PhysicalTableScan) node);
    }
    if (node instanceof PhysicalFilter) {
      return convertFilter((PhysicalFilter) node);
    }
    if (node instanceof PhysicalProject) {
      return convertProject((PhysicalProject) node);
    }
    if (node instanceof PhysicalAggregate) {
      return convertAggregate((PhysicalAggregate) node);
    }
    if (node instanceof PhysicalJoin) {
      return convertJoin((PhysicalJoin) node);
    }
    if (node instanceof PhysicalSort) {
      return convertSort((PhysicalSort) node);
    }
    if (node instanceof PhysicalWindow) {
      return convertWindow((PhysicalWindow) node);
    }
    throw new UnsupportedOperationException(
        "Unsupported physical node: " + node.getClass().getSimpleName());
  }

  /**
   * Handle a PhysicalExchange: the child subtree becomes a separate fragment. Returns null as a
   * placeholder — the parent fragment will wire an ExternalStream scan.
   *
   * <p>Exception: if the exchange's input is an intermediate join (no direct source index), the
   * child plan is inlined into the parent fragment instead of creating a separate fragment. This
   * ensures that for multi-way joins, all join operators stay in the COORDINATOR fragment while
   * only leaf table scans are dispatched to data nodes.
   */
  private PlanNode handleExchange(PhysicalExchange exchange) {
    // Recursively convert the child (the leaf/intermediate fragment)
    PlanNode childPlan = toVeloxPlan(exchange.getInput());

    // Determine fragment properties from the exchange's own distribution (what it enforces),
    // not the input's distribution. SINGLETON → gather to coordinator, HASH → shuffle scan.
    String sourceIndex = extractSourceIndex(exchange.getInput());
    RelDistribution exchangeDist = exchange.getDistribution();

    // If the exchange's input contains a join, it's an intermediate join fragment
    // (e.g., the inner join of a 3-way join). Inline it into the parent fragment so all
    // joins stay in the coordinator. Only leaf scans get their own fragments.
    if (containsJoin(exchange.getInput())
        && (exchangeDist == null
            || exchangeDist.getType() != RelDistribution.Type.HASH_DISTRIBUTED)) {
      return childPlan;
    }

    FragmentProperties props;
    if (exchangeDist != null && exchangeDist.getType() == RelDistribution.Type.HASH_DISTRIBUTED) {
      // Hash-partitioned fragment (MPP shuffle scan)
      List<Integer> keyChannels = exchangeDist.getKeys();
      props = FragmentProperties.shuffleScan(sourceIndex, null, keyChannels, 0);
    } else {
      // Source fragment (data node scan, gathered to coordinator)
      props = FragmentProperties.source(sourceIndex);
    }

    int childFragId = fragmentId.getAndIncrement();
    fragments.add(new PlanFragment(childFragId, childPlan, props, Collections.emptyList()));

    // Return null — the parent will create an exchange scan placeholder
    return null;
  }

  private PlanNode convertTableScan(PhysicalTableScan scan) {
    String nodeId = idGen.next();
    RelDataType rowType = scan.getRowType();

    List<String> names = new ArrayList<>();
    List<Type> types = new ArrayList<>();
    for (RelDataTypeField field : rowType.getFieldList()) {
      if (!METADATA_COLUMNS.contains(field.getName())) {
        names.add(field.getName());
        types.add(VeloxTypeConverter.toVeloxType(field.getType()));
      }
    }
    RowType outputType = new RowType(names, types);
    return new TableScanNode(
        nodeId, outputType, new ExternalStreamTableHandle(CONNECTOR_ID), Collections.emptyList());
  }

  private PlanNode convertFilter(PhysicalFilter filter) {
    String nodeId = idGen.next();
    PlanNode source = toVeloxPlan(filter.getInput());
    if (source == null) {
      source = createExchangeScan(filter.getInput().getRowType());
    }

    VeloxExprConverter exprConverter = new VeloxExprConverter(filter.getInput().getRowType());
    TypedExpr filterExpr = exprConverter.convert(filter.getCondition());

    return new FilterNode(nodeId, Collections.singletonList(source), filterExpr);
  }

  private PlanNode convertProject(PhysicalProject project) {
    String nodeId = idGen.next();
    PlanNode source = toVeloxPlan(project.getInput());
    if (source == null) {
      source = createExchangeScan(project.getInput().getRowType());
    }

    VeloxExprConverter exprConverter = new VeloxExprConverter(project.getInput().getRowType());

    List<String> names = new ArrayList<>();
    List<TypedExpr> projections = new ArrayList<>();
    List<RelDataTypeField> outputFields = project.getRowType().getFieldList();

    for (int i = 0; i < project.getProjects().size(); i++) {
      RexNode expr = project.getProjects().get(i);
      names.add(outputFields.get(i).getName());
      projections.add(exprConverter.convert(expr));
    }

    return new ProjectNode(nodeId, Collections.singletonList(source), names, projections);
  }

  private PlanNode convertAggregate(PhysicalAggregate agg) {
    // Check if this is a SINGLE aggregate above an exchange — needs two-stage split.
    // After VolcanoPlanner, inputs may be wrapped in RelSubset. Unwrap to find the
    // actual PhysicalExchange.
    RelNode rawInput = agg.getInput();
    if (rawInput instanceof RelSubset) {
      RelNode best = ((RelSubset) rawInput).getBest();
      if (best != null) rawInput = best;
    }
    if (agg.getStep() == PhysicalAggregate.Step.SINGLE && rawInput instanceof PhysicalExchange) {
      return convertTwoStageAggregate(agg, (PhysicalExchange) rawInput);
    }

    String nodeId = idGen.next();
    PlanNode source = toVeloxPlan(agg.getInput());
    if (source == null) {
      source = createExchangeScan(agg.getInput().getRowType());
    }

    RelDataType inputRowType = agg.getInput().getRowType();
    VeloxAggConverter aggConverter = new VeloxAggConverter(inputRowType);

    ImmutableBitSet groupSet = agg.getGroupSet();
    List<FieldAccessTypedExpr> groupingKeys = new ArrayList<>();
    for (int fieldIndex : groupSet) {
      RelDataTypeField field = inputRowType.getFieldList().get(fieldIndex);
      Type veloxType = VeloxTypeConverter.toVeloxType(field.getType());
      groupingKeys.add(FieldAccessTypedExpr.create(veloxType, field.getName()));
    }

    List<String> aggregateNames = new ArrayList<>();
    List<Aggregate> aggregates = new ArrayList<>();
    for (int i = 0; i < agg.getAggCallList().size(); i++) {
      AggregateCall aggCall = agg.getAggCallList().get(i);
      aggregateNames.add(aggConverter.resolveAggOutputName(aggCall, i));
      aggregates.add(aggConverter.convert(aggCall));
    }

    AggregateStep veloxStep;
    switch (agg.getStep()) {
      case PARTIAL:
        veloxStep = AggregateStep.PARTIAL;
        break;
      case FINAL:
        veloxStep = AggregateStep.FINAL;
        break;
      default:
        veloxStep = AggregateStep.SINGLE;
    }

    return new AggregationNode(
        nodeId,
        veloxStep,
        groupingKeys,
        Collections.emptyList(),
        aggregateNames,
        aggregates,
        false,
        false,
        Collections.singletonList(source),
        null,
        Collections.emptyList());
  }

  /**
   * Split a SINGLE aggregate above an exchange into PARTIAL (leaf) + FINAL (coordinator). The
   * PARTIAL runs on data nodes, the FINAL merges results on the coordinator.
   */
  private PlanNode convertTwoStageAggregate(PhysicalAggregate agg, PhysicalExchange exchange) {
    // 1. Convert the subtree below the exchange (scan + filter + project)
    PlanNode scanPlan = toVeloxPlan(exchange.getInput());

    // 2. Build the PARTIAL aggregate above the scan
    RelDataType inputRowType = exchange.getInput().getRowType();
    VeloxAggConverter aggConverter = new VeloxAggConverter(inputRowType);

    List<FieldAccessTypedExpr> groupingKeys = new ArrayList<>();
    for (int fieldIndex : agg.getGroupSet()) {
      RelDataTypeField field = inputRowType.getFieldList().get(fieldIndex);
      Type veloxType = VeloxTypeConverter.toVeloxType(field.getType());
      groupingKeys.add(FieldAccessTypedExpr.create(veloxType, field.getName()));
    }

    List<String> aggregateNames = new ArrayList<>();
    List<Aggregate> aggregates = new ArrayList<>();
    for (int i = 0; i < agg.getAggCallList().size(); i++) {
      AggregateCall aggCall = agg.getAggCallList().get(i);
      aggregateNames.add(aggConverter.resolveAggOutputName(aggCall, i));
      aggregates.add(aggConverter.convert(aggCall));
    }

    // Rewrite for PARTIAL step (intermediate accumulator types)
    List<Aggregate> partialAggs = new ArrayList<>(aggregates.size());
    for (Aggregate orig : aggregates) {
      Type intermediateType =
          resolveIntermediateType(
              orig.getCall().getFunctionName(),
              orig.getCall().getReturnType(),
              orig.getRawInputTypes());
      CallTypedExpr partialCall =
          new CallTypedExpr(
              intermediateType, orig.getCall().getInputs(), orig.getCall().getFunctionName());
      partialAggs.add(
          new Aggregate(
              partialCall,
              orig.getRawInputTypes(),
              orig.getMask(),
              orig.getSortingKeys(),
              orig.getSortingOrders(),
              orig.isDistinct()));
    }

    String partialId = idGen.next() + "_partial";
    AggregationNode partialAgg =
        new AggregationNode(
            partialId,
            AggregateStep.PARTIAL,
            groupingKeys,
            Collections.emptyList(),
            aggregateNames,
            partialAggs,
            false,
            false,
            Collections.singletonList(scanPlan),
            null,
            Collections.emptyList());

    // 3. Create the leaf fragment with PARTIAL agg
    String sourceIndex = extractSourceIndex(exchange.getInput());
    int leafFragId = fragmentId.getAndIncrement();
    fragments.add(
        new PlanFragment(
            leafFragId,
            partialAgg,
            FragmentProperties.source(sourceIndex),
            Collections.emptyList()));

    // 4. Build the FINAL aggregate (coordinator side)
    List<Aggregate> finalAggs = new ArrayList<>(aggregates.size());
    for (int i = 0; i < aggregates.size(); i++) {
      Aggregate orig = aggregates.get(i);
      String intermediateName = aggregateNames.get(i);
      TypedExpr intermediateRef =
          FieldAccessTypedExpr.create(orig.getCall().getReturnType(), intermediateName);
      CallTypedExpr finalCall =
          new CallTypedExpr(
              orig.getCall().getReturnType(),
              List.of(intermediateRef),
              orig.getCall().getFunctionName());
      finalAggs.add(
          new Aggregate(
              finalCall,
              orig.getRawInputTypes(),
              orig.getMask(),
              orig.getSortingKeys(),
              orig.getSortingOrders(),
              orig.isDistinct()));
    }

    String finalId = idGen.next() + "_final";
    return new AggregationNode(
        finalId,
        AggregateStep.FINAL,
        groupingKeys,
        Collections.emptyList(),
        aggregateNames,
        finalAggs,
        false,
        false,
        Collections.emptyList(),
        null,
        Collections.emptyList());
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
          if (inputType instanceof RealType || inputType instanceof DoubleType) {
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

  private PlanNode convertJoin(PhysicalJoin join) {
    String nodeId = idGen.next();

    PlanNode left = toVeloxPlan(join.getLeft());
    PlanNode right = toVeloxPlan(join.getRight());

    if (left == null) {
      left = createExchangeScan(join.getLeft().getRowType());
    }
    if (right == null) {
      right = createExchangeScan(join.getRight().getRowType());
    }

    // Handle column name conflicts: Velox requires unique names across left+right
    RelDataType leftRowType = join.getLeft().getRowType();
    RelDataType rightRowType = join.getRight().getRowType();
    RelDataType joinRowType = join.getRowType();
    int leftFieldCount = leftRowType.getFieldCount();

    // Check for name conflicts and wrap right side in rename project if needed
    Map<String, String> rightRenames = new LinkedHashMap<>();
    boolean hasConflict = false;
    for (int i = 0; i < rightRowType.getFieldCount(); i++) {
      String originalName = rightRowType.getFieldList().get(i).getName();
      String dedupName = joinRowType.getFieldList().get(leftFieldCount + i).getName();
      rightRenames.put(originalName, dedupName);
      if (!originalName.equals(dedupName)) {
        hasConflict = true;
      }
    }

    if (hasConflict) {
      String projId = idGen.next();
      List<String> projNames = new ArrayList<>();
      List<TypedExpr> projExprs = new ArrayList<>();
      for (int i = 0; i < rightRowType.getFieldCount(); i++) {
        String originalName = rightRowType.getFieldList().get(i).getName();
        if (METADATA_COLUMNS.contains(originalName)) continue;
        projNames.add(rightRenames.get(originalName));
        Type veloxType =
            VeloxTypeConverter.toVeloxType(rightRowType.getFieldList().get(i).getType());
        projExprs.add(FieldAccessTypedExpr.create(veloxType, originalName));
      }
      right = new ProjectNode(projId, Collections.singletonList(right), projNames, projExprs);
    }

    // Build output type
    RowType outputType = VeloxTypeConverter.toVeloxRowType(joinRowType);

    // Extract join keys
    List<FieldAccessTypedExpr> leftKeys = new ArrayList<>();
    List<FieldAccessTypedExpr> rightKeys = new ArrayList<>();
    extractJoinKeys(
        join.getCondition(),
        leftRowType,
        rightRowType,
        rightRenames,
        hasConflict,
        leftKeys,
        rightKeys);

    JoinType veloxJoinType = convertJoinType(join.getJoinType());

    return new HashJoinNode(
        nodeId, veloxJoinType, leftKeys, rightKeys, null, left, right, outputType, false, false);
  }

  private PlanNode convertSort(PhysicalSort sort) {
    // Check for two-stage TopN split: Sort(with fetch) above an Exchange
    RelNode rawInput = sort.getInput();
    if (rawInput instanceof RelSubset) {
      RelNode best = ((RelSubset) rawInput).getBest();
      if (best != null) rawInput = best;
    }
    if (sort.fetch != null && rawInput instanceof PhysicalExchange) {
      return convertTwoStageSort(sort, (PhysicalExchange) rawInput);
    }

    // Single-stage sort (no exchange below, or no limit)
    PlanNode source = toVeloxPlan(sort.getInput());
    if (source == null) {
      source = createExchangeScan(sort.getInput().getRowType());
    }

    return buildSortNodes(source, sort.getInput().getRowType(), sort);
  }

  /**
   * Split a Sort(with fetch) above an Exchange into two stages:
   *
   * <ul>
   *   <li>Leaf fragment: scan → OrderByNode → LimitNode(0, offset+limit) — partial TopN per shard
   *   <li>Coordinator fragment: ExchangeScan → OrderByNode → LimitNode(offset, limit) — final TopN
   * </ul>
   */
  private PlanNode convertTwoStageSort(PhysicalSort sort, PhysicalExchange exchange) {
    // 1. Convert the subtree below the exchange (scan + filter + project)
    PlanNode scanPlan = toVeloxPlan(exchange.getInput());
    RelDataType inputRowType = exchange.getInput().getRowType();

    // 2. Build PARTIAL sort+limit on the leaf (data node)
    long origOffset = sort.offset != null ? RexLiteral.intValue(sort.offset) : 0;
    long origLimit = sort.fetch != null ? RexLiteral.intValue(sort.fetch) : Long.MAX_VALUE;
    long partialLimit = origOffset + origLimit; // local top-K includes offset rows

    PlanNode partialPlan = scanPlan;
    if (sort.getCollation() != null && !sort.getCollation().getFieldCollations().isEmpty()) {
      partialPlan = buildOrderByNode(partialPlan, inputRowType, sort);
    }
    String partialLimitId = idGen.next() + "_partial_limit";
    partialPlan =
        new LimitNode(
            partialLimitId, Collections.singletonList(partialPlan), 0, partialLimit, true);

    // 3. Create the leaf fragment with partial sort+limit
    String sourceIndex = extractSourceIndex(exchange.getInput());
    int leafFragId = fragmentId.getAndIncrement();
    fragments.add(
        new PlanFragment(
            leafFragId,
            partialPlan,
            FragmentProperties.source(sourceIndex),
            Collections.emptyList()));

    // 4. Build FINAL sort+limit on the coordinator (with empty sources — wired during execution)
    PlanNode finalPlan = createExchangeScan(inputRowType);
    if (sort.getCollation() != null && !sort.getCollation().getFieldCollations().isEmpty()) {
      finalPlan = buildOrderByNode(finalPlan, inputRowType, sort);
    }
    String finalLimitId = idGen.next() + "_final_limit";
    finalPlan =
        new LimitNode(
            finalLimitId, Collections.singletonList(finalPlan), origOffset, origLimit, false);

    return finalPlan;
  }

  /** Build OrderByNode from a PhysicalSort's collation. */
  private PlanNode buildOrderByNode(PlanNode source, RelDataType inputRowType, PhysicalSort sort) {
    String orderNodeId = idGen.next();
    List<FieldAccessTypedExpr> sortingKeys = new ArrayList<>();
    List<SortOrder> sortingOrders = new ArrayList<>();

    for (RelFieldCollation fieldCollation : sort.getCollation().getFieldCollations()) {
      int fieldIndex = fieldCollation.getFieldIndex();
      RelDataTypeField field = inputRowType.getFieldList().get(fieldIndex);
      Type veloxType = VeloxTypeConverter.toVeloxType(field.getType());
      sortingKeys.add(FieldAccessTypedExpr.create(veloxType, field.getName()));

      boolean ascending = !fieldCollation.getDirection().isDescending();
      boolean nullsFirst = fieldCollation.nullDirection == RelFieldCollation.NullDirection.FIRST;
      sortingOrders.add(new SortOrder(ascending, nullsFirst));
    }

    return new OrderByNode(
        orderNodeId, Collections.singletonList(source), sortingKeys, sortingOrders, false);
  }

  /** Build sort + limit nodes for single-stage execution. */
  private PlanNode buildSortNodes(PlanNode source, RelDataType inputRowType, PhysicalSort sort) {
    if (sort.getCollation() != null && !sort.getCollation().getFieldCollations().isEmpty()) {
      source = buildOrderByNode(source, inputRowType, sort);
    }

    if (sort.fetch != null || sort.offset != null) {
      String limitNodeId = idGen.next();
      long offset = sort.offset != null ? RexLiteral.intValue(sort.offset) : 0;
      long count = sort.fetch != null ? RexLiteral.intValue(sort.fetch) : Long.MAX_VALUE;
      source = new LimitNode(limitNodeId, Collections.singletonList(source), offset, count, false);
    }

    return source;
  }

  private PlanNode convertWindow(PhysicalWindow window) {
    PlanNode source = toVeloxPlan(window.getInput());
    if (source == null) {
      source = createExchangeScan(window.getInput().getRowType());
    }

    RelDataType inputRowType = window.getInput().getRowType();
    String nodeId = idGen.next();

    // A Window can have multiple Groups (each with different partition/order specs).
    // Velox WindowNode supports one partition+sort spec, so we handle the first group.
    // Multiple groups would need chained WindowNodes (future enhancement).
    if (window.groups.isEmpty()) {
      return source;
    }

    Group group = window.groups.get(0);

    // Partition keys
    List<FieldAccessTypedExpr> partitionKeys = new ArrayList<>();
    for (int fieldIndex : group.keys) {
      RelDataTypeField field = inputRowType.getFieldList().get(fieldIndex);
      Type veloxType = VeloxTypeConverter.toVeloxType(field.getType());
      partitionKeys.add(FieldAccessTypedExpr.create(veloxType, field.getName()));
    }

    // Sort keys and orders
    List<FieldAccessTypedExpr> sortingKeys = new ArrayList<>();
    List<SortOrder> sortingOrders = new ArrayList<>();
    for (RelFieldCollation fieldCollation : group.orderKeys.getFieldCollations()) {
      int fieldIndex = fieldCollation.getFieldIndex();
      RelDataTypeField field = inputRowType.getFieldList().get(fieldIndex);
      Type veloxType = VeloxTypeConverter.toVeloxType(field.getType());
      sortingKeys.add(FieldAccessTypedExpr.create(veloxType, field.getName()));
      boolean ascending = !fieldCollation.getDirection().isDescending();
      boolean nullsFirst = fieldCollation.nullDirection == RelFieldCollation.NullDirection.FIRST;
      sortingOrders.add(new SortOrder(ascending, nullsFirst));
    }

    // Window frame
    WindowType windowType = group.isRows ? WindowType.ROWS : WindowType.RANGE;
    BoundType startBound = convertBoundType(group.lowerBound, true);
    BoundType endBound = convertBoundType(group.upperBound, false);
    WindowFrame frame = new WindowFrame(windowType, startBound, null, endBound, null);

    // Window functions (RexWinAggCall → WindowFunction)
    List<String> windowColumnNames = new ArrayList<>();
    List<WindowFunction> windowFunctions = new ArrayList<>();
    VeloxExprConverter exprConverter = new VeloxExprConverter(inputRowType);

    int outputFieldOffset = inputRowType.getFieldCount();
    for (int i = 0; i < group.aggCalls.size(); i++) {
      RexWinAggCall aggCall = group.aggCalls.get(i);

      // Output column name from the window's row type
      String colName = window.getRowType().getFieldList().get(outputFieldOffset + i).getName();
      windowColumnNames.add(colName);

      // Convert function call
      String funcName = aggCall.getOperator().getName().toLowerCase(Locale.ROOT);
      Type returnType = VeloxTypeConverter.toVeloxType(aggCall.getType());

      List<TypedExpr> args = new ArrayList<>();
      for (RexNode operand : aggCall.getOperands()) {
        args.add(exprConverter.convert(operand));
      }

      CallTypedExpr callExpr = new CallTypedExpr(returnType, args, funcName);
      windowFunctions.add(new WindowFunction(callExpr, frame, false));
    }

    return new WindowNode(
        nodeId,
        partitionKeys,
        sortingKeys,
        sortingOrders,
        windowColumnNames,
        windowFunctions,
        false,
        Collections.singletonList(source));
  }

  private BoundType convertBoundType(RexWindowBound bound, boolean isLower) {
    if (bound.isUnbounded()) {
      return isLower ? BoundType.UNBOUNDED_PRECEDING : BoundType.UNBOUNDED_FOLLOWING;
    }
    if (bound.isCurrentRow()) {
      return BoundType.CURRENT_ROW;
    }
    return isLower ? BoundType.PRECEDING : BoundType.FOLLOWING;
  }

  // ---- Helpers ----

  /** Create an ExternalStream scan placeholder for data arriving from a child fragment. */
  private PlanNode createExchangeScan(RelDataType rowType) {
    String nodeId = "exchange_scan_" + idGen.next();
    List<String> names = new ArrayList<>();
    List<Type> types = new ArrayList<>();
    for (RelDataTypeField field : rowType.getFieldList()) {
      if (!METADATA_COLUMNS.contains(field.getName())) {
        names.add(field.getName());
        types.add(VeloxTypeConverter.toVeloxType(field.getType()));
      }
    }
    RowType outputType = new RowType(names, types);
    return new TableScanNode(
        nodeId, outputType, new ExternalStreamTableHandle(CONNECTOR_ID), Collections.emptyList());
  }

  private void extractJoinKeys(
      RexNode condition,
      RelDataType leftRowType,
      RelDataType rightRowType,
      Map<String, String> rightRenames,
      boolean hasConflict,
      List<FieldAccessTypedExpr> leftKeys,
      List<FieldAccessTypedExpr> rightKeys) {
    if (condition == null) return;
    int leftFieldCount = leftRowType.getFieldCount();

    if (condition instanceof RexCall) {
      RexCall call = (RexCall) condition;

      if (call.getKind() == SqlKind.EQUALS) {
        RexNode op0 = call.getOperands().get(0);
        RexNode op1 = call.getOperands().get(1);
        if (op0 instanceof RexInputRef && op1 instanceof RexInputRef) {
          RexInputRef ref0 = (RexInputRef) op0;
          RexInputRef ref1 = (RexInputRef) op1;

          RexInputRef leftRef, rightRef;
          if (ref0.getIndex() < leftFieldCount && ref1.getIndex() >= leftFieldCount) {
            leftRef = ref0;
            rightRef = ref1;
          } else if (ref1.getIndex() < leftFieldCount && ref0.getIndex() >= leftFieldCount) {
            leftRef = ref1;
            rightRef = ref0;
          } else {
            return;
          }

          RelDataTypeField leftField = leftRowType.getFieldList().get(leftRef.getIndex());
          Type leftType = VeloxTypeConverter.toVeloxType(leftField.getType());
          leftKeys.add(FieldAccessTypedExpr.create(leftType, leftField.getName()));

          int rightIndex = rightRef.getIndex() - leftFieldCount;
          RelDataTypeField rightField = rightRowType.getFieldList().get(rightIndex);
          Type rightType = VeloxTypeConverter.toVeloxType(rightField.getType());
          String rightName = rightField.getName();
          if (hasConflict && rightRenames.containsKey(rightName)) {
            rightName = rightRenames.get(rightName);
          }
          rightKeys.add(FieldAccessTypedExpr.create(rightType, rightName));
          return;
        }
      }

      if (call.getKind() == SqlKind.AND) {
        for (RexNode operand : call.getOperands()) {
          extractJoinKeys(
              operand, leftRowType, rightRowType, rightRenames, hasConflict, leftKeys, rightKeys);
        }
      }
    }
  }

  private JoinType convertJoinType(JoinRelType calciteType) {
    switch (calciteType) {
      case INNER:
        return JoinType.INNER;
      case LEFT:
        return JoinType.LEFT;
      case RIGHT:
        return JoinType.RIGHT;
      case FULL:
        return JoinType.FULL;
      case SEMI:
        return JoinType.LEFT_SEMI_FILTER;
      case ANTI:
        return JoinType.ANTI;
      default:
        throw new UnsupportedOperationException("Unsupported join type: " + calciteType);
    }
  }

  /** Check if a RelNode subtree contains a PhysicalJoin (walking through single-input nodes). */
  private boolean containsJoin(RelNode node) {
    if (node instanceof PhysicalJoin) {
      return true;
    }
    for (RelNode input : node.getInputs()) {
      if (containsJoin(input)) {
        return true;
      }
    }
    return false;
  }

  private String extractSourceIndex(RelNode node) {
    if (node instanceof TableScan) {
      List<String> names = ((TableScan) node).getTable().getQualifiedName();
      return names.get(names.size() - 1);
    }
    for (RelNode input : node.getInputs()) {
      String index = extractSourceIndex(input);
      if (index != null) return index;
    }
    return null;
  }
}
