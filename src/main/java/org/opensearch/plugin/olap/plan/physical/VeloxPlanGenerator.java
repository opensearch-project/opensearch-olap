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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.boostscale.velox4j.aggregate.Aggregate;
import org.boostscale.velox4j.aggregate.AggregateStep;
import org.boostscale.velox4j.connector.ExternalStreamTableHandle;
import org.boostscale.velox4j.expression.CallTypedExpr;
import org.boostscale.velox4j.expression.CastTypedExpr;
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
import org.boostscale.velox4j.type.IntegerType;
import org.boostscale.velox4j.type.RealType;
import org.boostscale.velox4j.type.RowType;
import org.boostscale.velox4j.type.Type;
import org.boostscale.velox4j.window.BoundType;
import org.boostscale.velox4j.window.WindowFrame;
import org.boostscale.velox4j.window.WindowFunction;
import org.boostscale.velox4j.window.WindowType;
import org.opensearch.plugin.olap.plan.convert.FieldMapping;
import org.opensearch.plugin.olap.plan.convert.PlanIdGenerator;
import org.opensearch.plugin.olap.plan.convert.RelevanceSplitter;
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

  /**
   * Window functions whose Velox registration returns INTEGER (Spark dialect), not BIGINT.
   *
   * <p>velox4j's init registers Presto (BIGINT) first, then Spark (INTEGER) second, and the Spark
   * registration overwrites. So despite Presto's documented BIGINT return type, the live
   * registration actually returns INTEGER for these functions. Calcite's default {@code
   * deriveRankType} returns BIGINT, producing a plan-type mismatch that Velox rejects at
   * WindowFunction::create with "Expected INTEGER. Got BIGINT." We emit INTEGER at the WindowNode
   * and then insert a CastTypedExpr to bring the output back to Calcite's declared type so
   * downstream filters/projects that index the column by its Calcite-declared BIGINT still work.
   *
   * <p>Intentionally excludes {@code ntile} and {@code nth_value}: both take an explicit offset
   * argument whose type the Spark registration pins to INTEGER, while Calcite may present the
   * offset as BIGINT. Until we coerce the argument to INTEGER (and reject out-of-range literals),
   * these two are kept out of {@code SUPPORTED_WINDOW_FUNCTIONS} in {@code
   * VectorizedEngineExtension} so they fall back to the default engine cleanly. For {@code
   * nth_value} the return type is also not INTEGER ({@code T → T}), so widening here would be
   * actively wrong.
   */
  private static final Set<String> INTEGER_RETURNING_WINDOW_FUNCTIONS =
      Set.of("row_number", "rank", "dense_rank");

  private final PlanIdGenerator idGen = new PlanIdGenerator();
  private final AtomicInteger fragmentId = new AtomicInteger(0);
  private final List<PlanFragment> fragments = new ArrayList<>();

  // Field mapping from Calcite field indices to Velox scan output, built during convertTableScan.
  // Used by convertProject/convertFilter to remap field references for MAP→ROW conversion.
  private FieldMapping[] currentFieldMappings;
  private RowType currentVeloxOutputType;

  // Accumulates OpenSearch QueryBuilders peeled off by RelevanceSplitter during convertFilter.
  // Scoped to the *current* leaf fragment being built — handleExchange and the other
  // leaf-creating paths stamp this onto the freshly-created PlanFragment and reset it before
  // walking the next sibling. Multiple FilterNodes within the same fragment AND-combine.
  private org.opensearch.index.query.QueryBuilder currentFragmentPushdown;

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
    currentFieldMappings = null;
    currentVeloxOutputType = null;
    currentFragmentPushdown = null;

    // Convert the physical plan to Velox PlanNode, creating fragments at Exchange boundaries
    PlanNode rootPlan = toVeloxPlan(physicalPlan);

    // The root fragment is always the coordinator
    if (rootPlan != null) {
      String sourceIndex = extractSourceIndex(physicalPlan);
      // If the root plan IS a simple scan (no exchange), it's a single-fragment plan
      if (fragments.isEmpty()) {
        PlanFragment single =
            new PlanFragment(
                fragmentId.getAndIncrement(),
                rootPlan,
                FragmentProperties.source(sourceIndex),
                Collections.emptyList());
        single.setRelevancePushdown(currentFragmentPushdown);
        currentFragmentPushdown = null;
        fragments.add(single);
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
    PlanFragment leafFragment =
        new PlanFragment(childFragId, childPlan, props, Collections.emptyList());
    // Attach any relevance pushdown accumulated while converting this leaf's subtree, then
    // clear — a sibling subtree (other join side) must not inherit it.
    leafFragment.setRelevancePushdown(currentFragmentPushdown);
    currentFragmentPushdown = null;
    fragments.add(leafFragment);

    // Reset field mappings — the parent fragment has a different schema context
    currentFieldMappings = null;
    currentVeloxOutputType = null;

    // Return null — the parent will create an exchange scan placeholder
    return null;
  }

  private PlanNode convertTableScan(PhysicalTableScan scan) {
    String nodeId = idGen.next();
    RelDataType rowType = scan.getRowType();

    // Build field list, converting MAP<VARCHAR, ANY> (OpenSearch object) to ROW type
    // by inspecting sibling dot-path columns (e.g., cloud: MAP + cloud.region: VARCHAR
    // → cloud: ROW(region: VARCHAR)).
    // Also build a field mapping from Calcite indices to Velox indices for expression remapping.
    List<String> names = new ArrayList<>();
    List<Type> types = new ArrayList<>();
    FieldMapping[] mappings = new FieldMapping[rowType.getFieldCount()];
    Map<String, Integer> parentVeloxIndex = new LinkedHashMap<>();
    int veloxIdx = 0;

    for (RelDataTypeField field : rowType.getFieldList()) {
      String fieldName = field.getName();
      int calciteIdx = field.getIndex();
      if (METADATA_COLUMNS.contains(fieldName)) {
        // Metadata columns (_id, _index, etc.) are not present in the Velox scan output.
        // Mark as skipped (-1) so Project can drop references to them.
        mappings[calciteIdx] = new FieldMapping(-1, fieldName, null);
        continue;
      }
      SqlTypeName typeName = field.getType().getSqlTypeName();

      if (typeName == SqlTypeName.MAP) {
        // MAP parent (OpenSearch object field, e.g., "cloud", "metrics") — skip from scan output.
        // Its dot-path children are kept as flat top-level fields.
        // Mark as skipped (-1) in mapping. The output Project reconstructs structs in Java.
        mappings[calciteIdx] = new FieldMapping(-1, fieldName, null);
      } else {
        // Regular field or dot-path child (e.g., "@timestamp", "cloud.region", "metrics.size")
        // Keep as flat top-level field in Velox scan output.
        mappings[calciteIdx] = new FieldMapping(veloxIdx, fieldName, null);
        names.add(fieldName);
        types.add(VeloxTypeConverter.toVeloxType(field.getType()));
        veloxIdx++;
      }
    }

    RowType outputType = new RowType(names, types);
    this.currentFieldMappings = mappings;
    this.currentVeloxOutputType = outputType;

    return new TableScanNode(
        nodeId, outputType, new ExternalStreamTableHandle(CONNECTOR_ID), Collections.emptyList());
  }

  /**
   * Build a Velox ROW type from dot-path children of a parent field. For example, if the parent is
   * "cloud" and the row type contains "cloud.region: VARCHAR" and "cloud.provider: VARCHAR", this
   * returns ROW(region: VARCHAR, provider: VARCHAR). Handles multi-level nesting recursively.
   */
  private RowType buildRowTypeFromChildren(String parentName, RelDataType rowType) {
    String prefix = parentName + ".";
    List<String> childNames = new ArrayList<>();
    List<Type> childTypes = new ArrayList<>();

    for (RelDataTypeField field : rowType.getFieldList()) {
      String fieldName = field.getName();
      if (!fieldName.startsWith(prefix)) {
        continue;
      }
      String childPath = fieldName.substring(prefix.length());
      // Only take direct children (no dots in remaining path)
      if (childPath.contains(".")) {
        // This is a grandchild — check if the intermediate parent is already added
        String directChild = childPath.substring(0, childPath.indexOf('.'));
        if (!childNames.contains(directChild)) {
          // Recursively build nested ROW for intermediate object
          RowType nestedRow = buildRowTypeFromChildren(prefix + directChild, rowType);
          if (nestedRow != null && nestedRow.size() > 0) {
            childNames.add(directChild);
            childTypes.add(nestedRow);
          }
        }
      } else {
        SqlTypeName typeName = field.getType().getSqlTypeName();
        if (typeName != SqlTypeName.MAP && typeName != SqlTypeName.ANY) {
          childNames.add(childPath);
          childTypes.add(VeloxTypeConverter.toVeloxType(field.getType()));
        }
      }
    }
    if (childNames.isEmpty()) {
      return null;
    }
    return new RowType(childNames, childTypes);
  }

  private PlanNode convertFilter(PhysicalFilter filter) {
    PlanNode source = toVeloxPlan(filter.getInput());
    if (source == null) {
      source = createExchangeScan(filter.getInput().getRowType());
    }

    // Peel any relevance-function calls out of the filter before they reach Velox. The Lucene
    // QueryBuilder is accumulated on the generator and shipped as a transport-level side channel;
    // the residual (if any) flows through VeloxExprConverter as a normal FilterNode.
    RelevanceSplitter splitter =
        new RelevanceSplitter(filter.getCluster().getRexBuilder(), filter.getInput().getRowType());
    RelevanceSplitter.Result split = splitter.split(filter.getCondition());
    if (split.ruledOut) {
      // canVectorize should have rejected this plan before we got here. Log and continue with
      // the filter intact (relevance calls will fail Velox compilation — but that's strictly
      // better than a wrong result, and a missed canVectorize gate is a bug we want to see).
      logger.warn(
          "Relevance splitter ruled out a filter that canVectorize accepted: {}",
          filter.getCondition());
    }
    if (split.pushdownQuery != null) {
      currentFragmentPushdown = andCombine(currentFragmentPushdown, split.pushdownQuery);
    }
    if (split.residualCondition == null) {
      return source;
    }
    VeloxExprConverter exprConverter = createExprConverter(filter.getInput().getRowType());
    TypedExpr filterExpr = exprConverter.convert(split.residualCondition);
    return new FilterNode(idGen.next(), Collections.singletonList(source), filterExpr);
  }

  /** AND-combine two QueryBuilders via {@link BoolQueryBuilder#must}. */
  private static org.opensearch.index.query.QueryBuilder andCombine(
      org.opensearch.index.query.QueryBuilder a, org.opensearch.index.query.QueryBuilder b) {
    if (a == null) return b;
    if (b == null) return a;
    return new org.opensearch.index.query.BoolQueryBuilder().must(a).must(b);
  }

  private PlanNode convertProject(PhysicalProject project) {
    String nodeId = idGen.next();

    // Snapshot the scan-level flat schema BEFORE descending into the input, so we can expand
    // MAP parent projections into their flat dot-path children regardless of how deep this
    // Project sits above the scan.
    RowType scanOutputType = currentVeloxOutputType;

    PlanNode source = toVeloxPlan(project.getInput());
    if (source == null) {
      source = createExchangeScan(project.getInput().getRowType());
    }
    if (scanOutputType == null) {
      // Reconstruct a flat row type from the input's Calcite row type when mappings weren't
      // populated (e.g., exchange-scan placeholder above a coordinator fragment).
      scanOutputType = buildFlatRowType(project.getInput().getRowType());
    }

    VeloxExprConverter exprConverter = createExprConverter(project.getInput().getRowType());

    List<String> names = new ArrayList<>();
    List<TypedExpr> projections = new ArrayList<>();
    List<RelDataTypeField> outputFields = project.getRowType().getFieldList();
    FieldMapping[] newMappings = new FieldMapping[outputFields.size()];
    Map<String, Integer> emittedIndex = new LinkedHashMap<>();

    for (int i = 0; i < project.getProjects().size(); i++) {
      RexNode expr = project.getProjects().get(i);
      String outputName = outputFields.get(i).getName();

      if (isMapParentRef(expr)) {
        // MAP parent projection — expand into one projection per flat dot-path child
        // (e.g., "metrics" → "metrics.size", "metrics.tmin"). The flat columns survive
        // through downstream operators and Java's reconstructStructs reassembles them
        // into a nested map in the final output.
        String prefix = outputName + ".";
        int expanded = 0;
        for (int j = 0; j < scanOutputType.size(); j++) {
          String flatName = scanOutputType.getNames().get(j);
          if (flatName.startsWith(prefix) && !emittedIndex.containsKey(flatName)) {
            emittedIndex.put(flatName, names.size());
            names.add(flatName);
            projections.add(
                FieldAccessTypedExpr.create(scanOutputType.getChildren().get(j), flatName));
            expanded++;
          }
        }
        // Mark the Calcite MAP field as "expanded" — veloxIndex=-1 tells operators above to
        // skip direct refs. Children remain accessible by name via reconstructStructs.
        newMappings[i] = new FieldMapping(-1, outputName, null);
        if (expanded == 0) {
          logger.warn(
              "MAP parent projection {} has no flat dot-path children in scan output", outputName);
        }
        continue;
      }
      // Skip duplicate outputs — a Project may reference both "cloud" (MAP) and "cloud.region" in
      // the same output; the MAP-parent expansion already covers the child.
      Integer existing = emittedIndex.get(outputName);
      if (existing != null) {
        newMappings[i] = new FieldMapping(existing, outputName, null);
        continue;
      }
      emittedIndex.put(outputName, names.size());
      newMappings[i] = new FieldMapping(names.size(), outputName, null);
      names.add(outputName);
      projections.add(exprConverter.convert(expr));
    }

    // Build the Project's Velox output type from the emitted names/projections so downstream
    // operators can still resolve field references (e.g., Sort collation field indices).
    List<Type> outTypes = new ArrayList<>(projections.size());
    for (TypedExpr p : projections) {
      outTypes.add(p.getReturnType());
    }
    currentVeloxOutputType = new RowType(new ArrayList<>(names), outTypes);
    currentFieldMappings = newMappings;

    return new ProjectNode(nodeId, Collections.singletonList(source), names, projections);
  }

  /** Build a Velox RowType from a Calcite row type, skipping metadata and MAP parent columns. */
  private RowType buildFlatRowType(RelDataType rowType) {
    List<String> names = new ArrayList<>();
    List<Type> types = new ArrayList<>();
    for (RelDataTypeField field : rowType.getFieldList()) {
      if (METADATA_COLUMNS.contains(field.getName())) {
        continue;
      }
      if (field.getType().getSqlTypeName() == SqlTypeName.MAP) {
        continue;
      }
      names.add(field.getName());
      types.add(VeloxTypeConverter.toVeloxType(field.getType()));
    }
    return new RowType(names, types);
  }

  /**
   * Check if a RexNode is a reference to a column absent from the Velox scan output. This includes:
   *
   * <ul>
   *   <li>MAP/ANY parents (OpenSearch object types) — reconstructed as structs in Java post-scan.
   *   <li>OpenSearch metadata columns (_id, _index, etc.) — not exposed through doc values.
   * </ul>
   */
  private boolean isMapParentRef(RexNode expr) {
    if (!(expr instanceof RexInputRef)) {
      return false;
    }
    // Within the scan subtree, use field mappings (veloxIndex < 0 means skipped).
    if (currentFieldMappings != null) {
      int calciteIdx = ((RexInputRef) expr).getIndex();
      if (calciteIdx < currentFieldMappings.length
          && currentFieldMappings[calciteIdx] != null
          && currentFieldMappings[calciteIdx].getVeloxIndex() < 0) {
        return true;
      }
    }
    // Above the scan subtree, detect MAP parents by Calcite type.
    SqlTypeName typeName = expr.getType().getSqlTypeName();
    return typeName == SqlTypeName.MAP;
  }

  /**
   * Resolve a Calcite field index to a Velox FieldAccessTypedExpr, applying MAP→ROW field mapping
   * when available. Used by sort keys, aggregation group keys, and other non-expression contexts.
   */
  private FieldAccessTypedExpr resolveFieldAccess(int calciteIndex, RelDataType inputRowType) {
    if (currentFieldMappings != null
        && calciteIndex < currentFieldMappings.length
        && currentFieldMappings[calciteIndex] != null) {
      FieldMapping mapping = currentFieldMappings[calciteIndex];
      int veloxIndex = mapping.getVeloxIndex();
      if (veloxIndex < 0) {
        // MAP parent column (skipped from scan) — return null to skip this sort key.
        // This happens when SystemLimit adds default sort on ALL columns including MAP parents.
        return null;
      }
      Type veloxType = currentVeloxOutputType.getChildren().get(veloxIndex);
      String fieldName = currentVeloxOutputType.getNames().get(veloxIndex);
      return FieldAccessTypedExpr.create(veloxType, fieldName);
    }
    RelDataTypeField field = inputRowType.getFieldList().get(calciteIndex);
    return FieldAccessTypedExpr.create(
        VeloxTypeConverter.toVeloxType(field.getType()), field.getName());
  }

  /**
   * Create a VeloxExprConverter with field mappings when available. Only uses mappings when the
   * input row type matches the scan row type (same field count) — mappings are invalid for
   * operators above aggregation/exchange that change the schema.
   */
  private VeloxExprConverter createExprConverter(RelDataType inputRowType) {
    if (currentFieldMappings != null
        && currentVeloxOutputType != null
        && inputRowType.getFieldCount() == currentFieldMappings.length) {
      return new VeloxExprConverter(inputRowType, currentFieldMappings, currentVeloxOutputType);
    }
    return new VeloxExprConverter(inputRowType);
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

    // Aggregate reshapes the schema — mappings into the scan are invalid above this point.
    currentFieldMappings = null;
    currentVeloxOutputType = null;

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
    PlanFragment partialAggFragment =
        new PlanFragment(
            leafFragId,
            partialAgg,
            FragmentProperties.source(sourceIndex),
            Collections.emptyList());
    partialAggFragment.setRelevancePushdown(currentFragmentPushdown);
    currentFragmentPushdown = null;
    fragments.add(partialAggFragment);

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
    AggregationNode finalAgg =
        new AggregationNode(
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

    // Aggregate reshapes the schema — mappings into the scan are invalid above this point.
    currentFieldMappings = null;
    currentVeloxOutputType = null;
    return finalAgg;
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

    HashJoinNode joinNode =
        new HashJoinNode(
            nodeId,
            veloxJoinType,
            leftKeys,
            rightKeys,
            null,
            left,
            right,
            outputType,
            false,
            false,
            false);

    // Join reshapes the schema — mappings into either scan are invalid above this point.
    currentFieldMappings = null;
    currentVeloxOutputType = null;
    return joinNode;
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
    PlanFragment partialSortFragment =
        new PlanFragment(
            leafFragId,
            partialPlan,
            FragmentProperties.source(sourceIndex),
            Collections.emptyList());
    partialSortFragment.setRelevancePushdown(currentFragmentPushdown);
    currentFragmentPushdown = null;
    fragments.add(partialSortFragment);

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

    // Mappings are only valid when the sort's input schema matches the scan's schema.
    // Operators that reshape the schema (Project, Aggregate, Join, Window) clear mappings
    // on exit, so they are null here. But even a Filter above the scan preserves the schema,
    // so we check the field count to distinguish.
    boolean mappingsApplicable =
        currentFieldMappings != null && inputRowType.getFieldCount() == currentFieldMappings.length;

    for (RelFieldCollation fieldCollation : sort.getCollation().getFieldCollations()) {
      int fieldIndex = fieldCollation.getFieldIndex();
      if (mappingsApplicable
          && fieldIndex < currentFieldMappings.length
          && currentFieldMappings[fieldIndex] != null) {
        FieldAccessTypedExpr sortKey = resolveFieldAccess(fieldIndex, inputRowType);
        if (sortKey == null) {
          // MAP parent column — skip
          continue;
        }
        sortingKeys.add(sortKey);
      } else {
        // No mapping — use direct field access (non-MAP tables, coordinator fragments)
        RelDataTypeField field = inputRowType.getFieldList().get(fieldIndex);
        Type veloxType = VeloxTypeConverter.toVeloxType(field.getType());
        sortingKeys.add(FieldAccessTypedExpr.create(veloxType, field.getName()));
      }

      boolean ascending = !fieldCollation.getDirection().isDescending();
      boolean nullsFirst = fieldCollation.nullDirection == RelFieldCollation.NullDirection.FIRST;
      sortingOrders.add(new SortOrder(ascending, nullsFirst));
    }

    // If all sort keys were MAP parents (skipped), return source without sorting
    if (sortingKeys.isEmpty()) {
      return source;
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

    // Snapshot the source's mappings BEFORE building our output schema. The outer Project above
    // Window relies on these (e.g., to skip refs to metadata columns or MAP parents that the scan
    // dropped — the scan marked them with veloxIndex=-1).
    FieldMapping[] sourceMappings = currentFieldMappings;
    RowType sourceVeloxType = currentVeloxOutputType;
    // When the Window sits above an exchange (handleExchange cleared mappings, createExchangeScan
    // didn't repopulate them), reconstruct the source's Velox schema and mappings from Calcite's
    // input row type using the same drop-MAP-and-metadata rules the scan uses. Without this,
    // every pre-window column gets veloxIndex=-1 in the rebuild below, silently dropping any
    // downstream Sort/Filter/Project reference to a real input field.
    if (sourceVeloxType == null) {
      sourceVeloxType = buildFlatRowType(inputRowType);
      Map<String, Integer> nameToIdx = new LinkedHashMap<>();
      for (int i = 0; i < sourceVeloxType.getNames().size(); i++) {
        nameToIdx.put(sourceVeloxType.getNames().get(i), i);
      }
      FieldMapping[] rebuilt = new FieldMapping[inputRowType.getFieldCount()];
      for (int i = 0; i < inputRowType.getFieldCount(); i++) {
        RelDataTypeField f = inputRowType.getFieldList().get(i);
        Integer vIdx = nameToIdx.get(f.getName());
        rebuilt[i] = new FieldMapping(vIdx != null ? vIdx : -1, f.getName(), null);
      }
      sourceMappings = rebuilt;
    }

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

    // Track ranking functions whose Velox registration emits INTEGER but whose Calcite-declared
    // type is wider (usually BIGINT). For each, record the Calcite-declared type so we can insert
    // a cast projection above the WindowNode to re-widen the output.
    List<Integer> rankingColumnsToCast = new ArrayList<>();
    List<Type> rankingColumnTargetTypes = new ArrayList<>();

    int outputFieldOffset = inputRowType.getFieldCount();
    for (int i = 0; i < group.aggCalls.size(); i++) {
      RexWinAggCall aggCall = group.aggCalls.get(i);

      // Output column name from the window's row type
      String colName = window.getRowType().getFieldList().get(outputFieldOffset + i).getName();
      windowColumnNames.add(colName);

      // Convert function call
      String funcName = aggCall.getOperator().getName().toLowerCase(Locale.ROOT);
      Type calciteDeclaredType = VeloxTypeConverter.toVeloxType(aggCall.getType());

      // Velox's registered ranking functions return INTEGER (Spark dialect wins the registration
      // race against Presto's BIGINT in Init.cc). Override the WindowNode's declared return type
      // so Velox accepts the plan; the cast projection below restores Calcite's declared type.
      Type veloxReturnType = calciteDeclaredType;
      if (INTEGER_RETURNING_WINDOW_FUNCTIONS.contains(funcName)
          && !(calciteDeclaredType instanceof IntegerType)) {
        veloxReturnType = new IntegerType();
        rankingColumnsToCast.add(i);
        rankingColumnTargetTypes.add(calciteDeclaredType);
      }

      List<TypedExpr> args = new ArrayList<>();
      for (RexNode operand : aggCall.getOperands()) {
        args.add(exprConverter.convert(operand));
      }

      CallTypedExpr callExpr = new CallTypedExpr(veloxReturnType, args, funcName);
      windowFunctions.add(new WindowFunction(callExpr, frame, false));
    }

    WindowNode windowNode =
        new WindowNode(
            nodeId,
            partitionKeys,
            sortingKeys,
            sortingOrders,
            windowColumnNames,
            windowFunctions,
            false,
            Collections.singletonList(source));

    // If any ranking function needed type-narrowing to match Velox's registration, wrap the
    // WindowNode in a ProjectNode that casts the narrowed output back to Calcite's declared type.
    // Without this cast, downstream operators that index these columns by their Calcite-declared
    // type (BIGINT) would see a type mismatch against the Velox INTEGER output.
    PlanNode result = windowNode;
    List<String> outVeloxNames = new ArrayList<>();
    List<Type> outVeloxTypes = new ArrayList<>();
    if (!rankingColumnsToCast.isEmpty()) {
      List<TypedExpr> projExprs = new ArrayList<>();
      // Pass through the source's ACTUAL Velox output columns. We can't use Calcite's inputRowType
      // here because the scan drops MAP parents (OpenSearch object fields like "cloud") and
      // metadata columns (_id, _score, etc.) — those don't exist in the WindowNode output and
      // referencing them would trigger "Field not found" in Velox.
      if (sourceVeloxType != null) {
        List<String> srcNames = sourceVeloxType.getNames();
        List<Type> srcTypes = sourceVeloxType.getChildren();
        for (int i = 0; i < srcNames.size(); i++) {
          outVeloxNames.add(srcNames.get(i));
          outVeloxTypes.add(srcTypes.get(i));
          projExprs.add(FieldAccessTypedExpr.create(srcTypes.get(i), srcNames.get(i)));
        }
      } else {
        // Fallback: iterate Calcite fields (shouldn't normally happen — source conversion
        // populates currentVeloxOutputType for TableScan and Project).
        for (RelDataTypeField field : inputRowType.getFieldList()) {
          Type t = VeloxTypeConverter.toVeloxType(field.getType());
          outVeloxNames.add(field.getName());
          outVeloxTypes.add(t);
          projExprs.add(FieldAccessTypedExpr.create(t, field.getName()));
        }
      }
      // Pass through window columns, casting ranking ones up to their Calcite-declared type.
      for (int i = 0; i < group.aggCalls.size(); i++) {
        String colName = windowColumnNames.get(i);
        int castIdx = rankingColumnsToCast.indexOf(i);
        if (castIdx >= 0) {
          Type widened = rankingColumnTargetTypes.get(castIdx);
          outVeloxNames.add(colName);
          outVeloxTypes.add(widened);
          projExprs.add(
              CastTypedExpr.create(
                  widened,
                  FieldAccessTypedExpr.create(new IntegerType(), colName),
                  /* isTryCast */ false));
        } else {
          RelDataType calciteType =
              window.getRowType().getFieldList().get(outputFieldOffset + i).getType();
          Type t = VeloxTypeConverter.toVeloxType(calciteType);
          outVeloxNames.add(colName);
          outVeloxTypes.add(t);
          projExprs.add(FieldAccessTypedExpr.create(t, colName));
        }
      }
      result =
          new ProjectNode(
              idGen.next(), Collections.singletonList(windowNode), outVeloxNames, projExprs);
    } else {
      // No cast project — WindowNode's output directly is (source flat cols) + (window cols).
      if (sourceVeloxType != null) {
        outVeloxNames.addAll(sourceVeloxType.getNames());
        outVeloxTypes.addAll(sourceVeloxType.getChildren());
      }
      for (int i = 0; i < group.aggCalls.size(); i++) {
        RelDataType calciteType =
            window.getRowType().getFieldList().get(outputFieldOffset + i).getType();
        outVeloxNames.add(windowColumnNames.get(i));
        outVeloxTypes.add(VeloxTypeConverter.toVeloxType(calciteType));
      }
    }

    // Rebuild field mappings for Window's Calcite output = inputRowType + windowColumnNames.
    // For input fields: carry over the source's mapping (preserves -1 for MAP/metadata).
    // For window columns: map to their newly-emitted Velox index.
    List<RelDataTypeField> windowOutputFields = window.getRowType().getFieldList();
    FieldMapping[] newMappings = new FieldMapping[windowOutputFields.size()];
    // Build a name->veloxIndex lookup for the Velox output.
    Map<String, Integer> veloxNameToIdx = new LinkedHashMap<>();
    for (int i = 0; i < outVeloxNames.size(); i++) {
      veloxNameToIdx.put(outVeloxNames.get(i), i);
    }
    for (int i = 0; i < inputRowType.getFieldCount(); i++) {
      String fieldName = inputRowType.getFieldList().get(i).getName();
      if (sourceMappings != null && i < sourceMappings.length && sourceMappings[i] != null) {
        // Carry over source mapping (keeps veloxIndex=-1 for dropped MAP/metadata columns).
        newMappings[i] = sourceMappings[i];
      } else {
        Integer vIdx = veloxNameToIdx.get(fieldName);
        newMappings[i] = new FieldMapping(vIdx != null ? vIdx : -1, fieldName, null);
      }
    }
    for (int i = 0; i < group.aggCalls.size(); i++) {
      int calciteIdx = outputFieldOffset + i;
      String colName = windowColumnNames.get(i);
      Integer vIdx = veloxNameToIdx.get(colName);
      newMappings[calciteIdx] = new FieldMapping(vIdx != null ? vIdx : -1, colName, null);
    }
    currentFieldMappings = newMappings;
    currentVeloxOutputType = new RowType(outVeloxNames, outVeloxTypes);
    return result;
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
