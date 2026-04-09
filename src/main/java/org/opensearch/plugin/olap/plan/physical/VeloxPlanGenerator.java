/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.boostscale.velox4j.aggregate.Aggregate;
import org.boostscale.velox4j.aggregate.AggregateStep;
import org.boostscale.velox4j.connector.ExternalStreamTableHandle;
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
import org.boostscale.velox4j.sort.SortOrder;
import org.boostscale.velox4j.type.RowType;
import org.boostscale.velox4j.type.Type;
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
 * <p>The generator walks the physical plan tree. At each {@link PhysicalExchange} node, it creates a
 * fragment boundary. Within each fragment, it converts physical RelNodes to Velox PlanNodes using
 * the existing converter utilities ({@link VeloxExprConverter}, {@link VeloxAggConverter}, {@link
 * VeloxTypeConverter}).
 *
 * <p>This replaces both the old {@code VeloxPlanConverter} (for Velox PlanNode generation) and
 * {@code PlanFragmenter} (for fragment splitting).
 */
public class VeloxPlanGenerator {

  private static final Logger logger = LogManager.getLogger(VeloxPlanGenerator.class);
  private static final String CONNECTOR_ID = "connector-external-stream";

  private static final java.util.Set<String> METADATA_COLUMNS =
      java.util.Set.of("_id", "_index", "_score", "_maxscore", "_sort", "_routing");

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

    return new ArrayList<>(fragments);
  }

  // ---- Physical -> Velox conversion ----

  private PlanNode toVeloxPlan(RelNode node) {
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
    throw new UnsupportedOperationException(
        "Unsupported physical node: " + node.getClass().getSimpleName());
  }

  /**
   * Handle a PhysicalExchange: the child subtree becomes a separate fragment. Returns null as a
   * placeholder — the parent fragment will wire an ExternalStream scan.
   */
  private PlanNode handleExchange(PhysicalExchange exchange) {
    // Recursively convert the child (the leaf/intermediate fragment)
    PlanNode childPlan = toVeloxPlan(exchange.getInput());

    // Determine fragment properties from the exchange's input distribution
    String sourceIndex = extractSourceIndex(exchange.getInput());
    RelDistribution inputDist =
        exchange
            .getInput()
            .getTraitSet()
            .getTrait(org.apache.calcite.rel.RelDistributionTraitDef.INSTANCE);

    FragmentProperties props;
    if (inputDist != null && inputDist.getType() == RelDistribution.Type.HASH_DISTRIBUTED) {
      // Hash-partitioned fragment (MPP worker)
      List<Integer> keyChannels = inputDist.getKeys();
      props = FragmentProperties.shuffleScan(sourceIndex, null, keyChannels, 0);
    } else {
      // Source fragment (data node scan)
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
    java.util.Map<String, String> rightRenames = new java.util.LinkedHashMap<>();
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
    PlanNode source = toVeloxPlan(sort.getInput());
    if (source == null) {
      source = createExchangeScan(sort.getInput().getRowType());
    }
    RelDataType inputRowType = sort.getInput().getRowType();

    if (sort.getCollation() != null && !sort.getCollation().getFieldCollations().isEmpty()) {
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

      source =
          new OrderByNode(
              orderNodeId, Collections.singletonList(source), sortingKeys, sortingOrders, false);
    }

    if (sort.fetch != null || sort.offset != null) {
      String limitNodeId = idGen.next();
      long offset = sort.offset != null ? RexLiteral.intValue(sort.offset) : 0;
      long count = sort.fetch != null ? RexLiteral.intValue(sort.fetch) : Long.MAX_VALUE;
      source = new LimitNode(limitNodeId, Collections.singletonList(source), offset, count, false);
    }

    return source;
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
      java.util.Map<String, String> rightRenames,
      boolean hasConflict,
      List<FieldAccessTypedExpr> leftKeys,
      List<FieldAccessTypedExpr> rightKeys) {
    if (condition == null) return;
    int leftFieldCount = leftRowType.getFieldCount();

    if (condition instanceof org.apache.calcite.rex.RexCall) {
      org.apache.calcite.rex.RexCall call = (org.apache.calcite.rex.RexCall) condition;

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

  private JoinType convertJoinType(org.apache.calcite.rel.core.JoinRelType calciteType) {
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
