/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.convert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
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

/**
 * Converts a Calcite RelNode tree to a velox4j PlanNode tree.
 *
 * <p>This is the central plan conversion component. It walks the Calcite logical plan top-down and
 * constructs the corresponding velox4j plan nodes with proper expression and type conversions.
 *
 * <p>Supported Calcite nodes:
 *
 * <ul>
 *   <li>LogicalTableScan → TableScanNode (with ExternalStreamTableHandle)
 *   <li>LogicalFilter → FilterNode
 *   <li>LogicalProject → ProjectNode
 *   <li>LogicalAggregate → AggregationNode
 *   <li>LogicalJoin → HashJoinNode
 *   <li>LogicalSort → OrderByNode + LimitNode
 * </ul>
 */
public class VeloxPlanConverter {

  private static final String EXTERNAL_STREAM_CONNECTOR_ID = "connector-external-stream";

  private final PlanIdGenerator idGenerator;

  public VeloxPlanConverter() {
    this.idGenerator = new PlanIdGenerator();
  }

  /** Convert a Calcite RelNode tree to a velox4j PlanNode tree. */
  public PlanNode convert(RelNode relNode) {
    idGenerator.reset();
    return visitNode(relNode);
  }

  private PlanNode visitNode(RelNode relNode) {
    if (relNode instanceof TableScan) {
      return visitTableScan((TableScan) relNode);
    } else if (relNode instanceof LogicalFilter) {
      return visitFilter((LogicalFilter) relNode);
    } else if (relNode instanceof LogicalProject) {
      return visitProject((LogicalProject) relNode);
    } else if (relNode instanceof LogicalAggregate) {
      return visitAggregate((LogicalAggregate) relNode);
    } else if (relNode instanceof LogicalJoin) {
      return visitJoin((LogicalJoin) relNode);
    } else if (relNode instanceof Sort) {
      // Handles both LogicalSort and LogicalSystemLimit (system-imposed row limit)
      return visitSort((Sort) relNode);
    }
    throw new UnsupportedOperationException(
        "Unsupported RelNode type: " + relNode.getClass().getSimpleName());
  }

  private PlanNode visitTableScan(TableScan scan) {
    String nodeId = idGenerator.next();
    RelDataType rowType = scan.getRowType();
    // TODO: The scan's rowType includes metadata columns (_id, _index, _score, _maxscore, _sort,
    //  _routing) from CalciteLogicalIndexScan that the query may not need. Consider trimming
    //  outputType to only the columns actually referenced by upstream operators (Project/Filter)
    //  to reduce data flowing through the Lucene → Arrow → ExternalStream pipeline.
    RowType outputType = VeloxTypeConverter.toVeloxRowType(rowType);

    ExternalStreamTableHandle tableHandle =
        new ExternalStreamTableHandle(EXTERNAL_STREAM_CONNECTOR_ID);

    // ExternalStream is a pass-through connector — it reads pre-formed RowVectors from a
    // BlockingQueue without column projection. The C++ side enforces empty assignments:
    //   VELOX_CHECK(columnHandles.empty(), "ExternalStreamConnector doesn't accept column handles")
    // The schema is defined solely by outputType.
    return new TableScanNode(nodeId, outputType, tableHandle, Collections.emptyList());
  }

  private PlanNode visitFilter(LogicalFilter filter) {
    String nodeId = idGenerator.next();
    PlanNode source = visitNode(filter.getInput());

    VeloxExprConverter exprConverter = new VeloxExprConverter(filter.getInput().getRowType());
    TypedExpr filterExpr = exprConverter.convert(filter.getCondition());

    return new FilterNode(nodeId, Collections.singletonList(source), filterExpr);
  }

  private PlanNode visitProject(LogicalProject project) {
    String nodeId = idGenerator.next();
    PlanNode source = visitNode(project.getInput());

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

  private PlanNode visitAggregate(LogicalAggregate agg) {
    String nodeId = idGenerator.next();
    PlanNode source = visitNode(agg.getInput());

    RelDataType inputRowType = agg.getInput().getRowType();
    VeloxAggConverter aggConverter = new VeloxAggConverter(inputRowType);

    // Convert grouping keys
    ImmutableBitSet groupSet = agg.getGroupSet();
    List<FieldAccessTypedExpr> groupingKeys = new ArrayList<>();
    for (int fieldIndex : groupSet) {
      RelDataTypeField field = inputRowType.getFieldList().get(fieldIndex);
      Type veloxType = VeloxTypeConverter.toVeloxType(field.getType());
      groupingKeys.add(FieldAccessTypedExpr.create(veloxType, field.getName()));
    }

    // Convert aggregate functions
    List<String> aggregateNames = new ArrayList<>();
    List<Aggregate> aggregates = new ArrayList<>();
    List<AggregateCall> aggCalls = agg.getAggCallList();
    for (int i = 0; i < aggCalls.size(); i++) {
      AggregateCall aggCall = aggCalls.get(i);
      aggregateNames.add(aggConverter.resolveAggOutputName(aggCall, i));
      aggregates.add(aggConverter.convert(aggCall));
    }

    // Use SINGLE step for non-distributed execution.
    // The PlanFragmenter will split this into PARTIAL + FINAL for distributed plans.
    return new AggregationNode(
        nodeId,
        AggregateStep.SINGLE,
        groupingKeys,
        Collections.emptyList(), // preGroupedKeys
        aggregateNames,
        aggregates,
        false, // ignoreNullKeys
        false, // noGroupsSpanBatches
        Collections.singletonList(source),
        null, // groupId
        Collections.emptyList() // globalGroupingSets
        );
  }

  private PlanNode visitJoin(LogicalJoin join) {
    String nodeId = idGenerator.next();
    PlanNode left = visitNode(join.getLeft());
    PlanNode right = visitNode(join.getRight());

    JoinType veloxJoinType = convertJoinType(join.getJoinType());
    RowType outputType = VeloxTypeConverter.toVeloxRowType(join.getRowType());

    // Extract join keys from the condition.
    // For equi-joins, the condition is typically AND(EQ(left.col, right.col), ...).
    JoinKeyExtractor keyExtractor =
        new JoinKeyExtractor(
            join.getLeft().getRowType(), join.getRight().getRowType(), join.getCondition());

    return new HashJoinNode(
        nodeId,
        veloxJoinType,
        keyExtractor.getLeftKeys(),
        keyExtractor.getRightKeys(),
        keyExtractor.getResidualFilter(),
        left,
        right,
        outputType,
        false, // nullAware
        false // useHashTableCache
        );
  }

  private PlanNode visitSort(Sort sort) {
    PlanNode source = visitNode(sort.getInput());
    RelDataType inputRowType = sort.getInput().getRowType();

    // If there's a collation, create OrderByNode
    if (sort.getCollation() != null && !sort.getCollation().getFieldCollations().isEmpty()) {
      String orderNodeId = idGenerator.next();
      List<FieldAccessTypedExpr> sortingKeys = new ArrayList<>();
      List<SortOrder> sortingOrders = new ArrayList<>();

      for (RelFieldCollation fieldCollation : sort.getCollation().getFieldCollations()) {
        int fieldIndex = fieldCollation.getFieldIndex();
        RelDataTypeField field = inputRowType.getFieldList().get(fieldIndex);
        Type veloxType = VeloxTypeConverter.toVeloxType(field.getType());
        sortingKeys.add(FieldAccessTypedExpr.create(veloxType, field.getName()));

        boolean ascending = fieldCollation.getDirection().isDescending() ? false : true;
        boolean nullsFirst = fieldCollation.nullDirection == RelFieldCollation.NullDirection.FIRST;
        sortingOrders.add(new SortOrder(ascending, nullsFirst));
      }

      source =
          new OrderByNode(
              orderNodeId,
              Collections.singletonList(source),
              sortingKeys,
              sortingOrders,
              false // not partial
              );
    }

    // If there's a LIMIT/OFFSET, wrap with LimitNode
    if (sort.fetch != null || sort.offset != null) {
      String limitNodeId = idGenerator.next();
      long offset = sort.offset != null ? RexLiteral.intValue(sort.offset) : 0;
      long count = sort.fetch != null ? RexLiteral.intValue(sort.fetch) : Long.MAX_VALUE;

      source =
          new LimitNode(
              limitNodeId, Collections.singletonList(source), offset, count, false // not partial
              );
    }

    return source;
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

  /**
   * Extracts equi-join keys from a join condition. Decomposes the condition into left keys, right
   * keys, and residual filter.
   */
  private static class JoinKeyExtractor {
    private final List<FieldAccessTypedExpr> leftKeys = new ArrayList<>();
    private final List<FieldAccessTypedExpr> rightKeys = new ArrayList<>();
    private TypedExpr residualFilter;

    JoinKeyExtractor(RelDataType leftRowType, RelDataType rightRowType, RexNode condition) {
      extract(leftRowType, rightRowType, condition);
    }

    List<FieldAccessTypedExpr> getLeftKeys() {
      return leftKeys;
    }

    List<FieldAccessTypedExpr> getRightKeys() {
      return rightKeys;
    }

    TypedExpr getResidualFilter() {
      return residualFilter;
    }

    private void extract(RelDataType leftRowType, RelDataType rightRowType, RexNode condition) {
      if (condition == null) {
        return;
      }

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
              // Both from same side - treat as residual
              VeloxExprConverter converter =
                  new VeloxExprConverter(mergeRowTypes(leftRowType, rightRowType));
              this.residualFilter = converter.convert(condition);
              return;
            }

            RelDataTypeField leftField = leftRowType.getFieldList().get(leftRef.getIndex());
            Type leftType = VeloxTypeConverter.toVeloxType(leftField.getType());
            leftKeys.add(FieldAccessTypedExpr.create(leftType, leftField.getName()));

            int rightIndex = rightRef.getIndex() - leftFieldCount;
            RelDataTypeField rightField = rightRowType.getFieldList().get(rightIndex);
            Type rightType = VeloxTypeConverter.toVeloxType(rightField.getType());
            rightKeys.add(FieldAccessTypedExpr.create(rightType, rightField.getName()));
            return;
          }
        }

        if (call.getKind() == SqlKind.AND) {
          for (RexNode operand : call.getOperands()) {
            JoinKeyExtractor sub = new JoinKeyExtractor(leftRowType, rightRowType, operand);
            leftKeys.addAll(sub.leftKeys);
            rightKeys.addAll(sub.rightKeys);
            if (sub.residualFilter != null) {
              // TODO: combine residual filters with AND
              this.residualFilter = sub.residualFilter;
            }
          }
          return;
        }
      }

      // Non-equi condition → residual filter
      VeloxExprConverter converter =
          new VeloxExprConverter(mergeRowTypes(leftRowType, rightRowType));
      this.residualFilter = converter.convert(condition);
    }

    private RelDataType mergeRowTypes(RelDataType left, RelDataType right) {
      // Create a combined row type for expression conversion
      // The join's input is [left fields | right fields]
      org.apache.calcite.rel.type.RelDataTypeFactory.Builder builder =
          new org.apache.calcite.jdbc.JavaTypeFactoryImpl().builder();
      for (RelDataTypeField f : left.getFieldList()) {
        builder.add(f.getName(), f.getType());
      }
      for (RelDataTypeField f : right.getFieldList()) {
        builder.add(f.getName(), f.getType());
      }
      return builder.build();
    }
  }
}
