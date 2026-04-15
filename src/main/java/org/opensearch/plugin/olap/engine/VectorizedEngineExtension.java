/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.engine;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.plugin.olap.plan.convert.VeloxExprConverter;
import org.opensearch.plugin.olap.plan.convert.VeloxTypeConverter;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit;
import org.opensearch.sql.calcite.plan.rel.OpenSearchTableScan;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionContext;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalIndexScan;
import org.opensearch.sql.planner.physical.PhysicalPlan;

/**
 * Implementation of the SQL plugin's {@link ExecutionEngine} extension point.
 *
 * <p>This class is discovered by the SQL plugin via SPI when the OLAP plugin is installed. It
 * overrides {@link #canVectorize(RelNode)} to advertise support for specific Calcite plan shapes,
 * and delegates actual execution to the {@link VeloxExecutionEngine}.
 *
 * <p>The non-Calcite methods ({@code execute(PhysicalPlan, ...)}, {@code explain(...)}) are never
 * called on extension engines — they throw {@link UnsupportedOperationException}.
 */
public class VectorizedEngineExtension implements ExecutionEngine {

  private static final Logger logger = LogManager.getLogger(VectorizedEngineExtension.class);

  private static final Set<Class<? extends RelNode>> SUPPORTED_REL_NODES = new HashSet<>();

  static {
    SUPPORTED_REL_NODES.add(LogicalTableScan.class);
    SUPPORTED_REL_NODES.add(OpenSearchTableScan.class);
    SUPPORTED_REL_NODES.add(CalciteLogicalIndexScan.class);
    SUPPORTED_REL_NODES.add(LogicalFilter.class);
    SUPPORTED_REL_NODES.add(LogicalProject.class);
    SUPPORTED_REL_NODES.add(LogicalAggregate.class);
    SUPPORTED_REL_NODES.add(LogicalJoin.class);
    SUPPORTED_REL_NODES.add(LogicalSort.class);
    SUPPORTED_REL_NODES.add(LogicalSystemLimit.class);
    SUPPORTED_REL_NODES.add(LogicalWindow.class);
  }

  // Set by OlapPlugin during initialization
  private static volatile VeloxExecutionEngine veloxEngine;

  /** Called by OlapPlugin to wire the execution engine after components are created. */
  public static void setEngine(VeloxExecutionEngine executionEngine) {
    veloxEngine = executionEngine;
  }

  @Override
  public boolean canVectorize(RelNode plan) {
    if (veloxEngine == null || !veloxEngine.isAvailable()) {
      return false;
    }
    // force_vectorize=true bypasses all checks — for integration testing to verify Velox coverage.
    // Queries with unsupported types/functions will fail explicitly instead of falling back.
    if (veloxEngine.isForceVectorize()) {
      return true;
    }
    String unsupportedNode = findUnsupportedNode(plan);
    if (unsupportedNode != null) {
      logger.info(
          "Cannot vectorize plan: unsupported node [{}] in {}", unsupportedNode, plan.getDigest());
      return false;
    }
    return true;
  }

  @Override
  public void execute(
      RelNode plan, CalcitePlanContext context, ResponseListener<QueryResponse> listener) {
    if (veloxEngine == null) {
      listener.onFailure(new IllegalStateException("VeloxExecutionEngine not initialized"));
      return;
    }
    try {
      QueryResponse response = veloxEngine.execute(plan);
      listener.onResponse(response);
    } catch (Exception e) {
      logger.error("Velox execution failed", e);
      listener.onFailure(e);
    }
  }

  @Override
  public void execute(PhysicalPlan plan, ResponseListener<QueryResponse> listener) {
    listener.onFailure(
        new UnsupportedOperationException(
            "Vectorized engine extension does not support PhysicalPlan execution"));
  }

  @Override
  public void execute(
      PhysicalPlan plan, ExecutionContext context, ResponseListener<QueryResponse> listener) {
    listener.onFailure(
        new UnsupportedOperationException(
            "Vectorized engine extension does not support PhysicalPlan execution"));
  }

  @Override
  public void explain(
      RelNode plan,
      ExplainMode mode,
      CalcitePlanContext context,
      ResponseListener<ExplainResponse> listener) {
    if (veloxEngine == null) {
      listener.onFailure(new IllegalStateException("VeloxExecutionEngine not initialized"));
      return;
    }
    try {
      ExplainResponse response = veloxEngine.explain(plan);
      listener.onResponse(response);
    } catch (Exception e) {
      logger.error("Velox explain failed", e);
      listener.onFailure(e);
    }
  }

  @Override
  public void explain(PhysicalPlan plan, ResponseListener<ExplainResponse> listener) {
    listener.onFailure(
        new UnsupportedOperationException(
            "Vectorized engine extension does not support PhysicalPlan explain"));
  }

  /**
   * Recursively finds the first unsupported RelNode or RexCall function in the plan tree. Returns a
   * description of the unsupported element, or null if all elements are supported.
   */
  private String findUnsupportedNode(RelNode node) {
    if (!SUPPORTED_REL_NODES.contains(node.getClass())) {
      return node.getClass().getName();
    }

    // Check field types in scan nodes for unsupported types (e.g. ANY from match_only_text)
    if (node instanceof LogicalTableScan
        || node.getClass().getSimpleName().contains("TableScan")
        || node.getClass().getSimpleName().contains("IndexScan")) {
      for (RelDataTypeField field : node.getRowType().getFieldList()) {
        if (!VeloxTypeConverter.isSupported(field.getType())) {
          return "type:" + field.getType().getSqlTypeName() + " in field " + field.getName();
        }
      }
    }

    // Check expressions in Filter, Project, and Join for unsupported functions
    String unsupportedFunc = findUnsupportedFunction(node);
    if (unsupportedFunc != null) {
      return unsupportedFunc;
    }

    for (RelNode input : node.getInputs()) {
      String unsupported = findUnsupportedNode(input);
      if (unsupported != null) {
        return unsupported;
      }
    }
    return null;
  }

  /**
   * Check expressions in a RelNode for unsupported functions. Returns the function name if
   * unsupported, null if all functions are supported.
   */
  private String findUnsupportedFunction(RelNode node) {
    List<RexNode> expressions = null;
    if (node instanceof LogicalFilter) {
      expressions = List.of(((LogicalFilter) node).getCondition());
    } else if (node instanceof LogicalProject) {
      expressions = ((LogicalProject) node).getProjects();
    } else if (node instanceof LogicalJoin) {
      RexNode cond = ((LogicalJoin) node).getCondition();
      if (cond != null) {
        expressions = List.of(cond);
      }
    }
    if (expressions == null) {
      return null;
    }
    for (RexNode expr : expressions) {
      String unsupported = findUnsupportedRexCall(expr);
      if (unsupported != null) {
        return unsupported;
      }
    }
    return null;
  }

  /** Recursively check a RexNode tree for unsupported function calls. */
  private String findUnsupportedRexCall(RexNode node) {
    if (node instanceof RexCall) {
      RexCall call = (RexCall) node;
      if (!VeloxExprConverter.isSupported(call)) {
        return "function:" + call.getOperator().getName();
      }
      for (RexNode operand : call.getOperands()) {
        String unsupported = findUnsupportedRexCall(operand);
        if (unsupported != null) {
          return unsupported;
        }
      }
    }
    return null;
  }
}
