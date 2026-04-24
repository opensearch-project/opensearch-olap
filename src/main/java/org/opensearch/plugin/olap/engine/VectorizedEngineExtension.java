/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.engine;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.plugin.olap.plan.convert.VeloxExprConverter;
import org.opensearch.plugin.olap.plan.convert.VeloxTypeConverter;
import org.opensearch.plugin.olap.plan.physical.DateTimeUdfRewriter;
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

  /**
   * Window aggregate functions we know Velox handles through {@link
   * org.opensearch.plugin.olap.plan.physical.VeloxPlanGenerator#convertWindow}. Ranking functions
   * (row_number/rank/dense_rank) are type-narrowed to Velox's INTEGER registration and cast back to
   * Calcite's BIGINT; sum/count/avg/min/max reuse the same aggregate registrations as regular
   * AggregationNode. Unlisted window functions (lag, lead, first_value, last_value, ntile,
   * nth_value, etc.) fall back to the default engine until explicitly verified — ntile and
   * nth_value in particular have offset-argument type mismatches (Spark registers them with INTEGER
   * offsets while Calcite may declare BIGINT), so whitelisting them without an argument coercion
   * would fail Velox signature resolution instead of falling back cleanly.
   */
  private static final Set<String> SUPPORTED_WINDOW_FUNCTIONS =
      Set.of("ROW_NUMBER", "RANK", "DENSE_RANK", "SUM", "SUM0", "COUNT", "AVG", "MIN", "MAX");

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

    // Check field types in scan nodes for unsupported types.
    // MAP<VARCHAR, ANY> (OpenSearch object fields) are accepted — they are converted to ROW types
    // in VeloxPlanGenerator.convertTableScan() using the dot-path sibling columns.
    // Dot-path child columns (e.g., cloud.region) are also accepted (they feed into the ROW).
    if (node instanceof LogicalTableScan
        || node.getClass().getSimpleName().contains("TableScan")
        || node.getClass().getSimpleName().contains("IndexScan")) {
      for (RelDataTypeField field : node.getRowType().getFieldList()) {
        SqlTypeName typeName = field.getType().getSqlTypeName();
        // MAP parents (OpenSearch object fields) are accepted — handled by flat scan
        // + struct reconstruction in Java result conversion.
        if (typeName == SqlTypeName.MAP) {
          continue;
        }
        // Top-level ANY fields (match_only_text, empty objects, unresolved fields) cannot be
        // read from OpenSearch doc values. Reject so the query falls back to the default engine.
        if (typeName == SqlTypeName.ANY) {
          return "type:ANY in field " + field.getName();
        }
        if (!VeloxTypeConverter.isSupported(field.getType())) {
          return "type:" + typeName + " in field " + field.getName();
        }
      }
    }

    // Check expressions in Filter, Project, and Join for unsupported functions
    String unsupportedFunc = findUnsupportedFunction(node);
    if (unsupportedFunc != null) {
      return unsupportedFunc;
    }

    // LogicalWindow carries window aggregate calls (RexWinAggCall) that aren't visited by the
    // regular RexCall walker above — inspect each group's aggCalls explicitly.
    if (node instanceof LogicalWindow) {
      LogicalWindow window = (LogicalWindow) node;
      for (org.apache.calcite.rel.core.Window.Group group : window.groups) {
        for (org.apache.calcite.rel.core.Window.RexWinAggCall aggCall : group.aggCalls) {
          String name = aggCall.getOperator().getName().toUpperCase(Locale.ROOT);
          if (!SUPPORTED_WINDOW_FUNCTIONS.contains(name)) {
            return "window_function:" + name;
          }
        }
      }
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
      // PPL's timestamp()/date()/time() UDFs are stripped by DateTimeUdfRewriter before Velox
      // conversion when they wrap a string literal or a matching UDT ref.
      if (DateTimeUdfRewriter.isRewritable(call)) {
        return null;
      }
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
