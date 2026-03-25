/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.plugin.olap.engine;

import java.util.HashSet;
import java.util.Set;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionContext;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.planner.physical.PhysicalPlan;

/**
 * Implementation of the SQL plugin's {@link ExecutionEngine} extension point.
 *
 * <p>This class is discovered by the SQL plugin via SPI when the OLAP plugin is installed.
 * It overrides {@link #canVectorize(RelNode)} to advertise support for specific Calcite plan
 * shapes, and delegates actual execution to the {@link VeloxExecutionEngine}.
 *
 * <p>The non-Calcite methods ({@code execute(PhysicalPlan, ...)}, {@code explain(...)})
 * are never called on extension engines — they throw {@link UnsupportedOperationException}.
 */
public class OlapExecutionExtensionImpl implements ExecutionEngine {

    private static final Logger logger = LogManager.getLogger(OlapExecutionExtensionImpl.class);

    private static final Set<Class<? extends RelNode>> SUPPORTED_REL_NODES = new HashSet<>();

    static {
        SUPPORTED_REL_NODES.add(LogicalTableScan.class);
        SUPPORTED_REL_NODES.add(LogicalFilter.class);
        SUPPORTED_REL_NODES.add(LogicalProject.class);
        SUPPORTED_REL_NODES.add(LogicalAggregate.class);
        SUPPORTED_REL_NODES.add(LogicalJoin.class);
        SUPPORTED_REL_NODES.add(LogicalSort.class);
    }

    // Set by OlapPlugin during initialization
    private static volatile VeloxExecutionEngine veloxEngine;

    /**
     * Called by OlapPlugin to wire the execution engine after components are created.
     */
    public static void setEngine(VeloxExecutionEngine executionEngine) {
        veloxEngine = executionEngine;
    }

    @Override
    public boolean canVectorize(RelNode plan) {
        if (veloxEngine == null || !veloxEngine.isAvailable()) {
            return false;
        }
        return checkSupported(plan);
    }

    @Override
    public void execute(
        RelNode plan,
        CalcitePlanContext context,
        ResponseListener<QueryResponse> listener
    ) {
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
        throw new UnsupportedOperationException("OLAP extension does not support PhysicalPlan execution");
    }

    @Override
    public void execute(
        PhysicalPlan plan, ExecutionContext context, ResponseListener<QueryResponse> listener
    ) {
        throw new UnsupportedOperationException("OLAP extension does not support PhysicalPlan execution");
    }

    @Override
    public void explain(PhysicalPlan plan, ResponseListener<ExplainResponse> listener) {
        throw new UnsupportedOperationException("OLAP extension does not support PhysicalPlan explain");
    }

    /**
     * Recursively checks if all RelNode operators in the plan tree are supported by Velox.
     */
    private boolean checkSupported(RelNode node) {
        if (!SUPPORTED_REL_NODES.contains(node.getClass())) {
            logger.debug("Unsupported RelNode type for Velox: {}", node.getClass().getSimpleName());
            return false;
        }
        for (RelNode input : node.getInputs()) {
            if (!checkSupported(input)) {
                return false;
            }
        }
        return true;
    }
}
