/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.plugin.olap.engine;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.transport.TransportService;

import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.pagination.Cursor;

import org.opensearch.plugin.olap.common.QueryId;
import org.opensearch.plugin.olap.execution.VeloxLifecycleService;
import org.opensearch.plugin.olap.plan.convert.VeloxPlanConverter;
import org.opensearch.plugin.olap.plan.fragment.PlanFragment;
import org.opensearch.plugin.olap.plan.fragment.PlanFragmenter;
import org.opensearch.plugin.olap.scheduler.ExecutionPolicy;
import org.opensearch.plugin.olap.scheduler.QueryExecution;
import org.opensearch.plugin.olap.scheduler.QueryScheduler;
import org.opensearch.plugin.olap.transport.ExecuteFragmentResponse;
import org.opensearch.plugin.olap.transport.NodeResultCollector;

import org.boostscale.velox4j.plan.PlanNode;

/**
 * Velox-based execution engine that integrates with the SQL plugin's execution pipeline.
 *
 * <p>The SQL plugin handles SQL/PPL to Calcite RelNode conversion, then delegates execution
 * to this engine which:
 * <ol>
 *   <li>Converts Calcite RelNode to Velox PlanNode via {@link VeloxPlanConverter}</li>
 *   <li>Fragments the plan for distributed execution via {@link PlanFragmenter}</li>
 *   <li>Schedules fragments across data nodes via {@link QueryScheduler}</li>
 *   <li>Collects Arrow IPC results and converts to the SQL plugin's QueryResponse format</li>
 * </ol>
 */
public class VeloxExecutionEngine {

    private static final Logger logger = LogManager.getLogger(VeloxExecutionEngine.class);

    private final VeloxPlanConverter planConverter;
    private final PlanFragmenter planFragmenter;
    private final QueryScheduler queryScheduler;
    private final VeloxLifecycleService veloxLifecycle;
    private final TransportService transportService;

    public VeloxExecutionEngine(
        VeloxLifecycleService veloxLifecycle,
        QueryScheduler queryScheduler,
        TransportService transportService
    ) {
        this.veloxLifecycle = veloxLifecycle;
        this.planConverter = new VeloxPlanConverter();
        this.planFragmenter = new PlanFragmenter();
        this.queryScheduler = queryScheduler;
        this.transportService = transportService;
    }

    /**
     * Execute a Calcite RelNode plan through the Velox engine.
     *
     * @param relNode the Calcite logical plan from the SQL plugin's analyzer
     * @return QueryResponse in the SQL plugin's format
     */
    public ExecutionEngine.QueryResponse execute(RelNode relNode) {
        QueryId queryId = QueryId.generate();
        logger.info("Executing query {} via Velox engine", queryId);

        try {
            // Step 1: Convert Calcite RelNode to Velox PlanNode
            PlanNode veloxPlan = planConverter.convert(relNode);

            // Step 2: Extract source index name from the plan
            String sourceIndex = extractSourceIndex(relNode);

            // Step 3: Fragment the plan for distributed execution
            List<PlanFragment> fragments = planFragmenter.fragment(veloxPlan, sourceIndex);

            // Step 4: Schedule and execute fragments across data nodes
            QueryExecution execution = queryScheduler.schedule(fragments, ExecutionPolicy.PHASED);

            // Step 5: Collect results from all nodes
            NodeResultCollector collector = new NodeResultCollector(transportService, queryScheduler);
            List<ExecuteFragmentResponse> responses = collector.dispatchAndCollect(execution);

            // Step 6: Convert Arrow IPC results to SQL plugin's QueryResponse
            return buildQueryResponse(relNode.getRowType(), responses);

        } catch (Exception e) {
            logger.error("Velox execution failed for query {}", queryId, e);
            throw new RuntimeException("Velox execution failed: " + e.getMessage(), e);
        }
    }

    public boolean isAvailable() {
        return veloxLifecycle.isEnabled();
    }

    private String extractSourceIndex(RelNode relNode) {
        if (relNode instanceof LogicalTableScan) {
            return String.join(".", ((LogicalTableScan) relNode).getTable().getQualifiedName());
        }
        for (RelNode input : relNode.getInputs()) {
            String index = extractSourceIndex(input);
            if (index != null) {
                return index;
            }
        }
        return null;
    }

    private ExecutionEngine.QueryResponse buildQueryResponse(
        RelDataType rowType,
        List<ExecuteFragmentResponse> responses
    ) {
        List<ExecutionEngine.Schema.Column> columns = new ArrayList<>();
        for (RelDataTypeField field : rowType.getFieldList()) {
            ExprType exprType = mapToExprType(field.getType().getSqlTypeName());
            columns.add(new ExecutionEngine.Schema.Column(field.getName(), null, exprType));
        }
        ExecutionEngine.Schema schema = new ExecutionEngine.Schema(columns);

        List<ExprValue> results = new ArrayList<>();
        for (ExecuteFragmentResponse response : responses) {
            if (response.getStatus() != ExecuteFragmentResponse.Status.SUCCESS) {
                throw new RuntimeException(
                    "Fragment execution failed: " + response.getErrorMessage());
            }
            byte[] arrowData = response.getResultData();
            if (arrowData != null && arrowData.length > 0) {
                results.addAll(readArrowIpcToExprValues(arrowData));
            }
        }

        return new ExecutionEngine.QueryResponse(schema, results, Cursor.None);
    }

    private List<ExprValue> readArrowIpcToExprValues(byte[] arrowIpc) {
        List<ExprValue> rows = new ArrayList<>();
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

        try (ArrowStreamReader reader = new ArrowStreamReader(
            new ByteArrayInputStream(arrowIpc), allocator
        )) {
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                List<Field> fields = root.getSchema().getFields();
                int rowCount = root.getRowCount();

                for (int row = 0; row < rowCount; row++) {
                    LinkedHashMap<String, Object> tupleValues = new LinkedHashMap<>();
                    for (int col = 0; col < fields.size(); col++) {
                        String name = fields.get(col).getName();
                        Object value = root.getVector(col).getObject(row);
                        tupleValues.put(name, value);
                    }
                    rows.add(ExprValueUtils.tupleValue(tupleValues));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read Arrow IPC result", e);
        } finally {
            allocator.close();
        }
        return rows;
    }

    private ExprType mapToExprType(SqlTypeName typeName) {
        switch (typeName) {
            case BOOLEAN:
                return ExprCoreType.BOOLEAN;
            case TINYINT:
                return ExprCoreType.BYTE;
            case SMALLINT:
                return ExprCoreType.SHORT;
            case INTEGER:
                return ExprCoreType.INTEGER;
            case BIGINT:
                return ExprCoreType.LONG;
            case FLOAT:
            case REAL:
                return ExprCoreType.FLOAT;
            case DOUBLE:
            case DECIMAL:
                return ExprCoreType.DOUBLE;
            case CHAR:
            case VARCHAR:
                return ExprCoreType.STRING;
            case DATE:
                return ExprCoreType.DATE;
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return ExprCoreType.TIMESTAMP;
            default:
                return ExprCoreType.UNKNOWN;
        }
    }
}
