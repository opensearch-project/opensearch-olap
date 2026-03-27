/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.engine;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.boostscale.velox4j.config.Config;
import org.boostscale.velox4j.config.ConnectorConfig;
import org.boostscale.velox4j.connector.ExternalStreamConnectorSplit;
import org.boostscale.velox4j.connector.ExternalStreams.BlockingQueue;
import org.boostscale.velox4j.data.RowVector;
import org.boostscale.velox4j.iterator.CloseableIterator;
import org.boostscale.velox4j.iterator.UpIterators;
import org.boostscale.velox4j.plan.PlanNode;
import org.boostscale.velox4j.plan.TableScanNode;
import org.boostscale.velox4j.query.Query;
import org.boostscale.velox4j.query.SerialTask;
import org.boostscale.velox4j.serde.Serde;
import org.boostscale.velox4j.session.Session;
import org.opensearch.plugin.olap.common.QueryId;
import org.opensearch.plugin.olap.execution.VeloxLifecycleService;
import org.opensearch.plugin.olap.plan.convert.VeloxPlanConverter;
import org.opensearch.plugin.olap.plan.fragment.PlanFragment;
import org.opensearch.plugin.olap.plan.fragment.PlanFragmenter;
import org.opensearch.plugin.olap.scheduler.ExecutionPolicy;
import org.opensearch.plugin.olap.scheduler.QueryExecution;
import org.opensearch.plugin.olap.scheduler.QueryScheduler;
import org.opensearch.plugin.olap.scheduler.Stage;
import org.opensearch.plugin.olap.transport.ExecuteFragmentResponse;
import org.opensearch.plugin.olap.transport.NodeResultCollector;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.transport.TransportService;

/**
 * Velox-based execution engine that integrates with the SQL plugin's execution pipeline.
 *
 * <p>The SQL plugin handles SQL/PPL to Calcite RelNode conversion, then delegates execution to this
 * engine which:
 *
 * <ol>
 *   <li>Converts Calcite RelNode to Velox PlanNode via {@link VeloxPlanConverter}
 *   <li>Fragments the plan for distributed execution via {@link PlanFragmenter}
 *   <li>Schedules fragments across data nodes via {@link QueryScheduler}
 *   <li>Collects Arrow IPC results and converts to the SQL plugin's QueryResponse format
 * </ol>
 */
public class VeloxExecutionEngine {

  private static final Logger logger = LogManager.getLogger(VeloxExecutionEngine.class);

  private final VeloxPlanConverter planConverter;
  private final PlanFragmenter planFragmenter;
  private final QueryScheduler queryScheduler;
  private final VeloxLifecycleService veloxLifecycle;
  private volatile TransportService transportService;

  public VeloxExecutionEngine(
      VeloxLifecycleService veloxLifecycle,
      QueryScheduler queryScheduler,
      TransportService transportService) {
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

      // Step 4: Schedule fragments
      QueryExecution execution = queryScheduler.schedule(fragments, ExecutionPolicy.PHASED);
      NodeResultCollector collector = new NodeResultCollector(transportService, queryScheduler);

      // Step 5: Phased execution
      // Separate leaf stages (SOURCE on data nodes) from coordinator stages (FINAL agg)
      List<Stage> leafStages = execution.getLeafStages();
      List<PlanFragment> coordinatorFragments = new ArrayList<>();
      for (PlanFragment f : fragments) {
        if (f.isRoot() && !f.isLeaf()) {
          coordinatorFragments.add(f);
        }
      }

      // Phase 1: Dispatch leaf stages to data nodes and collect partial results
      List<ExecuteFragmentResponse> leafResponses =
          collector.dispatchAndCollect(execution, leafStages);

      List<ExecuteFragmentResponse> finalResponses;
      if (coordinatorFragments.isEmpty()) {
        // Single-stage plan (no aggregation split) — leaf results are the final results
        finalResponses = leafResponses;
      } else {
        // Phase 2: Execute coordinator fragment locally through Velox,
        // feeding partial results from data nodes into an ExternalStream
        logger.info(
            "Executing coordinator fragment for query {} with {} partial results",
            queryId,
            leafResponses.size());
        finalResponses = executeCoordinatorFragment(coordinatorFragments.get(0), leafResponses);
      }

      // Step 6: Convert Arrow IPC results to SQL plugin's QueryResponse
      return buildQueryResponse(relNode.getRowType(), finalResponses);

    } catch (Exception e) {
      logger.error("Velox execution failed for query {}", queryId, e);
      throw new RuntimeException("Velox execution failed: " + e.getMessage(), e);
    }
  }

  /**
   * Execute a coordinator fragment (e.g. FINAL aggregation) locally through Velox. Partial results
   * from data nodes (in Velox native serialization format) are deserialized and fed into an
   * ExternalStream. The coordinator plan reads from it. This preserves Velox's intermediate
   * accumulator state (e.g., avg's {sum, count} pair) which would be lost in an Arrow round-trip.
   */
  private List<ExecuteFragmentResponse> executeCoordinatorFragment(
      PlanFragment coordinatorFragment, List<ExecuteFragmentResponse> partialResponses) {
    Session session = veloxLifecycle.getSession();
    String connectorId = "connector-external-stream";
    org.boostscale.velox4j.data.BaseVectors baseVectorOps = session.baseVectorOps();

    BlockingQueue queue = session.externalStreamOps().newBlockingQueue();
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    try {
      // Get the intermediate output type from the first deserialized partial result
      org.boostscale.velox4j.type.Type intermediateType = null;
      for (ExecuteFragmentResponse response : partialResponses) {
        if (response.hasNativeResults()) {
          byte[] firstBatch = response.getNativeResultBatches().get(0);
          org.boostscale.velox4j.data.BaseVector vec =
              baseVectorOps.deserializeOneFromBuf(firstBatch);
          intermediateType = vec.getType();
          break;
        }
      }

      if (intermediateType == null) {
        logger.warn("No partial results to feed into coordinator fragment");
        return List.of(ExecuteFragmentResponse.success(0, new byte[0]));
      }

      // Build the coordinator plan: wire a TableScanNode as the source of the FINAL agg
      TableScanNode exchangeScan =
          new TableScanNode(
              "exchange_scan",
              intermediateType,
              new org.boostscale.velox4j.connector.ExternalStreamTableHandle(connectorId),
              Collections.emptyList());

      PlanNode coordinatorPlan =
          wireSourceIntoPlan(coordinatorFragment.getPlanRoot(), exchangeScan);

      // Execute through Velox
      ConnectorConfig connectorConfig = ConnectorConfig.create(Map.of(connectorId, Config.empty()));
      Query query = new Query(coordinatorPlan, Config.empty(), connectorConfig);

      logger.info("Executing coordinator plan: {}", Serde.toJson(query));

      SerialTask serialTask = session.queryOps().execute(query);

      // Add split BEFORE starting feeder
      ExternalStreamConnectorSplit split =
          new ExternalStreamConnectorSplit(connectorId, queue.id());
      serialTask.addSplit("exchange_scan", split);
      serialTask.noMoreSplits("exchange_scan");

      // Feeder thread: deserialize native batches and push into queue
      Thread feederThread =
          new Thread(
              () -> {
                try {
                  int batchCount = 0;
                  for (ExecuteFragmentResponse response : partialResponses) {
                    if (response.getStatus() != ExecuteFragmentResponse.Status.SUCCESS) continue;
                    if (!response.hasNativeResults()) continue;
                    for (byte[] nativeBatch : response.getNativeResultBatches()) {
                      org.boostscale.velox4j.data.BaseVector vec =
                          baseVectorOps.deserializeOneFromBuf(nativeBatch);
                      queue.put(vec.asRowVector());
                      batchCount++;
                    }
                  }
                  logger.info("Coordinator feeder: pushed {} native batches", batchCount);
                  queue.noMoreInput();
                } catch (Throwable e) {
                  logger.error("Error feeding partial results to coordinator fragment", e);
                  queue.noMoreInput();
                }
              },
              "olap-coordinator-feeder");
      feederThread.setDaemon(true);
      feederThread.start();

      // Collect results
      logger.info("Coordinator: calling resultIterator.hasNext()...");
      CloseableIterator<RowVector> resultIterator = UpIterators.asJavaIterator(serialTask);
      java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
      org.apache.arrow.vector.ipc.ArrowStreamWriter writer = null;
      boolean hasData = false;

      logger.info("Coordinator: entering result iteration loop");
      // Run the Velox serial task with a timeout to detect deadlocks
      java.util.concurrent.Future<byte[]> resultFuture =
          java.util.concurrent.Executors.newSingleThreadExecutor()
              .submit(
                  () -> {
                    java.io.ByteArrayOutputStream innerBaos = new java.io.ByteArrayOutputStream();
                    org.apache.arrow.vector.ipc.ArrowStreamWriter innerWriter = null;
                    boolean innerHasData = false;
                    try {
                      while (resultIterator.hasNext()) {
                        logger.info("Coordinator: got result batch");
                        RowVector batch = resultIterator.next();
                        if (batch == null) break;

                        VectorSchemaRoot arrowRoot =
                            org.boostscale.velox4j.arrow.Arrow.toArrowVectorSchemaRoot(
                                allocator, batch);
                        if (innerWriter == null) {
                          innerWriter =
                              new org.apache.arrow.vector.ipc.ArrowStreamWriter(
                                  arrowRoot,
                                  null,
                                  java.nio.channels.Channels.newChannel(innerBaos));
                          innerWriter.start();
                        }
                        innerWriter.writeBatch();
                        innerHasData = true;
                        arrowRoot.close();
                      }
                      if (innerWriter != null) {
                        innerWriter.end();
                        innerWriter.close();
                      }
                      resultIterator.close();
                    } catch (Exception e) {
                      throw new RuntimeException("Coordinator Velox execution error", e);
                    }
                    return innerHasData ? innerBaos.toByteArray() : new byte[0];
                  });

      byte[] resultData;
      try {
        resultData = resultFuture.get(30, java.util.concurrent.TimeUnit.SECONDS);
      } catch (java.util.concurrent.TimeoutException e) {
        logger.error(
            "Coordinator fragment execution timed out after 30s. " + "Feeder thread alive={}",
            feederThread.isAlive());
        resultFuture.cancel(true);
        throw new RuntimeException("Coordinator fragment timed out");
      }

      feederThread.join(5_000);
      return List.of(ExecuteFragmentResponse.success(0, resultData));

    } catch (Exception e) {
      throw new RuntimeException("Coordinator fragment execution failed", e);
    } finally {
      try {
        allocator.close();
      } catch (IllegalStateException e) {
        logger.debug("Arrow allocator close warning: {}", e.getMessage());
      }
    }
  }

  /**
   * Wire a source TableScanNode into a plan that has empty sources (e.g. FINAL AggregationNode).
   * Recursively walks the plan tree, finds the AggregationNode with empty sources, and reconstructs
   * the tree with the source wired in.
   */
  private PlanNode wireSourceIntoPlan(PlanNode node, TableScanNode source) {
    if (node instanceof org.boostscale.velox4j.plan.AggregationNode) {
      org.boostscale.velox4j.plan.AggregationNode agg =
          (org.boostscale.velox4j.plan.AggregationNode) node;
      if (agg.getSources().isEmpty()) {
        return new org.boostscale.velox4j.plan.AggregationNode(
            agg.getId(),
            agg.getStep(),
            agg.getGroupingKeys(),
            agg.getPreGroupedKeys(),
            agg.getAggregateNames(),
            agg.getAggregates(),
            agg.isIgnoreNullKeys(),
            agg.isNoGroupsSpanBatches(),
            List.of(source),
            null,
            Collections.emptyList());
      }
    }

    // Recurse into children and reconstruct the node with wired sources
    List<PlanNode> sources = getNodeSources(node);
    if (sources != null && !sources.isEmpty()) {
      List<PlanNode> newSources = new ArrayList<>();
      boolean changed = false;
      for (PlanNode child : sources) {
        PlanNode wired = wireSourceIntoPlan(child, source);
        newSources.add(wired);
        if (wired != child) changed = true;
      }
      if (changed) {
        return reconstructNode(node, newSources);
      }
    }
    return node;
  }

  /** Reconstruct a PlanNode with new sources. */
  private PlanNode reconstructNode(PlanNode node, List<PlanNode> newSources) {
    if (node instanceof org.boostscale.velox4j.plan.ProjectNode) {
      org.boostscale.velox4j.plan.ProjectNode p = (org.boostscale.velox4j.plan.ProjectNode) node;
      return new org.boostscale.velox4j.plan.ProjectNode(
          p.getId(), newSources, p.getNames(), p.getProjections());
    } else if (node instanceof org.boostscale.velox4j.plan.LimitNode) {
      org.boostscale.velox4j.plan.LimitNode l = (org.boostscale.velox4j.plan.LimitNode) node;
      return new org.boostscale.velox4j.plan.LimitNode(
          l.getId(), newSources, l.getOffset(), l.getCount(), l.isPartial());
    } else if (node instanceof org.boostscale.velox4j.plan.FilterNode) {
      org.boostscale.velox4j.plan.FilterNode f = (org.boostscale.velox4j.plan.FilterNode) node;
      return new org.boostscale.velox4j.plan.FilterNode(f.getId(), newSources, f.getFilter());
    } else if (node instanceof org.boostscale.velox4j.plan.AggregationNode) {
      org.boostscale.velox4j.plan.AggregationNode a =
          (org.boostscale.velox4j.plan.AggregationNode) node;
      return new org.boostscale.velox4j.plan.AggregationNode(
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
    }
    return node;
  }

  @SuppressWarnings("unchecked")
  private List<PlanNode> getNodeSources(PlanNode node) {
    if (node instanceof org.boostscale.velox4j.plan.AggregationNode) {
      return ((org.boostscale.velox4j.plan.AggregationNode) node).getSources();
    }
    try {
      java.lang.reflect.Method m = PlanNode.class.getDeclaredMethod("getSources");
      m.setAccessible(true);
      return (List<PlanNode>) m.invoke(node);
    } catch (Exception e) {
      return Collections.emptyList();
    }
  }

  /** Convert an Arrow Schema to a velox4j RowType. */
  private org.boostscale.velox4j.type.RowType arrowSchemaToVeloxRowType(
      org.apache.arrow.vector.types.pojo.Schema schema) {
    List<String> names = new ArrayList<>();
    List<org.boostscale.velox4j.type.Type> types = new ArrayList<>();
    for (Field field : schema.getFields()) {
      names.add(field.getName());
      types.add(arrowTypeToVeloxType(field));
    }
    return new org.boostscale.velox4j.type.RowType(names, types);
  }

  /** Convert an Arrow Field to a velox4j Type. */
  private org.boostscale.velox4j.type.Type arrowTypeToVeloxType(Field field) {
    org.apache.arrow.vector.types.pojo.ArrowType arrowType = field.getType();
    if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Bool) {
      return new org.boostscale.velox4j.type.BooleanType();
    } else if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Int) {
      int bitWidth = ((org.apache.arrow.vector.types.pojo.ArrowType.Int) arrowType).getBitWidth();
      if (bitWidth <= 32) return new org.boostscale.velox4j.type.IntegerType();
      return new org.boostscale.velox4j.type.BigIntType();
    } else if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint) {
      var precision =
          ((org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint) arrowType).getPrecision();
      if (precision == org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE) {
        return new org.boostscale.velox4j.type.RealType();
      }
      return new org.boostscale.velox4j.type.DoubleType();
    } else if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Utf8) {
      return new org.boostscale.velox4j.type.VarCharType();
    } else if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Binary) {
      return new org.boostscale.velox4j.type.VarbinaryType();
    } else if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Struct) {
      // Nested row type — recurse into children
      List<String> childNames = new ArrayList<>();
      List<org.boostscale.velox4j.type.Type> childTypes = new ArrayList<>();
      for (Field child : field.getChildren()) {
        childNames.add(child.getName());
        childTypes.add(arrowTypeToVeloxType(child));
      }
      return new org.boostscale.velox4j.type.RowType(childNames, childTypes);
    }
    // Fallback
    return new org.boostscale.velox4j.type.VarbinaryType();
  }

  private String findScanNodeId(PlanNode node) {
    if (node instanceof TableScanNode) {
      return node.getId();
    }
    try {
      java.lang.reflect.Method m = PlanNode.class.getDeclaredMethod("getSources");
      m.setAccessible(true);
      @SuppressWarnings("unchecked")
      List<PlanNode> sources = (List<PlanNode>) m.invoke(node);
      if (sources != null) {
        for (PlanNode source : sources) {
          String id = findScanNodeId(source);
          if (id != null) return id;
        }
      }
    } catch (Exception e) {
      // ignore
    }
    return null;
  }

  public void setTransportService(TransportService transportService) {
    this.transportService = transportService;
  }

  public boolean isAvailable() {
    return veloxLifecycle.isEnabled();
  }

  private String extractSourceIndex(RelNode relNode) {
    if (relNode instanceof TableScan) {
      List<String> names = ((TableScan) relNode).getTable().getQualifiedName();
      // Qualified name is ["catalog", "index"] or ["index"] — use the last element
      return names.get(names.size() - 1);
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
      RelDataType rowType, List<ExecuteFragmentResponse> responses) {
    List<ExecutionEngine.Schema.Column> columns = new ArrayList<>();
    for (RelDataTypeField field : rowType.getFieldList()) {
      ExprType exprType = mapToExprType(field.getType().getSqlTypeName());
      columns.add(new ExecutionEngine.Schema.Column(field.getName(), null, exprType));
    }
    ExecutionEngine.Schema schema = new ExecutionEngine.Schema(columns);

    List<ExprValue> results = new ArrayList<>();
    for (ExecuteFragmentResponse response : responses) {
      if (response.getStatus() != ExecuteFragmentResponse.Status.SUCCESS) {
        throw new RuntimeException("Fragment execution failed: " + response.getErrorMessage());
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

    try (ArrowStreamReader reader =
        new ArrowStreamReader(new ByteArrayInputStream(arrowIpc), allocator)) {
      while (reader.loadNextBatch()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        List<Field> fields = root.getSchema().getFields();
        int rowCount = root.getRowCount();

        for (int row = 0; row < rowCount; row++) {
          LinkedHashMap<String, Object> tupleValues = new LinkedHashMap<>();
          for (int col = 0; col < fields.size(); col++) {
            String name = fields.get(col).getName();
            Object value = root.getVector(col).getObject(row);
            // Arrow Utf8 vectors return Text objects; convert to String for ExprValueUtils
            if (value instanceof org.apache.arrow.vector.util.Text) {
              value = value.toString();
            }
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
