/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.transport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.boostscale.velox4j.connector.ExternalStreams;
import org.boostscale.velox4j.data.BaseVector;
import org.boostscale.velox4j.data.BaseVectors;
import org.boostscale.velox4j.data.RowVector;
import org.boostscale.velox4j.expression.TypedExpr;
import org.boostscale.velox4j.iterator.CloseableIterator;
import org.boostscale.velox4j.partition.HashPartitionFunctionSpec;
import org.boostscale.velox4j.plan.FilterNode;
import org.boostscale.velox4j.plan.PlanNode;
import org.boostscale.velox4j.plan.TableScanNode;
import org.boostscale.velox4j.query.Query;
import org.boostscale.velox4j.serde.Serde;
import org.boostscale.velox4j.session.Session;
import org.boostscale.velox4j.type.RowType;
import org.boostscale.velox4j.type.Type;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugin.olap.engine.VeloxExecutionEngine;
import org.opensearch.plugin.olap.execution.ExternalStreamBridge;
import org.opensearch.plugin.olap.execution.LuceneArrowReader;
import org.opensearch.plugin.olap.execution.LuceneFilterConverter;
import org.opensearch.plugin.olap.execution.RuntimeFilterBuilder;
import org.opensearch.plugin.olap.execution.VeloxExecutor;
import org.opensearch.plugin.olap.execution.VeloxLifecycleService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

/**
 * Handles execution of a Velox plan fragment on a data node.
 *
 * <p>Supports three execution modes:
 *
 * <ul>
 *   <li><b>Normal scan</b>: Reads local shards, executes Velox plan, returns results
 *   <li><b>Broadcast join</b>: Reads local shards (probe) + deserializes broadcast data (build),
 *       executes join locally
 *   <li><b>Shuffle scan</b>: Reads local shards, hash-partitions results, sends partitions to
 *       target workers
 *   <li><b>Shuffle join</b>: Reads from ShuffleManager buffer, executes join locally
 * </ul>
 */
public class TransportExecuteFragmentAction
    extends HandledTransportAction<ExecuteFragmentRequest, ExecuteFragmentResponse> {

  private static final Logger logger = LogManager.getLogger(TransportExecuteFragmentAction.class);

  private final IndicesService indicesService;
  private final ThreadPool threadPool;
  private final VeloxLifecycleService veloxLifecycle;
  private final TransportService transportService;
  private final ClusterService clusterService;
  private final ShuffleManager shuffleManager;
  private final ExecutorService segmentExecutor;

  @Inject
  public TransportExecuteFragmentAction(
      TransportService transportService,
      ActionFilters actionFilters,
      IndicesService indicesService,
      ThreadPool threadPool,
      VeloxLifecycleService veloxLifecycle,
      VeloxExecutionEngine veloxExecutionEngine,
      ClusterService clusterService,
      ShuffleManager shuffleManager) {
    super(ExecuteFragmentAction.NAME, transportService, actionFilters, ExecuteFragmentRequest::new);
    this.indicesService = indicesService;
    this.threadPool = threadPool;
    this.veloxLifecycle = veloxLifecycle;
    this.transportService = transportService;
    this.clusterService = clusterService;
    this.shuffleManager = shuffleManager;
    veloxExecutionEngine.setTransportService(transportService);

    // Shared thread pool for parallel segment reads within a shard
    int parallelism = veloxLifecycle.getSegmentParallelism();
    this.segmentExecutor =
        Executors.newFixedThreadPool(
            parallelism,
            r -> {
              Thread t = new Thread(r, "olap-segment-reader");
              t.setDaemon(true);
              return t;
            });
  }

  @Override
  protected void doExecute(
      Task task, ExecuteFragmentRequest request, ActionListener<ExecuteFragmentResponse> listener) {
    threadPool
        .executor(ThreadPool.Names.SEARCH)
        .execute(
            () -> {
              try {
                ExecuteFragmentResponse response;
                if (request.hasBroadcastData()) {
                  response = executeBroadcastJoinFragment(request);
                } else if (request.isShuffleScan()) {
                  response = executeShuffleScanFragment(request);
                } else if (request.isShuffleJoin()) {
                  response = executeShuffleJoinFragment(request);
                } else {
                  response = executeFragment(request);
                }
                listener.onResponse(response);
              } catch (Exception e) {
                logger.error(
                    "Failed to execute fragment {} for query {}",
                    request.getFragmentId(),
                    request.getQueryId(),
                    e);
                listener.onResponse(ExecuteFragmentResponse.failure(e.getMessage()));
              }
            });
  }

  // ---- Normal Scan Execution (existing) ----

  private ExecuteFragmentResponse executeFragment(ExecuteFragmentRequest request) {
    String queryId = request.getQueryId();
    List<ShardId> shardIds = request.getShardIds();

    logger.info(
        "Executing fragment {} for query {} on {} shards",
        request.getFragmentId(),
        queryId,
        shardIds.size());

    VeloxExecutor executor = new VeloxExecutor(veloxLifecycle.getSession());
    ExternalStreamBridge bridge = new ExternalStreamBridge(veloxLifecycle.getSession());
    LuceneArrowReader reader = new LuceneArrowReader(indicesService, bridge.getAllocator());

    try {
      bridge.open();

      List<String> scanFields = extractScanFields(request.getPlanFragmentJson());
      bridge.setRequestedFields(scanFields);

      org.apache.lucene.search.Query pushdownQuery =
          extractPushdownQuery(request.getPlanFragmentJson());
      if (pushdownQuery != null) {
        logger.info("Predicate pushdown enabled for query {}: {}", queryId, pushdownQuery);
      }

      final org.apache.lucene.search.Query finalPushdownQuery = pushdownQuery;
      final boolean useParallelReads = veloxLifecycle.getSegmentParallelism() > 1;
      Thread feederThread =
          new Thread(
              () -> {
                try {
                  for (ShardId shardId : shardIds) {
                    if (useParallelReads) {
                      reader.readShardIntoStreamParallel(
                          shardId,
                          request.getSourceIndex(),
                          bridge,
                          finalPushdownQuery,
                          segmentExecutor);
                    } else {
                      reader.readShardIntoStream(
                          shardId, request.getSourceIndex(), bridge, finalPushdownQuery);
                    }
                  }
                  bridge.noMoreInput();
                } catch (Throwable e) {
                  logger.error("Error feeding data for query {}", queryId, e);
                  bridge.abort(e instanceof Exception ? (Exception) e : new RuntimeException(e));
                }
              },
              "olap-feeder-" + queryId + "-" + request.getFragmentId());
      feederThread.setDaemon(true);
      feederThread.start();

      boolean isPartialAgg = request.getPlanFragmentJson().contains("\"step\":\"PARTIAL\"");

      ExecuteFragmentResponse response;
      if (isPartialAgg) {
        List<byte[]> nativeBatches =
            executor.executeNative(
                request.getPlanFragmentJson(), bridge.getConnectorId(), bridge.getQueue());
        feederThread.join(30_000);
        response = ExecuteFragmentResponse.successNative(bridge.getRowCount(), nativeBatches);
      } else {
        byte[] resultData =
            executor.execute(
                request.getPlanFragmentJson(), bridge.getConnectorId(), bridge.getQueue());
        feederThread.join(30_000);
        response = ExecuteFragmentResponse.success(bridge.getRowCount(), resultData);
      }

      return response;

    } catch (Exception e) {
      logger.error("Fragment execution failed for query {}", queryId, e);
      return ExecuteFragmentResponse.failure(e.getMessage());
    } finally {
      bridge.close();
    }
  }

  // ---- Broadcast Join Execution ----

  /**
   * Execute a broadcast join fragment on a probe-side data node. Deserializes broadcast build-side
   * data into one BlockingQueue, scans local probe-side shards into another, and runs HashJoinNode
   * locally.
   */
  private ExecuteFragmentResponse executeBroadcastJoinFragment(ExecuteFragmentRequest request) {
    String queryId = request.getQueryId();
    logger.info(
        "Executing broadcast join fragment {} for query {} with {} broadcast batches",
        request.getFragmentId(),
        queryId,
        request.getBroadcastData().size());

    Session session = veloxLifecycle.getSession();
    VeloxExecutor executor = new VeloxExecutor(session);
    String connectorId = "connector-external-stream";

    // Find the two TableScanNode IDs in the join plan.
    // broadcastBuildScanIndex tells us which scan is the build side (default 1 = right).
    Query query = Serde.fromJson(request.getPlanFragmentJson(), Query.class);
    List<String> scanIds = executor.findAllTableScanNodeIds(query.getPlan());
    if (scanIds.size() < 2) {
      return ExecuteFragmentResponse.failure(
          "Broadcast join plan must have 2 TableScanNodes, found " + scanIds.size());
    }
    int buildIdx = request.getBroadcastBuildScanIndex();
    int probeIdx = (buildIdx == 0) ? 1 : 0;
    String probeScanId = scanIds.get(probeIdx);
    String buildScanId = scanIds.get(buildIdx);

    // Create probe-side bridge (reads from local shards)
    ExternalStreamBridge probeBridge = new ExternalStreamBridge(session);
    LuceneArrowReader reader = new LuceneArrowReader(indicesService, probeBridge.getAllocator());

    // Create build-side queue (fed from broadcast data)
    ExternalStreams.BlockingQueue buildQueue = session.externalStreamOps().newBlockingQueue();

    try {
      probeBridge.open();

      // Extract field names from the probe-side scan (not the build-side scan)
      List<String> scanFields = extractScanFieldsFromNode(query.getPlan(), probeScanId);
      if (scanFields.isEmpty()) {
        scanFields = extractScanFields(request.getPlanFragmentJson());
      }
      probeBridge.setRequestedFields(scanFields);

      // Build runtime filter query if present in request
      org.apache.lucene.search.Query rfQuery = buildRuntimeFilterQuery(request);
      if (rfQuery != null) {
        logger.info(
            "Runtime filter applied for query {}: field={}, {} values",
            queryId,
            request.getRfFieldName(),
            request.getRfValues().size());
      }

      // Extract pushdown from the probe-side leaf fragment plan (safe — only probe branch).
      // The coordinator join plan has no FilterNode for probe-side predicates, so we extract
      // pushdown from the probe leaf and apply it at the Lucene level here.
      org.apache.lucene.search.Query probePushdown = null;
      if (request.getProbePlanJson() != null) {
        probePushdown = extractPushdownQuery(request.getProbePlanJson());
      }
      org.apache.lucene.search.Query combinedQuery =
          RuntimeFilterBuilder.combine(probePushdown, rfQuery);

      // Start probe-side feeder thread (reads local shards with RF pushdown)
      final org.apache.lucene.search.Query finalCombinedQuery = combinedQuery;
      Thread probeFeeder =
          new Thread(
              () -> {
                try {
                  for (ShardId shardId : request.getShardIds()) {
                    if (veloxLifecycle.getSegmentParallelism() > 1) {
                      reader.readShardIntoStreamParallel(
                          shardId,
                          request.getSourceIndex(),
                          probeBridge,
                          finalCombinedQuery,
                          segmentExecutor);
                    } else {
                      reader.readShardIntoStream(
                          shardId, request.getSourceIndex(), probeBridge, finalCombinedQuery);
                    }
                  }
                  probeBridge.noMoreInput();
                } catch (Throwable e) {
                  logger.error("Probe feeder error for query {}", queryId, e);
                  probeBridge.abort(
                      e instanceof Exception ? (Exception) e : new RuntimeException(e));
                }
              },
              "olap-probe-feeder-" + queryId);
      probeFeeder.setDaemon(true);
      probeFeeder.start();

      // Start build-side feeder thread (deserializes broadcast data)
      Thread buildFeeder =
          new Thread(
              () -> {
                try {
                  for (byte[] batch : request.getBroadcastData()) {
                    BaseVector vec = session.baseVectorOps().deserializeOneFromBuf(batch);
                    buildQueue.put(vec.asRowVector());
                  }
                  buildQueue.noMoreInput();
                } catch (Throwable e) {
                  logger.error("Build feeder error for query {}", queryId, e);
                  buildQueue.noMoreInput();
                }
              },
              "olap-build-feeder-" + queryId);
      buildFeeder.setDaemon(true);
      buildFeeder.start();

      // Execute the join plan with both inputs
      boolean isPartialAgg = request.getPlanFragmentJson().contains("\"step\":\"PARTIAL\"");
      ExecuteFragmentResponse response;

      if (isPartialAgg) {
        List<byte[]> nativeBatches =
            executor.executeDualInputNative(
                request.getPlanFragmentJson(),
                connectorId,
                probeBridge.getQueue(),
                probeScanId,
                buildQueue,
                buildScanId);
        probeFeeder.join(30_000);
        buildFeeder.join(5_000);
        response = ExecuteFragmentResponse.successNative(probeBridge.getRowCount(), nativeBatches);
      } else {
        byte[] resultData =
            executor.executeDualInput(
                request.getPlanFragmentJson(),
                connectorId,
                probeBridge.getQueue(),
                probeScanId,
                buildQueue,
                buildScanId);
        probeFeeder.join(30_000);
        buildFeeder.join(5_000);
        response = ExecuteFragmentResponse.success(probeBridge.getRowCount(), resultData);
      }

      return response;

    } catch (Exception e) {
      logger.error("Broadcast join execution failed for query {}", queryId, e);
      return ExecuteFragmentResponse.failure(e.getMessage());
    } finally {
      probeBridge.close();
    }
  }

  // ---- Shuffle Scan Execution ----

  /**
   * Execute a shuffle scan fragment: scan local shards, hash-partition each batch by join key, and
   * send partitions to target worker nodes via ShuffleDataAction.
   */
  private ExecuteFragmentResponse executeShuffleScanFragment(ExecuteFragmentRequest request) {
    String queryId = request.getQueryId();
    logger.info(
        "Executing shuffle scan fragment {} for query {}, side={}, {} partitions",
        request.getFragmentId(),
        queryId,
        request.getShuffleSide(),
        request.getShuffleNumPartitions());

    Session session = veloxLifecycle.getSession();
    VeloxExecutor executor = new VeloxExecutor(session);
    ExternalStreamBridge bridge = new ExternalStreamBridge(session);
    LuceneArrowReader reader = new LuceneArrowReader(indicesService, bridge.getAllocator());

    try {
      bridge.open();
      List<String> scanFields = extractScanFields(request.getPlanFragmentJson());
      bridge.setRequestedFields(scanFields);

      // Start feeder thread for Lucene data
      Thread feederThread =
          new Thread(
              () -> {
                try {
                  for (ShardId shardId : request.getShardIds()) {
                    reader.readShardIntoStream(shardId, request.getSourceIndex(), bridge, null);
                  }
                  bridge.noMoreInput();
                } catch (Throwable e) {
                  logger.error("Shuffle scan feeder error for query {}", queryId, e);
                  bridge.abort(e instanceof Exception ? (Exception) e : new RuntimeException(e));
                }
              },
              "olap-shuffle-feeder-" + queryId + "-" + request.getFragmentId());
      feederThread.setDaemon(true);
      feederThread.start();

      // Execute the scan plan and get raw RowVector iterator
      CloseableIterator<RowVector> iter =
          executor.executeToIterator(
              request.getPlanFragmentJson(), bridge.getConnectorId(), bridge.getQueue());

      // Resolve target worker nodes
      List<String> targetNodeIds = request.getShuffleTargetNodeIds();
      List<Integer> keyChannels = request.getShuffleKeyChannels();
      int numPartitions = request.getShuffleNumPartitions();
      String side = request.getShuffleSide();
      int targetStageId = request.getShuffleTargetStageId();

      DiscoveryNode[] targetNodes = new DiscoveryNode[targetNodeIds.size()];
      for (int i = 0; i < targetNodeIds.size(); i++) {
        targetNodes[i] = clusterService.state().nodes().get(targetNodeIds.get(i));
        if (targetNodes[i] == null) {
          throw new IllegalStateException("Shuffle target node not found: " + targetNodeIds.get(i));
        }
      }

      // Hash-partition each batch and send to target workers
      long totalRows = 0;
      while (iter.hasNext()) {
        RowVector batch = iter.next();
        if (batch == null) break;

        // Partition by hash using HashPartitionFunctionSpec and serialize each partition.
        HashPartitionFunctionSpec hashSpec =
            new HashPartitionFunctionSpec((RowType) batch.getType(), keyChannels);
        List<RowVector> partitionVectors =
            session.rowVectorOps().partitionBySpec(batch, hashSpec, numPartitions);
        byte[][] partitions = new byte[partitionVectors.size()][];
        for (int p = 0; p < partitionVectors.size(); p++) {
          if (partitionVectors.get(p) != null) {
            partitions[p] = BaseVectors.serializeOneToBuf(partitionVectors.get(p));
          }
        }

        for (int i = 0; i < partitions.length; i++) {
          if (partitions[i] != null) {
            sendShuffleData(targetNodes[i], queryId, targetStageId, side, partitions[i], false);
          }
        }
        totalRows += batch.getSize();
      }
      iter.close();

      // Send "done" signal to all target workers
      for (DiscoveryNode targetNode : targetNodes) {
        sendShuffleData(targetNode, queryId, targetStageId, side, null, true);
      }

      feederThread.join(30_000);
      logger.info("Shuffle scan complete: {} rows sent for query {}", totalRows, queryId);

      return ExecuteFragmentResponse.success(totalRows, new byte[0]);

    } catch (Exception e) {
      logger.error("Shuffle scan execution failed for query {}", queryId, e);
      return ExecuteFragmentResponse.failure(e.getMessage());
    } finally {
      bridge.close();
    }
  }

  // ---- Shuffle Join Execution ----

  /**
   * Execute a shuffle join fragment: read pre-shuffled data from ShuffleManager buffer and execute
   * the join plan locally.
   */
  private ExecuteFragmentResponse executeShuffleJoinFragment(ExecuteFragmentRequest request) {
    String queryId = request.getQueryId();
    String shuffleQueryId = request.getShuffleJoinQueryId();
    int stageId = request.getShuffleJoinStageId();

    logger.info(
        "Executing shuffle join fragment {} for query {}", request.getFragmentId(), queryId);

    Session session = veloxLifecycle.getSession();
    VeloxExecutor executor = new VeloxExecutor(session);
    String connectorId = "connector-external-stream";

    // Get or create the shuffle buffer and set expected sender counts
    ShuffleManager.ShuffleBuffer buffer = shuffleManager.getOrCreateBuffer(shuffleQueryId, stageId);
    buffer.setExpectedSenders(request.getExpectedLeftSenders(), request.getExpectedRightSenders());

    try {
      // Wait for all shuffle data to arrive
      if (!buffer.awaitReady(300_000)) { // 5 minute timeout
        return ExecuteFragmentResponse.failure("Shuffle data timeout for query " + queryId);
      }

      // Find the two TableScanNode IDs in the join plan
      Query query = Serde.fromJson(request.getPlanFragmentJson(), Query.class);
      List<String> scanIds = executor.findAllTableScanNodeIds(query.getPlan());
      if (scanIds.size() < 2) {
        return ExecuteFragmentResponse.failure(
            "Shuffle join plan must have 2 TableScanNodes, found " + scanIds.size());
      }
      String leftScanId = scanIds.get(0);
      String rightScanId = scanIds.get(1);

      // Create BlockingQueues for both sides
      ExternalStreams.BlockingQueue leftQueue = session.externalStreamOps().newBlockingQueue();
      ExternalStreams.BlockingQueue rightQueue = session.externalStreamOps().newBlockingQueue();

      // Feed left shuffle data
      Thread leftFeeder =
          new Thread(
              () -> {
                try {
                  for (byte[] data : buffer.getLeftData()) {
                    BaseVector vec = session.baseVectorOps().deserializeOneFromBuf(data);
                    leftQueue.put(vec.asRowVector());
                  }
                  leftQueue.noMoreInput();
                } catch (Throwable e) {
                  logger.error("Left shuffle feeder error", e);
                  leftQueue.noMoreInput();
                }
              },
              "olap-shuffle-left-feeder-" + queryId);
      leftFeeder.setDaemon(true);
      leftFeeder.start();

      // Feed right shuffle data
      Thread rightFeeder =
          new Thread(
              () -> {
                try {
                  for (byte[] data : buffer.getRightData()) {
                    BaseVector vec = session.baseVectorOps().deserializeOneFromBuf(data);
                    rightQueue.put(vec.asRowVector());
                  }
                  rightQueue.noMoreInput();
                } catch (Throwable e) {
                  logger.error("Right shuffle feeder error", e);
                  rightQueue.noMoreInput();
                }
              },
              "olap-shuffle-right-feeder-" + queryId);
      rightFeeder.setDaemon(true);
      rightFeeder.start();

      // Execute the join
      boolean isPartialAgg = request.getPlanFragmentJson().contains("\"step\":\"PARTIAL\"");
      ExecuteFragmentResponse response;

      if (isPartialAgg) {
        List<byte[]> nativeBatches =
            executor.executeDualInputNative(
                request.getPlanFragmentJson(),
                connectorId,
                leftQueue,
                leftScanId,
                rightQueue,
                rightScanId);
        leftFeeder.join(30_000);
        rightFeeder.join(30_000);
        response = ExecuteFragmentResponse.successNative(0, nativeBatches);
      } else {
        byte[] resultData =
            executor.executeDualInput(
                request.getPlanFragmentJson(),
                connectorId,
                leftQueue,
                leftScanId,
                rightQueue,
                rightScanId);
        leftFeeder.join(30_000);
        rightFeeder.join(30_000);
        response = ExecuteFragmentResponse.success(0, resultData);
      }

      return response;

    } catch (Exception e) {
      logger.error("Shuffle join execution failed for query {}", queryId, e);
      return ExecuteFragmentResponse.failure(e.getMessage());
    } finally {
      shuffleManager.removeBuffer(shuffleQueryId, stageId);
    }
  }

  // ---- Shuffle Data Sending ----

  private void sendShuffleData(
      DiscoveryNode target,
      String queryId,
      int targetStageId,
      String side,
      byte[] data,
      boolean isLast) {
    ShuffleDataRequest shuffleRequest =
        new ShuffleDataRequest(queryId, targetStageId, side, data, isLast);

    transportService.sendRequest(
        target,
        ShuffleDataAction.NAME,
        shuffleRequest,
        new TransportResponseHandler<ShuffleDataResponse>() {
          @Override
          public ShuffleDataResponse read(StreamInput in) throws IOException {
            return new ShuffleDataResponse(in);
          }

          @Override
          public void handleResponse(ShuffleDataResponse response) {
            // Shuffle data acknowledged
          }

          @Override
          public void handleException(TransportException exp) {
            logger.error(
                "Failed to send shuffle data to {}: {}", target.getName(), exp.getMessage());
          }

          @Override
          public String executor() {
            return ThreadPool.Names.SEARCH;
          }
        });
  }

  // ---- Plan Introspection Helpers ----

  /** Extract field names from a specific TableScanNode by ID. */
  /** Build a Lucene runtime filter query from the request's RF metadata. */
  private org.apache.lucene.search.Query buildRuntimeFilterQuery(ExecuteFragmentRequest request) {
    if (!request.hasRuntimeFilter()) return null;

    Set<Object> values = new LinkedHashSet<>();
    String fieldType = request.getRfFieldType();
    for (String v : request.getRfValues()) {
      try {
        switch (fieldType) {
          case "integer":
            values.add(Integer.parseInt(v));
            break;
          case "long":
            values.add(Long.parseLong(v));
            break;
          default:
            values.add(v);
        }
      } catch (NumberFormatException e) {
        values.add(v);
      }
    }

    return RuntimeFilterBuilder.build(request.getRfFieldName(), values, fieldType);
  }

  private List<String> extractScanFieldsFromNode(PlanNode root, String targetScanId) {
    List<TableScanNode> scans = new ArrayList<>();
    collectTableScanNodes(root, scans);
    for (TableScanNode scan : scans) {
      if (scan.getId().equals(targetScanId)) {
        Type outputType = scan.getOutputType();
        if (outputType instanceof RowType) {
          return flattenRowTypeNames((RowType) outputType, "");
        }
      }
    }
    return List.of();
  }

  private void collectTableScanNodes(PlanNode node, List<TableScanNode> result) {
    if (node instanceof TableScanNode) {
      result.add((TableScanNode) node);
    }
    for (PlanNode source : node.getSources()) {
      collectTableScanNodes(source, result);
    }
  }

  private List<String> extractScanFields(String planJson) {
    Query query = Serde.fromJson(planJson, Query.class);
    PlanNode scanNode = findTableScanNode(query.getPlan());
    if (scanNode instanceof TableScanNode) {
      TableScanNode tableScan = (TableScanNode) scanNode;
      Type outputType = tableScan.getOutputType();
      if (outputType instanceof RowType) {
        // Flatten ROW-typed fields into dot-path names for Lucene doc-value reading.
        // e.g., cloud: ROW(region: VARCHAR) → "cloud.region"
        return flattenRowTypeNames((RowType) outputType, "");
      }
    }
    return List.of();
  }

  /**
   * Flatten a RowType into dot-path field names. Scalar fields are added as-is. ROW-typed fields
   * (nested objects) are recursively flattened with dot prefix.
   */
  private List<String> flattenRowTypeNames(RowType rowType, String prefix) {
    List<String> result = new ArrayList<>();
    for (int i = 0; i < rowType.size(); i++) {
      String name =
          prefix.isEmpty() ? rowType.getNames().get(i) : prefix + "." + rowType.getNames().get(i);
      Type childType = rowType.getChildren().get(i);
      if (childType instanceof RowType) {
        result.addAll(flattenRowTypeNames((RowType) childType, name));
      } else {
        result.add(name);
      }
    }
    return result;
  }

  private org.apache.lucene.search.Query extractPushdownQuery(String planJson) {
    try {
      Query query = Serde.fromJson(planJson, Query.class);
      FilterNode filterNode = findFilterNode(query.getPlan());
      if (filterNode == null) {
        return null;
      }
      TypedExpr filterExpr = filterNode.getFilter();
      LuceneFilterConverter converter = new LuceneFilterConverter();
      return converter.convert(filterExpr);
    } catch (Exception e) {
      logger.warn(
          "Failed to extract pushdown query, falling back to full scan: {}", e.getMessage());
      return null;
    }
  }

  private FilterNode findFilterNode(PlanNode node) {
    for (PlanNode source : node.getSources()) {
      FilterNode deeper = findFilterNode(source);
      if (deeper != null) {
        return deeper;
      }
    }
    if (node instanceof FilterNode) {
      return (FilterNode) node;
    }
    return null;
  }

  private PlanNode findTableScanNode(PlanNode node) {
    if (node instanceof TableScanNode) {
      return node;
    }
    for (PlanNode source : node.getSources()) {
      PlanNode found = findTableScanNode(source);
      if (found != null) {
        return found;
      }
    }
    return null;
  }
}
