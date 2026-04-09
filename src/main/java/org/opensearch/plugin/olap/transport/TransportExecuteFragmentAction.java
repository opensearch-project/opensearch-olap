/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.transport;

import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.boostscale.velox4j.connector.ExternalStreams;
import org.boostscale.velox4j.data.BaseVector;
import org.boostscale.velox4j.data.RowVector;
import org.boostscale.velox4j.expression.TypedExpr;
import org.boostscale.velox4j.iterator.CloseableIterator;
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
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugin.olap.engine.VeloxExecutionEngine;
import org.opensearch.plugin.olap.execution.ExternalStreamBridge;
import org.opensearch.plugin.olap.execution.LuceneArrowReader;
import org.opensearch.plugin.olap.execution.LuceneFilterConverter;
import org.opensearch.plugin.olap.execution.VeloxExecutor;
import org.opensearch.plugin.olap.execution.VeloxLifecycleService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
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
      Thread feederThread =
          new Thread(
              () -> {
                try {
                  for (ShardId shardId : shardIds) {
                    reader.readShardIntoStream(
                        shardId, request.getSourceIndex(), bridge, finalPushdownQuery);
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

    // Find the two TableScanNode IDs in the join plan (left=probe, right=build)
    Query query = Serde.fromJson(request.getPlanFragmentJson(), Query.class);
    List<String> scanIds = executor.findAllTableScanNodeIds(query.getPlan());
    if (scanIds.size() < 2) {
      return ExecuteFragmentResponse.failure(
          "Broadcast join plan must have 2 TableScanNodes, found " + scanIds.size());
    }
    String probeScanId = scanIds.get(0);
    String buildScanId = scanIds.get(1);

    // Create probe-side bridge (reads from local shards)
    ExternalStreamBridge probeBridge = new ExternalStreamBridge(session);
    LuceneArrowReader reader = new LuceneArrowReader(indicesService, probeBridge.getAllocator());

    // Create build-side queue (fed from broadcast data)
    ExternalStreams.BlockingQueue buildQueue = session.externalStreamOps().newBlockingQueue();

    try {
      probeBridge.open();

      List<String> scanFields = extractScanFields(request.getPlanFragmentJson());
      probeBridge.setRequestedFields(scanFields);

      // Start probe-side feeder thread (reads local shards)
      Thread probeFeeder =
          new Thread(
              () -> {
                try {
                  for (ShardId shardId : request.getShardIds()) {
                    reader.readShardIntoStream(
                        shardId, request.getSourceIndex(), probeBridge, null);
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

        byte[][] partitions =
            session.rowVectorOps().hashPartitionAndSerialize(batch, keyChannels, numPartitions);

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
        new org.opensearch.transport.TransportResponseHandler<ShuffleDataResponse>() {
          @Override
          public ShuffleDataResponse read(org.opensearch.core.common.io.stream.StreamInput in)
              throws java.io.IOException {
            return new ShuffleDataResponse(in);
          }

          @Override
          public void handleResponse(ShuffleDataResponse response) {
            // Shuffle data acknowledged
          }

          @Override
          public void handleException(org.opensearch.transport.TransportException exp) {
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

  private List<String> extractScanFields(String planJson) {
    Query query = Serde.fromJson(planJson, Query.class);
    PlanNode scanNode = findTableScanNode(query.getPlan());
    if (scanNode instanceof TableScanNode) {
      TableScanNode tableScan = (TableScanNode) scanNode;
      Type outputType = tableScan.getOutputType();
      if (outputType instanceof RowType) {
        return ((RowType) outputType).getNames();
      }
    }
    return List.of();
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
    try {
      java.lang.reflect.Method m = PlanNode.class.getDeclaredMethod("getSources");
      m.setAccessible(true);
      @SuppressWarnings("unchecked")
      List<PlanNode> sources = (List<PlanNode>) m.invoke(node);
      if (sources != null) {
        for (PlanNode source : sources) {
          FilterNode deeper = findFilterNode(source);
          if (deeper != null) {
            return deeper;
          }
        }
      }
    } catch (Exception e) {
      logger.warn("Cannot traverse plan node for filter extraction: {}", e.getMessage());
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
    try {
      java.lang.reflect.Method m = PlanNode.class.getDeclaredMethod("getSources");
      m.setAccessible(true);
      @SuppressWarnings("unchecked")
      List<PlanNode> sources = (List<PlanNode>) m.invoke(node);
      if (sources != null) {
        for (PlanNode source : sources) {
          PlanNode found = findTableScanNode(source);
          if (found != null) {
            return found;
          }
        }
      }
    } catch (Exception e) {
      logger.warn("Cannot traverse plan node: {}", e.getMessage());
    }
    return null;
  }
}
