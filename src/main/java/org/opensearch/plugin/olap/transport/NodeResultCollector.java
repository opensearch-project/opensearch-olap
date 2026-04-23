/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.transport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.boostscale.velox4j.config.Config;
import org.boostscale.velox4j.config.ConnectorConfig;
import org.boostscale.velox4j.query.Query;
import org.boostscale.velox4j.serde.Serde;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.plugin.olap.common.QueryId;
import org.opensearch.plugin.olap.execution.OlapBloomFilter;
import org.opensearch.plugin.olap.execution.ResultTooLargeException;
import org.opensearch.plugin.olap.execution.RuntimeFilterPayload;
import org.opensearch.plugin.olap.scheduler.BadResourceTracker;
import org.opensearch.plugin.olap.scheduler.ErrorClassifier;
import org.opensearch.plugin.olap.scheduler.ErrorClassifier.ErrorCategory;
import org.opensearch.plugin.olap.scheduler.QueryExecution;
import org.opensearch.plugin.olap.scheduler.QueryScheduler;
import org.opensearch.plugin.olap.scheduler.ShardRouter;
import org.opensearch.plugin.olap.scheduler.Stage;
import org.opensearch.plugin.olap.scheduler.TaskDescriptor;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

/**
 * Dispatches fragment execution requests to data nodes and collects results.
 *
 * <p>Supports dispatch modes for normal scans, broadcast joins, shuffle scans, and shuffle joins.
 */
public class NodeResultCollector {

  private static final Logger logger = LogManager.getLogger(NodeResultCollector.class);
  private static final long DEFAULT_TIMEOUT_MS = 300_000; // 5 minutes

  private final TransportService transportService;
  private final QueryScheduler scheduler;
  private final int maxRetries;
  private final long inflightCapBytes;
  private volatile boolean profileEnabled;

  public NodeResultCollector(TransportService transportService, QueryScheduler scheduler) {
    this(transportService, scheduler, 0, Long.MAX_VALUE);
  }

  public NodeResultCollector(
      TransportService transportService, QueryScheduler scheduler, int maxRetries) {
    this(transportService, scheduler, maxRetries, Long.MAX_VALUE);
  }

  /**
   * @param inflightCapBytes aggregate cap on {@code resultData + nativeResultBatches} bytes
   *     accumulated across all fragments of a single query. Exceeding this fails the query with
   *     {@link ResultTooLargeException}. Use {@link Long#MAX_VALUE} to disable.
   */
  public NodeResultCollector(
      TransportService transportService,
      QueryScheduler scheduler,
      int maxRetries,
      long inflightCapBytes) {
    this.transportService = transportService;
    this.scheduler = scheduler;
    this.maxRetries = maxRetries;
    this.inflightCapBytes = inflightCapBytes;
  }

  /** Estimate the memory footprint of a single fragment response. */
  private static long responseBytes(ExecuteFragmentResponse response) {
    long total = 0;
    byte[] data = response.getResultData();
    if (data != null) {
      total += data.length;
    }
    if (response.hasNativeResults()) {
      for (byte[] b : response.getNativeResultBatches()) {
        if (b != null) total += b.length;
      }
    }
    byte[] bloom = response.getPartialBloomBytes();
    if (bloom != null) {
      total += bloom.length;
    }
    return total;
  }

  /**
   * When true, every outgoing {@link ExecuteFragmentRequest} is stamped with {@code
   * profileEnabled=true} so data nodes produce per-task profiles. Set once per query by {@link
   * org.opensearch.plugin.olap.engine.VeloxExecutionEngine} from the PPL request's {@code
   * profile=true} flag.
   */
  public void setProfileEnabled(boolean profileEnabled) {
    this.profileEnabled = profileEnabled;
  }

  public boolean isProfileEnabled() {
    return profileEnabled;
  }

  /** Dispatch all tasks for a query execution and collect results. */
  public List<ExecuteFragmentResponse> dispatchAndCollect(QueryExecution execution) {
    return dispatchAndCollect(execution, execution.getStages());
  }

  /** Dispatch tasks for specific stages and collect results. */
  public List<ExecuteFragmentResponse> dispatchAndCollect(
      QueryExecution execution, List<Stage> stages) {
    QueryId queryId = execution.getQueryId();
    List<TaskDescriptor> allTasks = new ArrayList<>();
    for (var stage : stages) {
      allTasks.addAll(stage.getTasks());
    }

    return dispatchTasks(queryId, allTasks, task -> createNormalRequest(queryId, task));
  }

  /**
   * Dispatch build-side tasks for a broadcast join and request a two-stage PARTIAL BLOOM build
   * alongside the scan. Every dispatched request is stamped with the same {@code fieldName}, {@code
   * fieldType}, and {@code expectedInsertions} so the returned partial blooms share bit layout and
   * can be merged at the coordinator via {@link OlapBloomFilter#merge}.
   */
  public List<ExecuteFragmentResponse> dispatchAndCollectWithPartialBloom(
      QueryExecution execution,
      List<Stage> stages,
      String fieldName,
      String fieldType,
      int expectedInsertions) {
    QueryId queryId = execution.getQueryId();
    List<TaskDescriptor> allTasks = new ArrayList<>();
    for (var stage : stages) {
      allTasks.addAll(stage.getTasks());
    }

    return dispatchTasks(
        queryId,
        allTasks,
        task -> {
          ExecuteFragmentRequest request = createNormalRequest(queryId, task);
          request.setBuildPartialBloom(fieldName, fieldType, expectedInsertions);
          return request;
        });
  }

  /** Dispatch broadcast join tasks: each task receives the broadcast data along with the plan. */
  public List<ExecuteFragmentResponse> dispatchAndCollectBroadcast(
      QueryExecution execution, List<Stage> stages, List<byte[]> broadcastData) {
    return dispatchAndCollectBroadcast(execution, stages, broadcastData, 1);
  }

  /**
   * Dispatch broadcast join tasks with explicit build scan index.
   *
   * @param buildScanIndex which TableScanNode in the plan is the build side (0=left, 1=right)
   */
  public List<ExecuteFragmentResponse> dispatchAndCollectBroadcast(
      QueryExecution execution,
      List<Stage> stages,
      List<byte[]> broadcastData,
      int buildScanIndex) {
    return dispatchAndCollectBroadcast(
        execution, stages, broadcastData, buildScanIndex, RuntimeFilterPayload.none(), null);
  }

  /**
   * Dispatch broadcast join tasks with build scan index and an optional runtime filter. The RF kind
   * (TERMS / BLOOM / NONE) is encoded in {@code payload} — see {@link RuntimeFilterPayload}.
   */
  public List<ExecuteFragmentResponse> dispatchAndCollectBroadcast(
      QueryExecution execution,
      List<Stage> stages,
      List<byte[]> broadcastData,
      int buildScanIndex,
      RuntimeFilterPayload payload,
      String probePlanJson) {
    QueryId queryId = execution.getQueryId();
    List<TaskDescriptor> allTasks = new ArrayList<>();
    for (var stage : stages) {
      allTasks.addAll(stage.getTasks());
    }

    return dispatchTasks(
        queryId,
        allTasks,
        task -> {
          ExecuteFragmentRequest request = createNormalRequest(queryId, task);
          request.setBroadcastData(broadcastData, buildScanIndex);
          if (payload != null) {
            switch (payload.getKind()) {
              case TERMS:
                request.setRuntimeFilter(
                    payload.getFieldName(), payload.getFieldType(), payload.getTermsValues());
                break;
              case BLOOM:
                request.setBloomRuntimeFilter(
                    payload.getFieldName(), payload.getFieldType(), payload.getBloomBytes());
                break;
              case NONE:
              default:
                break;
            }
          }
          if (probePlanJson != null) {
            request.setProbePlanJson(probePlanJson);
          }
          return request;
        });
  }

  /**
   * Dispatch shuffle scan tasks: each task receives the shuffle config telling it how to partition
   * and where to send data.
   */
  public List<ExecuteFragmentResponse> dispatchAndCollectShuffle(
      QueryExecution execution, List<Stage> stages, List<String> workerNodeIds, int targetStageId) {
    QueryId queryId = execution.getQueryId();
    List<TaskDescriptor> allTasks = new ArrayList<>();
    for (var stage : stages) {
      allTasks.addAll(stage.getTasks());
    }

    return dispatchTasks(
        queryId,
        allTasks,
        task -> {
          ExecuteFragmentRequest request = createNormalRequest(queryId, task);
          var props = task.getFragment().getProperties();
          if (props.isShuffleScan()) {
            request.setShuffleConfig(
                workerNodeIds,
                props.getShuffleKeyChannels(),
                props.getJoinSide(),
                targetStageId,
                workerNodeIds.size());
          }
          return request;
        });
  }

  /** Dispatch shuffle join tasks: each task reads from ShuffleManager buffer. */
  public List<ExecuteFragmentResponse> dispatchAndCollectShuffleJoin(
      QueryExecution execution,
      List<Stage> stages,
      String shuffleQueryId,
      int shuffleStageId,
      int expectedLeftSenders,
      int expectedRightSenders) {
    QueryId queryId = execution.getQueryId();
    List<TaskDescriptor> allTasks = new ArrayList<>();
    for (var stage : stages) {
      allTasks.addAll(stage.getTasks());
    }

    return dispatchTasks(
        queryId,
        allTasks,
        task -> {
          ExecuteFragmentRequest request = createNormalRequest(queryId, task);
          request.setShuffleJoinConfig(
              shuffleQueryId, shuffleStageId, expectedLeftSenders, expectedRightSenders);
          return request;
        });
  }

  // ---- Internal ----

  @FunctionalInterface
  private interface RequestFactory {
    ExecuteFragmentRequest create(TaskDescriptor task);
  }

  private List<ExecuteFragmentResponse> dispatchTasks(
      QueryId queryId, List<TaskDescriptor> tasks, RequestFactory requestFactory) {
    CountDownLatch latch = new CountDownLatch(tasks.size());
    List<ExecuteFragmentResponse> responses =
        new ArrayList<>(Collections.nCopies(tasks.size(), null));
    AtomicReference<Exception> firstError = new AtomicReference<>();
    AtomicLong inflightBytes = new AtomicLong(0);
    BadResourceTracker tracker = new BadResourceTracker();

    for (int i = 0; i < tasks.size(); i++) {
      dispatchWithRetry(
          queryId,
          tasks.get(i),
          i,
          responses,
          latch,
          requestFactory,
          tracker,
          firstError,
          inflightBytes,
          maxRetries);
    }

    try {
      if (!latch.await(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
        throw new RuntimeException("Timed out waiting for fragment execution for query " + queryId);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted waiting for query " + queryId, e);
    }

    Exception error = firstError.get();
    if (error != null) {
      throw new RuntimeException("Query " + queryId + " failed", error);
    }

    return responses;
  }

  private void dispatchWithRetry(
      QueryId queryId,
      TaskDescriptor task,
      int index,
      List<ExecuteFragmentResponse> responses,
      CountDownLatch latch,
      RequestFactory requestFactory,
      BadResourceTracker tracker,
      AtomicReference<Exception> firstError,
      AtomicLong inflightBytes,
      int retriesLeft) {

    ExecuteFragmentRequest request = requestFactory.create(task);
    request.setProfileEnabled(profileEnabled);

    transportService.sendRequest(
        task.getTargetNode(),
        ExecuteFragmentAction.NAME,
        request,
        new TransportResponseHandler<ExecuteFragmentResponse>() {
          @Override
          public ExecuteFragmentResponse read(StreamInput in) throws IOException {
            return new ExecuteFragmentResponse(in);
          }

          @Override
          public void handleResponse(ExecuteFragmentResponse response) {
            if (response.getStatus() == ExecuteFragmentResponse.Status.SUCCESS) {
              long added = responseBytes(response);
              long total = inflightBytes.addAndGet(added);
              if (total > inflightCapBytes) {
                ResultTooLargeException resourceError =
                    new ResultTooLargeException(
                        "Aggregate fragment response size "
                            + total
                            + " exceeded plugins.velox.coordinator_inflight_bytes cap "
                            + inflightCapBytes);
                logger.warn(
                    "Cancelling query {}: aggregate response bytes {} exceeded cap {}",
                    queryId,
                    total,
                    inflightCapBytes);
                firstError.compareAndSet(null, resourceError);
                scheduler.onTaskFailed(queryId, task.getTaskId(), resourceError.getMessage());
                // Still countDown so latch completes for the remaining responses.
                latch.countDown();
                return;
              }
              responses.set(index, response);
              scheduler.onTaskCompleted(queryId, task.getTaskId());
              latch.countDown();
              return;
            }

            // Task failed — classify and decide retry
            ErrorCategory category = ErrorClassifier.classify(response.getErrorMessage());
            if (retriesLeft > 0
                && category != ErrorCategory.FATAL
                && category != ErrorCategory.RESOURCE_EXCEEDED) {
              logger.warn(
                  "Task {} failed ({}), retrying ({} left): {}",
                  task.getTaskId(),
                  category,
                  retriesLeft,
                  response.getErrorMessage());
              TaskDescriptor retryTask = prepareRetry(task, category, tracker);
              if (retryTask != null) {
                dispatchWithRetry(
                    queryId,
                    retryTask,
                    index,
                    responses,
                    latch,
                    requestFactory,
                    tracker,
                    firstError,
                    inflightBytes,
                    retriesLeft - 1);
                return;
              }
              logger.warn("No alternative node for task {}, failing", task.getTaskId());
            }

            // No retry — record failure
            responses.set(index, response);
            scheduler.onTaskFailed(queryId, task.getTaskId(), response.getErrorMessage());
            firstError.compareAndSet(null, new RuntimeException(response.getErrorMessage()));
            latch.countDown();
          }

          @Override
          public void handleException(TransportException exp) {
            // Transport exception — node likely unreachable
            ErrorCategory category = ErrorClassifier.classify(exp);
            if (retriesLeft > 0
                && category != ErrorCategory.FATAL
                && category != ErrorCategory.RESOURCE_EXCEEDED) {
              logger.warn(
                  "Transport error for task {} ({}), retrying ({} left): {}",
                  task.getTaskId(),
                  category,
                  retriesLeft,
                  exp.getMessage());
              TaskDescriptor retryTask = prepareRetry(task, category, tracker);
              if (retryTask != null) {
                dispatchWithRetry(
                    queryId,
                    retryTask,
                    index,
                    responses,
                    latch,
                    requestFactory,
                    tracker,
                    firstError,
                    inflightBytes,
                    retriesLeft - 1);
                return;
              }
              logger.warn("No alternative node for task {}, failing", task.getTaskId());
            }

            logger.error("Task {} failed permanently: {}", task.getTaskId(), exp.getMessage());
            scheduler.onTaskFailed(queryId, task.getTaskId(), exp.getMessage());
            firstError.compareAndSet(null, exp);
            latch.countDown();
          }

          @Override
          public String executor() {
            return ThreadPool.Names.SEARCH;
          }
        });
  }

  /**
   * Prepare a retry by marking bad resources and finding an alternative node. Returns a new
   * TaskDescriptor targeting a different node, or null if no alternative is available.
   */
  private TaskDescriptor prepareRetry(
      TaskDescriptor failed, ErrorCategory category, BadResourceTracker tracker) {
    String failedNodeId = failed.getTargetNode().getId();

    switch (category) {
      case RETRYABLE_NODE:
        tracker.addBadNode(failedNodeId);
        break;
      case RETRYABLE_SHARD:
        for (ShardId shardId : failed.getShardIds()) {
          tracker.addBadShardOnNode(failedNodeId, shardId);
        }
        break;
      case RETRYABLE_TRANSIENT:
        // Transient error — retry on same node (don't mark as bad)
        return failed;
      default:
        return null;
    }

    // Find alternative routing excluding bad resources
    String sourceIndex = failed.getFragment().getProperties().getSourceIndex();
    if (sourceIndex == null) {
      return null;
    }

    try {
      ShardRouter router = new ShardRouter();
      Map<DiscoveryNode, List<ShardId>> newRouting =
          router.routeShardsWithExclusions(
              scheduler.getClusterService().state(), sourceIndex, tracker);

      // Find the node that covers the most of the failed task's shards.
      // Replicas may be spread across multiple nodes — pick the best single node.
      // Any shards not covered will be lost, but this is better than failing entirely.
      Set<ShardId> failedShards = new HashSet<>(failed.getShardIds());
      DiscoveryNode bestNode = null;
      List<ShardId> bestShards = Collections.emptyList();

      for (Map.Entry<DiscoveryNode, List<ShardId>> entry : newRouting.entrySet()) {
        List<ShardId> overlap = new ArrayList<>();
        for (ShardId s : entry.getValue()) {
          if (failedShards.contains(s)) {
            overlap.add(s);
          }
        }
        if (overlap.size() > bestShards.size()) {
          bestNode = entry.getKey();
          bestShards = overlap;
        }
      }

      if (bestNode != null && !bestShards.isEmpty()) {
        logger.info(
            "Retrying task {} on node {} ({}/{} shards, was {})",
            failed.getTaskId(),
            bestNode.getName(),
            bestShards.size(),
            failed.getShardIds().size(),
            failed.getTargetNode().getName());
        return new TaskDescriptor(failed.getTaskId(), failed.getFragment(), bestNode, bestShards);
      }
    } catch (Exception e) {
      logger.warn("Failed to find alternative node for retry: {}", e.getMessage());
    }

    return null;
  }

  private ExecuteFragmentRequest createNormalRequest(QueryId queryId, TaskDescriptor task) {
    return new ExecuteFragmentRequest(
        queryId.getId(),
        task.getFragment().getFragmentId(),
        task.getTaskId().getPartitionId(),
        serializePlanFragment(task),
        task.getShardIds(),
        task.getFragment().getProperties().getSourceIndex());
  }

  private String serializePlanFragment(TaskDescriptor task) {
    Query query =
        new Query(task.getFragment().getPlanRoot(), Config.empty(), ConnectorConfig.empty());
    return Serde.toJson(query);
  }
}
