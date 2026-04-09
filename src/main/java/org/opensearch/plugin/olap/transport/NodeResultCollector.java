/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.transport;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.boostscale.velox4j.config.Config;
import org.boostscale.velox4j.config.ConnectorConfig;
import org.boostscale.velox4j.query.Query;
import org.boostscale.velox4j.serde.Serde;
import org.opensearch.plugin.olap.common.QueryId;
import org.opensearch.plugin.olap.scheduler.QueryExecution;
import org.opensearch.plugin.olap.scheduler.QueryScheduler;
import org.opensearch.plugin.olap.scheduler.TaskDescriptor;
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

  public NodeResultCollector(TransportService transportService, QueryScheduler scheduler) {
    this.transportService = transportService;
    this.scheduler = scheduler;
  }

  /** Dispatch all tasks for a query execution and collect results. */
  public List<ExecuteFragmentResponse> dispatchAndCollect(QueryExecution execution) {
    return dispatchAndCollect(execution, execution.getStages());
  }

  /** Dispatch tasks for specific stages and collect results. */
  public List<ExecuteFragmentResponse> dispatchAndCollect(
      QueryExecution execution, List<org.opensearch.plugin.olap.scheduler.Stage> stages) {
    QueryId queryId = execution.getQueryId();
    List<TaskDescriptor> allTasks = new ArrayList<>();
    for (var stage : stages) {
      allTasks.addAll(stage.getTasks());
    }

    return dispatchTasks(queryId, allTasks, task -> createNormalRequest(queryId, task));
  }

  /** Dispatch broadcast join tasks: each task receives the broadcast data along with the plan. */
  public List<ExecuteFragmentResponse> dispatchAndCollectBroadcast(
      QueryExecution execution,
      List<org.opensearch.plugin.olap.scheduler.Stage> stages,
      List<byte[]> broadcastData) {
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
          request.setBroadcastData(broadcastData);
          return request;
        });
  }

  /**
   * Dispatch shuffle scan tasks: each task receives the shuffle config telling it how to partition
   * and where to send data.
   */
  public List<ExecuteFragmentResponse> dispatchAndCollectShuffle(
      QueryExecution execution,
      List<org.opensearch.plugin.olap.scheduler.Stage> stages,
      List<String> workerNodeIds,
      int targetStageId) {
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
      List<org.opensearch.plugin.olap.scheduler.Stage> stages,
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
        new ArrayList<>(java.util.Collections.nCopies(tasks.size(), null));
    AtomicReference<Exception> firstError = new AtomicReference<>();

    for (int i = 0; i < tasks.size(); i++) {
      final int index = i;
      TaskDescriptor task = tasks.get(i);
      ExecuteFragmentRequest request = requestFactory.create(task);

      transportService.sendRequest(
          task.getTargetNode(),
          ExecuteFragmentAction.NAME,
          request,
          new org.opensearch.transport.TransportResponseHandler<ExecuteFragmentResponse>() {
            @Override
            public ExecuteFragmentResponse read(org.opensearch.core.common.io.stream.StreamInput in)
                throws java.io.IOException {
              return new ExecuteFragmentResponse(in);
            }

            @Override
            public void handleResponse(ExecuteFragmentResponse response) {
              responses.set(index, response);
              if (response.getStatus() == ExecuteFragmentResponse.Status.SUCCESS) {
                scheduler.onTaskCompleted(queryId, task.getTaskId());
              } else {
                scheduler.onTaskFailed(queryId, task.getTaskId(), response.getErrorMessage());
                firstError.compareAndSet(null, new RuntimeException(response.getErrorMessage()));
              }
              latch.countDown();
            }

            @Override
            public void handleException(org.opensearch.transport.TransportException exp) {
              logger.error("Transport error for task {}", task.getTaskId(), exp);
              scheduler.onTaskFailed(queryId, task.getTaskId(), exp.getMessage());
              firstError.compareAndSet(null, exp);
              latch.countDown();
            }

            @Override
            public String executor() {
              return org.opensearch.threadpool.ThreadPool.Names.SEARCH;
            }
          });
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
