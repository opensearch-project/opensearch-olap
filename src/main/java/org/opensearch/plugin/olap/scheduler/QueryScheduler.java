/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.scheduler;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.plugin.olap.common.QueryId;
import org.opensearch.plugin.olap.plan.fragment.FragmentProperties;
import org.opensearch.plugin.olap.plan.fragment.PlanFragment;
import org.opensearch.threadpool.ThreadPool;

/**
 * Central query scheduler that orchestrates distributed query execution.
 *
 * <p>The scheduler converts plan fragments into stages, creates tasks for each stage based on shard
 * routing, and dispatches tasks to target data nodes.
 *
 * <p>Execution flow:
 *
 * <ol>
 *   <li>Receive fragmented plan from VeloxPlanConverter + PlanFragmenter
 *   <li>Build stages and create tasks via ShardRouter
 *   <li>Dispatch leaf-stage tasks to data nodes (via TransportService)
 *   <li>When leaf tasks complete, dispatch dependent stages
 *   <li>Collect final results from root stage
 * </ol>
 *
 * <p>Inspired by Presto's SqlQueryScheduler which manages stage lifecycle and coordinates task
 * creation across nodes. Key differences:
 *
 * <ul>
 *   <li>Uses OpenSearch TransportService instead of HTTP for task dispatch
 *   <li>Shard routing is derived from OpenSearch ClusterState
 *   <li>Tasks feed Lucene data via ExternalStream (no Hive splits)
 * </ul>
 */
public class QueryScheduler {

  private static final Logger logger = LogManager.getLogger(QueryScheduler.class);

  private final ClusterService clusterService;
  private final ThreadPool threadPool;
  private final ShardRouter shardRouter;
  private final Map<QueryId, QueryExecution> activeQueries;

  public QueryScheduler(ClusterService clusterService, ThreadPool threadPool) {
    this.clusterService = clusterService;
    this.threadPool = threadPool;
    this.shardRouter = new ShardRouter();
    this.activeQueries = new ConcurrentHashMap<>();
  }

  /**
   * Schedule a fragmented query for execution.
   *
   * @param fragments Ordered list of plan fragments (leaf-first, root-last)
   * @param policy Execution ordering policy
   * @return QueryExecution handle for tracking progress
   */
  public QueryExecution schedule(List<PlanFragment> fragments, ExecutionPolicy policy) {
    QueryId queryId = QueryId.generate();
    QueryExecution execution = new QueryExecution(queryId, fragments);
    activeQueries.put(queryId, execution);

    try {
      execution.buildStages();
      execution.setState(QueryExecution.State.SCHEDULING);

      // Create tasks for each stage based on shard routing
      for (Stage stage : execution.getStages()) {
        createTasksForStage(execution, stage);
      }

      execution.setState(QueryExecution.State.RUNNING);

      // Dispatch tasks according to execution policy
      if (policy == ExecutionPolicy.ALL_AT_ONCE) {
        dispatchAllStages(execution);
      } else {
        dispatchLeafStages(execution);
      }

    } catch (Exception e) {
      logger.error("Failed to schedule query {}", queryId, e);
      execution.fail(e);
    }

    return execution;
  }

  /**
   * Called when a task completes on a data node. Advances the query execution by dispatching ready
   * stages.
   */
  public void onTaskCompleted(QueryId queryId, TaskId taskId) {
    QueryExecution execution = activeQueries.get(queryId);
    if (execution == null) {
      logger.warn("Received completion for unknown query: {}", queryId);
      return;
    }

    execution.getTaskTracker().updateState(taskId, TaskState.FINISHED);

    // Update stage state
    Stage stage = execution.getStage(taskId.getStageId().getStageNumber());
    if (stage != null) {
      stage.updateStateFromTasks();

      // If stage finished, check if downstream stages are ready
      if (stage.getState() == Stage.StageState.FINISHED) {
        List<Stage> readyStages = execution.getReadyStages();
        for (Stage readyStage : readyStages) {
          dispatchStage(execution, readyStage);
        }
      }
    }

    // Check if all stages are done
    if (execution.getStages().stream().allMatch(s -> s.getState() == Stage.StageState.FINISHED)) {
      execution.setState(QueryExecution.State.FINISHED);
      activeQueries.remove(queryId);
    }
  }

  /** Called when a task fails on a data node. */
  public void onTaskFailed(QueryId queryId, TaskId taskId, String reason) {
    QueryExecution execution = activeQueries.get(queryId);
    if (execution == null) {
      return;
    }

    execution.getTaskTracker().markFailed(taskId, reason);
    execution.fail(new RuntimeException("Task " + taskId + " failed: " + reason));
    activeQueries.remove(queryId);
  }

  public QueryExecution getExecution(QueryId queryId) {
    return activeQueries.get(queryId);
  }

  private void createTasksForStage(QueryExecution execution, Stage stage) {
    PlanFragment fragment = stage.getFragment();
    FragmentProperties props = fragment.getProperties();

    if (props.getDistribution() == FragmentProperties.Distribution.SOURCE) {
      // Route to nodes owning the source index shards
      String indexName = props.getSourceIndex();
      Map<DiscoveryNode, List<ShardId>> routing =
          shardRouter.routeShards(clusterService.state(), indexName);

      int partitionId = 0;
      for (Map.Entry<DiscoveryNode, List<ShardId>> entry : routing.entrySet()) {
        TaskId taskId = new TaskId(stage.getStageId(), partitionId++);
        TaskDescriptor task =
            new TaskDescriptor(taskId, fragment, entry.getKey(), entry.getValue());
        stage.addTask(task);
        execution.getTaskTracker().register(task);
      }
    } else if (props.getDistribution() == FragmentProperties.Distribution.COORDINATOR) {
      // Single task on the coordinator node
      DiscoveryNode localNode = clusterService.localNode();
      TaskId taskId = new TaskId(stage.getStageId(), 0);
      TaskDescriptor task = new TaskDescriptor(taskId, fragment, localNode, List.of());
      stage.addTask(task);
      execution.getTaskTracker().register(task);
    }

    logger.debug("Created {} tasks for stage {}", stage.getTasks().size(), stage.getStageId());
  }

  private void dispatchAllStages(QueryExecution execution) {
    for (Stage stage : execution.getStages()) {
      dispatchStage(execution, stage);
    }
  }

  private void dispatchLeafStages(QueryExecution execution) {
    for (Stage stage : execution.getLeafStages()) {
      dispatchStage(execution, stage);
    }
  }

  private void dispatchStage(QueryExecution execution, Stage stage) {
    if (!stage.transitionTo(Stage.StageState.SCHEDULING)) {
      return;
    }

    logger.info("Dispatching stage {} with {} tasks", stage.getStageId(), stage.getTasks().size());

    for (TaskDescriptor task : stage.getTasks()) {
      dispatchTask(execution.getQueryId(), task);
    }

    stage.transitionTo(Stage.StageState.RUNNING);
  }

  /**
   * Dispatch a task to its target node via TransportService. This method is called from the
   * scheduler and sends ExecuteFragmentRequest to the target data node.
   *
   * <p>The actual transport dispatch is handled by NodeResultCollector which holds a reference to
   * TransportService.
   */
  private void dispatchTask(QueryId queryId, TaskDescriptor task) {
    task.setState(TaskState.RUNNING);

    // The actual dispatch is performed by the transport layer.
    // This will be wired via NodeResultCollector.dispatch() which sends
    // ExecuteFragmentRequest via TransportService to the target node.
    logger.debug(
        "Dispatching task {} to node {}", task.getTaskId(), task.getTargetNode().getName());
  }
}
