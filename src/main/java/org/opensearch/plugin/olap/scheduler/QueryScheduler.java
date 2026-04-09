/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.scheduler;

import java.util.ArrayList;
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
 * <p>Supports scheduling for:
 *
 * <ul>
 *   <li>SOURCE: route to nodes owning index shards
 *   <li>COORDINATOR: single task on local coordinator node
 *   <li>BROADCAST: route to nodes owning probe-side shards (with broadcast data)
 *   <li>HASH_PARTITIONED: create N worker tasks distributed across data nodes
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

  public QueryExecution schedule(List<PlanFragment> fragments, ExecutionPolicy policy) {
    QueryId queryId = QueryId.generate();
    QueryExecution execution = new QueryExecution(queryId, fragments);
    activeQueries.put(queryId, execution);

    try {
      execution.buildStages();
      execution.setState(QueryExecution.State.SCHEDULING);

      for (Stage stage : execution.getStages()) {
        createTasksForStage(execution, stage);
      }

      execution.setState(QueryExecution.State.RUNNING);

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

  public void onTaskCompleted(QueryId queryId, TaskId taskId) {
    QueryExecution execution = activeQueries.get(queryId);
    if (execution == null) {
      logger.warn("Received completion for unknown query: {}", queryId);
      return;
    }

    execution.getTaskTracker().updateState(taskId, TaskState.FINISHED);

    Stage stage = execution.getStage(taskId.getStageId().getStageNumber());
    if (stage != null) {
      stage.updateStateFromTasks();

      if (stage.getState() == Stage.StageState.FINISHED) {
        List<Stage> readyStages = execution.getReadyStages();
        for (Stage readyStage : readyStages) {
          dispatchStage(execution, readyStage);
        }
      }
    }

    if (execution.getStages().stream().allMatch(s -> s.getState() == Stage.StageState.FINISHED)) {
      execution.setState(QueryExecution.State.FINISHED);
      activeQueries.remove(queryId);
    }
  }

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

  public ClusterService getClusterService() {
    return clusterService;
  }

  /**
   * Get all data node IDs from the cluster. Used for assigning shuffle workers.
   *
   * @return list of data node IDs
   */
  public List<String> getDataNodeIds() {
    List<String> nodeIds = new ArrayList<>();
    for (DiscoveryNode node : clusterService.state().nodes().getDataNodes().values()) {
      nodeIds.add(node.getId());
    }
    return nodeIds;
  }

  /**
   * Get the shard routing for an index.
   *
   * @return map of node to shard IDs
   */
  public Map<DiscoveryNode, List<ShardId>> routeShards(String indexName) {
    return shardRouter.routeShards(clusterService.state(), indexName);
  }

  private void createTasksForStage(QueryExecution execution, Stage stage) {
    PlanFragment fragment = stage.getFragment();
    FragmentProperties props = fragment.getProperties();

    switch (props.getDistribution()) {
      case SOURCE:
        createSourceTasks(execution, stage, props.getSourceIndex());
        break;
      case COORDINATOR:
        createCoordinatorTask(execution, stage);
        break;
      case BROADCAST:
        // Broadcast fragments run on probe-side shard-owning nodes
        createSourceTasks(execution, stage, props.getSourceIndex());
        break;
      case HASH_PARTITIONED:
        createHashPartitionedTasks(execution, stage, props.getPartitionCount());
        break;
      case ANY:
        createCoordinatorTask(execution, stage);
        break;
    }

    logger.debug("Created {} tasks for stage {}", stage.getTasks().size(), stage.getStageId());
  }

  private void createSourceTasks(QueryExecution execution, Stage stage, String indexName) {
    Map<DiscoveryNode, List<ShardId>> routing =
        shardRouter.routeShards(clusterService.state(), indexName);

    int partitionId = 0;
    for (Map.Entry<DiscoveryNode, List<ShardId>> entry : routing.entrySet()) {
      TaskId taskId = new TaskId(stage.getStageId(), partitionId++);
      TaskDescriptor task =
          new TaskDescriptor(taskId, stage.getFragment(), entry.getKey(), entry.getValue());
      stage.addTask(task);
      execution.getTaskTracker().register(task);
    }
  }

  private void createCoordinatorTask(QueryExecution execution, Stage stage) {
    DiscoveryNode localNode = clusterService.localNode();
    TaskId taskId = new TaskId(stage.getStageId(), 0);
    TaskDescriptor task = new TaskDescriptor(taskId, stage.getFragment(), localNode, List.of());
    stage.addTask(task);
    execution.getTaskTracker().register(task);
  }

  /**
   * Create tasks for a hash-partitioned stage (shuffle join). Distributes N partitions across data
   * nodes round-robin.
   */
  private void createHashPartitionedTasks(
      QueryExecution execution, Stage stage, int partitionCount) {
    List<DiscoveryNode> dataNodes =
        new ArrayList<>(clusterService.state().nodes().getDataNodes().values());
    if (dataNodes.isEmpty()) {
      throw new IllegalStateException("No data nodes available for hash-partitioned execution");
    }

    // If partitionCount == 0, use number of data nodes
    int actualPartitions = partitionCount > 0 ? partitionCount : dataNodes.size();

    for (int i = 0; i < actualPartitions; i++) {
      DiscoveryNode targetNode = dataNodes.get(i % dataNodes.size());
      TaskId taskId = new TaskId(stage.getStageId(), i);
      TaskDescriptor task = new TaskDescriptor(taskId, stage.getFragment(), targetNode, List.of());
      stage.addTask(task);
      execution.getTaskTracker().register(task);
    }
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

  private void dispatchTask(QueryId queryId, TaskDescriptor task) {
    task.setState(TaskState.RUNNING);
    logger.debug(
        "Dispatching task {} to node {}", task.getTaskId(), task.getTargetNode().getName());
  }
}
