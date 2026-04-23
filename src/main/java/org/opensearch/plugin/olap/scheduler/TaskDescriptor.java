/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.scheduler;

import java.util.List;
import java.util.Objects;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.plugin.olap.plan.fragment.PlanFragment;

/**
 * Describes a single task to be executed on a specific data node.
 *
 * <p>A task represents the execution of a PlanFragment on a specific node, processing a specific
 * set of shards. The coordinator creates TaskDescriptors and dispatches them to target nodes via
 * TransportService.
 */
public class TaskDescriptor {

  private final TaskId taskId;
  private final PlanFragment fragment;
  private final DiscoveryNode targetNode;
  private final List<ShardId> shardIds;
  private final boolean pinnedToNode;
  private volatile TaskState state;
  private volatile String failureReason;

  public TaskDescriptor(
      TaskId taskId, PlanFragment fragment, DiscoveryNode targetNode, List<ShardId> shardIds) {
    this(taskId, fragment, targetNode, shardIds, false);
  }

  /**
   * @param pinnedToNode when true, the task requires colocation that a reroute cannot preserve
   *     (e.g. Co-Routing — shard {@code i} of two indexes must live on the same node). The retry
   *     path skips shard-level rerouting for pinned tasks; same-node transient retry is still
   *     allowed.
   */
  public TaskDescriptor(
      TaskId taskId,
      PlanFragment fragment,
      DiscoveryNode targetNode,
      List<ShardId> shardIds,
      boolean pinnedToNode) {
    this.taskId = Objects.requireNonNull(taskId);
    this.fragment = Objects.requireNonNull(fragment);
    this.targetNode = Objects.requireNonNull(targetNode);
    this.shardIds = Objects.requireNonNull(shardIds);
    this.pinnedToNode = pinnedToNode;
    this.state = TaskState.PENDING;
  }

  public boolean isPinnedToNode() {
    return pinnedToNode;
  }

  public TaskId getTaskId() {
    return taskId;
  }

  public PlanFragment getFragment() {
    return fragment;
  }

  public DiscoveryNode getTargetNode() {
    return targetNode;
  }

  public List<ShardId> getShardIds() {
    return shardIds;
  }

  public TaskState getState() {
    return state;
  }

  public void setState(TaskState state) {
    this.state = state;
  }

  public String getFailureReason() {
    return failureReason;
  }

  public void setFailureReason(String failureReason) {
    this.failureReason = failureReason;
  }

  @Override
  public String toString() {
    return "TaskDescriptor{"
        + "taskId="
        + taskId
        + ", node="
        + targetNode.getName()
        + ", shards="
        + shardIds.size()
        + ", state="
        + state
        + '}';
  }
}
