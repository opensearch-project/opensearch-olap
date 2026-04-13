/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.scheduler;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.opensearch.core.index.shard.ShardId;

/**
 * Tracks nodes and shards that have failed during query execution. Used by retry logic to exclude
 * bad resources when reassigning tasks to alternative nodes or replicas.
 *
 * <p>Thread-safe — multiple retry callbacks may update concurrently.
 */
public class BadResourceTracker {

  private final Set<String> badNodeIds = ConcurrentHashMap.newKeySet();
  private final Map<String, Set<ShardId>> badShardsPerNode = new ConcurrentHashMap<>();

  /** Mark a node as bad (unreachable). All shards on this node are excluded. */
  public void addBadNode(String nodeId) {
    badNodeIds.add(nodeId);
  }

  /**
   * Mark a specific shard as bad on a specific node. Other replicas on other nodes are still OK.
   */
  public void addBadShardOnNode(String nodeId, ShardId shardId) {
    badShardsPerNode.computeIfAbsent(nodeId, k -> ConcurrentHashMap.newKeySet()).add(shardId);
  }

  public boolean isNodeBad(String nodeId) {
    return badNodeIds.contains(nodeId);
  }

  public boolean isShardBadOnNode(String nodeId, ShardId shardId) {
    Set<ShardId> badShards = badShardsPerNode.get(nodeId);
    return badShards != null && badShards.contains(shardId);
  }

  public Set<String> getBadNodeIds() {
    return badNodeIds;
  }

  public boolean isEmpty() {
    return badNodeIds.isEmpty() && badShardsPerNode.isEmpty();
  }
}
