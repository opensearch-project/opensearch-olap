/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.core.index.shard.ShardId;

/**
 * Routes plan fragments to data nodes based on shard assignments.
 *
 * <p>Uses the cluster routing table to determine which nodes own which shards for a given index.
 * Groups shards by their owning node so that each task processes all local shards on a single node.
 */
public class ShardRouter {

  /**
   * For a given index, group all active primary shards by their owning node.
   *
   * @return Map from DiscoveryNode to the list of ShardIds on that node
   */
  public Map<DiscoveryNode, List<ShardId>> routeShards(
      ClusterState clusterState, String indexName) {
    return routeShardsWithExclusions(clusterState, indexName, null);
  }

  /**
   * Route shards with bad resource exclusion. Skips nodes in the tracker's bad node list and shards
   * marked bad on specific nodes. Falls back to replicas when available.
   *
   * @param tracker bad resource tracker (null for no exclusions)
   * @return Map from DiscoveryNode to the list of ShardIds on that node
   */
  public Map<DiscoveryNode, List<ShardId>> routeShardsWithExclusions(
      ClusterState clusterState, String indexName, BadResourceTracker tracker) {
    Map<DiscoveryNode, List<ShardId>> nodeToShards = new HashMap<>();

    IndexRoutingTable indexRouting = clusterState.getRoutingTable().index(indexName);
    if (indexRouting == null) {
      throw new IllegalArgumentException("Index not found in routing table: " + indexName);
    }

    for (int shardNum = 0; shardNum < indexRouting.shards().size(); shardNum++) {
      IndexShardRoutingTable shardRoutingTable = indexRouting.shard(shardNum);
      ShardRouting selected = selectShard(clusterState, shardRoutingTable, tracker);

      if (selected == null) {
        throw new IllegalStateException(
            "No available shards for "
                + indexName
                + "["
                + shardNum
                + "] (all nodes/replicas excluded or unavailable)");
      }

      DiscoveryNode node = clusterState.nodes().get(selected.currentNodeId());
      if (node == null) {
        throw new IllegalStateException("Node not found: " + selected.currentNodeId());
      }

      nodeToShards.computeIfAbsent(node, k -> new ArrayList<>()).add(selected.shardId());
    }

    return nodeToShards;
  }

  /**
   * Select the best shard copy, skipping bad nodes and bad shards. Prefers primary, falls back to
   * replicas, respects exclusions.
   */
  private ShardRouting selectShard(
      ClusterState clusterState,
      IndexShardRoutingTable shardRoutingTable,
      BadResourceTracker tracker) {

    // Try primary first
    ShardRouting primary = shardRoutingTable.primaryShard();
    if (primary != null && primary.active() && isAcceptable(primary, tracker)) {
      return primary;
    }

    // Fall back to any active shard not excluded by the tracker
    for (ShardRouting shard : shardRoutingTable.activeShards()) {
      if (isAcceptable(shard, tracker)) {
        return shard;
      }
    }

    // No acceptable shard found
    return null;
  }

  private boolean isAcceptable(ShardRouting shard, BadResourceTracker tracker) {
    if (tracker == null) {
      return true;
    }
    String nodeId = shard.currentNodeId();
    if (tracker.isNodeBad(nodeId)) {
      return false;
    }
    return !tracker.isShardBadOnNode(nodeId, shard.shardId());
  }
}
