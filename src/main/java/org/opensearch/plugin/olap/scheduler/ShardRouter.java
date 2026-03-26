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
    Map<DiscoveryNode, List<ShardId>> nodeToShards = new HashMap<>();

    IndexRoutingTable indexRouting = clusterState.getRoutingTable().index(indexName);
    if (indexRouting == null) {
      throw new IllegalArgumentException("Index not found in routing table: " + indexName);
    }

    for (int shardNum = 0; shardNum < indexRouting.shards().size(); shardNum++) {
      IndexShardRoutingTable shardRoutingTable = indexRouting.shard(shardNum);

      // Prefer the primary shard for consistent reads
      ShardRouting primaryShard = shardRoutingTable.primaryShard();
      if (primaryShard == null || !primaryShard.active()) {
        // Fallback to any active shard
        List<ShardRouting> activeShards = shardRoutingTable.activeShards();
        if (activeShards.isEmpty()) {
          throw new IllegalStateException(
              "No active shards for " + indexName + "[" + shardNum + "]");
        }
        primaryShard = activeShards.get(0);
      }

      String nodeId = primaryShard.currentNodeId();
      DiscoveryNode node = clusterState.nodes().get(nodeId);
      if (node == null) {
        throw new IllegalStateException("Node not found: " + nodeId);
      }

      nodeToShards.computeIfAbsent(node, k -> new ArrayList<>()).add(primaryShard.shardId());
    }

    return nodeToShards;
  }
}
