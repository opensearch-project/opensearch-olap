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

  /**
   * For a Co-Routing join, match up shard {@code i} of the left index with shard {@code i} of the
   * right index on a node that hosts both copies. Returns one {@link CoRoutedShardPair} per shard
   * index. Both indexes must have identical {@code number_of_shards}; if not, returns null so the
   * caller can fall back to BROADCAST/HASH_SHUFFLE.
   *
   * <p>A pair is only emitted when a single node hosts <em>both</em> shard {@code i} copies — that
   * is the invariant that lets us join locally with zero shuffle. If any shard index cannot be
   * aligned on a common node (e.g. replicas diverged due to allocation), the whole join is declared
   * misaligned and null is returned.
   */
  public List<CoRoutedShardPair> routeCoRoutedPairs(
      ClusterState clusterState, String leftIndex, String rightIndex) {
    IndexRoutingTable left = clusterState.getRoutingTable().index(leftIndex);
    IndexRoutingTable right = clusterState.getRoutingTable().index(rightIndex);
    if (left == null || right == null) {
      return null;
    }
    int leftCount = left.shards().size();
    int rightCount = right.shards().size();
    if (leftCount != rightCount) {
      return null;
    }

    List<CoRoutedShardPair> pairs = new ArrayList<>(leftCount);
    for (int i = 0; i < leftCount; i++) {
      IndexShardRoutingTable leftTable = left.shard(i);
      IndexShardRoutingTable rightTable = right.shard(i);
      ColocatedShard colocated = findColocatedShardCopy(clusterState, leftTable, rightTable);
      if (colocated == null) {
        return null;
      }
      pairs.add(
          new CoRoutedShardPair(
              colocated.node, colocated.leftShard.shardId(), colocated.rightShard.shardId()));
    }
    return pairs;
  }

  /**
   * Find a node that hosts an active copy of both shards. Prefers the primary pair, then any active
   * replica that's co-located.
   */
  private ColocatedShard findColocatedShardCopy(
      ClusterState clusterState,
      IndexShardRoutingTable leftTable,
      IndexShardRoutingTable rightTable) {
    ShardRouting leftPrimary = leftTable.primaryShard();
    ShardRouting rightPrimary = rightTable.primaryShard();
    if (leftPrimary != null
        && rightPrimary != null
        && leftPrimary.active()
        && rightPrimary.active()
        && leftPrimary.currentNodeId().equals(rightPrimary.currentNodeId())) {
      DiscoveryNode node = clusterState.nodes().get(leftPrimary.currentNodeId());
      if (node != null) {
        return new ColocatedShard(node, leftPrimary, rightPrimary);
      }
    }
    for (ShardRouting leftShard : leftTable.activeShards()) {
      for (ShardRouting rightShard : rightTable.activeShards()) {
        if (leftShard.currentNodeId().equals(rightShard.currentNodeId())) {
          DiscoveryNode node = clusterState.nodes().get(leftShard.currentNodeId());
          if (node != null) {
            return new ColocatedShard(node, leftShard, rightShard);
          }
        }
      }
    }
    return null;
  }

  private static final class ColocatedShard {
    final DiscoveryNode node;
    final ShardRouting leftShard;
    final ShardRouting rightShard;

    ColocatedShard(DiscoveryNode node, ShardRouting leftShard, ShardRouting rightShard) {
      this.node = node;
      this.leftShard = leftShard;
      this.rightShard = rightShard;
    }
  }

  /** One aligned shard pair for a Co-Routing join. */
  public static final class CoRoutedShardPair {
    public final DiscoveryNode node;
    public final ShardId leftShard;
    public final ShardId rightShard;

    public CoRoutedShardPair(DiscoveryNode node, ShardId leftShard, ShardId rightShard) {
      this.node = node;
      this.leftShard = leftShard;
      this.rightShard = rightShard;
    }
  }
}
