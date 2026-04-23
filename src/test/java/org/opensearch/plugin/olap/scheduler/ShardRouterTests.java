/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.scheduler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

public class ShardRouterTests extends OpenSearchTestCase {

  private static ShardId shard(String indexName, int id) {
    return new ShardId(new Index(indexName, "_na_"), id);
  }

  private static ShardRouting routing(
      String indexName, int shardId, String nodeId, boolean active) {
    ShardRouting s = mock(ShardRouting.class);
    when(s.shardId()).thenReturn(shard(indexName, shardId));
    when(s.currentNodeId()).thenReturn(nodeId);
    when(s.active()).thenReturn(active);
    return s;
  }

  private static IndexShardRoutingTable shardTable(
      ShardRouting primary, List<ShardRouting> active) {
    IndexShardRoutingTable t = mock(IndexShardRoutingTable.class);
    when(t.primaryShard()).thenReturn(primary);
    when(t.activeShards()).thenReturn(active);
    return t;
  }

  private static IndexRoutingTable indexTable(List<IndexShardRoutingTable> shards) {
    IndexRoutingTable irt = mock(IndexRoutingTable.class);
    Map<Integer, IndexShardRoutingTable> shardMap = new java.util.LinkedHashMap<>();
    for (int i = 0; i < shards.size(); i++) {
      shardMap.put(i, shards.get(i));
      when(irt.shard(i)).thenReturn(shards.get(i));
    }
    when(irt.shards()).thenReturn(shardMap);
    return irt;
  }

  private static ClusterState clusterStateWithIndexes(
      Map<String, IndexRoutingTable> indexes, Map<String, DiscoveryNode> nodes) {
    ClusterState state = mock(ClusterState.class);
    RoutingTable rt = mock(RoutingTable.class);
    indexes.forEach((name, irt) -> when(rt.index(name)).thenReturn(irt));
    when(state.getRoutingTable()).thenReturn(rt);

    DiscoveryNodes dn = mock(DiscoveryNodes.class);
    nodes.forEach((id, node) -> when(dn.get(id)).thenReturn(node));
    when(state.nodes()).thenReturn(dn);
    return state;
  }

  public void testAlignedShardsOnSameNodes() {
    // 2 shards, each index hosted on same node pair: shard0→nodeA, shard1→nodeB.
    DiscoveryNode a = mock(DiscoveryNode.class);
    when(a.getId()).thenReturn("nodeA");
    DiscoveryNode b = mock(DiscoveryNode.class);
    when(b.getId()).thenReturn("nodeB");

    ShardRouting leftPrim0 = routing("left", 0, "nodeA", true);
    ShardRouting leftPrim1 = routing("left", 1, "nodeB", true);
    ShardRouting rightPrim0 = routing("right", 0, "nodeA", true);
    ShardRouting rightPrim1 = routing("right", 1, "nodeB", true);

    IndexRoutingTable leftIdx =
        indexTable(
            List.of(
                shardTable(leftPrim0, List.of(leftPrim0)),
                shardTable(leftPrim1, List.of(leftPrim1))));
    IndexRoutingTable rightIdx =
        indexTable(
            List.of(
                shardTable(rightPrim0, List.of(rightPrim0)),
                shardTable(rightPrim1, List.of(rightPrim1))));

    ClusterState state =
        clusterStateWithIndexes(
            Map.of("left", leftIdx, "right", rightIdx), Map.of("nodeA", a, "nodeB", b));

    List<ShardRouter.CoRoutedShardPair> pairs =
        new ShardRouter().routeCoRoutedPairs(state, "left", "right");

    assertNotNull(pairs);
    assertEquals(2, pairs.size());
    assertSame(a, pairs.get(0).node);
    assertEquals(0, pairs.get(0).leftShard.id());
    assertEquals(0, pairs.get(0).rightShard.id());
    assertSame(b, pairs.get(1).node);
    assertEquals(1, pairs.get(1).leftShard.id());
  }

  public void testMismatchedShardCountsReturnsNull() {
    ShardRouting leftPrim0 = routing("left", 0, "nodeA", true);
    ShardRouting rightPrim0 = routing("right", 0, "nodeA", true);
    ShardRouting rightPrim1 = routing("right", 1, "nodeA", true);

    IndexRoutingTable leftIdx = indexTable(List.of(shardTable(leftPrim0, List.of(leftPrim0))));
    IndexRoutingTable rightIdx =
        indexTable(
            List.of(
                shardTable(rightPrim0, List.of(rightPrim0)),
                shardTable(rightPrim1, List.of(rightPrim1))));

    DiscoveryNode a = mock(DiscoveryNode.class);
    ClusterState state =
        clusterStateWithIndexes(Map.of("left", leftIdx, "right", rightIdx), Map.of("nodeA", a));

    List<ShardRouter.CoRoutedShardPair> pairs =
        new ShardRouter().routeCoRoutedPairs(state, "left", "right");
    assertNull(pairs);
  }

  public void testDriftedAllocationReturnsNull() {
    // Shard counts match, but shard 0 of the two indexes sits on different nodes with no replica
    // overlap → cannot align → null (caller falls back to BROADCAST/HASH_SHUFFLE).
    DiscoveryNode a = mock(DiscoveryNode.class);
    DiscoveryNode b = mock(DiscoveryNode.class);

    ShardRouting leftPrim0 = routing("left", 0, "nodeA", true);
    ShardRouting rightPrim0 = routing("right", 0, "nodeB", true);

    IndexRoutingTable leftIdx = indexTable(List.of(shardTable(leftPrim0, List.of(leftPrim0))));
    IndexRoutingTable rightIdx = indexTable(List.of(shardTable(rightPrim0, List.of(rightPrim0))));

    ClusterState state =
        clusterStateWithIndexes(
            Map.of("left", leftIdx, "right", rightIdx), Map.of("nodeA", a, "nodeB", b));

    List<ShardRouter.CoRoutedShardPair> pairs =
        new ShardRouter().routeCoRoutedPairs(state, "left", "right");
    assertNull(pairs);
  }

  public void testReplicaFallbackFindsColocation() {
    // Left primary on nodeA; right primary on nodeB, but right has a replica on nodeA → aligned.
    DiscoveryNode a = mock(DiscoveryNode.class);
    DiscoveryNode b = mock(DiscoveryNode.class);

    ShardRouting leftPrim = routing("left", 0, "nodeA", true);
    ShardRouting rightPrim = routing("right", 0, "nodeB", true);
    ShardRouting rightRepl = routing("right", 0, "nodeA", true);

    IndexRoutingTable leftIdx = indexTable(List.of(shardTable(leftPrim, List.of(leftPrim))));
    IndexRoutingTable rightIdx =
        indexTable(List.of(shardTable(rightPrim, List.of(rightPrim, rightRepl))));

    ClusterState state =
        clusterStateWithIndexes(
            Map.of("left", leftIdx, "right", rightIdx), Map.of("nodeA", a, "nodeB", b));

    List<ShardRouter.CoRoutedShardPair> pairs =
        new ShardRouter().routeCoRoutedPairs(state, "left", "right");
    assertNotNull(pairs);
    assertEquals(1, pairs.size());
    assertSame(a, pairs.get(0).node);
  }

  public void testMissingIndexReturnsNull() {
    DiscoveryNode a = mock(DiscoveryNode.class);
    ShardRouting leftPrim = routing("left", 0, "nodeA", true);
    IndexRoutingTable leftIdx = indexTable(List.of(shardTable(leftPrim, List.of(leftPrim))));

    ClusterState state = clusterStateWithIndexes(Map.of("left", leftIdx), Map.of("nodeA", a));
    // Right index not in routing table.
    assertNull(new ShardRouter().routeCoRoutedPairs(state, "left", "right"));
  }
}
