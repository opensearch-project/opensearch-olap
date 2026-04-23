/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.scheduler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.plugin.olap.execution.CoRoutedPairs;
import org.opensearch.test.OpenSearchTestCase;

public class CostEstimatorTests extends OpenSearchTestCase {

  private ClusterService mockClusterService(
      String index1, int shards1, String index2, int shards2) {
    ClusterService clusterService = mock(ClusterService.class);
    ClusterState clusterState = mock(ClusterState.class);
    Metadata metadata = mock(Metadata.class);

    IndexMetadata meta1 = mock(IndexMetadata.class);
    when(meta1.getNumberOfShards()).thenReturn(shards1);

    IndexMetadata meta2 = mock(IndexMetadata.class);
    when(meta2.getNumberOfShards()).thenReturn(shards2);

    when(metadata.index(index1)).thenReturn(meta1);
    when(metadata.index(index2)).thenReturn(meta2);
    when(clusterState.metadata()).thenReturn(metadata);
    when(clusterService.state()).thenReturn(clusterState);

    return clusterService;
  }

  public void testSmallRightSideSelectsBroadcast() {
    // left: 10 shards, right: 1 shard, threshold: 2
    ClusterService cs = mockClusterService("orders", 10, "customers", 1);
    CostEstimator estimator = new CostEstimator(cs, 2);

    JoinStrategy strategy = estimator.selectJoinStrategy("orders", "customers");
    assertEquals(JoinStrategy.BROADCAST, strategy);
  }

  public void testSmallLeftSideSelectsBroadcast() {
    // left: 1 shard, right: 10 shards, threshold: 2
    ClusterService cs = mockClusterService("dim_table", 1, "fact_table", 10);
    CostEstimator estimator = new CostEstimator(cs, 2);

    JoinStrategy strategy = estimator.selectJoinStrategy("dim_table", "fact_table");
    assertEquals(JoinStrategy.BROADCAST, strategy);
  }

  public void testBothLargeSidesSelectsHashShuffle() {
    // Both sides have many shards
    ClusterService cs = mockClusterService("orders", 10, "lineitem", 20);
    CostEstimator estimator = new CostEstimator(cs, 2);

    JoinStrategy strategy = estimator.selectJoinStrategy("orders", "lineitem");
    assertEquals(JoinStrategy.HASH_SHUFFLE, strategy);
  }

  public void testExactThresholdSelectsBroadcast() {
    // Smaller side equals threshold → broadcast
    ClusterService cs = mockClusterService("left", 5, "right", 2);
    CostEstimator estimator = new CostEstimator(cs, 2);

    JoinStrategy strategy = estimator.selectJoinStrategy("left", "right");
    assertEquals(JoinStrategy.BROADCAST, strategy);
  }

  public void testAboveThresholdSelectsHashShuffle() {
    // Smaller side is just above threshold
    ClusterService cs = mockClusterService("left", 3, "right", 5);
    CostEstimator estimator = new CostEstimator(cs, 2);

    JoinStrategy strategy = estimator.selectJoinStrategy("left", "right");
    assertEquals(JoinStrategy.HASH_SHUFFLE, strategy);
  }

  public void testSelectBuildSideLeftSmaller() {
    ClusterService cs = mockClusterService("small", 1, "big", 10);
    CostEstimator estimator = new CostEstimator(cs, 2);

    assertEquals("left", estimator.selectBuildSide("small", "big"));
  }

  public void testSelectBuildSideRightSmaller() {
    ClusterService cs = mockClusterService("big", 10, "small", 1);
    CostEstimator estimator = new CostEstimator(cs, 2);

    assertEquals("right", estimator.selectBuildSide("big", "small"));
  }

  public void testSelectBuildSideEqualPicksLeft() {
    ClusterService cs = mockClusterService("a", 5, "b", 5);
    CostEstimator estimator = new CostEstimator(cs, 2);

    assertEquals("left", estimator.selectBuildSide("a", "b"));
  }

  public void testBothMissingIndicesDefaultsToHashShuffle() {
    ClusterService cs = mock(ClusterService.class);
    ClusterState state = mock(ClusterState.class);
    Metadata metadata = mock(Metadata.class);
    when(metadata.index("missing1")).thenReturn(null);
    when(metadata.index("missing2")).thenReturn(null);

    when(state.metadata()).thenReturn(metadata);
    when(cs.state()).thenReturn(state);

    CostEstimator estimator = new CostEstimator(cs, 2);
    // Both "missing" indices return Integer.MAX_VALUE shards → hash shuffle
    JoinStrategy strategy = estimator.selectJoinStrategy("missing1", "missing2");
    assertEquals(JoinStrategy.HASH_SHUFFLE, strategy);
  }

  // ---- CBO: selectBuildSide uses row counts when available ----

  /**
   * Without CBO: both indices have 1 shard → selectBuildSide returns "left" (1 <= 1). With CBO:
   * employees=5 rows, departments=3 rows → selectBuildSide returns "right" (3 < 5). This is the
   * exact scenario that caused the broadcast join filter bug.
   */
  public void testCboBuildSideUsesRowCountsNotShardCount() {
    // Both have 1 shard — shard-count heuristic would pick "left"
    ClusterService cs = mockClusterService("employees", 1, "departments", 1);

    // Without CBO stats
    CostEstimator noCbo = new CostEstimator(cs, 2);
    assertEquals(
        "Without CBO: equal shards → left",
        "left",
        noCbo.selectBuildSide("employees", "departments"));

    // With CBO stats: employees=5 rows, departments=3 rows
    Map<String, TableStatistics> stats =
        Map.of(
            "employees", new TableStatistics("employees", 5, 5000, 1),
            "departments", new TableStatistics("departments", 3, 3000, 1));
    CostEstimator withCbo = new CostEstimator(cs, 2, stats);
    assertEquals(
        "With CBO: departments has fewer rows → right",
        "right",
        withCbo.selectBuildSide("employees", "departments"));
  }

  public void testCboBuildSideLargeVsSmallTable() {
    ClusterService cs = mockClusterService("fact_table", 10, "dim_table", 5);

    // Shard count: fact=10, dim=5 → "right" (fewer shards)
    CostEstimator noCbo = new CostEstimator(cs, 2);
    assertEquals("right", noCbo.selectBuildSide("fact_table", "dim_table"));

    // Row count: fact=1000000, dim=100 → "right" (far fewer rows) — same direction
    Map<String, TableStatistics> stats =
        Map.of(
            "fact_table", new TableStatistics("fact_table", 1000000, 100000000, 10),
            "dim_table", new TableStatistics("dim_table", 100, 10000, 5));
    CostEstimator withCbo = new CostEstimator(cs, 2, stats);
    assertEquals("right", withCbo.selectBuildSide("fact_table", "dim_table"));
  }

  public void testCboBuildSideFlipsWhenRowCountDisagreesWithShardCount() {
    // Shard count says left is smaller (1 shard), but row count says left is MUCH larger
    ClusterService cs = mockClusterService("big_single_shard", 1, "small_multi_shard", 10);

    // Without CBO: 1 shard vs 10 shards → "left" (fewer shards)
    CostEstimator noCbo = new CostEstimator(cs, 2);
    assertEquals("left", noCbo.selectBuildSide("big_single_shard", "small_multi_shard"));

    // With CBO: big=10M rows, small=100 rows → "right" (fewer rows)
    Map<String, TableStatistics> stats =
        Map.of(
            "big_single_shard", new TableStatistics("big_single_shard", 10000000, 1000000000, 1),
            "small_multi_shard", new TableStatistics("small_multi_shard", 100, 10000, 10));
    CostEstimator withCbo = new CostEstimator(cs, 2, stats);
    assertEquals(
        "With CBO: row count overrides shard count",
        "right",
        withCbo.selectBuildSide("big_single_shard", "small_multi_shard"));
  }

  public void testCboFallsBackToShardCountWhenStatsEmpty() {
    ClusterService cs = mockClusterService("left", 1, "right", 10);

    // Empty stats map → falls back to shard count
    CostEstimator estimator = new CostEstimator(cs, 2, Map.of());
    assertEquals("left", estimator.selectBuildSide("left", "right"));
  }

  public void testCboFallsBackWhenOneStatMissing() {
    ClusterService cs = mockClusterService("with_stats", 1, "no_stats", 10);

    // Only one index has stats → one row count is 0 → falls back to shard count
    Map<String, TableStatistics> stats =
        Map.of("with_stats", new TableStatistics("with_stats", 500, 50000, 1));
    CostEstimator estimator = new CostEstimator(cs, 2, stats);
    assertEquals("left", estimator.selectBuildSide("with_stats", "no_stats"));
  }

  public void testCboGetRowCount() {
    ClusterService cs = mockClusterService("test", 1, "test2", 1);
    Map<String, TableStatistics> stats = Map.of("test", new TableStatistics("test", 42, 4200, 1));
    CostEstimator estimator = new CostEstimator(cs, 2, stats);

    assertEquals(42, estimator.getRowCount("test"));
    assertEquals(0, estimator.getRowCount("unknown"));
  }

  public void testOneMissingOneSmallSelectsBroadcast() {
    ClusterService cs = mock(ClusterService.class);
    ClusterState state = mock(ClusterState.class);
    Metadata metadata = mock(Metadata.class);
    when(metadata.index("missing")).thenReturn(null);

    IndexMetadata meta = mock(IndexMetadata.class);
    when(meta.getNumberOfShards()).thenReturn(1);
    when(metadata.index("existing")).thenReturn(meta);

    when(state.metadata()).thenReturn(metadata);
    when(cs.state()).thenReturn(state);

    CostEstimator estimator = new CostEstimator(cs, 2);
    // min(MAX_VALUE, 1) = 1 <= 2, so broadcast (the existing small side)
    JoinStrategy strategy = estimator.selectJoinStrategy("missing", "existing");
    assertEquals(JoinStrategy.BROADCAST, strategy);
  }

  // ---- Co-Routing selection ----

  public void testCoRoutingSelectedWhenEligible() {
    // Both indexes 3 shards, registered as co-routed, same join key.
    ClusterService cs = mockClusterService("orders", 3, "customers", 3);
    CostEstimator estimator = new CostEstimator(cs, 2);
    CoRoutedPairs pairs = CoRoutedPairs.parse(List.of("orders:customer_id,customers:customer_id"));

    JoinStrategy s =
        estimator.selectJoinStrategy(
            "orders", "customers", "customer_id", "customer_id", true, pairs);
    assertEquals(JoinStrategy.CO_ROUTING, s);
  }

  public void testCoRoutingFallsBackWhenShardCountsDiffer() {
    // Both registered, but shards mismatch → fall back (to HASH_SHUFFLE here since 3 > 2)
    ClusterService cs = mockClusterService("orders", 3, "customers", 4);
    CostEstimator estimator = new CostEstimator(cs, 2);
    CoRoutedPairs pairs = CoRoutedPairs.parse(List.of("orders:customer_id,customers:customer_id"));

    JoinStrategy s =
        estimator.selectJoinStrategy(
            "orders", "customers", "customer_id", "customer_id", true, pairs);
    assertEquals(JoinStrategy.HASH_SHUFFLE, s);
  }

  public void testCoRoutingNotSelectedWhenDisabled() {
    ClusterService cs = mockClusterService("orders", 3, "customers", 3);
    CostEstimator estimator = new CostEstimator(cs, 2);
    CoRoutedPairs pairs = CoRoutedPairs.parse(List.of("orders:customer_id,customers:customer_id"));

    // coRoutingEnabled=false → fall through even when the pair matches.
    JoinStrategy s =
        estimator.selectJoinStrategy(
            "orders", "customers", "customer_id", "customer_id", false, pairs);
    assertEquals(JoinStrategy.HASH_SHUFFLE, s);
  }

  public void testCoRoutingNotSelectedWhenPairNotRegistered() {
    ClusterService cs = mockClusterService("orders", 3, "customers", 3);
    CostEstimator estimator = new CostEstimator(cs, 2);

    JoinStrategy s =
        estimator.selectJoinStrategy(
            "orders", "customers", "customer_id", "customer_id", true, CoRoutedPairs.EMPTY);
    assertEquals(JoinStrategy.HASH_SHUFFLE, s);
  }

  public void testCoRoutingNotSelectedWithoutJoinKeys() {
    ClusterService cs = mockClusterService("orders", 3, "customers", 3);
    CostEstimator estimator = new CostEstimator(cs, 2);
    CoRoutedPairs pairs = CoRoutedPairs.parse(List.of("orders:customer_id,customers:customer_id"));

    // Missing left key → cannot match pair → fall back.
    JoinStrategy s =
        estimator.selectJoinStrategy("orders", "customers", null, "customer_id", true, pairs);
    assertEquals(JoinStrategy.HASH_SHUFFLE, s);
  }
}
