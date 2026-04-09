/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.scheduler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
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
}
