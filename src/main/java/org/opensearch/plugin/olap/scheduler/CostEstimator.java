/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.scheduler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;

/**
 * Estimates index sizes and selects the optimal join strategy for MPP execution.
 *
 * <p>Uses OpenSearch cluster metadata (shard count) as a proxy for table size. The heuristic: if
 * the smaller side has few shards relative to the broadcast threshold, use broadcast join;
 * otherwise use hash shuffle.
 */
public class CostEstimator {

  private static final Logger logger = LogManager.getLogger(CostEstimator.class);

  private final ClusterService clusterService;
  private final int broadcastMaxShards;

  /**
   * @param clusterService cluster metadata for index statistics
   * @param broadcastMaxShards max primary shard count for the build side to qualify for broadcast
   */
  public CostEstimator(ClusterService clusterService, int broadcastMaxShards) {
    this.clusterService = clusterService;
    this.broadcastMaxShards = broadcastMaxShards;
  }

  /**
   * Select the optimal join strategy based on index metadata.
   *
   * @return BROADCAST if the smaller side has few shards; HASH_SHUFFLE otherwise
   */
  public JoinStrategy selectJoinStrategy(String leftIndex, String rightIndex) {
    int leftShards = getShardCount(leftIndex);
    int rightShards = getShardCount(rightIndex);
    int smallerShards = Math.min(leftShards, rightShards);

    logger.info(
        "Cost estimation: left={} ({} shards), right={} ({} shards), broadcastThreshold={}",
        leftIndex,
        leftShards,
        rightIndex,
        rightShards,
        broadcastMaxShards);

    if (smallerShards <= broadcastMaxShards) {
      return JoinStrategy.BROADCAST;
    }
    return JoinStrategy.HASH_SHUFFLE;
  }

  /**
   * Determine which side to broadcast (the smaller one).
   *
   * @return "left" or "right"
   */
  public String selectBuildSide(String leftIndex, String rightIndex) {
    int leftShards = getShardCount(leftIndex);
    int rightShards = getShardCount(rightIndex);
    return leftShards <= rightShards ? "left" : "right";
  }

  private int getShardCount(String indexName) {
    IndexMetadata meta = clusterService.state().metadata().index(indexName);
    if (meta == null) {
      logger.warn("Index {} not found in cluster metadata, assuming large", indexName);
      return Integer.MAX_VALUE;
    }
    return meta.getNumberOfShards();
  }
}
