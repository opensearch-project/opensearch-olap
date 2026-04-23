/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.scheduler;

import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.plugin.olap.execution.CoRoutedPairs;

/**
 * Estimates index sizes and selects the optimal join strategy for MPP execution.
 *
 * <p>When CBO statistics are available (row counts from IndicesStatsResponse), uses real row
 * counts. Falls back to shard count heuristic when statistics are unavailable.
 *
 * <p>The broadcast threshold ({@code broadcast_max_shards}) is compared against shard count for
 * strategy selection (BROADCAST vs HASH_SHUFFLE). Build side selection uses row count when
 * available.
 */
public class CostEstimator {

  private static final Logger logger = LogManager.getLogger(CostEstimator.class);

  private final ClusterService clusterService;
  private final int broadcastMaxShards;
  private final Map<String, TableStatistics> statsMap;

  /**
   * Create a CostEstimator with CBO statistics.
   *
   * @param clusterService cluster metadata for shard count fallback
   * @param broadcastMaxShards max primary shard count for broadcast eligibility
   * @param statsMap table statistics from StatisticsCollector (may be empty)
   */
  public CostEstimator(
      ClusterService clusterService,
      int broadcastMaxShards,
      Map<String, TableStatistics> statsMap) {
    this.clusterService = clusterService;
    this.broadcastMaxShards = broadcastMaxShards;
    this.statsMap = statsMap != null ? statsMap : Map.of();
  }

  /** Backward-compatible constructor without CBO statistics. */
  public CostEstimator(ClusterService clusterService, int broadcastMaxShards) {
    this(clusterService, broadcastMaxShards, Map.of());
  }

  /**
   * Select the optimal join strategy based on index metadata.
   *
   * @return BROADCAST if the smaller side has few shards; HASH_SHUFFLE otherwise
   */
  public JoinStrategy selectJoinStrategy(String leftIndex, String rightIndex) {
    return selectJoinStrategy(leftIndex, rightIndex, null, null, false, CoRoutedPairs.EMPTY);
  }

  /**
   * Select the optimal join strategy, considering Co-Routing eligibility first.
   *
   * <p>Co-Routing is selected iff all of:
   *
   * <ul>
   *   <li>{@code coRoutingEnabled=true}
   *   <li>join keys are known on both sides (equi-join with exactly one key each)
   *   <li>the {@code (leftIndex, leftKey, rightIndex, rightKey)} tuple is registered in {@code
   *       coRoutedPairs} (in either order)
   *   <li>both indexes have identical {@code number_of_shards}
   * </ul>
   *
   * <p>Otherwise falls through to the standard BROADCAST / HASH_SHUFFLE selection.
   */
  public JoinStrategy selectJoinStrategy(
      String leftIndex,
      String rightIndex,
      String leftKey,
      String rightKey,
      boolean coRoutingEnabled,
      CoRoutedPairs coRoutedPairs) {
    int leftShards = getShardCount(leftIndex);
    int rightShards = getShardCount(rightIndex);
    int smallerShards = Math.min(leftShards, rightShards);

    long leftRows = getRowCount(leftIndex);
    long rightRows = getRowCount(rightIndex);

    logger.info(
        "Cost estimation: left={} ({} rows, {} shards), right={} ({} rows, {} shards),"
            + " broadcastThreshold={}, coRoutingEnabled={}",
        leftIndex,
        leftRows,
        leftShards,
        rightIndex,
        rightRows,
        rightShards,
        broadcastMaxShards,
        coRoutingEnabled);

    if (coRoutingEnabled
        && leftKey != null
        && rightKey != null
        && coRoutedPairs != null
        && coRoutedPairs.matches(leftIndex, leftKey, rightIndex, rightKey)) {
      if (leftShards == rightShards) {
        logger.info(
            "Co-Routing eligible: {}:{} ⋈ {}:{} (shards={})",
            leftIndex,
            leftKey,
            rightIndex,
            rightKey,
            leftShards);
        return JoinStrategy.CO_ROUTING;
      }
      logger.warn(
          "Co-Routing pair registered but shard counts differ ({}={}, {}={}) — falling back",
          leftIndex,
          leftShards,
          rightIndex,
          rightShards);
    }

    if (smallerShards <= broadcastMaxShards) {
      return JoinStrategy.BROADCAST;
    }
    return JoinStrategy.HASH_SHUFFLE;
  }

  /**
   * Determine which side to broadcast (the smaller one). Uses row count when CBO statistics are
   * available, falls back to shard count.
   *
   * @return "left" or "right"
   */
  public String selectBuildSide(String leftIndex, String rightIndex) {
    long leftRows = getRowCount(leftIndex);
    long rightRows = getRowCount(rightIndex);

    // Use row counts when both are available (non-zero)
    if (leftRows > 0 && rightRows > 0) {
      return leftRows <= rightRows ? "left" : "right";
    }

    // Fallback to shard count
    int leftShards = getShardCount(leftIndex);
    int rightShards = getShardCount(rightIndex);
    return leftShards <= rightShards ? "left" : "right";
  }

  /** Get row count from CBO statistics, or 0 if unavailable. */
  public long getRowCount(String indexName) {
    TableStatistics stats = statsMap.get(indexName);
    return stats != null ? stats.getRowCount() : 0;
  }

  /** Get shard count from cluster metadata. */
  public int getShardCount(String indexName) {
    // First check CBO stats (may have cached shard count)
    TableStatistics stats = statsMap.get(indexName);
    if (stats != null && stats.getShardCount() > 0) {
      return stats.getShardCount();
    }
    // Fallback to cluster metadata
    IndexMetadata meta = clusterService.state().metadata().index(indexName);
    if (meta == null) {
      logger.warn("Index {} not found in cluster metadata, assuming large", indexName);
      return Integer.MAX_VALUE;
    }
    return meta.getNumberOfShards();
  }
}
