/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.scheduler;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.stats.IndexStats;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.transport.client.Client;

/**
 * Collects table-level statistics (row count, data size) from OpenSearch's IndicesStatsResponse.
 * Used at query time when CBO statistics mode is RUNTIME.
 *
 * <p>The collection is lightweight (~10-20ms) as it reads pre-computed shard-level stats from the
 * cluster, not from Lucene directly.
 */
public class StatisticsCollector {

  private static final Logger logger = LogManager.getLogger(StatisticsCollector.class);

  private final Client client;
  private final ClusterService clusterService;

  public StatisticsCollector(Client client, ClusterService clusterService) {
    this.client = client;
    this.clusterService = clusterService;
  }

  /**
   * Collect statistics for the given index names.
   *
   * @param indexNames set of index names to collect stats for
   * @return map of index name → TableStatistics
   */
  public Map<String, TableStatistics> collect(Set<String> indexNames) {
    Map<String, TableStatistics> statsMap = new HashMap<>();

    if (indexNames.isEmpty()) {
      return statsMap;
    }

    try {
      IndicesStatsRequest request = new IndicesStatsRequest();
      request.indices(indexNames.toArray(new String[0]));
      request.docs(true);
      request.store(true);

      IndicesStatsResponse response = client.admin().indices().stats(request).actionGet();

      for (String indexName : indexNames) {
        IndexStats indexStats = response.getIndex(indexName);
        if (indexStats != null && indexStats.getPrimaries() != null) {
          long rowCount =
              indexStats.getPrimaries().getDocs() != null
                  ? indexStats.getPrimaries().getDocs().getCount()
                  : 0;
          long sizeInBytes =
              indexStats.getPrimaries().getStore() != null
                  ? indexStats.getPrimaries().getStore().getSizeInBytes()
                  : 0;
          int shardCount = getShardCount(indexName);

          TableStatistics stats = new TableStatistics(indexName, rowCount, sizeInBytes, shardCount);
          statsMap.put(indexName, stats);
          logger.debug("Collected statistics for {}: {}", indexName, stats);
        } else {
          // Fallback: use shard count only
          int shardCount = getShardCount(indexName);
          statsMap.put(indexName, new TableStatistics(indexName, 0, 0, shardCount));
          logger.warn("No stats available for index {}, using defaults", indexName);
        }
      }
    } catch (Exception e) {
      logger.warn("Failed to collect statistics: {}. Using defaults.", e.getMessage());
      // Fallback: return empty stats for all indices
      for (String indexName : indexNames) {
        int shardCount = getShardCount(indexName);
        statsMap.put(indexName, new TableStatistics(indexName, 0, 0, shardCount));
      }
    }

    return statsMap;
  }

  private int getShardCount(String indexName) {
    IndexMetadata meta = clusterService.state().metadata().index(indexName);
    // When metadata is unavailable (alias, wildcard, unresolved name), return MAX_VALUE
    // to match CostEstimator's conservative fallback — forces HASH_SHUFFLE over BROADCAST.
    return meta != null ? meta.getNumberOfShards() : Integer.MAX_VALUE;
  }
}
