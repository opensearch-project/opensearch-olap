/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.scheduler;

/**
 * Statistics for an OpenSearch index, collected at query time from IndicesStatsResponse. Used by
 * CostEstimator for join strategy selection and by PhysicalOptimizer for Calcite cost-based
 * planning.
 */
public class TableStatistics {

  private final String indexName;
  private final long rowCount;
  private final long sizeInBytes;
  private final int shardCount;

  public TableStatistics(String indexName, long rowCount, long sizeInBytes, int shardCount) {
    this.indexName = indexName;
    this.rowCount = rowCount;
    this.sizeInBytes = sizeInBytes;
    this.shardCount = shardCount;
  }

  public String getIndexName() {
    return indexName;
  }

  public long getRowCount() {
    return rowCount;
  }

  public long getSizeInBytes() {
    return sizeInBytes;
  }

  public int getShardCount() {
    return shardCount;
  }

  /** Average row size in bytes. Returns 0 if no rows. */
  public long getAverageRowSize() {
    return rowCount > 0 ? sizeInBytes / rowCount : 0;
  }

  @Override
  public String toString() {
    return "TableStatistics{"
        + "index='"
        + indexName
        + "', rows="
        + rowCount
        + ", size="
        + sizeInBytes
        + ", shards="
        + shardCount
        + '}';
  }
}
