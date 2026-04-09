/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.fragment;

import java.util.List;

/** Metadata about a plan fragment's distribution requirements. */
public class FragmentProperties {

  public enum Distribution {
    /** Fragment runs on nodes that own the scanned shards. */
    SOURCE,
    /** Fragment runs on a single coordinator node. */
    COORDINATOR,
    /** Fragment can run on any node (e.g., for repartitioning). */
    ANY,
    /** Fragment runs on all probe-side nodes with broadcast data injected. */
    BROADCAST,
    /** Fragment runs on N workers, fed by hash-partitioned shuffle data. */
    HASH_PARTITIONED
  }

  private final Distribution distribution;
  private final List<String> partitionColumns;
  private final String sourceIndex;

  /**
   * For HASH_PARTITIONED: number of shuffle partitions (= number of workers). For BROADCAST: the
   * number of data nodes that will run the probe-side join.
   */
  private final int partitionCount;

  /**
   * For shuffle scan fragments: column indices in the scan output to partition by. Empty for
   * non-shuffle fragments.
   */
  private final List<Integer> shuffleKeyChannels;

  /**
   * For join fragments: which side of the join this fragment feeds. "left" = probe side, "right" =
   * build side. Null for non-join fragments.
   */
  private final String joinSide;

  private FragmentProperties(
      Distribution distribution,
      List<String> partitionColumns,
      String sourceIndex,
      int partitionCount,
      List<Integer> shuffleKeyChannels,
      String joinSide) {
    this.distribution = distribution;
    this.partitionColumns = partitionColumns;
    this.sourceIndex = sourceIndex;
    this.partitionCount = partitionCount;
    this.shuffleKeyChannels = shuffleKeyChannels;
    this.joinSide = joinSide;
  }

  public static FragmentProperties source(String indexName) {
    return new FragmentProperties(Distribution.SOURCE, List.of(), indexName, 0, List.of(), null);
  }

  public static FragmentProperties source(String indexName, String joinSide) {
    return new FragmentProperties(
        Distribution.SOURCE, List.of(), indexName, 0, List.of(), joinSide);
  }

  public static FragmentProperties coordinator() {
    return new FragmentProperties(Distribution.COORDINATOR, List.of(), null, 0, List.of(), null);
  }

  public static FragmentProperties broadcast(String probeIndex) {
    return new FragmentProperties(
        Distribution.BROADCAST, List.of(), probeIndex, 0, List.of(), null);
  }

  public static FragmentProperties shuffleScan(
      String sourceIndex, String joinSide, List<Integer> keyChannels, int partitionCount) {
    return new FragmentProperties(
        Distribution.SOURCE, List.of(), sourceIndex, partitionCount, keyChannels, joinSide);
  }

  public static FragmentProperties hashPartitioned(int partitionCount) {
    return new FragmentProperties(
        Distribution.HASH_PARTITIONED, List.of(), null, partitionCount, List.of(), null);
  }

  public Distribution getDistribution() {
    return distribution;
  }

  public List<String> getPartitionColumns() {
    return partitionColumns;
  }

  public String getSourceIndex() {
    return sourceIndex;
  }

  public int getPartitionCount() {
    return partitionCount;
  }

  public List<Integer> getShuffleKeyChannels() {
    return shuffleKeyChannels;
  }

  public String getJoinSide() {
    return joinSide;
  }

  public boolean isShuffleScan() {
    return shuffleKeyChannels != null && !shuffleKeyChannels.isEmpty();
  }
}
