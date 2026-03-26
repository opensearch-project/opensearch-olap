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
    ANY
  }

  private final Distribution distribution;
  private final List<String> partitionColumns;
  private final String sourceIndex;

  public FragmentProperties(
      Distribution distribution, List<String> partitionColumns, String sourceIndex) {
    this.distribution = distribution;
    this.partitionColumns = partitionColumns;
    this.sourceIndex = sourceIndex;
  }

  public static FragmentProperties source(String indexName) {
    return new FragmentProperties(Distribution.SOURCE, List.of(), indexName);
  }

  public static FragmentProperties coordinator() {
    return new FragmentProperties(Distribution.COORDINATOR, List.of(), null);
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
}
