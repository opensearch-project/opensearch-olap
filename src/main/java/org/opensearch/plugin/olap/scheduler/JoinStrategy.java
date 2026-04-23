/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.scheduler;

/**
 * Strategy for distributed join execution.
 *
 * <p>When {@code mpp_enabled=false}, only {@link #COORDINATOR_CENTRIC} is used. When {@code
 * mpp_enabled=true}, the {@link CostEstimator} selects between {@link #CO_ROUTING}, {@link
 * #BROADCAST}, and {@link #HASH_SHUFFLE} based on index statistics and the co-routed pair registry.
 */
public enum JoinStrategy {

  /** Both sides collected on coordinator, join executed locally. Default when MPP is off. */
  COORDINATOR_CENTRIC,

  /**
   * Both indexes are co-routed (same routing key + matching shard count). One shard pair is joined
   * locally on the owning node with zero shuffle. Strictly dominates BROADCAST and HASH_SHUFFLE
   * when applicable.
   */
  CO_ROUTING,

  /** Small (build) side broadcast to all probe-side data nodes; join runs in parallel on each. */
  BROADCAST,

  /** Both sides hash-partitioned by join key and shuffled to workers; join runs per partition. */
  HASH_SHUFFLE
}
