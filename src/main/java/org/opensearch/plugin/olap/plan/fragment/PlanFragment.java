/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.fragment;

import java.util.List;
import org.boostscale.velox4j.plan.PlanNode;

/**
 * A fragment of a distributed Velox query plan.
 *
 * <p>Each fragment contains a sub-tree of PlanNodes that can be executed as a unit on a single
 * node. Fragments are connected by exchange boundaries where data flows from one fragment's output
 * to another fragment's input (via ExternalStream).
 *
 * <p>For a simple SELECT ... FROM ... WHERE ... query on a single table:
 *
 * <ul>
 *   <li>Fragment 0 (leaf): TableScan + Filter + partial Aggregation → runs on data nodes
 *   <li>Fragment 1 (root): final Aggregation + Project → runs on coordinator
 * </ul>
 *
 * <p>For join queries, additional fragment types are used:
 *
 * <ul>
 *   <li>Coordinator-centric: left scan, right scan, coordinator join
 *   <li>Broadcast: build scan (collected), probe+join (on probe nodes)
 *   <li>Hash shuffle: left scan+partition, right scan+partition, join on workers
 * </ul>
 */
public class PlanFragment {

  private final int fragmentId;
  private final PlanNode planRoot;
  private final FragmentProperties properties;
  private final List<Integer> inputFragmentIds;

  /**
   * Serialized build-side batches for broadcast join. Set on probe-side fragments that need to
   * receive broadcast data. Null for non-broadcast fragments.
   */
  private List<byte[]> broadcastData;

  public PlanFragment(
      int fragmentId,
      PlanNode planRoot,
      FragmentProperties properties,
      List<Integer> inputFragmentIds) {
    this.fragmentId = fragmentId;
    this.planRoot = planRoot;
    this.properties = properties;
    this.inputFragmentIds = inputFragmentIds;
  }

  public int getFragmentId() {
    return fragmentId;
  }

  public PlanNode getPlanRoot() {
    return planRoot;
  }

  public FragmentProperties getProperties() {
    return properties;
  }

  public List<Integer> getInputFragmentIds() {
    return inputFragmentIds;
  }

  public boolean isLeaf() {
    return inputFragmentIds.isEmpty();
  }

  public boolean isRoot() {
    return properties.getDistribution() == FragmentProperties.Distribution.COORDINATOR;
  }

  public List<byte[]> getBroadcastData() {
    return broadcastData;
  }

  public void setBroadcastData(List<byte[]> broadcastData) {
    this.broadcastData = broadcastData;
  }
}
