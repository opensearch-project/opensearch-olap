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
 */
public class PlanFragment {

  private final int fragmentId;
  private final PlanNode planRoot;
  private final FragmentProperties properties;
  private final List<Integer> inputFragmentIds;

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
}
