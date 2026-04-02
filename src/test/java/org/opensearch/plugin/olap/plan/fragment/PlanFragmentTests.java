/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.fragment;

import java.util.Collections;
import java.util.List;
import org.boostscale.velox4j.connector.ExternalStreamTableHandle;
import org.boostscale.velox4j.plan.TableScanNode;
import org.boostscale.velox4j.type.IntegerType;
import org.boostscale.velox4j.type.RowType;
import org.opensearch.test.OpenSearchTestCase;

public class PlanFragmentTests extends OpenSearchTestCase {

  private TableScanNode buildScan() {
    RowType outputType = new RowType(List.of("id"), List.of(new IntegerType()));
    return new TableScanNode(
        "0",
        outputType,
        new ExternalStreamTableHandle("connector-external-stream"),
        Collections.emptyList());
  }

  public void testLeafFragmentIsLeafNotRoot() {
    PlanFragment frag =
        new PlanFragment(
            0, buildScan(), FragmentProperties.source("my-index"), Collections.emptyList());

    assertTrue(frag.isLeaf());
    assertFalse(frag.isRoot());
  }

  public void testRootFragmentIsRootNotLeaf() {
    PlanFragment frag =
        new PlanFragment(1, buildScan(), FragmentProperties.coordinator(), Collections.emptyList());

    assertTrue(frag.isRoot());
    // A root fragment with no input dependencies is technically also a "leaf" by the isEmpty()
    // check,
    // but in practice root fragments list input fragment IDs. Let's verify with an input.
    PlanFragment rootWithInput =
        new PlanFragment(1, buildScan(), FragmentProperties.coordinator(), List.of(0));
    assertTrue(rootWithInput.isRoot());
    assertFalse(rootWithInput.isLeaf());
  }

  public void testGetFragmentId() {
    PlanFragment frag =
        new PlanFragment(
            42, buildScan(), FragmentProperties.source("idx"), Collections.emptyList());
    assertEquals(42, frag.getFragmentId());
  }

  public void testGetPlanRoot() {
    TableScanNode scan = buildScan();
    PlanFragment frag =
        new PlanFragment(0, scan, FragmentProperties.source("idx"), Collections.emptyList());
    assertSame(scan, frag.getPlanRoot());
  }

  public void testGetProperties() {
    FragmentProperties props = FragmentProperties.source("test-index");
    PlanFragment frag = new PlanFragment(0, buildScan(), props, Collections.emptyList());
    assertSame(props, frag.getProperties());
  }

  public void testGetInputFragmentIds() {
    List<Integer> inputs = List.of(0, 1);
    PlanFragment frag = new PlanFragment(2, buildScan(), FragmentProperties.coordinator(), inputs);
    assertEquals(inputs, frag.getInputFragmentIds());
  }

  public void testFragmentPropertiesSourceDistribution() {
    FragmentProperties props = FragmentProperties.source("my-index");
    assertEquals(FragmentProperties.Distribution.SOURCE, props.getDistribution());
    assertEquals("my-index", props.getSourceIndex());
    assertTrue(props.getPartitionColumns().isEmpty());
  }

  public void testFragmentPropertiesCoordinatorDistribution() {
    FragmentProperties props = FragmentProperties.coordinator();
    assertEquals(FragmentProperties.Distribution.COORDINATOR, props.getDistribution());
    assertNull(props.getSourceIndex());
    assertTrue(props.getPartitionColumns().isEmpty());
  }
}
