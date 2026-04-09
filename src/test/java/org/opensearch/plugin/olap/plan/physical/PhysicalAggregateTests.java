/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical;

import org.opensearch.test.OpenSearchTestCase;

public class PhysicalAggregateTests extends OpenSearchTestCase {

  public void testStepEnum() {
    assertEquals(3, PhysicalAggregate.Step.values().length);
    assertEquals(PhysicalAggregate.Step.SINGLE, PhysicalAggregate.Step.valueOf("SINGLE"));
    assertEquals(PhysicalAggregate.Step.PARTIAL, PhysicalAggregate.Step.valueOf("PARTIAL"));
    assertEquals(PhysicalAggregate.Step.FINAL, PhysicalAggregate.Step.valueOf("FINAL"));
  }

  public void testStepOrdinals() {
    assertEquals(0, PhysicalAggregate.Step.SINGLE.ordinal());
    assertEquals(1, PhysicalAggregate.Step.PARTIAL.ordinal());
    assertEquals(2, PhysicalAggregate.Step.FINAL.ordinal());
  }
}
