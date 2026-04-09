/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical;

import org.apache.calcite.plan.ConventionTraitDef;
import org.opensearch.test.OpenSearchTestCase;

public class PhysicalConventionTests extends OpenSearchTestCase {

  public void testName() {
    assertEquals("PHYSICAL", PhysicalConvention.INSTANCE.getName());
  }

  public void testInterface() {
    assertEquals(PhysicalRel.class, PhysicalConvention.INSTANCE.getInterface());
  }

  public void testTraitDef() {
    assertSame(ConventionTraitDef.INSTANCE, PhysicalConvention.INSTANCE.getTraitDef());
  }

  public void testSatisfiesSelf() {
    assertTrue(PhysicalConvention.INSTANCE.satisfies(PhysicalConvention.INSTANCE));
  }

  public void testDoesNotSatisfyNone() {
    assertFalse(PhysicalConvention.INSTANCE.satisfies(org.apache.calcite.plan.Convention.NONE));
  }

  public void testCannotConvertConvention() {
    assertFalse(PhysicalConvention.INSTANCE.canConvertConvention(PhysicalConvention.INSTANCE));
  }

  public void testUseAbstractConvertersReturnsTrue() {
    assertTrue(
        PhysicalConvention.INSTANCE.useAbstractConvertersForConversion(
            org.apache.calcite.plan.RelTraitSet.createEmpty(),
            org.apache.calcite.plan.RelTraitSet.createEmpty()));
  }
}
