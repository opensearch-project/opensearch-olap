/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical.rules;

import org.opensearch.test.OpenSearchTestCase;

/** Tests for the PhysicalRules rule collections. */
public class PhysicalRulesTests extends OpenSearchTestCase {

  public void testBaseRulesNotEmpty() {
    assertFalse(PhysicalRules.BASE_RULES.isEmpty());
  }

  public void testBaseRulesHasSevenRules() {
    // TableScan, Filter, Project, Sort, Aggregate, Join, Window
    assertEquals(7, PhysicalRules.BASE_RULES.size());
  }

  public void testMppRulesNotEmpty() {
    assertFalse(PhysicalRules.MPP_RULES.isEmpty());
  }

  public void testMppRulesHasTwoRules() {
    // MppAggregate, MppJoin
    assertEquals(2, PhysicalRules.MPP_RULES.size());
  }

  public void testOptimizationRulesEmpty() {
    assertTrue(PhysicalRules.OPTIMIZATION_RULES.isEmpty());
  }

  public void testOptimizationRulesIsEmpty() {
    // Two-stage agg split is handled in VeloxPlanGenerator, not as a Calcite rule
    assertEquals(0, PhysicalRules.OPTIMIZATION_RULES.size());
  }

  public void testAllRulesHaveDescriptions() {
    for (var rule : PhysicalRules.BASE_RULES) {
      assertNotNull("Rule should have a description", rule.toString());
      assertFalse("Rule description should not be empty", rule.toString().isEmpty());
    }
    for (var rule : PhysicalRules.MPP_RULES) {
      assertNotNull(rule.toString());
    }
    for (var rule : PhysicalRules.OPTIMIZATION_RULES) {
      assertNotNull(rule.toString());
    }
  }
}
