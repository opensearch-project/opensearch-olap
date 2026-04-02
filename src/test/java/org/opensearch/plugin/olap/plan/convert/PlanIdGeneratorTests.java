/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.convert;

import org.opensearch.test.OpenSearchTestCase;

public class PlanIdGeneratorTests extends OpenSearchTestCase {

  public void testFirstIdIsZero() {
    PlanIdGenerator gen = new PlanIdGenerator();
    assertEquals("0", gen.next());
  }

  public void testIdsAreSequential() {
    PlanIdGenerator gen = new PlanIdGenerator();
    assertEquals("0", gen.next());
    assertEquals("1", gen.next());
    assertEquals("2", gen.next());
  }

  public void testResetRestartFromZero() {
    PlanIdGenerator gen = new PlanIdGenerator();
    gen.next();
    gen.next();
    gen.next();
    gen.reset();
    assertEquals("0", gen.next());
    assertEquals("1", gen.next());
  }

  public void testMultipleResets() {
    PlanIdGenerator gen = new PlanIdGenerator();
    for (int i = 0; i < 10; i++) {
      gen.next();
    }
    gen.reset();
    assertEquals("0", gen.next());
    gen.reset();
    assertEquals("0", gen.next());
  }

  public void testUniqueIdsWithinSession() {
    PlanIdGenerator gen = new PlanIdGenerator();
    int count = 100;
    java.util.Set<String> ids = new java.util.HashSet<>();
    for (int i = 0; i < count; i++) {
      ids.add(gen.next());
    }
    assertEquals(count, ids.size());
  }
}
