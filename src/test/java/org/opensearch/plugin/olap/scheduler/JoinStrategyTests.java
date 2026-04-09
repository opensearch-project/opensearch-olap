/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.scheduler;

import org.opensearch.test.OpenSearchTestCase;

public class JoinStrategyTests extends OpenSearchTestCase {

  public void testEnumValues() {
    assertEquals(3, JoinStrategy.values().length);
    assertEquals(JoinStrategy.COORDINATOR_CENTRIC, JoinStrategy.valueOf("COORDINATOR_CENTRIC"));
    assertEquals(JoinStrategy.BROADCAST, JoinStrategy.valueOf("BROADCAST"));
    assertEquals(JoinStrategy.HASH_SHUFFLE, JoinStrategy.valueOf("HASH_SHUFFLE"));
  }

  public void testOrdinalOrder() {
    assertEquals(0, JoinStrategy.COORDINATOR_CENTRIC.ordinal());
    assertEquals(1, JoinStrategy.BROADCAST.ordinal());
    assertEquals(2, JoinStrategy.HASH_SHUFFLE.ordinal());
  }
}
