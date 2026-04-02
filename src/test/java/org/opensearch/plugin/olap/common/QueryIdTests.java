/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.common;

import org.opensearch.test.OpenSearchTestCase;

public class QueryIdTests extends OpenSearchTestCase {

  public void testGenerateProducesNonNullId() {
    QueryId id = QueryId.generate();
    assertNotNull(id);
    assertNotNull(id.getId());
    assertFalse(id.getId().isEmpty());
  }

  public void testGenerateProducesUniqueIds() {
    QueryId id1 = QueryId.generate();
    QueryId id2 = QueryId.generate();
    assertNotEquals(id1, id2);
    assertNotEquals(id1.getId(), id2.getId());
  }

  public void testOfWrapsGivenId() {
    QueryId id = QueryId.of("test-query-123");
    assertEquals("test-query-123", id.getId());
  }

  public void testToStringReturnsId() {
    QueryId id = QueryId.of("abc-def");
    assertEquals("abc-def", id.toString());
  }

  public void testEqualsAndHashCode() {
    QueryId a = QueryId.of("same-id");
    QueryId b = QueryId.of("same-id");
    QueryId c = QueryId.of("different-id");

    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertNotEquals(a, c);
    assertNotEquals(a.hashCode(), c.hashCode());
  }

  public void testEqualsWithSelf() {
    QueryId id = QueryId.of("self");
    assertEquals(id, id);
  }

  public void testEqualsWithNull() {
    QueryId id = QueryId.of("value");
    assertNotEquals(id, null);
  }

  public void testEqualsWithDifferentType() {
    QueryId id = QueryId.of("value");
    assertNotEquals(id, "value");
  }

  public void testOfThrowsOnNullId() {
    expectThrows(NullPointerException.class, () -> QueryId.of(null));
  }
}
