/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.execution;

import java.util.List;
import org.opensearch.test.OpenSearchTestCase;

public class CoRoutedPairsTests extends OpenSearchTestCase {

  public void testParseEmpty() {
    assertTrue(CoRoutedPairs.parse(null).isEmpty());
    assertTrue(CoRoutedPairs.parse(List.of()).isEmpty());
    assertTrue(CoRoutedPairs.parse(List.of("")).isEmpty());
  }

  public void testParseSinglePair() {
    CoRoutedPairs pairs = CoRoutedPairs.parse(List.of("orders:customer_id,customers:customer_id"));
    assertEquals(1, pairs.asList().size());
    CoRoutedPairs.Pair p = pairs.asList().get(0);
    assertEquals("orders", p.leftIndex);
    assertEquals("customer_id", p.leftKey);
    assertEquals("customers", p.rightIndex);
    assertEquals("customer_id", p.rightKey);
  }

  public void testParseMultiplePairs() {
    CoRoutedPairs pairs =
        CoRoutedPairs.parse(
            List.of("orders:customer_id,customers:customer_id", "events:user_id,profiles:user_id"));
    assertEquals(2, pairs.asList().size());
    assertEquals("events", pairs.asList().get(1).leftIndex);
  }

  public void testParseIgnoresMalformed() {
    CoRoutedPairs pairs =
        CoRoutedPairs.parse(
            List.of(
                "orders:customer_id,customers:customer_id",
                "bad-entry", // no comma
                "orders:customer_id", // only one tuple
                "orders,customers:customer_id")); // missing key on left
    assertEquals(1, pairs.asList().size());
  }

  public void testMatchesExactOrder() {
    CoRoutedPairs pairs = CoRoutedPairs.parse(List.of("orders:customer_id,customers:customer_id"));
    assertTrue(pairs.matches("orders", "customer_id", "customers", "customer_id"));
  }

  public void testMatchesReversedOrder() {
    CoRoutedPairs pairs = CoRoutedPairs.parse(List.of("orders:customer_id,customers:customer_id"));
    // Reverse — same pair, different join syntax
    assertTrue(pairs.matches("customers", "customer_id", "orders", "customer_id"));
  }

  public void testRejectsMismatchedKey() {
    CoRoutedPairs pairs = CoRoutedPairs.parse(List.of("orders:customer_id,customers:customer_id"));
    assertFalse(pairs.matches("orders", "tenant_id", "customers", "customer_id"));
    assertFalse(pairs.matches("orders", "customer_id", "other", "customer_id"));
  }

  public void testMatchesOnlyListedPairs() {
    CoRoutedPairs pairs =
        CoRoutedPairs.parse(
            List.of("orders:customer_id,customers:customer_id", "events:user_id,profiles:user_id"));
    assertTrue(pairs.matches("events", "user_id", "profiles", "user_id"));
    assertFalse(pairs.matches("orders", "customer_id", "profiles", "customer_id"));
  }

  public void testEmptyRegistryMatchesNothing() {
    assertFalse(CoRoutedPairs.EMPTY.matches("a", "k", "b", "k"));
  }

  // ---- P3: exact match takes precedence over trailing-digit strip ----

  public void testLegitimateFieldNameWithTrailingDigitsMatches() {
    // User registered sku2 as the real routing field. Plan hands us "sku2" exactly — it should
    // match without being mangled to "sku".
    CoRoutedPairs pairs = CoRoutedPairs.parse(List.of("orders:sku2,inventory:sku2"));
    assertTrue(pairs.matches("orders", "sku2", "inventory", "sku2"));
  }

  public void testYear2024StyleFieldMatches() {
    // Another realistic trailing-digit field name.
    CoRoutedPairs pairs = CoRoutedPairs.parse(List.of("reports:year2024,metrics:year2024"));
    assertTrue(pairs.matches("reports", "year2024", "metrics", "year2024"));
  }

  public void testDisambiguationSuffixStripStillWorksAsFallback() {
    // Planner attached "0" to disambiguate. Exact match misses but the strip fallback should hit.
    CoRoutedPairs pairs = CoRoutedPairs.parse(List.of("orders:dept_id,depts:dept_id"));
    assertTrue(pairs.matches("orders", "dept_id0", "depts", "dept_id0"));
  }

  public void testExactMatchPreferredOverStrippedMatch() {
    // Two pairs: one for the legit "sku2" and one for "sku". An exact match against "sku2" must
    // hit the first pair (not silently fall through to the stripped lookup).
    CoRoutedPairs pairs =
        CoRoutedPairs.parse(List.of("orders:sku2,inventory:sku2", "orders:sku,inventory:sku"));
    assertTrue(pairs.matches("orders", "sku2", "inventory", "sku2"));
    assertTrue(pairs.matches("orders", "sku", "inventory", "sku"));
  }

  public void testStripFallbackStillWorksForDisambiguationSuffix() {
    // Registry has "sku"; plan sends "sku2" (a disambiguation suffix from the SQL plugin's
    // join rename). The exact-match-first semantics should miss on "sku2" then fall back to
    // "sku" and hit. We KEEP the strip fallback — it just no longer runs before exact match.
    CoRoutedPairs pairs = CoRoutedPairs.parse(List.of("orders:sku,inventory:sku"));
    assertTrue(pairs.matches("orders", "sku2", "inventory", "sku2"));
  }
}
