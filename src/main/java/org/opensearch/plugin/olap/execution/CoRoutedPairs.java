/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.execution;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Registry of co-routed index pairs parsed from {@code plugins.velox.co_routed_pairs}.
 *
 * <p>Each entry is a pair of {@code index:key} tuples — the user's explicit assertion that both
 * indexes were indexed with {@code _routing} equal to the listed key field and have matching {@code
 * number_of_shards}. The assertion is checked at run time before a Co-Routing join fires (shard
 * count, eligibility), but the pairing itself is opt-in — the plugin does not try to auto-discover
 * co-location.
 *
 * <p>Wire format (OpenSearch setting is {@code List<String>} — each element is one pair):
 *
 * <pre>
 *   "orders:customer_id,customers:customer_id"
 *   "events:user_id,profiles:user_id"
 * </pre>
 *
 * <p>Lookup is order-insensitive: a pair {@code (A,k,B,k)} matches a join {@code B ⋈ A} as well as
 * {@code A ⋈ B}.
 */
public final class CoRoutedPairs {

  private static final Logger logger = LogManager.getLogger(CoRoutedPairs.class);

  public static final CoRoutedPairs EMPTY = new CoRoutedPairs(List.of());

  /**
   * One registered pair. Keys are the join column on each side (usually the {@code _routing}
   * value).
   */
  public static final class Pair {
    public final String leftIndex;
    public final String leftKey;
    public final String rightIndex;
    public final String rightKey;

    Pair(String leftIndex, String leftKey, String rightIndex, String rightKey) {
      this.leftIndex = leftIndex;
      this.leftKey = leftKey;
      this.rightIndex = rightIndex;
      this.rightKey = rightKey;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Pair)) return false;
      Pair p = (Pair) o;
      return leftIndex.equals(p.leftIndex)
          && leftKey.equals(p.leftKey)
          && rightIndex.equals(p.rightIndex)
          && rightKey.equals(p.rightKey);
    }

    @Override
    public int hashCode() {
      return Objects.hash(leftIndex, leftKey, rightIndex, rightKey);
    }

    @Override
    public String toString() {
      return leftIndex + ":" + leftKey + " ⋈ " + rightIndex + ":" + rightKey;
    }
  }

  private final List<Pair> pairs;

  private CoRoutedPairs(List<Pair> pairs) {
    this.pairs = List.copyOf(pairs);
  }

  public List<Pair> asList() {
    return pairs;
  }

  public boolean isEmpty() {
    return pairs.isEmpty();
  }

  /**
   * Return true if {@code (leftIndex, leftKey) ⋈ (rightIndex, rightKey)} is a registered pair, in
   * either order.
   *
   * <p>Tries the supplied key names verbatim first. Only if that fails does it retry with a
   * trailing-digit strip on the keys (to handle the SQL plugin's join-disambiguation suffixes like
   * {@code dept_id0}). A field genuinely named {@code sku2} therefore matches before the strip ever
   * runs.
   */
  public boolean matches(String leftIndex, String leftKey, String rightIndex, String rightKey) {
    if (matchesExact(leftIndex, leftKey, rightIndex, rightKey)) {
      return true;
    }
    String strippedLeft = stripTrailingDigits(leftKey);
    String strippedRight = stripTrailingDigits(rightKey);
    if (strippedLeft.equals(leftKey) && strippedRight.equals(rightKey)) {
      return false; // nothing to strip — exact already tried.
    }
    return matchesExact(leftIndex, strippedLeft, rightIndex, strippedRight);
  }

  private boolean matchesExact(
      String leftIndex, String leftKey, String rightIndex, String rightKey) {
    for (Pair p : pairs) {
      if (p.leftIndex.equals(leftIndex)
          && p.leftKey.equals(leftKey)
          && p.rightIndex.equals(rightIndex)
          && p.rightKey.equals(rightKey)) {
        return true;
      }
      if (p.leftIndex.equals(rightIndex)
          && p.leftKey.equals(rightKey)
          && p.rightIndex.equals(leftIndex)
          && p.rightKey.equals(leftKey)) {
        return true;
      }
    }
    return false;
  }

  private static String stripTrailingDigits(String name) {
    if (name == null) return null;
    return name.replaceAll("\\d+$", "");
  }

  /**
   * Parse a raw setting value (list of "indexA:keyA,indexB:keyB" entries). Malformed entries are
   * logged and skipped — a bad entry never silently enables Co-Routing for the wrong pair.
   */
  public static CoRoutedPairs parse(List<String> rawEntries) {
    if (rawEntries == null || rawEntries.isEmpty()) {
      return EMPTY;
    }
    List<Pair> parsed = new ArrayList<>(rawEntries.size());
    for (String entry : rawEntries) {
      if (entry == null || entry.isBlank()) continue;
      String[] sides = entry.split(",");
      if (sides.length != 2) {
        logger.warn(
            "Invalid plugins.velox.co_routed_pairs entry (expected '<idxA>:<keyA>,<idxB>:<keyB>'):"
                + " {}",
            entry);
        continue;
      }
      String[] left = sides[0].split(":");
      String[] right = sides[1].split(":");
      if (left.length != 2 || right.length != 2) {
        logger.warn("Invalid plugins.velox.co_routed_pairs tuple: {}", entry);
        continue;
      }
      parsed.add(new Pair(left[0].trim(), left[1].trim(), right[0].trim(), right[1].trim()));
    }
    return new CoRoutedPairs(parsed);
  }
}
