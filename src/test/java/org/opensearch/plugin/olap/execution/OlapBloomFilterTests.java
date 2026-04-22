/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.execution;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.lucene.util.BytesRef;
import org.opensearch.test.OpenSearchTestCase;

public class OlapBloomFilterTests extends OpenSearchTestCase {

  public void testPutAndMightContainPositive() {
    OlapBloomFilter bf = OlapBloomFilter.create(1000);
    List<String> inserted = new ArrayList<>();
    for (int i = 0; i < 500; i++) {
      String v = "user_" + i;
      inserted.add(v);
      bf.add(new BytesRef(v));
    }
    for (String v : inserted) {
      assertTrue("bloom missed inserted value: " + v, bf.mightContain(new BytesRef(v)));
    }
  }

  public void testFalsePositiveRateWithinBound() {
    int expected = 10_000;
    double targetFpp = 0.01;
    OlapBloomFilter bf = OlapBloomFilter.create(expected, targetFpp);
    // Insert 'expected' distinct keyword values.
    for (int i = 0; i < expected; i++) {
      bf.add(new BytesRef("present_" + i));
    }
    // Probe with disjoint values.
    int trials = 100_000;
    int falsePositives = 0;
    Random rng = new Random(42);
    for (int i = 0; i < trials; i++) {
      String probe = "absent_" + rng.nextInt(Integer.MAX_VALUE);
      if (bf.mightContain(new BytesRef(probe))) falsePositives++;
    }
    double observed = (double) falsePositives / trials;
    // Allow generous headroom (3x) because sizing rounds up to next power-of-two and mis-sizing
    // can skew actual FPP; bound still below 0.05 for a 0.01 target.
    assertTrue(
        "observed FPP " + observed + " exceeds 3x target " + targetFpp, observed < targetFpp * 3.0);
  }

  public void testSerdeRoundTripPreservesBits() {
    OlapBloomFilter src = OlapBloomFilter.create(1000);
    String[] values = {"alpha", "bravo", "charlie", "delta", "echo"};
    for (String v : values) {
      src.add(new BytesRef(v));
    }
    byte[] bytes = src.toBytes();
    OlapBloomFilter decoded = OlapBloomFilter.fromBytes(bytes);
    for (String v : values) {
      assertTrue("decoded filter missed: " + v, decoded.mightContain(new BytesRef(v)));
    }
    assertEquals(src.hashCount(), decoded.hashCount());
    assertEquals(src.setSizeBits(), decoded.setSizeBits());
  }

  public void testEncodeKeyKeywordUtf8() {
    BytesRef br = OlapBloomFilter.encodeKey("hello", "keyword");
    assertNotNull(br);
    assertEquals("hello", br.utf8ToString());
  }

  public void testEncodeKeyIntegerBigEndian() {
    BytesRef br = OlapBloomFilter.encodeKey(0x01020304, "integer");
    assertNotNull(br);
    assertEquals(4, br.length);
    assertEquals((byte) 0x01, br.bytes[br.offset]);
    assertEquals((byte) 0x02, br.bytes[br.offset + 1]);
    assertEquals((byte) 0x03, br.bytes[br.offset + 2]);
    assertEquals((byte) 0x04, br.bytes[br.offset + 3]);
  }

  public void testEncodeKeyLongBigEndian() {
    BytesRef br = OlapBloomFilter.encodeKey(0x0102030405060708L, "long");
    assertNotNull(br);
    assertEquals(8, br.length);
    assertEquals((byte) 0x01, br.bytes[br.offset]);
    assertEquals((byte) 0x08, br.bytes[br.offset + 7]);
  }

  public void testEncodeKeySymmetryIntegerLong() {
    // Coordinator inserts (value, "integer") — probe reads numeric doc value as long and calls
    // encodeKey(long, "integer"). The encoded bytes must match so bloom membership holds.
    int value = 42;
    BytesRef coordSide = OlapBloomFilter.encodeKey(value, "integer");
    BytesRef probeSide = OlapBloomFilter.encodeKey((long) value, "integer");
    assertEquals(coordSide, probeSide);
  }

  public void testEncodeKeyRejectsUnsupportedType() {
    assertNull(OlapBloomFilter.encodeKey("value", "date"));
    assertNull(OlapBloomFilter.encodeKey(1.5, "double"));
  }

  public void testIsSupportedType() {
    assertTrue(OlapBloomFilter.isSupportedType("keyword"));
    assertTrue(OlapBloomFilter.isSupportedType("text"));
    assertTrue(OlapBloomFilter.isSupportedType("integer"));
    assertTrue(OlapBloomFilter.isSupportedType("long"));
    assertFalse(OlapBloomFilter.isSupportedType("double"));
    assertFalse(OlapBloomFilter.isSupportedType("date"));
    assertFalse(OlapBloomFilter.isSupportedType(null));
  }

  public void testFromBytesRejectsBadVersion() {
    byte[] garbage = new byte[] {99, 0, 0, 0, 0, 0, 0, 0, 0};
    expectThrows(IllegalArgumentException.class, () -> OlapBloomFilter.fromBytes(garbage));
  }

  public void testMergeUnionsMembership() {
    // Two partial blooms with identical sizing. After merge, membership reflects both partials.
    OlapBloomFilter left = OlapBloomFilter.create(1000);
    OlapBloomFilter right = OlapBloomFilter.create(1000);
    for (int i = 0; i < 200; i++) left.add(new BytesRef("left_" + i));
    for (int i = 0; i < 200; i++) right.add(new BytesRef("right_" + i));

    OlapBloomFilter merged = OlapBloomFilter.merge(List.of(left, right));
    assertNotNull(merged);
    for (int i = 0; i < 200; i++) {
      assertTrue(merged.mightContain(new BytesRef("left_" + i)));
      assertTrue(merged.mightContain(new BytesRef("right_" + i)));
    }
  }

  public void testMergeInPlaceDoesNotAffectOtherInput() {
    // Merging into `dst` must not mutate `src`.
    OlapBloomFilter dst = OlapBloomFilter.create(500);
    OlapBloomFilter src = OlapBloomFilter.create(500);
    src.add(new BytesRef("only_in_src"));

    assertFalse(dst.mightContain(new BytesRef("only_in_src")));
    dst.mergeInPlace(src);
    assertTrue(dst.mightContain(new BytesRef("only_in_src")));
    // src still contains its value and nothing else.
    assertTrue(src.mightContain(new BytesRef("only_in_src")));
  }

  public void testMergeRejectsMismatchedSizing() {
    OlapBloomFilter a = OlapBloomFilter.create(1000);
    OlapBloomFilter b = OlapBloomFilter.create(100_000);
    expectThrows(IllegalArgumentException.class, () -> a.mergeInPlace(b));
  }

  public void testMergeSerdeRoundTrip() {
    // Simulate the two-stage wire path: partial blooms are serialized on data nodes and
    // deserialized on the coordinator before merging.
    OlapBloomFilter a = OlapBloomFilter.create(1000);
    OlapBloomFilter b = OlapBloomFilter.create(1000);
    for (int i = 0; i < 50; i++) a.add(new BytesRef("a_" + i));
    for (int i = 0; i < 50; i++) b.add(new BytesRef("b_" + i));

    OlapBloomFilter ra = OlapBloomFilter.fromBytes(a.toBytes());
    OlapBloomFilter rb = OlapBloomFilter.fromBytes(b.toBytes());
    OlapBloomFilter merged = OlapBloomFilter.merge(List.of(ra, rb));

    for (int i = 0; i < 50; i++) {
      assertTrue(merged.mightContain(new BytesRef("a_" + i)));
      assertTrue(merged.mightContain(new BytesRef("b_" + i)));
    }
  }

  public void testMergeReturnsNullForEmpty() {
    assertNull(OlapBloomFilter.merge(List.of()));
  }
}
