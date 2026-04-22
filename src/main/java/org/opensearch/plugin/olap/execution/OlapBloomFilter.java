/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.execution;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.hash.MurmurHash3;

/**
 * Incremental bloom filter used for runtime-filter probes on the build side of a broadcast join.
 *
 * <p>OpenSearch core's {@code org.opensearch.index.codec.fuzzy.BloomFilter} requires all elements
 * at construction (iterator-based {@code addAll}) and is designed for index-time codec use. The
 * runtime filter pipeline needs incremental inserts as the coordinator streams distinct join keys
 * off the broadcast build batches, so this class implements a self-contained bloom filter with a
 * stable serialization format.
 *
 * <p>Uses MurmurHash3_128 for double hashing (Kirsch-Mitzenmacher): {@code h_i(x) = h1 + i*h2} — a
 * single 128-bit hash call per insert/lookup produces {@code hashCount} independent indices. Bit
 * count is rounded up to a power of two so the modulo-set-size reduces to a bitwise AND.
 *
 * <p>Serialization (big-endian, version-tagged):
 *
 * <pre>
 *   [version: byte = 1]
 *   [hashCount: int]
 *   [setSizeLog2: int]       // number of bits = 1 &lt;&lt; setSizeLog2
 *   [bitset words: long[numWords]]
 * </pre>
 *
 * <p>Also exposes {@link #encodeKey(Object, String)} as the single source of truth for how join key
 * values are turned into {@link BytesRef}s on both the build and probe sides. If the two encoders
 * drift, the filter silently produces false negatives — so both sides must call this method.
 */
public final class OlapBloomFilter {

  static final byte WIRE_VERSION = 1;

  /** Default false-positive probability used when sizing the filter. */
  public static final double DEFAULT_FPP = 0.01;

  private final long[] bits;
  private final int setSizeMask; // bits.length * 64 - 1, usable because size is power of two
  private final int hashCount;

  private OlapBloomFilter(long[] bits, int hashCount) {
    this.bits = bits;
    this.hashCount = hashCount;
    long sizeBits = (long) bits.length * 64L;
    this.setSizeMask = (int) (sizeBits - 1);
  }

  /**
   * Create an empty bloom filter sized for the expected number of insertions at the given target
   * FPP. The bit count is rounded up to the next power of two.
   */
  public static OlapBloomFilter create(int expectedInsertions, double fpp) {
    if (expectedInsertions <= 0) {
      throw new IllegalArgumentException("expectedInsertions must be positive");
    }
    if (fpp <= 0.0 || fpp >= 1.0) {
      throw new IllegalArgumentException("fpp must be in (0, 1)");
    }
    // Standard bloom sizing: m = -n*ln(p) / (ln(2)^2)
    double ln2 = Math.log(2);
    int mBits =
        (int)
            Math.min(
                Integer.MAX_VALUE, Math.ceil(-expectedInsertions * Math.log(fpp) / (ln2 * ln2)));
    int setSizeBits = nextPow2(Math.max(mBits, 64));
    int hashCount =
        Math.max(1, (int) Math.round((setSizeBits / (double) expectedInsertions) * ln2));
    long[] bits = new long[setSizeBits / 64];
    return new OlapBloomFilter(bits, hashCount);
  }

  /** Convenience overload using {@link #DEFAULT_FPP}. */
  public static OlapBloomFilter create(int expectedInsertions) {
    return create(expectedInsertions, DEFAULT_FPP);
  }

  /** Insert a value. Idempotent. */
  public void add(BytesRef value) {
    MurmurHash3.Hash128 h = new MurmurHash3.Hash128();
    MurmurHash3.hash128(value.bytes, value.offset, value.length, 0L, h);
    long h1 = h.h1;
    long h2 = h.h2;
    for (int i = 0; i < hashCount; i++) {
      int pos = (int) ((h1 + (long) i * h2) & setSizeMask);
      bits[pos >>> 6] |= (1L << (pos & 63));
    }
  }

  /** Bloom probe: false → definitely not present, true → possibly present. */
  public boolean mightContain(BytesRef value) {
    MurmurHash3.Hash128 h = new MurmurHash3.Hash128();
    MurmurHash3.hash128(value.bytes, value.offset, value.length, 0L, h);
    long h1 = h.h1;
    long h2 = h.h2;
    for (int i = 0; i < hashCount; i++) {
      int pos = (int) ((h1 + (long) i * h2) & setSizeMask);
      if ((bits[pos >>> 6] & (1L << (pos & 63))) == 0L) {
        return false;
      }
    }
    return true;
  }

  /** Serialize to the wire format described in the class Javadoc. */
  public byte[] toBytes() {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos)) {
      out.writeByte(WIRE_VERSION);
      out.writeInt(hashCount);
      out.writeInt(Integer.numberOfTrailingZeros(setSizeMask + 1));
      for (long word : bits) {
        out.writeLong(word);
      }
      out.flush();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Inverse of {@link #toBytes()}. */
  public static OlapBloomFilter fromBytes(byte[] wire) {
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(wire))) {
      byte version = in.readByte();
      if (version != WIRE_VERSION) {
        throw new IllegalArgumentException("Unsupported OlapBloomFilter wire version: " + version);
      }
      int hashCount = in.readInt();
      int setSizeLog2 = in.readInt();
      int numWords = (1 << setSizeLog2) / 64;
      long[] bits = new long[numWords];
      for (int i = 0; i < numWords; i++) {
        bits[i] = in.readLong();
      }
      return new OlapBloomFilter(bits, hashCount);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Encode a join-key value to a {@link BytesRef} using a canonical form shared by build and probe
   * sides. Keyword values are UTF-8 bytes; integer and long are big-endian fixed-width (4 and 8
   * bytes). The encoding is intentionally independent of Lucene's point-packing so the probe side
   * (which reads raw numeric doc values) can produce bytes matching the build side.
   *
   * @return a BytesRef, or null if the type is unsupported
   */
  public static BytesRef encodeKey(Object value, String fieldType) {
    if (value == null || fieldType == null) return null;
    switch (fieldType) {
      case "keyword":
      case "text":
        return new BytesRef(value.toString());
      case "integer":
        {
          int v;
          if (value instanceof Number) {
            v = ((Number) value).intValue();
          } else {
            try {
              v = Integer.parseInt(value.toString());
            } catch (NumberFormatException e) {
              return null;
            }
          }
          byte[] b = new byte[4];
          b[0] = (byte) (v >>> 24);
          b[1] = (byte) (v >>> 16);
          b[2] = (byte) (v >>> 8);
          b[3] = (byte) v;
          return new BytesRef(b);
        }
      case "long":
        {
          long v;
          if (value instanceof Number) {
            v = ((Number) value).longValue();
          } else {
            try {
              v = Long.parseLong(value.toString());
            } catch (NumberFormatException e) {
              return null;
            }
          }
          byte[] b = new byte[8];
          for (int i = 7; i >= 0; i--) {
            b[i] = (byte) v;
            v >>>= 8;
          }
          return new BytesRef(b);
        }
      default:
        return null;
    }
  }

  /** Supported field types for {@link #encodeKey}. */
  public static boolean isSupportedType(String fieldType) {
    return "keyword".equals(fieldType)
        || "text".equals(fieldType)
        || "integer".equals(fieldType)
        || "long".equals(fieldType);
  }

  /** Number of bits in the backing bitset. Visible for tests/metrics. */
  public int setSizeBits() {
    return setSizeMask + 1;
  }

  /** Number of hash functions used. Visible for tests/metrics. */
  public int hashCount() {
    return hashCount;
  }

  /**
   * Union-merge {@code other} into this filter via bitwise-OR. The two filters must have been
   * created with identical sizing parameters (same {@code setSizeBits} and {@code hashCount}). The
   * coordinator enforces this by sending the same {@code expectedInsertions} to every data node in
   * a two-stage distributed build.
   *
   * <p>After a merge, membership reflects values inserted into either filter. This is the core of
   * the two-stage build: each data node produces a PARTIAL bloom, and the coordinator folds them
   * into a FINAL bloom with one OR pass per word.
   *
   * @throws IllegalArgumentException if bit size or hash count differ
   */
  public void mergeInPlace(OlapBloomFilter other) {
    if (other.bits.length != this.bits.length) {
      throw new IllegalArgumentException(
          "Cannot merge bloom filters with different bit sizes: "
              + this.bits.length * 64
              + " vs "
              + other.bits.length * 64);
    }
    if (other.hashCount != this.hashCount) {
      throw new IllegalArgumentException(
          "Cannot merge bloom filters with different hash counts: "
              + this.hashCount
              + " vs "
              + other.hashCount);
    }
    for (int i = 0; i < bits.length; i++) {
      bits[i] |= other.bits[i];
    }
  }

  /**
   * Union-merge all {@code filters} into a new filter. All filters must share identical sizing
   * parameters. Returns null if the list is empty.
   */
  public static OlapBloomFilter merge(List<OlapBloomFilter> filters) {
    if (filters == null || filters.isEmpty()) return null;
    OlapBloomFilter first = filters.get(0);
    long[] mergedBits = first.bits.clone();
    int hashCount = first.hashCount;
    OlapBloomFilter result = new OlapBloomFilter(mergedBits, hashCount);
    for (int i = 1; i < filters.size(); i++) {
      result.mergeInPlace(filters.get(i));
    }
    return result;
  }

  private static int nextPow2(int x) {
    if (x <= 1) return 1;
    int n = x - 1;
    n |= n >>> 1;
    n |= n >>> 2;
    n |= n >>> 4;
    n |= n >>> 8;
    n |= n >>> 16;
    return n + 1;
  }
}
