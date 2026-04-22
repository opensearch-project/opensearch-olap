/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.execution;

/**
 * Runtime filter variant selected at build-side extraction.
 *
 * <p>Ordered by cardinality: NONE when RF is disabled or cardinality exceeds all caps, TERMS for
 * small distinct-value sets (pushed to Lucene as TermInSetQuery/PointInSetQuery), BLOOM for larger
 * sets that would blow the TERMS serialization cost (applied as a Java-layer predicate in the probe
 * feeder).
 */
public enum RfKind {
  NONE((byte) 0),
  TERMS((byte) 1),
  BLOOM((byte) 2);

  private final byte wireByte;

  RfKind(byte wireByte) {
    this.wireByte = wireByte;
  }

  public byte toByte() {
    return wireByte;
  }

  public static RfKind fromByte(byte b) {
    switch (b) {
      case 0:
        return NONE;
      case 1:
        return TERMS;
      case 2:
        return BLOOM;
      default:
        throw new IllegalArgumentException("Unknown RfKind wire byte: " + b);
    }
  }
}
