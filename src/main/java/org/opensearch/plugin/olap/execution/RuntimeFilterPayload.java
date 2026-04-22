/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.execution;

import java.util.List;

/**
 * Runtime filter extracted from the build side of a broadcast hash join. Either TERMS (carries a
 * distinct-value list for Lucene pushdown) or BLOOM (carries serialized bloom bytes for a Java-
 * layer probe-feeder predicate), or NONE when no RF was produced.
 */
public final class RuntimeFilterPayload {

  private final RfKind kind;
  private final String fieldName;
  private final String fieldType;
  private final List<String> termsValues; // non-null iff kind == TERMS
  private final byte[] bloomBytes; // non-null iff kind == BLOOM

  private RuntimeFilterPayload(
      RfKind kind,
      String fieldName,
      String fieldType,
      List<String> termsValues,
      byte[] bloomBytes) {
    this.kind = kind;
    this.fieldName = fieldName;
    this.fieldType = fieldType;
    this.termsValues = termsValues;
    this.bloomBytes = bloomBytes;
  }

  public static RuntimeFilterPayload none() {
    return new RuntimeFilterPayload(RfKind.NONE, null, null, null, null);
  }

  public static RuntimeFilterPayload terms(
      String fieldName, String fieldType, List<String> values) {
    return new RuntimeFilterPayload(RfKind.TERMS, fieldName, fieldType, values, null);
  }

  public static RuntimeFilterPayload bloom(String fieldName, String fieldType, byte[] bloomBytes) {
    return new RuntimeFilterPayload(RfKind.BLOOM, fieldName, fieldType, null, bloomBytes);
  }

  public RfKind getKind() {
    return kind;
  }

  public String getFieldName() {
    return fieldName;
  }

  public String getFieldType() {
    return fieldType;
  }

  public List<String> getTermsValues() {
    return termsValues;
  }

  public byte[] getBloomBytes() {
    return bloomBytes;
  }

  public boolean hasFilter() {
    return kind != RfKind.NONE;
  }
}
