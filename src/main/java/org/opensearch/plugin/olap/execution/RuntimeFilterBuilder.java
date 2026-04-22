/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.execution;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.util.BytesRef;

/**
 * Builds a Lucene Query from runtime filter values extracted from the build side of a join. The
 * query is pushed down to the probe side's Lucene scan to skip non-matching documents at the index
 * level.
 *
 * <p>Supports keyword (TermInSetQuery), integer (IntPoint set query), and long (LongPoint set
 * query) field types.
 */
public final class RuntimeFilterBuilder {

  private RuntimeFilterBuilder() {}

  /**
   * Build a Lucene pushdown query from a set of join key values.
   *
   * @param fieldName the probe-side join key field name
   * @param values distinct join key values from the build side
   * @param fieldType OpenSearch field type ("keyword", "integer", "long", etc.)
   * @return a Lucene Query, or null if the type is unsupported or values are empty
   */
  public static Query build(String fieldName, Set<?> values, String fieldType) {
    if (values == null || values.isEmpty() || fieldName == null) {
      return null;
    }

    switch (fieldType) {
      case "keyword":
      case "text":
        return buildTermInSetQuery(fieldName, values);
      case "integer":
        return buildIntSetQuery(fieldName, values);
      case "long":
        return buildLongSetQuery(fieldName, values);
      default:
        return null;
    }
  }

  /**
   * Build a Lucene pushdown query from a BLOOM runtime filter. Returns a {@link BloomFilterQuery}
   * that iterates doc values for {@code fieldName} and keeps only docs passing {@link
   * OlapBloomFilter#mightContain}.
   */
  public static Query buildBloom(String fieldName, String fieldType, OlapBloomFilter bloom) {
    if (fieldName == null || bloom == null) return null;
    switch (fieldType) {
      case "keyword":
      case "text":
      case "integer":
      case "long":
        return new BloomFilterQuery(fieldName, fieldType, bloom);
      default:
        return null;
    }
  }

  /**
   * Combine an existing pushdown query with a runtime filter query using AND.
   *
   * @return the combined query, or whichever is non-null, or null if both are null
   */
  public static Query combine(Query existing, Query runtimeFilter) {
    if (existing == null && runtimeFilter == null) return null;
    if (existing == null) return runtimeFilter;
    if (runtimeFilter == null) return existing;
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    builder.add(existing, BooleanClause.Occur.MUST);
    builder.add(runtimeFilter, BooleanClause.Occur.MUST);
    return builder.build();
  }

  private static Query buildTermInSetQuery(String fieldName, Set<?> values) {
    List<BytesRef> terms = new ArrayList<>(values.size());
    for (Object v : values) {
      if (v != null) {
        terms.add(new BytesRef(v.toString()));
      }
    }
    if (terms.isEmpty()) return null;
    return new TermInSetQuery(fieldName, terms);
  }

  private static Query buildIntSetQuery(String fieldName, Set<?> values) {
    int[] intValues = new int[values.size()];
    int i = 0;
    for (Object v : values) {
      if (v instanceof Number) {
        intValues[i++] = ((Number) v).intValue();
      }
    }
    if (i == 0) return null;
    if (i < intValues.length) {
      int[] trimmed = new int[i];
      System.arraycopy(intValues, 0, trimmed, 0, i);
      return IntPoint.newSetQuery(fieldName, trimmed);
    }
    return IntPoint.newSetQuery(fieldName, intValues);
  }

  private static Query buildLongSetQuery(String fieldName, Set<?> values) {
    long[] longValues = new long[values.size()];
    int i = 0;
    for (Object v : values) {
      if (v instanceof Number) {
        longValues[i++] = ((Number) v).longValue();
      }
    }
    if (i == 0) return null;
    if (i < longValues.length) {
      long[] trimmed = new long[i];
      System.arraycopy(longValues, 0, trimmed, 0, i);
      return LongPoint.newSetQuery(fieldName, trimmed);
    }
    return LongPoint.newSetQuery(fieldName, longValues);
  }
}
