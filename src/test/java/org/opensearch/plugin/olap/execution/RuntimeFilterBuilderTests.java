/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.execution;

import java.util.Set;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.opensearch.test.OpenSearchTestCase;

public class RuntimeFilterBuilderTests extends OpenSearchTestCase {

  // ---- build() tests ----

  public void testBuildKeywordTermsQuery() {
    Set<Object> values = Set.of("Engineering", "Marketing", "Sales");
    Query q = RuntimeFilterBuilder.build("dept_name", values, "keyword");
    assertNotNull("Should build query for keyword", q);
    assertTrue("Should be TermInSetQuery", q instanceof TermInSetQuery);
  }

  public void testBuildTextTermsQuery() {
    Set<Object> values = Set.of("alice", "bob");
    Query q = RuntimeFilterBuilder.build("name", values, "text");
    assertNotNull("Should build query for text", q);
    assertTrue("Should be TermInSetQuery", q instanceof TermInSetQuery);
  }

  public void testBuildIntegerSetQuery() {
    Set<Object> values = Set.of(10, 20, 30);
    Query q = RuntimeFilterBuilder.build("dept_id", values, "integer");
    assertNotNull("Should build query for integer", q);
  }

  public void testBuildLongSetQuery() {
    Set<Object> values = Set.of(100L, 200L, 300L);
    Query q = RuntimeFilterBuilder.build("id", values, "long");
    assertNotNull("Should build query for long", q);
  }

  public void testBuildUnsupportedTypeReturnsNull() {
    Set<Object> values = Set.of(1.5, 2.5);
    Query q = RuntimeFilterBuilder.build("price", values, "double");
    assertNull("Should return null for unsupported type", q);
  }

  public void testBuildNullValuesReturnsNull() {
    assertNull(RuntimeFilterBuilder.build("field", null, "keyword"));
  }

  public void testBuildEmptyValuesReturnsNull() {
    assertNull(RuntimeFilterBuilder.build("field", Set.of(), "keyword"));
  }

  public void testBuildNullFieldNameReturnsNull() {
    assertNull(RuntimeFilterBuilder.build(null, Set.of("a"), "keyword"));
  }

  public void testBuildSingleValue() {
    Set<Object> values = Set.of("Engineering");
    Query q = RuntimeFilterBuilder.build("dept_name", values, "keyword");
    assertNotNull(q);
  }

  // ---- combine() tests ----

  public void testCombineBothNull() {
    assertNull(RuntimeFilterBuilder.combine(null, null));
  }

  public void testCombineExistingOnlyReturnsExisting() {
    Query existing = new MatchAllDocsQuery();
    assertSame(existing, RuntimeFilterBuilder.combine(existing, null));
  }

  public void testCombineRfOnlyReturnsRf() {
    Set<Object> values = Set.of(10, 20);
    Query rf = RuntimeFilterBuilder.build("dept_id", values, "integer");
    assertSame(rf, RuntimeFilterBuilder.combine(null, rf));
  }

  public void testCombineBothReturnsBooleanAnd() {
    Query existing = new MatchAllDocsQuery();
    Set<Object> values = Set.of(10, 20);
    Query rf = RuntimeFilterBuilder.build("dept_id", values, "integer");
    Query combined = RuntimeFilterBuilder.combine(existing, rf);
    assertNotNull(combined);
    assertTrue("Should be BooleanQuery", combined instanceof BooleanQuery);
    BooleanQuery bq = (BooleanQuery) combined;
    assertEquals("Should have 2 MUST clauses", 2, bq.clauses().size());
    for (BooleanClause clause : bq.clauses()) {
      assertEquals(BooleanClause.Occur.MUST, clause.occur());
    }
  }

  // ---- Bug regression: RF safety checks ----

  /**
   * Bug 1 regression: RF must only be applied for INNER joins. For outer joins, probe rows that
   * don't match any build-side key must be preserved (null-extended). If RF filters them at Lucene
   * level, they're lost. This test verifies that RuntimeFilterBuilder itself doesn't enforce join
   * type — that's the caller's responsibility — but combine(null, null) returns null (no filter
   * applied when RF is skipped).
   */
  public void testNoRfMeansNoFilter() {
    // When RF is skipped (e.g., for outer joins), combine(null, null) must return null
    // so that the probe scan reads ALL rows without any filter.
    assertNull(
        "No RF + no pushdown = no filter (all probe rows read)",
        RuntimeFilterBuilder.combine(null, null));
  }

  /**
   * Bug 2 regression: RF combine must not introduce cross-branch filters. When the caller correctly
   * passes only the RF query (not the build-side pushdown), combine(null, rf) must return exactly
   * the RF query — no additional filters from other branches.
   */
  public void testCombineRfOnlyDoesNotAddExtraFilters() {
    Set<Object> values = Set.of(10, 20, 30);
    Query rf = RuntimeFilterBuilder.build("dept_id", values, "integer");
    Query result = RuntimeFilterBuilder.combine(null, rf);
    // Must be exactly the RF query, not wrapped in a BooleanQuery
    assertSame("combine(null, rf) must return rf directly, not wrap it", rf, result);
  }

  /**
   * Verify that RF with values matching all probe rows still produces a valid query. Even if RF
   * doesn't filter anything, the query must be correct (not null or broken).
   */
  public void testRfWithAllMatchingValuesProducesValidQuery() {
    Set<Object> allDeptIds = Set.of(10, 20, 30);
    Query rf = RuntimeFilterBuilder.build("dept_id", allDeptIds, "integer");
    assertNotNull("RF with matching values should produce a non-null query", rf);
  }
}
