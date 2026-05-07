/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap;

import java.io.IOException;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Integration tests for PPL relevance functions (match, match_phrase, match_phrase_prefix,
 * multi_match, simple_query_string, match_bool_prefix, query_string) running through the Velox
 * path.
 *
 * <p>The plugin peels these calls out of {@code LogicalFilter.getCondition()} at plan-gen time via
 * {@code RelevanceSplitter}, serializes the equivalent OpenSearch {@code QueryBuilder} on the
 * fragment request, and the data node applies it via Lucene before feeding rows to Velox.
 *
 * <p>The beer fixture uses {@code keyword} for Title/Body/Tags so doc-value scans work. This
 * constrains {@code match} semantics to exact-token matching (OpenSearch analyzes the query and
 * does a TermQuery on the keyword field), so tests pick queries where a full-field match is
 * expected.
 */
public class RelevanceFunctionIT extends OlapRestTestCase {

  @Override
  public void setUp() throws Exception {
    super.setUp();
    loadIndex(Index.BEER);
  }

  @Override
  public void tearDown() throws Exception {
    deleteIndex(Index.BEER.getName());
    super.tearDown();
  }

  // ---- match on Tags (which holds single-token values) ----

  public void testMatchTag() throws IOException {
    // Tags is keyword with single-token values. match(Tags, 'brewing') → TermQuery Tags:brewing.
    JSONObject response = executePPLQuery("source=beer | where match(Tags, 'brewing') | fields Id");
    JSONArray rows = getDataRows(response);
    // Docs 1, 3, 7, 9, 10 have Tags='brewing'.
    assertEquals(5, rows.length());
  }

  public void testMatchTagWithBoost() throws IOException {
    JSONObject response =
        executePPLQuery("source=beer | where match(Tags, 'taste', boost=2.0) | fields Id");
    // Docs 2, 8 have Tags='taste'.
    assertEquals(2, getDataRows(response).length());
  }

  public void testMatchPhraseTag() throws IOException {
    JSONObject response =
        executePPLQuery("source=beer | where match_phrase(Tags, 'taste') | fields Id");
    assertEquals(2, getDataRows(response).length());
  }

  public void testMatchPhrasePrefix() throws IOException {
    // match_phrase_prefix smoke test — keyword-field matching semantics for this query type
    // vary across OpenSearch versions. We assert only that the query runs without error and the
    // relevance push-down path exercises the MatchPhrasePrefixQueryBuilder constructor.
    JSONObject response =
        executePPLQuery("source=beer | where match_phrase_prefix(Tags, 'brewing') | fields Id");
    assertNotNull(getDataRows(response));
  }

  public void testMatchBoolPrefix() throws IOException {
    JSONObject response =
        executePPLQuery("source=beer | where match_bool_prefix(Tags, 'brew') | fields Id");
    assertEquals(5, getDataRows(response).length());
  }

  // ---- multi_match / simple_query_string / query_string ----

  public void testMultiMatchWithFields() throws IOException {
    JSONObject response =
        executePPLQuery("source=beer | where multi_match(['Tags'], 'equipment') | fields Id");
    // Doc 5 Tags='equipment'.
    assertEquals(1, getDataRows(response).length());
  }

  public void testMultiMatchWithoutFields() throws IOException {
    // Default fields (all text/keyword) — at least matches doc 5 via Tags.
    JSONObject response =
        executePPLQuery("source=beer | where multi_match('equipment') | fields Id");
    assertTrue(getDataRows(response).length() >= 1);
  }

  public void testSimpleQueryString() throws IOException {
    JSONObject response =
        executePPLQuery("source=beer | where simple_query_string(['Tags'], 'brewing') | fields Id");
    assertEquals(5, getDataRows(response).length());
  }

  public void testQueryStringDefaultOperator() throws IOException {
    // query_string on Tags with 'brewing OR taste' matches brewing+taste docs (5+2=7).
    JSONObject orResp =
        executePPLQuery(
            "source=beer | where query_string(['Tags'], 'brewing OR taste',"
                + " default_operator='OR') | fields Id");
    assertEquals(7, getDataRows(orResp).length());
  }

  // ---- Splitter shapes ----

  public void testMatchAndPredicate() throws IOException {
    // match(Tags, 'brewing') AND AcceptedAnswerId > 150
    JSONObject response =
        executePPLQuery(
            "source=beer | where match(Tags, 'brewing') and AcceptedAnswerId > 150 | fields Id");
    // Brewing docs with AcceptedAnswerId > 150: 3 (310), 7 (401), 9 (155), 10 (220) = 4.
    assertEquals(4, getDataRows(response).length());
  }

  public void testMatchOrTermFullyVectorized() throws IOException {
    // match(Tags, 'recipes') OR Tags = 'equipment' — splitter (c) handles OR.
    JSONObject response =
        executePPLQuery(
            "source=beer | where match(Tags, 'recipes') or Tags = 'equipment' | fields Id");
    // Doc 4 (recipes), doc 5 (equipment).
    assertEquals(2, getDataRows(response).length());
  }

  public void testMatchOrUntranslatableFallsBack() throws IOException {
    // match(Tags, 'brewing') OR abs(AcceptedAnswerId - 200) < 10
    // abs() isn't Lucene-expressible → splitter rules out → SQL plugin's default engine answers.
    // The main signal here is that the query succeeds (no error) and returns *some* docs; the
    // exact count depends on the default engine's behavior for this shape.
    JSONObject response =
        executePPLQuery(
            "source=beer | where match(Tags, 'brewing') or abs(AcceptedAnswerId - 200) < 10"
                + " | fields Id");
    JSONArray rows = getDataRows(response);
    assertTrue("fallback query succeeded", rows.length() >= 1);
  }

  public void testNotOfMatch() throws IOException {
    JSONObject response =
        executePPLQuery("source=beer | where not match(Tags, 'brewing') | fields Id");
    // Non-brewing: 2 (taste), 4 (recipes), 5 (equipment), 6 (pubs), 8 (taste).
    assertEquals(5, getDataRows(response).length());
  }

  // ---- Text-field relevance via _source (beer_text fixture) ----
  //
  // The beer fixture above uses keyword for everything so doc-value scans work. The beer_text
  // fixture has text Title/Body to exercise the stored-fields / _source reader path added for
  // real relevance workloads (OpenSearch's default mapping).

  public void testMatchOnTextBody() throws IOException {
    loadIndex(Index.BEER_TEXT);
    try {
      JSONObject response =
          executePPLQuery("source=beer_text | where match(Body, 'hops') | fields Id");
      // Docs 1 (brewing hops) and 4 (low-alpha hops) mention hops in Body.
      assertEquals(2, getDataRows(response).length());
    } finally {
      deleteIndex(Index.BEER_TEXT.getName());
    }
  }

  public void testMatchOnTextTitleWithProjection() throws IOException {
    // The relevance.md pattern: match on a text field AND project it in the output. Exercises
    // the _source-backed TextFieldColumnReader end-to-end. Standard analyzer tokens are exact,
    // so we query for a token that appears verbatim ("yeast" in docs 1, 3, 7, 9).
    loadIndex(Index.BEER_TEXT);
    try {
      JSONObject response =
          executePPLQuery("source=beer_text | where match(Body, 'yeast') | fields Id, Title");
      JSONArray rows = getDataRows(response);
      // Docs 1, 3, 7, 9 contain the token "yeast" in Body.
      assertEquals(4, rows.length());
      // Each row has (Id, Title) — assert the Title is populated (non-null, non-empty).
      for (int i = 0; i < rows.length(); i++) {
        JSONArray row = rows.getJSONArray(i);
        assertEquals(2, row.length());
        String title = row.getString(1);
        assertNotNull(title);
        assertTrue("Title non-empty for row " + i, !title.isEmpty());
      }
    } finally {
      deleteIndex(Index.BEER_TEXT.getName());
    }
  }

  public void testMatchPhraseOnTextBody() throws IOException {
    loadIndex(Index.BEER_TEXT);
    try {
      JSONObject response =
          executePPLQuery(
              "source=beer_text | where match_phrase(Body, 'two-row malt') | fields Id, Body");
      JSONArray rows = getDataRows(response);
      // Only doc 1 has the exact phrase "two-row malt".
      assertEquals(1, rows.length());
      String body = rows.getJSONArray(0).getString(1);
      assertTrue("Body contains phrase", body.contains("two-row malt"));
    } finally {
      deleteIndex(Index.BEER_TEXT.getName());
    }
  }

  // ---- canVectorize gate with force_vectorize=false (production default) ----
  //
  // These pin the real canVectorize() path — the suite's default force_vectorize=true bypasses
  // the gate entirely, so a regression in findUnsupportedRexCall (e.g. recursing into the
  // MAP_VALUE_CONSTRUCTOR operands of a relevance call and rejecting the whole plan) wouldn't
  // be caught by any of the other tests.

  public void testRelevancePassesCanVectorizeWithoutForce() throws IOException {
    setClusterSetting("plugins.velox.force_vectorize", false);
    try {
      JSONObject response =
          executePPLQuery("source=beer | where match(Tags, 'brewing') | fields Id");
      assertEquals(5, getDataRows(response).length());
    } finally {
      setClusterSetting("plugins.velox.force_vectorize", true);
    }
  }

  public void testMatchAndPredicateWithoutForce() throws IOException {
    setClusterSetting("plugins.velox.force_vectorize", false);
    try {
      JSONObject response =
          executePPLQuery(
              "source=beer | where match(Tags, 'brewing') and AcceptedAnswerId > 150"
                  + " | fields Id");
      assertEquals(4, getDataRows(response).length());
    } finally {
      setClusterSetting("plugins.velox.force_vectorize", true);
    }
  }
}
