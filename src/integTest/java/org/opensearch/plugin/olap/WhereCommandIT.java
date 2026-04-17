/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap;

import java.io.IOException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Ignore;

/**
 * Integration tests for WHERE command on nested object fields. Migrated from the SQL plugin's
 * CalciteWhereCommandIT. Tests filter operations on nested fields like address.city, city.name,
 * projects.name etc.
 *
 * <p>These tests use OpenSearch {@code nested} type fields. Unlike {@code object} type (which
 * stores flattened dot-path doc values on the parent document), {@code nested} type stores data as
 * hidden Lucene documents that require special nested query handling. The OLAP plugin currently
 * supports {@code object} type (MAP→ROW with StructVector) but not {@code nested} type.
 */
public class WhereCommandIT extends OlapRestTestCase {

  @Override
  public void setUp() throws Exception {
    super.setUp();
    loadIndex(Index.NESTED_SIMPLE);
    loadIndex(Index.DEEP_NESTED);
  }

  @Override
  public void tearDown() throws Exception {
    deleteIndex(Index.NESTED_SIMPLE.getName());
    deleteIndex(Index.DEEP_NESTED.getName());
    super.tearDown();
  }

  @Ignore("Nested type requires hidden Lucene doc reading — not yet supported by LuceneArrowReader")
  public void testFilterOnNestedField() throws IOException {
    JSONObject result =
        executePPLQuery(
            "source = "
                + Index.NESTED_SIMPLE.getName()
                + " | where address.city = 'New york city' | fields address.city");
    JSONArray rows = getDataRows(result);
    assertEquals(1, rows.length());
    assertEquals("New york city", rows.getJSONArray(0).getString(0));
  }

  @Ignore("Nested type requires hidden Lucene doc reading — not yet supported by LuceneArrowReader")
  public void testFilterOnNestedFieldWithIn() throws IOException {
    JSONObject result =
        executePPLQuery(
            "source = "
                + Index.NESTED_SIMPLE.getName()
                + " | where address.city in ('Miami', 'san diego') | fields address.city");
    JSONArray rows = getDataRows(result);
    assertEquals(2, rows.length());
  }

  @Ignore("Nested type requires hidden Lucene doc reading — not yet supported by LuceneArrowReader")
  public void testFilterOnComputedNestedField() throws IOException {
    JSONObject result =
        executePPLQuery(
            "source = "
                + Index.DEEP_NESTED.getName()
                + " | eval proj_name_len=length(projects.name)"
                + " | fields projects.name, proj_name_len"
                + " | where proj_name_len > 29");
    JSONArray rows = getDataRows(result);
    assertEquals(1, rows.length());
    assertEquals("AWS Redshift Spectrum querying", rows.getJSONArray(0).getString(0));
    assertEquals(30, rows.getJSONArray(0).getInt(1));
  }

  @Ignore("Nested type requires hidden Lucene doc reading — not yet supported by LuceneArrowReader")
  public void testFilterOnNestedAndRootFields() throws IOException {
    JSONObject result =
        executePPLQuery(
            "source = "
                + Index.DEEP_NESTED.getName()
                + " | where city.name = 'Seattle' and length(projects.name) > 29"
                + " | fields city.name, projects.name");
    JSONArray rows = getDataRows(result);
    assertEquals(1, rows.length());
    assertEquals("Seattle", rows.getJSONArray(0).getString(0));
    assertEquals("AWS Redshift Spectrum querying", rows.getJSONArray(0).getString(1));
  }
}
