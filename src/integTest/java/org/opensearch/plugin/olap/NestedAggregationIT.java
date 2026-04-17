/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap;

import java.io.IOException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Ignore;

/**
 * Integration tests for aggregation on nested object fields. Migrated from the SQL plugin's
 * CalcitePPLNestedAggregationIT. Tests aggregation operations on nested fields like address.area,
 * address.city etc.
 *
 * <p>These tests use OpenSearch {@code nested} type fields which require hidden Lucene document
 * reading. The OLAP plugin currently supports {@code object} type (MAP→ROW with StructVector) but
 * not {@code nested} type.
 */
public class NestedAggregationIT extends OlapRestTestCase {

  @Override
  public void setUp() throws Exception {
    super.setUp();
    loadIndex(Index.NESTED_SIMPLE);
  }

  @Override
  public void tearDown() throws Exception {
    deleteIndex(Index.NESTED_SIMPLE.getName());
    super.tearDown();
  }

  @Ignore("Nested type requires hidden Lucene doc reading — not yet supported by LuceneArrowReader")
  public void testNestedAggregation() throws IOException {
    JSONObject result =
        executePPLQuery(
            "source = "
                + Index.NESTED_SIMPLE.getName()
                + " | stats count(address.area) as count_area,"
                + " min(address.area) as min_area,"
                + " max(address.area) as max_area,"
                + " avg(address.area) as avg_area,"
                + " avg(age) as avg_age");
    JSONArray rows = getDataRows(result);
    assertEquals(1, rows.length());
    JSONArray row = rows.getJSONArray(0);
    assertEquals(9, row.getLong(0));
    assertEquals(9.99, row.getDouble(1), 0.01);
    assertEquals(1000.99, row.getDouble(2), 0.01);
    assertEquals(300.115, row.getDouble(3), 0.1);
    assertEquals(25.2, row.getDouble(4), 0.01);
  }

  @Ignore("Nested type requires hidden Lucene doc reading — not yet supported by LuceneArrowReader")
  public void testNestedAggregationByName() throws IOException {
    JSONObject result =
        executePPLQuery(
            "source = "
                + Index.NESTED_SIMPLE.getName()
                + " | stats count(address.area) as count_area,"
                + " avg(age) as avg_age by name");
    JSONArray rows = getDataRows(result);
    assertEquals(5, rows.length());
  }

  @Ignore("Nested type requires hidden Lucene doc reading — not yet supported by LuceneArrowReader")
  public void testNestedAggregationSingleCount() throws IOException {
    JSONObject result =
        executePPLQuery(
            "source = "
                + Index.NESTED_SIMPLE.getName()
                + " | stats count(address.city), count(address.area)");
    JSONArray rows = getDataRows(result);
    assertEquals(1, rows.length());
    JSONArray row = rows.getJSONArray(0);
    assertEquals(11, row.getLong(0));
    assertEquals(9, row.getLong(1));
  }

  @Ignore("Nested type requires hidden Lucene doc reading — not yet supported by LuceneArrowReader")
  public void testNestedAggregationByNestedPath() throws IOException {
    JSONObject result =
        executePPLQuery(
            "source = "
                + Index.NESTED_SIMPLE.getName()
                + " | stats count(), min(age), min(address.area) by address.city");
    JSONArray rows = getDataRows(result);
    assertEquals(11, rows.length());
  }
}
