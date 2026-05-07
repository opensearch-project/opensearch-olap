/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap;

import java.io.IOException;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.client.Request;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * End-to-end test that {@link org.opensearch.plugin.olap.execution.TextFieldColumnReader} extracts
 * text-field values from a document whose {@code _source} was indexed as CBOR or SMILE, not JSON.
 *
 * <p>OpenSearch preserves the original XContent format of a document's {@code _source}, so an index
 * that ingests CBOR stores CBOR bytes. Earlier revisions of the text reader hard-coded {@code
 * MediaTypeRegistry.JSON} when calling {@code XContentHelper.convertToMap}, which silently failed
 * on non-JSON sources. These tests index a single doc using the requested XContent type and project
 * the text field through a PPL relevance query, exercising the full read path (Lucene filter →
 * Velox scan → {@code _source} → Arrow → response).
 */
public class TextFieldSourceFormatIT extends OlapRestTestCase {

  private static final String INDEX = "beer_text_source_format";

  @Override
  public void setUp() throws Exception {
    super.setUp();
    // Only the mapping is shared with BEER_TEXT; documents are indexed one-by-one below with a
    // chosen Content-Type so _source is stored in that format.
    String mapping = readResource("beer_text_mapping.json");
    Request create = new Request("PUT", "/" + INDEX);
    JSONObject m = new JSONObject(mapping);
    JSONObject settings = new JSONObject();
    settings.put("number_of_shards", 1);
    settings.put("number_of_replicas", 0);
    m.put("settings", settings);
    create.setJsonEntity(m.toString());
    client().performRequest(create);
  }

  @Override
  public void tearDown() throws Exception {
    deleteIndex(INDEX);
    super.tearDown();
  }

  public void testMatchReadsBodyFromCborSource() throws IOException {
    indexDoc("1", cborDoc(1, "Hoppy IPA", "brewing hops and yeast taste"));
    indexDoc("2", cborDoc(2, "Pilsner", "crisp water"));
    refresh();

    JSONObject response =
        executePPLQuery("source=" + INDEX + " | where match(Body, 'hops') | fields Id, Body");
    JSONArray rows = getDataRows(response);
    assertEquals(1, rows.length());
    JSONArray row = rows.getJSONArray(0);
    assertEquals(1, row.getInt(0));
    assertTrue("Body contains 'hops'", row.getString(1).contains("hops"));
  }

  public void testMatchReadsBodyFromSmileSource() throws IOException {
    indexDoc("1", smileDoc(1, "Stout", "roasted malt and coffee notes"));
    indexDoc("2", smileDoc(2, "Wheat", "banana esters"));
    refresh();

    JSONObject response =
        executePPLQuery("source=" + INDEX + " | where match(Body, 'coffee') | fields Id, Body");
    JSONArray rows = getDataRows(response);
    assertEquals(1, rows.length());
    JSONArray row = rows.getJSONArray(0);
    assertEquals(1, row.getInt(0));
    assertTrue("Body contains 'coffee'", row.getString(1).contains("coffee"));
  }

  // ---- helpers ----

  private void indexDoc(String id, EncodedDoc doc) throws IOException {
    Request req = new Request("PUT", "/" + INDEX + "/_doc/" + id);
    req.setEntity(new ByteArrayEntity(doc.bytes, ContentType.create(doc.contentType)));
    client().performRequest(req);
  }

  private void refresh() throws IOException {
    client().performRequest(new Request("POST", "/" + INDEX + "/_refresh"));
  }

  private static EncodedDoc cborDoc(int id, String title, String body) throws IOException {
    return encode(XContentFactory.cborBuilder(), "application/cbor", id, title, body);
  }

  private static EncodedDoc smileDoc(int id, String title, String body) throws IOException {
    return encode(XContentFactory.smileBuilder(), "application/smile", id, title, body);
  }

  private static EncodedDoc encode(
      XContentBuilder builder, String contentType, int id, String title, String body)
      throws IOException {
    try (XContentBuilder b = builder) {
      b.startObject()
          .field("Id", id)
          .field("Title", title)
          .field("Body", body)
          .field("Tags", "brewing")
          .field("CreationDate", "2024-01-01 00:00:00")
          .field("AcceptedAnswerId", 1)
          .endObject();
      return new EncodedDoc(BytesReference.toBytes(BytesReference.bytes(b)), contentType);
    }
  }

  private static final class EncodedDoc {
    final byte[] bytes;
    final String contentType;

    EncodedDoc(byte[] bytes, String contentType) {
      this.bytes = bytes;
      this.contentType = contentType;
    }
  }
}
