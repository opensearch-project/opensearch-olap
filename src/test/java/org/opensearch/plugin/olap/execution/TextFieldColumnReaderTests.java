/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.execution;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link TextFieldColumnReader}. Builds a tiny in-memory Lucene index with both
 * {@code _source} and {@code store:true} fields and verifies both read paths plus multi-valued
 * semantics.
 */
public class TextFieldColumnReaderTests extends OpenSearchTestCase {

  public void testStoredFieldReadsFirstValue() throws IOException {
    try (Directory dir = new ByteBuffersDirectory()) {
      writeDoc(
          dir, new StoredField("title", "Hoppy IPA"), new StoredField("body", "taste of hops"));

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReader leaf = reader.leaves().get(0).reader();
        TextFieldColumnReader text = new TextFieldColumnReader(Set.of("title", "body"), Set.of());
        text.open(leaf);
        assertEquals("Hoppy IPA", text.readString(0, "title"));
        assertEquals("taste of hops", text.readString(0, "body"));
      }
    }
  }

  public void testStoredFieldMultiValueReturnsFirst() throws IOException {
    try (Directory dir = new ByteBuffersDirectory()) {
      writeDoc(
          dir,
          new StoredField("tags", "brewing"),
          new StoredField("tags", "hops"),
          new StoredField("tags", "yeast"));

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReader leaf = reader.leaves().get(0).reader();
        TextFieldColumnReader text = new TextFieldColumnReader(Set.of("tags"), Set.of());
        text.open(leaf);
        assertEquals("brewing", text.readString(0, "tags"));
      }
    }
  }

  public void testStoredFieldMissingReturnsNull() throws IOException {
    try (Directory dir = new ByteBuffersDirectory()) {
      writeDoc(dir, new StoredField("title", "Hoppy IPA"));

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReader leaf = reader.leaves().get(0).reader();
        TextFieldColumnReader text = new TextFieldColumnReader(Set.of("body"), Set.of());
        text.open(leaf);
        assertNull(text.readString(0, "body"));
      }
    }
  }

  public void testSourceReadsNestedField() throws IOException {
    try (Directory dir = new ByteBuffersDirectory()) {
      // Simulate OpenSearch's _source: a stored binary field named _source containing JSON.
      String json = "{\"title\":\"Pilsner vs Lager\",\"body\":\"crisp taste\"}";
      writeDoc(dir, new StoredField(SourceFieldMapper.NAME, json.getBytes(StandardCharsets.UTF_8)));

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReader leaf = reader.leaves().get(0).reader();
        TextFieldColumnReader text = new TextFieldColumnReader(Set.of(), Set.of("title", "body"));
        text.open(leaf);
        assertEquals("Pilsner vs Lager", text.readString(0, "title"));
        assertEquals("crisp taste", text.readString(0, "body"));
      }
    }
  }

  public void testSourceReadsMultiValueFirst() throws IOException {
    try (Directory dir = new ByteBuffersDirectory()) {
      String json = "{\"tags\":[\"brewing\",\"hops\",\"yeast\"]}";
      writeDoc(dir, new StoredField(SourceFieldMapper.NAME, json.getBytes(StandardCharsets.UTF_8)));

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReader leaf = reader.leaves().get(0).reader();
        TextFieldColumnReader text = new TextFieldColumnReader(Set.of(), Set.of("tags"));
        text.open(leaf);
        assertEquals("brewing", text.readString(0, "tags"));
      }
    }
  }

  public void testSourceMissingFieldReturnsNull() throws IOException {
    try (Directory dir = new ByteBuffersDirectory()) {
      String json = "{\"title\":\"x\"}";
      writeDoc(dir, new StoredField(SourceFieldMapper.NAME, json.getBytes(StandardCharsets.UTF_8)));

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReader leaf = reader.leaves().get(0).reader();
        TextFieldColumnReader text = new TextFieldColumnReader(Set.of(), Set.of("body"));
        text.open(leaf);
        assertNull(text.readString(0, "body"));
      }
    }
  }

  public void testPerDocCacheReusesVisitor() throws IOException {
    // Two columns from the same doc — the reader must open the doc only once internally (we
    // can't observe the cache directly, but we can verify both reads succeed and return their
    // distinct values, which requires the shared visitor to have captured both fields).
    try (Directory dir = new ByteBuffersDirectory()) {
      String json = "{\"title\":\"a\",\"body\":\"b\"}";
      writeDoc(dir, new StoredField(SourceFieldMapper.NAME, json.getBytes(StandardCharsets.UTF_8)));

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReader leaf = reader.leaves().get(0).reader();
        TextFieldColumnReader text = new TextFieldColumnReader(Set.of(), Set.of("title", "body"));
        text.open(leaf);
        assertEquals("a", text.readString(0, "title"));
        assertEquals("b", text.readString(0, "body"));
        // A second read of title should still produce the same value (cached).
        assertEquals("a", text.readString(0, "title"));
      }
    }
  }

  public void testMixedStoredAndSource() throws IOException {
    try (Directory dir = new ByteBuffersDirectory()) {
      // Put `title` as a stored field AND embed body in _source. The reader should route each
      // by name.
      String json = "{\"body\":\"hops and yeast\"}";
      writeDoc(
          dir,
          new StoredField("title", "Hoppy IPA"),
          new StoredField(SourceFieldMapper.NAME, json.getBytes(StandardCharsets.UTF_8)));

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReader leaf = reader.leaves().get(0).reader();
        TextFieldColumnReader text = new TextFieldColumnReader(Set.of("title"), Set.of("body"));
        text.open(leaf);
        assertEquals("Hoppy IPA", text.readString(0, "title"));
        assertEquals("hops and yeast", text.readString(0, "body"));
      }
    }
  }

  // ---- Non-JSON _source encodings (P2 #3 regression) ----
  //
  // OpenSearch preserves the original XContent format of a document's _source — if ingestion was
  // CBOR or SMILE, the stored bytes are CBOR or SMILE. The reader must auto-detect the media
  // type rather than hard-coding JSON; these tests pin that behavior.

  public void testSourceCbor() throws IOException {
    try (Directory dir = new ByteBuffersDirectory()) {
      byte[] cborBytes = encodeSource(XContentFactory.cborBuilder(), "Pilsner vs Lager", "yeast");
      writeDoc(dir, new StoredField(SourceFieldMapper.NAME, cborBytes));

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReader leaf = reader.leaves().get(0).reader();
        TextFieldColumnReader text = new TextFieldColumnReader(Set.of(), Set.of("title", "body"));
        text.open(leaf);
        assertEquals("Pilsner vs Lager", text.readString(0, "title"));
        assertEquals("yeast", text.readString(0, "body"));
      }
    }
  }

  public void testSourceSmile() throws IOException {
    try (Directory dir = new ByteBuffersDirectory()) {
      byte[] smileBytes = encodeSource(XContentFactory.smileBuilder(), "Stout", "coffee notes");
      writeDoc(dir, new StoredField(SourceFieldMapper.NAME, smileBytes));

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReader leaf = reader.leaves().get(0).reader();
        TextFieldColumnReader text = new TextFieldColumnReader(Set.of(), Set.of("title", "body"));
        text.open(leaf);
        assertEquals("Stout", text.readString(0, "title"));
        assertEquals("coffee notes", text.readString(0, "body"));
      }
    }
  }

  // ---- helpers ----

  private void writeDoc(Directory dir, Field... fields) throws IOException {
    IndexWriterConfig cfg = new IndexWriterConfig(new StandardAnalyzer());
    try (IndexWriter w = new IndexWriter(dir, cfg)) {
      Document doc = new Document();
      // Anchor the doc with an indexed field so Lucene always has at least one indexed doc.
      // Avoid the reserved `_id` name which FieldsVisitor routes through a special asserter.
      doc.add(new StringField("doc_uid", "1", Field.Store.NO));
      for (Field f : fields) {
        doc.add(f);
      }
      w.addDocument(doc);
      w.commit();
    }
  }

  private static byte[] encodeSource(XContentBuilder builder, String title, String body)
      throws IOException {
    try (XContentBuilder b = builder) {
      b.startObject().field("title", title).field("body", body).endObject();
      return BytesReference.toBytes(BytesReference.bytes(b));
    }
  }
}
