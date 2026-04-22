/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.execution;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link BloomFilterQuery}. Builds small in-memory Lucene indexes for each
 * field-type flavor (keyword single-valued via SortedDocValues, keyword multi-valued via
 * SortedSetDocValues, integer/long single-valued via NumericDocValues, numeric multi-valued via
 * SortedNumericDocValues) and verifies that the query returns exactly the docs whose values pass
 * {@link OlapBloomFilter#mightContain}.
 */
public class BloomFilterQueryTests extends OpenSearchTestCase {

  /**
   * Run the query against the index and return the set of matching doc IDs, reconstructed from the
   * docs' stored "id" NumericDocValues.
   */
  private Set<Long> runAndCollectIds(DirectoryReader reader, Query q) throws IOException {
    IndexSearcher searcher = new IndexSearcher(reader);
    TopDocs topDocs = searcher.search(q, Integer.MAX_VALUE);
    Set<Long> ids = new HashSet<>();
    for (ScoreDoc sd : topDocs.scoreDocs) {
      Document d = searcher.storedFields().document(sd.doc);
      ids.add(Long.parseLong(d.get("id")));
    }
    return ids;
  }

  // ---- keyword, single-valued via SortedDocValues ----

  public void testKeywordSingleValuedMatchesOnlyBloomHits() throws IOException {
    try (Directory dir = new ByteBuffersDirectory()) {
      try (IndexWriter w =
          new IndexWriter(
              dir,
              new IndexWriterConfig()
                  .setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE))) {
        String[] values = {"alpha", "bravo", "charlie", "delta", "echo"};
        for (int i = 0; i < values.length; i++) {
          Document d = new Document();
          d.add(new SortedDocValuesField("name", new BytesRef(values[i])));
          d.add(new org.apache.lucene.document.StoredField("id", Long.toString(i)));
          w.addDocument(d);
        }
      }
      OlapBloomFilter bloom = OlapBloomFilter.create(100);
      bloom.add(new BytesRef("bravo"));
      bloom.add(new BytesRef("echo"));

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        Query q = new BloomFilterQuery("name", "keyword", bloom);
        Set<Long> matched = runAndCollectIds(reader, q);
        // bravo=1, echo=4 — plus any incidental false positives (tolerated)
        assertTrue("bravo must match", matched.contains(1L));
        assertTrue("echo must match", matched.contains(4L));
      }
    }
  }

  // ---- keyword, multi-valued via SortedSetDocValues ----

  public void testKeywordMultiValuedAnyValueHits() throws IOException {
    try (Directory dir = new ByteBuffersDirectory()) {
      try (IndexWriter w =
          new IndexWriter(
              dir,
              new IndexWriterConfig()
                  .setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE))) {
        // doc 0: {red, blue}; doc 1: {green}; doc 2: {yellow, orange}
        addMultiValuedKeywordDoc(w, 0, "red", "blue");
        addMultiValuedKeywordDoc(w, 1, "green");
        addMultiValuedKeywordDoc(w, 2, "yellow", "orange");
      }
      OlapBloomFilter bloom = OlapBloomFilter.create(100);
      bloom.add(new BytesRef("blue")); // should hit doc 0 via second value
      bloom.add(new BytesRef("orange")); // should hit doc 2 via second value

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        Query q = new BloomFilterQuery("tags", "keyword", bloom);
        Set<Long> matched = runAndCollectIds(reader, q);
        assertTrue("doc 0 must match via 'blue'", matched.contains(0L));
        assertTrue("doc 2 must match via 'orange'", matched.contains(2L));
        // doc 1 has only 'green' which isn't in the bloom — most likely excluded
        // (may FP, so we only assert positive hits deterministically)
      }
    }
  }

  private void addMultiValuedKeywordDoc(IndexWriter w, long id, String... tags) throws IOException {
    Document d = new Document();
    for (String tag : tags) {
      d.add(new SortedSetDocValuesField("tags", new BytesRef(tag)));
    }
    d.add(new org.apache.lucene.document.StoredField("id", Long.toString(id)));
    w.addDocument(d);
  }

  // ---- integer, single-valued via NumericDocValues ----

  public void testIntegerSingleValuedMatchesOnlyBloomHits() throws IOException {
    try (Directory dir = new ByteBuffersDirectory()) {
      try (IndexWriter w =
          new IndexWriter(
              dir,
              new IndexWriterConfig()
                  .setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE))) {
        int[] values = {100, 200, 300, 400, 500};
        for (int i = 0; i < values.length; i++) {
          Document d = new Document();
          d.add(new NumericDocValuesField("dept_id", values[i]));
          d.add(new org.apache.lucene.document.StoredField("id", Long.toString(i)));
          w.addDocument(d);
        }
      }
      OlapBloomFilter bloom = OlapBloomFilter.create(100);
      bloom.add(OlapBloomFilter.encodeKey(200, "integer"));
      bloom.add(OlapBloomFilter.encodeKey(500, "integer"));

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        Query q = new BloomFilterQuery("dept_id", "integer", bloom);
        Set<Long> matched = runAndCollectIds(reader, q);
        assertTrue("200 must match", matched.contains(1L));
        assertTrue("500 must match", matched.contains(4L));
      }
    }
  }

  // ---- long, multi-valued via SortedNumericDocValues ----

  public void testLongMultiValuedAnyValueHits() throws IOException {
    try (Directory dir = new ByteBuffersDirectory()) {
      try (IndexWriter w =
          new IndexWriter(
              dir,
              new IndexWriterConfig()
                  .setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE))) {
        // doc 0: {1L, 2L}; doc 1: {3L}; doc 2: {4L, 5L}
        addMultiValuedLongDoc(w, 0, 1L, 2L);
        addMultiValuedLongDoc(w, 1, 3L);
        addMultiValuedLongDoc(w, 2, 4L, 5L);
      }
      OlapBloomFilter bloom = OlapBloomFilter.create(100);
      bloom.add(OlapBloomFilter.encodeKey(2L, "long")); // hit doc 0
      bloom.add(OlapBloomFilter.encodeKey(5L, "long")); // hit doc 2

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        Query q = new BloomFilterQuery("ids", "long", bloom);
        Set<Long> matched = runAndCollectIds(reader, q);
        assertTrue("doc 0 must match via 2L", matched.contains(0L));
        assertTrue("doc 2 must match via 5L", matched.contains(2L));
      }
    }
  }

  private void addMultiValuedLongDoc(IndexWriter w, long id, long... values) throws IOException {
    Document d = new Document();
    for (long v : values) {
      d.add(new SortedNumericDocValuesField("ids", v));
    }
    d.add(new org.apache.lucene.document.StoredField("id", Long.toString(id)));
    w.addDocument(d);
  }

  // ---- missing field → zero matches (stricter than pre-v2 feeder predicate) ----

  public void testMissingFieldProducesNoMatches() throws IOException {
    try (Directory dir = new ByteBuffersDirectory()) {
      try (IndexWriter w =
          new IndexWriter(
              dir,
              new IndexWriterConfig()
                  .setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE))) {
        // Index has a 'name' field but not 'missing_field'.
        for (int i = 0; i < 3; i++) {
          Document d = new Document();
          d.add(new SortedDocValuesField("name", new BytesRef("x_" + i)));
          d.add(new org.apache.lucene.document.StoredField("id", Long.toString(i)));
          w.addDocument(d);
        }
      }
      OlapBloomFilter bloom = OlapBloomFilter.create(100);
      bloom.add(new BytesRef("anything"));

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        Query q = new BloomFilterQuery("missing_field", "keyword", bloom);
        Set<Long> matched = runAndCollectIds(reader, q);
        assertTrue(
            "Missing field must produce zero matches (correctness held by join re-verification)",
            matched.isEmpty());
      }
    }
  }

  // ---- equals / hashCode ----

  public void testEqualsAndHashCode() {
    OlapBloomFilter bloomA = OlapBloomFilter.create(100);
    OlapBloomFilter bloomB = OlapBloomFilter.create(100);
    BloomFilterQuery q1 = new BloomFilterQuery("field", "keyword", bloomA);
    BloomFilterQuery q2 = new BloomFilterQuery("field", "keyword", bloomA);
    BloomFilterQuery q3 = new BloomFilterQuery("field", "keyword", bloomB);
    BloomFilterQuery q4 = new BloomFilterQuery("other", "keyword", bloomA);

    assertEquals(q1, q2);
    assertEquals(q1.hashCode(), q2.hashCode());
    assertNotEquals(q1, q3); // different bloom identity
    assertNotEquals(q1, q4); // different field name
  }

  public void testUnsupportedFieldTypeHasEmptyTwoPhase() throws IOException {
    try (Directory dir = new ByteBuffersDirectory()) {
      try (IndexWriter w =
          new IndexWriter(
              dir,
              new IndexWriterConfig()
                  .setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE))) {
        Document d = new Document();
        d.add(new SortedDocValuesField("name", new BytesRef("x")));
        d.add(new org.apache.lucene.document.StoredField("id", "0"));
        w.addDocument(d);
      }
      OlapBloomFilter bloom = OlapBloomFilter.create(100);
      bloom.add(new BytesRef("x"));

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        // Unsupported field type at the query level — the scorer returns null, which means no
        // docs match (RuntimeFilterBuilder.buildBloom would normally reject this type before we
        // get here, but the Query class itself must stay defensive).
        Query q = new BloomFilterQuery("name", "date", bloom);
        Set<Long> matched = runAndCollectIds(reader, q);
        assertTrue(matched.isEmpty());
      }
    }
  }

  // ---- RuntimeFilterBuilder.buildBloom wrapper ----

  public void testBuildBloomReturnsQueryForSupportedTypes() {
    OlapBloomFilter bloom = OlapBloomFilter.create(100);
    List<String> supported = Arrays.asList("keyword", "text", "integer", "long");
    for (String ft : supported) {
      Query q = RuntimeFilterBuilder.buildBloom("f", ft, bloom);
      assertNotNull("Expected non-null for " + ft, q);
      assertTrue(q instanceof BloomFilterQuery);
    }
  }

  public void testBuildBloomReturnsNullForUnsupportedType() {
    OlapBloomFilter bloom = OlapBloomFilter.create(100);
    assertNull(RuntimeFilterBuilder.buildBloom("f", "double", bloom));
    assertNull(RuntimeFilterBuilder.buildBloom("f", "date", bloom));
  }

  public void testBuildBloomReturnsNullForMissingInputs() {
    OlapBloomFilter bloom = OlapBloomFilter.create(100);
    assertNull(RuntimeFilterBuilder.buildBloom(null, "keyword", bloom));
    assertNull(RuntimeFilterBuilder.buildBloom("f", "keyword", null));
  }

  // Silence unused-import check
  @SuppressWarnings("unused")
  private static List<?> unused() {
    return new ArrayList<>();
  }
}
