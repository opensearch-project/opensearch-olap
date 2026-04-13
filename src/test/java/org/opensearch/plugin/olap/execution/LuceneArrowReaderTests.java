/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.execution;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.plugin.olap.execution.ArrowBatchBuilder.ColumnSpec;
import org.opensearch.plugin.olap.execution.DocValueColumnReader.DocValueType;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for segment-level parallel reading. Uses an in-memory Lucene index with multiple
 * segments to verify that:
 *
 * <p>Note: Tests that use thread pools shut them down before returning to avoid thread leak
 * detection by the OpenSearch test framework.
 *
 * <ul>
 *   <li>ArrowBatchBuilder correctly reads from individual segments
 *   <li>Multiple segments can be read concurrently into a shared collection
 *   <li>Parallel reads produce the same aggregate data as sequential reads
 * </ul>
 */
public class LuceneArrowReaderTests extends OpenSearchTestCase {

  private static final int BATCH_SIZE = 4096;

  /**
   * Create a multi-segment Lucene index. Each call to writer.commit() between groups of docs forces
   * a new segment.
   */
  private DirectoryReader createMultiSegmentIndex(
      Directory dir, int docsPerSegment, int numSegments) throws IOException {
    IndexWriterConfig config = new IndexWriterConfig();
    config.setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE);
    IndexWriter writer = new IndexWriter(dir, config);

    int id = 0;
    for (int seg = 0; seg < numSegments; seg++) {
      for (int doc = 0; doc < docsPerSegment; doc++) {
        Document d = new Document();
        d.add(new NumericDocValuesField("id", id));
        d.add(new SortedDocValuesField("name", new BytesRef("name_" + id)));
        writer.addDocument(d);
        id++;
      }
      writer.commit();
    }
    writer.close();
    return DirectoryReader.open(dir);
  }

  private List<ColumnSpec> testColumnSpecs() {
    List<ColumnSpec> specs = new ArrayList<>();
    specs.add(new ColumnSpec("id", new ArrowType.Int(32, true), DocValueType.NUMERIC));
    specs.add(new ColumnSpec("name", new ArrowType.Utf8(), DocValueType.SORTED));
    return specs;
  }

  /** Collect all integer values from a column across batches. */
  private Set<Integer> collectIds(List<VectorSchemaRoot> batches) {
    Set<Integer> ids = new HashSet<>();
    for (VectorSchemaRoot batch : batches) {
      IntVector idVec = (IntVector) batch.getVector("id");
      for (int i = 0; i < batch.getRowCount(); i++) {
        ids.add(idVec.get(i));
      }
    }
    return ids;
  }

  /** Read all batches from a single segment. */
  private List<VectorSchemaRoot> readSegment(
      BufferAllocator allocator, List<ColumnSpec> specs, LeafReaderContext leafCtx)
      throws IOException {
    List<VectorSchemaRoot> batches = new ArrayList<>();
    ArrowBatchBuilder builder = new ArrowBatchBuilder(allocator, specs, BATCH_SIZE);
    int maxDoc = leafCtx.reader().maxDoc();
    for (int startDoc = 0; startDoc < maxDoc; startDoc += BATCH_SIZE) {
      int endDoc = Math.min(startDoc + BATCH_SIZE, maxDoc);
      batches.add(builder.buildBatch(leafCtx.reader(), startDoc, endDoc));
    }
    return batches;
  }

  // ---- Tests ----

  public void testMultiSegmentIndexHasExpectedSegments() throws IOException {
    Directory dir = new ByteBuffersDirectory();
    DirectoryReader reader = createMultiSegmentIndex(dir, 10, 3);
    assertEquals("Expected 3 segments", 3, reader.leaves().size());
    assertEquals("Expected 30 total docs", 30, reader.numDocs());
    reader.close();
    dir.close();
  }

  public void testSingleSegmentReadProducesAllDocs() throws IOException {
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    Directory dir = new ByteBuffersDirectory();
    DirectoryReader reader = createMultiSegmentIndex(dir, 10, 1);

    List<VectorSchemaRoot> batches =
        readSegment(allocator, testColumnSpecs(), reader.leaves().get(0));
    Set<Integer> ids = collectIds(batches);

    assertEquals("Single segment should have 10 docs", 10, ids.size());
    for (int i = 0; i < 10; i++) {
      assertTrue("Should contain id " + i, ids.contains(i));
    }

    batches.forEach(VectorSchemaRoot::close);
    reader.close();
    dir.close();
    allocator.close();
  }

  public void testSequentialMultiSegmentReadProducesAllDocs() throws IOException {
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    Directory dir = new ByteBuffersDirectory();
    int docsPerSegment = 100;
    int numSegments = 5;
    DirectoryReader reader = createMultiSegmentIndex(dir, docsPerSegment, numSegments);

    List<VectorSchemaRoot> allBatches = new ArrayList<>();
    for (LeafReaderContext leafCtx : reader.leaves()) {
      allBatches.addAll(readSegment(allocator, testColumnSpecs(), leafCtx));
    }

    Set<Integer> ids = collectIds(allBatches);
    assertEquals("All segments should produce 500 unique docs", 500, ids.size());

    allBatches.forEach(VectorSchemaRoot::close);
    reader.close();
    dir.close();
    allocator.close();
  }

  public void testParallelMultiSegmentReadProducesAllDocs() throws Exception {
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    Directory dir = new ByteBuffersDirectory();
    int docsPerSegment = 100;
    int numSegments = 5;
    DirectoryReader reader = createMultiSegmentIndex(dir, docsPerSegment, numSegments);

    // Read segments in parallel — same pattern as readShardIntoStreamParallel
    ExecutorService executor = Executors.newFixedThreadPool(4);
    List<ColumnSpec> specs = testColumnSpecs();
    List<Future<List<VectorSchemaRoot>>> futures = new ArrayList<>();

    for (LeafReaderContext leafCtx : reader.leaves()) {
      futures.add(
          executor.submit(
              () -> {
                // Each task creates its own ArrowBatchBuilder (thread-safe pattern)
                return readSegment(allocator, specs, leafCtx);
              }));
    }

    List<VectorSchemaRoot> allBatches = new ArrayList<>();
    for (Future<List<VectorSchemaRoot>> f : futures) {
      allBatches.addAll(f.get(30, TimeUnit.SECONDS));
    }

    Set<Integer> ids = collectIds(allBatches);
    assertEquals("Parallel read should produce all 500 unique docs", 500, ids.size());

    allBatches.forEach(VectorSchemaRoot::close);
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);
    reader.close();
    dir.close();
    allocator.close();
  }

  public void testParallelAndSequentialProduceSameResults() throws Exception {
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    Directory dir = new ByteBuffersDirectory();
    int docsPerSegment = 50;
    int numSegments = 4;
    DirectoryReader reader = createMultiSegmentIndex(dir, docsPerSegment, numSegments);
    List<ColumnSpec> specs = testColumnSpecs();

    // Sequential
    List<VectorSchemaRoot> seqBatches = new ArrayList<>();
    for (LeafReaderContext leafCtx : reader.leaves()) {
      seqBatches.addAll(readSegment(allocator, specs, leafCtx));
    }
    Set<Integer> seqIds = collectIds(seqBatches);

    // Parallel
    ExecutorService executor = Executors.newFixedThreadPool(4);
    List<Future<List<VectorSchemaRoot>>> futures = new ArrayList<>();
    for (LeafReaderContext leafCtx : reader.leaves()) {
      futures.add(executor.submit(() -> readSegment(allocator, specs, leafCtx)));
    }
    List<VectorSchemaRoot> parBatches = new ArrayList<>();
    for (Future<List<VectorSchemaRoot>> f : futures) {
      parBatches.addAll(f.get(30, TimeUnit.SECONDS));
    }
    Set<Integer> parIds = collectIds(parBatches);

    assertEquals(
        "Sequential and parallel should produce same doc count", seqIds.size(), parIds.size());
    assertEquals("Sequential and parallel should produce same doc IDs", seqIds, parIds);

    seqBatches.forEach(VectorSchemaRoot::close);
    parBatches.forEach(VectorSchemaRoot::close);
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);
    reader.close();
    dir.close();
    allocator.close();
  }

  public void testParallelReadWithLargeSegments() throws Exception {
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    Directory dir = new ByteBuffersDirectory();
    // 3 segments × 2000 docs each = 6000 docs, spans multiple batches (BATCH_SIZE=4096)
    int docsPerSegment = 2000;
    int numSegments = 3;
    DirectoryReader reader = createMultiSegmentIndex(dir, docsPerSegment, numSegments);

    ExecutorService executor = Executors.newFixedThreadPool(3);
    List<ColumnSpec> specs = testColumnSpecs();
    List<Future<List<VectorSchemaRoot>>> futures = new ArrayList<>();

    for (LeafReaderContext leafCtx : reader.leaves()) {
      futures.add(executor.submit(() -> readSegment(allocator, specs, leafCtx)));
    }

    List<VectorSchemaRoot> allBatches = new ArrayList<>();
    int totalRows = 0;
    for (Future<List<VectorSchemaRoot>> f : futures) {
      List<VectorSchemaRoot> batches = f.get(30, TimeUnit.SECONDS);
      for (VectorSchemaRoot batch : batches) {
        totalRows += batch.getRowCount();
      }
      allBatches.addAll(batches);
    }

    assertEquals("Should read all 6000 rows", 6000, totalRows);
    Set<Integer> ids = collectIds(allBatches);
    assertEquals("Should have 6000 unique IDs", 6000, ids.size());

    allBatches.forEach(VectorSchemaRoot::close);
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);
    reader.close();
    dir.close();
    allocator.close();
  }

  public void testConcurrentBatchBuildersAreThreadSafe() throws Exception {
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    Directory dir = new ByteBuffersDirectory();
    DirectoryReader reader = createMultiSegmentIndex(dir, 200, 8);
    List<ColumnSpec> specs = testColumnSpecs();

    // 8 segments read concurrently, each with its own ArrowBatchBuilder
    ExecutorService executor = Executors.newFixedThreadPool(8);
    AtomicInteger totalRows = new AtomicInteger(0);
    List<Future<?>> futures = new ArrayList<>();

    for (LeafReaderContext leafCtx : reader.leaves()) {
      futures.add(
          executor.submit(
              () -> {
                try {
                  ArrowBatchBuilder builder = new ArrowBatchBuilder(allocator, specs, BATCH_SIZE);
                  int maxDoc = leafCtx.reader().maxDoc();
                  for (int start = 0; start < maxDoc; start += BATCH_SIZE) {
                    int end = Math.min(start + BATCH_SIZE, maxDoc);
                    VectorSchemaRoot batch = builder.buildBatch(leafCtx.reader(), start, end);
                    totalRows.addAndGet(batch.getRowCount());
                    batch.close();
                  }
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              }));
    }

    for (Future<?> f : futures) {
      f.get(30, TimeUnit.SECONDS);
    }

    assertEquals("All 1600 rows should be read across 8 concurrent tasks", 1600, totalRows.get());

    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);
    reader.close();
    dir.close();
    allocator.close();
  }
}
