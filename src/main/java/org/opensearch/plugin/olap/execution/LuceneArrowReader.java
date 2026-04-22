/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.execution;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.opensearch.common.util.concurrent.FutureUtils;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugin.olap.execution.ArrowBatchBuilder.ColumnSpec;
import org.opensearch.plugin.olap.execution.DocValueColumnReader.DocValueType;

/**
 * Reads Lucene doc values from OpenSearch shards and produces Arrow batches.
 *
 * <p>This is the critical bridge between OpenSearch's row-oriented Lucene storage and the columnar
 * Arrow format needed by Velox. For each shard:
 *
 * <ol>
 *   <li>Acquire a Lucene searcher via IndexShard.acquireSearcher()
 *   <li>Iterate over leaf readers (segments)
 *   <li>Read doc values column-by-column into Arrow vectors
 *   <li>Feed the Arrow batches into ExternalStreamBridge
 * </ol>
 *
 * <p>Uses doc values (not stored fields or source) because:
 *
 * <ul>
 *   <li>Doc values are columnar on disk, matching Arrow's columnar layout
 *   <li>Doc values support sequential iteration (no random seeks)
 *   <li>Doc values are always forward-only per segment, ideal for scan operations
 * </ul>
 */
public class LuceneArrowReader {

  private static final Logger logger = LogManager.getLogger(LuceneArrowReader.class);
  private static final int BATCH_SIZE = 4096;

  private final IndicesService indicesService;
  private final BufferAllocator allocator;

  public LuceneArrowReader(IndicesService indicesService, BufferAllocator allocator) {
    this.indicesService = indicesService;
    this.allocator = allocator;
  }

  /**
   * Thread-safe accumulator for scan statistics surfaced up through feeder threads back to the
   * dispatcher. {@code docsRead} counts every live doc the reader visited (for a full scan this is
   * the shard's maxDoc minus deletions; for a pushdown scan it is the number of docs the Lucene
   * scorer's iterator advanced over) — i.e., the universe the runtime filter acted on. {@code
   * docsMatched} counts docs actually emitted into the Arrow bridge.
   *
   * <p>The delta between the two is the observable effect of BLOOM/TERMS pushdown: if BLOOM is
   * narrowing the scan at the Lucene level, {@code docsMatched} will be meaningfully below {@code
   * docsRead} for queries where the join selectivity is low. This is the signal the "profile=true"
   * flow reports back to the user.
   */
  public static final class ScanStats {
    private final LongAdder docsRead = new LongAdder();
    private final LongAdder docsMatched = new LongAdder();

    public void addDocsRead(long n) {
      docsRead.add(n);
    }

    public void addDocsMatched(long n) {
      docsMatched.add(n);
    }

    public long getDocsRead() {
      return docsRead.sum();
    }

    public long getDocsMatched() {
      return docsMatched.sum();
    }
  }

  /** Read all doc values from a shard and feed them into the ExternalStreamBridge (full scan). */
  public void readShardIntoStream(ShardId shardId, String indexName, ExternalStreamBridge bridge)
      throws IOException {
    readShardIntoStream(shardId, indexName, bridge, null, null);
  }

  /** Back-compat overload that discards scan stats. */
  public void readShardIntoStream(
      ShardId shardId, String indexName, ExternalStreamBridge bridge, Query pushdownQuery)
      throws IOException {
    readShardIntoStream(shardId, indexName, bridge, pushdownQuery, null);
  }

  /**
   * Read doc values from a shard and feed them into the ExternalStreamBridge.
   *
   * <p>When a Lucene {@code pushdownQuery} is provided, only matching documents are read — the
   * query is evaluated per-segment via {@link Weight}/{@link Scorer} to obtain a {@link
   * DocIdSetIterator} of matching doc IDs. This drastically reduces I/O for selective filters.
   *
   * <p>When {@code pushdownQuery} is null, falls back to a full scan of all documents.
   *
   * @param pushdownQuery optional Lucene query for predicate pushdown; null means full scan
   * @param stats optional scan counters; null when profiling is disabled
   */
  public void readShardIntoStream(
      ShardId shardId,
      String indexName,
      ExternalStreamBridge bridge,
      Query pushdownQuery,
      ScanStats stats)
      throws IOException {
    IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
    IndexShard shard = indexService.getShard(shardId.id());

    // Acquire a Lucene searcher (NRT reader snapshot)
    try (Engine.Searcher searcher = shard.acquireSearcher("olap-velox")) {
      List<ColumnSpec> columnSpecs = resolveColumnSpecs(indexService, bridge.getRequestedFields());

      ArrowBatchBuilder batchBuilder = new ArrowBatchBuilder(allocator, columnSpecs, BATCH_SIZE);

      if (pushdownQuery != null) {
        readWithPushdown(searcher, batchBuilder, bridge, shardId, pushdownQuery, stats);
      } else {
        readFullScan(searcher, batchBuilder, bridge, shardId, stats);
      }

      logger.debug(
          "Finished reading shard {}: segments={}, pushdown={}",
          shardId,
          searcher.getIndexReader().leaves().size(),
          pushdownQuery != null);
    }
  }

  /**
   * Read doc values from a shard using parallel segment reads.
   *
   * <p>Each Lucene segment is read by a separate task submitted to the executor. All tasks feed
   * batches into the shared bridge (thread-safe BlockingQueue). Falls back to sequential when there
   * is only one segment.
   *
   * @param segmentExecutor thread pool for parallel segment reads
   */
  public void readShardIntoStreamParallel(
      ShardId shardId,
      String indexName,
      ExternalStreamBridge bridge,
      Query pushdownQuery,
      ExecutorService segmentExecutor)
      throws IOException {
    readShardIntoStreamParallel(shardId, indexName, bridge, pushdownQuery, segmentExecutor, null);
  }

  /** Parallel variant with optional scan-stats accumulator. */
  public void readShardIntoStreamParallel(
      ShardId shardId,
      String indexName,
      ExternalStreamBridge bridge,
      Query pushdownQuery,
      ExecutorService segmentExecutor,
      ScanStats stats)
      throws IOException {
    IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
    IndexShard shard = indexService.getShard(shardId.id());

    try (Engine.Searcher searcher = shard.acquireSearcher("olap-velox")) {
      List<ColumnSpec> columnSpecs = resolveColumnSpecs(indexService, bridge.getRequestedFields());
      List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();

      if (leaves.size() <= 1) {
        // Single segment — no benefit from parallelism, use sequential path
        ArrowBatchBuilder batchBuilder = new ArrowBatchBuilder(allocator, columnSpecs, BATCH_SIZE);
        if (pushdownQuery != null) {
          readWithPushdown(searcher, batchBuilder, bridge, shardId, pushdownQuery, stats);
        } else {
          readFullScan(searcher, batchBuilder, bridge, shardId, stats);
        }
        return;
      }

      // Prepare pushdown weight once (Weight is thread-safe)
      Weight weight = null;
      if (pushdownQuery != null) {
        Query rewritten = searcher.rewrite(pushdownQuery);
        weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
      }

      long rowCountBefore = bridge.getRowCount();

      // Submit one task per segment
      List<Future<?>> futures = new ArrayList<>(leaves.size());
      final Weight finalWeight = weight;
      final ScanStats finalStats = stats;
      for (LeafReaderContext leafCtx : leaves) {
        futures.add(
            segmentExecutor.submit(
                () -> {
                  try {
                    // Each task creates its own ArrowBatchBuilder (thread-local DocValues)
                    ArrowBatchBuilder taskBuilder =
                        new ArrowBatchBuilder(allocator, columnSpecs, BATCH_SIZE);
                    if (finalWeight != null) {
                      readSegmentWithPushdown(
                          taskBuilder, bridge, shardId, leafCtx, finalWeight, finalStats);
                    } else {
                      readSegmentFullScan(taskBuilder, bridge, shardId, leafCtx, finalStats);
                    }
                  } catch (IOException e) {
                    throw new RuntimeException(
                        "Segment read failed: shard=" + shardId + " segment=" + leafCtx.ord, e);
                  }
                  return null;
                }));
      }

      // Await all segment tasks
      try {
        for (Future<?> f : futures) {
          f.get(120, TimeUnit.SECONDS);
        }
      } catch (Exception e) {
        // Cancel remaining tasks on failure
        for (Future<?> f : futures) {
          FutureUtils.cancel(f);
        }
        throw new IOException("Parallel segment read failed for shard " + shardId, e);
      }

      if (pushdownQuery != null) {
        long docsMatched = bridge.getRowCount() - rowCountBefore;
        logger.debug(
            "Pushdown scan completed for shard {}: {} docs matched ({} segments, parallel)",
            shardId,
            docsMatched,
            leaves.size());
      } else {
        logger.debug("Finished parallel reading shard {}: {} segments", shardId, leaves.size());
      }
    }
  }

  // ---- Sequential scan methods ----

  /** Full scan: iterate every document in every segment sequentially. */
  private void readFullScan(
      Engine.Searcher searcher,
      ArrowBatchBuilder batchBuilder,
      ExternalStreamBridge bridge,
      ShardId shardId,
      ScanStats stats)
      throws IOException {
    for (LeafReaderContext leafCtx : searcher.getIndexReader().leaves()) {
      readSegmentFullScan(batchBuilder, bridge, shardId, leafCtx, stats);
    }
  }

  /** Filtered scan: use a Lucene query to read only matching documents sequentially. */
  private void readWithPushdown(
      Engine.Searcher searcher,
      ArrowBatchBuilder batchBuilder,
      ExternalStreamBridge bridge,
      ShardId shardId,
      Query pushdownQuery,
      ScanStats stats)
      throws IOException {
    Query rewritten = searcher.rewrite(pushdownQuery);
    Weight weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1.0f);

    long rowCountBefore = bridge.getRowCount();
    for (LeafReaderContext leafCtx : searcher.getIndexReader().leaves()) {
      readSegmentWithPushdown(batchBuilder, bridge, shardId, leafCtx, weight, stats);
    }
    long docsMatched = bridge.getRowCount() - rowCountBefore;
    logger.debug("Pushdown scan completed for shard {}: {} docs matched", shardId, docsMatched);
  }

  // ---- Per-segment read methods (used by both sequential and parallel paths) ----

  /** Read a single segment (full scan) and feed batches to the bridge. */
  private void readSegmentFullScan(
      ArrowBatchBuilder batchBuilder,
      ExternalStreamBridge bridge,
      ShardId shardId,
      LeafReaderContext leafCtx,
      ScanStats stats)
      throws IOException {
    int maxDoc = leafCtx.reader().maxDoc();
    if (stats != null) {
      // For a full scan docsRead == docsMatched == maxDoc; record once per segment so the
      // accumulator reflects the real universe the RF acted on (zero pushdown selectivity).
      stats.addDocsRead(maxDoc);
      stats.addDocsMatched(maxDoc);
    }
    for (int startDoc = 0; startDoc < maxDoc; startDoc += BATCH_SIZE) {
      int endDoc = Math.min(startDoc + BATCH_SIZE, maxDoc);
      VectorSchemaRoot batch = batchBuilder.buildBatch(leafCtx.reader(), startDoc, endDoc);
      bridge.feedBatch(batch);
      logger.trace(
          "Fed batch [{}-{}) from shard {} segment {}", startDoc, endDoc, shardId, leafCtx.ord);
    }
  }

  /** Read a single segment (pushdown) and feed matching batches to the bridge. */
  private void readSegmentWithPushdown(
      ArrowBatchBuilder batchBuilder,
      ExternalStreamBridge bridge,
      ShardId shardId,
      LeafReaderContext leafCtx,
      Weight weight,
      ScanStats stats)
      throws IOException {
    // docsRead tracks the universe — the number of live docs in this segment that the pushdown
    // query had to examine, regardless of whether the scorer admitted them. Comparing this to
    // docsMatched below is the only honest way to see whether a BLOOM RF actually narrowed the
    // Lucene scan.
    if (stats != null) {
      stats.addDocsRead(leafCtx.reader().maxDoc());
    }
    Scorer scorer = weight.scorer(leafCtx);
    if (scorer == null) {
      logger.trace("Pushdown: no matches in shard {} segment {}", shardId, leafCtx.ord);
      return;
    }

    DocIdSetIterator docIdIter = scorer.iterator();
    int[] batch = new int[BATCH_SIZE];
    int count = 0;
    long emitted = 0;

    for (int docId = docIdIter.nextDoc();
        docId != DocIdSetIterator.NO_MORE_DOCS;
        docId = docIdIter.nextDoc()) {
      batch[count++] = docId;
      emitted++;
      if (count == BATCH_SIZE) {
        VectorSchemaRoot arrowBatch = batchBuilder.buildBatch(leafCtx.reader(), batch, count);
        bridge.feedBatch(arrowBatch);
        count = 0;
      }
    }

    if (count > 0) {
      VectorSchemaRoot arrowBatch = batchBuilder.buildBatch(leafCtx.reader(), batch, count);
      bridge.feedBatch(arrowBatch);
    }

    if (stats != null) {
      stats.addDocsMatched(emitted);
    }
  }

  /**
   * Resolve column specifications from index mappings. Maps OpenSearch field types to Arrow types
   * and DocValue types. Groups dot-path fields into nested StructColumnSpecs to match the Velox ROW
   * type structure (e.g., cloud.region + cloud.provider → cloud: Struct(region, provider)).
   */
  private List<ColumnSpec> resolveColumnSpecs(IndexService indexService, List<String> fieldNames) {
    // First, resolve all flat field specs
    List<ColumnSpec> flatSpecs = new ArrayList<>();
    for (String fieldName : fieldNames) {
      MappedFieldType fieldType = indexService.mapperService().fieldType(fieldName);
      if (fieldType == null) {
        logger.warn("Field {} not found in index mappings, skipping", fieldName);
        continue;
      }
      ArrowType arrowType = mapToArrowType(fieldType.typeName());
      DocValueType dvType = mapToDocValueType(fieldType.typeName());
      if (arrowType != null && dvType != null) {
        flatSpecs.add(new ColumnSpec(fieldName, arrowType, dvType));
      } else {
        logger.warn(
            "Unsupported field type {} for field {}, skipping", fieldType.typeName(), fieldName);
      }
    }

    // Return flat specs directly — no struct grouping needed.
    // The Velox scan output is flat (dot-path fields as top-level columns).
    // Struct reconstruction happens in Java result conversion.
    return flatSpecs;
  }

  /**
   * Group flat dot-path column specs into nested struct specs. Top-level fields (no dots) remain
   * flat. Dot-path fields are grouped by their first path component into StructColumnSpecs.
   */
  private List<ColumnSpec> buildNestedColumnSpecs(List<ColumnSpec> flatSpecs) {
    // Separate top-level and dot-path fields
    java.util.LinkedHashMap<String, List<ColumnSpec>> structGroups =
        new java.util.LinkedHashMap<>();
    List<ColumnSpec> result = new ArrayList<>();

    for (ColumnSpec spec : flatSpecs) {
      String name = spec.getName();
      int dotIndex = name.indexOf('.');
      if (dotIndex > 0) {
        String parent = name.substring(0, dotIndex);
        String childPath = name.substring(dotIndex + 1);
        structGroups
            .computeIfAbsent(parent, k -> new ArrayList<>())
            .add(new ColumnSpec(childPath, spec.getArrowType(), spec.getDocValueType()));
      } else {
        // Flush any pending struct group before this top-level field
        // (preserve original field order)
        result.add(spec);
      }
    }

    // Build struct specs from groups
    // We need to insert struct specs at the right position. Simplify: collect top-level names
    // first, then insert struct groups where their first child appeared.
    List<ColumnSpec> finalResult = new ArrayList<>();
    java.util.Set<String> addedStructs = new java.util.HashSet<>();

    for (ColumnSpec spec : flatSpecs) {
      String name = spec.getName();
      int dotIndex = name.indexOf('.');
      if (dotIndex > 0) {
        String parent = name.substring(0, dotIndex);
        if (!addedStructs.contains(parent)) {
          addedStructs.add(parent);
          List<ColumnSpec> children = structGroups.get(parent);
          // Recursively nest multi-level paths
          List<ColumnSpec> nestedChildren = buildNestedColumnSpecs(children);
          // Child names in the struct use their doc-value full path for reading
          // but the struct field name is the local name (without parent prefix)
          List<ColumnSpec> docValueChildren = new ArrayList<>();
          for (ColumnSpec child : nestedChildren) {
            if (child.isStruct()) {
              // Nested struct: prepend parent path to all leaf descendants' doc-value names.
              // e.g., for parent="log", child struct "file" with leaf "path",
              // the leaf's doc-value name must be "log.file.path" not just "file.path".
              docValueChildren.add(prependPathToStruct(parent, child));
            } else {
              // Leaf: use full doc-value path (parent.child) for reading
              docValueChildren.add(
                  new ColumnSpec(
                      parent + "." + child.getName(),
                      child.getArrowType(),
                      child.getDocValueType()));
            }
          }
          // Create struct spec with short child names (for Arrow schema)
          // but doc-value paths preserved in child specs (for reading)
          finalResult.add(new ColumnSpec(parent, docValueChildren));
        }
      } else {
        finalResult.add(spec);
      }
    }
    return finalResult;
  }

  /**
   * Prepend a parent path prefix to all leaf doc-value names in a nested struct ColumnSpec.
   * Recursively handles multi-level nesting. For example:
   *
   * <pre>
   *   prependPathToStruct("log", Struct("file", [Leaf("path")]))
   *   → Struct("file", [Leaf("log.file.path")])
   * </pre>
   */
  private ColumnSpec prependPathToStruct(String parentPath, ColumnSpec structSpec) {
    String fullPrefix = parentPath + "." + structSpec.getName();
    List<ColumnSpec> fixedChildren = new ArrayList<>();
    for (ColumnSpec child : structSpec.getChildren()) {
      if (child.isStruct()) {
        fixedChildren.add(prependPathToStruct(fullPrefix, child));
      } else {
        // Child name from recursive call is just the local name (e.g., "path").
        // Prepend the full path for doc-value reading (e.g., "log.file.path").
        String leafName = child.getName();
        // If the child already has a dot-path from the recursive call, extract just the leaf
        int lastDot = leafName.lastIndexOf('.');
        if (lastDot >= 0) {
          leafName = leafName.substring(lastDot + 1);
        }
        fixedChildren.add(
            new ColumnSpec(
                fullPrefix + "." + leafName, child.getArrowType(), child.getDocValueType()));
      }
    }
    return new ColumnSpec(structSpec.getName(), fixedChildren);
  }

  /** Map OpenSearch field type name to Arrow type. */
  private ArrowType mapToArrowType(String typeName) {
    switch (typeName) {
      case "boolean":
        return new ArrowType.Bool();
      case "byte":
        return new ArrowType.Int(8, true);
      case "short":
        return new ArrowType.Int(16, true);
      case "integer":
        return new ArrowType.Int(32, true);
      case "long":
        return new ArrowType.Int(64, true);
      case "half_float":
      case "float":
        return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
      case "double":
        return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
      case "keyword":
      case "text":
        return new ArrowType.Utf8();
      case "date":
        // Emit Arrow Timestamp(MICROSECOND, UTC) so the Arrow→Velox bridge produces a
        // Velox TimestampVector. velox4j's Arrow bridge is configured for microsecond unit
        // (see Arrow.cc makeOptions). Doc values store millis; ArrowBatchBuilder converts.
        return new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, null);
      default:
        return null;
    }
  }

  /** Map OpenSearch field type name to DocValue type. */
  private DocValueType mapToDocValueType(String typeName) {
    switch (typeName) {
      case "boolean":
      case "byte":
      case "short":
      case "integer":
      case "long":
      case "half_float":
      case "float":
      case "double":
      case "date":
        // OpenSearch stores all numeric types as SORTED_NUMERIC (supports multi-valued)
        return DocValueType.SORTED_NUMERIC;
      case "keyword":
      case "text":
        // OpenSearch stores keyword/text as SORTED_SET (supports multi-valued)
        return DocValueType.SORTED_SET;
      default:
        return null;
    }
  }
}
