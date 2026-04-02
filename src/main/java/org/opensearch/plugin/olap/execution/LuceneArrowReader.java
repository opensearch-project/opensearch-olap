/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.execution;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

  /** Read all doc values from a shard and feed them into the ExternalStreamBridge (full scan). */
  public void readShardIntoStream(ShardId shardId, String indexName, ExternalStreamBridge bridge)
      throws IOException {
    readShardIntoStream(shardId, indexName, bridge, null);
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
   */
  public void readShardIntoStream(
      ShardId shardId, String indexName, ExternalStreamBridge bridge, Query pushdownQuery)
      throws IOException {
    IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
    IndexShard shard = indexService.getShard(shardId.id());

    // Acquire a Lucene searcher (NRT reader snapshot)
    try (Engine.Searcher searcher = shard.acquireSearcher("olap-velox")) {
      List<ColumnSpec> columnSpecs = resolveColumnSpecs(indexService, bridge.getRequestedFields());

      ArrowBatchBuilder batchBuilder = new ArrowBatchBuilder(allocator, columnSpecs, BATCH_SIZE);

      if (pushdownQuery != null) {
        readWithPushdown(searcher, batchBuilder, bridge, shardId, pushdownQuery);
      } else {
        readFullScan(searcher, batchBuilder, bridge, shardId);
      }

      logger.debug(
          "Finished reading shard {}: segments={}, pushdown={}",
          shardId,
          searcher.getIndexReader().leaves().size(),
          pushdownQuery != null);
    }
  }

  /** Full scan: iterate every document in every segment. */
  private void readFullScan(
      Engine.Searcher searcher,
      ArrowBatchBuilder batchBuilder,
      ExternalStreamBridge bridge,
      ShardId shardId)
      throws IOException {
    for (LeafReaderContext leafCtx : searcher.getIndexReader().leaves()) {
      int maxDoc = leafCtx.reader().maxDoc();

      for (int startDoc = 0; startDoc < maxDoc; startDoc += BATCH_SIZE) {
        int endDoc = Math.min(startDoc + BATCH_SIZE, maxDoc);

        VectorSchemaRoot batch = batchBuilder.buildBatch(leafCtx.reader(), startDoc, endDoc);
        bridge.feedBatch(batch);

        logger.trace(
            "Fed batch [{}-{}) from shard {} segment {}", startDoc, endDoc, shardId, leafCtx.ord);
      }
    }
  }

  /** Filtered scan: use a Lucene query to read only matching documents per segment. */
  private void readWithPushdown(
      Engine.Searcher searcher,
      ArrowBatchBuilder batchBuilder,
      ExternalStreamBridge bridge,
      ShardId shardId,
      Query pushdownQuery)
      throws IOException {
    // Engine.Searcher extends IndexSearcher — use it directly
    Query rewritten = searcher.rewrite(pushdownQuery);
    Weight weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1.0f);

    long totalMatched = 0;

    for (LeafReaderContext leafCtx : searcher.getIndexReader().leaves()) {
      Scorer scorer = weight.scorer(leafCtx);
      if (scorer == null) {
        // No matches in this segment — skip entirely
        logger.trace("Pushdown: no matches in shard {} segment {}", shardId, leafCtx.ord);
        continue;
      }

      DocIdSetIterator docIdIter = scorer.iterator();
      int[] batch = new int[BATCH_SIZE];
      int count = 0;

      for (int docId = docIdIter.nextDoc();
          docId != DocIdSetIterator.NO_MORE_DOCS;
          docId = docIdIter.nextDoc()) {
        batch[count++] = docId;
        if (count == BATCH_SIZE) {
          VectorSchemaRoot arrowBatch = batchBuilder.buildBatch(leafCtx.reader(), batch, count);
          bridge.feedBatch(arrowBatch);
          totalMatched += count;
          count = 0;
        }
      }

      // Flush remaining docs in this segment
      if (count > 0) {
        VectorSchemaRoot arrowBatch = batchBuilder.buildBatch(leafCtx.reader(), batch, count);
        bridge.feedBatch(arrowBatch);
        totalMatched += count;
      }

      logger.trace("Pushdown: shard {} segment {} matched docs", shardId, leafCtx.ord);
    }

    logger.debug("Pushdown scan completed for shard {}: {} docs matched", shardId, totalMatched);
  }

  /**
   * Resolve column specifications from index mappings. Maps OpenSearch field types to Arrow types
   * and DocValue types.
   */
  private List<ColumnSpec> resolveColumnSpecs(IndexService indexService, List<String> fieldNames) {
    List<ColumnSpec> specs = new ArrayList<>();

    for (String fieldName : fieldNames) {
      MappedFieldType fieldType = indexService.mapperService().fieldType(fieldName);
      if (fieldType == null) {
        logger.warn("Field {} not found in index mappings, skipping", fieldName);
        continue;
      }

      ArrowType arrowType = mapToArrowType(fieldType.typeName());
      DocValueType dvType = mapToDocValueType(fieldType.typeName());

      if (arrowType != null && dvType != null) {
        specs.add(new ColumnSpec(fieldName, arrowType, dvType));
      } else {
        logger.warn(
            "Unsupported field type {} for field {}, skipping", fieldType.typeName(), fieldName);
      }
    }

    return specs;
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
        return new ArrowType.Int(64, true); // millis since epoch
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
