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

  /** Read all doc values from a shard and feed them into the ExternalStreamBridge. */
  public void readShardIntoStream(ShardId shardId, String indexName, ExternalStreamBridge bridge)
      throws IOException {
    IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
    IndexShard shard = indexService.getShard(shardId.id());

    // Acquire a Lucene searcher (NRT reader snapshot)
    try (Engine.Searcher searcher = shard.acquireSearcher("olap-velox")) {
      List<ColumnSpec> columnSpecs = resolveColumnSpecs(indexService, bridge.getRequestedFields());

      ArrowBatchBuilder batchBuilder = new ArrowBatchBuilder(allocator, columnSpecs, BATCH_SIZE);

      // Iterate over all leaf readers (segments)
      for (LeafReaderContext leafCtx : searcher.getIndexReader().leaves()) {
        int maxDoc = leafCtx.reader().maxDoc();

        // Process documents in batches
        for (int startDoc = 0; startDoc < maxDoc; startDoc += BATCH_SIZE) {
          int endDoc = Math.min(startDoc + BATCH_SIZE, maxDoc);

          VectorSchemaRoot batch = batchBuilder.buildBatch(leafCtx.reader(), startDoc, endDoc);

          bridge.feedBatch(batch);

          logger.trace(
              "Fed batch [{}-{}) from shard {} segment {}", startDoc, endDoc, shardId, leafCtx.ord);
        }
      }

      logger.debug(
          "Finished reading shard {}: segments={}",
          shardId,
          searcher.getIndexReader().leaves().size());
    }
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
