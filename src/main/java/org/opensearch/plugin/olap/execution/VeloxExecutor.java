/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.execution;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.boostscale.velox4j.arrow.Arrow;
import org.boostscale.velox4j.config.Config;
import org.boostscale.velox4j.config.ConnectorConfig;
import org.boostscale.velox4j.connector.ExternalStreamConnectorSplit;
import org.boostscale.velox4j.connector.ExternalStreams.BlockingQueue;
import org.boostscale.velox4j.data.BaseVectors;
import org.boostscale.velox4j.data.RowVector;
import org.boostscale.velox4j.iterator.CloseableIterator;
import org.boostscale.velox4j.iterator.UpIterators;
import org.boostscale.velox4j.plan.PlanNode;
import org.boostscale.velox4j.plan.TableScanNode;
import org.boostscale.velox4j.query.Queries;
import org.boostscale.velox4j.query.Query;
import org.boostscale.velox4j.query.SerialTask;
import org.boostscale.velox4j.serde.Serde;
import org.boostscale.velox4j.session.Session;

/**
 * Executes a Velox plan fragment via velox4j and returns results as Arrow IPC bytes.
 *
 * <p>Supports three execution modes:
 *
 * <ul>
 *   <li><b>Single-input</b>: Standard scan execution with one ExternalStream
 *   <li><b>Dual-input</b>: Join execution with two ExternalStreams (left + right)
 *   <li><b>Iterator</b>: Returns raw RowVector iterator for shuffle partitioning
 * </ul>
 */
public class VeloxExecutor {

  private static final Logger logger = LogManager.getLogger(VeloxExecutor.class);

  private final Session session;

  public VeloxExecutor(Session session) {
    this.session = session;
  }

  /**
   * Execute a Velox plan and return results as Arrow IPC bytes.
   *
   * @param planJson Serialized velox4j PlanNode JSON (wrapped in a Query)
   * @param connectorId The ExternalStream connector ID used in the plan
   * @param queue The local BlockingQueue feeding data into the ExternalStream
   * @return Arrow IPC serialized result batches
   */
  public byte[] execute(String planJson, String connectorId, BlockingQueue queue) {
    Queries queries = session.queryOps();

    Query originalQuery = Serde.fromJson(planJson, Query.class);
    ConnectorConfig connectorConfig = ConnectorConfig.create(Map.of(connectorId, Config.empty()));
    Query query =
        new Query(originalQuery.getPlan(), originalQuery.getQueryConfig(), connectorConfig);

    logger.info("Executing Velox query: {}", Serde.toJson(query));

    SerialTask serialTask = queries.execute(query);

    String scanNodeId = findTableScanNodeId(query.getPlan());
    if (scanNodeId == null) {
      throw new IllegalStateException("No TableScanNode found in plan");
    }

    ExternalStreamConnectorSplit split = new ExternalStreamConnectorSplit(connectorId, queue.id());
    serialTask.addSplit(scanNodeId, split);
    serialTask.noMoreSplits(scanNodeId);

    return collectArrowIpc(serialTask);
  }

  /**
   * Execute a Velox plan and return results as Velox native serialized bytes. Preserves Velox's
   * internal intermediate format (e.g., accumulator state for PARTIAL aggregation).
   *
   * @return List of Velox native serialized byte arrays, one per result batch
   */
  public List<byte[]> executeNative(String planJson, String connectorId, BlockingQueue queue) {
    Queries queries = session.queryOps();

    Query originalQuery = Serde.fromJson(planJson, Query.class);
    ConnectorConfig connectorConfig = ConnectorConfig.create(Map.of(connectorId, Config.empty()));
    Query query =
        new Query(originalQuery.getPlan(), originalQuery.getQueryConfig(), connectorConfig);

    SerialTask serialTask = queries.execute(query);

    String scanNodeId = findTableScanNodeId(query.getPlan());
    if (scanNodeId == null) {
      throw new IllegalStateException("No TableScanNode found in plan");
    }

    ExternalStreamConnectorSplit split = new ExternalStreamConnectorSplit(connectorId, queue.id());
    serialTask.addSplit(scanNodeId, split);
    serialTask.noMoreSplits(scanNodeId);

    CloseableIterator<RowVector> resultIterator = UpIterators.asJavaIterator(serialTask);
    List<byte[]> results = new ArrayList<>();

    try {
      while (resultIterator.hasNext()) {
        RowVector resultBatch = resultIterator.next();
        if (resultBatch == null) break;
        results.add(BaseVectors.serializeOneToBuf(resultBatch));
      }
      resultIterator.close();
      logger.debug("Velox native execution complete, {} batches", results.size());
      return results;
    } catch (Exception e) {
      throw new RuntimeException("Failed to execute Velox plan (native serde)", e);
    }
  }

  /**
   * Execute a Velox plan with two ExternalStream inputs (for join operations). Returns results as
   * Arrow IPC bytes.
   *
   * @param planJson Serialized plan JSON containing a HashJoinNode with two TableScanNode sources
   * @param connectorId The ExternalStream connector ID
   * @param leftQueue BlockingQueue for the left (probe) side data
   * @param leftScanNodeId Plan node ID of the left TableScanNode
   * @param rightQueue BlockingQueue for the right (build) side data
   * @param rightScanNodeId Plan node ID of the right TableScanNode
   * @return Arrow IPC serialized result batches
   */
  public byte[] executeDualInput(
      String planJson,
      String connectorId,
      BlockingQueue leftQueue,
      String leftScanNodeId,
      BlockingQueue rightQueue,
      String rightScanNodeId) {
    Queries queries = session.queryOps();

    Query originalQuery = Serde.fromJson(planJson, Query.class);
    ConnectorConfig connectorConfig = ConnectorConfig.create(Map.of(connectorId, Config.empty()));
    Query query =
        new Query(originalQuery.getPlan(), originalQuery.getQueryConfig(), connectorConfig);

    logger.info("Executing dual-input Velox join query");

    SerialTask serialTask = queries.execute(query);

    // Add splits for both input sides
    serialTask.addSplit(
        leftScanNodeId, new ExternalStreamConnectorSplit(connectorId, leftQueue.id()));
    serialTask.addSplit(
        rightScanNodeId, new ExternalStreamConnectorSplit(connectorId, rightQueue.id()));
    serialTask.noMoreSplits(leftScanNodeId);
    serialTask.noMoreSplits(rightScanNodeId);

    return collectArrowIpc(serialTask);
  }

  /**
   * Execute a Velox plan with two ExternalStream inputs and return Velox native serialized results.
   * Used for broadcast/shuffle join with PARTIAL aggregation above the join.
   */
  public List<byte[]> executeDualInputNative(
      String planJson,
      String connectorId,
      BlockingQueue leftQueue,
      String leftScanNodeId,
      BlockingQueue rightQueue,
      String rightScanNodeId) {
    Queries queries = session.queryOps();

    Query originalQuery = Serde.fromJson(planJson, Query.class);
    ConnectorConfig connectorConfig = ConnectorConfig.create(Map.of(connectorId, Config.empty()));
    Query query =
        new Query(originalQuery.getPlan(), originalQuery.getQueryConfig(), connectorConfig);

    SerialTask serialTask = queries.execute(query);

    serialTask.addSplit(
        leftScanNodeId, new ExternalStreamConnectorSplit(connectorId, leftQueue.id()));
    serialTask.addSplit(
        rightScanNodeId, new ExternalStreamConnectorSplit(connectorId, rightQueue.id()));
    serialTask.noMoreSplits(leftScanNodeId);
    serialTask.noMoreSplits(rightScanNodeId);

    CloseableIterator<RowVector> resultIterator = UpIterators.asJavaIterator(serialTask);
    List<byte[]> results = new ArrayList<>();

    try {
      while (resultIterator.hasNext()) {
        RowVector resultBatch = resultIterator.next();
        if (resultBatch == null) break;
        results.add(BaseVectors.serializeOneToBuf(resultBatch));
      }
      resultIterator.close();
      return results;
    } catch (Exception e) {
      throw new RuntimeException("Failed to execute dual-input Velox plan (native serde)", e);
    }
  }

  /**
   * Execute a Velox plan and return a raw RowVector iterator. Used for shuffle scans where the
   * caller needs to hash-partition each batch before serializing.
   */
  public CloseableIterator<RowVector> executeToIterator(
      String planJson, String connectorId, BlockingQueue queue) {
    Queries queries = session.queryOps();

    Query originalQuery = Serde.fromJson(planJson, Query.class);
    ConnectorConfig connectorConfig = ConnectorConfig.create(Map.of(connectorId, Config.empty()));
    Query query =
        new Query(originalQuery.getPlan(), originalQuery.getQueryConfig(), connectorConfig);

    SerialTask serialTask = queries.execute(query);

    String scanNodeId = findTableScanNodeId(query.getPlan());
    if (scanNodeId == null) {
      throw new IllegalStateException("No TableScanNode found in plan");
    }

    ExternalStreamConnectorSplit split = new ExternalStreamConnectorSplit(connectorId, queue.id());
    serialTask.addSplit(scanNodeId, split);
    serialTask.noMoreSplits(scanNodeId);

    return UpIterators.asJavaIterator(serialTask);
  }

  // ---- Internal Helpers ----

  /** Collect all result batches from a serial task as Arrow IPC bytes. */
  private byte[] collectArrowIpc(SerialTask serialTask) {
    CloseableIterator<RowVector> resultIterator = UpIterators.asJavaIterator(serialTask);
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      ArrowStreamWriter writer = null;
      boolean hasData = false;

      while (resultIterator.hasNext()) {
        RowVector resultBatch = resultIterator.next();
        if (resultBatch == null) break;

        VectorSchemaRoot arrowRoot = Arrow.toArrowVectorSchemaRoot(allocator, resultBatch);
        if (writer == null) {
          writer = new ArrowStreamWriter(arrowRoot, null, Channels.newChannel(baos));
          writer.start();
        }
        writer.writeBatch();
        hasData = true;
        arrowRoot.close();
      }

      if (writer != null) {
        writer.end();
        writer.close();
      }
      resultIterator.close();

      logger.debug("Velox execution complete, result size={} bytes", baos.size());
      return hasData ? baos.toByteArray() : new byte[0];

    } catch (Exception e) {
      throw new RuntimeException("Failed to execute Velox plan or serialize results", e);
    } finally {
      allocator.close();
    }
  }

  /** Recursively find the first TableScanNode ID in the plan tree. */
  private String findTableScanNodeId(PlanNode node) {
    if (node instanceof TableScanNode) {
      return node.getId();
    }
    for (PlanNode source : node.getSources()) {
      String id = findTableScanNodeId(source);
      if (id != null) {
        return id;
      }
    }
    return null;
  }

  /**
   * Find all TableScanNode IDs in the plan tree, ordered left-to-right (depth-first). For join
   * plans, returns [leftScanId, rightScanId].
   */
  public List<String> findAllTableScanNodeIds(PlanNode node) {
    List<String> ids = new ArrayList<>();
    collectTableScanNodeIds(node, ids);
    return ids;
  }

  private void collectTableScanNodeIds(PlanNode node, List<String> ids) {
    if (node instanceof TableScanNode) {
      ids.add(node.getId());
      return;
    }
    for (PlanNode source : node.getSources()) {
      collectTableScanNodeIds(source, ids);
    }
  }
}
