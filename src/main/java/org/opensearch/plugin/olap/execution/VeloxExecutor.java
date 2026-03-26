/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.execution;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
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
 * <p>The executor:
 *
 * <ol>
 *   <li>Deserializes the plan JSON into a velox4j PlanNode
 *   <li>Creates a Query with ConnectorConfig registering the ExternalStream connector
 *   <li>Executes the query and adds a split referencing the local BlockingQueue
 *   <li>Iterates over the SerialTask to collect result RowVectors
 *   <li>Converts results to Arrow IPC format for transport back to coordinator
 * </ol>
 *
 * <p>The plan's TableScanNode reads from an ExternalStream (BlockingQueue) that is being fed by the
 * LuceneArrowReader on a separate thread.
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

    // Deserialize the plan from JSON
    Query originalQuery = Serde.fromJson(planJson, Query.class);

    // Rebuild the query with ConnectorConfig registering the ExternalStream connector
    ConnectorConfig connectorConfig = ConnectorConfig.create(Map.of(connectorId, Config.empty()));
    Query query =
        new Query(originalQuery.getPlan(), originalQuery.getQueryConfig(), connectorConfig);

    // Log the query for debugging
    logger.info("Executing Velox query: {}", Serde.toJson(query));

    // Create a serial task (does not start execution until splits are added)
    SerialTask serialTask = queries.execute(query);

    // Find the TableScanNode ID in the plan tree
    String scanNodeId = findTableScanNodeId(query.getPlan());
    if (scanNodeId == null) {
      throw new IllegalStateException("No TableScanNode found in plan");
    }

    // Add a split referencing the local BlockingQueue, keyed by the scan node ID
    ExternalStreamConnectorSplit split = new ExternalStreamConnectorSplit(connectorId, queue.id());
    serialTask.addSplit(scanNodeId, split);
    serialTask.noMoreSplits(scanNodeId);

    // Wrap in Java iterator for hasNext()/next() pattern
    CloseableIterator<RowVector> resultIterator = UpIterators.asJavaIterator(serialTask);

    // Collect results
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      ArrowStreamWriter writer = null;
      boolean hasData = false;

      // Iterate over result batches from the Velox task
      while (resultIterator.hasNext()) {
        RowVector resultBatch = resultIterator.next();
        if (resultBatch == null) {
          break;
        }

        // Convert Velox RowVector → Arrow VectorSchemaRoot (static method)
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

  /** Recursively find the TableScanNode ID in the plan tree. */
  private String findTableScanNodeId(PlanNode node) {
    if (node instanceof TableScanNode) {
      return node.getId();
    }
    try {
      java.lang.reflect.Method m = PlanNode.class.getDeclaredMethod("getSources");
      m.setAccessible(true);
      @SuppressWarnings("unchecked")
      java.util.List<PlanNode> sources = (java.util.List<PlanNode>) m.invoke(node);
      if (sources != null) {
        for (PlanNode source : sources) {
          String id = findTableScanNodeId(source);
          if (id != null) {
            return id;
          }
        }
      }
    } catch (Exception e) {
      logger.warn(
          "Cannot traverse plan node {}: {}", node.getClass().getSimpleName(), e.getMessage());
    }
    return null;
  }
}
