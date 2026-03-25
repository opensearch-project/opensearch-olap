/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.plugin.olap.execution;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.boostscale.velox4j.arrow.Arrow;
import org.boostscale.velox4j.data.RowVector;
import org.boostscale.velox4j.iterator.CloseableIterator;
import org.boostscale.velox4j.iterator.UpIterators;
import org.boostscale.velox4j.query.Queries;
import org.boostscale.velox4j.query.Query;
import org.boostscale.velox4j.query.SerialTask;
import org.boostscale.velox4j.serde.Serde;
import org.boostscale.velox4j.session.Session;

/**
 * Executes a Velox plan fragment via velox4j and returns results as Arrow IPC bytes.
 *
 * <p>The executor:
 * <ol>
 *   <li>Deserializes the plan JSON into a velox4j Query</li>
 *   <li>Executes the query via velox4j's QueryExecutor (single-threaded serial execution)</li>
 *   <li>Iterates over the SerialTask to collect result RowVectors</li>
 *   <li>Converts results to Arrow IPC format for transport back to coordinator</li>
 * </ol>
 *
 * <p>The plan's TableScanNode reads from an ExternalStream (BlockingQueue) that is
 * being fed by the LuceneArrowReader on a separate thread.
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
     * @param planJson    Serialized velox4j Query JSON
     * @param connectorId The ExternalStream connector ID used in the plan
     * @return Arrow IPC serialized result batches
     */
    public byte[] execute(String planJson, String connectorId) {
        Queries queries = session.queryOps();

        // Deserialize the plan from JSON
        Query query = Serde.fromJson(planJson, Query.class);

        // Execute the query - this creates a Velox task that reads from ExternalStream
        SerialTask serialTask = queries.execute(query);

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
                    writer = new ArrowStreamWriter(
                        arrowRoot,
                        null,
                        Channels.newChannel(baos)
                    );
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
}
