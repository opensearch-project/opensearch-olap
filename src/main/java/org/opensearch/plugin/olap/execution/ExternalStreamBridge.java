/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.execution;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.boostscale.velox4j.arrow.Arrow;
import org.boostscale.velox4j.connector.ExternalStreams;
import org.boostscale.velox4j.connector.ExternalStreams.BlockingQueue;
import org.boostscale.velox4j.data.RowVector;
import org.boostscale.velox4j.session.Session;

/**
 * Bridges Arrow batches from LuceneArrowReader into velox4j's ExternalStream.
 *
 * <p>This is the key integration point where Java-side Lucene data enters the Velox C++ execution
 * engine. The flow:
 *
 * <ol>
 *   <li>LuceneArrowReader produces Arrow VectorSchemaRoot batches
 *   <li>ExternalStreamBridge converts Arrow → Velox RowVector via Arrow C Data Interface
 *   <li>RowVectors are pushed into a velox4j BlockingQueue
 *   <li>Velox's TableScanNode reads from the BlockingQueue via ExternalStreamTableHandle
 * </ol>
 *
 * <p>The BlockingQueue provides back-pressure: if Velox consumes slower than Lucene produces, the
 * put() call will block until space is available.
 */
public class ExternalStreamBridge implements AutoCloseable {

  private static final Logger logger = LogManager.getLogger(ExternalStreamBridge.class);

  private final Session session;
  private final BufferAllocator allocator;
  private final long allocatorLimitBytes;
  private BlockingQueue queue;
  private String connectorId;
  private final AtomicLong rowCount = new AtomicLong(0);
  private final AtomicLong peakBytesInFlight = new AtomicLong(0);
  private List<String> requestedFields = new ArrayList<>();

  /**
   * Create an unbounded bridge. Retained for tests; production code should use the overload that
   * takes an explicit byte limit so the Arrow allocator cannot grow without bound.
   */
  public ExternalStreamBridge(Session session) {
    this(session, Long.MAX_VALUE);
  }

  public ExternalStreamBridge(Session session, long allocatorLimitBytes) {
    this.session = session;
    this.allocatorLimitBytes = allocatorLimitBytes;
    this.allocator = new RootAllocator(allocatorLimitBytes);
  }

  /** Open the bridge, creating the native BlockingQueue. */
  public void open() {
    ExternalStreams streams = session.externalStreamOps();
    this.queue = streams.newBlockingQueue();
    this.connectorId = "connector-external-stream";
    logger.debug("Opened ExternalStreamBridge with queue id={}", queue.id());
  }

  /**
   * Feed an Arrow batch into the Velox ExternalStream.
   *
   * <p>Converts the Arrow VectorSchemaRoot to a Velox RowVector using the Arrow C Data Interface
   * (zero-copy when possible), then pushes it into the native blocking queue.
   */
  public void feedBatch(VectorSchemaRoot arrowBatch) {
    if (queue == null) {
      throw new IllegalStateException("Bridge not opened");
    }

    Arrow arrowOps = session.arrowOps();
    RowVector rowVector;
    try {
      // Convert Arrow VectorSchemaRoot → Velox RowVector via Arrow C Data Interface
      rowVector = arrowOps.fromArrowVectorSchemaRoot(allocator, arrowBatch);
    } catch (OutOfMemoryException oom) {
      // Arrow refused the allocation because the hard cap was hit. Surface as a transient
      // backpressure signal so task retry can pick it up.
      throw new BackpressureTimeoutException(
          "Arrow allocator exceeded per-fragment cap "
              + allocatorLimitBytes
              + " bytes (live="
              + allocator.getAllocatedMemory()
              + ")",
          oom);
    }
    queue.put(rowVector);

    long batchRows = arrowBatch.getRowCount();
    rowCount.addAndGet(batchRows);

    // Record peak Java-side Arrow memory for profile reporting.
    long current = allocator.getAllocatedMemory();
    long prev;
    do {
      prev = peakBytesInFlight.get();
      if (current <= prev) break;
    } while (!peakBytesInFlight.compareAndSet(prev, current));

    // Close the Arrow batch after conversion (data now owned by Velox)
    arrowBatch.close();
  }

  /**
   * Signal that no more data will be fed. This unblocks the Velox TableScanNode which will return
   * end-of-data.
   */
  public void noMoreInput() {
    if (queue != null) {
      queue.noMoreInput();
      logger.debug("ExternalStreamBridge: noMoreInput signaled, total rows={}", rowCount.get());
    }
  }

  /** Abort the stream due to an error. */
  public void abort(Exception cause) {
    logger.error("ExternalStreamBridge aborted", cause);
    noMoreInput();
  }

  @Override
  public void close() {
    // The native BlockingQueue is managed by Velox's memory pool
    // and will be cleaned up when the session is closed.
    queue = null;
  }

  public String getConnectorId() {
    return connectorId;
  }

  public BlockingQueue getQueue() {
    return queue;
  }

  public long getRowCount() {
    return rowCount.get();
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  public long getAllocatorLimitBytes() {
    return allocatorLimitBytes;
  }

  public long getBytesInFlight() {
    return allocator.getAllocatedMemory();
  }

  public long getPeakBytesInFlight() {
    return peakBytesInFlight.get();
  }

  public List<String> getRequestedFields() {
    return requestedFields;
  }

  public void setRequestedFields(List<String> fields) {
    this.requestedFields = fields;
  }
}
