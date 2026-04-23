/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.transport;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Manages shuffle data buffers for hash shuffle join.
 *
 * <p>Each worker node has a ShuffleManager singleton that accumulates incoming shuffle partitions.
 * Data is keyed by (queryId, stageId) and separated into left and right join sides.
 *
 * <p>The buffer tracks sender completion via done signals. When all expected senders for both sides
 * have finished, the buffer is ready for the join task to consume.
 */
public class ShuffleManager {

  private static final Logger logger = LogManager.getLogger(ShuffleManager.class);

  private final ConcurrentHashMap<String, ShuffleBuffer> buffers = new ConcurrentHashMap<>();
  private volatile long bufferMaxBytes = Long.MAX_VALUE;

  public ShuffleManager() {}

  /**
   * Configure the per-partition byte cap for newly created buffers. Dynamic updates of {@code
   * plugins.velox.shuffle_buffer_bytes} route through here.
   */
  public void setBufferMaxBytes(long bufferMaxBytes) {
    this.bufferMaxBytes = bufferMaxBytes;
  }

  public long getBufferMaxBytes() {
    return bufferMaxBytes;
  }

  /** Get or create a shuffle buffer for the given query and target stage. */
  public ShuffleBuffer getOrCreateBuffer(String queryId, int targetStageId) {
    String key = queryId + ":" + targetStageId;
    return buffers.computeIfAbsent(key, k -> new ShuffleBuffer(bufferMaxBytes));
  }

  /** Get an existing buffer, or null if it doesn't exist. */
  public ShuffleBuffer getBuffer(String queryId, int targetStageId) {
    return buffers.get(queryId + ":" + targetStageId);
  }

  /** Remove and clean up a buffer after the join task completes. */
  public void removeBuffer(String queryId, int targetStageId) {
    buffers.remove(queryId + ":" + targetStageId);
  }

  /**
   * Buffer for shuffle data from both sides of a join.
   *
   * <p>Thread-safe: multiple transport threads may concurrently add data. Completion tracking uses
   * CountDownLatch initialized lazily when expected sender counts are set.
   */
  public static class ShuffleBuffer {
    private final List<byte[]> leftData = Collections.synchronizedList(new ArrayList<>());
    private final List<byte[]> rightData = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger leftDoneCount = new AtomicInteger(0);
    private final AtomicInteger rightDoneCount = new AtomicInteger(0);
    private volatile int expectedLeftSenders = -1;
    private volatile int expectedRightSenders = -1;
    private final CountDownLatch leftReady = new CountDownLatch(1);
    private final CountDownLatch rightReady = new CountDownLatch(1);
    private final AtomicLong currentBytes = new AtomicLong(0);
    private final AtomicLong rejectedCount = new AtomicLong(0);
    private final long maxBytes;

    public ShuffleBuffer() {
      this(Long.MAX_VALUE);
    }

    public ShuffleBuffer(long maxBytes) {
      this.maxBytes = maxBytes;
    }

    public void setExpectedSenders(int leftSenders, int rightSenders) {
      this.expectedLeftSenders = leftSenders;
      this.expectedRightSenders = rightSenders;
      // Check if already complete (edge case: senders finished before expected counts set)
      checkCompletion("left");
      checkCompletion("right");
    }

    public void addData(String side, byte[] data) {
      if ("left".equals(side)) {
        leftData.add(data);
      } else {
        rightData.add(data);
      }
      if (data != null) {
        currentBytes.addAndGet(data.length);
      }
    }

    /**
     * Accept data if the current buffer has room, else reject. The sender is expected to retry on
     * rejection with exponential backoff.
     *
     * @return true if accepted, false if rejected because the per-partition cap was exceeded
     */
    public boolean tryAddData(String side, byte[] data) {
      int size = data == null ? 0 : data.length;
      long newTotal = currentBytes.addAndGet(size);
      if (newTotal > maxBytes) {
        currentBytes.addAndGet(-size);
        rejectedCount.incrementAndGet();
        return false;
      }
      if ("left".equals(side)) {
        leftData.add(data);
      } else {
        rightData.add(data);
      }
      return true;
    }

    public long getCurrentBytes() {
      return currentBytes.get();
    }

    public long getMaxBytes() {
      return maxBytes;
    }

    public long getRejectedCount() {
      return rejectedCount.get();
    }

    /**
     * Release all accounted bytes. Called by the consumer when it has drained the partition data
     * and the producers can safely flood new data for a follow-up stage on the same buffer
     * (typically unused today, but cheap to support).
     */
    public void releaseData() {
      currentBytes.set(0);
    }

    public void senderDone(String side) {
      if ("left".equals(side)) {
        leftDoneCount.incrementAndGet();
        checkCompletion("left");
      } else {
        rightDoneCount.incrementAndGet();
        checkCompletion("right");
      }
    }

    private void checkCompletion(String side) {
      if ("left".equals(side)) {
        if (expectedLeftSenders >= 0 && leftDoneCount.get() >= expectedLeftSenders) {
          leftReady.countDown();
        }
      } else {
        if (expectedRightSenders >= 0 && rightDoneCount.get() >= expectedRightSenders) {
          rightReady.countDown();
        }
      }
    }

    /**
     * Wait until all shuffle data has been received for both sides.
     *
     * @return true if completed within the timeout
     */
    public boolean awaitReady(long timeoutMs) throws InterruptedException {
      long deadline = System.currentTimeMillis() + timeoutMs;
      long remaining = timeoutMs;

      if (!leftReady.await(remaining, TimeUnit.MILLISECONDS)) {
        logger.warn(
            "Shuffle left side timed out: received {}/{} senders",
            leftDoneCount.get(),
            expectedLeftSenders);
        return false;
      }

      remaining = deadline - System.currentTimeMillis();
      if (remaining <= 0) return false;

      if (!rightReady.await(remaining, TimeUnit.MILLISECONDS)) {
        logger.warn(
            "Shuffle right side timed out: received {}/{} senders",
            rightDoneCount.get(),
            expectedRightSenders);
        return false;
      }

      return true;
    }

    public List<byte[]> getLeftData() {
      return leftData;
    }

    public List<byte[]> getRightData() {
      return rightData;
    }
  }
}
