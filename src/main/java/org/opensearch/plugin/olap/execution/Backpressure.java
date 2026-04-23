/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.execution;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Backpressure gate for upstream producers.
 *
 * <p>velox4j's {@code BlockingQueue} only blocks <em>after</em> a RowVector has been allocated and
 * handed off. That makes it a poor fit for bounding Java-side Arrow memory, since the producer has
 * already paid the allocation cost by the time it blocks. This utility provides an <em>active</em>
 * gate: callers ask whether the downstream allocator has room <em>before</em> building the next
 * Arrow batch.
 *
 * <p>The gauge is Arrow's own {@link BufferAllocator#getAllocatedMemory()}. Velox takes ownership
 * of Arrow buffers via the C Data Interface, so each {@code arrowBatch.close()} releases the
 * Java-side memory — the allocator's live byte count is therefore a direct measurement of how much
 * has not yet been consumed by Velox.
 *
 * <p>Poll loop (not a condition variable) is used because Arrow has no allocation listener hook.
 * Poll cost is negligible relative to query runtime.
 */
public final class Backpressure {

  private static final long INITIAL_SLEEP_NANOS = TimeUnit.MICROSECONDS.toNanos(200);
  private static final long MAX_SLEEP_NANOS = TimeUnit.MILLISECONDS.toNanos(5);

  private Backpressure() {}

  /**
   * Park until {@code allocator.getAllocatedMemory() <= watermarkBytes} or the timeout elapses.
   *
   * @param allocator allocator whose live byte count drives the gate
   * @param watermarkBytes soft watermark — parking ends as soon as live bytes drop below this
   * @param timeoutNanos maximum total time to wait; negative means no timeout
   * @param waitNanos counter incremented by the total nanoseconds spent parked (may be null)
   * @throws BackpressureTimeoutException if the timeout elapses before the allocator drains
   * @throws InterruptedException if the thread is interrupted while sleeping
   */
  public static void awaitBelow(
      BufferAllocator allocator, long watermarkBytes, long timeoutNanos, LongAdder waitNanos)
      throws InterruptedException {
    long allocated = allocator.getAllocatedMemory();
    if (allocated <= watermarkBytes) {
      return;
    }

    long start = System.nanoTime();
    long deadline = timeoutNanos < 0 ? Long.MAX_VALUE : start + timeoutNanos;
    long sleep = INITIAL_SLEEP_NANOS;

    while (true) {
      long now = System.nanoTime();
      if (now >= deadline) {
        long waited = now - start;
        if (waitNanos != null) {
          waitNanos.add(waited);
        }
        throw new BackpressureTimeoutException(
            "Backpressure gate timed out after "
                + TimeUnit.NANOSECONDS.toMillis(waited)
                + "ms: allocator live bytes "
                + allocator.getAllocatedMemory()
                + " > watermark "
                + watermarkBytes);
      }

      long remaining = deadline - now;
      long toSleep = Math.min(sleep, remaining);
      TimeUnit.NANOSECONDS.sleep(toSleep);

      allocated = allocator.getAllocatedMemory();
      if (allocated <= watermarkBytes) {
        if (waitNanos != null) {
          waitNanos.add(System.nanoTime() - start);
        }
        return;
      }

      sleep = Math.min(sleep * 2, MAX_SLEEP_NANOS);
    }
  }
}
