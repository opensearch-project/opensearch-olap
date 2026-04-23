/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.execution;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.opensearch.test.OpenSearchTestCase;

public class BackpressureTests extends OpenSearchTestCase {

  public void testAwaitBelowReturnsImmediatelyWhenUnderWatermark() throws Exception {
    try (BufferAllocator allocator = new RootAllocator(10 * 1024 * 1024)) {
      LongAdder waitNanos = new LongAdder();
      long start = System.nanoTime();
      Backpressure.awaitBelow(allocator, 1024, TimeUnit.SECONDS.toNanos(5), waitNanos);
      long elapsed = System.nanoTime() - start;
      // No allocation → gate returns immediately, waitNanos should not be incremented
      assertEquals(0L, waitNanos.sum());
      assertTrue(
          "should return quickly, elapsed=" + elapsed, elapsed < TimeUnit.MILLISECONDS.toNanos(50));
    }
  }

  public void testAwaitBelowWaitsUntilBufferReleased() throws Exception {
    try (BufferAllocator allocator = new RootAllocator(10 * 1024 * 1024)) {
      // Allocate 8 KB so we're over a 1 KB watermark.
      ArrowBuf buf = allocator.buffer(8 * 1024);
      assertTrue(allocator.getAllocatedMemory() >= 8 * 1024);

      LongAdder waitNanos = new LongAdder();

      // Schedule a release in another thread after a short delay.
      Thread releaser =
          new Thread(
              () -> {
                try {
                  Thread.sleep(50);
                } catch (InterruptedException ignored) {
                  Thread.currentThread().interrupt();
                }
                buf.close();
              });
      releaser.setDaemon(true);
      releaser.start();

      long start = System.nanoTime();
      Backpressure.awaitBelow(allocator, 1024, TimeUnit.SECONDS.toNanos(5), waitNanos);
      long elapsed = System.nanoTime() - start;

      releaser.join(1_000);
      assertTrue(
          "should have waited at least 40ms, elapsed=" + elapsed,
          elapsed >= TimeUnit.MILLISECONDS.toNanos(40));
      assertTrue("waitNanos should be recorded: " + waitNanos.sum(), waitNanos.sum() > 0);
      assertEquals(0L, allocator.getAllocatedMemory());
    }
  }

  public void testAwaitBelowTimesOut() {
    try (BufferAllocator allocator = new RootAllocator(10 * 1024 * 1024)) {
      ArrowBuf buf = allocator.buffer(8 * 1024);
      try {
        LongAdder waitNanos = new LongAdder();
        Backpressure.awaitBelow(allocator, 1024, TimeUnit.MILLISECONDS.toNanos(50), waitNanos);
        fail("expected BackpressureTimeoutException");
      } catch (BackpressureTimeoutException e) {
        assertTrue(e.getMessage().contains("Backpressure gate timed out"));
      } catch (InterruptedException e) {
        fail("unexpected interrupt");
      } finally {
        buf.close();
      }
    }
  }
}
