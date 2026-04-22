/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.execution;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link LuceneArrowReader.ScanStats}. The accumulator is safe to share across
 * segment-reader threads (that's the whole point of using {@code LongAdder}), so the contract is:
 * many writers, one reader; final sum equals the arithmetic sum of all {@code add*} calls.
 */
public class LuceneArrowReaderScanStatsTests extends OpenSearchTestCase {

  public void testInitialCountersAreZero() {
    LuceneArrowReader.ScanStats stats = new LuceneArrowReader.ScanStats();
    assertEquals(0L, stats.getDocsRead());
    assertEquals(0L, stats.getDocsMatched());
  }

  public void testSingleThreadedAccumulation() {
    LuceneArrowReader.ScanStats stats = new LuceneArrowReader.ScanStats();
    stats.addDocsRead(100);
    stats.addDocsRead(50);
    stats.addDocsMatched(30);
    assertEquals(150L, stats.getDocsRead());
    assertEquals(30L, stats.getDocsMatched());
  }

  /**
   * Multi-threaded accumulation matches the single-threaded sum. This is the invariant that matters
   * when parallel segment readers share one ScanStats instance.
   */
  public void testConcurrentAdditionsProduceCorrectSum() throws InterruptedException {
    final int threads = 8;
    final int iterations = 10_000;
    LuceneArrowReader.ScanStats stats = new LuceneArrowReader.ScanStats();
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch ready = new CountDownLatch(threads);
    CountDownLatch go = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threads);

    for (int t = 0; t < threads; t++) {
      pool.submit(
          () -> {
            ready.countDown();
            try {
              go.await();
              for (int i = 0; i < iterations; i++) {
                stats.addDocsRead(1);
                if ((i & 1) == 0) stats.addDocsMatched(1);
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } finally {
              done.countDown();
            }
          });
    }

    ready.await();
    go.countDown();
    assertTrue(done.await(30, TimeUnit.SECONDS));
    pool.shutdown();
    assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));

    assertEquals((long) threads * iterations, stats.getDocsRead());
    assertEquals((long) threads * (iterations / 2), stats.getDocsMatched());
  }

  /** docsMatched can never exceed docsRead when the reader uses them correctly. */
  public void testMatchedLessThanOrEqualRead() {
    LuceneArrowReader.ScanStats stats = new LuceneArrowReader.ScanStats();
    stats.addDocsRead(500);
    stats.addDocsMatched(123);
    assertTrue(
        "docsMatched must not exceed docsRead", stats.getDocsMatched() <= stats.getDocsRead());
  }
}
