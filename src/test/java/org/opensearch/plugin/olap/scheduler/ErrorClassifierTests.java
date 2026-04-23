/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.scheduler;

import org.opensearch.plugin.olap.scheduler.ErrorClassifier.ErrorCategory;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.NodeNotConnectedException;
import org.opensearch.transport.TransportException;

public class ErrorClassifierTests extends OpenSearchTestCase {

  // ---- TransportException classification ----

  public void testConnectTransportExceptionIsRetryableNode() {
    TransportException ex = new ConnectTransportException(null, "connect failed");
    assertEquals(ErrorCategory.RETRYABLE_NODE, ErrorClassifier.classify(ex));
  }

  public void testNodeNotConnectedExceptionIsRetryableNode() {
    TransportException ex = new NodeNotConnectedException(null, "not connected");
    assertEquals(ErrorCategory.RETRYABLE_NODE, ErrorClassifier.classify(ex));
  }

  public void testGenericTransportExceptionIsFatal() {
    TransportException ex = new TransportException("some unknown error");
    assertEquals(ErrorCategory.FATAL, ErrorClassifier.classify(ex));
  }

  // ---- String message classification ----

  public void testShardNotFoundIsRetryableShard() {
    assertEquals(
        ErrorCategory.RETRYABLE_SHARD, ErrorClassifier.classify("Shard not found for index"));
  }

  public void testNoActiveShardsIsRetryableShard() {
    assertEquals(ErrorCategory.RETRYABLE_SHARD, ErrorClassifier.classify("No active shards"));
  }

  public void testTimeoutIsRetryableTransient() {
    assertEquals(
        ErrorCategory.RETRYABLE_TRANSIENT, ErrorClassifier.classify("Execution timed out"));
  }

  public void testCircuitBreakingIsRetryableTransient() {
    assertEquals(
        ErrorCategory.RETRYABLE_TRANSIENT,
        ErrorClassifier.classify("circuit_breaking_exception: data too large"));
  }

  public void testRejectedExecutionIsRetryableTransient() {
    assertEquals(
        ErrorCategory.RETRYABLE_TRANSIENT, ErrorClassifier.classify("Rejected execution of query"));
  }

  public void testVeloxErrorIsFatal() {
    assertEquals(ErrorCategory.FATAL, ErrorClassifier.classify("VeloxUserError: INVALID_ARGUMENT"));
  }

  public void testNullMessageIsFatal() {
    assertEquals(ErrorCategory.FATAL, ErrorClassifier.classify((String) null));
  }

  public void testUnknownMessageIsFatal() {
    assertEquals(ErrorCategory.FATAL, ErrorClassifier.classify("something completely unexpected"));
  }

  public void testNodeConnectionMessageIsRetryableNode() {
    assertEquals(
        ErrorCategory.RETRYABLE_NODE,
        ErrorClassifier.classify("ConnectTransportException: node-1 not reachable"));
  }

  public void testResultTooLargeIsResourceExceeded() {
    assertEquals(
        ErrorCategory.RESOURCE_EXCEEDED,
        ErrorClassifier.classify("ResultTooLargeException: plugins.velox.max_result_bytes cap"));
  }

  public void testMaxResultBytesKeywordIsResourceExceeded() {
    assertEquals(
        ErrorCategory.RESOURCE_EXCEEDED,
        ErrorClassifier.classify(
            "Aggregate response exceeded plugins.velox.coordinator_inflight_bytes"));
  }

  public void testBackpressureTimeoutIsRetryableTransient() {
    assertEquals(
        ErrorCategory.RETRYABLE_TRANSIENT,
        ErrorClassifier.classify("BackpressureTimeoutException: drain timed out"));
  }

  public void testShuffleBufferFullIsRetryableTransient() {
    assertEquals(
        ErrorCategory.RETRYABLE_TRANSIENT,
        ErrorClassifier.classify("shuffle buffer full on data-1 after retries"));
  }
}
