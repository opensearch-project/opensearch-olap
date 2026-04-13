/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.scheduler;

import java.util.Locale;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.NodeNotConnectedException;
import org.opensearch.transport.TransportException;

/**
 * Classifies execution errors to determine retry strategy.
 *
 * <ul>
 *   <li>RETRYABLE_NODE — node unreachable, retry on a different node
 *   <li>RETRYABLE_SHARD — shard unavailable on this node, retry on replica
 *   <li>RETRYABLE_TRANSIENT — timeout or temporary error, retry on same node
 *   <li>FATAL — logic error, unsupported operation, don't retry
 * </ul>
 */
public final class ErrorClassifier {

  public enum ErrorCategory {
    RETRYABLE_NODE,
    RETRYABLE_SHARD,
    RETRYABLE_TRANSIENT,
    FATAL
  }

  private ErrorClassifier() {}

  /** Classify a TransportException (from handleException callback). */
  public static ErrorCategory classify(TransportException ex) {
    if (ex instanceof ConnectTransportException || ex instanceof NodeNotConnectedException) {
      return ErrorCategory.RETRYABLE_NODE;
    }
    String msg = ex.getMessage();
    if (msg != null) {
      return classifyMessage(msg);
    }
    return ErrorCategory.FATAL;
  }

  /** Classify an error message from ExecuteFragmentResponse. */
  public static ErrorCategory classify(String errorMessage) {
    if (errorMessage == null) {
      return ErrorCategory.FATAL;
    }
    return classifyMessage(errorMessage);
  }

  private static ErrorCategory classifyMessage(String msg) {
    String lower = msg.toLowerCase(Locale.ROOT);

    // Node-level failures
    if (lower.contains("connecttransportexception")
        || lower.contains("nodenotconnectedexception")
        || lower.contains("node not connected")
        || lower.contains("connect_transport_exception")) {
      return ErrorCategory.RETRYABLE_NODE;
    }

    // Shard-level failures
    if (lower.contains("shard not found")
        || lower.contains("shardnotfoundexception")
        || lower.contains("no active shards")
        || lower.contains("shard is not available")
        || lower.contains("shardtransientunavailableexception")) {
      return ErrorCategory.RETRYABLE_SHARD;
    }

    // Transient failures (retry on same node)
    if (lower.contains("timeout")
        || lower.contains("timed out")
        || lower.contains("circuit_breaking_exception")
        || lower.contains("rejected execution")
        || lower.contains("too_many_requests")) {
      return ErrorCategory.RETRYABLE_TRANSIENT;
    }

    // Everything else is fatal
    return ErrorCategory.FATAL;
  }
}
