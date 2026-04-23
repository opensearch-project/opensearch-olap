/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.execution;

/**
 * Thrown when a backpressure gate fails to drain within its timeout, or when an Arrow allocator
 * refuses a buffer because its hard cap is exceeded. Classified as {@code RETRYABLE_TRANSIENT} —
 * the fragment is retried under the assumption that the downstream pressure will ease.
 */
public class BackpressureTimeoutException extends RuntimeException {
  public BackpressureTimeoutException(String message) {
    super(message);
  }

  public BackpressureTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }
}
