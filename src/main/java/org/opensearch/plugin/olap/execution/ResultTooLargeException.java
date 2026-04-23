/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.execution;

/**
 * Thrown when a fragment's serialized result size exceeds the configured cap ({@code
 * plugins.velox.max_result_bytes}) or when the aggregate response size at the coordinator exceeds
 * {@code plugins.velox.coordinator_inflight_bytes}. Classified as {@code RESOURCE_EXCEEDED} —
 * non-retryable, since retrying the same query with the same data will fail identically.
 */
public class ResultTooLargeException extends RuntimeException {
  public ResultTooLargeException(String message) {
    super(message);
  }
}
