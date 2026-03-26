/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.result;

import java.util.List;
import org.opensearch.plugin.olap.transport.ExecuteFragmentResponse;

/**
 * Interface for collecting and merging results from distributed fragment execution.
 *
 * <p>The coordinator receives ExecuteFragmentResponse from each data node. The ResultCollector
 * merges these partial results into a final QueryResult.
 *
 * <p>TODO: Implement with support for: - Merging Arrow IPC streams from multiple nodes - Applying
 * final aggregation on coordinator for PARTIAL → FINAL plans - Applying final ORDER BY / LIMIT on
 * merged results - Memory-efficient streaming for large result sets
 */
public interface ResultCollector {

  /**
   * Collect partial results from all fragment executions and produce a final result.
   *
   * @param responses Responses from all data nodes
   * @return Merged QueryResult
   */
  QueryResult collect(List<ExecuteFragmentResponse> responses);
}
