/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.plugin.olap.result;

import java.util.List;

/**
 * Interface representing the result of an OLAP query execution.
 *
 * TODO: Implement with support for:
 * - Column metadata (names, types)
 * - Row iteration
 * - Arrow IPC serialization for streaming results
 * - Pagination for large result sets
 */
public interface QueryResult extends AutoCloseable {

    /**
     * Get column names in the result.
     */
    List<String> getColumnNames();

    /**
     * Get column type names.
     */
    List<String> getColumnTypes();

    /**
     * Total number of rows in the result.
     */
    long getRowCount();

    /**
     * Get the result data as Arrow IPC bytes.
     */
    byte[] toArrowIpc();

    /**
     * Check if the query execution was successful.
     */
    boolean isSuccess();

    /**
     * Get the error message if execution failed.
     */
    String getErrorMessage();
}
