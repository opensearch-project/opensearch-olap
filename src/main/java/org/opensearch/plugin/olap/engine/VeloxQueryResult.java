/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.engine;

import java.util.List;
import java.util.Map;

/**
 * Result of a Velox query execution.
 *
 * <p>Contains the column metadata and row data from the distributed Velox execution. This is an
 * intermediate result format that can be converted to the SQL plugin's {@code
 * ExecutionEngine.QueryResponse} by the integration layer.
 */
public class VeloxQueryResult {

  private final List<String> columnNames;
  private final List<String> columnTypes;
  private final List<Map<String, Object>> rows;

  public VeloxQueryResult(
      List<String> columnNames, List<String> columnTypes, List<Map<String, Object>> rows) {
    this.columnNames = columnNames;
    this.columnTypes = columnTypes;
    this.rows = rows;
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public List<String> getColumnTypes() {
    return columnTypes;
  }

  public List<Map<String, Object>> getRows() {
    return rows;
  }

  public long getRowCount() {
    return rows.size();
  }
}
