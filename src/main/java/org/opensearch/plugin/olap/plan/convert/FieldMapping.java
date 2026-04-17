/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.convert;

/**
 * Maps a Calcite field index to its position in the Velox scan output. Used for MAP→ROW conversion
 * where OpenSearch object fields (MAP) are converted to Velox ROW types, causing field index
 * changes.
 *
 * <p>For dot-path child fields (e.g., cloud.region), {@code childPath} provides the path segments
 * to navigate into the parent ROW struct.
 */
public class FieldMapping {
  private final int veloxIndex;
  private final String veloxFieldName;
  private final String[] childPath;

  public FieldMapping(int veloxIndex, String veloxFieldName, String[] childPath) {
    this.veloxIndex = veloxIndex;
    this.veloxFieldName = veloxFieldName;
    this.childPath = childPath;
  }

  public int getVeloxIndex() {
    return veloxIndex;
  }

  public String getVeloxFieldName() {
    return veloxFieldName;
  }

  public String[] getChildPath() {
    return childPath;
  }

  public boolean isDotPathChild() {
    return childPath != null;
  }
}
