/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.scheduler;

import org.opensearch.test.OpenSearchTestCase;

public class TableStatisticsTests extends OpenSearchTestCase {

  public void testBasicProperties() {
    TableStatistics stats = new TableStatistics("test_index", 1000, 50000, 3);
    assertEquals("test_index", stats.getIndexName());
    assertEquals(1000, stats.getRowCount());
    assertEquals(50000, stats.getSizeInBytes());
    assertEquals(3, stats.getShardCount());
  }

  public void testAverageRowSize() {
    TableStatistics stats = new TableStatistics("test", 100, 10000, 1);
    assertEquals(100, stats.getAverageRowSize());
  }

  public void testAverageRowSizeZeroRows() {
    TableStatistics stats = new TableStatistics("test", 0, 0, 1);
    assertEquals(0, stats.getAverageRowSize());
  }

  public void testToString() {
    TableStatistics stats = new TableStatistics("employees", 5, 5000, 1);
    String s = stats.toString();
    assertTrue(s.contains("employees"));
    assertTrue(s.contains("rows=5"));
    assertTrue(s.contains("size=5000"));
    assertTrue(s.contains("shards=1"));
  }
}
