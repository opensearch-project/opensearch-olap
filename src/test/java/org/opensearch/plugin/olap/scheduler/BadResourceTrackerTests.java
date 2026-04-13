/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.scheduler;

import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

public class BadResourceTrackerTests extends OpenSearchTestCase {

  private ShardId shard(String index, int id) {
    return new ShardId(new Index(index, "_na_"), id);
  }

  public void testEmptyTrackerAcceptsAll() {
    BadResourceTracker tracker = new BadResourceTracker();
    assertTrue(tracker.isEmpty());
    assertFalse(tracker.isNodeBad("node-1"));
    assertFalse(tracker.isShardBadOnNode("node-1", shard("test", 0)));
  }

  public void testBadNodeIsTracked() {
    BadResourceTracker tracker = new BadResourceTracker();
    tracker.addBadNode("node-1");

    assertTrue(tracker.isNodeBad("node-1"));
    assertFalse(tracker.isNodeBad("node-2"));
    assertFalse(tracker.isEmpty());
  }

  public void testBadShardOnNodeIsTracked() {
    BadResourceTracker tracker = new BadResourceTracker();
    ShardId s0 = shard("test", 0);
    ShardId s1 = shard("test", 1);
    tracker.addBadShardOnNode("node-1", s0);

    assertTrue(tracker.isShardBadOnNode("node-1", s0));
    assertFalse(tracker.isShardBadOnNode("node-1", s1));
    assertFalse(tracker.isShardBadOnNode("node-2", s0));
    assertFalse(tracker.isEmpty());
  }

  public void testMultipleBadNodes() {
    BadResourceTracker tracker = new BadResourceTracker();
    tracker.addBadNode("node-1");
    tracker.addBadNode("node-2");

    assertTrue(tracker.isNodeBad("node-1"));
    assertTrue(tracker.isNodeBad("node-2"));
    assertFalse(tracker.isNodeBad("node-3"));
    assertEquals(2, tracker.getBadNodeIds().size());
  }

  public void testMultipleBadShardsOnSameNode() {
    BadResourceTracker tracker = new BadResourceTracker();
    ShardId s0 = shard("test", 0);
    ShardId s1 = shard("test", 1);
    tracker.addBadShardOnNode("node-1", s0);
    tracker.addBadShardOnNode("node-1", s1);

    assertTrue(tracker.isShardBadOnNode("node-1", s0));
    assertTrue(tracker.isShardBadOnNode("node-1", s1));
  }

  public void testBadNodeDoesNotAffectShardTracking() {
    BadResourceTracker tracker = new BadResourceTracker();
    tracker.addBadNode("node-1");

    // Node is bad, but shard tracking is separate
    assertFalse(tracker.isShardBadOnNode("node-1", shard("test", 0)));
    assertTrue(tracker.isNodeBad("node-1"));
  }
}
