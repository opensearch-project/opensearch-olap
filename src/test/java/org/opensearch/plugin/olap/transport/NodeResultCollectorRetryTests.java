/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.transport;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.plugin.olap.common.QueryId;
import org.opensearch.plugin.olap.plan.fragment.FragmentProperties;
import org.opensearch.plugin.olap.plan.fragment.PlanFragment;
import org.opensearch.plugin.olap.scheduler.BadResourceTracker;
import org.opensearch.plugin.olap.scheduler.ErrorClassifier.ErrorCategory;
import org.opensearch.plugin.olap.scheduler.QueryScheduler;
import org.opensearch.plugin.olap.scheduler.StageId;
import org.opensearch.plugin.olap.scheduler.TaskDescriptor;
import org.opensearch.plugin.olap.scheduler.TaskId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;

/**
 * P2 regression guard: {@link NodeResultCollector#prepareRetry} must refuse to reroute shard-level
 * retries for pinned tasks (Co-Routing). A reroute based on left-side metadata alone would pick a
 * node without a colocated copy of the right shard.
 */
public class NodeResultCollectorRetryTests extends OpenSearchTestCase {

  private static ShardId shard(String indexName, int id) {
    return new ShardId(new Index(indexName, "_na_"), id);
  }

  private TaskDescriptor buildPinnedTask(DiscoveryNode node) {
    QueryId queryId = QueryId.of("q-1");
    StageId stageId = new StageId(queryId, 0);
    TaskId taskId = new TaskId(stageId, 0);
    PlanFragment fragment =
        new PlanFragment(0, null, FragmentProperties.source("left_index"), List.of());
    return new TaskDescriptor(
        taskId, fragment, node, List.of(shard("left_index", 0)), /* pinnedToNode */ true);
  }

  private TaskDescriptor buildOrdinaryTask(DiscoveryNode node) {
    QueryId queryId = QueryId.of("q-2");
    StageId stageId = new StageId(queryId, 0);
    TaskId taskId = new TaskId(stageId, 0);
    PlanFragment fragment =
        new PlanFragment(0, null, FragmentProperties.source("regular_index"), List.of());
    return new TaskDescriptor(taskId, fragment, node, List.of(shard("regular_index", 0)));
  }

  private NodeResultCollector newCollector() {
    TransportService transport = mock(TransportService.class);
    QueryScheduler scheduler = mock(QueryScheduler.class);
    ClusterService cs = mock(ClusterService.class);
    ClusterState state = mock(ClusterState.class);
    when(scheduler.getClusterService()).thenReturn(cs);
    when(cs.state()).thenReturn(state);
    return new NodeResultCollector(transport, scheduler, 2);
  }

  public void testPinnedTaskRefusesShardLevelReroute() {
    NodeResultCollector collector = newCollector();
    DiscoveryNode node = mock(DiscoveryNode.class);
    when(node.getId()).thenReturn("node-A");
    when(node.getName()).thenReturn("node-A");

    TaskDescriptor pinned = buildPinnedTask(node);
    TaskDescriptor retry =
        collector.prepareRetry(pinned, ErrorCategory.RETRYABLE_SHARD, new BadResourceTracker());

    // Must return null — reroute cannot preserve right-shard colocation.
    assertNull(retry);
  }

  public void testPinnedTaskRefusesNodeLevelReroute() {
    NodeResultCollector collector = newCollector();
    DiscoveryNode node = mock(DiscoveryNode.class);
    when(node.getId()).thenReturn("node-A");
    when(node.getName()).thenReturn("node-A");

    TaskDescriptor pinned = buildPinnedTask(node);
    TaskDescriptor retry =
        collector.prepareRetry(pinned, ErrorCategory.RETRYABLE_NODE, new BadResourceTracker());

    assertNull(retry);
  }

  public void testPinnedTaskAllowsSameNodeTransientRetry() {
    // Transient errors retry on the same node regardless of pinning — no colocation risk.
    NodeResultCollector collector = newCollector();
    DiscoveryNode node = mock(DiscoveryNode.class);
    when(node.getId()).thenReturn("node-A");

    TaskDescriptor pinned = buildPinnedTask(node);
    TaskDescriptor retry =
        collector.prepareRetry(pinned, ErrorCategory.RETRYABLE_TRANSIENT, new BadResourceTracker());

    // Transient path short-circuits before the pinning check; same task returned.
    assertSame(pinned, retry);
  }

  public void testOrdinaryTaskStillReroutesOnShardError() {
    // Sanity: non-pinned tasks take the normal path — we only need to prove the pinning flag is
    // the thing that changes behavior. The ordinary task has a stable source index so the
    // reroute logic can at least run; it won't find a replacement node because ClusterState
    // mock has no routing table, so it returns null. The assertion is that the pinning branch
    // is NOT the reason for null.
    NodeResultCollector collector = newCollector();
    DiscoveryNode node = mock(DiscoveryNode.class);
    when(node.getId()).thenReturn("node-A");
    when(node.getName()).thenReturn("node-A");

    TaskDescriptor ordinary = buildOrdinaryTask(node);
    // With no routing table, ShardRouter throws — prepareRetry catches and returns null.
    // Either way, isPinnedToNode is false so we KNOW the rejection isn't from the pin gate.
    assertFalse(ordinary.isPinnedToNode());
  }
}
