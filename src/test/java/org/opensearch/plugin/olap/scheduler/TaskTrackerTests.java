/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.scheduler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.plugin.olap.common.QueryId;
import org.opensearch.plugin.olap.plan.fragment.PlanFragment;
import org.opensearch.test.OpenSearchTestCase;

public class TaskTrackerTests extends OpenSearchTestCase {

  private TaskDescriptor buildTask(QueryId queryId, int stageNum, int partition) {
    StageId stageId = new StageId(queryId, stageNum);
    TaskId taskId = new TaskId(stageId, partition);
    PlanFragment fragment = mock(PlanFragment.class);
    when(fragment.getFragmentId()).thenReturn(stageNum);
    DiscoveryNode node = mock(DiscoveryNode.class);
    when(node.getName()).thenReturn("node-" + partition);
    return new TaskDescriptor(taskId, fragment, node, Collections.emptyList());
  }

  public void testRegisterAndGet() {
    TaskTracker tracker = new TaskTracker();
    QueryId qid = QueryId.of("q1");
    TaskDescriptor task = buildTask(qid, 0, 0);

    tracker.register(task);
    assertSame(task, tracker.get(task.getTaskId()));
  }

  public void testGetReturnsNullForUnregisteredTask() {
    TaskTracker tracker = new TaskTracker();
    QueryId qid = QueryId.of("q1");
    TaskId unknownId = new TaskId(new StageId(qid, 99), 0);
    assertNull(tracker.get(unknownId));
  }

  public void testUpdateStateChangesTaskState() {
    TaskTracker tracker = new TaskTracker();
    QueryId qid = QueryId.of("q1");
    TaskDescriptor task = buildTask(qid, 0, 0);
    tracker.register(task);

    assertEquals(TaskState.PENDING, task.getState());
    tracker.updateState(task.getTaskId(), TaskState.RUNNING);
    assertEquals(TaskState.RUNNING, task.getState());
    tracker.updateState(task.getTaskId(), TaskState.FINISHED);
    assertEquals(TaskState.FINISHED, task.getState());
  }

  public void testUpdateStateOnUnregisteredTaskIsNoOp() {
    TaskTracker tracker = new TaskTracker();
    QueryId qid = QueryId.of("q1");
    TaskId unknownId = new TaskId(new StageId(qid, 99), 0);
    // Should not throw
    tracker.updateState(unknownId, TaskState.RUNNING);
  }

  public void testMarkFailedSetsStateAndReason() {
    TaskTracker tracker = new TaskTracker();
    QueryId qid = QueryId.of("q1");
    TaskDescriptor task = buildTask(qid, 0, 0);
    tracker.register(task);

    tracker.markFailed(task.getTaskId(), "disk full");
    assertEquals(TaskState.FAILED, task.getState());
    assertEquals("disk full", task.getFailureReason());
  }

  public void testMarkFailedOnUnregisteredTaskIsNoOp() {
    TaskTracker tracker = new TaskTracker();
    QueryId qid = QueryId.of("q1");
    TaskId unknownId = new TaskId(new StageId(qid, 99), 0);
    // Should not throw
    tracker.markFailed(unknownId, "some error");
  }

  public void testAllTerminalWhenAllFinished() {
    TaskTracker tracker = new TaskTracker();
    QueryId qid = QueryId.of("q1");

    TaskDescriptor t0 = buildTask(qid, 0, 0);
    TaskDescriptor t1 = buildTask(qid, 0, 1);
    tracker.register(t0);
    tracker.register(t1);

    t0.setState(TaskState.FINISHED);
    t1.setState(TaskState.FINISHED);
    assertTrue(tracker.allTerminal());
  }

  public void testAllTerminalFalseWhenOnePending() {
    TaskTracker tracker = new TaskTracker();
    QueryId qid = QueryId.of("q1");

    TaskDescriptor t0 = buildTask(qid, 0, 0);
    TaskDescriptor t1 = buildTask(qid, 0, 1);
    tracker.register(t0);
    tracker.register(t1);

    t0.setState(TaskState.FINISHED);
    // t1 stays PENDING
    assertFalse(tracker.allTerminal());
  }

  public void testAnyFailedReturnsTrueOnFailure() {
    TaskTracker tracker = new TaskTracker();
    QueryId qid = QueryId.of("q1");

    TaskDescriptor t0 = buildTask(qid, 0, 0);
    TaskDescriptor t1 = buildTask(qid, 0, 1);
    tracker.register(t0);
    tracker.register(t1);

    t0.setState(TaskState.FINISHED);
    t1.setState(TaskState.FAILED);
    assertTrue(tracker.anyFailed());
  }

  public void testAnyFailedReturnsFalseWhenNoneFailed() {
    TaskTracker tracker = new TaskTracker();
    QueryId qid = QueryId.of("q1");

    TaskDescriptor t0 = buildTask(qid, 0, 0);
    tracker.register(t0);
    t0.setState(TaskState.FINISHED);
    assertFalse(tracker.anyFailed());
  }

  public void testCountByState() {
    TaskTracker tracker = new TaskTracker();
    QueryId qid = QueryId.of("q1");

    TaskDescriptor t0 = buildTask(qid, 0, 0);
    TaskDescriptor t1 = buildTask(qid, 0, 1);
    TaskDescriptor t2 = buildTask(qid, 0, 2);
    tracker.register(t0);
    tracker.register(t1);
    tracker.register(t2);

    t0.setState(TaskState.FINISHED);
    t1.setState(TaskState.FINISHED);
    // t2 stays PENDING

    assertEquals(2, tracker.countByState(TaskState.FINISHED));
    assertEquals(1, tracker.countByState(TaskState.PENDING));
    assertEquals(0, tracker.countByState(TaskState.FAILED));
  }

  public void testCancelAllSetsNonTerminalToCancelled() {
    TaskTracker tracker = new TaskTracker();
    QueryId qid = QueryId.of("q1");

    TaskDescriptor t0 = buildTask(qid, 0, 0);
    TaskDescriptor t1 = buildTask(qid, 0, 1);
    TaskDescriptor t2 = buildTask(qid, 0, 2);
    tracker.register(t0);
    tracker.register(t1);
    tracker.register(t2);

    t0.setState(TaskState.RUNNING);
    t1.setState(TaskState.FINISHED); // already terminal
    // t2 stays PENDING

    tracker.cancelAll();

    assertEquals(TaskState.CANCELLED, t0.getState());
    assertEquals(TaskState.FINISHED, t1.getState()); // unaffected
    assertEquals(TaskState.CANCELLED, t2.getState());
  }

  public void testAllTerminalTrueForMixedTerminalStates() {
    TaskTracker tracker = new TaskTracker();
    QueryId qid = QueryId.of("q1");

    TaskDescriptor t0 = buildTask(qid, 0, 0);
    TaskDescriptor t1 = buildTask(qid, 0, 1);
    TaskDescriptor t2 = buildTask(qid, 0, 2);
    tracker.register(t0);
    tracker.register(t1);
    tracker.register(t2);

    t0.setState(TaskState.FINISHED);
    t1.setState(TaskState.FAILED);
    t2.setState(TaskState.CANCELLED);

    assertTrue(tracker.allTerminal());
  }

  public void testAllTerminalTrueForEmptyTracker() {
    TaskTracker tracker = new TaskTracker();
    // No tasks registered — stream().allMatch() on empty stream returns true
    assertTrue(tracker.allTerminal());
  }
}
