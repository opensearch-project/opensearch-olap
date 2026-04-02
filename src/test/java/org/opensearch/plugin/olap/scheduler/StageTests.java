/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.scheduler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.plugin.olap.common.QueryId;
import org.opensearch.plugin.olap.plan.fragment.FragmentProperties;
import org.opensearch.plugin.olap.plan.fragment.PlanFragment;
import org.opensearch.test.OpenSearchTestCase;

public class StageTests extends OpenSearchTestCase {

  private static PlanFragment mockFragment(int id) {
    PlanFragment frag = mock(PlanFragment.class);
    when(frag.getFragmentId()).thenReturn(id);
    when(frag.getInputFragmentIds()).thenReturn(Collections.emptyList());
    when(frag.getProperties()).thenReturn(FragmentProperties.source("idx"));
    return frag;
  }

  private static TaskDescriptor buildTask(QueryId queryId, int stageNum, int partition) {
    StageId stageId = new StageId(queryId, stageNum);
    TaskId taskId = new TaskId(stageId, partition);
    PlanFragment fragment = mockFragment(stageNum);
    DiscoveryNode node = mock(DiscoveryNode.class);
    when(node.getName()).thenReturn("node-" + partition);
    return new TaskDescriptor(taskId, fragment, node, Collections.emptyList());
  }

  public void testInitialStateIsPlanned() {
    QueryId qid = QueryId.of("q1");
    StageId sid = new StageId(qid, 0);
    Stage stage = new Stage(sid, mockFragment(0), Collections.emptyList());
    assertEquals(Stage.StageState.PLANNED, stage.getState());
  }

  public void testValidTransitionPlannedToScheduling() {
    QueryId qid = QueryId.of("q1");
    StageId sid = new StageId(qid, 0);
    Stage stage = new Stage(sid, mockFragment(0), Collections.emptyList());
    boolean transitioned = stage.transitionTo(Stage.StageState.SCHEDULING);
    assertTrue(transitioned);
    assertEquals(Stage.StageState.SCHEDULING, stage.getState());
  }

  public void testInvalidTransitionPlannedToFinished() {
    QueryId qid = QueryId.of("q1");
    StageId sid = new StageId(qid, 0);
    Stage stage = new Stage(sid, mockFragment(0), Collections.emptyList());
    boolean transitioned = stage.transitionTo(Stage.StageState.FINISHED);
    assertFalse(transitioned);
    assertEquals(Stage.StageState.PLANNED, stage.getState()); // unchanged
  }

  public void testTransitionSchedulingToRunning() {
    QueryId qid = QueryId.of("q1");
    StageId sid = new StageId(qid, 0);
    Stage stage = new Stage(sid, mockFragment(0), Collections.emptyList());
    stage.transitionTo(Stage.StageState.SCHEDULING);
    boolean result = stage.transitionTo(Stage.StageState.RUNNING);
    assertTrue(result);
    assertEquals(Stage.StageState.RUNNING, stage.getState());
  }

  public void testTransitionRunningToFinished() {
    QueryId qid = QueryId.of("q1");
    StageId sid = new StageId(qid, 0);
    Stage stage = new Stage(sid, mockFragment(0), Collections.emptyList());
    stage.transitionTo(Stage.StageState.SCHEDULING);
    stage.transitionTo(Stage.StageState.RUNNING);
    boolean result = stage.transitionTo(Stage.StageState.FINISHED);
    assertTrue(result);
    assertEquals(Stage.StageState.FINISHED, stage.getState());
  }

  public void testNoTransitionFromTerminalState() {
    QueryId qid = QueryId.of("q1");
    StageId sid = new StageId(qid, 0);
    Stage stage = new Stage(sid, mockFragment(0), Collections.emptyList());
    stage.transitionTo(Stage.StageState.SCHEDULING);
    stage.transitionTo(Stage.StageState.RUNNING);
    stage.transitionTo(Stage.StageState.FINISHED);
    // Can't transition out of FINISHED
    boolean result = stage.transitionTo(Stage.StageState.FAILED);
    assertFalse(result);
    assertEquals(Stage.StageState.FINISHED, stage.getState());
  }

  public void testAddTask() {
    QueryId qid = QueryId.of("q1");
    StageId sid = new StageId(qid, 0);
    Stage stage = new Stage(sid, mockFragment(0), Collections.emptyList());
    TaskDescriptor task = buildTask(qid, 0, 0);
    stage.addTask(task);
    assertEquals(1, stage.getTasks().size());
    assertSame(task, stage.getTasks().get(0));
  }

  public void testUpdateStateFromTasksAllFinished() {
    QueryId qid = QueryId.of("q1");
    StageId sid = new StageId(qid, 0);
    Stage stage = new Stage(sid, mockFragment(0), Collections.emptyList());
    stage.transitionTo(Stage.StageState.SCHEDULING);
    stage.transitionTo(Stage.StageState.RUNNING);

    TaskDescriptor t0 = buildTask(qid, 0, 0);
    TaskDescriptor t1 = buildTask(qid, 0, 1);
    t0.setState(TaskState.FINISHED);
    t1.setState(TaskState.FINISHED);
    stage.addTask(t0);
    stage.addTask(t1);

    stage.updateStateFromTasks();
    assertEquals(Stage.StageState.FINISHED, stage.getState());
  }

  public void testUpdateStateFromTasksAnyFailed() {
    QueryId qid = QueryId.of("q1");
    StageId sid = new StageId(qid, 0);
    Stage stage = new Stage(sid, mockFragment(0), Collections.emptyList());
    stage.transitionTo(Stage.StageState.SCHEDULING);
    stage.transitionTo(Stage.StageState.RUNNING);

    TaskDescriptor t0 = buildTask(qid, 0, 0);
    TaskDescriptor t1 = buildTask(qid, 0, 1);
    t0.setState(TaskState.FINISHED);
    t1.setState(TaskState.FAILED);
    stage.addTask(t0);
    stage.addTask(t1);

    stage.updateStateFromTasks();
    assertEquals(Stage.StageState.FAILED, stage.getState());
  }

  public void testUpdateStateFromTasksAnyRunning() {
    QueryId qid = QueryId.of("q1");
    StageId sid = new StageId(qid, 0);
    Stage stage = new Stage(sid, mockFragment(0), Collections.emptyList());
    stage.transitionTo(Stage.StageState.SCHEDULING);

    TaskDescriptor t0 = buildTask(qid, 0, 0);
    TaskDescriptor t1 = buildTask(qid, 0, 1);
    t0.setState(TaskState.RUNNING);
    t1.setState(TaskState.PENDING);
    stage.addTask(t0);
    stage.addTask(t1);

    // But SCHEDULING → RUNNING is valid
    stage.updateStateFromTasks();
    assertEquals(Stage.StageState.RUNNING, stage.getState());
  }

  public void testUpdateStateFromTasksNoOpWhenEmpty() {
    QueryId qid = QueryId.of("q1");
    StageId sid = new StageId(qid, 0);
    Stage stage = new Stage(sid, mockFragment(0), Collections.emptyList());
    // No tasks — should not change state
    stage.updateStateFromTasks();
    assertEquals(Stage.StageState.PLANNED, stage.getState());
  }

  public void testUpstreamStageIds() {
    QueryId qid = QueryId.of("q1");
    StageId s0 = new StageId(qid, 0);
    StageId s1 = new StageId(qid, 1);
    Stage stage = new Stage(s1, mockFragment(1), List.of(s0));
    assertEquals(1, stage.getUpstreamStageIds().size());
    assertEquals(s0, stage.getUpstreamStageIds().get(0));
  }

  public void testNullUpstreamIdsBecomesEmptyList() {
    QueryId qid = QueryId.of("q1");
    StageId sid = new StageId(qid, 0);
    Stage stage = new Stage(sid, mockFragment(0), null);
    assertNotNull(stage.getUpstreamStageIds());
    assertTrue(stage.getUpstreamStageIds().isEmpty());
  }
}
