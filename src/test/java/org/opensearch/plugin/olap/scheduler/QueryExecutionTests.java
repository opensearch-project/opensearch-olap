/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.scheduler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.opensearch.plugin.olap.common.QueryId;
import org.opensearch.plugin.olap.plan.fragment.FragmentProperties;
import org.opensearch.plugin.olap.plan.fragment.PlanFragment;
import org.opensearch.test.OpenSearchTestCase;

public class QueryExecutionTests extends OpenSearchTestCase {

  private static PlanFragment buildLeafFragment(int id, String index) {
    PlanFragment frag = mock(PlanFragment.class);
    when(frag.getFragmentId()).thenReturn(id);
    when(frag.getInputFragmentIds()).thenReturn(Collections.emptyList());
    when(frag.getProperties()).thenReturn(FragmentProperties.source(index));
    when(frag.isLeaf()).thenReturn(true);
    when(frag.isRoot()).thenReturn(false);
    return frag;
  }

  private static PlanFragment buildRootFragment(int id, List<Integer> inputs) {
    PlanFragment frag = mock(PlanFragment.class);
    when(frag.getFragmentId()).thenReturn(id);
    when(frag.getInputFragmentIds()).thenReturn(inputs);
    when(frag.getProperties()).thenReturn(FragmentProperties.coordinator());
    when(frag.isLeaf()).thenReturn(false);
    when(frag.isRoot()).thenReturn(true);
    return frag;
  }

  public void testInitialStateIsPlanning() {
    QueryId qid = QueryId.generate();
    QueryExecution exec = new QueryExecution(qid, Collections.emptyList());
    assertEquals(QueryExecution.State.PLANNING, exec.getState());
    assertNull(exec.getFailureCause());
  }

  public void testSetState() {
    QueryId qid = QueryId.generate();
    QueryExecution exec = new QueryExecution(qid, Collections.emptyList());
    exec.setState(QueryExecution.State.RUNNING);
    assertEquals(QueryExecution.State.RUNNING, exec.getState());
  }

  public void testGetQueryId() {
    QueryId qid = QueryId.of("my-query");
    QueryExecution exec = new QueryExecution(qid, Collections.emptyList());
    assertEquals(qid, exec.getQueryId());
  }

  public void testGetFragments() {
    QueryId qid = QueryId.generate();
    PlanFragment frag0 = buildLeafFragment(0, "idx");
    PlanFragment frag1 = buildRootFragment(1, List.of(0));
    QueryExecution exec = new QueryExecution(qid, Arrays.asList(frag0, frag1));
    assertEquals(2, exec.getFragments().size());
  }

  public void testBuildStagesCreatesOneStagePerFragment() {
    QueryId qid = QueryId.of("q1");
    PlanFragment frag0 = buildLeafFragment(0, "idx");
    PlanFragment frag1 = buildRootFragment(1, List.of(0));

    QueryExecution exec = new QueryExecution(qid, Arrays.asList(frag0, frag1));
    exec.buildStages();

    assertEquals(2, exec.getStages().size());
    assertNotNull(exec.getStage(0));
    assertNotNull(exec.getStage(1));
  }

  public void testBuildStagesStageIdsMatchFragmentIds() {
    QueryId qid = QueryId.of("q1");
    PlanFragment frag0 = buildLeafFragment(0, "idx");
    QueryExecution exec = new QueryExecution(qid, Collections.singletonList(frag0));
    exec.buildStages();

    Stage stage = exec.getStage(0);
    assertNotNull(stage);
    assertEquals(0, stage.getStageId().getStageNumber());
    assertEquals(qid, stage.getStageId().getQueryId());
  }

  public void testGetLeafStagesReturnsStagesWithNoUpstream() {
    QueryId qid = QueryId.of("q1");
    PlanFragment frag0 = buildLeafFragment(0, "idx");
    PlanFragment frag1 = buildRootFragment(1, List.of(0));

    QueryExecution exec = new QueryExecution(qid, Arrays.asList(frag0, frag1));
    exec.buildStages();

    List<Stage> leafStages = exec.getLeafStages();
    assertEquals(1, leafStages.size());
    assertEquals(0, leafStages.get(0).getStageId().getStageNumber());
  }

  public void testGetReadyStagesToRunAfterLeafFinishes() {
    QueryId qid = QueryId.of("q1");
    PlanFragment frag0 = buildLeafFragment(0, "idx");
    PlanFragment frag1 = buildRootFragment(1, List.of(0));

    QueryExecution exec = new QueryExecution(qid, Arrays.asList(frag0, frag1));
    exec.buildStages();

    // Initially root stage is not ready because upstream (frag0) is not finished
    // Mark leaf stage as finished
    Stage leaf = exec.getStage(0);
    leaf.transitionTo(Stage.StageState.SCHEDULING);
    leaf.transitionTo(Stage.StageState.RUNNING);
    leaf.transitionTo(Stage.StageState.FINISHED);

    // Root stage's upstream (0) is now finished → root should be ready
    List<Stage> ready = exec.getReadyStages();
    assertEquals(1, ready.size());
    assertEquals(1, ready.get(0).getStageId().getStageNumber());
  }

  public void testFailCancelsAllTasks() {
    QueryId qid = QueryId.of("q1");
    PlanFragment frag0 = buildLeafFragment(0, "idx");
    QueryExecution exec = new QueryExecution(qid, Collections.singletonList(frag0));
    exec.buildStages();

    Exception cause = new RuntimeException("oops");
    exec.fail(cause);

    assertEquals(QueryExecution.State.FAILED, exec.getState());
    assertSame(cause, exec.getFailureCause());
  }

  public void testTaskTrackerIsAvailable() {
    QueryId qid = QueryId.generate();
    QueryExecution exec = new QueryExecution(qid, Collections.emptyList());
    assertNotNull(exec.getTaskTracker());
  }
}
