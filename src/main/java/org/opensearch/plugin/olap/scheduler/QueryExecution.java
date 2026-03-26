/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.plugin.olap.common.QueryId;
import org.opensearch.plugin.olap.plan.fragment.PlanFragment;

/**
 * Tracks the full lifecycle of a single distributed query execution.
 *
 * <p>A QueryExecution is created by the QueryScheduler for each incoming query. It holds the plan
 * fragments, stages, and overall execution state.
 *
 * <p>Modeled after Presto's SqlQueryExecution: the coordinator creates stages from plan fragments
 * and orchestrates their execution.
 */
public class QueryExecution {

  public enum State {
    PLANNING,
    SCHEDULING,
    RUNNING,
    FINISHING,
    FINISHED,
    FAILED
  }

  private final QueryId queryId;
  private final List<PlanFragment> fragments;
  private final Map<Integer, Stage> stagesByFragmentId;
  private final TaskTracker taskTracker;
  private volatile State state;
  private volatile Exception failureCause;

  public QueryExecution(QueryId queryId, List<PlanFragment> fragments) {
    this.queryId = queryId;
    this.fragments = Collections.unmodifiableList(fragments);
    this.stagesByFragmentId = new HashMap<>();
    this.taskTracker = new TaskTracker();
    this.state = State.PLANNING;
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public List<PlanFragment> getFragments() {
    return fragments;
  }

  public TaskTracker getTaskTracker() {
    return taskTracker;
  }

  public State getState() {
    return state;
  }

  public void setState(State state) {
    this.state = state;
  }

  public Exception getFailureCause() {
    return failureCause;
  }

  public Stage getStage(int fragmentId) {
    return stagesByFragmentId.get(fragmentId);
  }

  public List<Stage> getStages() {
    return new ArrayList<>(stagesByFragmentId.values());
  }

  /**
   * Create stages from plan fragments. Each fragment becomes a stage identified by StageId(queryId,
   * fragmentId).
   */
  public void buildStages() {
    for (PlanFragment fragment : fragments) {
      StageId stageId = new StageId(queryId, fragment.getFragmentId());

      List<StageId> upstreamIds = new ArrayList<>();
      for (int inputFragId : fragment.getInputFragmentIds()) {
        upstreamIds.add(new StageId(queryId, inputFragId));
      }

      Stage stage = new Stage(stageId, fragment, upstreamIds);
      stagesByFragmentId.put(fragment.getFragmentId(), stage);
    }
  }

  /** Returns leaf stages (no upstream dependencies) that should be scheduled first. */
  public List<Stage> getLeafStages() {
    List<Stage> leafStages = new ArrayList<>();
    for (Stage stage : stagesByFragmentId.values()) {
      if (stage.getUpstreamStageIds().isEmpty()) {
        leafStages.add(stage);
      }
    }
    return leafStages;
  }

  /** Returns stages whose upstream dependencies have all finished. */
  public List<Stage> getReadyStages() {
    List<Stage> ready = new ArrayList<>();
    for (Stage stage : stagesByFragmentId.values()) {
      if (stage.getState() != Stage.StageState.PLANNED) {
        continue;
      }
      boolean allUpstreamDone =
          stage.getUpstreamStageIds().stream()
              .allMatch(
                  upId -> {
                    Stage upstream = stagesByFragmentId.get(upId.getStageNumber());
                    return upstream != null && upstream.getState() == Stage.StageState.FINISHED;
                  });
      if (allUpstreamDone) {
        ready.add(stage);
      }
    }
    return ready;
  }

  public void fail(Exception cause) {
    this.state = State.FAILED;
    this.failureCause = cause;
    this.taskTracker.cancelAll();
  }
}
