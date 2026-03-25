/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.plugin.olap.scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.opensearch.plugin.olap.plan.fragment.PlanFragment;

/**
 * Represents an execution stage in a distributed query.
 *
 * <p>A stage corresponds to a PlanFragment and consists of one or more tasks,
 * each running on a different data node. The stage tracks the collective state
 * of all its tasks.
 *
 * <p>Modeled after Presto's SqlStageExecution: a stage is the unit of scheduling,
 * while tasks are the unit of execution on individual nodes.
 */
public class Stage {

    private final StageId stageId;
    private final PlanFragment fragment;
    private final List<TaskDescriptor> tasks;
    private final List<StageId> upstreamStageIds;
    private final AtomicReference<StageState> state;

    public enum StageState {
        PLANNED,
        SCHEDULING,
        RUNNING,
        FINISHED,
        FAILED,
        CANCELLED
    }

    public Stage(StageId stageId, PlanFragment fragment, List<StageId> upstreamStageIds) {
        this.stageId = stageId;
        this.fragment = fragment;
        this.tasks = new CopyOnWriteArrayList<>();
        this.upstreamStageIds = upstreamStageIds != null
            ? Collections.unmodifiableList(upstreamStageIds) : Collections.emptyList();
        this.state = new AtomicReference<>(StageState.PLANNED);
    }

    public StageId getStageId() {
        return stageId;
    }

    public PlanFragment getFragment() {
        return fragment;
    }

    public List<TaskDescriptor> getTasks() {
        return Collections.unmodifiableList(tasks);
    }

    public List<StageId> getUpstreamStageIds() {
        return upstreamStageIds;
    }

    public StageState getState() {
        return state.get();
    }

    public void addTask(TaskDescriptor task) {
        tasks.add(task);
    }

    public boolean transitionTo(StageState newState) {
        StageState current = state.get();
        if (isValidTransition(current, newState)) {
            return state.compareAndSet(current, newState);
        }
        return false;
    }

    /**
     * Recompute stage state based on task states.
     */
    public void updateStateFromTasks() {
        if (tasks.isEmpty()) {
            return;
        }

        boolean allFinished = true;
        boolean anyFailed = false;
        boolean anyRunning = false;

        for (TaskDescriptor task : tasks) {
            TaskState ts = task.getState();
            if (ts == TaskState.FAILED) {
                anyFailed = true;
            } else if (ts == TaskState.RUNNING) {
                anyRunning = true;
                allFinished = false;
            } else if (!ts.isTerminal()) {
                allFinished = false;
            }
        }

        if (anyFailed) {
            transitionTo(StageState.FAILED);
        } else if (allFinished) {
            transitionTo(StageState.FINISHED);
        } else if (anyRunning) {
            transitionTo(StageState.RUNNING);
        }
    }

    private boolean isValidTransition(StageState from, StageState to) {
        switch (from) {
            case PLANNED:
                return to == StageState.SCHEDULING || to == StageState.CANCELLED;
            case SCHEDULING:
                return to == StageState.RUNNING || to == StageState.FAILED
                    || to == StageState.CANCELLED;
            case RUNNING:
                return to == StageState.FINISHED || to == StageState.FAILED
                    || to == StageState.CANCELLED;
            default:
                return false;
        }
    }
}
