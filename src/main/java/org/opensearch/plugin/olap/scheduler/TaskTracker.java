/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.plugin.olap.scheduler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks task states and results across a query execution.
 */
public class TaskTracker {

    private final Map<TaskId, TaskDescriptor> tasks = new ConcurrentHashMap<>();

    public void register(TaskDescriptor task) {
        tasks.put(task.getTaskId(), task);
    }

    public TaskDescriptor get(TaskId taskId) {
        return tasks.get(taskId);
    }

    public void updateState(TaskId taskId, TaskState newState) {
        TaskDescriptor task = tasks.get(taskId);
        if (task != null) {
            task.setState(newState);
        }
    }

    public void markFailed(TaskId taskId, String reason) {
        TaskDescriptor task = tasks.get(taskId);
        if (task != null) {
            task.setState(TaskState.FAILED);
            task.setFailureReason(reason);
        }
    }

    public boolean allTerminal() {
        return tasks.values().stream().allMatch(t -> t.getState().isTerminal());
    }

    public boolean anyFailed() {
        return tasks.values().stream().anyMatch(t -> t.getState() == TaskState.FAILED);
    }

    public long countByState(TaskState state) {
        return tasks.values().stream().filter(t -> t.getState() == state).count();
    }

    public void cancelAll() {
        for (TaskDescriptor task : tasks.values()) {
            if (!task.getState().isTerminal()) {
                task.setState(TaskState.CANCELLED);
            }
        }
    }
}
