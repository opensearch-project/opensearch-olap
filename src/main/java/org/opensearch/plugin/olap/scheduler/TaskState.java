/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.plugin.olap.scheduler;

public enum TaskState {
    PENDING,
    RUNNING,
    FINISHED,
    FAILED,
    CANCELLED;

    public boolean isTerminal() {
        return this == FINISHED || this == FAILED || this == CANCELLED;
    }
}
