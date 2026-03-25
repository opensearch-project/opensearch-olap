/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.plugin.olap.scheduler;

/**
 * Controls the execution ordering of stages in a multi-stage query.
 */
public enum ExecutionPolicy {
    /**
     * Launch all stages at once. Suitable for streaming/pipelined execution.
     */
    ALL_AT_ONCE,

    /**
     * Launch stages in dependency order, waiting for upstream stages to complete
     * before starting downstream stages. Suitable for batch execution.
     */
    PHASED
}
