/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.plugin.olap.transport;

import org.opensearch.action.ActionType;

/**
 * Action definition for executing a Velox plan fragment on a data node.
 *
 * <p>This action is registered by the plugin and serves two purposes:
 * <ol>
 *   <li>Client-facing: REST → TransportExecuteFragmentAction on coordinator</li>
 *   <li>Inter-node: Coordinator dispatches fragments to data nodes via TransportService</li>
 * </ol>
 */
public class ExecuteFragmentAction extends ActionType<ExecuteFragmentResponse> {

    public static final String NAME = "indices:data/read/olap/execute_fragment";
    public static final ExecuteFragmentAction INSTANCE = new ExecuteFragmentAction();

    private ExecuteFragmentAction() {
        super(NAME, ExecuteFragmentResponse::new);
    }
}
