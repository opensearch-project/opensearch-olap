/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.plugin.olap.transport;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.action.ActionListener;
import org.opensearch.transport.TransportService;

import org.opensearch.plugin.olap.common.QueryId;
import org.opensearch.plugin.olap.scheduler.QueryExecution;
import org.opensearch.plugin.olap.scheduler.QueryScheduler;
import org.opensearch.plugin.olap.scheduler.TaskDescriptor;
import org.opensearch.plugin.olap.scheduler.TaskId;

/**
 * Dispatches fragment execution requests to data nodes and collects results.
 *
 * <p>This is the coordinator-side component that sends ExecuteFragmentRequest
 * messages to target data nodes via TransportService and aggregates responses.
 *
 * <p>Modeled after Presto's RemoteTaskFactory/HttpRemoteTask pattern, adapted
 * to use OpenSearch TransportService instead of HTTP.
 */
public class NodeResultCollector {

    private static final Logger logger = LogManager.getLogger(NodeResultCollector.class);
    private static final long DEFAULT_TIMEOUT_MS = 300_000; // 5 minutes

    private final TransportService transportService;
    private final QueryScheduler scheduler;

    public NodeResultCollector(TransportService transportService, QueryScheduler scheduler) {
        this.transportService = transportService;
        this.scheduler = scheduler;
    }

    /**
     * Dispatch all tasks for a query execution and collect results.
     */
    public List<ExecuteFragmentResponse> dispatchAndCollect(QueryExecution execution) {
        QueryId queryId = execution.getQueryId();
        List<TaskDescriptor> allTasks = new ArrayList<>();
        for (var stage : execution.getStages()) {
            allTasks.addAll(stage.getTasks());
        }

        CountDownLatch latch = new CountDownLatch(allTasks.size());
        List<ExecuteFragmentResponse> responses = new ArrayList<>(
            java.util.Collections.nCopies(allTasks.size(), null)
        );
        AtomicReference<Exception> firstError = new AtomicReference<>();

        for (int i = 0; i < allTasks.size(); i++) {
            final int index = i;
            TaskDescriptor task = allTasks.get(i);

            ExecuteFragmentRequest request = new ExecuteFragmentRequest(
                queryId.getId(),
                task.getFragment().getFragmentId(),
                task.getTaskId().getPartitionId(),
                serializePlanFragment(task),
                task.getShardIds(),
                task.getFragment().getProperties().getSourceIndex()
            );

            transportService.sendRequest(
                task.getTargetNode(),
                ExecuteFragmentAction.NAME,
                request,
                new org.opensearch.transport.TransportResponseHandler<ExecuteFragmentResponse>() {
                    @Override
                    public ExecuteFragmentResponse read(
                        org.opensearch.core.common.io.stream.StreamInput in
                    ) throws java.io.IOException {
                        return new ExecuteFragmentResponse(in);
                    }

                    @Override
                    public void handleResponse(ExecuteFragmentResponse response) {
                        responses.set(index, response);
                        if (response.getStatus() == ExecuteFragmentResponse.Status.SUCCESS) {
                            scheduler.onTaskCompleted(queryId, task.getTaskId());
                        } else {
                            scheduler.onTaskFailed(
                                queryId, task.getTaskId(), response.getErrorMessage()
                            );
                            firstError.compareAndSet(null,
                                new RuntimeException(response.getErrorMessage()));
                        }
                        latch.countDown();
                    }

                    @Override
                    public void handleException(
                        org.opensearch.transport.TransportException exp
                    ) {
                        logger.error("Transport error for task {}", task.getTaskId(), exp);
                        scheduler.onTaskFailed(
                            queryId, task.getTaskId(), exp.getMessage()
                        );
                        firstError.compareAndSet(null, exp);
                        latch.countDown();
                    }

                    @Override
                    public String executor() {
                        return org.opensearch.threadpool.ThreadPool.Names.SEARCH;
                    }
                }
            );
        }

        try {
            if (!latch.await(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                throw new RuntimeException(
                    "Timed out waiting for fragment execution for query " + queryId
                );
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted waiting for query " + queryId, e);
        }

        Exception error = firstError.get();
        if (error != null) {
            throw new RuntimeException("Query " + queryId + " failed", error);
        }

        return responses;
    }

    private String serializePlanFragment(TaskDescriptor task) {
        // Serialize the PlanNode tree to JSON for transmission.
        // velox4j PlanNode extends ISerializable which supports JSON serialization.
        // TODO: Use velox4j's Jackson ObjectMapper for serialization
        return "{}";
    }
}
