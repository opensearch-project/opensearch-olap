/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.transport;

import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugin.olap.execution.ExternalStreamBridge;
import org.opensearch.plugin.olap.execution.LuceneArrowReader;
import org.opensearch.plugin.olap.execution.VeloxExecutor;
import org.opensearch.plugin.olap.execution.VeloxLifecycleService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Handles execution of a Velox plan fragment on a data node.
 *
 * <p>This is the core data-node handler. When the coordinator dispatches a fragment to this node,
 * the handler:
 *
 * <ol>
 *   <li>Deserializes the Velox plan from JSON
 *   <li>Acquires Lucene searchers for the assigned shards
 *   <li>Reads doc values into Arrow batches via LuceneArrowReader
 *   <li>Feeds Arrow batches into velox4j ExternalStream
 *   <li>Executes the Velox plan via velox4j QueryExecutor
 *   <li>Serializes results and sends response back to coordinator
 * </ol>
 *
 * <p>Inspired by Presto's TaskResource/SqlTaskExecution which receives plan fragments and creates
 * Velox tasks. Key difference: we read data from Lucene instead of Hive splits, feeding it through
 * ExternalStream.
 */
public class TransportExecuteFragmentAction
    extends HandledTransportAction<ExecuteFragmentRequest, ExecuteFragmentResponse> {

  private static final Logger logger = LogManager.getLogger(TransportExecuteFragmentAction.class);

  private final IndicesService indicesService;
  private final ThreadPool threadPool;
  private final VeloxLifecycleService veloxLifecycle;

  @Inject
  public TransportExecuteFragmentAction(
      TransportService transportService,
      ActionFilters actionFilters,
      IndicesService indicesService,
      ThreadPool threadPool,
      VeloxLifecycleService veloxLifecycle) {
    super(ExecuteFragmentAction.NAME, transportService, actionFilters, ExecuteFragmentRequest::new);
    this.indicesService = indicesService;
    this.threadPool = threadPool;
    this.veloxLifecycle = veloxLifecycle;
  }

  @Override
  protected void doExecute(
      Task task, ExecuteFragmentRequest request, ActionListener<ExecuteFragmentResponse> listener) {
    // Execute on the SEARCH thread pool to avoid blocking transport threads
    threadPool
        .executor(ThreadPool.Names.SEARCH)
        .execute(
            () -> {
              try {
                ExecuteFragmentResponse response = executeFragment(request);
                listener.onResponse(response);
              } catch (Exception e) {
                logger.error(
                    "Failed to execute fragment {} for query {}",
                    request.getFragmentId(),
                    request.getQueryId(),
                    e);
                listener.onResponse(ExecuteFragmentResponse.failure(e.getMessage()));
              }
            });
  }

  private ExecuteFragmentResponse executeFragment(ExecuteFragmentRequest request) {
    String queryId = request.getQueryId();
    List<ShardId> shardIds = request.getShardIds();

    logger.info(
        "Executing fragment {} for query {} on {} shards",
        request.getFragmentId(),
        queryId,
        shardIds.size());

    // Step 1: Create the Velox executor with a session from the lifecycle service
    VeloxExecutor executor = new VeloxExecutor(veloxLifecycle.getSession());

    // Step 2: Create the bridge that connects Lucene data to Velox
    ExternalStreamBridge bridge = new ExternalStreamBridge(veloxLifecycle.getSession());

    // Step 3: Read Lucene doc values and feed into ExternalStream
    LuceneArrowReader reader = new LuceneArrowReader(indicesService);
    long rowCount = 0;

    try {
      // Create the ExternalStream and get its connector ID for the plan
      bridge.open();

      // Start a background thread to feed data from Lucene into ExternalStream
      Thread feederThread =
          new Thread(
              () -> {
                try {
                  for (ShardId shardId : shardIds) {
                    reader.readShardIntoStream(shardId, request.getSourceIndex(), bridge);
                  }
                  bridge.noMoreInput();
                } catch (Exception e) {
                  logger.error("Error feeding data for query {}", queryId, e);
                  bridge.abort(e);
                }
              },
              "olap-feeder-" + queryId + "-" + request.getFragmentId());
      feederThread.setDaemon(true);
      feederThread.start();

      // Step 4: Execute the Velox plan, reading from the ExternalStream
      byte[] resultData = executor.execute(request.getPlanFragmentJson(), bridge.getConnectorId());

      // Wait for feeder to finish
      feederThread.join(30_000);

      rowCount = bridge.getRowCount();

      return ExecuteFragmentResponse.success(rowCount, resultData);

    } catch (Exception e) {
      logger.error("Fragment execution failed for query {}", queryId, e);
      return ExecuteFragmentResponse.failure(e.getMessage());
    } finally {
      bridge.close();
    }
  }
}
