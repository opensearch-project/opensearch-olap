/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Handles incoming shuffle data on a worker node.
 *
 * <p>When a data node hash-partitions its scan output and sends a partition to this worker, this
 * action stores the data in the {@link ShuffleManager} buffer for later consumption by the join
 * task.
 */
public class TransportShuffleDataAction
    extends HandledTransportAction<ShuffleDataRequest, ShuffleDataResponse> {

  private static final Logger logger = LogManager.getLogger(TransportShuffleDataAction.class);

  private final ShuffleManager shuffleManager;

  @Inject
  public TransportShuffleDataAction(
      TransportService transportService,
      ActionFilters actionFilters,
      ShuffleManager shuffleManager) {
    super(ShuffleDataAction.NAME, transportService, actionFilters, ShuffleDataRequest::new);
    this.shuffleManager = shuffleManager;
  }

  @Override
  protected void doExecute(
      Task task, ShuffleDataRequest request, ActionListener<ShuffleDataResponse> listener) {
    try {
      ShuffleManager.ShuffleBuffer buffer =
          shuffleManager.getOrCreateBuffer(request.getQueryId(), request.getTargetStageId());

      if (request.getData() != null) {
        buffer.addData(request.getSide(), request.getData());
      }

      if (request.isLast()) {
        buffer.senderDone(request.getSide());
        logger.debug(
            "Shuffle sender done: query={}, stage={}, side={}",
            request.getQueryId(),
            request.getTargetStageId(),
            request.getSide());
      }

      listener.onResponse(new ShuffleDataResponse());
    } catch (Exception e) {
      logger.error("Failed to process shuffle data", e);
      listener.onFailure(e);
    }
  }
}
