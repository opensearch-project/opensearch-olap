/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.transport;

import java.io.IOException;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Acknowledgement response for a shuffle data delivery.
 *
 * <p>When {@link #isBackpressure()} is true the receiver did not accept the payload — its
 * per-partition shuffle buffer exceeded {@code plugins.velox.shuffle_buffer_bytes}. The sender
 * should retry with exponential backoff rather than treat this as a fatal failure.
 */
public class ShuffleDataResponse extends ActionResponse {

  private boolean success;
  private boolean backpressure;

  public ShuffleDataResponse() {
    this(true, false);
  }

  public ShuffleDataResponse(boolean success, boolean backpressure) {
    this.success = success;
    this.backpressure = backpressure;
  }

  public ShuffleDataResponse(StreamInput in) throws IOException {
    super(in);
    this.success = in.readBoolean();
    this.backpressure = in.readBoolean();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeBoolean(success);
    out.writeBoolean(backpressure);
  }

  public boolean isSuccess() {
    return success;
  }

  public boolean isBackpressure() {
    return backpressure;
  }

  public static ShuffleDataResponse backpressureReject() {
    return new ShuffleDataResponse(false, true);
  }
}
