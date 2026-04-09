/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.transport;

import java.io.IOException;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/** Acknowledgement response for a shuffle data delivery. */
public class ShuffleDataResponse extends ActionResponse {

  private boolean success;

  public ShuffleDataResponse() {
    this.success = true;
  }

  public ShuffleDataResponse(StreamInput in) throws IOException {
    super(in);
    this.success = in.readBoolean();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeBoolean(success);
  }

  public boolean isSuccess() {
    return success;
  }
}
