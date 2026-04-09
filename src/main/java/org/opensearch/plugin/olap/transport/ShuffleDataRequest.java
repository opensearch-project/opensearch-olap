/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.transport;

import java.io.IOException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Request to deliver a partition of shuffle data to a target worker node.
 *
 * <p>During hash shuffle join, data nodes scan their local shards, hash-partition each batch by the
 * join key, and send each partition to the designated worker via this request.
 */
public class ShuffleDataRequest extends ActionRequest {

  private String queryId;
  private int targetStageId;
  private String side; // "left" or "right"
  private byte[] data; // Velox native serialized partition data (null if isLast with no data)
  private boolean isLast; // true = this sender is done for this side

  public ShuffleDataRequest() {}

  public ShuffleDataRequest(StreamInput in) throws IOException {
    super(in);
    this.queryId = in.readString();
    this.targetStageId = in.readVInt();
    this.side = in.readString();
    if (in.readBoolean()) {
      this.data = in.readByteArray();
    }
    this.isLast = in.readBoolean();
  }

  public ShuffleDataRequest(
      String queryId, int targetStageId, String side, byte[] data, boolean isLast) {
    this.queryId = queryId;
    this.targetStageId = targetStageId;
    this.side = side;
    this.data = data;
    this.isLast = isLast;
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeString(queryId);
    out.writeVInt(targetStageId);
    out.writeString(side);
    if (data != null) {
      out.writeBoolean(true);
      out.writeByteArray(data);
    } else {
      out.writeBoolean(false);
    }
    out.writeBoolean(isLast);
  }

  @Override
  public ActionRequestValidationException validate() {
    return null;
  }

  public String getQueryId() {
    return queryId;
  }

  public int getTargetStageId() {
    return targetStageId;
  }

  public String getSide() {
    return side;
  }

  public byte[] getData() {
    return data;
  }

  public boolean isLast() {
    return isLast;
  }
}
