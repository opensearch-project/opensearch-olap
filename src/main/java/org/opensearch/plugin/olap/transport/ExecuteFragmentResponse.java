/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.transport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * Response from executing a Velox plan fragment on a data node.
 *
 * <p>Contains the execution status, row count, and serialized result data. For leaf fragments, the
 * results are Arrow-serialized batches. For the root fragment, the results are the final query
 * output.
 */
public class ExecuteFragmentResponse extends ActionResponse implements ToXContentObject {

  public enum Status {
    SUCCESS,
    FAILURE
  }

  private Status status;
  private long rowCount;
  private byte[] resultData;

  /** Velox native serialized partial results (preserves intermediate accumulator state). */
  private List<byte[]> nativeResultBatches;

  private String errorMessage;

  public ExecuteFragmentResponse() {}

  public ExecuteFragmentResponse(StreamInput in) throws IOException {
    super(in);
    this.status = in.readEnum(Status.class);
    this.rowCount = in.readVLong();
    if (in.readBoolean()) {
      this.resultData = in.readByteArray();
    } else {
      this.resultData = null;
    }
    // Read native result batches
    int nativeBatchCount = in.readVInt();
    if (nativeBatchCount > 0) {
      this.nativeResultBatches = new ArrayList<>(nativeBatchCount);
      for (int i = 0; i < nativeBatchCount; i++) {
        this.nativeResultBatches.add(in.readByteArray());
      }
    }
    this.errorMessage = in.readOptionalString();
  }

  public ExecuteFragmentResponse(Status status, long rowCount, byte[] resultData) {
    this.status = status;
    this.rowCount = rowCount;
    this.resultData = resultData;
  }

  public static ExecuteFragmentResponse success(long rowCount, byte[] resultData) {
    return new ExecuteFragmentResponse(Status.SUCCESS, rowCount, resultData);
  }

  public static ExecuteFragmentResponse successNative(long rowCount, List<byte[]> nativeBatches) {
    ExecuteFragmentResponse response = new ExecuteFragmentResponse(Status.SUCCESS, rowCount, null);
    response.nativeResultBatches = nativeBatches;
    return response;
  }

  public static ExecuteFragmentResponse failure(String errorMessage) {
    ExecuteFragmentResponse response = new ExecuteFragmentResponse();
    response.status = Status.FAILURE;
    response.rowCount = 0;
    response.errorMessage = errorMessage;
    return response;
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeEnum(status);
    out.writeVLong(rowCount);
    if (resultData != null) {
      out.writeBoolean(true);
      out.writeByteArray(resultData);
    } else {
      out.writeBoolean(false);
    }
    // Write native result batches
    if (nativeResultBatches != null && !nativeResultBatches.isEmpty()) {
      out.writeVInt(nativeResultBatches.size());
      for (byte[] batch : nativeResultBatches) {
        out.writeByteArray(batch);
      }
    } else {
      out.writeVInt(0);
    }
    out.writeOptionalString(errorMessage);
  }

  @Override
  public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
    builder.startObject();
    builder.field("status", status.name());
    builder.field("row_count", rowCount);
    if (errorMessage != null) {
      builder.field("error", errorMessage);
    }
    builder.endObject();
    return builder;
  }

  public Status getStatus() {
    return status;
  }

  public long getRowCount() {
    return rowCount;
  }

  public byte[] getResultData() {
    return resultData;
  }

  public List<byte[]> getNativeResultBatches() {
    return nativeResultBatches;
  }

  public boolean hasNativeResults() {
    return nativeResultBatches != null && !nativeResultBatches.isEmpty();
  }

  public String getErrorMessage() {
    return errorMessage;
  }
}
