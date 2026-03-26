/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.transport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.shard.ShardId;

/**
 * Request to execute a Velox plan fragment on a data node.
 *
 * <p>Contains the serialized plan fragment (as JSON), the query ID, fragment ID, and the list of
 * shards this task should read from.
 *
 * <p>Modeled after Presto's TaskUpdateRequest which carries the PlanFragment, session properties,
 * and split assignments to worker nodes.
 */
public class ExecuteFragmentRequest extends ActionRequest {

  private String queryId;
  private int fragmentId;
  private int partitionId;
  private String planFragmentJson;
  private List<ShardId> shardIds;
  private String sourceIndex;

  public ExecuteFragmentRequest() {}

  public ExecuteFragmentRequest(StreamInput in) throws IOException {
    super(in);
    this.queryId = in.readString();
    this.fragmentId = in.readVInt();
    this.partitionId = in.readVInt();
    this.planFragmentJson = in.readString();
    int numShards = in.readVInt();
    this.shardIds = new ArrayList<>(numShards);
    for (int i = 0; i < numShards; i++) {
      this.shardIds.add(new ShardId(in));
    }
    this.sourceIndex = in.readOptionalString();
  }

  public ExecuteFragmentRequest(
      String queryId,
      int fragmentId,
      int partitionId,
      String planFragmentJson,
      List<ShardId> shardIds,
      String sourceIndex) {
    this.queryId = queryId;
    this.fragmentId = fragmentId;
    this.partitionId = partitionId;
    this.planFragmentJson = planFragmentJson;
    this.shardIds = shardIds;
    this.sourceIndex = sourceIndex;
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeString(queryId);
    out.writeVInt(fragmentId);
    out.writeVInt(partitionId);
    out.writeString(planFragmentJson);
    out.writeVInt(shardIds.size());
    for (ShardId shardId : shardIds) {
      shardId.writeTo(out);
    }
    out.writeOptionalString(sourceIndex);
  }

  @Override
  public ActionRequestValidationException validate() {
    ActionRequestValidationException errors = null;
    if (queryId == null || queryId.isEmpty()) {
      errors = new ActionRequestValidationException();
      errors.addValidationError("queryId is required");
    }
    if (planFragmentJson == null || planFragmentJson.isEmpty()) {
      if (errors == null) errors = new ActionRequestValidationException();
      errors.addValidationError("planFragmentJson is required");
    }
    return errors;
  }

  public String getQueryId() {
    return queryId;
  }

  public int getFragmentId() {
    return fragmentId;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public String getPlanFragmentJson() {
    return planFragmentJson;
  }

  public List<ShardId> getShardIds() {
    return shardIds;
  }

  public String getSourceIndex() {
    return sourceIndex;
  }
}
