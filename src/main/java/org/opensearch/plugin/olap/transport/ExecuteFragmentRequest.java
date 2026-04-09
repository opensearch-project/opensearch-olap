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
 * <p>Extended for MPP join support:
 *
 * <ul>
 *   <li><b>Broadcast join</b>: {@code broadcastData} carries serialized build-side batches
 *   <li><b>Shuffle scan</b>: {@code shuffleTargetNodeIds}, {@code shuffleKeyChannels} etc. tell the
 *       data node how to partition and where to send shuffle data
 *   <li><b>Shuffle join</b>: {@code shuffleJoinQueryId}, {@code shuffleJoinStageId} tell the worker
 *       to read from ShuffleManager
 * </ul>
 */
public class ExecuteFragmentRequest extends ActionRequest {

  private String queryId;
  private int fragmentId;
  private int partitionId;
  private String planFragmentJson;
  private List<ShardId> shardIds;
  private String sourceIndex;

  // --- Broadcast join fields ---
  /** Velox native serialized build-side batches for broadcast join. Null if not broadcast. */
  private List<byte[]> broadcastData;

  // --- Shuffle scan fields ---
  /** Target node IDs for each shuffle partition. Null if not a shuffle scan. */
  private List<String> shuffleTargetNodeIds;

  /** Column indices in the scan output to hash-partition by. */
  private List<Integer> shuffleKeyChannels;

  /** Which side of the join this shuffle scan feeds ("left" or "right"). */
  private String shuffleSide;

  /** The target join stage ID that will consume the shuffled data. */
  private int shuffleTargetStageId;

  /** Number of shuffle partitions. */
  private int shuffleNumPartitions;

  // --- Shuffle join fields ---
  /** Query ID to look up ShuffleManager buffer. Null if not a shuffle join. */
  private String shuffleJoinQueryId;

  /** Stage ID to look up ShuffleManager buffer. */
  private int shuffleJoinStageId;

  /** Expected number of left-side senders for this shuffle join. */
  private int expectedLeftSenders;

  /** Expected number of right-side senders for this shuffle join. */
  private int expectedRightSenders;

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

    // Broadcast data
    int broadcastCount = in.readVInt();
    if (broadcastCount > 0) {
      this.broadcastData = new ArrayList<>(broadcastCount);
      for (int i = 0; i < broadcastCount; i++) {
        this.broadcastData.add(in.readByteArray());
      }
    }

    // Shuffle scan config
    int targetNodeCount = in.readVInt();
    if (targetNodeCount > 0) {
      this.shuffleTargetNodeIds = new ArrayList<>(targetNodeCount);
      for (int i = 0; i < targetNodeCount; i++) {
        this.shuffleTargetNodeIds.add(in.readString());
      }
      int keyChannelCount = in.readVInt();
      this.shuffleKeyChannels = new ArrayList<>(keyChannelCount);
      for (int i = 0; i < keyChannelCount; i++) {
        this.shuffleKeyChannels.add(in.readVInt());
      }
      this.shuffleSide = in.readString();
      this.shuffleTargetStageId = in.readVInt();
      this.shuffleNumPartitions = in.readVInt();
    }

    // Shuffle join config
    this.shuffleJoinQueryId = in.readOptionalString();
    if (this.shuffleJoinQueryId != null) {
      this.shuffleJoinStageId = in.readVInt();
      this.expectedLeftSenders = in.readVInt();
      this.expectedRightSenders = in.readVInt();
    }
  }

  /** Constructor for normal scan requests (backward compatible). */
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

    // Broadcast data
    if (broadcastData != null && !broadcastData.isEmpty()) {
      out.writeVInt(broadcastData.size());
      for (byte[] batch : broadcastData) {
        out.writeByteArray(batch);
      }
    } else {
      out.writeVInt(0);
    }

    // Shuffle scan config
    if (shuffleTargetNodeIds != null && !shuffleTargetNodeIds.isEmpty()) {
      out.writeVInt(shuffleTargetNodeIds.size());
      for (String nodeId : shuffleTargetNodeIds) {
        out.writeString(nodeId);
      }
      out.writeVInt(shuffleKeyChannels.size());
      for (int ch : shuffleKeyChannels) {
        out.writeVInt(ch);
      }
      out.writeString(shuffleSide);
      out.writeVInt(shuffleTargetStageId);
      out.writeVInt(shuffleNumPartitions);
    } else {
      out.writeVInt(0);
    }

    // Shuffle join config
    out.writeOptionalString(shuffleJoinQueryId);
    if (shuffleJoinQueryId != null) {
      out.writeVInt(shuffleJoinStageId);
      out.writeVInt(expectedLeftSenders);
      out.writeVInt(expectedRightSenders);
    }
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

  // --- Getters ---

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

  public List<byte[]> getBroadcastData() {
    return broadcastData;
  }

  public boolean hasBroadcastData() {
    return broadcastData != null && !broadcastData.isEmpty();
  }

  public List<String> getShuffleTargetNodeIds() {
    return shuffleTargetNodeIds;
  }

  public List<Integer> getShuffleKeyChannels() {
    return shuffleKeyChannels;
  }

  public String getShuffleSide() {
    return shuffleSide;
  }

  public int getShuffleTargetStageId() {
    return shuffleTargetStageId;
  }

  public int getShuffleNumPartitions() {
    return shuffleNumPartitions;
  }

  public boolean isShuffleScan() {
    return shuffleTargetNodeIds != null && !shuffleTargetNodeIds.isEmpty();
  }

  public String getShuffleJoinQueryId() {
    return shuffleJoinQueryId;
  }

  public int getShuffleJoinStageId() {
    return shuffleJoinStageId;
  }

  public int getExpectedLeftSenders() {
    return expectedLeftSenders;
  }

  public int getExpectedRightSenders() {
    return expectedRightSenders;
  }

  public boolean isShuffleJoin() {
    return shuffleJoinQueryId != null;
  }

  // --- Setters for builder-style construction ---

  public void setBroadcastData(List<byte[]> broadcastData) {
    this.broadcastData = broadcastData;
  }

  public void setShuffleConfig(
      List<String> targetNodeIds,
      List<Integer> keyChannels,
      String side,
      int targetStageId,
      int numPartitions) {
    this.shuffleTargetNodeIds = targetNodeIds;
    this.shuffleKeyChannels = keyChannels;
    this.shuffleSide = side;
    this.shuffleTargetStageId = targetStageId;
    this.shuffleNumPartitions = numPartitions;
  }

  public void setShuffleJoinConfig(
      String shuffleQueryId, int stageId, int leftSenders, int rightSenders) {
    this.shuffleJoinQueryId = shuffleQueryId;
    this.shuffleJoinStageId = stageId;
    this.expectedLeftSenders = leftSenders;
    this.expectedRightSenders = rightSenders;
  }
}
