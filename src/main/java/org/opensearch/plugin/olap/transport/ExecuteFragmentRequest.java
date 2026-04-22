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
import org.opensearch.plugin.olap.execution.RfKind;

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

  /** Index of the build-side TableScanNode in the join plan (0=left, 1=right). Default 1. */
  private int broadcastBuildScanIndex = 1;

  // --- Runtime filter fields ---
  /**
   * Probe-side join key field name — populated for BOTH TERMS and BLOOM. Reconstructed on the
   * receiver from whichever trailer is present (terms values or bloom bytes).
   */
  private String rfFieldName;

  /** OpenSearch field type of the RF field ("keyword", "integer", "long"). */
  private String rfFieldType;

  /**
   * Distinct join key values for a TERMS runtime filter, serialized as strings. Null for BLOOM or
   * NONE.
   */
  private List<String> rfValues;

  /** Serialized bloom filter bytes for a BLOOM runtime filter. Null for TERMS or NONE. */
  private byte[] rfBloomBytes;

  /** Probe-side leaf fragment plan JSON for extracting pushdown query in broadcast join. */
  private String probePlanJson;

  // --- Two-stage BLOOM build fields (build-side request) ---
  /**
   * Column name in the build-side scan output to feed into a PARTIAL bloom. Non-null signals the
   * data node to build a partial bloom over this column and attach it to the response. Null skips
   * PARTIAL bloom construction (v1 single-stage path).
   */
  private String buildBloomFieldName;

  /**
   * Field type ("keyword"/"text"/"integer"/"long") used by OlapBloomFilter.encodeKey on both sides.
   */
  private String buildBloomFieldType;

  /**
   * Expected insertion count — must be identical across all data nodes in the same build, so the
   * partial blooms share sizing (bit count + hash count) and can be merged via bitwise-OR at the
   * coordinator.
   */
  private int buildBloomExpectedInsertions;

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
      this.broadcastBuildScanIndex = in.readVInt();
    }

    // Runtime filter (TERMS). rfFieldName is also shared with BLOOM — if fieldName is present
    // but no rfValues follow, this is a BLOOM request (the bloom trailer at the end of the
    // stream carries the bytes).
    this.rfFieldName = in.readOptionalString();
    if (this.rfFieldName != null) {
      this.rfFieldType = in.readString();
      boolean hasTerms = in.readBoolean();
      if (hasTerms) {
        int rfCount = in.readVInt();
        this.rfValues = new ArrayList<>(rfCount);
        for (int i = 0; i < rfCount; i++) {
          this.rfValues.add(in.readString());
        }
      }
    }

    // Probe plan JSON for broadcast pushdown
    this.probePlanJson = in.readOptionalString();

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

    // Bloom RF trailer: an optional byte array. Sender always emits the length-prefixed
    // byte array (zero-length means "no bloom"), so this is unconditional and cluster-wide
    // version-homogeneous because it ships as part of a single plugin deploy.
    this.rfBloomBytes = in.readByteArray();
    if (this.rfBloomBytes.length == 0) {
      this.rfBloomBytes = null;
    }

    // Two-stage BLOOM build trailer: fieldName/fieldType/expectedInsertions. Only populated
    // when the coordinator asks the data node to produce a PARTIAL bloom over the build output.
    this.buildBloomFieldName = in.readOptionalString();
    if (this.buildBloomFieldName != null) {
      this.buildBloomFieldType = in.readString();
      this.buildBloomExpectedInsertions = in.readVInt();
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
      out.writeVInt(broadcastBuildScanIndex);
    } else {
      out.writeVInt(0);
    }

    // Runtime filter (TERMS half of the block). rfFieldName/rfFieldType are shared with BLOOM;
    // rfValues are only present for TERMS. Bloom bytes ride the trailer at the end of the stream.
    out.writeOptionalString(rfFieldName);
    if (rfFieldName != null) {
      out.writeString(rfFieldType);
      boolean hasTerms = rfValues != null;
      out.writeBoolean(hasTerms);
      if (hasTerms) {
        out.writeVInt(rfValues.size());
        for (String v : rfValues) {
          out.writeString(v);
        }
      }
    }

    // Probe plan JSON for broadcast pushdown
    out.writeOptionalString(probePlanJson);

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

    // Bloom RF trailer: always write a (possibly empty) byte array — simpler than a separate
    // boolean tag, and the length prefix handles the "no bloom" case at zero overhead.
    out.writeByteArray(rfBloomBytes == null ? EMPTY_BYTES : rfBloomBytes);

    // Two-stage BLOOM build trailer.
    out.writeOptionalString(buildBloomFieldName);
    if (buildBloomFieldName != null) {
      out.writeString(buildBloomFieldType);
      out.writeVInt(buildBloomExpectedInsertions);
    }
  }

  private static final byte[] EMPTY_BYTES = new byte[0];

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

  public String getRfFieldName() {
    return rfFieldName;
  }

  public String getRfFieldType() {
    return rfFieldType;
  }

  public List<String> getRfValues() {
    return rfValues;
  }

  public byte[] getRfBloomBytes() {
    return rfBloomBytes;
  }

  public boolean hasBloomFilter() {
    return rfFieldName != null && rfBloomBytes != null && rfBloomBytes.length > 0;
  }

  public boolean hasTermsFilter() {
    return rfFieldName != null && rfValues != null && !rfValues.isEmpty();
  }

  public boolean hasRuntimeFilter() {
    return hasTermsFilter() || hasBloomFilter();
  }

  public RfKind getRfKind() {
    if (hasBloomFilter()) return RfKind.BLOOM;
    if (hasTermsFilter()) return RfKind.TERMS;
    return RfKind.NONE;
  }

  public String getProbePlanJson() {
    return probePlanJson;
  }

  public void setProbePlanJson(String probePlanJson) {
    this.probePlanJson = probePlanJson;
  }

  public void setRuntimeFilter(String fieldName, String fieldType, List<String> values) {
    this.rfFieldName = fieldName;
    this.rfFieldType = fieldType;
    this.rfValues = values;
    this.rfBloomBytes = null;
  }

  public void setBloomRuntimeFilter(String fieldName, String fieldType, byte[] bloomBytes) {
    this.rfFieldName = fieldName;
    this.rfFieldType = fieldType;
    this.rfValues = null;
    this.rfBloomBytes = bloomBytes;
  }

  public String getBuildBloomFieldName() {
    return buildBloomFieldName;
  }

  public String getBuildBloomFieldType() {
    return buildBloomFieldType;
  }

  public int getBuildBloomExpectedInsertions() {
    return buildBloomExpectedInsertions;
  }

  public boolean shouldBuildPartialBloom() {
    return buildBloomFieldName != null
        && buildBloomFieldType != null
        && buildBloomExpectedInsertions > 0;
  }

  public void setBuildPartialBloom(String fieldName, String fieldType, int expectedInsertions) {
    this.buildBloomFieldName = fieldName;
    this.buildBloomFieldType = fieldType;
    this.buildBloomExpectedInsertions = expectedInsertions;
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

  public int getBroadcastBuildScanIndex() {
    return broadcastBuildScanIndex;
  }

  public void setBroadcastData(List<byte[]> broadcastData) {
    this.broadcastData = broadcastData;
  }

  public void setBroadcastData(List<byte[]> broadcastData, int buildScanIndex) {
    this.broadcastData = broadcastData;
    this.broadcastBuildScanIndex = buildScanIndex;
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
