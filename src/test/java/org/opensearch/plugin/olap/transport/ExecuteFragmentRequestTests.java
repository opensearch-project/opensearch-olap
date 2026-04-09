/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.transport;

import java.io.IOException;
import java.util.List;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

public class ExecuteFragmentRequestTests extends OpenSearchTestCase {

  private ShardId testShardId(int id) {
    return new ShardId(new Index("test-index", "_na_"), id);
  }

  public void testSerializeDeserializeBasic() throws IOException {
    ExecuteFragmentRequest original =
        new ExecuteFragmentRequest("q1", 0, 1, "{\"plan\":true}", List.of(testShardId(0)), "idx");

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    ExecuteFragmentRequest deserialized = new ExecuteFragmentRequest(in);

    assertEquals("q1", deserialized.getQueryId());
    assertEquals(0, deserialized.getFragmentId());
    assertEquals(1, deserialized.getPartitionId());
    assertEquals("{\"plan\":true}", deserialized.getPlanFragmentJson());
    assertEquals(1, deserialized.getShardIds().size());
    assertEquals("idx", deserialized.getSourceIndex());
    assertFalse(deserialized.hasBroadcastData());
    assertFalse(deserialized.isShuffleScan());
    assertFalse(deserialized.isShuffleJoin());
  }

  public void testSerializeDeserializeBroadcastData() throws IOException {
    ExecuteFragmentRequest original =
        new ExecuteFragmentRequest("q2", 1, 0, "{}", List.of(testShardId(0)), "probe");
    original.setBroadcastData(List.of(new byte[] {10, 20}, new byte[] {30, 40, 50}));

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    ExecuteFragmentRequest deserialized = new ExecuteFragmentRequest(in);

    assertTrue(deserialized.hasBroadcastData());
    assertEquals(2, deserialized.getBroadcastData().size());
    assertArrayEquals(new byte[] {10, 20}, deserialized.getBroadcastData().get(0));
    assertArrayEquals(new byte[] {30, 40, 50}, deserialized.getBroadcastData().get(1));
  }

  public void testSerializeDeserializeShuffleConfig() throws IOException {
    ExecuteFragmentRequest original =
        new ExecuteFragmentRequest("q3", 0, 0, "{}", List.of(testShardId(0)), "left_idx");
    original.setShuffleConfig(List.of("node-1", "node-2", "node-3"), List.of(0, 2), "left", 5, 3);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    ExecuteFragmentRequest deserialized = new ExecuteFragmentRequest(in);

    assertTrue(deserialized.isShuffleScan());
    assertEquals(List.of("node-1", "node-2", "node-3"), deserialized.getShuffleTargetNodeIds());
    assertEquals(List.of(0, 2), deserialized.getShuffleKeyChannels());
    assertEquals("left", deserialized.getShuffleSide());
    assertEquals(5, deserialized.getShuffleTargetStageId());
    assertEquals(3, deserialized.getShuffleNumPartitions());
  }

  public void testSerializeDeserializeShuffleJoinConfig() throws IOException {
    ExecuteFragmentRequest original = new ExecuteFragmentRequest("q4", 2, 0, "{}", List.of(), null);
    original.setShuffleJoinConfig("shuffle-q4", 2, 3, 2);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    ExecuteFragmentRequest deserialized = new ExecuteFragmentRequest(in);

    assertTrue(deserialized.isShuffleJoin());
    assertEquals("shuffle-q4", deserialized.getShuffleJoinQueryId());
    assertEquals(2, deserialized.getShuffleJoinStageId());
    assertEquals(3, deserialized.getExpectedLeftSenders());
    assertEquals(2, deserialized.getExpectedRightSenders());
  }

  public void testSerializeDeserializeAllFieldsEmpty() throws IOException {
    ExecuteFragmentRequest original = new ExecuteFragmentRequest("q5", 0, 0, "{}", List.of(), null);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    ExecuteFragmentRequest deserialized = new ExecuteFragmentRequest(in);

    assertFalse(deserialized.hasBroadcastData());
    assertFalse(deserialized.isShuffleScan());
    assertFalse(deserialized.isShuffleJoin());
  }

  public void testValidateMissingQueryId() {
    ExecuteFragmentRequest request = new ExecuteFragmentRequest(null, 0, 0, "{}", List.of(), null);
    assertNotNull(request.validate());
  }

  public void testValidateMissingPlanJson() {
    ExecuteFragmentRequest request = new ExecuteFragmentRequest("q1", 0, 0, null, List.of(), null);
    assertNotNull(request.validate());
  }

  public void testValidateHappyPath() {
    ExecuteFragmentRequest request = new ExecuteFragmentRequest("q1", 0, 0, "{}", List.of(), null);
    assertNull(request.validate());
  }
}
