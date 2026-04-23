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
import org.opensearch.plugin.olap.execution.RfKind;
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

  // ---- Runtime filter serde ----

  public void testSerializeDeserializeTermsRuntimeFilter() throws IOException {
    ExecuteFragmentRequest original =
        new ExecuteFragmentRequest("q6", 0, 0, "{}", List.of(testShardId(0)), "probe");
    original.setRuntimeFilter("user_id", "keyword", List.of("a", "b", "c"));

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    ExecuteFragmentRequest deserialized = new ExecuteFragmentRequest(in);

    assertTrue(deserialized.hasRuntimeFilter());
    assertTrue(deserialized.hasTermsFilter());
    assertFalse(deserialized.hasBloomFilter());
    assertEquals(RfKind.TERMS, deserialized.getRfKind());
    assertEquals("user_id", deserialized.getRfFieldName());
    assertEquals("keyword", deserialized.getRfFieldType());
    assertEquals(List.of("a", "b", "c"), deserialized.getRfValues());
    assertNull(deserialized.getRfBloomBytes());
  }

  public void testSerializeDeserializeBloomRuntimeFilter() throws IOException {
    byte[] bloomBytes = new byte[] {1, 2, 3, 4, 5};
    ExecuteFragmentRequest original =
        new ExecuteFragmentRequest("q7", 0, 0, "{}", List.of(testShardId(0)), "probe");
    original.setBloomRuntimeFilter("user_id", "keyword", bloomBytes);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    ExecuteFragmentRequest deserialized = new ExecuteFragmentRequest(in);

    assertTrue(deserialized.hasRuntimeFilter());
    assertFalse(deserialized.hasTermsFilter());
    assertTrue(deserialized.hasBloomFilter());
    assertEquals(RfKind.BLOOM, deserialized.getRfKind());
    assertEquals("user_id", deserialized.getRfFieldName());
    assertEquals("keyword", deserialized.getRfFieldType());
    assertArrayEquals(bloomBytes, deserialized.getRfBloomBytes());
    assertNull(deserialized.getRfValues());
  }

  public void testSerializeDeserializeNoRuntimeFilter() throws IOException {
    ExecuteFragmentRequest original =
        new ExecuteFragmentRequest("q8", 0, 0, "{}", List.of(testShardId(0)), "probe");

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    ExecuteFragmentRequest deserialized = new ExecuteFragmentRequest(in);

    assertFalse(deserialized.hasRuntimeFilter());
    assertEquals(RfKind.NONE, deserialized.getRfKind());
    assertNull(deserialized.getRfBloomBytes());
    assertNull(deserialized.getRfValues());
  }

  // ---- Two-stage BLOOM build trailer serde ----

  public void testSerializeDeserializeBuildPartialBloom() throws IOException {
    ExecuteFragmentRequest original =
        new ExecuteFragmentRequest("q9", 0, 0, "{}", List.of(testShardId(0)), "build_idx");
    original.setBuildPartialBloom("user_id", "keyword", 1_000_000);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    ExecuteFragmentRequest deserialized = new ExecuteFragmentRequest(in);

    assertTrue(deserialized.shouldBuildPartialBloom());
    assertEquals("user_id", deserialized.getBuildBloomFieldName());
    assertEquals("keyword", deserialized.getBuildBloomFieldType());
    assertEquals(1_000_000, deserialized.getBuildBloomExpectedInsertions());
  }

  public void testSerializeDeserializeBuildPartialBloomAbsent() throws IOException {
    ExecuteFragmentRequest original =
        new ExecuteFragmentRequest("q10", 0, 0, "{}", List.of(testShardId(0)), "probe");

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    ExecuteFragmentRequest deserialized = new ExecuteFragmentRequest(in);

    assertFalse(deserialized.shouldBuildPartialBloom());
    assertNull(deserialized.getBuildBloomFieldName());
  }

  public void testSerializeDeserializeProfileEnabled() throws IOException {
    ExecuteFragmentRequest original =
        new ExecuteFragmentRequest("q11", 0, 0, "{}", List.of(testShardId(0)), "idx");
    original.setProfileEnabled(true);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    ExecuteFragmentRequest deserialized = new ExecuteFragmentRequest(in);

    assertTrue(deserialized.isProfileEnabled());
  }

  public void testSerializeDeserializeProfileDisabledByDefault() throws IOException {
    ExecuteFragmentRequest original =
        new ExecuteFragmentRequest("q12", 0, 0, "{}", List.of(testShardId(0)), "idx");

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    ExecuteFragmentRequest deserialized = new ExecuteFragmentRequest(in);

    assertFalse(deserialized.isProfileEnabled());
  }

  public void testSerializeDeserializeCoRoutingWithLeafPlans() throws IOException {
    // P1 regression guard: co-routing requests must carry left/right leaf plan JSONs so the
    // data-node handler can apply per-side pushdown. A round-trip asserts the wire format
    // actually preserves both sides' plans.
    ExecuteFragmentRequest original =
        new ExecuteFragmentRequest("q-co", 0, 0, "{\"coord\":true}", List.of(), null);
    ShardId leftShard = new ShardId(new Index("orders", "_na_"), 3);
    ShardId rightShard = new ShardId(new Index("customers", "_na_"), 3);
    original.setCoRoutingJoin("orders", "customers", leftShard, rightShard, 0);
    original.setCoRoutingLeafPlans("{\"left_leaf\":\"filter\"}", "{\"right_leaf\":\"project\"}");

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    ExecuteFragmentRequest deserialized = new ExecuteFragmentRequest(in);

    assertTrue(deserialized.isCoRoutingJoin());
    assertEquals("orders", deserialized.getCoRoutingLeftIndex());
    assertEquals("customers", deserialized.getCoRoutingRightIndex());
    assertEquals(3, deserialized.getCoRoutingLeftShardId().id());
    assertEquals(3, deserialized.getCoRoutingRightShardId().id());
    assertEquals(0, deserialized.getCoRoutingLeftScanIndex());
    assertEquals("{\"left_leaf\":\"filter\"}", deserialized.getCoRoutingLeftPlanJson());
    assertEquals("{\"right_leaf\":\"project\"}", deserialized.getCoRoutingRightPlanJson());
  }

  public void testSerializeDeserializeCoRoutingLeafPlansOptional() throws IOException {
    // Query with no leaf-side pushdown (full scan both sides) should still round-trip cleanly
    // with null leaf plan JSONs.
    ExecuteFragmentRequest original =
        new ExecuteFragmentRequest("q-co-null", 0, 0, "{}", List.of(), null);
    ShardId leftShard = new ShardId(new Index("a", "_na_"), 0);
    ShardId rightShard = new ShardId(new Index("b", "_na_"), 0);
    original.setCoRoutingJoin("a", "b", leftShard, rightShard, 0);
    // Leaf plans deliberately unset.

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    ExecuteFragmentRequest deserialized = new ExecuteFragmentRequest(out.bytes().streamInput());
    assertTrue(deserialized.isCoRoutingJoin());
    assertNull(deserialized.getCoRoutingLeftPlanJson());
    assertNull(deserialized.getCoRoutingRightPlanJson());
  }
}
