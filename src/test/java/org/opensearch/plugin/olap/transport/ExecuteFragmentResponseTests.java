/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.transport;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

public class ExecuteFragmentResponseTests extends OpenSearchTestCase {

  public void testSuccessFactoryMethod() {
    ExecuteFragmentResponse response = ExecuteFragmentResponse.success(42, new byte[] {1, 2, 3});

    assertEquals(ExecuteFragmentResponse.Status.SUCCESS, response.getStatus());
    assertEquals(42, response.getRowCount());
    assertArrayEquals(new byte[] {1, 2, 3}, response.getResultData());
    assertNull(response.getErrorMessage());
    assertFalse(response.hasNativeResults());
  }

  public void testSuccessNativeFactoryMethod() {
    List<byte[]> batches = Arrays.asList(new byte[] {10, 11}, new byte[] {20, 21});
    ExecuteFragmentResponse response = ExecuteFragmentResponse.successNative(100, batches);

    assertEquals(ExecuteFragmentResponse.Status.SUCCESS, response.getStatus());
    assertEquals(100, response.getRowCount());
    assertNull(response.getResultData());
    assertTrue(response.hasNativeResults());
    assertEquals(2, response.getNativeResultBatches().size());
    assertArrayEquals(new byte[] {10, 11}, response.getNativeResultBatches().get(0));
    assertArrayEquals(new byte[] {20, 21}, response.getNativeResultBatches().get(1));
  }

  public void testFailureFactoryMethod() {
    ExecuteFragmentResponse response = ExecuteFragmentResponse.failure("out of memory");

    assertEquals(ExecuteFragmentResponse.Status.FAILURE, response.getStatus());
    assertEquals(0, response.getRowCount());
    assertNull(response.getResultData());
    assertEquals("out of memory", response.getErrorMessage());
    assertFalse(response.hasNativeResults());
  }

  public void testHasNativeResultsReturnsFalseWhenNull() {
    ExecuteFragmentResponse response = ExecuteFragmentResponse.success(0, null);
    assertFalse(response.hasNativeResults());
  }

  public void testHasNativeResultsReturnsFalseWhenEmpty() {
    ExecuteFragmentResponse response =
        ExecuteFragmentResponse.successNative(0, java.util.Collections.emptyList());
    assertFalse(response.hasNativeResults());
  }

  public void testSerializeDeserializeSuccess() throws IOException {
    ExecuteFragmentResponse original = ExecuteFragmentResponse.success(55, new byte[] {5, 6, 7});

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    ExecuteFragmentResponse deserialized = new ExecuteFragmentResponse(in);

    assertEquals(ExecuteFragmentResponse.Status.SUCCESS, deserialized.getStatus());
    assertEquals(55, deserialized.getRowCount());
    assertArrayEquals(new byte[] {5, 6, 7}, deserialized.getResultData());
    assertNull(deserialized.getErrorMessage());
    assertFalse(deserialized.hasNativeResults());
  }

  public void testSerializeDeserializeNullResultData() throws IOException {
    ExecuteFragmentResponse original = ExecuteFragmentResponse.success(0, null);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    ExecuteFragmentResponse deserialized = new ExecuteFragmentResponse(in);

    assertEquals(ExecuteFragmentResponse.Status.SUCCESS, deserialized.getStatus());
    assertNull(deserialized.getResultData());
  }

  public void testSerializeDeserializeNativeResultBatches() throws IOException {
    List<byte[]> batches = Arrays.asList(new byte[] {1, 2, 3}, new byte[] {4, 5, 6});
    ExecuteFragmentResponse original = ExecuteFragmentResponse.successNative(200, batches);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    ExecuteFragmentResponse deserialized = new ExecuteFragmentResponse(in);

    assertEquals(ExecuteFragmentResponse.Status.SUCCESS, deserialized.getStatus());
    assertEquals(200, deserialized.getRowCount());
    assertTrue(deserialized.hasNativeResults());
    assertEquals(2, deserialized.getNativeResultBatches().size());
    assertArrayEquals(new byte[] {1, 2, 3}, deserialized.getNativeResultBatches().get(0));
    assertArrayEquals(new byte[] {4, 5, 6}, deserialized.getNativeResultBatches().get(1));
  }

  public void testSerializeDeserializeFailure() throws IOException {
    ExecuteFragmentResponse original = ExecuteFragmentResponse.failure("connection timeout");

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    ExecuteFragmentResponse deserialized = new ExecuteFragmentResponse(in);

    assertEquals(ExecuteFragmentResponse.Status.FAILURE, deserialized.getStatus());
    assertEquals(0, deserialized.getRowCount());
    assertEquals("connection timeout", deserialized.getErrorMessage());
    assertNull(deserialized.getResultData());
  }

  public void testSerializeDeserializeNullErrorMessage() throws IOException {
    ExecuteFragmentResponse original = ExecuteFragmentResponse.success(10, new byte[] {0});

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    ExecuteFragmentResponse deserialized = new ExecuteFragmentResponse(in);

    assertNull(deserialized.getErrorMessage());
  }

  public void testStatusEnumValues() {
    assertEquals(2, ExecuteFragmentResponse.Status.values().length);
    assertEquals(
        ExecuteFragmentResponse.Status.SUCCESS, ExecuteFragmentResponse.Status.valueOf("SUCCESS"));
    assertEquals(
        ExecuteFragmentResponse.Status.FAILURE, ExecuteFragmentResponse.Status.valueOf("FAILURE"));
  }

  public void testLargeRowCount() throws IOException {
    long bigRowCount = Long.MAX_VALUE / 2;
    ExecuteFragmentResponse original = ExecuteFragmentResponse.success(bigRowCount, null);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    ExecuteFragmentResponse deserialized = new ExecuteFragmentResponse(in);
    assertEquals(bigRowCount, deserialized.getRowCount());
  }

  public void testSerializeDeserializePartialBloomBytes() throws IOException {
    ExecuteFragmentResponse original = ExecuteFragmentResponse.success(10, new byte[] {0});
    byte[] partialBloom = new byte[] {0x42, 0x43, 0x44, 0x45, 0x46};
    original.setPartialBloomBytes(partialBloom);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    ExecuteFragmentResponse deserialized = new ExecuteFragmentResponse(in);

    assertTrue(deserialized.hasPartialBloom());
    assertArrayEquals(partialBloom, deserialized.getPartialBloomBytes());
  }

  public void testSerializeDeserializeNoPartialBloom() throws IOException {
    ExecuteFragmentResponse original = ExecuteFragmentResponse.success(10, new byte[] {0});

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    ExecuteFragmentResponse deserialized = new ExecuteFragmentResponse(in);

    assertFalse(deserialized.hasPartialBloom());
    assertNull(deserialized.getPartialBloomBytes());
  }

  public void testSerializeDeserializeWithTaskProfile() throws IOException {
    ExecuteFragmentResponse original = ExecuteFragmentResponse.success(42, new byte[] {1});
    org.opensearch.plugin.olap.profile.OlapTaskProfile tp =
        new org.opensearch.plugin.olap.profile.OlapTaskProfile(
            2, 5, "data-3", 99_999_999L, 500L, 42L, 42L, "BLOOM", 256);
    original.setTaskProfile(tp);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    ExecuteFragmentResponse deserialized = new ExecuteFragmentResponse(in);

    assertTrue(deserialized.hasTaskProfile());
    assertEquals(tp, deserialized.getTaskProfile());
  }

  public void testSerializeDeserializeNoTaskProfile() throws IOException {
    ExecuteFragmentResponse original = ExecuteFragmentResponse.success(10, new byte[] {0});

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    ExecuteFragmentResponse deserialized = new ExecuteFragmentResponse(in);

    assertFalse(deserialized.hasTaskProfile());
    assertNull(deserialized.getTaskProfile());
  }
}
