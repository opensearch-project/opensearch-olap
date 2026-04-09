/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.transport;

import java.io.IOException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

public class ShuffleDataRequestTests extends OpenSearchTestCase {

  public void testSerializeDeserializeWithData() throws IOException {
    byte[] data = new byte[] {1, 2, 3, 4, 5};
    ShuffleDataRequest original = new ShuffleDataRequest("q1", 3, "left", data, false);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    ShuffleDataRequest deserialized = new ShuffleDataRequest(in);

    assertEquals("q1", deserialized.getQueryId());
    assertEquals(3, deserialized.getTargetStageId());
    assertEquals("left", deserialized.getSide());
    assertArrayEquals(data, deserialized.getData());
    assertFalse(deserialized.isLast());
  }

  public void testSerializeDeserializeLastWithNullData() throws IOException {
    ShuffleDataRequest original = new ShuffleDataRequest("q2", 5, "right", null, true);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    ShuffleDataRequest deserialized = new ShuffleDataRequest(in);

    assertEquals("q2", deserialized.getQueryId());
    assertEquals(5, deserialized.getTargetStageId());
    assertEquals("right", deserialized.getSide());
    assertNull(deserialized.getData());
    assertTrue(deserialized.isLast());
  }

  public void testSerializeDeserializeLastWithData() throws IOException {
    // Last batch can carry data too (final batch from sender)
    byte[] data = new byte[] {99};
    ShuffleDataRequest original = new ShuffleDataRequest("q3", 1, "left", data, true);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    ShuffleDataRequest deserialized = new ShuffleDataRequest(in);

    assertArrayEquals(data, deserialized.getData());
    assertTrue(deserialized.isLast());
  }

  public void testValidateReturnsNull() {
    ShuffleDataRequest request = new ShuffleDataRequest("q1", 0, "left", null, true);
    assertNull(request.validate());
  }
}
