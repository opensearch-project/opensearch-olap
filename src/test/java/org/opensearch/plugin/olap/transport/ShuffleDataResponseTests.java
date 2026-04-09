/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.transport;

import java.io.IOException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

public class ShuffleDataResponseTests extends OpenSearchTestCase {

  public void testDefaultIsSuccess() {
    ShuffleDataResponse response = new ShuffleDataResponse();
    assertTrue(response.isSuccess());
  }

  public void testSerializeDeserialize() throws IOException {
    ShuffleDataResponse original = new ShuffleDataResponse();

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    ShuffleDataResponse deserialized = new ShuffleDataResponse(in);

    assertTrue(deserialized.isSuccess());
  }
}
