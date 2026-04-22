/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.profile;

import java.io.IOException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

public class OlapTaskProfileTests extends OpenSearchTestCase {

  public void testRoundTrip() throws IOException {
    OlapTaskProfile original =
        new OlapTaskProfile(
            /* fragmentId */ 3,
            /* partitionId */ 1,
            /* nodeId */ "data-1",
            /* durationNanos */ 12_345_678L,
            /* docsRead */ 10_000L,
            /* docsMatched */ 120L,
            /* rowsEmitted */ 120L,
            /* rfKind */ "BLOOM",
            /* rfBloomBytes */ 512);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    OlapTaskProfile decoded = new OlapTaskProfile(in);

    assertEquals(original, decoded);
    assertEquals("BLOOM", decoded.getRfKind());
    assertEquals(120L, decoded.getDocsMatched());
    assertEquals(512, decoded.getRfBloomBytes());
  }

  public void testNullsAreNormalized() throws IOException {
    OlapTaskProfile p = new OlapTaskProfile(0, 0, null, 1L, 0L, 0L, 0L, null, 0);
    BytesStreamOutput out = new BytesStreamOutput();
    p.writeTo(out);
    OlapTaskProfile decoded = new OlapTaskProfile(out.bytes().streamInput());
    assertEquals("", decoded.getNodeId());
    assertEquals("NONE", decoded.getRfKind());
  }

  public void testRejectsBadVersion() throws IOException {
    BytesStreamOutput out = new BytesStreamOutput();
    out.writeByte((byte) 99);
    expectThrows(IOException.class, () -> new OlapTaskProfile(out.bytes().streamInput()));
  }
}
