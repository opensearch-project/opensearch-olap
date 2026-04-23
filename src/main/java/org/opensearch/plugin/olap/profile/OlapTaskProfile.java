/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.profile;

import java.io.IOException;
import java.util.Objects;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

/**
 * Per-task profiling record produced on a data node and carried back to the coordinator as an
 * optional trailer on {@link org.opensearch.plugin.olap.transport.ExecuteFragmentResponse}.
 *
 * <p>Populated only when the originating {@link
 * org.opensearch.plugin.olap.transport.ExecuteFragmentRequest} had {@code profileEnabled=true}.
 * Keeps the wire payload small (~60 bytes packed) so profiling can be collected across every task
 * without meaningful network overhead.
 *
 * <p>Counters are intentionally coarse-grained — enough to answer questions like "did the BLOOM RF
 * actually narrow the Lucene scan?" by comparing {@link #getDocsMatched()} across runs with RF
 * enabled vs disabled. Sub-operator-level timing belongs to a future iteration.
 *
 * <p>Wire format carries a leading version byte. The plugin ships with the OpenSearch release, so
 * both peers always speak the same version — bump {@link #WIRE_VERSION} whenever the layout
 * changes.
 */
public final class OlapTaskProfile implements Writeable {

  /** Current serialization version. */
  static final byte WIRE_VERSION = 2;

  private final int fragmentId;
  private final int partitionId;
  private final String nodeId;
  private final long durationNanos;
  private final long docsRead;
  private final long docsMatched;
  private final long rowsEmitted;
  private final String rfKind;
  private final int rfBloomBytes;
  private final long peakArrowBytes;
  private final long backpressureWaitNanos;
  private final long resultBytes;
  private final long shuffleRejectCount;

  public OlapTaskProfile(
      int fragmentId,
      int partitionId,
      String nodeId,
      long durationNanos,
      long docsRead,
      long docsMatched,
      long rowsEmitted,
      String rfKind,
      int rfBloomBytes) {
    this(
        fragmentId,
        partitionId,
        nodeId,
        durationNanos,
        docsRead,
        docsMatched,
        rowsEmitted,
        rfKind,
        rfBloomBytes,
        0L,
        0L,
        0L,
        0L);
  }

  public OlapTaskProfile(
      int fragmentId,
      int partitionId,
      String nodeId,
      long durationNanos,
      long docsRead,
      long docsMatched,
      long rowsEmitted,
      String rfKind,
      int rfBloomBytes,
      long peakArrowBytes,
      long backpressureWaitNanos,
      long resultBytes,
      long shuffleRejectCount) {
    this.fragmentId = fragmentId;
    this.partitionId = partitionId;
    this.nodeId = nodeId == null ? "" : nodeId;
    this.durationNanos = durationNanos;
    this.docsRead = docsRead;
    this.docsMatched = docsMatched;
    this.rowsEmitted = rowsEmitted;
    this.rfKind = rfKind == null ? "NONE" : rfKind;
    this.rfBloomBytes = rfBloomBytes;
    this.peakArrowBytes = peakArrowBytes;
    this.backpressureWaitNanos = backpressureWaitNanos;
    this.resultBytes = resultBytes;
    this.shuffleRejectCount = shuffleRejectCount;
  }

  public OlapTaskProfile(StreamInput in) throws IOException {
    byte version = in.readByte();
    if (version != WIRE_VERSION) {
      throw new IOException("Unsupported OlapTaskProfile wire version " + version);
    }
    this.fragmentId = in.readVInt();
    this.partitionId = in.readVInt();
    this.nodeId = in.readString();
    this.durationNanos = in.readVLong();
    this.docsRead = in.readVLong();
    this.docsMatched = in.readVLong();
    this.rowsEmitted = in.readVLong();
    this.rfKind = in.readString();
    this.rfBloomBytes = in.readVInt();
    this.peakArrowBytes = in.readVLong();
    this.backpressureWaitNanos = in.readVLong();
    this.resultBytes = in.readVLong();
    this.shuffleRejectCount = in.readVLong();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeByte(WIRE_VERSION);
    out.writeVInt(fragmentId);
    out.writeVInt(partitionId);
    out.writeString(nodeId);
    out.writeVLong(durationNanos);
    out.writeVLong(docsRead);
    out.writeVLong(docsMatched);
    out.writeVLong(rowsEmitted);
    out.writeString(rfKind);
    out.writeVInt(rfBloomBytes);
    out.writeVLong(peakArrowBytes);
    out.writeVLong(backpressureWaitNanos);
    out.writeVLong(resultBytes);
    out.writeVLong(shuffleRejectCount);
  }

  public int getFragmentId() {
    return fragmentId;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public String getNodeId() {
    return nodeId;
  }

  public long getDurationNanos() {
    return durationNanos;
  }

  public long getDocsRead() {
    return docsRead;
  }

  public long getDocsMatched() {
    return docsMatched;
  }

  public long getRowsEmitted() {
    return rowsEmitted;
  }

  public String getRfKind() {
    return rfKind;
  }

  public int getRfBloomBytes() {
    return rfBloomBytes;
  }

  public long getPeakArrowBytes() {
    return peakArrowBytes;
  }

  public long getBackpressureWaitNanos() {
    return backpressureWaitNanos;
  }

  public long getResultBytes() {
    return resultBytes;
  }

  public long getShuffleRejectCount() {
    return shuffleRejectCount;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof OlapTaskProfile)) return false;
    OlapTaskProfile that = (OlapTaskProfile) o;
    return fragmentId == that.fragmentId
        && partitionId == that.partitionId
        && durationNanos == that.durationNanos
        && docsRead == that.docsRead
        && docsMatched == that.docsMatched
        && rowsEmitted == that.rowsEmitted
        && rfBloomBytes == that.rfBloomBytes
        && peakArrowBytes == that.peakArrowBytes
        && backpressureWaitNanos == that.backpressureWaitNanos
        && resultBytes == that.resultBytes
        && shuffleRejectCount == that.shuffleRejectCount
        && nodeId.equals(that.nodeId)
        && rfKind.equals(that.rfKind);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        fragmentId,
        partitionId,
        nodeId,
        durationNanos,
        docsRead,
        docsMatched,
        rowsEmitted,
        rfKind,
        rfBloomBytes,
        peakArrowBytes,
        backpressureWaitNanos,
        resultBytes,
        shuffleRejectCount);
  }

  @Override
  public String toString() {
    return "OlapTaskProfile{frag="
        + fragmentId
        + ",part="
        + partitionId
        + ",node="
        + nodeId
        + ",durNs="
        + durationNanos
        + ",docsRead="
        + docsRead
        + ",docsMatched="
        + docsMatched
        + ",rows="
        + rowsEmitted
        + ",rf="
        + rfKind
        + ",bloomBytes="
        + rfBloomBytes
        + ",peakArrow="
        + peakArrowBytes
        + ",bpWaitNs="
        + backpressureWaitNanos
        + ",resultBytes="
        + resultBytes
        + ",shuffleRejects="
        + shuffleRejectCount
        + "}";
  }
}
