/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.scheduler;

import java.util.Objects;

public final class TaskId {
  private final StageId stageId;
  private final int partitionId;

  public TaskId(StageId stageId, int partitionId) {
    this.stageId = Objects.requireNonNull(stageId);
    this.partitionId = partitionId;
  }

  public StageId getStageId() {
    return stageId;
  }

  public int getPartitionId() {
    return partitionId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof TaskId)) return false;
    TaskId other = (TaskId) o;
    return partitionId == other.partitionId && stageId.equals(other.stageId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(stageId, partitionId);
  }

  @Override
  public String toString() {
    return stageId + "." + partitionId;
  }
}
