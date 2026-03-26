/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.scheduler;

import java.util.Objects;
import org.opensearch.plugin.olap.common.QueryId;

public final class StageId {
  private final QueryId queryId;
  private final int stageNumber;

  public StageId(QueryId queryId, int stageNumber) {
    this.queryId = Objects.requireNonNull(queryId);
    this.stageNumber = stageNumber;
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public int getStageNumber() {
    return stageNumber;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof StageId)) return false;
    StageId other = (StageId) o;
    return stageNumber == other.stageNumber && queryId.equals(other.queryId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(queryId, stageNumber);
  }

  @Override
  public String toString() {
    return queryId + "." + stageNumber;
  }
}
