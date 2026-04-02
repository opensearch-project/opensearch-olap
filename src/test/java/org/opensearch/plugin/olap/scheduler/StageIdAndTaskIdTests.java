/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.scheduler;

import org.opensearch.plugin.olap.common.QueryId;
import org.opensearch.test.OpenSearchTestCase;

public class StageIdAndTaskIdTests extends OpenSearchTestCase {

  // -------------------------------------------------------------------
  // StageId tests
  // -------------------------------------------------------------------

  public void testStageIdGetters() {
    QueryId qid = QueryId.of("q1");
    StageId sid = new StageId(qid, 2);
    assertEquals(qid, sid.getQueryId());
    assertEquals(2, sid.getStageNumber());
  }

  public void testStageIdToString() {
    QueryId qid = QueryId.of("q1");
    StageId sid = new StageId(qid, 3);
    assertEquals("q1.3", sid.toString());
  }

  public void testStageIdEquality() {
    QueryId qid = QueryId.of("q1");
    StageId a = new StageId(qid, 0);
    StageId b = new StageId(qid, 0);
    StageId c = new StageId(qid, 1);

    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertNotEquals(a, c);
  }

  public void testStageIdNullQueryIdThrows() {
    expectThrows(NullPointerException.class, () -> new StageId(null, 0));
  }

  public void testStageIdEqualsDifferentQuery() {
    StageId a = new StageId(QueryId.of("q1"), 0);
    StageId b = new StageId(QueryId.of("q2"), 0);
    assertNotEquals(a, b);
  }

  // -------------------------------------------------------------------
  // TaskId tests
  // -------------------------------------------------------------------

  public void testTaskIdGetters() {
    QueryId qid = QueryId.of("q1");
    StageId sid = new StageId(qid, 1);
    TaskId tid = new TaskId(sid, 5);
    assertEquals(sid, tid.getStageId());
    assertEquals(5, tid.getPartitionId());
  }

  public void testTaskIdToString() {
    QueryId qid = QueryId.of("q1");
    StageId sid = new StageId(qid, 1);
    TaskId tid = new TaskId(sid, 5);
    assertEquals("q1.1.5", tid.toString());
  }

  public void testTaskIdEquality() {
    QueryId qid = QueryId.of("q1");
    StageId sid = new StageId(qid, 1);
    TaskId a = new TaskId(sid, 0);
    TaskId b = new TaskId(sid, 0);
    TaskId c = new TaskId(sid, 1);

    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertNotEquals(a, c);
  }

  public void testTaskIdNullStageIdThrows() {
    expectThrows(NullPointerException.class, () -> new TaskId(null, 0));
  }

  public void testTaskIdEqualsDifferentStage() {
    QueryId qid = QueryId.of("q1");
    StageId s1 = new StageId(qid, 0);
    StageId s2 = new StageId(qid, 1);
    TaskId a = new TaskId(s1, 0);
    TaskId b = new TaskId(s2, 0);
    assertNotEquals(a, b);
  }

  public void testTaskIdHashCodeConsistency() {
    QueryId qid = QueryId.of("q1");
    StageId sid = new StageId(qid, 1);
    TaskId tid = new TaskId(sid, 3);
    // hashCode should be consistent across multiple calls
    assertEquals(tid.hashCode(), tid.hashCode());
  }
}
