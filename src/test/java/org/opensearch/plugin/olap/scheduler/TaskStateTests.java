/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.scheduler;

import org.opensearch.test.OpenSearchTestCase;

public class TaskStateTests extends OpenSearchTestCase {

  public void testTerminalStates() {
    assertTrue(TaskState.FINISHED.isTerminal());
    assertTrue(TaskState.FAILED.isTerminal());
    assertTrue(TaskState.CANCELLED.isTerminal());
  }

  public void testNonTerminalStates() {
    assertFalse(TaskState.PENDING.isTerminal());
    assertFalse(TaskState.RUNNING.isTerminal());
    assertFalse(TaskState.RETRYING.isTerminal());
  }

  public void testAllStatesAreDefined() {
    TaskState[] values = TaskState.values();
    assertEquals(6, values.length);
  }
}
