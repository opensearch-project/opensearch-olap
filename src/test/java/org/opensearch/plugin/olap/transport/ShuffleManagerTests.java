/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.transport;

import org.opensearch.test.OpenSearchTestCase;

public class ShuffleManagerTests extends OpenSearchTestCase {

  public void testGetOrCreateBufferCreatesNew() {
    ShuffleManager manager = new ShuffleManager();
    ShuffleManager.ShuffleBuffer buffer = manager.getOrCreateBuffer("q1", 2);
    assertNotNull(buffer);
  }

  public void testGetOrCreateBufferReturnsSameInstance() {
    ShuffleManager manager = new ShuffleManager();
    ShuffleManager.ShuffleBuffer b1 = manager.getOrCreateBuffer("q1", 2);
    ShuffleManager.ShuffleBuffer b2 = manager.getOrCreateBuffer("q1", 2);
    assertSame(b1, b2);
  }

  public void testDifferentKeysReturnDifferentBuffers() {
    ShuffleManager manager = new ShuffleManager();
    ShuffleManager.ShuffleBuffer b1 = manager.getOrCreateBuffer("q1", 0);
    ShuffleManager.ShuffleBuffer b2 = manager.getOrCreateBuffer("q2", 0);
    assertNotSame(b1, b2);
  }

  public void testGetBufferReturnsNullForMissing() {
    ShuffleManager manager = new ShuffleManager();
    assertNull(manager.getBuffer("q1", 0));
  }

  public void testGetBufferReturnsExisting() {
    ShuffleManager manager = new ShuffleManager();
    ShuffleManager.ShuffleBuffer created = manager.getOrCreateBuffer("q1", 1);
    ShuffleManager.ShuffleBuffer retrieved = manager.getBuffer("q1", 1);
    assertSame(created, retrieved);
  }

  public void testRemoveBuffer() {
    ShuffleManager manager = new ShuffleManager();
    manager.getOrCreateBuffer("q1", 0);
    manager.removeBuffer("q1", 0);
    assertNull(manager.getBuffer("q1", 0));
  }

  public void testBufferAddLeftData() {
    ShuffleManager.ShuffleBuffer buffer = new ShuffleManager.ShuffleBuffer();
    buffer.addData("left", new byte[] {1, 2, 3});
    buffer.addData("left", new byte[] {4, 5, 6});
    assertEquals(2, buffer.getLeftData().size());
    assertArrayEquals(new byte[] {1, 2, 3}, buffer.getLeftData().get(0));
    assertArrayEquals(new byte[] {4, 5, 6}, buffer.getLeftData().get(1));
  }

  public void testBufferAddRightData() {
    ShuffleManager.ShuffleBuffer buffer = new ShuffleManager.ShuffleBuffer();
    buffer.addData("right", new byte[] {10, 20});
    assertEquals(1, buffer.getRightData().size());
    assertTrue(buffer.getLeftData().isEmpty());
  }

  public void testBufferCompletionWithSingleSender() throws InterruptedException {
    ShuffleManager.ShuffleBuffer buffer = new ShuffleManager.ShuffleBuffer();
    buffer.setExpectedSenders(1, 1);

    buffer.addData("left", new byte[] {1});
    buffer.senderDone("left");
    buffer.addData("right", new byte[] {2});
    buffer.senderDone("right");

    assertTrue(buffer.awaitReady(1000));
  }

  public void testBufferCompletionWithMultipleSenders() throws InterruptedException {
    ShuffleManager.ShuffleBuffer buffer = new ShuffleManager.ShuffleBuffer();
    buffer.setExpectedSenders(3, 2);

    // 3 left senders
    buffer.senderDone("left");
    buffer.senderDone("left");
    buffer.senderDone("left");

    // 2 right senders
    buffer.senderDone("right");
    buffer.senderDone("right");

    assertTrue(buffer.awaitReady(1000));
  }

  public void testBufferNotReadyWhenIncomplete() throws InterruptedException {
    ShuffleManager.ShuffleBuffer buffer = new ShuffleManager.ShuffleBuffer();
    buffer.setExpectedSenders(2, 1);

    buffer.senderDone("left"); // only 1 of 2
    buffer.senderDone("right"); // 1 of 1

    // Should timeout because left has only 1 of 2 senders done
    assertFalse(buffer.awaitReady(100));
  }

  public void testBufferSendersCompletedBeforeExpectedCounts() throws InterruptedException {
    // Edge case: senderDone called before setExpectedSenders
    ShuffleManager.ShuffleBuffer buffer = new ShuffleManager.ShuffleBuffer();

    buffer.senderDone("left");
    buffer.senderDone("right");

    // Now set expected counts — completion should be detected retroactively
    buffer.setExpectedSenders(1, 1);
    assertTrue(buffer.awaitReady(1000));
  }

  public void testBufferDataAndCompletionInterleaved() throws InterruptedException {
    ShuffleManager.ShuffleBuffer buffer = new ShuffleManager.ShuffleBuffer();
    buffer.setExpectedSenders(2, 1);

    buffer.addData("left", new byte[] {1});
    buffer.senderDone("left");
    buffer.addData("left", new byte[] {2});
    buffer.senderDone("left");

    buffer.addData("right", new byte[] {3});
    buffer.senderDone("right");

    assertTrue(buffer.awaitReady(1000));
    assertEquals(2, buffer.getLeftData().size());
    assertEquals(1, buffer.getRightData().size());
  }
}
