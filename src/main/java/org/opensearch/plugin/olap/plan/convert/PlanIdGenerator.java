/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.convert;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Generates unique plan node IDs within a single query planning session. Velox requires each
 * PlanNode in a plan tree to have a unique string ID.
 */
public class PlanIdGenerator {
  private final AtomicInteger counter = new AtomicInteger(0);

  public String next() {
    return String.valueOf(counter.getAndIncrement());
  }

  public void reset() {
    counter.set(0);
  }
}
