/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.common;

import java.util.Objects;
import java.util.UUID;

public final class QueryId {
  private final String id;

  private QueryId(String id) {
    this.id = Objects.requireNonNull(id);
  }

  public static QueryId generate() {
    return new QueryId(UUID.randomUUID().toString());
  }

  public static QueryId of(String id) {
    return new QueryId(id);
  }

  public String getId() {
    return id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof QueryId)) return false;
    return id.equals(((QueryId) o).id);
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public String toString() {
    return id;
  }
}
