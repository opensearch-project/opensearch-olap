/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.transport;

import org.opensearch.action.ActionType;

/** Transport action type for sending shuffle partition data between nodes during hash shuffle. */
public class ShuffleDataAction extends ActionType<ShuffleDataResponse> {

  public static final ShuffleDataAction INSTANCE = new ShuffleDataAction();
  public static final String NAME = "indices:data/read/olap/shuffle";

  private ShuffleDataAction() {
    super(NAME, ShuffleDataResponse::new);
  }
}
