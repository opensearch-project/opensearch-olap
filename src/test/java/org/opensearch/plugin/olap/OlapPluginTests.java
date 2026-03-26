/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap;

import org.opensearch.test.OpenSearchTestCase;

public class OlapPluginTests extends OpenSearchTestCase {

  public void testPluginInstantiation() {
    OlapPlugin plugin = new OlapPlugin();
    assertNotNull(plugin);
  }
}
