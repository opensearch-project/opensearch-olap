/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical;

import org.apache.calcite.rel.RelNode;

/**
 * Marker interface for physical relational operators.
 *
 * <p>All physical nodes produced by the {@link PhysicalOptimizer} implement this interface. Used by
 * {@link PhysicalConvention#getInterface()} and by {@link VeloxPlanGenerator} for type checking
 * during plan generation.
 */
public interface PhysicalRel extends RelNode {}
