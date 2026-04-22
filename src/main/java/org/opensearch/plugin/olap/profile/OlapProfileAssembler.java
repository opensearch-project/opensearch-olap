/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.profile;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.plugin.olap.transport.ExecuteFragmentResponse;
import org.opensearch.sql.monitor.profile.ProfilePlanNode;
import org.opensearch.sql.monitor.profile.ProfilePlanNodeMetrics;
import org.opensearch.sql.monitor.profile.QueryProfile;

/**
 * Builds a {@link ProfilePlanNode} tree from per-task profiles returned by data nodes, so the
 * standard PPL {@code profile=true} flow (owned by the SQL plugin) can surface OLAP execution
 * details in the response.
 *
 * <p>Shape of the tree:
 *
 * <pre>
 *   VeloxQuery rf=BLOOM bloomBytes=512
 *     Fragment[0] rf=BLOOM (4 tasks)
 *       Task frag=0 part=0 node=data-1 docsRead=10000 docsMatched=120 rows=120
 *       ...
 *     Fragment[1] (1 tasks)
 *       Task frag=1 part=0 node=coordinator rows=4500
 * </pre>
 *
 * <p>Summed stage numbers (docsRead/docsMatched/rows) are encoded in the node name so a user can
 * inspect the whole tree without needing custom metric keys. Per-task time is captured in the
 * {@link ProfilePlanNodeMetrics} {@code timeNanos} accumulator on the leaf. Per-fragment rollups
 * sum the task times, which is a conservative overestimate (wall-clock parallelism not modeled),
 * but matches the SQL plugin's existing "child time may exceed parent" convention.
 */
public final class OlapProfileAssembler {

  private OlapProfileAssembler() {}

  /**
   * Build the root plan node for a query.
   *
   * @param rfSummary optional descriptor of the runtime filter applied (e.g. "rf=BLOOM
   *     bloomBytes=512"). Null or empty shows up as "VeloxQuery" with no RF suffix.
   * @param responses every {@link ExecuteFragmentResponse} collected by the coordinator across all
   *     stages of the query. Responses without a {@link OlapTaskProfile} are ignored.
   */
  public static ProfilePlanNode buildPlan(
      String rfSummary, Collection<ExecuteFragmentResponse> responses) {
    // Group task profiles by fragmentId, preserving insertion order for stable output.
    Map<Integer, List<OlapTaskProfile>> byFragment = new LinkedHashMap<>();
    for (ExecuteFragmentResponse r : responses) {
      if (r == null || !r.hasTaskProfile()) continue;
      OlapTaskProfile tp = r.getTaskProfile();
      byFragment.computeIfAbsent(tp.getFragmentId(), k -> new ArrayList<>()).add(tp);
    }

    List<ProfilePlanNode> fragmentNodes = new ArrayList<>();
    long queryDocsRead = 0;
    long queryDocsMatched = 0;
    long queryRows = 0;
    long queryTimeNanos = 0;
    for (Map.Entry<Integer, List<OlapTaskProfile>> entry : byFragment.entrySet()) {
      ProfilePlanNode fragNode = buildFragmentNode(entry.getKey(), entry.getValue());
      queryDocsRead += sumDocsRead(entry.getValue());
      queryDocsMatched += sumDocsMatched(entry.getValue());
      queryRows += sumRows(entry.getValue());
      queryTimeNanos += fragNode.metrics().timeNanos();
      fragmentNodes.add(fragNode);
    }

    String label = "VeloxQuery";
    if (rfSummary != null && !rfSummary.isEmpty()) {
      label += " " + rfSummary;
    }
    label +=
        " docsRead=" + queryDocsRead + " docsMatched=" + queryDocsMatched + " rows=" + queryRows;

    ProfilePlanNode root = new ProfilePlanNode(label, fragmentNodes);
    root.metrics().addTimeNanos(queryTimeNanos);
    // ProfilePlanNodeMetrics only exposes incrementRows (no bulk add), so the row count is
    // carried in the node label rather than the metric. This matches how the SQL plugin displays
    // scan-level rows — aggregate row counts at fragment/query level aren't plan-shape invariants.
    return root;
  }

  /**
   * Convenience snapshot helper: produce the {@link QueryProfile.PlanNode} directly (immutable
   * JSON-shaped view). Equivalent to {@code buildPlan(...).snapshot()}.
   */
  public static QueryProfile.PlanNode buildSnapshot(
      String rfSummary, Collection<ExecuteFragmentResponse> responses) {
    return buildPlan(rfSummary, responses).snapshot();
  }

  private static ProfilePlanNode buildFragmentNode(int fragmentId, List<OlapTaskProfile> tasks) {
    List<ProfilePlanNode> taskNodes = new ArrayList<>();
    long fragTime = 0;
    long fragRows = 0;
    String rfKind = "NONE";
    long fragDocsRead = 0;
    long fragDocsMatched = 0;
    for (OlapTaskProfile tp : tasks) {
      taskNodes.add(buildTaskNode(tp));
      fragTime += tp.getDurationNanos();
      fragRows += tp.getRowsEmitted();
      fragDocsRead += tp.getDocsRead();
      fragDocsMatched += tp.getDocsMatched();
      if (!"NONE".equals(tp.getRfKind())) {
        rfKind = tp.getRfKind();
      }
    }
    String label =
        "Fragment["
            + fragmentId
            + "] rf="
            + rfKind
            + " docsRead="
            + fragDocsRead
            + " docsMatched="
            + fragDocsMatched
            + " rows="
            + fragRows
            + " ("
            + tasks.size()
            + " tasks)";
    ProfilePlanNode node = new ProfilePlanNode(label, taskNodes);
    node.metrics().addTimeNanos(fragTime);
    return node;
  }

  private static ProfilePlanNode buildTaskNode(OlapTaskProfile tp) {
    String label =
        "Task frag="
            + tp.getFragmentId()
            + " part="
            + tp.getPartitionId()
            + " node="
            + tp.getNodeId()
            + " rf="
            + tp.getRfKind()
            + " docsRead="
            + tp.getDocsRead()
            + " docsMatched="
            + tp.getDocsMatched()
            + " rows="
            + tp.getRowsEmitted();
    ProfilePlanNode node = new ProfilePlanNode(label, Collections.emptyList());
    node.metrics().addTimeNanos(tp.getDurationNanos());
    return node;
  }

  private static long sumDocsRead(List<OlapTaskProfile> tasks) {
    long s = 0;
    for (OlapTaskProfile t : tasks) s += t.getDocsRead();
    return s;
  }

  private static long sumDocsMatched(List<OlapTaskProfile> tasks) {
    long s = 0;
    for (OlapTaskProfile t : tasks) s += t.getDocsMatched();
    return s;
  }

  private static long sumRows(List<OlapTaskProfile> tasks) {
    long s = 0;
    for (OlapTaskProfile t : tasks) s += t.getRowsEmitted();
    return s;
  }
}
