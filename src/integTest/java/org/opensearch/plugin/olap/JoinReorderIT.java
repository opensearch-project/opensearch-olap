/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Integration tests for join reorder optimization. Verifies that multi-way joins produce correct
 * results with and without CBO statistics, and that join reorder does not change query semantics.
 */
public class JoinReorderIT extends OlapRestTestCase {

  @Override
  public void setUp() throws Exception {
    super.setUp();
    setClusterSetting("plugins.velox.cbo_statistics_mode", "RUNTIME");
    loadIndex(Index.EMPLOYEES);
    loadIndex(Index.DEPARTMENTS);
    loadIndex(Index.PROJECTS);
  }

  @Override
  public void tearDown() throws Exception {
    deleteIndex(Index.EMPLOYEES.getName());
    deleteIndex(Index.DEPARTMENTS.getName());
    deleteIndex(Index.PROJECTS.getName());
    setClusterSetting("plugins.velox.cbo_statistics_mode", "NONE");
    super.tearDown();
  }

  // ---- Two-table join reorder correctness ----

  public void testTwoTableJoinWithCboProducesCorrectResults() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = "
                + Index.EMPLOYEES.getName()
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + Index.DEPARTMENTS.getName()
                + " | fields e.name, d.dept_name");
    JSONArray rows = getDataRows(response);

    assertEquals("Expected 5 rows from inner join", 5, rows.length());

    Map<String, String> empDept = new HashMap<>();
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      empDept.put(row.getString(0), row.getString(1));
    }
    assertEquals("Engineering", empDept.get("Alice"));
    assertEquals("Marketing", empDept.get("Bob"));
    assertEquals("Sales", empDept.get("Diana"));
  }

  // ---- CBO vs no-CBO produce same results ----

  public void testJoinReorderCboVsNoCboSameResults() throws IOException {
    String query =
        "source = "
            + Index.EMPLOYEES.getName()
            + " | inner join left=e right=d ON e.dept_id = d.dept_id "
            + Index.DEPARTMENTS.getName()
            + " | stats count() by d.dept_name";

    // With CBO (join reorder uses real row counts)
    setClusterSetting("plugins.velox.cbo_statistics_mode", "RUNTIME");
    JSONObject cboResp = executePPLQuery(query);
    JSONArray cboRows = getDataRows(cboResp);

    // Without CBO (join reorder uses Calcite default estimates)
    setClusterSetting("plugins.velox.cbo_statistics_mode", "NONE");
    JSONObject noCboResp = executePPLQuery(query);
    JSONArray noCboRows = getDataRows(noCboResp);

    assertEquals("Same number of result rows", cboRows.length(), noCboRows.length());

    // Both should produce the same aggregation values
    Map<String, Long> cboCounts = extractCountByString(cboRows, 1, 0);
    Map<String, Long> noCboCounts = extractCountByString(noCboRows, 1, 0);
    assertEquals("Same counts per department", cboCounts, noCboCounts);
  }

  // ---- Three-table join correctness ----

  public void testThreeTableInnerJoin() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = "
                + Index.EMPLOYEES.getName()
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + Index.DEPARTMENTS.getName()
                + " | inner join left=ed right=p ON ed.dept_id = p.dept_id "
                + Index.PROJECTS.getName()
                + " | fields e.name, d.dept_name, p.project_name");
    JSONArray rows = getDataRows(response);

    // employees(5) JOIN departments(3) → 5 rows, JOIN projects(4) → matches by dept_id
    // dept_id=10: Alice,Charlie × (Alpha,Beta) = 4 rows
    // dept_id=20: Bob,Eve × (Gamma) = 2 rows
    // dept_id=30: Diana × (Delta) = 1 row
    // Total: 7 rows
    assertEquals("Expected 7 rows from 3-way join", 7, rows.length());
  }

  public void testThreeTableJoinWithAggregation() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = "
                + Index.EMPLOYEES.getName()
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + Index.DEPARTMENTS.getName()
                + " | inner join left=ed right=p ON ed.dept_id = p.dept_id "
                + Index.PROJECTS.getName()
                + " | stats count() by d.dept_name");
    JSONArray rows = getDataRows(response);

    Map<String, Long> countByDept = extractCountByString(rows, 1, 0);
    assertEquals(
        "Engineering: 2 emp × 2 projects = 4", Long.valueOf(4), countByDept.get("Engineering"));
    assertEquals("Marketing: 2 emp × 1 project = 2", Long.valueOf(2), countByDept.get("Marketing"));
    assertEquals("Sales: 1 emp × 1 project = 1", Long.valueOf(1), countByDept.get("Sales"));
  }

  // ---- Three-table join CBO vs no-CBO ----

  public void testThreeTableJoinCboVsNoCboSameResults() throws IOException {
    String query =
        "source = "
            + Index.EMPLOYEES.getName()
            + " | inner join left=e right=d ON e.dept_id = d.dept_id "
            + Index.DEPARTMENTS.getName()
            + " | inner join left=ed right=p ON ed.dept_id = p.dept_id "
            + Index.PROJECTS.getName()
            + " | stats count() by d.dept_name";

    setClusterSetting("plugins.velox.cbo_statistics_mode", "RUNTIME");
    JSONObject cboResp = executePPLQuery(query);

    setClusterSetting("plugins.velox.cbo_statistics_mode", "NONE");
    JSONObject noCboResp = executePPLQuery(query);

    Map<String, Long> cboCounts = extractCountByString(getDataRows(cboResp), 1, 0);
    Map<String, Long> noCboCounts = extractCountByString(getDataRows(noCboResp), 1, 0);
    assertEquals("3-way join counts should match with and without CBO", cboCounts, noCboCounts);
  }

  // ---- Left join preserved with reorder ----

  public void testLeftJoinPreservedWithReorder() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = "
                + Index.EMPLOYEES.getName()
                + " | left join left=e right=d ON e.dept_id = d.dept_id "
                + Index.DEPARTMENTS.getName()
                + " | fields e.name, d.dept_name");
    JSONArray rows = getDataRows(response);

    // All 5 employees should appear (left join preserves left side)
    assertEquals("Left join should preserve all employees", 5, rows.length());
  }

  // ---- Plan structure via Velox PlanNode.toFormatString() ----

  private String threeWayJoinQuery() {
    return "source = "
        + Index.EMPLOYEES.getName()
        + " | inner join left=e right=d ON e.dept_id = d.dept_id "
        + Index.DEPARTMENTS.getName()
        + " | inner join left=ed right=p ON ed.dept_id = p.dept_id "
        + Index.PROJECTS.getName()
        + " | fields e.name, d.dept_name, p.project_name";
  }

  /**
   * Strip Velox plan node IDs (non-deterministic) for snapshot comparison. Node IDs appear as the
   * first [...] after "-- NodeName", e.g. "-- Project[4][...]" → "-- Project[*][...]". Limit values
   * like [50000] must be preserved.
   */
  private String stripPlanNodeIds(String plan) {
    // Strip node IDs: "NodeName[123]" → "NodeName[*]" (first bracket after node name)
    return plan.replaceAll("(-- \\w+)\\[\\d+\\]", "$1[*]")
        .replaceAll("\\[exchange_scan_\\d+\\]", "[exchange_scan]")
        .stripTrailing();
  }

  /** 2-table join: full Velox plan snapshot comparison. */
  public void testTwoTableJoinPlan() throws IOException {
    String plan =
        stripPlanNodeIds(
            explainVeloxPlan(
                "source = "
                    + Index.EMPLOYEES.getName()
                    + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                    + Index.DEPARTMENTS.getName()
                    + " | fields e.name, d.dept_name"));

    // @formatter:off
    String expected =
"""
Fragment 0 [SOURCE]
-- Project[*][expressions: (name:VARCHAR, "name"), (dept_id:INTEGER, "dept_id"), (salary:DOUBLE, "salary"), (emp_id:INTEGER, "emp_id")] -> name:VARCHAR, dept_id:INTEGER, salary:DOUBLE, emp_id:INTEGER
  -- TableScan[*][ExternalStreamTableHandle] -> name:VARCHAR, dept_id:INTEGER, salary:DOUBLE, emp_id:INTEGER

Fragment 1 [SOURCE]
-- Limit[*][50000] -> dept_id:INTEGER, dept_name:VARCHAR
  -- Project[*][expressions: (dept_id:INTEGER, "dept_id"), (dept_name:VARCHAR, "dept_name")] -> dept_id:INTEGER, dept_name:VARCHAR
    -- TableScan[*][ExternalStreamTableHandle] -> dept_id:INTEGER, dept_name:VARCHAR

Fragment 2 [COORDINATOR]
-- Limit[*][10000] -> name:VARCHAR, dept_name:VARCHAR
  -- Project[*][expressions: (name:VARCHAR, "name"), (dept_name:VARCHAR, "dept_name")] -> name:VARCHAR, dept_name:VARCHAR
    -- HashJoin[*][INNER dept_id=dept_id0] -> name:VARCHAR, dept_id:INTEGER, salary:DOUBLE, emp_id:INTEGER, dept_id0:INTEGER, dept_name:VARCHAR
      -- TableScan[exchange_scan][ExternalStreamTableHandle] -> name:VARCHAR, dept_id:INTEGER, salary:DOUBLE, emp_id:INTEGER
      -- Project[*][expressions: (dept_id0:INTEGER, "dept_id"), (dept_name:VARCHAR, "dept_name")] -> dept_id0:INTEGER, dept_name:VARCHAR
        -- TableScan[exchange_scan][ExternalStreamTableHandle] -> dept_id:INTEGER, dept_name:VARCHAR\
""";
    // @formatter:on
    assertEquals(expected.stripTrailing(), plan);
  }

  /**
   * 3-way join with CBO: full Velox plan snapshot. The coordinator fragment must contain 2 nested
   * HashJoinNodes with 3 exchange scans — all joins inlined into the coordinator.
   */
  public void testThreeWayJoinCboPlan() throws IOException {
    setClusterSetting("plugins.velox.cbo_statistics_mode", "RUNTIME");
    String plan = stripPlanNodeIds(explainVeloxPlan(threeWayJoinQuery()));

    // @formatter:off
    String expected =
"""
Fragment 0 [SOURCE]
-- Project[*][expressions: (name:VARCHAR, "name"), (dept_id:INTEGER, "dept_id"), (salary:DOUBLE, "salary"), (emp_id:INTEGER, "emp_id")] -> name:VARCHAR, dept_id:INTEGER, salary:DOUBLE, emp_id:INTEGER
  -- TableScan[*][ExternalStreamTableHandle] -> name:VARCHAR, dept_id:INTEGER, salary:DOUBLE, emp_id:INTEGER

Fragment 1 [SOURCE]
-- Limit[*][50000] -> dept_id:INTEGER, dept_name:VARCHAR
  -- Project[*][expressions: (dept_id:INTEGER, "dept_id"), (dept_name:VARCHAR, "dept_name")] -> dept_id:INTEGER, dept_name:VARCHAR
    -- TableScan[*][ExternalStreamTableHandle] -> dept_id:INTEGER, dept_name:VARCHAR

Fragment 2 [SOURCE]
-- Limit[*][50000] -> dept_id:INTEGER, project_name:VARCHAR, project_id:INTEGER, budget:DOUBLE
  -- Project[*][expressions: (dept_id:INTEGER, "dept_id"), (project_name:VARCHAR, "project_name"), (project_id:INTEGER, "project_id"), (budget:DOUBLE, "budget")] -> dept_id:INTEGER, project_name:VARCHAR, project_id:INTEGER, budget:DOUBLE
    -- TableScan[*][ExternalStreamTableHandle] -> dept_id:INTEGER, project_name:VARCHAR, project_id:INTEGER, budget:DOUBLE

Fragment 3 [COORDINATOR]
-- Limit[*][10000] -> name:VARCHAR, dept_name:VARCHAR, project_name:VARCHAR
  -- Project[*][expressions: (name:VARCHAR, "name"), (dept_name:VARCHAR, "dept_name"), (project_name:VARCHAR, "project_name")] -> name:VARCHAR, dept_name:VARCHAR, project_name:VARCHAR
    -- HashJoin[*][INNER dept_id=dept_id0] -> name:VARCHAR, dept_id:INTEGER, salary:DOUBLE, emp_id:INTEGER, "d.dept_id":INTEGER, dept_name:VARCHAR, dept_id0:INTEGER, project_name:VARCHAR, project_id:INTEGER, budget:DOUBLE
      -- Project[*][expressions: (name:VARCHAR, "name"), (dept_id:INTEGER, "dept_id"), (salary:DOUBLE, "salary"), (emp_id:INTEGER, "emp_id"), (d.dept_id:INTEGER, "dept_id0"), (dept_name:VARCHAR, "dept_name")] -> name:VARCHAR, dept_id:INTEGER, salary:DOUBLE, emp_id:INTEGER, "d.dept_id":INTEGER, dept_name:VARCHAR
        -- HashJoin[*][INNER dept_id=dept_id0] -> name:VARCHAR, dept_id:INTEGER, salary:DOUBLE, emp_id:INTEGER, dept_id0:INTEGER, dept_name:VARCHAR
          -- TableScan[exchange_scan][ExternalStreamTableHandle] -> name:VARCHAR, dept_id:INTEGER, salary:DOUBLE, emp_id:INTEGER
          -- Project[*][expressions: (dept_id0:INTEGER, "dept_id"), (dept_name:VARCHAR, "dept_name")] -> dept_id0:INTEGER, dept_name:VARCHAR
            -- TableScan[exchange_scan][ExternalStreamTableHandle] -> dept_id:INTEGER, dept_name:VARCHAR
      -- Project[*][expressions: (dept_id0:INTEGER, "dept_id"), (project_name:VARCHAR, "project_name"), (project_id:INTEGER, "project_id"), (budget:DOUBLE, "budget")] -> dept_id0:INTEGER, project_name:VARCHAR, project_id:INTEGER, budget:DOUBLE
        -- TableScan[exchange_scan][ExternalStreamTableHandle] -> dept_id:INTEGER, project_name:VARCHAR, project_id:INTEGER, budget:DOUBLE\
""";
    // @formatter:on
    assertEquals(expected.stripTrailing(), plan);
  }

  /** CBO and no-CBO 3-way plans must have identical structure (test data is small). */
  public void testCboVsNoCboPlansAreValid() throws IOException {
    setClusterSetting("plugins.velox.cbo_statistics_mode", "RUNTIME");
    String cboPlan = stripPlanNodeIds(explainVeloxPlan(threeWayJoinQuery()));

    setClusterSetting("plugins.velox.cbo_statistics_mode", "NONE");
    String noCboPlan = stripPlanNodeIds(explainVeloxPlan(threeWayJoinQuery()));

    assertEquals("CBO and no-CBO should produce same plan for small test data", cboPlan, noCboPlan);
  }

  // ---- Helpers ----

  private Map<String, Long> extractCountByString(JSONArray rows, int keyCol, int valCol) {
    Map<String, Long> map = new HashMap<>();
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      map.put(row.getString(keyCol), row.getLong(valCol));
    }
    return map;
  }
}
