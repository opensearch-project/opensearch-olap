/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;

/**
 * Integration tests for distributed join queries executed through the Velox engine. Uses PPL join
 * syntax to join two OpenSearch indices. The OLAP plugin detects the join plan and routes it
 * through the coordinator-centric join path (mpp_enabled=false by default).
 *
 * <p>Test data — two indices:
 *
 * <p><b>employees</b> (5 rows):
 *
 * <pre>
 *   emp_id=1, name=Alice,   dept_id=10, salary=120000
 *   emp_id=2, name=Bob,     dept_id=20, salary=95000
 *   emp_id=3, name=Charlie, dept_id=10, salary=150000
 *   emp_id=4, name=Diana,   dept_id=30, salary=110000
 *   emp_id=5, name=Eve,     dept_id=20, salary=88000
 * </pre>
 *
 * <p><b>departments</b> (3 rows):
 *
 * <pre>
 *   dept_id=10, dept_name=Engineering
 *   dept_id=20, dept_name=Marketing
 *   dept_id=30, dept_name=Sales
 * </pre>
 */
public class JoinIT extends OlapRestTestCase {

  private static final String EMPLOYEES_INDEX = "employees";
  private static final String DEPARTMENTS_INDEX = "departments";

  @Override
  public void setUp() throws Exception {
    super.setUp();
    createJoinTestIndices();
  }

  @Override
  public void tearDown() throws Exception {
    deleteIndex(EMPLOYEES_INDEX);
    deleteIndex(DEPARTMENTS_INDEX);
    super.tearDown();
  }

  // ---- Inner Join Tests ----

  public void testInnerJoin() throws IOException {
    // Join employees with departments on dept_id
    JSONObject response =
        executePPLQuery(
            "source = "
                + EMPLOYEES_INDEX
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + DEPARTMENTS_INDEX
                + " | fields e.name, d.dept_name");
    JSONArray rows = getDataRows(response);

    // All 5 employees should match (every dept_id has a corresponding department)
    assertEquals("Expected 5 rows from inner join", 5, rows.length());

    Map<String, String> empDept = new HashMap<>();
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      empDept.put(row.getString(0), row.getString(1));
    }

    assertEquals("Alice in Engineering", "Engineering", empDept.get("Alice"));
    assertEquals("Bob in Marketing", "Marketing", empDept.get("Bob"));
    assertEquals("Charlie in Engineering", "Engineering", empDept.get("Charlie"));
    assertEquals("Diana in Sales", "Sales", empDept.get("Diana"));
    assertEquals("Eve in Marketing", "Marketing", empDept.get("Eve"));
  }

  public void testInnerJoinWithFilter() throws IOException {
    // Join + filter: only employees in Engineering (dept_id=10)
    JSONObject response =
        executePPLQuery(
            "source = "
                + EMPLOYEES_INDEX
                + " | where dept_id = 10"
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + DEPARTMENTS_INDEX
                + " | fields e.name, d.dept_name");
    JSONArray rows = getDataRows(response);

    assertEquals("Expected 2 Engineering employees", 2, rows.length());
    Set<String> names = extractStringColumn(rows, 0);
    assertTrue(names.contains("Alice"));
    assertTrue(names.contains("Charlie"));
  }

  public void testInnerJoinWithAggregation() throws IOException {
    // Join + aggregation: count employees per department
    JSONObject response =
        executePPLQuery(
            "source = "
                + EMPLOYEES_INDEX
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + DEPARTMENTS_INDEX
                + " | stats count() by d.dept_name");
    JSONArray rows = getDataRows(response);

    Map<String, Long> countByDept = new HashMap<>();
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      countByDept.put(row.getString(1), row.getLong(0));
    }

    assertEquals("Expected 3 departments", 3, countByDept.size());
    assertEquals("Engineering count", Long.valueOf(2), countByDept.get("Engineering"));
    assertEquals("Marketing count", Long.valueOf(2), countByDept.get("Marketing"));
    assertEquals("Sales count", Long.valueOf(1), countByDept.get("Sales"));
  }

  public void testInnerJoinWithAvgAggregation() throws IOException {
    // Join + avg: average salary per department
    JSONObject response =
        executePPLQuery(
            "source = "
                + EMPLOYEES_INDEX
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + DEPARTMENTS_INDEX
                + " | stats avg(e.salary) by d.dept_name");
    JSONArray rows = getDataRows(response);

    Map<String, Double> avgByDept = new HashMap<>();
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      avgByDept.put(row.getString(1), row.getDouble(0));
    }

    assertEquals("Engineering avg", 135000.0, avgByDept.get("Engineering"), 0.01);
    assertEquals("Marketing avg", 91500.0, avgByDept.get("Marketing"), 0.01);
    assertEquals("Sales avg", 110000.0, avgByDept.get("Sales"), 0.01);
  }

  // ---- Left Join Tests ----

  public void testLeftJoin() throws IOException {
    // Left join: all employees, with department name if available
    JSONObject response =
        executePPLQuery(
            "source = "
                + EMPLOYEES_INDEX
                + " | left join left=e right=d ON e.dept_id = d.dept_id "
                + DEPARTMENTS_INDEX
                + " | fields e.name, d.dept_name");
    JSONArray rows = getDataRows(response);

    // All 5 employees should appear (left join preserves all left rows)
    assertEquals("Expected 5 rows from left join", 5, rows.length());
  }

  // ---- Join Correctness Tests ----

  public void testJoinProjectsCorrectColumns() throws IOException {
    // Verify column selection works correctly across join
    JSONObject response =
        executePPLQuery(
            "source = "
                + EMPLOYEES_INDEX
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + DEPARTMENTS_INDEX
                + " | fields e.name, e.salary, d.dept_name");
    JSONArray rows = getDataRows(response);

    assertEquals(5, rows.length());
    // Each row should have exactly 3 columns
    for (int i = 0; i < rows.length(); i++) {
      assertEquals(3, rows.getJSONArray(i).length());
    }
  }

  public void testJoinWithLimit() throws IOException {
    // Join + limit: only return first 3 rows
    JSONObject response =
        executePPLQuery(
            "source = "
                + EMPLOYEES_INDEX
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + DEPARTMENTS_INDEX
                + " | head 3"
                + " | fields e.name, d.dept_name");
    JSONArray rows = getDataRows(response);

    assertEquals("Expected 3 rows", 3, rows.length());
  }

  // ---- Log Verification ----

  public void testJoinAppearsInLogs() throws IOException {
    long beforeCount = countLogLines("join");

    executePPLQuery(
        "source = "
            + EMPLOYEES_INDEX
            + " | inner join left=e right=d ON e.dept_id = d.dept_id "
            + DEPARTMENTS_INDEX
            + " | fields e.name, d.dept_name");

    long afterCount = countLogLines("join");
    assertTrue("Expected join-related log entries", afterCount > beforeCount);
  }

  // ---- Helpers ----

  private void createJoinTestIndices() throws IOException {
    // Create employees index
    Request createEmployees = new Request("PUT", "/" + EMPLOYEES_INDEX);
    createEmployees.setJsonEntity(
        "{"
            + "\"settings\": {\"number_of_shards\": 1, \"number_of_replicas\": 0},"
            + "\"mappings\": {\"properties\": {"
            + "\"emp_id\": {\"type\": \"integer\"},"
            + "\"name\": {\"type\": \"keyword\"},"
            + "\"dept_id\": {\"type\": \"integer\"},"
            + "\"salary\": {\"type\": \"double\"}"
            + "}}"
            + "}");
    client().performRequest(createEmployees);

    // Create departments index
    Request createDepts = new Request("PUT", "/" + DEPARTMENTS_INDEX);
    createDepts.setJsonEntity(
        "{"
            + "\"settings\": {\"number_of_shards\": 1, \"number_of_replicas\": 0},"
            + "\"mappings\": {\"properties\": {"
            + "\"dept_id\": {\"type\": \"integer\"},"
            + "\"dept_name\": {\"type\": \"keyword\"}"
            + "}}"
            + "}");
    client().performRequest(createDepts);

    // Bulk insert employees
    Request bulkEmp = new Request("POST", "/_bulk?refresh=true");
    bulkEmp.setJsonEntity(
        "{\"index\": {\"_index\": \"employees\"}}\n"
            + "{\"emp_id\": 1, \"name\": \"Alice\", \"dept_id\": 10, \"salary\": 120000}\n"
            + "{\"index\": {\"_index\": \"employees\"}}\n"
            + "{\"emp_id\": 2, \"name\": \"Bob\", \"dept_id\": 20, \"salary\": 95000}\n"
            + "{\"index\": {\"_index\": \"employees\"}}\n"
            + "{\"emp_id\": 3, \"name\": \"Charlie\", \"dept_id\": 10, \"salary\": 150000}\n"
            + "{\"index\": {\"_index\": \"employees\"}}\n"
            + "{\"emp_id\": 4, \"name\": \"Diana\", \"dept_id\": 30, \"salary\": 110000}\n"
            + "{\"index\": {\"_index\": \"employees\"}}\n"
            + "{\"emp_id\": 5, \"name\": \"Eve\", \"dept_id\": 20, \"salary\": 88000}\n");
    client().performRequest(bulkEmp);

    // Bulk insert departments
    Request bulkDept = new Request("POST", "/_bulk?refresh=true");
    bulkDept.setJsonEntity(
        "{\"index\": {\"_index\": \"departments\"}}\n"
            + "{\"dept_id\": 10, \"dept_name\": \"Engineering\"}\n"
            + "{\"index\": {\"_index\": \"departments\"}}\n"
            + "{\"dept_id\": 20, \"dept_name\": \"Marketing\"}\n"
            + "{\"index\": {\"_index\": \"departments\"}}\n"
            + "{\"dept_id\": 30, \"dept_name\": \"Sales\"}\n");
    client().performRequest(bulkDept);
  }

  private void deleteIndex(String indexName) throws IOException {
    try {
      client().performRequest(new Request("DELETE", "/" + indexName));
    } catch (ResponseException e) {
      if (e.getResponse().getStatusLine().getStatusCode() != 404) {
        throw e;
      }
    }
  }

  // ---- Plan structure verification ----

  public void testInnerJoinPlanContainsHashJoinNode() throws IOException {
    String plan =
        explainVeloxPlan(
            "source = "
                + EMPLOYEES_INDEX
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + DEPARTMENTS_INDEX
                + " | fields e.name, d.dept_name");
    assertTrue("Plan should contain HashJoin node", plan.contains("HashJoin"));
    assertTrue("Plan should indicate INNER join", plan.contains("INNER"));
  }

  public void testLeftJoinPlanContainsLeftJoinType() throws IOException {
    String plan =
        explainVeloxPlan(
            "source = "
                + EMPLOYEES_INDEX
                + " | left join left=e right=d ON e.dept_id = d.dept_id "
                + DEPARTMENTS_INDEX
                + " | fields e.name, d.dept_name");
    assertTrue("Plan should contain HashJoin node", plan.contains("HashJoin"));
    assertTrue("Plan should indicate LEFT join", plan.contains("LEFT"));
  }

  private Set<String> extractStringColumn(JSONArray rows, int colIndex) {
    Set<String> values = new HashSet<>();
    for (int i = 0; i < rows.length(); i++) {
      values.add(rows.getJSONArray(i).getString(colIndex));
    }
    return values;
  }
}
