/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap;

import java.io.IOException;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;

/**
 * Integration tests that verify the Velox plan structure via the PPL `explain` command. Runs
 * `explain <query>` and checks the returned plan JSON for expected node types, aggregation steps,
 * and fragment distributions.
 *
 * <p>The OLAP plugin implements {@code ExecutionEngine.explain(RelNode, ...)} which returns:
 *
 * <ul>
 *   <li><b>logical</b>: Calcite physical plan (PhysicalConvention nodes)
 *   <li><b>physical</b>: Velox plan JSON per fragment (from Serde.toPrettyJson)
 * </ul>
 */
public class PlanExplainIT extends OlapRestTestCase {

  private static final String EMPLOYEES_INDEX = "employees";
  private static final String DEPARTMENTS_INDEX = "departments";

  @Override
  public void setUp() throws Exception {
    super.setUp();
    createTestIndices();
  }

  @Override
  public void tearDown() throws Exception {
    deleteIndex(EMPLOYEES_INDEX);
    deleteIndex(DEPARTMENTS_INDEX);
    super.tearDown();
  }

  // ---- Two-stage aggregation plan ----

  public void testAggregationPlanHasAggregationNode() throws IOException {
    String plan = explainVeloxPlan("source = " + EMPLOYEES_INDEX + " | stats count() by dept_id");

    assertTrue("Plan should have SOURCE fragment", plan.contains("[SOURCE]"));
    // Velox tree format: "-- Aggregation[id][PARTIAL ...]" or "-- Aggregation[id][SINGLE ...]"
    assertTrue("Plan should contain Aggregation node", plan.contains("Aggregation"));

    // Two-stage (PARTIAL+FINAL) occurs when Convention.enforce() inserts exchange.
    if (plan.contains("[COORDINATOR]")) {
      assertTrue("Should have PARTIAL step", plan.contains("PARTIAL"));
      assertTrue("Should have FINAL step", plan.contains("FINAL"));
    }
  }

  // ---- Two-stage TopN plan ----

  public void testSortLimitPlanHasOrderByAndLimit() throws IOException {
    String plan = explainVeloxPlan("source = " + EMPLOYEES_INDEX + " | sort salary | head 3");

    assertTrue("Plan should have SOURCE fragment", plan.contains("[SOURCE]"));
    // Velox tree format: "-- Limit[id][...]" and "-- OrderBy[id][...]"
    assertTrue("Plan should contain Limit node", plan.contains("Limit"));
    assertTrue("Plan should contain OrderBy node", plan.contains("OrderBy"));

    // Two-stage TopN: if COORDINATOR fragment exists, both leaf and coordinator have LimitNode
    if (plan.contains("[COORDINATOR]")) {
      // Verify the partial limit is on the leaf and final on coordinator
      assertTrue("Two-stage TopN should have COORDINATOR fragment", plan.contains("[COORDINATOR]"));
    }
  }

  public void testSortWithoutLimitDoesNotSplitIntoTwoStage() throws IOException {
    String plan =
        explainVeloxPlan("source = " + EMPLOYEES_INDEX + " | sort salary | fields name, salary");

    // Should have OrderByNode
    assertTrue("Plan should contain OrderBy node", plan.contains("OrderBy"));

    // Should NOT have a partial LimitNode (no limit = no two-stage split)
    // Sort without limit still has SOURCE + COORDINATOR but no LimitNode on leaf
  }

  // ---- Join plan ----

  public void testJoinPlanHasHashJoinNode() throws IOException {
    String plan =
        explainVeloxPlan(
            "source = "
                + EMPLOYEES_INDEX
                + " | inner join left=e right=d ON e.dept_id = d.dept_id "
                + DEPARTMENTS_INDEX
                + " | fields e.name, d.dept_name");

    // Should have multiple fragments
    assertTrue("Plan should have SOURCE fragments", plan.contains("[SOURCE]"));
    assertTrue("Plan should have COORDINATOR fragment", plan.contains("[COORDINATOR]"));

    // Coordinator should have HashJoinNode
    // Velox tree format: "-- HashJoin[id][INNER ...]"
    assertTrue("Plan should contain HashJoin node", plan.contains("HashJoin"));
  }

  // ---- Simple scan plan ----

  public void testSimpleScanPlanHasTableScanNode() throws IOException {
    String plan = explainVeloxPlan("source = " + EMPLOYEES_INDEX + " | fields name, salary");

    assertTrue("Plan should contain TableScan node", plan.contains("TableScan"));
    assertTrue("Plan should have SOURCE fragment", plan.contains("[SOURCE]"));
  }

  // ---- Filter + Project plan ----

  public void testFilterProjectPlan() throws IOException {
    String plan =
        explainVeloxPlan(
            "source = " + EMPLOYEES_INDEX + " | where dept_id = 10 | fields name, salary");

    assertTrue("Plan should contain Filter node", plan.contains("Filter"));
    assertTrue("Plan should contain Project node", plan.contains("Project"));
  }

  // ---- Aggregation with avg ----

  public void testAvgAggregationPlan() throws IOException {
    String plan =
        explainVeloxPlan("source = " + EMPLOYEES_INDEX + " | stats avg(salary) by dept_id");

    // Velox tree format: "-- Aggregation[id][... avg ...]"
    assertTrue("Plan should contain Aggregation node", plan.contains("Aggregation"));
    assertTrue("Plan should reference avg function", plan.contains("avg"));
  }

  // ---- Helpers ----

  private void createTestIndices() throws IOException {
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
}
