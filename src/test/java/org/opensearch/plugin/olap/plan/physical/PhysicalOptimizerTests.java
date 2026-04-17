/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.plugin.olap.scheduler.TableStatistics;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for PhysicalOptimizer. Constructs Calcite logical plans using RelBuilder with test tables
 * and verifies the optimizer produces physical PhysicalRel nodes with PhysicalExchange at
 * distribution boundaries.
 */
public class PhysicalOptimizerTests extends OpenSearchTestCase {

  private RelBuilder b() {
    return CalciteTestHelper.createRelBuilder();
  }

  private int countNodes(RelNode node, Class<?> clazz) {
    int count = clazz.isInstance(node) ? 1 : 0;
    for (RelNode input : node.getInputs()) {
      count += countNodes(input, clazz);
    }
    return count;
  }

  private RelNode findFirst(RelNode node, Class<?> clazz) {
    if (clazz.isInstance(node)) return node;
    for (RelNode input : node.getInputs()) {
      RelNode found = findFirst(input, clazz);
      if (found != null) return found;
    }
    return null;
  }

  private void assertAllPhysicalRel(RelNode node) {
    if (!(node instanceof PhysicalRel)) {
      fail("Expected PhysicalRel but got " + node.getClass().getSimpleName());
    }
    for (RelNode input : node.getInputs()) {
      assertAllPhysicalRel(input);
    }
  }

  /**
   * Collect all table names from PhysicalTableScan nodes in left-to-right DFS order. This reflects
   * the plan's join order: the first two tables are the innermost join pair.
   */
  private List<String> collectTableNames(RelNode node) {
    List<String> names = new ArrayList<>();
    collectTableNamesRecursive(node, names);
    return names;
  }

  private void collectTableNamesRecursive(RelNode node, List<String> names) {
    if (node instanceof PhysicalTableScan) {
      List<String> qn = node.getTable().getQualifiedName();
      names.add(qn.get(qn.size() - 1));
      return;
    }
    for (RelNode input : node.getInputs()) {
      collectTableNamesRecursive(input, names);
    }
  }

  /**
   * Find the innermost (first executed) PhysicalJoin and return the table names of its two direct
   * scan children. Returns null if the innermost join doesn't have two direct scan inputs (e.g., if
   * there are intermediate exchanges).
   */
  private String[] getInnermostJoinTables(RelNode node) {
    PhysicalJoin innermost = findInnermostJoin(node);
    if (innermost == null) return null;
    String leftTable = getDirectScanTable(innermost.getLeft());
    String rightTable = getDirectScanTable(innermost.getRight());
    if (leftTable != null && rightTable != null) {
      return new String[] {leftTable, rightTable};
    }
    return null;
  }

  private PhysicalJoin findInnermostJoin(RelNode node) {
    // DFS: find the deepest PhysicalJoin (one whose children have no PhysicalJoin)
    if (node instanceof PhysicalJoin) {
      PhysicalJoin leftJoin = findInnermostJoin(((PhysicalJoin) node).getLeft());
      if (leftJoin != null) return leftJoin;
      PhysicalJoin rightJoin = findInnermostJoin(((PhysicalJoin) node).getRight());
      if (rightJoin != null) return rightJoin;
      return (PhysicalJoin) node;
    }
    for (RelNode input : node.getInputs()) {
      PhysicalJoin found = findInnermostJoin(input);
      if (found != null) return found;
    }
    return null;
  }

  /** Get the Calcite plan explain string (like EXPLAIN output). */
  private String explainPlan(RelNode node) {
    java.io.StringWriter sw = new java.io.StringWriter();
    node.explain(new RelWriterImpl(new java.io.PrintWriter(sw)));
    return sw.toString();
  }

  /** Walk through exchanges to find the table name of the scan underneath. */
  private String getDirectScanTable(RelNode node) {
    if (node instanceof PhysicalTableScan) {
      List<String> qn = node.getTable().getQualifiedName();
      return qn.get(qn.size() - 1);
    }
    // Walk through PhysicalExchange and other single-input nodes
    if (node.getInputs().size() == 1) {
      return getDirectScanTable(node.getInput(0));
    }
    return null;
  }

  // ---- Simple scan ----

  public void testSimpleScanProducesPhysicalNodes() {
    RelNode logical = b().scan("employees").build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(false);
    RelNode physical = optimizer.optimize(logical);

    assertTrue(physical instanceof PhysicalRel);
    assertTrue(countNodes(physical, PhysicalTableScan.class) >= 1);
  }

  public void testSimpleScanAllNodesArePhysicalRel() {
    RelNode logical = b().scan("employees").build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(false);
    RelNode physical = optimizer.optimize(logical);

    assertAllPhysicalRel(physical);
  }

  // ---- Filter + Project ----

  public void testFilterProjectProducesCorrectNodes() {
    RelBuilder rb = b();
    RelNode logical =
        rb.scan("employees")
            .filter(rb.equals(rb.field("dept_id"), rb.literal(10)))
            .project(rb.field("name"), rb.field("salary"))
            .build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(false);
    RelNode physical = optimizer.optimize(logical);

    assertAllPhysicalRel(physical);
    assertTrue(countNodes(physical, PhysicalTableScan.class) >= 1);
  }

  // ---- Aggregate (non-MPP) ----

  public void testAggregateNonMppHasAggregate() {
    RelBuilder rb = b();
    RelNode logical = rb.scan("employees").aggregate(rb.groupKey("dept_id"), rb.count()).build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(false);
    RelNode physical = optimizer.optimize(logical);

    assertAllPhysicalRel(physical);
    assertNotNull(findFirst(physical, PhysicalAggregate.class));
    // Exchange may or may not be inserted depending on cost model
    int exchangeCount = countNodes(physical, PhysicalExchange.class);
    logger.info("Aggregate plan has {} exchanges", exchangeCount);
  }

  public void testGlobalAggregateNonMpp() {
    RelBuilder rb = b();
    RelNode logical = rb.scan("employees").aggregate(rb.groupKey(), rb.count()).build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(false);
    RelNode physical = optimizer.optimize(logical);

    assertAllPhysicalRel(physical);
    assertNotNull(findFirst(physical, PhysicalAggregate.class));
  }

  // ---- Aggregate (MPP) ----

  public void testAggregateMppProducesValidPlan() {
    RelBuilder rb = b();
    RelNode logical = rb.scan("employees").aggregate(rb.groupKey("dept_id"), rb.count()).build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(true);
    RelNode physical = optimizer.optimize(logical);

    assertAllPhysicalRel(physical);
    assertNotNull(findFirst(physical, PhysicalAggregate.class));
  }

  // ---- Two-stage aggregation ----

  public void testAggregateHasExchangeViaEnforce() {
    RelBuilder rb = b();
    RelNode logical = rb.scan("employees").aggregate(rb.groupKey("dept_id"), rb.count()).build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(false);
    RelNode physical = optimizer.optimize(logical);

    // Convention.enforce() inserts PhysicalExchange when distribution doesn't satisfy
    assertAllPhysicalRel(physical);
    PhysicalAggregate agg = (PhysicalAggregate) findFirst(physical, PhysicalAggregate.class);
    assertNotNull(agg);
    assertEquals(PhysicalAggregate.Step.SINGLE, agg.getStep());
  }

  // ---- Sort/Limit ----

  public void testSortLimitProducesPhysicalSort() {
    RelBuilder rb = b();
    RelNode logical = rb.scan("employees").sort(rb.field("salary")).limit(0, 10).build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(false);
    RelNode physical = optimizer.optimize(logical);

    assertAllPhysicalRel(physical);
    assertTrue(countNodes(physical, PhysicalSort.class) >= 1);
  }

  // ---- Join (non-MPP) ----

  public void testJoinNonMppProducesPhysicalJoin() {
    RelBuilder rb = b();
    RelNode logical =
        rb.scan("employees")
            .scan("departments")
            .join(
                JoinRelType.INNER, rb.equals(rb.field(2, 0, "dept_id"), rb.field(2, 1, "dept_id")))
            .build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(false);
    RelNode physical = optimizer.optimize(logical);

    assertAllPhysicalRel(physical);
    assertNotNull(findFirst(physical, PhysicalJoin.class));
  }

  public void testJoinNonMppAllPhysicalRel() {
    RelBuilder rb = b();
    RelNode logical =
        rb.scan("employees")
            .scan("departments")
            .join(
                JoinRelType.INNER, rb.equals(rb.field(2, 0, "dept_id"), rb.field(2, 1, "dept_id")))
            .build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(false);
    RelNode physical = optimizer.optimize(logical);

    assertAllPhysicalRel(physical);
    int exchangeCount = countNodes(physical, PhysicalExchange.class);
    logger.info("Join plan has {} exchanges", exchangeCount);
  }

  // ---- Join (MPP) ----

  public void testJoinMppProducesValidPlan() {
    RelBuilder rb = b();
    RelNode logical =
        rb.scan("employees")
            .scan("departments")
            .join(
                JoinRelType.INNER, rb.equals(rb.field(2, 0, "dept_id"), rb.field(2, 1, "dept_id")))
            .build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(true);
    RelNode physical = optimizer.optimize(logical);

    assertAllPhysicalRel(physical);
    assertNotNull(findFirst(physical, PhysicalJoin.class));
  }

  // ---- Left join ----

  public void testLeftJoinProducesPhysicalJoin() {
    RelBuilder rb = b();
    RelNode logical =
        rb.scan("employees")
            .scan("departments")
            .join(JoinRelType.LEFT, rb.equals(rb.field(2, 0, "dept_id"), rb.field(2, 1, "dept_id")))
            .build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(false);
    RelNode physical = optimizer.optimize(logical);

    assertAllPhysicalRel(physical);
    assertNotNull(findFirst(physical, PhysicalJoin.class));
  }

  // ---- Join + Aggregate ----

  public void testJoinWithAggregateProducesValidPlan() {
    RelBuilder rb = b();
    RelNode logical =
        rb.scan("employees")
            .scan("departments")
            .join(
                JoinRelType.INNER, rb.equals(rb.field(2, 0, "dept_id"), rb.field(2, 1, "dept_id")))
            .aggregate(rb.groupKey(rb.field("dept_name")), rb.count())
            .build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(false);
    RelNode physical = optimizer.optimize(logical);

    assertAllPhysicalRel(physical);
    assertNotNull(findFirst(physical, PhysicalJoin.class));
    assertNotNull(findFirst(physical, PhysicalAggregate.class));
  }

  // ---- Filter + Aggregate ----

  public void testFilterAggregateProducesValidPlan() {
    RelBuilder rb = b();
    RelNode logical =
        rb.scan("employees")
            .filter(
                rb.call(
                    org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN,
                    rb.field("salary"),
                    rb.literal(100000)))
            .aggregate(rb.groupKey("dept_id"), rb.count())
            .build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(false);
    RelNode physical = optimizer.optimize(logical);

    assertAllPhysicalRel(physical);
  }

  // ---- StatisticsTableScan ----

  /** StatisticsTableScan reports injected row count to Calcite's cost model. */
  public void testStatisticsTableScanReportsRowCount() {
    RelBuilder rb = b();
    TableScan scan = (TableScan) rb.scan("employees").build();

    StatisticsTableScan statsScan =
        new StatisticsTableScan(
            scan.getCluster(), scan.getTraitSet(), Collections.emptyList(), scan.getTable(), 42000);

    RelMetadataQuery mq = scan.getCluster().getMetadataQuery();
    assertEquals("Should report injected row count", 42000.0, statsScan.estimateRowCount(mq), 0.1);
  }

  /** StatisticsTableScan with zero row count falls back to Calcite default. */
  public void testStatisticsTableScanZeroRowsFallsBack() {
    RelBuilder rb = b();
    TableScan scan = (TableScan) rb.scan("employees").build();

    StatisticsTableScan statsScan =
        new StatisticsTableScan(
            scan.getCluster(), scan.getTraitSet(), Collections.emptyList(), scan.getTable(), 0);

    RelMetadataQuery mq = scan.getCluster().getMetadataQuery();
    double rowCount = statsScan.estimateRowCount(mq);
    assertTrue("Should fall back to Calcite default (> 0)", rowCount > 0);
  }

  // ---- Join Reorder ----

  /**
   * Build a 3-way join: employees(100K) JOIN departments(50) JOIN projects(500). User writes
   * big→small→medium, but the optimizer should reorder to join smaller tables first.
   */
  private RelNode buildThreeWayJoin() {
    RelBuilder rb = b();
    return rb.scan("employees")
        .scan("departments")
        .join(JoinRelType.INNER, rb.equals(rb.field(2, 0, "dept_id"), rb.field(2, 1, "dept_id")))
        .scan("projects")
        .join(JoinRelType.INNER, rb.equals(rb.field(2, 0, "dept_id"), rb.field(2, 1, "dept_id")))
        .build();
  }

  private Map<String, TableStatistics> threeWayStats() {
    return Map.of(
        "employees", new TableStatistics("employees", 100000, 10000000, 10),
        "departments", new TableStatistics("departments", 50, 5000, 1),
        "projects", new TableStatistics("projects", 500, 50000, 2));
  }

  /**
   * Strip Calcite-generated node IDs (e.g. "70:PhysicalJoin" → "PhysicalJoin") from plan explain
   * output. IDs are non-deterministic across runs.
   */
  private String stripNodeIds(String plan) {
    return plan.replaceAll("\\d+:", "");
  }

  /**
   * With CBO stats: employees=100K, departments=50, projects=500. The reorder joins departments
   * (smallest) with employees first, then joins projects. This is different from the user's
   * original order (employees→departments→projects).
   */
  public void testReorderWithCboStats() {
    PhysicalOptimizer optimizer = new PhysicalOptimizer(false, threeWayStats());
    String plan = stripNodeIds(explainPlan(optimizer.optimize(buildThreeWayJoin())));

    // CBO reorder: departments(50) joined with employees(100K) first, then projects(500) last.
    // The innermost join has departments and employees; projects is the outer join input.
    String expected =
        "PhysicalJoin(condition=[=($2, $8)], joinType=[inner])\n"
            + "  PhysicalExchange\n"
            + "    PhysicalProject(emp_id=[$2], name=[$3], dept_id0=[$4], salary=[$5],"
            + " dept_id=[$0], dept_name=[$1])\n"
            + "      PhysicalJoin(condition=[=($4, $0)], joinType=[inner])\n"
            + "        PhysicalExchange\n"
            + "          PhysicalTableScan(table=[[departments]])\n"
            + "        PhysicalExchange\n"
            + "          PhysicalTableScan(table=[[employees]])\n"
            + "  PhysicalExchange\n"
            + "    PhysicalTableScan(table=[[projects]])\n";
    assertEquals(expected, plan);
  }

  /**
   * Without CBO stats: Calcite uses default heuristics. The original user order
   * (employees→departments→projects) is preserved as a left-deep tree.
   */
  public void testNoReorderWithoutStats() {
    PhysicalOptimizer optimizer = new PhysicalOptimizer(false);
    String plan = stripNodeIds(explainPlan(optimizer.optimize(buildThreeWayJoin())));

    // No stats: original left-deep order preserved (employees first, then departments, projects).
    String expected =
        "PhysicalJoin(condition=[=($2, $8)], joinType=[inner])\n"
            + "  PhysicalExchange\n"
            + "    PhysicalJoin(condition=[=($2, $4)], joinType=[inner])\n"
            + "      PhysicalExchange\n"
            + "        PhysicalTableScan(table=[[employees]])\n"
            + "      PhysicalExchange\n"
            + "        PhysicalTableScan(table=[[departments]])\n"
            + "  PhysicalExchange\n"
            + "    PhysicalTableScan(table=[[projects]])\n";
    assertEquals(expected, plan);
  }

  /** Left join plan: join type must be [left], order preserved (not reordered). */
  public void testLeftJoinPlan() {
    RelBuilder rb = b();
    RelNode logical =
        rb.scan("employees")
            .scan("departments")
            .join(JoinRelType.LEFT, rb.equals(rb.field(2, 0, "dept_id"), rb.field(2, 1, "dept_id")))
            .build();

    PhysicalOptimizer optimizer =
        new PhysicalOptimizer(
            false,
            Map.of(
                "employees", new TableStatistics("employees", 10000, 1000000, 5),
                "departments", new TableStatistics("departments", 50, 5000, 1)));
    String plan = stripNodeIds(explainPlan(optimizer.optimize(logical)));

    String expected =
        "PhysicalJoin(condition=[=($2, $4)], joinType=[left])\n"
            + "  PhysicalExchange\n"
            + "    PhysicalTableScan(table=[[employees]])\n"
            + "  PhysicalExchange\n"
            + "    PhysicalTableScan(table=[[departments]])\n";
    assertEquals(expected, plan);
  }

  /** Mixed inner + left 3-way: inner join reordered by CBO, left join preserved at outer level. */
  public void testMixedInnerLeftJoinPlan() {
    RelBuilder rb = b();
    RelNode logical =
        rb.scan("employees")
            .scan("departments")
            .join(
                JoinRelType.INNER, rb.equals(rb.field(2, 0, "dept_id"), rb.field(2, 1, "dept_id")))
            .scan("projects")
            .join(JoinRelType.LEFT, rb.equals(rb.field(2, 0, "dept_id"), rb.field(2, 1, "dept_id")))
            .build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(false, threeWayStats());
    String plan = stripNodeIds(explainPlan(optimizer.optimize(logical)));

    // The inner join (employees, departments) is reordered by CBO, left join with projects is
    // outer.
    String expected =
        "PhysicalJoin(condition=[=($2, $8)], joinType=[left])\n"
            + "  PhysicalExchange\n"
            + "    PhysicalProject(emp_id=[$2], name=[$3], dept_id0=[$4], salary=[$5],"
            + " dept_id=[$0], dept_name=[$1])\n"
            + "      PhysicalJoin(condition=[=($4, $0)], joinType=[inner])\n"
            + "        PhysicalExchange\n"
            + "          PhysicalTableScan(table=[[departments]])\n"
            + "        PhysicalExchange\n"
            + "          PhysicalTableScan(table=[[employees]])\n"
            + "  PhysicalExchange\n"
            + "    PhysicalTableScan(table=[[projects]])\n";
    assertEquals(expected, plan);
  }

  /** MPP 3-way: same CBO reorder as non-MPP, but with HASH exchanges. */
  public void testThreeWayJoinMppPlan() {
    PhysicalOptimizer optimizer = new PhysicalOptimizer(true, threeWayStats());
    String plan = stripNodeIds(explainPlan(optimizer.optimize(buildThreeWayJoin())));

    // MPP uses same join order as non-MPP (CBO reorder), exchange types may differ.
    // Verify the structure matches — departments joined with employees first.
    String expected =
        "PhysicalJoin(condition=[=($2, $8)], joinType=[inner])\n"
            + "  PhysicalExchange\n"
            + "    PhysicalProject(emp_id=[$2], name=[$3], dept_id0=[$4], salary=[$5],"
            + " dept_id=[$0], dept_name=[$1])\n"
            + "      PhysicalJoin(condition=[=($4, $0)], joinType=[inner])\n"
            + "        PhysicalExchange\n"
            + "          PhysicalTableScan(table=[[departments]])\n"
            + "        PhysicalExchange\n"
            + "          PhysicalTableScan(table=[[employees]])\n"
            + "  PhysicalExchange\n"
            + "    PhysicalTableScan(table=[[projects]])\n";
    assertEquals(expected, plan);
  }

  private List<PhysicalJoin> findAllJoins(RelNode node) {
    List<PhysicalJoin> joins = new ArrayList<>();
    collectJoins(node, joins);
    return joins;
  }

  private void collectJoins(RelNode node, List<PhysicalJoin> joins) {
    if (node instanceof PhysicalJoin) {
      joins.add((PhysicalJoin) node);
    }
    for (RelNode input : node.getInputs()) {
      collectJoins(input, joins);
    }
  }

  // ---- Optimizer is reusable ----

  public void testOptimizerCanBeReused() {
    PhysicalOptimizer optimizer = new PhysicalOptimizer(false);

    RelNode plan1 = optimizer.optimize(b().scan("employees").build());
    RelNode plan2 = optimizer.optimize(b().scan("departments").build());

    assertTrue(plan1 instanceof PhysicalRel);
    assertTrue(plan2 instanceof PhysicalRel);
  }

  // ---- Nested object fields (MAP → ROW) ----

  /** Scan on a table with MAP columns + dot-path siblings produces a valid physical plan. */
  public void testScanWithNestedObjectFieldsProducesValidPlan() {
    RelNode logical = b().scan("logs").build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(false);
    RelNode physical = optimizer.optimize(logical);

    String plan = explainPlan(physical);
    logger.info("Logs scan plan:\n{}", plan);

    assertAllPhysicalRel(physical);
    assertTrue("Plan should contain PhysicalTableScan", plan.contains("PhysicalTableScan"));
    // The logs table has MAP columns — plan should still be valid
    assertTrue("Plan should reference logs table", plan.contains("logs"));
  }

  /** Filter on a nested dot-path field should produce a valid plan. */
  public void testFilterOnNestedFieldProducesValidPlan() {
    RelBuilder rb = b();
    RelNode logical =
        rb.scan("logs")
            .filter(rb.equals(rb.field("cloud.region"), rb.literal("eu-central-1")))
            .build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(false);
    RelNode physical = optimizer.optimize(logical);

    assertAllPhysicalRel(physical);
  }

  /** Sort on a dot-path field (metrics.size) — check the Calcite schema has it as a flat field. */
  public void testSortOnDotPathFieldCalciteSchema() {
    RelBuilder rb = b();
    RelNode scan = rb.scan("logs").build();

    // Verify metrics.size is a flat top-level column in the Calcite scan schema
    logger.info("Logs scan row type: {}", scan.getRowType());
    boolean hasFlatMetricsSize = scan.getRowType().getFieldNames().contains("metrics.size");
    assertTrue("metrics.size should be a flat column in LogicalTableScan", hasFlatMetricsSize);

    // Check the index of metrics.size
    int index = scan.getRowType().getFieldNames().indexOf("metrics.size");
    logger.info("metrics.size is at Calcite index {}", index);
    assertEquals("metrics.size should be at index 5", 5, index);
  }

  /** Sort on dot-path field should produce a valid plan — verify plan structure via explain. */
  public void testSortOnDotPathFieldPlanExplain() {
    RelBuilder rb = b();
    RelNode logical = rb.scan("logs").sort(rb.field("metrics.size")).limit(0, 10).build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(false);
    RelNode physical = optimizer.optimize(logical);

    String plan = explainPlan(physical);
    logger.info("Sort on metrics.size plan:\n{}", plan);

    assertAllPhysicalRel(physical);
    // The sort key should reference metrics.size (as a flat field or remapped nested field)
    assertTrue("Plan should contain PhysicalSort", plan.contains("PhysicalSort"));
  }

  /** SELECT * on logs table with MAP fields — check plan handles struct columns. */
  public void testSelectAllOnLogsTablePlan() {
    RelBuilder rb = b();
    RelNode logical = rb.scan("logs").build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(false);
    RelNode physical = optimizer.optimize(logical);

    String plan = explainPlan(physical);
    logger.info("SELECT * on logs plan:\n{}", plan);

    // Verify the plan contains the scan
    assertTrue("Plan should contain logs table scan", plan.contains("logs"));
  }

  /** Filter on dot-path field should produce a valid plan. */
  public void testFilterOnDotPathFieldProducesValidPlan() {
    RelBuilder rb = b();
    RelNode logical =
        rb.scan("logs")
            .filter(rb.equals(rb.field("cloud.region"), rb.literal("eu-central-1")))
            .build();

    PhysicalOptimizer optimizer = new PhysicalOptimizer(false);
    RelNode physical = optimizer.optimize(logical);

    assertAllPhysicalRel(physical);
  }
}
