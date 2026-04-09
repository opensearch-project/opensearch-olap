/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.fragment;

import java.util.Collections;
import java.util.List;
import org.boostscale.velox4j.connector.ExternalStreamTableHandle;
import org.boostscale.velox4j.expression.FieldAccessTypedExpr;
import org.boostscale.velox4j.join.JoinType;
import org.boostscale.velox4j.plan.HashJoinNode;
import org.boostscale.velox4j.plan.LimitNode;
import org.boostscale.velox4j.plan.PlanNode;
import org.boostscale.velox4j.plan.ProjectNode;
import org.boostscale.velox4j.plan.TableScanNode;
import org.boostscale.velox4j.type.DoubleType;
import org.boostscale.velox4j.type.IntegerType;
import org.boostscale.velox4j.type.RowType;
import org.boostscale.velox4j.type.VarCharType;
import org.opensearch.plugin.olap.scheduler.JoinStrategy;
import org.opensearch.test.OpenSearchTestCase;

public class JoinPlanFragmenterTests extends OpenSearchTestCase {

  private static final String CONNECTOR_ID = "connector-external-stream";

  private TableScanNode buildScan(
      String id, List<String> names, List<org.boostscale.velox4j.type.Type> types) {
    RowType outputType = new RowType(names, types);
    return new TableScanNode(
        id, outputType, new ExternalStreamTableHandle(CONNECTOR_ID), Collections.emptyList());
  }

  private HashJoinNode buildHashJoin(
      String id, PlanNode left, PlanNode right, String leftKey, String rightKey) {
    RowType leftType = (RowType) ((TableScanNode) findScan(left)).getOutputType();
    RowType rightType = (RowType) ((TableScanNode) findScan(right)).getOutputType();

    // Build combined output type
    List<String> names = new java.util.ArrayList<>(leftType.getNames());
    List<org.boostscale.velox4j.type.Type> types =
        new java.util.ArrayList<>(leftType.getChildren());
    names.addAll(rightType.getNames());
    types.addAll(rightType.getChildren());
    RowType outputType = new RowType(names, types);

    return new HashJoinNode(
        id,
        JoinType.INNER,
        List.of(FieldAccessTypedExpr.create(new IntegerType(), leftKey)),
        List.of(FieldAccessTypedExpr.create(new IntegerType(), rightKey)),
        null,
        left,
        right,
        outputType,
        false,
        false);
  }

  /** Recursively find the TableScanNode in a plan tree. */
  private PlanNode findScan(PlanNode node) {
    if (node instanceof TableScanNode) return node;
    try {
      java.lang.reflect.Method m = PlanNode.class.getDeclaredMethod("getSources");
      m.setAccessible(true);
      @SuppressWarnings("unchecked")
      List<PlanNode> sources = (List<PlanNode>) m.invoke(node);
      if (sources != null) {
        for (PlanNode s : sources) {
          PlanNode found = findScan(s);
          if (found != null) return found;
        }
      }
    } catch (Exception e) {
      // ignore
    }
    return node;
  }

  // ---- containsJoin detection ----

  public void testContainsJoinDetectsHashJoin() {
    PlanFragmenter fragmenter = new PlanFragmenter();
    TableScanNode leftScan =
        buildScan("l", List.of("id", "name"), List.of(new IntegerType(), new VarCharType()));
    TableScanNode rightScan =
        buildScan("r", List.of("id", "value"), List.of(new IntegerType(), new DoubleType()));
    HashJoinNode join = buildHashJoin("j", leftScan, rightScan, "id", "id");

    assertTrue(fragmenter.containsJoin(join));
  }

  public void testContainsJoinDetectsNestedJoin() {
    PlanFragmenter fragmenter = new PlanFragmenter();
    TableScanNode leftScan =
        buildScan("l", List.of("id", "name"), List.of(new IntegerType(), new VarCharType()));
    TableScanNode rightScan =
        buildScan("r", List.of("id", "value"), List.of(new IntegerType(), new DoubleType()));
    HashJoinNode join = buildHashJoin("j", leftScan, rightScan, "id", "id");

    // Wrap in a project
    ProjectNode project =
        new ProjectNode(
            "p",
            List.of(join),
            List.of("name"),
            List.of(FieldAccessTypedExpr.create(new VarCharType(), "name")));

    assertTrue(fragmenter.containsJoin(project));
  }

  public void testContainsJoinReturnsFalseForNonJoin() {
    PlanFragmenter fragmenter = new PlanFragmenter();
    TableScanNode scan = buildScan("s", List.of("id"), List.of(new IntegerType()));
    assertFalse(fragmenter.containsJoin(scan));
  }

  // ---- Coordinator-Centric Join ----

  public void testCoordinatorCentricProducesThreeFragments() {
    PlanFragmenter fragmenter = new PlanFragmenter();
    TableScanNode leftScan =
        buildScan("l", List.of("id", "name"), List.of(new IntegerType(), new VarCharType()));
    TableScanNode rightScan =
        buildScan("r", List.of("id", "value"), List.of(new IntegerType(), new DoubleType()));
    HashJoinNode join = buildHashJoin("j", leftScan, rightScan, "id", "id");

    List<PlanFragment> fragments =
        fragmenter.fragmentJoin(join, "orders", "customers", JoinStrategy.COORDINATOR_CENTRIC, 0);

    assertEquals(3, fragments.size());
  }

  public void testCoordinatorCentricLeftFragment() {
    PlanFragmenter fragmenter = new PlanFragmenter();
    TableScanNode leftScan =
        buildScan("l", List.of("id", "name"), List.of(new IntegerType(), new VarCharType()));
    TableScanNode rightScan =
        buildScan("r", List.of("id", "value"), List.of(new IntegerType(), new DoubleType()));
    HashJoinNode join = buildHashJoin("j", leftScan, rightScan, "id", "id");

    List<PlanFragment> fragments =
        fragmenter.fragmentJoin(join, "orders", "customers", JoinStrategy.COORDINATOR_CENTRIC, 0);

    PlanFragment leftFrag = fragments.get(0);
    assertEquals(
        FragmentProperties.Distribution.SOURCE, leftFrag.getProperties().getDistribution());
    assertEquals("orders", leftFrag.getProperties().getSourceIndex());
    assertEquals("left", leftFrag.getProperties().getJoinSide());
    assertTrue(leftFrag.isLeaf());
    assertTrue(leftFrag.getPlanRoot() instanceof TableScanNode);
  }

  public void testCoordinatorCentricRightFragment() {
    PlanFragmenter fragmenter = new PlanFragmenter();
    TableScanNode leftScan =
        buildScan("l", List.of("id", "name"), List.of(new IntegerType(), new VarCharType()));
    TableScanNode rightScan =
        buildScan("r", List.of("id", "value"), List.of(new IntegerType(), new DoubleType()));
    HashJoinNode join = buildHashJoin("j", leftScan, rightScan, "id", "id");

    List<PlanFragment> fragments =
        fragmenter.fragmentJoin(join, "orders", "customers", JoinStrategy.COORDINATOR_CENTRIC, 0);

    PlanFragment rightFrag = fragments.get(1);
    assertEquals(
        FragmentProperties.Distribution.SOURCE, rightFrag.getProperties().getDistribution());
    assertEquals("customers", rightFrag.getProperties().getSourceIndex());
    assertEquals("right", rightFrag.getProperties().getJoinSide());
    assertTrue(rightFrag.isLeaf());
  }

  public void testCoordinatorCentricCoordinatorFragment() {
    PlanFragmenter fragmenter = new PlanFragmenter();
    TableScanNode leftScan =
        buildScan("l", List.of("id", "name"), List.of(new IntegerType(), new VarCharType()));
    TableScanNode rightScan =
        buildScan("r", List.of("id", "value"), List.of(new IntegerType(), new DoubleType()));
    HashJoinNode join = buildHashJoin("j", leftScan, rightScan, "id", "id");

    List<PlanFragment> fragments =
        fragmenter.fragmentJoin(join, "orders", "customers", JoinStrategy.COORDINATOR_CENTRIC, 0);

    PlanFragment coordFrag = fragments.get(2);
    assertEquals(
        FragmentProperties.Distribution.COORDINATOR, coordFrag.getProperties().getDistribution());
    assertTrue(coordFrag.isRoot());
    // Dependencies on both leaf fragments
    assertTrue(coordFrag.getInputFragmentIds().contains(0));
    assertTrue(coordFrag.getInputFragmentIds().contains(1));
    // Root plan should be HashJoinNode
    assertTrue(coordFrag.getPlanRoot() instanceof HashJoinNode);
  }

  public void testCoordinatorCentricWithProjectAboveJoin() {
    PlanFragmenter fragmenter = new PlanFragmenter();
    TableScanNode leftScan =
        buildScan("l", List.of("id", "name"), List.of(new IntegerType(), new VarCharType()));
    TableScanNode rightScan =
        buildScan("r", List.of("id", "value"), List.of(new IntegerType(), new DoubleType()));
    HashJoinNode join = buildHashJoin("j", leftScan, rightScan, "id", "id");
    ProjectNode project =
        new ProjectNode(
            "p",
            List.of(join),
            List.of("name"),
            List.of(FieldAccessTypedExpr.create(new VarCharType(), "name")));

    List<PlanFragment> fragments =
        fragmenter.fragmentJoin(
            project, "orders", "customers", JoinStrategy.COORDINATOR_CENTRIC, 0);

    assertEquals(3, fragments.size());
    // Coordinator fragment root should be ProjectNode wrapping HashJoinNode
    PlanNode coordRoot = fragments.get(2).getPlanRoot();
    assertTrue(coordRoot instanceof ProjectNode);
  }

  public void testCoordinatorCentricWithLimitAboveJoin() {
    PlanFragmenter fragmenter = new PlanFragmenter();
    TableScanNode leftScan =
        buildScan("l", List.of("id", "name"), List.of(new IntegerType(), new VarCharType()));
    TableScanNode rightScan =
        buildScan("r", List.of("id", "value"), List.of(new IntegerType(), new DoubleType()));
    HashJoinNode join = buildHashJoin("j", leftScan, rightScan, "id", "id");
    LimitNode limit = new LimitNode("lim", List.of(join), 0, 100, false);

    List<PlanFragment> fragments =
        fragmenter.fragmentJoin(limit, "orders", "customers", JoinStrategy.COORDINATOR_CENTRIC, 0);

    assertEquals(3, fragments.size());
    assertTrue(fragments.get(2).getPlanRoot() instanceof LimitNode);
  }

  // ---- Broadcast Join ----

  public void testBroadcastJoinProducesFragments() {
    PlanFragmenter fragmenter = new PlanFragmenter();
    TableScanNode leftScan =
        buildScan("l", List.of("id", "name"), List.of(new IntegerType(), new VarCharType()));
    TableScanNode rightScan =
        buildScan("r", List.of("id", "value"), List.of(new IntegerType(), new DoubleType()));
    HashJoinNode join = buildHashJoin("j", leftScan, rightScan, "id", "id");

    List<PlanFragment> fragments =
        fragmenter.fragmentJoin(join, "orders", "customers", JoinStrategy.BROADCAST, 0);

    // Without aggregation: build scan + broadcast probe join = 2 fragments
    assertEquals(2, fragments.size());

    // First fragment: build side (SOURCE, right)
    PlanFragment buildFrag = fragments.get(0);
    assertEquals(
        FragmentProperties.Distribution.SOURCE, buildFrag.getProperties().getDistribution());
    assertEquals("right", buildFrag.getProperties().getJoinSide());
    assertTrue(buildFrag.isLeaf());

    // Second fragment: broadcast probe+join
    PlanFragment probeFrag = fragments.get(1);
    assertEquals(
        FragmentProperties.Distribution.BROADCAST, probeFrag.getProperties().getDistribution());
  }

  // ---- Hash Shuffle Join ----

  public void testHashShuffleJoinProducesFragments() {
    PlanFragmenter fragmenter = new PlanFragmenter();
    TableScanNode leftScan =
        buildScan("l", List.of("id", "name"), List.of(new IntegerType(), new VarCharType()));
    TableScanNode rightScan =
        buildScan("r", List.of("id", "value"), List.of(new IntegerType(), new DoubleType()));
    HashJoinNode join = buildHashJoin("j", leftScan, rightScan, "id", "id");

    List<PlanFragment> fragments =
        fragmenter.fragmentJoin(join, "orders", "customers", JoinStrategy.HASH_SHUFFLE, 4);

    // Without aggregation: left scan + right scan + join workers + coordinator = 4 fragments
    assertEquals(4, fragments.size());
  }

  public void testHashShuffleLeftScanFragment() {
    PlanFragmenter fragmenter = new PlanFragmenter();
    TableScanNode leftScan =
        buildScan("l", List.of("id", "name"), List.of(new IntegerType(), new VarCharType()));
    TableScanNode rightScan =
        buildScan("r", List.of("id", "value"), List.of(new IntegerType(), new DoubleType()));
    HashJoinNode join = buildHashJoin("j", leftScan, rightScan, "id", "id");

    List<PlanFragment> fragments =
        fragmenter.fragmentJoin(join, "orders", "customers", JoinStrategy.HASH_SHUFFLE, 4);

    PlanFragment leftFrag = fragments.get(0);
    assertEquals(
        FragmentProperties.Distribution.SOURCE, leftFrag.getProperties().getDistribution());
    assertEquals("left", leftFrag.getProperties().getJoinSide());
    assertTrue(leftFrag.getProperties().isShuffleScan());
    assertEquals(4, leftFrag.getProperties().getPartitionCount());
    // Key channel should be 0 (first column "id")
    assertEquals(List.of(0), leftFrag.getProperties().getShuffleKeyChannels());
  }

  public void testHashShuffleRightScanFragment() {
    PlanFragmenter fragmenter = new PlanFragmenter();
    TableScanNode leftScan =
        buildScan("l", List.of("id", "name"), List.of(new IntegerType(), new VarCharType()));
    TableScanNode rightScan =
        buildScan("r", List.of("id", "value"), List.of(new IntegerType(), new DoubleType()));
    HashJoinNode join = buildHashJoin("j", leftScan, rightScan, "id", "id");

    List<PlanFragment> fragments =
        fragmenter.fragmentJoin(join, "orders", "customers", JoinStrategy.HASH_SHUFFLE, 4);

    PlanFragment rightFrag = fragments.get(1);
    assertEquals("right", rightFrag.getProperties().getJoinSide());
    assertTrue(rightFrag.getProperties().isShuffleScan());
    assertEquals(List.of(0), rightFrag.getProperties().getShuffleKeyChannels());
  }

  public void testHashShuffleJoinFragment() {
    PlanFragmenter fragmenter = new PlanFragmenter();
    TableScanNode leftScan =
        buildScan("l", List.of("id", "name"), List.of(new IntegerType(), new VarCharType()));
    TableScanNode rightScan =
        buildScan("r", List.of("id", "value"), List.of(new IntegerType(), new DoubleType()));
    HashJoinNode join = buildHashJoin("j", leftScan, rightScan, "id", "id");

    List<PlanFragment> fragments =
        fragmenter.fragmentJoin(join, "orders", "customers", JoinStrategy.HASH_SHUFFLE, 4);

    PlanFragment joinFrag = fragments.get(2);
    assertEquals(
        FragmentProperties.Distribution.HASH_PARTITIONED,
        joinFrag.getProperties().getDistribution());
    assertEquals(4, joinFrag.getProperties().getPartitionCount());
    // Dependencies on both scan fragments
    assertTrue(joinFrag.getInputFragmentIds().contains(0));
    assertTrue(joinFrag.getInputFragmentIds().contains(1));
  }

  public void testHashShuffleCoordinatorFragment() {
    PlanFragmenter fragmenter = new PlanFragmenter();
    TableScanNode leftScan =
        buildScan("l", List.of("id", "name"), List.of(new IntegerType(), new VarCharType()));
    TableScanNode rightScan =
        buildScan("r", List.of("id", "value"), List.of(new IntegerType(), new DoubleType()));
    HashJoinNode join = buildHashJoin("j", leftScan, rightScan, "id", "id");

    List<PlanFragment> fragments =
        fragmenter.fragmentJoin(join, "orders", "customers", JoinStrategy.HASH_SHUFFLE, 4);

    PlanFragment coordFrag = fragments.get(3);
    assertEquals(
        FragmentProperties.Distribution.COORDINATOR, coordFrag.getProperties().getDistribution());
    assertTrue(coordFrag.isRoot());
  }

  // ---- FragmentProperties tests ----

  public void testFragmentPropertiesSource() {
    FragmentProperties props = FragmentProperties.source("my-index");
    assertEquals(FragmentProperties.Distribution.SOURCE, props.getDistribution());
    assertEquals("my-index", props.getSourceIndex());
    assertNull(props.getJoinSide());
    assertFalse(props.isShuffleScan());
  }

  public void testFragmentPropertiesSourceWithJoinSide() {
    FragmentProperties props = FragmentProperties.source("my-index", "left");
    assertEquals("left", props.getJoinSide());
  }

  public void testFragmentPropertiesCoordinator() {
    FragmentProperties props = FragmentProperties.coordinator();
    assertEquals(FragmentProperties.Distribution.COORDINATOR, props.getDistribution());
    assertNull(props.getSourceIndex());
  }

  public void testFragmentPropertiesBroadcast() {
    FragmentProperties props = FragmentProperties.broadcast("probe-idx");
    assertEquals(FragmentProperties.Distribution.BROADCAST, props.getDistribution());
    assertEquals("probe-idx", props.getSourceIndex());
  }

  public void testFragmentPropertiesShuffleScan() {
    FragmentProperties props = FragmentProperties.shuffleScan("idx", "left", List.of(0, 2), 8);
    assertEquals(FragmentProperties.Distribution.SOURCE, props.getDistribution());
    assertEquals("idx", props.getSourceIndex());
    assertEquals("left", props.getJoinSide());
    assertTrue(props.isShuffleScan());
    assertEquals(List.of(0, 2), props.getShuffleKeyChannels());
    assertEquals(8, props.getPartitionCount());
  }

  public void testFragmentPropertiesHashPartitioned() {
    FragmentProperties props = FragmentProperties.hashPartitioned(4);
    assertEquals(FragmentProperties.Distribution.HASH_PARTITIONED, props.getDistribution());
    assertEquals(4, props.getPartitionCount());
  }
}
