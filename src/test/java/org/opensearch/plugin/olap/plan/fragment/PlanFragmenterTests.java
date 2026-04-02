/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.fragment;

import java.util.Collections;
import java.util.List;
import org.boostscale.velox4j.aggregate.Aggregate;
import org.boostscale.velox4j.aggregate.AggregateStep;
import org.boostscale.velox4j.connector.ExternalStreamTableHandle;
import org.boostscale.velox4j.expression.CallTypedExpr;
import org.boostscale.velox4j.expression.FieldAccessTypedExpr;
import org.boostscale.velox4j.plan.AggregationNode;
import org.boostscale.velox4j.plan.FilterNode;
import org.boostscale.velox4j.plan.LimitNode;
import org.boostscale.velox4j.plan.PlanNode;
import org.boostscale.velox4j.plan.ProjectNode;
import org.boostscale.velox4j.plan.TableScanNode;
import org.boostscale.velox4j.type.BigIntType;
import org.boostscale.velox4j.type.DoubleType;
import org.boostscale.velox4j.type.IntegerType;
import org.boostscale.velox4j.type.RowType;
import org.boostscale.velox4j.type.Type;
import org.opensearch.test.OpenSearchTestCase;

public class PlanFragmenterTests extends OpenSearchTestCase {

  private static final String SOURCE_INDEX = "test-index";
  private static final String CONNECTOR_ID = "connector-external-stream";

  // Build a minimal TableScanNode
  private TableScanNode buildTableScan(String nodeId) {
    RowType outputType =
        new RowType(List.of("age", "salary"), List.of(new IntegerType(), new DoubleType()));
    ExternalStreamTableHandle handle = new ExternalStreamTableHandle(CONNECTOR_ID);
    return new TableScanNode(nodeId, outputType, handle, Collections.emptyList());
  }

  // Build a SINGLE-step aggregation node (count(*) on age, grouped by nothing)
  private AggregationNode buildSingleAggNode(String nodeId, PlanNode source) {
    FieldAccessTypedExpr ageRef = FieldAccessTypedExpr.create(new IntegerType(), "age");
    CallTypedExpr countCall =
        new CallTypedExpr(new BigIntType(), Collections.singletonList(ageRef), "count");
    Aggregate countAgg =
        new Aggregate(
            countCall,
            Collections.singletonList(new IntegerType()),
            null,
            Collections.emptyList(),
            Collections.emptyList(),
            false);

    return new AggregationNode(
        nodeId,
        AggregateStep.SINGLE,
        Collections.emptyList(), // no grouping keys
        Collections.emptyList(),
        List.of("count_age"),
        List.of(countAgg),
        false,
        false,
        Collections.singletonList(source),
        null,
        Collections.emptyList());
  }

  // Build a SINGLE-step avg aggregation
  private AggregationNode buildAvgAggNode(String nodeId, PlanNode source) {
    FieldAccessTypedExpr salaryRef = FieldAccessTypedExpr.create(new DoubleType(), "salary");
    CallTypedExpr avgCall =
        new CallTypedExpr(new DoubleType(), Collections.singletonList(salaryRef), "avg");
    Aggregate avgAgg =
        new Aggregate(
            avgCall,
            Collections.singletonList(new DoubleType()),
            null,
            Collections.emptyList(),
            Collections.emptyList(),
            false);

    return new AggregationNode(
        nodeId,
        AggregateStep.SINGLE,
        Collections.emptyList(),
        Collections.emptyList(),
        List.of("avg_salary"),
        List.of(avgAgg),
        false,
        false,
        Collections.singletonList(source),
        null,
        Collections.emptyList());
  }

  // -------------------------------------------------------------------
  // Single-fragment plan (no aggregation)
  // -------------------------------------------------------------------

  public void testNoAggregationProducesSingleFragment() {
    PlanFragmenter fragmenter = new PlanFragmenter();
    TableScanNode scan = buildTableScan("0");

    List<PlanFragment> fragments = fragmenter.fragment(scan, SOURCE_INDEX);

    assertEquals(1, fragments.size());
    PlanFragment frag = fragments.get(0);
    assertEquals(0, frag.getFragmentId());
    assertTrue(frag.isLeaf());
    assertFalse(frag.isRoot());
    assertEquals(FragmentProperties.Distribution.SOURCE, frag.getProperties().getDistribution());
    assertEquals(SOURCE_INDEX, frag.getProperties().getSourceIndex());
  }

  public void testFilterOnlyPlanProducesSingleFragment() {
    PlanFragmenter fragmenter = new PlanFragmenter();
    TableScanNode scan = buildTableScan("0");
    FieldAccessTypedExpr ageRef = FieldAccessTypedExpr.create(new IntegerType(), "age");
    CallTypedExpr filterExpr =
        new CallTypedExpr(
            new org.boostscale.velox4j.type.BooleanType(),
            List.of(ageRef, new CallTypedExpr(new IntegerType(), Collections.emptyList(), "30")),
            "greaterthan");
    FilterNode filter = new FilterNode("1", Collections.singletonList(scan), filterExpr);

    List<PlanFragment> fragments = fragmenter.fragment(filter, SOURCE_INDEX);

    assertEquals(1, fragments.size());
    assertEquals(0, fragments.get(0).getFragmentId());
  }

  // -------------------------------------------------------------------
  // Two-fragment plan (with SINGLE aggregation → PARTIAL + FINAL split)
  // -------------------------------------------------------------------

  public void testAggregationPlanProducesTwoFragments() {
    PlanFragmenter fragmenter = new PlanFragmenter();
    TableScanNode scan = buildTableScan("0");
    AggregationNode agg = buildSingleAggNode("1", scan);

    List<PlanFragment> fragments = fragmenter.fragment(agg, SOURCE_INDEX);

    assertEquals(2, fragments.size());
  }

  public void testLeafFragmentContainsPartialAgg() {
    PlanFragmenter fragmenter = new PlanFragmenter();
    TableScanNode scan = buildTableScan("0");
    AggregationNode agg = buildSingleAggNode("1", scan);

    List<PlanFragment> fragments = fragmenter.fragment(agg, SOURCE_INDEX);

    PlanFragment leafFrag = fragments.get(0);
    assertTrue(leafFrag.isLeaf());
    assertEquals(
        FragmentProperties.Distribution.SOURCE, leafFrag.getProperties().getDistribution());

    PlanNode leafRoot = leafFrag.getPlanRoot();
    assertTrue(leafRoot instanceof AggregationNode);
    assertEquals(AggregateStep.PARTIAL, ((AggregationNode) leafRoot).getStep());
  }

  public void testRootFragmentContainsFinalAgg() {
    PlanFragmenter fragmenter = new PlanFragmenter();
    TableScanNode scan = buildTableScan("0");
    AggregationNode agg = buildSingleAggNode("1", scan);

    List<PlanFragment> fragments = fragmenter.fragment(agg, SOURCE_INDEX);

    PlanFragment rootFrag = fragments.get(1);
    assertTrue(rootFrag.isRoot());
    assertEquals(
        FragmentProperties.Distribution.COORDINATOR, rootFrag.getProperties().getDistribution());

    PlanNode rootNode = rootFrag.getPlanRoot();
    assertTrue(rootNode instanceof AggregationNode);
    assertEquals(AggregateStep.FINAL, ((AggregationNode) rootNode).getStep());
  }

  public void testRootFragmentDependsOnLeafFragment() {
    PlanFragmenter fragmenter = new PlanFragmenter();
    TableScanNode scan = buildTableScan("0");
    AggregationNode agg = buildSingleAggNode("1", scan);

    List<PlanFragment> fragments = fragmenter.fragment(agg, SOURCE_INDEX);

    PlanFragment leafFrag = fragments.get(0);
    PlanFragment rootFrag = fragments.get(1);
    assertTrue(rootFrag.getInputFragmentIds().contains(leafFrag.getFragmentId()));
  }

  public void testPartialAggHasNodeIdSuffix() {
    PlanFragmenter fragmenter = new PlanFragmenter();
    TableScanNode scan = buildTableScan("0");
    AggregationNode agg = buildSingleAggNode("agg1", scan);

    List<PlanFragment> fragments = fragmenter.fragment(agg, SOURCE_INDEX);

    AggregationNode partial = (AggregationNode) fragments.get(0).getPlanRoot();
    assertTrue(partial.getId().contains("_partial"));
  }

  public void testFinalAggHasNodeIdSuffix() {
    PlanFragmenter fragmenter = new PlanFragmenter();
    TableScanNode scan = buildTableScan("0");
    AggregationNode agg = buildSingleAggNode("agg1", scan);

    List<PlanFragment> fragments = fragmenter.fragment(agg, SOURCE_INDEX);

    AggregationNode finalAgg = (AggregationNode) fragments.get(1).getPlanRoot();
    assertTrue(finalAgg.getId().contains("_final"));
  }

  public void testFinalAggHasEmptySources() {
    // FINAL agg sources are wired during execution — must be empty here
    PlanFragmenter fragmenter = new PlanFragmenter();
    TableScanNode scan = buildTableScan("0");
    AggregationNode agg = buildSingleAggNode("agg1", scan);

    List<PlanFragment> fragments = fragmenter.fragment(agg, SOURCE_INDEX);

    AggregationNode finalAgg = (AggregationNode) fragments.get(1).getPlanRoot();
    assertTrue(finalAgg.getSources().isEmpty());
  }

  public void testFinalAggPreservesAggregateNames() {
    PlanFragmenter fragmenter = new PlanFragmenter();
    TableScanNode scan = buildTableScan("0");
    AggregationNode agg = buildSingleAggNode("agg1", scan);

    List<PlanFragment> fragments = fragmenter.fragment(agg, SOURCE_INDEX);

    AggregationNode finalAgg = (AggregationNode) fragments.get(1).getPlanRoot();
    assertEquals(List.of("count_age"), finalAgg.getAggregateNames());
  }

  public void testCountIntermediateTypeIsBigInt() {
    // count intermediate type must be BIGINT
    PlanFragmenter fragmenter = new PlanFragmenter();
    TableScanNode scan = buildTableScan("0");
    AggregationNode agg = buildSingleAggNode("agg1", scan);

    List<PlanFragment> fragments = fragmenter.fragment(agg, SOURCE_INDEX);

    AggregationNode partial = (AggregationNode) fragments.get(0).getPlanRoot();
    Aggregate partialAgg = partial.getAggregates().get(0);
    Type intermediateType = partialAgg.getCall().getReturnType();
    assertTrue(
        "count intermediate type should be BigIntType", intermediateType instanceof BigIntType);
  }

  public void testAvgIntermediateTypeIsRow() {
    // avg intermediate type must be ROW(sum DOUBLE, count BIGINT)
    PlanFragmenter fragmenter = new PlanFragmenter();
    TableScanNode scan = buildTableScan("0");
    AggregationNode agg = buildAvgAggNode("agg1", scan);

    List<PlanFragment> fragments = fragmenter.fragment(agg, SOURCE_INDEX);

    AggregationNode partial = (AggregationNode) fragments.get(0).getPlanRoot();
    Aggregate partialAgg = partial.getAggregates().get(0);
    Type intermediateType = partialAgg.getCall().getReturnType();
    assertTrue("avg intermediate type should be RowType", intermediateType instanceof RowType);
    RowType rowType = (RowType) intermediateType;
    assertEquals(List.of("sum", "count"), rowType.getNames());
    assertTrue(rowType.getChildren().get(0) instanceof DoubleType);
    assertTrue(rowType.getChildren().get(1) instanceof BigIntType);
  }

  public void testFinalAggCallReferencesIntermediateByName() {
    // FINAL agg call must reference the intermediate column by field name (via
    // FieldAccessTypedExpr)
    PlanFragmenter fragmenter = new PlanFragmenter();
    TableScanNode scan = buildTableScan("0");
    AggregationNode agg = buildSingleAggNode("agg1", scan);

    List<PlanFragment> fragments = fragmenter.fragment(agg, SOURCE_INDEX);

    AggregationNode finalAgg = (AggregationNode) fragments.get(1).getPlanRoot();
    Aggregate finalAggCall = finalAgg.getAggregates().get(0);
    List<?> inputs = finalAggCall.getCall().getInputs();
    assertEquals(1, inputs.size());
    assertTrue(inputs.get(0) instanceof FieldAccessTypedExpr);
    FieldAccessTypedExpr ref = (FieldAccessTypedExpr) inputs.get(0);
    // Should reference the aggregate output name "count_age"
    assertEquals("count_age", ref.getFieldName());
  }

  // -------------------------------------------------------------------
  // Plans with operators above the aggregation
  // -------------------------------------------------------------------

  public void testProjectAboveAggSplitsCorrectly() {
    PlanFragmenter fragmenter = new PlanFragmenter();
    TableScanNode scan = buildTableScan("0");
    AggregationNode agg = buildSingleAggNode("1", scan);

    // Project wrapping the agg
    FieldAccessTypedExpr countRef = FieldAccessTypedExpr.create(new BigIntType(), "count_age");
    ProjectNode project =
        new ProjectNode(
            "2",
            Collections.singletonList(agg),
            List.of("total"),
            Collections.singletonList(countRef));

    List<PlanFragment> fragments = fragmenter.fragment(project, SOURCE_INDEX);

    assertEquals(2, fragments.size());

    // Root fragment should be ProjectNode wrapping FINAL AggregationNode
    PlanNode rootNode = fragments.get(1).getPlanRoot();
    assertTrue(rootNode instanceof ProjectNode);

    // Leaf fragment should be PARTIAL AggregationNode
    PlanNode leafNode = fragments.get(0).getPlanRoot();
    assertTrue(leafNode instanceof AggregationNode);
    assertEquals(AggregateStep.PARTIAL, ((AggregationNode) leafNode).getStep());
  }

  public void testLimitAboveAggSplitsCorrectly() {
    PlanFragmenter fragmenter = new PlanFragmenter();
    TableScanNode scan = buildTableScan("0");
    AggregationNode agg = buildSingleAggNode("1", scan);

    LimitNode limit = new LimitNode("2", Collections.singletonList(agg), 0, 100, false);

    List<PlanFragment> fragments = fragmenter.fragment(limit, SOURCE_INDEX);

    assertEquals(2, fragments.size());

    PlanNode rootNode = fragments.get(1).getPlanRoot();
    assertTrue(rootNode instanceof LimitNode);

    PlanNode leafNode = fragments.get(0).getPlanRoot();
    assertTrue(leafNode instanceof AggregationNode);
    assertEquals(AggregateStep.PARTIAL, ((AggregationNode) leafNode).getStep());
  }

  // -------------------------------------------------------------------
  // Re-fragmentation after reset
  // -------------------------------------------------------------------

  public void testFragmenterCanBeReused() {
    PlanFragmenter fragmenter = new PlanFragmenter();

    TableScanNode scan1 = buildTableScan("0");
    List<PlanFragment> first = fragmenter.fragment(scan1, "index1");
    assertEquals(1, first.size());
    assertEquals(0, first.get(0).getFragmentId());

    TableScanNode scan2 = buildTableScan("0");
    AggregationNode agg2 = buildSingleAggNode("1", scan2);
    List<PlanFragment> second = fragmenter.fragment(agg2, "index2");
    // Counter resets between calls, so IDs start from 0 again
    assertEquals(2, second.size());
    assertEquals(0, second.get(0).getFragmentId());
    assertEquals(1, second.get(1).getFragmentId());
  }

  // -------------------------------------------------------------------
  // Sum intermediate type tests
  // -------------------------------------------------------------------

  public void testSumIntegerIntermediateTypeIsBigInt() {
    PlanFragmenter fragmenter = new PlanFragmenter();
    TableScanNode scan = buildTableScan("0");

    // Build sum(age) where age is INTEGER
    FieldAccessTypedExpr ageRef = FieldAccessTypedExpr.create(new IntegerType(), "age");
    CallTypedExpr sumCall =
        new CallTypedExpr(new BigIntType(), Collections.singletonList(ageRef), "sum");
    Aggregate sumAgg =
        new Aggregate(
            sumCall,
            Collections.singletonList(new IntegerType()),
            null,
            Collections.emptyList(),
            Collections.emptyList(),
            false);

    AggregationNode agg =
        new AggregationNode(
            "agg1",
            AggregateStep.SINGLE,
            Collections.emptyList(),
            Collections.emptyList(),
            List.of("sum_age"),
            List.of(sumAgg),
            false,
            false,
            Collections.singletonList(scan),
            null,
            Collections.emptyList());

    List<PlanFragment> fragments = fragmenter.fragment(agg, SOURCE_INDEX);

    AggregationNode partial = (AggregationNode) fragments.get(0).getPlanRoot();
    Type intermediateType = partial.getAggregates().get(0).getCall().getReturnType();
    assertTrue(
        "sum(integer) intermediate should be BigIntType", intermediateType instanceof BigIntType);
  }

  public void testSumDoubleIntermediateTypeIsDouble() {
    PlanFragmenter fragmenter = new PlanFragmenter();
    TableScanNode scan = buildTableScan("0");

    // Build sum(salary) where salary is DOUBLE
    FieldAccessTypedExpr salaryRef = FieldAccessTypedExpr.create(new DoubleType(), "salary");
    CallTypedExpr sumCall =
        new CallTypedExpr(new DoubleType(), Collections.singletonList(salaryRef), "sum");
    Aggregate sumAgg =
        new Aggregate(
            sumCall,
            Collections.singletonList(new DoubleType()),
            null,
            Collections.emptyList(),
            Collections.emptyList(),
            false);

    AggregationNode agg =
        new AggregationNode(
            "agg1",
            AggregateStep.SINGLE,
            Collections.emptyList(),
            Collections.emptyList(),
            List.of("sum_salary"),
            List.of(sumAgg),
            false,
            false,
            Collections.singletonList(scan),
            null,
            Collections.emptyList());

    List<PlanFragment> fragments = fragmenter.fragment(agg, SOURCE_INDEX);

    AggregationNode partial = (AggregationNode) fragments.get(0).getPlanRoot();
    Type intermediateType = partial.getAggregates().get(0).getCall().getReturnType();
    assertTrue(
        "sum(double) intermediate should be DoubleType", intermediateType instanceof DoubleType);
  }

  public void testMinMaxIntermediateTypeSameAsFinalType() {
    PlanFragmenter fragmenter = new PlanFragmenter();
    TableScanNode scan = buildTableScan("0");

    FieldAccessTypedExpr ageRef = FieldAccessTypedExpr.create(new IntegerType(), "age");
    CallTypedExpr maxCall =
        new CallTypedExpr(new IntegerType(), Collections.singletonList(ageRef), "max");
    Aggregate maxAgg =
        new Aggregate(
            maxCall,
            Collections.singletonList(new IntegerType()),
            null,
            Collections.emptyList(),
            Collections.emptyList(),
            false);

    AggregationNode agg =
        new AggregationNode(
            "agg1",
            AggregateStep.SINGLE,
            Collections.emptyList(),
            Collections.emptyList(),
            List.of("max_age"),
            List.of(maxAgg),
            false,
            false,
            Collections.singletonList(scan),
            null,
            Collections.emptyList());

    List<PlanFragment> fragments = fragmenter.fragment(agg, SOURCE_INDEX);

    AggregationNode partial = (AggregationNode) fragments.get(0).getPlanRoot();
    Type intermediateType = partial.getAggregates().get(0).getCall().getReturnType();
    // min/max intermediate is same as final type → IntegerType
    assertTrue("max intermediate should be IntegerType", intermediateType instanceof IntegerType);
  }
}
