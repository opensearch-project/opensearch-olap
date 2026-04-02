/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.convert;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.boostscale.velox4j.plan.PlanNode;
import org.boostscale.velox4j.plan.TableScanNode;
import org.boostscale.velox4j.type.RowType;
import org.opensearch.test.OpenSearchTestCase;

public class VeloxPlanConverterTests extends OpenSearchTestCase {

  private static RelDataType mockCalciteType(SqlTypeName typeName) {
    RelDataType t = mock(RelDataType.class);
    when(t.getSqlTypeName()).thenReturn(typeName);
    return t;
  }

  private static RelDataTypeField mockField(String name, SqlTypeName typeName) {
    RelDataType type = mockCalciteType(typeName);
    RelDataTypeField f = mock(RelDataTypeField.class);
    when(f.getName()).thenReturn(name);
    when(f.getType()).thenReturn(type);
    return f;
  }

  private static RelDataType buildRowType(List<RelDataTypeField> fields) {
    RelDataType rowType = mock(RelDataType.class);
    when(rowType.getFieldList()).thenReturn(fields);
    when(rowType.getSqlTypeName()).thenReturn(SqlTypeName.ROW);
    return rowType;
  }

  private static TableScan buildMockScan(List<RelDataTypeField> fields) {
    RelDataType rowType = buildRowType(fields);
    TableScan scan = mock(TableScan.class);
    // getRowType() is final in AbstractRelNode and cannot be mocked directly.
    // Inject the rowType field via reflection instead.
    try {
      java.lang.reflect.Field f =
          org.apache.calcite.rel.AbstractRelNode.class.getDeclaredField("rowType");
      f.setAccessible(true);
      f.set(scan, rowType);
    } catch (Exception e) {
      throw new RuntimeException("Failed to inject rowType into mock", e);
    }
    return scan;
  }

  // -------------------------------------------------------------------
  // TableScan → TableScanNode conversion and metadata column filtering
  // -------------------------------------------------------------------

  public void testTableScanWithOnlyUserColumnsNoFiltering() {
    TableScan scan =
        buildMockScan(
            Arrays.asList(
                mockField("age", SqlTypeName.INTEGER),
                mockField("name", SqlTypeName.VARCHAR),
                mockField("salary", SqlTypeName.DOUBLE)));

    VeloxPlanConverter converter = new VeloxPlanConverter();
    PlanNode planNode = converter.convert(scan);

    assertTrue(planNode instanceof TableScanNode);
    TableScanNode tsn = (TableScanNode) planNode;
    RowType outputType = (RowType) tsn.getOutputType();
    assertEquals(List.of("age", "name", "salary"), outputType.getNames());
    assertEquals(3, outputType.size());
  }

  public void testTableScanFiltersOutAllMetadataColumns() {
    // All 6 metadata columns plus 2 user columns
    TableScan scan =
        buildMockScan(
            Arrays.asList(
                mockField("_id", SqlTypeName.VARCHAR),
                mockField("_index", SqlTypeName.VARCHAR),
                mockField("_score", SqlTypeName.DOUBLE),
                mockField("_maxscore", SqlTypeName.DOUBLE),
                mockField("_sort", SqlTypeName.VARCHAR),
                mockField("_routing", SqlTypeName.VARCHAR),
                mockField("name", SqlTypeName.VARCHAR),
                mockField("age", SqlTypeName.INTEGER)));

    VeloxPlanConverter converter = new VeloxPlanConverter();
    PlanNode planNode = converter.convert(scan);

    TableScanNode tsn = (TableScanNode) planNode;
    RowType outputType = (RowType) tsn.getOutputType();
    assertEquals(List.of("name", "age"), outputType.getNames());
    assertEquals(2, outputType.size());
  }

  public void testTableScanFiltersOutIdMetadataColumn() {
    TableScan scan =
        buildMockScan(
            Arrays.asList(
                mockField("_id", SqlTypeName.VARCHAR), mockField("title", SqlTypeName.VARCHAR)));

    VeloxPlanConverter converter = new VeloxPlanConverter();
    PlanNode planNode = converter.convert(scan);

    TableScanNode tsn = (TableScanNode) planNode;
    RowType outputType = (RowType) tsn.getOutputType();
    assertEquals(List.of("title"), outputType.getNames());
  }

  public void testTableScanWithOnlyMetadataColumnsProducesEmptyOutputType() {
    TableScan scan =
        buildMockScan(
            Arrays.asList(
                mockField("_id", SqlTypeName.VARCHAR),
                mockField("_index", SqlTypeName.VARCHAR),
                mockField("_score", SqlTypeName.DOUBLE)));

    VeloxPlanConverter converter = new VeloxPlanConverter();
    PlanNode planNode = converter.convert(scan);

    TableScanNode tsn = (TableScanNode) planNode;
    RowType outputType = (RowType) tsn.getOutputType();
    assertEquals(0, outputType.size());
    assertTrue(outputType.getNames().isEmpty());
  }

  public void testTableScanNodeHasEmptyAssignments() {
    TableScan scan =
        buildMockScan(Collections.singletonList(mockField("value", SqlTypeName.BIGINT)));

    VeloxPlanConverter converter = new VeloxPlanConverter();
    PlanNode planNode = converter.convert(scan);

    TableScanNode tsn = (TableScanNode) planNode;
    assertTrue(tsn.getAssignments().isEmpty());
  }

  public void testConvertResetsIdGeneratorBetweenCalls() {
    TableScan scan = buildMockScan(Collections.singletonList(mockField("id", SqlTypeName.INTEGER)));

    VeloxPlanConverter converter = new VeloxPlanConverter();
    PlanNode first = converter.convert(scan);
    PlanNode second = converter.convert(scan);

    // Both should use ID "0" since the generator resets on each convert() call
    assertEquals("0", ((TableScanNode) first).getId());
    assertEquals("0", ((TableScanNode) second).getId());
  }

  public void testUnsupportedRelNodeThrows() {
    // Use a concrete unknown RelNode type (can't mock Calcite interfaces due to module
    // restrictions)
    org.apache.calcite.plan.hep.HepPlanner planner =
        new org.apache.calcite.plan.hep.HepPlanner(
            org.apache.calcite.plan.hep.HepProgram.builder().build());
    org.apache.calcite.plan.RelOptCluster cluster =
        org.apache.calcite.plan.RelOptCluster.create(
            planner,
            new org.apache.calcite.rex.RexBuilder(
                new org.apache.calcite.jdbc.JavaTypeFactoryImpl()));
    org.apache.calcite.rel.RelNode unknown =
        new org.apache.calcite.rel.AbstractRelNode(cluster, cluster.traitSet()) {};

    VeloxPlanConverter converter = new VeloxPlanConverter();
    expectThrows(UnsupportedOperationException.class, () -> converter.convert(unknown));
  }

  public void testMetadataColumnNamesAreExact() {
    // Verify that columns named similarly but not exactly like metadata columns are NOT filtered
    TableScan scan =
        buildMockScan(
            Arrays.asList(
                mockField("id", SqlTypeName.INTEGER), // "id" != "_id"
                mockField("index", SqlTypeName.INTEGER), // "index" != "_index"
                mockField("score", SqlTypeName.DOUBLE), // "score" != "_score"
                mockField("_id", SqlTypeName.VARCHAR) // this one IS metadata
                ));

    VeloxPlanConverter converter = new VeloxPlanConverter();
    PlanNode planNode = converter.convert(scan);

    TableScanNode tsn = (TableScanNode) planNode;
    RowType outputType = (RowType) tsn.getOutputType();
    assertEquals(3, outputType.size());
    assertTrue(outputType.getNames().contains("id"));
    assertTrue(outputType.getNames().contains("index"));
    assertTrue(outputType.getNames().contains("score"));
    assertFalse(outputType.getNames().contains("_id"));
  }

  public void testIndividualMetadataColumnFiltering() {
    // Each metadata column should be filtered independently
    String[] metadataCols = {"_id", "_index", "_score", "_maxscore", "_sort", "_routing"};
    for (String metaCol : metadataCols) {
      TableScan scan =
          buildMockScan(
              Arrays.asList(
                  mockField(metaCol, SqlTypeName.VARCHAR),
                  mockField("user_col", SqlTypeName.INTEGER)));

      VeloxPlanConverter converter = new VeloxPlanConverter();
      PlanNode planNode = converter.convert(scan);
      TableScanNode tsn = (TableScanNode) planNode;
      RowType outputType = (RowType) tsn.getOutputType();
      assertFalse(
          "Metadata column " + metaCol + " should be filtered",
          outputType.getNames().contains(metaCol));
      assertTrue("User column should remain", outputType.getNames().contains("user_col"));
    }
  }
}
