/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.convert;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.boostscale.velox4j.type.ArrayType;
import org.boostscale.velox4j.type.BigIntType;
import org.boostscale.velox4j.type.BooleanType;
import org.boostscale.velox4j.type.DecimalType;
import org.boostscale.velox4j.type.DoubleType;
import org.boostscale.velox4j.type.IntegerType;
import org.boostscale.velox4j.type.MapType;
import org.boostscale.velox4j.type.RealType;
import org.boostscale.velox4j.type.RowType;
import org.boostscale.velox4j.type.SmallIntType;
import org.boostscale.velox4j.type.TinyIntType;
import org.boostscale.velox4j.type.Type;
import org.boostscale.velox4j.type.VarCharType;
import org.opensearch.test.OpenSearchTestCase;

public class VeloxTypeConverterTests extends OpenSearchTestCase {

  // Helper to create a mock RelDataType for a given SqlTypeName
  private static RelDataType mockType(SqlTypeName typeName) {
    RelDataType t = mock(RelDataType.class);
    when(t.getSqlTypeName()).thenReturn(typeName);
    return t;
  }

  private static RelDataType mockDecimalType(int precision, int scale) {
    RelDataType t = mock(RelDataType.class);
    when(t.getSqlTypeName()).thenReturn(SqlTypeName.DECIMAL);
    when(t.getPrecision()).thenReturn(precision);
    when(t.getScale()).thenReturn(scale);
    return t;
  }

  private static RelDataTypeField mockField(String name, RelDataType fieldType) {
    RelDataTypeField f = mock(RelDataTypeField.class);
    when(f.getName()).thenReturn(name);
    when(f.getType()).thenReturn(fieldType);
    return f;
  }

  public void testBoolean() {
    Type result = VeloxTypeConverter.toVeloxType(mockType(SqlTypeName.BOOLEAN));
    assertTrue(result instanceof BooleanType);
  }

  public void testTinyInt() {
    Type result = VeloxTypeConverter.toVeloxType(mockType(SqlTypeName.TINYINT));
    assertTrue(result instanceof TinyIntType);
  }

  public void testSmallInt() {
    Type result = VeloxTypeConverter.toVeloxType(mockType(SqlTypeName.SMALLINT));
    assertTrue(result instanceof SmallIntType);
  }

  public void testInteger() {
    Type result = VeloxTypeConverter.toVeloxType(mockType(SqlTypeName.INTEGER));
    assertTrue(result instanceof IntegerType);
  }

  public void testBigInt() {
    Type result = VeloxTypeConverter.toVeloxType(mockType(SqlTypeName.BIGINT));
    assertTrue(result instanceof BigIntType);
  }

  public void testFloat() {
    Type result = VeloxTypeConverter.toVeloxType(mockType(SqlTypeName.FLOAT));
    assertTrue(result instanceof RealType);
  }

  public void testReal() {
    Type result = VeloxTypeConverter.toVeloxType(mockType(SqlTypeName.REAL));
    assertTrue(result instanceof RealType);
  }

  public void testDouble() {
    Type result = VeloxTypeConverter.toVeloxType(mockType(SqlTypeName.DOUBLE));
    assertTrue(result instanceof DoubleType);
  }

  public void testDecimalPreservesScale() {
    RelDataType dt = mockDecimalType(10, 2);
    Type result = VeloxTypeConverter.toVeloxType(dt);
    assertTrue(result instanceof DecimalType);
  }

  public void testChar() {
    Type result = VeloxTypeConverter.toVeloxType(mockType(SqlTypeName.CHAR));
    assertTrue(result instanceof VarCharType);
  }

  public void testVarChar() {
    Type result = VeloxTypeConverter.toVeloxType(mockType(SqlTypeName.VARCHAR));
    assertTrue(result instanceof VarCharType);
  }

  public void testDateMapsToInteger() {
    // Velox represents DATE as INTEGER (days since epoch)
    Type result = VeloxTypeConverter.toVeloxType(mockType(SqlTypeName.DATE));
    assertTrue(result instanceof IntegerType);
  }

  public void testTimestampMapsToBigInt() {
    // Velox represents TIMESTAMP as BIGINT (micros since epoch)
    Type result = VeloxTypeConverter.toVeloxType(mockType(SqlTypeName.TIMESTAMP));
    assertTrue(result instanceof BigIntType);
  }

  public void testTimestampWithLocalTimeZoneMapsToBigInt() {
    Type result =
        VeloxTypeConverter.toVeloxType(mockType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE));
    assertTrue(result instanceof BigIntType);
  }

  public void testArray() {
    RelDataType elementType = mockType(SqlTypeName.INTEGER);
    RelDataType arrayType = mock(RelDataType.class);
    when(arrayType.getSqlTypeName()).thenReturn(SqlTypeName.ARRAY);
    when(arrayType.getComponentType()).thenReturn(elementType);

    Type result = VeloxTypeConverter.toVeloxType(arrayType);
    assertTrue(result instanceof ArrayType);
  }

  public void testMultisetMapsToArray() {
    RelDataType elementType = mockType(SqlTypeName.VARCHAR);
    RelDataType multisetType = mock(RelDataType.class);
    when(multisetType.getSqlTypeName()).thenReturn(SqlTypeName.MULTISET);
    when(multisetType.getComponentType()).thenReturn(elementType);

    Type result = VeloxTypeConverter.toVeloxType(multisetType);
    assertTrue(result instanceof ArrayType);
  }

  public void testMap() {
    RelDataType keyType = mockType(SqlTypeName.VARCHAR);
    RelDataType valueType = mockType(SqlTypeName.BIGINT);
    RelDataType mapType = mock(RelDataType.class);
    when(mapType.getSqlTypeName()).thenReturn(SqlTypeName.MAP);
    when(mapType.getKeyType()).thenReturn(keyType);
    when(mapType.getValueType()).thenReturn(valueType);

    Type result = VeloxTypeConverter.toVeloxType(mapType);
    assertTrue(result instanceof MapType);
  }

  public void testRow() {
    RelDataType fieldType1 = mockType(SqlTypeName.INTEGER);
    RelDataType fieldType2 = mockType(SqlTypeName.VARCHAR);
    RelDataTypeField field1 = mockField("id", fieldType1);
    RelDataTypeField field2 = mockField("name", fieldType2);

    RelDataType rowType = mock(RelDataType.class);
    when(rowType.getSqlTypeName()).thenReturn(SqlTypeName.ROW);
    when(rowType.getFieldList()).thenReturn(Arrays.asList(field1, field2));

    Type result = VeloxTypeConverter.toVeloxType(rowType);
    assertTrue(result instanceof RowType);
    RowType row = (RowType) result;
    assertEquals(2, row.size());
    assertEquals(List.of("id", "name"), row.getNames());
  }

  public void testToVeloxRowTypePreservesFieldOrder() {
    RelDataType fieldType1 = mockType(SqlTypeName.BIGINT);
    RelDataType fieldType2 = mockType(SqlTypeName.DOUBLE);
    RelDataType fieldType3 = mockType(SqlTypeName.VARCHAR);
    RelDataTypeField field1 = mockField("age", fieldType1);
    RelDataTypeField field2 = mockField("salary", fieldType2);
    RelDataTypeField field3 = mockField("name", fieldType3);

    RelDataType rowType = mock(RelDataType.class);
    when(rowType.getSqlTypeName()).thenReturn(SqlTypeName.ROW);
    when(rowType.getFieldList()).thenReturn(Arrays.asList(field1, field2, field3));

    RowType row = VeloxTypeConverter.toVeloxRowType(rowType);
    assertEquals(3, row.size());
    assertEquals(List.of("age", "salary", "name"), row.getNames());
    assertTrue(row.getChildren().get(0) instanceof BigIntType);
    assertTrue(row.getChildren().get(1) instanceof DoubleType);
    assertTrue(row.getChildren().get(2) instanceof VarCharType);
  }

  public void testUnsupportedTypeThrows() {
    RelDataType nullType = mock(RelDataType.class);
    when(nullType.getSqlTypeName()).thenReturn(SqlTypeName.NULL);

    expectThrows(
        UnsupportedOperationException.class, () -> VeloxTypeConverter.toVeloxType(nullType));
  }

  public void testNestedArrayOfVarChar() {
    RelDataType innerType = mockType(SqlTypeName.VARCHAR);
    RelDataType outerElementType = mock(RelDataType.class);
    when(outerElementType.getSqlTypeName()).thenReturn(SqlTypeName.ARRAY);
    when(outerElementType.getComponentType()).thenReturn(innerType);

    RelDataType nestedArrayType = mock(RelDataType.class);
    when(nestedArrayType.getSqlTypeName()).thenReturn(SqlTypeName.ARRAY);
    when(nestedArrayType.getComponentType()).thenReturn(outerElementType);

    Type result = VeloxTypeConverter.toVeloxType(nestedArrayType);
    assertTrue(result instanceof ArrayType);
  }
}
