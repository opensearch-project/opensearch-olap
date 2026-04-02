/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.convert;

import java.util.Collections;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlMinMaxAggFunction;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.boostscale.velox4j.aggregate.Aggregate;
import org.boostscale.velox4j.expression.CallTypedExpr;
import org.boostscale.velox4j.expression.FieldAccessTypedExpr;
import org.boostscale.velox4j.expression.TypedExpr;
import org.boostscale.velox4j.type.BigIntType;
import org.boostscale.velox4j.type.DoubleType;
import org.opensearch.test.OpenSearchTestCase;

public class VeloxAggConverterTests extends OpenSearchTestCase {

  private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

  // Build a simple input row type with fields: age (BIGINT), salary (DOUBLE)
  private RelDataType buildInputRowType() {
    RelDataTypeFactory.Builder builder = typeFactory.builder();
    builder.add("age", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.BIGINT));
    builder.add(
        "salary", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.DOUBLE));
    return builder.build();
  }

  public void testCountStarHasNoArgs() {
    RelDataType rowType = buildInputRowType();
    VeloxAggConverter converter = new VeloxAggConverter(rowType);

    SqlCountAggFunction countFunc = new SqlCountAggFunction("COUNT");
    RelDataType bigintType =
        typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.BIGINT);
    AggregateCall call =
        AggregateCall.create(countFunc, false, Collections.emptyList(), -1, bigintType, null);

    Aggregate agg = converter.convert(call);

    assertNotNull(agg);
    CallTypedExpr callExpr = agg.getCall();
    assertEquals("count", callExpr.getFunctionName());
    assertTrue(callExpr.getInputs().isEmpty());
    assertTrue(agg.getRawInputTypes().isEmpty());
    assertTrue(callExpr.getReturnType() instanceof BigIntType);
  }

  public void testSumOnBigintField() {
    RelDataType rowType = buildInputRowType();
    VeloxAggConverter converter = new VeloxAggConverter(rowType);

    RelDataType bigintType =
        typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.BIGINT);
    SqlSumAggFunction sumFunc = new SqlSumAggFunction(bigintType);
    AggregateCall call =
        AggregateCall.create(sumFunc, false, Collections.singletonList(0), -1, bigintType, null);

    Aggregate agg = converter.convert(call);

    assertNotNull(agg);
    CallTypedExpr callExpr = agg.getCall();
    assertEquals("sum", callExpr.getFunctionName());
    assertEquals(1, callExpr.getInputs().size());
    TypedExpr input = callExpr.getInputs().get(0);
    assertTrue(input instanceof FieldAccessTypedExpr);
    assertEquals("age", ((FieldAccessTypedExpr) input).getFieldName());
    assertEquals(1, agg.getRawInputTypes().size());
    assertTrue(agg.getRawInputTypes().get(0) instanceof BigIntType);
  }

  public void testAvgOnDoubleField() {
    RelDataType rowType = buildInputRowType();
    VeloxAggConverter converter = new VeloxAggConverter(rowType);

    RelDataType doubleType =
        typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.DOUBLE);
    SqlAvgAggFunction avgFunc = new SqlAvgAggFunction(SqlKind.AVG);
    AggregateCall call =
        AggregateCall.create(avgFunc, false, Collections.singletonList(1), -1, doubleType, null);

    Aggregate agg = converter.convert(call);

    assertNotNull(agg);
    CallTypedExpr callExpr = agg.getCall();
    assertEquals("avg", callExpr.getFunctionName());
    assertEquals(1, callExpr.getInputs().size());
    TypedExpr input = callExpr.getInputs().get(0);
    assertTrue(input instanceof FieldAccessTypedExpr);
    assertEquals("salary", ((FieldAccessTypedExpr) input).getFieldName());
    assertTrue(callExpr.getReturnType() instanceof DoubleType);
  }

  public void testMinFunction() {
    RelDataType rowType = buildInputRowType();
    VeloxAggConverter converter = new VeloxAggConverter(rowType);

    RelDataType bigintType =
        typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.BIGINT);
    SqlMinMaxAggFunction minFunc = new SqlMinMaxAggFunction(SqlKind.MIN);
    AggregateCall call =
        AggregateCall.create(minFunc, false, Collections.singletonList(0), -1, bigintType, null);

    Aggregate agg = converter.convert(call);
    assertEquals("min", agg.getCall().getFunctionName());
  }

  public void testMaxFunction() {
    RelDataType rowType = buildInputRowType();
    VeloxAggConverter converter = new VeloxAggConverter(rowType);

    RelDataType bigintType =
        typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.BIGINT);
    SqlMinMaxAggFunction maxFunc = new SqlMinMaxAggFunction(SqlKind.MAX);
    AggregateCall call =
        AggregateCall.create(maxFunc, false, Collections.singletonList(0), -1, bigintType, null);

    Aggregate agg = converter.convert(call);
    assertEquals("max", agg.getCall().getFunctionName());
  }

  public void testSum0MapsToSumFunction() {
    RelDataType rowType = buildInputRowType();
    VeloxAggConverter converter = new VeloxAggConverter(rowType);

    RelDataType bigintType =
        typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.BIGINT);
    SqlSumEmptyIsZeroAggFunction sum0Func = new SqlSumEmptyIsZeroAggFunction();
    AggregateCall call =
        AggregateCall.create(sum0Func, false, Collections.singletonList(0), -1, bigintType, null);

    Aggregate agg = converter.convert(call);
    assertEquals("sum", agg.getCall().getFunctionName());
  }

  public void testDistinctFlagPreserved() {
    RelDataType rowType = buildInputRowType();
    VeloxAggConverter converter = new VeloxAggConverter(rowType);

    RelDataType bigintType =
        typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.BIGINT);
    SqlCountAggFunction countFunc = new SqlCountAggFunction("COUNT");
    AggregateCall call =
        AggregateCall.create(
            countFunc, true /* distinct */, Collections.singletonList(0), -1, bigintType, null);

    Aggregate agg = converter.convert(call);
    assertTrue(agg.isDistinct());
  }

  public void testResolveAggOutputNameUsesCallName() {
    RelDataType rowType = buildInputRowType();
    VeloxAggConverter converter = new VeloxAggConverter(rowType);

    RelDataType bigintType =
        typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.BIGINT);
    SqlCountAggFunction countFunc = new SqlCountAggFunction("COUNT");
    AggregateCall call =
        AggregateCall.create(countFunc, false, Collections.emptyList(), -1, bigintType, "my_count");

    String name = converter.resolveAggOutputName(call, 0);
    assertEquals("my_count", name);
  }

  public void testResolveAggOutputNameFallsBackToIndex() {
    RelDataType rowType = buildInputRowType();
    VeloxAggConverter converter = new VeloxAggConverter(rowType);

    RelDataType bigintType =
        typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.BIGINT);
    SqlCountAggFunction countFunc = new SqlCountAggFunction("COUNT");
    AggregateCall call =
        AggregateCall.create(countFunc, false, Collections.emptyList(), -1, bigintType, null);

    String name = converter.resolveAggOutputName(call, 3);
    assertEquals("agg_3", name);
  }

  public void testCountWithSingleArgField() {
    // COUNT(age) — one arg referring to index 0
    RelDataType rowType = buildInputRowType();
    VeloxAggConverter converter = new VeloxAggConverter(rowType);

    RelDataType bigintType =
        typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.BIGINT);
    SqlCountAggFunction countFunc = new SqlCountAggFunction("COUNT");
    AggregateCall call =
        AggregateCall.create(countFunc, false, Collections.singletonList(0), -1, bigintType, null);

    Aggregate agg = converter.convert(call);
    assertEquals("count", agg.getCall().getFunctionName());
    assertEquals(1, agg.getCall().getInputs().size());
    assertEquals("age", ((FieldAccessTypedExpr) agg.getCall().getInputs().get(0)).getFieldName());
  }

  public void testRawInputTypesMatchFieldTypes() {
    RelDataType rowType = buildInputRowType();
    VeloxAggConverter converter = new VeloxAggConverter(rowType);

    RelDataType doubleType =
        typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.DOUBLE);
    SqlAvgAggFunction avgFunc = new SqlAvgAggFunction(SqlKind.AVG);
    AggregateCall call =
        AggregateCall.create(avgFunc, false, Collections.singletonList(1), -1, doubleType, null);

    Aggregate agg = converter.convert(call);
    assertEquals(1, agg.getRawInputTypes().size());
    assertTrue(agg.getRawInputTypes().get(0) instanceof DoubleType);
  }
}
