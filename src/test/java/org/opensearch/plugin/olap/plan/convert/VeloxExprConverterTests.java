/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.convert;

import java.math.BigDecimal;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.boostscale.velox4j.expression.CallTypedExpr;
import org.boostscale.velox4j.expression.ConstantTypedExpr;
import org.boostscale.velox4j.expression.FieldAccessTypedExpr;
import org.boostscale.velox4j.expression.TypedExpr;
import org.boostscale.velox4j.type.BigIntType;
import org.boostscale.velox4j.type.BooleanType;
import org.boostscale.velox4j.type.DateType;
import org.boostscale.velox4j.type.DoubleType;
import org.boostscale.velox4j.type.IntegerType;
import org.boostscale.velox4j.type.TimestampType;
import org.boostscale.velox4j.type.VarCharType;
import org.boostscale.velox4j.variant.BigIntValue;
import org.boostscale.velox4j.variant.IntegerValue;
import org.boostscale.velox4j.variant.TimestampValue;
import org.boostscale.velox4j.variant.Variant;
import org.opensearch.test.OpenSearchTestCase;

public class VeloxExprConverterTests extends OpenSearchTestCase {

  private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
  private final RexBuilder rexBuilder = new RexBuilder(typeFactory);

  // Build a two-field input row type: (age INTEGER, name VARCHAR)
  private RelDataType buildRowType() {
    RelDataTypeFactory.Builder builder = typeFactory.builder();
    builder.add("age", typeFactory.createSqlType(SqlTypeName.INTEGER));
    builder.add("name", typeFactory.createSqlType(SqlTypeName.VARCHAR, 255));
    return builder.build();
  }

  public void testConvertInputRefToFieldAccess() {
    RelDataType rowType = buildRowType();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);

    RexInputRef ref = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
    TypedExpr result = converter.convert(ref);

    assertTrue(result instanceof FieldAccessTypedExpr);
    assertEquals("age", ((FieldAccessTypedExpr) result).getFieldName());
    assertTrue(result.getReturnType() instanceof IntegerType);
  }

  public void testConvertSecondInputRefToFieldAccess() {
    RelDataType rowType = buildRowType();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);

    RexInputRef ref =
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR, 255), 1);
    TypedExpr result = converter.convert(ref);

    assertTrue(result instanceof FieldAccessTypedExpr);
    assertEquals("name", ((FieldAccessTypedExpr) result).getFieldName());
    assertTrue(result.getReturnType() instanceof VarCharType);
  }

  public void testConvertBooleanLiteral() {
    RelDataType rowType = buildRowType();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);

    RexLiteral literal = rexBuilder.makeLiteral(true);
    TypedExpr result = converter.convert(literal);

    assertTrue(result instanceof ConstantTypedExpr);
    assertTrue(result.getReturnType() instanceof BooleanType);
  }

  public void testConvertStringLiteralVarChar() {
    RelDataType rowType = buildRowType();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);

    RexLiteral literal = rexBuilder.makeLiteral("hello");
    TypedExpr result = converter.convert(literal);

    assertTrue(result instanceof ConstantTypedExpr);
    assertTrue(result.getReturnType() instanceof VarCharType);
  }

  public void testConvertIntegerLiteral() {
    RelDataType rowType = buildRowType();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);

    // makeLiteral for integer: returns a DECIMAL literal with scale 0 (Calcite convention)
    RexLiteral literal =
        (RexLiteral)
            rexBuilder.makeLiteral(42, typeFactory.createSqlType(SqlTypeName.INTEGER), false);

    TypedExpr result = converter.convert(literal);
    assertTrue(result instanceof ConstantTypedExpr);
    // INTEGER literals produce IntegerType
    assertTrue(result.getReturnType() instanceof IntegerType);
  }

  public void testConvertDecimalWithScaleZeroFitsIntProducesInteger() {
    // Calcite represents integer literals as DECIMAL(scale=0)
    RelDataType rowType = buildRowType();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);

    // makeBigintLiteral creates a DECIMAL literal with scale 0 for value 30
    RexLiteral literal =
        (RexLiteral)
            rexBuilder.makeLiteral(
                BigDecimal.valueOf(30),
                typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 0),
                false);

    TypedExpr result = converter.convert(literal);
    assertTrue(result instanceof ConstantTypedExpr);
    // Value 30 fits in int range and scale is 0 → IntegerType
    assertTrue(result.getReturnType() instanceof IntegerType);
  }

  public void testConvertDecimalWithNonZeroScaleProducesDouble() {
    RelDataType rowType = buildRowType();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);

    RexLiteral literal =
        (RexLiteral)
            rexBuilder.makeLiteral(
                new BigDecimal("3.14"),
                typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 2),
                false);

    TypedExpr result = converter.convert(literal);
    assertTrue(result instanceof ConstantTypedExpr);
    assertTrue(result.getReturnType() instanceof DoubleType);
  }

  public void testConvertGreaterThanCall() {
    RelDataType rowType = buildRowType();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);

    RexInputRef ageRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
    RexLiteral thirtyLit =
        (RexLiteral)
            rexBuilder.makeLiteral(
                BigDecimal.valueOf(30),
                typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 0),
                false);

    RexNode call = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, ageRef, thirtyLit);

    TypedExpr result = converter.convert(call);
    assertTrue(result instanceof CallTypedExpr);
    assertEquals("greaterthan", ((CallTypedExpr) result).getFunctionName());
    assertEquals(2, result.getInputs().size());
  }

  public void testConvertEqualsCall() {
    RelDataType rowType = buildRowType();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);

    RexInputRef ageRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
    RexLiteral fortyLit =
        (RexLiteral)
            rexBuilder.makeLiteral(
                BigDecimal.valueOf(40),
                typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 0),
                false);

    RexNode call = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ageRef, fortyLit);

    TypedExpr result = converter.convert(call);
    assertTrue(result instanceof CallTypedExpr);
    assertEquals("equalto", ((CallTypedExpr) result).getFunctionName());
  }

  public void testConvertNotEqualsCall() {
    RelDataType rowType = buildRowType();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);

    RexInputRef ageRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
    RexLiteral zeroLit =
        (RexLiteral)
            rexBuilder.makeLiteral(
                BigDecimal.ZERO, typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 0), false);

    RexNode call = rexBuilder.makeCall(SqlStdOperatorTable.NOT_EQUALS, ageRef, zeroLit);

    TypedExpr result = converter.convert(call);
    // NOT_EQUALS lowers to not(equalto(a, b)) — Velox's Spark comparison registration has
    // `notequalto` only for DECIMAL, so we can't call it directly for generic types.
    assertTrue(result instanceof CallTypedExpr);
    CallTypedExpr outer = (CallTypedExpr) result;
    assertEquals("not", outer.getFunctionName());
    assertEquals(1, outer.getInputs().size());
    assertTrue(outer.getInputs().get(0) instanceof CallTypedExpr);
    CallTypedExpr inner = (CallTypedExpr) outer.getInputs().get(0);
    assertEquals("equalto", inner.getFunctionName());
  }

  public void testConvertLessThanCall() {
    RelDataType rowType = buildRowType();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);

    RexInputRef ageRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
    RexLiteral lit =
        (RexLiteral)
            rexBuilder.makeLiteral(
                BigDecimal.valueOf(65),
                typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 0),
                false);

    RexNode call = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, ageRef, lit);

    TypedExpr result = converter.convert(call);
    assertTrue(result instanceof CallTypedExpr);
    assertEquals("lessthan", ((CallTypedExpr) result).getFunctionName());
  }

  public void testConvertIsNullCall() {
    RelDataType rowType = buildRowType();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);

    RexInputRef ageRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
    RexNode call = rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, ageRef);

    TypedExpr result = converter.convert(call);
    assertTrue(result instanceof CallTypedExpr);
    assertEquals("is_null", ((CallTypedExpr) result).getFunctionName());
  }

  public void testConvertIsNotNullCallWrapsIsNullWithNot() {
    RelDataType rowType = buildRowType();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);

    RexInputRef ageRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
    RexNode call = rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, ageRef);

    TypedExpr result = converter.convert(call);
    // IS NOT NULL → not(is_null(x))
    assertTrue(result instanceof CallTypedExpr);
    assertEquals("not", ((CallTypedExpr) result).getFunctionName());
    assertEquals(1, result.getInputs().size());
    TypedExpr inner = result.getInputs().get(0);
    assertTrue(inner instanceof CallTypedExpr);
    assertEquals("is_null", ((CallTypedExpr) inner).getFunctionName());
  }

  public void testConvertAndCall() {
    RelDataType rowType = buildRowType();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);

    RexInputRef ageRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
    RexLiteral lit18 =
        (RexLiteral)
            rexBuilder.makeLiteral(
                BigDecimal.valueOf(18),
                typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 0),
                false);
    RexLiteral lit65 =
        (RexLiteral)
            rexBuilder.makeLiteral(
                BigDecimal.valueOf(65),
                typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 0),
                false);

    RexNode gt = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, ageRef, lit18);
    RexNode lt = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, ageRef, lit65);
    RexNode andCall = rexBuilder.makeCall(SqlStdOperatorTable.AND, gt, lt);

    TypedExpr result = converter.convert(andCall);
    assertTrue(result instanceof CallTypedExpr);
    assertEquals("and", ((CallTypedExpr) result).getFunctionName());
    assertEquals(2, result.getInputs().size());
  }

  public void testConvertCastCall() {
    RelDataType rowType = buildRowType();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);

    RexInputRef ageRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
    RexNode castCall = rexBuilder.makeCast(typeFactory.createSqlType(SqlTypeName.BIGINT), ageRef);

    TypedExpr result = converter.convert(castCall);
    assertTrue(result.getReturnType() instanceof BigIntType);
  }

  public void testConvertUnsupportedRexNodeThrows() {
    RelDataType rowType = buildRowType();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);

    // RexNode is abstract; use a known concrete subclass that's not supported
    // RexDynamicParam extends RexNode and is not InputRef, Literal, or Call
    org.apache.calcite.rex.RexDynamicParam dynamicParam =
        rexBuilder.makeDynamicParam(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);

    expectThrows(UnsupportedOperationException.class, () -> converter.convert(dynamicParam));
  }

  public void testAllComparisonOperatorsMapped() {
    RelDataType rowType = buildRowType();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);

    RexInputRef ageRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
    RexLiteral zeroLit =
        (RexLiteral)
            rexBuilder.makeLiteral(
                BigDecimal.ZERO, typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 0), false);

    // NOT_EQUALS is excluded here because it rewrites to not(equalto(...)) — see
    // testConvertNotEqualsCall for the shape assertion.
    org.apache.calcite.sql.SqlOperator[] operators = {
      SqlStdOperatorTable.EQUALS,
      SqlStdOperatorTable.GREATER_THAN,
      SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
      SqlStdOperatorTable.LESS_THAN,
      SqlStdOperatorTable.LESS_THAN_OR_EQUAL
    };
    String[] expected = {
      "equalto", "greaterthan", "greaterthanorequal", "lessthan", "lessthanorequal"
    };

    for (int i = 0; i < operators.length; i++) {
      RexNode call = rexBuilder.makeCall(operators[i], ageRef, zeroLit);
      TypedExpr result = converter.convert(call);
      assertTrue(result instanceof CallTypedExpr);
      assertEquals(
          "Operator " + operators[i].getKind() + " should map to " + expected[i],
          expected[i],
          ((CallTypedExpr) result).getFunctionName());
    }
  }

  public void testConvertNullLiteralProducesTypedNull() {
    RelDataType rowType = buildRowType();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);

    RexLiteral nullLit = rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.INTEGER));
    TypedExpr result = converter.convert(nullLit);

    // Null literals produce a ConstantTypedExpr with a typed null variant (e.g.,
    // IntegerValue(null))
    assertTrue(result instanceof ConstantTypedExpr);
    assertTrue(result.getReturnType() instanceof IntegerType);
  }

  // ---- PPL UDF → Velox function name mapping (NAME_MAP) ----

  /** Row type with 3 fields for UDF tests: name(VARCHAR,0), age(INTEGER,1), salary(DOUBLE,2). */
  private RelDataType buildUdfRowType() {
    return typeFactory
        .builder()
        .add("name", typeFactory.createSqlType(SqlTypeName.VARCHAR, 255))
        .add("age", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("salary", typeFactory.createSqlType(SqlTypeName.DOUBLE))
        .build();
  }

  /** Verify math functions map to correct Velox names. */
  public void testMathFunctionNameMapping() {
    RelDataType rowType = buildUdfRowType();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);
    RexInputRef doubleRef =
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.DOUBLE), 2);

    // ABS → abs
    TypedExpr abs = converter.convert(rexBuilder.makeCall(SqlStdOperatorTable.ABS, doubleRef));
    assertEquals("abs", ((CallTypedExpr) abs).getFunctionName());

    // CEIL → ceil
    TypedExpr ceil = converter.convert(rexBuilder.makeCall(SqlStdOperatorTable.CEIL, doubleRef));
    assertEquals("ceil", ((CallTypedExpr) ceil).getFunctionName());

    // FLOOR → floor
    TypedExpr floor = converter.convert(rexBuilder.makeCall(SqlStdOperatorTable.FLOOR, doubleRef));
    assertEquals("floor", ((CallTypedExpr) floor).getFunctionName());

    // ROUND → round
    TypedExpr round = converter.convert(rexBuilder.makeCall(SqlStdOperatorTable.ROUND, doubleRef));
    assertEquals("round", ((CallTypedExpr) round).getFunctionName());

    // POWER → power
    RexLiteral two = rexBuilder.makeLiteral(2.0, typeFactory.createSqlType(SqlTypeName.DOUBLE));
    TypedExpr power =
        converter.convert(rexBuilder.makeCall(SqlStdOperatorTable.POWER, doubleRef, two));
    assertEquals("power", ((CallTypedExpr) power).getFunctionName());

    // EXP → exp
    TypedExpr exp = converter.convert(rexBuilder.makeCall(SqlStdOperatorTable.EXP, doubleRef));
    assertEquals("exp", ((CallTypedExpr) exp).getFunctionName());

    // LN → ln
    TypedExpr ln = converter.convert(rexBuilder.makeCall(SqlStdOperatorTable.LN, doubleRef));
    assertEquals("ln", ((CallTypedExpr) ln).getFunctionName());

    // LOG10 → log10
    TypedExpr log10 = converter.convert(rexBuilder.makeCall(SqlStdOperatorTable.LOG10, doubleRef));
    assertEquals("log10", ((CallTypedExpr) log10).getFunctionName());

    // SIGN → sign
    TypedExpr sign = converter.convert(rexBuilder.makeCall(SqlStdOperatorTable.SIGN, doubleRef));
    assertEquals("sign", ((CallTypedExpr) sign).getFunctionName());
  }

  /** Verify trig functions map to correct Velox names. */
  public void testTrigFunctionNameMapping() {
    RelDataType rowType = buildUdfRowType();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);
    RexInputRef doubleRef =
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.DOUBLE), 2);

    String[][] funcs = {
      {"COS", "cos"}, {"SIN", "sin"}, {"TAN", "tan"},
      {"ACOS", "acos"}, {"ASIN", "asin"}, {"ATAN", "atan"},
      {"RADIANS", "radians"}, {"DEGREES", "degrees"}
    };

    for (String[] pair : funcs) {
      var op =
          SqlStdOperatorTable.instance().getOperatorList().stream()
              .filter(o -> o.getName().equals(pair[0]) && o.getOperandCountRange().isValidCount(1))
              .findFirst()
              .orElse(null);
      assertNotNull("Operator " + pair[0] + " should exist", op);
      TypedExpr result = converter.convert(rexBuilder.makeCall(op, doubleRef));
      assertEquals(
          pair[0] + " should map to " + pair[1],
          pair[1],
          ((CallTypedExpr) result).getFunctionName());
    }
  }

  /** Verify string functions map to correct Velox names. */
  public void testStringFunctionNameMapping() {
    RelDataType rowType = buildUdfRowType();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);
    RexInputRef varcharRef =
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0);

    // UPPER → upper
    TypedExpr upper = converter.convert(rexBuilder.makeCall(SqlStdOperatorTable.UPPER, varcharRef));
    assertEquals("upper", ((CallTypedExpr) upper).getFunctionName());

    // LOWER → lower
    TypedExpr lower = converter.convert(rexBuilder.makeCall(SqlStdOperatorTable.LOWER, varcharRef));
    assertEquals("lower", ((CallTypedExpr) lower).getFunctionName());

    // CHAR_LENGTH → length (PPL LENGTH maps to Calcite CHAR_LENGTH)
    TypedExpr length =
        converter.convert(rexBuilder.makeCall(SqlStdOperatorTable.CHAR_LENGTH, varcharRef));
    assertEquals("length", ((CallTypedExpr) length).getFunctionName());

    // SUBSTRING → substr
    RexLiteral one = rexBuilder.makeLiteral(1, typeFactory.createSqlType(SqlTypeName.INTEGER));
    RexLiteral three = rexBuilder.makeLiteral(3, typeFactory.createSqlType(SqlTypeName.INTEGER));
    TypedExpr substr =
        converter.convert(
            rexBuilder.makeCall(SqlStdOperatorTable.SUBSTRING, varcharRef, one, three));
    assertEquals("substring", ((CallTypedExpr) substr).getFunctionName());
  }

  /** Verify CASE → switch mapping. */
  public void testCaseConvertsToSwitch() {
    RelDataType rowType = buildUdfRowType();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);

    // CASE WHEN age > 30 THEN 'old' ELSE 'young' END
    RexInputRef ageRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1);
    RexNode cond =
        rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            ageRef,
            rexBuilder.makeLiteral(30, typeFactory.createSqlType(SqlTypeName.INTEGER)));
    RexNode thenVal = rexBuilder.makeLiteral("old", typeFactory.createSqlType(SqlTypeName.VARCHAR));
    RexNode elseVal =
        rexBuilder.makeLiteral("young", typeFactory.createSqlType(SqlTypeName.VARCHAR));
    RexNode caseExpr = rexBuilder.makeCall(SqlStdOperatorTable.CASE, cond, thenVal, elseVal);

    TypedExpr result = converter.convert(caseExpr);
    assertTrue(result instanceof CallTypedExpr);
    assertEquals("switch", ((CallTypedExpr) result).getFunctionName());
    // switch has 3 args: condition, then-value, else-value
    assertEquals(3, ((CallTypedExpr) result).getInputs().size());
  }

  /** Verify isSupported returns true for mapped functions and false for unknown. */
  public void testIsSupportedForMappedFunctions() {
    RexInputRef doubleRef =
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.DOUBLE), 0);
    RexInputRef varcharRef =
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0);
    RexInputRef intRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);

    // Supported: math, string, comparison
    assertTrue(
        VeloxExprConverter.isSupported(
            (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.ABS, doubleRef)));
    assertTrue(
        VeloxExprConverter.isSupported(
            (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.UPPER, varcharRef)));
    assertTrue(
        VeloxExprConverter.isSupported(
            (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.COS, doubleRef)));
    assertTrue(
        VeloxExprConverter.isSupported(
            (RexCall)
                rexBuilder.makeCall(
                    SqlStdOperatorTable.EQUALS,
                    intRef,
                    rexBuilder.makeLiteral(30, typeFactory.createSqlType(SqlTypeName.INTEGER)))));
  }

  // ---- ITEM (nested object field access) ----

  /** ITEM(parent, 'field') should be recognized as supported by isSupported(). */
  public void testIsSupportedForItemAccess() {
    // Build MAP<VARCHAR, ANY> type (simulates OpenSearch object field)
    RelDataType mapType =
        typeFactory.createMapType(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            typeFactory.createSqlType(SqlTypeName.ANY));
    RexInputRef mapRef = rexBuilder.makeInputRef(mapType, 0);
    RexLiteral fieldKey =
        rexBuilder.makeLiteral("region", typeFactory.createSqlType(SqlTypeName.VARCHAR));
    RexNode itemCall = rexBuilder.makeCall(SqlStdOperatorTable.ITEM, mapRef, fieldKey);
    assertTrue(
        "ITEM access should be supported", VeloxExprConverter.isSupported((RexCall) itemCall));
  }

  /** ITEM(parent, 'field') should convert to nested FieldAccessTypedExpr. */
  public void testItemConvertsToNestedFieldAccess() {
    // Row type: cloud is a ROW(region: VARCHAR), simulating MAP→ROW conversion
    RelDataType innerRowType = typeFactory.builder().add("region", SqlTypeName.VARCHAR).build();
    RelDataType rowType =
        typeFactory.builder().add("cloud", innerRowType).add("name", SqlTypeName.VARCHAR).build();

    VeloxExprConverter converter = new VeloxExprConverter(rowType);

    // Build ITEM(cloud, 'region') — simulates PPL's cloud.region access
    RexInputRef parentRef = rexBuilder.makeInputRef(innerRowType, 0);
    RexLiteral fieldKey =
        rexBuilder.makeLiteral("region", typeFactory.createSqlType(SqlTypeName.VARCHAR));
    RexNode itemCall = rexBuilder.makeCall(SqlStdOperatorTable.ITEM, parentRef, fieldKey);

    TypedExpr result = converter.convert(itemCall);
    assertTrue(result instanceof FieldAccessTypedExpr);
    assertEquals("region", ((FieldAccessTypedExpr) result).getFieldName());
  }

  // ---- Datetime literal conversions ----

  public void testConvertTimestampLiteralToTimestampValue() {
    VeloxExprConverter converter = new VeloxExprConverter(buildRowType());

    // 2023-01-01 00:00:00 UTC = 1672531200000 millis since epoch
    long millis = 1_672_531_200_000L;
    RexLiteral literal =
        rexBuilder.makeTimestampLiteral(TimestampString.fromMillisSinceEpoch(millis), 3);

    TypedExpr result = converter.convert(literal);

    assertTrue(result instanceof ConstantTypedExpr);
    assertTrue(result.getReturnType() instanceof TimestampType);
    Variant variant = ((ConstantTypedExpr) result).getValue();
    assertTrue(variant instanceof TimestampValue);
    TimestampValue tv = (TimestampValue) variant;
    assertEquals(millis / 1000L, tv.getSeconds());
    assertEquals((millis % 1000L) * 1_000_000L, tv.getNanos());
  }

  public void testConvertTimestampLiteralWithMillisPrecision() {
    VeloxExprConverter converter = new VeloxExprConverter(buildRowType());

    // 2023-01-01 00:00:00.123 → seconds=1672531200, nanos=123_000_000
    long millis = 1_672_531_200_123L;
    RexLiteral literal =
        rexBuilder.makeTimestampLiteral(TimestampString.fromMillisSinceEpoch(millis), 3);

    TypedExpr result = converter.convert(literal);

    TimestampValue tv = (TimestampValue) ((ConstantTypedExpr) result).getValue();
    assertEquals(1_672_531_200L, tv.getSeconds());
    assertEquals(123_000_000L, tv.getNanos());
  }

  public void testConvertDateLiteralProducesDateTypedConstant() {
    VeloxExprConverter converter = new VeloxExprConverter(buildRowType());

    // 2023-01-01 = 19358 days since epoch
    RexLiteral literal = rexBuilder.makeDateLiteral(new DateString(2023, 1, 1));

    TypedExpr result = converter.convert(literal);

    assertTrue(result instanceof ConstantTypedExpr);
    // The ConstantTypedExpr must carry Velox DateType so it binds against DATE-typed fields.
    // velox4j has no DateValue variant; IntegerValue is the int32 backing for days since epoch.
    assertTrue(result.getReturnType() instanceof DateType);
    Variant variant = ((ConstantTypedExpr) result).getValue();
    assertTrue(variant instanceof IntegerValue);
    assertEquals(Integer.valueOf(19358), ((IntegerValue) variant).getValue());
  }

  public void testConvertTimeLiteralToBigIntValue() {
    VeloxExprConverter converter = new VeloxExprConverter(buildRowType());

    // 12:34:56.000 = 45_296_000 millis since midnight
    RexLiteral literal = rexBuilder.makeTimeLiteral(new TimeString(12, 34, 56), 3);

    TypedExpr result = converter.convert(literal);

    assertTrue(result instanceof ConstantTypedExpr);
    // Velox TIME is BIGINT-backed (millis from midnight); velox4j has no TimeType wrapper.
    assertTrue(result.getReturnType() instanceof BigIntType);
    Variant variant = ((ConstantTypedExpr) result).getValue();
    assertTrue(variant instanceof BigIntValue);
    assertEquals(Long.valueOf(45_296_000L), ((BigIntValue) variant).getValue());
  }

  public void testConvertNullTimestampLiteralProducesNullTimestampVariant() {
    VeloxExprConverter converter = new VeloxExprConverter(buildRowType());

    RelDataType tsType = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
    RexLiteral literal = (RexLiteral) rexBuilder.makeNullLiteral(tsType);

    TypedExpr result = converter.convert(literal);

    assertTrue(result.getReturnType() instanceof TimestampType);
    Variant variant = ((ConstantTypedExpr) result).getValue();
    assertTrue(variant instanceof TimestampValue);
    assertNull(((TimestampValue) variant).getValue());
  }

  public void testConvertNullDateLiteralProducesNullIntegerVariant() {
    VeloxExprConverter converter = new VeloxExprConverter(buildRowType());

    RelDataType dateType = typeFactory.createSqlType(SqlTypeName.DATE);
    RexLiteral literal = (RexLiteral) rexBuilder.makeNullLiteral(dateType);

    TypedExpr result = converter.convert(literal);

    assertTrue(result.getReturnType() instanceof DateType);
    Variant variant = ((ConstantTypedExpr) result).getValue();
    assertTrue(variant instanceof IntegerValue);
    assertNull(((IntegerValue) variant).getValue());
  }

  public void testConvertNullTimeLiteralProducesNullBigIntVariant() {
    VeloxExprConverter converter = new VeloxExprConverter(buildRowType());

    RelDataType timeType = typeFactory.createSqlType(SqlTypeName.TIME);
    RexLiteral literal = (RexLiteral) rexBuilder.makeNullLiteral(timeType);

    TypedExpr result = converter.convert(literal);

    assertTrue(result.getReturnType() instanceof BigIntType);
    Variant variant = ((ConstantTypedExpr) result).getValue();
    assertTrue(variant instanceof BigIntValue);
    assertNull(((BigIntValue) variant).getValue());
  }

  // ---- PPL span() conversion tests ----

  /** Minimal stand-in for PPL's SPAN operator — matches the real one on name and SqlKind. */
  private static final SqlFunction SPAN_OP =
      new SqlFunction(
          "SPAN",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0,
          null,
          OperandTypes.VARIADIC,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);

  /** Row type with a TIMESTAMP `ts` column and an INTEGER `price` column. */
  private RelDataType buildSpanRowType() {
    RelDataTypeFactory.Builder builder = typeFactory.builder();
    builder.add("ts", typeFactory.createSqlType(SqlTypeName.TIMESTAMP));
    builder.add("price", typeFactory.createSqlType(SqlTypeName.INTEGER));
    builder.add("weight", typeFactory.createSqlType(SqlTypeName.DOUBLE));
    return builder.build();
  }

  private RexNode makeSpanCall(RexNode field, int count, String unit) {
    RexLiteral countLit =
        (RexLiteral)
            rexBuilder.makeLiteral(count, typeFactory.createSqlType(SqlTypeName.INTEGER), false);
    RexNode unitLit =
        unit == null
            ? rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.VARCHAR, 8))
            : rexBuilder.makeLiteral(unit);
    return rexBuilder.makeCall(SPAN_OP, field, countLit, unitLit);
  }

  public void testConvertSpanTimestamp1mUsesDateTrunc() {
    RelDataType rowType = buildSpanRowType();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);
    RexNode tsRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), 0);

    TypedExpr result = converter.convert(makeSpanCall(tsRef, 1, "m"));

    assertTrue(result instanceof CallTypedExpr);
    CallTypedExpr outer = (CallTypedExpr) result;
    assertEquals("date_trunc", outer.getFunctionName());
    assertTrue(outer.getReturnType() instanceof TimestampType);
    assertEquals(2, outer.getInputs().size());
    TypedExpr unitArg = outer.getInputs().get(0);
    assertTrue(unitArg instanceof ConstantTypedExpr);
    assertTrue(unitArg.getReturnType() instanceof VarCharType);
  }

  public void testConvertSpanTimestamp1yUsesDateTruncYear() {
    RelDataType rowType = buildSpanRowType();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);
    RexNode tsRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), 0);

    TypedExpr result = converter.convert(makeSpanCall(tsRef, 1, "y"));

    CallTypedExpr outer = (CallTypedExpr) result;
    assertEquals("date_trunc", outer.getFunctionName());
  }

  public void testConvertSpanTimestamp5mUsesFromUnixtimeFloor() {
    // span(ts, 5, 'm') → from_unixtime(multiply(floor(divide(to_unixtime(ts), 300.0)), 300.0))
    RelDataType rowType = buildSpanRowType();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);
    RexNode tsRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), 0);

    TypedExpr result = converter.convert(makeSpanCall(tsRef, 5, "m"));

    CallTypedExpr fromUnix = (CallTypedExpr) result;
    assertEquals("from_unixtime", fromUnix.getFunctionName());
    assertTrue(fromUnix.getReturnType() instanceof TimestampType);

    CallTypedExpr multiply = (CallTypedExpr) fromUnix.getInputs().get(0);
    assertEquals("multiply", multiply.getFunctionName());
    CallTypedExpr floor = (CallTypedExpr) multiply.getInputs().get(0);
    assertEquals("floor", floor.getFunctionName());
    CallTypedExpr divide = (CallTypedExpr) floor.getInputs().get(0);
    assertEquals("divide", divide.getFunctionName());
    CallTypedExpr toUnix = (CallTypedExpr) divide.getInputs().get(0);
    assertEquals("to_unixtime", toUnix.getFunctionName());
  }

  public void testConvertSpanTimestamp90sUsesBucket90() {
    RelDataType rowType = buildSpanRowType();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);
    RexNode tsRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), 0);

    TypedExpr result = converter.convert(makeSpanCall(tsRef, 90, "s"));

    CallTypedExpr fromUnix = (CallTypedExpr) result;
    CallTypedExpr multiply = (CallTypedExpr) fromUnix.getInputs().get(0);
    // bucket constant is the second arg to multiply
    TypedExpr bucketArg = multiply.getInputs().get(1);
    assertTrue(bucketArg instanceof ConstantTypedExpr);
    assertTrue(bucketArg.getReturnType() instanceof DoubleType);
  }

  public void testConvertSpanNumericIntegerStaysInIntegerSpace() {
    // span(price_int, 100) lowers to the Euclidean-modulo shape in INTEGER space:
    //   col - ((col mod w + w) mod w).
    // Staying in integer space preserves BIGINT precision above 2^53. The mod-based form
    // (instead of floor(divide)) matches `floor` semantics for negative values — Velox's
    // integer divide truncates toward zero and integer floor is a no-op.
    RelDataType rowType = buildSpanRowType();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);
    RexNode priceRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1);

    TypedExpr result = converter.convert(makeSpanCall(priceRef, 100, null));

    assertTrue(result.getReturnType() instanceof IntegerType);
    CallTypedExpr minus = (CallTypedExpr) result;
    assertEquals("minus", minus.getFunctionName());
    assertTrue(minus.getReturnType() instanceof IntegerType);
    CallTypedExpr outerMod = (CallTypedExpr) minus.getInputs().get(1);
    assertEquals("mod", outerMod.getFunctionName());
    assertTrue(outerMod.getReturnType() instanceof IntegerType);
    CallTypedExpr plus = (CallTypedExpr) outerMod.getInputs().get(0);
    assertEquals("plus", plus.getFunctionName());
    assertTrue(plus.getReturnType() instanceof IntegerType);
    CallTypedExpr innerMod = (CallTypedExpr) plus.getInputs().get(0);
    assertEquals("mod", innerMod.getFunctionName());
    assertTrue(innerMod.getReturnType() instanceof IntegerType);
    // Confirm nothing transits through DOUBLE — that would signal regressed precision.
    for (TypedExpr child : innerMod.getInputs()) {
      assertFalse(
          "mod operand should not be cast to DOUBLE", child.getReturnType() instanceof DoubleType);
    }
  }

  public void testConvertSpanNumericBigIntStaysInBigIntSpace() {
    // Regression guard for the 2^53 precision bug: BIGINT must not transit through DOUBLE.
    RelDataType rowType = typeFactory.builder().add("id", SqlTypeName.BIGINT).build();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);
    RexNode idRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0);

    TypedExpr result = converter.convert(makeSpanCall(idRef, 1, null));

    assertTrue(result.getReturnType() instanceof BigIntType);
    CallTypedExpr minus = (CallTypedExpr) result;
    assertEquals("minus", minus.getFunctionName());
    assertTrue(minus.getReturnType() instanceof BigIntType);
    CallTypedExpr outerMod = (CallTypedExpr) minus.getInputs().get(1);
    assertEquals("mod", outerMod.getFunctionName());
    assertTrue(outerMod.getReturnType() instanceof BigIntType);
  }

  public void testConvertSpanNumericIntegerNegativeValueUsesEuclideanMod() {
    // Regression guard for Codex P1: span on a signed integer with a negative value like -1
    // must yield -width (not 0). We assert the emitted tree shape matches col - ((col%w+w)%w),
    // which gives the correct floor-bucket for negatives.
    RelDataType rowType = buildSpanRowType();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);
    RexNode priceRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1);

    TypedExpr result = converter.convert(makeSpanCall(priceRef, 10, null));

    CallTypedExpr minus = (CallTypedExpr) result;
    // Left operand of minus is the column (FieldAccess), right is the Euclidean modulo term.
    TypedExpr left = minus.getInputs().get(0);
    assertTrue(
        "minus left operand should be a field reference, not a cast",
        left instanceof FieldAccessTypedExpr);
    // Outer mod's second operand is the width.
    CallTypedExpr outerMod = (CallTypedExpr) minus.getInputs().get(1);
    TypedExpr outerWidth = outerMod.getInputs().get(1);
    assertTrue(outerWidth instanceof ConstantTypedExpr);
  }

  public void testConvertSpanNumericDoubleWidthStaysDouble() {
    RelDataType rowType = buildSpanRowType();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);
    RexNode weightRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.DOUBLE), 2);

    // span(weight_double, 2) → numeric span on DOUBLE column, no cast needed.
    TypedExpr result = converter.convert(makeSpanCall(weightRef, 2, null));

    assertTrue(result.getReturnType() instanceof DoubleType);
    CallTypedExpr multiply = (CallTypedExpr) result;
    assertEquals("multiply", multiply.getFunctionName());
  }

  public void testIsSupportedRejectsSpanMillisecond() {
    RelDataType rowType = buildSpanRowType();
    RexNode tsRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), 0);
    RexCall call = (RexCall) makeSpanCall(tsRef, 1, "ms");
    assertFalse(VeloxExprConverter.isSupported(call));
  }

  public void testIsSupportedRejectsSpanMonthCountGreaterThanOne() {
    RelDataType rowType = buildSpanRowType();
    RexNode tsRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), 0);
    RexCall call = (RexCall) makeSpanCall(tsRef, 3, "M");
    assertFalse(VeloxExprConverter.isSupported(call));
  }

  public void testIsSupportedRejectsSpanMultiWeek() {
    // `span(ts, 2w)` would anchor on Thursdays via floor(to_unixtime/604800)*604800 but
    // `span(ts, 1w)` anchors on Mondays via date_trunc('week'). Let this fall back.
    RelDataType rowType = buildSpanRowType();
    RexNode tsRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), 0);
    RexCall call = (RexCall) makeSpanCall(tsRef, 2, "w");
    assertFalse(VeloxExprConverter.isSupported(call));
  }

  public void testIsSupportedAcceptsSpanSingleWeek() {
    RelDataType rowType = buildSpanRowType();
    RexNode tsRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), 0);
    RexCall call = (RexCall) makeSpanCall(tsRef, 1, "w");
    assertTrue(VeloxExprConverter.isSupported(call));
  }

  public void testIsSupportedRejectsSpanDecimalField() {
    // Decimal columns aren't safe through integer or DOUBLE paths — fall back.
    RelDataType rowType =
        typeFactory
            .builder()
            .add("price", typeFactory.createSqlType(SqlTypeName.DECIMAL, 20, 4))
            .build();
    RexNode priceRef =
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.DECIMAL, 20, 4), 0);
    RexCall call = (RexCall) makeSpanCall(priceRef, 100, null);
    assertFalse(VeloxExprConverter.isSupported(call));
  }

  /** Build a numeric SPAN call with a fractional-width RexLiteral (scaled DECIMAL). */
  private RexCall makeSpanCallWithFractionalWidth(RexNode field, String width) {
    BigDecimal bd = new BigDecimal(width);
    RexLiteral widthLit =
        (RexLiteral)
            rexBuilder.makeLiteral(
                bd,
                typeFactory.createSqlType(SqlTypeName.DECIMAL, bd.precision(), bd.scale()),
                false);
    RexNode unitLit = rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.VARCHAR, 8));
    return (RexCall) rexBuilder.makeCall(SPAN_OP, field, widthLit, unitLit);
  }

  public void testIsSupportedAcceptsSpanDoubleFieldFractionalWidth() {
    // span(double_col, 0.5) should be vectorized — the DOUBLE arithmetic path handles fractional
    // widths correctly. Regression guard for P2: the DECIMAL-scale rejection was overbroad.
    RexNode weightRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.DOUBLE), 2);
    RexCall call = makeSpanCallWithFractionalWidth(weightRef, "0.5");
    assertTrue(VeloxExprConverter.isSupported(call));
  }

  public void testIsSupportedRejectsSpanIntegerFieldFractionalWidth() {
    // span(int_col, 0.5) forces DOUBLE conversion, which risks BIGINT precision loss. Reject
    // so integer spans always stay in integer space.
    RexNode priceRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1);
    RexCall call = makeSpanCallWithFractionalWidth(priceRef, "0.5");
    assertFalse(VeloxExprConverter.isSupported(call));
  }

  public void testConvertSpanDoubleFieldFractionalWidthUsesDoublePath() {
    // Confirms the lowering shape for span(double_col, 0.5) is floor(divide)*multiply in DOUBLE.
    RelDataType rowType = buildSpanRowType();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);
    RexNode weightRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.DOUBLE), 2);
    TypedExpr result = converter.convert(makeSpanCallWithFractionalWidth(weightRef, "0.5"));

    assertTrue(result.getReturnType() instanceof DoubleType);
    CallTypedExpr multiply = (CallTypedExpr) result;
    assertEquals("multiply", multiply.getFunctionName());
    CallTypedExpr floor = (CallTypedExpr) multiply.getInputs().get(0);
    assertEquals("floor", floor.getFunctionName());
    CallTypedExpr divide = (CallTypedExpr) floor.getInputs().get(0);
    assertEquals("divide", divide.getFunctionName());
  }

  public void testIsSupportedRejectsSpanZeroCount() {
    RelDataType rowType = buildSpanRowType();
    RexNode tsRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), 0);
    RexCall call = (RexCall) makeSpanCall(tsRef, 0, "m");
    assertFalse(VeloxExprConverter.isSupported(call));
  }

  public void testIsSupportedRejectsSpanNonLiteralWidth() {
    RelDataType rowType = buildSpanRowType();
    RexNode tsRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), 0);
    // Build a span call where operand[1] is a RexInputRef, not a RexLiteral.
    RexNode nonLiteralWidth =
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1);
    RexNode unitLit = rexBuilder.makeLiteral("m");
    RexCall call = (RexCall) rexBuilder.makeCall(SPAN_OP, tsRef, nonLiteralWidth, unitLit);
    assertFalse(VeloxExprConverter.isSupported(call));
  }

  public void testIsSupportedAcceptsSpanSecond1() {
    RelDataType rowType = buildSpanRowType();
    RexNode tsRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), 0);
    RexCall call = (RexCall) makeSpanCall(tsRef, 1, "s");
    assertTrue(VeloxExprConverter.isSupported(call));
  }

  public void testIsSupportedAcceptsSpanMinute5() {
    RelDataType rowType = buildSpanRowType();
    RexNode tsRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), 0);
    RexCall call = (RexCall) makeSpanCall(tsRef, 5, "m");
    assertTrue(VeloxExprConverter.isSupported(call));
  }

  public void testIsSupportedAcceptsSpanNumeric() {
    RelDataType rowType = buildSpanRowType();
    RexNode priceRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1);
    RexCall call = (RexCall) makeSpanCall(priceRef, 100, null);
    assertTrue(VeloxExprConverter.isSupported(call));
  }

  public void testIsSupportedRejectsSpanNumericZeroWidth() {
    // Width = 0 would divide by zero inside the lowered expression. Enforce width > 0 so the
    // query falls back to the default engine instead of crashing in Velox.
    RexNode priceRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1);
    RexCall call = (RexCall) makeSpanCall(priceRef, 0, null);
    assertFalse(VeloxExprConverter.isSupported(call));
  }

  public void testIsSupportedRejectsSpanNumericNegativeWidth() {
    // Negative widths produce nonsensical bucket boundaries. Match the temporal branch which
    // already rejects non-positive counts.
    RexNode priceRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1);
    RexCall call = (RexCall) makeSpanCall(priceRef, -5, null);
    assertFalse(VeloxExprConverter.isSupported(call));
  }

  public void testConvertSpanIntegerUsesVeloxRegisteredModName() {
    // Regression guard: Velox registers the modulo scalar as "mod" (Presto + Spark both use
    // "mod"/"pmod"/"remainder"; no dialect registers "modulus"). If the Euclidean-mod rewrite
    // ever drifts to a different name, integer spans would fail at Velox signature resolution.
    RelDataType rowType = buildSpanRowType();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);
    RexNode priceRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1);

    TypedExpr result = converter.convert(makeSpanCall(priceRef, 10, null));

    CallTypedExpr minus = (CallTypedExpr) result;
    CallTypedExpr outerMod = (CallTypedExpr) minus.getInputs().get(1);
    assertEquals("mod", outerMod.getFunctionName());
    CallTypedExpr plus = (CallTypedExpr) outerMod.getInputs().get(0);
    CallTypedExpr innerMod = (CallTypedExpr) plus.getInputs().get(0);
    assertEquals("mod", innerMod.getFunctionName());
  }

  // ---- DATE span tests ----

  public void testConvertSpanDate1dUsesDateTrunc() {
    RelDataType rowType = typeFactory.builder().add("d", SqlTypeName.DATE).build();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);
    RexNode dateRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.DATE), 0);

    TypedExpr result = converter.convert(makeSpanCall(dateRef, 1, "d"));

    assertTrue(result instanceof CallTypedExpr);
    CallTypedExpr outer = (CallTypedExpr) result;
    assertEquals("date_trunc", outer.getFunctionName());
    assertTrue(outer.getReturnType() instanceof DateType);
    TypedExpr unitArg = outer.getInputs().get(0);
    assertTrue(unitArg instanceof ConstantTypedExpr);
  }

  public void testConvertSpanDate1MUsesDateTrunc() {
    RelDataType rowType = typeFactory.builder().add("d", SqlTypeName.DATE).build();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);
    RexNode dateRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.DATE), 0);

    TypedExpr result = converter.convert(makeSpanCall(dateRef, 1, "M"));

    CallTypedExpr outer = (CallTypedExpr) result;
    assertEquals("date_trunc", outer.getFunctionName());
    assertTrue(outer.getReturnType() instanceof DateType);
  }

  public void testIsSupportedRejectsSpanDateSubDayUnit() {
    RexNode dateRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.DATE), 0);
    // date_trunc(DATE) rejects sub-day units — fall back.
    assertFalse(VeloxExprConverter.isSupported((RexCall) makeSpanCall(dateRef, 1, "h")));
    assertFalse(VeloxExprConverter.isSupported((RexCall) makeSpanCall(dateRef, 1, "m")));
    assertFalse(VeloxExprConverter.isSupported((RexCall) makeSpanCall(dateRef, 1, "s")));
  }

  public void testIsSupportedRejectsSpanDateCountGreaterThanOne() {
    // date_trunc only handles count=1; count>1 on DATE would need interval math we don't emit.
    RexNode dateRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.DATE), 0);
    assertFalse(VeloxExprConverter.isSupported((RexCall) makeSpanCall(dateRef, 2, "d")));
    assertFalse(VeloxExprConverter.isSupported((RexCall) makeSpanCall(dateRef, 3, "M")));
  }

  public void testIsSupportedAcceptsSpanDateWeekMonthQuarterYear() {
    RexNode dateRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.DATE), 0);
    assertTrue(VeloxExprConverter.isSupported((RexCall) makeSpanCall(dateRef, 1, "d")));
    assertTrue(VeloxExprConverter.isSupported((RexCall) makeSpanCall(dateRef, 1, "w")));
    assertTrue(VeloxExprConverter.isSupported((RexCall) makeSpanCall(dateRef, 1, "M")));
    assertTrue(VeloxExprConverter.isSupported((RexCall) makeSpanCall(dateRef, 1, "q")));
    assertTrue(VeloxExprConverter.isSupported((RexCall) makeSpanCall(dateRef, 1, "y")));
  }

  // ---- TIME span tests (must fall back) ----

  public void testIsSupportedRejectsSpanTime() {
    // velox4j has no TimeType wrapper, so we can't emit date_trunc(TIME). Falling back
    // protects users from the silent numeric-path miscompile that would bucket raw millis.
    RexNode timeRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.TIME), 0);
    assertFalse(VeloxExprConverter.isSupported((RexCall) makeSpanCall(timeRef, 1, "h")));
    assertFalse(VeloxExprConverter.isSupported((RexCall) makeSpanCall(timeRef, 30, "m")));
    assertFalse(VeloxExprConverter.isSupported((RexCall) makeSpanCall(timeRef, 1, "s")));
  }
}
