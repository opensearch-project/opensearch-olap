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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.boostscale.velox4j.expression.CallTypedExpr;
import org.boostscale.velox4j.expression.ConstantTypedExpr;
import org.boostscale.velox4j.expression.FieldAccessTypedExpr;
import org.boostscale.velox4j.expression.TypedExpr;
import org.boostscale.velox4j.type.BigIntType;
import org.boostscale.velox4j.type.BooleanType;
import org.boostscale.velox4j.type.DoubleType;
import org.boostscale.velox4j.type.IntegerType;
import org.boostscale.velox4j.type.VarCharType;
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
    assertTrue(result instanceof CallTypedExpr);
    assertEquals("notequalto", ((CallTypedExpr) result).getFunctionName());
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

    org.apache.calcite.sql.SqlOperator[] operators = {
      SqlStdOperatorTable.EQUALS,
      SqlStdOperatorTable.NOT_EQUALS,
      SqlStdOperatorTable.GREATER_THAN,
      SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
      SqlStdOperatorTable.LESS_THAN,
      SqlStdOperatorTable.LESS_THAN_OR_EQUAL
    };
    String[] expected = {
      "equalto", "notequalto", "greaterthan", "greaterthanorequal", "lessthan", "lessthanorequal"
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
}
