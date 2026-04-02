/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.convert;

import java.math.BigDecimal;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
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

  public void testConvertNullLiteralThrows() {
    RelDataType rowType = buildRowType();
    VeloxExprConverter converter = new VeloxExprConverter(rowType);

    RexLiteral nullLit = rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.INTEGER));

    // Null literals are not yet supported — ConstantTypedExpr requires a non-null variant
    expectThrows(IllegalArgumentException.class, () -> converter.convert(nullLit));
  }
}
