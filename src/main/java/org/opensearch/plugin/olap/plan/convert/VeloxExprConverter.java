/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.convert;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.boostscale.velox4j.expression.CallTypedExpr;
import org.boostscale.velox4j.expression.CastTypedExpr;
import org.boostscale.velox4j.expression.ConstantTypedExpr;
import org.boostscale.velox4j.expression.FieldAccessTypedExpr;
import org.boostscale.velox4j.expression.TypedExpr;
import org.boostscale.velox4j.type.BooleanType;
import org.boostscale.velox4j.type.Type;
import org.boostscale.velox4j.variant.BigIntValue;
import org.boostscale.velox4j.variant.BooleanValue;
import org.boostscale.velox4j.variant.DoubleValue;
import org.boostscale.velox4j.variant.IntegerValue;
import org.boostscale.velox4j.variant.RealValue;
import org.boostscale.velox4j.variant.VarCharValue;
import org.boostscale.velox4j.variant.Variant;

/** Converts Calcite RexNode expressions to velox4j TypedExpr. */
public class VeloxExprConverter {

  private static final Map<SqlKind, String> FUNCTION_MAP = new HashMap<>();

  static {
    // Comparison operators (Velox uses Presto-style full names)
    FUNCTION_MAP.put(SqlKind.EQUALS, "equalto");
    FUNCTION_MAP.put(SqlKind.NOT_EQUALS, "notequalto");
    FUNCTION_MAP.put(SqlKind.GREATER_THAN, "greaterthan");
    FUNCTION_MAP.put(SqlKind.GREATER_THAN_OR_EQUAL, "greaterthanorequal");
    FUNCTION_MAP.put(SqlKind.LESS_THAN, "lessthan");
    FUNCTION_MAP.put(SqlKind.LESS_THAN_OR_EQUAL, "lessthanorequal");

    // Logical operators
    FUNCTION_MAP.put(SqlKind.AND, "and");
    FUNCTION_MAP.put(SqlKind.OR, "or");
    FUNCTION_MAP.put(SqlKind.NOT, "not");

    // Arithmetic operators
    FUNCTION_MAP.put(SqlKind.PLUS, "plus");
    FUNCTION_MAP.put(SqlKind.MINUS, "minus");
    FUNCTION_MAP.put(SqlKind.TIMES, "multiply");
    FUNCTION_MAP.put(SqlKind.DIVIDE, "divide");
    FUNCTION_MAP.put(SqlKind.MOD, "modulus");

    // String operators
    FUNCTION_MAP.put(SqlKind.LIKE, "like");

    // Null checks
    FUNCTION_MAP.put(SqlKind.IS_NULL, "is_null");
    FUNCTION_MAP.put(SqlKind.IS_NOT_NULL, "not");
  }

  private final RelDataType inputRowType;

  public VeloxExprConverter(RelDataType inputRowType) {
    this.inputRowType = inputRowType;
  }

  public TypedExpr convert(RexNode rexNode) {
    if (rexNode instanceof RexInputRef) {
      return convertInputRef((RexInputRef) rexNode);
    } else if (rexNode instanceof RexLiteral) {
      return convertLiteral((RexLiteral) rexNode);
    } else if (rexNode instanceof RexCall) {
      return convertCall((RexCall) rexNode);
    }
    throw new UnsupportedOperationException(
        "Unsupported RexNode type: " + rexNode.getClass().getSimpleName());
  }

  private TypedExpr convertInputRef(RexInputRef inputRef) {
    int index = inputRef.getIndex();
    RelDataTypeField field = inputRowType.getFieldList().get(index);
    Type veloxType = VeloxTypeConverter.toVeloxType(field.getType());
    return FieldAccessTypedExpr.create(veloxType, field.getName());
  }

  private TypedExpr convertLiteral(RexLiteral literal) {
    Variant variant = toVariant(literal);
    // Derive the type from the variant to ensure consistency (e.g., DECIMAL(30)
    // produces IntegerValue, so type must be IntegerType, not DoubleType)
    Type veloxType =
        (variant != null)
            ? variantToType(variant)
            : VeloxTypeConverter.toVeloxType(literal.getType());
    return ConstantTypedExpr.create(veloxType, variant);
  }

  private Type variantToType(Variant variant) {
    if (variant instanceof IntegerValue) {
      return new org.boostscale.velox4j.type.IntegerType();
    } else if (variant instanceof BigIntValue) {
      return new org.boostscale.velox4j.type.BigIntType();
    } else if (variant instanceof DoubleValue) {
      return new org.boostscale.velox4j.type.DoubleType();
    } else if (variant instanceof RealValue) {
      return new org.boostscale.velox4j.type.RealType();
    } else if (variant instanceof BooleanValue) {
      return new org.boostscale.velox4j.type.BooleanType();
    } else if (variant instanceof VarCharValue) {
      return new org.boostscale.velox4j.type.VarCharType();
    }
    throw new UnsupportedOperationException("Unknown variant type: " + variant.getClass());
  }

  private Variant toVariant(RexLiteral literal) {
    if (literal.isNull()) {
      return null;
    }
    SqlTypeName typeName = literal.getTypeName();
    switch (typeName) {
      case BOOLEAN:
        return new BooleanValue(RexLiteral.booleanValue(literal));
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return new IntegerValue(literal.getValueAs(Integer.class));
      case BIGINT:
        return new BigIntValue(literal.getValueAs(Long.class));
      case FLOAT:
      case REAL:
        return new RealValue(literal.getValueAs(Float.class));
      case DOUBLE:
        return new DoubleValue(literal.getValueAs(Double.class));
      case DECIMAL:
        BigDecimal bd = literal.getValueAs(BigDecimal.class);
        // Calcite represents integer literals (e.g. 30) as DECIMAL with scale 0.
        // Velox requires exact type match in expressions, so produce an integer
        // variant when the value fits, to avoid type mismatch with integer columns.
        if (bd.scale() <= 0
            && bd.compareTo(BigDecimal.valueOf(Integer.MAX_VALUE)) <= 0
            && bd.compareTo(BigDecimal.valueOf(Integer.MIN_VALUE)) >= 0) {
          return new IntegerValue(bd.intValue());
        }
        if (bd.scale() <= 0) {
          return new BigIntValue(bd.longValue());
        }
        return new DoubleValue(bd.doubleValue());
      case CHAR:
      case VARCHAR:
        NlsString nls = literal.getValueAs(NlsString.class);
        return new VarCharValue(nls != null ? nls.getValue() : null);
      default:
        throw new UnsupportedOperationException("Unsupported literal type: " + typeName);
    }
  }

  private TypedExpr convertCall(RexCall call) {
    SqlKind kind = call.getKind();

    // Handle CAST specially
    if (kind == SqlKind.CAST) {
      TypedExpr input = convert(call.getOperands().get(0));
      Type targetType = VeloxTypeConverter.toVeloxType(call.getType());
      return CastTypedExpr.create(targetType, input, false);
    }

    // Handle IS_NOT_NULL as not(is_null(x))
    if (kind == SqlKind.IS_NOT_NULL) {
      TypedExpr operand = convert(call.getOperands().get(0));
      TypedExpr isNull =
          new CallTypedExpr(new BooleanType(), Collections.singletonList(operand), "is_null");
      return new CallTypedExpr(new BooleanType(), Collections.singletonList(isNull), "not");
    }

    // Handle IN as chain of OR(EQ(...))
    if (kind == SqlKind.IN) {
      return convertIn(call);
    }

    // Handle BETWEEN as AND(GTE, LTE)
    if (kind == SqlKind.BETWEEN) {
      return convertBetween(call);
    }

    // General function call conversion
    String functionName = FUNCTION_MAP.get(kind);
    if (functionName == null) {
      // Fallback: use the operator name in lower case
      functionName = call.getOperator().getName().toLowerCase(java.util.Locale.ROOT);
    }

    List<TypedExpr> inputs = new ArrayList<>(call.getOperands().size());
    for (RexNode operand : call.getOperands()) {
      inputs.add(convert(operand));
    }

    Type returnType = VeloxTypeConverter.toVeloxType(call.getType());
    return new CallTypedExpr(returnType, inputs, functionName);
  }

  private TypedExpr convertIn(RexCall call) {
    List<RexNode> operands = call.getOperands();
    TypedExpr lhs = convert(operands.get(0));

    TypedExpr result = null;
    for (int i = 1; i < operands.size(); i++) {
      TypedExpr rhs = convert(operands.get(i));
      TypedExpr eq = new CallTypedExpr(new BooleanType(), List.of(lhs, rhs), "eq");
      if (result == null) {
        result = eq;
      } else {
        result = new CallTypedExpr(new BooleanType(), List.of(result, eq), "or");
      }
    }
    return result;
  }

  private TypedExpr convertBetween(RexCall call) {
    List<RexNode> operands = call.getOperands();
    TypedExpr value = convert(operands.get(0));
    TypedExpr lower = convert(operands.get(1));
    TypedExpr upper = convert(operands.get(2));

    TypedExpr gte = new CallTypedExpr(new BooleanType(), List.of(value, lower), "gte");
    TypedExpr lte = new CallTypedExpr(new BooleanType(), List.of(value, upper), "lte");
    return new CallTypedExpr(new BooleanType(), List.of(gte, lte), "and");
  }
}
