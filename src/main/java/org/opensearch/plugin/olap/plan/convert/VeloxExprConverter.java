/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.convert;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.boostscale.velox4j.expression.CallTypedExpr;
import org.boostscale.velox4j.expression.CastTypedExpr;
import org.boostscale.velox4j.expression.ConstantTypedExpr;
import org.boostscale.velox4j.expression.FieldAccessTypedExpr;
import org.boostscale.velox4j.expression.TypedExpr;
import org.boostscale.velox4j.type.BigIntType;
import org.boostscale.velox4j.type.BooleanType;
import org.boostscale.velox4j.type.DoubleType;
import org.boostscale.velox4j.type.IntegerType;
import org.boostscale.velox4j.type.RealType;
import org.boostscale.velox4j.type.SmallIntType;
import org.boostscale.velox4j.type.TinyIntType;
import org.boostscale.velox4j.type.Type;
import org.boostscale.velox4j.type.VarCharType;
import org.boostscale.velox4j.variant.BigIntValue;
import org.boostscale.velox4j.variant.BooleanValue;
import org.boostscale.velox4j.variant.DoubleValue;
import org.boostscale.velox4j.variant.IntegerValue;
import org.boostscale.velox4j.variant.RealValue;
import org.boostscale.velox4j.variant.VarCharValue;
import org.boostscale.velox4j.variant.Variant;

/**
 * Converts Calcite RexNode expressions to velox4j TypedExpr.
 *
 * <p>Function mapping uses two layers:
 *
 * <ul>
 *   <li>{@link #FUNCTION_MAP} — SqlKind-based operators (=, +, AND, etc.)
 *   <li>{@link #NAME_MAP} — Calcite operator name → Velox Presto-style function name
 * </ul>
 *
 * <p>The {@link #SUPPORTED_FUNCTIONS} set is used by {@code VectorizedEngineExtension.canVectorize}
 * to reject queries with unsupported functions at plan time.
 */
public class VeloxExprConverter {

  // ---- Layer 1: SqlKind → Velox function name ----

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

  // ---- Layer 2: Calcite operator name → Velox function name ----

  private static final Map<String, String> NAME_MAP = new HashMap<>();

  static {
    // Math functions
    NAME_MAP.put("ABS", "abs");
    NAME_MAP.put("CEIL", "ceil");
    NAME_MAP.put("FLOOR", "floor");
    NAME_MAP.put("ROUND", "round");
    NAME_MAP.put("TRUNCATE", "truncate");
    NAME_MAP.put("SIGN", "sign");
    NAME_MAP.put("POWER", "power");
    NAME_MAP.put("CBRT", "cbrt");
    NAME_MAP.put("EXP", "exp");
    NAME_MAP.put("LN", "ln");
    NAME_MAP.put("LOG10", "log10");
    NAME_MAP.put("LOG2", "log2");
    NAME_MAP.put("LOG", "ln"); // PPL LOG(x) = natural log in Calcite
    NAME_MAP.put("PI", "pi");
    NAME_MAP.put("E", "e");
    NAME_MAP.put("RAND", "rand");
    NAME_MAP.put("SQRT", "sqrt");

    // Trigonometric functions
    NAME_MAP.put("COS", "cos");
    NAME_MAP.put("SIN", "sin");
    NAME_MAP.put("TAN", "tan");
    NAME_MAP.put("ACOS", "acos");
    NAME_MAP.put("ASIN", "asin");
    NAME_MAP.put("ATAN", "atan");
    NAME_MAP.put("ATAN2", "atan2");
    NAME_MAP.put("COSH", "cosh");
    NAME_MAP.put("COT", "cot");
    NAME_MAP.put("RADIANS", "radians");
    NAME_MAP.put("DEGREES", "degrees");

    // String functions
    NAME_MAP.put("UPPER", "upper");
    NAME_MAP.put("LOWER", "lower");
    NAME_MAP.put("CHAR_LENGTH", "length"); // PPL LENGTH → Calcite CHAR_LENGTH
    NAME_MAP.put("CONCAT", "concat");
    NAME_MAP.put("CONCAT_WS", "concat_ws");
    NAME_MAP.put("SUBSTRING", "substring"); // PPL SUBSTRING/SUBSTR → Calcite SUBSTRING
    NAME_MAP.put("REVERSE", "reverse");
    NAME_MAP.put("REPLACE", "replace");
    NAME_MAP.put("ASCII", "codepoint"); // Velox uses codepoint for ASCII value
    NAME_MAP.put("POSITION", "strpos"); // PPL LOCATE/POSITION → Calcite POSITION → Velox strpos
    // LEFT/RIGHT need argument rewrite (LEFT(s,n) → substr(s,1,n), RIGHT needs length calc).
    // Removed from NAME_MAP until rewrite is implemented — queries fall back to default engine.
    NAME_MAP.put("LPAD", "lpad");
    NAME_MAP.put("RPAD", "rpad");
    NAME_MAP.put("REGEXP_LIKE", "regexp_like");
    NAME_MAP.put("ILIKE", "like"); // TODO: case-insensitive like needs lower() wrapping

    // PPL arithmetic operators (may use PPLBuiltinOperators instead of SqlStdOperatorTable)
    NAME_MAP.put("DIVIDE", "divide");
    NAME_MAP.put("MOD", "modulus");
    NAME_MAP.put("/", "divide");
    NAME_MAP.put("%", "modulus");

    // Conditional functions
    NAME_MAP.put("COALESCE", "coalesce");

    // Hash functions
    NAME_MAP.put("MD5", "md5");
    NAME_MAP.put("SHA1", "sha1");
    NAME_MAP.put("SHA256", "sha256");
    NAME_MAP.put("CRC32", "crc32");

    // Date/time extraction (PPLBuiltinOperators → Velox)
    NAME_MAP.put("YEAR", "year");
    NAME_MAP.put("MONTH", "month");
    NAME_MAP.put("DAY", "day");
    NAME_MAP.put("HOUR", "hour");
    NAME_MAP.put("MINUTE", "minute");
    NAME_MAP.put("SECOND", "second");
    NAME_MAP.put("QUARTER", "quarter");
    NAME_MAP.put("WEEK", "week");
    NAME_MAP.put("DAY_OF_WEEK", "dow");
    NAME_MAP.put("DAY_OF_YEAR", "doy");
    NAME_MAP.put("NOW", "now");
    NAME_MAP.put("CURRENT_DATE", "current_date");
    NAME_MAP.put("CURRENT_TIME", "current_time");

    // Date arithmetic
    NAME_MAP.put("DATE_ADD", "date_add");
    // DATE_SUB needs interval negation (date_sub(x,n) → date_add(x,-n)).
    // Removed from NAME_MAP until rewrite is implemented — queries fall back to default engine.
    NAME_MAP.put("DATEDIFF", "date_diff");
    NAME_MAP.put("DATE_FORMAT", "date_format");
    NAME_MAP.put("DATE_TRUNC", "date_trunc");
    NAME_MAP.put("FROM_UNIXTIME", "from_unixtime");
    NAME_MAP.put("UNIX_TIMESTAMP", "to_unixtime");
    NAME_MAP.put("LAST_DAY", "last_day_of_month");
  }

  /**
   * Set of all Calcite operator names that we can convert to Velox. Used by canVectorize() to
   * reject plans with unsupported functions.
   */
  public static final Set<String> SUPPORTED_FUNCTIONS = Set.copyOf(NAME_MAP.keySet());

  /**
   * SqlKinds that are handled natively (not via NAME_MAP). Used by canVectorize() to allow these
   * through without checking NAME_MAP.
   */
  public static final Set<SqlKind> SUPPORTED_SQLKINDS;

  static {
    Set<SqlKind> kinds = new HashSet<>(FUNCTION_MAP.keySet());
    // These SqlKinds are handled by special-case code, not FUNCTION_MAP
    kinds.add(SqlKind.CAST);
    kinds.add(SqlKind.IS_NOT_NULL);
    kinds.add(SqlKind.IN);
    kinds.add(SqlKind.BETWEEN);
    kinds.add(SqlKind.SEARCH);
    kinds.add(SqlKind.CASE);
    kinds.add(SqlKind.TRIM);
    kinds.add(SqlKind.OTHER_FUNCTION); // Named functions resolved via NAME_MAP
    kinds.add(SqlKind.OTHER); // Named functions resolved via NAME_MAP
    SUPPORTED_SQLKINDS = Set.copyOf(kinds);
  }

  private final RelDataType inputRowType;
  private final RexBuilder rexBuilder;

  public VeloxExprConverter(RelDataType inputRowType) {
    this.inputRowType = inputRowType;
    this.rexBuilder = new RexBuilder(new JavaTypeFactoryImpl());
  }

  public TypedExpr convert(RexNode rexNode) {
    if (rexNode instanceof RexInputRef) {
      return convertInputRef((RexInputRef) rexNode);
    } else if (rexNode instanceof RexLiteral) {
      return convertLiteral((RexLiteral) rexNode);
    } else if (rexNode instanceof RexCall) {
      RexCall call = (RexCall) rexNode;
      // Calcite rewrites OR conditions on the same column (e.g. city='A' OR city='B')
      // into SEARCH(ref, Sarg[...]) which uses SARG literals. Expand back to OR/AND.
      if (call.getKind() == SqlKind.SEARCH) {
        RexNode expanded = RexUtil.expandSearch(rexBuilder, null, call);
        return convert(expanded);
      }
      return convertCall(call);
    }
    throw new UnsupportedOperationException(
        "Unsupported RexNode type: " + rexNode.getClass().getSimpleName());
  }

  /**
   * Check if a RexCall can be converted to a Velox expression. Returns true if the function is in
   * FUNCTION_MAP, NAME_MAP, or is a special-case SqlKind.
   */
  public static boolean isSupported(RexCall call) {
    SqlKind kind = call.getKind();

    // SqlKind-based operators and special cases
    if (FUNCTION_MAP.containsKey(kind)
        || kind == SqlKind.CAST
        || kind == SqlKind.IS_NOT_NULL
        || kind == SqlKind.IN
        || kind == SqlKind.BETWEEN
        || kind == SqlKind.SEARCH
        || kind == SqlKind.TRIM
        || kind == SqlKind.CASE) {
      return true;
    }

    // Named functions: check operator name against NAME_MAP
    String opName = call.getOperator().getName().toUpperCase(Locale.ROOT);
    return SUPPORTED_FUNCTIONS.contains(opName);
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
      return new IntegerType();
    } else if (variant instanceof BigIntValue) {
      return new BigIntType();
    } else if (variant instanceof DoubleValue) {
      return new DoubleType();
    } else if (variant instanceof RealValue) {
      return new RealType();
    } else if (variant instanceof BooleanValue) {
      return new BooleanType();
    } else if (variant instanceof VarCharValue) {
      return new VarCharType();
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

    // Handle TRIM with flag (LEADING → ltrim, TRAILING → rtrim, BOTH → trim)
    if (kind == SqlKind.TRIM) {
      return convertTrim(call);
    }

    // Handle CASE → Velox switch expression
    if (kind == SqlKind.CASE) {
      return convertCase(call);
    }

    // General function call conversion
    String functionName = FUNCTION_MAP.get(kind);
    if (functionName == null) {
      // Layer 2: look up operator name in NAME_MAP
      String opName = call.getOperator().getName().toUpperCase(Locale.ROOT);
      functionName = NAME_MAP.get(opName);
      if (functionName == null) {
        // Final fallback: use the operator name in lower case
        functionName = call.getOperator().getName().toLowerCase(Locale.ROOT);
      }
    }

    List<TypedExpr> inputs = new ArrayList<>(call.getOperands().size());
    for (RexNode operand : call.getOperands()) {
      inputs.add(convert(operand));
    }

    // Velox requires exact type match for all operators. When operand types differ
    // (e.g., DOUBLE field vs INTEGER literal), insert implicit casts.
    // Check both SqlKind (for standard operators) and function name (for PPL operators
    // that use SqlKind.OTHER_FUNCTION but map to arithmetic Velox functions).
    if (inputs.size() == 2
        && (isComparison(kind) || isArithmetic(kind) || isArithmeticFunction(functionName))) {
      inputs = coerceBinaryTypes(inputs);
    }

    // Velox math/trig functions are registered for DOUBLE only. Cast integer args to DOUBLE
    // when calling these functions (e.g., abs(INTEGER) → abs(CAST(INTEGER AS DOUBLE))).
    if (isDoubleMathFunction(functionName)) {
      inputs = castIntArgsToDouble(inputs);
    }

    Type returnType = VeloxTypeConverter.toVeloxType(call.getType());
    return new CallTypedExpr(returnType, inputs, functionName);
  }

  /**
   * Convert TRIM(flag, trimChar, string) → Velox ltrim/rtrim/trim. Calcite's TRIM has 3 operands:
   * [0]=flag (LEADING/TRAILING/BOTH), [1]=trim character, [2]=input string.
   */
  private TypedExpr convertTrim(RexCall call) {
    List<RexNode> operands = call.getOperands();
    // Determine trim direction from the flag operand
    String veloxFunc = "trim";
    if (operands.get(0) instanceof RexLiteral) {
      RexLiteral flag = (RexLiteral) operands.get(0);
      SqlTrimFunction.Flag trimFlag = flag.getValueAs(SqlTrimFunction.Flag.class);
      if (trimFlag == SqlTrimFunction.Flag.LEADING) {
        veloxFunc = "ltrim";
      } else if (trimFlag == SqlTrimFunction.Flag.TRAILING) {
        veloxFunc = "rtrim";
      }
    }
    // Velox trim functions take just the input string (trim default whitespace)
    TypedExpr input = convert(operands.get(2));
    Type returnType = VeloxTypeConverter.toVeloxType(call.getType());
    return new CallTypedExpr(returnType, Collections.singletonList(input), veloxFunc);
  }

  /**
   * Convert CASE WHEN c1 THEN v1 WHEN c2 THEN v2 ... ELSE vN END → Velox switch expression.
   * Calcite's CASE has operands: [cond1, val1, cond2, val2, ..., elseVal].
   */
  private TypedExpr convertCase(RexCall call) {
    List<RexNode> operands = call.getOperands();
    List<TypedExpr> inputs = new ArrayList<>();
    for (RexNode operand : operands) {
      inputs.add(convert(operand));
    }
    Type returnType = VeloxTypeConverter.toVeloxType(call.getType());
    return new CallTypedExpr(returnType, inputs, "switch");
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

  private static boolean isComparison(SqlKind kind) {
    return kind == SqlKind.EQUALS
        || kind == SqlKind.NOT_EQUALS
        || kind == SqlKind.GREATER_THAN
        || kind == SqlKind.GREATER_THAN_OR_EQUAL
        || kind == SqlKind.LESS_THAN
        || kind == SqlKind.LESS_THAN_OR_EQUAL;
  }

  /** Functions that Velox registers only for DOUBLE arguments. Integer args need casting. */
  private static final Set<String> DOUBLE_MATH_FUNCTIONS =
      Set.of(
          "abs",
          "ceil",
          "floor",
          "round",
          "truncate",
          "sign",
          "power",
          "sqrt",
          "cbrt",
          "exp",
          "ln",
          "log10",
          "log2",
          "cos",
          "sin",
          "tan",
          "acos",
          "asin",
          "atan",
          "atan2",
          "cosh",
          "cot",
          "radians",
          "degrees");

  private static boolean isDoubleMathFunction(String functionName) {
    return DOUBLE_MATH_FUNCTIONS.contains(functionName);
  }

  /** Cast integer-typed arguments to DOUBLE for math functions that only accept DOUBLE. */
  private List<TypedExpr> castIntArgsToDouble(List<TypedExpr> inputs) {
    List<TypedExpr> result = new ArrayList<>(inputs.size());
    for (TypedExpr input : inputs) {
      if (isIntegerType(input.getReturnType())) {
        result.add(CastTypedExpr.create(new DoubleType(), input, false));
      } else {
        result.add(input);
      }
    }
    return result;
  }

  private boolean isIntegerType(Type type) {
    return type instanceof TinyIntType
        || type instanceof SmallIntType
        || type instanceof IntegerType
        || type instanceof BigIntType;
  }

  private static boolean isArithmetic(SqlKind kind) {
    return kind == SqlKind.PLUS
        || kind == SqlKind.MINUS
        || kind == SqlKind.TIMES
        || kind == SqlKind.DIVIDE
        || kind == SqlKind.MOD;
  }

  private static final Set<String> ARITHMETIC_FUNCTIONS =
      Set.of("plus", "minus", "multiply", "divide", "modulus");

  private static boolean isArithmeticFunction(String functionName) {
    return ARITHMETIC_FUNCTIONS.contains(functionName);
  }

  /**
   * Coerce mismatched types in binary comparisons by inserting a CAST on the narrower operand.
   * Velox requires exact type match for comparison functions. Calcite represents integer literals
   * as DECIMAL(scale=0) which our converter turns into IntegerValue/BigIntValue, but the column may
   * be DOUBLE — causing a type mismatch crash in Velox.
   */
  private List<TypedExpr> coerceBinaryTypes(List<TypedExpr> inputs) {
    TypedExpr left = inputs.get(0);
    TypedExpr right = inputs.get(1);
    Type leftType = left.getReturnType();
    Type rightType = right.getReturnType();

    if (leftType.getClass().equals(rightType.getClass())) {
      return inputs; // types match, no coercion needed
    }

    // Determine the wider type and cast the other operand
    Type wider = widerNumericType(leftType, rightType);
    if (wider == null) {
      return inputs; // non-numeric or unknown types, leave as-is
    }

    List<TypedExpr> coerced = new ArrayList<>(2);
    coerced.add(
        leftType.getClass().equals(wider.getClass())
            ? left
            : CastTypedExpr.create(wider, left, false));
    coerced.add(
        rightType.getClass().equals(wider.getClass())
            ? right
            : CastTypedExpr.create(wider, right, false));
    return coerced;
  }

  /** Return the wider of two numeric types, or null if not both numeric. */
  private Type widerNumericType(Type a, Type b) {
    int rankA = numericRank(a);
    int rankB = numericRank(b);
    if (rankA < 0 || rankB < 0) return null;
    return rankA >= rankB ? a : b;
  }

  private int numericRank(Type t) {
    if (t instanceof TinyIntType) return 0;
    if (t instanceof SmallIntType) return 1;
    if (t instanceof IntegerType) return 2;
    if (t instanceof BigIntType) return 3;
    if (t instanceof RealType) return 4;
    if (t instanceof DoubleType) return 5;
    return -1;
  }
}
