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
import org.boostscale.velox4j.type.TimestampType;
import org.boostscale.velox4j.type.TinyIntType;
import org.boostscale.velox4j.type.Type;
import org.boostscale.velox4j.type.VarCharType;
import org.boostscale.velox4j.variant.BigIntValue;
import org.boostscale.velox4j.variant.BooleanValue;
import org.boostscale.velox4j.variant.DoubleValue;
import org.boostscale.velox4j.variant.IntegerValue;
import org.boostscale.velox4j.variant.RealValue;
import org.boostscale.velox4j.variant.TimestampValue;
import org.boostscale.velox4j.variant.VarCharValue;
import org.boostscale.velox4j.variant.Variant;
import org.opensearch.sql.calcite.type.AbstractExprRelDataType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;

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
    // NOT_EQUALS is intentionally NOT mapped here — it is rewritten to not(equalto(a,b)) in
    // convertCall. Velox's Spark registration has `notequalto` only for DECIMAL; generic types
    // would fail at compile time. See the NOT_EQUALS branch in convertCall.
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
    // Velox registers the modulo scalar as "mod" (Presto: CheckedModulusFunction for integers,
    // ModulusFunction for floats); there is no "modulus" registration under any dialect.
    FUNCTION_MAP.put(SqlKind.MOD, "mod");

    // String operators
    FUNCTION_MAP.put(SqlKind.LIKE, "like");

    // Null checks
    FUNCTION_MAP.put(SqlKind.IS_NULL, "is_null");
    FUNCTION_MAP.put(SqlKind.IS_NOT_NULL, "not");
  }

  // ---- Layer 1b: PPL extract(<unit>, <datetime>) → Velox date-part scalar name ----
  //
  // PPL's `extract(part FROM datetime)` compiles to the SQL plugin's EXTRACT UDF
  // (org.opensearch.sql.expression.function.udf.datetime.ExtractFunction), NOT Calcite's
  // built-in SqlStdOperatorTable.EXTRACT. Its RexCall has SqlKind.OTHER_FUNCTION, operator name
  // "EXTRACT", and two operands: a VARCHAR literal (the part name) and the datetime column. We
  // route the upper-cased part name to a dedicated Velox scalar (`year`, `month`, `minute`, ...)
  // because Velox has no generic `extract(VARCHAR, TIMESTAMP)` registration.
  //
  // The same table also handles Calcite's built-in EXTRACT (SqlKind.EXTRACT), whose first
  // operand is a TimeUnitRange symbol literal — we resolve its `.name()` through this map.
  private static final Map<String, String> EXTRACT_UNIT_NAME_TO_VELOX_FN;

  static {
    Map<String, String> m = new HashMap<>();
    m.put("YEAR", "year");
    m.put("QUARTER", "quarter");
    m.put("MONTH", "month");
    // Calcite WEEK / PPL "week" map to Velox's week_of_year.
    m.put("WEEK", "week_of_year");
    m.put("WEEK_OF_YEAR", "week_of_year");
    m.put("DAY", "day");
    m.put("DAY_OF_MONTH", "day");
    m.put("DOW", "day_of_week");
    m.put("DAY_OF_WEEK", "day_of_week");
    m.put("DOY", "day_of_year");
    m.put("DAY_OF_YEAR", "day_of_year");
    m.put("HOUR", "hour");
    m.put("MINUTE", "minute");
    m.put("SECOND", "second");
    EXTRACT_UNIT_NAME_TO_VELOX_FN = Map.copyOf(m);
  }

  // ---- Layer 1c: PPL span(<field>, <value>, <unit>) → Velox bucketing expression ----
  //
  // PPL's `span()` is a 3-operand SqlUserDefinedFunction with SqlKind.OTHER_FUNCTION, operator
  // name "SPAN". Operand layout (from CalciteRexNodeVisitor.visitSpan in the SQL plugin):
  //   [0] field (RexInputRef or RexCall)
  //   [1] numeric width literal (INTEGER for time spans; any numeric for numeric spans)
  //   [2] VARCHAR unit literal (short code from SpanUnit.getName()) for time spans, or
  //       cast(null) for numeric spans.
  // Velox has no native `span` scalar; we lower to one of three expression shapes in
  // convertSpan: date_trunc (count=1), arithmetic seconds bucket (fixed-length unit,
  // count>1), or floor+divide+multiply (numeric).

  // Unit code → Velox date_trunc unit name. Case-sensitive: "M" (month) vs "m" (minute).
  // `ms`/`us` intentionally absent — Velox date_trunc only supports second..year.
  private static final Map<String, String> SPAN_UNIT_TO_DATE_TRUNC =
      Map.ofEntries(
          Map.entry("s", "second"),
          Map.entry("m", "minute"),
          Map.entry("h", "hour"),
          Map.entry("d", "day"),
          Map.entry("w", "week"),
          Map.entry("M", "month"),
          Map.entry("q", "quarter"),
          Map.entry("y", "year"));

  // Unit code → whole-second length. Month/quarter/year excluded — they're variable-length
  // and the seconds-arithmetic shortcut (to_unixtime/floor/from_unixtime) would be wrong.
  // Week ("w") is also excluded for count > 1: date_trunc('week', ts) anchors to Monday,
  // but floor(to_unixtime(ts)/604800)*604800 anchors to Thursday (Unix epoch 1970-01-01 was
  // a Thursday). Multi-week buckets via the arithmetic path would drift by 3 days relative
  // to both count=1 week spans and the default engine, silently returning wrong groups. So
  // span(ts, Nw) for N>1 falls back to the default engine.
  private static final Map<String, Long> SPAN_UNIT_SECONDS =
      Map.of("s", 1L, "m", 60L, "h", 3600L, "d", 86_400L);

  // Units valid for date_trunc on a DATE input. Velox's DATE variant of date_trunc only
  // accepts day-and-above (sub-day units make no sense on a day-granularity type).
  private static final Set<String> SPAN_UNIT_DATE_VALID = Set.of("d", "w", "M", "q", "y");

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
    NAME_MAP.put("MOD", "mod");
    NAME_MAP.put("/", "divide");
    NAME_MAP.put("%", "mod");

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
    kinds.add(SqlKind.ITEM); // Struct/MAP field access (e.g., cloud['region'])
    kinds.add(SqlKind.OTHER_FUNCTION); // Named functions resolved via NAME_MAP
    kinds.add(SqlKind.OTHER); // Named functions resolved via NAME_MAP
    SUPPORTED_SQLKINDS = Set.copyOf(kinds);
  }

  private final RelDataType inputRowType;
  private final RexBuilder rexBuilder;

  private final FieldMapping[] fieldMappings; // MAP→ROW index remapping (nullable)
  private final org.boostscale.velox4j.type.RowType veloxOutputType; // Velox scan output type

  public VeloxExprConverter(RelDataType inputRowType) {
    this(inputRowType, null, null);
  }

  /**
   * Create a converter with field mappings for MAP→ROW remapping. The mappings array has one entry
   * per Calcite field index. Each entry specifies the Velox output index and optional child path
   * for nested struct access.
   */
  public VeloxExprConverter(
      RelDataType inputRowType,
      FieldMapping[] fieldMappings,
      org.boostscale.velox4j.type.RowType veloxOutputType) {
    this.inputRowType = inputRowType;
    this.fieldMappings = fieldMappings;
    this.veloxOutputType = veloxOutputType;
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
        || kind == SqlKind.NOT_EQUALS
        || kind == SqlKind.IN
        || kind == SqlKind.BETWEEN
        || kind == SqlKind.SEARCH
        || kind == SqlKind.TRIM
        || kind == SqlKind.CASE
        || kind == SqlKind.ITEM) {
      return true;
    }

    // EXTRACT is supported only for unit names that map to a registered Velox scalar (year /
    // month / quarter / week_of_year / day / day_of_week / day_of_year / hour / minute / second).
    // Other units (EPOCH, DECADE, CENTURY, MILLENNIUM, ...) cause canVectorize to reject the
    // query so it falls back to the default engine. Matches both Calcite's built-in EXTRACT
    // (SqlKind.EXTRACT) and PPL's EXTRACT UDF (SqlKind.OTHER_FUNCTION, name "EXTRACT").
    if (kind == SqlKind.EXTRACT || isPplExtractFunction(call)) {
      if (call.getOperands().size() == 2 && call.getOperands().get(0) instanceof RexLiteral) {
        String unitName = extractUnitName((RexLiteral) call.getOperands().get(0));
        return unitName != null
            && EXTRACT_UNIT_NAME_TO_VELOX_FN.containsKey(unitName.toUpperCase(Locale.ROOT));
      }
      return false;
    }

    // SPAN is supported for four shapes: TIMESTAMP (count=1 via date_trunc), TIMESTAMP
    // (count>1 via fixed-length seconds arithmetic on s/m/h/d, not w), DATE (count=1 via
    // date_trunc on d/w/M/q/y), and numeric (floor/divide/multiply in integer or DOUBLE
    // space). Rejects that keep queries correct by falling back:
    //   - TIME inputs (PPL's ExprTimeType or SqlTypeName.TIME) — velox4j Java has no TimeType
    //     wrapper, so we cannot emit date_trunc(TIME). Falling back to the numeric path would
    //     silently bucket raw millis ignoring the unit string.
    //   - DATE span with sub-day units (s/m/h/ms/us) or count>1 — Velox's date_trunc(DATE)
    //     only accepts day-and-above; multi-count DATE spans would need interval math we
    //     don't emit here.
    //   - TIMESTAMP `ms`/`us` units — date_trunc doesn't accept them.
    //   - TIMESTAMP `w` with count>1 — epoch-anchored arithmetic (Thursday) disagrees with
    //     date_trunc's Monday anchor.
    //   - TIMESTAMP `M`/`q`/`y` with count>1 — variable-length units.
    //   - DECIMAL numeric columns — precision-preserving DECIMAL arithmetic isn't wired up.
    //   - Non-literal width; zero or negative count.
    if (isPplSpanFunction(call)) {
      List<RexNode> ops = call.getOperands();
      if (!(ops.get(1) instanceof RexLiteral)) {
        return false;
      }
      RexNode unitRex = ops.get(2);
      boolean hasUnit = unitRex instanceof RexLiteral && !((RexLiteral) unitRex).isNull();
      SpanFieldKind fieldKind = spanFieldKind(ops.get(0));

      if (fieldKind == SpanFieldKind.TIMESTAMP) {
        if (!hasUnit) {
          return false;
        }
        String unit = ((RexLiteral) unitRex).getValueAs(String.class);
        long count = readSpanCount((RexLiteral) ops.get(1));
        if (count <= 0) {
          return false;
        }
        if (count == 1) {
          return SPAN_UNIT_TO_DATE_TRUNC.containsKey(unit);
        }
        return SPAN_UNIT_SECONDS.containsKey(unit);
      }
      if (fieldKind == SpanFieldKind.DATE) {
        if (!hasUnit) {
          return false;
        }
        String unit = ((RexLiteral) unitRex).getValueAs(String.class);
        long count = readSpanCount((RexLiteral) ops.get(1));
        return count == 1 && SPAN_UNIT_DATE_VALID.contains(unit);
      }
      if (fieldKind == SpanFieldKind.TIME) {
        // See comment above — velox4j has no TimeType wrapper, so TIME span falls back.
        return false;
      }
      // fieldKind == NONE: numeric span. The unit operand must be null (no string unit on a
      // numeric value), else this is a malformed or unsupported shape.
      if (hasUnit) {
        return false;
      }
      // Width must be a strictly-positive literal. Zero would produce divide-by-zero in either
      // lowering path; negative values produce nonsensical bucket boundaries. Match the
      // temporal branch's `count > 0` check.
      BigDecimal widthValue = readNumericWidth((RexLiteral) ops.get(1));
      if (widthValue == null || widthValue.signum() <= 0) {
        return false;
      }
      // Reject DECIMAL field columns — precision-preserving DECIMAL arithmetic isn't wired up
      // here, and routing a DECIMAL column through DOUBLE would merge distinct buckets for
      // large values.
      SqlTypeName fieldType = ops.get(0).getType().getSqlTypeName();
      SqlTypeName widthType = ops.get(1).getType().getSqlTypeName();
      if (fieldType == SqlTypeName.DECIMAL) {
        return false;
      }
      // Scaled DECIMAL width (e.g. `0.5`): only safe if the field is floating-point, where the
      // DOUBLE arithmetic path handles the fractional width correctly. For integer fields, a
      // scaled width would force DOUBLE conversion and lose BIGINT precision — reject.
      if (widthType == SqlTypeName.DECIMAL) {
        boolean integralWidth = widthValue.scale() <= 0;
        boolean floatingField =
            fieldType == SqlTypeName.DOUBLE
                || fieldType == SqlTypeName.REAL
                || fieldType == SqlTypeName.FLOAT;
        if (!integralWidth && !floatingField) {
          return false;
        }
      }
      return true;
    }

    // Named functions: check operator name against NAME_MAP
    String opName = call.getOperator().getName().toUpperCase(Locale.ROOT);
    return SUPPORTED_FUNCTIONS.contains(opName);
  }

  private TypedExpr convertInputRef(RexInputRef inputRef) {
    int calciteIndex = inputRef.getIndex();

    // Use field mappings for MAP→ROW index remapping when available
    if (fieldMappings != null
        && calciteIndex < fieldMappings.length
        && fieldMappings[calciteIndex] != null) {
      return convertMappedInputRef(calciteIndex);
    }

    // Fallback: no mapping (exchange scans, join contexts, etc.)
    RelDataTypeField field = inputRowType.getFieldList().get(calciteIndex);
    Type veloxType = VeloxTypeConverter.toVeloxType(field.getType());
    return FieldAccessTypedExpr.create(veloxType, field.getName());
  }

  /**
   * Convert a RexInputRef using field mapping. All fields are flat in the Velox scan output. MAP
   * parent columns (veloxIndex=-1) are skipped — they should only appear in output Projects, not in
   * sort/filter expressions.
   */
  private TypedExpr convertMappedInputRef(int calciteIndex) {
    FieldMapping mapping = fieldMappings[calciteIndex];
    int veloxIndex = mapping.getVeloxIndex();
    if (veloxIndex < 0) {
      // MAP parent column (cloud, metrics, etc.) — skipped from scan output.
      // This happens when SELECT * projects the parent MAP column.
      // Return a VARCHAR placeholder — the actual struct reconstruction happens in Java.
      return FieldAccessTypedExpr.create(new VarCharType(), mapping.getVeloxFieldName());
    }
    Type veloxType = veloxOutputType.getChildren().get(veloxIndex);
    String fieldName = veloxOutputType.getNames().get(veloxIndex);
    return FieldAccessTypedExpr.create(veloxType, fieldName);
  }

  private TypedExpr convertLiteral(RexLiteral literal) {
    Variant variant = toVariant(literal);
    // Prefer the Calcite literal's declared type so DATE/TIME literals carry DateType /
    // BigIntType (Velox TIME) rather than whatever the backing variant happens to be
    // (IntegerValue / BigIntValue). For the DECIMAL→INTEGER/BIGINT/DOUBLE widening case,
    // the variant does encode the promoted type, so fall back to variant-derived type only
    // for SqlTypeName.DECIMAL (and nulls).
    Type veloxType;
    if (literal.getTypeName() == SqlTypeName.DECIMAL && variant != null) {
      veloxType = variantToType(variant);
    } else {
      veloxType = VeloxTypeConverter.toVeloxType(literal.getType());
    }
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
    } else if (variant instanceof TimestampValue) {
      return new TimestampType();
    }
    throw new UnsupportedOperationException("Unknown variant type: " + variant.getClass());
  }

  private Variant toVariant(RexLiteral literal) {
    if (literal.isNull()) {
      return toNullVariant(literal.getType().getSqlTypeName());
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
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        {
          Long millis = literal.getValueAs(Long.class);
          if (millis == null) {
            return TimestampValue.createNull();
          }
          return timestampVariantFromMillis(millis);
        }
      case DATE:
        {
          // Calcite DATE literals: days since epoch. Velox DateType is int32 days since epoch.
          Integer days = literal.getValueAs(Integer.class);
          return new IntegerValue(days);
        }
      case TIME:
      case TIME_WITH_LOCAL_TIME_ZONE:
        {
          // Calcite TIME literals: millis since midnight. Velox TIME is BIGINT-backed.
          Integer timeMillis = literal.getValueAs(Integer.class);
          return new BigIntValue(timeMillis == null ? null : timeMillis.longValue());
        }
      default:
        throw new UnsupportedOperationException("Unsupported literal type: " + typeName);
    }
  }

  /** Build a TimestampValue variant from epoch milliseconds. */
  public static TimestampValue timestampVariantFromMillis(long millis) {
    long seconds = Math.floorDiv(millis, 1000L);
    long nanos = Math.floorMod(millis, 1000L) * 1_000_000L;
    return TimestampValue.create(seconds, nanos);
  }

  /**
   * Create a typed Variant with null inner value. velox4j requires a non-null Variant object to
   * represent a null constant — the Variant wraps the null value while preserving the type.
   */
  private Variant toNullVariant(SqlTypeName typeName) {
    switch (typeName) {
      case BOOLEAN:
        return new BooleanValue(null);
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return new IntegerValue(null);
      case BIGINT:
        return new BigIntValue(null);
      case FLOAT:
      case REAL:
        return new RealValue(null);
      case DOUBLE:
      case DECIMAL:
        return new DoubleValue(null);
      case CHAR:
      case VARCHAR:
      case NULL:
      case ANY:
        return new VarCharValue(null);
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return TimestampValue.createNull();
      case DATE:
        return new IntegerValue(null);
      case TIME:
      case TIME_WITH_LOCAL_TIME_ZONE:
        return new BigIntValue(null);
      default:
        return new VarCharValue(null);
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

    // Handle NOT_EQUALS as not(equalto(a, b)) — Velox's Spark comparison registration provides
    // only `equalto` for generic types. There's a `notequalto` registration but only for DECIMAL,
    // so using it for (VARCHAR,VARCHAR) or (INTEGER,INTEGER) would fail signature resolution.
    // Rewriting to not(equalto) always resolves cleanly. Operands are run through the same
    // binary-type coercion used for other comparisons so (SMALLINT, INTEGER) from e.g.
    // `short_col != 0` doesn't hit equalto's exact-match signature requirement.
    if (kind == SqlKind.NOT_EQUALS) {
      TypedExpr left = convert(call.getOperands().get(0));
      TypedExpr right = convert(call.getOperands().get(1));
      List<TypedExpr> coerced = coerceBinaryTypes(List.of(left, right));
      TypedExpr eq = new CallTypedExpr(new BooleanType(), coerced, "equalto");
      return new CallTypedExpr(new BooleanType(), Collections.singletonList(eq), "not");
    }

    // Handle IN as chain of OR(EQ(...))
    if (kind == SqlKind.IN) {
      return convertIn(call);
    }

    // Handle BETWEEN as AND(GTE, LTE)
    if (kind == SqlKind.BETWEEN) {
      return convertBetween(call);
    }

    // Handle ITEM(parent, 'field') → nested FieldAccess for struct/ROW columns.
    // The SQL plugin generates ITEM(cloud, 'region') for PPL's cloud.region access.
    if (kind == SqlKind.ITEM) {
      return convertItem(call);
    }

    // Handle TRIM with flag (LEADING → ltrim, TRAILING → rtrim, BOTH → trim)
    if (kind == SqlKind.TRIM) {
      return convertTrim(call);
    }

    // Handle CASE → Velox switch expression
    if (kind == SqlKind.CASE) {
      return convertCase(call);
    }

    // Handle EXTRACT — both Calcite's SqlStdOperatorTable.EXTRACT (SqlKind.EXTRACT) and PPL's
    // own EXTRACT UDF (SqlKind.OTHER_FUNCTION, operator name "EXTRACT"). PPL typically routes
    // through the latter. Both shapes lower to a dedicated Velox date-part scalar here.
    if (kind == SqlKind.EXTRACT || isPplExtractFunction(call)) {
      TypedExpr extracted = convertExtract(call);
      if (extracted != null) {
        return extracted;
      }
      // Unit not supported — fall through to the generic path (which will emit the generic
      // extract(VARCHAR, TIMESTAMP) call and fail at Velox signature resolution).
      // isSupported() below rejects unsupported units up front, so this branch is only
      // reachable via the force_vectorize escape hatch.
    }

    // Handle PPL's span(field, width, unit) — lowers to date_trunc (count=1), an arithmetic
    // seconds bucket (count>1 on fixed-length units), or floor/divide/multiply (numeric).
    // Velox has no native span scalar. isSupported() rejects unsupported cases; this branch
    // only produces an expression for the supported ones.
    if (isPplSpanFunction(call)) {
      TypedExpr lowered = convertSpan(call);
      if (lowered != null) {
        return lowered;
      }
      // Fall through to generic path — only reachable via force_vectorize.
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

  /**
   * True for PPL's own EXTRACT UDF (SqlKind.OTHER_FUNCTION with operator name "EXTRACT"). PPL's
   * {@code extract(part FROM datetime)} lowers to this UDF rather than Calcite's built-in
   * SqlStdOperatorTable.EXTRACT.
   */
  private static boolean isPplExtractFunction(RexCall call) {
    return "EXTRACT".equalsIgnoreCase(call.getOperator().getName())
        && call.getOperands().size() == 2;
  }

  /**
   * Convert EXTRACT(&lt;unit&gt;, &lt;datetime&gt;) to a Velox date-part scalar call. Handles two
   * shapes:
   *
   * <ul>
   *   <li>PPL's EXTRACT UDF — first operand is a VARCHAR RexLiteral holding the part name.
   *   <li>Calcite's built-in SqlStdOperatorTable.EXTRACT — first operand is a symbol literal whose
   *       getValue() is a TimeUnitRange enum; we use its name().
   * </ul>
   *
   * <p>Returns null when the unit isn't in {@link #EXTRACT_UNIT_NAME_TO_VELOX_FN} — caller treats
   * null as "fall through to the generic path". The Velox registrations return INTEGER (Spark
   * dialect overwrites Presto's BIGINT during velox4j init); Calcite's declared return type is
   * BIGINT, so we wrap the scalar in a CAST to match the declared type downstream operators expect.
   */
  private TypedExpr convertExtract(RexCall call) {
    List<RexNode> operands = call.getOperands();
    if (operands.size() != 2 || !(operands.get(0) instanceof RexLiteral)) {
      return null;
    }
    String unitName = extractUnitName((RexLiteral) operands.get(0));
    if (unitName == null) {
      return null;
    }
    String veloxFn = EXTRACT_UNIT_NAME_TO_VELOX_FN.get(unitName.toUpperCase(Locale.ROOT));
    if (veloxFn == null) {
      return null;
    }
    TypedExpr arg = convert(operands.get(1));
    // velox4j registers BOTH Presto (BIGINT) and Spark (INTEGER) variants of year/month/day/etc.
    // under the same name. Unlike window ranking functions where Spark overwrites, these scalar
    // registrations coexist because their physical signatures differ only in the return type.
    // Asking for either INTEGER or BIGINT explicitly hits Velox's "incompatible return types"
    // error at compile time when the resolver sees both candidates. Picking Calcite's declared
    // type (normally BIGINT) resolves unambiguously and avoids the extra cast.
    Type returnType = VeloxTypeConverter.toVeloxType(call.getType());
    return new CallTypedExpr(returnType, Collections.singletonList(arg), veloxFn);
  }

  /**
   * Extract the unit name (e.g. "MINUTE") from the first operand of an EXTRACT call. Handles both
   * VARCHAR literals (PPL UDF) and SYMBOL literals carrying a Calcite {@code TimeUnitRange}.
   * Returns null when the literal shape is unexpected.
   */
  private static String extractUnitName(RexLiteral unitLit) {
    SqlTypeName typeName = unitLit.getTypeName();
    if (typeName == SqlTypeName.CHAR || typeName == SqlTypeName.VARCHAR) {
      String s = unitLit.getValueAs(String.class);
      return s == null ? null : s.trim();
    }
    if (typeName == SqlTypeName.SYMBOL) {
      // TimeUnitRange (or TimeUnit) — use the enum name. Avoid a hard reference to the Avatica
      // class so we don't need it on the compile classpath.
      Object v = unitLit.getValue();
      if (v instanceof Enum<?>) {
        return ((Enum<?>) v).name();
      }
      return v == null ? null : v.toString();
    }
    return null;
  }

  /** True for PPL's span UDF (SqlKind.OTHER_FUNCTION with operator name "SPAN", 3 operands). */
  private static boolean isPplSpanFunction(RexCall call) {
    return "SPAN".equalsIgnoreCase(call.getOperator().getName()) && call.getOperands().size() == 3;
  }

  /**
   * Classification of a span field's logical temporal kind. We must inspect Calcite's {@link
   * RelDataType} (not the Velox type) because PPL's UDTs ({@link AbstractExprRelDataType}) report
   * {@link SqlTypeName#VARCHAR} but tag themselves with an {@link ExprUDT} and map to distinct
   * Velox types ({@code TimestampType}, {@code DateType}, {@code BigIntType} for TIME). A plain
   * numeric column lands in {@link #NONE}.
   */
  private enum SpanFieldKind {
    TIMESTAMP,
    DATE,
    TIME, // not supported — velox4j Java has no TimeType wrapper, so we can't emit date_trunc(TIME)
    NONE
  }

  /** Inspect a span's field operand and decide which lowering path applies. */
  private static SpanFieldKind spanFieldKind(RexNode fieldRex) {
    RelDataType type = fieldRex.getType();
    if (type instanceof AbstractExprRelDataType<?>) {
      ExprUDT udt = ((AbstractExprRelDataType<?>) type).getUdt();
      switch (udt) {
        case EXPR_TIMESTAMP:
          return SpanFieldKind.TIMESTAMP;
        case EXPR_DATE:
          return SpanFieldKind.DATE;
        case EXPR_TIME:
          return SpanFieldKind.TIME;
        default:
          break;
      }
    }
    switch (type.getSqlTypeName()) {
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return SpanFieldKind.TIMESTAMP;
      case DATE:
        return SpanFieldKind.DATE;
      case TIME:
      case TIME_WITH_LOCAL_TIME_ZONE:
        return SpanFieldKind.TIME;
      default:
        return SpanFieldKind.NONE;
    }
  }

  /**
   * Convert PPL's {@code span(field, width, unit)} into a Velox bucketing expression. Four shapes
   * are supported, matching {@link #isSupported}:
   *
   * <ul>
   *   <li>TIMESTAMP span, count = 1 → {@code date_trunc(<truncUnit>, ts)}.
   *   <li>TIMESTAMP span, count > 1 on fixed-length second-aligned units (s/m/h/d) → {@code
   *       from_unixtime(floor(to_unixtime(ts) / N) * N)} with {@code N = count × unit_seconds}.
   *       Week is excluded for count &gt; 1 because epoch is Thursday-aligned but {@code
   *       date_trunc('week')} is Monday-aligned.
   *   <li>DATE span, count = 1 on day/week/month/quarter/year → {@code date_trunc(<truncUnit>,
   *       date)}. Velox's DATE variant of date_trunc only accepts day-and-above units.
   *   <li>Numeric span → {@code floor(col / width) * width}. Integer operands stay in integer space
   *       to preserve BIGINT precision above 2^53.
   * </ul>
   *
   * <p>Returns null when the call shape isn't one of the above. {@code isSupported} rejects
   * unsupported cases up front on the normal path. TIME inputs fall back because velox4j has no
   * TimeType wrapper — we can't emit {@code date_trunc(TIME)} from the Java binding.
   */
  private TypedExpr convertSpan(RexCall call) {
    List<RexNode> ops = call.getOperands();
    if (!(ops.get(1) instanceof RexLiteral)) {
      return null;
    }
    RexLiteral widthLit = (RexLiteral) ops.get(1);
    RexNode unitRex = ops.get(2);
    TypedExpr fieldExpr = convert(ops.get(0));
    Type returnType = VeloxTypeConverter.toVeloxType(call.getType());

    SpanFieldKind kind = spanFieldKind(ops.get(0));
    boolean hasUnit = unitRex instanceof RexLiteral && !((RexLiteral) unitRex).isNull();

    if (kind == SpanFieldKind.TIMESTAMP && hasUnit) {
      String unit = ((RexLiteral) unitRex).getValueAs(String.class);
      long count = readSpanCount(widthLit);
      if (count <= 0) {
        return null;
      }
      return buildTimeSpanExpr(fieldExpr, count, unit, returnType);
    }
    if (kind == SpanFieldKind.DATE && hasUnit) {
      String unit = ((RexLiteral) unitRex).getValueAs(String.class);
      long count = readSpanCount(widthLit);
      if (count != 1 || !SPAN_UNIT_DATE_VALID.contains(unit)) {
        return null; // isSupported rejects these; this is the defensive branch.
      }
      String truncUnit = SPAN_UNIT_TO_DATE_TRUNC.get(unit);
      if (truncUnit == null) {
        return null;
      }
      TypedExpr unitConst =
          ConstantTypedExpr.create(new VarCharType(), new VarCharValue(truncUnit));
      return new CallTypedExpr(returnType, List.of(unitConst, fieldExpr), "date_trunc");
    }
    if (kind != SpanFieldKind.NONE) {
      // TIME span — no velox4j TimeType wrapper available, so we cannot emit date_trunc(TIME).
      // Also catch any stray temporal kind: we must not drop to the numeric path or we'd
      // bucket raw millis/days ignoring the unit. isSupported rejects these; defensive.
      return null;
    }
    // Defense in depth: never emit divide/mod-by-zero even if force_vectorize bypasses
    // isSupported. A non-positive width also indicates nonsensical buckets; fall back.
    BigDecimal widthValue = readNumericWidth(widthLit);
    if (widthValue == null || widthValue.signum() <= 0) {
      return null;
    }
    return buildNumericSpanExpr(fieldExpr, widthLit, returnType);
  }

  /**
   * Time-span lowering. {@code count == 1} → {@code date_trunc(unit, ts)}. {@code count > 1} on
   * fixed-length units → {@code from_unixtime(floor(to_unixtime(ts) / N) * N)} where {@code N =
   * count × unit_seconds}. Returns null if the unit isn't in the lookup tables.
   */
  private TypedExpr buildTimeSpanExpr(
      TypedExpr fieldExpr, long count, String unit, Type returnType) {
    if (count == 1) {
      String truncUnit = SPAN_UNIT_TO_DATE_TRUNC.get(unit);
      if (truncUnit == null) {
        return null;
      }
      TypedExpr unitConst =
          ConstantTypedExpr.create(new VarCharType(), new VarCharValue(truncUnit));
      return new CallTypedExpr(returnType, List.of(unitConst, fieldExpr), "date_trunc");
    }
    Long unitSeconds = SPAN_UNIT_SECONDS.get(unit);
    if (unitSeconds == null) {
      return null;
    }
    long bucketSeconds = Math.multiplyExact(unitSeconds, count);
    DoubleType doubleType = new DoubleType();
    TypedExpr bucketConst =
        ConstantTypedExpr.create(doubleType, new DoubleValue((double) bucketSeconds));
    TypedExpr toUnix = new CallTypedExpr(doubleType, List.of(fieldExpr), "to_unixtime");
    TypedExpr divided = new CallTypedExpr(doubleType, List.of(toUnix, bucketConst), "divide");
    TypedExpr floored = new CallTypedExpr(doubleType, List.of(divided), "floor");
    TypedExpr multiplied = new CallTypedExpr(doubleType, List.of(floored, bucketConst), "multiply");
    return new CallTypedExpr(returnType, List.of(multiplied), "from_unixtime");
  }

  /**
   * Numeric-span lowering: semantically {@code floor(col / width) * width}.
   *
   * <p>Two paths to preserve precision:
   *
   * <ul>
   *   <li><b>Integer types</b> (TINYINT, SMALLINT, INTEGER, BIGINT): emit the Euclidean-modulo form
   *       {@code col - ((col mod width + width) mod width)} in integer space. This matches {@code
   *       floor} semantics for both positive and negative values — a naive {@code
   *       floor(integer_divide(col, w)) * w} would round {@code -1/10} to {@code 0} (Velox integer
   *       divide truncates toward zero and {@code floor} on integers is a no-op), producing the
   *       wrong bucket. Staying in integer space preserves full range for {@code BIGINT} values
   *       above 2<sup>53</sup> that would silently merge when routed through DOUBLE.
   *   <li><b>Floating-point types</b> (REAL, DOUBLE): emit the DOUBLE arithmetic {@code
   *       floor(col/width)*width}. Precision already matches Calcite's declared type and {@code
   *       floor} on DOUBLE is a real floor (not a no-op).
   * </ul>
   *
   * <p>DECIMAL fields fall back at {@link #isSupported}; scaled DECIMAL widths against a
   * floating-point field reach the DOUBLE path.
   */
  private TypedExpr buildNumericSpanExpr(
      TypedExpr fieldExpr, RexLiteral widthLit, Type returnType) {
    TypedExpr widthExpr = convert(widthLit);
    Type fieldType = fieldExpr.getReturnType();
    Type widthType = widthExpr.getReturnType();

    // Integer path: both sides integral. Emit `col - ((col mod w + w) mod w)` so the result is
    // equivalent to `floor(col / w) * w` across the sign range.
    if (isIntegerType(fieldType) && isIntegerType(widthType)) {
      Type wider = numericRank(fieldType) >= numericRank(widthType) ? fieldType : widthType;
      TypedExpr col = castIfDifferent(fieldExpr, wider);
      TypedExpr w = castIfDifferent(widthExpr, wider);
      TypedExpr colModW = new CallTypedExpr(wider, List.of(col, w), "mod");
      TypedExpr colModWPlusW = new CallTypedExpr(wider, List.of(colModW, w), "plus");
      TypedExpr euclideanMod = new CallTypedExpr(wider, List.of(colModWPlusW, w), "mod");
      TypedExpr bucketStart = new CallTypedExpr(wider, List.of(col, euclideanMod), "minus");
      return castIfDifferent(bucketStart, returnType);
    }

    // DOUBLE/REAL path — safe for values inside floating-point precision. Velox's DOUBLE floor
    // is a real floor (not a no-op), so `floor(col/w)*w` has the correct sign behavior here.
    DoubleType doubleType = new DoubleType();
    TypedExpr fieldAsDouble = castIfDifferent(fieldExpr, doubleType);
    TypedExpr widthAsDouble = castIfDifferent(widthExpr, doubleType);
    TypedExpr divided =
        new CallTypedExpr(doubleType, List.of(fieldAsDouble, widthAsDouble), "divide");
    TypedExpr floored = new CallTypedExpr(doubleType, List.of(divided), "floor");
    TypedExpr multiplied =
        new CallTypedExpr(doubleType, List.of(floored, widthAsDouble), "multiply");
    return castIfDifferent(multiplied, returnType);
  }

  /** Wrap {@code expr} in a CAST to {@code target} unless its return type is already target. */
  private static TypedExpr castIfDifferent(TypedExpr expr, Type target) {
    return expr.getReturnType().getClass().equals(target.getClass())
        ? expr
        : CastTypedExpr.create(target, expr, /* isTryCast */ false);
  }

  /**
   * Read a span-count RexLiteral as a positive long. Returns -1 for non-integral values (e.g.,
   * DECIMAL with non-zero scale) — the caller treats that as "not a valid time-span count".
   */
  private static long readSpanCount(RexLiteral widthLit) {
    // Check BigDecimal first: Calcite's getValueAs(Long.class) on a DECIMAL returns the
    // *unscaled* long (0.5 → 5), silently losing scale. We must verify scale <= 0 before
    // accepting a value as integral.
    try {
      BigDecimal bd = widthLit.getValueAs(BigDecimal.class);
      if (bd != null && bd.scale() <= 0) {
        return bd.longValueExact();
      }
      if (bd != null) {
        // Fractional — not a valid time-span count, nor an integer-semantics width.
        return -1L;
      }
    } catch (Exception ignored) {
      // fall through to Long path (non-DECIMAL numeric literal)
    }
    try {
      Long v = widthLit.getValueAs(Long.class);
      if (v != null) {
        return v;
      }
    } catch (Exception ignored) {
      // fall through
    }
    return -1L;
  }

  /**
   * Read a numeric-span width RexLiteral as a signed {@link BigDecimal}. Returns null when the
   * literal can't be interpreted as a numeric value. Callers use this to enforce positivity and to
   * distinguish integral vs fractional widths.
   */
  private static BigDecimal readNumericWidth(RexLiteral widthLit) {
    try {
      BigDecimal bd = widthLit.getValueAs(BigDecimal.class);
      if (bd != null) {
        return bd;
      }
    } catch (Exception ignored) {
      // fall through to Double path (non-DECIMAL numeric literal)
    }
    try {
      Double d = widthLit.getValueAs(Double.class);
      if (d != null) {
        return BigDecimal.valueOf(d);
      }
    } catch (Exception ignored) {
      // fall through
    }
    return null;
  }

  /**
   * Convert ITEM(parent, 'fieldName') → Velox FieldAccessTypedExpr. The SQL plugin generates ITEM
   * calls for nested object field access (e.g., cloud.region → ITEM(cloud, 'region')). The scan
   * produces cloud as a ROW(region: VARCHAR), so this becomes FieldAccessTypedExpr(parent,
   * "region") — a nested field access into the struct.
   */
  private TypedExpr convertItem(RexCall call) {
    List<RexNode> operands = call.getOperands();
    TypedExpr parent = convert(operands.get(0));
    // The second operand is the field name as a string literal
    RexNode keyNode = operands.get(1);
    String fieldName;
    if (keyNode instanceof RexLiteral) {
      fieldName = ((RexLiteral) keyNode).getValueAs(String.class);
    } else {
      throw new UnsupportedOperationException(
          "ITEM access with non-literal key: " + keyNode.getClass().getSimpleName());
    }
    return FieldAccessTypedExpr.create(parent, fieldName);
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
      Set.of("plus", "minus", "multiply", "divide", "mod");

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
