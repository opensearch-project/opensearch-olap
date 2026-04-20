/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.convert;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.boostscale.velox4j.type.ArrayType;
import org.boostscale.velox4j.type.BigIntType;
import org.boostscale.velox4j.type.BooleanType;
import org.boostscale.velox4j.type.DateType;
import org.boostscale.velox4j.type.DecimalType;
import org.boostscale.velox4j.type.DoubleType;
import org.boostscale.velox4j.type.IntegerType;
import org.boostscale.velox4j.type.MapType;
import org.boostscale.velox4j.type.RealType;
import org.boostscale.velox4j.type.RowType;
import org.boostscale.velox4j.type.SmallIntType;
import org.boostscale.velox4j.type.TimestampType;
import org.boostscale.velox4j.type.TinyIntType;
import org.boostscale.velox4j.type.Type;
import org.boostscale.velox4j.type.VarCharType;
import org.opensearch.sql.calcite.type.AbstractExprRelDataType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;

/** Converts Calcite RelDataType to velox4j Type. */
public final class VeloxTypeConverter {

  private VeloxTypeConverter() {}

  public static Type toVeloxType(RelDataType calciteType) {
    // OpenSearch UDT types (ExprDateType, ExprTimeType, ExprTimeStampType) report
    // SqlTypeName.VARCHAR but each has a distinct logical Velox mapping:
    //   EXPR_TIMESTAMP → TimestampType (struct of seconds+nanos)
    //   EXPR_DATE      → DateType      (int32 days since epoch)
    //   EXPR_TIME      → BigIntType    (millis from midnight; Velox TIME is BIGINT-backed,
    //                                   velox4j Java bindings do not expose a TimeType wrapper)
    // LuceneArrowReader emits Arrow Timestamp(micros) for OpenSearch date fields — the Arrow→
    // Velox bridge converts that to Velox Timestamp. EXPR_DATE/EXPR_TIME only arise from
    // expression-level casts/UDFs, not scans.
    if (calciteType instanceof AbstractExprRelDataType<?>) {
      ExprUDT udt = ((AbstractExprRelDataType<?>) calciteType).getUdt();
      switch (udt) {
        case EXPR_TIMESTAMP:
          return new TimestampType();
        case EXPR_DATE:
          return new DateType();
        case EXPR_TIME:
          return new BigIntType();
        case EXPR_IP:
        case EXPR_BINARY:
          return new VarCharType();
        default:
          // Fall through to SqlTypeName-based handling
      }
    }

    SqlTypeName typeName = calciteType.getSqlTypeName();
    switch (typeName) {
      case BOOLEAN:
        return new BooleanType();
      case TINYINT:
        return new TinyIntType();
      case SMALLINT:
        return new SmallIntType();
      case INTEGER:
        return new IntegerType();
      case BIGINT:
        return new BigIntType();
      case FLOAT:
      case REAL:
        return new RealType();
      case DOUBLE:
        return new DoubleType();
      case DECIMAL:
        return new DecimalType(calciteType.getPrecision(), calciteType.getScale());
      case CHAR:
      case VARCHAR:
        return new VarCharType();
      case DATE:
        return new DateType();
      case TIME:
      case TIME_WITH_LOCAL_TIME_ZONE:
        // Velox TIME is BIGINT-backed (millis from midnight); velox4j has no TimeType wrapper.
        return new BigIntType();
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return new TimestampType();
      case NULL:
        // NULL type (from literal NULL expressions) — treat as VARCHAR
        return new VarCharType();
      case ANY:
        // ANY type (from match_only_text, empty objects, or unresolved fields).
        // Treat as VARCHAR — the actual data is handled by scan-level MAP→ROW conversion.
        return new VarCharType();
      case ARRAY:
      case MULTISET:
        return ArrayType.create(toVeloxType(calciteType.getComponentType()));
      case MAP:
        return MapType.create(
            toVeloxType(calciteType.getKeyType()), toVeloxType(calciteType.getValueType()));
      case ROW:
        return toVeloxRowType(calciteType);
      default:
        throw new UnsupportedOperationException("Unsupported Calcite type: " + typeName);
    }
  }

  /** Check if a Calcite type can be converted to a Velox type. */
  public static boolean isSupported(RelDataType calciteType) {
    // OpenSearch UDT types map to Velox BIGINT/VARCHAR above. All supported.
    if (calciteType instanceof AbstractExprRelDataType<?>) {
      return true;
    }
    SqlTypeName typeName = calciteType.getSqlTypeName();
    switch (typeName) {
      case BOOLEAN:
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case REAL:
      case DOUBLE:
      case DECIMAL:
      case CHAR:
      case VARCHAR:
      case DATE:
      case TIME:
      case TIME_WITH_LOCAL_TIME_ZONE:
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      case NULL:
      case ANY:
        return true;
      case ARRAY:
      case MULTISET:
        return calciteType.getComponentType() != null
            && isSupported(calciteType.getComponentType());
      case MAP:
        return calciteType.getKeyType() != null
            && calciteType.getValueType() != null
            && isSupported(calciteType.getKeyType())
            && isSupported(calciteType.getValueType());
      case ROW:
        return calciteType.getFieldList().stream().allMatch(f -> isSupported(f.getType()));
      default:
        return false;
    }
  }

  public static RowType toVeloxRowType(RelDataType calciteType) {
    List<RelDataTypeField> fields = calciteType.getFieldList();
    List<String> names = new ArrayList<>(fields.size());
    List<Type> types = new ArrayList<>(fields.size());
    for (RelDataTypeField field : fields) {
      names.add(field.getName());
      types.add(toVeloxType(field.getType()));
    }
    return new RowType(names, types);
  }
}
