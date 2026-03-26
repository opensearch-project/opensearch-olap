/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.convert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlKind;
import org.boostscale.velox4j.aggregate.Aggregate;
import org.boostscale.velox4j.expression.CallTypedExpr;
import org.boostscale.velox4j.expression.FieldAccessTypedExpr;
import org.boostscale.velox4j.expression.TypedExpr;
import org.boostscale.velox4j.type.Type;

/** Converts Calcite AggregateCall to velox4j Aggregate. */
public class VeloxAggConverter {

  private static final Map<SqlKind, String> AGG_FUNCTION_MAP = new HashMap<>();

  static {
    AGG_FUNCTION_MAP.put(SqlKind.COUNT, "count");
    AGG_FUNCTION_MAP.put(SqlKind.SUM, "sum");
    AGG_FUNCTION_MAP.put(SqlKind.SUM0, "sum");
    AGG_FUNCTION_MAP.put(SqlKind.AVG, "avg");
    AGG_FUNCTION_MAP.put(SqlKind.MIN, "min");
    AGG_FUNCTION_MAP.put(SqlKind.MAX, "max");
  }

  private final RelDataType inputRowType;

  public VeloxAggConverter(RelDataType inputRowType) {
    this.inputRowType = inputRowType;
  }

  public Aggregate convert(AggregateCall aggCall) {
    String functionName = resolveAggFunction(aggCall);
    List<TypedExpr> args = convertArgs(aggCall);
    Type returnType = VeloxTypeConverter.toVeloxType(aggCall.getType());

    CallTypedExpr call = new CallTypedExpr(returnType, args, functionName);
    List<Type> rawInputTypes = resolveRawInputTypes(aggCall);

    return new Aggregate(
        call,
        rawInputTypes,
        null, // mask (no filtering)
        Collections.emptyList(), // sortingKeys
        Collections.emptyList(), // sortingOrders
        aggCall.isDistinct());
  }

  public String resolveAggOutputName(AggregateCall aggCall, int index) {
    if (aggCall.getName() != null) {
      return aggCall.getName();
    }
    return "agg_" + index;
  }

  private String resolveAggFunction(AggregateCall aggCall) {
    SqlKind kind = aggCall.getAggregation().getKind();
    String mapped = AGG_FUNCTION_MAP.get(kind);
    if (mapped != null) {
      return mapped;
    }
    return aggCall.getAggregation().getName().toLowerCase(java.util.Locale.ROOT);
  }

  private List<TypedExpr> convertArgs(AggregateCall aggCall) {
    List<Integer> argList = aggCall.getArgList();
    if (argList.isEmpty()) {
      // COUNT(*) has no args
      return Collections.emptyList();
    }

    List<TypedExpr> args = new ArrayList<>(argList.size());
    for (int fieldIndex : argList) {
      RelDataTypeField field = inputRowType.getFieldList().get(fieldIndex);
      Type veloxType = VeloxTypeConverter.toVeloxType(field.getType());
      args.add(FieldAccessTypedExpr.create(veloxType, field.getName()));
    }
    return args;
  }

  private List<Type> resolveRawInputTypes(AggregateCall aggCall) {
    List<Integer> argList = aggCall.getArgList();
    if (argList.isEmpty()) {
      return Collections.emptyList();
    }

    List<Type> types = new ArrayList<>(argList.size());
    for (int fieldIndex : argList) {
      RelDataTypeField field = inputRowType.getFieldList().get(fieldIndex);
      types.add(VeloxTypeConverter.toVeloxType(field.getType()));
    }
    return types;
  }
}
