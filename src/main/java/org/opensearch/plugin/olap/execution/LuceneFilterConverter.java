/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.execution;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.boostscale.velox4j.expression.CallTypedExpr;
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
 * Converts velox4j filter expressions ({@link TypedExpr}) to Lucene queries for predicate pushdown.
 *
 * <p>This converter operates on the data node side, working with the deserialized velox4j plan. It
 * extracts the {@link org.boostscale.velox4j.plan.FilterNode}'s filter expression and converts
 * pushable predicates to Lucene point range queries or term queries. This uses the inverted index /
 * BKD tree to skip non-matching documents before reading doc values.
 *
 * <p>Supported conversions:
 *
 * <ul>
 *   <li>Comparisons: equalto, notequalto, greaterthan, greaterthanorequal, lessthan,
 *       lessthanorequal
 *   <li>Logical: and, or, not
 *   <li>Null checks: is_null (wrapped in not for IS NOT NULL)
 * </ul>
 *
 * <p>Returns {@code null} for expressions that cannot be pushed down. The Velox FilterNode is kept
 * as a safety net.
 */
public class LuceneFilterConverter {

  private static final Logger logger = LogManager.getLogger(LuceneFilterConverter.class);

  /**
   * Convert a velox4j TypedExpr filter to a Lucene Query.
   *
   * @return a Lucene Query, or null if the expression cannot be pushed down
   */
  public Query convert(TypedExpr expr) {
    if (expr instanceof CallTypedExpr) {
      return convertCall((CallTypedExpr) expr);
    }
    return null;
  }

  private Query convertCall(CallTypedExpr call) {
    String funcName = call.getFunctionName();

    switch (funcName) {
      case "and":
        return convertAnd(call);
      case "or":
        return convertOr(call);
      case "not":
        return convertNot(call);
      case "equalto":
        return convertComparison(call, CompOp.EQ);
      case "notequalto":
        return convertComparison(call, CompOp.NEQ);
      case "greaterthan":
        return convertComparison(call, CompOp.GT);
      case "greaterthanorequal":
        return convertComparison(call, CompOp.GTE);
      case "lessthan":
        return convertComparison(call, CompOp.LT);
      case "lessthanorequal":
        return convertComparison(call, CompOp.LTE);
      case "is_null":
        return convertIsNull(call);
      default:
        logger.debug("Unsupported function for pushdown: {}", funcName);
        return null;
    }
  }

  private Query convertAnd(CallTypedExpr call) {
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    for (TypedExpr input : call.getInputs()) {
      Query sub = convert(input);
      if (sub == null) {
        // Skip non-pushable conjuncts — Velox FilterNode handles them
        continue;
      }
      builder.add(sub, BooleanClause.Occur.MUST);
    }
    BooleanQuery bq = builder.build();
    return bq.clauses().isEmpty() ? null : bq;
  }

  private Query convertOr(CallTypedExpr call) {
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    for (TypedExpr input : call.getInputs()) {
      Query sub = convert(input);
      if (sub == null) {
        // If any disjunct can't be pushed, the entire OR can't be pushed
        return null;
      }
      builder.add(sub, BooleanClause.Occur.SHOULD);
    }
    builder.setMinimumNumberShouldMatch(1);
    return builder.build();
  }

  private Query convertNot(CallTypedExpr call) {
    TypedExpr inner = call.getInputs().get(0);

    // Handle not(is_null(x)) → IS NOT NULL → FieldExistsQuery
    if (inner instanceof CallTypedExpr
        && "is_null".equals(((CallTypedExpr) inner).getFunctionName())) {
      return convertIsNotNull((CallTypedExpr) inner);
    }

    // Use strict conversion: the inner expression must be fully pushable.
    // Partial pushdown under NOT changes semantics — e.g. NOT(A AND B) where only A is
    // pushable would become NOT(A), incorrectly filtering out rows that match A but not B.
    Query innerQuery = convertStrict(inner);
    if (innerQuery == null) {
      return null;
    }
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
    builder.add(innerQuery, BooleanClause.Occur.MUST_NOT);
    return builder.build();
  }

  /**
   * Strict conversion: returns a Lucene Query only if the entire expression is fully pushable. Used
   * inside NOT where partial pushdown would change semantics.
   */
  private Query convertStrict(TypedExpr expr) {
    if (!(expr instanceof CallTypedExpr)) {
      return null;
    }
    CallTypedExpr call = (CallTypedExpr) expr;
    String funcName = call.getFunctionName();

    if ("and".equals(funcName)) {
      // All conjuncts must be fully pushable
      BooleanQuery.Builder builder = new BooleanQuery.Builder();
      for (TypedExpr input : call.getInputs()) {
        Query sub = convertStrict(input);
        if (sub == null) {
          return null;
        }
        builder.add(sub, BooleanClause.Occur.MUST);
      }
      return builder.build();
    }
    if ("or".equals(funcName)) {
      return convertOr(call); // OR already requires all disjuncts to be pushable
    }

    // For leaf predicates (comparisons, is_null, etc.), convert() is strict by nature
    return convertCall(call);
  }

  private Query convertIsNull(CallTypedExpr call) {
    TypedExpr operand = call.getInputs().get(0);
    if (!(operand instanceof FieldAccessTypedExpr)) {
      return null;
    }
    String fieldName = ((FieldAccessTypedExpr) operand).getFieldName();
    // IS NULL = NOT EXISTS
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
    builder.add(new FieldExistsQuery(fieldName), BooleanClause.Occur.MUST_NOT);
    return builder.build();
  }

  private Query convertIsNotNull(CallTypedExpr isNullCall) {
    TypedExpr operand = isNullCall.getInputs().get(0);
    if (!(operand instanceof FieldAccessTypedExpr)) {
      return null;
    }
    String fieldName = ((FieldAccessTypedExpr) operand).getFieldName();
    return new FieldExistsQuery(fieldName);
  }

  private Query convertComparison(CallTypedExpr call, CompOp op) {
    if (call.getInputs().size() != 2) {
      return null;
    }

    TypedExpr left = call.getInputs().get(0);
    TypedExpr right = call.getInputs().get(1);

    // Handle field op literal or literal op field
    FieldAccessTypedExpr fieldExpr;
    ConstantTypedExpr constExpr;
    boolean flipped = false;

    if (left instanceof FieldAccessTypedExpr && right instanceof ConstantTypedExpr) {
      fieldExpr = (FieldAccessTypedExpr) left;
      constExpr = (ConstantTypedExpr) right;
    } else if (right instanceof FieldAccessTypedExpr && left instanceof ConstantTypedExpr) {
      fieldExpr = (FieldAccessTypedExpr) right;
      constExpr = (ConstantTypedExpr) left;
      flipped = true;
    } else {
      return null;
    }

    if (flipped) {
      op = op.flip();
    }

    String fieldName = fieldExpr.getFieldName();
    Variant value = constExpr.getValue();
    if (value == null) {
      return null;
    }

    Type fieldType = fieldExpr.getReturnType();

    if (op == CompOp.EQ) {
      return createEqualityQuery(fieldName, fieldType, value);
    }
    if (op == CompOp.NEQ) {
      Query eq = createEqualityQuery(fieldName, fieldType, value);
      if (eq == null) return null;
      BooleanQuery.Builder builder = new BooleanQuery.Builder();
      builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
      builder.add(eq, BooleanClause.Occur.MUST_NOT);
      return builder.build();
    }

    return createRangeQuery(fieldName, fieldType, value, op);
  }

  private Query createEqualityQuery(String fieldName, Type fieldType, Variant value) {
    if (fieldType instanceof IntegerType
        || fieldType instanceof SmallIntType
        || fieldType instanceof TinyIntType) {
      return IntPoint.newExactQuery(fieldName, variantToInt(value));
    }
    if (fieldType instanceof BigIntType) {
      return LongPoint.newExactQuery(fieldName, variantToLong(value));
    }
    if (fieldType instanceof RealType) {
      return FloatPoint.newExactQuery(fieldName, variantToFloat(value));
    }
    if (fieldType instanceof DoubleType) {
      return DoublePoint.newExactQuery(fieldName, variantToDouble(value));
    }
    if (fieldType instanceof VarCharType) {
      String strVal = variantToString(value);
      if (strVal == null) return null;
      return new TermQuery(new Term(fieldName, strVal));
    }
    if (fieldType instanceof BooleanType) {
      long boolVal = variantToBoolean(value) ? 1L : 0L;
      return LongPoint.newExactQuery(fieldName, boolVal);
    }
    return null;
  }

  private Query createRangeQuery(String fieldName, Type fieldType, Variant value, CompOp op) {
    if (fieldType instanceof IntegerType
        || fieldType instanceof SmallIntType
        || fieldType instanceof TinyIntType) {
      int v = variantToInt(value);
      int lo, hi;
      switch (op) {
        case GT:
          lo = Math.addExact(v, 1);
          hi = Integer.MAX_VALUE;
          break;
        case GTE:
          lo = v;
          hi = Integer.MAX_VALUE;
          break;
        case LT:
          lo = Integer.MIN_VALUE;
          hi = Math.addExact(v, -1);
          break;
        case LTE:
          lo = Integer.MIN_VALUE;
          hi = v;
          break;
        default:
          return null;
      }
      return IntPoint.newRangeQuery(fieldName, lo, hi);
    }

    if (fieldType instanceof BigIntType) {
      long v = variantToLong(value);
      long lo, hi;
      switch (op) {
        case GT:
          lo = Math.addExact(v, 1);
          hi = Long.MAX_VALUE;
          break;
        case GTE:
          lo = v;
          hi = Long.MAX_VALUE;
          break;
        case LT:
          lo = Long.MIN_VALUE;
          hi = Math.addExact(v, -1);
          break;
        case LTE:
          lo = Long.MIN_VALUE;
          hi = v;
          break;
        default:
          return null;
      }
      return LongPoint.newRangeQuery(fieldName, lo, hi);
    }

    if (fieldType instanceof RealType) {
      float v = variantToFloat(value);
      float lo, hi;
      switch (op) {
        case GT:
          lo = Math.nextUp(v);
          hi = Float.POSITIVE_INFINITY;
          break;
        case GTE:
          lo = v;
          hi = Float.POSITIVE_INFINITY;
          break;
        case LT:
          lo = Float.NEGATIVE_INFINITY;
          hi = Math.nextDown(v);
          break;
        case LTE:
          lo = Float.NEGATIVE_INFINITY;
          hi = v;
          break;
        default:
          return null;
      }
      return FloatPoint.newRangeQuery(fieldName, lo, hi);
    }

    if (fieldType instanceof DoubleType) {
      double v = variantToDouble(value);
      double lo, hi;
      switch (op) {
        case GT:
          lo = Math.nextUp(v);
          hi = Double.POSITIVE_INFINITY;
          break;
        case GTE:
          lo = v;
          hi = Double.POSITIVE_INFINITY;
          break;
        case LT:
          lo = Double.NEGATIVE_INFINITY;
          hi = Math.nextDown(v);
          break;
        case LTE:
          lo = Double.NEGATIVE_INFINITY;
          hi = v;
          break;
        default:
          return null;
      }
      return DoublePoint.newRangeQuery(fieldName, lo, hi);
    }

    return null;
  }

  // --- Variant value extraction ---

  private int variantToInt(Variant v) {
    if (v instanceof IntegerValue) return ((IntegerValue) v).getValue();
    if (v instanceof BigIntValue) return ((BigIntValue) v).getValue().intValue();
    if (v instanceof DoubleValue) return ((DoubleValue) v).getValue().intValue();
    if (v instanceof RealValue) return ((RealValue) v).getValue().intValue();
    throw new UnsupportedOperationException("Cannot convert " + v.getClass() + " to int");
  }

  private long variantToLong(Variant v) {
    if (v instanceof BigIntValue) return ((BigIntValue) v).getValue();
    if (v instanceof IntegerValue) return ((IntegerValue) v).getValue().longValue();
    if (v instanceof DoubleValue) return ((DoubleValue) v).getValue().longValue();
    if (v instanceof RealValue) return ((RealValue) v).getValue().longValue();
    throw new UnsupportedOperationException("Cannot convert " + v.getClass() + " to long");
  }

  private float variantToFloat(Variant v) {
    if (v instanceof RealValue) return ((RealValue) v).getValue();
    if (v instanceof DoubleValue) return ((DoubleValue) v).getValue().floatValue();
    if (v instanceof IntegerValue) return ((IntegerValue) v).getValue().floatValue();
    if (v instanceof BigIntValue) return ((BigIntValue) v).getValue().floatValue();
    throw new UnsupportedOperationException("Cannot convert " + v.getClass() + " to float");
  }

  private double variantToDouble(Variant v) {
    if (v instanceof DoubleValue) return ((DoubleValue) v).getValue();
    if (v instanceof RealValue) return ((RealValue) v).getValue().doubleValue();
    if (v instanceof IntegerValue) return ((IntegerValue) v).getValue().doubleValue();
    if (v instanceof BigIntValue) return ((BigIntValue) v).getValue().doubleValue();
    throw new UnsupportedOperationException("Cannot convert " + v.getClass() + " to double");
  }

  private String variantToString(Variant v) {
    if (v instanceof VarCharValue) return ((VarCharValue) v).getValue();
    return null;
  }

  private boolean variantToBoolean(Variant v) {
    if (v instanceof BooleanValue) return ((BooleanValue) v).getValue();
    throw new UnsupportedOperationException("Cannot convert " + v.getClass() + " to boolean");
  }

  /** Comparison operator enum with flip support. */
  enum CompOp {
    EQ,
    NEQ,
    GT,
    GTE,
    LT,
    LTE;

    CompOp flip() {
      switch (this) {
        case GT:
          return LT;
        case GTE:
          return LTE;
        case LT:
          return GT;
        case LTE:
          return GTE;
        default:
          return this;
      }
    }
  }
}
