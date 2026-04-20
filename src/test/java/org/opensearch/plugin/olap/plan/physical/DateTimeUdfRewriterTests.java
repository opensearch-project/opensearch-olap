/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical;

import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link DateTimeUdfRewriter}. Covers the three PPL datetime coercion UDFs ({@code
 * timestamp()}, {@code date()}, {@code time()}) across the rewrite shapes:
 *
 * <ul>
 *   <li>{@code UDF(<string literal>)} → Calcite literal of the target type
 *   <li>{@code UDF(<matching UDT or SQL-type ref>)} → the ref itself (identity)
 *   <li>{@code UDF(UDF(x))} → nested stripping
 * </ul>
 *
 * <p>Also verifies cross-type wrappers are NOT stripped (they are real casts, not identities) and
 * that unsupported shapes (e.g. {@code timestamp(CONCAT(...))}) fall back cleanly via {@link
 * DateTimeUdfRewriter#isRewritable}.
 */
public class DateTimeUdfRewriterTests extends OpenSearchTestCase {

  private final OpenSearchTypeFactory typeFactory = OpenSearchTypeFactory.TYPE_FACTORY;
  private final RexBuilder rexBuilder = new RexBuilder(typeFactory);

  // ========================================================================
  // isRewritable() — timestamp()
  // ========================================================================

  public void testIsRewritableAcceptsTimestampOfStringLiteral() {
    RexCall call = makeTimestampCall(rexBuilder.makeLiteral("2023-01-01 00:00:00"));
    assertTrue(DateTimeUdfRewriter.isRewritable(call));
  }

  public void testIsRewritableAcceptsTimestampOfIsoStringLiteral() {
    RexCall call = makeTimestampCall(rexBuilder.makeLiteral("2023-01-01T00:00:00Z"));
    assertTrue(DateTimeUdfRewriter.isRewritable(call));
  }

  public void testIsRewritableRejectsMalformedTimestampLiteral() {
    RexCall call = makeTimestampCall(rexBuilder.makeLiteral("not a timestamp"));
    assertFalse(DateTimeUdfRewriter.isRewritable(call));
  }

  public void testIsRewritableAcceptsTimestampOfExprTimestampRef() {
    RexInputRef ref = rexBuilder.makeInputRef(typeFactory.createUDT(ExprUDT.EXPR_TIMESTAMP), 0);
    RexCall call = makeTimestampCall(ref);
    assertTrue(DateTimeUdfRewriter.isRewritable(call));
  }

  public void testIsRewritableRejectsTimestampOfExprDateRef() {
    // PPL timestamp(DATE) casts to midnight — NOT an identity, so we don't strip it.
    RexInputRef ref = rexBuilder.makeInputRef(typeFactory.createUDT(ExprUDT.EXPR_DATE), 0);
    RexCall call = makeTimestampCall(ref);
    assertFalse(DateTimeUdfRewriter.isRewritable(call));
  }

  public void testIsRewritableRejectsTimestampOfExprTimeRef() {
    // PPL timestamp(TIME) casts to today-at-that-time — NOT an identity.
    RexInputRef ref = rexBuilder.makeInputRef(typeFactory.createUDT(ExprUDT.EXPR_TIME), 0);
    RexCall call = makeTimestampCall(ref);
    assertFalse(DateTimeUdfRewriter.isRewritable(call));
  }

  public void testIsRewritableAcceptsNestedTimestampOfStringLiteral() {
    // timestamp(timestamp('...')) — inner reduces to TIMESTAMP literal, outer identity strips.
    RexCall inner = makeTimestampCall(rexBuilder.makeLiteral("2023-01-01 00:00:00"));
    RexCall outer = makeTimestampCall(inner);
    assertTrue(DateTimeUdfRewriter.isRewritable(outer));
  }

  public void testIsRewritableRejectsTimestampOfUnsupportedCall() {
    // timestamp(CONCAT('2023', '-01-01')) — inner is VARCHAR but not a literal and not a nested
    // rewritable timestamp, so isRewritable should reject and let the query fall back.
    RexNode concat =
        rexBuilder.makeCall(
            SqlStdOperatorTable.CONCAT,
            rexBuilder.makeLiteral("2023"),
            rexBuilder.makeLiteral("-01-01"));
    RexCall call = makeTimestampCall(concat);
    assertFalse(DateTimeUdfRewriter.isRewritable(call));
  }

  public void testIsRewritableRejectsUnrelatedUdf() {
    // UPPER(...) isn't a datetime coercion UDF — must reject even though it takes a string.
    RexNode upper = rexBuilder.makeCall(SqlStdOperatorTable.UPPER, rexBuilder.makeLiteral("hi"));
    assertFalse(DateTimeUdfRewriter.isRewritable((RexCall) upper));
  }

  public void testIsRewritableRejectsDateOfUnsupportedNestedDateCall() {
    // date(date(concat(x,y))) — the inner date() call's return type is EXPR_DATE, which would
    // fool a naive type-match shortcut. But the inner call is itself not rewritable (concat
    // can't be folded), so the outer must also be rejected to force fallback cleanly.
    RexNode concat =
        rexBuilder.makeCall(
            SqlStdOperatorTable.CONCAT,
            rexBuilder.makeLiteral("2023"),
            rexBuilder.makeLiteral("-01-01"));
    RexCall innerDate = makeDateCall(concat);
    RexCall outerDate = makeDateCall(innerDate);
    assertFalse(DateTimeUdfRewriter.isRewritable(outerDate));
  }

  // ========================================================================
  // isRewritable() — date()
  // ========================================================================

  public void testIsRewritableAcceptsDateOfStringLiteral() {
    RexCall call = makeDateCall(rexBuilder.makeLiteral("2023-01-01"));
    assertTrue(DateTimeUdfRewriter.isRewritable(call));
  }

  public void testIsRewritableRejectsMalformedDateLiteral() {
    // Not an ISO date.
    RexCall call = makeDateCall(rexBuilder.makeLiteral("2023/01/01"));
    assertFalse(DateTimeUdfRewriter.isRewritable(call));
  }

  public void testIsRewritableAcceptsDateOfExprDateRef() {
    RexInputRef ref = rexBuilder.makeInputRef(typeFactory.createUDT(ExprUDT.EXPR_DATE), 0);
    RexCall call = makeDateCall(ref);
    assertTrue(DateTimeUdfRewriter.isRewritable(call));
  }

  public void testIsRewritableRejectsDateOfExprTimestampRef() {
    // PPL date(TIMESTAMP) truncates to the date part — a real cast, not an identity.
    RexInputRef ref = rexBuilder.makeInputRef(typeFactory.createUDT(ExprUDT.EXPR_TIMESTAMP), 0);
    RexCall call = makeDateCall(ref);
    assertFalse(DateTimeUdfRewriter.isRewritable(call));
  }

  // ========================================================================
  // isRewritable() — time()
  // ========================================================================

  public void testIsRewritableAcceptsTimeOfStringLiteral() {
    RexCall call = makeTimeCall(rexBuilder.makeLiteral("23:59:59"));
    assertTrue(DateTimeUdfRewriter.isRewritable(call));
  }

  public void testIsRewritableAcceptsTimeOfStringLiteralWithMillis() {
    RexCall call = makeTimeCall(rexBuilder.makeLiteral("23:59:59.123"));
    assertTrue(DateTimeUdfRewriter.isRewritable(call));
  }

  public void testIsRewritableRejectsMalformedTimeLiteral() {
    RexCall call = makeTimeCall(rexBuilder.makeLiteral("25:00:00"));
    assertFalse(DateTimeUdfRewriter.isRewritable(call));
  }

  public void testIsRewritableAcceptsTimeOfExprTimeRef() {
    RexInputRef ref = rexBuilder.makeInputRef(typeFactory.createUDT(ExprUDT.EXPR_TIME), 0);
    RexCall call = makeTimeCall(ref);
    assertTrue(DateTimeUdfRewriter.isRewritable(call));
  }

  public void testIsRewritableRejectsTimeOfExprTimestampRef() {
    // PPL time(TIMESTAMP) extracts time-of-day — a real cast, not an identity.
    RexInputRef ref = rexBuilder.makeInputRef(typeFactory.createUDT(ExprUDT.EXPR_TIMESTAMP), 0);
    RexCall call = makeTimeCall(ref);
    assertFalse(DateTimeUdfRewriter.isRewritable(call));
  }

  // ========================================================================
  // rewrite() end-to-end — uses Project (not Filter) because RelBuilder's Filter aggressively
  // simplifies conditions like IS_NOT_NULL(<non-null literal>) → TRUE and elides the filter.
  // ========================================================================

  public void testRewriteStripsTimestampOfStringLiteral() {
    RelBuilder b = CalciteTestHelper.createRelBuilder();
    RexCall call = makeTimestampCall(rexBuilder.makeLiteral("2023-01-01 00:00:00"));
    RelNode plan = b.scan("employees").project(call).build();

    RelNode rewritten = DateTimeUdfRewriter.rewrite(plan);

    RexNode projected = projectExpr(rewritten, 0);
    assertFalse(
        "timestamp() wrapper should have been stripped",
        projected instanceof RexCall && isCoercionCall((RexCall) projected));
    assertTrue("expected TIMESTAMP literal after rewrite", projected instanceof RexLiteral);
    assertEquals(SqlTypeName.TIMESTAMP, projected.getType().getSqlTypeName());
  }

  public void testRewriteStripsNestedTimestampOfStringLiteral() {
    RelBuilder b = CalciteTestHelper.createRelBuilder();
    RexCall inner = makeTimestampCall(rexBuilder.makeLiteral("2023-01-01 00:00:00"));
    RexCall outer = makeTimestampCall(inner);
    RelNode plan = b.scan("employees").project(outer).build();

    RelNode rewritten = DateTimeUdfRewriter.rewrite(plan);

    RexNode projected = projectExpr(rewritten, 0);
    assertFalse(
        "both timestamp() wrappers should have been stripped",
        projected instanceof RexCall && isCoercionCall((RexCall) projected));
    assertTrue("expected TIMESTAMP literal after rewrite", projected instanceof RexLiteral);
  }

  public void testRewriteLeavesUnsupportedTimestampCallIntact() {
    RelBuilder b = CalciteTestHelper.createRelBuilder();
    RexNode concat =
        rexBuilder.makeCall(
            SqlStdOperatorTable.CONCAT,
            rexBuilder.makeLiteral("2023"),
            rexBuilder.makeLiteral("-01-01"));
    RexCall call = makeTimestampCall(concat);
    RelNode plan = b.scan("employees").project(call).build();

    RelNode rewritten = DateTimeUdfRewriter.rewrite(plan);

    // Can't be simplified — preserve so canVectorize rejects and we fall back.
    RexNode projected = projectExpr(rewritten, 0);
    assertTrue("unsupported timestamp() call should be preserved", projected instanceof RexCall);
    assertTrue(
        "expected the outer timestamp() wrapper still present",
        isCoercionCall((RexCall) projected));
  }

  public void testRewriteStripsTimestampOfTimestampRef() {
    RelBuilder b = CalciteTestHelper.createRelBuilder();
    b.scan("datetime_data");
    RexInputRef ref = (RexInputRef) b.field("ts_col");
    RexCall call = makeTimestampCall(ref);
    RelNode plan = b.project(call).build();

    RelNode rewritten = DateTimeUdfRewriter.rewrite(plan);

    RexNode projected = projectExpr(rewritten, 0);
    assertTrue("expected the inner ref, not the wrapper", projected instanceof RexInputRef);
  }

  public void testRewriteStripsDateOfStringLiteral() {
    RelBuilder b = CalciteTestHelper.createRelBuilder();
    RexCall call = makeDateCall(rexBuilder.makeLiteral("2023-01-01"));
    RelNode plan = b.scan("employees").project(call).build();

    RelNode rewritten = DateTimeUdfRewriter.rewrite(plan);

    RexNode projected = projectExpr(rewritten, 0);
    assertFalse(
        "date() wrapper should have been stripped",
        projected instanceof RexCall && isCoercionCall((RexCall) projected));
    assertTrue("expected DATE literal after rewrite", projected instanceof RexLiteral);
    assertEquals(SqlTypeName.DATE, projected.getType().getSqlTypeName());
  }

  public void testRewriteStripsDateOfDateRef() {
    RelBuilder b = CalciteTestHelper.createRelBuilder();
    b.scan("datetime_data");
    RexInputRef ref = (RexInputRef) b.field("date_col");
    RexCall call = makeDateCall(ref);
    RelNode plan = b.project(call).build();

    RelNode rewritten = DateTimeUdfRewriter.rewrite(plan);

    RexNode projected = projectExpr(rewritten, 0);
    assertTrue("expected the inner ref, not the wrapper", projected instanceof RexInputRef);
    assertEquals(SqlTypeName.DATE, projected.getType().getSqlTypeName());
  }

  public void testRewriteStripsTimeOfStringLiteral() {
    RelBuilder b = CalciteTestHelper.createRelBuilder();
    RexCall call = makeTimeCall(rexBuilder.makeLiteral("23:59:59"));
    RelNode plan = b.scan("employees").project(call).build();

    RelNode rewritten = DateTimeUdfRewriter.rewrite(plan);

    RexNode projected = projectExpr(rewritten, 0);
    assertFalse(
        "time() wrapper should have been stripped",
        projected instanceof RexCall && isCoercionCall((RexCall) projected));
    assertTrue("expected TIME literal after rewrite", projected instanceof RexLiteral);
    assertEquals(SqlTypeName.TIME, projected.getType().getSqlTypeName());
  }

  public void testRewriteStripsTimeOfTimeRef() {
    RelBuilder b = CalciteTestHelper.createRelBuilder();
    b.scan("datetime_data");
    RexInputRef ref = (RexInputRef) b.field("time_col");
    RexCall call = makeTimeCall(ref);
    RelNode plan = b.project(call).build();

    RelNode rewritten = DateTimeUdfRewriter.rewrite(plan);

    RexNode projected = projectExpr(rewritten, 0);
    assertTrue("expected the inner ref, not the wrapper", projected instanceof RexInputRef);
    assertEquals(SqlTypeName.TIME, projected.getType().getSqlTypeName());
  }

  // ========================================================================
  // helpers
  // ========================================================================

  private static final SqlOperator TIMESTAMP_UDF_STUB =
      new SqlFunction(
          "TIMESTAMP",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(
              OpenSearchTypeFactory.TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIMESTAMP)),
          null,
          null,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);

  private static final SqlOperator DATE_UDF_STUB =
      new SqlFunction(
          "DATE",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(OpenSearchTypeFactory.TYPE_FACTORY.createUDT(ExprUDT.EXPR_DATE)),
          null,
          null,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);

  private static final SqlOperator TIME_UDF_STUB =
      new SqlFunction(
          "TIME",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(OpenSearchTypeFactory.TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIME)),
          null,
          null,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);

  private RexCall makeTimestampCall(RexNode operand) {
    RelDataType returnType = typeFactory.createUDT(ExprUDT.EXPR_TIMESTAMP);
    return (RexCall) rexBuilder.makeCall(returnType, TIMESTAMP_UDF_STUB, List.of(operand));
  }

  private RexCall makeDateCall(RexNode operand) {
    RelDataType returnType = typeFactory.createUDT(ExprUDT.EXPR_DATE);
    return (RexCall) rexBuilder.makeCall(returnType, DATE_UDF_STUB, List.of(operand));
  }

  private RexCall makeTimeCall(RexNode operand) {
    RelDataType returnType = typeFactory.createUDT(ExprUDT.EXPR_TIME);
    return (RexCall) rexBuilder.makeCall(returnType, TIME_UDF_STUB, List.of(operand));
  }

  private static RexNode projectExpr(RelNode project, int index) {
    return ((Project) project).getProjects().get(index);
  }

  private static boolean isCoercionCall(RexCall call) {
    String name = call.getOperator().getName();
    return "TIMESTAMP".equalsIgnoreCase(name)
        || "DATE".equalsIgnoreCase(name)
        || "TIME".equalsIgnoreCase(name);
  }
}
