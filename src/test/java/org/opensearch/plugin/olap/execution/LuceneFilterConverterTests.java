/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.execution;

import java.util.List;
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
import org.boostscale.velox4j.type.IntegerType;
import org.boostscale.velox4j.type.VarCharType;
import org.boostscale.velox4j.variant.BigIntValue;
import org.boostscale.velox4j.variant.IntegerValue;
import org.boostscale.velox4j.variant.VarCharValue;
import org.opensearch.test.OpenSearchTestCase;

public class LuceneFilterConverterTests extends OpenSearchTestCase {

  private final LuceneFilterConverter converter = new LuceneFilterConverter();

  // --- Helper methods to build velox4j expressions ---

  private FieldAccessTypedExpr intField(String name) {
    return FieldAccessTypedExpr.create(new IntegerType(), name);
  }

  private FieldAccessTypedExpr longField(String name) {
    return FieldAccessTypedExpr.create(new BigIntType(), name);
  }

  private FieldAccessTypedExpr varcharField(String name) {
    return FieldAccessTypedExpr.create(new VarCharType(), name);
  }

  private ConstantTypedExpr intConst(int value) {
    return ConstantTypedExpr.create(new IntegerType(), new IntegerValue(value));
  }

  private ConstantTypedExpr longConst(long value) {
    return ConstantTypedExpr.create(new BigIntType(), new BigIntValue(value));
  }

  private ConstantTypedExpr varcharConst(String value) {
    return ConstantTypedExpr.create(new VarCharType(), new VarCharValue(value));
  }

  private CallTypedExpr call(String funcName, TypedExpr... inputs) {
    return new CallTypedExpr(new BooleanType(), List.of(inputs), funcName);
  }

  // --- Tests ---

  public void testEqualToInt() {
    // age == 30
    TypedExpr expr = call("equalto", intField("age"), intConst(30));
    Query result = converter.convert(expr);
    assertNotNull(result);
    assertEquals(IntPoint.newExactQuery("age", 30), result);
  }

  public void testEqualToLong() {
    // timestamp == 1000L
    TypedExpr expr = call("equalto", longField("timestamp"), longConst(1000L));
    Query result = converter.convert(expr);
    assertNotNull(result);
    assertEquals(LongPoint.newExactQuery("timestamp", 1000L), result);
  }

  public void testEqualToVarchar() {
    // name == 'foo'
    TypedExpr expr = call("equalto", varcharField("name"), varcharConst("foo"));
    Query result = converter.convert(expr);
    assertNotNull(result);
    assertEquals(new TermQuery(new Term("name", "foo")), result);
  }

  public void testGreaterThan() {
    // age > 30
    TypedExpr expr = call("greaterthan", intField("age"), intConst(30));
    Query result = converter.convert(expr);
    assertNotNull(result);
    assertEquals(IntPoint.newRangeQuery("age", 31, Integer.MAX_VALUE), result);
  }

  public void testGreaterThanOrEqual() {
    // age >= 30
    TypedExpr expr = call("greaterthanorequal", intField("age"), intConst(30));
    Query result = converter.convert(expr);
    assertNotNull(result);
    assertEquals(IntPoint.newRangeQuery("age", 30, Integer.MAX_VALUE), result);
  }

  public void testLessThan() {
    // age < 30
    TypedExpr expr = call("lessthan", intField("age"), intConst(30));
    Query result = converter.convert(expr);
    assertNotNull(result);
    assertEquals(IntPoint.newRangeQuery("age", Integer.MIN_VALUE, 29), result);
  }

  public void testLessThanOrEqual() {
    // age <= 30
    TypedExpr expr = call("lessthanorequal", intField("age"), intConst(30));
    Query result = converter.convert(expr);
    assertNotNull(result);
    assertEquals(IntPoint.newRangeQuery("age", Integer.MIN_VALUE, 30), result);
  }

  public void testNotEqualTo() {
    // age != 30
    TypedExpr expr = call("notequalto", intField("age"), intConst(30));
    Query result = converter.convert(expr);
    assertNotNull(result);
    assertTrue(result instanceof BooleanQuery);
    BooleanQuery bq = (BooleanQuery) result;
    assertEquals(2, bq.clauses().size());
    assertEquals(BooleanClause.Occur.MUST, bq.clauses().get(0).occur());
    assertTrue(bq.clauses().get(0).query() instanceof MatchAllDocsQuery);
    assertEquals(BooleanClause.Occur.MUST_NOT, bq.clauses().get(1).occur());
    assertEquals(IntPoint.newExactQuery("age", 30), bq.clauses().get(1).query());
  }

  public void testAndCondition() {
    // age > 20 AND age < 50
    TypedExpr left = call("greaterthan", intField("age"), intConst(20));
    TypedExpr right = call("lessthan", intField("age"), intConst(50));
    TypedExpr expr = call("and", left, right);
    Query result = converter.convert(expr);
    assertNotNull(result);
    assertTrue(result instanceof BooleanQuery);
    BooleanQuery bq = (BooleanQuery) result;
    assertEquals(2, bq.clauses().size());
    for (BooleanClause clause : bq.clauses()) {
      assertEquals(BooleanClause.Occur.MUST, clause.occur());
    }
  }

  public void testOrCondition() {
    // age == 20 OR age == 30
    TypedExpr left = call("equalto", intField("age"), intConst(20));
    TypedExpr right = call("equalto", intField("age"), intConst(30));
    TypedExpr expr = call("or", left, right);
    Query result = converter.convert(expr);
    assertNotNull(result);
    assertTrue(result instanceof BooleanQuery);
    BooleanQuery bq = (BooleanQuery) result;
    assertEquals(2, bq.clauses().size());
    for (BooleanClause clause : bq.clauses()) {
      assertEquals(BooleanClause.Occur.SHOULD, clause.occur());
    }
    assertEquals(1, bq.getMinimumNumberShouldMatch());
  }

  public void testNotCondition() {
    // NOT(age == 30)
    TypedExpr inner = call("equalto", intField("age"), intConst(30));
    TypedExpr expr = call("not", inner);
    Query result = converter.convert(expr);
    assertNotNull(result);
    assertTrue(result instanceof BooleanQuery);
    BooleanQuery bq = (BooleanQuery) result;
    assertEquals(2, bq.clauses().size());
    assertEquals(BooleanClause.Occur.MUST, bq.clauses().get(0).occur());
    assertEquals(BooleanClause.Occur.MUST_NOT, bq.clauses().get(1).occur());
  }

  public void testIsNull() {
    // is_null(age)
    TypedExpr expr = call("is_null", intField("age"));
    Query result = converter.convert(expr);
    assertNotNull(result);
    assertTrue(result instanceof BooleanQuery);
    BooleanQuery bq = (BooleanQuery) result;
    assertEquals(2, bq.clauses().size());
    assertEquals(BooleanClause.Occur.MUST, bq.clauses().get(0).occur());
    assertEquals(BooleanClause.Occur.MUST_NOT, bq.clauses().get(1).occur());
    assertTrue(bq.clauses().get(1).query() instanceof FieldExistsQuery);
  }

  public void testIsNotNull() {
    // not(is_null(age)) — how VeloxExprConverter encodes IS NOT NULL
    TypedExpr isNull = call("is_null", intField("age"));
    TypedExpr expr = call("not", isNull);
    Query result = converter.convert(expr);
    assertNotNull(result);
    assertTrue(result instanceof FieldExistsQuery);
  }

  public void testFlippedOperands() {
    // 30 < age → should produce age > 30
    TypedExpr expr = call("lessthan", intConst(30), intField("age"));
    Query result = converter.convert(expr);
    assertNotNull(result);
    assertEquals(IntPoint.newRangeQuery("age", 31, Integer.MAX_VALUE), result);
  }

  public void testFieldToFieldComparisonReturnsNull() {
    // age == salary (field-to-field, not pushable)
    TypedExpr expr = call("equalto", intField("age"), intField("salary"));
    Query result = converter.convert(expr);
    assertNull(result);
  }

  public void testUnsupportedFunctionReturnsNull() {
    // like(name, '%foo%') — not supported for pushdown
    TypedExpr expr = call("like", varcharField("name"), varcharConst("%foo%"));
    Query result = converter.convert(expr);
    assertNull(result);
  }

  public void testAndWithNonPushableConjunct() {
    // age > 30 AND like(name, '%foo%')
    // The pushable part (age > 30) should still be pushed
    TypedExpr pushable = call("greaterthan", intField("age"), intConst(30));
    TypedExpr nonPushable = call("like", varcharField("name"), varcharConst("%foo%"));
    TypedExpr expr = call("and", pushable, nonPushable);
    Query result = converter.convert(expr);
    assertNotNull(result);
    // Should only contain the pushable clause
    assertTrue(result instanceof BooleanQuery);
    BooleanQuery bq = (BooleanQuery) result;
    assertEquals(1, bq.clauses().size());
  }

  public void testOrWithNonPushableDisjunctReturnsNull() {
    // age > 30 OR like(name, '%foo%')
    // Entire OR must be null because one disjunct isn't pushable
    TypedExpr pushable = call("greaterthan", intField("age"), intConst(30));
    TypedExpr nonPushable = call("like", varcharField("name"), varcharConst("%foo%"));
    TypedExpr expr = call("or", pushable, nonPushable);
    Query result = converter.convert(expr);
    assertNull(result);
  }

  public void testLongRangeQuery() {
    // timestamp > 1000L
    TypedExpr expr = call("greaterthan", longField("timestamp"), longConst(1000L));
    Query result = converter.convert(expr);
    assertNotNull(result);
    assertEquals(LongPoint.newRangeQuery("timestamp", 1001L, Long.MAX_VALUE), result);
  }

  public void testNotWithPartiallyPushableAndReturnsNull() {
    // NOT(age > 30 AND like(name, '%foo%'))
    // The AND has a non-pushable conjunct. If NOT partially pushed NOT(age > 30),
    // it would incorrectly filter out rows matching age > 30 but not the LIKE.
    // So NOT must return null here.
    TypedExpr pushable = call("greaterthan", intField("age"), intConst(30));
    TypedExpr nonPushable = call("like", varcharField("name"), varcharConst("%foo%"));
    TypedExpr andExpr = call("and", pushable, nonPushable);
    TypedExpr notExpr = call("not", andExpr);
    Query result = converter.convert(notExpr);
    assertNull(result);
  }

  public void testNotWithFullyPushableChildSucceeds() {
    // NOT(age > 30) — fully pushable, should work
    TypedExpr inner = call("greaterthan", intField("age"), intConst(30));
    TypedExpr notExpr = call("not", inner);
    Query result = converter.convert(notExpr);
    assertNotNull(result);
    assertTrue(result instanceof BooleanQuery);
    BooleanQuery bq = (BooleanQuery) result;
    assertEquals(2, bq.clauses().size());
    assertEquals(BooleanClause.Occur.MUST_NOT, bq.clauses().get(1).occur());
  }

  public void testNotWithFullyPushableAndSucceeds() {
    // NOT(age > 20 AND age < 50) — both conjuncts pushable, should work
    TypedExpr left = call("greaterthan", intField("age"), intConst(20));
    TypedExpr right = call("lessthan", intField("age"), intConst(50));
    TypedExpr andExpr = call("and", left, right);
    TypedExpr notExpr = call("not", andExpr);
    Query result = converter.convert(notExpr);
    assertNotNull(result);
  }
}
