/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.convert;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link RelevanceSplitter} covering the split contract under each shape: AND with
 * relevance, OR with translatable leaves, OR ruled out, NOT, options forwarding.
 */
public class RelevanceSplitterTests extends OpenSearchTestCase {

  private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
  private final RexBuilder rexBuilder = new RexBuilder(typeFactory);

  /** Row type with a VARCHAR body/tags and a numeric price/age columns. */
  private RelDataType buildRow() {
    RelDataTypeFactory.Builder b = typeFactory.builder();
    b.add("body", typeFactory.createSqlType(SqlTypeName.VARCHAR, 1024));
    b.add("title", typeFactory.createSqlType(SqlTypeName.VARCHAR, 1024));
    b.add("tags", typeFactory.createSqlType(SqlTypeName.VARCHAR, 255));
    b.add("price", typeFactory.createSqlType(SqlTypeName.INTEGER));
    return b.build();
  }

  /** Mimics PPL's registered SqlUserDefinedFunction for each relevance name. */
  private SqlFunction makeRelevanceOp(String name) {
    return new SqlFunction(
        name,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
  }

  /**
   * Build a relevance RexCall in the shape the SQL plugin emits: each operand is a
   * MAP_VALUE_CONSTRUCTOR RexCall wrapping (aliasLiteral, valueRex).
   */
  private RexNode makeRelevanceCall(String funcName, Object... alternatingKV) {
    List<RexNode> operands = new ArrayList<>();
    for (int i = 0; i < alternatingKV.length; i += 2) {
      String name = (String) alternatingKV[i];
      Object value = alternatingKV[i + 1];
      RexNode alias = rexBuilder.makeLiteral(name);
      RexNode valueRex;
      if (value instanceof RexNode) {
        valueRex = (RexNode) value;
      } else if (value instanceof String) {
        valueRex = rexBuilder.makeLiteral((String) value);
      } else if (value instanceof Number) {
        Number n = (Number) value;
        valueRex =
            rexBuilder.makeLiteral(
                new BigDecimal(n.toString()),
                typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 0),
                false);
      } else {
        throw new IllegalArgumentException("unsupported test value: " + value);
      }
      operands.add(rexBuilder.makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, alias, valueRex));
    }
    return rexBuilder.makeCall(makeRelevanceOp(funcName), operands);
  }

  /**
   * Build a fields-map operand for multi-field functions: MAP_VALUE_CONSTRUCTOR with interleaved
   * (fieldName, boost).
   */
  private RexNode makeFieldsMap(String... fieldNamesAndBoosts) {
    List<RexNode> ops = new ArrayList<>();
    for (int i = 0; i < fieldNamesAndBoosts.length; i += 2) {
      ops.add(rexBuilder.makeLiteral(fieldNamesAndBoosts[i]));
      double boost = Double.parseDouble(fieldNamesAndBoosts[i + 1]);
      ops.add(
          rexBuilder.makeLiteral(
              new BigDecimal(String.valueOf(boost)),
              typeFactory.createSqlType(SqlTypeName.DOUBLE),
              false));
    }
    return rexBuilder.makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, ops);
  }

  // ---- Tests ----

  public void testPureMatchProducesMatchQueryBuilder() {
    RelDataType row = buildRow();
    RelevanceSplitter s = new RelevanceSplitter(rexBuilder, row);

    RexNode field = rexBuilder.makeInputRef(row, 0);
    RexNode cond = makeRelevanceCall("MATCH", "field", field, "query", "pooh");

    RelevanceSplitter.Result r = s.split(cond);
    assertFalse("not ruled out", r.ruledOut);
    assertNull("residual null when everything pushed", r.residualCondition);
    assertTrue(r.pushdownQuery instanceof MatchQueryBuilder);
    MatchQueryBuilder m = (MatchQueryBuilder) r.pushdownQuery;
    assertEquals("body", m.fieldName());
    assertEquals("pooh", m.value());
  }

  public void testMatchAndPredicateSplitsCleanly() {
    RelDataType row = buildRow();
    RelevanceSplitter s = new RelevanceSplitter(rexBuilder, row);

    RexNode bodyRef = rexBuilder.makeInputRef(row, 0);
    RexNode priceRef = rexBuilder.makeInputRef(row, 3);
    RexNode hundred =
        rexBuilder.makeLiteral(
            BigDecimal.valueOf(100), typeFactory.createSqlType(SqlTypeName.INTEGER), false);
    RexNode matchRex = makeRelevanceCall("MATCH", "field", bodyRef, "query", "pooh");
    RexNode gt = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, priceRef, hundred);
    RexNode cond = rexBuilder.makeCall(SqlStdOperatorTable.AND, matchRex, gt);

    RelevanceSplitter.Result r = s.split(cond);
    assertFalse(r.ruledOut);
    assertNotNull("predicate residual preserved", r.residualCondition);
    assertTrue(r.pushdownQuery instanceof MatchQueryBuilder);
  }

  public void testMatchOrTermFullyPushable() {
    RelDataType row = buildRow();
    RelevanceSplitter s = new RelevanceSplitter(rexBuilder, row);

    RexNode bodyRef = rexBuilder.makeInputRef(row, 0);
    RexNode tagsRef = rexBuilder.makeInputRef(row, 2);
    RexNode beer = rexBuilder.makeLiteral("beer");
    RexNode matchRex = makeRelevanceCall("MATCH", "field", bodyRef, "query", "pooh");
    RexNode eq = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, tagsRef, beer);
    RexNode cond = rexBuilder.makeCall(SqlStdOperatorTable.OR, matchRex, eq);

    RelevanceSplitter.Result r = s.split(cond);
    assertFalse(r.ruledOut);
    assertNull("both disjuncts pushed, no residual", r.residualCondition);
    assertTrue("combined via BoolQueryBuilder SHOULD", r.pushdownQuery instanceof BoolQueryBuilder);
    BoolQueryBuilder bool = (BoolQueryBuilder) r.pushdownQuery;
    assertEquals("2 SHOULD clauses", 2, bool.should().size());
  }

  public void testMatchOrUntranslatablePredicateRuledOut() {
    RelDataType row = buildRow();
    RelevanceSplitter s = new RelevanceSplitter(rexBuilder, row);

    RexNode bodyRef = rexBuilder.makeInputRef(row, 0);
    RexNode priceRef = rexBuilder.makeInputRef(row, 3);
    RexNode matchRex = makeRelevanceCall("MATCH", "field", bodyRef, "query", "pooh");
    // abs(price) > 0 — arithmetic on a field has no Lucene-native form, so the OR fails (c).
    RexNode absPrice = rexBuilder.makeCall(SqlStdOperatorTable.ABS, priceRef);
    RexNode zero =
        rexBuilder.makeLiteral(
            BigDecimal.ZERO, typeFactory.createSqlType(SqlTypeName.INTEGER), false);
    RexNode gt = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, absPrice, zero);
    RexNode cond = rexBuilder.makeCall(SqlStdOperatorTable.OR, matchRex, gt);

    RelevanceSplitter.Result r = s.split(cond);
    assertTrue("untranslatable OR leaf rules the whole plan out", r.ruledOut);
  }

  public void testNotOfRelevanceWrapsMustNot() {
    RelDataType row = buildRow();
    RelevanceSplitter s = new RelevanceSplitter(rexBuilder, row);

    RexNode bodyRef = rexBuilder.makeInputRef(row, 0);
    RexNode matchRex = makeRelevanceCall("MATCH", "field", bodyRef, "query", "pooh");
    RexNode notCond = rexBuilder.makeCall(SqlStdOperatorTable.NOT, matchRex);

    RelevanceSplitter.Result r = s.split(notCond);
    assertFalse(r.ruledOut);
    assertNull(r.residualCondition);
    assertTrue(r.pushdownQuery instanceof BoolQueryBuilder);
    BoolQueryBuilder bool = (BoolQueryBuilder) r.pushdownQuery;
    assertEquals("1 MUST_NOT clause", 1, bool.mustNot().size());
  }

  public void testMultipleRelevanceAndedTogether() {
    RelDataType row = buildRow();
    RelevanceSplitter s = new RelevanceSplitter(rexBuilder, row);

    RexNode bodyRef = rexBuilder.makeInputRef(row, 0);
    RexNode titleRef = rexBuilder.makeInputRef(row, 1);
    RexNode a = makeRelevanceCall("MATCH", "field", bodyRef, "query", "pooh");
    RexNode b = makeRelevanceCall("MATCH_PHRASE", "field", titleRef, "query", "winnie the");
    RexNode cond = rexBuilder.makeCall(SqlStdOperatorTable.AND, a, b);

    RelevanceSplitter.Result r = s.split(cond);
    assertFalse(r.ruledOut);
    assertNull(r.residualCondition);
    assertTrue(r.pushdownQuery instanceof BoolQueryBuilder);
    BoolQueryBuilder bool = (BoolQueryBuilder) r.pushdownQuery;
    assertEquals("2 MUST clauses", 2, bool.must().size());
  }

  public void testMatchOptionsAreApplied() {
    RelDataType row = buildRow();
    RelevanceSplitter s = new RelevanceSplitter(rexBuilder, row);

    RexNode bodyRef = rexBuilder.makeInputRef(row, 0);
    RexNode cond =
        makeRelevanceCall(
            "MATCH", "field", bodyRef, "query", "pooh", "operator", "AND", "boost", "2.0");

    RelevanceSplitter.Result r = s.split(cond);
    MatchQueryBuilder m = (MatchQueryBuilder) r.pushdownQuery;
    assertEquals(org.opensearch.index.query.Operator.AND, m.operator());
    assertEquals(2.0f, m.boost(), 1e-6);
  }

  public void testMultiMatchWithFieldsAndBoost() {
    RelDataType row = buildRow();
    RelevanceSplitter s = new RelevanceSplitter(rexBuilder, row);

    RexNode fields = makeFieldsMap("title", "2.0", "body", "1.0");
    RexNode cond = makeRelevanceCall("MULTI_MATCH", "fields", fields, "query", "pooh");

    RelevanceSplitter.Result r = s.split(cond);
    assertFalse(r.ruledOut);
    assertTrue(r.pushdownQuery instanceof MultiMatchQueryBuilder);
    MultiMatchQueryBuilder mm = (MultiMatchQueryBuilder) r.pushdownQuery;
    assertEquals(2, mm.fields().size());
    assertEquals(2.0f, mm.fields().get("title"), 1e-6);
    assertEquals(1.0f, mm.fields().get("body"), 1e-6);
  }

  public void testMultiMatchWithoutFields() {
    RelDataType row = buildRow();
    RelevanceSplitter s = new RelevanceSplitter(rexBuilder, row);

    RexNode cond = makeRelevanceCall("MULTI_MATCH", "query", "pooh");

    RelevanceSplitter.Result r = s.split(cond);
    assertFalse(r.ruledOut);
    assertTrue(r.pushdownQuery instanceof MultiMatchQueryBuilder);
    MultiMatchQueryBuilder mm = (MultiMatchQueryBuilder) r.pushdownQuery;
    assertTrue("no fields when bare form", mm.fields().isEmpty());
  }

  public void testNonRelevanceFilterUnchanged() {
    RelDataType row = buildRow();
    RelevanceSplitter s = new RelevanceSplitter(rexBuilder, row);

    RexNode priceRef = rexBuilder.makeInputRef(row, 3);
    RexNode hundred =
        rexBuilder.makeLiteral(
            BigDecimal.valueOf(100), typeFactory.createSqlType(SqlTypeName.INTEGER), false);
    RexNode cond = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, priceRef, hundred);

    RelevanceSplitter.Result r = s.split(cond);
    assertFalse(r.ruledOut);
    assertNull("nothing to push", r.pushdownQuery);
    assertEquals("residual passes through unchanged", cond, r.residualCondition);
  }

  public void testUnknownOptionRulesOut() {
    RelDataType row = buildRow();
    RelevanceSplitter s = new RelevanceSplitter(rexBuilder, row);

    RexNode bodyRef = rexBuilder.makeInputRef(row, 0);
    RexNode cond =
        makeRelevanceCall("MATCH", "field", bodyRef, "query", "pooh", "nonsense_option", "x");

    RelevanceSplitter.Result r = s.split(cond);
    assertTrue(r.ruledOut);
  }
}
