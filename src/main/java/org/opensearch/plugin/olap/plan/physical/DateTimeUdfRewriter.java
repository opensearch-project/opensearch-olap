/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.opensearch.sql.calcite.type.AbstractExprRelDataType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;
import org.opensearch.sql.utils.DateTimeFormatters;

/**
 * Strips PPL's datetime coercion UDFs ({@code timestamp()}, {@code date()}, {@code time()}) from
 * the plan so Velox can execute the underlying comparison directly.
 *
 * <p>The SQL plugin's {@code CoercionUtils} wraps both sides of a comparison between a datetime UDT
 * column and a string literal in the matching coercion UDF. For example {@code @timestamp >=
 * '2023-01-01 00:00:00'} becomes {@code timestamp(@timestamp) >= timestamp('2023-01-01 00:00:00')};
 * {@code @date >= '2023-01-01'} becomes {@code date(@date) >= date('2023-01-01')}. Velox has no
 * scalar for these UDFs over VARCHAR, so the queries fail when routed through the OLAP plugin.
 *
 * <p>This rewriter handles three UDF shapes, each with identical plan-time semantics:
 *
 * <ul>
 *   <li>{@code timestamp(<TIMESTAMP expression>)}, {@code date(<DATE expression>)}, {@code
 *       time(<TIME expression>)} → the inner expression (identity: the UDF's {@code (T) -> T}
 *       signature is a no-op)
 *   <li>{@code timestamp('yyyy-MM-dd HH:mm:ss[...]')} → Calcite TIMESTAMP literal
 *   <li>{@code date('yyyy-MM-dd')} → Calcite DATE literal (days since epoch)
 *   <li>{@code time('HH:mm:ss[.nnn]')} → Calcite TIME literal (millis since midnight)
 *   <li>Nested calls (e.g. {@code timestamp(timestamp(x))}) — inner rewritten first by the
 *       RexShuttle; outer then stripped by the identity rule
 * </ul>
 *
 * <p><b>Cross-type wrappers are intentionally not stripped.</b> {@code timestamp(<DATE>)} casts to
 * midnight-of-that-day, {@code date(<TIMESTAMP>)} truncates to the date part, {@code
 * time(<TIMESTAMP>)} extracts the time-of-day. These are real casts, not identities. Shapes that
 * can't be simplified at plan time (cross-type wrappers, {@code timestamp(concat(x,y))}, etc.) are
 * rejected by {@link #isRewritable} — those queries fall back to the default engine via {@code
 * canVectorize()}.
 *
 * <p>After rewrite, both sides of the comparison share a Velox type — TIMESTAMP, DATE (int32 days),
 * or TIME (BIGINT millis) — so the comparison binds to native Velox operators.
 */
public final class DateTimeUdfRewriter {

  private DateTimeUdfRewriter() {}

  public static RelNode rewrite(RelNode root) {
    RexBuilder rexBuilder = root.getCluster().getRexBuilder();
    return root.accept(new RelShuttle(new RexRewriter(rexBuilder)));
  }

  /**
   * Returns true if {@code call} is a supported datetime coercion UDF in a shape this rewriter can
   * simplify. Used by {@code canVectorize} so plans containing rewritable calls are not rejected
   * before optimization. Cross-type wrappers (e.g. {@code timestamp(<DATE ref>)}) are rejected —
   * those are real casts and fall back via {@code canVectorize()}.
   */
  public static boolean isRewritable(RexCall call) {
    TargetKind kind = targetKind(call);
    if (kind == null || call.getOperands().size() != 1) {
      return false;
    }
    RexNode operand = call.getOperands().get(0);
    if (operand instanceof RexCall) {
      // A nested call's return type may be the right kind (e.g. date(concat(...)) is EXPR_DATE),
      // but that doesn't make the outer rewritable — the inner must itself be a rewritable
      // coercion that will reduce to something the outer can strip. Check explicitly rather than
      // trusting the declared return type of an arbitrary call.
      RexCall innerCall = (RexCall) operand;
      TargetKind innerKind = targetKind(innerCall);
      return innerKind == kind && isRewritable(innerCall);
    }
    // Identity: UDF(<same-type ref/literal/etc.>) is a no-op in PPL; the rewriter strips it.
    if (kind.matches(operand.getType())) {
      return true;
    }
    if (operand instanceof RexLiteral) {
      RexLiteral literal = (RexLiteral) operand;
      if (literal.isNull()) {
        return false;
      }
      SqlTypeName typeName = literal.getTypeName();
      if (typeName != SqlTypeName.CHAR && typeName != SqlTypeName.VARCHAR) {
        return false;
      }
      NlsString nls = literal.getValueAs(NlsString.class);
      return nls != null && kind.parse(nls.getValue()) != null;
    }
    return false;
  }

  /** Applies a RexShuttle to each RelNode in the tree, recursing into inputs. */
  private static final class RelShuttle extends RelShuttleImpl {
    private final RexShuttle rexShuttle;

    RelShuttle(RexShuttle rexShuttle) {
      this.rexShuttle = rexShuttle;
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
      RelNode input = filter.getInput().accept(this);
      RelNode rewritten = filter.copy(filter.getTraitSet(), List.of(input));
      return rewritten.accept(rexShuttle);
    }

    @Override
    public RelNode visit(LogicalProject project) {
      RelNode input = project.getInput().accept(this);
      RelNode rewritten = project.copy(project.getTraitSet(), List.of(input));
      return rewritten.accept(rexShuttle);
    }

    @Override
    public RelNode visit(LogicalJoin join) {
      RelNode left = join.getLeft().accept(this);
      RelNode right = join.getRight().accept(this);
      RelNode rewritten = join.copy(join.getTraitSet(), List.of(left, right));
      return rewritten.accept(rexShuttle);
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
      RelNode input = aggregate.getInput().accept(this);
      return aggregate.copy(aggregate.getTraitSet(), List.of(input));
    }

    @Override
    public RelNode visit(LogicalSort sort) {
      RelNode input = sort.getInput().accept(this);
      return sort.copy(sort.getTraitSet(), List.of(input));
    }

    @Override
    public RelNode visit(RelNode other) {
      if (other instanceof Sort) {
        RelNode input = ((Sort) other).getInput().accept(this);
        return other.copy(other.getTraitSet(), List.of(input));
      }
      List<RelNode> inputs =
          other.getInputs().stream().map(i -> i.accept(this)).collect(Collectors.toList());
      RelNode rewritten = inputs.isEmpty() ? other : other.copy(other.getTraitSet(), inputs);
      return rewritten.accept(rexShuttle);
    }
  }

  /** Rewrites {@code timestamp(...)}, {@code date(...)}, {@code time(...)} in RexNode trees. */
  private static final class RexRewriter extends RexShuttle {
    private final RexBuilder rexBuilder;

    RexRewriter(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      RexCall rewrittenCall = (RexCall) super.visitCall(call);
      TargetKind kind = targetKind(rewrittenCall);
      if (kind == null) {
        return rewrittenCall;
      }
      if (rewrittenCall.getOperands().size() != 1) {
        // Two-arg forms (e.g. timestamp(datetime, addTime)) have no plan-time shortcut.
        return rewrittenCall;
      }
      RexNode operand = rewrittenCall.getOperands().get(0);
      RexNode simplified = simplify(kind, operand);
      return simplified != null ? simplified : rewrittenCall;
    }

    private RexNode simplify(TargetKind kind, RexNode operand) {
      // Identity: UDF(<same-type expression>) is a no-op — strip the wrapper. Covers UDT column
      // refs (EXPR_TIMESTAMP/EXPR_DATE/EXPR_TIME), already-parsed plain-SQL literals, and the outer
      // layer of nested UDF(UDF(...)) after the inner call has been rewritten.
      if (kind.matches(operand.getType())) {
        return operand;
      }
      if (operand instanceof RexLiteral) {
        return simplifyLiteral(kind, (RexLiteral) operand);
      }
      return null;
    }

    private RexNode simplifyLiteral(TargetKind kind, RexLiteral literal) {
      if (literal.isNull()) {
        return null;
      }
      SqlTypeName typeName = literal.getTypeName();
      if (typeName != SqlTypeName.CHAR && typeName != SqlTypeName.VARCHAR) {
        return null;
      }
      NlsString nls = literal.getValueAs(NlsString.class);
      if (nls == null) {
        return null;
      }
      return kind.makeLiteral(rexBuilder, nls.getValue());
    }
  }

  /**
   * The three PPL datetime coercion UDFs we handle. Each encapsulates (a) the UDF name we match on,
   * (b) the type check used for identity stripping, and (c) how to parse a string literal into a
   * Calcite literal of the target type.
   */
  private enum TargetKind {
    TIMESTAMP("TIMESTAMP", ExprUDT.EXPR_TIMESTAMP) {
      @Override
      boolean matchesSqlType(SqlTypeName t) {
        return t == SqlTypeName.TIMESTAMP || t == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
      }

      @Override
      Long parse(String text) {
        try {
          LocalDateTime ldt =
              LocalDateTime.parse(text, DateTimeFormatters.DATE_TIMESTAMP_FORMATTER);
          return ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
        } catch (DateTimeParseException ignored) {
          // Fall through to ISO 8601 attempt.
        }
        try {
          ZonedDateTime zdt = ZonedDateTime.parse(text, DateTimeFormatter.ISO_DATE_TIME);
          return zdt.toInstant().toEpochMilli();
        } catch (DateTimeParseException ignored) {
          return null;
        }
      }

      @Override
      RexNode makeLiteral(RexBuilder b, String text) {
        Long millis = parse(text);
        if (millis == null) {
          return null;
        }
        // Millisecond precision (3) matches the Velox TimestampType on the other side of the
        // comparison. VeloxExprConverter maps this to a TimestampValue variant.
        return b.makeTimestampLiteral(TimestampString.fromMillisSinceEpoch(millis), 3);
      }
    },
    DATE("DATE", ExprUDT.EXPR_DATE) {
      @Override
      boolean matchesSqlType(SqlTypeName t) {
        return t == SqlTypeName.DATE;
      }

      @Override
      Integer parse(String text) {
        try {
          return (int) LocalDate.parse(text, DateTimeFormatter.ISO_LOCAL_DATE).toEpochDay();
        } catch (DateTimeParseException ignored) {
          return null;
        }
      }

      @Override
      RexNode makeLiteral(RexBuilder b, String text) {
        Integer days = parse(text);
        if (days == null) {
          return null;
        }
        LocalDate ld = LocalDate.ofEpochDay(days);
        return b.makeDateLiteral(
            new DateString(ld.getYear(), ld.getMonthValue(), ld.getDayOfMonth()));
      }
    },
    TIME("TIME", ExprUDT.EXPR_TIME) {
      @Override
      boolean matchesSqlType(SqlTypeName t) {
        return t == SqlTypeName.TIME || t == SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE;
      }

      @Override
      Integer parse(String text) {
        try {
          LocalTime lt = LocalTime.parse(text, DateTimeFormatter.ISO_LOCAL_TIME);
          // Calcite TIME literals are millis since midnight.
          return (int) (lt.toNanoOfDay() / 1_000_000L);
        } catch (DateTimeParseException ignored) {
          return null;
        }
      }

      @Override
      RexNode makeLiteral(RexBuilder b, String text) {
        Integer millis = parse(text);
        if (millis == null) {
          return null;
        }
        LocalTime lt = LocalTime.ofNanoOfDay(millis * 1_000_000L);
        // Millisecond precision (3). VeloxTypeConverter maps TIME → BigIntType.
        return b.makeTimeLiteral(
            new TimeString(lt.getHour(), lt.getMinute(), lt.getSecond())
                .withMillis((int) (lt.getNano() / 1_000_000L)),
            3);
      }
    };

    private final String udfName;
    private final ExprUDT udt;

    TargetKind(String udfName, ExprUDT udt) {
      this.udfName = udfName;
      this.udt = udt;
    }

    /** True when {@code type} already carries this kind's logical type (UDT or plain SQL type). */
    boolean matches(RelDataType type) {
      if (type instanceof AbstractExprRelDataType<?>) {
        return ((AbstractExprRelDataType<?>) type).getUdt() == udt;
      }
      return matchesSqlType(type.getSqlTypeName());
    }

    abstract boolean matchesSqlType(SqlTypeName t);

    /** Parse a literal string; returns null if it doesn't match this kind's format. */
    abstract Object parse(String text);

    /** Build a Calcite literal for this kind from a string, or null on parse failure. */
    abstract RexNode makeLiteral(RexBuilder b, String text);
  }

  /** Returns the kind matching the operator name, or null if this isn't a coercion UDF call. */
  private static TargetKind targetKind(RexCall call) {
    String name = call.getOperator().getName().toUpperCase(Locale.ROOT);
    for (TargetKind k : TargetKind.values()) {
      if (k.udfName.equals(name)) {
        return k;
      }
    }
    return null;
  }
}
