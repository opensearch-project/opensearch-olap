/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.engine;

import java.util.Collections;
import java.util.List;
import org.boostscale.velox4j.expression.FieldAccessTypedExpr;
import org.boostscale.velox4j.expression.TypedExpr;
import org.boostscale.velox4j.join.JoinType;
import org.boostscale.velox4j.plan.HashJoinNode;
import org.boostscale.velox4j.plan.PlanNode;
import org.boostscale.velox4j.plan.ProjectNode;
import org.boostscale.velox4j.plan.TableScanNode;
import org.boostscale.velox4j.type.BigIntType;
import org.boostscale.velox4j.type.IntegerType;
import org.boostscale.velox4j.type.RowType;
import org.boostscale.velox4j.type.Type;
import org.boostscale.velox4j.type.VarCharType;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link VeloxExecutionEngine#resolveRawBuildKeyName(HashJoinNode, boolean, String)}.
 *
 * <p>The resolver reverses the rename that {@code VeloxPlanGenerator.convertJoin} inserts on the
 * build side when the two join inputs share a column name — Calcite's uniquifier ({@code
 * SqlValidatorUtil.EXPR_SUGGESTER}) appends an attempt counter (e.g. {@code dept_id} becomes {@code
 * dept_id0}, {@code sku2} becomes {@code sku20}). A naive regex {@code \d+$} strip can't
 * distinguish the disambiguation counter from legitimate trailing digits in real field names, so
 * the resolver walks the plan instead: the {@link ProjectNode} above the build scan carries the
 * rename mapping (names→raw field refs), and we reverse it.
 *
 * <p>Regression guard: without the schema-aware resolver, field names like {@code sku2} and {@code
 * year2024} get mangled, the data-node bloom builder finds nothing in the scan schema, and the
 * probe ends up filtered to zero rows.
 */
public class VeloxExecutionEngineTests extends OpenSearchTestCase {

  // ---- Helpers to build velox4j plan fragments for testing ----

  private TableScanNode scan(String id, RowType output) {
    return new TableScanNode(
        id,
        output,
        new org.boostscale.velox4j.connector.ExternalStreamTableHandle("connector-external-stream"),
        Collections.emptyList());
  }

  private RowType rowType(String... namesWithTypes) {
    List<String> names = new java.util.ArrayList<>();
    List<Type> types = new java.util.ArrayList<>();
    for (int i = 0; i < namesWithTypes.length; i += 2) {
      names.add(namesWithTypes[i]);
      types.add(parseType(namesWithTypes[i + 1]));
    }
    return new RowType(names, types);
  }

  private Type parseType(String s) {
    switch (s) {
      case "int":
        return new IntegerType();
      case "bigint":
        return new BigIntType();
      case "varchar":
      default:
        return new VarCharType();
    }
  }

  private FieldAccessTypedExpr field(String name, Type type) {
    return FieldAccessTypedExpr.create(type, name);
  }

  private ProjectNode rename(String id, PlanNode source, String[][] renames) {
    List<String> names = new java.util.ArrayList<>();
    List<TypedExpr> projections = new java.util.ArrayList<>();
    for (String[] pair : renames) {
      // pair[0] = new name (exposed to parent), pair[1] = source field name
      names.add(pair[0]);
      projections.add(field(pair[1], new IntegerType()));
    }
    return new ProjectNode(id, Collections.singletonList(source), names, projections);
  }

  private HashJoinNode join(PlanNode left, PlanNode right, String leftKey, String rightKey) {
    return new HashJoinNode(
        "j",
        JoinType.INNER,
        List.of(field(leftKey, new IntegerType())),
        List.of(field(rightKey, new IntegerType())),
        null,
        left,
        right,
        // output type is approximate for this test — the resolver only looks at sources.
        rowType("x", "int"),
        /* nullAware */ false,
        /* nullAsValue */ false,
        /* useHashTableCache */ false);
  }

  // ---- Tests ----

  /**
   * No rename project on the build side — the key name in the join IS the raw name. The resolver
   * must return it unchanged even if it happens to end in digits.
   */
  public void testResolveRawBuildKeyName_noRename_returnsOriginal() {
    TableScanNode probe = scan("probe", rowType("name", "varchar", "dept_id", "int"));
    TableScanNode build = scan("build", rowType("dept_id", "int", "dept_name", "varchar"));
    HashJoinNode j = join(probe, build, "dept_id", "dept_id");

    assertEquals(
        "dept_id",
        VeloxExecutionEngine.resolveRawBuildKeyName(j, /* isBuildLeft= */ false, "dept_id"));
  }

  /**
   * Realistic Calcite disambiguation: two sides have "dept_id". Calcite renames the build-side
   * occurrence to "dept_id0" and VeloxPlanGenerator inserts a ProjectNode to apply that rename. The
   * resolver must undo the rename and return the raw "dept_id" used by the data-node scan.
   */
  public void testResolveRawBuildKeyName_renamedViaProject_returnsRawName() {
    TableScanNode probe = scan("probe", rowType("dept_id", "int"));
    TableScanNode build = scan("build", rowType("dept_id", "int", "dept_name", "varchar"));
    ProjectNode renamed =
        rename(
            "rename",
            build,
            new String[][] {
              {"dept_id0", "dept_id"}, {"dept_name", "dept_name"},
            });
    HashJoinNode j = join(probe, renamed, "dept_id", "dept_id0");

    assertEquals(
        "dept_id",
        VeloxExecutionEngine.resolveRawBuildKeyName(j, /* isBuildLeft= */ false, "dept_id0"));
  }

  /**
   * The critical regression case Codex flagged. Field name "sku2" is a real index column. Calcite
   * disambiguates the duplicate to "sku20" — a regex \d+$ would strip it to "sku" (wrong: "sku" is
   * not a column). The resolver must return "sku2" by reading the rename project.
   */
  public void testResolveRawBuildKeyName_realFieldEndingInDigits() {
    TableScanNode probe = scan("probe", rowType("sku2", "int"));
    TableScanNode build = scan("build", rowType("sku2", "int", "price", "int"));
    ProjectNode renamed =
        rename(
            "rename",
            build,
            new String[][] {
              {"sku20", "sku2"}, {"price", "price"},
            });
    HashJoinNode j = join(probe, renamed, "sku2", "sku20");

    assertEquals(
        "sku2", VeloxExecutionEngine.resolveRawBuildKeyName(j, /* isBuildLeft= */ false, "sku20"));
  }

  /**
   * Another real-field-ending-in-digits case: year2024. Calcite rename → year20240 (append attempt
   * 0). Naive strip would yield "year" — completely wrong. The resolver must return "year2024".
   */
  public void testResolveRawBuildKeyName_year2024() {
    TableScanNode probe = scan("probe", rowType("year2024", "int"));
    TableScanNode build = scan("build", rowType("year2024", "int"));
    ProjectNode renamed = rename("rename", build, new String[][] {{"year20240", "year2024"}});
    HashJoinNode j = join(probe, renamed, "year2024", "year20240");

    assertEquals(
        "year2024",
        VeloxExecutionEngine.resolveRawBuildKeyName(j, /* isBuildLeft= */ false, "year20240"));
  }

  /** When the build side is on the left, the resolver reads the left child. */
  public void testResolveRawBuildKeyName_buildOnLeft() {
    TableScanNode build = scan("build", rowType("dept_id", "int"));
    TableScanNode probe = scan("probe", rowType("dept_id", "int"));
    // Note: VeloxPlanGenerator only wraps the RIGHT side in a rename project. Build-on-left
    // means the build side has raw names. The resolver finds a TableScanNode (not a
    // ProjectNode) and returns the supplied name unchanged.
    HashJoinNode j = join(build, probe, "dept_id", "dept_id");

    assertEquals(
        "dept_id",
        VeloxExecutionEngine.resolveRawBuildKeyName(j, /* isBuildLeft= */ true, "dept_id"));
  }

  /** Null and missing-key guards. */
  public void testResolveRawBuildKeyName_handlesNulls() {
    // Null joinNode: can't resolve, fall back to the passed-in key name.
    assertEquals("x", VeloxExecutionEngine.resolveRawBuildKeyName(null, false, "x"));
    // Null key name: nothing to resolve, return null.
    TableScanNode probe = scan("p", rowType("a", "int"));
    TableScanNode build = scan("b", rowType("a", "int"));
    HashJoinNode j = join(probe, build, "a", "a");
    assertNull(VeloxExecutionEngine.resolveRawBuildKeyName(j, false, null));
  }

  /**
   * When the rename project exists but doesn't contain the requested key, fall back to the key
   * unchanged. This path would only trigger if the join's build key somehow didn't match any of the
   * project's output names — in practice that shouldn't happen, but the resolver shouldn't throw.
   */
  public void testResolveRawBuildKeyName_keyNotInRename_returnsOriginal() {
    TableScanNode probe = scan("probe", rowType("a", "int"));
    TableScanNode build = scan("build", rowType("a", "int", "b", "int"));
    ProjectNode renamed = rename("rename", build, new String[][] {{"b0", "b"}});
    HashJoinNode j = join(probe, renamed, "a", "a0");

    // "a0" isn't in the rename project's names — fall back to the original.
    assertEquals(
        "a0", VeloxExecutionEngine.resolveRawBuildKeyName(j, /* isBuildLeft= */ false, "a0"));
  }
}
