/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.plan.physical;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

/**
 * Helper for constructing Calcite RelNode trees in tests. Provides a schema with test tables
 * (employees, departments) and a configured RelBuilder.
 */
public class CalciteTestHelper {

  private static final RelDataTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl();

  /** A simple in-memory table definition. */
  private static class SimpleTable extends AbstractTable {
    private final RelDataType rowType;

    SimpleTable(RelDataType rowType) {
      this.rowType = rowType;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return rowType;
    }
  }

  /**
   * Create a RelBuilder with test tables: employees(emp_id, name, dept_id, salary) and
   * departments(dept_id, dept_name).
   */
  public static RelBuilder createRelBuilder() {
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);

    // employees table
    RelDataType empType =
        TYPE_FACTORY
            .builder()
            .add("emp_id", SqlTypeName.INTEGER)
            .add("name", SqlTypeName.VARCHAR)
            .add("dept_id", SqlTypeName.INTEGER)
            .add("salary", SqlTypeName.DOUBLE)
            .build();
    rootSchema.add("employees", new SimpleTable(empType));

    // departments table
    RelDataType deptType =
        TYPE_FACTORY
            .builder()
            .add("dept_id", SqlTypeName.INTEGER)
            .add("dept_name", SqlTypeName.VARCHAR)
            .build();
    rootSchema.add("departments", new SimpleTable(deptType));

    // projects table (for multi-way join tests)
    RelDataType projType =
        TYPE_FACTORY
            .builder()
            .add("project_id", SqlTypeName.INTEGER)
            .add("project_name", SqlTypeName.VARCHAR)
            .add("dept_id", SqlTypeName.INTEGER)
            .add("budget", SqlTypeName.DOUBLE)
            .build();
    rootSchema.add("projects", new SimpleTable(projType));

    // logs table (for nested object field tests, simulates big5-like schema)
    // OpenSearch object fields appear as MAP<VARCHAR, ANY> with dot-path siblings.
    RelDataType mapType =
        TYPE_FACTORY.createMapType(
            TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR),
            TYPE_FACTORY.createSqlType(SqlTypeName.ANY));
    RelDataType logsType =
        TYPE_FACTORY
            .builder()
            .add("@timestamp", SqlTypeName.TIMESTAMP)
            .add("message", SqlTypeName.VARCHAR)
            .add("cloud", mapType)
            .add("cloud.region", SqlTypeName.VARCHAR)
            .add("metrics", mapType)
            .add("metrics.size", SqlTypeName.BIGINT)
            .add("metrics.tmin", SqlTypeName.BIGINT)
            .build();
    rootSchema.add("logs", new SimpleTable(logsType));

    return RelBuilder.create(Frameworks.newConfigBuilder().defaultSchema(rootSchema).build());
  }
}
