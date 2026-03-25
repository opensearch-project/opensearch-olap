# OpenSearch OLAP Plugin - Development Context

## What this is
An OpenSearch plugin integrating Apache Velox (via velox4j) as a vectorized execution engine for analytical queries. Co-works with the SQL plugin (`opensearch-sql`) — SQL plugin handles SQL/PPL → Calcite RelNode, OLAP plugin handles RelNode → Velox → execution.

## Architecture
```
SQL Plugin: SQL/PPL → Calcite Analyzer → RelNode
                                           │
                              DelegatingExecutionEngine
                              checks canVectorize(plan)
                                           │
                         ┌─── true ────────┴──── false ───┐
                         ▼                                 ▼
OLAP Plugin: OlapExecutionExtensionImpl     OpenSearchExecutionEngine
             → VeloxExecutionEngine              (default)
             → VeloxPlanConverter
             → PlanFragmenter
             → QueryScheduler
             → Data Nodes (Lucene → Arrow → Velox C++)
```

## Key packages
- `engine/` - SQL plugin integration: `VeloxExecutionEngine` (orchestrates full pipeline), `OlapExecutionExtensionImpl` (implements `ExecutionEngine` with `canVectorize()`)
- `plan/convert/` - Calcite RelNode → velox4j PlanNode conversion (TableScan, Filter, Project, Aggregate, Join, Sort)
- `plan/fragment/` - Splits plan at aggregation boundaries (PARTIAL on data nodes, FINAL on coordinator)
- `scheduler/` - Presto-inspired Stage/Task scheduler using OpenSearch ClusterState for shard routing
- `transport/` - Inter-node communication via OpenSearch TransportService (`ExecuteFragmentAction`)
- `execution/` - Lucene DocValues → Arrow → velox4j ExternalStream.BlockingQueue → Velox C++ execution
- `result/` - Interface stubs for result collection (not yet implemented)

## SQL plugin co-work design
The OLAP plugin implements `ExecutionEngine` (the SQL plugin's own interface) with:
- `canVectorize(RelNode plan)` — returns true if all operators in the plan tree are supported
- `execute(RelNode plan, CalcitePlanContext context, ResponseListener<QueryResponse> listener)` — delegates to VeloxExecutionEngine

The SQL plugin side:
- `SQLPlugin` implements `ExtensiblePlugin`, loads `ExecutionEngine` implementations via SPI in `loadExtensions()`
- `OpenSearchPluginModule` wraps the default engine + extensions in a `DelegatingExecutionEngine`
- `DelegatingExecutionEngine.execute(RelNode, ...)` checks each extension's `canVectorize()`, routes to the first match, falls back to default
- `QueryService` is unchanged — just calls `executionEngine.execute()` which may be the delegating wrapper

SPI service file: `META-INF/services/org.opensearch.sql.executor.ExecutionEngine` → `OlapExecutionExtensionImpl`

## Dependencies
- OpenSearch 3.6.0-SNAPSHOT (compileOnly)
- opensearch-sql 3.6.0.0-SNAPSHOT (extended plugin — classloader sees SQL plugin classes)
- Apache Calcite 1.41.0 (compileOnly — provided by SQL plugin at runtime)
- Apache Arrow 18.1.0
- velox4j 0.1.0-SNAPSHOT — repackaged at build time to strip javax.annotation and org.slf4j (avoids jar hell with SQL plugin's jsr305)

## Build
```bash
./gradlew build
```
Note: `testingConventions` task requires at least one test class. Use `./gradlew assemble` to skip tests.

## Jar hell avoidance
- `calcite-core` is `compileOnly` (provided by SQL plugin)
- `velox4j` is repackaged via `repackageVelox4j` task to remove `javax.annotation` and `org.slf4j` classes
- `jsr305` is excluded globally via `configurations.all`

## Graceful degradation
`VeloxLifecycleService` catches native library load failures and disables itself (logs a warning). This allows the plugin to install on unsupported platforms (e.g. macOS/aarch64) without crashing OpenSearch. `canVectorize()` returns `false` when Velox is unavailable.

## Installation
```bash
bin/opensearch-plugin install opensearch-sql
bin/opensearch-plugin install file:///path/to/opensearch-olap-3.6.0-SNAPSHOT.zip
```

## API reference repositories
- SQL plugin: `/Users/ltjin/github/search-plugins-sql-calcite`
  - `ExecutionEngine` interface: `core/src/main/java/org/opensearch/sql/executor/ExecutionEngine.java`
  - `DelegatingExecutionEngine`: `core/src/main/java/org/opensearch/sql/executor/DelegatingExecutionEngine.java`
- OpenSearch core: `/Users/ltjin/github/OpenSearch`
- Presto: `/Users/ltjin/github/presto` — scheduler design reference
- velox4j: `/Users/ltjin/github/velox4j` — Velox JNI bridge
