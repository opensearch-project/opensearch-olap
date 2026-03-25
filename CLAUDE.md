# OpenSearch OLAP Plugin - Development Context

## What this is
An OpenSearch plugin integrating Apache Velox (via velox4j) as a vectorized execution engine for analytical queries. Co-works with the SQL plugin (`opensearch-sql`) — SQL plugin handles SQL/PPL → Calcite RelNode, OLAP plugin handles RelNode → Velox → execution.

## Architecture
```
SQL Plugin: SQL/PPL → Calcite Analyzer → RelNode
OLAP Plugin: VeloxPlanConverter → PlanFragmenter → QueryScheduler → Data Nodes
Data Nodes: Lucene DocValues → Arrow batches → ExternalStream → Velox C++ execution
```

## Key packages
- `engine/` - SQL plugin integration: `VeloxExecutionEngine` (orchestrates full pipeline), `OlapExecutionExtension` (ExtensiblePlugin interface for SQL plugin discovery)
- `plan/convert/` - Calcite RelNode → velox4j PlanNode conversion (TableScan, Filter, Project, Aggregate, Join, Sort)
- `plan/fragment/` - Splits plan at aggregation boundaries (PARTIAL on data nodes, FINAL on coordinator)
- `scheduler/` - Presto-inspired Stage/Task scheduler using OpenSearch ClusterState for shard routing
- `transport/` - Inter-node communication via OpenSearch TransportService (`ExecuteFragmentAction`)
- `execution/` - Lucene DocValues → Arrow → velox4j ExternalStream.BlockingQueue → Velox C++ execution
- `result/` - Interface stubs for result collection (not yet implemented)

## Dependencies
- OpenSearch 3.5.0-SNAPSHOT (compileOnly)
- opensearch-sql (extended plugin — classloader sees SQL plugin classes)
- Apache Calcite 1.41.0
- Apache Arrow 18.1.0
- velox4j 0.1.0-SNAPSHOT (from https://central.sonatype.com/repository/maven-snapshots/)

## Build
```bash
./gradlew build
```
Build is NOT yet compiling — dependency resolution and compilation errors expected.

## Key design decisions
- Uses OpenSearch `ExtensiblePlugin` mechanism: OLAP plugin declares `extendedPlugins = ['opensearch-sql']` so its classloader can access SQL plugin's classes
- `OlapExecutionExtensionImpl` is the bridge — SQL plugin calls `loader.loadExtensions(OlapExecutionExtension.class)` to find it
- velox4j `ExternalStream.BlockingQueue` bridges Java (Arrow batches) to C++ (Velox) with zero-copy via Arrow C Data Interface
- No duplicate SQL parsing — all SQL/PPL → Calcite is handled by the SQL plugin

## SQL plugin integration (requires changes in opensearch-sql)
The SQL plugin needs to:
1. Implement `ExtensiblePlugin` in `SQLPlugin.java`
2. Call `loader.loadExtensions(OlapExecutionExtension.class)` in `loadExtensions()`
3. In `QueryService.executeWithCalcite()`, check `olapExtension.canExecute(relNode)` and delegate if true

## API reference repositories
- SQL plugin: `/Users/ltjin/github/search-plugins-sql-calcite` — `ExecutionEngine` interface in `core/src/main/java/org/opensearch/sql/executor/ExecutionEngine.java`
- OpenSearch core: `/Users/ltjin/github/OpenSearch`
- Presto: `/Users/ltjin/github/presto` — scheduler design reference
- velox4j: `/Users/ltjin/github/velox4j` — Velox JNI bridge
