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
  - `unified-query-core`, `unified-query-common`, `unified-query-opensearch` (compileOnly, transitive=false)
- Apache Calcite 1.41.0 (compileOnly — provided by SQL plugin at runtime)
- Apache Arrow 18.1.0 — `arrow-vector`, `arrow-memory-core`, `arrow-memory-unsafe`, `arrow-c-data`, `arrow-format`
- `com.google.flatbuffers:flatbuffers-java:24.3.25` — required by `arrow-c-data` at runtime (not pulled transitively)
- velox4j 0.1.0-SNAPSHOT — repackaged at build time to strip javax.annotation and org.slf4j (avoids jar hell with SQL plugin's jsr305)

## Build
```bash
./gradlew build
```
Note: `testingConventions` task requires at least one test class. Use `./gradlew assemble` to skip tests.

## Jar hell and classloader isolation
OpenSearch plugins run in isolated classloaders that cannot see classes from OpenSearch core or other plugins (except declared `extendedPlugins`). This causes several dependency conflicts:

- `calcite-core` is `compileOnly` (provided by SQL plugin at runtime via extendedPlugins classloader delegation)
- `velox4j` is repackaged via `repackageVelox4j` task to remove `javax.annotation` and `org.slf4j` classes (avoids jar hell with SQL plugin's jsr305)
- `jsr305` is excluded globally via `configurations.all`
- **Arrow memory allocator**: Must use `arrow-memory-unsafe` instead of `arrow-memory-netty`. OpenSearch loads Netty in the system classloader, but plugin classloaders are isolated and cannot access those classes. `arrow-memory-netty` triggers a fatal `NoClassDefFoundError` (`io.netty.buffer.PooledByteBufAllocatorL`) on the search thread, crashing the JVM. `arrow-memory-unsafe` uses `sun.misc.Unsafe` directly with zero external dependencies. This is safe because Velox manages its own memory in C++ — the Java-side Arrow allocator is only used for the Lucene-to-Arrow bridge feeding ExternalStream, where pooling overhead is negligible.
- **Arrow C Data transitive deps**: `arrow-c-data` requires `arrow-format` (provides `org.apache.arrow.flatbuf.*` classes) and `com.google.flatbuffers:flatbuffers-java` (provides `com.google.flatbuffers.Table`). These are not pulled transitively by Gradle and must be declared explicitly. Without them, `Data.exportSchema()` throws fatal `NoClassDefFoundError` on the feeder thread.
- **Thread context classloader**: `VeloxLifecycleService` must set `Thread.currentThread().setContextClassLoader(Velox4j.class.getClassLoader())` before calling `Velox4j.initialize()`. The velox4j native library discovery uses the thread context classloader to find `velox4j-lib/Linux/amd64/` resources, which are inside the plugin's repackaged jar but invisible to the default thread context classloader.
- **TransportService wiring**: `TransportService` is not available in `Plugin.createComponents()`. It is injected via Guice into `TransportExecuteFragmentAction`, which wires it back to `VeloxExecutionEngine` via `setTransportService()`.

## SQL plugin RelNode types
The SQL plugin uses custom Calcite RelNode subclasses, not always the standard `Logical*` variants:
- **Table scan**: `CalciteLogicalIndexScan` (from `unified-query-opensearch`), extends `TableScan` via `AbstractCalciteIndexScan`. Use `instanceof TableScan` (the base class) instead of `LogicalTableScan` to match both.
- **System limit**: `LogicalSystemLimit` (from `unified-query-core`), extends `Sort`. Applied automatically by the SQL plugin to cap query results (default 10000 rows). Handle as a `Sort` with fetch/offset.
- **Table qualified name**: `TableScan.getTable().getQualifiedName()` returns `["OpenSearch", "index_name"]`. Extract the last element for the actual index name.

## velox4j integration details
- **ExternalStream connector ID**: The connector is registered in velox4j's C++ init as `"connector-external-stream"` (not `"external_stream"`). The `ExternalStreamTableHandle` and `ExternalStreamConnectorSplit` must use this exact ID.
- **Velox function names**: Velox uses Presto-style full names for comparison operators: `greaterthan`, `greaterthanorequal`, `lessthan`, `lessthanorequal`, `equalto`, `notequalto`. Not short forms like `gt`, `gte`, `lt`, `lte`, `eq`, `neq`.
- **ExternalStream split wiring**: `SerialTask.addSplit(planNodeId, split)` takes the **plan node ID** of the `TableScanNode` (not the connector ID). The `ExternalStreamConnectorSplit` takes `(connectorId, queueId)` where `queueId` is `BlockingQueue.id()`.
- **Query construction**: A velox4j `Query` wraps `PlanNode` + `Config` + `ConnectorConfig`. The `ConnectorConfig` must register the connector ID with `ConnectorConfig.create(Map.of("connector-external-stream", Config.empty()))`. Plan serialization uses `Serde.toJson(query)` / `Serde.fromJson(json, Query.class)`.
- **Calcite DECIMAL literals**: Calcite represents integer literals (e.g. `30`) as `DECIMAL` type with scale 0. Velox requires exact type match in expressions, so `VeloxExprConverter` must produce `IntegerValue`/`BigIntValue` (not `DoubleValue`) for zero-scale decimals.
- **PARTIAL/FINAL aggregation exchange**: Arrow IPC does NOT preserve Velox's intermediate accumulator state (e.g., avg's `{sum, count}`). The coordinator exchange must use Velox native serialization (`BaseVectors.serializeToBuf/deserializeFromBuf`) — added to velox4j as a custom extension. Without this, the FINAL aggregation hangs because it can't interpret Arrow-deserialized data as valid intermediate state.
- **ExternalStream empty assignments**: `ExternalStreamConnector` enforces `columnHandles.empty()` in C++. `TableScanNode` for ExternalStream must have empty assignments list — the schema is defined solely by `outputType`.
- **velox4j source**: `../velox4j`

## Distributed aggregation execution flow
For aggregation queries on multi-node clusters, the execution is phased:

1. **PlanFragmenter** splits `AggregationNode(SINGLE)` into two fragments:
   - Leaf fragment: `TableScan → ... → AggregationNode(PARTIAL)` — runs on data nodes
   - Root fragment: `AggregationNode(FINAL) → Project → Limit` — runs on coordinator
2. **Phase 1 (data nodes)**: `NodeResultCollector` dispatches only leaf-stage tasks. Each data node runs PARTIAL aggregation via Velox C++. Results serialized with `BaseVectors.serializeToBuf()` (Velox native binary format, NOT Arrow IPC).
3. **Phase 2 (coordinator)**: `VeloxExecutionEngine.executeCoordinatorFragment()`:
   - Deserializes partial results via `BaseVectors.deserializeOneFromBuf()` — preserves intermediate accumulator types
   - Pushes deserialized RowVectors into a local `ExternalStream.BlockingQueue`
   - Dynamically wires a `TableScanNode(exchange_scan)` as the source of the FINAL `AggregationNode` (since the FINAL node was created with empty sources)
   - Executes the FINAL plan through Velox C++ locally on the coordinator
   - Converts final results to Arrow IPC for the SQL plugin response

Key implementation details:
- `PlanFragmenter.replaceSource()` reconstructs parent nodes (LimitNode, ProjectNode) when replacing the aggregation split point
- `PlanFragmenter.getNodeSources()` uses reflection (`setAccessible(true)`) because `PlanNode.getSources()` is `protected`
- `TransportExecuteFragmentAction` detects PARTIAL plans via `planJson.contains("\"step\":\"PARTIAL\"")` to decide Arrow IPC vs native serde
- The feeder thread must start AFTER `serialTask.addSplit()` + `noMoreSplits()` to avoid race conditions

## Known issues / TODOs
- **Filter + projection data correctness**: Non-aggregation queries (`where age > 30 | fields name, age, salary`) return incorrect data. The `TableScanNode` outputType includes 10 columns (4 user columns + 6 metadata: `_id`, `_index`, `_score`, `_maxscore`, `_sort`, `_routing`) but `LuceneArrowReader` only reads the 4 user columns that have doc values. Column positions mismatch causes Velox to read wrong data. Fix: trim outputType to only columns actually referenced by the query (see TODO in `VeloxPlanConverter.visitTableScan()`).
- **OpenSearch doc values types**: OpenSearch stores all numeric types as `SORTED_NUMERIC` (not `NUMERIC`) and keyword/text as `SORTED_SET` (not `SORTED`). `LuceneArrowReader.mapToDocValueType()` handles this.
- **Arrow Text → String**: Arrow Utf8 vectors return `org.apache.arrow.vector.util.Text` objects. Must convert to `String` before passing to `ExprValueUtils.tupleValue()` in `VeloxExecutionEngine.readArrowIpcToExprValues()`.
- **Velox temp dirs in /tmp**: Each Velox initialization creates ~490MB temp dir under `/tmp`. Multiple restarts or multi-node clusters on the same host can fill `/tmp` (tmpfs). Clean with `rm -rf /tmp/opensearch-*`.
- **velox4j `PlanNode.getSources()` is protected**: Cannot traverse plan trees without reflection. Affects `PlanFragmenter`, `VeloxExecutor.findTableScanNodeId()`, `VeloxExecutionEngine.wireSourceIntoPlan()`, and `TransportExecuteFragmentAction.findTableScanNode()`.

## Graceful degradation
`VeloxLifecycleService` catches native library load failures and disables itself (logs a warning). This allows the plugin to install on unsupported platforms (e.g. macOS/aarch64) without crashing OpenSearch. `canVectorize()` returns `false` when Velox is unavailable.

## Installation
```bash
bin/opensearch-plugin install opensearch-sql
bin/opensearch-plugin install file:///path/to/opensearch-olap-3.6.0-SNAPSHOT.zip
```

## API reference repositories
All repositories are siblings under the same parent directory (`../`):
- SQL plugin: `../search-plugins-sql`
  - `ExecutionEngine` interface: `core/src/main/java/org/opensearch/sql/executor/ExecutionEngine.java`
  - `DelegatingExecutionEngine`: `core/src/main/java/org/opensearch/sql/executor/DelegatingExecutionEngine.java`
- OpenSearch core: `../OpenSearch`
- Presto: `../presto` — scheduler design reference
- velox4j: `../velox4j` — Velox JNI bridge (C++ connector init in `src/main/cpp/main/velox4j/init/Init.cc`)
- Gluten-Flink: `../gluten` — reference for how Flink uses velox4j (different architecture: streaming, uses custom `StatefulPlanNode`, Flink handles shuffle)
