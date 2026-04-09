# OpenSearch OLAP Plugin - Development Context

## What this is
An OpenSearch plugin integrating Apache Velox (via velox4j) as a vectorized execution engine for analytical queries. Co-works with the SQL plugin (`opensearch-sql`) — SQL plugin handles SQL/PPL → Calcite RelNode, OLAP plugin handles RelNode → Velox → execution.

## Architecture
```
SQL Plugin: SQL/PPL → Calcite Analyzer → RelNode (Convention.NONE)
                                           │
                              DelegatingExecutionEngine
                              checks canVectorize(plan)
                                           │
                         ┌─── true ────────┴──── false ───┐
                         ▼                                 ▼
OLAP Plugin: VeloxExecutionEngine           OpenSearchExecutionEngine
             │                                   (default)
             ▼
  PhysicalOptimizer
    1. ClusterCopyShuttle: deep-copies plan into new VolcanoPlanner
       - Strips CalciteLogicalIndexScan → plain LogicalTableScan
       - New cluster has RelDistributionTraitDef from start
    2. HepPlanner: FilterMergeRule (lightweight logical cleanup)
    3. VolcanoPlanner with ConverterRules:
       - Convention.NONE → PhysicalConvention
       - PhysicalAggregateRule inserts PhysicalExchange(SINGLETON) before agg
       - PhysicalJoinRule inserts PhysicalExchange(SINGLETON) for both join inputs
       - MPP rules: HASH distribution alternatives (when mpp_enabled=true)
             │
             ▼
  VeloxPlanGenerator
    - Walks physical plan, splits at PhysicalExchange boundaries
    - Two-stage agg split: Aggregate(SINGLE)+Exchange → PARTIAL+FINAL
    - Converts to Velox PlanNodes (reuses plan/convert/ utilities)
    - Produces List<PlanFragment>
             │
             ▼
  QueryScheduler → NodeResultCollector → Velox C++
```

## Key packages
- `engine/` - SQL plugin integration: `VeloxExecutionEngine` (orchestrates full pipeline), `VectorizedEngineExtension` (implements `ExecutionEngine` with `canVectorize()`)
- `plan/convert/` - Calcite → Velox expression/type/aggregate converters (reused by both paths)
- `plan/fragment/` - PlanFragment, FragmentProperties data classes + PlanFragmenter (current path)
- `plan/physical/` - Calcite Convention-based physical planning framework
  - Physical RelNode classes: `PhysicalTableScan`, `PhysicalFilter`, `PhysicalProject`, `PhysicalAggregate`, `PhysicalJoin`, `PhysicalSort`, `PhysicalExchange`
  - `PhysicalConvention` with `enforce()` for auto Exchange insertion
  - `PhysicalOptimizer` runs VolcanoPlanner with distribution traits
  - `VeloxPlanGenerator` converts physical plan → Velox PlanNodes + PlanFragments
  - `rules/` - ConverterRules (NONE→PHYSICAL), MPP rules, TwoStageAggRule
- `scheduler/` - Presto-inspired Stage/Task scheduler using OpenSearch ClusterState for shard routing. Also contains `CostEstimator` and `JoinStrategy` for MPP join selection.
- `transport/` - Inter-node communication: `ExecuteFragmentAction` for fragment dispatch, `ShuffleDataAction` for P2P shuffle data exchange, `ShuffleManager` for shuffle buffer management
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

## MPP join support
Three join strategies controlled by `plugins.velox.mpp_enabled` (default false):
- **Coordinator-centric** (mpp_enabled=false): Both sides gathered to coordinator, join runs locally
- **Broadcast** (mpp_enabled=true, small build side): Small table broadcast to all probe nodes
- **Hash shuffle** (mpp_enabled=true, both large): Both sides hash-partitioned by join key, shuffled P2P via `ShuffleDataAction`

Cost estimator uses shard count heuristic (`plugins.velox.broadcast_max_shards`, default 2). `plugins.velox.shuffle_partitions` controls partition count (0 = auto, uses number of data nodes).

### Join column name conflict
Velox validates that left and right output types have no duplicate names. The SQL plugin's `CalciteRelNodeVisitor.visitJoin()` adds a rename Project above the join (e.g. `dept_id0` → `d.dept_id`). The converter inserts a `ProjectNode` around the right side to rename conflicting columns. The scan keeps original names (for `LuceneArrowReader`), and ExternalStream maps by position, so the rename is transparent.

## Calcite physical planning framework (plan/physical/)
Uses Calcite's Convention + VolcanoPlanner, wired into `VeloxExecutionEngine.execute()`:
- `PhysicalConvention` — custom Convention with `enforce()` for auto Exchange insertion
- `PhysicalConvention.useAbstractConvertersForConversion()` returns `true` for distribution enforcement
- Physical nodes extend Calcite base classes (Filter, Project, etc.) and implement `PhysicalRel` marker interface
- `PhysicalTableScan` overrides `deriveRowType()` to preserve the SQL plugin's scan row type
- ConverterRules convert Convention.NONE → PhysicalConvention with explicit PhysicalExchange insertion
- MPP rules (MppAggregateRule, MppJoinRule) registered only when mpp_enabled=true
- `PhysicalOptimizer` creates a **new VolcanoPlanner + RelOptCluster** with `RelDistributionTraitDef`. Deep-copies the incoming plan via `ClusterCopyShuttle` to decouple from the SQL plugin's planner. Runs `FilterMergeRule` via HepPlanner before VolcanoPlanner.
- `VeloxPlanGenerator` walks the physical plan, splits at PhysicalExchange nodes, handles two-stage aggregation split (PARTIAL/FINAL with Velox-specific intermediate types)
- Reuses Calcite's built-in `RelDistribution` (SINGLETON, HASH_DISTRIBUTED, RANDOM_DISTRIBUTED, ANY)
- **Guava is compileOnly** in build.gradle — needed because Calcite base classes use `ImmutableList` in constructors

### ClusterCopyShuttle design
The SQL plugin's `CalciteLogicalIndexScan.register()` adds pushdown rules (`FilterIndexScanRule`, `AggregateIndexScanRule`, etc.) to whatever planner it's registered with. If the OLAP plugin reused the SQL plugin's planner, these rules would fire and fold operators into the scan — eliminating the LogicalAggregate/LogicalFilter that Velox needs.

Solution: `PhysicalOptimizer` creates its own `VolcanoPlanner` + `RelOptCluster` and deep-copies the plan into it. `ClusterCopyShuttle` converts `CalciteLogicalIndexScan` → plain `LogicalTableScan` (stripping PushDownContext), so the pushdown rules are never registered. All logical operators remain in the plan tree.

### Two-stage aggregation in VeloxPlanGenerator
The PARTIAL/FINAL split requires Velox-specific intermediate accumulator types (e.g., avg → ROW(DOUBLE, BIGINT)) that don't map to Calcite's type system. The `TwoStageAggRule` Calcite rule can't produce these types. Instead, `VeloxPlanGenerator.convertTwoStageAggregate()` detects `Aggregate(SINGLE) → Exchange` and splits it with proper Velox accumulator types.

## Per-query session creation
`VeloxLifecycleService.getSession()` creates a new `Session` per call (not a shared singleton). Each session gets its own memory pool namespace, preventing "Leaf child memory pool already exists" collisions between sequential queries. The `MemoryManager` is shared across sessions.

## Known issues / TODOs
- **OpenSearch doc values types**: OpenSearch stores all numeric types as `SORTED_NUMERIC` (not `NUMERIC`) and keyword/text as `SORTED_SET` (not `SORTED`). `LuceneArrowReader.mapToDocValueType()` handles this.
- **Arrow Text → String**: Arrow Utf8 vectors return `org.apache.arrow.vector.util.Text` objects. Must convert to `String` before passing to `ExprValueUtils.tupleValue()` in `VeloxExecutionEngine.readArrowIpcToExprValues()`.
- **Velox temp dirs in /tmp**: Each Velox initialization creates ~490MB temp dir under `/tmp`. Multiple restarts or multi-node clusters on the same host can fill `/tmp` (tmpfs). Clean with `rm -rf /tmp/opensearch-*`.
- **velox4j `PlanNode.getSources()` is protected**: Cannot traverse plan trees without reflection. A fix to make it `public` is pending in velox4j (branch `fix/unique-memory-pool-names`). Once merged, remove all reflection hacks in PlanFragmenter, VeloxExecutor, VeloxExecutionEngine, TransportExecuteFragmentAction.

## Graceful degradation
`VeloxLifecycleService` catches native library load failures and disables itself (logs a warning). This allows the plugin to install on unsupported platforms (e.g. macOS/aarch64) without crashing OpenSearch. `canVectorize()` returns `false` when Velox is unavailable.

## Installation
```bash
bin/opensearch-plugin install opensearch-sql
bin/opensearch-plugin install file:///path/to/opensearch-olap-3.6.0-SNAPSHOT.zip
```

## Design reference
RFC 4812: https://github.com/opensearch-project/sql/issues/4812
- Summary folder: `../RFC_4812`

## API reference repositories
All repositories are siblings under the same parent directory (`../`):
- SQL plugin: `../search-plugins-sql`
  - `ExecutionEngine` interface: `core/src/main/java/org/opensearch/sql/executor/ExecutionEngine.java`
  - `DelegatingExecutionEngine`: `core/src/main/java/org/opensearch/sql/executor/DelegatingExecutionEngine.java`
- OpenSearch core: `../OpenSearch`
- Calcite: `../calcite`
- Presto: `../presto` — scheduler design reference
- velox4j: `../velox4j` — Velox JNI bridge (C++ connector init in `src/main/cpp/main/velox4j/init/Init.cc`)
- Gluten-Flink: `../gluten` — reference for how Flink uses velox4j (different architecture: streaming, uses custom `StatefulPlanNode`, Flink handles shuffle)
