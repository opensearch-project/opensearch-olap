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
       - Injects CBO row counts via StatisticsTableScan (when available)
       - New cluster has RelDistributionTraitDef from start
    2. HepPlanner: FilterMergeRule, join reorder
       (JoinToMultiJoinRule → MultiJoinOptimizeBushyRule / LoptOptimizeJoinRule)
    3. VolcanoPlanner with ConverterRules:
       - Convention.NONE → PhysicalConvention
       - PhysicalAggregateRule inserts PhysicalExchange(SINGLETON) before agg
       - PhysicalJoinRule inserts PhysicalExchange(SINGLETON) for both join inputs
       - MPP rules: MppJoinRule/MppAggregateRule insert PhysicalExchange(HASH) (when mpp_enabled=true)
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
- `common/` - Shared utilities: `QueryId`
- `engine/` - SQL plugin integration: `VeloxExecutionEngine` (orchestrates full pipeline), `VectorizedEngineExtension` (implements `ExecutionEngine` with `canVectorize()`), `VeloxQueryResult`
- `plan/convert/` - Calcite → Velox expression/type/aggregate converters: `VeloxExprConverter`, `VeloxTypeConverter`, `VeloxAggConverter`, `PlanIdGenerator` (reused by VeloxPlanGenerator)
- `plan/fragment/` - PlanFragment, FragmentProperties data classes
- `plan/physical/` - Calcite Convention-based physical planning framework
  - Physical RelNode classes: `PhysicalTableScan`, `PhysicalFilter`, `PhysicalProject`, `PhysicalAggregate`, `PhysicalJoin`, `PhysicalSort`, `PhysicalExchange`, `PhysicalWindow`
  - `PhysicalConvention` with `enforce()` for auto Exchange insertion
  - `PhysicalOptimizer` runs VolcanoPlanner with distribution traits (contains `ClusterCopyShuttle`)
  - `StatisticsTableScan` — injects CBO row counts for join reorder
  - `VeloxPlanGenerator` converts physical plan → Velox PlanNodes + PlanFragments
  - `rules/` - ConverterRules (NONE→PHYSICAL), MPP rules, TwoStageAggRule, `PhysicalWindowRule`
- `scheduler/` - Presto-inspired Stage/Task scheduler using OpenSearch ClusterState for shard routing. Also contains `CostEstimator`, `JoinStrategy`, `ErrorClassifier` (error categorization for retry), `BadResourceTracker` (failed node/shard exclusion), `ShardRouter` (with replica failover), `StatisticsCollector`/`TableStatistics` (CBO stats)
- `transport/` - Inter-node communication: `ExecuteFragmentAction` for fragment dispatch, `ShuffleDataAction` for P2P shuffle data exchange, `ShuffleManager` for shuffle buffer management
- `execution/` - Lucene DocValues → Arrow → Velox C++ execution: `LuceneArrowReader`, `ArrowBatchBuilder`, `DocValueColumnReader`, `ExternalStreamBridge` (bridges Arrow batches to velox4j `ExternalStream.BlockingQueue`), `RuntimeFilterBuilder` (runtime filter pushdown), `LuceneFilterConverter` (Calcite filter → Lucene query), `VeloxLifecycleService`/`VeloxExecutor`
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

SPI service file: `META-INF/services/org.opensearch.sql.executor.ExecutionEngine` → `VectorizedEngineExtension`

## Dependencies
- OpenSearch 3.6.0-SNAPSHOT (compileOnly)
- opensearch-sql 3.6.0.0-SNAPSHOT (extended plugin — classloader sees SQL plugin classes)
  - `unified-query-core`, `unified-query-common`, `unified-query-opensearch` (compileOnly, transitive=false)
- Apache Calcite 1.41.0 (compileOnly — provided by SQL plugin at runtime)
- Apache Arrow 18.1.0 — `arrow-vector`, `arrow-memory-core`, `arrow-memory-unsafe`, `arrow-c-data`, `arrow-format`
- `com.google.flatbuffers:flatbuffers-java:24.3.25` — required by `arrow-c-data` at runtime (not pulled transitively)
- velox4j 0.1.0-SNAPSHOT — repackaged at build time to strip javax.annotation and org.slf4j (avoids jar hell with SQL plugin's jsr305)

## Coding
Use simple class name + import in coding as much as possible, except there is obvious existence of class naming conflicts.

## Build & Test
```bash
./gradlew build                    # Full build (compile + test + checks)
./gradlew assemble                 # Compile only (skip tests — useful when testingConventions needs at least one test class)
./gradlew test                     # Unit tests
./gradlew integTest                # Integration tests (requires running OpenSearch cluster)
./gradlew spotlessApply            # Auto-format code (always run after Java code changes)
./gradlew test --tests "*.VeloxExprConverterTests"  # Run a specific test class
```

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

1. **PhysicalOptimizer** converts `LogicalAggregate → LogicalTableScan` into `PhysicalAggregate → PhysicalExchange(SINGLETON) → PhysicalTableScan`
2. **VeloxPlanGenerator** detects `Aggregate(SINGLE) → Exchange` and splits into two fragments:
   - Leaf fragment: `AggregationNode(PARTIAL) → TableScanNode` — runs on data nodes
   - Root fragment: `AggregationNode(FINAL)` (sources wired during execution) — runs on coordinator
3. **Phase 1 (data nodes)**: `NodeResultCollector` dispatches leaf-stage tasks. Each data node runs PARTIAL aggregation via Velox C++. Results serialized with `BaseVectors.serializeToBuf()` (Velox native binary format, NOT Arrow IPC).
4. **Phase 2 (coordinator)**: `VeloxExecutionEngine.executeCoordinatorFragment()`:
   - Deserializes partial results via `BaseVectors.deserializeOneFromBuf()` — preserves intermediate accumulator types
   - Pushes deserialized RowVectors into a local `ExternalStream.BlockingQueue`
   - Dynamically wires a `TableScanNode(exchange_scan)` as the source of the FINAL `AggregationNode`
   - Executes the FINAL plan through Velox C++ locally on the coordinator
   - Converts final results to Arrow IPC for the SQL plugin response

Key implementation details:
- `TransportExecuteFragmentAction` detects PARTIAL plans via `planJson.contains("\"step\":\"PARTIAL\"")` to decide Arrow IPC vs native serde
- The feeder thread must start AFTER `serialTask.addSplit()` + `noMoreSplits()` to avoid race conditions

## MPP join support
Three join strategies controlled by `plugins.velox.mpp_enabled` (default false, **dynamic** — can be toggled at runtime via cluster settings API):
- **Coordinator-centric** (mpp_enabled=false): Both sides gathered to coordinator, join runs locally
- **Broadcast** (mpp_enabled=true, small build side): Small table broadcast to all probe nodes
- **Hash shuffle** (mpp_enabled=true, both large): Both sides hash-partitioned by join key, shuffled P2P via `ShuffleDataAction`

Cost estimator uses shard count heuristic (`plugins.velox.broadcast_max_shards`, default 2). `plugins.velox.shuffle_partitions` controls partition count (0 = auto, uses number of data nodes). All three settings are **dynamic** (`Setting.Property.Dynamic`).

### MPP rule design (MppJoinRule, MppAggregateRule)
MPP rules use the same explicit PhysicalExchange insertion pattern as the base rules:
1. Convert children to PhysicalConvention (convention conversion only)
2. Explicitly insert `PhysicalExchange.create(child, RelDistributions.hash(keys))`
3. The VolcanoPlanner explores both SINGLETON (base rules) and HASH (MPP rules) alternatives, picks the lower-cost plan

This avoids `CannotPlanException` — the VolcanoPlanner can't decompose cross-convention + cross-distribution conversion (NONE+ANY → PHYSICAL+HASH) in one step, so the exchange must be inserted explicitly rather than requested via traits.

`PhysicalExchange.computeSelfCost()` gives HASH_DISTRIBUTED 0.8x the cost of SINGLETON, so when `mpp_enabled=true` the planner prefers HASH exchanges for joins. The `CostEstimator` then decides BROADCAST vs HASH_SHUFFLE at execution time.

### MPP execution in VeloxExecutionEngine
`executeFragments()` uses `CostEstimator` when `mpp_enabled=true` and multiple leaf stages (join) to select between BROADCAST and HASH_SHUFFLE:
- **BROADCAST** (`executeBroadcastFragments`): Collects build side, converts to native serde, dispatches coordinator join plan to probe-side nodes with broadcast data. Uses `broadcastBuildScanIndex` to tell data nodes which exchange scan is build vs probe.
- **HASH_SHUFFLE** (`executeShuffleFragments`): Replaces COORDINATOR fragment with HASH_PARTITIONED for worker execution, assigns join sides to shuffle scan stages, dispatches via `dispatchAndCollectShuffle()` + `dispatchAndCollectShuffleJoin()`.

### VeloxPlanGenerator exchange handling
`handleExchange()` checks the exchange's own distribution (`exchange.getDistribution()`), not the input's:
- SINGLETON exchange → `FragmentProperties.source()` (gather to coordinator)
- HASH_DISTRIBUTED exchange → `FragmentProperties.shuffleScan()` (hash-partition by key channels)

### Join column name conflict
Velox validates that left and right output types have no duplicate names. The SQL plugin's `CalciteRelNodeVisitor.visitJoin()` adds a rename Project above the join (e.g. `dept_id0` → `d.dept_id`). The converter inserts a `ProjectNode` around the right side to rename conflicting columns. The scan keeps original names (for `LuceneArrowReader`), and ExternalStream maps by position, so the rename is transparent.

## Calcite physical planning framework (plan/physical/)
Uses Calcite's Convention + VolcanoPlanner, wired into `VeloxExecutionEngine.execute()`:
- `PhysicalConvention` — custom Convention with `enforce()` for auto Exchange insertion; `useAbstractConvertersForConversion()` returns `true` for distribution enforcement
- Physical nodes extend Calcite base classes (Filter, Project, etc.) and implement `PhysicalRel` marker interface
- `PhysicalTableScan` overrides `deriveRowType()` to preserve the SQL plugin's scan row type
- ConverterRules convert Convention.NONE → PhysicalConvention with explicit PhysicalExchange insertion
- MPP rules (MppAggregateRule, MppJoinRule) registered only when mpp_enabled=true
- Reuses Calcite's built-in `RelDistribution` (SINGLETON, HASH_DISTRIBUTED, RANDOM_DISTRIBUTED, ANY)
- **Guava is compileOnly** in build.gradle — needed because Calcite base classes use `ImmutableList` in constructors
- See subsections below for ClusterCopyShuttle, join reorder, and two-stage aggregation details

### ClusterCopyShuttle design
The SQL plugin's `CalciteLogicalIndexScan.register()` adds pushdown rules (`FilterIndexScanRule`, `AggregateIndexScanRule`, etc.) to whatever planner it's registered with. If the OLAP plugin reused the SQL plugin's planner, these rules would fire and fold operators into the scan — eliminating the LogicalAggregate/LogicalFilter that Velox needs.

Solution: `PhysicalOptimizer` creates its own `VolcanoPlanner` + `RelOptCluster` and deep-copies the plan into it. `ClusterCopyShuttle` converts `CalciteLogicalIndexScan` → plain `LogicalTableScan` (stripping PushDownContext), so the pushdown rules are never registered. All logical operators remain in the plan tree. HepPlanner then runs: `FilterMergeRule`, `PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW` (decomposes `LogicalProject(RexOver)` → `LogicalWindow` + `LogicalProject`), then join reorder rules. Finally the VolcanoPlanner runs with ConverterRules.

### Join reorder
The SQL plugin produces left-deep join trees preserving the user's PPL pipe order (no reordering). The HepPlanner phase in `PhysicalOptimizer` now runs Calcite's join reorder pipeline:
1. `JoinToMultiJoinRule` flattens the binary join tree into an N-ary `MultiJoin`
2. `MultiJoinOptimizeBushyRule` reorders using a greedy heuristic (pairs with largest row count difference joined first). Uses `RelMetadataQuery.getRowCount()` which is fed by `StatisticsTableScan` (CBO) or Calcite defaults.
3. `LoptOptimizeJoinRule` (fallback) handles cases the bushy rule refuses (outer joins present) — produces left-deep tree preserving outer join semantics.

`StatisticsTableScan` extends `TableScan` (not `LogicalTableScan` which is final) and overrides `estimateRowCount()` with CBO row counts. Created in `ClusterCopyShuttle.visit(TableScan)` when stats are available for the index. `PhysicalTableScanRule` matches on `TableScan.class`, so `StatisticsTableScan` is handled correctly by all existing rules.

For MPP: binary joins (2 tables) use BROADCAST/HASH_SHUFFLE as before. Multi-way joins (3+ tables) currently fall back to coordinator-centric execution. TODO: staged MPP execution for multi-way joins.

### Two-stage aggregation in VeloxPlanGenerator
The PARTIAL/FINAL split requires Velox-specific intermediate accumulator types (e.g., avg → ROW(DOUBLE, BIGINT)) that don't map to Calcite's type system. The `TwoStageAggRule` Calcite rule can't produce these types. Instead, `VeloxPlanGenerator.convertTwoStageAggregate()` detects `Aggregate(SINGLE) → Exchange` and splits it with proper Velox accumulator types.

## Per-query session creation
`VeloxLifecycleService.getSession()` creates a new `Session` per call (not a shared singleton). Each session gets its own memory pool namespace, preventing "Leaf child memory pool already exists" collisions between sequential queries. The `MemoryManager` is shared across sessions.

## Known issues / TODOs
- **OpenSearch doc values types**: OpenSearch stores all numeric types as `SORTED_NUMERIC` (not `NUMERIC`) and keyword/text as `SORTED_SET` (not `SORTED`). `LuceneArrowReader.mapToDocValueType()` handles this.
- **Arrow Text → String**: Arrow Utf8 vectors return `org.apache.arrow.vector.util.Text` objects. Must convert to `String` before passing to `ExprValueUtils.tupleValue()` in `VeloxExecutionEngine.readArrowIpcToExprValues()`.
- **Velox temp dirs in /tmp**: Each Velox initialization creates ~490MB temp dir under `/tmp`. Multiple restarts or multi-node clusters on the same host can fill `/tmp` (tmpfs). Clean with `rm -rf /tmp/opensearch-*`.

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
  - `PPL command documentation`: `docs/user/ppl/index.md`
- OpenSearch core: `../OpenSearch`
- Calcite: `../calcite`
- Presto: `../presto` — scheduler design reference
- velox4j: `../velox4j` — Velox JNI bridge (C++ connector init in `src/main/cpp/main/velox4j/init/Init.cc`)
- Gluten-Flink: `../gluten` — reference for how Flink uses velox4j (different architecture: streaming, uses custom `StatefulPlanNode`, Flink handles shuffle)
