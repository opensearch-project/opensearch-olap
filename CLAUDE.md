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
- OpenSearch 3.7.0-SNAPSHOT (compileOnly)
- opensearch-sql 3.7.0.0-SNAPSHOT (extended plugin — classloader sees SQL plugin classes)
  - `unified-query-core`, `unified-query-common`, `unified-query-opensearch` (compileOnly, transitive=false)
- Apache Calcite 1.41.0 (compileOnly — provided by SQL plugin at runtime)
- Apache Arrow 18.1.0 — `arrow-vector`, `arrow-memory-core`, `arrow-memory-unsafe`, `arrow-c-data`, `arrow-format`
- `com.google.flatbuffers:flatbuffers-java:24.3.25` — required by `arrow-c-data` at runtime (not pulled transitively)
- velox4j 0.1.0-SNAPSHOT — repackaged at build time to strip javax.annotation and org.slf4j (avoids jar hell with SQL plugin's jsr305)

## Coding
Use simple class name + import in coding as much as possible, except there is obvious existence of class naming conflicts.

When adding a variant to a public enum (e.g. `JoinStrategy`, `ErrorCategory`), grep for `.ordinal()` in tests — any `testOrdinalOrder`/`testEnumValues` assertions break deterministically and must be updated in the same PR.

## Build & Test
```bash
./gradlew build                    # Full build (compile + test + checks)
./gradlew assemble                 # Compile only (skip tests — useful when testingConventions needs at least one test class)
./gradlew test                     # Unit tests
./gradlew integTest                # Integration tests (requires running OpenSearch cluster)
./gradlew spotlessApply            # Auto-format code (always run after Java code changes)
./gradlew test --tests "*.VeloxExprConverterTests"  # Run a specific test class
./gradlew build -x integTest       # Fast pre-commit check (~45s): spotlessCheck + jacoco + unit tests, no cluster
```
Integration test logs are at `build/testclusters/integTest-0/logs/integTest.log`. `./gradlew integTest --rerun-tasks` wipes the log — capture it between runs if diffing behavior. Native JVM crashes leave `hs_err_pid*.log` in `build/testclusters/integTest-0/distro/.../logs/`.

**Byte-valued settings**: Use `Setting.byteSizeSetting(key, defaultBSV, minBSV, maxBSV, NodeScope, Dynamic)` with `ByteSizeValue`/`ByteSizeUnit`. The setter accepts `ByteSizeValue` and caches `.getBytes()` into a `volatile long`. See `PER_FRAGMENT_ARROW_BYTES` in `VeloxLifecycleService`.

**JDK 21 / JDK 25 compile classpath**: Project targets JDK 21 bytecode (`sourceCompatibility = VERSION_21`) but also compiles cleanly under JDK 25. JDK 25's javac is stricter about missing annotation class files — it fails hard where JDK 21 merely warned. Four `compileOnly` entries in `build.gradle` supply the annotations/helper classes referenced by Calcite/Arrow/SQL-plugin bytecode: `org.checkerframework:checker-qual`, `org.apiguardian:apiguardian-api`, `com.fasterxml.jackson.core:jackson-annotations`, `org.apache.calcite:calcite-linq4j`. If a new JDK version surfaces another `CompletionFailure: class file for X not found`, add that library as a `compileOnly` entry in the same block.

**Benchmark triage**: after an `integTest` run, classify PASS/SKIP/FAIL from a suite XML with
`python3 -c "c=open('build/test-results/integTest/TEST-<class>.xml').read(); print('cases=',c.count('<testcase'),'skip=',c.count('<skipped'),'fail=',c.count('<failure'))"`.

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
- **UDT types**: `ExprDateType`/`ExprTimeStampType`/`ExprTimeType` report `SqlTypeName.VARCHAR`. Check `instanceof AbstractExprRelDataType` and `getUdt()` in `VeloxTypeConverter` — `EXPR_TIMESTAMP → TimestampType`, `EXPR_DATE → DateType` (int32 days), `EXPR_TIME → BigIntType` (millis, no velox4j wrapper), `EXPR_IP`/`EXPR_BINARY → VarCharType`.
- **Datetime coercion UDFs**: SQL plugin's `CoercionUtils` wraps both sides of comparisons between a datetime UDT column and a string literal in matching coercion UDFs — `@timestamp >= '2023-01-01 00:00:00'` becomes `timestamp(@timestamp) >= timestamp('2023-01-01 00:00:00')`, same pattern for `date()` and `time()`. Velox has no `timestamp(varchar)`/`date(varchar)`/`time(varchar)` scalar, so `DateTimeUdfRewriter` strips these at plan time. `timestamp(<TIMESTAMP>)`/`date(<DATE>)`/`time(<TIME>)` are identity strips; cross-type wrappers (e.g. `timestamp(<DATE>)`) are real casts and fall back via `canVectorize()`.
- **Outer sort + system limit**: `QueryService.convertToCalcitePlan` wraps every plan in `LogicalSort(collation from input) → LogicalSystemLimit(fetch=query_size_limit)`. The outer Sort inherits the inner sort's collation; schema-reshaping operators below must rewrite/clear field-index references before this outer layer consumes them.
- **RexLiteral.getValueAs(Long.class) on DECIMAL returns the *unscaled* long** (`0.5` → `5`), silently losing scale. To detect "is this literal an integer?", check `getValueAs(BigDecimal.class).scale() <= 0` first, fall back to `getValueAs(Long.class)` only for non-DECIMAL numerics. See `VeloxExprConverter.readSpanCount` for the pattern.
- **PPL UDFs vs Calcite built-ins**: `EXTRACT` and `SPAN` are PPL's own UDFs (`SqlKind.OTHER_FUNCTION`, operator name `"EXTRACT"`/`"SPAN"`), not Calcite's `SqlStdOperatorTable.EXTRACT`/span. Detect by operator name, not SqlKind. `SpanUnit.getName()` returns short codes (`"m"`=minute, `"M"`=month — case-sensitive).

## velox4j integration details
- **ExternalStream connector ID**: The connector is registered in velox4j's C++ init as `"connector-external-stream"` (not `"external_stream"`). The `ExternalStreamTableHandle` and `ExternalStreamConnectorSplit` must use this exact ID.
- **Velox function names**: Presto-style full names for comparisons: `greaterthan`, `greaterthanorequal`, `lessthan`, `lessthanorequal`, `equalto` (not `gt`/`gte`/`lt`/`lte`/`eq`). **No generic `notequalto`** — Spark registers it only for DECIMAL. `VeloxExprConverter` rewrites `SqlKind.NOT_EQUALS` to `not(equalto(a, b))` with binary-type coercion.
- **Velox modulo is `mod`, not `modulus`**. No dialect registers `modulus`. `FUNCTION_MAP[SqlKind.MOD]` and `NAME_MAP["MOD"]`/`NAME_MAP["%"]` map to `"mod"`.
- **ExternalStream split wiring**: `SerialTask.addSplit(planNodeId, split)` takes the **plan node ID** of the `TableScanNode` (not the connector ID). The `ExternalStreamConnectorSplit` takes `(connectorId, queueId)` where `queueId` is `BlockingQueue.id()`.
- **Query construction**: A velox4j `Query` wraps `PlanNode` + `Config` + `ConnectorConfig`. The `ConnectorConfig` must register the connector ID with `ConnectorConfig.create(Map.of("connector-external-stream", Config.empty()))`. Plan serialization uses `Serde.toJson(query)` / `Serde.fromJson(json, Query.class)`.
- **Calcite DECIMAL literals**: Calcite represents integer literals (e.g. `30`) as `DECIMAL` type with scale 0. Velox requires exact type match in expressions, so `VeloxExprConverter` must produce `IntegerValue`/`BigIntValue` (not `DoubleValue`) for zero-scale decimals.
- **DATE/TIME constants**: velox4j has no `DateValue`/`TimeValue` variants. Use `ConstantTypedExpr.create(new DateType(), new IntegerValue(days))` and `ConstantTypedExpr.create(new BigIntType(), new BigIntValue(millis))`. The Velox return type is carried on `ConstantTypedExpr`, not derived from the variant — typing the constant as `IntegerType` breaks binding against a `DateType` column.
- **PARTIAL/FINAL aggregation exchange**: Arrow IPC does NOT preserve Velox's intermediate accumulator state (e.g., avg's `{sum, count}`). The coordinator exchange must use Velox native serialization (`BaseVectors.serializeToBuf/deserializeFromBuf`) — added to velox4j as a custom extension. Without this, the FINAL aggregation hangs because it can't interpret Arrow-deserialized data as valid intermediate state.
- **ExternalStream empty assignments**: `ExternalStreamConnector` enforces `columnHandles.empty()` in C++. `TableScanNode` for ExternalStream must have empty assignments list — the schema is defined solely by `outputType`.
- **BlockingQueue API is minimal**: `ExternalStreams.BlockingQueue` exposes only `put(RowVector)` and `noMoreInput()` — no `size()`, no drain callback. `put()` blocks only *after* the RowVector has already been allocated, so it's a poor fit for bounding Java-side Arrow memory. For byte-level backpressure use `BufferAllocator.getAllocatedMemory()` as the gauge — Velox takes ownership via `fromArrowVectorSchemaRoot()`, so Java-side live bytes drop as Velox consumes. See `execution/Backpressure.java`.
- **velox4j init order**: Presto first (`registerAllWindowFunctions()`), then Spark second with `overwrite=true` (`registerWindowFunctions("")`). For functions both dialects register, **Spark wins**: `row_number`/`rank`/`dense_rank` return INTEGER (not Presto's BIGINT). For scalar date-part functions (`year`, `month`, `minute`, …) Velox keeps BOTH physical signatures under one name — request Calcite's declared BIGINT and the resolver picks the right one (requesting INTEGER explicitly hits "Found incompatible return types").
- **Velox integer divide truncates toward zero; integer `floor` is a no-op**. For bucketing (span, width_bucket-like): naive `floor(col/w)*w` mis-buckets negatives — emit `col - ((col mod w + w) mod w)` (Euclidean modulo) to preserve `floor` semantics across signs while staying in integer space. Also preserves BIGINT precision above 2^53 that would be lost via DOUBLE.
- **No `TimeType` in velox4j Java bindings**. `ExprTimeType` maps to `BigIntType` (millis from midnight); can't emit `date_trunc(TIME)` from Java. Time spans fall back to default engine.
- **`date_trunc('week')` anchors to Monday**; Unix epoch is Thursday. Don't use `from_unixtime(floor(to_unixtime(ts)/604800)*604800)` as a multi-week substitute — buckets drift by 3 days.
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
Four join strategies controlled by `plugins.velox.mpp_enabled` (default false, **dynamic** — can be toggled at runtime via cluster settings API):
- **Coordinator-centric** (mpp_enabled=false): Both sides gathered to coordinator, join runs locally
- **Co-Routing** (mpp_enabled=true, pair registered in `co_routed_pairs`, matching shard counts, shards co-located): shard-local join, zero shuffle. Scheduler emits one task per aligned shard pair via `ShardRouter.routeCoRoutedPairs`. Falls back to BROADCAST/HASH_SHUFFLE if alignment fails.
- **Broadcast** (mpp_enabled=true, small build side): Small table broadcast to all probe nodes
- **Hash shuffle** (mpp_enabled=true, both large): Both sides hash-partitioned by join key, shuffled P2P via `ShuffleDataAction`

Cost estimator uses shard count heuristic (`plugins.velox.broadcast_max_shards`, default 2). `plugins.velox.shuffle_partitions` controls partition count (0 = auto, uses number of data nodes). All three settings are **dynamic** (`Setting.Property.Dynamic`).

**Reuse OpenSearch routing hash** for any routing-aware partitioning (Co-Routing, future `ES_ROUTING_SHUFFLE`): `org.opensearch.cluster.routing.Murmur3HashFunction` and `OperationRouting.generateShardId(IndexMetadata, id, routing)` are the authoritative hash. Do NOT reimplement — any drift silently corrupts shard alignment and produces wrong join results without erroring.

**Custom task placement for a new execution strategy**: build `TaskDescriptor`s directly (don't re-enter `QueryScheduler.schedule()` which assumes a Stage graph) and dispatch via a dedicated `NodeResultCollector.dispatchAndCollect*` overload taking a `BiConsumer<TaskDescriptor, ExecuteFragmentRequest>` that stamps strategy-specific request fields. See `executeCoRoutingFragments` + `dispatchAndCollectCoRouting` for the shape.

### Exchange insertion (pragmatic, not trait-driven in practice)
`PhysicalConvention.enforce()` + `AbstractConverter.ExpandConversionRule` are wired up, but enforce does **not** reliably fire at scan→parent boundaries in this codebase: `PhysicalTableScan` declares distribution=`ANY`, which satisfies every other distribution, so enforce never sees a mismatch at that boundary. As a result, any operator that needs a specific exchange boundary (for fragment splits, two-stage aggregation, hash shuffle joins) inserts `PhysicalExchange.create()` explicitly: `PhysicalJoinRule` (SINGLETON), `MppJoinRule` (HASH per side), `MppAggregateRule` (HASH on group keys). `PhysicalAggregateRule` and `PhysicalSortRule` declare SINGLETON on their output but do NOT insert exchanges — they work today because typical plans resolve to single-node execution, not because enforce is doing anything. `PlanExplainTests.testAggregationProducesTwoStageFragments` explicitly notes that enforce "may or may not fire in unit test VolcanoPlanner"; the end-to-end two-stage split behavior is exercised by `AggregationIT`. `PhysicalExchange.computeSelfCost()` gives HASH 0.8x SINGLETON so when `mpp_enabled=true` the planner prefers HASH alternatives for joins and aggregations.

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
Velox validates that left and right output types have no duplicate names. When the two join inputs share a column (e.g. both have `dept_id`), Calcite's `SqlValidatorUtil.EXPR_SUGGESTER` uniquifies by appending an attempt counter to the original name (`dept_id` → `dept_id0`, `sku2` → `sku20`). `VeloxPlanGenerator.convertJoin` mirrors this by wrapping the **right** input in a `ProjectNode` that maps renamed → raw (`names=[dept_id0, dept_name], projections=[FieldAccess("dept_id"), FieldAccess("dept_name")]`). The downstream SQL plugin's final projection then renames `dept_id0` to `d.dept_id` for user display. Scans keep original names (needed by `LuceneArrowReader`), and ExternalStream maps by position.

**Resolving a join-key name back to the raw scan column — use `VeloxExecutionEngine.resolveRawBuildKeyName(joinNode, isBuildLeft, keyName)`.** When any code path needs to map a HashJoin's key name (which may be the Calcite-uniquified form) back to the raw column name in the build-side scan schema — e.g. runtime-filter bloom build, co-routing pair eligibility — **always** use this helper, never a regex strip. The helper walks the plan: if the build-side child is the rename `ProjectNode`, it reverses the `names[i] → projections[i].fieldName` mapping; otherwise it returns the key unchanged. Regex stripping (`\\d+$`) silently corrupts legitimate field names that end in digits (`sku2`, `year2024`) — the uniquifier's counter is indistinguishable from real trailing digits without the schema. Call sites today: `executeBroadcastFragments` (two-stage bloom), `extractRuntimeFilter` (single-stage), `selectJoinStrategy` (co-routing eligibility). See `VeloxExecutionEngineTests.testResolveRawBuildKeyName_realFieldEndingInDigits` for the regression-guard test.

**When adding a new PPL integration test that exercises joins** — especially with aggregation on the join output, runtime filters, or co-routing — prefer a join key whose column name does not end in a digit (`dept_id`, `customer_id`). If a new test must use a digit-ending name (`sku2`, `year2024`, `id2`), confirm both sides' bloom/RF/matching behavior end-to-end and add a unit test in `VeloxExecutionEngineTests` that pins the resolver's handling of that specific shape. The integration symptom of a broken resolver is "join returns zero rows despite matching data" — caused by an empty bloom filtering out all probe rows. Check the data-node log for `PARTIAL bloom built: field=<name>, inserted=0` when triaging.

## Calcite physical planning framework (plan/physical/)
Uses Calcite's Convention + VolcanoPlanner, wired into `VeloxExecutionEngine.execute()`:
- `PhysicalConvention` — custom Convention with `enforce()` for auto Exchange insertion; `useAbstractConvertersForConversion()` returns `true` for distribution enforcement
- Physical nodes extend Calcite base classes (Filter, Project, etc.) and implement `PhysicalRel` marker interface
- `PhysicalTableScan` overrides `deriveRowType()` to preserve the SQL plugin's scan row type
- ConverterRules convert Convention.NONE → PhysicalConvention. Any rule needing a concrete exchange boundary (joins, MPP aggregate) inserts `PhysicalExchange.create()` explicitly — `Convention.enforce()` is wired up but does not fire at scan→parent boundaries because PhysicalTableScan's distribution is ANY. See "Exchange insertion" subsection below.
- MPP rules (MppAggregateRule, MppJoinRule) registered only when mpp_enabled=true
- Reuses Calcite's built-in `RelDistribution` (SINGLETON, HASH_DISTRIBUTED, RANDOM_DISTRIBUTED, ANY)
- **Guava is compileOnly** in build.gradle — needed because Calcite base classes use `ImmutableList` in constructors
- See subsections below for ClusterCopyShuttle, join reorder, and two-stage aggregation details

### ClusterCopyShuttle design
The SQL plugin's `CalciteLogicalIndexScan.register()` adds pushdown rules (`FilterIndexScanRule`, `AggregateIndexScanRule`, etc.) to whatever planner it's registered with. If the OLAP plugin reused the SQL plugin's planner, these rules would fire and fold operators into the scan — eliminating the LogicalAggregate/LogicalFilter that Velox needs.

Solution: `PhysicalOptimizer` creates its own `VolcanoPlanner` + `RelOptCluster` and deep-copies the plan into it. `ClusterCopyShuttle` converts `CalciteLogicalIndexScan` → plain `LogicalTableScan` (stripping PushDownContext), so the pushdown rules are never registered. All logical operators remain in the plan tree. HepPlanner then runs: `FilterMergeRule`, `PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW` (decomposes `LogicalProject(RexOver)` → `LogicalWindow` + `LogicalProject`), then join reorder rules. Finally the VolcanoPlanner runs with ConverterRules.

**Hint cluster-copy trap**: the SQL plugin registers `AGG_ARGS` hint strategies on its own `RelOptCluster.HintStrategyTable` (for `bucket_nullable=false` and nested-agg bookkeeping). `ClusterCopyShuttle` creates a fresh cluster with an empty strategy table; when any Calcite rule inspects hints, `HintStrategyTable.canApply` asserts `"hint <name> must be present"`. Fix: strip hints in `visit(LogicalAggregate)` by passing `List.of()` to `LogicalAggregate.create`. Semantic effects (isNotNull filter) are already materialized upstream before we copy, so stripping is safe.

### Join reorder
The SQL plugin produces left-deep join trees preserving the user's PPL pipe order (no reordering). The HepPlanner phase in `PhysicalOptimizer` now runs Calcite's join reorder pipeline:
1. `JoinToMultiJoinRule` flattens the binary join tree into an N-ary `MultiJoin`
2. `MultiJoinOptimizeBushyRule` reorders using a greedy heuristic (pairs with largest row count difference joined first). Uses `RelMetadataQuery.getRowCount()` which is fed by `StatisticsTableScan` (CBO) or Calcite defaults.
3. `LoptOptimizeJoinRule` (fallback) handles cases the bushy rule refuses (outer joins present) — produces left-deep tree preserving outer join semantics.

`StatisticsTableScan` extends `TableScan` (not `LogicalTableScan` which is final) and overrides `estimateRowCount()` with CBO row counts. Created in `ClusterCopyShuttle.visit(TableScan)` when stats are available for the index. `PhysicalTableScanRule` matches on `TableScan.class`, so `StatisticsTableScan` is handled correctly by all existing rules.

For MPP: binary joins (2 tables) use BROADCAST/HASH_SHUFFLE as before. Multi-way joins (3+ tables) currently fall back to coordinator-centric execution. TODO: staged MPP execution for multi-way joins.

### Two-stage aggregation in VeloxPlanGenerator
The PARTIAL/FINAL split requires Velox-specific intermediate accumulator types (e.g., avg → ROW(DOUBLE, BIGINT)) that don't map to Calcite's type system. The `TwoStageAggRule` Calcite rule can't produce these types. Instead, `VeloxPlanGenerator.convertTwoStageAggregate()` detects `Aggregate(SINGLE) → Exchange` and splits it with proper Velox accumulator types.

### MAP parent / object field handling
OpenSearch `object` fields appear as `MAP<VARCHAR, ANY>` in Calcite, with flat dot-path children as sibling fields (e.g., `cloud: MAP` + `cloud.region: VARCHAR`). Three-way invariant:
1. **Scan** (`convertTableScan`): drops MAP parents from Velox output (veloxIndex=-1), keeps flat dot-path children as top-level columns.
2. **Project** (`convertProject`): when a MAP parent is projected, **expand** into one FieldAccess per flat child already in scan output (e.g., projecting `metrics` emits `metrics.size`, `metrics.tmin`). De-dupe against other flat projections in the same Project.
3. **Java result** (`reconstructStructs` in `VeloxExecutionEngine`): reassembles flat dot-path columns back into nested maps for the final response.

Dropping a MAP parent ref anywhere else (filter/sort/outer Project) breaks output width — operators above index Calcite fields by position and will reference non-existent columns.

`ANY` top-level fields (`match_only_text`, empty objects, unresolved fields) are rejected by `canVectorize()` — not readable from doc values. Only MAP is treated as an object parent.

Metadata columns (`_id`, `_index`, `_score`, `_maxscore`, `_sort`, `_routing`) must be skipped in scan with veloxIndex=-1 mappings so projections referencing them are dropped.

### Field mapping scope in VeloxPlanGenerator
`currentFieldMappings` / `currentVeloxOutputType` track Calcite→Velox field index remapping. They are **scoped to the scan subtree** and must be rebuilt/cleared when schema changes:
- **Project** rebuilds mappings to describe its output (including `-1` for expanded MAP parents).
- **Aggregate / Join / Window** clear mappings (new schema, no mapping needed above — use name-based resolution).
- **Filter / Sort** pass through (same schema as input).
- **Exchange boundary** (`handleExchange`) clears mappings — parent fragment has a different schema.
- `buildOrderByNode` field-count guard: mappings are only valid when `inputRowType.getFieldCount() == currentFieldMappings.length`.

## Per-query session creation
`VeloxLifecycleService.getSession()` creates a new `Session` per call (not a shared singleton). Each session gets its own memory pool namespace, preventing "Leaf child memory pool already exists" collisions between sequential queries. The `MemoryManager` is shared across sessions.

## Runtime filter: TERMS, BLOOM, two-stage BLOOM
`VeloxExecutionEngine.extractRuntimeFilter()` picks an `RfKind` by cardinality ladder:
- `≤ runtime_filter_max_cardinality` → TERMS (Lucene TermInSetQuery/PointInSetQuery via `RuntimeFilterBuilder`)
- `(terms-cap, runtime_filter_bloom_max_cardinality]` → BLOOM (`OlapBloomFilter`, per-doc predicate in `LuceneArrowReader`)
- otherwise → NONE (abandon)

BLOOM has two build modes controlled by `plugins.velox.runtime_filter_bloom_two_stage` (default true):
- **Two-stage** (default): each data node builds a PARTIAL bloom over its local shards during the build fragment (`VeloxExecutor.executeWithPartialBloom` writes Arrow IPC AND builds bloom in one iteration pass); coordinator merges via bitwise-OR (`OlapBloomFilter.merge` / `mergeInPlace`). The coordinator passes `runtime_filter_bloom_max_cardinality` as `expectedInsertions` to all nodes — **identical sizing across partials is the bit-alignment invariant**. Dispatched via `NodeResultCollector.dispatchAndCollectWithPartialBloom`; responses carry `partialBloomBytes`.
- **Single-stage**: coordinator rebuilds bloom from broadcast build rows (fallback path; also the v1 implementation).

Wire format additions on `ExecuteFragmentRequest`: `buildBloomFieldName/Type/ExpectedInsertions` trailer. On `ExecuteFragmentResponse`: length-prefixed `partialBloomBytes` trailer (zero-length = none).

BLOOM is pushed down as a Lucene `BloomFilterQuery` (via `RuntimeFilterBuilder.buildBloom`), same layer as TERMS. The Query's Weight/Scorer walks doc values for the RF field and matches docs passing `bloom.mightContain`. **Missing-field semantics**: a segment without the RF field (or a doc without a value) yields zero matches — stricter than the earlier feeder-layer predicate, which passed missing docs through. Correctness is preserved because the hash-join above re-verifies keys: a doc without the join key cannot match the join anyway.

## PPL profile integration
The plugin participates in the SQL plugin's PPL `{"profile":true}` flow (see `../search-plugins-sql/docs/user/ppl/interfaces/endpoint.md` § Profile). Flow:

1. Coordinator checks `QueryProfiling.current().isEnabled()` in `VeloxExecutionEngine.execute()`.
2. If enabled, `NodeResultCollector.setProfileEnabled(true)` stamps every outgoing `ExecuteFragmentRequest` with `profileEnabled=true`.
3. On each data node, `TransportExecuteFragmentAction` threads a `LuceneArrowReader.ScanStats` counter through the feeder, and stamps the response with an `OlapTaskProfile` carrying `{fragmentId, partitionId, nodeId, durationNanos, docsRead, docsMatched, rowsEmitted, rfKind, rfBloomBytes}`. `docsRead` counts every live doc the Lucene scorer iterated over; `docsMatched` counts those emitted into the Arrow bridge — their delta is the observable effect of the runtime filter at the Lucene level.
4. Coordinator accumulates every `ExecuteFragmentResponse` in a thread-local list (`VeloxExecutionEngine.profileAccumulator`), hands them to `OlapProfileAssembler.buildPlan` which produces a `ProfilePlanNode` tree, and calls `QueryProfiling.current().setPlanRoot(root)`.
5. SQL plugin's `SimpleJsonResponseFormatter` snapshots the profile and emits it as `profile.plan` in the PPL response.

**User verification** for "did Lucene BLOOM narrow the scan?": run the same query twice with `profile=true`, once with `runtime_filter_enabled=false` (baseline) and once forced to BLOOM via low TERMS cap. Compare `docsMatched` across the leaf-task nodes — BLOOM should narrow. See `ProfileBloomNarrowsScanIT` for the encoded assertion.

Wire format: `ExecuteFragmentRequest.profileEnabled` (boolean trailer), `ExecuteFragmentResponse.taskProfile` (optional `OlapTaskProfile` block, tagged by leading bool).

**Wire format evolution**: Plugin ships with the OpenSearch release, so both peers always speak the same `WIRE_VERSION`. Bump the constant on any layout change. **Do NOT add `StreamOutput.getVersion().onOrAfter(...)` gates** — they're meaningful for OpenSearch core but dead code here (every peer is on the same plugin build at the same time). Cross-version decode branches are not needed.

## Backpressure / flow control
All Arrow allocators are bounded (no more `RootAllocator(Long.MAX_VALUE)`). Bounds come from `plugins.velox.*_bytes` settings on `VeloxLifecycleService`; the gauge at every boundary is `BufferAllocator.getAllocatedMemory()`. Producers call `Backpressure.awaitBelow(allocator, softWatermark, timeoutNanos, scanStats.backpressureWaitNanos)` before allocating the next Arrow batch — `LuceneArrowReader` does this around every `bridge.feedBatch()`, `VeloxExecutionEngine.feedArrowIpcToQueue()` does it around every `queue.put()`.

Failure semantics:
- Arrow allocator refuses → `ExternalStreamBridge.feedBatch` rethrows as `BackpressureTimeoutException` → `RETRYABLE_TRANSIENT`
- `max_result_bytes` or `coordinator_inflight_bytes` exceeded → `ResultTooLargeException` → new `RESOURCE_EXCEEDED` category, non-retryable
- Shuffle per-partition cap exceeded → `ShuffleBuffer.tryAddData` rejects; response carries `backpressure=true`; sender exponentially backs off (5 retries: 100ms → 3.2s) then fails. Transport handlers must NOT block — always reject with a retry signal.

## Known issues / TODOs
- **OpenSearch doc values types**: OpenSearch stores all numeric types as `SORTED_NUMERIC` (not `NUMERIC`) and keyword/text as `SORTED_SET` (not `SORTED`). `LuceneArrowReader.mapToDocValueType()` handles this.
- **Arrow Text → String**: Arrow Utf8 vectors return `org.apache.arrow.vector.util.Text` objects. Must convert to `String` before passing to `ExprValueUtils.tupleValue()` in `VeloxExecutionEngine.readArrowIpcToExprValues()`.
- **Velox temp dirs in /tmp**: Each Velox initialization creates ~490MB temp dir under `/tmp`. Multiple restarts or multi-node clusters on the same host can fill `/tmp` (tmpfs). Clean with `rm -rf /tmp/opensearch-*`.
- **Big5IT @Ignore'd tests**: unsupported PPL lowerings — `map(VARCHAR,VARCHAR)` helper from PPL match(), `width_bucket`/`||`/`rex_extract`/`to_unixtime(BIGINT)` not registered in Velox, `coalesce` with mixed VARCHAR/BIGINT operands. `row_number()` / `hint AGG_ARGS` / `!=` / EXTRACT / SPAN were fixed and re-enabled. Each remaining test's `@Ignore` reason documents the specific missing lowering.
- **ClickBench: 43/43 passing.** `PPLClickBenchIT` migrated to `ClickBenchIT` under `integTest`.

## Graceful degradation
`VeloxLifecycleService` catches native library load failures and disables itself (logs a warning). This allows the plugin to install on unsupported platforms (e.g. macOS/aarch64) without crashing OpenSearch. `canVectorize()` returns `false` when Velox is unavailable.

## Installation
```bash
bin/opensearch-plugin install opensearch-sql
bin/opensearch-plugin install file:///path/to/opensearch-olap-3.7.0-SNAPSHOT.zip
```

## Design reference
RFC 4812: https://github.com/opensearch-project/sql/issues/4812
- Summary folder: `../RFC_4812`

Plugin docs (under `docs/`):
- `architecture.md` — internal design, MPP, fragment dispatch, backpressure internals, reference (project structure, operator conversion tables, dependencies, SQL-plugin changes).
- `configuration.md` — full settings reference.
- `running.md` — single-node / multi-node / Docker walkthroughs.
- `velox-function-support.md` — PPL → Velox function coverage matrix.

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
