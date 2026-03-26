# OpenSearch OLAP Plugin

An OpenSearch plugin that integrates [Apache Velox](https://velox-lib.io/) as a vectorized execution engine for analytical (OLAP) queries. Instead of OpenSearch's row-oriented query-then-fetch pipeline, this plugin reads columnar data from Lucene doc values, converts it to Apache Arrow format, and executes queries through Velox's C++ vectorized engine via [velox4j](https://github.com/velox4j/velox4j).

## Co-work with SQL Plugin

This plugin is designed to **co-work with the OpenSearch SQL plugin** (`opensearch-sql`), not replace it. The SQL plugin handles SQL/PPL parsing and Calcite RelNode generation. The OLAP plugin provides an alternative execution engine that takes the RelNode and executes it through Velox.

```
[SQL Plugin]                                    [OLAP Plugin]

SQL/PPL Query
    │
    ▼
Calcite Analyzer
    │
    ▼
RelNode (Logical Plan)
    │
    ▼
DelegatingExecutionEngine
    │
    ├── canVectorize() = true ──► OlapExecutionExtensionImpl ──► VeloxExecutionEngine
    │                                                                │
    │                                                         VeloxPlanConverter
    │                                                         (RelNode → PlanNode)
    │                                                                │
    │                                                         PlanFragmenter
    │                                                         (split for distribution)
    │                                                                │
    │                                                         QueryScheduler
    │                                                         (route by shard)
    │                                                                │
    │                                                   ┌────────────┼────────────┐
    │                                                   ▼            ▼            ▼
    │                                                Data Node    Data Node    Data Node
    │                                                Lucene →     Lucene →     Lucene →
    │                                                Arrow →      Arrow →      Arrow →
    │                                                Velox C++    Velox C++    Velox C++
    │
    └── canVectorize() = false ─► OpenSearchExecutionEngine (default row-oriented)
```

### Integration via ExecutionEngine

The SQL plugin discovers the OLAP engine using OpenSearch's `ExtensiblePlugin` mechanism:

1. The OLAP plugin declares `extended.plugins=opensearch-sql` in its plugin descriptor
2. The OLAP plugin implements `ExecutionEngine` and registers via SPI (`META-INF/services/org.opensearch.sql.executor.ExecutionEngine`)
3. The SQL plugin implements `ExtensiblePlugin` and loads `ExecutionEngine` extensions in `loadExtensions()`
4. Extensions are wrapped with the default engine in a `DelegatingExecutionEngine`
5. When a query arrives, `DelegatingExecutionEngine` calls `canVectorize(RelNode plan)` on each extension
6. If an extension returns `true`, execution is delegated to it; otherwise, the default engine is used

This design keeps `QueryService` unchanged — it just calls `executionEngine.execute()`.

## Architecture

```
                          +-----------------------+
                          |     Coordinator Node  |
                          |                       |
  SQL Query ──> SQL Plugin ──> Calcite Analyzer  |
                          |        │              |
                          | DelegatingExecutionEngine
                          |        │              |
                          | OlapExecutionExtensionImpl
                          |        │              |
                          | VeloxPlanConverter    |
                          |  (RelNode → PlanNode) |
                          |        │              |
                          |  PlanFragmenter       |
                          |  (split at exchanges) |
                          |        │              |
                          |  QueryScheduler       |
                          |  (route by shard)     |
                          +--------│──────────────+
                     ┌─────────────┼─────────────┐
                     │             │             │
              ┌──────▼──────┐ ┌───▼──────┐ ┌───▼──────┐
              │  Data Node  │ │Data Node │ │Data Node │
              │             │ │          │ │          │
              │ Lucene      │ │          │ │          │
              │  DocValues  │ │  ...     │ │  ...     │
              │    │        │ │          │ │          │
              │ Arrow       │ │          │ │          │
              │  Batches    │ │          │ │          │
              │    │        │ │          │ │          │
              │ ExternalStream          │ │          │
              │  BlockingQueue          │ │          │
              │    │        │ │          │ │          │
              │ Velox C++   │ │          │ │          │
              │  Execution  │ │          │ │          │
              └─────────────┘ └──────────┘ └──────────┘
```

### Query Execution Flow

1. **SQL Parsing** (SQL plugin) - SQL/PPL is parsed and analyzed into a Calcite RelNode tree
2. **Routing** - `DelegatingExecutionEngine` calls `canVectorize()` on the OLAP extension
3. **Plan Conversion** - `VeloxPlanConverter` translates Calcite RelNodes to velox4j PlanNodes
4. **Fragmentation** - `PlanFragmenter` splits the plan at exchange boundaries (e.g., PARTIAL agg on data nodes, FINAL agg on coordinator)
5. **Scheduling** - `QueryScheduler` uses `ClusterState` routing table to assign fragments to data nodes owning the target shards
6. **Transport** - Coordinator dispatches `ExecuteFragmentRequest` to data nodes via OpenSearch `TransportService`
7. **Data Node Execution**:
   - Acquire Lucene searcher via `IndexShard.acquireSearcher()`
   - Read doc values column-by-column into Arrow `VectorSchemaRoot` batches
   - Feed Arrow batches into velox4j `ExternalStream.BlockingQueue` (zero-copy via Arrow C Data Interface)
   - Velox `TableScanNode` reads from the queue and executes the plan fragment natively in C++
8. **Result Collection** - Coordinator merges partial results from all data nodes into the final response

### Key Design Decisions

- **Co-work, not duplicate**: The SQL plugin handles SQL/PPL → Calcite. The OLAP plugin only handles Calcite → Velox → execution.
- **`canVectorize(RelNode)` on `ExecutionEngine`**: A default method returning `false`. Extensions override it to advertise support for specific plan shapes. The `DelegatingExecutionEngine` routes based on this.
- **Doc values over stored fields**: Doc values are columnar on disk, matching Arrow/Velox's columnar layout. Sequential iteration per segment is ideal for full-scan analytics.
- **ExternalStream bridge**: velox4j's `BlockingQueue` eliminates the need for a custom C++ OpenSearch connector in Velox. Java pushes data, C++ pulls it.
- **No core changes**: The plugin uses only public OpenSearch APIs (`Plugin`, `ActionPlugin`, `TransportService`, `IndicesService`, `IndexShard.acquireSearcher()`).
- **Presto-inspired scheduler**: Stage/Task hierarchy and fragment dispatch modeled after Presto's `SqlStageExecution` and `RemoteTaskFactory`, adapted for OpenSearch's transport layer.
- **Graceful degradation**: If Velox native libraries are unavailable (e.g. macOS/aarch64), the plugin logs a warning and disables itself. `canVectorize()` returns `false`, all queries fall back to the default engine.

## Project Structure

```
src/main/java/org/opensearch/plugin/olap/
├── OlapPlugin.java                    # Plugin entry point
├── common/
│   └── QueryId.java                   # Query identifier
├── engine/                            # SQL plugin integration
│   ├── VeloxExecutionEngine.java      #   RelNode → Velox pipeline orchestration
│   └── OlapExecutionExtensionImpl.java#   ExecutionEngine impl (canVectorize + execute)
├── plan/
│   ├── convert/                       # Calcite → Velox plan conversion
│   │   ├── VeloxPlanConverter.java    #   RelNode → PlanNode tree
│   │   ├── VeloxExprConverter.java    #   RexNode → TypedExpr
│   │   ├── VeloxTypeConverter.java    #   RelDataType → velox4j Type
│   │   ├── VeloxAggConverter.java     #   AggregateCall → Aggregate
│   │   └── PlanIdGenerator.java       #   Unique plan node IDs
│   └── fragment/                      # Distributed plan fragmentation
│       ├── PlanFragmenter.java        #   Split at exchange boundaries
│       ├── PlanFragment.java          #   Fragment with plan subtree
│       └── FragmentProperties.java    #   Distribution metadata
├── scheduler/                         # Query scheduling
│   ├── QueryScheduler.java            #   Orchestrates distributed execution
│   ├── QueryExecution.java            #   Single query lifecycle
│   ├── Stage.java                     #   Execution stage (1 fragment → N tasks)
│   ├── ShardRouter.java               #   Routes to nodes by shard assignment
│   ├── TaskDescriptor.java            #   Task metadata for a node
│   ├── TaskTracker.java               #   Tracks task states
│   ├── StageId.java / TaskId.java     #   Identifiers
│   ├── TaskState.java                 #   Task lifecycle enum
│   └── ExecutionPolicy.java           #   ALL_AT_ONCE vs PHASED
├── transport/                         # Inter-node communication
│   ├── ExecuteFragmentAction.java     #   Action type definition
│   ├── ExecuteFragmentRequest.java    #   Plan + shard IDs (serializable)
│   ├── ExecuteFragmentResponse.java   #   Status + Arrow IPC results
│   ├── TransportExecuteFragmentAction.java  # Data node handler
│   └── NodeResultCollector.java       #   Fan-out + collect results
├── execution/                         # Lucene → Arrow → Velox pipeline
│   ├── LuceneArrowReader.java         #   Reads shards into Arrow batches
│   ├── DocValueColumnReader.java      #   Single column from DocValues
│   ├── ArrowBatchBuilder.java         #   Builds VectorSchemaRoot
│   ├── ExternalStreamBridge.java      #   Arrow → velox4j BlockingQueue
│   ├── VeloxExecutor.java             #   Executes plan via velox4j
│   └── VeloxLifecycleService.java     #   Velox engine init/shutdown
└── result/                            # Result handling (interfaces)
    ├── QueryResult.java               #   Query result interface
    └── ResultCollector.java           #   Merge partial results
```

## Supported Operators

### Plan Node Conversion (Calcite → Velox)

| Calcite RelNode | Velox PlanNode |
|----------------|---------------|
| `TableScan` (`LogicalTableScan`, `CalciteLogicalIndexScan`) | `TableScanNode` + `ExternalStreamTableHandle` |
| `LogicalFilter` | `FilterNode` |
| `LogicalProject` | `ProjectNode` |
| `LogicalAggregate` | `AggregationNode` (SINGLE / PARTIAL+FINAL) |
| `LogicalJoin` | `HashJoinNode` (equi-join key extraction) |
| `Sort` (`LogicalSort`, `LogicalSystemLimit`) | `OrderByNode` + `LimitNode` |

### Expression Conversion (RexNode → TypedExpr)

| Calcite Expression | Velox Expression |
|-------------------|-----------------|
| `RexInputRef` | `FieldAccessTypedExpr` |
| `RexLiteral` | `ConstantTypedExpr` (BooleanValue, IntegerValue, BigIntValue, DoubleValue, VarCharValue) |
| `RexCall` (=, !=, <, >, AND, OR, +, -, *, /) | `CallTypedExpr` (equalto, notequalto, lessthan, greaterthan, and, or, plus, minus, multiply, divide) |
| `CAST` | `CastTypedExpr` |
| `IS NULL` / `IS NOT NULL` | `CallTypedExpr` (is_null / not(is_null)) |
| `IN` | Chain of eq + or |
| `BETWEEN` | gte + lte + and |

### Aggregate Functions

COUNT, SUM, AVG, MIN, MAX (with DISTINCT support)

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `plugins.velox.enabled` | `true` | Enable/disable the OLAP plugin |
| `plugins.velox.memory_limit_bytes` | `4294967296` (4 GB) | Velox engine memory limit |
| `plugins.velox.num_threads` | `4` | Velox execution threads |

## Dependencies

| Dependency | Version | Scope | Purpose |
|-----------|---------|-------|---------|
| OpenSearch | 3.6.0 | compileOnly | Host platform |
| opensearch-sql | 3.6.0.0 | compileOnly + runtime (extended plugin) | SQL/PPL parsing, `ExecutionEngine` interface, Calcite |
| Apache Calcite | 1.41.0 | compileOnly | Plan conversion — provided by SQL plugin at runtime |
| Apache Arrow | 18.1.0 | implementation | `arrow-vector`, `arrow-memory-core`, `arrow-c-data`, `arrow-format` + `arrow-memory-unsafe` (runtime) |
| flatbuffers-java | 24.3.25 | implementation | Required by `arrow-c-data` (not pulled transitively) |
| velox4j | 0.1.0 | compileOnly + repackaged runtime | JNI bridge to Velox C++ engine |

### Jar Hell Avoidance

The OLAP plugin extends the SQL plugin's classloader (`extendedPlugins = ['opensearch-sql']`). To avoid duplicate class errors:

- **Calcite** is `compileOnly` — already bundled by the SQL plugin
- **velox4j** is repackaged at build time (`repackageVelox4j` task) to strip `javax.annotation` and `org.slf4j` classes that conflict with the SQL plugin's `jsr305` jar
- **jsr305** is excluded globally via `configurations.all`
- **Arrow memory**: Uses `arrow-memory-unsafe` instead of `arrow-memory-netty` — OpenSearch's Netty is in the system classloader and invisible to plugin classloaders

## Building

```bash
# Full build (requires at least one test class)
./gradlew build

# Assemble only (skip tests)
./gradlew assemble
```

The plugin ZIP will be generated at `build/distributions/opensearch-olap-3.6.0-SNAPSHOT.zip`.

## Installation

The OLAP plugin requires the SQL plugin to be installed first:

```bash
# Install SQL plugin
bin/opensearch-plugin install opensearch-sql

# Install OLAP plugin
bin/opensearch-plugin install file:///path/to/opensearch-olap-3.6.0-SNAPSHOT.zip
```

On platforms without Velox native library support (e.g. macOS/aarch64), the plugin will start but disable itself:
```
[WARN] Velox engine unavailable on this platform, OLAP plugin will be disabled
```

All queries will fall back to the default OpenSearch execution engine.

## SQL Plugin Changes Required

The following changes are needed in the SQL plugin (`opensearch-sql`) for the OLAP plugin to work:

1. **`ExecutionEngine.java`** — Added `default boolean canVectorize(RelNode plan)` returning `false`
2. **`DelegatingExecutionEngine.java`** (new) — Wraps default engine + SPI extensions; routes to the first extension where `canVectorize()` returns `true`
3. **`SQLPlugin.java`** — Implements `ExtensiblePlugin`, loads `ExecutionEngine` extensions via SPI
4. **`OpenSearchPluginModule.java`** — Wraps the execution engine in `DelegatingExecutionEngine` when extensions are present

`QueryService` is **not modified** — the delegation is transparent.

## License

This code is licensed under the Apache 2.0 License. See [LICENSE.txt](LICENSE.txt).

## Copyright

Copyright OpenSearch Contributors. See [NOTICE](NOTICE.txt) for details.
