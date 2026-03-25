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
    ├──[default]──► OpenSearchExecutionEngine
    │               (row-oriented, single-node)
    │
    └──[olap]───► OlapExecutionExtension ──────► VeloxExecutionEngine
                                                     │
                                                VeloxPlanConverter
                                                (RelNode → PlanNode)
                                                     │
                                                PlanFragmenter
                                                (split for distribution)
                                                     │
                                                QueryScheduler
                                                (route by shard)
                                                     │
                                          ┌──────────┼──────────┐
                                          ▼          ▼          ▼
                                       Data Node  Data Node  Data Node
                                       Lucene →   Lucene →   Lucene →
                                       Arrow →    Arrow →    Arrow →
                                       Velox C++  Velox C++  Velox C++
```

### Integration via ExtensiblePlugin

The SQL plugin discovers the OLAP engine using OpenSearch's `ExtensiblePlugin` mechanism:

1. The OLAP plugin declares `extended.plugins=opensearch-sql` in its plugin descriptor
2. The SQL plugin implements `ExtensiblePlugin` and loads `OlapExecutionExtension` instances
3. When a query arrives, the SQL plugin checks if the OLAP engine can handle the RelNode plan
4. If yes, execution is delegated to `VeloxExecutionEngine`; otherwise, the default engine is used

## Architecture

```
                          +-----------------------+
                          |     Coordinator Node  |
                          |                       |
  SQL Query ──> SQL Plugin ──> Calcite Analyzer  |
                          |        │              |
                          |  OlapExecutionExtension
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
2. **Plan Conversion** - `VeloxPlanConverter` translates Calcite RelNodes to velox4j PlanNodes
3. **Fragmentation** - `PlanFragmenter` splits the plan at exchange boundaries (e.g., PARTIAL agg on data nodes, FINAL agg on coordinator)
4. **Scheduling** - `QueryScheduler` uses `ClusterState` routing table to assign fragments to data nodes owning the target shards
5. **Transport** - Coordinator dispatches `ExecuteFragmentRequest` to data nodes via OpenSearch `TransportService`
6. **Data Node Execution**:
   - Acquire Lucene searcher via `IndexShard.acquireSearcher()`
   - Read doc values column-by-column into Arrow `VectorSchemaRoot` batches
   - Feed Arrow batches into velox4j `ExternalStream.BlockingQueue` (zero-copy via Arrow C Data Interface)
   - Velox `TableScanNode` reads from the queue and executes the plan fragment natively in C++
7. **Result Collection** - Coordinator merges partial results from all data nodes into the final response

### Key Design Decisions

- **Co-work, not duplicate**: The SQL plugin handles SQL/PPL → Calcite. The OLAP plugin only handles Calcite → Velox → execution.
- **Doc values over stored fields**: Doc values are columnar on disk, matching Arrow/Velox's columnar layout. Sequential iteration per segment is ideal for full-scan analytics.
- **ExternalStream bridge**: velox4j's `BlockingQueue` eliminates the need for a custom C++ OpenSearch connector in Velox. Java pushes data, C++ pulls it.
- **No core changes**: The plugin uses only public OpenSearch APIs (`Plugin`, `ActionPlugin`, `TransportService`, `IndicesService`, `IndexShard.acquireSearcher()`).
- **Presto-inspired scheduler**: Stage/Task hierarchy and fragment dispatch modeled after Presto's `SqlStageExecution` and `RemoteTaskFactory`, adapted for OpenSearch's transport layer.

## Project Structure

```
src/main/java/org/opensearch/plugin/olap/
├── OlapPlugin.java                    # Plugin entry point
├── common/
│   └── QueryId.java                   # Query identifier
├── engine/                            # SQL plugin integration
│   ├── VeloxExecutionEngine.java      #   RelNode → Velox pipeline orchestration
│   ├── VeloxQueryResult.java          #   Query result model
│   ├── OlapExecutionExtension.java    #   Extension point interface
│   └── OlapExecutionExtensionImpl.java#   Extension implementation
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

### Implementation Status

| Component | Status | Lines |
|-----------|--------|-------|
| SQL Plugin Integration (VeloxExecutionEngine) | Implemented | ~170 |
| Plan Conversion (Calcite → Velox) | Implemented | ~890 |
| Plan Fragmentation | Implemented | ~280 |
| Task Scheduler | Implemented | ~830 |
| Transport Layer | Implemented | ~530 |
| Execution Pipeline (Lucene → Arrow → Velox) | Implemented | ~910 |
| Result Collection | Interface | ~80 |

## Supported Operators

### Plan Node Conversion (Calcite → Velox)

| Calcite RelNode | Velox PlanNode |
|----------------|---------------|
| `LogicalTableScan` | `TableScanNode` + `ExternalStreamTableHandle` |
| `LogicalFilter` | `FilterNode` |
| `LogicalProject` | `ProjectNode` |
| `LogicalAggregate` | `AggregationNode` (SINGLE / PARTIAL+FINAL) |
| `LogicalJoin` | `HashJoinNode` (equi-join key extraction) |
| `LogicalSort` | `OrderByNode` + `LimitNode` |

### Expression Conversion (RexNode → TypedExpr)

| Calcite Expression | Velox Expression |
|-------------------|-----------------|
| `RexInputRef` | `FieldAccessTypedExpr` |
| `RexLiteral` | `ConstantTypedExpr` (BooleanValue, IntegerValue, BigIntValue, DoubleValue, VarCharValue) |
| `RexCall` (=, !=, <, >, AND, OR, +, -, *, /) | `CallTypedExpr` (eq, neq, lt, gt, and, or, plus, minus, multiply, divide) |
| `CAST` | `CastTypedExpr` |
| `IS NULL` / `IS NOT NULL` | `CallTypedExpr` (is_null / not(is_null)) |
| `IN` | Chain of eq + or |
| `BETWEEN` | gte + lte + and |

### Aggregate Functions

COUNT, SUM, AVG, MIN, MAX (with DISTINCT support)

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `olap.enabled` | `true` | Enable/disable the OLAP plugin |
| `olap.velox.memory_limit_bytes` | `4294967296` (4 GB) | Velox engine memory limit |
| `olap.velox.num_threads` | `4` | Velox execution threads |

## Dependencies

| Dependency | Version | Purpose |
|-----------|---------|---------|
| OpenSearch | 3.5.0 | Host platform (compileOnly) |
| opensearch-sql | (runtime) | SQL/PPL parsing, Calcite RelNode generation (extended plugin) |
| Apache Calcite | 1.41.0 | Plan conversion (RelNode types) |
| Apache Arrow | 18.1.0 | Columnar in-memory format |
| velox4j | 0.1.0 | JNI bridge to Velox C++ engine |

## Building

```bash
./gradlew build
```

The plugin ZIP will be generated at `build/distributions/opensearch-olap-*.zip`.

## Installation

The OLAP plugin requires the SQL plugin to be installed first:

```bash
bin/opensearch-plugin install opensearch-sql
bin/opensearch-plugin install file:///path/to/opensearch-olap-*.zip
```

## SQL Plugin Integration (Required Changes)

For the SQL plugin to discover and use the OLAP engine, a small change is needed in the SQL plugin:

1. The `SQLPlugin` class should implement `ExtensiblePlugin`
2. In `loadExtensions()`, load `OlapExecutionExtension` instances
3. In `QueryService` or `OpenSearchExecutionEngine`, check for the OLAP extension and delegate eligible queries

Example change in the SQL plugin:

```java
// In SQLPlugin.java
public class SQLPlugin extends Plugin implements ExtensiblePlugin {
    private List<OlapExecutionExtension> olapExtensions = Collections.emptyList();

    @Override
    public void loadExtensions(ExtensionLoader loader) {
        this.olapExtensions = loader.loadExtensions(OlapExecutionExtension.class);
    }
}
```

## License

This code is licensed under the Apache 2.0 License. See [LICENSE.txt](LICENSE.txt).

## Copyright

Copyright OpenSearch Contributors. See [NOTICE](NOTICE.txt) for details.
