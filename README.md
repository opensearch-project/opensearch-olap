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
   - For non-aggregation queries: results serialized as Arrow IPC bytes
   - For PARTIAL aggregation: results serialized using Velox native format (`BaseVectors.serializeToBuf`) to preserve intermediate accumulator state
8. **Coordinator FINAL Aggregation** (for aggregation queries):
   - Receives Velox native serialized partial results from all data nodes
   - Deserializes via `BaseVectors.deserializeOneFromBuf` (preserves intermediate types like avg's `{sum, count}`)
   - Feeds deserialized RowVectors into a local ExternalStream BlockingQueue
   - Executes FINAL aggregation plan through Velox C++ locally on the coordinator
   - Results converted to Arrow IPC for the final response
9. **Result Collection** - Arrow IPC results converted to SQL plugin's `QueryResponse` format

### Key Design Decisions

- **Co-work, not duplicate**: The SQL plugin handles SQL/PPL → Calcite. The OLAP plugin only handles Calcite → Velox → execution.
- **`canVectorize(RelNode)` on `ExecutionEngine`**: A default method returning `false`. Extensions override it to advertise support for specific plan shapes. The `DelegatingExecutionEngine` routes based on this.
- **Doc values over stored fields**: Doc values are columnar on disk, matching Arrow/Velox's columnar layout. Sequential iteration per segment is ideal for full-scan analytics.
- **ExternalStream bridge**: velox4j's `BlockingQueue` eliminates the need for a custom C++ OpenSearch connector in Velox. Java pushes data, C++ pulls it.
- **Velox native serde for exchange**: PARTIAL aggregation results are serialized using Velox's internal binary format (`BaseVectors.serializeToBuf/deserializeFromBuf`) instead of Arrow IPC. This preserves intermediate accumulator state (e.g., avg's `{sum, count}` pair) that would be lost in an Arrow round-trip. Arrow IPC is still used for final results to the SQL plugin.
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

## Manual Testing

### Prerequisites

- OpenSearch 3.6.0-SNAPSHOT built locally (e.g. at `../OpenSearch/build/distribution/local/opensearch-3.6.0-SNAPSHOT`)
- SQL plugin installed
- OLAP plugin built and installed (see [Building](#building) and [Installation](#installation))

### Single-Node Testing

#### 1. Start OpenSearch

```bash
cd ../OpenSearch/build/distribution/local/opensearch-3.6.0-SNAPSHOT
bin/opensearch -d -p /tmp/opensearch.pid
```

Wait for startup and verify Velox initialized:

```bash
# Wait until OpenSearch is ready
curl -s http://localhost:9200

# Check Velox engine status in logs
grep "Velox engine" logs/opensearch.log | tail -1
# Expected: Velox engine initialized successfully
```

#### 2. Create test index and insert data

```bash
# Create index with typed mappings
curl -s -X PUT "http://localhost:9200/test_olap" \
  -H "Content-Type: application/json" \
  -d '{
  "mappings": {
    "properties": {
      "name": {"type": "keyword"},
      "age": {"type": "integer"},
      "city": {"type": "keyword"},
      "salary": {"type": "double"}
    }
  }
}'

# Insert sample data
curl -s -X POST "http://localhost:9200/test_olap/_doc" -H "Content-Type: application/json" -d '{"name":"Alice","age":35,"city":"Seattle","salary":120000}'
curl -s -X POST "http://localhost:9200/test_olap/_doc" -H "Content-Type: application/json" -d '{"name":"Bob","age":28,"city":"Portland","salary":95000}'
curl -s -X POST "http://localhost:9200/test_olap/_doc" -H "Content-Type: application/json" -d '{"name":"Charlie","age":42,"city":"Seattle","salary":150000}'
curl -s -X POST "http://localhost:9200/test_olap/_doc" -H "Content-Type: application/json" -d '{"name":"Diana","age":31,"city":"Denver","salary":110000}'
curl -s -X POST "http://localhost:9200/test_olap/_doc" -H "Content-Type: application/json" -d '{"name":"Eve","age":26,"city":"Portland","salary":88000}'

# Verify data
curl -s "http://localhost:9200/test_olap/_count"
# Expected: {"count":5, ...}
```

#### 3. Run queries through Velox

```bash
# Aggregation - count by city
curl -s -X POST "http://localhost:9200/_plugins/_ppl" \
  -H "Content-Type: application/json" \
  -d '{"query": "source=test_olap | stats count() by city"}'

# Aggregation - avg salary by city
curl -s -X POST "http://localhost:9200/_plugins/_ppl" \
  -H "Content-Type: application/json" \
  -d '{"query": "source=test_olap | stats avg(salary) by city"}'

# Aggregation - sum and count (no group by)
curl -s -X POST "http://localhost:9200/_plugins/_ppl" \
  -H "Content-Type: application/json" \
  -d '{"query": "source=test_olap | stats sum(age), count()"}'

# SQL query (same delegation path)
curl -s -X POST "http://localhost:9200/_plugins/_sql" \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT city, COUNT(*) FROM test_olap GROUP BY city"}'
```

#### 4. Verify OLAP plugin handled the query

Check logs for the Velox execution path:

```bash
grep -E "Routing query to extension|Executing query.*Velox|Executing fragment|Dispatching stage|Cannot vectorize" logs/opensearch.log | tail -10
```

Expected log sequence when query is handled by Velox:
```
[o.o.s.e.DelegatingExecutionEngine] Routing query to extension engine : VectorizedEngineExtension
[o.o.p.o.e.VeloxExecutionEngine]    Executing query <id> via Velox engine
[o.o.p.o.s.QueryScheduler]          Dispatching stage <id> with 1 tasks
[o.o.p.o.t.TransportExecuteFragmentAction] Executing fragment 0 for query <id> on 1 shards
```

If the query falls back to the default engine, you will see:
```
[o.o.p.o.e.VectorizedEngineExtension] Cannot vectorize plan: unsupported node [<class>] in <plan>
```

If no OLAP plugin log appears at all, `canVectorize()` returned `false` because Velox is unavailable — check for `Velox engine unavailable` at startup.

#### 5. Enable debug logging (optional)

```bash
curl -s -X PUT "http://localhost:9200/_cluster/settings" \
  -H "Content-Type: application/json" \
  -d '{"transient": {"logger.org.opensearch.plugin.olap": "DEBUG"}}'
```

#### 6. Reinstall after code changes

```bash
# Stop OpenSearch
kill $(cat /tmp/opensearch.pid)

# Rebuild
cd /path/to/opensearch-olap
./gradlew assemble

# Reinstall
cd ../OpenSearch/build/distribution/local/opensearch-3.6.0-SNAPSHOT
bin/opensearch-plugin remove opensearch-olap
bin/opensearch-plugin install file:///absolute/path/to/opensearch-olap/build/distributions/opensearch-olap-3.6.0-SNAPSHOT.zip

# Restart
bin/opensearch -d -p /tmp/opensearch.pid
```

### Multi-Node Testing

Tests distributed execution where fragments are dispatched to remote data nodes via TransportService.

#### 1. Set up a 2-node local cluster

Copy the existing build to create a second node:

```bash
BASE=../OpenSearch/build/distribution/local
cp -r "$BASE/opensearch-3.6.0-SNAPSHOT" "$BASE/opensearch-node2"
```

Configure node 1 (`$BASE/opensearch-3.6.0-SNAPSHOT/config/opensearch.yml`):

```yaml
cluster.name: olap-test-cluster
node.name: node-1
network.host: 127.0.0.1
http.port: 9200
transport.port: 9300
discovery.seed_hosts: ["127.0.0.1:9300", "127.0.0.1:9301"]
cluster.initial_cluster_manager_nodes: ["node-1", "node-2"]
```

Configure node 2 (`$BASE/opensearch-node2/config/opensearch.yml`):

```yaml
cluster.name: olap-test-cluster
node.name: node-2
network.host: 127.0.0.1
http.port: 9201
transport.port: 9301
discovery.seed_hosts: ["127.0.0.1:9300", "127.0.0.1:9301"]
cluster.initial_cluster_manager_nodes: ["node-1", "node-2"]
```

Clean old data from both nodes (important if they previously ran as single-node):

```bash
rm -rf $BASE/opensearch-3.6.0-SNAPSHOT/data/*
rm -rf $BASE/opensearch-node2/data/*
rm -rf /tmp/opensearch-*
```

#### 2. Start both nodes

```bash
cd $BASE/opensearch-3.6.0-SNAPSHOT && bin/opensearch -d -p /tmp/opensearch-node1.pid
cd $BASE/opensearch-node2 && bin/opensearch -d -p /tmp/opensearch-node2.pid
```

Wait for the cluster to form and verify both nodes joined:

```bash
curl -s "http://localhost:9200/_cat/nodes?v"
```

Expected output (2 nodes):
```
ip        heap.percent ram.percent cpu node.role cluster_manager name
127.0.0.1           14          92   5 dimr      *               node-1
127.0.0.1           13          92   5 dimr      -               node-2
```

#### 3. Create test index with multiple shards

Use 2 shards and 0 replicas so each shard goes to a different node:

```bash
curl -s -X PUT "http://localhost:9200/test_olap" \
  -H "Content-Type: application/json" \
  -d '{
  "settings": {"number_of_shards": 2, "number_of_replicas": 0},
  "mappings": {
    "properties": {
      "name": {"type": "keyword"},
      "age": {"type": "integer"},
      "city": {"type": "keyword"},
      "salary": {"type": "double"}
    }
  }
}'

# Insert sample data
curl -s -X POST "http://localhost:9200/test_olap/_doc" -H "Content-Type: application/json" -d '{"name":"Alice","age":35,"city":"Seattle","salary":120000}'
curl -s -X POST "http://localhost:9200/test_olap/_doc" -H "Content-Type: application/json" -d '{"name":"Bob","age":28,"city":"Portland","salary":95000}'
curl -s -X POST "http://localhost:9200/test_olap/_doc" -H "Content-Type: application/json" -d '{"name":"Charlie","age":42,"city":"Seattle","salary":150000}'
curl -s -X POST "http://localhost:9200/test_olap/_doc" -H "Content-Type: application/json" -d '{"name":"Diana","age":31,"city":"Denver","salary":110000}'
curl -s -X POST "http://localhost:9200/test_olap/_doc" -H "Content-Type: application/json" -d '{"name":"Eve","age":26,"city":"Portland","salary":88000}'

# Verify shards are distributed across both nodes
curl -s "http://localhost:9200/_cat/shards/test_olap?v"
```

Expected output (shards on different nodes):
```
index     shard prirep state   docs store ip        node
test_olap 0     p      STARTED    3 5.1kb 127.0.0.1 node-2
test_olap 1     p      STARTED    2 4.9kb 127.0.0.1 node-1
```

#### 4. Run distributed queries

```bash
# Aggregation - count by city (distributed across both nodes)
curl -s -X POST "http://localhost:9200/_plugins/_ppl" \
  -H "Content-Type: application/json" \
  -d '{"query": "source=test_olap | stats count() by city"}'

# Aggregation - avg salary by city
curl -s -X POST "http://localhost:9200/_plugins/_ppl" \
  -H "Content-Type: application/json" \
  -d '{"query": "source=test_olap | stats avg(salary) by city"}'
```

Expected results:
```json
{"datarows": [[2,"Seattle"],[2,"Portland"],[1,"Denver"]], ...}
{"datarows": [[135000.0,"Seattle"],[91500.0,"Portland"],[110000.0,"Denver"]], ...}
```

#### 5. Verify distributed execution in logs

Check that both nodes participated in query execution:

```bash
# Node 1 (coordinator): should show routing + scheduling + local fragment execution
grep -E "Routing query|Dispatching stage|Executing fragment" \
  $BASE/opensearch-3.6.0-SNAPSHOT/logs/olap-test-cluster.log | tail -5

# Node 2 (remote data node): should show fragment execution
grep -E "Executing fragment" \
  $BASE/opensearch-node2/logs/olap-test-cluster.log | tail -5
```

Expected: coordinator dispatches **2 tasks** (one per shard), each node executes a fragment:
```
[node-1] Dispatching stage <id> with 2 tasks
[node-1] Executing fragment 0 for query <id> on 1 shards
[node-2] Executing fragment 0 for query <id> on 1 shards
```

Note: the log file is named `olap-test-cluster.log` (matching `cluster.name` in the config), not `opensearch.log`.

#### 6. Stop the cluster

```bash
kill $(cat /tmp/opensearch-node1.pid) $(cat /tmp/opensearch-node2.pid)
rm -rf /tmp/opensearch-*
```

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
