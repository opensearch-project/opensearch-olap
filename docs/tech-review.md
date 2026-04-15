# OpenSearch OLAP Plugin — Technical Review

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Motivation](#2-motivation)
3. [Design Principles](#3-design-principles)
4. [Architecture](#4-architecture)
   - 4.1 End-to-End Query Flow
   - 4.2 Plugin Integration Model
   - 4.3 Distributed Execution (MPP)
   - 4.4 Fragment Dispatch Pipeline
   - 4.5 Data Node Pipeline
   - 4.6 Threading Model
5. [Key Design Decisions](#5-key-design-decisions)
6. [Comparison with RFC #4812](#6-comparison-with-rfc-4812)
7. [Current Scope and Roadmap](#7-current-scope-and-roadmap)

---

## 1. Executive Summary

The OpenSearch OLAP plugin adds a vectorized, columnar execution engine to OpenSearch for analytical queries. It **co-works with the existing SQL plugin** — the SQL plugin continues to handle SQL/PPL parsing and Calcite plan generation, and the OLAP plugin provides an alternative execution backend powered by [Apache Velox](https://velox-lib.io/) (a C++ vectorized engine accessed via [velox4j](https://github.com/velox4j/velox4j)).

**At a glance:**
- Co-works with the SQL plugin — no replacement, no fork
- 6 minimal changes to the SQL plugin; zero changes to OpenSearch core
- Calcite Convention-based physical planning (VolcanoPlanner + PhysicalConvention + ConverterRules)
- Supports MPP execution: two-phase distributed aggregation, coordinator-centric/broadcast/shuffle joins
- Runtime Filter pushdown (build-side join keys → Lucene TermInSetQuery on probe scan)
- CBO statistics for row-count-based join build side selection
- Two-stage TopN, window functions (eventstats), fault tolerance with task retry
- Segment-level parallel reads, predicate pushdown to Lucene, per-query session isolation
- Plan explain via PPL `explain` command (Velox plan tree output)
- Graceful degradation — if Velox is unavailable, queries fall back to the default engine transparently

---

## 2. Motivation

OpenSearch's default execution is row-oriented and optimized for search. Analytical workloads — aggregations over millions of rows, joins, complex projections — benefit significantly from columnar, vectorized execution.

The [RFC (opensearch-project/sql#4812)](https://github.com/opensearch-project/sql/issues/4812) describes an internal OLAP system in production on thousands of nodes for two years, achieving 10,000+ QPS for JOINs and sub-second latency on million-row aggregations. This plugin is a community-oriented implementation targeting the same architectural goals using the open-source Velox engine.

---

## 3. Design Principles

| Principle | Approach |
|-----------|----------|
| **Co-work, not replace** | SQL plugin owns parsing and planning. OLAP plugin only owns execution. |
| **Minimal invasion** | 6 SQL plugin file changes. `QueryService` untouched. No OpenSearch core changes. |
| **Pluggable engine** | Any engine can implement `canVectorize(RelNode)` on the `ExecutionEngine` interface. Velox is the first; others (DataFusion, etc.) can follow the same pattern. |
| **Graceful degradation** | If native libraries fail to load, the plugin disables itself. `canVectorize()` returns `false`, all queries fall back to the default engine. No user-visible errors. |

---

## 4. Architecture

### 4.1 End-to-End Query Flow

```
  SQL/PPL Query
      │
      ▼
  SQL Plugin: Parse → Calcite Analyzer → RelNode (Convention.NONE)
      │
      ▼
  DelegatingExecutionEngine
      │
      ├── canVectorize() = true ──► OLAP Plugin
      │                              │
      │                    1. Optimize: PhysicalOptimizer (VolcanoPlanner)
      │                       - ClusterCopyShuttle → new cluster (strip pushdown)
      │                       - HepPlanner: FilterMergeRule
      │                       - ConverterRules: NONE → PhysicalConvention
      │                       - Inserts PhysicalExchange at distribution boundaries
      │                    2. Generate: VeloxPlanGenerator
      │                       - Split at PhysicalExchange → PlanFragments
      │                       - Two-stage agg: SINGLE → PARTIAL + FINAL
      │                       - Convert to Velox PlanNodes
      │                    3. Schedule: route fragments to data nodes by shard
      │                    4. Execute:  data nodes run Velox on local shards
      │                    5. Collect:  coordinator merges partial results
      │                    6. Return:   QueryResponse → SQL Plugin
      │
      └── canVectorize() = false ─► Default Engine (row-oriented)
```

The delegation is transparent — `QueryService` calls `executionEngine.execute()` without knowing which backend handles it.

### 4.2 Plugin Integration Model

The OLAP plugin hooks into the SQL plugin through OpenSearch's `ExtensiblePlugin` mechanism:

```
                OpenSearch Bootstrap
                        │
           ┌────────────┴────────────┐
           ▼                         ▼
       SQL Plugin                OLAP Plugin
  (ExtensiblePlugin)       (extendedPlugins = ['opensearch-sql'])
           │                         │
  loadExtensions()          SPI: META-INF/services/
     discovers ◄──────────── ExecutionEngine
           │
  createComponents()
     wraps default engine
     in DelegatingExecutionEngine
           │
     Query arrives →
     canVectorize()?
         yes → OLAP
         no  → default
```

**SQL plugin changes (6 files):**
- `ExecutionEngine.canVectorize(RelNode)` — default method returning `false`; vectorized engines override it
- `DelegatingExecutionEngine` — wraps default + extensions; routes to first match
- `EngineExtensionsHolder` — makes extensions injectable via OpenSearch's Guice
- `SQLPlugin` — implements `ExtensiblePlugin.loadExtensions()`
- `OpenSearchPluginModule` — conditional wrapping when extensions are present
- `TransportPPLQueryAction` — injects extensions holder (bug fix: previously had no extensions)

### 4.3 Distributed Execution (MPP)

For aggregation queries, the plan is split into two phases:

```
     ┌──────────────────────────────────────────┐
     │           Coordinator Node                │
     │                                           │
     │  RelNode → PhysicalOptimizer              │
     │         → VeloxPlanGenerator              │
     │              │                            │
     │     ┌────────┴────────┐                   │
     │     │   PARTIAL frag  │  FINAL frag       │
     │     │  (per data node)│  (coordinator)    │
     │     └────────┬────────┘       │           │
     └──────────────┼────────────────┼───────────┘
                    │                │
        ┌───────────┼───────────┐    │
        ▼           ▼           ▼    │
   Data Node A  Data Node B  Data Node C
   Scan+Filter  Scan+Filter  Scan+Filter
   PARTIAL Agg  PARTIAL Agg  PARTIAL Agg
        │           │           │
        └───────────┼───────────┘
                    │  partial results
                    ▼
              Coordinator
              FINAL Agg
                    │
                    ▼
              QueryResponse
```

- **Leaf fragments** (SOURCE) run on data nodes that own the index shards
- **Root fragment** (COORDINATOR) runs on the coordinator, consuming partial results
- Plans without aggregation use simple scatter-gather (single fragment type)

### 4.4 Fragment Dispatch Pipeline

The coordinator runs a multi-stage pipeline to transform a logical plan into distributed execution:

```
  Calcite RelNode (Convention.NONE)
       │
       ▼
  ① PhysicalOptimizer
       │  Creates a new VolcanoPlanner + RelOptCluster.
       │  ClusterCopyShuttle deep-copies the plan into the new cluster,
       │  stripping CalciteLogicalIndexScan → plain LogicalTableScan
       │  (prevents SQL plugin pushdown rules from leaking in).
       │  Runs HepPlanner (FilterMergeRule) then VolcanoPlanner:
       │    Convention.NONE → PhysicalConvention
       │    PhysicalAggregateRule inserts PhysicalExchange(SINGLETON)
       │    PhysicalJoinRule inserts PhysicalExchange(SINGLETON) on both inputs
       │    MPP rules (when mpp_enabled=true):
       │      MppJoinRule inserts PhysicalExchange(HASH) on both inputs
       │      MppAggregateRule inserts PhysicalExchange(HASH) on input
       │    Planner explores SINGLETON + HASH alternatives, picks lower cost
       ▼
  Physical RelNode tree (PhysicalConvention + PhysicalExchange boundaries)
       │
       ▼
  ② VeloxPlanGenerator
       │  Walks the physical plan and splits at PhysicalExchange boundaries.
       │  Detects Aggregate(SINGLE) + Exchange → two-stage split:
       │    Fragment 0 (leaf, SOURCE):   PARTIAL Agg → TableScanNode
       │    Fragment 1 (root, COORDINATOR): FINAL Agg (wired during execution)
       │  For joins: inserts ProjectNode to rename conflicting column names.
       │  Converts physical RelNodes to velox4j PlanNodes.
       ▼
  List<PlanFragment>  (ordered leaf-first, root-last)
       │
       ▼
  ③ QueryScheduler.schedule()
       │  Creates a QueryExecution with a unique QueryId.
       │  For each PlanFragment, creates a Stage.
       │  For each Stage:
       │    SOURCE distribution → ShardRouter assigns tasks to nodes
       │    COORDINATOR distribution → single task on local node
       ▼
  QueryExecution  (Stages → Tasks)
       │
       ▼
  ④ NodeResultCollector.dispatchAndCollect()
       │  Serializes each task's PlanFragment to velox4j JSON.
       │  Sends ExecuteFragmentRequest to each target node via TransportService.
       │  Awaits all responses with a CountDownLatch (timeout: 5 min).
       ▼
  List<ExecuteFragmentResponse>  (Arrow IPC or Velox native bytes)
       │
       ▼
  ⑤ Coordinator FINAL aggregation  (if two-phase)
       │  Deserializes partial results from data nodes.
       │  Wires them into the FINAL AggregationNode's input.
       │  Executes locally via VeloxExecutor.
       ▼
  QueryResponse  (returned to SQL plugin)
```

#### Shard Routing

`ShardRouter.routeShards()` reads OpenSearch's `ClusterState` routing table and groups **primary shards** by their owning data node. This produces a map of `DiscoveryNode → List<ShardId>`. The scheduler creates one `TaskDescriptor` per entry — so a 3-node cluster with 5 primary shards might produce:

```
  Node A → [shard-0, shard-3]  →  TaskDescriptor(partition=0)
  Node B → [shard-1, shard-4]  →  TaskDescriptor(partition=1)
  Node C → [shard-2]           →  TaskDescriptor(partition=2)
```

Each task receives the same PlanFragment (same Velox plan), but operates on different shards. The data node's `LuceneArrowReader` iterates only the assigned shards.

#### Stage / Task Hierarchy

The scheduling model is inspired by Presto's stage-based execution:

```
  QueryExecution
    ├── Stage 0 (leaf, SOURCE)
    │     ├── Task (partition=0) → Node A  [shard-0, shard-3]
    │     ├── Task (partition=1) → Node B  [shard-1, shard-4]
    │     └── Task (partition=2) → Node C  [shard-2]
    │
    └── Stage 1 (root, COORDINATOR)
          └── Task (partition=0) → Coordinator  (FINAL Agg)
```

Each `Stage` tracks its state (`PLANNED → SCHEDULING → RUNNING → FINISHED`) via atomic CAS transitions. When all tasks in a stage complete, `QueryScheduler.onTaskCompleted()` checks for downstream stages that are now ready and dispatches them. If any task fails, `onTaskFailed()` fails the entire query and cancels remaining tasks.

#### Two-Phase Aggregation Wire Format

PARTIAL aggregation results use Velox's **native serialization** (not Arrow IPC) to preserve intermediate accumulator state. For example, `AVG(x)` produces `{sum, count}` pairs that cannot be represented in standard Arrow format. The coordinator deserializes these native batches, feeds them into the FINAL aggregation plan, and produces the final Arrow IPC result.

### 4.5 Data Node Pipeline

Each data node executes a fragment using two cooperating threads connected by a back-pressure queue:

```
  olap_feeder thread               SEARCH thread
  ┌────────────────────┐     ┌──────────────────────┐
  │ LuceneArrowReader   │     │ VeloxExecutor         │
  │   │                 │     │   │                   │
  │   ▼                 │     │   ▼                   │
  │ DocValues → Arrow   │     │ SerialTask.next()     │
  │   │                 │     │ (drives Velox C++ ops │
  │   ▼                 │     │  on the calling       │
  │ ExternalStreamBridge│────►│  Java thread)         │
  │   (Arrow C Data     │     │   │                   │
  │    Interface)       │     │   ▼                   │
  │   │                 │     │ Result bytes           │
  │   ▼                 │     │ (Arrow IPC or native) │
  │ noMoreInput()       │     │                       │
  └────────────────────┘     └──────────────────────┘
         BlockingQueue
       (back-pressure)
```

#### Step 1: Reading Lucene Doc Values into Arrow (`LuceneArrowReader`)

The feeder thread reads data from OpenSearch's Lucene index. For each assigned shard:

1. **Acquire NRT searcher** — `shard.acquireSearcher("olap-velox")` obtains a near-real-time snapshot of the index.
2. **Resolve column specs** — Maps each requested field to an Arrow type and Lucene doc-value type via the index mapping service:
   ```
   OpenSearch Type     Doc-Value Type     Arrow Type
   ───────────────     ──────────────     ──────────
   keyword, text       SORTED_SET         Utf8
   long, integer       SORTED_NUMERIC     Int(64/32)
   double, float       SORTED_NUMERIC     FloatingPoint
   boolean             SORTED_NUMERIC     Bool
   date                SORTED_NUMERIC     Int(64)
   ```
3. **Iterate segments** — A Lucene index shard consists of multiple segments (immutable on-disk units). The reader iterates each `LeafReaderContext` in the searcher.
4. **Batch documents** — Within each segment, documents are read in batches of 4,096 rows. For each batch, `ArrowBatchBuilder` creates a `VectorSchemaRoot`:
   - Opens `DocValueColumnReader` per column per segment (calls `DocValues.getSortedNumeric()` or `DocValues.getSortedSet()`)
   - Allocates Arrow vectors (`BigIntVector`, `VarCharVector`, `Float8Vector`, etc.)
   - Iterates each document in the batch range: calls `advanceExact(docId)` then reads the value into the Arrow vector
   - Returns a populated `VectorSchemaRoot` with schema + columnar data

```
  Shard 0
    Segment 0 (10,000 docs)
      Batch [0, 4096)    → VectorSchemaRoot → bridge.feedBatch()
      Batch [4096, 8192) → VectorSchemaRoot → bridge.feedBatch()
      Batch [8192, 10000) → VectorSchemaRoot → bridge.feedBatch()
    Segment 1 (5,000 docs)
      Batch [0, 4096)    → VectorSchemaRoot → bridge.feedBatch()
      Batch [4096, 5000) → VectorSchemaRoot → bridge.feedBatch()
  Shard 3
    ...same pattern...
  bridge.noMoreInput()
```

#### Step 2: Arrow → Velox Conversion (`ExternalStreamBridge`)

The bridge converts Arrow batches to Velox format and feeds them into a `BlockingQueue`:

1. **`bridge.open()`** — Creates a native `BlockingQueue` via velox4j's `ExternalStreams` API. This queue lives in Velox's C++ memory space.
2. **`bridge.feedBatch(arrowBatch)`** — For each Arrow batch:
   - Converts the `VectorSchemaRoot` to a Velox `RowVector` via the **Arrow C Data Interface** (zero-copy when memory layouts are compatible)
   - Calls `queue.put(rowVector)` — **blocks if the queue is full**, providing natural back-pressure: if Velox is processing slower than Lucene is reading, the feeder pauses
   - Closes the Arrow batch (data now owned by Velox's memory pool)
3. **`bridge.noMoreInput()`** — Signals end-of-stream. The Velox `TableScanNode` sees EOF and finishes its operator pipeline.

The `BlockingQueue` eliminates the need for a custom Velox connector. Java pushes data from the Lucene side, and Velox's C++ `TableScanNode` pulls it — connected by a bounded queue that handles synchronization and back-pressure automatically.

#### Step 3: Velox Execution (`VeloxExecutor`)

The SEARCH thread creates and drives the Velox execution:

1. **Rebuild query with connector config** — Registers the `ExternalStream` connector so Velox knows how to read from the `BlockingQueue`.
2. **Create `SerialTask`** — `queries.execute(query)` creates a task but doesn't start execution yet.
3. **Add split** — `serialTask.addSplit(scanNodeId, ExternalStreamConnectorSplit)` tells the `TableScanNode` which queue to read from. This starts execution.
4. **Call `noMoreSplits()`** — Signals that no more data sources will be added.
5. **Iterate results** — `serialTask` is wrapped in a Java iterator. Each call to `next()` drives Velox's C++ operators on the calling Java thread (serial mode — no internal Velox threading). Velox applies the plan (filter, project, aggregate) and produces result batches.
6. **Serialize results** — Each result `RowVector` is converted back to Arrow and written to an `ArrowStreamWriter` (Arrow IPC format). For PARTIAL aggregation, results are serialized using Velox's native format to preserve intermediate accumulator state (e.g., `{sum, count}` for `AVG`).

#### Key Design Choices

- **Doc values as data source** — Columnar on disk, sequential per segment — matches Arrow/Velox columnar memory layout. Stored fields would require row-by-row decompression.
- **Batch size 4,096** — Balances memory footprint against vectorization efficiency. Small enough to pipeline (feeder producing while Velox consuming), large enough for SIMD benefits in Velox.
- **Zero-copy via Arrow C Data Interface** — The `fromArrowVectorSchemaRoot()` call uses Arrow's C Data Interface to share memory between Java and C++ without copying the columnar buffers.
- **Serial Velox mode** — The Java thread that calls `next()` is the same thread that runs Velox operators. No JNI `AttachCurrentThread`, no cross-language thread synchronization. Parallelism is at the node/shard level, not within Velox.

### 4.6 Threading Model

```
Coordinator thread
  └─ VeloxExecutionEngine.execute()
       └─ NodeResultCollector.dispatchAndCollect()
            ├─ sendRequest → Node A ─┐
            ├─ sendRequest → Node B ─┤  (async fan-out)
            └─ sendRequest → Node C ─┘
            latch.await() (block until all respond)

Data Node (per fragment):
  ├─ SEARCH thread:      Velox SerialTask.next() loop
  └─ olap_feeder thread: Lucene doc values → Arrow → ExternalStream
```

| Thread Pool | Purpose |
|-------------|---------|
| `SEARCH` (built-in) | Drives Velox execution on data nodes |
| `olap_feeder` (custom, scaling 1→N CPUs) | Reads Lucene doc values into Arrow batches |
| Transport threads (built-in) | Inter-node fragment dispatch |

---

## 5. Key Design Decisions

### Why co-work instead of standalone?

The SQL plugin already implements SQL/PPL parsing, Calcite integration, and query lifecycle management. Rebuilding that would be a massive effort with no user benefit. By only replacing the execution backend, we get Velox acceleration with minimal code and risk.

### Why `canVectorize()` on `ExecutionEngine` instead of a separate interface?

This avoids adding a new parameter to `QueryService`'s constructor. The delegation is transparent — `QueryService` just calls `executionEngine.execute()` and doesn't know about extensions. This keeps the SQL plugin's internal architecture unchanged.

### Why serial Velox mode?

velox4j only supports serial mode. In parallel mode, Velox's internal threads would require `JNI AttachCurrentThread`, thread-safe output buffers shared between C++ and Java, and complex synchronization with GC. Serial mode avoids all of this — the Java framework manages parallelism (one serial task per data node), and Velox processes data single-threaded per task.

### Why doc values instead of stored fields?

Doc values are columnar on disk — sequential iteration per segment is ideal for full-scan analytics and matches Arrow/Velox's columnar memory layout. Stored fields are row-oriented and require decompression per document.

### Why ExternalStream instead of a custom Velox connector?

Writing a custom C++ Velox connector for OpenSearch would require building and maintaining native code. velox4j's `ExternalStream` with `BlockingQueue` lets us stay entirely in Java for data reading (Lucene → Arrow) and use a simple producer-consumer pattern to feed Velox. The Arrow C Data Interface makes the handoff efficient.

---

## 6. Comparison with RFC #4812

The RFC describes a mature system. Our implementation is a focused subset targeting the same architectural goals.

### Shared Foundations

| Aspect | Shared Approach |
|--------|----------------|
| **Plan representation** | Calcite RelNodes as the lingua franca |
| **Scheduling model** | Presto-inspired Stage/Task distributed execution |
| **Data format** | Apache Arrow for columnar data transfer |
| **Pluggable engine** | Separate plan optimization from engine-specific execution |
| **MPP** | Split aggregation across data nodes |

### Key Differences

| Aspect | RFC #4812 | Our Implementation |
|--------|-----------|-------------------|
| **Scope** | Full rewrite of query pipeline (parsing → execution) | Extends existing SQL plugin (execution only) |
| **Optimization** | 6-stage optimizer chain (Logical → CBO → Physical → RuntimeFilter → Engine → Exec) | PhysicalOptimizer: ClusterCopyShuttle + HepPlanner + VolcanoPlanner with PhysicalConvention |
| **Exchange insertion** | Trait-driven via Convention.enforce() with RelDistributionTraitDef | Explicit in ConverterRules: base rules insert PhysicalExchange(SINGLETON), MPP rules insert PhysicalExchange(HASH). VolcanoPlanner explores both. |
| **Engine abstraction** | EngineOptimizer + EngineBridge interfaces | `canVectorize()` + `execute(RelNode)` on `ExecutionEngine` |
| **Execution model** | Push-based pipeline with Operators and Consumers | Pull-based (Velox SerialTask.next()) |
| **Data reading** | Concurrent reads at segment granularity | Segment-level parallel reads with configurable parallelism (`segment_parallelism`) |
| **Fault tolerance** | Task retries, node health monitoring | Task retry with error classification (node/shard/transient), bad resource tracking, replica failover (`task_max_retries`) |
| **Join reorder** | DP algorithm for bushy join reordering with statistics | Calcite `MultiJoinOptimizeBushyRule` (greedy heuristic) + CBO row counts via `StatisticsTableScan` |
| **Maturity** | Production on thousands of nodes | End-to-end working: scan, filter, project, aggregate, join, sort, window, TopN |

### Our approach as stepping stone

Our design extends the SQL plugin rather than rewriting it, while adopting key RFC architectural patterns:
- **Calcite Convention-based physical planning** — same pattern as the RFC's Physical Optimizer, using VolcanoPlanner with ConverterRules and PhysicalExchange insertion
- **ClusterCopyShuttle** — decouples from the SQL plugin's planner, stripping pushdown context to prevent rule leaks (solves a real integration challenge the RFC doesn't address)
- **Distributed execution** — Presto-inspired Stage/Task model with shard routing, same as RFC
- **Pluggable engine** — `canVectorize()` extension point validated with Velox; other engines can follow the same pattern
- Parallel reads, task retries, and runtime filters are already implemented, with the infrastructure supporting further incremental improvements (BLOOM RF, adaptive execution, join reorder)

---

## 7. Current Scope and Roadmap

### What Works Today

- Full table scan, filter, projection, aggregation, sort/limit, hash join
- Two-phase distributed aggregation (PARTIAL/FINAL)
- Predicate pushdown to Lucene (filter pushed to doc-value-level BKD/term queries)
- Shard-aware scheduling via ClusterState routing
- MPP join support: coordinator-centric, broadcast, and hash shuffle strategies (all wired through PhysicalOptimizer → VeloxPlanGenerator → executeFragments)
- Calcite physical planning: PhysicalOptimizer (VolcanoPlanner + ClusterCopyShuttle) → VeloxPlanGenerator
- Dynamic MPP settings: `mpp_enabled`, `broadcast_max_shards`, `shuffle_partitions` togglable at runtime via cluster settings API
- Cost-based MPP strategy selection: CostEstimator selects BROADCAST vs HASH_SHUFFLE (shard count for strategy, row count for build side when CBO enabled)
- Segment-level parallel reads: each Lucene segment read by a separate thread, configurable via `plugins.velox.segment_parallelism` (default 4, dynamic)
- Fault tolerance with task retry: per-task retry with error classification (node/shard/transient), bad resource tracking, and replica failover via `plugins.velox.task_max_retries` (default 2, dynamic)
- Runtime Filter (TERMS): extracts build-side join key values and pushes as Lucene TermInSetQuery/PointInSetQuery to probe scan, skipping non-matching docs at the index level. Configurable via `plugins.velox.runtime_filter_enabled` and `runtime_filter_max_cardinality` (both dynamic).
- Two-stage TopN: Sort+Limit queries split into partial sort+limit on data nodes (top-K per shard) + final sort+limit on coordinator, reducing data transfer from O(N) to O(K × shards)
- Window functions: eventstats (COUNT, SUM, AVG, MIN, MAX with PARTITION BY) via Velox WindowNode. ProjectToWindowRule decomposes RexOver → LogicalWindow → PhysicalWindow → WindowNode.
- CBO statistics: query-time row count + data size collection from IndicesStatsResponse. Feeds into CostEstimator for row-count-based build side selection. Configurable via `plugins.velox.cbo_statistics_mode` (RUNTIME/NONE, default RUNTIME, dynamic).
- Join reorder: Calcite-based bushy join reordering using CBO row counts. `JoinToMultiJoinRule` flattens binary join trees into N-ary `MultiJoin`, `MultiJoinOptimizeBushyRule` produces optimal bushy tree using greedy heuristic. `LoptOptimizeJoinRule` handles outer join cases. CBO row counts injected via `StatisticsTableScan` into Calcite's cost model. Coordinator-centric execution supports N-way joins (3+ tables). MPP multi-way falls back to coordinator-centric with TODO for staged execution.
- PPL UDF → Velox function mapping: 89 of 195 PPL functions mapped to Velox Presto-style names (46% coverage). Math (90%), trig (100%), string (64%), conditional (64%), date extraction (34%). `canVectorize()` checks function support and field types — unsupported functions/types cause query-level fallback. See `docs/velox-function-support.md` for the full matrix.
- Plan explain: PPL `explain` command outputs Velox plan tree via `PlanNode.toFormatString()`, showing operator pipeline, column projections, and filter expressions.
- Big5 benchmark: 58 PPL queries from the SQL plugin's Big5 benchmark migrated. Currently all fall back to the default engine due to nested object fields (`MAP<VARCHAR, ANY>` type). Flat-schema queries with keyword/numeric/date fields execute through Velox.
- Per-query session creation (prevents memory pool collisions)
- Graceful degradation on unsupported platforms
- Data types: boolean, integer, long, float, double, keyword, date, timestamp

### What's Next

Gaps identified by comparison with [RFC #4812](https://github.com/opensearch-project/sql/issues/4812), prioritized by production impact.

#### Phase 1 — Performance & Stability Foundation

| Priority | Item | Gap vs RFC | Current State |
|----------|------|-----------|---------------|
| ~~**Critical**~~ | ~~Runtime Filter (TERMS)~~ | ~~RFC shows 2min → 100ms for 2B×2K row joins~~ | **Done** — build-side join key values extracted and pushed as Lucene TermInSetQuery/PointInSetQuery to probe scan; configurable via `runtime_filter_enabled` + `runtime_filter_max_cardinality` |
| ~~**High**~~ | ~~Segment-level parallel reads~~ | ~~RFC splits TableScan across Lucene segments with concurrent workers~~ | **Done** — `readShardIntoStreamParallel()` submits one task per segment; configurable via `segment_parallelism` |
| ~~**High**~~ | ~~Fault tolerance + task retry~~ | ~~RFC has task-level retry, bad node tracking, shard replica failover~~ | **Done** — `ErrorClassifier` categorizes errors, `BadResourceTracker` excludes failed nodes/shards, `NodeResultCollector` retries with replica failover; configurable via `task_max_retries` |
| **High** | Backpressure / flow control | RFC has sink buffer limits with reverse pressure propagation | Basic BlockingQueue back-pressure only |

#### Phase 2 — Query Optimization

| Priority | Item | Gap vs RFC | Current State |
|----------|------|-----------|---------------|
| ~~**High**~~ | ~~CBO statistics~~ | ~~RFC uses runtime statistics~~ | **Done** — table-level row count + size from IndicesStatsResponse; feeds CostEstimator for row-count-based build side selection; `cbo_statistics_mode` setting |
| ~~**High**~~ | ~~Join reorder~~ | ~~RFC uses DP algorithm for bushy join reordering with statistics~~ | **Done** — Calcite `JoinToMultiJoinRule` + `MultiJoinOptimizeBushyRule` in HepPlanner; CBO row counts injected via `StatisticsTableScan`; `LoptOptimizeJoinRule` fallback for outer joins; coordinator-centric N-way join execution |
| **Medium** | Cost-based join strategy (real stats) | RFC uses table cardinality + selectivity estimation | Row count available via CBO; column cardinality deferred (Lucene segment stats, Option B) |
| **Medium** | Runtime Filter (BLOOM) | RFC supports probabilistic BLOOM variant for high-cardinality keys | Not implemented (depends on TERMS RF) |
| ~~**Medium**~~ | ~~TopN optimization~~ | ~~RFC pushes ORDER BY + LIMIT as ranking subquery to data nodes~~ | **Done** — two-stage TopN splits Sort+Limit into partial (data nodes) + final (coordinator); subquery push deferred |

#### Phase 3 — Advanced Distributed Execution

| Priority | Item | Gap vs RFC | Current State |
|----------|------|-----------|---------------|
| **Medium** | Co-Routing / ES_ROUTING_SHUFFLE | RFC enables shard-local joins when both sides share routing key — no shuffle needed | Not implemented; all joins require data movement |
| **Medium** | Adaptive query execution | RFC replans at runtime based on intermediate result sizes | All decisions at plan time |
| **Medium** | Two-stage Runtime Filter construction | RFC builds PARTIAL RF locally, merges into FINAL RF globally for distributed builds | Not implemented (depends on TERMS RF) |
| **Low** | Convention.enforce() for distribution | RFC uses trait-driven exchange insertion via Calcite's automatic enforcement | Explicit PhysicalExchange insertion in rules (works but verbose) |

#### Phase 4 — Operator & Feature Completeness

| Priority | Item | Gap vs RFC | Current State |
|----------|------|-----------|---------------|
| ~~**Medium**~~ | ~~Window functions~~ | ~~ROW_NUMBER, RANK, LAG, LEAD, etc.~~ | **Done** — eventstats (COUNT, SUM, AVG, MIN, MAX) via PhysicalWindow → Velox WindowNode. LAG/LEAD/ROW_NUMBER deferred. |
| **Medium** | UNION / INTERSECT / EXCEPT | Set operations | Not implemented |
| **Low** | Recursive CTE (WITH RECURSIVE) | Fixpoint iteration | Not implemented |
| **Low** | Cross-cluster query | Analytics across multiple OpenSearch clusters | Not implemented |
| ~~**Low**~~ | ~~EXPLAIN visualization~~ | ~~RFC has multi-stage plan visualization + DOT format~~ | **Done** — PPL `explain` command outputs Velox plan tree via `PlanNode.toFormatString()`; shows operator pipeline, column projections, and filter expressions |
