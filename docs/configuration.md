# Configuration

All settings below are prefixed with `plugins.velox.`. Most are **dynamic** — apply at runtime via `PUT /_cluster/settings`.

## Core engine and MPP

| Setting | Default | Description |
|---------|---------|-------------|
| `enabled` | `true` | Enable/disable the OLAP plugin |
| `memory_limit_bytes` | `4294967296` (4 GB) | Velox engine memory limit |
| `num_threads` | `4` | Velox execution threads |
| `mpp_enabled` | `false` | Enable MPP join strategies (broadcast + hash shuffle). When false, joins use coordinator-centric execution. **Dynamic.** |
| `broadcast_max_shards` | `2` | Max primary-shard count for the smaller join side to qualify for broadcast join (MPP only). **Dynamic.** |
| `shuffle_partitions` | `0` | Number of hash-shuffle partitions. `0` = auto (= number of data nodes). **Dynamic.** |
| `segment_parallelism` | `4` | Parallel threads for reading Lucene segments within a shard. `1` to disable. **Dynamic.** |
| `task_max_retries` | `2` | Max retries per failed task (uses replica shards on different nodes when available). `0` to disable. **Dynamic.** |

## Runtime filters and CBO

| Setting | Default | Description |
|---------|---------|-------------|
| `runtime_filter_enabled` | `true` | Enable runtime-filter pushdown for broadcast joins. Pushes build-side join keys as a Lucene `TermInSetQuery`/`PointInSetQuery` (TERMS) or `BloomFilterQuery` (BLOOM) on the probe scan. **Dynamic.** |
| `runtime_filter_max_cardinality` | `10000` | TERMS cap. Above this, BLOOM is tried (subject to bloom cap); otherwise RF is skipped. **Dynamic.** |
| `runtime_filter_bloom_enabled` | `true` | Enable BLOOM variant for high-cardinality join keys. **Dynamic.** |
| `runtime_filter_bloom_max_cardinality` | `10000000` | BLOOM cap. Beyond this, RF is skipped. Also sizes partial blooms in two-stage mode. **Dynamic.** |
| `runtime_filter_bloom_two_stage` | `true` | Two-stage BLOOM build: each data node builds a PARTIAL bloom over local shards, coordinator merges via bitwise-OR into a FINAL bloom. **Dynamic.** |
| `cbo_statistics_mode` | `RUNTIME` | CBO mode. `RUNTIME` collects row counts from `IndicesStatsResponse` before optimization. `NONE` skips (uses shard-count heuristic). **Dynamic.** |

## Co-Routing

| Setting | Default | Description |
|---------|---------|-------------|
| `co_routing_enabled` | `false` | Enable Co-Routing join optimization. Binary equi-joins between registered pairs run shard-local with zero shuffle. Requires `mpp_enabled=true`. **Dynamic.** |
| `co_routed_pairs` | `[]` | Co-routed index pairs. Each entry is `"<indexA>:<keyA>,<indexB>:<keyB>"` — a user assertion that both indexes share `_routing` on the listed key and have identical `number_of_shards`. **Dynamic.** |

Example — register a pair and enable the optimization:

```bash
curl -sS -H 'Content-Type: application/json' -X PUT localhost:9200/_cluster/settings -d '{
  "persistent": {
    "plugins.velox.mpp_enabled": true,
    "plugins.velox.co_routing_enabled": true,
    "plugins.velox.co_routed_pairs": [
      "orders:customer_id,customers:customer_id",
      "events:user_id,profiles:user_id"
    ]
  }
}'
```

**Safety note**: `_routing` is supplied at index time and isn't declared in the mapping. The `co_routed_pairs` setting is load-bearing — register a pair whose indexes were not actually co-routed and you'll get incorrect join results. Only register pairs whose indexing pipeline you control.

## Backpressure / memory bounds

| Setting | Default | Description |
|---------|---------|-------------|
| `per_fragment_arrow_bytes` | `256mb` | Hard cap on the Arrow allocator feeding Velox on data nodes. Soft watermark gates the feeder before each Arrow batch. **Dynamic.** |
| `result_allocator_bytes` | `512mb` | Hard cap on the Arrow allocator used by `VeloxExecutor` to serialize results. **Dynamic.** |
| `max_result_bytes` | `1gb` | Max serialized size of a single fragment result. Overflow → `ResultTooLargeException` (non-retryable). **Dynamic.** |
| `coordinator_allocator_bytes` | `1gb` | Hard cap on coordinator-side Arrow allocators. **Dynamic.** |
| `coordinator_queue_bytes` | `256mb` | Soft watermark for the FINAL-aggregation feed queue on the coordinator. **Dynamic.** |
| `coordinator_inflight_bytes` | `2gb` | Aggregate cap across all fragment responses for a single query. Overflow → query fails fast. **Dynamic.** |
| `shuffle_buffer_bytes` | `512mb` | Per-partition shuffle buffer cap. Sends over the cap receive `backpressure=true`; sender backs off (5 retries, 100 ms → 3.2 s). **Dynamic.** |
| `backpressure_wait_ms` | `30000` | Max time a producer waits on the soft-watermark gate before giving up with `BackpressureTimeoutException` (retryable transient). **Dynamic.** |
| `backpressure_watermark_pct` | `80` | Soft watermark as a percentage of the matching hard cap. Below 100 gives headroom between "pause producer" and "fail". **Dynamic.** |

See [`architecture.md` §4.7](architecture.md#47-backpressure-and-memory-bounds) for the boundary diagram and the reasoning behind the defaults.
