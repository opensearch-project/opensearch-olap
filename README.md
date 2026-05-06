# OpenSearch OLAP Plugin

## Welcome!

An OpenSearch plugin that accelerates analytical (OLAP) queries by executing them through [Apache Velox](https://velox-lib.io/), a C++ vectorized columnar engine, via the [velox4j](https://github.com/velox4j/velox4j) JNI bridge. Instead of OpenSearch's row-oriented query-then-fetch pipeline, the plugin reads columnar data from Lucene doc values into Apache Arrow and runs aggregations, joins, and sorts through Velox.

The plugin **co-works with the OpenSearch SQL plugin** — the SQL plugin continues to parse SQL/PPL and generate Calcite plans; the OLAP plugin registers itself as an alternative `ExecutionEngine` and advertises the plan shapes it can vectorize. Queries it can't handle fall back to the default engine automatically.

## Project Resources

* [Project Website](https://opensearch.org/)
* [Downloads](https://opensearch.org/downloads.html)
* [Documentation](docs/)
* Need help? Try the [Forum](https://forum.opensearch.org/c/plugins/sql/8)
* [Project Principles](https://opensearch.org/#principles)
* [Contributing to OpenSearch k-NN](CONTRIBUTING.md)
* [Maintainer Responsibilities](MAINTAINERS.md)

## Features

- Vectorized columnar execution for aggregations, joins, sorts, and window functions
- Distributed MPP execution: two-phase aggregation (PARTIAL/FINAL), broadcast joins, hash-shuffle joins
- Co-Routing joins: shard-local, zero-shuffle joins between co-indexed tables
- Runtime Filter pushdown (TERMS and BLOOM) from build side to probe-side Lucene scan
- CBO row-count statistics for join build-side selection and join reorder
- Two-stage TopN (partial sort+limit per shard → final merge)
- Window functions: eventstats (COUNT/SUM/AVG/MIN/MAX with PARTITION BY) + ranking (ROW_NUMBER, RANK, DENSE_RANK)
- PPL `span()` lowered to Velox (time bucketing via `date_trunc` / arithmetic; numeric bucketing)
- Segment-level parallel Lucene reads
- End-to-end backpressure: bounded Arrow allocators, per-fragment and aggregate result caps, shuffle-buffer flow control
- Fault tolerance: per-task retry with error classification (node/shard/transient), bad-resource tracking, replica failover
- Plan introspection via PPL `explain` and `{"profile": true}`
- Graceful degradation — if Velox native libraries are unavailable (e.g. macOS/aarch64), the plugin disables itself and all queries fall back to the default engine

For internal design, the end-to-end query flow, MPP details, backpressure internals, profiling wire format, and project structure, see [`docs/architecture.md`](docs/architecture.md).

## Prerequisites

- **OpenSearch 3.7.0-SNAPSHOT** built locally. Typical layout: `../OpenSearch/build/distribution/local/opensearch-3.7.0-SNAPSHOT`.
- **opensearch-sql** plugin (3.7.0.0-SNAPSHOT) and **opensearch-job-scheduler** (3.7.0.0-SNAPSHOT) built and available.
- **JDK 21** or later (JDK 25 compiles cleanly).
- **Linux x86_64** for runtime. macOS/aarch64 can build and install the plugin, but Velox native libraries won't load — the plugin will disable itself at startup and all queries fall back. Use the Docker workflow below if you need to test on an incompatible host.

## Build

```bash
# Full build (compile + test + checks)
./gradlew build

# Assemble only (skip tests)
./gradlew assemble

# Auto-format after Java code changes
./gradlew spotlessApply

# Fast pre-commit check (~45s): spotlessCheck + jacoco + unit tests, no cluster
./gradlew build -x integTest
```

The plugin ZIP lands at `build/distributions/opensearch-olap-3.7.0-SNAPSHOT.zip`.

**Integration tests** (`./gradlew integTest`) spin up a managed single-node cluster with job-scheduler, SQL, and OLAP plugins installed. Takes ~1–2 minutes including Velox engine init.

## Install

The OLAP plugin requires the SQL plugin to be installed first:

```bash
bin/opensearch-plugin install opensearch-sql
bin/opensearch-plugin install file:///path/to/opensearch-olap-3.7.0-SNAPSHOT.zip
```

On platforms without Velox native-library support (e.g. macOS/aarch64), the plugin will start but disable itself:

```
[WARN] Velox engine unavailable on this platform, OLAP plugin will be disabled
```

All queries then fall back to the default OpenSearch execution engine.

## Configuration

The plugin exposes ~30 settings under the `plugins.velox.` namespace (engine tuning, MPP toggles, runtime filters, CBO, Co-Routing pair registration, backpressure / memory bounds). Most are **dynamic** — apply at runtime via `PUT /_cluster/settings`.

See [`docs/configuration.md`](docs/configuration.md) for the full settings reference, including the Co-Routing registration example and safety note, and the backpressure/memory tuning table.

## Run / Manual Testing

Hands-on walkthroughs for single-node, multi-node, and Docker setups live in [`docs/running.md`](docs/running.md).

Quick smoke test once the plugin is installed:

```bash
# Start OpenSearch and wait for Velox init
bin/opensearch -d -p /tmp/opensearch.pid
grep "Velox engine initialized successfully" logs/opensearch.log

# Create a tiny index and run an aggregation through Velox
curl -s -X PUT "http://localhost:9200/test_olap" -H "Content-Type: application/json" -d '{
  "mappings": {"properties": {"city": {"type": "keyword"}, "age": {"type": "integer"}}}
}'
curl -s -X POST "http://localhost:9200/test_olap/_doc?refresh=true" -H "Content-Type: application/json" -d '{"city":"Seattle","age":35}'
curl -s -X POST "http://localhost:9200/_plugins/_ppl" -H "Content-Type: application/json" -d '{"query": "source=test_olap | stats count() by city"}'

# Confirm Velox handled it
grep "Executing query.*Velox engine" logs/opensearch.log | tail -1
```

If you see the `Executing query <id> via Velox engine` log line, the plugin is active. See [`docs/running.md`](docs/running.md) for the full single-node workflow (index creation, join queries, log verification, reinstall-after-build), the two-node cluster setup, and the CentOS 7 Docker recipe for hosts whose Velox native libraries aren't compatible.

## Troubleshooting

**Is a query being vectorized?** Run it with `{"profile": true}`:

```bash
curl -sS -H 'Content-Type: application/json' -X POST localhost:9200/_plugins/_ppl -d '{
  "profile": true,
  "query": "source=test_olap | stats count() by city"
}'
```

If `profile.plan.node` starts with `VeloxQuery`, the OLAP plugin handled the query. If `profile.plan` is absent or doesn't mention Velox, the query fell back to the default engine. The server log line `Cannot vectorize plan: unsupported node [<class>]` tells you which operator forced the fallback. See [`docs/architecture.md` §4.9](docs/architecture.md#49-query-profiling) for the full profile schema and counter meanings.

**Is the Velox engine loaded?** Look for one of these at startup:

```
Velox engine initialized successfully                      # happy path
Velox engine unavailable on this platform, OLAP plugin ... # native library not loadable
```

**Force fallback for a query**: the plugin respects `"force_vectorize=false"` set on the SQL plugin's query hint block; alternatively, unsupported plan shapes always fall back automatically.

**Backpressure diagnosis**: if a query succeeds but feels slow, look for non-zero `backpressureWaitNanos` or `shuffleRejectCount` in the profile tree. If a query fails with `ResultTooLargeException`, raise `max_result_bytes` / `coordinator_inflight_bytes` or narrow the query. Settings live in [`docs/configuration.md`](docs/configuration.md#backpressure--memory-bounds); the design diagram lives in [`docs/architecture.md` §4.7](docs/architecture.md#47-backpressure-and-memory-bounds).

**PPL function not supported?** See [`docs/velox-function-support.md`](docs/velox-function-support.md) for the full coverage matrix. Unsupported functions cause a per-query fallback; the plan is otherwise unchanged.

## Further Reading

- [`docs/configuration.md`](docs/configuration.md) — full settings reference for engine, MPP, runtime filters, CBO, Co-Routing, and backpressure / memory bounds.
- [`docs/running.md`](docs/running.md) — single-node, multi-node, and Docker walkthroughs for running the plugin locally.
- [`docs/architecture.md`](docs/architecture.md) — internal design: end-to-end query flow, plugin integration model, MPP, fragment dispatch, data-node pipeline, threading, backpressure internals, join column-name disambiguation, query profiling wire format, comparison with RFC #4812, project structure, supported-operator conversion tables.
- [`docs/velox-function-support.md`](docs/velox-function-support.md) — PPL → Velox function coverage matrix.
- [RFC #4812](https://github.com/opensearch-project/sql/issues/4812) — upstream design reference for the OLAP architecture.

## Security

If you discover a potential security issue in this project we ask that you notify OpenSearch Security directly via email to security@opensearch.org. Please do **not** create a public GitHub issue.

## License

This code is licensed under the Apache 2.0 License. See [LICENSE.txt](LICENSE.txt).

## Copyright

Copyright OpenSearch Contributors. See [NOTICE](NOTICE.txt) for details.
