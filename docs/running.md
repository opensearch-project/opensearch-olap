# Running / Manual Testing

Hands-on walkthroughs for running the OpenSearch OLAP plugin locally. Covers single-node and multi-node setups, plus a Docker recipe for hosts whose Velox native libraries aren't compatible.

Prerequisites — build and install the plugin first. See the [README](../README.md) for Build and Install.

## Single-Node Testing

### 1. Start OpenSearch

```bash
cd ../OpenSearch/build/distribution/local/opensearch-3.7.0-SNAPSHOT
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

### 2. Create test index and insert data

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

### 3. Run queries through Velox

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

### 3b. Run join queries (requires two indices)

```bash
# Create a departments index
curl -s -X PUT "http://localhost:9200/departments" \
  -H "Content-Type: application/json" \
  -d '{"mappings": {"properties": {"dept_id": {"type": "integer"}, "dept_name": {"type": "keyword"}}}}'

# Insert departments
curl -s -X POST "http://localhost:9200/_bulk?refresh=true" \
  -H "Content-Type: application/json" \
  -d '{"index":{"_index":"departments"}}
{"dept_id": 1, "dept_name": "Engineering"}
{"index":{"_index":"departments"}}
{"dept_id": 2, "dept_name": "Marketing"}
'

# Add dept_id to test_olap (recreate with dept_id field)
# ... then run join queries:

# Inner join using PPL
curl -s -X POST "http://localhost:9200/_plugins/_ppl" \
  -H "Content-Type: application/json" \
  -d '{"query": "source = employees | inner join left=e right=d ON e.dept_id = d.dept_id departments | fields e.name, d.dept_name"}'

# Join + aggregation
curl -s -X POST "http://localhost:9200/_plugins/_ppl" \
  -H "Content-Type: application/json" \
  -d '{"query": "source = employees | inner join left=e right=d ON e.dept_id = d.dept_id departments | stats count() by d.dept_name"}'
```

### 4. Verify OLAP plugin handled the query

Check logs for the Velox execution path:

```bash
grep -E "Routing query to extension|Executing query.*Velox|Executing fragment|Dispatching stage|Cannot vectorize" logs/opensearch.log | tail -10
```

Expected log sequence when the query is handled by Velox:

```
[o.o.s.e.DelegatingExecutionEngine] Routing query to extension engine : VectorizedEngineExtension
[o.o.p.o.e.VeloxExecutionEngine]    Executing query <id> via Velox engine
[o.o.p.o.s.QueryScheduler]          Dispatching stage <id> with 1 tasks
[o.o.p.o.t.TransportExecuteFragmentAction] Executing fragment 0 for query <id> on 1 shards
```

If the query falls back to the default engine:

```
[o.o.p.o.e.VectorizedEngineExtension] Cannot vectorize plan: unsupported node [<class>] in <plan>
```

If no OLAP plugin log appears at all, `canVectorize()` returned `false` because Velox is unavailable — check for `Velox engine unavailable` at startup.

### 5. Enable debug logging (optional)

```bash
curl -s -X PUT "http://localhost:9200/_cluster/settings" \
  -H "Content-Type: application/json" \
  -d '{"transient": {"logger.org.opensearch.plugin.olap": "DEBUG"}}'
```

### 6. Reinstall after code changes

```bash
# Stop OpenSearch
kill $(cat /tmp/opensearch.pid)

# Rebuild
cd /path/to/opensearch-olap
./gradlew assemble

# Reinstall
cd ../OpenSearch/build/distribution/local/opensearch-3.7.0-SNAPSHOT
bin/opensearch-plugin remove opensearch-olap
bin/opensearch-plugin install file:///absolute/path/to/opensearch-olap/build/distributions/opensearch-olap-3.7.0-SNAPSHOT.zip

# Restart
bin/opensearch -d -p /tmp/opensearch.pid
```

## Multi-Node Testing

Tests distributed execution where fragments are dispatched to remote data nodes via TransportService.

### 1. Set up a 2-node local cluster

Copy the existing build to create a second node:

```bash
BASE=../OpenSearch/build/distribution/local
cp -r "$BASE/opensearch-3.7.0-SNAPSHOT" "$BASE/opensearch-node2"
```

Configure node 1 (`$BASE/opensearch-3.7.0-SNAPSHOT/config/opensearch.yml`):

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
rm -rf $BASE/opensearch-3.7.0-SNAPSHOT/data/*
rm -rf $BASE/opensearch-node2/data/*
rm -rf /tmp/opensearch-*
```

### 2. Start both nodes

```bash
cd $BASE/opensearch-3.7.0-SNAPSHOT && bin/opensearch -d -p /tmp/opensearch-node1.pid
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

### 3. Create test index with multiple shards

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

### 4. Run distributed queries

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

### 5. Verify distributed execution in logs

Check that both nodes participated in query execution:

```bash
# Node 1 (coordinator): should show routing + scheduling + local fragment execution
grep -E "Routing query|Dispatching stage|Executing fragment" \
  $BASE/opensearch-3.7.0-SNAPSHOT/logs/olap-test-cluster.log | tail -5

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

The log file is named `olap-test-cluster.log` (matching `cluster.name` in the config), not `opensearch.log`.

### 6. Stop the cluster

```bash
kill $(cat /tmp/opensearch-node1.pid) $(cat /tmp/opensearch-node2.pid)
rm -rf /tmp/opensearch-*
```

## Multi-Node Testing (Docker)

When the host platform cannot run the Velox native libraries (e.g., the `libvelox.so` in the Maven-published velox4j jar was built on CentOS 7), use a CentOS 7 Docker container to run the cluster.

### 1. Build the OLAP plugin on the host

```bash
./gradlew clean assemble
```

### 2. Start a CentOS 7 container with volume mounts

```bash
docker run --init -d --name olap-test \
  -v /path/to/OpenSearch/build/distribution/local/opensearch-3.7.0-SNAPSHOT:/opensearch-src:ro \
  -v /path/to/opensearch-olap/build/distributions:/olap-plugin:ro \
  -v /path/to/search-plugins-sql/plugin/build/distributions:/sql-plugin:ro \
  -v /path/to/job-scheduler/build/distributions:/job-scheduler-plugin:ro \
  -p 9200:9200 -p 9201:9201 \
  centos:7 sleep infinity
```

### 3. Install Java and set up the cluster inside the container

```bash
docker exec olap-test bash -c '
  # Fix CentOS 7 EOL mirrors
  sed -i -e "s|mirrorlist=|#mirrorlist=|g" /etc/yum.repos.d/CentOS-*.repo
  sed -i -e "s|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g" /etc/yum.repos.d/CentOS-*.repo
  yum install -y tar

  # Install Amazon Corretto 21
  rpm --import https://yum.corretto.aws/corretto.key
  curl -sLo /etc/yum.repos.d/corretto.repo https://yum.corretto.aws/corretto.repo
  yum install -y java-21-amazon-corretto-devel

  # Copy OpenSearch for 2 nodes
  cp -r /opensearch-src /opensearch-node1
  cp -r /opensearch-src /opensearch-node2

  # Install plugins on both nodes
  for node in /opensearch-node1 /opensearch-node2; do
    rm -rf "$node/plugins/opensearch-sql" "$node/plugins/opensearch-olap" \
           "$node/plugins/opensearch-job-scheduler" "$node/data"
    "$node/bin/opensearch-plugin" install -b file:///job-scheduler-plugin/opensearch-job-scheduler-3.7.0.0-SNAPSHOT.zip
    "$node/bin/opensearch-plugin" install -b file:///sql-plugin/opensearch-sql-3.7.0.0-SNAPSHOT.zip
    "$node/bin/opensearch-plugin" install -b file:///olap-plugin/opensearch-olap-3.7.0-SNAPSHOT.zip
  done

  # Configure node 1
  cat > /opensearch-node1/config/opensearch.yml << EOF
cluster.name: olap-test-cluster
node.name: node-1
network.host: 0.0.0.0
http.port: 9200
transport.port: 9300
discovery.seed_hosts: ["127.0.0.1:9300", "127.0.0.1:9301"]
cluster.initial_cluster_manager_nodes: ["node-1", "node-2"]
EOF

  # Configure node 2
  cat > /opensearch-node2/config/opensearch.yml << EOF
cluster.name: olap-test-cluster
node.name: node-2
network.host: 0.0.0.0
http.port: 9201
transport.port: 9301
discovery.seed_hosts: ["127.0.0.1:9300", "127.0.0.1:9301"]
cluster.initial_cluster_manager_nodes: ["node-1", "node-2"]
EOF

  # Create non-root user (OpenSearch refuses to run as root)
  useradd -m opensearch
  chown -R opensearch:opensearch /opensearch-node1 /opensearch-node2

  # Start nodes with staggered timing
  su opensearch -c "/opensearch-node1/bin/opensearch -d -p /tmp/node1.pid"
  sleep 10
  su opensearch -c "/opensearch-node2/bin/opensearch -d -p /tmp/node2.pid"
'
```

### 4. Wait for cluster and run queries

```bash
# Wait for both nodes
curl -s "http://localhost:9200/_cat/nodes?v"

# Create index, insert data, and run queries (same as single-node testing steps 2–3)
```

### 5. Clean up

```bash
docker rm -f olap-test
```
