# Phase 5 Plan: Benchmark ingestion-kafka on Production Traffic

## Context

- **Source topic:** `parsed-incoming-es-logs-us-east-1a-prod-22` (40 partitions, ~1M msgs/min)
- **Destination topic:** `ingestion-kafka-formatted-logs` (5–7 partitions, matches shard count)
- **Message format:** Mixed flat JSON — jaeger spans, audit logs, engine access logs. No `_id` field in messages. No envelope format.
- **Cluster:** OpenSearch 3.6.0, existing production cluster
- **New nodes:** 3–5 dedicated nodes with `node.attr.box_type: kafka` for isolation
- **Baseline template:** Account `300` — 7 shards, custom `logzio` analyzer, `index.mapping.total_fields.limit: 10000`
- **Comparison:** ingestion-kafka `assign` vs `subscribe` vs `share` (not HTTP/gRPC — those are the existing push path baseline)
- **Plugin source:** `logzio/OpenSearch` fork, branch `feature/ingestion-kafka-upgrade-4.2-3.6.0`, produces `ingestion-kafka-3.6.0.zip`
- **Key constraint:** Source has 40 partitions, but destination topic has 5–7 partitions. The wrapper fans-in 40→N. For assign mode, `number_of_shards` must equal destination partition count.

---

## Architecture

```
parsed-incoming-es-logs-us-east-1a-prod-22   (40 partitions, raw JSON, ~1M msgs/min)
        │
        ▼
┌───────────────────────────┐
│  Kafka Message            │   Standalone Java app (consumer group → transformer → producer)
│  Wrapper Service          │   Runs as K8s deployment or Docker container
│                           │   - Consumes raw JSON from 40-partition source topic
│                           │   - Wraps as: {"_op_type":"index","_source":{...original...}}
│                           │   - Generates _id from logzio-signature or SHA-256
│                           │   - Produces to N-partition target topic (fan-in: 40 → N)
│                           │   - Partition key: _id (consistent hashing across N partitions)
└───────────┬───────────────┘
            │
            ▼
ingestion-kafka-formatted-logs   (5–7 partitions, envelope format)
            │
            ▼
┌───────────────────────────────────────────────────────┐
│  OpenSearch Cluster (3.6.0)                           │
│                                                       │
│  Dedicated nodes: box_type=kafka (3–5 nodes)          │
│  Plugin: ingestion-kafka-3.6.0.zip (from fork)        │
│                                                       │
│  Index: ingestion-kafka-benchmark-{mode}              │
│    - Assign mode:    shards = partition count (N)     │
│    - Subscribe mode: shards >= partition count        │
│    - Share mode:     shards independent               │
│    - index.routing.allocation.require.box_type: kafka │
│    - Settings from template 300 (analyzer, mappings)  │
└───────────────────────────────────────────────────────┘
```

**Why the wrapper is required (not optional):**
The source topic has 40 partitions. With 3–5 nodes and `total_shards_per_node: 3`, max shards = 15 (with replicas) or 15 primaries. Assign mode requires `shards == partitions`. Creating a 40-shard index on 5 nodes is too dense. The wrapper fans-in 40 → 5–7 partitions on a new topic, giving a shard count that fits the node capacity. The wrapper also formats messages into the ingestion-kafka envelope format.

---

## Step-by-Step Plan

### Step 1: Build the Plugin ZIP

**Where:** Local machine or CI
**Branch:** `feature/ingestion-kafka-upgrade-4.2-3.6.0` on `logzio/OpenSearch` fork

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"
git checkout feature/ingestion-kafka-upgrade-4.2-3.6.0
./gradlew :plugins:ingestion-kafka:assemble
ls -la plugins/ingestion-kafka/build/distributions/ingestion-kafka-3.6.0.zip
```

**Output:** `ingestion-kafka-3.6.0.zip` (~1.5MB)

**Validation:** Verify version matches cluster:
```bash
unzip -p ingestion-kafka-3.6.0.zip ingestion-kafka/plugin-descriptor.properties | grep opensearch.version
# Expected: opensearch.version=3.6.0
```

---

### Step 2: Provision Dedicated Nodes

**Where:** Kubernetes / infrastructure provisioning

Add 3–5 new data nodes to the existing cluster with:

```yaml
# opensearch.yml additions for kafka-dedicated nodes
node.attr.box_type: kafka
node.roles: [data]
```

**Plugin installation** (at node provisioning time, before OpenSearch starts):

```bash
bin/opensearch-plugin install file:///path/to/ingestion-kafka-3.6.0.zip
```

**Validation after nodes join:**
```bash
curl -s localhost:9200/_cat/nodeattrs?v&h=node,attr,value | grep box_type
# Expected: 3-5 rows with box_type=kafka
```

---

### Step 3: Create the Wrapper Topic

Create target topic with partition count matching desired shard count (5–7):

```bash
kafka-topics.sh --bootstrap-server $KAFKA_BROKERS \
  --create --topic ingestion-kafka-formatted-logs \
  --partitions 7 --replication-factor 3
```

**Why 7 partitions:** Matches the production shard count from template 300. With 5 nodes and `total_shards_per_node: 3`, 7 primaries + 7 replicas = 14 shards fits within 5 × 3 = 15 capacity.

---

### Step 4: Build and Deploy the Kafka Message Wrapper

**Purpose:** Consume from `parsed-incoming-es-logs-us-east-1a-prod-22`, wrap each message in the ingestion-kafka envelope format, produce to `ingestion-kafka-formatted-logs`.

**File:** `benchmark/wrapper/src/main/java/io/logz/kafka/wrapper/KafkaMessageWrapper.java`

**Logic:**
```
Configuration:
  - sourceConsumer = new KafkaConsumer(SOURCE_BOOTSTRAP_SERVERS, GROUP_ID, ...)
  - targetProducer = new KafkaProducer(TARGET_BOOTSTRAP_SERVERS, ...)
  - sourceConsumer.subscribe(SOURCE_TOPIC)

For each consumed record:
  1. Parse value as JSON
  2. Generate _id as composite SHA-256 to guarantee uniqueness:
     _id = SHA-256( logzio-signature + "@timestamp" + source-partition + source-offset )
       - logzio-signature: from JSON field (identifies log pattern)
       - @timestamp: from JSON field (differentiates instances)
       - source-partition: record.partition() (Kafka source partition)
       - source-offset: record.offset() (unique within partition)
     Truncate to 20 hex chars for compact _id.
     Fallback if fields missing:
       _id = SHA-256( source-partition + ":" + source-offset )
       (partition:offset is globally unique per topic — guaranteed by Kafka)
  3. Build envelope:
     {
       "_id": "<sha256_hex_20>",
       "_op_type": "index",
       "_source": <original JSON>
     }
  4. Produce to TARGET_TOPIC on TARGET_BOOTSTRAP_SERVERS
     - Partition key: _id bytes (consistent hashing across N target partitions)
```

**Why this _id strategy:**
- `logzio-signature` alone is NOT unique — it's a pattern hash, shared by many messages
- `partition:offset` alone IS unique per topic, but not idempotent across replays from different topics
- The composite `logzio-signature + @timestamp + partition + offset` is both unique AND deterministic — reprocessing the same Kafka message always produces the same `_id`, enabling deduplication

**Configuration (env vars):**

| Env Var                    | Value                                             |
| -------------------------- | ------------------------------------------------- |
| `SOURCE_TOPIC`             | `parsed-incoming-es-logs-us-east-1a-prod-22`      |
| `SOURCE_BOOTSTRAP_SERVERS` | Source Kafka broker list                          |
| `TARGET_TOPIC`             | `ingestion-kafka-formatted-logs`                  |
| `TARGET_BOOTSTRAP_SERVERS` | Target Kafka broker list (can differ from source) |
| `GROUP_ID`                 | `opensearch-wrapper-benchmark`                    |
| `ID_FIELD`                 | `logzio-signature`                                |

**The wrapper is required** because the source topic has 40 partitions and we need 5–7 for the target. Direct consumption from the 40-partition topic would require 40 shards, which doesn't fit on 3–5 nodes with `total_shards_per_node: 3`.

#### Wrapper Deployment to Kubernetes

**Docker image:**

```dockerfile
# benchmark/wrapper/Dockerfile
FROM eclipse-temurin:21-jre-alpine
COPY build/libs/kafka-message-wrapper.jar /app/wrapper.jar
ENTRYPOINT ["java", "-jar", "/app/wrapper.jar"]
```

**Build and push:**
```bash
cd benchmark/wrapper
./gradlew shadowJar
docker build -t <registry>/kafka-message-wrapper:latest .
docker push <registry>/kafka-message-wrapper:latest
```

**Kubernetes Deployment:**

```yaml
# benchmark/wrapper/k8s/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-message-wrapper
  namespace: opensearch-benchmark
  labels:
    app: kafka-message-wrapper
spec:
  replicas: 4  # 4 instances × 10 partitions each = 40 source partitions covered
  selector:
    matchLabels:
      app: kafka-message-wrapper
  template:
    metadata:
      labels:
        app: kafka-message-wrapper
    spec:
      containers:
        - name: wrapper
          image: <registry>/kafka-message-wrapper:latest
          resources:
            requests:
              cpu: "500m"
              memory: "512Mi"
            limits:
              cpu: "1000m"
              memory: "1Gi"
          env:
            - name: SOURCE_TOPIC
              value: "parsed-incoming-es-logs-us-east-1a-prod-22"
            - name: SOURCE_BOOTSTRAP_SERVERS
              value: "<SOURCE_KAFKA_BROKERS>"
            - name: TARGET_TOPIC
              value: "ingestion-kafka-formatted-logs"
            - name: TARGET_BOOTSTRAP_SERVERS
              value: "<TARGET_KAFKA_BROKERS>"
            - name: GROUP_ID
              value: "opensearch-wrapper-benchmark"
            - name: ID_FIELD
              value: "logzio-signature"
            - name: JAVA_OPTS
              value: "-Xms256m -Xmx512m"
          livenessProbe:
            exec:
              command: ["sh", "-c", "kill -0 1"]
            initialDelaySeconds: 30
            periodSeconds: 10
          readinessProbe:
            exec:
              command: ["sh", "-c", "kill -0 1"]
            initialDelaySeconds: 10
            periodSeconds: 10
      restartPolicy: Always
```

**Scaling notes:**
- **4 replicas** with consumer group `opensearch-wrapper-benchmark` — Kafka distributes 40 source partitions across 4 instances (10 partitions each)
- Each instance consumes ~250K msgs/min (1M/4), transforms, and produces to 7-partition target
- Scale up replicas if wrapper lag on source topic grows; max useful replicas = 40 (1 per partition)
- Consumer group rebalance handles instance add/remove automatically

**Monitoring:**
```bash
# Check wrapper consumer lag on source topic
kafka-consumer-groups.sh --bootstrap-server $SOURCE_KAFKA_BROKERS \
  --group opensearch-wrapper-benchmark --describe

# Check target topic is receiving data
kafka-console-consumer.sh --bootstrap-server $TARGET_KAFKA_BROKERS \
  --topic ingestion-kafka-formatted-logs --max-messages 1 --from-beginning
```

**Teardown after benchmark:**
```bash
kubectl delete deployment kafka-message-wrapper -n opensearch-benchmark
kafka-topics.sh --bootstrap-server $TARGET_KAFKA_BROKERS --delete --topic ingestion-kafka-formatted-logs
```

---

### Step 5: Create Index Template for ingestion-kafka

Based on template `300`, adapted for ingestion-kafka:

```json
PUT _index_template/ingestion-kafka-benchmark
{
  "index_patterns": ["ingestion-kafka-benchmark-*"],
  "priority": 100,
  "template": {
    "settings": {
      "index.number_of_shards": 7,
      "index.number_of_replicas": 1,
      "index.replication.type": "SEGMENT",
      "index.routing.allocation.require.box_type": "kafka",
      "index.routing.allocation.total_shards_per_node": 3,
      "index.allocation.max_retries": 10,
      "index.codec": "zstd",
      "index.codec.compression_level": 6,
      "index.refresh_interval": "30s",
      "index.mapping.total_fields.limit": 10000,
      "index.mapping.depth.limit": 25,
      "index.priority": 10,
      "index.requests.cache.enable": true,
      "index.query.default_field": ["logzio_content"],
      "index.analysis.analyzer.logzio.type": "custom",
      "index.analysis.analyzer.logzio.tokenizer": "keyword",
      "index.analysis.analyzer.logzio.char_filter": ["dot_replace"],
      "index.analysis.analyzer.logzio.filter": ["word_splitter", "lowercase", "token_length"],
      "index.analysis.analyzer.search_quote.type": "custom",
      "index.analysis.analyzer.search_quote.tokenizer": "keyword",
      "index.analysis.analyzer.search_quote.filter": ["word_splitter", "lowercase"],
      "index.analysis.char_filter.dot_replace.type": "pattern_replace",
      "index.analysis.char_filter.dot_replace.pattern": "\\.",
      "index.analysis.char_filter.dot_replace.replacement": " ",
      "index.analysis.filter.token_length.type": "length",
      "index.analysis.filter.token_length.min": 1,
      "index.analysis.filter.token_length.max": 100,
      "index.analysis.filter.word_splitter.type": "word_delimiter",
      "index.analysis.filter.word_splitter.generate_number_parts": true,
      "index.analysis.filter.word_splitter.preserve_original": true,
      "index.analysis.filter.word_splitter.split_on_numerics": false,
      "index.analysis.filter.word_splitter.stem_english_possessive": false,
      "index.indexing.slowlog.threshold.index.trace": "500ms",
      "index.indexing.slowlog.threshold.index.debug": "2s",
      "index.indexing.slowlog.threshold.index.info": "5s",
      "index.indexing.slowlog.threshold.index.warn": "10s",
      "index.search.slowlog.threshold.query.trace": "500ms",
      "index.search.slowlog.threshold.query.debug": "2s",
      "index.search.slowlog.threshold.query.info": "5s",
      "index.search.slowlog.threshold.query.warn": "10s",
      "index.search.slowlog.threshold.fetch.trace": "200ms",
      "index.search.slowlog.threshold.fetch.debug": "500ms",
      "index.search.slowlog.threshold.fetch.info": "800ms",
      "index.search.slowlog.threshold.fetch.warn": "1s"
    }
  }
}
```

**Matches production index settings exactly**, with these changes:
- `number_of_shards: 40` (was 7 — matches Kafka partition count for assign mode)
- `routing.allocation.require.box_type: kafka` (was `include.box_type: default,ingestion`)
- `replication.type: SEGMENT` (already SEGMENT in prod — required for ingestion-kafka)
- Preserved: `codec: zstd` level 6, `refresh_interval: 30s`, `total_shards_per_node: 3`, all slowlog settings, all analyzers

---

### Step 5.5: Monitoring Stack Setup

Three complementary monitoring sources — no custom polling scripts needed during the benchmark runs.

#### Source 1: Prometheus Elasticsearch Exporter (already deployed)

Covers cluster, node, and index-level metrics. Key metrics to watch:

| Prometheus Metric                                         | What It Tells You                           |
| --------------------------------------------------------- | ------------------------------------------- |
| `elasticsearch_indices_indexing_index_total`              | Cumulative docs indexed (rate = throughput) |
| `elasticsearch_indices_indexing_index_time_seconds_total` | Indexing latency                            |
| `elasticsearch_os_cpu_percent`                            | CPU per node                                |
| `elasticsearch_jvm_memory_used_bytes{area="heap"}`        | Heap usage per node                         |
| `elasticsearch_jvm_gc_collection_seconds_count`           | GC frequency                                |
| `elasticsearch_indices_segments_count`                    | Segment count per index                     |
| `elasticsearch_indices_flush_total`                       | Flush count                                 |
| `elasticsearch_indices_merges_total`                      | Merge activity                              |
| `elasticsearch_indices_refresh_time_seconds_total`        | Refresh cost                                |
| `elasticsearch_thread_pool_rejected_count{name="write"}`  | Backpressure indicator                      |

**Grafana dashboard filter:** Add a variable for `index=~"ingestion-kafka-benchmark-.*"` to isolate benchmark indices from production.

#### Source 2: klag-exporter (already deployed)

Covers Kafka consumer group lag — works for **subscribe mode** only (assign has no group, share uses a different protocol).

| Prometheus Metric                                                                       | What It Tells You                              |
| --------------------------------------------------------------------------------------- | ---------------------------------------------- |
| `kafka_consumer_group_lag{group="<index_uuid>",topic="ingestion-kafka-formatted-logs"}` | Per-partition lag for subscribe consumer group |
| `kafka_consumer_group_offset{group="<index_uuid>"}`                                     | Current consumed offset                        |
| `kafka_consumer_group_lag{group="opensearch-wrapper-benchmark"}`                        | Wrapper lag on source topic (all modes)        |

**Note:** The subscribe mode `group.id` is auto-generated from the index UUID. After creating the index, find it:
```bash
curl -s localhost:9200/ingestion-kafka-benchmark-subscribe/_settings | jq '.. | .index.uuid? // empty'
```

#### Source 3: Ingestion State API Polling Script (new, lightweight)

Covers ingestion-kafka specific metrics not exposed to Prometheus: per-shard lag, poller state, error counts. Runs during benchmark only.

```bash
# benchmark/harness/collect-ingestion-metrics.sh
#!/bin/bash
INDEX_NAME=${1:-ingestion-kafka-benchmark-assign}
INTERVAL=${2:-10}
OS_HOST=${3:-localhost:9200}
OUTFILE="/tmp/ingestion-metrics-$(echo $INDEX_NAME | sed 's/.*-//').tsv"

echo -e "timestamp\tshard\tstate\tlag\tpolled\tdropped" > "$OUTFILE"

while true; do
  TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  curl -s "$OS_HOST/_ingestion/$INDEX_NAME/_state" | \
    jq -r --arg ts "$TS" \
    '.shards[] | [$ts, .shard_id, .poller_state, .pointer_based_lag,
     .stats.consumer_stats.total_polled_count,
     .stats.consumer_stats.total_poller_message_dropped_count] | @tsv' \
    >> "$OUTFILE"
  sleep "$INTERVAL"
done
```

**Usage (run in background during each benchmark):**
```bash
# Start before index creation
./collect-ingestion-metrics.sh ingestion-kafka-benchmark-assign 10 &
METRICS_PID=$!

# ... run benchmark for 30 min ...

# Stop after benchmark
kill $METRICS_PID
```

#### Metric Coverage Matrix

| Metric                              | ES Exporter | klag-exporter | Ingestion API Script |
| ----------------------------------- | :---------: | :-----------: | :------------------: |
| Indexing throughput (docs/sec)      |      x      |               |                      |
| CPU per node                        |      x      |               |                      |
| Heap / GC                           |      x      |               |                      |
| Segment count / merge time          |      x      |               |                      |
| Refresh time                        |      x      |               |                      |
| Thread pool rejections              |      x      |               |                      |
| Consumer group lag (subscribe)      |             |       x       |                      |
| Wrapper lag on source topic         |             |       x       |                      |
| Per-shard ingestion lag (all modes) |             |               |          x           |
| Poller state                        |             |               |          x           |
| Polled / dropped count              |             |               |          x           |

---

### Step 6: Run Benchmark — Assign Mode

```json
PUT ingestion-kafka-benchmark-assign
{
  "settings": {
    "ingestion_source.type": "kafka",
    "ingestion_source.param.topic": "ingestion-kafka-formatted-logs",
    "ingestion_source.param.bootstrap_servers": "<KAFKA_BROKERS>",
    "ingestion_source.param.consumer_mode": "assign",
    "ingestion_source.pointer.init.reset": "latest",
    "ingestion_source.error_strategy": "drop",
    "ingestion_source.num_processor_threads": 2
  }
}
```

**Note:** `number_of_shards` (7 from template) MUST equal destination topic partition count (7) for assign mode. The wrapper already formatted the messages — no mapper config needed.

Run for 30 min. Metrics collected via monitoring stack (see Step 5.5). Then pause and delete:
```bash
POST /_ingestion/ingestion-kafka-benchmark-assign/_pause
DELETE ingestion-kafka-benchmark-assign
```

---

### Step 7: Run Benchmark — Subscribe Mode

```json
PUT ingestion-kafka-benchmark-subscribe
{
  "settings": {
    "ingestion_source.type": "kafka",
    "ingestion_source.param.topic": "ingestion-kafka-formatted-logs",
    "ingestion_source.param.bootstrap_servers": "<KAFKA_BROKERS>",
    "ingestion_source.param.consumer_mode": "subscribe",
    "ingestion_source.pointer.init.reset": "latest",
    "ingestion_source.error_strategy": "drop",
    "ingestion_source.num_processor_threads": 2
  }
}
```

**Note:** Shards (7) >= partitions (7) — each consumer gets 1 partition. `group.id` auto-generated from index UUID. `group.protocol=consumer` set automatically. klag-exporter will pick up the consumer group lag automatically.

Run for 30 min. Then pause and delete:
```bash
POST /_ingestion/ingestion-kafka-benchmark-subscribe/_pause
DELETE ingestion-kafka-benchmark-subscribe
```

---

### Step 8: Run Benchmark — Share Mode

**Prerequisite:** Kafka brokers must have share groups enabled:
```bash
kafka-features.sh --bootstrap-server $KAFKA_BROKERS upgrade --feature share.version=1
```

```json
PUT ingestion-kafka-benchmark-share
{
  "settings": {
    "ingestion_source.type": "kafka",
    "ingestion_source.param.topic": "ingestion-kafka-formatted-logs",
    "ingestion_source.param.bootstrap_servers": "<KAFKA_BROKERS>",
    "ingestion_source.param.consumer_mode": "share",
    "ingestion_source.param.group.id": "opensearch-share-benchmark",
    "ingestion_source.error_strategy": "drop",
    "ingestion_source.num_processor_threads": 2,
    "index.number_of_shards": 3
  }
}
```

**Note:** `number_of_shards` (3) is intentionally less than partition count (7) — this is the whole point of share mode: more partitions than consumers. No `pointer.init.reset` (not supported in share mode).

**Warning:** Share mode requires Kafka 4.2 brokers with `share.version=1`. If your production Kafka is < 4.2, share mode cannot be tested on production. Test on a separate Kafka cluster or skip this step.

**Note:** klag-exporter won't show share group lag (different protocol). Rely on the ingestion API script (`pointer_based_lag` returns -1 for share mode — monitor polled/dropped counts instead).

Run for 30 min. Then pause and delete:
```bash
POST /_ingestion/ingestion-kafka-benchmark-share/_pause
DELETE ingestion-kafka-benchmark-share
```

---

### Step 9: Collect Baseline (Existing Push Path)

For comparison, collect the same metrics from the existing push-based index that consumes from the same data:

```bash
# Identify the current index for account 300
curl -s localhost:9200/_cat/indices/logz-jopnbwmknooanqwzxgpybunufztysazs-*?v&h=index,docs.count,store.size,indexing.index_total

# Collect 30 min of stats from the existing index
curl -s localhost:9200/logz-jopnbwmknooanqwzxgpybunufztysazs-*/_stats/indexing | jq '.indices[].total.indexing'
```

This gives the push-path baseline without changing anything.

---

### Step 10: Generate Results Report

| Metric                  | Source                                       | Push (baseline) | Assign | Subscribe | Share |
| ----------------------- | -------------------------------------------- | --------------- | ------ | --------- | ----- |
| Docs indexed (30 min)   | ES exporter                                  |                 |        |           |       |
| Throughput (docs/sec)   | ES exporter `rate(indexing_index_total[1m])` |                 |        |           |       |
| CPU avg (kafka nodes)   | ES exporter `os_cpu_percent`                 |                 |        |           |       |
| Heap avg (kafka nodes)  | ES exporter `jvm_memory_used_bytes`          |                 |        |           |       |
| GC pauses               | ES exporter `jvm_gc_collection_seconds`      |                 |        |           |       |
| Write rejections        | ES exporter `thread_pool_rejected{write}`    | N/A             |        |           |       |
| Consumer group lag      | klag-exporter                                | N/A             | N/A    |           | N/A   |
| Wrapper lag (source)    | klag-exporter `group=wrapper`                | N/A             |        |           |       |
| Per-shard ingestion lag | Ingestion API script                         | N/A             |        |           | -1    |
| Poller state            | Ingestion API script                         | N/A             |        |           |       |
| Polled count            | Ingestion API script                         | N/A             |        |           |       |
| Dropped count           | Ingestion API script                         | N/A             |        |           |       |
| Segment count           | ES exporter `segments_count`                 |                 |        |           |       |
| Merge time              | ES exporter `merges_total_time`              |                 |        |           |       |
| Refresh time            | ES exporter `refresh_time_seconds`           |                 |        |           |       |

---

## Risks and Mitigations

| Risk                                              | Mitigation                                                                                   |
| ------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| Plugin version mismatch (3.6.0 vs cluster)        | Verify `plugin-descriptor.properties` before install                                         |
| Mixed message types cause mapping explosion       | `total_fields.limit: 10000` + `error_strategy: drop`                                         |
| Share mode requires Kafka 4.2 brokers             | Skip share test if prod Kafka < 4.2, or use separate cluster                                 |
| Wrapper throughput bottleneck                     | Scale wrapper instances; monitor consumer lag on source topic                                |
| 7 shards + 7 replicas on 5 nodes (14 total)       | Fits within `total_shards_per_node: 3` (5×3=15)                                              |
| `logzio-signature` missing on some messages       | Auto-generated `_id` for those docs — acceptable                                             |
| Consuming from prod topic affects other consumers | ingestion-kafka uses `assign` (no group) or separate group — no impact on existing consumers |

---

## Shard Count Strategy

| Mode      | Shards | Partitions | Why                                                            |
| --------- | ------ | ---------- | -------------------------------------------------------------- |
| Assign    | 7      | 7          | Must equal partition count (destination topic)                 |
| Subscribe | 7      | 7          | shards >= partitions, 1:1 optimal                              |
| Share     | 3      | 7          | Intentionally fewer shards than partitions — proves decoupling |

---

## Execution Order

| Step | What                                      | Duration | Depends On             |
| ---- | ----------------------------------------- | -------- | ---------------------- |
| 1    | Build plugin ZIP                          | 5 min    | Branch ready           |
| 2    | Provision nodes + install plugin          | 30 min   | Step 1                 |
| 3    | Create wrapper topic (if using wrapper)   | 2 min    | Kafka access           |
| 4    | Deploy wrapper OR configure field_mapping | 15 min   | Step 3                 |
| 5    | Create index template                     | 2 min    | Step 2                 |
| 6    | **Benchmark: assign mode**                | 30 min   | Steps 2, 4, 5          |
| 7    | **Benchmark: subscribe mode**             | 30 min   | Step 6 done            |
| 8    | **Benchmark: share mode**                 | 30 min   | Step 7 done, Kafka 4.2 |
| 9    | Collect push baseline                     | 30 min   | Can run in parallel    |
| 10   | Generate report                           | 30 min   | Steps 6-9              |

**Total:** ~3-4 hours of active work (excluding node provisioning wait time)

---

## Files to Create

```
benchmark/
  plan.md                          ← this file
  plugin/
    build-plugin.sh                # Builds ZIP from fork
  wrapper/
    src/main/java/io/logz/kafka/wrapper/KafkaMessageWrapper.java
    build.gradle                   # Standalone Gradle build with shadowJar
    Dockerfile
    k8s/
      namespace.yaml               # opensearch-benchmark namespace
      deployment.yaml               # 4 replicas, env vars, resource limits
  harness/
    create-template.sh             # Creates index template
    run-assign.sh                  # Creates assign index, runs 30 min
    run-subscribe.sh               # Creates subscribe index, runs 30 min
    run-share.sh                   # Creates share index, runs 30 min
    collect-ingestion-metrics.sh   # Ingestion API polling (lag, poller state, polled/dropped)
    cleanup.sh                     # Pauses + deletes benchmark indices, scales down wrapper
  config/
    index-settings-assign.json
    index-settings-subscribe.json
    index-settings-share.json
    index-template.json
  results/
    README.md                      # Where to put results
```
