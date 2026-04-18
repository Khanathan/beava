# Beava: Honest Comparisons

See also: [docs/architecture.md](architecture.md) · [docs/faq.md](faq.md) · [benchmark/](../benchmark/)

Beava is a single-binary real-time feature server. It is smaller than Feast, Flink, and
Redpanda in scope, and it is honest about that. This page is the pairwise tradeoff
reference -- not a marketing comparison. Every claim is either sourced from committed
benchmark data or qualified as an estimate.

**Committed baseline (fraud-pipeline, 47 features, 5 entity types, 10-core Apple M4 laptop):**
- TCP push-batch: 315 K events/sec sustained
- HTTP push-batch: 100 K+ EPS
- Crash recovery: 7 s for 4.7 GB of state

These numbers appear in `benchmark/LAUNCH-VERIFY.md`. Do not treat estimates at other
scales as guarantees.

---

## Beava vs Feast

### What each system is

**Feast** is an open-source feature store that manages the full feature lifecycle:
offline computation (via Spark or BigQuery), online serving (via Redis, DynamoDB, or
Bigtable), feature registries, materialization jobs, and point-in-time joins.

**Beava** is an online-only real-time feature server. It owns the ingest path: you push
events, Beava computes windowed aggregations immediately, and you read features with
sub-millisecond latency. There is no offline store, no materialization job, no
feature registry beyond what is in your Python file.

### Head-to-head

| Dimension | Beava | Feast |
|-----------|-------|-------|
| Offline store | No | Yes (Snowflake, BigQuery, Parquet) |
| Online store | Yes (in-memory, <1ms p99) | Yes (Redis, DynamoDB, Bigtable) |
| Streaming ingest | Yes (native, TCP + HTTP) | Yes (via Kafka + Flink connector) |
| Batch materialization | No | Yes (Spark / pandas) |
| Feature registry | Python file = registry | Centralized YAML registry |
| Point-in-time joins | No | Yes (for ML training) |
| Monitoring / lineage | No | Partial (depends on provider) |
| Ops complexity | 1 binary | 3-5 components per provider |
| Horizontal scale | No (v1.0) | Provider-dependent |

### Where Beava wins

- **No latency gap between compute and serve.** Feast materializes batch features
  asynchronously; there is always a lag between when the event happens and when the
  feature is available. Beava computes on-push -- the feature is ready on the next
  GET with no materialization delay.
- **Simpler deployment.** One binary, no feature registry service, no offline store
  to configure.
- **Real-time event-time semantics.** Beava tracks event-time watermarks, late
  arrivals, and per-window buckets natively. Feast's streaming path depends on
  an external Flink job for this.

### Where Feast wins

- **Offline features.** If your features are computed from historical Snowflake or
  BigQuery data, Feast handles this natively. Beava has no offline store.
- **Training dataset generation.** Point-in-time joins for ML training data
  are a core Feast feature. Beava does not produce training datasets.
- **Ecosystem.** Feast integrates with dozens of storage providers and has a mature
  community and enterprise support (Tecton). Beava is early-stage.
- **Feature registry and versioning.** Feast tracks feature versions, ownership, and
  lineage. Beava's "registry" is your Python file.

### When to pick Beava

Pick Beava if your features are **computed over event streams** and you need them
**served at sub-millisecond latency**. If your features are **batch-computed** from
a data warehouse and you need point-in-time joins for training, pick Feast (or Tecton).

The two are not mutually exclusive: some teams run Feast for batch-derived features and
Beava for real-time streaming features.

---

## Beava vs Flink + Redis

### What each system is

**Flink + Redis** is the standard production streaming feature stack: Kafka for ingest,
Flink for stateful computation, Redis for low-latency feature serving. Mature, battle-
tested, horizontally scalable, and genuinely powerful.

**Beava** replaces the entire stack with one binary. The tradeoff is scope vs simplicity.

### Head-to-head

| Dimension | Beava | Flink + Redis |
|-----------|-------|---------------|
| Deployment | 1 binary | Kafka (3+ brokers) + Flink (JM + TMs) + Redis + ZooKeeper: 8-15 nodes |
| Exactly-once | No (at-least-once) | Yes |
| Horizontal scale | No (v1.0) | Yes (Kafka partitions, Flink parallelism) |
| Event-time windows | Yes (native) | Yes (Flink event-time + watermarks) |
| Complex windowing | Sliding windows, per-bucket | Full Flink API: session, tumbling, global |
| SQL | No | Yes (Flink SQL, ksqlDB) |
| Connector ecosystem | TCP + HTTP | Kafka connectors (hundreds) |
| State per key | RAM, ~2 KB/feature/entity | RocksDB (disk-backed, any size) |
| Ops overhead | Near-zero | 0.5-1.0 FTE |

### Where Beava wins

- **Zero ops overhead.** No Kafka tuning, no Flink checkpoint configuration, no
  Redis sentinel setup. Start in 60 seconds.
- **Latency.** Beava reads are in-memory pointer dereferences (<100 µs p99).
  Flink → Redis adds serialization + a network hop for every read.
- **Developer experience.** Define a pipeline in Python, register it, push events.
  No Java, no Avro schemas, no connector YAML.

### Where Flink + Redis wins

- **Scale.** A well-tuned Flink cluster processes millions of events/sec across dozens
  of nodes. Beava is one box.
- **Exactly-once.** Flink provides exactly-once state semantics across distributed
  operators. Beava is at-least-once.
- **Complex windowing.** Session windows, global windows, custom triggers, and temporal
  pattern matching (CEP) are Flink-native. Beava supports sliding windows only.
- **Fault tolerance.** A Flink cluster survives node failures without losing state.
  Beava's fault tolerance is single-process crash recovery (WAL + snapshot).
- **State size.** Flink stores state in RocksDB -- it is not RAM-limited. Beava holds
  all state in memory.

### When to pick Beava

Pick Beava if your team does not have a streaming infrastructure engineer, your event
volume fits on one box (up to 315 K EPS TCP / 100 K+ EPS HTTP on a 10-core laptop),
and your state fits in RAM. Pick Flink + Redis if you need multi-node fault tolerance,
exactly-once, or state that exceeds available RAM.

---

## Beava vs Redpanda / Kafka

### What each system is

**Redpanda** (and Kafka) are durable, distributed message brokers. They ingest events,
store them reliably, and let consumers replay them. They do not compute features --
they are infrastructure that other systems build on top of.

**Beava** is a feature server, not a broker. It is not a replacement for Redpanda or
Kafka; it is a potential consumer of one.

### Head-to-head

| Dimension | Beava | Redpanda / Kafka |
|-----------|-------|------------------|
| Purpose | Feature computation + serving | Durable message transport |
| Durability guarantee | At-least-once (WAL + fsync every 1s) | Configurable (acks=all = strong) |
| Replay / rewind | Limited (per-stream WAL, 72h default TTL) | Unlimited (configurable retention) |
| Consumer groups | Not applicable | Native multi-consumer fan-out |
| SQL / streaming queries | No | Kafka Streams, ksqlDB, Flink SQL |
| Horizontal scale | No (v1.0) | Yes (partitions, replication) |
| Ops complexity | 1 binary | 3+ brokers, ZooKeeper or KRaft |

### Where Beava fits with Redpanda

They are complementary, not competing. A common pattern:

```
Redpanda (durable ingest)  →  Beava consumer  →  feature serving
```

Beava can consume from a Kafka/Redpanda topic (via a small bridge process that reads
from the consumer group and calls `POST /push-batch`). Beava provides the feature
computation and serving layer; Redpanda provides the durable, replayable event log.

### Where Redpanda wins

- **Durability.** Redpanda's replication guarantees far exceed Beava's single-node WAL.
- **Multi-consumer fan-out.** Redpanda lets dozens of consumers independently read the
  same event stream. Beava's event log is single-consumer (for crash recovery only).
- **Long retention.** Beava's default WAL retention is 72 hours. Redpanda can retain
  events indefinitely.
- **Stream processing.** Redpanda has Wasm transforms and integrates with Flink;
  Beava's processing model is limited to its 16 built-in operators.

### When to pick Beava without Redpanda

If your event sources push directly to Beava via HTTP or TCP and you do not need
multi-consumer fan-out or long-term event retention, Beava's own WAL is sufficient.

---

## Beava vs ksqlDB / Materialize / RisingWave

These are **streaming SQL databases** -- incremental view maintenance over streaming
data with SQL interfaces. They are more powerful query models than Beava's operator
API but more complex to operate and reason about.

| System | Model | SQL | Scale | Ops |
|--------|-------|-----|-------|-----|
| Beava | Feature server (16 operators) | No | Single node | 1 binary |
| ksqlDB | Streaming SQL (on Kafka) | Yes | Multi-node | Kafka required |
| Materialize | Incremental view maintenance | Yes | Distributed | Managed or self-hosted |
| RisingWave | Streaming SQL DB (Postgres wire) | Yes | Cloud-native | Kubernetes |

If your features are naturally expressed as SQL views over event streams, Materialize
or RisingWave are worth evaluating. Beava is simpler to operate for the common case
of keyed aggregations over sliding windows.

---

## Summary: When to Choose What

| Pick this | If you need |
|-----------|-------------|
| **Beava** | Real-time streaming features, single-node, sub-ms reads, simple ops |
| **Feast** | Offline + online feature store, training data, Snowflake/BigQuery integration |
| **Flink + Redis** | Multi-node scale, exactly-once, complex windowing, connector ecosystem |
| **Redpanda** | Durable event transport, multi-consumer, long retention (use alongside Beava) |
| **ksqlDB / Materialize** | SQL-based incremental views, complex query patterns |
| **Tecton / Fennel** | Managed platform, full lifecycle management, enterprise support |

Beava is the right choice when you want **one binary, one API, one mental model** for
real-time feature computation and serving — and your workload fits on one machine.

For a deep dive on Beava's architecture and scaling roadmap, see
[docs/architecture.md](architecture.md).
