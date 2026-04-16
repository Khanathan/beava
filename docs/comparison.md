# Beava vs Flink + Kafka + Redis

## The Problem

Today's real-time feature stack looks like this:

```
Kafka (3+ brokers)  -->  Flink (JobManager + TaskManagers)  -->  Redis (primary + replica)
      |                        |                                       |
  Schema Registry         ZooKeeper / K8s                          Sentinel
      |                        |
  Connect workers         Checkpoint storage (S3/HDFS)
```

That is 18-25 nodes, 5-8 distinct systems, each with its own failure modes, configuration language, and upgrade process. Operating this stack is a 0.5-1.0 FTE job. Not because any individual component is bad -- Kafka and Flink are excellent distributed systems -- but because the assembly is complex by nature.

Most teams running this stack are computing fewer than 100 features at fewer than 100K events per second. They do not need horizontal scalability across dozens of nodes. They need the features to be correct, fast, and easy to change.

That is what Beava is for.

## Side-by-Side: The Same Pipeline, Different Stacks

The benchmark pipeline: a fraud detection system for a mid-size fintech. 5 entity types (user, merchant, device, IP, card), 47 features across 4 window tiers (30m, 1h, 24h, 7d), cross-key lookups, derived signals.

### Beava: ~60 Lines of Python

```python
import beava as bv

@bv.stream
class RawTransactions:
    user_id: str
    merchant_id: str
    amount: float
    country: str

@bv.table(key="user_id")
def UserTransactions(txs: RawTransactions) -> bv.Table:
    return (
        txs.group_by("user_id")
        .agg(
            tx_count_30m=bv.count(window="30m"),
            tx_count_1h=bv.count(window="1h"),
            tx_count_24h=bv.count(window="24h"),
            tx_sum_1h=bv.sum("amount", window="1h"),
            tx_avg_24h=bv.avg("amount", window="24h"),
            tx_max_24h=bv.max("amount", window="24h"),
            unique_merchants_24h=bv.count_distinct("merchant_id", window="24h"),
            unique_countries_24h=bv.count_distinct("country", window="24h"),
            last_country=bv.last("country"),
            last_amount=bv.last("amount"),
        )
        .with_columns(
            velocity_spike=(bv.col("tx_count_1h") / 1) / (bv.col("tx_count_24h") / 24),
            amount_vs_avg=bv.col("last_amount") / bv.col("tx_avg_24h"),
        )
    )

@bv.table(key="merchant_id")
def MerchantActivity(txs: RawTransactions) -> bv.Table:
    return txs.group_by("merchant_id").agg(
        merch_tx_count_24h=bv.count(window="24h"),
        merch_unique_users_24h=bv.count_distinct("user_id", window="24h"),
        merch_avg_amount=bv.avg("amount", window="24h"),
    )

# ... similar for DeviceActivity, IPActivity, UserFailedTxns

app = bv.App("localhost:6400")
app.register(RawTransactions, UserTransactions, MerchantActivity, ...)
features = app.push(RawTransactions, event)  # features returned synchronously
```

Infrastructure: 1 binary, 1 node. Start with `./beava` or `docker compose up`.

### Flink + Kafka + Redis: ~400+ Lines of Java, YAML, and Glue

The equivalent Flink pipeline requires:

1. **Kafka producer** -- serialize events, publish to topic, handle backpressure
2. **Kafka topic configuration** -- partitions, replication factor, retention, compaction
3. **Schema Registry** -- Avro/Protobuf schema, compatibility checks
4. **Flink job (Java/Scala)** -- DataStream API, keyed windows, process functions, custom aggregators for each operator, state backend configuration
5. **Flink state backend** -- RocksDB configuration, checkpointing interval, incremental snapshots
6. **Redis sink** -- custom Flink sink to write computed features, handle connection pooling, retries
7. **Redis read path** -- application code to read features, handle cache misses, TTL management
8. **Deployment** -- Kubernetes manifests or YARN configs for Flink, Kafka broker configs, Redis Sentinel/Cluster setup

Each windowed aggregation in Flink is a custom `ProcessFunction` or `AggregateFunction` with explicit state management. Derived features require a separate computation step. Cross-key lookups require side inputs or async I/O.

Infrastructure: Kafka (3 brokers + ZooKeeper), Flink (1 JobManager + 2-4 TaskManagers), Redis (primary + replica + Sentinel), checkpoint storage (S3 or HDFS). Minimum 10-12 nodes.

## Why the Difference

The gap is not primarily about lines of code. It is about what happens at runtime.

### The JVM Serialization Tax

Flink stores operator state in RocksDB (the recommended production state backend). Every state access requires:

1. Serialize the key (Java object -> bytes)
2. RocksDB lookup (LSM tree, potentially hitting disk)
3. Deserialize the value (bytes -> Java object)
4. Modify the object
5. Serialize back (Java object -> bytes)
6. Write to RocksDB

Each RocksDB access costs 5-15 microseconds. A single event that updates 10 features pays this cost 10 times.

Beava stores state in a Rust `HashMap`. Each access is a pointer dereference. Cost: 0.1-0.2 microseconds. No serialization, no deserialization, no LSM compaction, no write amplification.

### The GC Cliff

The JVM garbage collector works well when heap usage is moderate. But as state grows (more entities, more features, more windows), GC pressure increases non-linearly. At high heap utilization, GC pauses can spike from milliseconds to seconds -- the "GC cliff." This manifests as tail latency spikes that are difficult to diagnose and tune.

Flink mitigates this by storing state off-heap in RocksDB, but that reintroduces the serialization tax described above.

Rust has no garbage collector. Memory is freed deterministically. Latency is predictable regardless of state size.

### Object Overhead

A Java `HashMap<String, Double>` entry consumes roughly 80-120 bytes of overhead (object headers, boxing, pointers, alignment padding) before the actual data. A Rust `HashMap<String, f64>` entry consumes roughly 40-50 bytes including the String allocation. For millions of entities with dozens of features each, this 2-3x overhead difference translates directly to infrastructure cost.

## Cost Comparison

These estimates are based on Beava's measured performance (430-510K events/sec on a 48-core Xeon) and typical Flink+Kafka+Redis deployments at equivalent throughput. Cloud costs use on-demand pricing; reserved instances reduce both columns proportionally.

| Scale | Beava | Flink + Kafka + Redis |
|-------|-------|-----------------------|
| 10K eps, 100K entities | 1 node (4 vCPU, 16 GB), ~$120/mo | Kafka (3 small brokers) + Flink (JM + 2 TM) + Redis: 6-8 nodes, ~$800-1,500/mo |
| 50K eps, 1M entities | 1 node (8 vCPU, 64 GB), ~$400/mo | Kafka (3 brokers) + Flink (JM + 3 TM) + Redis (cluster): 10-12 nodes, ~$3,000-5,000/mo |
| 200K eps, 5M entities | 1 node (48 vCPU, 192 GB), ~$1,500/mo | Kafka (3-5 brokers) + Flink (JM + 6-8 TM) + Redis (cluster): 15-20 nodes, ~$8,000-15,000/mo |
| 500K eps, 10M entities | 1 large node (96 vCPU, 384 GB), ~$3,000/mo | Full production stack: 20-25 nodes, ~$15,000-25,000/mo |

**Important caveats:**

- Beava is a single-process server today. It scales vertically on one machine. If your workload exceeds what one large instance can handle, Beava is not the right tool yet.
- The Flink stack costs include ops overhead that Beava eliminates, but they also buy you things Beava does not provide: multi-node fault tolerance, exactly-once semantics across distributed state, and a mature ecosystem of connectors.
- At the 500K eps tier, Beava's numbers are based on benchmarks, not production deployments at that scale. Treat them as indicative.

## What Beava Does NOT Replace

Be clear-eyed about this. Beava is not a general-purpose distributed streaming engine. It does not replace Flink or Kafka for workloads that genuinely need their capabilities:

- **Multi-TB state** -- Beava holds all state in memory on one machine. If your state exceeds what fits in RAM on the largest available instance (384 GB-768 GB), you need a distributed state backend.
- **Exactly-once distributed processing** -- Beava provides crash recovery via snapshots and event log replay, but it is not a distributed system with exactly-once guarantees across nodes.
- **Complex event processing** -- Temporal pattern matching, session windows with custom gap logic, event-time watermarks with late-arrival handling. Flink's event-time processing model is genuinely sophisticated and hard to replicate.
- **Connector ecosystem** -- Flink has connectors for hundreds of sources and sinks. Beava has a TCP protocol and an HTTP API.
- **Multi-tenant, multi-job deployments** -- Flink is designed to run hundreds of independent jobs on a shared cluster. Beava runs one pipeline per process.

If you are processing 1M+ events/sec, managing 100+ TB of state, or running dozens of independent streaming jobs, use Flink. It is an excellent system built by smart people for exactly those problems.

## Comparison with Other Tools

### RisingWave

Streaming database with Postgres wire protocol and SQL interface. Distributed, cloud-native, built in Rust. RisingWave is aiming to be the full streaming database -- SQL queries over streaming data with materialized views. Beava is narrower: it computes features for keyed entities, not arbitrary SQL. If you want SQL over streams, RisingWave is a strong choice. If you want a feature server with minimal ops, Beava is simpler to operate.

### Arroyo

Rust-based stream processor with SQL support, designed as a Flink alternative. Cloud-native, supports exactly-once. Arroyo is closer to Flink's model (distributed processing, connectors, checkpointing) reimplemented in Rust for better performance. Beava is not a stream processor -- it is a feature server. Different abstraction level, different use case.

### Materialize

Streaming SQL database built on Timely Dataflow. Excellent for incremental view maintenance over streaming data. More powerful query model than Beava, but also more complex to operate and reason about. If your features are naturally expressed as SQL views over event streams, Materialize is worth evaluating.

### Feast

Open-source feature store focused on the offline/online feature serving split. Feast manages feature definitions, offline computation (via Spark/BigQuery), and online serving (via Redis/DynamoDB). Beava is complementary: it computes real-time features that Feast does not handle natively. Some teams use Feast for batch features and a real-time engine for streaming features.

### Tecton

Managed feature platform (recently acquired by Databricks). Full lifecycle: feature definitions, batch/streaming/real-time computation, serving, monitoring. Enterprise product with enterprise pricing. Beava covers a slice of what Tecton does (real-time feature computation and serving) at a fraction of the complexity and cost, but does not provide the full platform experience.

## When to Choose Beava

Choose Beava when:

- You need real-time features (windowed counts, sums, averages, distinct counts, derived signals) served at sub-millisecond latency
- Your event volume fits on one machine (up to ~500K events/sec sustained)
- Your feature state fits in memory (up to ~50M entities at ~8 KB each = ~400 GB)
- You want to define features in Python and have them running in minutes, not days
- You do not want to operate Kafka, Flink, and Redis
- You are building fraud detection, ML feature serving, or real-time context for AI agents

Choose the Flink stack when:

- You need distributed, multi-node fault tolerance
- Your state exceeds what fits on one machine
- You need complex event processing (session windows, temporal patterns, event-time watermarks)
- You need connectors to dozens of external systems
- You are already running Kafka and Flink and the ops cost is acceptable
- Your organization has a dedicated streaming infrastructure team

Choose a managed platform (Tecton, Fennel, etc.) when:

- You want feature lifecycle management (versioning, monitoring, lineage)
- You need both batch and streaming features in one system
- You prefer paying for a managed service over operating anything yourself
- Your team does not want to think about infrastructure at all
