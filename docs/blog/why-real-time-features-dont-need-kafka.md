# Your Real-Time Feature Pipeline Doesn't Need Kafka

Your real-time feature pipeline doesn't need Kafka. It probably doesn't need Flink either. Here's why.

If you're building fraud detection, ML feature serving, or real-time context for AI agents, there's a good chance you're running -- or about to build -- a stack that looks like this:

```
Kafka (3 brokers) -> Flink (JobManager + TaskManagers) -> Redis (primary + replica)
     |                       |                                    |
Schema Registry         ZooKeeper / K8s                       Sentinel
     |                       |
Connect workers         Checkpoint storage (S3/HDFS)
```

That's 18-25 nodes, 5-8 distinct systems, each with its own failure modes, configuration language, and upgrade process. At 50K events per second with 1M entities, you're looking at $3,000-5,000/month in cloud costs and 0.5-1.0 FTE just to keep the lights on.

Most teams running this stack are computing fewer than 100 features at fewer than 100K events per second. They don't need horizontal scalability across dozens of nodes. They need the features to be correct, fast, and easy to change.

So why does everyone build it this way?

## The Root Cause: One JVM Limitation Creates Ten Components

Here's the part that doesn't get discussed enough.

Apache Flink is a JVM application. The JVM uses garbage collection to manage memory. GC works well when heap usage is moderate -- you allocate objects, the collector reclaims them, life is fine.

But streaming operators are stateful. Every windowed count, every running sum, every HyperLogLog sketch lives in memory for the duration of its window. A fraud detection pipeline with 47 features across 5 entity types at 1M active entities isn't a modest heap. It's tens of gigabytes of live objects that the GC can never collect, because they're not garbage -- they're your state.

At roughly 10 GB of heap, the JVM hits what I'll call the **GC cliff**. GC pauses spike from milliseconds to seconds. Tail latency becomes unpredictable. The system starts fighting itself.

Flink's solution: don't store state on the JVM heap. Use RocksDB as an off-heap state backend.

This works. But it has consequences:

1. **Serialization tax.** Every state access now requires serializing a Java object to bytes, doing a RocksDB lookup (LSM tree, potentially hitting disk), deserializing back, modifying, serializing again, and writing back. Each access costs 5-15 microseconds. A single event updating 10 features pays this 10 times.

2. **RocksDB tuning.** Now you need to configure block cache sizes, bloom filters, compaction strategies, write buffer sizes. These interact with each other in non-obvious ways.

3. **Checkpointing.** RocksDB state needs periodic checkpoints to S3 or HDFS for fault tolerance. You need to tune checkpoint intervals, incremental vs. full snapshots, and checkpoint timeout durations. Large state means slow checkpoints means larger recovery windows.

4. **SSDs.** RocksDB performs poorly on spinning disk. Your Flink TaskManagers need local SSDs.

5. **Memory fragmentation.** Off-heap memory and on-heap memory compete for the same physical RAM. You need to carefully partition JVM heap, managed memory, and network buffers.

One JVM limitation cascades into five additional configuration surfaces, each with its own failure modes. And we haven't even talked about Kafka broker tuning, schema registry compatibility modes, or Redis cluster rebalancing.

This is the real cost. Not the $5,000/month in compute. The $5,000/month in engineering time understanding why your p99 latency spiked at 3 AM.

## What If You Just... Didn't?

Rust has no garbage collector. Memory is allocated and freed deterministically. There is no GC cliff.

A Rust `HashMap` with 200 GB of state has the same access latency as one with 200 MB. A pointer dereference is a pointer dereference. There's no collector scanning those objects, no stop-the-world pauses, no off-heap escape hatch needed.

This means:

- **No serialization tax.** State lives in a HashMap. Access is a pointer dereference: 0.1-0.2 microseconds. Not 5-15 microseconds through RocksDB.
- **No RocksDB.** No LSM trees, no compaction, no write amplification, no bloom filter tuning.
- **No checkpoint complexity.** Periodic snapshots to disk, like Redis RDB. Serialize, write, done.
- **No memory partitioning.** All memory is just memory. No heap vs. off-heap vs. managed memory.

And the object overhead difference is significant. A Java `HashMap<String, Double>` entry consumes 80-120 bytes of overhead (object headers, boxing, pointers, alignment padding) before the actual data. The Rust equivalent: 40-50 bytes. For millions of entities with dozens of features, this 2-3x difference determines whether your state fits on one machine or requires a cluster.

Here's the insight most teams miss: **most real-time feature workloads fit on a single fat node.** 10 million entities at 8 KB each is 80 GB. That's one cloud instance. No cluster coordination, no distributed consensus, no split-brain scenarios.

## What We Built

We built [Tally](https://github.com/petrpan26/tally), a real-time feature server in Rust. One binary. No dependencies.

Push an event over TCP, get updated features back in the same response. Not eventual consistency -- synchronous. The features are computed and returned before the TCP response frame is sent.

The programming model is Python, but Python never touches the hot path. Pipeline definitions are serialized to JSON and sent to the server at registration time. All computation happens in Rust.

Here's a 47-feature fraud detection pipeline -- the same one we benchmark with:

```python
import tally as tl

@tl.source
class RawTransactions:
    pass

@tl.dataset(depends_on=[RawTransactions])
class UserTransactions:
    features = tl.group_by("user_id").agg(
        tx_count_30m=tl.count(window="30m"),
        tx_count_1h=tl.count(window="1h"),
        tx_count_24h=tl.count(window="24h"),
        tx_count_7d=tl.count(window="7d"),
        tx_sum_1h=tl.sum("amount", window="1h"),
        tx_sum_24h=tl.sum("amount", window="24h"),
        tx_avg_1h=tl.avg("amount", window="1h"),
        tx_avg_24h=tl.avg("amount", window="24h"),
        tx_max_24h=tl.max("amount", window="24h"),
        tx_min_24h=tl.min("amount", window="24h"),
        tx_stddev_24h=tl.stddev("amount", window="24h"),
        unique_merchants_1h=tl.distinct_count("merchant_id", window="1h"),
        unique_merchants_24h=tl.distinct_count("merchant_id", window="24h"),
        unique_countries_24h=tl.distinct_count("country", window="24h"),
        unique_devices_24h=tl.distinct_count("device_id", window="24h"),
        unique_ips_24h=tl.distinct_count("ip_address", window="24h"),
        last_country=tl.last("country"),
        last_merchant=tl.last("merchant_id"),
        last_amount=tl.last("amount"),
    )
    velocity_spike = tl.derive("(tx_count_1h / 1) / (tx_count_24h / 24)")
    amount_vs_avg = tl.derive("last_amount / tx_avg_24h")
    spend_acceleration = tl.derive("tx_sum_1h / (tx_sum_24h / 24)")
    high_value_ratio = tl.derive("tx_max_24h / tx_avg_24h")
    merchant_diversity_1h = tl.derive("unique_merchants_1h / tx_count_1h")
    country_hop_flag = tl.derive("unique_countries_24h > 3")

@tl.dataset(depends_on=[RawTransactions], filter="status == 'failed'")
class UserFailedTxns:
    features = tl.group_by("user_id").agg(
        failed_count_30m=tl.count(window="30m"),
        failed_count_1h=tl.count(window="1h"),
        failed_count_24h=tl.count(window="24h"),
        failed_sum_24h=tl.sum("amount", window="24h"),
    )

@tl.dataset(depends_on=[RawTransactions])
class MerchantActivity:
    features = tl.group_by("merchant_id").agg(
        merch_tx_count_1h=tl.count(window="1h"),
        merch_tx_count_24h=tl.count(window="24h"),
        merch_tx_sum_24h=tl.sum("amount", window="24h"),
        merch_avg_amount=tl.avg("amount", window="24h"),
        merch_unique_users_1h=tl.distinct_count("user_id", window="1h"),
        merch_unique_users_24h=tl.distinct_count("user_id", window="24h"),
        merch_max_amount_24h=tl.max("amount", window="24h"),
        merch_stddev_24h=tl.stddev("amount", window="24h"),
    )

@tl.dataset(depends_on=[RawTransactions])
class DeviceActivity:
    features = tl.group_by("device_id").agg(
        device_tx_count_1h=tl.count(window="1h"),
        device_tx_count_24h=tl.count(window="24h"),
        device_unique_users_1h=tl.distinct_count("user_id", window="1h"),
        device_unique_users_24h=tl.distinct_count("user_id", window="24h"),
        device_unique_merchants_24h=tl.distinct_count("merchant_id", window="24h"),
    )

@tl.dataset(depends_on=[RawTransactions])
class IPActivity:
    features = tl.group_by("ip_address").agg(
        ip_tx_count_1h=tl.count(window="1h"),
        ip_tx_count_24h=tl.count(window="24h"),
        ip_unique_users_1h=tl.distinct_count("user_id", window="1h"),
        ip_unique_users_24h=tl.distinct_count("user_id", window="24h"),
        ip_unique_devices_24h=tl.distinct_count("device_id", window="24h"),
    )
```

That's 5 entity types, 47 features, 4 window tiers, cross-entity fan-out, filtered streams, and derived signals. Register the pipeline, push events, get features. No YAML. No job graph. No deployment pipeline.

Tally has 16 operators: count, sum, avg, min, max, stddev, percentile, distinct_count (adaptive HLL++), last, first, lag, ema, last_n, exact_min, exact_max, and derive. Windowed aggregations use bucketed ring buffers. Derive expressions are parsed into an AST at registration time by a winnow Pratt parser and evaluated in Rust at event time. Pipeline cascades propagate events through a DAG in topological order.

## The Numbers

Measured on a 48-core Xeon with the 47-feature fraud pipeline above. Zipfian distribution over 10K users, 2K merchants, 5K devices, 8K IPs.

| Metric | Value |
|--------|-------|
| Throughput (8 clients, async batch) | 430-510K events/sec |
| Throughput (single client) | 270K events/sec |
| Sustained load | 29M events, 722K entities, no degradation |
| Memory per entity | 7.6 KB (15 features including HLL++) |
| Latency (p99) | < 100 us |

At 7.6 KB per entity, 10 million entities fit in 76 GB of RAM. 50 million fit in 380 GB. One machine.

The cost comparison at different scales:

| Scale | Tally | Flink + Kafka + Redis |
|-------|-------|-----------------------|
| 10K eps, 100K entities | 1 node, ~$120/mo | 6-8 nodes, ~$800-1,500/mo |
| 50K eps, 1M entities | 1 node, ~$400/mo | 10-12 nodes, ~$3,000-5,000/mo |
| 200K eps, 5M entities | 1 node, ~$1,500/mo | 15-20 nodes, ~$8,000-15,000/mo |

The benchmark script is in the repo: [`benchmark/fraud-pipeline/bench_fraud.py`](https://github.com/petrpan26/tally/blob/main/benchmark/fraud-pipeline/bench_fraud.py). Run it yourself.

## What Tally Is NOT

I want to be honest about this, because nothing is more annoying than a project that oversells itself.

**Tally is not distributed.** It's a single-process server. All state lives in memory on one machine. If your state exceeds what fits in RAM on the largest available instance (384-768 GB), Tally is the wrong tool.

**Tally is not for multi-TB state.** If you have billions of entities with hundreds of features each, you need a distributed state backend. That's Flink's strength.

**Tally is not for complex event processing.** Session windows with custom gap logic, event-time watermarks with late-arrival handling, temporal pattern matching -- Flink's event-time model is genuinely sophisticated. Tally does sliding windows with bucketed ring buffers. Simpler, faster, but less expressive.

**Tally has no connector ecosystem.** Flink has connectors for hundreds of sources and sinks. Tally has a TCP protocol and an HTTP API. You push events to it and read features from it. That's the interface.

**Tally is not a streaming SQL engine.** If you want SQL over streams, look at RisingWave or Materialize. Tally computes features for keyed entities, not arbitrary queries.

Kafka and Flink are excellent systems built by smart people for genuine problems. If you're processing millions of events per second across hundreds of terabytes of state, use them. They're battle-tested at that scale.

But if you're one of the 90% of teams that need fast aggregations over fewer than 10 million entities at fewer than 500K events per second -- which is most fraud detection, most ML feature serving, most real-time personalization -- you probably don't need a distributed streaming stack.

You need a feature server.

## Try It

Tally is open source under Apache 2.0.

```bash
git clone https://github.com/petrpan26/tally.git
cd tally
docker compose up -d
cd python && pip install -e .
python3 benchmark/fraud-pipeline/bench_fraud.py --events 100000 --clients 4
```

GitHub: [https://github.com/petrpan26/tally](https://github.com/petrpan26/tally)

If you use Claude Code, type `/tally` for a guided setup that generates a pipeline for your use case, pushes test data, and runs capacity planning on your hardware.

Docs: [Architecture](https://github.com/petrpan26/tally/blob/main/docs/architecture.md) | [Operators](https://github.com/petrpan26/tally/blob/main/docs/operators.md) | [Tally vs Flink](https://github.com/petrpan26/tally/blob/main/docs/comparison.md)
