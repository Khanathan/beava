# Building a Real-Time Compute Engine for the Rest of Us

At three different companies (Faire, Viggle, Fennel), I kept building the same thing: a pipeline that takes payment events, computes windowed aggregations per user and merchant, and serves them to a fraud model. Counts, sums, averages, distinct counts over sliding windows. The logic was always simple. The infrastructure never was.

The standard approach is Kafka for ingestion, Flink for computation, Redis for serving. It works. It's also 10-20 nodes, 5-8 systems, and someone on the team spends half their time keeping it running. For what's fundamentally a HashMap with some math on top.

I kept thinking: for a team that needs 50-100 features over a few million entities at under 500K events per second, there should be a simpler way. So I built one.

## The tradeoff I chose

The core observation is that most real-time compute workloads fit in memory on a single machine. 10 million entities at 8 KB each is 80 GB. That's one cloud instance.

If you accept that constraint -- single node, all state in RAM -- a lot of complexity goes away:

- No distributed coordination, no consensus protocols, no split-brain recovery
- No serialization to disk on the hot path (state lives in a HashMap, access is ~0.1 us)
- No checkpoint orchestration across nodes
- No separate serving layer (reads come from the same in-memory state)

The tradeoff is real. You give up horizontal scalability, distributed fault tolerance, and multi-TB state. If you need those things, Flink is the right answer. It's a well-engineered system and I have a lot of respect for it.

But in my experience, most teams doing fraud detection, ML feature serving, or real-time personalization don't actually need those things. They need their aggregations to be correct, fast, and easy to change.

## Why Rust

I built this in Rust because of one specific property: deterministic memory management.

The JVM uses garbage collection. For most applications that's fine. But streaming operators are stateful -- windowed counts, running sums, HyperLogLog sketches all live in memory for the duration of their window. They're not garbage. The GC has to scan them but can never reclaim them. With the G1 collector, this creates pressure at larger heap sizes (32-64 GB of live state). ZGC helps with pause times but doesn't eliminate the fundamental tension.

This is why Flink uses RocksDB for state -- it moves the problem off-heap. It's a good solution, but it introduces serialization costs on every state access (1-15 us depending on cache hits vs disk), plus tuning complexity for block cache, compaction, bloom filters, and checkpoint intervals.

Rust has no GC. A `HashMap` with 50 GB of state has no GC-induced latency variance compared to one with 500 MB. You still pay for CPU cache and NUMA effects, but access time is deterministic and predictable. State lives in a HashMap. A lookup costs ~0.1 us. No serialization, no LSM tree, no compaction.

This isn't a criticism of the JVM. It's an observation that for this specific problem shape -- millions of keyed entities with bounded-memory operators -- native memory management lets you skip a layer of infrastructure.

## What I built

[Tally](https://github.com/petrpan26/tally) is a single Rust binary. You define pipelines, push events over a binary TCP protocol, and read results from in-memory state. Every write is synchronous and atomic -- all operators across all pipeline stages update in one pass. Reads serve the latest state in microseconds.

Here's a fraud detection pipeline with 47 features across 5 entity types:

```python
import tally as tl

@tl.source
class RawTransactions:
    pass

@tl.dataset(depends_on=[RawTransactions])
class UserTransactions:
    features = tl.group_by("user_id").agg(
        tx_count_1h=tl.count(window="1h"),
        tx_count_24h=tl.count(window="24h"),
        tx_sum_1h=tl.sum("amount", window="1h"),
        tx_avg_24h=tl.avg("amount", window="24h"),
        unique_merchants_24h=tl.distinct_count("merchant_id", window="24h"),
        unique_countries_24h=tl.distinct_count("country", window="24h"),
        last_country=tl.last("country"),
    )
    velocity_spike = tl.derive("(tx_count_1h / 1) / (tx_count_24h / 24)")
    country_hop_flag = tl.derive("unique_countries_24h > 3")

@tl.dataset(depends_on=[RawTransactions], filter="status == 'failed'")
class UserFailedTxns:
    features = tl.group_by("user_id").agg(
        failed_count_1h=tl.count(window="1h"),
        failed_count_24h=tl.count(window="24h"),
    )

@tl.dataset(depends_on=[RawTransactions])
class MerchantActivity:
    features = tl.group_by("merchant_id").agg(
        merch_tx_count_24h=tl.count(window="24h"),
        merch_unique_users_24h=tl.distinct_count("user_id", window="24h"),
    )
```

(Showing a subset. The full 47-feature pipeline is in the benchmark: [`bench_fraud.py`](https://github.com/petrpan26/tally/blob/main/benchmark/fraud-pipeline/bench_fraud.py))

Register the pipeline, push events, read results. One event can fan out to multiple entity types (a transaction updates both user and merchant state). Pipeline stages cascade through a DAG in topological order, all within one write.

The first SDK is Python, but Python never touches the hot path -- pipeline definitions are serialized to JSON at registration time, all computation happens in Rust. The binary TCP protocol is documented and open, so clients in Go, Java, or any language can be built against the spec.

## Numbers

Measured on a 48-core Xeon with the full 47-feature pipeline. Zipfian distribution over 10K users, 2K merchants, 5K devices, 8K IPs.

| Metric | Value |
|--------|-------|
| Throughput (8 clients) | 430-510K events/sec |
| Throughput (single client) | 270K events/sec |
| Sustained load | 29M events, 722K entities, no degradation |
| Memory per entity | 7.6 KB (15 features incl. HLL++) |
| p99 latency | < 100 us |

These are benchmark numbers, not production numbers at scale. Take them as indicative. The benchmark script is in the repo -- run it on your hardware.

At 7.6 KB per entity, 10M entities fit in 76 GB. That's one instance.

## What Tally is not

I want to be straightforward about limitations.

**Single node today.** All state lives in memory on one machine. Modern cloud instances go up to 2-4 TB of RAM (x2idn.metal, u-series), which holds hundreds of millions of entities, and is still cheaper than a Flink cluster at the same scale. Failover via standby replicas is on the roadmap. Distributed sharding across nodes is a future option but not required for most workloads.

**No connector ecosystem.** Flink has connectors for hundreds of sources and sinks. Tally has a TCP protocol and an HTTP API. You push events to it and read results. That's the interface.

**Not yet in v0:** SQL access layer, session windows, event-time watermarks, and temporal pattern matching are on the roadmap. Nothing in the architecture prevents them. v0 ships with a Python SDK, sliding windows, and processing-time semantics.

Kafka and Flink are well-built systems for real problems. If you're processing millions of events per second across hundreds of terabytes of state with exactly-once guarantees, use them.

Tally is for teams that need real-time compute but don't need the distributed infrastructure. In my experience, that's most of them.

## Try it

Apache 2.0. One binary.

```bash
git clone https://github.com/petrpan26/tally.git && cd tally
docker compose up -d
cd python && pip install -e .
python3 benchmark/fraud-pipeline/bench_fraud.py --events 100000 --clients 4
```

[GitHub](https://github.com/petrpan26/tally) | [Architecture](https://github.com/petrpan26/tally/blob/main/docs/architecture.md) | [Operators](https://github.com/petrpan26/tally/blob/main/docs/operators.md) | [Comparison](https://github.com/petrpan26/tally/blob/main/docs/comparison.md)

I'd appreciate feedback on the design, the API, or the benchmarks.
