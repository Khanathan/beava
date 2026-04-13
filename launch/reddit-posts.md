# Reddit Launch Posts

---

## Post 1: r/rust

### Title

[P] I built a real-time compute engine in Rust. Some notes on DashMap, postcard, winnow, and what I'd do differently.

### Flair

Show (or Project, depending on subreddit options)

### Body

I've been building [Tally](https://github.com/petrpan26/tally), a real-time compute engine for streaming aggregations. Single binary, all state in memory, push events over TCP, read results in microseconds. The use case is fraud detection, ML feature serving, real-time personalization -- anywhere you need windowed counts, sums, or distinct counts updated on every event.

Sharing some Rust-specific notes from the build. Not a tutorial, just what I ran into.

**Concurrency model**

State is a `DashMap<EntityKey, EntityState>`. Pipeline definitions (stream schemas, the DAG) are behind a `tokio::sync::RwLock`, read-locked on every push, write-locked only on register (rare). Event log, metrics, and snapshots each get their own `parking_lot::Mutex`. The goal was no single lock serializing all connections. I started with `RwLock<HashMap>` for everything and should have gone to DashMap sooner -- contention under load was the first bottleneck.

**Snapshots: postcard + serde**

Periodic snapshots serialize state to disk (base every ~5 min, delta every 30s for dirty keys). Went with postcard over bincode for smaller output. The snapshot path clones from the DashMap, then serializes on `spawn_blocking`. Atomic writes via tmp + fsync + rename. Plus an append-only event log (WAL) per stream for durability between snapshots.

**Expression engine: winnow Pratt parser**

Derived features are string expressions like `"(tx_count_1h / 1) / (tx_count_24h / 24)"`. Parsed into an AST at registration time using winnow, evaluated by walking the AST at event time. 21 builtins. winnow was pleasant -- good error messages and the combinator model fits Pratt parsing naturally.

One thing that bothers me: the evaluator allocates on every eval (field name lookups create temporary strings). Not the bottleneck yet, but it's there.

**Adaptive HLL++**

The `distinct_count` operator transitions automatically: exact counting (sorted array) for small cardinality, AHashSet for moderate, HLL++ (p=12, Google bias correction) for high cardinality. Each window bucket holds its own sketch. Typical memory: ~2 KB per entity for HLL features, zero error for most entities since they stay in the exact phase.

**Ring buffers**

All windowed operators use a generic `RingBuffer<T>` with lazy expiration. A 1h window with 1-min buckets = 60 slots. The bucket type is generic: `u64` for counts, `f64` for sums, `Hll` for distinct counts.

**Numbers**

47-feature fraud pipeline, 48-core Xeon, Zipfian distribution:
- 430-510K eps (8 clients), 270K single-client
- 7.6 KB per entity
- Sub-100us p99

Benchmark in the repo: `benchmark/fraud-pipeline/bench_fraud.py`

**What I'd do differently**

- DashMap from day one instead of RwLock<HashMap>
- The expression evaluator allocations need attention eventually
- postcard error messages on schema mismatch are rough. Added manual version bytes for snapshot migrations.

Apache 2.0. The codebase is about 22K lines of Rust + 2.3K lines of Python SDK. Feedback on the concurrency model or the ring buffer design is welcome.

https://github.com/petrpan26/tally

---

## Post 2: r/MachineLearning

### Title

[P] Open-sourced a real-time compute engine for ML feature serving -- 47-feature fraud pipeline, sub-100us p99

### Flair

[P] Project

### Body

I kept building the same thing at every company: a pipeline that takes events, computes windowed aggregations per entity, and serves them to a model. At Viggle, setting up Kafka for this took three weeks. The computation logic took a day.

So I built [Tally](https://github.com/petrpan26/tally). Single Rust binary, all state in memory. Define pipelines, push events, read results. The use case is real-time features for online inference: velocity signals, amount anomalies, cardinality tracking, failure rates.

**What it looks like**

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
        tx_avg_24h=tl.avg("amount", window="24h"),
        unique_merchants_24h=tl.distinct_count("merchant_id", window="24h"),
    )
    velocity_spike = tl.derive("(tx_count_1h / 1) / (tx_count_24h / 24)")

app = tl.App("localhost:6400")
app.register(RawTransactions, UserTransactions)

# Push events (fire-and-forget, fast)
app.push(RawTransactions, {"user_id": "u123", "amount": 50.0, "merchant_id": "m456"})
app.flush()

# Read features for inference
features = app.get("u123")
model_input = [features.tx_count_1h, features.velocity_spike, ...]
```

Every write is synchronous and atomic on the server. Reads return the latest state. No eventual consistency, no cache miss on cold keys.

**16 operators**

count, sum, avg, min, max, stddev, percentile, distinct_count (adaptive HLL++), last, first, lag, ema, last_n, exact_min, exact_max, derive. Sliding windows with configurable granularity.

The distinct_count is adaptive: exact for low cardinality, transitions to HyperLogLog automatically. Zero error for most entities.

**Benchmark**

47-feature fraud pipeline (5 entity types, Zipfian distribution):
- 430-510K eps (8 clients), 7.6 KB/entity, sub-100us p99

Benchmark script in the repo. Run it yourself.

**What it's not**

Not a feature store (no offline store, no versioning, no lineage). Not distributed. Use it as the real-time serving layer alongside your existing batch pipeline. Batch features come via SET/MSET. Real-time features come from Tally. Your model sees both in one GET.

SQL, session windows, and event-time semantics are on the roadmap but not in v0.

Apache 2.0: https://github.com/petrpan26/tally

Wrote up the longer story here: [Streaming Shouldn't Require a Platform Team](https://github.com/petrpan26/tally/blob/main/docs/blog/streaming-shouldnt-require-a-platform-team.md)

---

## Post 3: r/dataengineering

### Title

I built a single-binary alternative to Kafka+Flink+Redis for real-time compute. Here's where it makes sense and where it doesn't.

### Body

I want to share something I built and be straightforward about the tradeoffs.

At Viggle, we needed real-time aggregations for recommendations. Standard stuff: windowed counts, distinct counts, moving averages. Setting up Kafka took three weeks. We were a small team, no platform engineer. I saw the same pattern at Faire and Fennel. Small teams that needed simple math over streaming data, running 10-20 nodes of infrastructure to get it.

Most streaming platforms assume you already have Kafka. Most startups don't. And Kafka + Flink takes years to master the tuning. Stateful management is genuinely hard. Not everyone has an engineer who can debug checkpoint failures at 3 AM.

So I built [Tally](https://github.com/petrpan26/tally). Single Rust binary, all state in memory, push events over TCP, read results in microseconds. The tradeoff: you're bounded by RAM on one machine. For most real-time feature workloads (< 10M entities, < 500K eps), that's enough. Modern instances go up to 2-4 TB.

**What it does**

Define pipelines, push events, read results. 16 operators (counts, sums, HLL distinct counts, percentiles, etc.). Sliding windows. Pipeline DAGs that cascade automatically. Every write is synchronous and atomic. Durable via WAL + periodic snapshots.

**Numbers**

47-feature fraud pipeline (5 entity types, Zipfian distribution, 48-core Xeon):

| Metric | Value |
|--------|-------|
| Throughput (8 clients) | 430-510K eps |
| Memory per entity | 7.6 KB |
| p99 latency | < 100 us |
| Sustained | 29M events, no degradation |

These are benchmark numbers. Run the script yourself: `benchmark/fraud-pipeline/bench_fraud.py`

**Where Tally is the wrong answer**

- Your state exceeds one machine's RAM. You need distributed state.
- You need exactly-once distributed processing.
- You need the Kafka connector ecosystem (JDBC sinks, S3 sources, etc.)
- You already have a working Flink stack and the ops cost is acceptable. Don't rip out working infrastructure.
- You need event-time watermarks with late arrival handling (on the roadmap, not in v0).

**Where it might be worth trying**

- You need 20-100 real-time aggregations at < 500K eps
- You don't want to operate Kafka, Flink, and Redis
- Your team doesn't have a dedicated streaming infrastructure person
- You want something you can spin up in 5 minutes and test
- You're building a new system and want to start simple

The full story: [Streaming Shouldn't Require a Platform Team](https://github.com/petrpan26/tally/blob/main/docs/blog/streaming-shouldnt-require-a-platform-team.md)

Apache 2.0: https://github.com/petrpan26/tally

Happy to answer questions about the architecture, the benchmarks, or where this fits relative to tools you're already running.
