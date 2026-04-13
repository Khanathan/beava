# Reddit Launch Posts

---

## Post 1: r/rust

### Title

I built a real-time feature server in Rust -- DashMap, postcard, winnow, and some lessons learned

### Body

I've been working on [Tally](https://github.com/petrpan26/tally), a real-time feature server for fraud detection and ML feature serving. Push events over TCP, get computed features (windowed counts, sums, HLL distinct counts, derived expressions) back synchronously. Single binary, all state in memory.

Wanted to share some Rust-specific details and lessons from the build.

**Concurrency: DashMap + RwLock layering**

The hot path is concurrent PUSH commands from many TCP connections. State is a `DashMap<EntityKey, EntityState>` -- different entity keys never contend. The pipeline engine (which holds stream definitions and the DAG) is behind a `tokio::sync::RwLock` -- read-locked on every PUSH, write-locked only on REGISTER (rare). Event log, metrics, and snapshot coordination each get their own `parking_lot::Mutex`. The goal was no single lock that serializes all connections. Took a few iterations to get the lock granularity right.

**Serialization: postcard + serde for snapshots**

Snapshots serialize the full state store to disk periodically (base snapshots every ~5 minutes, delta snapshots every 30 seconds for dirty keys only). I went with postcard over bincode for the smaller output size and better no_std compatibility. The snapshot path clones state from the DashMap, then serializes on `tokio::task::spawn_blocking` to avoid blocking the event loop. Atomic writes via tmp-file + fsync + rename.

**Expression engine: winnow Pratt parser**

Users define derived features as string expressions like `"(tx_count_1h / 1) / (tx_count_24h / 24)"` or `"unique_countries_24h > 3"`. These get parsed into an AST at pipeline registration time using a winnow-based Pratt parser, then evaluated by walking the AST in Rust at event time. 18 builtin functions (abs, sqrt, log, clamp, if/else, coalesce, string ops). winnow was pleasant to work with -- the error messages are good and the combinator model fits Pratt parsing naturally.

**Adaptive HLL: three-phase distinct counting**

The `distinct_count` operator uses an adaptive approach: exact counting (sorted array) for <= 16 elements, AHashSet for moderate cardinality, HLL++ (p=12, Google bias correction) for high cardinality. Transitions are automatic and one-directional. Each ring buffer bucket holds its own HLL sketch; windowed reads merge all non-expired buckets. Typical memory: 2 KB per entity for the HLL features, zero error for the majority of entities that have low cardinality.

**Ring buffers for windowed aggregations**

All windowed operators (count, sum, avg, min, max, stddev, percentile, distinct_count) use a generic `RingBuffer<T>` with lazy expiration. Buckets are zeroed on `advance_to(now)`, not by background timers. A 1-hour window with 1-minute buckets = 60 slots. The bucket type is generic -- `u64` for counts, `f64` for sums, `MinBucket`/`MaxBucket` for extremes, `TDigest` for percentiles, `Hll` for distinct counts.

**Pipeline DAG: petgraph**

Streams can declare `depends_on` to form a cascade. The engine uses petgraph to build a directed graph, detect cycles via toposort, and maintain a pre-computed topological order. On PUSH, events propagate downstream with enrichment overlays -- downstream streams can reference upstream features in their derive expressions.

**Performance**

On a 48-core Xeon with a 47-feature fraud pipeline (5 entity types, 4 window tiers, Zipfian distribution):

- 430-510K events/sec with 8 client processes (async batch mode)
- 270K events/sec single client
- 7.6 KB per entity
- Sub-100us p99 latency

The benchmark is in the repo if you want to run it: `benchmark/fraud-pipeline/bench_fraud.py`

**What I'd do differently**

- Started with a single `RwLock<HashMap>` for state. Should have gone to DashMap earlier -- the contention under load was the first bottleneck I hit.
- The expression evaluator allocates on every eval (field name lookups create temporary strings). Haven't optimized this yet -- it's not the bottleneck, but it bothers me.
- postcard's error messages when schema evolution goes wrong are not great. I added a version byte per snapshot format to handle migrations manually.

Apache 2.0, contributions welcome. Particularly interested in feedback on the DashMap concurrency model and whether the ring buffer abstraction makes sense.

GitHub: https://github.com/petrpan26/tally

---

## Post 2: r/MachineLearning

### Title

Open-source real-time feature server -- 47-feature fraud pipeline in 60 lines of Python, sub-100us p99

### Body

I built [Tally](https://github.com/petrpan26/tally), an open-source real-time feature server designed for ML feature serving, fraud detection, and real-time personalization.

**The problem it solves**

If you need real-time features -- windowed counts, running averages, velocity signals, distinct counts -- for online inference, you typically have two options:

1. **The Kafka/Flink/Redis stack.** Powerful, but 15-20 nodes and a dedicated ops person. Overkill for most teams.
2. **Feast/Tecton with a real-time source.** Better abstraction, but Feast's real-time support is limited, and Tecton's pricing scales with your event volume.

Most fraud detection and scoring models need a modest number of features (20-100) computed over a modest number of entities (100K-10M) at moderate throughput (10K-200K events/sec). That workload fits on a single machine if the server is efficient enough.

**How Tally works**

Define features in Python. Push events over TCP. Get features back synchronously in the response.

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
        last_country=tl.last("country"),
    )
    velocity_spike = tl.derive("(tx_count_1h / 1) / (tx_count_24h / 24)")
    amount_vs_avg = tl.derive("last_amount / tx_avg_24h")

app = tl.App("localhost:6400")
app.register(RawTransactions, UserTransactions)

# Push event, get features in the response -- synchronously
features = app.push(RawTransactions, {
    "user_id": "u123",
    "amount": 50.0,
    "merchant_id": "m456",
    "country": "US",
})

# Use features for online inference
model_input = [features.tx_count_1h, features.velocity_spike, ...]
score = model.predict(model_input)
```

The key property: **synchronous push-through**. You push an event and get features in the same response. No polling, no eventual consistency, no cache miss on cold keys. This matters for fraud detection where you need features at the moment of the transaction, not 500ms later.

**The fraud detection benchmark**

The repo includes a full benchmark: 5 entity types (user, merchant, device, IP, card), 47 features across 4 window tiers (30m, 1h, 24h, 7d), with velocity spikes, amount anomalies, failure rates, and cardinality signals.

Results on a 48-core Xeon:

- 430-510K events/sec with 8 clients
- 7.6 KB per entity (including HyperLogLog sketches for distinct counting)
- Sub-100us p99 latency
- 29M events sustained, zero degradation

At 7.6 KB/entity, 10M entities = 76 GB RAM. One machine.

**16 operators**

count, sum, avg, min, max, stddev, percentile (approximate, t-digest), distinct_count (adaptive HLL++), last, first, lag, ema (exponential moving average), last_n, exact_min, exact_max, derive (expression over other features).

The distinct_count is adaptive: exact counting for low cardinality, transitions to HyperLogLog automatically. Zero error for most entities.

**What it's NOT**

- Not a feature store like Feast/Tecton. No offline store, no feature versioning, no lineage tracking. It computes and serves real-time features only.
- Not distributed. Single process. If your state doesn't fit on one large instance, you need something else.
- Not a replacement for batch features. Use it alongside your existing batch pipeline -- Tally supports direct writes (SET/MSET) for features computed offline.

It's the real-time serving layer. Batch features come from your existing Spark/Airflow pipeline via SET. Real-time features come from Tally. Your model sees both in one GET.

Apache 2.0: https://github.com/petrpan26/tally

The benchmark script is at `benchmark/fraud-pipeline/bench_fraud.py` -- run it on your own hardware.

---

## Post 3: r/dataengineering

### Title

We replaced Kafka + Flink + Redis with a single Rust binary for real-time features. Here's when that makes sense (and when it doesn't).

### Body

I want to share something I built and be honest about where it fits.

[Tally](https://github.com/petrpan26/tally) is a real-time feature server. You push events over TCP, it computes streaming features (windowed aggregations, derived signals, cross-entity lookups), and returns them in the response. One binary, all state in memory, periodic snapshots to disk.

**The stack it replaces**

For teams computing real-time features for fraud detection, ML scoring, or personalization, the standard stack is:

```
Kafka (3+ brokers) -> Flink (JM + TMs) -> Redis (primary + replica)
     + Schema Registry + ZooKeeper + checkpoint storage
```

That's 10-20 nodes, $3,000-15,000/month in cloud costs depending on scale, and a meaningful chunk of someone's time keeping it running.

**The core argument: most teams don't need distributed streaming**

Most real-time feature workloads I've seen in practice:

- < 100 features
- < 100K events per second
- < 10M active entities
- State that fits in 80-200 GB of RAM

That workload fits on one machine. The reason teams run a distributed stack isn't that they need the distribution -- it's that the JVM-based tools (Flink) hit memory management limits (GC cliff at ~10 GB heap) that force off-heap state storage (RocksDB), which creates the need for checkpointing, SSDs, and careful memory partitioning. One runtime limitation creates the need for 5 additional infrastructure components.

Tally is written in Rust. No GC, no serialization tax, no off-heap escape hatch. State lives in a HashMap. Access cost: 0.1-0.2 microseconds per lookup, vs. 5-15 microseconds through RocksDB.

**Cost comparison**

| Scale | Tally | Flink + Kafka + Redis |
|-------|-------|-----------------------|
| 10K eps, 100K entities | 1 node, ~$120/mo | 6-8 nodes, ~$800-1,500/mo |
| 50K eps, 1M entities | 1 node, ~$400/mo | 10-12 nodes, ~$3,000-5,000/mo |
| 200K eps, 5M entities | 1 node, ~$1,500/mo | 15-20 nodes, ~$8,000-15,000/mo |

These are on-demand cloud prices. The Tally numbers are based on measured performance (430-510K events/sec on a 48-core Xeon with a 47-feature fraud pipeline). The Flink numbers are typical mid-size deployments.

**What you eliminate**

| Component | Purpose | Gone? |
|-----------|---------|-------|
| Kafka brokers | Event transport | Yes -- events pushed directly to Tally |
| Schema Registry | Schema management | Yes -- pipeline defined in Python |
| ZooKeeper | Kafka coordination | Yes |
| Flink JobManager | Job scheduling | Yes |
| Flink TaskManagers | Computation | Yes -- Tally does it |
| RocksDB | State backend | Yes -- in-memory HashMap |
| Checkpoint storage (S3) | Fault tolerance | Yes -- local snapshots |
| Redis | Feature serving | Yes -- Tally serves directly |
| Redis Sentinel | HA | Yes |
| Kafka Connect | Ingestion glue | Yes |

**When Tally is the wrong answer**

I want to be direct about this.

- **Your state exceeds one machine's RAM (384-768 GB).** You need distributed state. Use Flink.
- **You need exactly-once distributed processing.** Tally provides crash recovery via snapshots + event log replay, not distributed exactly-once.
- **You need complex event processing.** Session windows, event-time watermarks, late arrival handling. Flink's event-time model is genuinely sophisticated.
- **You need a connector ecosystem.** Tally has TCP and HTTP. That's it. No Kafka connector, no JDBC sink, no S3 source.
- **You're running dozens of independent streaming jobs.** Flink is designed for multi-tenant job clusters.
- **You already have the Flink stack and the ops cost is acceptable.** Don't rip out working infrastructure to save $3K/month.

**When Tally is the right answer**

- You need 20-100 real-time features at < 500K events/sec
- You don't want to operate Kafka, Flink, and Redis
- You want features in the response to the push (synchronous, not eventual)
- You're building a new system and want to start simple
- Your team doesn't have a dedicated streaming infrastructure person

**Numbers**

47-feature fraud pipeline (5 entity types, 4 window tiers, derived signals):

- 430-510K events/sec (8 clients)
- 7.6 KB per entity
- Sub-100us p99 latency
- 29M events sustained, no degradation

Benchmark script is in the repo. Run it yourself: `benchmark/fraud-pipeline/bench_fraud.py`

Apache 2.0: https://github.com/petrpan26/tally

I wrote a longer piece on the JVM GC cliff and why it creates the infrastructure complexity: [blog post](https://github.com/petrpan26/tally/blob/main/docs/blog/streaming-shouldnt-require-a-platform-team.md)

Happy to answer questions about the architecture, the benchmarks, or where this fits relative to tools you're already running.
