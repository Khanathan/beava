# Reddit Launch Posts

---

## Post 1: r/rust

### Title

[P] I built a single-binary feature server in Rust. Notes on DashMap, postcard, winnow, HLL++, and what I'd do differently.

### Flair

Show (or Project, depending on subreddit options)

### Body

I've been building [Beava](https://github.com/petrpan26/beava), a real-time
feature server. Single binary, all state in memory, push events over TCP,
read results in microseconds. The use cases are fraud scoring, ML
feature serving, real-time personalization — anywhere you need windowed
counts, sums, or distinct counts updated per event.

Sharing some Rust-specific notes from the build. Not a tutorial, just
what I ran into.

**Concurrency model**

State is a `DashMap<EntityKey, EntityState>`. Pipeline definitions
(stream schemas, the DAG) are behind a `tokio::sync::RwLock`,
read-locked on every push, write-locked only on register (rare). Event
log, metrics, and snapshots each get their own `parking_lot::Mutex`. The
goal was no single lock serializing all connections. I started with
`RwLock<HashMap>` for everything and should have gone to DashMap sooner
— contention under load was the first bottleneck.

**Snapshots: postcard + serde**

Periodic snapshots serialize state to disk (base every ~5 min, delta
every 30s for dirty keys). Went with postcard over bincode for smaller
output. The snapshot path clones from the DashMap, then serializes on
`spawn_blocking`. Atomic writes via tmp + fsync + rename. Plus an
append-only event log (WAL) per stream for durability between snapshots.

**Expression engine: winnow Pratt parser**

Derived features are string expressions like
`"(tx_count_1h / 1) / (tx_count_24h / 24)"`. Parsed into an AST at
registration time using winnow, evaluated by walking the AST at event
time. 21 builtins. winnow was pleasant — good error messages and the
combinator model fits Pratt parsing naturally.

One thing that bothers me: the evaluator allocates on every eval (field
name lookups create temporary strings). Not the bottleneck yet, but
it's there.

**Adaptive HLL++**

The `distinct_count` operator transitions automatically: exact counting
(sorted array) for small cardinality, AHashSet for moderate, HLL++
(p=12, Google bias correction) for high cardinality. Each window bucket
holds its own sketch. Typical memory: ~2 KB per entity for HLL features,
zero error for most entities since they stay in the exact phase.

**Ring buffers**

All windowed operators use a generic `RingBuffer<T>` with lazy
expiration. A 1h window with 1-min buckets = 60 slots. The bucket type
is generic: `u64` for counts, `f64` for sums, `Hll` for distinct counts.

**The fork primitive (what makes the API feel different)**

`bv.fork()` from the Python SDK opens a TCP connection to a remote
Beava server, requests a snapshot of state matching a scope filter, and
lands that in a local in-memory replica. You compute features against
it; prod never sees your reads. Default mode is snapshot isolation;
pass `tail=True` for streaming replication via CDC. Server-side it's
just another GET-with-filter on the snapshot path; the trick is the
wire protocol cleanly separates "snapshot read" from "tail
subscription" so the client can do either without round-trips.

**Numbers**

47-feature fraud pipeline, Zipfian distribution:
- 544K eps sustained, 8 client processes, 16-core Hetzner AX
- 314K eps on a 10-core M-series Mac (for the laptop folks)
- 7.6 KB per entity
- Sub-100µs p99 single-client

Reproduce: `bash benchmark/fraud-pipeline/run_bench.sh`

**What I'd do differently**

- DashMap from day one instead of `RwLock<HashMap>`
- The expression evaluator allocations need attention eventually
- postcard error messages on schema mismatch are rough — added manual
  version bytes for snapshot migrations

Apache 2.0. ~22K lines of Rust + ~2.3K lines of Python SDK.
[UNSAFE.md](https://github.com/petrpan26/beava/blob/main/UNSAFE.md) audits
the 4 unsafe blocks (all libc FFI in `event_log.rs`). Bus factor of 1
disclosed up front — sole maintainer today, Apache 2.0 + no CLA is the
contingency. Feedback on the concurrency model or the ring buffer design
welcome.

https://github.com/petrpan26/beava

---

## Post 2: r/MachineLearning

### Title

[P] Open-sourced a feature server with `bv.fork()` — scoped replica of live prod for feature iteration. 47-feature fraud pipeline, sub-100µs p99.

### Flair

[P] Project

### Body

I kept building the same thing at every company: a pipeline that takes
events, computes windowed aggregations per entity, and serves them to a
model. At Viggle, setting up Kafka for this took three weeks. The
computation logic took a day.

So I built [Beava](https://github.com/petrpan26/beava). Single Rust
binary, all state in memory. Define pipelines, push events, read
results. The use case is real-time features for online inference:
velocity signals, amount anomalies, cardinality tracking, failure rates.

The novel part is `bv.fork()`. A Python `with` block that spawns a
local replica of live prod state, scoped to whatever keys you name. You
iterate features against REAL production bytes, close the context, prod
doesn't care.

```python
import beava as bv

with bv.fork("beava-prod.internal", scope={"user_id": "u123"}):
    # Scoped copy of live prod state. Hack on the feature.
    # Close the context → prod is untouched.
    print(UserFeatures.get("u123").velocity_spike)
```

Closes the staging-data skew axis — the bug where your test says 47.3
and prod says 50.1 and you burn two days finding the difference. (To
be honest: doesn't solve all skew. Logic drift between replay and live
push is still your job. But the test-data-lying axis is gone.)

**What it looks like end-to-end**

```python
import beava as bv

@bv.stream
class RawTransactions:
    user_id: str
    amount: float
    merchant_id: str

@bv.table(key="user_id")
def UserTransactions(txs: RawTransactions) -> bv.Table:
    return (
        txs.group_by("user_id")
        .agg(
            tx_count_1h=bv.count(window="1h"),
            tx_count_24h=bv.count(window="24h"),
            tx_avg_24h=bv.avg("amount", window="24h"),
            unique_merchants_24h=bv.count_distinct("merchant_id", window="24h"),
        )
        .with_columns(
            velocity_spike=(bv.col("tx_count_1h") / 1) / (bv.col("tx_count_24h") / 24),
        )
    )

app = bv.App("localhost:6400")
app.register(RawTransactions, UserTransactions)

# Push events (fire-and-forget, maximum throughput)
app.push(RawTransactions, {"user_id": "u123", "amount": 50.0, "merchant_id": "m456"})
app.flush()

# Read features for inference
features = app.get("u123")
model_input = [features.tx_count_1h, features.velocity_spike, ...]
```

Every write is synchronous and atomic on the server. Reads return the
latest state. No eventual consistency, no cache miss on cold keys.

**16 operators**

count, sum, avg, min, max, stddev, percentile, distinct_count (adaptive
HLL++), last, first, lag, ema, last_n, exact_min, exact_max, derive.
Sliding windows with configurable granularity.

distinct_count is adaptive: exact for low cardinality, transitions to
HyperLogLog automatically. Zero error for most entities.

**Benchmark**

47-feature fraud pipeline (5 entity types, Zipfian distribution):
- 544K eps on a 16-core Hetzner box
- 314K eps on a 10-core M-series Mac
- 7.6 KB/entity, sub-100µs p99 single-client

Reproduce: `bash benchmark/fraud-pipeline/run_bench.sh`

**What it's not**

Not a feature store (no offline store, no versioning, no lineage). Not
distributed. Use it as the real-time serving layer alongside your batch
pipeline. Batch features come via SET/MSET. Real-time features come
from Beava. Your model sees both in one GET.

SQL, session windows, and event-time semantics are on the roadmap, not
in v0.

Apache 2.0. Two design partner slots open this quarter — 10 hrs/week of
my time for 90 days, direct Slack channel. Profile + agreement on the
landing page.

Bus factor of 1, disclosed up front. Sole maintainer today. Apache 2.0
+ no CLA means if I disappear, you can fork everything. (Honest about
the risk.)

https://github.com/petrpan26/beava

Longer story: [Streaming Shouldn't Require a Platform Team](https://github.com/petrpan26/beava/blob/main/docs/blog/streaming-shouldnt-require-a-platform-team.md)

---

## Post 3: r/dataengineering

### Title

I built a single-binary alternative to Kafka+Flink+Redis for real-time compute. Here's where it's the right answer and where it's not.

### Body

I want to share something I built and be straightforward about the
tradeoffs.

At Viggle, we needed real-time aggregations for recommendations.
Standard stuff: windowed counts, distinct counts, moving averages.
Setting up Kafka took three weeks. We were a small team, no platform
engineer. I saw the same pattern at Faire and Fennel — small teams that
needed simple math over streaming data, running 10-20 nodes of
infrastructure to get it.

Most streaming platforms assume you already have Kafka. Most startups
don't. And Kafka + Flink takes years to master the tuning. Stateful
management is genuinely hard. Not everyone has an engineer who can
debug checkpoint failures at 3 AM.

So I built [Beava](https://github.com/petrpan26/beava). Single Rust
binary, all state in memory, push events over TCP, read results in
microseconds. The tradeoff: bounded by RAM on one machine. For most
real-time feature workloads (under 10M entities, under 1M eps), that's
enough. Modern instances go up to 2-4 TB.

**What it does**

Define pipelines, push events, read results. 16 operators (counts,
sums, HLL distinct counts, percentiles, etc.). Sliding windows.
Pipeline DAGs that cascade automatically. Every write is synchronous
and atomic. Durable via WAL + periodic snapshots.

The novel piece is `bv.fork()` — a Python `with` block that gives you a
scoped replica of live prod state for feature iteration. Closes the
staging-data skew axis, the one where your test says 47.3 and prod says
50.1 and you burn two days finding the difference. Full semantics in
[SEMANTICS.md](https://github.com/petrpan26/beava/blob/main/SEMANTICS.md),
grounded in source pointers.

**Numbers**

47-feature fraud pipeline (5 entity types, Zipfian distribution):

| Hardware | eps |
|---|---|
| 16-core Hetzner AX | 544K |
| 10-core M-series Mac | 314K |

Other: 7.6 KB/entity, <100µs p99 single-client, 29M-event sustained run
no degradation. Reproduce yourself: `bash benchmark/fraud-pipeline/run_bench.sh`

**Where Beava is the wrong answer**

- Your state exceeds one machine's RAM. You need distributed state.
- You need exactly-once distributed processing across regions.
- You need the Kafka connector ecosystem (JDBC sinks, S3 sources, etc.)
- You already have a working Flink stack and the ops cost is
  acceptable. Don't rip out working infrastructure — Flink's the right
  tool there.
- You need event-time watermarks with late-arrival handling (on the
  roadmap, not in v0).
- You need SOC2, HIPAA, or PCI today. Beava Cloud, Q4 2026 target.

**Where it might be worth 5 minutes**

- 20-100 real-time aggregations at under 1M eps
- You don't want to operate Kafka, Flink, and Redis
- Your team doesn't have a dedicated streaming infrastructure person
- You want something you can spin up in 5 minutes and test
- You're building a new system and want to start simple
- You've ever burned two days because staging data lied to you about
  what prod was computing — `bv.fork()` is built for that exact loop

The full story: [Streaming Shouldn't Require a Platform Team](https://github.com/petrpan26/beava/blob/main/docs/blog/streaming-shouldnt-require-a-platform-team.md)

Apache 2.0: https://github.com/petrpan26/beava

Bus factor of 1 disclosed up front — sole maintainer today, Apache 2.0
+ no CLA is the contingency.

Happy to answer questions about the architecture, the benchmarks, or
where this fits relative to tools you're already running.
