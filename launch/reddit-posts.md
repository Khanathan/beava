# Reddit Launch Posts

---

## Post 1: r/rust

### Title

[P] Beava — a single-binary feature server in Rust. Notes on DashMap, postcard, winnow, HLL++, and what we'd do differently.

### Flair

Show (or Project, depending on subreddit options)

### Body

Launching [Beava](https://github.com/petrpan26/beava), a real-time
feature server. Single binary, all state in memory, push events over
TCP, read results in microseconds. Use cases: fraud scoring, ML feature
serving, agent session state, real-time personalization — anywhere you
need windowed counts, sums, or distinct counts updated per event.

Sharing some Rust-specific notes from the build. Not a tutorial, just
what we ran into.

**Concurrency model**

State is a `DashMap<EntityKey, EntityState>`. Pipeline definitions
(stream schemas, the DAG) are behind a `tokio::sync::RwLock`,
read-locked on every push, write-locked only on register (rare). Event
log, metrics, and snapshots each get their own `parking_lot::Mutex`.
Goal: no single lock serializing all connections. We started with
`RwLock<HashMap>` for everything and should have gone to DashMap sooner
— contention under load was the first bottleneck.

**Snapshots: postcard + serde**

Periodic snapshots serialize state to disk (base every ~5 min, delta
every 30s for dirty keys). Went with postcard over bincode for smaller
output. Snapshot path clones from the DashMap, then serializes on
`spawn_blocking`. Atomic writes via tmp + fsync + rename. Plus an
append-only WAL per stream for durability between snapshots.

**Expression engine: winnow Pratt parser**

Derived features are string expressions like
`"(tx_count_1h / 1) / (tx_count_24h / 24)"`. Parsed into an AST at
registration time using winnow, evaluated by walking the AST at event
time. 21 builtins. winnow was pleasant — good error messages and the
combinator model fits Pratt parsing naturally.

One thing we're not happy with: the evaluator allocates on every eval
(field name lookups create temporary strings). Not the bottleneck yet,
but it's there.

**Adaptive HLL++**

The `distinct_count` operator transitions automatically: exact counting
(sorted array) for small cardinality, AHashSet for moderate, HLL++
(p=12, Google bias correction) for high cardinality. Each window
bucket holds its own sketch. Typical memory: ~2 KB per entity for HLL
features, zero error for most entities since they stay in the exact
phase.

**Ring buffers**

All windowed operators use a generic `RingBuffer<T>` with lazy
expiration. A 1h window with 1-min buckets = 60 slots. The bucket type
is generic: `u64` for counts, `f64` for sums, `Hll` for distinct
counts.

**The fork primitive (what makes the API feel different)**

`bv.fork()` from the Python SDK opens a TCP connection to a remote
Beava server, requests a snapshot of state matching a scope filter,
and lands that in a local in-memory replica. You compute features
against it; prod never sees your reads. Default mode is snapshot
isolation; pass `tail=True` for streaming replication via CDC.
Server-side it's a GET-with-filter on the snapshot path; the trick is
the wire protocol cleanly separates "snapshot read" from "tail
subscription" so the client can do either without round-trips.

**Numbers**

47-feature fraud pipeline, Zipfian distribution:
- 544K eps sustained, 8 client processes, 16-core Hetzner AX52
- 314K eps on a 10-core M-series Mac (also committed in repo)
- ~8 KB per entity
- p99 <100µs single-client reads (HdrHistogram, 256B payload, 1M-key
  cardinality, coordinated-omission corrected)
- Contention curve: 180µs @ 8 writers · 480µs @ 32 · 1.2ms @ 64 on
  one key

Reproduce in 70 seconds: `bash benchmark/fraud-pipeline/run_bench.sh`

**What we'd do differently**

- DashMap from day one instead of `RwLock<HashMap>`
- The expression evaluator allocations need attention eventually
- postcard error messages on schema mismatch are rough — added manual
  version bytes for snapshot migrations

Apache 2.0. ~22K lines of Rust + ~2.3K lines of Python SDK.
[UNSAFE.md](https://github.com/petrpan26/beava/blob/main/UNSAFE.md)
audits the 4 unsafe blocks (all libc FFI in `event_log.rs`).
Solo-maintainer today; lock-in exit ramp via Apache 2.0 + no CLA +
documented on-disk log format. Feedback on the concurrency model or
the ring buffer design welcome.

https://github.com/petrpan26/beava

---

## Post 2: r/MachineLearning

### Title

[P] Beava — feature server with `bv.fork()` for iterating against live prod state. 47-feature fraud pipeline, 544K eps, sub-100µs reads.

### Flair

[P] Project

### Body

We kept building the same thing at every company: a pipeline that
takes events, computes windowed aggregations per entity, and serves
them to a model. At Viggle, setting up Kafka for this took three
weeks. The computation logic took a day.

So we built [Beava](https://github.com/petrpan26/beava). Single Rust
binary, all state in memory. Define pipelines, push events, read
results. Use cases: real-time features for online inference, agent
session state, session signals for recsys, real-time dashboards.

The novel part is `bv.fork()`. A Python `with` block that spawns a
scoped replica of live production state. You iterate features against
real production bytes, close the context, prod doesn't care.

```python
import beava as bv

with bv.fork("beava-prod.internal", scope={"user_id": "u123"}):
    # Scoped copy of live prod state. Hack on the feature.
    # Close the context → prod is untouched.
    print(UserFeatures.get("u123").velocity_spike)
```

Closes the "staging data says 47.3, prod says 50.1, you burn two days
finding the difference" bug. (Doesn't solve all skew. Logic drift
between replay and live push is still your job. But the
test-data-lying axis is gone.)

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

app.push(RawTransactions, {"user_id": "u123", "amount": 50.0, "merchant_id": "m456"})
app.flush()

features = app.get("u123")
model_input = [features.tx_count_1h, features.velocity_spike, ...]
```

Every write is synchronous and atomic on the server. Reads return the
latest state. No eventual consistency, no cache miss on cold keys.

**16 operators**

count, sum, avg, min, max, stddev, percentile, distinct_count
(adaptive HLL++), last, first, lag, ema, last_n, exact_min, exact_max,
derive. Sliding windows with configurable granularity. distinct_count
is adaptive: exact for low cardinality, transitions to HyperLogLog
automatically. Zero error for most entities.

**Benchmark**

47-feature fraud pipeline (5 entity types, Zipfian distribution):
- 544K eps on a 16-core Hetzner box
- 314K eps on a 10-core M-series Mac
- ~8 KB/entity
- p99 <100µs single-client reads
- ~180µs p99 at 8 concurrent writers on hot keys

Reproduce in 70 seconds: `bash benchmark/fraud-pipeline/run_bench.sh`.
Full methodology at
[benchmark/README.md](https://github.com/petrpan26/beava/blob/main/benchmark/README.md).

**What it's not**

Not a managed feature store (no offline store, no versioning, no
lineage). Not distributed. Use it as the real-time serving layer
alongside your batch pipeline. SQL, session windows, event-time
watermarks: roadmap, not v0.

**Agent session state (for the LLM folks)**

Per-agent session state with TTL eviction — replaces Postgres + Redis
+ cron for things like "tool-call count per session in the last 5
minutes, error rate, last tool used". Run it as a sidecar to your
agent loop.

Apache 2.0. Solo-maintainer today; lock-in exit ramp via Apache 2.0 +
no CLA + documented on-disk log format. Two design partner slots open
this quarter — 10 hrs/week of our time for 90 days, direct Slack
channel. Profile + agreement on the landing page.

https://github.com/petrpan26/beava

Longer story: [Streaming Shouldn't Require a Platform Team](https://github.com/petrpan26/beava/blob/main/docs/blog/streaming-shouldnt-require-a-platform-team.md)

---

## Post 3: r/dataengineering

### Title

Beava — a single-binary feature server in Rust. Here's where it's the right answer and where it's not.

### Body

Sharing something we built and being straightforward about the
tradeoffs.

At Viggle, we needed real-time aggregations for recommendations.
Standard stuff: windowed counts, distinct counts, moving averages.
Setting up Kafka took three weeks. We were a small team, no platform
engineer. Same pattern at Faire and Fennel — small teams that needed
simple math over streaming data, running 10-20 nodes of infrastructure
to get it.

Most streaming platforms assume you already have Kafka. Most startups
don't. Kafka + Flink takes years to master the tuning. Stateful
management is genuinely hard. Not everyone has an engineer who can
debug checkpoint failures at 3 AM.

So we built [Beava](https://github.com/petrpan26/beava). Single Rust
binary, all state in memory, push events over TCP, read results in
microseconds. The tradeoff: bounded by RAM on one machine. For most
real-time feature workloads (under 10M entities, under 1M eps), that's
enough. Modern instances reach 1.5 TB+.

**What it does**

Define pipelines, push events, read results. 16 operators (counts,
sums, HLL distinct counts, percentiles, etc.). Sliding windows.
Pipeline DAGs that cascade automatically. Every write is synchronous
and atomic. Durable via WAL fsync before ack + periodic snapshots.

The novel piece is `bv.fork()` — a Python `with` block that gives you
a scoped replica of live prod state for feature iteration. Closes the
staging-data skew axis. Full semantics in
[SEMANTICS.md](https://github.com/petrpan26/beava/blob/main/SEMANTICS.md),
grounded in source pointers.

**Numbers**

47-feature fraud pipeline (5 entity types, Zipfian distribution):

| Hardware | eps |
|---|---|
| 16-core Hetzner AX52 | 544K |
| 10-core M-series Mac | 314K |

Other: ~8 KB/entity, p99 <100µs single-client reads, ~180µs p99 at
8-client hot-key contention. Reproduce yourself in 70 seconds:
`bash benchmark/fraud-pipeline/run_bench.sh`

**Failure modes (what we disclose up front)**

- WAL fsync before client ack; ~1s worst-case data loss on crash; ~30s recovery per 10M events on NVMe
- At RAM ceiling: rejects new writes with STATUS_SERVER_BUSY; SDK retries with exponential backoff (default 5 retries, 50ms initial, 2× factor, cap 1s)
- Process crash mid-window: at-least-once replay; dedup via `event_id` (per-key LRU Bloom filter, 64 B/key, 5-min window, target FPR 0.1%)
- Single node, no HA today. No primary/replica, no automated failover — on the Cloud roadmap (Q4 2026)
- fsync/snapshot stalls: p99 ingest lag stays <20ms at 500K eps on NVMe; gp3 degrades ~2×
- Hot-key contention: shard or debounce beyond ~8 concurrent writers on one key

Observability: Prometheus at `/metrics`, JSON logs, `/health`, RUNBOOK.md.

**Where Beava is the wrong answer**

- Your state exceeds one machine's RAM
- You need distributed exactly-once across regions
- You need the Kafka connector ecosystem (JDBC sinks, S3 sources, etc.)
- You already have a working Flink stack and the ops cost is
  acceptable — don't rip out working infrastructure. Flink's the right
  tool there.
- You need event-time watermarks with late-arrival handling (roadmap)
- You need SOC2, HIPAA, or PCI today (Beava Cloud, Q4 2026)

**Where it might be worth 5 minutes**

- 20-100 real-time aggregations at under 1M eps
- You don't want to operate Kafka, Flink, and Redis
- Your team doesn't have a dedicated streaming infrastructure person
- You want something you can spin up in 5 minutes and test
- You've ever burned two days because staging data lied to you about
  what prod was computing — `bv.fork()` is built for that exact loop

Apache 2.0. Solo-maintainer today; lock-in exit ramp via Apache 2.0 +
no CLA + documented on-disk log format.

https://github.com/petrpan26/beava

Longer story: [Streaming Shouldn't Require a Platform Team](https://github.com/petrpan26/beava/blob/main/docs/blog/streaming-shouldnt-require-a-platform-team.md)

Happy to answer questions about the architecture, the benchmarks, or
where this fits relative to tools you're already running.
