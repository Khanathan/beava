# Show HN Post

## Title

Show HN: Tally -- Real-time feature server in Rust. No Kafka, no Flink, one binary

## URL

https://github.com/petrpan26/tally

## First Comment (post immediately after submission)

Hey HN -- I built Tally because I kept seeing the same pattern at every company I worked at (Faire, Viggle, Fennel): teams that needed 50-100 real-time features for fraud detection or ML scoring, running a stack of Kafka + Flink + Redis across 15-20 nodes to get them. The operational burden was enormous relative to what they actually needed.

**What it is:** A single Rust binary that ingests events over a custom TCP protocol, computes stateful streaming features (windowed aggregations, derived expressions, cross-entity cascades), and returns them synchronously in the response. Push an event, get features back. Not eventual consistency -- the features are computed before the TCP frame is sent.

**Architecture highlights:**

- State lives in a `DashMap` (concurrent HashMap with per-shard locking). No RocksDB, no LSM trees, no serialization tax. Different entity keys never contend.
- Pipeline engine uses `petgraph` for DAG-based cascade execution. One event can propagate through multiple downstream datasets in topological order.
- 16 operators including adaptive distinct counting (exact -> HashSet -> HLL++ with Google bias correction, transitions automatically based on cardinality).
- Expression engine: winnow Pratt parser at registration time, AST evaluation in Rust at event time. 18 builtins. Python defines pipelines but never touches the hot path.
- Persistence: periodic base+delta snapshots via postcard/serde, plus per-stream append-only event logs for backfill replay. Redis-style `everysec` fsync.

**Numbers** (48-core Xeon, 47-feature fraud pipeline, 5 entity types, Zipfian distribution):

- 430-510K events/sec with 8 client processes
- 270K events/sec single client
- 7.6 KB per entity (15 features including HLL++)
- Sub-100us p99 latency
- 29M events sustained, 722K entities, no degradation

The benchmark pipeline and runner are in the repo: `benchmark/fraud-pipeline/bench_fraud.py`. Run it yourself.

**What it is NOT:**

- Not distributed. Single process, all state in memory. If your state doesn't fit on one machine, this isn't the right tool.
- Not a streaming SQL engine. It computes features for keyed entities, not arbitrary queries. For SQL over streams, look at RisingWave or Materialize.
- Not for complex event processing. No session windows, no event-time watermarks, no temporal patterns. Flink is genuinely better at those.
- Not for multi-tenant job clusters. One pipeline per process.

The core argument: the JVM GC cliff forces Flink into RocksDB for state, which creates the need for checkpointing, SSDs, incremental snapshots, and careful memory partitioning. Rust has no GC, so a HashMap with 200 GB of state has the same access latency as 200 MB. Most real-time feature workloads (< 10M entities, < 500K eps) fit on a single fat node. I wrote up the full technical argument here: [Why Your Real-Time Feature Pipeline Doesn't Need Kafka](https://github.com/petrpan26/tally/blob/main/docs/blog/why-real-time-features-dont-need-kafka.md).

Apache 2.0. Would love feedback on the design, the API, or the benchmarks.
