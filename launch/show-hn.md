# Show HN Post

## Title

Show HN: Tally -- Real-time compute engine in Rust. One binary replaces Kafka+Flink+Redis

## URL

https://github.com/petrpan26/tally

## First Comment (post immediately after submission)

Hey HN -- I built Tally because I kept seeing the same pattern at every company I worked at (Faire, Viggle, Fennel): teams that needed real-time aggregations for fraud detection or ML scoring, running Kafka + Flink + Redis across 10-20 nodes to get them. The operational burden was enormous relative to what they actually needed.

**What it is:** A single Rust binary that ingests events over a binary TCP protocol and computes streaming aggregations (windowed counts, sums, HLL distinct counts, 16 operators total). Define pipelines, push events, read results from in-memory state. Every write is synchronous and atomic. All state in RAM on one node, so reads are microseconds.

**Numbers** (48-core Xeon, 47-feature fraud pipeline, 5 entity types, Zipfian distribution):
- 430-510K events/sec (8 clients), 270K single-client
- 7.6 KB per entity, sub-100us p99 latency
- Benchmark in the repo, run it yourself: `benchmark/fraud-pipeline/bench_fraud.py`

**What it is NOT:** Not distributed (single node, all state in memory). Not streaming SQL (for that, use RisingWave or Materialize). Not for complex event processing (no event-time watermarks, Flink is genuinely better at that).

The full technical argument for why most teams don't need Kafka+Flink: [blog post](https://github.com/petrpan26/tally/blob/main/docs/blog/why-real-time-features-dont-need-kafka.md). Apache 2.0. Would love feedback on the design or the benchmarks.
