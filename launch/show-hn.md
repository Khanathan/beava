# Show HN Post

## Title

Show HN: Tally -- single-binary real-time compute engine in Rust (in-memory, 430K eps)

## URL

https://github.com/petrpan26/tally

## First Comment (post immediately after submission)

I kept building the same system at every company I worked at: take payment events, compute windowed aggregations per user/merchant, serve them to a fraud model. The logic was always simple. The infrastructure (Kafka + Flink + Redis, 10-20 nodes) never was.

Tally is my attempt at a simpler answer for teams that don't need distributed streaming. It's a single Rust binary. Everything in memory on one node. Define pipelines, push events over TCP, read results. 16 operators (counts, sums, HLL distinct counts, etc.), sliding windows, pipeline DAGs.

The core tradeoff: you give up horizontal scalability and distributed fault tolerance. In exchange, state access is ~0.1 us (no RocksDB, no serialization), and the whole thing runs on one machine with no ops. For most fraud detection and ML feature serving workloads I've seen, the state fits in memory.

Numbers (47-feature fraud pipeline, 48-core Xeon): 430-510K eps, 7.6 KB/entity, sub-100us p99. Benchmark script in the repo, run it yourself.

Not distributed. SQL, session windows, and event-time semantics are on the roadmap but not in v0. It's for the smaller use case that doesn't need a platform team.

Wrote up the technical reasoning here: [blog post](https://github.com/petrpan26/tally/blob/main/docs/blog/why-real-time-features-dont-need-kafka.md). Apache 2.0. Would appreciate feedback on the design.
