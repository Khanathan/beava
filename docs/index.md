# Tally Documentation

Tally is a real-time feature server. Push events in, get computed features out -- synchronously, in one request-response cycle. One Rust binary, sub-millisecond latency, zero infrastructure.

Tally ingests events over a custom TCP protocol, computes streaming features (windowed aggregations, derived expressions, cross-stream cascades), and serves them immediately in the response. No Kafka, no Flink, no cluster. Designed for fraud detection, ML feature serving, and real-time context for AI agents.

## Quick Links

- **[Quick Start](quickstart.md)** -- Get Tally running and push your first event in under 5 minutes.
- **[Python SDK](python-sdk.md)** -- Define pipelines, push events, and read features from Python.
- **[Operators Reference](operators.md)** -- All 16 built-in operators: count, sum, avg, min, max, stddev, percentile, distinct_count, and more.
- **[Architecture](architecture.md)** -- How Tally works under the hood: single-threaded core, in-memory state, snapshot persistence.

## What is Tally?

Tally is a lightweight, single-binary server that replaces the Kafka + Flink + Redis stack typically required for real-time feature computation. You define pipelines in Python using the Tally SDK, register them with the server, and push events over a persistent TCP connection. Tally computes windowed aggregations, derived expressions, and cross-stream lookups entirely in memory, returning updated feature values in the push response. There is no eventual consistency -- every push is synchronous. State is periodically snapshotted to disk for crash recovery, and the server restarts in seconds. If you need real-time features without the operational burden of a distributed streaming system, Tally is built for that.
