# Beava

Real-time feature server for ML. Single binary, event-time semantics, HTTP + CDC fork.

[![CI](https://github.com/petrpan26/beava/actions/workflows/ci.yml/badge.svg)](https://github.com/petrpan26/beava/actions/workflows/ci.yml)
[![Docker Pulls](https://img.shields.io/docker/pulls/beavadb/beava.svg)](https://hub.docker.com/r/beavadb/beava)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

---

## 60-second quickstart

```bash
docker run -p 6900:6900 beavadb/beava:latest
curl -X POST http://localhost:6900/push/clicks -d '{"user":"alice","page":"/home"}'
curl http://localhost:6900/features/alice
```

Beava ingested the event, bucketed by event-time, and served the feature vector keyed by
`alice`. No broker, no ETL, one binary.

Full walkthrough: [docs/getting-started.md](docs/getting-started.md).

## Iterate features against live prod — fork

```bash
tally fork --remote prod.example.com --streams transactions --pipeline-file pipeline.py
```

A scoped replica of live prod state. Define a new feature, see the value it produces from
real production history, close the context — production never sees your reads.

Details: [docs/architecture.md](docs/architecture.md) § Fork model.

## Why Beava

Replaces Postgres triggers + Redis counters + the cron job that heals drift. Same pipeline
from laptop to production. 315K EPS single-binary TCP push; 100K+ EPS over HTTP. See
[benchmark/README.md](benchmark/README.md) for reproducible numbers and
[docs/comparison.md](docs/comparison.md) for honest tradeoffs vs Flink / Feast.

## Learn more

- [Getting started](docs/getting-started.md) — 60-second path on a fresh machine
- [Concepts](docs/concepts.md) — streams, tables, operators, fork, event-time
- [HTTP API](docs/http-api.md) — curl examples for all endpoints
- [Architecture](docs/architecture.md) — single-node design, fork model, scaling posture
- [Operations](docs/operations.md) — sizing, durability, crash recovery
- [Event time](docs/event-time.md) — bucketing, watermarks, TTL semantics
- [Comparison](docs/comparison.md) — vs Feast, Flink+Redis, Redpanda
- [Examples](examples/) — fraud-scoring, session-features, curl-ingest

[Apache 2.0](LICENSE) · [CHANGELOG](CHANGELOG.md) · [GOVERNANCE](GOVERNANCE.md) · [SECURITY](SECURITY.md)
