<p align="center">
  <b><font size="6">Beava</font></b>
  <br>
  <i>A single-binary feature server in Rust</i>
</p>

<p align="center">
  <a href="https://github.com/petrpan26/beava/actions/workflows/ci.yml"><img src="https://github.com/petrpan26/beava/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License"></a>
</p>

---

Define a pipeline in Python. Push events over TCP. Query aggregated features in microseconds. One process. All state in RAM. Apache 2.0.

Replaces Postgres triggers + Redis counters + the cron job that heals drift. Same code from laptop to production: backfill against history, iterate against live prod state with `bv.fork()`, ship.

```python
import beava as bv

@bv.stream
class Click:
    user_id: str
    page: str

@bv.table(key="user_id")
def UserActivity(c: Click) -> bv.Table:
    return c.group_by("user_id").agg(
        clicks_10m = bv.count(window="10m"),
    )

app = bv.App("localhost:6400")
app.register(Click, UserActivity)

app.push(Click, {"user_id": "u123", "page": "/checkout"})
print(app.get("u123").clicks_10m)   # 1
```

- [Quick Start](#quick-start-60-seconds)
- [What Beava replaces](#what-beava-replaces-in-your-stack)
- [How Beava compares](#how-beava-compares-to-kafka--flink--redis)
- [Iterate features against live prod state — `bv.fork()`](#iterate-features-against-live-prod-state--bvfork)
- [Performance](#performance)
- [Failure modes](#failure-modes)
- [Security baseline](#security-baseline-self-hosted-today)
- [Honest scope](#honest-scope)
- [Maintainer status + lock-in exit ramp](#maintainer-status--lock-in-exit-ramp)
- [Documentation](#docs)

## Key properties

- **Single binary** — one process, one port, one data directory. Ship it under systemd or docker, same binary either way.
- **No separate message broker** — push events directly over TCP. No Kafka, no Pulsar, no Redpanda in the path.
- **Synchronous and atomic writes** — push an event, every operator across every pipeline stage updates in one pass. Reads return the latest state, no eventual consistency window.
- **Sub-microsecond state access** — all state in RAM on one node. A `HashMap::get` costs ~0.1µs.
- **Pipeline cascades** — multi-stage DAGs with `depends_on`. Events propagate in topological order in one request.
- **16 operators** — count, sum, avg, min, max, stddev, percentile, distinct_count (adaptive HLL++), last, first, lag, ema, last_n, exact_min, exact_max, derive.
- **Durable** — append-only WAL fsync'd before ack, periodic snapshots. Worst-case data loss: ~1s. See [Failure modes](#failure-modes).
- **Zero `unsafe` outside FFI in the hot path** — 4 unsafe blocks total, all libc FFI in `event_log.rs`. Audit in [UNSAFE.md](UNSAFE.md).

## Quick Start (60 seconds)

### 1. Start the server

```bash
git clone https://github.com/petrpan26/beava.git
cd beava
docker compose up -d
```

Or build from source:

```bash
cargo build --release && ./target/release/beava
```

### 2. Run the demo

```bash
bash examples/fraud/demo.sh
```

You'll see a real-time fraud feature vector for user `u123` — tx count, sum, avg, max, distinct merchants, last seen — computed across a sliding 1h window as 200 events stream in. That's a Beava pipeline: one binary, one JSON definition, features served from memory in microseconds.

```
==> Fetching features for u123

    tx_count_1h              62
    tx_sum_1h                $ 2,296.46
    avg_amount               $ 37.04
    max_amount_1h            $ 200.77
    unique_merchants         8
    last_merchant            shell_gas
    last_amount              $ 47.05
```

### 3. Author your own feature

Read the [walkthrough in `examples/fraud/`](examples/fraud/README.md), or jump to the [Python SDK guide](docs/python-sdk.md).

### Install the Python SDK directly

```bash
cd python && pip install -e .
```

## What Beava replaces in your stack

For teams without a streaming platform group:

| What you run today | What Beava is |
|---|---|
| Postgres triggers + Redis counters + a nightly cron to heal drift | One binary, one API, one mental model |
| A weekend of Kafka setup before the first feature ships | A Python function decorated with `@bv.stream` |
| Staging that silently drifts from prod | `bv.fork()` — a scoped replica of live prod state |
| A platform team rewriting your DS prototype as a Flink job | The same Python file, laptop to production |

For teams **already running** Flink or Kafka well — those are excellent systems, keep running them. Beava exists for the long tail that hasn't built that platform yet, not as a replacement for teams who have.

## How Beava compares to Kafka + Flink + Redis

A fair comparison — with the asymmetries called out, not hidden.

| Dimension | Kafka + Flink + Redis | Beava |
|---|---|---|
| Systems to deploy | 5-8 | 1 |
| Separate message broker | Yes (Kafka) | No — direct TCP push |
| State access (hot path) | Heap state: <1µs · RocksDB fallback: 5-15µs when state exceeds heap | In-process RAM: ~0.1µs single-core lookup |
| Durability model | Replicated Kafka log + Flink checkpoints | SSD WAL fsync + snapshot + primary/replica; manual failover today |
| Hot-key contention p99 | Configurable via state backend | ~180µs at 8 concurrent writers; shard the key for higher fanout |
| Deploy | Kubernetes + Helm + operators | Single binary under systemd |
| Ops surface | 0.5-1.0 FTE cluster ops | Prometheus `/metrics`, structured JSON logs, `/health` |
| Time to first feature | Weeks | Minutes |

See [`docs/comparison.md`](docs/comparison.md) for the deeper analysis (operator semantics, watermark handling, connector ecosystem).

## Iterate features against live prod state — `bv.fork()`

A Python `with` block that gives you a scoped replica of live production state. Iterate against real production bytes, close the context, production never sees your reads.

```python
with bv.fork("beava-prod.internal", scope={"user_id": "u123"}):
    print(OnboardingSignals.get("u123").clicks_10m)
```

Default is snapshot isolation (replica frozen at entry). Pass `tail=True` to follow CDC updates from prod in real time. Production writes don't propagate back — forks are read-only by design.

**What this fixes:** the "my staging data says 47.3 and prod says 50.1 and I burn two days finding the difference" bug. Staging-data skew is gone.

**What this doesn't fix:** feature-logic drift between replay and live push (if you forgot to handle late events, fork won't save you), or operational differences between your laptop and prod hardware. Full semantics in [SEMANTICS.md](SEMANTICS.md).

## AI editor skill (Claude Code / Cursor / Codex)

Beava ships a skill that teaches modern AI editors how to build, debug, and capacity-plan Beava pipelines — with real numbers from `/debug/*`, not hand-wavey advice.

```bash
beava install-skill          # user-level: ~/.agents/skills/beava/
beava install-skill --repo   # or: ./.agents/skills/beava/ in the current repo
```

Then in your editor:

- **Claude Code:** `/beava` (or `/beava feature`, `/beava debug`, `/beava plan`, `/beava estimate`)
- **Cursor** (Agent mode, ⌘L): `@beava` or describe the task — *"add a velocity feature at 10M users scale"*
- **Codex CLI:** `/skills beava`

Point the skill at a cluster with `export BEAVA_URL=https://...` and `BEAVA_TOKEN=...`.

## Performance

47-feature fraud pipeline, Zipfian key distribution. Reproduce in 70 seconds with `bash benchmark/fraud-pipeline/run_bench.sh`.

| Metric | 16-core Hetzner AX52 | 10-core M-series laptop |
|--------|---|---|
| Throughput (sustained) | 544K events/sec | 314K events/sec |
| Memory per entity | ~8 KB (15 features incl. HLL++) | ~8 KB |
| p99 reads — single-client | <100µs | ~3ms batched |
| p99 reads — 8-client contention | ~180µs server-side | (hot-key bound) |
| Sustained load | 29M events, zero degradation | 60s × 8 clients, no drift |

Full methodology + the "how batch-p99 differs from per-event-p99" caveat: [`benchmark/README.md`](benchmark/README.md). Baseline data committed in `benchmark/fraud-pipeline/results/baseline/`.

Why this fast: everything in memory on one node. No network hops between services, no serialization to RocksDB, no GC pauses. A single `HashMap::get` costs ~0.1µs.

## Failure modes

Engineer-honest scope. If you're going to put this in your on-call path, these are the scenarios you need to know.

**Every push fsynced to WAL before ack.** Worst-case data loss: ~1 second (group-commit window). Snapshots every 5 min (tunable via `BEAVA_SNAPSHOT_INTERVAL`). On crash: replay from latest snapshot + WAL tail. Typical recovery: ~30 seconds per 10M events.

**At RAM ceiling.** Beava rejects new writes and preserves committed state — it does not spill to disk. Plan capacity with `BEAVA_MEMORY_LIMIT_MB`. If you hit it, resize the box and restart. Fail-loud by default.

**Process crash, mid-window.** WAL replay on restart; in-flight events since last fsync re-deliver. At-least-once. For exactly-once counters, dedup via `event_id` at ingest.

**Single binary = single blast radius.** Primary/replica replication ships today with manual promotion on primary death. Automated HA failover is Cloud. If you need redundancy now, run a primary/replica pair and dedup at the edge via `event_id`.

**Hot-key contention.** Single-writer keys stay sub-50µs even to 10M entities. 8 concurrent writers on the same key reaches ~180µs p99 — contention, not scale regression. Shard hot keys (e.g. `user_id + hour_bucket`) or debounce them.

**Observability.** Prometheus metrics at `/metrics`, structured JSON logs to stdout, `/health` endpoint for orchestrators. Sample Grafana dashboard in `deploy/`.

## Security baseline (self-hosted, today)

What the binary ships with:

- Admin-only HTTP endpoints (pipeline registration, debug introspection) gated by `BEAVA_ADMIN_TOKEN`
- `/health` and `/metrics` are safe to expose; all other endpoints require the token
- Deploy behind a TLS terminator (Caddy, nginx, traefik) for wire encryption
- Filesystem encryption (LUKS, dm-crypt, ZFS native) is your choice for WAL + snapshot at rest

What we're building:

- Built-in TLS on the binary TCP ingest port (v1.x roadmap)
- mTLS + RBAC + structured audit log (v1.0)
- AES-256 at rest, SOC2 Type II, HIPAA BAA (Beava Cloud, Q4 2026 target)

**Regulated deployments** — Beava is not a SOC2/HIPAA vendor today. If compliance is a hard gate, wait for Beava Cloud when it ships.

## Honest scope

- Pre-launch OSS. API will move between v0.x releases.
- No SOC2, HIPAA, or PCI today (Beava Cloud, Q4 2026 target).
- Single region. No cross-region replication.
- Working set must fit in RAM. Modern instances reach 1.5 TB+ (~100M+ keyed entities at 40 features).
- Primary/replica with manual failover. Automated HA is Cloud.
- At-least-once delivery. Dedup via `event_id` for exactly-once counters.
- No embedding generation today. On roadmap if demand is there.

If any of those is a hard stop — star the repo, come back when Cloud ships.

## Maintainer status + lock-in exit ramp

Beava is a solo-maintainer project today. Commit cadence and contributor stats are public on GitHub. We commit to publishing a handover plan and moving to a multi-maintainer model before charging for Cloud.

**Lock-in exit ramp.** The on-disk event log is a documented format (see [SEMANTICS.md](SEMANTICS.md)). State export to Parquet via `bv export` is on roadmap. If the project goes quiet for 90 days, `abandoned.md` lands in the repo — Apache 2.0 + no CLA means you can fork everything (server, SDK, debug endpoints, Claude skill) with no legal friction.

Full disclosure: [MAINTAINERS.md](MAINTAINERS.md) · [GOVERNANCE.md](GOVERNANCE.md).

## Docs

- [Architecture](docs/architecture.md) — system design, event flow, state management
- [Operators Reference](docs/operators.md) — all 16 operators with signatures, memory, examples
- [TCP Protocol](docs/protocol.md) — binary wire format. Build a client in any language.
- [HTTP Management API](docs/http-api.md) — health, metrics, debug endpoints
- [Python SDK Guide](docs/python-sdk.md) — installation, pipeline definition, client usage
- [Fork Semantics](SEMANTICS.md) — consistency model, dedup, watermarks
- [Comparison](docs/comparison.md) — Beava vs Flink+Kafka+Redis: cost, complexity, performance
- [Governance](GOVERNANCE.md) — Apache 2.0 perpetuity, Cloud line-drawing, trademark posture
- [Benchmarks](benchmark/README.md) — reproducers + methodology
- [UNSAFE audit](UNSAFE.md) — every unsafe block, line by line

## Architecture

```
                    +-----------+
                    |  Clients  |   (Python SDK, or any TCP client)
                    +-----+-----+
                          | Binary TCP protocol (port 6400)
                          v
+------------------------------------------------------+
|                    Beava Server                       |
|                                                      |
|   +------------------+     +---------------------+   |
|   | Command Handler  | --> | Pipeline Engine     |   |
|   | PUSH / GET / SET |     | DAG cascade, 16 ops,|   |
|   | MSET / REGISTER  |     | expressions, windows|   |
|   +------------------+     +----------+----------+   |
|                                       |              |
|                            +----------v----------+   |
|                            | State Store         |   |
|   +------------------+     | In-memory (DashMap) |   |
|   | HTTP Management  |     | All state in RAM    |   |
|   | /health /metrics |     +----------+----------+   |
|   | /debug /pipelines|                |              |
|   +------------------+     +----------v----------+   |
|     (port 6401)            | WAL + Snapshots     |   |
|                            | (local disk)        |   |
|                            +---------------------+   |
+------------------------------------------------------+
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `BEAVA_TCP_PORT` | `6400` | Binary protocol port |
| `BEAVA_HTTP_PORT` | `6401` | HTTP management port |
| `BEAVA_WORKER_THREADS` | `4` | Tokio worker threads |
| `BEAVA_SNAPSHOT` | `true` | Periodic snapshots to disk |
| `BEAVA_EVENT_LOG` | `true` | Append-only WAL for replay |
| `BEAVA_ADMIN_TOKEN` | _(unset)_ | Required for admin endpoints |
| `BEAVA_MEMORY_LIMIT_MB` | _(unset)_ | Fail-loud RAM cap |

## Community

- [GitHub Issues](https://github.com/petrpan26/beava/issues) — bugs and feature requests
- [GitHub Discussions](https://github.com/petrpan26/beava/discussions) — questions, proposals, design partner inquiries

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and PR process.

```bash
cargo test -- --test-threads=1            # Rust tests
cd python && python -m pytest tests/ -q   # Python SDK tests
```

## See Also

- [beava.dev](https://beava.dev) — landing page
- [Streaming Shouldn't Require a Platform Team](docs/blog/streaming-shouldnt-require-a-platform-team.md) — the long-form story
- [Benchmark README](benchmark/README.md) — run the numbers yourself
- [Design Partners — 2 slots this quarter](https://beava.dev#design-partner) — 90 days, direct Slack channel

## License

[Apache 2.0](LICENSE)
