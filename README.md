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

Define a pipeline in Python. Push events over TCP. Read aggregated features in microseconds. Apache 2.0.

Replaces Postgres triggers + Redis counters + the cron job that heals drift. Same code from laptop to production: backfill against history, iterate against live prod state with `bv.fork()`, deploy.

```bash
curl -L beava.dev/install | sh
beava serve --data ./beava.db
# 42 MB binary. ~40 MB RSS at idle. ~200 MB per 1M keyed entities.
```

```ini
# /etc/systemd/system/beava.service
[Unit]
Description=Beava
After=network.target
[Service]
ExecStart=/usr/local/bin/beava serve --data /var/lib/beava
Restart=on-failure
Environment=BEAVA_ADMIN_TOKEN=...
[Install]
WantedBy=multi-user.target
```

## New to real-time? (10-second version)

Your app saves events — clicks, logins, purchases. Beava counts them in **running windows** (a window = a rolling time bucket, e.g. the last 10 minutes) and gives you the current count in microseconds.

No message queue, no stream processor, no cache. One Python file, one binary.

```python
import beava as bv

@bv.stream
class Click:                           # one event type
    user_id: str
    page: str

@bv.table(key="user_id")               # one thing to compute per user
def UserActivity(c: Click) -> bv.Table:
    return c.group_by("user_id").agg(
        clicks_10m = bv.count(window="10m"),
    )

app = bv.App("localhost:6400")
app.register(Click, UserActivity)

app.push(Click, {"user_id": "u123", "page": "/checkout"})
print(app.get("u123").clicks_10m)      # → 1, instantly
```

If you already run Kafka + Flink + Redis, the comparison table below is the apples-to-apples.

## Key properties

- **Single binary** — 42 MB · ~40 MB RSS idle · ~200 MB per 1M keyed entities · one port · one data dir
- **No separate message broker** — push events directly over TCP
- **Synchronous and atomic writes** — every operator across every pipeline stage updates in one pass
- **Sub-microsecond state access** — all state in RAM (~0.1µs `HashMap::get`)
- **16 operators** — count, sum, avg, min, max, stddev, percentile, distinct_count (adaptive HLL++), last, first, lag, ema, last_n, exact_min, exact_max, derive
- **Durable** — WAL fsync before client ack, snapshot every 5 min. Single-node today; HA is Cloud.
- **Rust, zero `unsafe` outside 4 libc FFI blocks** (write/fdatasync/fsync in `src/state/event_log.rs`, ~15 LoC total, audited) — see [UNSAFE.md](UNSAFE.md)

## Quick Start

### 1. Start the server

```bash
git clone https://github.com/petrpan26/beava.git
cd beava && docker compose up -d
```

Or build from source: `cargo build --release && ./target/release/beava`.

### 2. Run the demo

```bash
bash examples/fraud/demo.sh
```

You'll see a real-time fraud feature vector for user `u123` — tx count, sum, avg, max, distinct merchants, last seen — computed across a sliding 1h window as 200 events stream in.

```
==> Fetching features for u123
    tx_count_1h              62
    tx_sum_1h                $ 2,296.46
    avg_amount               $ 37.04
    max_amount_1h            $ 200.77
    unique_merchants         8
    last_merchant            shell_gas
```

### 3. Author your own feature

See the [walkthrough in `examples/fraud/`](examples/fraud/README.md) or the [Python SDK guide](docs/python-sdk.md).

## Where Beava fits in your stack

Sources → Beava → Sinks. Beava is the online state; your existing batch/storage stack doesn't move.

**Sources:** Segment webhooks · Kafka topic consumer · Snowflake CDC stream · direct SDK push · replay from Parquet/JSONL.

**Sinks:** HTTP GET from your model server (Vertex, SageMaker, TorchServe, custom) · Parquet export for offline training parity (`bv export`) · webhook fan-out.

If your offline features live in dbt on Snowflake, Beava is the online mirror — same aggregations, microsecond reads, training/serving parity via the same Python decorators.

**Typical time-to-integrate:** Segment webhook → first feature live in under a day on a fresh host.

## What Beava replaces

| What you run today | What Beava is |
|---|---|
| Postgres triggers + Redis counters + a nightly cron to heal drift | One binary, one API, one mental model |
| A weekend of Kafka setup before the first feature ships | A Python function decorated with `@bv.stream` |
| Staging that silently drifts from prod | `bv.fork()` — a scoped replica of live prod state |
| A platform team rewriting your DS prototype as a Flink job | The same Python file, laptop to production |

Teams already running Flink or Kafka well — those are excellent systems, keep running them. Beava exists for the long tail that hasn't built that platform yet.

## How Beava compares to Kafka + Flink + Redis

| Dimension | Kafka + Flink + Redis | Beava |
|---|---|---|
| Systems to deploy | 5-8 | 1 |
| Separate message broker | Yes (Kafka) | No — direct TCP push |
| State access (hot path) | Heap state: <1µs · RocksDB fallback: 5-15µs | In-process RAM: ~0.1µs |
| Durability model | Replicated Kafka log + Flink checkpoints | SSD WAL fsync before client ack + periodic snapshot · single-node today, HA is Cloud |
| Hot-key contention p99 | Configurable via state backend | 180µs @ 8 writers · 480µs @ 32 · 1.2ms @ 64 · [contention curve](benchmark/contention.md) |
| Deploy | Kubernetes + Helm + operators | Single binary under systemd |
| Ops surface | 0.5-1.0 FTE cluster ops | Prometheus `/metrics`, JSON logs, `/health` · [RUNBOOK.md](deploy/RUNBOOK.md) |
| Time to first feature | Weeks | Minutes |

See [`docs/comparison.md`](docs/comparison.md) for deeper analysis.

## Iterate features against live prod state — `bv.fork()`

A Python `with` block that gives you a scoped replica of live production state. Define a new feature inside it, see the value it would produce from real production history, close the context, production never sees your reads.

```python
with bv.fork("beava-prod.internal", scope={"user_id": "u123"}) as fork:
    @fork.table(key="user_id")
    def UserActivityV2(c: Click) -> bv.Table:
        return c.group_by("user_id").agg(
            clicks_10m = bv.count(window="10m"),
            clicks_30m = bv.count(window="30m"),     # new
        )
    print(fork.get("u123").clicks_30m)   # computed from real prod history

# Happy with the number? Ship it:
#   bv deploy user_activity_v2.py
```

**Offline/online parity receipt:** the number you see in the fork is byte-for-byte what the online server will return after you deploy this definition. There is no separate offline path — the same operators process the fork's scoped replica as process live push, by construction. See [SEMANTICS.md § fork-parity](SEMANTICS.md) for the proof sketch.

What fork doesn't fix: feature-logic bugs that depend on timing (late events, out-of-order windows).

## Use cases

### Real-time fraud scoring
47-feature pipeline, sub-100µs single-client reads, 544K eps sustained. See [examples/fraud/](examples/fraud/).

### Agent session state, with TTL

Per-agent session windows:

```python
@bv.table(key="session_id", ttl="1h")   # ttl = idle time since last event
def AgentState(t: ToolCall) -> bv.Table:
    return t.group_by("session_id").agg(
        tool_calls_5m = bv.count(window="5m"),
        error_rate_5m = bv.mean(t.ok == False, window="5m"),
    )
# Per-session state ~200 B. 1M concurrent sessions ≈ 200 MB RAM.
# Wire from your agent step — LangGraph, Inngest, Modal.
```

### Session signals for recsys
Last-N-click windows, recency, velocity, distinct-merchant counts.

### Real-time dashboards
Aggregations served from memory, no cache invalidation.

## Performance

47-feature fraud pipeline, Zipfian distribution. Reproduce in 70 seconds with `bash benchmark/fraud-pipeline/run_bench.sh`.

| Metric | 16-core Hetzner AX52 | 10-core M-series laptop |
|--------|---|---|
| Throughput (sustained) | 544K events/sec | 314K events/sec |
| Memory per entity | ~8 KB | ~8 KB |
| p99 reads — single-client | <100µs | ~3ms batched |
| p99 reads — 8-client contention | ~180µs server-side | (hot-key bound) |
| Sustained load | 29M events, zero degradation | 60s × 8 clients, no drift |

HdrHistogram, 256B payload, 1M-key cardinality, coordinated-omission corrected. Full methodology: [`benchmark/README.md`](benchmark/README.md).

## Failure modes

**Every push fsynced to WAL before client ack.** Worst-case data loss on crash: ~1 second (group-commit window). Recovery: ~30s per 10M events on NVMe ([reproducer](benchmark/recovery.md)).

**At RAM ceiling.** Beava rejects new writes with `STATUS_SERVER_BUSY`. Python SDK retries (default: 5 retries, 50ms initial, 2× factor, cap 1s). Committed state preserved — no disk spill.

**Process crash, mid-window.** WAL replay on restart; at-least-once. For exactly-once counters, dedup via `event_id` — per-key LRU Bloom filter, 64 B/key, 5-min window (tunable). Target FPR 0.1% at 544K eps sustained (~163M events per window).

**fsync + snapshot stalls.** p99 ingest lag during snapshot stays <20ms at 500K eps on NVMe. Cloud-standard gp3 degrades ~2×; plan headroom. Alert on `beava_fsync_stall_seconds > 2s` sustained 5m.

**Single node, no HA today.** No primary/replica, no automated failover. For redundancy: run a cold standby and periodically snapshot to it, or accept the ~30s restart window. Automated HA failover is on the Cloud roadmap (Q4 2026).

**Hot-key contention.** 180µs @ 8 writers · 480µs @ 32 · 1.2ms @ 64 on one key. Shard (`user_id + hour_bucket`) or debounce beyond.

**Observability.** Prometheus at `/metrics`, JSON logs to stdout, `/health` for orchestrators. [RUNBOOK.md](deploy/RUNBOOK.md) lists the 5 metrics that should page.

## Security baseline (self-hosted, today)

- Admin-only HTTP endpoints gated by `BEAVA_ADMIN_TOKEN`
- `/health` and `/metrics` safe to expose; all other endpoints require the token
- Deploy behind a TLS terminator (Caddy, nginx, traefik) for wire encryption
- Filesystem encryption (LUKS, dm-crypt, ZFS native) at rest is your choice

Roadmap: built-in TLS on ingest (v1.x), mTLS + RBAC + audit log (v1.0), AES-256 at rest + SOC2 Type II + HIPAA BAA (Beava Cloud, Q4 2026). Regulated deployments today — wait for Cloud.

## Scope

- Pre-launch OSS. API will move between v0.x releases.
- No SOC2, HIPAA, PCI today (Cloud, Q4 2026 target).
- Single region. No cross-region replication.
- Working set must fit in RAM. Modern instances reach 1.5 TB+.
- Single node, no HA today. Automated failover is Cloud.
- At-least-once delivery. Dedup via `event_id` for exactly-once counters.
- No embedding generation today. On roadmap if demand is there.

## Maintainer status + lock-in exit ramp

Beava is a solo-maintainer project today. Commit cadence and contributor stats are public on GitHub.

**Dated commitment:** second committer with merge rights by end of Q3 2026. If that date passes without a second committer, a scheduled GitHub Action commits `abandoned.md` to the repo automatically. You have full fork rights under Apache 2.0 + no CLA with no legal friction.

**Lock-in exit ramp:**
- On-disk event log is a documented format (see [SEMANTICS.md](SEMANTICS.md))
- `bv export` dumps state to Parquet for training-data reuse or migration (roadmap v0.2)
- All 16 operators are open specifications — reimplementable in another engine

Full disclosure: [MAINTAINERS.md](MAINTAINERS.md) · [GOVERNANCE.md](GOVERNANCE.md).

## Documentation

- [Architecture](docs/architecture.md)
- [Operators Reference](docs/operators.md)
- [TCP Protocol](docs/protocol.md)
- [HTTP Management API](docs/http-api.md)
- [Python SDK Guide](docs/python-sdk.md)
- [Fork Semantics](SEMANTICS.md)
- [Comparison](docs/comparison.md)
- [Governance](GOVERNANCE.md)
- [Benchmarks](benchmark/README.md)
- [UNSAFE audit](UNSAFE.md)

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

- [GitHub Issues](https://github.com/petrpan26/beava/issues)
- [GitHub Discussions](https://github.com/petrpan26/beava/discussions)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

```bash
cargo test -- --test-threads=1
cd python && python -m pytest tests/ -q
```

## License

[Apache 2.0](LICENSE)
