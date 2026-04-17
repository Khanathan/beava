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
# Single Rust binary, in-memory state, WAL + snapshot on local disk.
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

- **Single binary** — one process, one port, one data directory; deploy under systemd (see [deploy/beava.service](deploy/beava.service))
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

**Sources:** anything that calls `app.push()` — a webhook handler, a Kafka consumer, a Snowflake CDC reader, a replay script. Beava ships the TCP/Python surface; the glue to your event source is code you write (typically < 50 lines).

**Sinks:** HTTP GET from your model server (Vertex, SageMaker, TorchServe, custom) · Parquet export for offline training parity (roadmap) · webhook fan-out.

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
| Deploy | Kubernetes + Helm + operators | Single binary under systemd |
| Ops surface | 0.5-1.0 FTE cluster ops | Prometheus `/metrics`, JSON logs, `/health` |
| Time to first feature | Weeks | Minutes |

See [`docs/comparison.md`](docs/comparison.md) for deeper analysis.

## Iterate features against live prod state — `bv.fork()`

A Python `with` block that gives you a scoped replica of live production state. Define a new feature inside it, see the value it would produce from real production history, close the context, production never sees your reads.

```python
# Define your new table definition with the usual decorator
@bv.table(key="user_id")
def UserActivityV2(c: Click) -> bv.Table:
    return c.group_by("user_id").agg(
        clicks_10m = bv.count(window="10m"),
        clicks_30m = bv.count(window="30m"),     # new
    )

# Fork against prod, scoped to specific streams + keys, pass your
# candidate pipeline — fork materializes it against live prod state.
with bv.fork(
    remote="beava-prod.internal:6400",
    streams=[Click],
    keys=["u123"],
    pipelines=[UserActivityV2],
) as fork:
    print(fork.get(UserActivityV2, key="u123"))   # real prod history
```

**Offline/online parity:** the same operators process events in fork and in live push. See [SEMANTICS.md](SEMANTICS.md) for the consistency model.

What fork doesn't fix: feature-logic bugs that depend on timing (late events, out-of-order windows).

## Training data — point-in-time extract

When you need historical feature values to train a model, launch a fork with one or more extraction timestamps. The replica replays production's event log and snapshots per-entity feature state as it crosses each Tᵢ, exactly as the online server would have returned them at that instant.

```bash
beava fork \
  --remote beava-prod.internal:6400 \
  --streams Click,Transaction \
  --keys u123,u456 \
  --pipeline-file pipeline.json \
  --extract-at 2026-03-01T10:00:00Z,2026-03-15T10:00:00Z,2026-04-01T10:00:00Z \
  --token $BEAVA_REPLICA_TOKEN
# Wait for the "catchup complete" banner, then:
curl localhost:7400/extracts | jq .
```

Output is JSON — one entry per `(timestamp, entity_key)`. Convert downstream with pandas for anything columnar:

```python
import pandas as pd, requests
resp = requests.get("http://localhost:7400/extracts").json()["extracts"]
rows = [
    {"timestamp": ts, "key": key, **features}
    for ts, per_key in resp.items()
    for key, features in per_key.items()
]
pd.DataFrame(rows).to_parquet("training.parquet")
```

The shape is what training pipelines actually want — point-in-time features keyed by entity, correct by construction because the same operators ran in replay as run online. A dedicated `bv export` would wrap this three-line pandas step and the audit flagged it as fabricated — the real extract API is `--extract-at` + `GET /extracts`, above.

## Use cases

### Real-time fraud scoring
47-feature pipeline, 315K events/sec sustained on a 10-core Apple M4 laptop (server p99 42 µs per event). See [examples/fraud/](examples/fraud/).

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

47-feature fraud pipeline, 10K-key Zipfian distribution. Reproduce in ~75 seconds with `bash benchmark/fraud-pipeline/run_bench.sh`.

| Metric | 10-core Apple M4 laptop, 32 GB RAM |
|--------|---|
| Throughput (sustained) | 315K events/sec |
| Per-event wall time (aggregate, 8 clients) | 3.18 µs |
| Server-side PUSH latency (per event) | p50 21 µs · p95 31 µs · p99 42 µs |
| Memory per entity (47-feature fraud pipeline) | ~616 KB |
| Client batch latency (1000-event `push_many`) | p50 ~2.6 ms · p99 ~126 ms |
| Sustained load | 60s × 8 clients, no drift |

Committed run: [`benchmark/fraud-pipeline/results/baseline/summary.json`](benchmark/fraud-pipeline/results/baseline/summary.json). Per-entity memory varies widely by operator mix — the 47-feature fraud pipeline carries several HLL sketches per key, so simpler pipelines run an order of magnitude less. Full methodology + the batch-p99-vs-per-event caveat: [`benchmark/README.md`](benchmark/README.md).

## Failure modes

**Every push `write()`-appended to the WAL before client ack; fsync is on a timer** (Redis `appendfsync everysec` pattern). The hot PUSH path does not call `fsync` — a background tokio task does, every `BEAVA_FSYNC_INTERVAL_MS` (default 1000ms). Data-loss window on an ungraceful crash = the fsync interval plus the delta snapshot interval (default 30s). Recovery on restart = load the last snapshot (base + N deltas). The WAL is **not** replayed into operator state on restart; it exists for at-least-once push durability and for backfilling newly-registered features against history.

Measured on a 10-core Apple M4 (NVMe), peak-load server-side p99 per event:

| `BEAVA_FSYNC_INTERVAL_MS` | throughput | server p99 | data-loss window |
|---|---:|---:|---:|
| 1000 (Redis-parity default) | ~320K eps | 65 µs | ≤1s |
| 100 | ~335K eps | 51 µs | ≤100ms |
| 1 | ~340K eps | 44 µs | ≤1ms |

fsync does not block the hot PUSH path (it's a background timer), but more frequent fsyncs write less accumulated dirty data per syscall and cause less disk-I/O-queue contention with in-flight writes — hence the lower p99 at shorter intervals. On NVMe, 1ms is viable; on slower disks, prefer the 100ms default.

**Snapshot-interval tuning** (`BEAVA_SNAPSHOT_INTERVAL_MS`, clamped [100ms, 600s], default 30000ms). Snapshot writes run on `tokio::spawn_blocking` and never block the hot PUSH path, but the entity clone that precedes each write walks the store's DashMap briefly:

| `BEAVA_SNAPSHOT_INTERVAL_MS` | throughput | server p99 | notes |
|---|---:|---:|---|
| 30000 (default) | ~320K eps | 65 µs | baseline |
| 10000 | ~360K eps | 39 µs | best observed |
| 5000 | ~355K eps | 41 µs | similar |
| 1000 | **~215K eps (−33%)** | **133 µs (+2×)** | snapshots run back-to-back |

10s is the sweet spot on this hardware: narrower data-loss window than the default with slightly better throughput. 1s is counter-productive — snapshots queue up and dominate disk I/O. Recovery wall-clock measured at 10s interval: 9s (vs 7s at 30s default) — noise-close, recovery cost scales with snapshot **size**, not cadence.

**Retention**: old base snapshots and deltas are auto-deleted on each new base cycle (see `cleanup_old_snapshots` in `src/main.rs`). Event-log entries older than per-stream `history_ttl` (default 90 days) are compacted out on a 60s timer. No unbounded disk growth.

Recovery wall-clock, measured on a 10-core Apple M4 (NVMe): **7.04s for a 4.7 GB on-disk state (10.3M events, 24,945 entities)**. Repro: `bash benchmark/recovery/run_recovery_bench.sh`. Scales with snapshot size; snapshot load is a synchronous postcard deserialization before the TCP listener binds, so `/debug/ready` flips 200 the moment the server is serving-correct.

**At RAM ceiling.** `BEAVA_MEMORY_LIMIT_MB` is a fail-loud cap — committed state is preserved but new writes stop until you resize. There is no disk spill today.

**Process crash, mid-window.** Snapshot-based restore; delivery is at-least-once (push reconnect-and-resend on broken pipe). Server-side `event_id` dedup is not shipped and is no longer on the roadmap — at-least-once is the stable semantic. Idempotency on accumulating operators (`count`, `sum`) is the client's responsibility.

**fsync + snapshot stalls.** Group-commit batches push fsyncs; the snapshot loop runs every 5 min on `spawn_blocking` so ingest isn't synchronously blocked. Quantified latency under snapshot load is not yet benchmarked — planning headroom is the operator's call.

**Single node, no HA today.** No primary/replica, no automated failover. For redundancy: run a cold standby and periodically snapshot to it, or accept the ~30s restart window. Automated HA failover is on the Cloud roadmap (Q4 2026).

**Hot-key contention.** Concurrent writers on a single key contend on per-key locks. The laptop baseline ran 8 clients against 10K keys (Zipfian) — contended p99 is visibly worse than single-client. Shard hot keys (e.g. `user_id + hour_bucket`) or debounce if this matters for your workload.

**Observability.** Prometheus at `/metrics`, structured JSON logs to stdout, `/health` for orchestrators. Sample Caddyfile + systemd unit in [deploy/](deploy/).

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
- Working set must fit in RAM. Capacity scales with available memory; actual per-entity cost depends heavily on the operator mix (HLL sketches are the big line item).
- Single node, no HA today. Automated failover is Cloud.
- At-least-once delivery. Dedup via `event_id` for exactly-once counters.
- No embedding generation today. On roadmap if demand is there.

## Maintainer status + lock-in exit ramp

Beava is a solo-maintainer project today. Commit cadence and contributor stats are public on GitHub.

**Maintainer status:** bringing on a second committer is a goal — no committed timeline today (see [MAINTAINERS.md](MAINTAINERS.md) and [GOVERNANCE.md](GOVERNANCE.md) for the actual disclosure). Apache 2.0 + no CLA means if the project stalls, you can fork everything with no legal friction — that is the real contingency.

**Lock-in exit ramp:**
- On-disk event log is a documented format (see [SEMANTICS.md](SEMANTICS.md))
- Parquet state export for training-data reuse or migration (roadmap)
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
