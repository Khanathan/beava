<p align="center">
  <b><font size="6">Beava</font></b>
  <br>
  <i>The one-binary feature server</i>
</p>

<p align="center">
  <a href="https://github.com/petrpan26/beava/actions/workflows/ci.yml"><img src="https://github.com/petrpan26/beava/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License"></a>
</p>

---

So what breaks when you write a real-time feature today ?

You write Python. Someone rewrites it in Scala. Three sprints later it
ships and the values don't match your offline work. You debug via Slack
pings to someone who doesn't own the business context. Your PM is angry.
You are angry (with yourself, with platform, with streaming, a little
with yourself again).

**Beava is the loop that doesn't do that.**

```python
import beava as bv

with bv.fork("beava-prod.internal", scope={"user_id": "u123"}):
    # Scoped copy of live prod state. Iterate against real bytes.
    # Close the context → prod is untouched.
    print(OnboardingSignals.get("u123").clicks_10m)
```

One file. `@bv.stream`, `@bv.table`, `bv.replay()`, `bv.fork()`. Backfill
against history. Fork a scoped replica of prod. Ship. Same code laptop
to production. No Scala rewrite, no handoff (we've all been there).

- [So what is Beava ?](#so-what-is-beava-)
- [Quick Start](#quick-start-60-seconds)
- [So what is `bv.fork()` ?](#so-what-is-bvfork-)
- [Performance](#performance)
- [Comparison](#comparison)
- [Honest limits](#honest-limits)
- [Documentation](#docs)
- [Configuration](#configuration)
- [Community](#community)

## So what is Beava ?

It's basically a feature server that run on one box. Single Rust binary,
all state in memory, append-only event log for durability (group-commit
fsync before ack), Python SDK. Apache 2.0.

The four primitives are the whole API:

- `@bv.stream` — declare an event shape
- `@bv.table` — declare an aggregation, keyed or keyless
- `bv.replay()` — backfill against historical events
- `bv.fork()` — scoped replica of live prod state for feature iteration

That last one is the part I'm proudest of. Every other feature store
makes you pick between *stale staging* (your test says 47.3, prod says
50.1, you burn two days) or *poke at prod and pray* (no isolation, your
PM is angry again). Fork is the third option.

**Use cases:** real-time fraud scoring · ML feature serving · session
features (last-N-click) · rate limits with sliding windows · recsys
freshness · gaming leaderboards · AI agent context · IoT anomaly
detection.

**Key properties:**

- **Synchronous and atomic writes** — push an event, all operators across
  all pipeline stages update in one pass. State is immediately consistent.
  No eventual consistency, no propagation delay. Easy to reason about.
- **Sub-microsecond reads** — all state in RAM on one node. A `HashMap::get`
  costs ~0.1µs. A Flink RocksDB state access costs 5-15µs.
- **Pipeline cascades** — multi-stage DAGs with `depends_on`. Events
  propagate in topological order, all in one request.
- **16 operators** — count, sum, avg, min, max, stddev, percentile,
  distinct_count (adaptive HLL++), last, first, lag, ema, last_n,
  exact_min, exact_max, derive.
- **Sliding windows** — configurable granularity (30m, 1h, 24h, 7d).
  Bucketed ring buffers for bounded memory.
- **Expression engine** — derive expressions, where-clause filters,
  cross-stream references. 21 builtins.
- **Binary TCP protocol** — persistent connections, length-prefixed frames,
  minimal overhead. Any language can implement a client.
- **Durable** — append-only event log + periodic snapshots. On crash,
  state recovers from snapshot + log replay. Worst-case ~1s of data loss.
- **Zero unsafe in the hot path** — 4 unsafe blocks total, all libc FFI
  in `event_log.rs`. See [UNSAFE.md](UNSAFE.md).

## Quick Start (60 seconds)

### 1. Start the server

```bash
git clone https://github.com/petrpan26/beava.git
cd beava
docker compose up -d
```

### 2. Run the demo

```bash
bash examples/fraud/demo.sh
```

You'll see a real-time fraud feature vector for user `u123` — tx count,
sum, avg, max, distinct merchants, last seen — computed across a sliding
1h window as 200 events stream in. That's a Beava pipeline: one binary,
one JSON definition, features served from memory in microseconds.

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

Read the [walkthrough in `examples/fraud/`](examples/fraud/README.md) to
see exactly how the pipeline and push flow work, or jump straight to the
[Python SDK guide](docs/python-sdk.md).

### Alternatives

Build from source instead of Docker:

```bash
cargo build --release && ./target/release/beava
```

Or install the Python SDK directly (for `import beava as bv`):

```bash
cd python && pip install -e .
```

## So what is `bv.fork()` ?

It's basically a `with` block that gives you a local replica of live
prod state, scoped to whatever keys you care about. You iterate features
against REAL production bytes — not stale staging data — then close the
context and prod doesn't care.

That's it. That's the primitive.

```python
import beava as bv

with bv.fork("beava-prod.internal", scope={"user_id": "u123"}):
    # Replica is frozen at entry by default (snapshot isolation).
    # Pass tail=True to follow CDC updates from prod.
    print(OnboardingSignals.get("u123").clicks_10m)

    # Hack on the feature, push a synthetic event into the local replica
    # to validate, all without prod ever seeing your reads.
```

Does it solve all skew ? No. **It closes the staging-data skew axis.**
Feature-logic drift between replay and live push is still your job — if
you forgot to handle late events, fork won't save you. But the *"my test
data is lying to me"* axis, the one that actually burns two days of
debugging — that's gone.

I wanted this for two years and never got it. Full semantics —
consistency model, dedup, watermarks, all grounded in source pointers —
in [SEMANTICS.md](SEMANTICS.md).

### AI editor skill (Claude Code / Cursor / Codex)

Beava ships a skill that teaches modern AI editors how to build, debug,
and capacity-plan Beava pipelines — with real numbers from `/debug/*`,
not hand-wavey advice. Install it once:

```bash
beava install-skill          # user-level: ~/.agents/skills/beava/
beava install-skill --repo   # or: ./.agents/skills/beava/ in the current repo
```

Then in your editor:

- **Claude Code:** `/beava` (no args for the guided walk-through, or `/beava feature`, `/beava debug`, `/beava plan`, `/beava estimate`).
- **Cursor** (Agent mode, ⌘L): `@beava` or describe the task — *"add a velocity feature at 10M users scale"*, *"why is beava at prod.example.com using 40 GB"*.
- **Codex CLI:** `/skills beava`.

The skill walks you through the 5 things that matter: picking the right
operators, sizing memory before you push data, projecting capacity
against real cloud instance prices, debugging a running server via its
`/debug/memory`, `/debug/key/{id}`, and `/debug/topology` endpoints.
Point it at a cluster with `export BEAVA_URL=https://...` and
`BEAVA_TOKEN=...`.

### Define a pipeline and push events

```python
import beava as bv

@bv.stream
class RawTransactions:
    user_id: str
    amount: float
    merchant_id: str

@bv.table(key="user_id")
def UserFeatures(txs: RawTransactions) -> bv.Table:
    return (
        txs.group_by("user_id")
        .agg(
            tx_count_1h      = bv.count(window="1h"),
            tx_count_24h     = bv.count(window="24h"),
            tx_sum_1h        = bv.sum("amount", window="1h"),
            avg_amount       = bv.avg("amount", window="24h"),
            unique_merchants = bv.count_distinct("merchant_id", window="24h"),
        )
        .with_columns(
            velocity=(bv.col("tx_count_1h") / 1) / (bv.col("tx_count_24h") / 24),
        )
    )

app = bv.App("localhost:6400")
app.register(RawTransactions, UserFeatures)

# Push events (fire-and-forget, maximum throughput)
app.push(RawTransactions, {"user_id": "u123", "amount": 50.0, "merchant_id": "m456"})
app.push(RawTransactions, {"user_id": "u123", "amount": 120.0, "merchant_id": "m789"})
app.push(RawTransactions, {"user_id": "u123", "amount": 25.0, "merchant_id": "m456"})
app.flush()

# Read computed results (instant, from in-memory state)
features = app.get("u123")
print(features.tx_count_1h)        # 3
print(features.unique_merchants)   # 2
print(features.velocity)           # 1.2
```

The Python SDK is the first client. The underlying [binary TCP protocol](docs/protocol.md)
is simple enough that clients in Go, Java, Rust, or any language can be
built against the spec.

## Performance

47-feature fraud pipeline, 8 client processes, Zipfian key distribution.
Reproduce with `bash benchmark/fraud-pipeline/run_bench.sh`. Your
numbers will vary with hardware (in particular: 16-core Hetzner box hits
544K eps; a 10-core M-series Mac hits 314K — both committed in
`benchmark/fraud-pipeline/results/`).

| Metric | Value |
|--------|-------|
| Throughput (8 clients, 16c Hetzner) | 544K events/sec, each computing 47 features |
| Throughput (single client, batched) | ~553K events/sec |
| Sustained load | 29M events, 722K entities, zero degradation |
| Memory per entity | 7.6 KB (15 features incl. HLL++) |
| Latency p99 (single-client) | < 100µs |
| Latency p99 (8-client, hot keys) | ~1.6ms (contention, not scale) |

Why this fast: everything in memory on one node. No network hops between
services, no serialization to RocksDB, no GC pauses. A single
`HashMap::get` costs ~0.1µs.

See [`benchmark/fraud-pipeline/bench.py`](benchmark/fraud-pipeline/bench.py)
for the full benchmark, or `bash benchmark/fraud-pipeline/run_bench.sh`
for one-command reproduction with saved results.

## Comparison

Real-time compute today usually means Kafka + Flink + Redis: 10-25
nodes, $3-15K/mo in infra, 0.5-1.0 FTE in ops. Beava does the same shape
of work on one node.

| | Beava | Kafka + Flink + Redis |
|---|---|---|
| Nodes | 1 | 10-25 |
| Systems to manage | 1 | 5-8 |
| State access latency | ~0.1µs (in-memory) | 5-15µs (RocksDB) |
| Deploy | Single binary, `systemd` | Kubernetes + Helm + operators |
| Ops burden | Check the dashboard | 0.5-1.0 FTE |
| Infra cost (50K eps) | ~$400/mo (one node) | $3-5K/mo |

Beava is for the 99% of use cases that fit on a single node. If you
need distributed exactly-once processing, multi-TB state, or the Kafka
connector ecosystem, use Flink. Flink and Kafka are excellent systems
built by smart people. Beava exists because most teams don't need that
complexity.

See [full comparison](docs/comparison.md) for the deeper analysis.

## Honest limits

- Pre-launch OSS. API stabilizing — minor breakage between v0.x releases.
- No SOC2, HIPAA, or PCI today. (Beava Cloud, Q4 2026 target.)
- Single region. No cross-region replication.
- Working set must fit in RAM. 128 GB box ≈ 10M keyed entities.
- Primary/replica ships. Automated HA failover is Cloud.
- At-least-once delivery. Dedup via `event_id` for exactly-once counters.
- No embedding generation today. On roadmap if anyone wants it enough.

If any of those is a hard stop for you — star the repo, come back when
Cloud ships the compliance tier.

## Docs

- [Architecture](docs/architecture.md) — system design, event flow, state management
- [Operators Reference](docs/operators.md) — all 16 operators with signatures, memory, examples
- [TCP Protocol](docs/protocol.md) — binary wire format. Build a client in any language.
- [HTTP Management API](docs/http-api.md) — health, metrics, debug endpoints, pipeline management
- [Python SDK Guide](docs/python-sdk.md) — installation, pipeline definition, client usage
- [Fork Semantics](SEMANTICS.md) — consistency model, dedup, watermarks
- [Comparison](docs/comparison.md) — Beava vs Flink+Kafka+Redis: cost, complexity, performance
- [Governance](GOVERNANCE.md) — Apache 2.0 perpetuity, Cloud line-drawing, trademark posture
- [Maintainers](MAINTAINERS.md) — sole maintainer today, hiring a second committer (Sept 2026)
- [Unsafe Audit](UNSAFE.md) — every unsafe block, line by line

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
|     (port 6401)            | Snapshots + Event   |   |
|                            | Log (local disk)    |   |
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
| `BEAVA_EVENT_LOG` | `true` | Append-only event log for replay |

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

- [beava.dev](https://beava.dev) — landing page, what people read first
- [Streaming Shouldn't Require a Platform Team](docs/blog/streaming-shouldnt-require-a-platform-team.md) — why we built Beava and the tradeoffs we chose
- [Beava vs Flink+Kafka+Redis](docs/comparison.md) — full cost and complexity comparison
- [TCP Protocol Spec](docs/protocol.md) — build a client in any language
- [Fraud Detection Benchmark](benchmark/fraud-pipeline/bench.py) — 47-feature pipeline, run it yourself
- [Design Partners — 2 slots this quarter](https://beava.dev#design-partner) — 90 days, direct Slack channel

## License

[Apache 2.0](LICENSE)
