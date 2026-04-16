<p align="center">
  <b><font size="6">Beava</font></b>
  <br>
  <i>Real-time compute engine</i>
</p>

<p align="center">
  <a href="https://github.com/petrpan26/beava/actions/workflows/ci.yml"><img src="https://github.com/petrpan26/beava/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License"></a>
</p>

---

Beava is a real-time compute engine. Define pipelines. Push events. Get results
in microseconds. One binary, completely in-memory, zero infrastructure.

Everything lives in memory on a single node, so reads and writes are
sub-microsecond with no network hops, no serialization tax, no distributed
coordination overhead. Durable via append-only event log (WAL) + periodic snapshots.

- [What is Beava?](#what-is-beava)
- [Quick Start](#quick-start)
- [Performance](#performance)
- [Documentation](#docs)
- [Comparison: Beava vs Flink+Kafka+Redis](#comparison)
- [Configuration](#configuration)
- [Community](#community)
- [Contributing](#contributing)

## What is Beava?

Beava **ingests** events over a binary TCP protocol, **computes** streaming
aggregations (windowed counts, sums, averages, percentiles, HLL distinct counts,
and more), and **cascades** results through multi-stage pipeline DAGs. Every write
is synchronous and atomic -- all operators update in one pass, state is immediately
consistent. Push events at 400K/sec, read results in microseconds.

Today, real-time compute means Kafka + Flink + Redis. That's 10-25 nodes,
5-8 systems, and a platform team to keep it running. Most teams never get past
the evaluation phase. Beava makes real-time compute accessible to any team:
one Rust binary, a pipeline definition, and `push()`.

**Use cases:** fraud detection, ML feature serving, real-time personalization,
gaming leaderboards, AI agent context, IoT anomaly detection.

**Key properties:**

- **Every write is synchronous and atomic** -- push an event, all operators across all pipeline stages update in one pass. State is immediately consistent. No eventual consistency, no propagation delay. Easy to reason about.
- **Fast writes, instant reads** -- push is fire-and-forget for maximum throughput. GET serves the latest state from memory in microseconds. Read-after-write is always consistent.
- **Completely in-memory** -- all state lives in RAM on a single node. No disk reads on the hot path. Sub-microsecond state access.
- **Pipeline cascades** -- define multi-stage pipelines with `depends_on`. Events propagate through the DAG in topological order, all within one request.
- **16 operators** -- count, sum, avg, min, max, stddev, percentile, distinct_count (adaptive HLL++), last, first, lag, ema, last_n, exact_min, exact_max, derive.
- **Sliding windows** -- configurable granularity (30m, 1h, 24h, 7d). Bucketed ring buffers for bounded memory.
- **Expression engine** -- derive expressions, where-clause filters, cross-stream references. 21 builtins.
- **Binary TCP protocol** -- persistent connections, length-prefixed frames, minimal overhead. Any language can implement a client.
- **Durable** -- append-only event log (WAL) + periodic snapshots. On crash, state recovers from snapshot + WAL replay. At most ~1s of data loss in the worst case.

## Quick Start

### Option A: Docker

```bash
git clone https://github.com/petrpan26/beava.git && cd beava
docker compose up -d
```

### Option B: Build from source

```bash
git clone https://github.com/petrpan26/beava.git && cd beava
cargo build --release
./target/release/beava
```

### Install the Python SDK (first available client)

```bash
cd python && pip install -e .
```

### AI editor skill (Claude Code / Cursor / Codex)

Beava ships a skill that teaches modern AI editors how to build, debug, and
capacity-plan Beava pipelines — with real numbers from `/debug/*`, not
hand-wavey advice. Install it once:

```bash
beava install-skill          # user-level: ~/.agents/skills/beava/
beava install-skill --repo   # or: ./.agents/skills/beava/ in the current repo
```

Then in your editor:

- **Claude Code:** `/beava` (no args for the guided walk-through, or `/beava feature`, `/beava debug`, `/beava plan`, `/beava estimate`).
- **Cursor** (Agent mode, ⌘L): `@beava` or describe the task — *"add a velocity feature at 10M users scale"*, *"why is beava at prod.example.com using 40 GB"*.
- **Codex CLI:** `/skills beava`.

The skill walks you through the 5 things that matter: picking the right
operators, sizing memory before you push data, projecting capacity against
real cloud instance prices, and debugging a running server via its
`/debug/memory`, `/debug/key/{id}`, and `/debug/topology` endpoints. Point it
at a cluster with `export BEAVA_URL=https://...` and `BEAVA_TOKEN=...`.

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

The Python SDK is the first client implementation. The underlying
[binary TCP protocol](docs/protocol.md) is simple enough that clients
in Go, Java, Rust, or any language can be built against the spec.

## Performance

Measured on a 48-core Xeon with 8 Python client processes, realistic fraud
detection pipeline (47 features across 5 entity types, Zipfian key distribution).
Your results will vary with hardware.

| Metric | Value |
|--------|-------|
| Throughput (8 clients) | 430-510K events/sec (each computing 47 features) |
| Throughput (single client) | 270K events/sec |
| Sustained load | 29M events, 722K entities, zero degradation |
| Memory per entity | 7.6 KB (15 features incl. HLL++) |
| Latency (p99) | < 100 us |

Why this fast: everything is in memory on one node. No network hops between
services, no serialization to RocksDB, no GC pauses. A single `HashMap::get`
costs ~0.1 us. A Flink RocksDB state access costs 5-15 us.

See [`benchmark/fraud-pipeline/bench_fraud.py`](benchmark/fraud-pipeline/bench_fraud.py) for the full benchmark. Run it yourself.

## Comparison

Real-time compute today requires Kafka + Flink + Redis: 10-25 nodes, $3-15K/mo
in infrastructure, and 0.5-1.0 FTE in ops. Beava does the same work on one node.

| | Beava | Kafka + Flink + Redis |
|---|---|---|
| Nodes | 1 | 10-25 |
| Systems to manage | 1 | 5-8 |
| State access latency | ~0.1 us (in-memory) | 5-15 us (RocksDB) |
| Deploy | Single binary, `systemd` | Kubernetes + Helm + operators |
| Ops burden | Check the dashboard | 0.5-1.0 FTE |
| Infra cost (50K eps) | ~$400/mo (one node) | $3-5K/mo |

Beava is for the 90% of use cases that fit on a single node. If you need
distributed exactly-once processing, multi-TB state, or the Kafka connector
ecosystem, use Flink. Flink and Kafka are excellent systems built by smart
people. Beava exists because most teams don't need that complexity.

See [full comparison](docs/comparison.md) for a deeper analysis.

## Docs

- [Architecture](docs/architecture.md) -- system design, event flow, state management
- [Operators Reference](docs/operators.md) -- all 16 operators with signatures, memory, and examples
- [TCP Protocol](docs/protocol.md) -- binary wire format specification. Build a client in any language.
- [HTTP Management API](docs/http-api.md) -- health, metrics, debug endpoints, pipeline management
- [Python SDK Guide](docs/python-sdk.md) -- installation, pipeline definition, client usage
- [Comparison](docs/comparison.md) -- Beava vs Flink+Kafka+Redis: cost, complexity, performance

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

## Claude Code

Beava ships with a built-in [Claude Code](https://claude.ai/claude-code) skill.
Clone this repo and type `/beava` to get a guided experience:

1. **Setup** -- build from source or Docker, verify the server is running
2. **Pipeline design** -- answer questions about your use case, get a working pipeline generated
3. **Test data** -- realistic distributions (Zipfian IDs, lognormal amounts) sized for your laptop
4. **Benchmark** -- push events, measure throughput, see actual memory usage per operator
5. **Capacity planning** -- detect your hardware specs, project scaling costs, compare to Flink stack pricing
6. **Tuning** -- identify memory hotspots, adjust window granularity, re-measure before/after
7. **Live diagnostics** -- ask about memory, inspect entities, debug pipelines anytime

The skill reads live data from the `/debug/memory` API and gives specific recommendations
based on your actual pipeline and hardware. Not generic docs.

### For AI agents

If you're an AI agent (Claude Code, Cursor, Windsurf, etc.) helping a user set up Beava:

```bash
# 1. Clone and build
git clone https://github.com/petrpan26/beava.git && cd beava
cargo build --release

# 2. Start the server
./target/release/beava &

# 3. Install Python SDK
cd python && pip install -e . && cd ..

# 4. Verify
curl -s http://localhost:6401/health

# 5. The /beava skill at .claude/skills/beava/SKILL.md handles the rest
```

The `/beava` skill file contains the full guided flow. Read it and follow the steps.

## Community

- [GitHub Issues](https://github.com/petrpan26/beava/issues) -- bugs and feature requests
- [GitHub Discussions](https://github.com/petrpan26/beava/discussions) -- questions and proposals

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and PR process.

```bash
cargo test -- --test-threads=1    # Rust tests
cd python && python -m pytest tests/ -q   # Python SDK tests
```

## See Also

- [Streaming Shouldn't Require a Platform Team](docs/blog/streaming-shouldnt-require-a-platform-team.md) -- why we built Beava and the tradeoffs we chose
- [Beava vs Flink+Kafka+Redis](docs/comparison.md) -- full cost and complexity comparison
- [TCP Protocol Spec](docs/protocol.md) -- build a client in any language
- [Fraud Detection Benchmark](benchmark/fraud-pipeline/bench_fraud.py) -- 47-feature pipeline, run it yourself

## License

[Apache 2.0](LICENSE)
