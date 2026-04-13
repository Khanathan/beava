# Tally

[![CI](https://github.com/petrpan26/tally/actions/workflows/ci.yml/badge.svg)](https://github.com/petrpan26/tally/actions/workflows/ci.yml)

Real-time feature server. Push events, get features. One binary, sub-millisecond, zero infrastructure.

Tally ingests events over a custom TCP protocol, computes streaming features (windowed aggregations, derived expressions, cross-stream cascades), and serves them synchronously in the response. No Kafka, no Flink, no cluster. One Rust binary with in-memory state. Designed for fraud detection, ML feature serving, and real-time context for AI agents.

## Quick Start

```bash
# Build and start
cargo build --release
./target/release/tally
```

```python
import tally as tl

@tl.source
class RawTransactions:
    pass

@tl.dataset(depends_on=[RawTransactions])
class UserFeatures:
    features = tl.group_by("user_id").agg(
        tx_count_1h  = tl.count(window="1h"),
        tx_sum_1h    = tl.sum("amount", window="1h"),
        avg_amount   = tl.avg("amount", window="24h"),
        unique_merchants = tl.distinct_count("merchant_id", window="24h"),
    )
    velocity = tl.derive("(tx_count_1h / 1) / (tx_count_24h / 24)")

app = tl.App("localhost:6400")
app.register(RawTransactions, UserFeatures)

# Push an event -- get updated features in the response
features = app.push(RawTransactions, {
    "user_id": "u123",
    "amount": 50.0,
    "merchant_id": "m456",
})

print(features.tx_count_1h)        # 7
print(features.unique_merchants)   # 4
```

## Key Features

- **Synchronous push-through** -- push an event, get all updated features in the response. Not eventual consistency.
- **Pipeline cascades** -- define multi-stage pipelines with `depends_on`. Events automatically propagate through the DAG in topological order.
- **16 operators** -- count, sum, avg, min, max, stddev, percentile, distinct_count (HLL++), last, first, lag, ema, last_n, exact_min, exact_max, derive.
- **Windowed aggregations** -- sliding windows with configurable granularity (30m, 1h, 24h, 7d).
- **Expression engine** -- derive expressions, where-clause filters, cross-stream lookups. 18 builtins.
- **Adaptive distinct counting** -- three-phase Exact → HashSet → HLL++ (p=12, Google bias correction). 2 KB/entity typical, zero error for low-cardinality entities.
- **Feature projection** -- `select()`/`drop()` to control which features appear in responses.
- **Local validation** -- `pipeline.validate()` catches cycles, missing deps, and type mismatches before hitting the server.

## Installation

### Pre-built binary

```bash
# From source (requires Rust toolchain)
cargo build --release
./target/release/tally
```

### Python SDK

```bash
cd python && pip install -e .
```

### Docker

```bash
# Coming soon
```

## Configuration

```bash
TALLY_TCP_PORT=6400         # TCP protocol port
TALLY_HTTP_PORT=6401        # HTTP management port
TALLY_WORKER_THREADS=4      # Tokio worker threads (default: 4)
TALLY_SNAPSHOT=true          # Enable periodic snapshots
TALLY_EVENT_LOG=true         # Enable SSD event log
```

## Performance

Measured on a 48-core Xeon with 8 Python client processes, realistic fraud detection pipeline (15 features, 3 entity types, Zipfian distribution):

| Metric | Value |
|--------|-------|
| Throughput (8 clients, batch) | 430-510K events/sec |
| Throughput (single client) | 270K events/sec |
| Sustained load | 29M events, 722K entities, no degradation |
| Memory per entity | 7.6 KB (15 features incl. HLL++) |
| Latency (p99) | < 100 µs |

## Documentation

- [Architecture](docs/architecture.md)
- [Operators Reference](docs/operators.md)
- [TCP Protocol](docs/protocol.md)
- [HTTP Management API](docs/http-api.md)
- [Python SDK Guide](docs/python-sdk.md)
- [Benchmark Guide](benchmark/fraud-pipeline/bench_fraud.py)

## Community

- [GitHub Issues](https://github.com/petrpan26/tally/issues) -- bug reports and feature requests
- [GitHub Discussions](https://github.com/petrpan26/tally/discussions) -- questions and design proposals

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, test commands, and PR process.

```bash
# Run tests
cargo test          # 792 Rust tests
cd python && python -m pytest tests/ -q   # 313 Python tests
```

## License

TBD
