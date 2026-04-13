# Tally

Real-time feature server. One binary. Sub-millisecond.

<!-- CI badges here -->

## What is Tally

Tally is a lightweight, single-binary server that ingests events over a custom TCP protocol, computes stateful streaming features (windowed aggregations, derived expressions, cross-stream lookups), and serves them with sub-millisecond latency. No Kafka, no Flink, no cluster -- one Rust binary, in-memory state, persistent TCP connections. Designed for fraud detection, ML feature serving, and real-time context for AI agents.

Push an event, get updated features in the response. Not eventual consistency -- immediate.

## Quick Start

### Build and start the server

```bash
cargo build --release
./target/release/tally
# TCP on :6400, HTTP on :6401
```

### Install the Python SDK

```bash
pip install -e python/
```

### Define a pipeline, push events, read features

```python
import tally as tl

# Define an event source
@tl.source
class Transactions:
    pass

# Define features grouped by user_id
@tl.dataset(depends_on=[Transactions])
class UserFeatures:
    features = tl.group_by("user_id").agg(
        tx_count_1h=tl.count(window="1h"),
        tx_sum_1h=tl.sum("amount", window="1h"),
        avg_amount_1h=tl.avg("amount", window="1h"),
    )
    velocity = tl.derive("tx_count_1h / (tx_count_24h / 24)")

# Connect, register, push
app = tl.App("localhost:6400")
app.register(Transactions, UserFeatures)

features = app.push(Transactions, {
    "user_id": "u123",
    "amount": 50.0,
    "merchant_id": "m456",
    "status": "success",
})

print(features["tx_count_1h"])   # 1
print(features["tx_sum_1h"])     # 50.0
print(features["avg_amount_1h"]) # 50.0

# Read all features for an entity
all_features = app.get("user_id:u123")
```

## API

Full fraud detection pipeline with 5 entity types and 47 features:

```python
import tally as tl

# Raw event source
@tl.source
class RawTransactions:
    pass

# User transaction behavior (25 features)
@tl.dataset(depends_on=[RawTransactions])
class UserTransactions:
    features = tl.group_by("user_id").agg(
        tx_count_30m=tl.count(window="30m"),
        tx_count_1h=tl.count(window="1h"),
        tx_count_24h=tl.count(window="24h"),
        tx_count_7d=tl.count(window="7d"),
        tx_sum_1h=tl.sum("amount", window="1h"),
        tx_sum_24h=tl.sum("amount", window="24h"),
        tx_avg_1h=tl.avg("amount", window="1h"),
        tx_avg_24h=tl.avg("amount", window="24h"),
        tx_max_24h=tl.max("amount", window="24h"),
        tx_min_24h=tl.min("amount", window="24h"),
        tx_stddev_24h=tl.stddev("amount", window="24h"),
        unique_merchants_1h=tl.distinct_count("merchant_id", window="1h"),
        unique_merchants_24h=tl.distinct_count("merchant_id", window="24h"),
        unique_countries_24h=tl.distinct_count("country", window="24h"),
        unique_devices_24h=tl.distinct_count("device_id", window="24h"),
        unique_ips_24h=tl.distinct_count("ip_address", window="24h"),
        last_country=tl.last("country"),
        last_merchant=tl.last("merchant_id"),
        last_amount=tl.last("amount"),
    )
    velocity_spike = tl.derive("(tx_count_1h / 1) / (tx_count_24h / 24)")
    amount_vs_avg = tl.derive("last_amount / tx_avg_24h")
    spend_acceleration = tl.derive("tx_sum_1h / (tx_sum_24h / 24)")
    high_value_ratio = tl.derive("tx_max_24h / tx_avg_24h")
    merchant_diversity_1h = tl.derive("unique_merchants_1h / tx_count_1h")
    country_hop_flag = tl.derive("unique_countries_24h > 3")

# Filtered dataset: failed transactions only
@tl.dataset(depends_on=[RawTransactions], filter="status == 'failed'")
class UserFailedTxns:
    features = tl.group_by("user_id").agg(
        failed_count_30m=tl.count(window="30m"),
        failed_count_1h=tl.count(window="1h"),
        failed_count_24h=tl.count(window="24h"),
        failed_sum_24h=tl.sum("amount", window="24h"),
    )

# Merchant risk profile (8 features)
@tl.dataset(depends_on=[RawTransactions])
class MerchantActivity:
    features = tl.group_by("merchant_id").agg(
        merch_tx_count_1h=tl.count(window="1h"),
        merch_tx_count_24h=tl.count(window="24h"),
        merch_tx_sum_24h=tl.sum("amount", window="24h"),
        merch_avg_amount=tl.avg("amount", window="24h"),
        merch_unique_users_1h=tl.distinct_count("user_id", window="1h"),
        merch_unique_users_24h=tl.distinct_count("user_id", window="24h"),
        merch_max_amount_24h=tl.max("amount", window="24h"),
        merch_stddev_24h=tl.stddev("amount", window="24h"),
    )

# Device fingerprint (5 features)
@tl.dataset(depends_on=[RawTransactions])
class DeviceActivity:
    features = tl.group_by("device_id").agg(
        device_tx_count_1h=tl.count(window="1h"),
        device_tx_count_24h=tl.count(window="24h"),
        device_unique_users_1h=tl.distinct_count("user_id", window="1h"),
        device_unique_users_24h=tl.distinct_count("user_id", window="24h"),
        device_unique_merchants_24h=tl.distinct_count("merchant_id", window="24h"),
    )

# IP address activity (5 features)
@tl.dataset(depends_on=[RawTransactions])
class IPActivity:
    features = tl.group_by("ip_address").agg(
        ip_tx_count_1h=tl.count(window="1h"),
        ip_tx_count_24h=tl.count(window="24h"),
        ip_unique_users_1h=tl.distinct_count("user_id", window="1h"),
        ip_unique_users_24h=tl.distinct_count("user_id", window="24h"),
        ip_unique_devices_24h=tl.distinct_count("device_id", window="24h"),
    )
```

### Client operations

```python
app = tl.App("localhost:6400")
app.register(RawTransactions, UserTransactions, UserFailedTxns,
             MerchantActivity, DeviceActivity, IPActivity)

# Push a single event -- returns updated features synchronously
features = app.push(RawTransactions, {
    "user_id": "u123",
    "merchant_id": "m456",
    "device_id": "dev_001",
    "ip_address": "203.0.113.42",
    "amount": 50.0,
    "country": "US",
    "status": "success",
})

# Batch push -- high-throughput ingestion
app.push_many(RawTransactions, [event1, event2, event3, ...])
app.flush()

# Read all features for an entity
all_features = app.get("user_id:u123")

# Direct write -- batch features computed offline
app.set("user_id:u123", {"lifetime_value": 4500.0, "segment": "high_value"})

# Bulk write -- daily batch job
app.mset({
    "user_id:u123": {"lifetime_value": 4500.0},
    "user_id:u456": {"lifetime_value": 1200.0},
})
```

## Operators

| Operator | Description | State Size |
|---|---|---|
| `count` | Event count in window | O(window/bucket) |
| `sum` | Sum a numeric field in window | O(window/bucket) |
| `avg` | Average a numeric field in window | O(window/bucket) |
| `min` | Minimum value in window (bucketed) | O(window/bucket) |
| `max` | Maximum value in window (bucketed) | O(window/bucket) |
| `exact_min` | Exact minimum (retains all values) | O(events in window) |
| `exact_max` | Exact maximum (retains all values) | O(events in window) |
| `stddev` | Standard deviation in window | O(window/bucket) |
| `percentile` | Approximate percentile (t-digest) | O(compression param) |
| `distinct_count` | Approximate unique count (adaptive HLL++) | O(1) -- 40B to 12KB |
| `last` | Most recent value of a field | O(1) |
| `first` | First value seen in window | O(1) |
| `last_n` | Last N values of a field | O(N) |
| `lag` | Previous value of a field | O(1) |
| `ema` | Exponential moving average | O(1) |
| `derive` | Expression over other features (computed on read) | O(1) -- no state |
| `lookup` | Cross-key feature reference | O(1) -- no state |

### Window implementation

Sliding windows use bucketed ring buffers. A 30-minute window with 1-minute bucket granularity uses 30 buckets. On event arrival, the current bucket is updated. On read, all non-expired buckets are summed. Bucket granularity is configurable -- more buckets means more accuracy and more memory.

### Expression language

The `derive` and `filter` expressions are parsed at registration time into an AST and evaluated in Rust at event time. No Python in the hot path.

```
# Field access
field_name
StreamName.field_name
_event.field_name

# Arithmetic
+  -  *  /

# Comparison
>  <  >=  <=  ==  !=

# Boolean
and  or  not

# Builtins
abs()  min()  max()  now()
```

## Architecture

```
                    Clients (Python SDK)
                           |
                    TCP :6400 (binary protocol)
                           |
              +------------+------------+
              |    Command Handler      |
              |  PUSH / GET / SET /     |
              |  MSET / MGET / REGISTER |
              +------------+------------+
                           |
              +------------v------------+
              |    Pipeline Engine      |
              |  Cascade DAG (petgraph) |
              |  @source -> @dataset    |
              |  -> @dataset -> ...     |
              |  Enriched event         |
              |  propagation            |
              +------------+------------+
                           |
              +------------v------------+
              |    State Store          |
              |  DashMap per stream     |
              |  Per-key operator state |
              |  Static features (SET)  |
              |  TTL-based eviction     |
              +------------+------------+
                           |
              +------+-----+-----+------+
              |                         |
     +--------v--------+    +----------v----------+
     | Snapshot         |    | Event Log           |
     | Persistence      |    | Append-only replay  |
     | Base + Delta     |    | log for recovery    |
     | (postcard/serde) |    |                     |
     +-----------------+    +---------------------+

              HTTP :6401 (management + debug UI)
```

**Key design choices:**

- **Multi-threaded tokio runtime** with configurable worker threads (default 4). DashMap provides per-stream concurrent access without a global lock.
- **Cascade DAG** -- datasets form a directed acyclic graph. A single event pushed to a `@source` propagates through all dependent `@dataset` nodes, updating features at each level. The enriched event (original fields + computed features) flows downstream.
- **Synchronous push-through** -- PUSH returns updated features in the response. No background processing, no eventual consistency.
- **Binary TCP protocol** -- length-prefixed frames with opcodes. Persistent connections, minimal framing overhead. Batch mode (`push_many`) amortizes per-event overhead.

### Wire format

```
[4 bytes: payload length (u32 big-endian)]
[1 byte: opcode]
[payload bytes]
```

String encoding: `[2 bytes: length (u16 big-endian)][N bytes: UTF-8]`

## Performance

Benchmarked on the 47-feature fraud detection pipeline (5 entity types, cross-key fan-out, 16 distinct_count features, Zipfian key distribution):

| Metric | Value |
|---|---|
| Single-client throughput | 270K events/sec |
| 8-client throughput | 430--510K events/sec |
| Sustained volume | 29M events before memory limit |
| Entity count at capacity | 722K entities |
| Memory per entity | 7.6 KB average |
| Push latency (p99) | Sub-millisecond |
| GET latency (p99) | Sub-millisecond |

### Adaptive distinct counting

The `distinct_count` operator uses a three-phase adaptive approach (ClickHouse-style) that transitions automatically based on cardinality:

| Phase | Condition | Implementation | Error | Memory |
|---|---|---|---|---|
| Exact | <=16 uniques | Flat sorted array | 0% | ~128 bytes |
| HashSet | <=threshold uniques | AHashSet of u64 hashes | 0% | ~8 bytes/unique |
| HLL++ | Unlimited | HyperLogLog++ (p=12, Google bias correction) | ~0.8% | ~4--12 KB |

This matters for fraud detection: most users visit ~5 merchants per hour. Phases 1--2 handle that with zero error and far less memory than any probabilistic sketch.

**Memory impact per entity (30-bucket windowed distinct_count):**

| Cardinality per bucket | Memory per feature |
|---|---|
| 5 uniques | 30 x 40B = ~1.2 KB |
| 50 uniques | 30 x 400B = ~12 KB |
| 5000 uniques | 30 x 4KB = ~120 KB |

## Configuration

All configuration via environment variables. Defaults are production-reasonable.

| Variable | Default | Description |
|---|---|---|
| `TALLY_TCP_PORT` | `6400` | TCP protocol listener port |
| `TALLY_HTTP_PORT` | `6401` | HTTP management API port |
| `TALLY_WORKER_THREADS` | `4` | Tokio runtime worker threads |
| `TALLY_SNAPSHOT` | `true` | Enable/disable periodic snapshots (`false` or `0` to disable) |
| `TALLY_SNAPSHOT_PATH` | `tally.snapshot` | Path for snapshot files |
| `TALLY_FULL_SNAPSHOT_INTERVAL` | `10` | Base snapshot every Nth cycle (others are deltas) |
| `TALLY_EVENT_LOG` | `true` | Enable/disable append-only event log (`false` or `0` to disable) |
| `TALLY_DATA_DIR` | `.` | Directory for event log storage |
| `TALLY_TTL_MULTIPLIER` | `2` | TTL = multiplier x largest window (for key eviction) |

```bash
# Example: high-throughput production config
TALLY_WORKER_THREADS=8 \
TALLY_SNAPSHOT_PATH=/var/lib/tally/tally.snapshot \
TALLY_DATA_DIR=/var/lib/tally \
./target/release/tally
```

## HTTP Management API

Secondary HTTP API on port 6401 for management, debugging, and the built-in dashboard UI.

| Endpoint | Method | Description |
|---|---|---|
| `/health` | GET | Health check (`{"status": "ok"}`) |
| `/pipelines` | GET | List registered pipelines |
| `/pipelines` | POST | Register pipeline definitions |
| `/pipelines/{name}` | GET | Get pipeline definition and feature list |
| `/pipelines/{name}` | DELETE | Remove a pipeline |
| `/metrics` | GET | Prometheus-format metrics |
| `/debug/key/{key}` | GET | Inspect full state for a key (operator internals) |
| `/debug/memory` | GET | Memory usage breakdown |
| `/debug/topology` | GET | Pipeline DAG topology |
| `/debug/throughput` | GET | Throughput counters |
| `/debug/latency` | GET | Latency histogram data |
| `/debug/backfill` | GET | Backfill task status |
| `/snapshot` | POST | Trigger manual snapshot |
| `/` | GET | Built-in debug dashboard UI |

## Testing

```bash
# Rust tests (792 unit + integration tests)
cargo test

# Python tests (313 tests)
cd python && pytest

# Total: 1100+ tests
```

Tests cover: operator correctness, window semantics, expression parsing, protocol encoding/decoding, snapshot save/load, concurrent access, batch primitives, push coalescing, incremental snapshots, and end-to-end pipeline behavior.

## Project Structure

```
tally/
+-- Cargo.toml
+-- src/
|   +-- main.rs                # Entry point, config, server startup
|   +-- lib.rs                 # Library root
|   +-- types.rs               # Value, Timestamp, FeatureMap, etc.
|   +-- error.rs               # Error types
|   +-- engine/
|   |   +-- mod.rs
|   |   +-- pipeline.rs        # Pipeline/stream definitions, cascade DAG
|   |   +-- operators.rs       # All 16 operators
|   |   +-- window.rs          # Sliding window ring buffer
|   |   +-- expression.rs      # Expression parser and evaluator (winnow)
|   |   +-- hll.rs             # Adaptive HLL++ (exact -> hashset -> HLL)
|   +-- server/
|   |   +-- mod.rs
|   |   +-- tcp.rs             # TCP listener, connection handling, DashMap state
|   |   +-- protocol.rs        # Binary protocol parsing/serialization
|   |   +-- http.rs            # HTTP management API (axum)
|   |   +-- throughput.rs      # Throughput tracking
|   |   +-- latency.rs         # Latency histogram
|   |   +-- ui.rs              # Embedded debug dashboard
|   |   +-- ui/                # Static assets (HTML/CSS/JS)
|   +-- state/
|       +-- mod.rs
|       +-- store.rs           # DashMap-based state store
|       +-- snapshot.rs        # Base + delta snapshot persistence
|       +-- eviction.rs        # TTL-based key eviction
|       +-- event_log.rs       # Append-only event log
+-- python/
|   +-- pyproject.toml
|   +-- tally/
|   |   +-- __init__.py        # Public API exports
|   |   +-- _app.py            # App client (register, push, get, set, mset)
|   |   +-- _client.py         # TCP connection management
|   |   +-- _protocol.py       # Binary protocol encoding/decoding
|   |   +-- _operators.py      # Operator constructors (count, sum, avg, ...)
|   |   +-- _types.py          # FeatureResult, error types
|   |   +-- _source.py         # @tl.source decorator
|   |   +-- _dataset.py        # @tl.dataset, group_by, union
|   |   +-- _schema.py         # EventSet, FeatureSet, Field
|   |   +-- _validate.py       # Pipeline validation
|   +-- tests/
+-- tests/                     # Rust integration tests
+-- benchmark/
    +-- fraud-pipeline/        # 47-feature fraud detection benchmark
    +-- tally-throughput/      # Raw throughput benchmark
```

## License

TBD
