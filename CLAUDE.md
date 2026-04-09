# Streamlet

A real-time feature server. No Kafka. No Flink. No cluster. One binary. Push events, get features.

## What Is This

Streamlet is a lightweight, single-binary server that ingests events over a custom TCP protocol, computes stateful streaming features (windowed aggregations, derived expressions, cross-stream lookups), and serves them with sub-millisecond latency. It is designed for fraud detection, ML feature serving, and real-time context for AI agents.

Think of it as **Redis with a built-in streaming aggregation engine**. Events go in, features come out — synchronously, in one request-response cycle.

## Core Design Principles

- **Zero infrastructure.** No Kafka, no Flink, no external dependencies. One Rust binary.
- **Zero ops.** Run it, forget about it. Crash recovery from local disk snapshots.
- **Synchronous push-through.** POST an event, get updated features in the response. Not eventual consistency — immediate.
- **In-memory everything.** All state lives in memory. Periodic snapshots to local disk for recovery. No embedded KV, no WAL, no LSM tree.
- **Single-threaded core (v1).** Like Redis — one thread, no locks, no contention. Key-partitioned multi-threading comes later.
- **Features, not streams.** Users think in features (a named value for a key), not streams/tables/DAGs/operators.

## Architecture

```
┌──────────────────────────────────────────────────┐
│                 Streamlet Server                  │
│                                                  │
│  TCP Protocol (binary, persistent connections)   │
│  ┌────────────────────────────────────────────┐  │
│  │            Command Handler                 │  │
│  │  PUSH  - ingest event, return features     │  │
│  │  GET   - read current features for a key   │  │
│  │  SET   - direct write (batch features)     │  │
│  │  MSET  - bulk direct write                 │  │
│  └────────────┬───────────────────────────────┘  │
│               │                                  │
│  ┌────────────▼───────────────────────────────┐  │
│  │          Pipeline Engine                   │  │
│  │  - Registered stream definitions           │  │
│  │  - Operator evaluation (count, sum, etc)   │  │
│  │  - Derive expression evaluation            │  │
│  │  - Cross-stream view recomputation         │  │
│  │  - Lookup resolution                       │  │
│  └────────────┬───────────────────────────────┘  │
│               │                                  │
│  ┌────────────▼───────────────────────────────┐  │
│  │          State Store (in-memory)           │  │
│  │  HashMap<EntityKey, FeatureMap>            │  │
│  │  - LiveValue: operator state + value       │  │
│  │  - StaticValue: directly written value     │  │
│  │  - TTL-based eviction for inactive keys    │  │
│  └────────────┬───────────────────────────────┘  │
│               │                                  │
│  ┌────────────▼───────────────────────────────┐  │
│  │       Snapshot Persistence                 │  │
│  │  - Periodic serialization to local file    │  │
│  │  - bincode/serde, versioned format         │  │
│  │  - Load on startup for recovery            │  │
│  │  - Cooperative yielding during snapshot    │  │
│  └────────────────────────────────────────────┘  │
│                                                  │
│  HTTP API (secondary, for management only)       │
│  - Pipeline registration                         │
│  - Health checks, metrics                        │
│  - Debug queries                                 │
└──────────────────────────────────────────────────┘
```

## TCP Protocol

Custom binary protocol over persistent TCP connections. Designed for minimal overhead.

### Wire Format

Length-prefixed binary frames:
```
[4 bytes: message length (u32 big-endian)]
[1 byte: opcode]
[payload]
```

### Commands

```
PUSH    (0x01) - Push event to a stream, returns updated features
  stream_name: string
  payload: JSON bytes (must contain the key field)
  → Response: JSON map of feature_name → value

GET     (0x02) - Read current features for a key
  key: string
  → Response: JSON map of feature_name → value (across all streams/views)

SET     (0x03) - Direct write of feature values for a key
  key: string
  payload: JSON map of feature_name → value
  → Response: OK

MSET    (0x04) - Bulk direct write
  count: u32
  [key, payload] × count
  → Response: OK (processed in chunks with cooperative yielding)

REGISTER (0x05) - Register a pipeline definition
  definition: JSON (serialized from Python SDK)
  → Response: OK or validation error
```

### String Encoding in Protocol
```
[2 bytes: string length (u16 big-endian)]
[N bytes: UTF-8 string data]
```

## Python SDK

The SDK is a thin client. Pipeline definitions are built in Python, serialized to JSON, and sent to the server. Python never touches the hot path.

### Stream Definition API

```python
import streamlet as st

@st.stream(key="user_id")
class Transactions:
    tx_count_30m       = st.count(window="30m")
    tx_count_1h        = st.count(window="1h")
    tx_count_24h       = st.count(window="24h")
    tx_sum_1h          = st.sum("amount", window="1h")
    avg_amount_1h      = st.avg("amount", window="1h")
    max_amount_24h     = st.max("amount", window="24h")
    unique_merchants   = st.distinct_count("merchant_id", window="24h")
    failed_tx_30m      = st.count(window="30m", where="status == 'failed'")
    failure_rate       = st.derive("failed_tx_30m / tx_count_30m")
    velocity_spike     = st.derive("(tx_count_1h / 1) / (tx_count_24h / 24)")
    amount_vs_avg      = st.derive("_event.amount / avg_amount_1h")
    last_country       = st.last("country")
    last_merchant      = st.last("merchant_id")
```

### Reusable Mixins

```python
class VelocityMixin:
    count_1h  = st.count(window="1h")
    count_24h = st.count(window="24h")
    rate_spike = st.derive("(count_1h / 1) / (count_24h / 24)")

class AmountMixin:
    total_1h = st.sum("amount", window="1h")
    avg_1h   = st.avg("amount", window="1h")
    max_1h   = st.max("amount", window="1h")

@st.stream(key="user_id")
class Transactions(VelocityMixin, AmountMixin):
    failed_count_1h = st.count(window="1h", where="status == 'failed'")
    failure_rate    = st.derive("failed_count_1h / count_1h")
```

### Cross-Stream Views

```python
@st.stream(key="user_id")
class Logins:
    login_count_1h = st.count(window="1h")

@st.view(key="user_id")
class UserRisk:
    tx_to_login_ratio = st.derive("Transactions.tx_count_1h / Logins.login_count_1h")
    is_suspicious     = st.derive("Transactions.tx_count_1h > 10 and Logins.login_count_1h < 2")
```

### Cross-Key Lookups

```python
@st.stream(key="merchant_id")
class MerchantActivity:
    chargeback_count_24h = st.count(window="24h", where="type == 'chargeback'")

@st.view(key="user_id")
class FraudSignals:
    merchant_chargebacks = st.lookup(
        MerchantActivity.chargeback_count_24h,
        on="merchant_id"
    )
    risk_score = st.derive("Transactions.velocity_spike > 3 and merchant_chargebacks > 5")
```

### Event Fan-Out

A single event can update multiple streams if it contains keys for each:
```python
# This event has both user_id and merchant_id
app.push(Transactions, {
    "user_id": "u123",
    "merchant_id": "m456",
    "amount": 50.0,
    "status": "success",
    "country": "US"
})
# Updates Transactions state for key "u123"
# AND MerchantActivity state for key "m456"
```

### Client Usage

```python
app = st.App("localhost:6400")
app.register(Transactions, Logins, MerchantActivity, UserRisk, FraudSignals)

# Push event — synchronous, returns all updated features
features = app.push(Transactions, {
    "user_id": "u123",
    "amount": 50.0,
    "status": "success",
    "merchant_id": "m456",
    "country": "US"
})
print(features.tx_count_30m)     # 7
print(features.failure_rate)     # 0.14
print(features.velocity_spike)   # 2.3

# Read all features for an entity
all_features = app.get("u123")

# Batch write (for features computed offline)
app.set("u123", {"lifetime_value": 4500.0, "segment": "high_value"})

# Bulk batch write (daily job)
app.mset({
    "u123": {"lifetime_value": 4500.0},
    "u456": {"lifetime_value": 1200.0},
    # ...
})
```

## Operators

### v1 Operator Set

| Operator | Description | State Size | Implementation |
|----------|-------------|------------|----------------|
| `count` | Count events in window | O(window/bucket) | Bucketed counter, ring buffer |
| `sum` | Sum a field in window | O(window/bucket) | Bucketed accumulator |
| `avg` | Average a field in window | O(window/bucket) | Sum + count, divide on read |
| `min` | Minimum value in window | O(window/bucket) | Per-bucket min tracking |
| `max` | Maximum value in window | O(window/bucket) | Per-bucket max tracking |
| `distinct_count` | Approximate unique count | O(1) fixed ~12KB | HyperLogLog per window |
| `last` | Most recent value of a field | O(1) | Single value + timestamp |
| `derive` | Expression over other features | O(1) | Evaluated on read, no state |
| `lookup` | Cross-key feature reference | O(1) | HashMap lookup in another stream |

### Window Implementation

Sliding windows use a **bucketed ring buffer** approach:
- Window divided into fixed-size buckets (e.g., 30m window = 30 × 1-minute buckets)
- Each bucket holds a partial aggregate (count, sum, etc.)
- On event arrival: add to current bucket
- On read: sum all non-expired buckets
- Bucket granularity is configurable (tradeoff: more buckets = more accuracy, more memory)

### Expression Evaluator

The `derive` and `where` expressions use a simple expression language:
- Field access: `field_name`, `StreamName.field_name`, `_event.field_name`
- Arithmetic: `+`, `-`, `*`, `/`
- Comparison: `>`, `<`, `>=`, `<=`, `==`, `!=`
- Boolean: `and`, `or`, `not`
- Builtins: `abs()`, `min()`, `max()`, `now()`

Parsed at pipeline registration time into an AST. Evaluated in Rust at event time. String expressions, not Python lambdas — keeps Python out of the hot path.

## State Management

### In-Memory State Structure

```rust
// Top-level: entity key → feature map
HashMap<String, EntityState>

struct EntityState {
    // Features from streaming pipelines
    live_features: HashMap<String, LiveFeature>,
    // Features from direct writes (SET/MSET)
    static_features: HashMap<String, StaticFeature>,
    // Last event timestamp for TTL eviction
    last_event_at: Timestamp,
}

struct LiveFeature {
    operator_state: OperatorState, // window buckets, HLL, t-digest, etc.
    current_value: Value,          // cached computed value
}

struct StaticFeature {
    value: Value,
    updated_at: Timestamp,
}

enum OperatorState {
    Counter { buckets: RingBuffer<u64> },
    Sum { buckets: RingBuffer<f64> },
    Avg { count_buckets: RingBuffer<u64>, sum_buckets: RingBuffer<f64> },
    Min { buckets: RingBuffer<f64> },
    Max { buckets: RingBuffer<f64> },
    DistinctCount { hll: HyperLogLog },
    Last { value: Value, timestamp: Timestamp },
}
```

### Memory Bounds

Every operator has bounded memory per key:
- `count`, `sum`, `avg`, `min`, `max`: `O(window_size / bucket_granularity)` — typically a few KB
- `distinct_count`: Fixed ~12KB (HyperLogLog with 14-bit precision)
- `last`: O(1) — single value
- `derive`: O(1) — no state, computed on read

### TTL Eviction

Keys that receive no events for a configurable duration (default: 2× largest window) are evicted from memory. On next event for that key, state is re-initialized fresh.

### Snapshots

- Periodic serialization of full state to a local file (default: every 30 seconds)
- Format: versioned binary using serde + bincode
- On startup: load latest snapshot, accept that features may be slightly stale until new events arrive
- Snapshot write uses cooperative yielding (process chunks, yield to event loop, continue) to avoid blocking the hot path
- Snapshot format is forward-compatible — version byte per key allows migration on read

## Batch Ingestion

Batch features (computed offline in Spark/Airflow/etc.) are written via `SET`/`MSET`. These bypass the pipeline engine entirely and land as `StaticFeature` entries in the same state map.

### MSET Handling

Large MSET operations are chunked internally to avoid blocking the hot path:
```
MSET 100K keys arrives
  → process 1024 keys
  → yield to event loop (handle waiting PUSH/GET)
  → process next 1024 keys
  → yield
  → ...
  → MSET complete
```

PUSH and GET requests are always prioritized over pending MSET writes.

## HTTP Management API

Secondary HTTP API on a separate port (default: 6401) for management and debugging:

```
POST   /pipelines           - Register pipeline definitions
GET    /pipelines           - List registered pipelines
GET    /pipelines/:name     - Get pipeline definition
DELETE /pipelines/:name     - Remove pipeline

GET    /health              - Health check
GET    /metrics             - Prometheus-format metrics
GET    /debug/key/:key      - Inspect full state for a key (including operator internals)
GET    /debug/memory        - Memory usage breakdown
POST   /snapshot            - Trigger manual snapshot
```

## Project Structure

```
streamlet/
├── Cargo.toml
├── src/
│   ├── main.rs              # Entry point, config, server startup
│   ├── server/
│   │   ├── mod.rs
│   │   ├── tcp.rs            # TCP listener, connection handling
│   │   ├── protocol.rs       # Binary protocol parsing/serialization
│   │   └── http.rs           # HTTP management API
│   ├── engine/
│   │   ├── mod.rs
│   │   ├── pipeline.rs       # Pipeline/stream definitions, registration
│   │   ├── operators.rs      # count, sum, avg, min, max, distinct_count, last
│   │   ├── window.rs         # Sliding window bucketing logic
│   │   ├── expression.rs     # Expression parser and evaluator
│   │   ├── hll.rs            # HyperLogLog implementation
│   │   └── view.rs           # Cross-stream views, derive, lookup
│   ├── state/
│   │   ├── mod.rs
│   │   ├── store.rs          # In-memory HashMap state store
│   │   ├── snapshot.rs       # Serialization/deserialization to disk
│   │   └── eviction.rs       # TTL-based key eviction
│   └── types.rs              # Value, Timestamp, FeatureMap, etc.
├── python/
│   ├── pyproject.toml
│   └── streamlet/
│       ├── __init__.py
│       ├── client.py         # TCP client, connection pooling
│       ├── stream.py         # @st.stream decorator, operator classes
│       ├── view.py           # @st.view decorator
│       ├── operators.py      # st.count, st.sum, etc.
│       └── types.py          # Feature result types
├── tests/
│   ├── test_operators.rs     # Unit tests for each operator
│   ├── test_window.rs        # Window semantics tests
│   ├── test_expression.rs    # Expression parser tests
│   ├── test_protocol.rs      # Protocol encoding/decoding tests
│   ├── test_pipeline.rs      # End-to-end pipeline tests
│   └── test_snapshot.rs      # Snapshot save/load tests
└── benches/
    ├── throughput.rs          # Events per second benchmark
    └── latency.rs             # Per-event latency benchmark
```

## Implementation Order

### Phase 1: Core Engine (Week 1-2)
1. In-memory state store (HashMap<String, EntityState>)
2. Window implementation (ring buffer buckets)
3. Basic operators: count, sum, avg
4. Expression parser and evaluator (arithmetic + comparison + boolean)

### Phase 2: Server (Week 3-4)
5. TCP server with tokio (accept connections, read/write frames)
6. Binary protocol (PUSH, GET, SET commands)
7. Pipeline registration (REGISTER command from JSON definition)
8. Synchronous push-through (event → update operators → evaluate derives → return features)

### Phase 3: Python SDK (Week 5)
9. @st.stream decorator and operator classes
10. Client with TCP connection and protocol encoding
11. @st.view, st.derive, st.lookup
12. Typed feature results

### Phase 4: Persistence & Polish (Week 6-7)
13. Snapshot persistence (periodic serde+bincode to file)
14. Snapshot recovery on startup
15. TTL-based key eviction
16. MSET with chunked yielding
17. HTTP management API (health, metrics, debug)

### Phase 5: Remaining Operators (Week 8)
18. min, max operators
19. distinct_count (HyperLogLog)
20. last operator
21. Cross-key lookup resolution
22. Event fan-out to multiple streams

## Key Technical Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Language | Rust | Memory safety for "zero ops" promise, single binary distribution, ecosystem alignment |
| Threading | Single-threaded v1 | Simplicity. Keys are independent so key-partitioned multi-threading is a drop-in upgrade later |
| State storage | In-memory HashMap | Fastest possible. Embedded KV adds latency to hot path for durability we don't need |
| Persistence | Periodic snapshots | Redis RDB model. Simple, sufficient. Losing 30s of state on crash is acceptable |
| Protocol | Custom binary TCP | HTTP is too heavy for hot path. Persistent connections, minimal framing overhead |
| Expression language | String-based, parsed server-side | Keeps Python out of hot path. Simpler than AST capture. Tiny parser covers 95% of use cases |
| Window type | Sliding with buckets | Covers tumbling (bucket = window) and sliding. Session windows deferred |
| Approximate operators | HyperLogLog for distinct count | Bounded memory per key. Configurable precision. Well-understood error bounds |
| Management API | HTTP on separate port | For debugging, monitoring, pipeline registration. Not on hot path |

## Scaling Path (NOT in v1)

1. **Vertical** — single instance handles 100K-200K events/sec. Most users never outgrow this.
2. **Key-partitioned multi-threading** — shard keyspace across cores within one process. No locks needed.
3. **Client-side sharding** — run N instances, hash key to determine instance. Document, don't build.
4. **Cluster mode** — probably never needed. If someone asks, it's a good problem to have.

## Benchmarks to Hit

- Single-event PUSH latency: < 100µs (p99)
- GET latency: < 50µs (p99)
- Throughput: > 100K events/sec sustained (single thread)
- Snapshot write: < 1 second for 1M keys
- Snapshot recovery: < 5 seconds for 1M keys
- Memory per key (10 features, mixed operators): < 5KB average

## What This Is NOT

- Not a distributed streaming engine (no Kafka, no Flink replacement)
- Not a batch compute engine (no Spark, no offline feature computation)
- Not a general-purpose database or cache (no arbitrary queries)
- Not an ML platform (no model training, no model serving)
- Not a feature store in the Feast/Tecton sense (no offline store, no lineage, no data versioning)

It is a **real-time feature server** — a single-purpose tool that computes and serves streaming features with minimal latency and zero operational burden.

## Skill routing

When the user's request matches an available skill, ALWAYS invoke it using the Skill
tool as your FIRST action. Do NOT answer directly, do NOT use other tools first.
The skill has specialized workflows that produce better results than ad-hoc answers.

Key routing rules:
- Product ideas, "is this worth building", brainstorming → invoke office-hours
- Bugs, errors, "why is this broken", 500 errors → invoke investigate
- Ship, deploy, push, create PR → invoke ship
- QA, test the site, find bugs → invoke qa
- Code review, check my diff → invoke review
- Update docs after shipping → invoke document-release
- Weekly retro → invoke retro
- Design system, brand → invoke design-consultation
- Visual audit, design polish → invoke design-review
- Architecture review → invoke plan-eng-review
- Save progress, checkpoint, resume → invoke checkpoint
- Code quality, health check → invoke health
