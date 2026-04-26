# Tally Architecture

## Overview

Tally is a real-time feature server that ingests events over a custom binary TCP protocol, computes stateful streaming features (windowed aggregations, derived expressions, cross-stream cascades), and serves them with sub-millisecond latency. It ships as a single Rust binary with zero external dependencies -- no Kafka, no Flink, no cluster. Push an event, get updated features back in the same response. All state lives in memory, with periodic snapshots and an append-only event log for crash recovery.

## System Architecture

```
                     Clients (Python SDK, any TCP client)
                              |
                    +---------+---------+
                    |                   |
              TCP :6400           HTTP :6401
          (hot path)           (management)
                    |                   |
  +-----------------+-----+ +-----------+-----------+
  | Binary Protocol Layer | | Axum HTTP Router      |
  | protocol.rs           | | http.rs               |
  |                       | |                       |
  | PUSH   (0x01)         | | GET  /health          |
  | GET    (0x02)         | | CRUD /pipelines       |
  | SET    (0x03)         | | GET  /metrics          |
  | MSET   (0x04)         | | GET  /debug/*         |
  | REGISTER (0x05)       | | POST /snapshot        |
  | MGET   (0x06)         | +-----------------------+
  | PUSH_ASYNC (0x07)     |
  | FLUSH  (0x08)         |
  | PUSH_BATCH (0x0A)     |
  +---------+-------------+
            |
  +---------v-----------------------------------------+
  | ConcurrentAppState (tcp.rs)                       |
  |                                                   |
  |  engine: RwLock<PipelineEngine>  (read-heavy)     |
  |  store:  StateStore (DashMap)    (per-key locks)  |
  |  event_log: PLMutex<Option<EventLog>>             |
  |  metrics / throughput / latency: PLMutex each     |
  |  snapshot_*: PLMutex each                         |
  +---------+-----------------------------------------+
            |
  +---------v-------------------+  +------------------+
  | Pipeline Engine             |  | State Store      |
  | engine/pipeline.rs          |  | state/store.rs   |
  |                             |  |                  |
  | - Stream definitions        |  | DashMap<Key,     |
  | - DAG (petgraph)            |  |   EntityState>   |
  | - Topological cascade       |  | - per-stream     |
  | - Operator instantiation    |  |   operators      |
  | - Derive evaluation         |  | - static         |
  | - Fan-out dispatch          |  |   features       |
  +-----------------------------+  +--------+---------+
                                            |
                              +-------------+-------------+
                              |                           |
                   +----------v----------+   +------------v-----------+
                   | Snapshot Persistence |   | Event Log              |
                   | state/snapshot.rs    |   | state/event_log.rs     |
                   |                     |   |                        |
                   | - postcard + serde  |   | - Per-stream files     |
                   | - Base + Delta      |   | - Append-only          |
                   | - v6 format         |   | - BufWriter (memcpy)   |
                   | - 30s interval      |   | - 1s fsync interval    |
                   +---------------------+   | - 60s compaction       |
                                             +------------------------+
```

## Request Flow

### PUSH Event Flow

When a client sends a `PUSH` command, the following sequence executes synchronously in a single request-response cycle:

```
Client sends PUSH frame
  |
  v
1. Protocol decode (protocol.rs :: parse_command)
   - Read 4-byte length prefix (u32 BE)
   - Read 1-byte opcode (0x01)
   - Decode stream_name (2-byte length + UTF-8)
   - Decode event payload (JSON or binary wire format)
   - Preserve raw_payload bytes for event log (zero-copy)
  |
  v
2. Acquire engine read lock (RwLock -- concurrent with other PUSHes)
  |
  v
3. Cascade-aware push (pipeline.rs :: push_with_cascade)
   - Look up StreamDefinition by name
   - Extract entity key from event payload via key_field
   - Check stream-level filter expression (skip if no match)
   - Build depends_on cascade: collect all downstream streams
   |
   v
4. For each stream in topological order:
   a. Extract entity key for this stream
   b. DashMap::entry(key) -- per-key lock, no global contention
   c. For each FeatureDef in stream.features:
      - If where_expr exists, evaluate filter
      - Call operator.push(event, enrichment, now)
      - Operator updates its RingBuffer / HLL / etc.
   d. Collect enrichment values for downstream cascade
   |
   v
5. (No feature read on the write path.)
   Sync PUSH returns an empty JSON `{}` as acknowledgment. Clients that need
   the updated values issue a subsequent GET. Async PUSH (0x07) and PUSH_BATCH
   (0x0A) do not produce any response body at all on success. This keeps the
   write path fast and uniform -- read work happens only on demand via GET.
   |
   v
6. Mark entity key dirty for incremental snapshots
  |
  v
7. Fan-out: push to other streams whose key_field exists in event
   (skipping primary stream, cascade targets already visited)
  |
  v
8. Append to event log (acquire event_log lock -- separate from state)
   - Prefix with LOG_FMT_BINARY (0x01) or LOG_FMT_JSON (0x00)
   - BufWriter::write_all (~100-300ns memcpy, no fsync)
  |
  v
9. Update metrics (events_total, push_latency)
  |
  v
10. Encode response frame: STATUS_OK + JSON feature map
    - Send back to client on same TCP connection
```

Key invariant: the entity key's DashMap shard lock is held only for the duration of operator mutations (step 4b-4c). Different entity keys proceed concurrently. The event log lock is acquired separately after all state work completes.

### Async Push Flow

`PUSH_ASYNC` (0x07) uses server-side coalescing. Per-connection `ConnAccumulator` buffers up to 64 events or a 200-microsecond deadline, then dispatches them through `handle_push_batch` under a single engine read lock. Feature reads and derive evaluation are skipped (the `push_no_features` path), yielding ~140x speedup on large pipelines. Errors are surfaced on the next synchronous call.

## Pipeline Engine

The pipeline engine (`src/engine/pipeline.rs`) is the core orchestration layer. It holds all registered `StreamDefinition`s and coordinates the push-through flow.

### StreamDefinition

```rust
pub struct StreamDefinition {
    pub name: String,
    pub key_field: Option<String>,       // Entity key extraction field
    pub features: Vec<(String, FeatureDef)>,  // Named feature definitions
    pub depends_on: Option<Vec<String>>, // Upstream dependencies
    pub filter: Option<Expr>,            // Stream-level event filter
    pub entity_ttl: Option<Duration>,    // Per-stream TTL override
    pub history_ttl: Option<Duration>,   // Event log retention
    pub projection: Option<Projection>,  // Response field filter
    pub ephemeral: Option<bool>,         // Schema-only flag
    // ...
}
```

### DAG and Cascade

Streams can declare `depends_on` to form a composable pipeline DAG. The engine uses `petgraph` to:

1. Build a directed graph where edges flow from upstream to downstream
2. Detect circular dependencies via `petgraph::algo::toposort`
3. Maintain a pre-computed `topo_order: Vec<String>` for cascade execution
4. Maintain a `downstream_map: AHashMap<String, Vec<String>>` for fast cascade target lookup

When a PUSH arrives for stream A that has downstream dependents B and C:
- The engine walks `topo_order`, visiting only streams reachable from A
- Each downstream stream receives the same event payload plus an enrichment overlay containing features computed by upstream streams
- This enables composable pipelines where B can reference A's features in its derive expressions

The DAG is rebuilt on every `register()` / `remove_stream()` call (`rebuild_dag()`).

### Fan-Out

A single event can update multiple independent streams if it contains key fields for each. Fan-out targets are computed by `fan_out_targets()` and filtered to exclude the primary stream and any cascade targets already visited.

### Schema Evolution

Re-registering a stream computes a `SchemaDiff` classifying features as added, removed, unchanged, or backfilling. Changing an operator's type (e.g., count to sum) on an existing feature name is rejected. New features marked with `backfill: true` trigger automatic replay from the event log.

## State Store

The state store (`src/state/store.rs`) maps entity keys to their feature state using a `DashMap` for per-key concurrency.

### Structure

```rust
pub struct StateStore {
    entities: DashMap<EntityKey, EntityState>,
    dirty_keys: parking_lot::Mutex<AHashSet<EntityKey>>,   // For delta snapshots
    deleted_keys: parking_lot::Mutex<AHashSet<EntityKey>>, // For delta snapshots
}

pub struct EntityState {
    pub streams: AHashMap<String, StreamEntityState>,      // Per-stream operators
    pub static_features: AHashMap<String, StaticFeature>,  // From SET/MSET
}

pub struct StreamEntityState {
    pub operators: Vec<(String, OperatorState)>,
    pub last_event_at: Option<SystemTime>,  // Per-stream TTL tracking
}
```

Entity state is grouped by stream name. Each stream within an entity has its own operator instances and `last_event_at` timestamp, enabling independent per-stream TTL eviction.

### Concurrency Model

- `DashMap` provides shard-level locking: events for different entity keys never contend
- `PipelineEngine` is behind a `RwLock`: many concurrent reads (PUSH/GET), write only on REGISTER
- Event log, metrics, throughput, latency, and snapshot coordination each have independent `parking_lot::Mutex` locks
- No single lock serializes all connections

### Static Features

Features from `SET`/`MSET` commands bypass the pipeline engine entirely and land as `StaticFeature` entries in the same entity state. They are served alongside live features on `GET`.

## Operators

Tally implements 16 streaming operators, all defined as variants of `FeatureDef` (pipeline registration) and `OperatorState` (runtime state). Each implements the `Operator` trait with `push()` and `read()` methods.

| Operator | Description | State |
|----------|-------------|-------|
| `count` | Event count in window | `RingBuffer<u64>` |
| `sum` | Sum a field in window | `RingBuffer<f64>` |
| `avg` | Average a field in window | `RingBuffer<u64>` + `RingBuffer<f64>` |
| `min` | Minimum value in window (bucketed) | `RingBuffer<MinBucket>` |
| `max` | Maximum value in window (bucketed) | `RingBuffer<MaxBucket>` |
| `exact_min` | Exact minimum in window | `RingBuffer<BTreeMap>` |
| `exact_max` | Exact maximum in window | `RingBuffer<BTreeMap>` |
| `stddev` | Standard deviation in window | `RingBuffer` (sum + sum_sq + count) |
| `percentile` | Approximate percentile in window | `RingBuffer<TDigest>` |
| `distinct_count` | Approximate unique count | `RingBuffer<Hll>` (adaptive) |
| `last` | Most recent value of a field | Single value + timestamp |
| `first` | First value of a field | Single value + timestamp |
| `last_n` | Last N values of a field | `VecDeque` (bounded) |
| `lag` | Nth previous value of a field | `VecDeque` (bounded) |
| `ema` | Exponential moving average | Single value + timestamp |
| `derive` | Expression over other features | Stateless -- evaluated on read |

All operators have bounded memory per key. See `docs/operators.md` for detailed behavior, edge cases, and memory profiles.

## Expression Engine

The expression engine (`src/engine/expression.rs`) parses string expressions at pipeline registration time into an AST (`Expr`) using a winnow Pratt parser, then evaluates them at event time by walking the AST with an `EvalContext`.

### Syntax

- **Field access**: `field_name` (local), `StreamName.field_name` (qualified), `_event.field_name` (raw event)
- **Arithmetic**: `+`, `-`, `*`, `/`
- **Comparison**: `>`, `<`, `>=`, `<=`, `==`, `!=`
- **Boolean**: `and`, `or`, `not`
- **Literals**: numbers (`42`, `3.14`), strings (`'hello'`)
- **Unary**: `-x`, `not x`

### AST

```rust
pub enum Expr {
    Literal(f64),
    StringLit(String),
    FieldAccess(FieldRef),
    BinaryOp { op: BinOp, left: Box<Expr>, right: Box<Expr> },
    UnaryOp { op: UnOp, operand: Box<Expr> },
    FnCall { name: String, args: Vec<Expr> },
}

pub enum FieldRef {
    Local(String),                   // "tx_count_30m"
    Qualified(String, String),       // "Transactions.tx_count_30m"
    Event(String),                   // "_event.amount"
}
```

### Builtin Functions

| Function | Args | Description |
|----------|------|-------------|
| `abs(x)` | 1 | Absolute value |
| `min(a, b)` | 2 | Minimum of two values |
| `max(a, b)` | 2 | Maximum of two values |
| `sqrt(x)` | 1 | Square root |
| `log(x)` | 1 | Natural logarithm |
| `pow(x, n)` | 2 | Exponentiation |
| `ceil(x)` | 1 | Ceiling |
| `floor(x)` | 1 | Floor |
| `round(x)` | 1 | Round to nearest integer |
| `clamp(x, lo, hi)` | 3 | Clamp value to range |
| `now()` | 0 | Current timestamp (epoch seconds) |
| `if(cond, then, else)` | 3 | Conditional expression |
| `coalesce(x, default)` | 2 | Return x if present, else default |
| `is_missing(x)` | 1 | Returns 1 if x is Missing, else 0 |
| `len(s)` | 1 | String length |
| `lower(s)` | 1 | Lowercase string |
| `upper(s)` | 1 | Uppercase string |
| `contains(s, sub)` | 2 | String contains substring |
| `starts_with(s, prefix)` | 2 | String starts with prefix |
| `concat(a, b)` | 2 | String concatenation |

Expressions used in `derive` features are evaluated on every read (stateless). Expressions used in `where` clauses are evaluated on every push to filter events before operator processing.

## Windowed Aggregations

All windowed operators use a **bucketed ring buffer** (`src/engine/window.rs :: RingBuffer<T>`).

### How It Works

```
Window: 30 minutes, Bucket: 1 minute  =>  30 buckets in a ring

  [b0] [b1] [b2] ... [b28] [b29]
                        ^
                       head (current bucket)

On event arrival:
  1. advance_to(now) -- expire stale buckets by zeroing them
  2. Add value to current (head) bucket

On read:
  1. advance_to(now) -- expire stale buckets
  2. Sum/aggregate all non-expired buckets
```

- **Bucket count** = `ceil(window_duration / bucket_duration)`
- **Lazy expiration**: buckets are zeroed on `advance_to()`, not by background timers
- **Gap handling**: if the time gap exceeds the full window, all buckets are zeroed
- **First event**: initializes `current_bucket_start` from the bucket-aligned time

The bucket granularity determines accuracy vs. memory tradeoff. A 1-hour window with 1-minute buckets uses 60 buckets; with 1-second buckets it uses 3600. The default bucket size is `window / 30`.

### Distinct Count (HLL)

The `distinct_count` operator uses an adaptive three-phase approach (`src/engine/hll.rs`):

1. **Exact** (<=16 elements): flat sorted array, zero error, ~128 bytes
2. **HashSet** (<=threshold): AHashSet of u64 hashes, zero error, ~8 bytes/unique
3. **HLL++** (unlimited): HyperLogLog with bias correction, ~0.8% error at p=14, ~12KB

Each ring buffer bucket contains one HLL sketch. The windowed distinct count merges all non-expired bucket sketches on read.

## Persistence

### Snapshots

Snapshots (`src/state/snapshot.rs`) serialize the full in-memory state to disk periodically (default: every 30 seconds).

**Format**: version byte (v6) + type tag + postcard-serialized payload.

- **Base snapshots** (type tag `0x00`): full state -- all entities + pipeline definitions + backfill markers. Written every Nth cycle (default N=10, ~5 minutes).
- **Delta snapshots** (type tag `0x01`): only dirty/deleted keys since last snapshot. Written on all other cycles. References the base snapshot by sequence number.

**Recovery**: on startup, the server loads the latest base snapshot, then applies all subsequent deltas in sequence order. Legacy v5 single-file snapshots are auto-migrated.

**Snapshot cycle**:
1. Decide base vs. delta based on cycle counter
2. Clone required state from DashMap (base: all entities; delta: only dirty keys)
3. Clear dirty/deleted tracking
4. Serialize on a blocking thread pool (`tokio::task::spawn_blocking`)
5. Atomic write: serialize to `.tmp` file, `fsync`, rename
6. Cleanup old snapshot files (sequences older than previous base)

A `snapshot_in_progress` atomic flag prevents overlapping writes.

### Event Log

The event log (`src/state/event_log.rs`) provides per-stream append-only files for backfill replay.

- **Format**: per-stream files in `$TALLY_DATA_DIR/events/`, each entry is a `LogEntry` (timestamp + payload bytes)
- **Write path**: `BufWriter::write_all` (~100-300ns memcpy), never fsyncs on the hot path
- **Fsync**: background timer every 1 second (Redis `everysec` pattern)
- **Compaction**: background timer every 60 seconds removes entries older than `history_ttl` (default: 72 hours)
- **Payload format**: tagged with `LOG_FMT_BINARY` (0x01) or `LOG_FMT_JSON` (0x00) prefix byte for zero-copy forwarding of wire-format payloads

## HTTP Management API

The HTTP API runs on a separate port (default: 6401) using Axum. It is not on the hot path.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check (`{"status": "ok"}`) |
| `/pipelines` | GET | List registered pipeline names |
| `/pipelines` | POST | Register a pipeline definition |
| `/pipelines/{name}` | GET | Get pipeline definition details |
| `/pipelines/{name}` | DELETE | Remove a pipeline |
| `/metrics` | GET | Prometheus-format metrics |
| `/debug/key/{key}` | GET | Inspect full state for an entity key |
| `/debug/memory` | GET | Memory usage breakdown |
| `/debug/backfill` | GET | Backfill task status |
| `/debug/topology` | GET | Pipeline DAG topology |
| `/debug/throughput` | GET | Per-stream EWMA throughput |
| `/debug/latency` | GET | Per-command latency histograms |
| `/snapshot` | POST | Trigger manual snapshot |
| `/` | GET | Built-in debug UI |

See `docs/http-api.md` for request/response schemas and examples.

## Configuration

All configuration is via environment variables. No config files.

| Variable | Default | Description |
|----------|---------|-------------|
| `TALLY_TCP_PORT` | `6400` | TCP protocol port (hot path) |
| `TALLY_HTTP_PORT` | `6401` | HTTP management API port |
| `TALLY_SNAPSHOT_PATH` | `tally.snapshot` | Base path for snapshot files |
| `TALLY_DATA_DIR` | `.` | Directory for event log files (`$DIR/events/`) |
| `TALLY_TTL_MULTIPLIER` | `2` | Entity TTL = multiplier x largest window duration |
| `TALLY_WORKER_THREADS` | `4` | Tokio runtime worker threads |
| `TALLY_FULL_SNAPSHOT_INTERVAL` | `10` | Cycles between full base snapshots (at 30s/cycle = ~5min) |
| `TALLY_EVENT_LOG` | `true` | Enable/disable event log (`false` or `0` to disable) |
| `TALLY_SNAPSHOT` | `true` | Enable/disable snapshots (`false` or `0` to disable) |

Setting both `TALLY_EVENT_LOG=false` and `TALLY_SNAPSHOT=false` runs in ephemeral mode -- all state is lost on restart.

## Key Source Files

| File | Purpose |
|------|---------|
| `src/main.rs` | Entry point, config, runtime setup, snapshot/eviction/fsync timers |
| `src/server/tcp.rs` | TCP listener, connection handler, `ConcurrentAppState`, PUSH/GET dispatch |
| `src/server/protocol.rs` | Binary protocol encoding/decoding, command parsing |
| `src/server/http.rs` | HTTP management API routes and handlers |
| `src/engine/pipeline.rs` | `PipelineEngine`, `StreamDefinition`, DAG, cascade, push-through |
| `src/engine/operators.rs` | `Operator` trait, `CountOp`, `SumOp`, `AvgOp`, etc. |
| `src/engine/window.rs` | `RingBuffer<T>` -- bucketed sliding window |
| `src/engine/expression.rs` | Pratt parser, AST, evaluator, builtins |
| `src/engine/hll.rs` | Adaptive HyperLogLog (exact -> HashSet -> HLL++) |
| `src/state/store.rs` | `StateStore` (DashMap), `EntityState`, `StreamEntityState` |
| `src/state/snapshot.rs` | `OperatorState` enum, base/delta snapshot serialization |
| `src/state/event_log.rs` | Per-stream append-only event log, compaction |
| `src/state/eviction.rs` | TTL-based per-stream entity eviction |
