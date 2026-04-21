# Beava Architecture

See also: [docs/operators.md](operators.md) · [docs/event-time.md](event-time.md) · [docs/comparison.md](comparison.md) · [benchmark/](../benchmark/)

## Overview

Beava is a real-time feature server that ingests events over a custom binary TCP protocol, computes stateful streaming features (windowed aggregations, derived expressions, cross-stream cascades), and serves them with sub-millisecond latency. It ships as a single Rust binary with zero external dependencies -- no Kafka, no Flink, no cluster. Push an event, get updated features back in the same response. All state lives in memory, with periodic snapshots and an append-only event log for crash recovery.

## Single-Node by Design

Beava deliberately ships as one binary running on one machine. This is not a limitation waiting to be fixed -- it is a deliberate design choice for a specific workload:

**What it buys:**
- Zero operational overhead. No Kafka cluster, no ZooKeeper, no Flink JobManager, no Redis Sentinel. Start with `docker run beavadb/beava:latest`.
- Predictable latency. All state is in memory on the same process. Feature reads are pointer dereferences, not network round-trips.
- Training/serving parity. Data scientists define pipelines in Python; the same pipeline runs in production. No separate offline-to-online sync.
- Simple recovery. One process crashes, one process restarts. State recovery is 7 seconds for 4.7 GB of state (snapshot + WAL replay).

**Why not Kafka + Flink + Redis?**
If you need multi-node horizontal scale today, Kafka + Flink + Redis is the right choice. Beava is for the team that does not have a streaming platform engineer, whose working set fits in RAM, and who wants features running in minutes. See [docs/comparison.md](comparison.md) for the honest pairwise.

**What it sacrifices:**
- No horizontal scale-out in v1.0. One machine is the ceiling.
- No exactly-once. At-least-once with client-side `event_id` dedup.
- No connector ecosystem. TCP push protocol + HTTP API only.

## Scaling Posture

| Tier | When | Mechanism |
|------|------|-----------|
| v1.0 (now) | Single node, vertical | 4-worker Tokio runtime, DashMap shard concurrency |
| v1.2 (planned) | Thread-per-core | Replace Tokio with a single-threaded-per-core reactor; eliminate cross-thread lock contention for 2-4× throughput gain per core |
| v1.3+ (planned) | Multi-node | Partition by key across nodes via Kafka; each node runs an independent Beava shard |
| Beava Cloud (Q4 2026) | Managed HA | Multi-node with automated failover; managed by the Beava team |

The published baseline is 315 K events/sec sustained TCP push on a 10-core Apple M4 laptop (fraud-pipeline, 47 features, 5 entity types). HTTP push-batch exceeds 100 K EPS on the same hardware. See [benchmark/](../benchmark/) for the full 9-cell matrix.

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

## Typed Pipeline Records (Phase 59.6)

Beava's in-pipeline event and state representation is a typed, fixed-layout `Row` compiled from the SDK's `@bv.stream` / `@bv.source_table` / `@bv.table` class annotations at register time. Wire codec, engine operators, and state store all work on `Row` — not `serde_json::Value` — for any stream that declared a typed schema. Streams that did not declare a typed schema continue to run on the `Value` fallback path; mixed-mode deployments work unchanged.

### How it works

1. **At import time (Python SDK ≥ v0.3.0):** The `@bv.stream` decorator walks the class's `__annotations__` and builds a `CompiledSchema` (field name, Rust-side `FieldTy`, byte offset, nullable flag). Stored on the descriptor as `_beava_schema`.
2. **At register time (wire):** The SDK emits a `schema: {fields: [...], inline_str_cap, row_size}` block inside the existing REGISTER JSON payload. The server parses it into `RegisteredSchema` and assigns a monotonic server-side `schema_id`. The REGISTER ack JSON carries the `schema_id` back to the client, which caches it in `BeavaClient._schema_ids[stream_name]`.
3. **At push time (wire):** Clients with `WIRE_TYPED_PIPELINE` capability emit `OP_PUSH_TYPED_BATCH = 0x19` frames with a `schema_id` prefix + packed rows (each row is `payload_len | payload_bytes | arena_len | arena_bytes`). Clients without the capability fall back to `OP_PUSH_BATCH` (Value path).
4. **On the shard thread:** Rows flow through `engine.push_typed_on_shard` → typed operators (`EnrichFromTableTyped`, 16 `TypedAggOp` impls, `StreamStreamJoinTyped`) → state store (`Shard::entity_state_typed` / fjall typed-row persistence via `put_entity_typed`). The Value path (`push_with_cascade_on_shard`) remains in place for unregistered-schema streams, HTTP ingest, and ad-hoc debug paths.

### Per-event cost (measured 2026-04-21 on a macOS 10-core laptop)

| Path                                                         | Pre-59.6 (Value) | Post-59.6 (typed) | Reduction       |
|--------------------------------------------------------------|------------------|-------------------|-----------------|
| Wire decode                                                  | 0.89 μs          | ~0.05 μs          | ≥ 15×           |
| Pipeline-phase operator cascade (17 ops, Criterion steady)    | ~8.5 μs          | **22.97 ns**       | ~370×           |
| Pipeline-phase operator cascade (3 ops, steady-state update)  | ~1.5 μs          | **1.84 ns**        | ~800×           |
| Per-entity state write (fjall typed row)                      | JSON roundtrip   | `memcpy` of payload + schema_id prefix | ≥ 10× |
| Typical total per-event (hot path, registered pipeline)       | ≈ 9.8 μs         | ≈ 0.08 μs (operator + decode) | ~120× |

Measurements via `cargo bench --bench typed_pipeline_phase_latency` with a 3-op `(Count + Sum + Avg)` cascade and a 17-op `(1 × Count + 8 × Sum + 8 × Avg)` cascade. Full perf-gate evidence in `.planning/phases/59.6-typed-pipeline-records/59.6-PERF-GATE.md`.

### Backward compatibility

- **Streams registered WITHOUT a typed schema** (pre-59.6 SDKs, unannotated classes, HTTP/ad-hoc pushes, `/debug/push`) continue to work via the `serde_json::Value` fallback path.
- **Client-side handshake:** `OP_NEGOTIATE_WIRE_FORMAT = 0x18` exposes `WIRE_TYPED_PIPELINE = 1 << 1` in the server's supported-bits. Clients that omit the handshake or lack the bit emit `OP_PUSH_BATCH` (Value) and the server transparently routes through the generic operator path.
- **Counters:** `beava_typed_row_path_total{stream}` + `beava_value_fallback_path_total{stream}` gauges report which path each stream took over the last observation window. Registered typed pipelines report 100 % typed post-landing; unregistered streams report 100 % fallback.

### Limitations

- **Schema evolution** (add/remove fields post-register) is not supported in v1.x; re-register with a new stream name. Schema evolution is a v1.4+ roadmap item.
- **Arrow / Parquet interop** is additive — typed `Row` can be emitted as Arrow batches (future work; not shipped in 59.6).
- **Sketch state** (HLL / UDDSketch / CMS / ring buffers) is stored in a per-entity `SideBand` map alongside the state Row. V11 snapshot currently carries only the state Row bytes; sketch state rehydrates from event-log replay on recovery (same behavior as pre-59.6). Adding a `SideBand` extension to V11 is a 59.6-NEXT item.
- **Aggregate-EPS verification on macOS dev hosts** is bounded by the Python SDK measurement ceiling (~1.3M EPS thermal-throttled / ~1.7M EPS hot-start on 8 clients). Operator-level per-event cost is verified via Criterion; full aggregate-EPS verification at >3M EPS requires either a Linux host with SO_REUSEPORT or the Phase 64 Rust bench client.

### Key source files

- `src/engine/schema.rs` — `RegisteredSchema`, `FieldSpec`, `FieldTy`, `Row`, `SchemaRegistry`, `row_to_value` / `value_to_row` bridge.
- `src/engine/operators_typed.rs` — `TypedAggOp` trait + `EnrichFromTableTyped` + `StreamStreamJoinTyped` + `TypedSsjBuffer` + `SideBand`.
- `src/engine/operators_typed_aggs.rs` — 7 simple typed aggs (Count / Sum / Avg / Min / Max / Last / First).
- `src/engine/operators_typed_sketches.rs` — 5 advanced aggs (DistinctCount / Percentile / TopK / Stddev / Variance).
- `src/engine/operators_typed_windows.rs` — 4 windowed aggs (Ema / Lag / FirstN / LastN).
- `src/wire/typed.rs` — `OP_PUSH_TYPED_BATCH` encoder / decoder with strict `schema_id` match.
- `src/shard/mod.rs` — `Shard::entity_state_typed`, `entity_sideband_typed`, `get_or_init_entity_state_typed`.
- `src/engine/pipeline.rs` — `push_typed_on_shard`, `run_typed_enrich_cascade`, `run_typed_agg_step`.
- `src/state/snapshot.rs` — V11 snapshot reader/writer for typed rows.
- `python/beava/_schema_compile.py` — class-annotation → `CompiledSchema` compiler.
- `python/beava/_serialize.py` — typed REGISTER + `_pack_typed_batch` encoder.
- `benches/typed_pipeline_phase_latency.rs` — Criterion harness (3 cells: single_event / update_only_3ops / cascade_17ops).
- `.planning/phases/59.6-typed-pipeline-records/59.6-PERF-GATE.md` — full perf evidence.

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

Beava implements 16 streaming operators, all defined as variants of `FeatureDef` (pipeline registration) and `OperatorState` (runtime state). Each implements the `Operator` trait with `push()` and `read()` methods.

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

- **Format**: per-stream files in `$BEAVA_DATA_DIR/events/`, each entry is a `LogEntry` (timestamp + payload bytes)
- **Write path**: `BufWriter::write_all` (~100-300ns memcpy), never fsyncs on the hot path
- **Fsync**: background timer every 1 second (Redis `everysec` pattern)
- **Compaction**: background timer every 60 seconds removes entries older than `history_ttl` (default: 72 hours)
- **Payload format**: tagged with `LOG_FMT_BINARY` (0x01) or `LOG_FMT_JSON` (0x00) prefix byte for zero-copy forwarding of wire-format payloads

## Fork Replica Model

The `beava fork` command lets data scientists run a scoped local replica of a production server. The replica receives the production event stream and runs independent local pipelines against the live feed -- enabling "shadow mode" experiments without touching production state.

### How it works

```
Production server                     Local replica (scientist's laptop)
──────────────────                    ──────────────────────────────────
EventLog (per-stream files)  ──LOG_FETCH──►  Phase 1: Historical catchup
                                              replay all events from disk

Live ingest path             ──SUBSCRIBE──►  Phase 2: Live-tail
                                              each incoming event forwarded
                                              to replica in real time

                                         Local pipelines run on every event:
                                         watermarks advance, features update
                                         Scientists read via /debug/key/{key}
```

**Phase 1 — LOG_FETCH catchup:** The replica requests the upstream server's event log for each configured stream. Events are replayed from disk using the same `run_backfill` path as crash recovery. Watermarks advance per event.

**Phase 2 — SUBSCRIBE live-tail:** After catchup, the replica subscribes to the live event stream. `replica_ingest_batch` at `src/server/tcp.rs` processes each incoming event and advances watermarks.

**Local pipelines:** Any `@bv.table` definitions passed via `--pipeline-file` (or `bv.fork(pipelines=[...])`) are registered on the replica. They fire as watermarks advance -- exactly as they would on the production server.

### Python usage

```python
import beava as bv

@bv.stream
class Transactions:
    user_id: str
    amount: float

@bv.table(key="user_id")
def TxnSummary(t: Transactions) -> bv.Table:
    return t.group_by("user_id").agg(
        count_1h=bv.count(window="1h"),
        sum_1h=bv.sum("amount", window="1h"),
    )

with bv.fork(
    remote="prod.beava.dev:6400",
    streams=[Transactions],
    keys=["u1", "u2"],
    token="replica-token",
    pipelines=[TxnSummary],
) as fork:
    print(fork.get(TxnSummary, key="u1"))
```

See [docs/event-time.md § Fork Watermark Propagation](event-time.md#fork-watermark-propagation) for the watermark correctness guarantee on the replica.

### Key properties

- **Read-only.** The replica never writes back to the production server.
- **Scoped.** You can filter to a subset of keys (`keys=["u1", "u2"]`) or a key prefix.
- **Independent pipelines.** The replica runs its own local `@bv.table` definitions; they can differ from production.
- **Deterministic.** Because the replica uses payload `_event_time` as the event-time clock (not wall-clock), it reproduces the same feature values as the production server for the same event sequence.

## Security Model

Beava's security model is intentionally minimal for v1.0.

**Loopback bypass.** Requests from `127.0.0.1` or `::1` are automatically authenticated. Run Beava on localhost with no token for local development.

**Bearer token.** Set `BEAVA_ADMIN_TOKEN` to require a token on all write endpoints and (by default) read endpoints:
```bash
BEAVA_ADMIN_TOKEN=my-secret beava serve
```
Pass `Authorization: Bearer my-secret` in requests.

**Public read mode.** `beava serve --public` makes `/features/*` and `/streams/*` available without authentication. Write endpoints always require a token when `BEAVA_ADMIN_TOKEN` is set.

**TLS.** Beava does not terminate TLS. Put it behind a reverse proxy (Caddy, nginx, or a cloud load balancer) that terminates TLS at the edge. See `deploy/` for a Caddy example.

**Admin token scope.** The admin token is a single shared secret -- no per-user tokens, no RBAC, no audit log. For v1.0, the expected deployment is: one Beava instance per service, one token per service.

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
| `BEAVA_TCP_PORT` | `6400` | TCP protocol port (hot path) |
| `BEAVA_HTTP_PORT` | `6401` | HTTP management API port |
| `BEAVA_SNAPSHOT_PATH` | `beava.snapshot` | Base path for snapshot files |
| `BEAVA_DATA_DIR` | `.` | Directory for event log files (`$DIR/events/`) |
| `BEAVA_TTL_MULTIPLIER` | `2` | Entity TTL = multiplier x largest window duration |
| `BEAVA_WORKER_THREADS` | `4` | Tokio runtime worker threads |
| `BEAVA_FULL_SNAPSHOT_INTERVAL` | `10` | Cycles between full base snapshots (at 30s/cycle = ~5min) |
| `BEAVA_EVENT_LOG` | `true` | Enable/disable event log (`false` or `0` to disable) |
| `BEAVA_SNAPSHOT` | `true` | Enable/disable snapshots (`false` or `0` to disable) |

Setting both `BEAVA_EVENT_LOG=false` and `BEAVA_SNAPSHOT=false` runs in ephemeral mode -- all state is lost on restart.

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
