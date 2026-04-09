# Architecture Research: v1.1 Composable Pipeline & Event Log

**Domain:** Integration of composable pipelines, SSD event log, backfill, debug UI into existing single-threaded real-time feature server
**Researched:** 2026-04-09
**Confidence:** HIGH (based on existing codebase analysis + Redis AOF patterns + Rust ecosystem)

## Existing Architecture Summary

The v1.0 codebase (~8,400 lines of Rust) follows a single-threaded Redis-like architecture:

```
main.rs
  tokio::main(flavor = "current_thread")
  Arc<Mutex<AppState>> shared across:
    - TCP server (port 6400) -- hot path
    - HTTP server (port 6401, axum) -- management
    - Snapshot timer (30s interval, clone-then-spawn_blocking)
    - Eviction timer (60s interval)

AppState {
  engine: PipelineEngine       // stream/view defs, push/get orchestration
  store: StateStore            // AHashMap<EntityKey, EntityState>
  metrics: Metrics             // events_total, push_latency, snapshot_duration
  snapshot_path: PathBuf
}
```

Key observation: The codebase uses `Arc<Mutex<AppState>>` (not `Rc<RefCell>` as the v1.0 architecture research recommended). This is because `spawn_blocking` for snapshots requires `Send`, and the HTTP server (axum) also needs `Send`. The Mutex is never contended in practice because all hot-path operations are synchronous (lock, process, unlock, no `.await` while locked).

## v1.1 Target Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Tally Server Process (v1.1)                       │
│                                                                         │
│  ┌──────────────────────┐    ┌────────────────────────────────────────┐ │
│  │  TCP Listener 6400   │    │  HTTP Management 6401 (axum)          │ │
│  │  PUSH GET SET MSET   │    │  /pipelines /health /metrics          │ │
│  │  MGET REGISTER       │    │  /debug/* endpoints                   │ │
│  └──────────┬───────────┘    │  /ui/* → embedded debug UI (SPA)      │ │
│             │                └──────────┬─────────────────────────────┘ │
│             │                           │                               │
│  ┌──────────▼───────────────────────────▼──────────────────────────┐   │
│  │                    AppState (Arc<Mutex<>>)                       │   │
│  │                                                                  │   │
│  │  ┌──────────────────────────────────────────────────────────┐   │   │
│  │  │  PipelineEngine (MODIFIED)                               │   │   │
│  │  │  - streams: AHashMap<String, StreamDefinition>           │   │   │
│  │  │  - views: AHashMap<String, ViewDefinition>               │   │   │
│  │  │  + keyless_streams: AHashMap<String, KeylessStreamDef>   │   │   │
│  │  │  + dependency_graph: DependencyGraph (NEW)               │   │   │
│  │  │  + schema_registry: SchemaRegistry (NEW)                 │   │   │
│  │  └──────────────────────────────────────────────────────────┘   │   │
│  │                                                                  │   │
│  │  ┌──────────────────────────────────────────────────────────┐   │   │
│  │  │  StateStore (MODIFIED)                                   │   │   │
│  │  │  entities: AHashMap<EntityKey, EntityState>              │   │   │
│  │  │  + per-dataset TTL configuration                         │   │   │
│  │  └──────────────────────────────────────────────────────────┘   │   │
│  │                                                                  │   │
│  │  ┌──────────────────────────────────────────────────────────┐   │   │
│  │  │  EventLog (NEW)                                          │   │   │
│  │  │  - append_only_writer: BufWriter<File>                   │   │   │
│  │  │  - log_index: AHashMap<StreamName, Vec<LogSegment>>      │   │   │
│  │  │  - fsync_policy: FsyncPolicy (everysec / no)             │   │   │
│  │  └──────────────────────────────────────────────────────────┘   │   │
│  │                                                                  │   │
│  │  Metrics (EXTENDED)                                              │   │
│  │  + event_log_bytes, backfill_progress, per-stream counters      │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  Background Tasks:                                                      │
│  - Snapshot timer (30s, MODIFIED for incremental)                       │
│  - Eviction timer (60s, MODIFIED for per-dataset TTL)                   │
│  + Fsync timer (1s, event log flush)                                    │
│  + Log compaction timer (configurable, rewrites old segments)           │
│  + Backfill task (on-demand, cooperative yielding)                      │
└─────────────────────────────────────────────────────────────────────────┘
```

## Component Analysis: New vs Modified

### NEW Components

| Component | File | Lines Est. | Purpose |
|-----------|------|-----------|---------|
| EventLog | `src/state/event_log.rs` | ~400 | Append-only SSD event log with segments, fsync, compaction |
| KeylessStreamDef | `src/engine/pipeline.rs` (extend) | ~80 | Stream definitions without key field (raw event capture) |
| DependencyGraph | `src/engine/dag.rs` | ~200 | Topological ordering of stream/view execution |
| SchemaRegistry | `src/engine/schema.rs` | ~250 | Schema diff, evolution, feature add/remove logic |
| BackfillEngine | `src/engine/backfill.rs` | ~300 | Replay events from log, cooperative yielding |
| DebugUI | `src/server/debug_ui.rs` | ~100 | Embedded SPA assets, WebSocket for live data |
| MGET handler | `src/server/tcp.rs` (extend) | ~40 | Batch GET command |

### MODIFIED Components

| Component | File | Change Scope | What Changes |
|-----------|------|-------------|--------------|
| PipelineEngine | `src/engine/pipeline.rs` | Medium | Add keyless stream support, DAG execution order, schema evolution hooks |
| StateStore | `src/state/store.rs` | Small | Per-dataset TTL, `get_many_features()` for MGET |
| Snapshot | `src/state/snapshot.rs` | Medium | Incremental serialization (dirty-key tracking) |
| Eviction | `src/state/eviction.rs` | Small | Per-dataset TTL instead of global TTL |
| TCP handler | `src/server/tcp.rs` | Small | MGET opcode, event log write on PUSH, backfill trigger |
| HTTP API | `src/server/http.rs` | Medium | Debug UI routes, backfill trigger endpoint, schema evolution endpoint |
| Protocol | `src/server/protocol.rs` | Small | MGET command parsing, schema evolution register semantics |
| main.rs | `src/main.rs` | Small | EventLog init, fsync timer, log compaction timer |
| types.rs | `src/types.rs` | Minimal | No changes expected |
| error.rs | `src/error.rs` | Small | New error variants for event log, backfill, schema |

## Detailed Component Designs

### 1. Event Log (`src/state/event_log.rs`)

**Design: Redis AOF-inspired, segmented, append-only.**

The event log is the foundation for backfill and keyless streams. It must not degrade the <100us PUSH latency target.

```rust
/// Configuration for event log behavior
struct EventLogConfig {
    /// Base directory for log segments
    log_dir: PathBuf,
    /// Maximum segment size before rotation (default: 64MB)
    max_segment_size: u64,
    /// Fsync policy: Everysec (default) or No
    fsync_policy: FsyncPolicy,
    /// Per-stream history TTL (None = no logging for this stream)
    /// Maps stream name -> retention duration
    stream_ttls: AHashMap<String, Duration>,
}

enum FsyncPolicy {
    /// fsync every second via background timer (default, like Redis)
    Everysec,
    /// Never fsync, let OS handle it (fastest, least durable)
    No,
}

/// A single log segment file
struct LogSegment {
    path: PathBuf,
    stream_name: String,
    start_offset: u64,
    end_offset: u64,
    created_at: SystemTime,
    size_bytes: u64,
}

/// Entry in the event log
struct LogEntry {
    timestamp: SystemTime,
    stream_name_len: u16,
    stream_name: String,   // which stream this event belongs to
    payload_len: u32,
    payload: Vec<u8>,      // raw JSON bytes (same as PUSH payload)
}

struct EventLog {
    config: EventLogConfig,
    /// Current write segment per stream
    writers: AHashMap<String, BufWriter<File>>,
    /// Segment index per stream (for replay)
    segments: AHashMap<String, Vec<LogSegment>>,
    /// Bytes written since last fsync (for everysec tracking)
    unfsynced_bytes: u64,
}
```

**Integration with PUSH hot path:**

The event log write happens AFTER the in-memory state update but BEFORE the response is sent. This is critical: the log captures the raw event for replay, but the response latency includes the buffered write (NOT the fsync).

```
PUSH arrives
  1. Lock AppState
  2. engine.push() -- update operators, compute features
  3. event_log.append(stream_name, payload) -- buffered write, ~200ns
  4. Unlock, return features
  ...
  [background: fsync timer fires every 1s, calls fdatasync()]
```

**Latency budget:** `BufWriter<File>::write()` for a ~300 byte event is ~100-300ns (memcpy into kernel page cache). No fsync on hot path. The 1-second fsync timer means at most 1 second of events lost on crash, which matches the existing ~30s snapshot loss tolerance.

**Per-stream opt-in:** Streams declare `history=True` in their definition. Streams without history skip the log write entirely (zero overhead). This is exposed in the Python SDK as:

```python
@st.stream(key="user_id", history=True)
class Transactions:
    ...
```

**Compaction:** A background timer periodically rewrites old segments. For keyed streams, compaction walks the current in-memory state and writes the minimal set of events that would reproduce it (similar to Redis AOF rewrite). The retention TTL per stream determines when segments are eligible for deletion.

**Confidence:** HIGH. Redis AOF has proven this pattern at scale. The buffered-write + periodic-fsync approach is well-understood. `std::fs::File` wrapped in `BufWriter` with periodic `fdatasync()` is the standard Rust approach.

### 2. Keyless Streams (`src/engine/pipeline.rs` extension)

**Design: Append-only event capture without aggregation.**

Keyless streams are event sinks. They have no key field, no operators, and no state in the in-memory store. They exist purely for the event log -- capturing raw events that downstream keyed streams can reference.

```rust
/// A keyless stream: raw event ingestion, no aggregation
struct KeylessStreamDef {
    name: String,
    /// Always true -- keyless streams exist for the event log
    history: bool,
    /// Optional schema for validation (field names + types)
    schema: Option<EventSchema>,
}
```

**Integration with PUSH:**

When a PUSH targets a keyless stream:
1. Validate event against schema (if defined)
2. Write to event log
3. Fan out to any keyed streams that depend on this keyless stream (via DAG)
4. Return OK (no features to return since there are no operators)

**Why this matters:** Keyless streams enable the composable pipeline pattern where raw events are captured first, then processed into keyed aggregations. A `RawTransactions` keyless stream can feed both `UserTransactions` (keyed by user_id) and `MerchantTransactions` (keyed by merchant_id) via explicit dependencies.

**Confidence:** HIGH. This is a straightforward extension. The existing PUSH handler already returns features -- for keyless streams it returns an empty map.

### 3. Dependency Graph / DAG Execution (`src/engine/dag.rs`)

**Design: Static topological ordering computed at registration time.**

The current v1.0 system has implicit dependencies: PUSH updates operators, then evaluates derives, then fan-out fires. Views are evaluated lazily on GET. This works but doesn't support explicit composition like "when RawTransactions is pushed, also update UserTransactions."

```rust
/// Represents a node in the pipeline dependency graph
enum PipelineNode {
    KeylessStream(String),
    KeyedStream(String),
    View(String),
}

/// Edge: source feeds into target
struct Dependency {
    source: PipelineNode,
    target: PipelineNode,
    /// How the source feeds the target
    join: JoinType,
}

enum JoinType {
    /// Target uses source's key field directly
    SameKey,
    /// Target uses a different field from source events for its key
    Rekey { source_field: String },
    /// Cross-key lookup (existing v1 behavior)
    Lookup { on_field: String },
}

struct DependencyGraph {
    nodes: Vec<PipelineNode>,
    edges: Vec<Dependency>,
    /// Pre-computed topological order (recomputed on register/remove)
    execution_order: Vec<PipelineNode>,
}
```

**How it changes PUSH:**

Current v1.0 PUSH flow:
```
PUSH(Transactions, event)
  -> engine.push("Transactions", event) -- update operators
  -> fan_out_targets() -- find other streams with matching key fields
  -> for each target: engine.push(target, event)
```

v1.1 PUSH flow with DAG:
```
PUSH(RawTransactions, event)  -- or PUSH(Transactions, event)
  -> event_log.append(stream_name, event)
  -> execution_order = dag.get_execution_order(stream_name)
  -> for each node in execution_order:
       if keyless_stream: skip (already logged)
       if keyed_stream: extract key, update operators
       if view: skip (views are still lazy on GET)
```

**Cycle detection:** Computed at registration time. If adding a new stream/view would create a cycle, the REGISTER command returns an error. Uses Kahn's algorithm (BFS-based topological sort) which naturally detects cycles.

**Implementation: Inline, not a crate dependency.** The graph is small (typically <20 nodes). A ~100-line topological sort is simpler and lighter than pulling in `daggy` or `petgraph`. The execution order is precomputed and stored as a `Vec` -- no graph traversal on the hot path.

**Confidence:** HIGH. Topological sort is well-understood. The existing fan-out logic already does something similar (iterate streams, check key fields). The DAG replaces that implicit logic with an explicit, validated order.

### 4. Schema Evolution (`src/engine/schema.rs`)

**Design: Diff-based reconciliation on re-register.**

Currently, re-registering a stream replaces the definition entirely (idempotent). Operator state for existing entities is preserved only because operator names happen to match. v1.1 makes this explicit.

```rust
struct SchemaRegistry {
    /// Version counter per stream (incremented on each schema change)
    versions: AHashMap<String, u64>,
    /// Previous definitions for diff computation
    previous_defs: AHashMap<String, StreamDefinition>,
}

enum SchemaChange {
    /// New feature added -- needs backfill if history available
    FeatureAdded { name: String, def: FeatureDef, needs_backfill: bool },
    /// Feature removed -- drop operator state for this feature
    FeatureRemoved { name: String },
    /// Feature changed incompatibly -- reset operator state
    FeatureChanged { name: String, old: FeatureDef, new: FeatureDef },
    /// Feature unchanged -- preserve operator state
    FeatureUnchanged { name: String },
}
```

**Diff logic:**

When a stream is re-registered:
1. Compare old and new feature lists by name
2. For each feature name:
   - Present in both, same type/window/field: `FeatureUnchanged` (preserve state)
   - Present in both, different type or window: `FeatureChanged` (reset this feature's operator state for all entities)
   - Present in new only: `FeatureAdded` (create new operators; if `backfill=True` on the stream, queue backfill)
   - Present in old only: `FeatureRemoved` (remove operator state from all entities)
3. Apply changes to EntityState for all affected entities

**State reconciliation on entities:**

This requires iterating affected entities. For `FeatureRemoved`, iterate all entities and remove the operator by name from `live_operators`. For `FeatureAdded`, the new operator is lazily initialized on the next PUSH (existing behavior -- operators are created on first push if missing). For `FeatureChanged`, iterate and replace the operator.

**The iterate-all-entities concern:** With 1M entities, iterating to remove/reset operators could block the event loop. Use the same cooperative yielding pattern as MSET: process 1024 entities per chunk, yield between chunks.

**Confidence:** MEDIUM. The diff logic is straightforward. The concern is the entity iteration cost for feature removal/reset. The cooperative yielding pattern from MSET is proven, but this is a new application of it.

### 5. Backfill Engine (`src/engine/backfill.rs`)

**Design: Event replay from log with cooperative yielding.**

When a new feature is added to a stream with `history=True`, backfill replays historical events from the event log to populate the new feature's operator state.

```rust
struct BackfillTask {
    stream_name: String,
    feature_names: Vec<String>,  // only the new features to populate
    from_segment: usize,         // start segment index
    events_processed: u64,
    events_total: u64,           // for progress tracking
    status: BackfillStatus,
}

enum BackfillStatus {
    Queued,
    Running { progress_pct: f32 },
    Complete,
    Failed(String),
}
```

**How backfill works:**

1. Schema evolution detects `FeatureAdded` with `needs_backfill=true`
2. Creates a `BackfillTask` and starts it as an async task
3. The task reads log segments sequentially:
   a. Deserialize LogEntry
   b. Extract entity key from event
   c. Find or create the new operator for this entity
   d. Call `operator.push(event, entry.timestamp)` with the HISTORICAL timestamp
   e. Every 1024 events, yield to event loop
4. During backfill, live PUSH events continue normally (new operator is already initialized, so new events update it concurrently with replay)

**Critical design choice: Use historical timestamps.** The ring buffer's `advance_to()` must process events in timestamp order for correct window computation. Since events are replayed from the log (which is append-order ~ timestamp order), this works correctly. The only subtlety: if a live PUSH arrives during backfill with a timestamp newer than the replay position, the ring buffer handles this correctly (it advances forward).

**Backfill does NOT re-run derives.** Derives are computed on read (GET), not stored. Only operator state needs population.

**Progress tracking:** Exposed via `GET /debug/backfill` and the debug UI.

**Confidence:** MEDIUM. The replay logic is straightforward, but the interaction between live pushes and historical replay on the same operator state needs careful testing. Edge cases: out-of-order timestamps, events spanning window boundaries during replay.

### 6. MGET Command (`src/server/tcp.rs` + `src/server/protocol.rs`)

**Design: Batch GET over the existing binary protocol.**

```
MGET (0x06)
  count: u32
  [key: string] x count
  -> Response: JSON array of { key: string, features: { ... } }
```

**Implementation in tcp.rs:**

```rust
Command::Mget { keys } => {
    let mut app = state.lock().unwrap_or_else(|e| e.into_inner());
    let AppState { ref engine, ref mut store, .. } = *app;
    let now = SystemTime::now();
    let results: Vec<serde_json::Value> = keys.iter().map(|key| {
        let features = engine.get_features(key, store, now);
        let map: serde_json::Map<_, _> = features.iter()
            .map(|(k, v)| (k.clone(), v.to_json_value()))
            .collect();
        serde_json::json!({ "key": key, "features": map })
    }).collect();
    Ok(serde_json::to_vec(&results).unwrap())
}
```

MGET is a synchronous command (like GET). It does NOT use cooperative yielding because the caller is waiting for a single response. For very large MGET requests (>10K keys), the lock hold time could be 5-10ms. This is acceptable -- MGET is a batch operation, not a latency-sensitive hot path.

**Confidence:** HIGH. This is a trivial extension of the existing GET logic. The protocol addition is mechanical.

### 7. Incremental Snapshots (`src/state/snapshot.rs` modification)

**Design: Dirty-key tracking with full snapshot fallback.**

The current approach clones ALL entities for every snapshot. At 1M entities this causes a significant memory spike. Incremental snapshots track which entities changed since the last snapshot and only serialize those.

```rust
/// Tracks which entity keys have been modified since last snapshot
struct DirtyTracker {
    /// Keys modified since last snapshot
    dirty_keys: AHashSet<String>,
    /// Keys deleted since last snapshot (eviction)
    deleted_keys: AHashSet<String>,
    /// Sequence number, incremented per snapshot
    snapshot_seq: u64,
}
```

**How it works:**

1. On every PUSH/SET/MSET that modifies an entity: `dirty_tracker.dirty_keys.insert(key)`
2. On every eviction: `dirty_tracker.deleted_keys.insert(key)`
3. On snapshot timer:
   - If `dirty_keys.len() < total_entities * 0.5`: incremental snapshot
     - Clone only dirty entities + serialize as delta
     - Periodically (every N snapshots), do a full snapshot to compact
   - Else: full snapshot (current behavior)
4. On load: apply base snapshot + deltas in order

**Snapshot file format:**

```
[1 byte: version]
[1 byte: snapshot_type (0=full, 1=delta)]
[8 bytes: snapshot_seq]
[postcard-encoded payload]
```

For delta snapshots, the payload is:
```rust
struct DeltaSnapshot {
    base_seq: u64,
    upserted_entities: Vec<(String, SerializableEntityState)>,
    deleted_keys: Vec<String>,
    pipelines: Vec<SerializablePipeline>,  // always include full pipeline defs
}
```

**Recovery:** On startup, find the latest full snapshot, then apply deltas in sequence order. If any delta is missing or corrupt, fall back to the last valid full snapshot.

**Confidence:** MEDIUM. The dirty-key tracking is straightforward. The concern is the snapshot recovery complexity (base + deltas). A simpler v1.1 approach: just track dirty keys to reduce the clone size, but still write a full snapshot each time. This eliminates the delta recovery complexity while still solving the memory spike problem.

**Recommendation:** Start with "clone only dirty keys + always write full snapshot" as v1.1 scope. True delta snapshots with recovery chaining is v1.2 optimization.

### 8. Debug UI (`src/server/debug_ui.rs`)

**Design: Embedded SPA served from the existing axum HTTP server.**

The debug UI is a single-page application (HTML + JS + CSS) embedded in the Rust binary at compile time using `rust-embed`. It connects to the existing HTTP API endpoints and optionally a WebSocket for live updates.

```rust
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "ui/dist/"]
struct DebugAssets;

/// Serve embedded UI assets
async fn serve_ui(path: Path<String>) -> impl IntoResponse {
    let path = if path.0.is_empty() { "index.html" } else { &path.0 };
    match DebugAssets::get(path) {
        Some(file) => {
            let mime = mime_guess::from_path(path).first_or_octet_stream();
            (StatusCode::OK, [(header::CONTENT_TYPE, mime.as_ref())], file.data).into_response()
        }
        None => {
            // SPA fallback: serve index.html for client-side routing
            let index = DebugAssets::get("index.html").unwrap();
            (StatusCode::OK, [(header::CONTENT_TYPE, "text/html")], index.data).into_response()
        }
    }
}
```

**Routes added to axum router:**

```rust
.route("/ui", get(|| async { Redirect::permanent("/ui/") }))
.route("/ui/", get(serve_ui))
.route("/ui/*path", get(serve_ui))
.route("/ws/live", get(websocket_handler))  // optional: live data feed
```

**UI tech stack:** Vanilla HTML + JS (no build step). The UI is a debug/observability tool, not a production application. Keeping it simple (no React, no build pipeline) means:
- Zero additional build dependencies
- Files go in `ui/dist/` in the repo
- `rust-embed` bundles them into the binary
- In dev mode (`debug-embed` feature disabled), files are served from disk for hot-reload

**UI pages:**
- **Dashboard:** Stream list, entity count, events/sec, memory usage (polls `/metrics` and `/debug/memory`)
- **Stream Inspector:** Select a stream, see its definition, operator types, feature list
- **Entity Inspector:** Enter a key, see all features (polls `/debug/key/:key`)
- **Backfill Status:** Progress of any active backfill tasks (polls `/debug/backfill`)
- **Event Log:** Show recent events for a stream (if history enabled)

**WebSocket for live updates (optional):** A WebSocket endpoint that pushes metric updates every second. This avoids polling overhead in the UI. The WebSocket handler reads from `Arc<Mutex<AppState>>` on a timer -- same pattern as the HTTP endpoints.

**Confidence:** HIGH. `rust-embed` is mature (v8.11, January 2026). Axum has first-class static file serving support. The UI itself is simple HTML/JS that consumes existing HTTP API endpoints.

### 9. Per-Dataset TTL (`src/state/eviction.rs` modification)

**Design: TTL configured per stream, not just global.**

Currently, TTL = `ttl_multiplier * max_window_duration` (global). v1.1 allows per-stream TTL:

```python
@st.stream(key="user_id", ttl="7d")
class Transactions:
    ...
```

The eviction timer already iterates all entities. The change: instead of one global TTL, each entity's TTL is determined by which streams have state for that entity. The entity's TTL = max of all stream TTLs that have operators for that key.

**Implementation detail:** `EntityState` already tracks `last_event_at`. We need to also track which streams contributed operators. This is already implicit in `live_operators` names -- but to make TTL per-stream efficient, store a `stream_name` field alongside each operator in `live_operators`:

```rust
// Current:  Vec<(String, OperatorState)>  -- (feature_name, state)
// Proposed: Vec<(String, String, OperatorState)>  -- (feature_name, stream_name, state)
```

Or simpler: store per-stream `last_event_at` instead of per-entity. This requires a small structural change to `EntityState`.

**Confidence:** HIGH. This is a small modification to the eviction logic. The main decision is the storage model for per-stream timestamps.

## Data Flow Changes

### v1.0 PUSH Flow (current)
```
Client -> TCP -> parse_command -> lock(AppState) ->
  engine.push(stream, event, store) ->
    extract key -> get_or_create entity -> push operators -> eval derives ->
  fan_out to secondary streams ->
  collect features -> unlock -> write response
```

### v1.1 PUSH Flow (proposed)
```
Client -> TCP -> parse_command -> lock(AppState) ->
  IF keyless stream:
    event_log.append(stream, event)  [buffered write, ~200ns]
    dag.get_downstream(stream) -> for each keyed stream:
      engine.push(keyed_stream, event, store)
    unlock -> write response (empty features)
  ELSE (keyed stream):
    engine.push(stream, event, store)
      extract key -> get_or_create entity -> push operators -> eval derives
    IF stream.history:
      event_log.append(stream, event)  [buffered write, ~200ns]
    dag.get_downstream(stream) -> for each dependent:
      engine.push(dependent, event, store)  [replaces fan_out_targets()]
    unlock -> write response (features)
```

**Key difference:** Fan-out is replaced by explicit DAG traversal. The event log write is interleaved with the state update. The response includes only the primary stream's features (same as v1.0).

### v1.1 GET Flow (unchanged for basic case)
```
Client -> TCP -> parse_command -> lock(AppState) ->
  store.get_all_features(key) ->
  engine.eval_derives(key, features) ->
  engine.eval_views(key, features, store) ->
  unlock -> write response
```

No changes to GET flow. Views are still evaluated lazily on GET.

### v1.1 Schema Evolution Flow (new)
```
REGISTER arrives with updated definition ->
  schema_registry.diff(old_def, new_def) -> Vec<SchemaChange> ->
  for FeatureRemoved: iterate entities, remove operator (chunked + yield)
  for FeatureChanged: iterate entities, reset operator (chunked + yield)
  for FeatureAdded:
    if stream.history && backfill requested:
      spawn BackfillTask
    else:
      operator created lazily on next PUSH (existing behavior)
  update PipelineEngine with new definition
```

### v1.1 Backfill Flow (new)
```
BackfillTask starts ->
  read log segments for stream in timestamp order ->
  for each LogEntry:
    deserialize event JSON
    extract entity key
    find/create new operator in entity.live_operators
    operator.push(event, historical_timestamp)
    every 1024 events: yield_now()
  mark BackfillTask as Complete
```

## File Structure Changes

```
tally/
├── src/
│   ├── main.rs              # + EventLog init, fsync timer, log compaction
│   ├── types.rs             # (unchanged)
│   ├── error.rs             # + EventLog, Backfill, Schema error variants
│   ├── engine/
│   │   ├── mod.rs
│   │   ├── pipeline.rs      # + KeylessStreamDef, schema evolution hooks
│   │   ├── dag.rs           # NEW: DependencyGraph, topological sort
│   │   ├── schema.rs        # NEW: SchemaRegistry, diff logic
│   │   ├── backfill.rs      # NEW: BackfillTask, replay engine
│   │   ├── operators.rs     # (unchanged)
│   │   ├── window.rs        # (unchanged)
│   │   ├── expression.rs    # (unchanged)
│   │   ├── hll.rs           # (unchanged)
│   ├── server/
│   │   ├── mod.rs
│   │   ├── tcp.rs           # + MGET handler, event log on PUSH
│   │   ├── protocol.rs      # + MGET opcode 0x06
│   │   ├── http.rs          # + debug UI routes, backfill endpoints
│   │   ├── debug_ui.rs      # NEW: rust-embed asset serving
│   ├── state/
│   │   ├── mod.rs
│   │   ├── store.rs         # + get_many_features(), per-dataset TTL
│   │   ├── snapshot.rs      # + dirty-key tracking, incremental serialize
│   │   ├── eviction.rs      # + per-dataset TTL
│   │   ├── event_log.rs     # NEW: append-only log, segments, compaction
├── ui/
│   └── dist/                # NEW: debug UI static assets
│       ├── index.html
│       ├── app.js
│       └── style.css
```

## Suggested Build Order

The build order is driven by dependencies between features. Each phase should be independently testable.

### Phase 1: Foundation (MGET + Schema Evolution)
**Why first:** MGET is trivial and independently useful. Schema evolution is the prerequisite for backfill (you need to detect FeatureAdded to know when to backfill).

1. **MGET command** (protocol.rs, tcp.rs) -- ~1 hour
   - Add OP_MGET opcode
   - Parse N keys from payload
   - Call get_features() for each key
   - Tests: unit + integration
   - *No dependencies on other v1.1 features*

2. **Schema evolution** (schema.rs, pipeline.rs, store.rs) -- ~3 hours
   - SchemaRegistry: diff old vs new definitions
   - Feature add/remove/change detection
   - Entity iteration with cooperative yielding for removals
   - Tests: unit tests for diff logic, integration tests for re-register
   - *Depends on: nothing new, uses existing PipelineEngine*

3. **Per-dataset TTL** (eviction.rs, store.rs, pipeline.rs) -- ~2 hours
   - Add TTL field to StreamDefinition
   - Modify eviction to use per-stream TTL
   - Track which stream contributed each operator
   - Tests: eviction with mixed TTLs
   - *Depends on: nothing new*

### Phase 2: DAG Execution
**Why second:** The DAG replaces the implicit fan-out logic. It must be in place before keyless streams can work (keyless streams depend on the DAG to know which keyed streams to feed).

4. **Dependency graph** (dag.rs, pipeline.rs) -- ~3 hours
   - DependencyGraph struct with topological sort
   - Cycle detection at registration time
   - Integration into PipelineEngine.register()
   - Replace fan_out_targets() with DAG traversal in PUSH handler
   - Tests: DAG construction, cycle detection, execution order
   - *Depends on: nothing new, replaces existing fan-out*

5. **Keyless streams** (pipeline.rs, tcp.rs) -- ~2 hours
   - KeylessStreamDef type
   - PUSH to keyless stream: validate, then DAG fan-out
   - REGISTER for keyless streams
   - Tests: keyless -> keyed flow
   - *Depends on: Phase 2 step 4 (DAG)*

### Phase 3: Event Log
**Why third:** The event log is the foundation for backfill. It depends on keyless streams (keyless streams always log) and keyed streams with `history=True`.

6. **Event log core** (event_log.rs) -- ~4 hours
   - LogEntry format, segment management
   - BufWriter append, segment rotation
   - Fsync background timer
   - Read iterator for replay
   - Tests: write/read round-trip, segment rotation, corrupt segment handling
   - *Depends on: nothing, but integrates with PUSH in step 7*

7. **Event log integration** (tcp.rs, main.rs, pipeline.rs) -- ~2 hours
   - Add `history: bool` to StreamDefinition
   - Event log write on PUSH path (for streams with history=True)
   - EventLog initialization in main.rs
   - Fsync timer in main.rs
   - Tests: E2E push with logging, verify log contents
   - *Depends on: Phase 3 step 6 (event log core)*

8. **Log compaction** (event_log.rs) -- ~2 hours
   - Walk current state, write minimal reproduction
   - Segment TTL-based deletion
   - Background compaction timer
   - Tests: compaction preserves replay correctness
   - *Depends on: Phase 3 step 6, step 7*

### Phase 4: Backfill
**Why fourth:** Backfill requires both the event log (to read from) and schema evolution (to trigger). It is the most complex new feature.

9. **Backfill engine** (backfill.rs) -- ~4 hours
   - BackfillTask: read segments, replay events, cooperative yielding
   - Integration with schema evolution (FeatureAdded triggers backfill)
   - Progress tracking via Metrics
   - Tests: backfill populates new feature correctly, concurrent live pushes
   - *Depends on: Phase 1 step 2 (schema), Phase 3 steps 6-7 (event log)*

### Phase 5: Incremental Snapshots
**Why fifth:** This is an optimization. The existing full-snapshot approach works. Incremental snapshots reduce memory spikes.

10. **Dirty-key tracking** (snapshot.rs, store.rs, tcp.rs) -- ~3 hours
    - DirtyTracker: mark keys on PUSH/SET/MSET/eviction
    - Clone only dirty keys for snapshot
    - Still write full snapshot (no delta recovery needed)
    - Tests: dirty tracking accuracy, snapshot size reduction
    - *Depends on: nothing new, modifies existing snapshot flow*

### Phase 6: Debug UI
**Why last:** The debug UI is a consumer of all other features. It needs the metrics, backfill status, and event log endpoints to be useful.

11. **Debug UI** (debug_ui.rs, http.rs, ui/dist/) -- ~4 hours
    - Build simple HTML/JS dashboard
    - rust-embed integration for compile-time bundling
    - New HTTP endpoints: `/debug/backfill`, `/debug/event-log`
    - WebSocket for live metrics (optional)
    - Tests: HTTP 200 on UI routes, asset serving
    - *Depends on: all other features (for full utility)*

### Python SDK Updates (parallel with any phase)

12. **SDK updates** (python/) -- ~2 hours
    - `history=True` parameter on `@st.stream`
    - `ttl="7d"` parameter on `@st.stream`
    - `app.mget(["key1", "key2", ...])` method
    - Keyless stream support: `@st.stream()` (no key parameter)
    - *Can be done in parallel with server-side work*

## Dependency Graph for Build Order

```
Phase 1: MGET ──────────────────────────────────────┐
Phase 1: Schema Evolution ──────────────────────────┤
Phase 1: Per-dataset TTL ──────────────────────────┤
                                                    │
Phase 2: DAG ──────────────────────────────────────┤
Phase 2: Keyless Streams (needs DAG) ──────────────┤
                                                    │
Phase 3: Event Log Core ──────────────────────────┤
Phase 3: Event Log Integration (needs log core) ──┤
Phase 3: Log Compaction (needs log integration) ──┤
                                                    │
Phase 4: Backfill (needs schema + event log) ──────┤
                                                    │
Phase 5: Incremental Snapshots ────────────────────┤
                                                    │
Phase 6: Debug UI (needs all above) ───────────────┘
```

## Integration Points Summary

| New Feature | Touches (existing files) | New Files |
|-------------|-------------------------|-----------|
| MGET | protocol.rs, tcp.rs | -- |
| Schema Evolution | pipeline.rs, store.rs | schema.rs |
| Per-dataset TTL | eviction.rs, store.rs, pipeline.rs | -- |
| DAG Execution | pipeline.rs, tcp.rs | dag.rs |
| Keyless Streams | pipeline.rs, tcp.rs, protocol.rs | -- |
| Event Log | tcp.rs, main.rs, pipeline.rs | event_log.rs |
| Log Compaction | event_log.rs | -- |
| Backfill | pipeline.rs | backfill.rs |
| Incremental Snapshots | snapshot.rs, store.rs, tcp.rs | -- |
| Debug UI | http.rs | debug_ui.rs, ui/dist/* |

## Risk Assessment

| Feature | Risk | Mitigation |
|---------|------|------------|
| Event log latency impact | LOW | BufWriter + no hot-path fsync. ~200ns per write. |
| Backfill + live push interaction | MEDIUM | Historical timestamps + forward-only ring buffer. Needs thorough testing. |
| Schema evolution entity iteration | LOW | Cooperative yielding (proven pattern from MSET). |
| DAG replacing fan-out | LOW | Same semantics, explicit instead of implicit. |
| Incremental snapshot recovery | MEDIUM | Mitigated by starting with "clone dirty, write full" approach. |
| Debug UI build complexity | LOW | Vanilla HTML/JS, no build pipeline. rust-embed is mature. |
| Event log disk usage | MEDIUM | Per-stream opt-in + compaction + TTL. Document disk math clearly. |

## Key Architectural Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Event log fsync policy | Everysec (like Redis) | <100us PUSH budget. No hot-path fsync. 1s data loss acceptable. |
| Event log format | Custom binary (not JSON lines) | Compact, fast to write, fast to read sequentially. |
| DAG library | Inline implementation | Graph is tiny (<20 nodes). No crate dependency needed. |
| Debug UI framework | Vanilla HTML/JS | Zero build deps. Debug tool, not production UI. |
| Debug UI embedding | rust-embed | Mature crate, compile-time bundling, zero runtime deps. |
| Incremental snapshots v1.1 | Dirty-key tracking, full write | Simpler than delta recovery. Solves the memory spike problem. |
| Keyless stream storage | Event log only, no in-memory state | They have no operators. Storing them in StateStore would waste memory. |
| Schema evolution backfill trigger | Automatic on FeatureAdded + history | User doesn't manually request backfill. It just works. |

## Sources

- [Redis Persistence Documentation](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/) -- AOF design, fsync policies, rewrite strategy
- [Redis 7.0 Multi-Part AOF Design](https://www.alibabacloud.com/blog/design-and-implementation-of-redis-7-0-multi-part-aof_599199) -- Segmented AOF architecture
- [rust-embed crate (v8.11)](https://crates.io/crates/rust-embed) -- Compile-time static asset embedding
- [Axum static file server example](https://github.com/tokio-rs/axum/blob/main/examples/static-file-server/src/main.rs) -- Serving static files with axum
- [Tokio file I/O documentation](https://docs.rs/tokio/latest/tokio/fs/index.html) -- Async file operations (delegates to threadpool)
- [daggy crate](https://github.com/mitchmindtree/daggy) -- Reference for DAG API design (not used as dependency)

---
*Architecture research for: v1.1 Composable Pipeline & Event Log integration into existing Tally server*
*Researched: 2026-04-09*
