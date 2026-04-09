# Phase 4: Persistence and Operational Readiness - Research

**Researched:** 2026-04-09
**Domain:** Rust snapshot persistence, TTL eviction, HTTP management API (axum), Prometheus metrics
**Confidence:** HIGH

## Summary

Phase 4 transforms Tally from a volatile in-memory server into a production-ready system that survives restarts, reclaims memory, and exposes observability endpoints. The three workstreams are: (1) snapshot persistence using postcard serialization with clone-then-spawn_blocking, (2) TTL-based key eviction via periodic sweep, and (3) HTTP management API expansion with pipeline CRUD, debug, and Prometheus metrics.

The primary technical challenge is serializing `Box<dyn Operator>` trait objects, which requires introducing an `OperatorState` enum wrapper. All operator types (CountOp, SumOp, AvgOp) already derive Serialize/Deserialize, so the enum wrapping is mechanical. The `postcard` crate (v1.1.3, already in Cargo.toml) provides `to_stdvec`/`from_bytes` with the `use-std` feature already enabled. The `tokio::task::spawn_blocking` function works with `current_thread` runtime by using a separate blocking thread pool, confirmed via official docs.

**Primary recommendation:** Implement in three sequential waves -- (1) OperatorState enum + snapshot save/load with tests, (2) TTL eviction with periodic timer, (3) HTTP endpoints. Each wave is independently testable.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Enum wrapper around operator state (CountState, SumState, etc.) serialized with postcard -- per locked decision to use postcard (not bincode, RUSTSEC-2025-0141)
- Periodic timer (default 30s) via tokio::time::interval -- matches CLAUDE.md spec
- Clone state then spawn_blocking for serialization -- simple, proven pattern (Redis RDB model)
- Single file with atomic rename (write temp, rename) -- crash-safe, simple
- Periodic sweep via tokio timer (every 60s) -- low overhead, predictable
- Default TTL: 2x largest window per entity -- matches CLAUDE.md spec
- Evicted keys re-initialize fresh on next event -- matches CLAUDE.md: "state is re-initialized fresh"
- Server-level config flag (--ttl-multiplier, default 2) -- simple, single knob
- Prometheus text format (text/plain) for /metrics -- industry standard
- Core operational metrics: keys_total, events_total, push_latency_seconds, snapshot_duration_seconds, memory_bytes
- Full operator internals via /debug/key/:key (ring buffer state, HLL sketch) -- matches CLAUDE.md spec
- Pipeline CRUD: GET/POST/DELETE /pipelines + GET /pipelines/:name -- matches CLAUDE.md spec

### Claude's Discretion
- Snapshot format versioning strategy (version byte per key or header version)
- Cooperative yielding granularity for MSET during snapshot
- Exact Prometheus metric naming conventions

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| PERS-01 | Periodic snapshot serialization of full state to local file (default every 30s) | postcard to_stdvec + tokio::time::interval + spawn_blocking pattern; atomic file rename via std::fs::rename |
| PERS-02 | Snapshot uses postcard + serde with versioned format (version byte per snapshot) | postcard 1.1.3 already in Cargo.toml with use-std feature; OperatorState enum for trait object serialization |
| PERS-03 | Server loads latest snapshot on startup for crash recovery | postcard::from_bytes for deserialization; version check on load, discard incompatible snapshots |
| PERS-04 | Snapshot write uses cooperative yielding to avoid blocking the event loop | Clone state under lock, spawn_blocking for serialization (separate thread pool, does not block current_thread runtime) |
| PERS-05 | TTL-based key eviction removes inactive keys (default: 2x largest window) | EntityState.last_event_at already exists; periodic sweep via tokio::time::interval(60s) |
| SRV-08 | HTTP management API serves health, metrics, debug, and pipeline CRUD on separate port | Existing axum router + SharedState pattern; add routes for /pipelines, /metrics, /debug/key/:key, /debug/memory, /snapshot |
</phase_requirements>

## Standard Stack

### Core (already in Cargo.toml)
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| postcard | 1.1.3 | Binary serialization for snapshots | Locked decision; safe alternative to bincode (RUSTSEC-2025-0141); compact varint encoding [VERIFIED: cargo tree output] |
| serde | 1.0.228 | Derive Serialize/Deserialize on state types | Already used throughout codebase [VERIFIED: Cargo.toml] |
| tokio | 1.51.1 | Async runtime, spawn_blocking, time::interval | Already used; spawn_blocking works with current_thread flavor [CITED: docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html] |
| axum | 0.8.8 | HTTP management API | Already used for /health endpoint [VERIFIED: Cargo.toml] |
| ahash | 0.8.12 | AHashMap for state store | Already used; locked decision [VERIFIED: Cargo.toml] |

### Supporting (may need to add)
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| tempfile | 3.x | Atomic file writes via NamedTempFile::persist | Safer than manual temp-file-then-rename; handles same-filesystem constraint [ASSUMED] |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| tempfile crate | Manual std::fs write + std::fs::rename | tempfile handles edge cases (cleanup on panic, same-filesystem guarantee), but adds dependency; manual approach is 5 lines of code and sufficient for this use case |

**Recommendation:** Use manual std::fs::write to temp file + std::fs::rename. The operation is simple enough (write bytes, rename) that tempfile is unnecessary overhead. The temp file should be in the same directory as the target to guarantee same-filesystem rename atomicity. [ASSUMED]

**Installation:** No new dependencies needed. All required crates are already in Cargo.toml.

## Architecture Patterns

### Recommended Project Structure Changes
```
src/
├── state/
│   ├── mod.rs           # Add: pub mod snapshot; pub mod eviction;
│   ├── store.rs          # Add: list_entities(), remove_entity(), memory_estimate()
│   ├── snapshot.rs       # NEW: SnapshotState, save/load, OperatorState enum
│   └── eviction.rs       # NEW: TTL eviction sweep logic
├── engine/
│   ├── pipeline.rs       # Add: list_streams(), remove_stream(), stream serialization
│   └── operators.rs      # Add: OperatorState enum, to_state()/from_state() on operators
├── server/
│   ├── http.rs           # Expand: pipeline CRUD, metrics, debug, snapshot endpoints
│   └── metrics.rs        # NEW (optional): Metrics collection struct, Prometheus formatting
└── main.rs               # Add: snapshot timer, eviction timer, snapshot recovery on startup
```

### Pattern 1: OperatorState Enum Wrapper for Serialization
**What:** Replace `Box<dyn Operator>` with a serializable enum that wraps each concrete operator.
**When to use:** Serializing/deserializing EntityState for snapshots.
**Why needed:** Trait objects (`Box<dyn Operator>`) are not serializable. The existing comment on store.rs line 26 explicitly anticipates this: "Not serializable via serde (trait objects) -- Phase 4 will use enum wrapper."

```rust
// Source: Derived from codebase analysis + CONTEXT.md decision
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OperatorState {
    Count(CountOp),
    Sum(SumOp),
    Avg(AvgOp),
    // Future phases will add: Min, Max, DistinctCount, Last
}

impl OperatorState {
    pub fn push(&mut self, event: &serde_json::Value, now: SystemTime) -> Result<(), TallyError> {
        match self {
            Self::Count(op) => op.push(event, now),
            Self::Sum(op) => op.push(event, now),
            Self::Avg(op) => op.push(event, now),
        }
    }

    pub fn read(&mut self, now: SystemTime) -> FeatureValue {
        match self {
            Self::Count(op) => op.read(now),
            Self::Sum(op) => op.read(now),
            Self::Avg(op) => op.read(now),
        }
    }
}
```

**Impact:** This changes `EntityState.live_operators` from `Vec<(String, Box<dyn Operator>)>` to `Vec<(String, OperatorState)>`. All call sites in pipeline.rs and store.rs that call `.push()` and `.read()` need updating. The Operator trait remains for conceptual documentation but is no longer used at runtime. [VERIFIED: codebase analysis]

### Pattern 2: Clone-then-Spawn-Blocking Snapshot
**What:** Clone the full state under the mutex lock, then serialize on a blocking thread.
**When to use:** Periodic snapshot writes (every 30s).
**Why:** Minimizes lock hold time. The clone is O(state_size) but fast (memcpy). Serialization (potentially slow) happens outside the lock on a separate thread pool.

```rust
// Source: Derived from CONTEXT.md decision + tokio docs
async fn snapshot_tick(state: SharedState, path: &Path) {
    // Clone under lock -- brief lock hold
    let snapshot_data = {
        let app = state.lock().unwrap_or_else(|e| e.into_inner());
        app.store.clone_for_snapshot() // Returns SnapshotState
    };

    // Serialize on blocking thread pool (does NOT block current_thread runtime)
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        let bytes = postcard::to_stdvec(&snapshot_data)
            .expect("snapshot serialization failed");
        // Write to temp file, then atomic rename
        let tmp_path = path.with_extension("tmp");
        std::fs::write(&tmp_path, &bytes)?;
        std::fs::rename(&tmp_path, &path)?;
        Ok::<_, std::io::Error>(bytes.len())
    }).await.unwrap().unwrap();
}
```

**Critical note:** `tokio::task::spawn_blocking` spawns onto a dedicated blocking thread pool even with `current_thread` runtime. This is confirmed by official tokio docs. The blocking thread pool can spawn up to ~512 threads. The `.await` on the JoinHandle yields control back to the event loop while serialization runs. [CITED: docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html]

### Pattern 3: Snapshot Format Versioning (Claude's Discretion)
**What:** Prepend a version byte to the snapshot file.
**Recommendation:** Use a single header version byte (not per-key versioning).

```rust
const SNAPSHOT_FORMAT_VERSION: u8 = 1;

fn save_snapshot(data: &SnapshotState) -> Vec<u8> {
    let mut buf = vec![SNAPSHOT_FORMAT_VERSION];
    buf.extend_from_slice(&postcard::to_stdvec(data).unwrap());
    buf
}

fn load_snapshot(bytes: &[u8]) -> Option<SnapshotState> {
    if bytes.is_empty() {
        return None;
    }
    let version = bytes[0];
    if version != SNAPSHOT_FORMAT_VERSION {
        // Incompatible version -- start from empty state (success criterion #5)
        eprintln!("Snapshot version mismatch: found {}, expected {}. Starting fresh.",
                  version, SNAPSHOT_FORMAT_VERSION);
        return None;
    }
    postcard::from_bytes(&bytes[1..]).ok()
}
```

**Rationale:** Header-level versioning is simpler than per-key versioning. When the format changes (new operator types in Phase 5), bump SNAPSHOT_FORMAT_VERSION. Old snapshots are discarded cleanly. Per-key versioning adds complexity with no benefit until incremental snapshots (v2 OPT-01). [ASSUMED]

### Pattern 4: TTL Eviction Sweep
**What:** Periodic timer iterates all entities, removes those whose `last_event_at` is older than their TTL.
**When:** Every 60 seconds (CONTEXT.md decision).

```rust
// Source: Derived from CONTEXT.md decisions
fn evict_expired_keys(store: &mut StateStore, engine: &PipelineEngine, now: SystemTime) -> usize {
    let ttl_multiplier = 2; // Default: 2x largest window
    let max_window = engine.max_window_duration(); // Needs to be added
    let ttl = max_window * ttl_multiplier;

    store.remove_expired_entities(now, ttl)
}
```

**Note:** The TTL is 2x the largest window *across all registered streams*. PipelineEngine needs a `max_window_duration()` method that scans all registered feature definitions. StateStore needs a `remove_expired_entities()` method that iterates and retains only non-expired entries. [VERIFIED: codebase analysis shows last_event_at already tracked]

### Pattern 5: Prometheus Text Format (Manual)
**What:** Hand-format Prometheus metrics text. No external metrics crate needed.
**Why:** Only ~5 metrics (CONTEXT.md decision). A full metrics framework is overkill.

```rust
// Source: Prometheus exposition format docs
fn format_metrics(metrics: &Metrics) -> String {
    let mut buf = String::new();
    buf.push_str("# HELP tally_keys_total Number of entity keys in memory\n");
    buf.push_str("# TYPE tally_keys_total gauge\n");
    buf.push_str(&format!("tally_keys_total {}\n", metrics.keys_total));

    buf.push_str("# HELP tally_events_total Total events processed\n");
    buf.push_str("# TYPE tally_events_total counter\n");
    buf.push_str(&format!("tally_events_total {}\n", metrics.events_total));

    // ... more metrics
    buf
}
```

Content-Type for the response: `text/plain; version=0.0.4` [CITED: prometheus.io/docs/instrumenting/exposition_formats/]

### Anti-Patterns to Avoid
- **Holding the mutex during serialization:** The serialization can take hundreds of milliseconds for large state. Lock must be released before serialization starts. Clone first, serialize outside lock.
- **Using tokio::spawn (not spawn_blocking) for serialization:** Serialization is CPU-bound. Using regular `tokio::spawn` on a `current_thread` runtime would block the entire event loop during serialization. `spawn_blocking` moves it to a separate thread pool.
- **Writing snapshot directly to target path:** If the process crashes mid-write, the snapshot file is corrupted. Always write to temp file first, then atomic rename.
- **Per-key TTL tracking with separate timers:** Thousands of individual timers would be expensive. A single periodic sweep is much simpler and sufficient for the eviction granularity we need.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Binary serialization | Custom byte packing | postcard + serde derive | Varint encoding, forward compatibility, battle-tested. Locked decision. |
| Atomic file writes | Low-level file descriptor manipulation | std::fs::write + std::fs::rename | Atomic rename on same filesystem is sufficient for our single-file snapshot model |
| HTTP routing/parsing | Manual HTTP parser | axum (already in use) | Already integrated; Router::merge or nest for new routes |
| Prometheus metrics format | Full metrics framework (prometheus crate) | Manual string formatting | Only 5 metrics; full framework adds unnecessary complexity |

## Common Pitfalls

### Pitfall 1: OperatorState Enum Must Stay In Sync with create_operator()
**What goes wrong:** Adding a new operator type in Phase 5 (min, max, distinct_count, last) requires updating OperatorState enum, the Operator -> OperatorState conversion, the OperatorState -> Operator conversion, and the snapshot version.
**Why it happens:** The enum wrapper is a parallel representation of the concrete operator types.
**How to avoid:** Add a compile-time check: if the `create_operator` match has more arms than `OperatorState`, compilation should fail. At minimum, add a comment linking the two locations. Bump SNAPSHOT_FORMAT_VERSION when adding new variants.
**Warning signs:** Panic on snapshot load after adding new operator type.

### Pitfall 2: Clone Cost of Full State
**What goes wrong:** Cloning the entire state for snapshot creates a peak memory spike of ~2x. With 1M keys, this could be significant.
**Why it happens:** Clone duplicates all ring buffer Vecs, all AHashMaps, all Strings.
**How to avoid:** This is the accepted tradeoff per CONTEXT.md decision ("clone+spawn_blocking chosen -- simpler, acceptable for v1"). Document the memory behavior. Future v2 (OPT-01) can use incremental snapshots.
**Warning signs:** OOM under heavy load with many keys.

### Pitfall 3: SystemTime Serialization with Postcard
**What goes wrong:** `SystemTime` serialization may not be portable across platforms or may have unexpected precision.
**Why it happens:** `SystemTime` is platform-specific. Serde's default SystemTime serialization uses `duration_since(UNIX_EPOCH)` which gives seconds + nanoseconds.
**How to avoid:** Verify postcard round-trips SystemTime correctly in unit tests. If issues arise, serialize as `u64` (seconds since epoch) instead.
**Warning signs:** Timestamp drift or deserialization errors after snapshot load.

### Pitfall 4: Snapshot File Left as .tmp on Crash
**What goes wrong:** If the process crashes between writing the temp file and renaming, a `.tmp` file is left on disk with no valid snapshot.
**How to avoid:** On startup, check for both the snapshot file and the `.tmp` file. If only `.tmp` exists, it was an incomplete write -- ignore it. If both exist, the rename failed -- use the snapshot file (it's the previous valid one).
**Warning signs:** Server starts with empty state when a `.tmp` file exists.

### Pitfall 5: Eviction Race with Concurrent PUSH
**What goes wrong:** A PUSH arrives for a key that was just marked for eviction.
**Why it happens:** In the single-threaded model this cannot actually happen (mutex serializes access), but the logic must still handle the case where `get_or_create_entity` is called for an evicted key.
**How to avoid:** Eviction removes the key entirely. `get_or_create_entity` already handles missing keys by creating fresh state. No special handling needed.
**Warning signs:** None -- the existing pattern handles this correctly.

### Pitfall 6: Pipeline Definitions Not Persisted in Snapshots
**What goes wrong:** After restart with snapshot recovery, entity state is restored but pipeline definitions (stream schemas) are lost. PUSH/GET commands fail because no streams are registered.
**Why it happens:** Snapshots might only serialize StateStore, not PipelineEngine.
**How to avoid:** The snapshot must include both `StateStore` state AND `PipelineEngine` stream definitions. Alternatively, require clients to re-register pipelines after restart (simpler but worse UX). Recommendation: serialize pipeline definitions in the snapshot.
**Warning signs:** "unknown stream" errors after restart.

## Code Examples

### SnapshotState Struct (Top-level Serializable State)

```rust
// Source: Derived from codebase analysis
use serde::{Serialize, Deserialize};

/// The complete serializable state for snapshot persistence.
/// Includes both entity state and pipeline definitions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotState {
    pub entities: Vec<(String, SerializableEntityState)>,
    pub pipelines: Vec<SerializablePipeline>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableEntityState {
    pub live_operators: Vec<(String, OperatorState)>,
    pub static_features: Vec<(String, StaticFeature)>,
    pub last_event_at: Option<SystemTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializablePipeline {
    pub name: String,
    pub key_field: String,
    pub features: Vec<(String, FeatureDef)>,
}
```

Note: AHashMap is not directly serializable with postcard. Convert to `Vec<(K, V)>` for serialization. [VERIFIED: AHashMap does not implement Serialize by default; confirmed by examining the type definition in store.rs]

### HTTP Endpoint Patterns (axum)

```rust
// Source: Existing http.rs pattern + axum docs
use axum::{
    routing::{get, post, delete},
    extract::{Path, State},
    Json, Router,
    http::StatusCode,
    response::IntoResponse,
};

async fn list_pipelines(
    State(state): State<SharedState>,
) -> Json<serde_json::Value> {
    let app = state.lock().unwrap_or_else(|e| e.into_inner());
    let names: Vec<&str> = app.engine.list_streams()
        .map(|s| s.name.as_str())
        .collect();
    Json(serde_json::json!({"pipelines": names}))
}

async fn debug_key(
    State(state): State<SharedState>,
    Path(key): Path<String>,
) -> Json<serde_json::Value> {
    let mut app = state.lock().unwrap_or_else(|e| e.into_inner());
    // Return full operator internals
    let debug_info = app.store.debug_entity(&key, SystemTime::now());
    Json(debug_info)
}

async fn metrics_endpoint(
    State(state): State<SharedState>,
) -> impl IntoResponse {
    let app = state.lock().unwrap_or_else(|e| e.into_inner());
    let body = format_prometheus_metrics(&app);
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        body,
    )
}
```

**Important:** The current HTTP server functions take `_state: SharedState` but don't wire it into axum's State extractor. Phase 4 needs to change `run_http_server` and `run_http_server_with_listener` to pass SharedState into the Router via `.with_state()`. [VERIFIED: http.rs line 17 shows Router without state]

### Main.rs Snapshot Timer Pattern

```rust
// Source: Derived from tokio docs + CONTEXT.md decisions
// In main.rs, after state creation:
let snapshot_path = PathBuf::from("tally.snapshot");

// Load snapshot on startup (PERS-03)
if snapshot_path.exists() {
    match load_snapshot(&snapshot_path) {
        Some(snapshot_state) => {
            let mut app = state.lock().unwrap();
            app.restore_from_snapshot(snapshot_state);
            eprintln!("Loaded snapshot from {}", snapshot_path.display());
        }
        None => {
            eprintln!("Snapshot incompatible or corrupt, starting fresh");
        }
    }
}

// Periodic snapshot timer (PERS-01)
let snap_state = state.clone();
let snap_path = snapshot_path.clone();
tokio::spawn(async move {
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    loop {
        interval.tick().await;
        snapshot_tick(&snap_state, &snap_path).await;
    }
});

// Periodic eviction timer (PERS-05)
let evict_state = state.clone();
tokio::spawn(async move {
    let mut interval = tokio::time::interval(Duration::from_secs(60));
    loop {
        interval.tick().await;
        let now = SystemTime::now();
        let mut app = evict_state.lock().unwrap_or_else(|e| e.into_inner());
        let evicted = evict_expired_keys(&mut app.store, &app.engine, now);
        if evicted > 0 {
            eprintln!("Evicted {} expired keys", evicted);
        }
    }
});
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| bincode for serialization | postcard | RUSTSEC-2025-0141 | bincode is unmaintained with security advisory; postcard is the safe alternative [VERIFIED: STATE.md decision] |
| Full metrics crate (prometheus-rs) | Manual text formatting | Project decision | For <10 metrics, manual formatting is simpler and avoids dependency |

**Deprecated/outdated:**
- bincode: RUSTSEC-2025-0141 advisory, unmaintained. Do not use. [VERIFIED: STATE.md]

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | tempfile crate unnecessary; manual write+rename is sufficient for atomic snapshots | Standard Stack | Low -- could add tempfile later if edge cases arise |
| A2 | Header-level version byte is better than per-key versioning for v1 | Architecture Patterns (Pattern 3) | Low -- per-key adds complexity with no benefit until incremental snapshots |
| A3 | AHashMap does not implement Serialize; needs conversion to Vec for snapshot | Code Examples | Medium -- if ahash adds Serialize, the conversion is unnecessary but not harmful |
| A4 | Manual Prometheus text formatting is sufficient for 5 metrics | Standard Stack | Low -- can always add prometheus crate later |

## Open Questions (RESOLVED)

1. **Pipeline persistence scope**
   - What we know: Entity state must be persisted for crash recovery. Pipeline definitions are needed to interpret entity state.
   - What's unclear: Should pipeline definitions be included in the snapshot, or should clients re-register after restart?
   - RESOLVED: Include pipeline definitions in the snapshot. Without them, restored entity state is useless (operators exist but no stream definition tells the engine how to evaluate them). This is also better UX -- restart is transparent to clients. Implemented via SerializablePipeline storing raw_register_json in SnapshotState.

2. **Snapshot file path configuration**
   - What we know: CLAUDE.md says "periodic serialization to local file"
   - What's unclear: Should the path be configurable via env var or CLI flag?
   - RESOLVED: Default to `tally.snapshot` in the working directory. Add `TALLY_SNAPSHOT_PATH` env var (consistent with existing `TALLY_TCP_PORT` / `TALLY_HTTP_PORT` pattern). Implemented in Plan 02 Task 1.

3. **Metrics persistence across restarts**
   - What we know: Counters like events_total are monotonically increasing
   - What's unclear: Should counters reset to 0 on restart or be persisted?
   - RESOLVED: Reset to 0 on restart. Prometheus handles counter resets via `rate()` function. Persisting counters adds complexity for no operational benefit. Metrics struct uses Default trait (all zeros).

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Rust built-in test framework (cargo test) |
| Config file | Cargo.toml (already configured) |
| Quick run command | `cargo test --lib` |
| Full suite command | `cargo test` |

### Phase Requirements to Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| PERS-01 | Periodic snapshot writes state to file | integration | `cargo test --test test_snapshot` | No -- Wave 0 |
| PERS-02 | Snapshot uses postcard with version byte | unit | `cargo test snapshot::tests` | No -- Wave 0 |
| PERS-03 | Server loads snapshot on startup | integration | `cargo test --test test_snapshot` | No -- Wave 0 |
| PERS-04 | Snapshot write doesn't block PUSH/GET | integration | `cargo test --test test_snapshot` | No -- Wave 0 |
| PERS-05 | TTL eviction removes inactive keys | unit + integration | `cargo test eviction::tests` | No -- Wave 0 |
| SRV-08 | HTTP endpoints: pipelines, metrics, debug | integration | `cargo test --test test_server` | Partially (test_health_endpoint exists) |

### Sampling Rate
- **Per task commit:** `cargo test --lib`
- **Per wave merge:** `cargo test`
- **Phase gate:** Full suite green before `/gsd-verify-work`

### Wave 0 Gaps
- [ ] `src/state/snapshot.rs` -- tests for OperatorState serialization round-trip, snapshot save/load, version mismatch handling
- [ ] `src/state/eviction.rs` -- tests for TTL calculation, eviction sweep, edge cases (no entities, all expired, none expired)
- [ ] `tests/test_snapshot.rs` -- integration test: push events, save snapshot, restore, verify features match (success criterion #1)
- [ ] Expand `tests/test_server.rs` -- integration tests for new HTTP endpoints (success criterion #4)

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | No | HTTP management port is internal-only per design |
| V3 Session Management | No | No sessions in management API |
| V4 Access Control | No | No auth on TCP or HTTP in v1 (explicitly out of scope) |
| V5 Input Validation | Yes | Validate snapshot version byte before deserialization; validate path inputs for debug endpoints |
| V6 Cryptography | No | No encryption of snapshots in v1 |

### Known Threat Patterns

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Malformed snapshot file | Tampering | Version byte check; postcard::from_bytes returns Err on corrupt data; wrap in Option return |
| Path traversal in /debug/key/:key | Information Disclosure | Key is used as HashMap lookup key, not file path -- no traversal risk |
| Denial of service via large /debug requests | Denial of Service | Limit response size; debug endpoint is on management port (internal only) |
| Snapshot file tampering on disk | Tampering | Out of scope for v1; future: add HMAC or checksum |

## Sources

### Primary (HIGH confidence)
- [Codebase analysis] -- store.rs, operators.rs, pipeline.rs, http.rs, tcp.rs, main.rs, types.rs examined in full
- [Cargo.toml] -- Verified postcard 1.1.3, tokio 1.51.1, axum 0.8.8, serde 1.0.228
- [CONTEXT.md] -- All locked decisions and code_context read
- [STATE.md] -- All accumulated decisions read

### Secondary (MEDIUM confidence)
- [tokio spawn_blocking docs](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html) -- spawn_blocking works with current_thread, uses separate thread pool
- [Prometheus exposition format](https://prometheus.io/docs/instrumenting/exposition_formats/) -- text/plain; version=0.0.4 content type
- [postcard docs](https://docs.rs/postcard/1.1.3/postcard/) -- to_stdvec requires use-std feature; from_bytes for deserialization

### Tertiary (LOW confidence)
- [tempfile crate](https://docs.rs/tempfile/latest/tempfile/) -- NamedTempFile::persist for atomic writes; decided not to use

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- all crates already in Cargo.toml and verified via cargo tree
- Architecture: HIGH -- patterns derived from locked decisions in CONTEXT.md + existing code patterns
- Pitfalls: HIGH -- identified from codebase analysis (trait object serialization, SystemTime, AHashMap serialization)
- Validation: HIGH -- test infrastructure exists, new test files are straightforward additions

**Research date:** 2026-04-09
**Valid until:** 2026-05-09 (stable domain, Rust crate versions locked)
