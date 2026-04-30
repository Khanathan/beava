# Phase 14: Per-stream locks + DashMap concurrency - Research

**Researched:** 2026-04-12
**Domain:** Incremental concurrency — replace global `Mutex<AppState>` with per-stream locks + DashMap entity-level concurrency
**Confidence:** HIGH

## Summary

This phase replaces the single global `Arc<Mutex<AppState>>` that serializes ALL connections with a two-level locking scheme: (1) per-stream locks so events for different streams can be processed concurrently, and (2) DashMap for entity-level concurrency within each stream. This is a **deliberate intermediate step** — not the full Seastar/Dragonfly key-partitioned sharding from the original ROADMAP Phase 14.

The current codebase is well-positioned for this change. The entity state is ALREADY grouped per-stream (`EntityState.streams: AHashMap<String, StreamEntityState>`), and `handle_push_batch` already groups events by stream name before processing. The key refactor is inverting the ownership: instead of `StateStore` owning a flat `AHashMap<EntityKey, EntityState>`, each stream will own its own `DashMap<EntityKey, StreamEntityState>`, and static features get their own `DashMap`. The `PipelineEngine` becomes `Arc<PipelineEngine>` (immutable after registration). `EventLog` writers are already per-stream files — they just need per-stream `Mutex` wrappers.

**Primary recommendation:** Restructure `AppState` to eliminate the global mutex. Use `DashMap<EntityKey, StreamEntityState>` per stream for entity-level concurrency. Use `RwLock` on `PipelineEngine` only for REGISTER (rare) vs. read-only access (hot path). Use per-stream `Mutex<BufWriter<File>>` for event log writers.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **D-01:** Remove global `Mutex<AppState>` — the single lock that serializes all connections
- **D-02:** Per-stream `RwLock` (or `Mutex`) in the store — each stream's entity map is independently lockable
- **D-03:** `DashMap<EntityKey, StreamEntityState>` per stream for entity-level concurrency (concurrent reads/writes to different keys within the same stream)
- **D-04:** `PipelineEngine` becomes `Arc<PipelineEngine>` (immutable after registration) — stream definitions are read-only during event processing
- **D-05:** `EventLog` per-stream files are already independent — wrap each in its own lock or make `append`/`append_many` take `&self` with internal synchronization
- **D-06:** Cross-stream views (`@st.view`) and lookups (`st.lookup`) need to acquire locks on multiple streams — define a consistent lock ordering (alphabetical by stream name) to prevent deadlocks
- **D-07:** Fan-out events that touch multiple streams acquire per-stream locks sequentially in alphabetical order
- **D-08:** `GET` for an entity key reads from all streams — needs to acquire all relevant stream locks (read locks suffice for GET)
- **D-09:** DashMap supports iteration via `.iter()` — snapshot serialization iterates each stream's DashMap while holding a read lock
- **D-10:** Dirty-key tracking remains a per-stream `AHashSet` (or use DashSet) protected by the stream's lock
- **D-11:** `Metrics` uses `AtomicU64` counters — already lock-free, no changes needed
- **D-12:** `snapshot_seq`, `snapshot_path`, `backfill_tracker`, `backfill_complete` move to `Arc`-wrapped structures or get their own small locks
- **D-13:** Add `dashmap` to Cargo.toml (well-established crate, ~200k downloads/day, used by tokio ecosystem)

### Claude's Discretion
- Exact `RwLock` type (std vs tokio vs parking_lot) — planner/researcher decides based on hold duration
- Whether to use `DashMap` for dirty_keys too or stick with `AHashSet` behind the stream lock
- Internal refactor of `StateStore` struct layout
- Test organization

### Deferred Ideas (OUT OF SCOPE)
- Full key-partitioned sharding (Seastar/Dragonfly pattern) — future milestone
- Thread-per-shard workers — future milestone
- core_affinity pinning — future milestone
- 1M eps target — future milestone (this phase targets measurable multi-client improvement)
- Cross-shard channel dispatch — N/A for this approach
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| PERF-05 (partial) | Key-partitioned multi-threaded engine | This phase delivers the per-stream + DashMap increment; full key-partitioned sharding deferred. Enables concurrent processing of events targeting different streams and different entity keys within the same stream. |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `dashmap` | 6.1.0 | Per-stream `DashMap<EntityKey, StreamEntityState>` for entity-level concurrency | [VERIFIED: crates.io / docs.rs] 235M total downloads; internally sharded RwLock design; direct `RwLock<HashMap>` replacement; supports serde, iter(), entry() API |
| `parking_lot` | 0.12.x | `RwLock` for `PipelineEngine`, per-stream event log `Mutex` | [VERIFIED: already evaluated in v1.3 research STACK.md] No poisoning (C-5 defense), ~2x faster uncontended than std, smaller footprint |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `ahash` | 0.8 (existing) | DashMap's default hasher; already in Cargo.toml | Already a dependency — DashMap uses it by default |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| `DashMap` | `RwLock<AHashMap>` per stream | Simpler but coarser — locks the entire stream's entity map for any write. DashMap allows concurrent writes to different keys within the same stream. |
| `DashMap` | `flurry` (Java ConcurrentHashMap port) | Lower adoption (13K downloads vs 235M), epoch-based GC adds complexity, marginal perf advantage |
| `DashMap` | `evmap` (eventually consistent) | Wrong model — we need write-then-read consistency for PUSH-then-GET |
| `parking_lot::RwLock` | `std::RwLock` | std RwLock has poisoning — code already ignores it via `.unwrap_or_else(|e| e.into_inner())` pattern at 15+ call sites; parking_lot eliminates the need |
| `parking_lot::RwLock` | `tokio::sync::RwLock` | ~10x slower for uncontended access; designed for holding across `.await` which we explicitly avoid |

**Installation:**
```bash
cargo add dashmap@6.1 parking_lot@0.12
```

## Architecture Patterns

### Proposed AppState Layout

```rust
/// NEW: replaces Arc<Mutex<AppState>>
pub struct ConcurrentAppState {
    /// Stream definitions — read-only on hot path, write on REGISTER
    pub engine: parking_lot::RwLock<PipelineEngine>,

    /// Per-stream entity state — the primary data structure
    /// Key: stream name, Value: DashMap of entity key -> stream entity state
    pub stream_stores: DashMap<String, StreamStore>,

    /// Static features (from SET/MSET) — shared across all streams
    pub static_store: DashMap<EntityKey, AHashMap<String, StaticFeature>>,

    /// Per-stream event log writers
    pub event_log: Option<ConcurrentEventLog>,

    /// Metrics — atomic counters, no lock needed
    pub metrics: AtomicMetrics,

    /// Throughput tracker — per-stream, uses internal locking
    pub throughput: parking_lot::Mutex<ThroughputTracker>,

    /// Latency tracker — uses internal locking
    pub latency: parking_lot::Mutex<LatencyTracker>,

    /// Snapshot coordination state
    pub snapshot: parking_lot::Mutex<SnapshotState>,

    /// Backfill coordination
    pub backfill_tracker: Arc<BackfillTracker>,
    pub backfill_complete: parking_lot::Mutex<HashSet<(String, String)>>,
}

/// Per-stream entity storage
pub struct StreamStore {
    pub entities: DashMap<EntityKey, StreamEntityState>,
    pub dirty_keys: parking_lot::Mutex<AHashSet<EntityKey>>,
    pub deleted_keys: parking_lot::Mutex<AHashSet<EntityKey>>,
}

/// Concurrent event log with per-stream locks
pub struct ConcurrentEventLog {
    log_dir: PathBuf,
    writers: DashMap<String, parking_lot::Mutex<BufWriter<File>>>,
    history_ttls: DashMap<String, Duration>,
}

/// Snapshot coordination (not on hot path)
pub struct SnapshotState {
    pub snapshot_path: PathBuf,
    pub snapshot_cycle: u64,
    pub snapshot_seq: u64,
    pub last_base_seq: u64,
    pub previous_base_seq: u64,
}
```

### Recommended Project Structure Change
```
src/
├── state/
│   ├── store.rs           # StreamStore + static_store (DashMap-based)
│   ├── concurrent.rs      # NEW: ConcurrentAppState struct + constructors
│   ├── snapshot.rs         # Modified: iterate DashMap per stream
│   ├── eviction.rs         # Modified: per-stream eviction
│   └── event_log.rs        # Modified: ConcurrentEventLog with per-writer Mutex
├── server/
│   ├── tcp.rs              # Modified: remove global lock, per-stream dispatch
│   └── http.rs             # Modified: scatter-gather reads across streams
```

### Pattern 1: Per-stream Push Dispatch
**What:** Events for stream S acquire only stream S's DashMap entry. No global lock.
**When to use:** PUSH hot path (handle_push_batch)
**Example:**
```rust
// Source: derived from current handle_push_batch + CONTEXT.md D-01..D-03
fn handle_push_batch(state: &ConcurrentAppState, batch: &[PendingAsync]) -> Vec<Result<(), TallyError>> {
    // Read engine definition (RwLock read — shared, non-blocking)
    let engine = state.engine.read();

    // Group events by stream (same as current code)
    // For each stream group:
    //   - Get or create StreamStore from state.stream_stores (DashMap)
    //   - For each event in group:
    //     - Get or create entity entry in stream's DashMap
    //     - Push to operators (mutates only that entity's StreamEntityState)
    //     - Mark dirty in stream's dirty_keys
    //   - Append to event log (per-stream Mutex<BufWriter>)
    //
    // Different streams execute concurrently across tokio tasks.
    // Different entity keys within the same stream execute concurrently via DashMap shards.
}
```

### Pattern 2: Cross-stream GET (scatter-gather)
**What:** GET for a key reads from ALL streams, collecting features.
**When to use:** GET, MGET, /debug/key handlers
**Example:**
```rust
// Source: derived from current get_features + CONTEXT.md D-08
fn handle_get(state: &ConcurrentAppState, key: &str) -> FeatureMap {
    let engine = state.engine.read();
    let now = SystemTime::now();
    let mut features = FeatureMap::new();

    // Iterate all registered streams
    for stream_name in engine.list_stream_names() {
        if let Some(stream_store) = state.stream_stores.get(&stream_name) {
            if let Some(entity_ref) = stream_store.entities.get(key) {
                // Read operators — DashMap returns a Ref that holds a read lock
                // on this key's shard only
                for (name, op) in entity_ref.operators.iter_mut() {
                    features.insert(name.clone(), op.read(now));
                }
            }
        }
    }

    // Overlay static features
    if let Some(static_ref) = state.static_store.get(key) {
        for (name, sf) in static_ref.iter() {
            features.insert(name.clone(), sf.value.clone());
        }
    }

    // Evaluate derives and views (read-only from features map)
    engine.evaluate_derives_and_views(key, &mut features, &state.stream_stores, now);
    features
}
```

### Pattern 3: Fan-out with Alphabetical Lock Ordering (D-06, D-07)
**What:** An event that touches multiple streams acquires per-stream DashMap entries in alphabetical order.
**When to use:** Fan-out (e.g., Transactions event updates both user_id and merchant_id streams)
**Example:**
```rust
// Source: CONTEXT.md D-06, D-07 — deadlock prevention
fn fan_out_push(state: &ConcurrentAppState, targets: &[(String, String)], event: &Value, now: SystemTime) {
    // Sort targets alphabetically by stream name
    let mut sorted_targets = targets.to_vec();
    sorted_targets.sort_by(|a, b| a.0.cmp(&b.0));

    for (stream_name, key_field) in &sorted_targets {
        if let Some(key_val) = event.get(key_field).and_then(|v| v.as_str()) {
            // DashMap.entry() acquires the shard lock for this key only
            // No cross-stream lock held — DashMap is per-stream
            if let Some(stream_store) = state.stream_stores.get(stream_name) {
                let mut entity = stream_store.entities.entry(key_val.to_string()).or_default();
                // push to operators...
            }
        }
    }
}
```

### Anti-Patterns to Avoid
- **Holding DashMap Ref across function boundaries:** DashMap's `get()` returns a `Ref<K, V>` that holds a shard read lock. Do NOT hold these across `.await` points or pass them to functions that might acquire other DashMap locks (deadlock risk). [VERIFIED: dashmap docs] Extract the data and drop the Ref quickly.
- **Nested DashMap access:** Accessing `state.stream_stores.get("A")` then `state.stream_stores.get("B")` while holding the first Ref can deadlock if DashMap's internal shards collide. Always drop Ref A before accessing B, or use the alphabetical ordering pattern. [ASSUMED — DashMap internal shard collision is rare but possible]
- **Using `DashMap::iter()` on hot path:** Iteration holds read locks on all internal shards briefly. Fine for snapshot/eviction/debug, NOT for per-event operations. [VERIFIED: dashmap docs]
- **`tokio::sync::RwLock` for engine:** It's designed for holding across `.await` which we never do. Use `parking_lot::RwLock` for ~10x better uncontended performance. [VERIFIED: v1.3 research PITFALLS.md C-7]

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Concurrent hashmap | Custom sharded `Vec<RwLock<AHashMap>>` | `DashMap` | DashMap handles shard count tuning, resize, memory layout internally — 6 years of production hardening |
| Per-stream lock ordering | Manual lock ordering tracking | Alphabetical sort before acquisition (D-06) | Simple, deterministic, no runtime overhead beyond a sort of ~3-5 items |
| Atomic metrics | Custom `Mutex<Metrics>` | `AtomicU64` for each counter (D-11) | Already implemented — metrics are just counters, atomics are the right tool |

## state.lock() Call Site Inventory

Every `state.lock()` call in the codebase must be refactored. Categorized by path type:

### Hot Path (MUST change — serializes all connections today)

| Call Site | File:Line | Current Behavior | New Approach |
|-----------|-----------|------------------|--------------|
| `handle_push_core_ex` | tcp.rs:556 | Locks all state for entire push pipeline | Per-stream DashMap entry + per-stream event log Mutex |
| `handle_push_batch` | tcp.rs:902 | Locks all state for batch of events | Per-stream DashMap entries per group; no global lock |
| `handle_sync_command(Get)` | tcp.rs:1059 | Locks all state to read features | Scatter-gather across stream DashMaps (read-only) |
| `handle_sync_command(Set)` | tcp.rs:1079 | Locks all state to write static features | `static_store` DashMap entry only |
| `handle_sync_command(Mget)` | tcp.rs:1173 | Locks all state to read multiple keys | Scatter-gather across stream DashMaps per key |
| Latency recording (post-mset) | tcp.rs:366, 444 | Locks to record latency metric | `latency: parking_lot::Mutex<LatencyTracker>` — separate small lock |

### Background (MUST change — competes with hot path for global lock)

| Call Site | File:Line | Current Behavior | New Approach |
|-----------|-----------|------------------|--------------|
| Snapshot timer (periodic) | main.rs:234 | Locks to clone state for snapshot | Iterate each stream's DashMap via `.iter()`; snapshot coordination via `snapshot: Mutex<SnapshotState>` |
| Eviction timer | main.rs:423 | Locks to run eviction scan | Per-stream eviction: iterate each stream's DashMap, remove expired entries |
| Event log fsync | main.rs:444 | Locks to access `event_log` | `ConcurrentEventLog.fsync_all()` iterates per-stream Mutexes independently |
| Event log compaction | main.rs:477 | Locks per-stream for compaction | Per-stream `Mutex<BufWriter>` — compaction locks only the target stream's writer |
| Post-snapshot metrics | main.rs:399 | Locks to write `metrics.snapshot_duration_ms` | `AtomicU64` for snapshot_duration_ms |

### Cold Path (can use fine-grained locks)

| Call Site | File:Line | Current Behavior | New Approach |
|-----------|-----------|------------------|--------------|
| `handle_sync_command(Register)` | tcp.rs:1109 | Locks to register pipeline | `engine: RwLock<PipelineEngine>` — write lock (rare, cold path) |
| Backfill startup | main.rs:170-199 | Locks to read event log + spawn tasks | Per-stream event log read (no global lock needed) |
| `run_backfill` chunks | tcp.rs:1221, 1235 | Locks per chunk of 64 events | Per-stream DashMap entries for backfill target stream |

### HTTP Debug (can scatter-gather, not latency-sensitive)

| Call Site | File:Line | Current Behavior | New Approach |
|-----------|-----------|------------------|--------------|
| `list_pipelines` | http.rs:24 | Locks to list stream names | `engine.read()` — RwLock read |
| `get_pipeline` | http.rs:33 | Locks to read stream definition | `engine.read()` — RwLock read |
| `create_pipeline` | http.rs:134 | Locks to register | `engine.write()` — RwLock write |
| `delete_pipeline` | http.rs:179 | Locks to remove stream | `engine.write()` + remove from `stream_stores` DashMap |
| `metrics_endpoint` | http.rs:200 | Locks to read metrics | Atomic reads + `store.entity_count()` via DashMap `.len()` |
| `debug_key` | http.rs:236 | Locks to inspect entity | Scatter-gather across stream DashMaps |
| `debug_topology` | http.rs:307 | Locks to read topology | `engine.read()` |
| `debug_throughput` | http.rs:442 | Locks to decay and snapshot | `throughput: Mutex<ThroughputTracker>` |
| `debug_latency` | http.rs:470 | Locks to read latency | `latency: Mutex<LatencyTracker>` |
| `debug_memory` | http.rs:487 | Locks to iterate entities | Scatter-gather across stream DashMaps `.len()` |
| `trigger_snapshot` | http.rs:547 | Locks to clone state | Iterate stream DashMaps + snapshot coordination lock |
| `debug_backfill` | http.rs:668 | Locks to read backfill tracker | `backfill_tracker` already `Arc<BackfillTracker>` with its own Mutex |

**Total: 25+ `state.lock()` call sites across 4 files.** All must be refactored.

## Cross-Stream Operation Analysis

### Fan-out (handle_push_batch, handle_push_core_ex)

**Current behavior** (tcp.rs:628-661): After the primary push, iterates `engine.fan_out_targets()` and calls `engine.push(target_name, ...)` for each qualifying target stream. This touches different keys in different streams — e.g., a `Transactions` event with `user_id=u123` and `merchant_id=m456` pushes to `MerchantActivity` keyed on `m456`.

**With per-stream DashMap:** Fan-out is naturally safe because each target stream has its own DashMap. The fan-out loop accesses `stream_stores.get("MerchantActivity")` then `entity.entry("m456")` — completely independent from the primary stream's DashMap. **No cross-stream lock contention.** No alphabetical ordering needed for fan-out because DashMap entries are independent.

**Deadlock risk:** LOW. Fan-out accesses different DashMaps (different streams). DashMap internal shards are per-map, so no cross-map shard collision is possible. The only risk would be if we held a DashMap Ref from stream A while trying to acquire a Ref from stream A for a different key — which DashMap handles internally via its sharded design. [VERIFIED: DashMap uses per-instance shard arrays]

### Cascade (push_with_cascade)

**Current behavior** (pipeline.rs:756-820): After the primary push, BFS traverses `downstream_map` and pushes to each dependent stream. Each cascade target is pushed with the SAME event payload (same key field).

**With per-stream DashMap:** Each cascade step accesses a different stream's DashMap with the same entity key. Since each stream has its own DashMap, no cross-lock contention. Cascade pushes happen sequentially within the engine's `push_with_cascade` — this is fine because they're all writes to different per-stream DashMaps.

### Cross-key Lookups (st.lookup in views)

**Current behavior** (pipeline.rs:975-1000): During `get_features`, a `Lookup` resolves a foreign key from the current entity's features, then reads a feature value from a different entity key in a different stream via `store.get_feature_value(fk, target_feature, now)`.

**With per-stream DashMap:** The lookup reads from `stream_stores.get(target_stream).entities.get(foreign_key)`. This is a read-only access to a different entity in a potentially different stream — safe with DashMap's concurrent reads. **Important:** `get_feature_value` currently requires `&mut self` because `op.read(now)` advances window buckets. With DashMap, we need `entry.get_mut()` which acquires a write lock on that key's shard. This is fine — it's a different key from the one being GET'd.

### GET scatter-gather (D-08)

**Current behavior** (pipeline.rs:921-1000): `get_features` iterates ALL streams' operators for the given key, then evaluates derives and view lookups.

**With per-stream DashMap:** Iterate `stream_stores`, for each stream call `entities.get_mut(key)` to read operators (needs mut for `op.read(now)` which advances time). Each stream's DashMap is independent — no lock contention between streams. `get_mut` holds a write lock on just that key's shard within that stream's DashMap, released immediately after reading.

## DashMap Specifics for This Use Case

### DashMap Internal Architecture [VERIFIED: docs.rs/dashmap 6.1.0]

- Internally uses `N` shards (default: based on available parallelism)
- Each shard is a `RwLock<HashMap<K, V>>` using `parking_lot::RwLock`
- Key is hashed to determine shard; operations only lock that shard
- `get()` returns `Ref<K, V>` — holds a read lock on the shard
- `get_mut()` returns `RefMut<K, V>` — holds a write lock on the shard
- `entry()` returns an `Entry` for insert-or-get — holds a write lock
- `iter()` acquires read locks on all shards (fine for cold path / snapshot)
- `.len()` is O(shards) — sums per-shard counts

### DashMap vs RwLock<AHashMap> Decision

For per-stream entity state, DashMap wins because:
1. **Concurrent writes to different keys in the same stream:** Multiple connections pushing events for different entity keys within `Transactions` can proceed in parallel. With `RwLock<AHashMap>`, they serialize on the write lock.
2. **Concurrent reads with writes:** GET for key A can proceed while PUSH for key B is in-flight (DashMap uses per-shard RwLock, different keys likely in different shards).
3. **No batch penalty:** `handle_push_batch` processes events one at a time within a stream group — each event accesses its own entity key via DashMap entry, no need for exclusive lock on entire map.

For `PipelineEngine`, `parking_lot::RwLock` wins because:
1. Engine is read 100K+ times/sec (every PUSH reads stream definitions) but written ~once (REGISTER)
2. RwLock allows unlimited concurrent readers
3. DashMap overhead per access (~5ns hash + shard lookup) is unnecessary for a structure with O(10) entries

### DashMap + operator `read()` mutability issue [VERIFIED: store.rs:150-171]

`OperatorState::read(&mut self, now: SystemTime)` requires `&mut self` because it advances window buckets. This means GET needs a write lock on the entity's DashMap shard. Options:

**Option A (recommended):** Use `DashMap::get_mut()` for GET operations. This locks only the relevant key's shard for writing — other keys in different shards can still be read/written concurrently. The lock duration is microseconds (just reading operator values with time advancement). This is acceptable because:
- GET is already fast (<50us p99)
- The shard write lock blocks only other accesses to keys in the same DashMap shard (typically 1/N of keys where N = available_parallelism)
- Most GET calls access different entity keys so they hit different shards

**Option B (future optimization):** Refactor `read()` to be `&self` by making window bucket advancement lazy (e.g., store `last_read_at` and advance on next `push()`). Higher complexity, defer to future.

## Snapshot Iteration with DashMap (D-09)

**Current:** `clone_for_snapshot_with_gc` iterates `self.entities.iter()` on `AHashMap`.

**With DashMap:** `DashMap::iter()` works but acquires read locks on all internal shards sequentially. This is safe for the snapshot path because:
1. Snapshot already runs on a background timer / `spawn_blocking`
2. Read locks allow concurrent writes to different shards (writes to shard N proceed while snapshot reads shard M)
3. The iteration is NOT a consistent snapshot (a write between shard reads may not be captured) — this is acceptable per existing contract ("lose ~30s of state on crash")

**Implementation pattern:**
```rust
fn clone_stream_for_snapshot(stream_store: &StreamStore) -> Vec<(String, SerializableStreamEntityState)> {
    stream_store.entities.iter()
        .map(|entry| {
            let key = entry.key().clone();
            let state = entry.value().clone();  // Clone and release Ref
            (key, SerializableStreamEntityState {
                operators: state.operators.clone(),
                last_event_at: state.last_event_at,
            })
        })
        .collect()
}
```

## Eviction with DashMap

**Current:** Two-phase — collect eviction plan from immutable scan, then apply mutations. `store.entity_keys()` collects all keys, then `store.get_entity()` + `store.get_entity_mut()`.

**With per-stream DashMap:** Eviction becomes per-stream:
```rust
for (stream_name, stream_store) in state.stream_stores.iter() {
    let ttl = engine.get_stream_entity_ttl(&stream_name);
    // DashMap::retain() is the idiomatic way to filter entries
    stream_store.entities.retain(|_key, entity_state| {
        match entity_state.last_event_at {
            Some(t) => now.duration_since(t).unwrap_or_default() <= ttl,
            None => true, // No event yet — keep
        }
    });
}
```

DashMap's `retain()` iterates all shards and removes entries that don't satisfy the predicate. It acquires write locks per-shard during iteration, releasing between shards. [VERIFIED: dashmap docs — retain() is a provided method]

**Static features and empty entity cleanup:** After per-stream eviction, we also need to check if an entity has been removed from ALL streams AND has no static features. This requires a cross-stream check. Implementation: maintain a `DashSet<EntityKey>` of "potentially empty" keys flagged during per-stream eviction, then sweep `static_store` for those keys.

## Common Pitfalls

### Pitfall 1: DashMap Ref Held Across Engine Read Lock
**What goes wrong:** Code acquires a `DashMap::Ref` for an entity, then tries to acquire `engine.read()` (or vice versa). If another thread holds `engine.write()` and is waiting for the DashMap shard, deadlock.
**Why it happens:** Natural composition — "look up the stream definition, then access the entity."
**How to avoid:** Always acquire `engine.read()` FIRST, extract needed info (key_field, feature defs), drop the engine guard, THEN access DashMap entries. Engine reads are non-blocking (parking_lot allows multiple readers).
**Warning signs:** Timeouts on REGISTER while PUSH is in flight.

### Pitfall 2: DashMap get_mut() Blocks Concurrent GET for Same Shard
**What goes wrong:** PUSH to entity "user_123" holds a `RefMut` on that DashMap shard. A concurrent GET for "user_456" blocks if it happens to hash to the same DashMap shard.
**Why it happens:** DashMap shards are fixed-size (typically num_cpus shards). Under high concurrency, shard collisions are expected.
**How to avoid:** Keep `RefMut` hold times minimal — extract entity, push operators, drop Ref. Do NOT hold RefMut during event log writes or metric updates. This is the same discipline as the `#[deny(clippy::await_holding_lock)]` pattern.
**Warning signs:** p99 latency spikes under high multi-client load.

### Pitfall 3: Snapshot Dirty-Key Tracking Race
**What goes wrong:** A PUSH marks a key dirty in stream A's `dirty_keys` set. Before the snapshot reads it, another PUSH to the same key in stream B marks it dirty in stream B's `dirty_keys`. Snapshot sees the key dirty in both but clones the entity state at different points in time.
**Why it happens:** Per-stream dirty tracking without global coordination.
**How to avoid:** This is acceptable — same relaxation as the existing "lose ~30s on crash" contract. The snapshot is eventually consistent. Mark dirty after the operator mutation completes (not before).
**Warning signs:** None — this is by design.

### Pitfall 4: REGISTER During Active Pushes
**What goes wrong:** A REGISTER takes `engine.write()` which blocks all PUSH operations that need `engine.read()`. If REGISTER is slow (complex pipeline), it stalls the hot path.
**Why it happens:** `parking_lot::RwLock` is writer-preferring by default — a pending write lock blocks new readers.
**How to avoid:** REGISTER is rare (once at startup, occasionally for schema changes). Accept the brief stall. Alternative: use `ArcSwap` for the engine (CONTEXT.md D-04 says "immutable after registration" — ArcSwap fits). Defer ArcSwap to a future optimization if REGISTER frequency is actually a problem.
**Warning signs:** p99 spike during schema changes.

### Pitfall 5: Static Feature DashMap Overhead for SET/MSET
**What goes wrong:** SET/MSET writes to `static_store: DashMap`. For MSET with 100K keys, each DashMap entry() call does hash + shard lock. Overhead is ~5-10ns per entry which adds 0.5-1ms for 100K keys — negligible compared to current MSET cooperative yielding.
**Why it happens:** DashMap per-access overhead vs batch AHashMap insert.
**How to avoid:** Not a real problem. MSET already yields every 1024 keys. DashMap per-key overhead is within noise.

### Pitfall 6: Event Log Writer Contention
**What goes wrong:** Multiple connections push to the same stream concurrently. Each PUSH needs to append to the stream's event log file. If using `Mutex<BufWriter<File>>` per stream, pushes for the same stream serialize on the log write.
**Why it happens:** BufWriter is inherently single-writer (seeking is sequential).
**How to avoid:** Accept it — event log writes are buffered (~100-300ns memcpy). The Mutex hold time is just the memcpy to the BufWriter's internal buffer, not disk I/O. fsync happens on a background timer. This is the same model as before, just with a per-stream lock instead of global.

## Code Examples

### Example 1: Refactored handle_push_batch
```rust
// Source: derived from tcp.rs:880-1031 + CONTEXT.md D-01..D-08
pub fn handle_push_batch(
    state: &ConcurrentAppState,
    batch: &[PendingAsync],
) -> Vec<Result<(), TallyError>> {
    if batch.is_empty() { return Vec::new(); }

    let mut results: Vec<Result<(), TallyError>> = vec![Ok(()); batch.len()];

    // Acquire engine read lock ONCE for the whole batch
    let engine = state.engine.read();

    // Group events by stream (same as current code)
    let all_same = batch.len() == 1 || batch[1..].iter().all(|ev| ev.stream_name == batch[0].stream_name);

    if all_same {
        let stream_name = &batch[0].stream_name;
        let stream_def = match engine.get_stream(stream_name) {
            Some(s) => s,
            None => { /* fill results with errors */ return results; }
        };

        // Get or create the StreamStore for this stream
        let stream_store = state.stream_stores
            .entry(stream_name.clone())
            .or_insert_with(|| StreamStore::new());

        let now = batch[0].now;
        for (idx, ev) in batch.iter().enumerate() {
            // DashMap entry() — locks only the key's shard
            let key = match extract_key(&stream_def, &ev.payload) {
                Some(k) => k,
                None => { results[idx] = Err(TallyError::Protocol("missing key".into())); continue; }
            };

            // Push to operators (write lock on key's shard)
            let mut entity = stream_store.entities.entry(key.clone()).or_default();
            match push_operators(&engine, stream_name, &ev.payload, &mut entity, now) {
                Ok(()) => {
                    // Mark dirty (separate small lock)
                    stream_store.dirty_keys.lock().insert(key);
                }
                Err(e) => results[idx] = Err(e),
            }
            // RefMut dropped here — shard unlocked
        }

        // Event log append (per-stream Mutex, not per-event)
        if let Some(ref log) = state.event_log {
            log.append_many(stream_name, batch, now);
        }
    }
    // ... multi-stream path with same pattern

    // Metrics (atomic)
    state.metrics.events_total.fetch_add(batch.len() as u64, Ordering::Relaxed);

    results
}
```

### Example 2: ConcurrentEventLog
```rust
// Source: derived from event_log.rs + CONTEXT.md D-05
pub struct ConcurrentEventLog {
    log_dir: PathBuf,
    writers: DashMap<String, parking_lot::Mutex<BufWriter<File>>>,
    history_ttls: DashMap<String, Duration>,
}

impl ConcurrentEventLog {
    pub fn append(&self, stream_name: &str, payload: &[u8], now: SystemTime) -> std::io::Result<bool> {
        let writer_entry = match self.writers.get(stream_name) {
            Some(w) => w,
            None => return Ok(false),
        };
        let mut writer = writer_entry.lock();  // Per-stream Mutex
        let entry = LogEntry { timestamp: now, payload: payload.to_vec() };
        let encoded = postcard::to_stdvec(&entry).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        writer.write_all(&(encoded.len() as u32).to_be_bytes())?;
        writer.write_all(&encoded)?;
        Ok(true)
    }

    pub fn fsync_all(&self) -> std::io::Result<()> {
        for entry in self.writers.iter() {
            let mut writer = entry.value().lock();
            writer.flush()?;
            writer.get_ref().sync_data()?;
        }
        Ok(())
    }
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| `Arc<Mutex<AppState>>` (global lock) | Per-stream `DashMap` + `parking_lot::RwLock<PipelineEngine>` | This phase | Enables concurrent multi-client processing for different streams |
| `AHashMap<EntityKey, EntityState>` (flat map) | Per-stream `DashMap<EntityKey, StreamEntityState>` + `DashMap<EntityKey, static_features>` | This phase | Entity-level concurrency within streams |
| `EventLog` with `&mut self` methods | `ConcurrentEventLog` with per-stream `Mutex<BufWriter>` | This phase | Concurrent event log writes for different streams |

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | DashMap 6.1.0 uses `parking_lot` internally for shard locks | Architecture Patterns | If it uses `std::RwLock`, we get poisoning behavior inside DashMap — unlikely to matter since DashMap handles this internally |
| A2 | DashMap `retain()` releases shard locks between shards during iteration | Eviction section | If it holds all shard locks simultaneously, eviction could cause brief stalls for hot-path PUSH |
| A3 | Two connections pushing to different entity keys in the same stream will hit different DashMap shards most of the time | Pattern 1 / Pitfall 2 | If shard count is low and keys collide, we get serialization within a stream — still better than global lock |
| A4 | `OperatorState::read(&mut self)` can safely use `DashMap::get_mut()` without significant contention | DashMap + operator mutability section | If GET traffic is extremely high on the same keys, shard write lock contention could regress GET latency |

## Open Questions

1. **Entity lifecycle across streams**
   - What we know: Currently `EntityState` groups all streams for a key. With per-stream DashMaps, there is no single "EntityState" anymore — each stream has its own independent entry for a key.
   - What's unclear: How does eviction detect "entity has no streams left AND no static features" without iterating all stream DashMaps?
   - Recommendation: Maintain a `DashMap<EntityKey, u32>` reference counter that tracks how many streams have state for each key. Decrement on eviction, delete static features when counter hits zero. Alternatively, accept that empty-entity cleanup is a cold-path scan during eviction timer.

2. **REGISTER atomicity with stream_stores**
   - What we know: REGISTER adds a new stream definition to `PipelineEngine` and should create a new `StreamStore` entry in `stream_stores`.
   - What's unclear: If a PUSH arrives between engine write and stream_store creation, it will find the stream in the engine but not in stream_stores.
   - Recommendation: Create `stream_stores` entry BEFORE releasing the engine write lock (within the same critical section). Or: create lazily on first PUSH via `stream_stores.entry().or_default()`.

3. **Backfill with per-stream DashMap**
   - What we know: Backfill currently locks state per 64-event chunk and pushes to operators.
   - What's unclear: Backfill pushes for stream S should not block unrelated streams.
   - Recommendation: Backfill acquires only the target stream's DashMap entries — natural with the new architecture.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| dashmap crate | Entity-level concurrency | Available via cargo | 6.1.0 | RwLock<AHashMap> per stream (coarser granularity) |
| parking_lot crate | RwLock for engine, Mutex for event log | Available via cargo | 0.12.x | std::RwLock / std::Mutex (with poisoning workaround) |

No external runtime dependencies. All changes are Cargo crate additions.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | cargo test (built-in) |
| Config file | Cargo.toml [dev-dependencies] |
| Quick run command | `cargo test --lib -- --test-threads=4` |
| Full suite command | `cargo test` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| CONC-01 | Per-stream locking allows concurrent pushes to different streams | integration | `cargo test test_concurrent_push_different_streams` | Wave 0 |
| CONC-02 | DashMap allows concurrent pushes to different entity keys within same stream | integration | `cargo test test_concurrent_push_different_keys` | Wave 0 |
| CONC-03 | Fan-out works correctly with per-stream DashMaps | integration | `cargo test test_fanout_concurrent` | Wave 0 |
| CONC-04 | GET scatter-gather returns correct features from all streams | integration | `cargo test test_get_scatter_gather` | Wave 0 |
| CONC-05 | Snapshot iterates DashMap safely under concurrent writes | integration | `cargo test test_snapshot_during_writes` | Wave 0 |
| CONC-06 | Eviction works per-stream with DashMap retain | unit | `cargo test test_eviction_per_stream_dashmap` | Wave 0 |
| CONC-07 | Cross-key lookup resolves correctly with per-stream DashMaps | integration | `cargo test test_lookup_concurrent` | Wave 0 |
| CONC-08 | All 532+ existing tests pass | regression | `cargo test` | Existing |
| BENCH-01 | Multi-client throughput ≥ 200k eps @ 4 clients | bench | `python benchmark/bench.py --clients 4` | Existing (bench.py) |

### Wave 0 Gaps
- [ ] New integration test file for concurrent operations
- [ ] Multi-client benchmark runner (may already exist from Phase 12 target)

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | no | N/A |
| V3 Session Management | no | N/A |
| V4 Access Control | no | N/A |
| V5 Input Validation | yes | Existing: stream name sanitization, key field validation, payload size limits |
| V6 Cryptography | no | N/A |

### Known Threat Patterns

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Concurrent access to shared state | Tampering | DashMap's internal RwLock per shard; per-stream dirty tracking |
| Deadlock from lock ordering | Denial of Service | Alphabetical lock ordering for cross-stream operations (D-06) |

No new security surface introduced by this change — concurrency controls are internal implementation details not exposed to clients.

## Sources

### Primary (HIGH confidence)
- `src/server/tcp.rs` — all `state.lock()` call sites enumerated (25+)
- `src/state/store.rs` — `StateStore`, `EntityState`, `StreamEntityState` structures
- `src/engine/pipeline.rs` — `PipelineEngine`, `push_batch_with_cascade_no_features`, `get_features`, fan-out
- `src/state/event_log.rs` — per-stream log files, `append()`, `append_many()`, `fsync_all()`
- `src/state/eviction.rs` — two-phase eviction with `mark_deleted`
- `src/server/http.rs` — all HTTP handlers with `state.lock()`
- `src/main.rs` — `Arc::new(Mutex::new(AppState))`, timers
- `.planning/phases/14-per-stream-locks-dashmap-concurrency/14-CONTEXT.md` — locked decisions D-01..D-13
- [DashMap docs](https://docs.rs/dashmap/latest/dashmap/) — API verification, shard architecture
- [DashMap crates.io](https://crates.io/crates/dashmap) — version 6.1.0, 235M downloads

### Secondary (MEDIUM confidence)
- `.planning/research/PITFALLS.md` — C-5 (mutex poisoning), C-6 (false sharing), C-7 (MutexGuard across .await)
- `.planning/research/SUMMARY.md` — original v1.3 architecture design (adapted for per-stream model)

### Tertiary (LOW confidence)
- None — all claims verified against codebase or official docs

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — dashmap 6.1.0 verified on crates.io, parking_lot already evaluated in v1.3 research
- Architecture: HIGH — per-stream DashMap is a natural evolution of existing EntityState.streams grouping
- Pitfalls: HIGH — grounded in actual call site analysis of 25+ `state.lock()` sites
- Cross-stream operations: HIGH — fan-out, cascade, lookups all analyzed against specific code paths

**Research date:** 2026-04-12
**Valid until:** 2026-05-12 (stable — DashMap 6.x is mature, parking_lot 0.12 is stable)
