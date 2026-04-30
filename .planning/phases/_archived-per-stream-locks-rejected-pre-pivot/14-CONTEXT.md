# Phase 14: Per-stream locks + DashMap concurrency - Context

**Gathered:** 2026-04-12
**Status:** Ready for planning
**Mode:** Auto — user-directed scope change from ROADMAP's key-partitioned sharding to incremental concurrency

<domain>
## Phase Boundary

Replace the single global `Mutex<AppState>` with **two levels of concurrency**:

1. **Per-stream locks:** Each registered stream gets its own lock. Events for different streams can be processed concurrently on separate tokio tasks. The `PipelineEngine` and `EventLog` become shared/concurrent structures.
2. **DashMap for entity state:** Within each stream, use `DashMap<EntityKey, EntityState>` (or `DashMap<EntityKey, StreamEntityState>`) instead of `AHashMap` for concurrent read/write access to different entity keys within the same stream.

This is an **incremental step** — NOT the full Seastar/Dragonfly key-partitioned model from the original ROADMAP. The full sharding becomes a future milestone if needed.

**In scope:**
- Refactor `AppState` to remove the global `Mutex` wrapper
- Per-stream locking strategy (one `Mutex` or `RwLock` per stream in the engine/store)
- DashMap for entity-level concurrency within streams
- Add `dashmap` crate to Cargo.toml
- Ensure cross-stream views and lookups still work (multi-lock coordination)
- Ensure snapshot serialization can iterate DashMap safely
- Ensure event log writes are concurrent-safe (per-stream log files are already independent)
- Multi-client throughput bench gate (the 200k+ 4-client target deferred from Phase 12)
- Mixed sync+async workload under concurrency

**Out of scope:**
- Thread-per-shard / Seastar / Glommio pattern (future milestone)
- Key-partitioned sharding across N worker threads
- Cross-shard channel dispatch
- core_affinity, crossbeam-channel (original ROADMAP Phase 14 crates)
- 1M eps target (that was for full sharding; realistic target for this approach: measure and document)

**User direction (2026-04-12):** "make it shared state but with lock at each dataset first not key partitioned yet" + "per stream locks, also maybe dashmap for entity lock"

</domain>

<decisions>
## Implementation Decisions

### Locking Architecture
- **D-01:** Remove global `Mutex<AppState>` — the single lock that serializes all connections
- **D-02:** Per-stream `RwLock` (or `Mutex`) in the store — each stream's entity map is independently lockable
- **D-03:** `DashMap<EntityKey, StreamEntityState>` per stream for entity-level concurrency (concurrent reads/writes to different keys within the same stream)
- **D-04:** `PipelineEngine` becomes `Arc<PipelineEngine>` (immutable after registration) — stream definitions are read-only during event processing
- **D-05:** `EventLog` per-stream files are already independent — wrap each in its own lock or make `append`/`append_many` take `&self` with internal synchronization

### Cross-Stream Coordination
- **D-06:** Cross-stream views (`@st.view`) and lookups (`st.lookup`) need to acquire locks on multiple streams — define a consistent lock ordering (alphabetical by stream name) to prevent deadlocks
- **D-07:** Fan-out events that touch multiple streams acquire per-stream locks sequentially in alphabetical order
- **D-08:** `GET` for an entity key reads from all streams — needs to acquire all relevant stream locks (read locks suffice for GET)

### Snapshot Compatibility
- **D-09:** DashMap supports iteration via `.iter()` — snapshot serialization iterates each stream's DashMap while holding a read lock
- **D-10:** Dirty-key tracking remains a per-stream `AHashSet` (or use DashSet) protected by the stream's lock

### Metrics and State
- **D-11:** `Metrics` uses `AtomicU64` counters — already lock-free, no changes needed
- **D-12:** `snapshot_seq`, `snapshot_path`, `backfill_tracker`, `backfill_complete` move to `Arc`-wrapped structures or get their own small locks

### New Crate
- **D-13:** Add `dashmap` to Cargo.toml (well-established crate, ~200k downloads/day, used by tokio ecosystem)

### Claude's Discretion
- Exact `RwLock` type (std vs tokio vs parking_lot) — planner/researcher decides based on hold duration
- Whether to use `DashMap` for dirty_keys too or stick with `AHashSet` behind the stream lock
- Internal refactor of `StateStore` struct layout
- Test organization

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Current Architecture
- `src/server/tcp.rs` lines 59-75 — `AppState` struct: engine, store, metrics, snapshot_path, event_log, backfill_tracker
- `src/state/store.rs` — `StateStore` with `entities: AHashMap<EntityKey, EntityState>`, `dirty_keys: AHashSet<EntityKey>`
- `src/state/store.rs` line 49 — `EntityState.streams: AHashMap<String, StreamEntityState>` (per-stream state already exists!)
- `src/main.rs` line 53 — `Arc::new(Mutex::new(AppState { ... }))` (the global lock)
- `src/server/tcp.rs` — every `state.lock().unwrap()` call site (dozens — all must be refactored)

### Phase 12 Context
- `src/server/tcp.rs` — `handle_push_batch` takes `&Mutex<AppState>` and calls `state.lock()` once per batch
- Phase 12 coalescer: ConnAccumulator batches → handle_push_batch → single lock → grouped dispatch

### Research
- `.planning/research/SUMMARY.md` — original Phase 14 design (for reference, NOT the plan)
- `.planning/research/PITFALLS.md` — C-7 (MutexGuard across .await), cache-line false sharing

### Requirements
- `.planning/REQUIREMENTS.md` — PERF-05 (multi-threaded engine) — scope reduced to per-stream+DashMap
- `.planning/ROADMAP.md` §"Phase 14" — original spec (will be updated with actual implementation)

</canonical_refs>

<code_context>
## Existing Code Insights

### Key Insight: Per-stream entity state ALREADY EXISTS
`EntityState.streams: AHashMap<String, StreamEntityState>` means the data is already keyed by stream. The refactor moves the lock boundary from "one lock around everything" to "one lock per stream's entity map."

### Reusable Assets
- `AHashMap<EntityKey, EntityState>` in StateStore — replace with DashMap
- Per-stream EventLog files — already independent, just need concurrent-safe append
- `handle_push_batch` groups events by stream — natural fit for per-stream locking (acquire stream lock once per group)
- `Metrics` already uses atomics — no changes needed

### Integration Points (all `state.lock()` call sites that must change)
- `handle_push_batch` in tcp.rs (hot path — most critical)
- `handle_get` in tcp.rs
- `handle_set`/`handle_mset` in tcp.rs
- `handle_register` in tcp.rs
- Snapshot writer in main.rs / snapshot.rs
- HTTP API handlers in http.rs
- Backfill runner in main.rs
- TTL eviction timer in main.rs

</code_context>

<specifics>
## Specific Ideas

The entity state is already structured per-stream (`EntityState.streams`). The main question is whether to restructure as:
- **Option A:** `DashMap<EntityKey, EntityState>` at the store level (entity-level concurrency, EntityState still has per-stream sub-map)
- **Option B:** Per-stream `DashMap<EntityKey, StreamEntityState>` (stream-level + entity-level concurrency, streams are top-level)

Option B gives more granular locking but requires restructuring how GET works (reads across streams). Option A is simpler but still has the per-entity benefit.

User mentioned "per stream locks, also maybe dashmap for entity lock" — this suggests Option B: per-stream lock + within-stream DashMap.

</specifics>

<deferred>
## Deferred Ideas

- Full key-partitioned sharding (Seastar/Dragonfly pattern) — future milestone
- Thread-per-shard workers — future milestone
- core_affinity pinning — future milestone
- 1M eps target — future milestone (this phase targets measurable multi-client improvement)
- Cross-shard channel dispatch — N/A for this approach

</deferred>

---

*Phase: 14-per-stream-locks-dashmap-concurrency*
*Context gathered: 2026-04-12 (user-directed scope change from original ROADMAP Phase 14)*
