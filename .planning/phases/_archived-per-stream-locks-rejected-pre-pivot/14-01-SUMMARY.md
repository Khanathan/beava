---
phase: 14
plan: 01
subsystem: server, state
tags: [concurrency, dashmap, parking_lot, per-field-locking, ConcurrentAppState]
dependency-graph:
  requires:
    - "12-02 handle_push_batch"
    - "12-02 ConnAccumulator"
    - "13-02 OP_PUSH_BATCH"
  provides:
    - "server.ConcurrentAppState"
    - "state.StreamStore"
    - "state.StateStore.to_concurrent"
    - "state.StateStore.from_concurrent"
    - "server.make_concurrent_state"
  affects:
    - "Phase 14 Plan 02 (snapshot optimization with DashMap iteration)"
    - "Phase 15 off-thread snapshot I/O"
tech-stack:
  added:
    - dashmap 6.1
    - parking_lot 0.12
  patterns:
    - "Per-field locking via ConcurrentAppState (RwLock<PipelineEngine> + PLMutex<StateStore> + PLMutex<Option<EventLog>> + independent small locks)"
    - "StreamStore with DashMap<EntityKey, StreamEntityState> for future per-stream entity concurrency"
    - "parking_lot::Mutex replaces std::sync::Mutex for non-poisoning, faster uncontended locks"
    - "parking_lot::RwLock for PipelineEngine (many-reader hot path, write-only on REGISTER)"
key-files:
  created: []
  modified:
    - Cargo.toml
    - Cargo.lock
    - src/state/store.rs
    - src/state/mod.rs
    - src/server/tcp.rs
    - src/server/http.rs
    - src/main.rs
    - tests/test_server.rs
    - tests/test_debug_ui.rs
    - tests/test_pipeline.rs
    - tests/test_push_coalescing.rs
    - tests/test_push_batch.rs
decisions:
  - "Per-field locking approach: ConcurrentAppState wraps each AppState field in its own lock (RwLock for engine, PLMutex for store/event_log/metrics/throughput/latency/snapshot fields) rather than one global Mutex<AppState>"
  - "StreamStore defined with DashMap for entity-level concurrency but StateStore retained as primary storage behind PLMutex for engine method compatibility (engine.push takes &mut StateStore)"
  - "parking_lot chosen over std::sync for no-poisoning semantics and ~2x faster uncontended access"
  - "BackfillStatus/BackfillTracker keep std::sync::Mutex (not parking_lot) since they need unwrap_or_else poisoning recovery pattern for cross-task safety"
metrics:
  duration: ~28min
  tasks_completed: 2
  completed_date: 2026-04-12
requirements: [PERF-05]
---

# Phase 14 Plan 01: ConcurrentAppState + Per-Field Locking Summary

Replaced the single global `Arc<Mutex<AppState>>` with `Arc<ConcurrentAppState>` where each field is independently lockable via parking_lot locks. Added `dashmap` 6.1 and `parking_lot` 0.12. Defined `StreamStore` with `DashMap<EntityKey, StreamEntityState>` for future per-stream entity concurrency. Refactored all 25+ `state.lock()` call sites across tcp.rs, http.rs, main.rs, and 5 integration test files.

## One-liner

Global Mutex<AppState> eliminated; ConcurrentAppState with RwLock<PipelineEngine> + PLMutex<StateStore> + 8 independent small locks for metrics/throughput/latency/snapshot/backfill/event_log; StreamStore with DashMap defined for per-stream entity concurrency; all 643 tests green.

## What Shipped

### Task 1 -- Add crates + define ConcurrentAppState + StreamStore (commit `84199ac`)

**Cargo.toml**: Added `dashmap = "6.1"` and `parking_lot = "0.12"` dependencies.

**ConcurrentAppState** (src/server/tcp.rs): Replaces the old `AppState` struct. Fields:
- `engine: RwLock<PipelineEngine>` -- many concurrent reads on PUSH/GET hot path, write-only on REGISTER (D-04)
- `store: PLMutex<StateStore>` -- separate from engine; handlers that need the store acquire only this lock
- `event_log: PLMutex<Option<EventLog>>` -- independent from store/engine
- `metrics: PLMutex<Metrics>` -- small independent lock
- `snapshot_path: PathBuf` -- immutable after startup
- `snapshot_cycle/seq/last_base_seq/previous_base_seq: PLMutex<u64>` -- snapshot coordination
- `backfill_tracker: Arc<BackfillTracker>` -- unchanged (already Arc-wrapped)
- `backfill_complete: PLMutex<HashSet<(String, String)>>` -- independent lock
- `throughput: PLMutex<ThroughputTracker>` -- independent lock
- `latency: PLMutex<LatencyTracker>` -- independent lock

**SharedState** type alias: `Arc<ConcurrentAppState>` (was `Arc<Mutex<AppState>>`).

**make_concurrent_state()**: Public helper for constructing SharedState, used by main.rs and all test helpers.

**StreamStore** (src/state/store.rs): Per-stream entity storage with `DashMap<String, StreamEntityState>`, plus `parking_lot::Mutex<AHashSet<String>>` for dirty_keys and deleted_keys tracking. Includes `to_concurrent()` and `from_concurrent()` conversion methods for bridging with StateStore during snapshot serialization.

### Task 2 -- Refactor all state.lock() call sites (commit `9f3bf6e`)

**tcp.rs hot path handlers:**
- `handle_push_core_ex`: `engine.read()` + `store.lock()` + `event_log.lock()`, drops store/event_log before acquiring throughput/metrics/latency locks
- `handle_push_batch`: Same per-field lock pattern; metrics bump via `state.metrics.lock()` after releasing store/event_log
- `handle_sync_command(Get)`: `engine.read()` + `store.lock()`, separate latency lock
- `handle_sync_command(Set)`: `store.lock()` only, separate latency lock
- `handle_sync_command(Register)`: `engine.write()` for exclusive access, event_log and backfill locks separate
- `handle_sync_command(Mget)`: `engine.read()` + `store.lock()`
- `handle_mset`: `store.lock()` per chunk with yield between
- `run_backfill`: `engine.read()` + `store.lock()` per 64-event chunk

**main.rs background tasks:**
- Snapshot timer: `engine.read()` + `store.lock()` + individual snapshot_cycle/seq locks
- Eviction timer: `engine.read()` + `store.lock()`
- Fsync timer: `event_log.lock()`
- Compaction timer: `event_log.lock()` per stream with yield
- Startup recovery: individual lock acquisitions for engine, store, backfill_complete

**http.rs handlers:** All 13 HTTP handlers refactored to use appropriate individual locks (engine.read() for reads, engine.write() for register/delete, store.lock() for entity access, metrics.lock()/throughput.lock()/latency.lock() for observability).

**Integration tests:** All 5 integration test files (test_server, test_debug_ui, test_pipeline, test_push_coalescing, test_push_batch) migrated from `Arc::new(Mutex::new(AppState{..}))` to `make_concurrent_state()` pattern.

## Test Coverage

| Suite | Count | Status |
|-------|-------|--------|
| lib | 505 | PASS |
| test_batch_primitives | 17 | PASS |
| test_push_coalescing | 19 | PASS |
| test_push_batch | 10 | PASS |
| test_server | 31 | PASS |
| test_pipeline | 23 | PASS |
| test_debug_ui | 25 | PASS |
| test_snapshot | 7 | PASS |
| test_incremental_snapshot | 6 | PASS |
| **Total** | **643** | **ALL PASS** |

## Deviations from Plan

### [Rule 2 - Critical] BackfillStatus/BackfillTracker retain std::sync::Mutex

- **Found during:** Task 1 struct definition.
- **Issue:** The plan suggested moving all Mutex to parking_lot. However, `BackfillStatus.completed_at` and `BackfillTracker.tasks` use `std::sync::Mutex` with `unwrap_or_else(|e| e.into_inner())` poisoning recovery across async task boundaries. Changing these to parking_lot would alter the cross-task panic behavior.
- **Fix:** Kept `std::sync::Mutex` for these two fields only. All other locks use parking_lot. This is a defensive choice -- parking_lot's no-poisoning semantics means a panicked backfill task would leave the lock accessible (correct behavior), but the existing code already handles poisoning explicitly.
- **Files modified:** src/server/tcp.rs
- **Commit:** 84199ac

### [Rule 3 - Blocking] StateStore retained as primary storage behind PLMutex

- **Found during:** Task 2 call site refactoring.
- **Issue:** The plan's Option A/B for engine method refactoring would require changing all engine push/get_features method signatures from `&mut StateStore` to DashMap-based types -- a 500+ line refactor of pipeline.rs that would dwarf the tcp.rs changes. Option C (moving logic to tcp.rs) would duplicate complex cascade/fan-out logic.
- **Fix:** Kept StateStore behind `PLMutex<StateStore>` as a separate lock from engine. The engine methods still take `&mut StateStore` via `&mut *store` deref on the MutexGuard. The StreamStore with DashMap is defined and exported for future use (Plan 02 can optimize snapshot iteration). The key concurrency win is that engine reads (PUSH/GET hot path) no longer block metric/throughput/latency/snapshot writes.
- **Files modified:** src/server/tcp.rs, src/state/store.rs
- **Commit:** 9f3bf6e

## Verification

| Criterion | Result |
|-----------|--------|
| `grep "dashmap" Cargo.toml` | 1 match |
| `grep "parking_lot" Cargo.toml` | 1 match |
| `grep "ConcurrentAppState" src/server/tcp.rs` | 7 matches |
| `grep "StreamStore" src/state/store.rs` | 18 matches |
| `grep "DashMap" src/state/store.rs` | 20 matches |
| `grep "state.lock()" src/server/tcp.rs src/server/http.rs src/main.rs` (excluding comments) | 0 matches |
| `grep "Mutex::new(AppState" src/main.rs` | 0 matches |
| `grep "deny(clippy::await_holding_lock)" src/server/tcp.rs` | 1 match (line 15) |
| `cargo test` all suites | 643 passed, 0 failed |

## Self-Check: PASSED

- `Cargo.toml` contains dashmap and parking_lot: FOUND
- `src/server/tcp.rs` contains ConcurrentAppState: FOUND
- `src/state/store.rs` contains StreamStore and DashMap: FOUND
- Commit 84199ac reachable from HEAD: FOUND
- Commit 9f3bf6e reachable from HEAD: FOUND
- All 643 tests pass: VERIFIED
- Zero `state.lock()` in src/ (non-comment): VERIFIED
- C-7 gate `#![deny(clippy::await_holding_lock)]` preserved: VERIFIED
