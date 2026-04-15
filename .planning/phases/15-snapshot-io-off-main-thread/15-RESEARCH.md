# Phase 15: Snapshot I/O off main thread - Research

**Researched:** 2026-04-11
**Domain:** Off-thread snapshot serialization via clone-and-spawn_blocking pattern in a tokio single-threaded (current_thread) Rust server
**Confidence:** HIGH

## Summary

Phase 15 moves snapshot serialization and disk I/O off the main event-loop thread. The current implementation already uses `spawn_blocking` for the write+fsync step (since main.rs:304), but the lock is held during the entire clone phase which includes iterating all dirty entities and deep-cloning their operator state. This is the stall window that blocks PUSH/GET.

The architecture is simpler than ROADMAP Phase 15 originally assumed: Phase 14 did NOT deliver per-shard state with shard-local snapshot files. It delivered `ConcurrentAppState` with a single `PLMutex<StateStore>`. So Phase 15 is a single clone-and-spawn pattern, not N per-shard parallel writes. No manifest protocol, no per-shard files, no shard coordination.

The key change: move from "lock -> clone dirty -> clear dirty -> release lock -> spawn_blocking(serialize + write)" (which is already nearly the current pattern) to adding (1) cycle serialization so two snapshots never overlap, (2) HTTP endpoint with wait/timeout semantics, (3) `.tmp` crash recovery hardening, and (4) a stress test proving <=5% throughput regression during writes.

**Primary recommendation:** Add an `AtomicBool` or `tokio::sync::Semaphore(1)` snapshot-in-flight guard, enhance the existing clone-then-spawn pattern with cycle skip logic, add `POST /snapshot?wait=true&timeout_ms=N`, and write a stress test that measures PUSH throughput DURING a snapshot write.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **D-01:** Acquire `StateStore` lock -> clone dirty entities + snapshot metadata -> release lock -> `spawn_blocking(|| serialize_and_write(cloned_state))`
- **D-02:** The clone step should be O(dirty_keys) not O(total_keys) -- clone only entities in the dirty set
- **D-03:** Use existing `tokio::task::spawn_blocking` -- no new crates, no new thread pool
- **D-04:** `max_blocking_threads` configuration: default to 2 (one for active snapshot, one spare for manual trigger)
- **D-05:** Never start a new snapshot cycle while the previous one is still writing
- **D-06:** Track in-flight snapshot via `AtomicBool` or `tokio::sync::Semaphore(1)` -- cheap, no lock
- **D-07:** If snapshot interval fires while previous cycle is active: skip, increment `snapshots_skipped` metric
- **D-08:** `/metrics` exposes `snapshots_skipped` counter -- alert if > 0
- **D-09:** Write to `.tmp` file -> fsync -> rename to final -> fsync parent dir
- **D-10:** On startup: if `.tmp` file exists without corresponding final file -> incomplete write -> ignore (roll back to previous snapshot)
- **D-11:** Preserve existing base + delta snapshot format and recovery logic
- **D-12:** `POST /snapshot?wait=true&timeout_ms=N` -- synchronous snapshot trigger
- **D-13:** Returns `200 {bytes, duration_ms}` on success, `408` on timeout, `409` if cycle already in progress
- **D-14:** Stress test: sustained PUSH load that DELIBERATELY overlaps a snapshot cycle
- **D-15:** PUSH throughput during snapshot must regress <=5% (was 15-25% in v1.2)
- **D-16:** Snapshot write time budget: <1 second per 100k entities

### Claude's Discretion
- Exact clone strategy (deep clone vs Arc-swap)
- Whether to use `oneshot` channel for completion notification
- Cleanup file patterns
- Test file organization

### Deferred Ideas (OUT OF SCOPE)
- Per-shard parallel snapshot writes -- requires full sharding (future milestone)
- Async compression (zstd) during snapshot write -- future optimization
- Snapshot to S3 Files -- see HORIZON-S3-FILES.md research
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| OPS-05 | Snapshot serialization runs off the main event-loop thread -- during a snapshot write, async PUSH throughput regresses by <=5% (was 15-25% on v1.2). Snapshot write completes within <1 second per 100k entities. | Clone-under-lock pattern with dirty-only cloning, AtomicBool cycle guard, spawn_blocking serialization, stress test during write |
</phase_requirements>

## Project Constraints (from CLAUDE.md)

- Rust codebase, single binary, zero external dependencies
- In-memory everything with periodic snapshots to local disk
- tokio runtime (currently `current_thread` flavor at `src/main.rs:26`)
- Custom binary TCP protocol on port 6400, HTTP management on port 6401
- Phase 14 delivered `ConcurrentAppState` with `PLMutex<StateStore>` (parking_lot mutex)
- All existing tests must remain green

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| tokio | 1.50 | `spawn_blocking`, runtime | Already in use [VERIFIED: Cargo.toml] |
| parking_lot | 0.12 | `PLMutex` for StateStore lock | Already in use [VERIFIED: Cargo.toml] |
| postcard | 1.1 | Snapshot serialization format | Already in use [VERIFIED: Cargo.toml] |

### Stack Additions
None. D-03 locks this: no new crates, no new thread pool. [VERIFIED: CONTEXT.md D-03]

## Architecture Patterns

### Current Snapshot Path (end-to-end)

The periodic snapshot timer lives in `src/main.rs:203-362`. The flow is: [VERIFIED: src/main.rs]

1. **30-second interval timer** fires (`tokio::time::interval(Duration::from_secs(30))`)
2. **Decide base vs delta**: check `cycle % full_snapshot_interval == 0` (default every 10th cycle)
3. **Acquire locks**: `engine.read()` + `store.lock()` + `snapshot_cycle.lock()` + `snapshot_seq.lock()`
4. **Clone state under lock**:
   - Base: `store.clone_for_snapshot_with_gc(&valid_features)` -- clones ALL entities
   - Delta: `store.clone_dirty_for_snapshot_with_gc(&valid_features)` -- clones only dirty entities
5. **Clear tracking**: `store.clear_dirty()` + `store.take_deleted()`
6. **Release all locks** (end of `let prepared` block)
7. **`spawn_blocking`**: serialize via `save_base_snapshot`/`save_delta_snapshot`, write to `.tmp`, fsync, rename, fsync dir, cleanup old files
8. **Await result**: log success/failure, update `metrics.snapshot_duration_ms`

**Key finding:** The existing code ALREADY does clone-then-spawn_blocking (since Phase 9). The lock is held for steps 3-6 only (clone + clear dirty). Serialization and I/O are already off-thread. [VERIFIED: src/main.rs:211-345]

### What Phase 15 Actually Needs to Change

Given the existing architecture, Phase 15's changes are:

1. **Cycle serialization (D-05/D-06/D-07):** Add an `AtomicBool` or `Semaphore(1)` to prevent overlapping snapshot cycles. Currently, if `spawn_blocking` takes longer than 30 seconds, the next timer tick will fire and start a new clone while the previous write is still in progress. This can cause the dirty set to be cleared twice and produce incomplete snapshots.

2. **HTTP endpoint enhancement (D-12/D-13):** Current `POST /snapshot` (http.rs:499-607) always does a full base snapshot synchronously. Enhance to support `?wait=true&timeout_ms=N`, return 409 if cycle in progress, 408 on timeout.

3. **`max_blocking_threads` (D-04):** Configure tokio runtime with `max_blocking_threads(2)`. Currently uses default (512). This prevents unbounded blocking thread creation.

4. **`.tmp` file cleanup on startup (D-10):** Scan for orphaned `.tmp` files and remove them.

5. **`snapshots_skipped` metric (D-08):** Add counter to Metrics struct, expose in `/metrics`.

6. **Stress test (D-14/D-15):** Bench that measures PUSH throughput DURING a snapshot write.

### Pattern: Cycle Serialization

```rust
// In ConcurrentAppState (tcp.rs):
pub snapshot_in_flight: AtomicBool,  // or Semaphore(1)

// In periodic timer:
if snap_state.snapshot_in_flight.compare_exchange(
    false, true, Ordering::AcqRel, Ordering::Acquire
).is_err() {
    snap_state.metrics.lock().snapshots_skipped += 1;
    eprintln!("Snapshot cycle skipped (previous still writing)");
    continue;
}

// After spawn_blocking completes (in the .await handler):
snap_state.snapshot_in_flight.store(false, Ordering::Release);
```
[ASSUMED -- based on standard AtomicBool CAS pattern]

**Recommendation: Use `AtomicBool` over `Semaphore(1)`.** Reasons:
- `AtomicBool` is zero-cost, no allocation, no async overhead
- `Semaphore(1)` would work but adds unnecessary tokio sync primitive for a binary flag
- The cycle check is always on the same (main) thread, so no contention concerns

### Pattern: HTTP Snapshot with Wait/Timeout

```rust
// POST /snapshot?wait=true&timeout_ms=5000
async fn trigger_snapshot(
    State(state): State<SharedState>,
    axum::extract::Query(params): axum::extract::Query<SnapshotParams>,
) -> impl IntoResponse {
    // Check cycle guard
    if !state.snapshot_in_flight.compare_exchange(false, true, ...).is_ok() {
        return (StatusCode::CONFLICT, Json(json!({"error": "snapshot cycle in progress"}))).into_response();
    }
    
    // Clone state under lock (same as periodic path)
    let cloned = { /* lock, clone, clear dirty, unlock */ };
    
    // spawn_blocking for serialize + write
    let handle = tokio::task::spawn_blocking(move || { /* serialize, write, fsync */ });
    
    if params.wait.unwrap_or(false) {
        let timeout = Duration::from_millis(params.timeout_ms.unwrap_or(30000));
        match tokio::time::timeout(timeout, handle).await {
            Ok(Ok(Ok(size))) => {
                state.snapshot_in_flight.store(false, Ordering::Release);
                (StatusCode::OK, Json(json!({"bytes": size, "duration_ms": elapsed}))).into_response()
            }
            Ok(Ok(Err(e))) => {
                state.snapshot_in_flight.store(false, Ordering::Release);
                (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e.to_string()}))).into_response()
            }
            Err(_) => {
                // Timeout -- snapshot still running in background
                (StatusCode::REQUEST_TIMEOUT, Json(json!({"error": "snapshot timed out"}))).into_response()
            }
        }
    } else {
        // Fire-and-forget: release guard when task completes (via spawn wrapper)
        tokio::spawn(async move {
            let _ = handle.await;
            state.snapshot_in_flight.store(false, Ordering::Release);
        });
        (StatusCode::ACCEPTED, Json(json!({"status": "snapshot started"}))).into_response()
    }
}
```
[ASSUMED -- design pattern, not verified against specific docs]

### Pattern: Shared Clone Logic (DRY)

Both the periodic timer (main.rs) and the HTTP trigger (http.rs) currently duplicate the clone logic. Extract into a shared function:

```rust
// In snapshot.rs or a new module:
pub fn prepare_snapshot_data(
    engine: &PipelineEngine,
    store: &mut StateStore,
    is_full: bool,
    seq: u64,
    last_base_seq: u64,
    backfill_complete: &[(String, String)],
) -> SnapshotData { ... }
```
[ASSUMED -- architectural recommendation]

### Data Cloned Under Lock

For a delta snapshot (the common case), the lock hold clones: [VERIFIED: src/state/store.rs:265-296]

1. **Dirty entities** -- `clone_dirty_for_snapshot_with_gc()` iterates `self.entities`, filters by `dirty_keys.contains()`, deep-clones each matching `EntityState` (streams + operators + static_features)
2. **Deleted keys** -- `take_deleted()` drains `deleted_keys` into a `Vec<String>`
3. **Dirty set clear** -- `clear_dirty()` empties `dirty_keys`

For a base snapshot (every 10th cycle): [VERIFIED: src/state/store.rs:308-338]

1. **All entities** -- `clone_for_snapshot_with_gc()` deep-clones everything
2. **Pipeline metadata** -- iterates `engine.list_streams()` + `engine.list_views()`, clones JSON strings
3. **Backfill markers** -- clones `backfill_complete` HashSet

**Clone cost analysis:**
- `EntityState` derives `Clone` [VERIFIED: src/state/store.rs:51]
- `StreamEntityState` derives `Clone` [VERIFIED: src/state/store.rs:33]
- `OperatorState` derives `Clone` [VERIFIED: src/state/snapshot.rs:36]
- Each operator (CountOp, SumOp, etc.) must implement Clone via `#[derive(Clone, Serialize, Deserialize)]` [VERIFIED: snapshot.rs:36]
- Deep clone cost per entity: proportional to number of operators x ring buffer size per operator. For a medium pipeline (~10 operators per entity), this is ~5-10KB of data to clone per entity.
- For delta with 1000 dirty keys: ~5-10MB clone, should take <1ms under lock [ASSUMED -- based on memcpy speed estimates]

### Dirty Key Tracking

Dirty keys are populated by: [VERIFIED: src/state/store.rs:210-228]

1. `mark_dirty(&mut self, key: &str)` -- called after each entity mutation in push handlers
2. `mark_dirty_many(keys)` -- batch variant from Phase 12's handle_push_batch
3. Keys removed from dirty by `mark_deleted(key)` -- eviction removes from dirty set

Dirty keys are consumed by:
1. `clone_dirty_for_snapshot_with_gc()` -- reads dirty_keys to filter entities
2. `clear_dirty()` -- empties the set after clone
3. `take_deleted()` -- drains deleted keys

**Critical ordering:** `clear_dirty()` and `take_deleted()` MUST happen while the store lock is still held (same lock acquisition as the clone). This is already the case in the current code. [VERIFIED: src/main.rs:252-253, 274]

### tokio Runtime Configuration

Currently: `#[tokio::main(flavor = "current_thread")]` [VERIFIED: src/main.rs:26]

This is important: `spawn_blocking` on a `current_thread` runtime creates OS threads from a blocking thread pool. The pool defaults to 512 max threads. D-04 wants `max_blocking_threads(2)`.

To configure `max_blocking_threads`, you need the builder API instead of the attribute macro:

```rust
// Replace:
#[tokio::main(flavor = "current_thread")]
async fn main() { ... }

// With:
fn main() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(2)
        .build()
        .unwrap();
    rt.block_on(async { /* former main body */ });
}
```
[VERIFIED: tokio docs -- `max_blocking_threads` is only available on Builder, not the macro]

**Caveat:** With `max_blocking_threads(2)` and one snapshot in flight, only 1 spare blocking thread remains. The HTTP manual trigger (D-12) could be blocked waiting for a thread if the periodic snapshot is already using one. This is acceptable per D-04 ("one for active snapshot, one spare for manual trigger").

### Existing HTTP POST /snapshot

Current implementation at `src/server/http.rs:499-607`: [VERIFIED]
- Always writes a full base snapshot (not delta)
- Already uses clone-then-spawn_blocking pattern
- Already does `.tmp` -> fsync -> rename -> fsync dir
- Does NOT check for in-flight periodic snapshot (no cycle guard)
- Does NOT support `?wait=true&timeout_ms=N` query params
- Returns `200 {status, bytes, duration_ms}` on success

Phase 15 must:
1. Add cycle guard check (return 409 if in-flight)
2. Add query param parsing for `wait` and `timeout_ms`
3. Share the clone logic with the periodic timer
4. Support both wait (200/408) and fire-and-forget (202) modes

### Crash Recovery for .tmp Files (D-10)

Current behavior: `.tmp` files are created, fsynced, then renamed. If the process crashes between write and rename, a `.tmp` file remains. Current startup code (`load_incremental_snapshots` in main.rs:473-549) only looks for files matching `tally.snapshot.base.*` and `tally.snapshot.delta.*` patterns -- `.tmp` files are naturally ignored. [VERIFIED: src/main.rs:480-494]

Phase 15 should add explicit cleanup of orphaned `.tmp` files on startup for cleanliness, but this is NOT a correctness issue -- they're already ignored by the recovery path.

### Cleanup Old Snapshots

Current `cleanup_old_snapshots` (main.rs:451-470) scans for `tally.snapshot.base.*` and `tally.snapshot.delta.*` files with seq < cutoff. [VERIFIED]

No changes needed to the cleanup logic itself. The `.tmp` cleanup is additive.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Cycle serialization | Custom locking/signaling | `AtomicBool` CAS | One atomic is sufficient for binary in-flight tracking |
| Blocking thread pool | Custom thread pool | `tokio::task::spawn_blocking` | Already in use, well-tested |
| Snapshot timeout | Manual timer tracking | `tokio::time::timeout` wrapping `JoinHandle` | Composable, correct cancellation semantics |
| Query params | Manual URL parsing | `axum::extract::Query<T>` with `#[derive(Deserialize)]` | Already used elsewhere in axum handlers |

## Common Pitfalls

### Pitfall 1: Overlapping Snapshot Cycles (H-4)
**What goes wrong:** At sustained high throughput (500k eps), dirty keys accumulate faster than serialize+write completes. Without cycle guard, the next 30s tick fires while the previous write is still in `spawn_blocking`. The new tick clears `dirty_keys` for a new clone, but the previous write hasn't finished -- entities modified between clone and clear are lost from the previous snapshot AND cleared from dirty, so they're in neither snapshot.
**Why it happens:** `clear_dirty()` runs under lock at clone time, not at write-completion time.
**How to avoid:** `AtomicBool` cycle guard (D-06). Skip + increment counter (D-07).
**Warning signs:** `snapshots_skipped > 0` in metrics.

### Pitfall 2: Holding Store Lock Across Await
**What goes wrong:** If the clone logic accidentally holds `store.lock()` across the `spawn_blocking(..).await`, the entire StateStore is locked for the duration of serialization + disk I/O.
**Why it happens:** The clone and spawn are in the same async block; easy to accidentally extend the lock scope.
**How to avoid:** The current code already structures this correctly with a `let prepared = { ... };` block that drops the lock. Preserve this pattern. The `#![deny(clippy::await_holding_lock)]` gate catches `std::sync::MutexGuard` but NOT `parking_lot::MutexGuard` across await -- parking_lot guards are `!Send` so they'll produce a compile error on multi-thread runtime but NOT on current_thread. [VERIFIED: clippy lint only applies to std::sync types]
**Warning signs:** PUSH latency spikes to seconds during snapshot write.

### Pitfall 3: AtomicBool Not Cleared on Panic (H-4 variant)
**What goes wrong:** If `spawn_blocking` panics (e.g., postcard serialization bug), the AtomicBool stays `true` forever, blocking all future snapshots.
**Why it happens:** The `store(false)` call is in the success/error path but not the panic path.
**How to avoid:** Use a RAII guard pattern or always clear in the outer match arm that handles `Err(JoinError)` (which is the panic case). The current code already handles this: `Err(e) => eprintln!("Snapshot task panicked: {}", e)` at main.rs:359 -- just add the `store(false)` there too.
**Warning signs:** After a snapshot panic, all subsequent snapshots are skipped and `snapshots_skipped` climbs forever.

### Pitfall 4: HTTP Trigger vs Periodic Timer Race
**What goes wrong:** HTTP `POST /snapshot` and the periodic timer both try to acquire the cycle guard simultaneously. Without the guard, both clone and write, producing two snapshot files with the same or adjacent sequence numbers.
**How to avoid:** Both paths check the same `AtomicBool`. HTTP returns 409 if guard is held. Periodic timer skips cycle.

### Pitfall 5: max_blocking_threads Requires Builder API
**What goes wrong:** `#[tokio::main(flavor = "current_thread")]` doesn't support `max_blocking_threads`. The macro silently ignores unknown attributes.
**How to avoid:** Switch to explicit `tokio::runtime::Builder::new_current_thread()` with `.max_blocking_threads(2)`. [VERIFIED: tokio macro doesn't expose this option]

### Pitfall 6: Stress Test Must Overlap Snapshot
**What goes wrong:** Running a throughput bench followed by a snapshot (sequential) measures nothing. The regression target is throughput DURING a snapshot write.
**How to avoid:** The stress test must: start sustained PUSH load -> trigger snapshot mid-load via HTTP -> measure throughput in the window where the snapshot is writing -> compare to throughput before/after.

## Code Examples

### Example 1: AtomicBool Cycle Guard with RAII Cleanup

```rust
use std::sync::atomic::{AtomicBool, Ordering};

// Add to ConcurrentAppState:
pub snapshot_in_flight: AtomicBool,

// Guard that auto-clears on drop (prevents pitfall 3):
struct SnapshotGuard<'a>(&'a AtomicBool);
impl<'a> Drop for SnapshotGuard<'a> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

// In snapshot timer:
if snap_state.snapshot_in_flight
    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
    .is_err()
{
    snap_state.metrics.lock().snapshots_skipped += 1;
    continue;
}
// Guard ensures cleanup even on panic
let _guard = SnapshotGuard(&snap_state.snapshot_in_flight);

// ... clone under lock, spawn_blocking ...
// Guard dropped at end of loop iteration or on early return
```
[ASSUMED -- standard RAII guard pattern]

### Example 2: Runtime Builder with max_blocking_threads

```rust
fn main() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(2)
        .build()
        .expect("Failed to build tokio runtime");
    
    rt.block_on(async_main());
}

async fn async_main() {
    // ... current main() body ...
}
```
[VERIFIED: tokio::runtime::Builder API supports max_blocking_threads]

### Example 3: axum Query Params for Snapshot Endpoint

```rust
#[derive(serde::Deserialize)]
struct SnapshotParams {
    #[serde(default)]
    wait: Option<bool>,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

async fn trigger_snapshot(
    State(state): State<SharedState>,
    axum::extract::Query(params): axum::extract::Query<SnapshotParams>,
) -> impl IntoResponse {
    // ...
}
```
[VERIFIED: axum 0.8 Query extractor with Deserialize]

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Rust built-in `#[test]` + `#[tokio::test]` |
| Config file | Cargo.toml (test deps: tempfile, sha2) |
| Quick run command | `cargo test --lib` |
| Full suite command | `cargo test` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| OPS-05-a | Cycle guard prevents overlapping snapshots | unit | `cargo test test_snapshot_cycle_guard -x` | No -- Wave 0 |
| OPS-05-b | Periodic snapshot uses dirty-only clone | integration | `cargo test --test test_incremental_snapshot -x` | Yes (existing, needs extension) |
| OPS-05-c | HTTP snapshot returns 409 when cycle in progress | integration | `cargo test test_snapshot_conflict -x` | No -- Wave 0 |
| OPS-05-d | HTTP snapshot with wait=true returns bytes/duration | integration | `cargo test test_snapshot_wait -x` | No -- Wave 0 |
| OPS-05-e | HTTP snapshot with timeout returns 408 | integration | `cargo test test_snapshot_timeout -x` | No -- Wave 0 |
| OPS-05-f | Orphaned .tmp files cleaned on startup | integration | `cargo test test_tmp_cleanup -x` | No -- Wave 0 |
| OPS-05-g | snapshots_skipped metric incremented on skip | unit | `cargo test test_snapshots_skipped_metric -x` | No -- Wave 0 |
| OPS-05-h | PUSH throughput <=5% regression during write | stress/bench | `python3 benchmark/tally-throughput/bench.py` | manual-only (bench harness exists) |

### Sampling Rate
- **Per task commit:** `cargo test --lib`
- **Per wave merge:** `cargo test`
- **Phase gate:** Full suite green + stress test passes throughput gate

### Wave 0 Gaps
- [ ] `tests/test_snapshot_offthread.rs` -- covers OPS-05-a through OPS-05-g
- [ ] Stress test harness for OPS-05-h (Python bench + concurrent HTTP snapshot trigger)

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | No | -- |
| V3 Session Management | No | -- |
| V4 Access Control | No | -- |
| V5 Input Validation | Yes (HTTP query params) | axum Query<T> with Deserialize -- rejects malformed params |
| V6 Cryptography | No | -- |

### Known Threat Patterns

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Denial of service via rapid POST /snapshot | Denial of Service | AtomicBool guard + 409 response prevents concurrent cycles |
| Orphaned .tmp files fill disk | Denial of Service | Startup cleanup + cleanup_old_snapshots |

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | Delta clone of 1000 dirty entities (~10 ops each) takes <1ms under lock | Architecture Patterns | If clone is slower, lock hold time could cause measurable PUSH regression -- would need profiling |
| A2 | RAII guard pattern for AtomicBool is the right approach | Code Examples | Low risk -- standard Rust pattern, alternative is explicit clear in all code paths |
| A3 | `parking_lot::MutexGuard` is `!Send` and will error on multi-thread runtime but NOT on current_thread | Pitfall 2 | If wrong, could silently hold lock across await on current_thread -- mitigated by code structure |

## Open Questions

1. **Should the HTTP trigger also support delta snapshots?**
   - What we know: Current HTTP trigger always does a full base snapshot. CONTEXT.md doesn't specify.
   - What's unclear: Whether users want quick delta snapshots via HTTP or always full.
   - Recommendation: Keep HTTP as full-base-only (simpler, matches current behavior). Delta is only for periodic optimization.

2. **Should `max_blocking_threads(2)` apply globally or just be documented?**
   - What we know: D-04 says "default to 2". The tokio builder sets this globally for the runtime.
   - What's unclear: Whether other code paths need blocking threads (event log fsync currently does NOT use spawn_blocking -- it runs inline under the event_log lock).
   - Recommendation: Set to 2. No other code path uses spawn_blocking. [VERIFIED: only snapshot code uses spawn_blocking]

## Sources

### Primary (HIGH confidence)
- `src/main.rs` -- periodic snapshot timer, clone-then-spawn pattern, runtime attribute
- `src/state/store.rs` -- StateStore, dirty_keys, clone methods, EntityState Clone derive
- `src/state/snapshot.rs` -- OperatorState Clone derive, save_base_snapshot, save_delta_snapshot, v6 format
- `src/server/tcp.rs` -- ConcurrentAppState struct, SharedState type, PLMutex usage
- `src/server/http.rs` -- existing POST /snapshot handler, route registration
- `Cargo.toml` -- tokio features (rt, rt-multi-thread), parking_lot 0.12, postcard 1.1

### Secondary (MEDIUM confidence)
- `.planning/research/PITFALLS.md` -- H-4 (dirty-set backpressure), H-5 (fsync ordering), C-3 (cross-shard consistency -- not applicable since no sharding)
- `.planning/phases/15-snapshot-io-off-main-thread/15-CONTEXT.md` -- locked decisions D-01 through D-16

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- no new crates, all verified in Cargo.toml
- Architecture: HIGH -- existing clone-then-spawn pattern verified in source, changes are incremental
- Pitfalls: HIGH -- all pitfalls grounded in verified code paths and existing PITFALLS.md research

**Research date:** 2026-04-11
**Valid until:** 2026-05-11 (stable domain, no external dependency changes)
