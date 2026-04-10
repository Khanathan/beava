---
phase: 09-incremental-snapshots
plan: 02
subsystem: snapshot, server
tags: [snapshot, delta, recovery, tcp, http, eviction]
requires:
  - 09-01-SUMMARY  # v6 snapshot format, dirty tracking, clone_dirty_for_snapshot_with_gc, load_snapshot_file, load_legacy_v5
provides:
  - Periodic incremental snapshot timer (cycle-driven base/delta rotation)
  - Startup recovery from base + N deltas with legacy v5 migration fallback
  - mark_dirty wired into PUSH, SET, MSET, and backfill mutation paths
  - mark_deleted wired into TTL eviction with two-phase apply
  - HTTP POST /snapshot writes a v6 full base snapshot with atomic rename
  - cleanup_old_snapshots helper removes stale files after each base
affects:
  - src/main.rs
  - src/server/http.rs
  - src/server/tcp.rs
  - src/state/eviction.rs
  - tests/test_incremental_snapshot.rs (new)
  - tests/test_pipeline.rs (helper struct update)
tech-stack:
  added: []
  patterns:
    - Cycle counter with modular base/delta decision inside a single lock acquisition
    - Two-phase eviction: collect plan, then apply mutations (avoids borrow checker conflict)
    - Delta-rot optimization: skip writing when no dirty/deleted keys in a cycle
    - Atomic snapshot write via tmp file + rename (inherited from Plan 01)
    - Legacy-first scan with v6 fallback at startup
key-files:
  created:
    - tests/test_incremental_snapshot.rs
  modified:
    - src/main.rs
    - src/server/http.rs
    - src/server/tcp.rs
    - src/state/eviction.rs
    - tests/test_pipeline.rs
decisions:
  - Isolated integration tests in new file tests/test_incremental_snapshot.rs instead of extending tests/test_snapshot.rs (pre-existing Phase 8 compile errors in that file are deferred)
  - Single-lock cycle logic: acquire lock once per snapshot tick to read cycle counter, clone state, clear dirty tracking, and advance counter -- avoids race on the single-threaded runtime
  - Delta-rot skip: if neither dirty nor deleted keys exist in a cycle the delta file is not written, reducing churn on idle workloads
  - cleanup_old_snapshots only runs after a successful base write so deltas are never deleted before their owning base exists on disk
  - TALLY_FULL_SNAPSHOT_INTERVAL defaults to 10 (every 10th cycle is a base) with env override
metrics:
  duration_minutes: 110
  tasks_completed: 2
  tasks_total: 2
  completed_date: 2026-04-09
---

# Phase 9 Plan 02: Server Wiring and Lifecycle Summary

Wire the Plan 01 incremental snapshot data layer into the live server so PUSH/SET/MSET/backfill mark entities dirty, TTL eviction marks entities deleted, the periodic timer rotates between full bases and deltas, startup recovers from base + deltas (with v5 migration fallback), and HTTP POST /snapshot writes a full base.

## What Was Built

### Task 1 -- Dirty/Deleted Tracking in Mutation Paths

Added `store.mark_dirty(key)` at every mutation site in `src/server/tcp.rs`:

1. PUSH primary key (after operator update)
2. Cascade target keys (cross-stream fan-out via secondary keys)
3. PUSH fan-out targets (keyed fan-out in if-let with key_val extraction)
4. SET handler (after the feature-map iteration, before success response)
5. MSET handler (per chunk, during cooperative yielding loop)
6. Backfill run_backfill (per replayed event, extracting the key field)

Restructured `src/state/eviction.rs::evict_expired_stream_entries` to a two-phase pattern to resolve the borrow-checker conflict between mutating per-entity streams and calling `store.mark_deleted(key)`:

- **Phase 1 (immutable borrows):** Collect eviction plan -- per entity, compute `streams_to_remove` and `will_be_empty = remaining_streams == 0 && static_features.is_empty()`.
- **Phase 2 (mutable borrows):** For each entry in the plan, call `store.mark_deleted(key)` when `will_be_empty`, then apply the actual stream removals.
- **Phase 3:** Existing `remove_empty_entities` call (unchanged).

Added 3 unit tests in `src/state/eviction.rs`:
- `test_eviction_marks_fully_removed_entity_deleted`
- `test_eviction_does_not_mark_deleted_when_static_features_remain`
- `test_eviction_does_not_mark_deleted_when_other_stream_remains`

**Commit:** `52230fb feat(09-02): wire dirty/deleted tracking into mutation paths (OPS-03)`

### Task 2 -- Periodic Timer, Startup Recovery, HTTP Trigger, Integration Tests

**AppState additions** in `src/server/tcp.rs`:
```rust
pub snapshot_cycle: u64,   // incremented after each successful snapshot write
pub snapshot_seq: u64,     // next sequence number; max existing + 1 at startup
```

**Environment config** in `src/main.rs`:
```
TALLY_FULL_SNAPSHOT_INTERVAL   // default 10; every Nth cycle is a base
```

**Periodic timer** (`src/main.rs`): the snapshot loop now acquires the state lock once per tick, reads the cycle counter, decides base vs delta (`cycle % interval == 0`), clones the appropriate state (full `clone_for_snapshot_with_gc` for base, `clone_dirty_for_snapshot_with_gc` + `take_deleted` for delta), clears the dirty set, advances the cycle counter and `snapshot_seq`, then releases the lock before the actual disk write.

- Base writes go through `save_base_snapshot(path, state, header)`.
- Delta writes go through `save_delta_snapshot(path, state, header)`.
- Delta cycles with no dirty keys AND no deleted keys are skipped entirely (no file written, counters still advance).
- File naming: `tally.snapshot.base.{seq:010}` / `tally.snapshot.delta.{seq:010}` (zero-padded 10 digits for lexical ordering).
- After each successful base write, `cleanup_old_snapshots(dir, current_base_seq)` removes files with seq < current.

**Startup recovery** (`src/main.rs::load_incremental_snapshots`):

1. Scan snapshot dir for `tally.snapshot.{base,delta}.{seq}` files
2. Pick the highest-seq base as the starting point; load via `load_snapshot_file`
3. Apply every delta with `seq > base_seq` in ascending order via `store.apply_delta`
4. If no v6 files found, fall back to `load_legacy_v5(&legacy_path)` (v5 single-file migration -- the loaded state becomes the first base on next snapshot tick)
5. Populate `snapshot_seq = max_seen_seq + 1`
6. Clear dirty/deleted sets after load so the first tick does not re-emit loaded data

**HTTP manual trigger** (`src/server/http.rs::trigger_snapshot`):

- Replaced the legacy single-file write with a v6 `save_base_snapshot` call.
- Reads `snapshot_seq` from AppState, builds `BaseSnapshotState` with header, clears dirty/deleted tracking, increments `snapshot_seq`, and writes to `tally.snapshot.base.{seq:010}`.

**Integration tests** in the new file `tests/test_incremental_snapshot.rs` (6 tests, all passing):

1. `test_incremental_snapshot_delta_contains_only_dirty_entities` -- writes a base with u1 & u2, pushes only u3, writes a delta, verifies delta contains exactly {u3}.
2. `test_incremental_snapshot_recovery_base_plus_two_deltas` -- base {u1,u2}, delta1 {u3}, delta2 {u1 updated}, recover and verify merged features.
3. `test_incremental_snapshot_deleted_keys_removed_on_recovery` -- base contains u1, delta marks u1 deleted, recovery results in empty state.
4. `test_legacy_v5_migration_loads_as_initial_base` -- writes v5 single-file snapshot, recovery loads it.
5. `test_full_snapshot_cycle_picks_base_at_zero_and_every_n` -- interval=3, verifies base at cycle 0, deltas at 1/2, base at 3.
6. `test_eviction_marks_deleted_and_delta_includes_it` -- TTL eviction removes entity, next delta snapshot reflects deletion on recovery.

Test helper `recover_from_dir()` mirrors the production `load_incremental_snapshots` logic.

**Commit:** `10a0685 feat(09-02): wire incremental snapshot timer and startup recovery (OPS-03/OPS-04)`

## Verification Results

| Check | Result |
| ----- | ------ |
| `cargo build` | clean (no warnings) |
| `cargo test --lib` | 455 passed / 0 failed |
| `cargo test --test test_incremental_snapshot` | 6 passed / 0 failed |
| `cargo test --test test_pipeline` | passing |

Acceptance-criteria grep checks (all pass):

| Pattern | File | Count |
| ------- | ---- | ----- |
| `snapshot_cycle` | src/main.rs | 5 |
| `snapshot_seq` | src/main.rs | 6 |
| `TALLY_FULL_SNAPSHOT_INTERVAL` | src/main.rs | 1 |
| `load_incremental_snapshots` | src/main.rs | 2 |
| `cleanup_old_snapshots` | src/main.rs | 2 |
| `tally.snapshot.base.` | src/main.rs | 4 |
| `tally.snapshot.delta.` | src/main.rs | 3 |
| `save_base_snapshot` | src/main.rs | 2 |
| `save_delta_snapshot` | src/main.rs | 2 |
| `load_snapshot_file` | src/main.rs | 3 |
| `load_legacy_v5` | src/main.rs | 2 |
| `mark_dirty` | src/server/tcp.rs | 6 |
| `mark_deleted` | src/state/eviction.rs | 5 |
| `save_base_snapshot` | src/server/http.rs | 1 |
| `snapshot_cycle`/`snapshot_seq` | src/server/tcp.rs (AppState) | 4 |
| `test_incremental_snapshot` | tests/test_incremental_snapshot.rs | 6 |

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Integration tests moved to a new file**

- **Found during:** Task 2 verification setup
- **Issue:** The plan asks integration tests to live in `tests/test_snapshot.rs`, but that file has pre-existing Phase 8 compile errors (missing `backfill_complete`, missing `backfill` fields) already logged in `.planning/phases/09-incremental-snapshots/deferred-items.md`. Modifying that file would force fixing out-of-scope Phase 8 drift.
- **Fix:** Created a new, isolated integration test file `tests/test_incremental_snapshot.rs` containing all 6 required tests. The acceptance criterion "file contains `test_incremental_snapshot`" is satisfied by the new file's function names; the `cargo test --test test_incremental_snapshot` target is used for verification.
- **Files created:** `tests/test_incremental_snapshot.rs`
- **Commit:** `10a0685`

**2. [Rule 2 - Correctness] Eviction borrow-checker restructuring**

- **Found during:** Task 1 implementation
- **Issue:** Calling `store.mark_deleted(key)` inside the eviction loop while holding a mutable borrow on the entity produced a borrow-checker conflict (both mutate `StateStore`).
- **Fix:** Two-phase implementation -- collect eviction plan with immutable borrows, then apply mutations (including `mark_deleted`) in a second pass.
- **Files modified:** `src/state/eviction.rs`
- **Commit:** `52230fb`

**3. [Rule 2 - Correctness] Delta-rot skip for empty cycles**

- **Found during:** Task 2 timer design
- **Issue:** Plan's naive timer wrote a delta file every tick even if no dirty/deleted keys existed, producing empty files on idle workloads and wasting disk/inode churn.
- **Fix:** Skip the disk write when both `dirty.is_empty()` and `deleted.is_empty()`; still advance `snapshot_cycle` so the next base interval is honored.
- **Files modified:** `src/main.rs`
- **Commit:** `10a0685`

**4. [Rule 2 - Correctness] cleanup ordering**

- **Found during:** Task 2 cleanup helper design
- **Issue:** Running `cleanup_old_snapshots` unconditionally could delete deltas before a new base was safely on disk, leaving recovery impossible after a crash mid-cycle.
- **Fix:** `cleanup_old_snapshots` runs only inside the base-write success branch. A failed base write leaves old files intact.
- **Files modified:** `src/main.rs`
- **Commit:** `10a0685`

## Pre-existing Items Not Touched

Per the Phase 9 planning notes and `deferred-items.md`, the following files still have pre-existing Phase 8 drift that is out of scope for this plan and is NOT addressed here:

- `tests/test_snapshot.rs` -- missing `backfill_complete` and `backfill` fields in AppState literal (Phase 8 drift)
- `tests/test_server.rs` -- missing AppState fields (Phase 8 drift)

These files are documented in `.planning/phases/09-incremental-snapshots/deferred-items.md` and should be addressed in a dedicated cleanup plan.

## Threat Register Mitigations Applied

| Threat ID | Mitigation |
| --------- | ---------- |
| T-09-04 (Tampering on load) | `load_incremental_snapshots` uses strict filename parsing (`tally.snapshot.{base\|delta}.{seq}`); any parse failure or postcard deserialization error causes the file to be skipped with a `continue`. |
| T-09-05 (DoS via cleanup) | `cleanup_old_snapshots` runs only after a successful base write; filesystem errors are logged and ignored; max ~10 deltas between bases bounds the deletion scope. |
| T-09-06 (Eviction race) | Single-threaded tokio runtime (`current_thread`); eviction runs inside a held state lock; `mark_deleted` cannot race with other mutations. |
| T-09-07 (Info disclosure via snapshot files) | Accepted -- same risk as legacy v5; no new exposure; users protect data directory permissions. |

## Key Decisions

1. **Cycle logic under a single lock** -- all cycle state (read counter, clone state, clear dirty, advance counter) happens inside one lock acquisition to make the rotation race-free on the single-threaded runtime.
2. **Skip empty deltas** -- idle cycles write nothing, saving inodes and disk I/O.
3. **Cleanup only after successful base** -- prevents orphaned deltas after a crash.
4. **Legacy-v5 fallback** -- if no v6 files exist, fall back to `load_legacy_v5`; the loaded state becomes the first base on next snapshot tick without an explicit migration step.
5. **New isolated test file** -- sidestep pre-existing Phase 8 compile errors in `tests/test_snapshot.rs`.

## Self-Check: PASSED

**Files verified to exist:**
- FOUND: src/main.rs (modified)
- FOUND: src/server/http.rs (modified)
- FOUND: src/server/tcp.rs (modified)
- FOUND: src/state/eviction.rs (modified)
- FOUND: tests/test_pipeline.rs (modified)
- FOUND: tests/test_incremental_snapshot.rs (created)

**Commits verified to exist:**
- FOUND: 52230fb feat(09-02): wire dirty/deleted tracking into mutation paths (OPS-03)
- FOUND: 10a0685 feat(09-02): wire incremental snapshot timer and startup recovery (OPS-03/OPS-04)

**Tests verified to pass:**
- FOUND: cargo build (clean, no warnings)
- FOUND: cargo test --lib -> 455 passed / 0 failed
- FOUND: cargo test --test test_incremental_snapshot -> 6 passed / 0 failed
