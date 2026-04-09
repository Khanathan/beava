---
phase: 04-persistence-and-operational-readiness
plan: 02
subsystem: state
tags: [snapshot, persistence, eviction, ttl, crash-recovery, tokio, spawn-blocking, atomic-rename]

# Dependency graph
requires:
  - phase: 04-persistence-and-operational-readiness
    plan: 01
    provides: OperatorState enum, SnapshotState, save_snapshot/load_snapshot, evict_expired_keys, clone_for_snapshot, restore_from_snapshot, raw_register_json storage
provides:
  - Snapshot recovery on startup from disk
  - Periodic snapshot timer (30s) with clone-then-spawn_blocking and atomic rename
  - Periodic eviction timer (60s) with configurable TTL multiplier
  - TALLY_SNAPSHOT_PATH and TALLY_TTL_MULTIPLIER environment variables
  - Integration tests validating snapshot round-trip, version mismatch, corruption, eviction, atomic write
affects: [04-03-PLAN]

# Tech tracking
tech-stack:
  added: [tempfile (dev-dependency for test temp directories)]
  patterns: [clone-then-spawn_blocking for non-blocking serialization, atomic rename for crash-safe writes, destructured AppState borrow for eviction timer]

key-files:
  created:
    - tests/test_snapshot.rs
  modified:
    - src/main.rs
    - Cargo.toml

key-decisions:
  - "Serialize serde_json::Value to String via serde_json::to_string for SerializablePipeline.raw_register_json (postcard compatibility bridge)"
  - "Parse raw_register_json String back to serde_json::Value then to RegisterRequest on snapshot load (two-step deserialization)"
  - "Re-store raw_register_json in PipelineEngine after snapshot restore so subsequent snapshot cycles include pipeline definitions"

patterns-established:
  - "Clone-then-spawn_blocking: clone state under brief lock, serialize on blocking thread pool to avoid blocking event loop"
  - "Atomic rename: write to .tmp, rename to final path for crash-safe snapshot persistence"
  - "Instant::now() capture before spawn_blocking: timing measurement ready for metrics wiring in Plan 03"

requirements-completed: [PERS-01, PERS-03, PERS-04, PERS-05]

# Metrics
duration: 3min
completed: 2026-04-09
---

# Phase 04 Plan 02: Snapshot Wiring and Eviction Timers Summary

**Startup snapshot recovery, 30s periodic snapshot with clone-then-spawn_blocking atomic writes, 60s eviction timer, and 7 integration tests validating the full persistence cycle**

## Performance

- **Duration:** 3 min
- **Started:** 2026-04-09T19:11:33Z
- **Completed:** 2026-04-09T19:14:44Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- Wired snapshot recovery on startup: loads snapshot file, restores entity state, re-registers pipeline definitions from stored JSON
- Added periodic snapshot timer (30s) using clone-then-spawn_blocking pattern with atomic rename for crash safety
- Added periodic eviction timer (60s) using evict_expired_keys with configurable TTL multiplier
- Added 7 integration tests covering snapshot round-trip, version mismatch, corruption, empty bytes, eviction behavior, and atomic write pattern
- Total test count: 280 (245 unit + 17 server + 11 pipeline + 7 snapshot)

## Task Commits

Each task was committed atomically:

1. **Task 1: Wire snapshot recovery, snapshot timer, and eviction timer into main.rs** - `27c4c1e` (feat)
2. **Task 2: Integration tests for snapshot persistence and eviction** - `83c0b33` (test)

**Plan metadata:** (pending)

## Files Created/Modified
- `src/main.rs` - Startup snapshot recovery, periodic snapshot timer (30s), periodic eviction timer (60s), env var parsing for TALLY_SNAPSHOT_PATH and TALLY_TTL_MULTIPLIER
- `tests/test_snapshot.rs` - 7 integration tests for snapshot persistence and TTL eviction
- `Cargo.toml` - Added tempfile dev-dependency for test temp directories

## Decisions Made
- Used two-step deserialization for snapshot pipeline restore: String -> serde_json::Value -> RegisterRequest (because SerializablePipeline stores raw_register_json as String for postcard compatibility, but RegisterRequest parsing needs serde_json::Value)
- Re-store raw_register_json in PipelineEngine after snapshot restore so the next snapshot cycle can persist pipeline definitions again
- Used destructured AppState borrow pattern (ref engine, ref mut store) in eviction timer to satisfy Rust borrow checker (same pattern as tcp.rs command handlers)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed Rust borrow checker conflict in eviction timer**
- **Found during:** Task 1 (eviction timer implementation)
- **Issue:** `evict_expired_keys(&mut app.store, &app.engine, ...)` causes E0502: cannot borrow `app` as immutable and mutable simultaneously
- **Fix:** Destructured AppState with `let AppState { ref engine, ref mut store } = *app;` before calling evict_expired_keys
- **Files modified:** src/main.rs
- **Verification:** `cargo build` succeeds
- **Committed in:** 27c4c1e

---

**Total deviations:** 1 auto-fixed (1 bug)
**Impact on plan:** Standard Rust borrow checker fix. No scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Snapshot persistence fully operational: startup recovery + periodic writes + atomic crash safety
- Eviction timer running: idle keys reclaimed automatically based on max window duration * TTL multiplier
- Plan 03 can wire snapshot_duration_ms metric: Instant::now() timing capture already in place in the Ok(Ok(_)) arm of the snapshot timer
- All 280 tests passing

---
*Phase: 04-persistence-and-operational-readiness*
*Completed: 2026-04-09*
