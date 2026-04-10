---
phase: 08-backfill-schema-evolution
plan: 02
subsystem: engine
tags: [backfill, replay, cooperative-yielding, event-timestamps, idempotent-restart, schema-evolution]

# Dependency graph
requires:
  - phase: 08-backfill-schema-evolution
    plan: 01
    provides: SchemaDiff, backfill flag on FeatureDef, lazy GC, valid_features_map()
provides:
  - push_for_backfill() method for targeted operator replay with event timestamps
  - run_backfill() async function with 64-event cooperative yield chunks
  - BackfillStatus/BackfillTracker structs for progress tracking
  - GET /debug/backfill HTTP endpoint for backfill status monitoring
  - backfill_complete persistence in SnapshotState for crash recovery
  - Startup incomplete backfill detection and re-spawn logic
affects: [snapshot-persistence, http-management-api, integration-tests]

# Tech tracking
tech-stack:
  added: []
  patterns: [cooperative-backfill-replay, idempotent-restart-via-operator-clear, event-timestamp-bucketing]

key-files:
  created: []
  modified:
    - src/engine/pipeline.rs
    - src/server/tcp.rs
    - src/server/http.rs
    - src/state/snapshot.rs
    - src/main.rs
    - tests/test_pipeline.rs

key-decisions:
  - "run_backfill clears existing operator state for backfill features before replay -- ensures idempotent restart produces same result"
  - "Snapshot format bumped to v5 for backfill_complete field with serde(default) backward compat"
  - "Backfill spawned via tokio::spawn from synchronous REGISTER handler (no .await needed)"
  - "Event log entries read under lock, then lock released before spawning backfill task"
  - "Pre-existing crate::error::TallyError binary crate reference fixed to tally::error::TallyError"

patterns-established:
  - "Cooperative async backfill: 64-event chunks with yield_now() between -- same pattern as MSET cooperative yielding"
  - "Operator clearing before replay: ensures idempotent restart by removing stale partial state"
  - "AppState destructuring for split borrows in integration tests: let AppState { ref engine, ref mut store, .. } = *app"

requirements-completed: [SCHM-03, SCHM-04, SCHM-05]

# Metrics
duration: 11min
completed: 2026-04-10
---

# Phase 08 Plan 02: Backfill Execution from Event Log Summary

**Cooperative backfill replay from event log with 64-event yielding, event timestamp determinism, completion persistence, idempotent restart detection, and full integration test coverage**

## Performance

- **Duration:** 11 min
- **Started:** 2026-04-10T02:51:33Z
- **Completed:** 2026-04-10T03:03:30Z
- **Tasks:** 2
- **Files modified:** 6

## Accomplishments
- `push_for_backfill()` pushes to only specified operators using event timestamp (not wall clock), leaving existing operators untouched
- `run_backfill()` async function processes events in 64-event cooperative yield chunks, preventing live traffic starvation
- Backfill operators cleared before replay for idempotent restart (re-run after crash produces identical results)
- REGISTER handler detects backfill features, reads event log, spawns background backfill task
- BackfillStatus/BackfillTracker track active and completed backfills with progress counters
- `backfill_complete` field persisted in SnapshotState (format v5) with `serde(default)` backward compatibility
- Startup detects incomplete backfills by comparing registered features with backfill=true against backfill_complete set
- GET /debug/backfill HTTP endpoint returns status of all backfill tasks
- 4 new integration tests prove deterministic replay, event timestamp bucketing, schema evolution add/remove, and idempotent restart
- All 415 Rust library tests pass, all 23 integration tests pass

## Task Commits

Each task was committed atomically:

1. **Task 1: Backfill engine + push_for_backfill + BackfillTracker + cooperative replay + completion persistence + idempotent restart** - `e24feee` (feat)
2. **Task 2: HTTP /debug/backfill endpoint + integration tests** - `57adfb8` (feat)

## Files Created/Modified
- `src/engine/pipeline.rs` - Added push_for_backfill() method: targeted operator push using event timestamps, where-clause support, stream filter evaluation
- `src/server/tcp.rs` - BackfillStatus, BackfillTracker structs, run_backfill() async function with 64-event cooperative yielding and operator clearing, REGISTER handler backfill spawning, AppState extended with backfill_tracker and backfill_complete
- `src/server/http.rs` - GET /debug/backfill endpoint returning task status JSON, backfill_complete field included in trigger_snapshot SnapshotState
- `src/state/snapshot.rs` - SnapshotState.backfill_complete field with serde(default), format version bumped to 5, roundtrip test for backfill_complete
- `src/main.rs` - AppState initialization with backfill_tracker/backfill_complete, snapshot restore of backfill markers, incomplete backfill detection and re-spawn on startup, periodic snapshot includes backfill_complete, fixed crate::error reference
- `tests/test_pipeline.rs` - 4 new integration tests (backfill_replay_deterministic, event_timestamps_not_wall_clock, schema_evolution_add_remove, backfill_idempotent_restart), fixed missing backfill fields in existing test FeatureDef constructions

## Decisions Made
- run_backfill clears existing operator state for backfill features before replay -- ensures idempotent restart produces same result regardless of partial state from crashed prior attempt
- Snapshot format bumped to v5 (from v4) with serde(default) for backward compat -- old snapshots deserialize with empty backfill_complete vec
- Backfill spawned from synchronous REGISTER handler via tokio::spawn (does not need .await) -- entries read under lock, task spawned outside lock scope
- Pre-existing binary crate path error (crate::error vs tally::error) fixed as Rule 3 blocking issue

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Fixed crate::error::TallyError reference in main.rs**
- **Found during:** Task 1
- **Issue:** Binary crate (main.rs) used `crate::error::TallyError` which refers to the binary namespace, not the library
- **Fix:** Changed to `tally::error::TallyError`
- **Files modified:** src/main.rs
- **Committed in:** e24feee (Task 1)

**2. [Rule 3 - Blocking] Fixed missing backfill fields in test_pipeline.rs**
- **Found during:** Task 2
- **Issue:** Integration test file had FeatureDef constructions without the `backfill: bool` field added in Plan 01
- **Fix:** Added `backfill: false` to all existing FeatureDef constructions
- **Files modified:** tests/test_pipeline.rs
- **Committed in:** 57adfb8 (Task 2)

**3. [Rule 2 - Critical] Added operator clearing for idempotent restart**
- **Found during:** Task 2 (test_backfill_idempotent_restart failure)
- **Issue:** Re-running backfill on restored snapshot with existing operator state caused double-counting (1100 instead of 550)
- **Fix:** run_backfill clears existing operators for backfill features before replay, ensuring deterministic results
- **Files modified:** src/server/tcp.rs
- **Committed in:** 57adfb8 (Task 2)

---

**Total deviations:** 3 auto-fixed (1 blocking reference fix, 1 blocking missing fields, 1 critical correctness fix)
**Impact on plan:** Operator clearing was essential for correctness of idempotent restart. No scope creep.

## Issues Encountered
None beyond the auto-fixed deviations.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Backfill execution complete: users can add features with backfill=true and have them automatically populated from event log
- Schema evolution (add/remove/preserve) fully operational across registration, backfill, and restart cycles
- Phase 08 (Backfill & Schema Evolution) is complete

---
*Phase: 08-backfill-schema-evolution*
*Completed: 2026-04-10*
