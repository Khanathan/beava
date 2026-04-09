---
phase: 06-foundation
plan: 01
subsystem: state
tags: [entitystate, per-stream-grouping, snapshot-v4, ahashmap, ttl]

# Dependency graph
requires: []
provides:
  - "StreamEntityState struct with per-stream operators and last_event_at"
  - "EntityState restructured with streams: AHashMap<String, StreamEntityState>"
  - "Snapshot format v4 with SerializableStreamEntityState"
  - "get_or_create_stream, is_empty, remove_empty_entities helpers"
affects: [06-02, 06-03, 06-04, 07-ttl, 08-backfill, 09-snapshots]

# Tech tracking
tech-stack:
  added: []
  patterns: [per-stream-state-grouping, stream-scoped-ttl]

key-files:
  created: []
  modified:
    - src/state/store.rs
    - src/state/snapshot.rs
    - src/engine/pipeline.rs
    - src/state/eviction.rs
    - src/server/http.rs
    - tests/test_snapshot.rs

key-decisions:
  - "Per-stream entity TTL uses most-recent last_event_at across all streams for entity-level eviction"
  - "Borrow conflict in push() resolved by scoped borrows of entity.streams.get_mut() instead of long-lived stream_state reference"
  - "HTTP debug endpoint enhanced with stream name in live_operators JSON output"

patterns-established:
  - "Per-stream operator access: entity.get_or_create_stream(name).operators for scoped operator management"
  - "Scoped borrow pattern: { let stream_state = entity.streams.get_mut(name).unwrap(); ... } to avoid borrow conflicts with entity.static_features"

requirements-completed: [OPS-02]

# Metrics
duration: 33min
completed: 2026-04-09
---

# Phase 6 Plan 1: EntityState Per-Stream Restructure Summary

**Restructured EntityState from flat operator list to per-stream grouped AHashMap with v4 snapshot format for independent stream TTL management**

## Performance

- **Duration:** 33 min
- **Started:** 2026-04-09T22:57:26Z
- **Completed:** 2026-04-09T23:30:03Z
- **Tasks:** 2
- **Files modified:** 6

## Accomplishments
- EntityState now groups live operators by stream name using AHashMap<String, StreamEntityState>
- Each StreamEntityState tracks its own last_event_at independently, enabling per-stream TTL (OPS-02)
- Snapshot format bumped to v4; v3 snapshots gracefully rejected with clean startup
- Full test suite passes: 387 tests, 0 failures (341 lib + 11 pipeline + 28 server + 7 snapshot)

## Task Commits

Each task was committed atomically:

1. **Task 1: Restructure EntityState types and snapshot format** - `909d566` (feat)
   - Includes pipeline.rs, eviction.rs, http.rs, test_snapshot.rs updates needed for compilation

**Note:** Task 2 (pipeline engine verification) produced no additional file changes because Rust's whole-crate compilation model required all dependent files to be updated in Task 1. Task 2 verified the full test suite (387 tests) passes.

## Files Created/Modified
- `src/state/store.rs` - Added StreamEntityState, restructured EntityState with per-stream grouping, updated get_all_features/get_feature_value/clone_for_snapshot/restore_from_snapshot/remove_expired_entities
- `src/state/snapshot.rs` - Added SerializableStreamEntityState, restructured SerializableEntityState for v4, bumped SNAPSHOT_FORMAT_VERSION to 4
- `src/engine/pipeline.rs` - Updated push() to use entity.get_or_create_stream() and stream_state.operators; updated last_event_at to per-stream
- `src/state/eviction.rs` - Updated tests to use per-stream last_event_at API
- `src/server/http.rs` - Updated debug endpoint to iterate entity.streams for operator info, added stream name to debug JSON
- `tests/test_snapshot.rs` - Updated eviction integration tests to use per-stream API, updated corrupt data test version byte to 4

## Decisions Made
- Per-stream entity eviction uses the most-recent last_event_at across all streams (entity is evicted only when ALL streams are expired). This preserves backward compatibility with the existing TTL semantics while enabling future per-stream TTL in Phase 7.
- Resolved Rust borrow checker conflict in push() by using scoped borrows of `entity.streams.get_mut(name)` instead of a long-lived `stream_state` reference. This allows accessing `entity.static_features` between stream operations without lifetime conflicts.
- Enhanced HTTP debug endpoint to include `"stream"` field in live_operators JSON, improving observability of per-stream state grouping.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Pipeline, eviction, HTTP, and integration test changes included in Task 1 commit**
- **Found during:** Task 1 (EntityState restructure)
- **Issue:** Rust compiles the entire crate as a unit; changing EntityState struct fields causes compilation failures in all files that reference those fields (pipeline.rs, eviction.rs, http.rs, tests/test_snapshot.rs)
- **Fix:** Included all dependent file updates in the Task 1 commit to achieve a compiling state
- **Files modified:** src/engine/pipeline.rs, src/state/eviction.rs, src/server/http.rs, tests/test_snapshot.rs
- **Verification:** Full test suite (387 tests) passes with 0 failures
- **Committed in:** 909d566 (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Task 2 changes were pulled into Task 1 due to Rust's whole-crate compilation model. No scope creep; all changes are within plan scope.

## Issues Encountered
- Borrow checker conflict in pipeline.rs push() method: `entity.get_or_create_stream()` returns a mutable reference that conflicts with later immutable access to `entity.static_features`. Resolved by using scoped borrows that drop the stream_state reference before accessing static_features.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- EntityState per-stream grouping is the foundation for all subsequent v1.1 work
- Per-stream TTL (OPS-02) can now be implemented by comparing individual stream last_event_at values
- Pipeline DAG execution (Phase 7+) can cleanly isolate operators by stream name
- Schema evolution can target specific streams without affecting others

## Self-Check: PASSED

- All key files exist (store.rs, snapshot.rs, pipeline.rs, SUMMARY.md)
- Commit 909d566 verified in git log
- Full test suite: 387 tests, 0 failures

---
*Phase: 06-foundation*
*Completed: 2026-04-09*
