---
phase: 04-persistence-and-operational-readiness
plan: 01
subsystem: state
tags: [postcard, serialization, snapshot, eviction, ttl, ring-buffer]

# Dependency graph
requires:
  - phase: 01-core-engine
    provides: CountOp, SumOp, AvgOp operators with Serialize/Deserialize derives; RingBuffer with serde support
  - phase: 02-tcp-server
    provides: REGISTER command handler (tcp.rs) and RegisterRequest DTO (protocol.rs)
provides:
  - OperatorState enum replacing Box<dyn Operator> for full EntityState serializability
  - SnapshotState with save_snapshot/load_snapshot (postcard + version byte)
  - TTL eviction sweep via evict_expired_keys
  - PipelineEngine max_window_duration, list_streams, remove_stream, raw_register_json storage
  - StateStore clone_for_snapshot, restore_from_snapshot, remove_expired_entities
affects: [04-02-PLAN, 04-03-PLAN, 05-remaining-operators]

# Tech tracking
tech-stack:
  added: [postcard (serialization, already in Cargo.toml)]
  patterns: [version-prefixed snapshot format, String-based JSON storage for postcard compatibility, delegating enum pattern]

key-files:
  created:
    - src/state/eviction.rs
  modified:
    - src/state/snapshot.rs
    - src/state/store.rs
    - src/engine/pipeline.rs
    - src/server/tcp.rs
    - src/state/mod.rs

key-decisions:
  - "Use String (not serde_json::Value) for raw_register_json in SerializablePipeline -- postcard cannot serialize serde_json::Value (WontImplement error)"
  - "Store raw register JSON alongside stream definitions in PipelineEngine for snapshot pipeline persistence"
  - "Version byte 0x01 prefix on snapshot data; mismatched version returns None for clean fresh startup"

patterns-established:
  - "Delegating enum: OperatorState enum wraps concrete operator types, delegates push/read via match"
  - "Version-prefixed serialization: [1 byte version][postcard payload] for forward-compatible snapshots"
  - "AHashMap to Vec conversion: clone_for_snapshot converts AHashMap to Vec<(K,V)> since postcard cannot serialize AHashMap"

requirements-completed: [PERS-01, PERS-02, PERS-04, PERS-05]

# Metrics
duration: 5min
completed: 2026-04-09
---

# Phase 04 Plan 01: Serialization Foundation Summary

**OperatorState enum replaces Box<dyn Operator>, postcard snapshot save/load with versioning, TTL eviction sweep, and raw register JSON storage for pipeline persistence**

## Performance

- **Duration:** 5 min
- **Started:** 2026-04-09T19:02:33Z
- **Completed:** 2026-04-09T19:07:54Z
- **Tasks:** 2
- **Files modified:** 6

## Accomplishments
- Replaced Box<dyn Operator> with serializable OperatorState enum throughout codebase (snapshot.rs, store.rs, pipeline.rs)
- Implemented snapshot save/load with version-prefixed postcard serialization; corrupt/mismatched data returns None (no panics)
- Created TTL eviction sweep (evict_expired_keys) using 2x max_window_duration as TTL
- Added raw register JSON storage to PipelineEngine and wired it into TCP REGISTER handler for snapshot pipeline persistence
- 24 new tests across snapshot, eviction, and pipeline modules; total 245 tests passing

## Task Commits

Each task was committed atomically:

1. **Task 1: OperatorState enum and EntityState refactor** - `51d900c` (feat)
2. **Task 2: Snapshot save/load + eviction sweep + raw register JSON storage + unit tests** - `98b9451` (feat)

**Plan metadata:** (pending)

## Files Created/Modified
- `src/state/snapshot.rs` - OperatorState enum, SnapshotState/SerializableEntityState/SerializablePipeline types, save_snapshot/load_snapshot functions
- `src/state/store.rs` - EntityState uses OperatorState, clone_for_snapshot, restore_from_snapshot, remove_expired_entities
- `src/state/eviction.rs` - evict_expired_keys function with TTL multiplier
- `src/engine/pipeline.rs` - max_window_duration, list_streams, remove_stream, raw_register_jsons field, store/get_raw_register_json
- `src/server/tcp.rs` - REGISTER handler stores raw JSON payload via store_raw_register_json
- `src/state/mod.rs` - pub mod snapshot; pub mod eviction declarations

## Decisions Made
- Used String (not serde_json::Value) for raw_register_json in SerializablePipeline because postcard returns WontImplement for serde_json::Value serialization
- Raw register JSON stored in PipelineEngine via separate store_raw_register_json method (called after register) rather than modifying register() signature
- Version byte approach for snapshots: single byte prefix allows future format migrations without breaking existing snapshots

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed SerializablePipeline.raw_register_json type for postcard compatibility**
- **Found during:** Task 2 (snapshot round-trip tests)
- **Issue:** Plan specified `serde_json::Value` for raw_register_json field, but postcard returns `WontImplement` error when serializing serde_json::Value
- **Fix:** Changed field type from `serde_json::Value` to `String` in SerializablePipeline; callers serialize JSON to string before storing
- **Files modified:** src/state/snapshot.rs
- **Verification:** test_snapshot_state_roundtrip passes; all 13 snapshot tests pass
- **Committed in:** 98b9451

**2. [Rule 2 - Missing Critical] Wired raw register JSON storage into TCP REGISTER handler**
- **Found during:** Task 2 (raw register JSON storage)
- **Issue:** Plan specified store_raw_register_json method but did not explicitly specify wiring it into the TCP command handler
- **Fix:** Added payload.clone() and store_raw_register_json call to Command::Register handler in tcp.rs
- **Files modified:** src/server/tcp.rs
- **Verification:** Existing REGISTER tests continue to pass; get_raw_register_json tests verify storage
- **Committed in:** 98b9451

---

**Total deviations:** 2 auto-fixed (1 bug, 1 missing critical)
**Impact on plan:** Both auto-fixes necessary for correctness. No scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Snapshot save/load infrastructure ready for Plan 02 (periodic snapshot timer, startup recovery)
- Eviction sweep function ready for Plan 02 (periodic eviction timer)
- Raw register JSON storage enables pipeline persistence in snapshots
- PipelineEngine.max_window_duration enables TTL calculation for eviction

## Self-Check: PASSED

All files verified present. All commit hashes verified in git log.

---
*Phase: 04-persistence-and-operational-readiness*
*Completed: 2026-04-09*
