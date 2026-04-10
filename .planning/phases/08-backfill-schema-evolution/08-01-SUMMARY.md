---
phase: 08-backfill-schema-evolution
plan: 01
subsystem: engine
tags: [schema-evolution, backfill, pipeline, snapshot-gc, protocol]

# Dependency graph
requires:
  - phase: 07-composable-pipeline
    provides: PipelineEngine with DAG, register(), push_with_cascade()
provides:
  - SchemaDiff struct and diff_features() for non-destructive stream re-registration
  - backfill: bool field on all stateful FeatureDef variants
  - clone_for_snapshot_with_gc() for lazy GC of removed operator state
  - valid_features_map() helper for snapshot callers
  - REGISTER command returns diff JSON (status/added/removed/backfilling)
  - Python SDK backfill=False kwarg on all stateful operators
affects: [08-02-backfill-execution, snapshot-persistence, python-sdk]

# Tech tracking
tech-stack:
  added: []
  patterns: [schema-diff-on-reregister, lazy-gc-on-snapshot, conditional-json-omission]

key-files:
  created: []
  modified:
    - src/engine/pipeline.rs
    - src/server/protocol.rs
    - src/state/store.rs
    - src/server/tcp.rs
    - src/main.rs
    - src/server/http.rs
    - src/state/eviction.rs
    - python/tally/_operators.py
    - python/tests/test_operators.py

key-decisions:
  - "Schema diff uses std::mem::discriminant for type equality -- no false positives from field changes"
  - "Lazy GC on snapshot (not on re-register) to avoid blocking the hot path"
  - "backfill field omitted from Derive variant (no state, computed on read)"
  - "Both snapshot callers (main.rs periodic + http.rs trigger) use clone_for_snapshot_with_gc"

patterns-established:
  - "Schema diff on re-registration: diff_features() classifies added/removed/unchanged"
  - "Lazy GC via clone_for_snapshot_with_gc: filter orphan operators during snapshot serialization"
  - "Conditional JSON omission in Python SDK: backfill only serialized when True"

requirements-completed: [SCHM-01, SCHM-02]

# Metrics
duration: 11min
completed: 2026-04-10
---

# Phase 08 Plan 01: Schema Diff and Backfill Type System Summary

**Schema diff engine for non-destructive stream evolution with lazy GC, diff-aware REGISTER response, and Python SDK backfill kwarg**

## Performance

- **Duration:** 11 min
- **Started:** 2026-04-10T02:38:20Z
- **Completed:** 2026-04-10T02:49:21Z
- **Tasks:** 2
- **Files modified:** 9

## Accomplishments
- Schema diff engine classifies features as added/removed/unchanged on re-registration with type change rejection
- Lazy GC filters orphan operators during snapshot serialization (both periodic timer and manual trigger)
- REGISTER command returns JSON diff summary for streams (status/added/removed/backfilling)
- Python SDK operators accept backfill=False kwarg with conditional omission in to_json()
- Existing operator state preserved after re-registration with added features (verified by test)
- All 412 Rust unit tests pass, all 53 Python operator tests pass

## Task Commits

Each task was committed atomically:

1. **Task 1: Schema diff engine + backfill field on FeatureDef + lazy GC** - `710c80d` (feat)
2. **Task 2: REGISTER handler diff response + Python SDK backfill kwarg** - `5e44343` (feat)

## Files Created/Modified
- `src/engine/pipeline.rs` - SchemaDiff struct, diff_features(), same_operator_type(), get_backfill_flag(), valid_features_map(), backfill field on all stateful FeatureDef variants, register() returns SchemaDiff
- `src/server/protocol.rs` - backfill: Option<bool> on FeatureDefRequest, passed through convert_register_request()
- `src/state/store.rs` - clone_for_snapshot_with_gc() filters orphan operators by valid features map
- `src/server/tcp.rs` - REGISTER handler returns diff JSON for streams, updated test
- `src/main.rs` - Periodic snapshot timer uses clone_for_snapshot_with_gc
- `src/server/http.rs` - trigger_snapshot uses clone_for_snapshot_with_gc, register() return type handled
- `src/state/eviction.rs` - backfill: false added to test FeatureDef constructions
- `python/tally/_operators.py` - backfill=False kwarg on Count, Sum, Avg, Min, Max, Last, DistinctCount
- `python/tests/test_operators.py` - 11 new backfill tests

## Decisions Made
- Schema diff uses std::mem::discriminant for type equality check -- simple, correct, no false positives from field value changes
- Lazy GC happens during snapshot serialization (not on re-register) to avoid blocking the push hot path
- backfill field not added to Derive variant (no state, computed on read) or Lookup (views)
- Both snapshot callers (main.rs periodic timer + http.rs POST /snapshot) wired to use clone_for_snapshot_with_gc
- REGISTER returns diff JSON only for streams; views continue returning empty response

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed test_register_valid_stream assertion**
- **Found during:** Task 2
- **Issue:** Existing test asserted REGISTER returns empty vec, but now returns diff JSON
- **Fix:** Updated assertion to verify diff JSON content (status=ok, added contains feature name)
- **Files modified:** src/server/tcp.rs
- **Verification:** Test passes with new assertion
- **Committed in:** 5e44343 (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (1 bug fix)
**Impact on plan:** Necessary test update for changed return type. No scope creep.

## Issues Encountered
None -- plan executed smoothly.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Schema diff engine ready for Plan 02 (backfill execution)
- backfill flag on FeatureDef provides the metadata Plan 02 needs to trigger event log replay
- valid_features_map() available for any future GC or introspection needs

---
*Phase: 08-backfill-schema-evolution*
*Completed: 2026-04-10*
