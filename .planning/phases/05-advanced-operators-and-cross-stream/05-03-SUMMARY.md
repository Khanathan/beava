---
phase: 05-advanced-operators-and-cross-stream
plan: 03
subsystem: engine
tags: [rust, cross-stream, views, lookups, fan-out, distinct-count, pipeline, tcp]

# Dependency graph
requires:
  - phase: 05-advanced-operators-and-cross-stream plan 01
    provides: MinOp, MaxOp, LastOp, where-clause filtering, FeatureDef variants, OperatorState enum
  - phase: 05-advanced-operators-and-cross-stream plan 02
    provides: DistinctCountOp, Hll struct, hll module
provides:
  - DistinctCountOp fully wired through OperatorState/FeatureDef/protocol/snapshot/HTTP
  - ViewDefinition type with ViewFeatureDef::Derive and ViewFeatureDef::Lookup
  - Cross-stream view evaluation in GET path with qualified field resolution
  - Cross-key lookup via StateStore.get_feature_value point-reads
  - Event fan-out in PUSH handler to secondary streams
  - convert_view_register_request for view type registration
  - Multi-stream entity coexistence (operators from different streams per entity)
affects: [python-sdk, integration-tests]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Per-entity multi-stream operator coexistence: operators from different streams accumulate in one entity, no clobbering"
    - "Qualified field resolution: features populated as both 'feature_name' and 'Stream.feature_name' for view derives"
    - "Fan-out iteration: PUSH handler iterates fan_out_targets, pushes to secondary streams with matching key_field"
    - "Lookup foreign key resolution: check 'last_{on_field}' then '{on_field}' features for stored foreign key"

key-files:
  created: []
  modified:
    - src/engine/pipeline.rs
    - src/server/protocol.rs
    - src/server/tcp.rs
    - src/server/http.rs
    - src/state/snapshot.rs
    - src/state/store.rs
    - tests/test_snapshot.rs

key-decisions:
  - "Multi-stream entity reconciliation: operators from different streams coexist per entity; push only adds missing operators for current stream, never clears others"
  - "PUSH returns primary stream features only; GET returns all streams + views + qualified names"
  - "Lookup foreign key resolution uses 'last_{on_field}' or '{on_field}' feature name convention"
  - "Fan-out skips primary stream (no double-push) and empty key values"
  - "View registration uses definition_type='view' in RegisterRequest to dispatch to convert_view_register_request"
  - "Snapshot version stays at 3 (DistinctCount variant already added in snapshot.rs by Plan 02 prep)"

patterns-established:
  - "fan_out_targets() returns (stream_name, key_field) pairs for PUSH fan-out iteration"
  - "convert_view_register_request: parse 'StreamName.feature_name' target into (stream, feature) tuple"
  - "ViewFeatureDef enum: Derive for cross-stream expressions, Lookup for cross-key point-reads"

requirements-completed: [OPS-04, XSTR-01, XSTR-02, XSTR-03]

# Metrics
duration: 8min
completed: 2026-04-09
---

# Phase 05 Plan 03: Cross-Stream Views, Cross-Key Lookups, Event Fan-Out Summary

**DistinctCountOp fully wired through the stack, cross-stream views with qualified field resolution, cross-key lookups via StateStore, and event fan-out from single PUSH to multiple entity keys**

## Performance

- **Duration:** 8 min
- **Started:** 2026-04-09T20:52:57Z
- **Completed:** 2026-04-09T21:00:59Z
- **Tasks:** 2
- **Files modified:** 7

## Accomplishments
- Wired DistinctCountOp into full protocol/HTTP/snapshot stack (distinct_count in convert_register_request, HTTP get_pipeline, snapshot OperatorState)
- Implemented ViewDefinition with cross-stream derives (qualified field resolution) and cross-key lookups (StateStore point-reads)
- Implemented event fan-out in PUSH handler: single event with multiple key fields updates all matching streams
- Fixed multi-stream entity reconciliation so operators from different streams coexist per entity without clobbering
- 376 total tests passing (337 lib + 11 protocol + 28 integration + 7 snapshot), 7 new fan-out tests

## Task Commits

Each task was committed atomically:

1. **Task 1: DistinctCount wiring, ViewDefinition, cross-stream view evaluation, and lookup resolution** - `29fc35e` (feat)
2. **Task 2: Event fan-out in PUSH handler and end-to-end integration tests** - `0caa00c` (feat)

_Note: TDD tasks -- tests written first (RED), then implementation (GREEN), verified passing._

## Files Created/Modified
- `src/engine/pipeline.rs` - ViewDefinition, ViewFeatureDef types, view registration, get_features with view evaluation, multi-stream operator reconciliation, fan_out_targets
- `src/server/protocol.rs` - distinct_count branch in convert_register_request, convert_view_register_request function, definition_type field in RegisterRequest
- `src/server/tcp.rs` - Fan-out logic in PUSH handler, view registration dispatch in REGISTER, 7 fan-out/integration tests
- `src/server/http.rs` - DistinctCount match arm in get_pipeline, view-aware create_pipeline
- `src/state/snapshot.rs` - DistinctCount OperatorState tests (push/read/roundtrip)
- `src/state/store.rs` - get_feature_value for cross-key lookup point-reads
- `tests/test_snapshot.rs` - Fixed version byte (2->3) for DistinctCount variant

## Decisions Made
- Multi-stream entity reconciliation: push only adds missing operators for current stream, never clears operators from other streams -- enables cross-stream features per entity key
- PUSH returns primary stream features only per CLAUDE.md spec; GET returns all streams + views + qualified names
- Lookup foreign key resolution checks features named "last_{on_field}" first, then "{on_field}" -- matches the pattern of storing st.last("merchant_id") as "last_merchant_id"
- Fan-out iterates fan_out_targets (bounded by stream count <10), skips primary stream and empty key values (T-5-09 mitigation)
- View registration dispatched via definition_type="view" in RegisterRequest, maintaining backward compatibility for stream registration

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed multi-stream entity operator clobbering**
- **Found during:** Task 1 (view evaluation tests)
- **Issue:** Pushing to different streams for the same entity key caused operator reconciliation to clear and rebuild all operators, destroying state from other streams
- **Fix:** Changed operator reconciliation to only add missing operators for the current stream, leaving existing operators from other streams intact
- **Files modified:** src/engine/pipeline.rs
- **Verification:** test_view_derive_resolves_qualified_fields_from_two_streams passes
- **Committed in:** 29fc35e (Task 1 commit)

**2. [Rule 1 - Bug] Fixed test_snapshot.rs version byte mismatch**
- **Found during:** Task 1 (test verification)
- **Issue:** tests/test_snapshot.rs referenced version byte 0x02 but snapshot format was bumped to 3 for DistinctCount variant
- **Fix:** Updated version byte from 0x02 to 0x03 in test_snapshot_corrupt_data_returns_none
- **Files modified:** tests/test_snapshot.rs
- **Verification:** cargo test passes
- **Committed in:** 29fc35e (Task 1 commit)

---

**Total deviations:** 2 auto-fixed (2 bugs)
**Impact on plan:** Both auto-fixes necessary for correctness. The multi-stream entity fix is essential for cross-stream features to work. No scope creep.

## Issues Encountered
None.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- All Phase 5 requirements complete: OPS-01 through OPS-05, XSTR-01 through XSTR-03
- Full CLAUDE.md operator set is functional: count, sum, avg, min, max, distinct_count, last, derive, lookup
- Cross-stream views evaluate on GET with qualified field resolution
- Cross-key lookups resolve via StateStore point-reads
- Event fan-out updates multiple entity keys from single PUSH
- Ready for Python SDK phase (Phase 6) to expose these features via SDK decorators

## Self-Check: PASSED

All 7 modified files exist on disk. Both task commits (29fc35e, 0caa00c) verified in git log. 383 tests passing (337 lib + 11 protocol + 28 integration + 7 snapshot), 0 failures.

---
*Phase: 05-advanced-operators-and-cross-stream*
*Completed: 2026-04-09*
