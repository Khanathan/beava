---
phase: 07-composable-pipeline
plan: 01
subsystem: engine
tags: [rust, pipeline, keyless-streams, composable, petgraph, filter]

requires:
  - phase: 06-foundation
    provides: StreamDefinition with entity_ttl/history_ttl, per-stream EntityState, event log

provides:
  - StreamDefinition with optional key_field (keyless stream support)
  - StreamDefinition with depends_on field (DAG edge declaration)
  - StreamDefinition with filter field (stream-level event filtering)
  - RegisterRequest with optional key_field, depends_on, filter
  - Keyless stream validation (rejects windowed operators)
  - Filter evaluation before operator processing in push()
  - fan_out_targets excludes keyless streams
  - petgraph dependency (ready for DAG construction)

affects: [07-02, 07-03, 07-04, composable-pipeline]

tech-stack:
  added: [petgraph 0.8]
  patterns: [optional-key-field, stream-level-filter, keyless-validation]

key-files:
  created: []
  modified:
    - Cargo.toml
    - src/engine/pipeline.rs
    - src/server/protocol.rs
    - src/server/tcp.rs
    - src/server/http.rs
    - src/main.rs
    - src/state/eviction.rs
    - tests/test_pipeline.rs
    - tests/test_snapshot.rs

key-decisions:
  - "key_field changed to Option<String> -- None means keyless stream, Some means keyed"
  - "Keyless streams reject all windowed operators at registration (count, sum, avg, min, max, distinct_count, last) -- only derive allowed"
  - "Keyless stream push returns empty FeatureMap immediately (no entity state created)"
  - "Stream-level filter evaluated before keyless check and key extraction in push()"
  - "fan_out_targets uses filter_map to exclude keyless streams (T-07-03)"
  - "SerializablePipeline.key_field uses unwrap_or_default() for keyless streams (empty string in snapshot)"
  - "convert_view_register_request extracts key_field with ok_or_else (views always require a key)"

patterns-established:
  - "Optional key_field pattern: None = keyless, Some = keyed -- used throughout engine"
  - "Stream-level filter: pre-parsed Expr evaluated before any operator processing"
  - "Keyless validation: matches! macro on FeatureDef variants for windowed operator detection"

requirements-completed: [PIPE-01, PIPE-02, PIPE-05]

duration: 10min
completed: 2026-04-10
---

# Phase 7 Plan 1: Composable Pipeline Foundation Summary

**Keyless stream support with optional key_field, depends_on DAG edges, and stream-level filter expression evaluation in Rust engine**

## Performance

- **Duration:** 10 min
- **Started:** 2026-04-10T01:29:02Z
- **Completed:** 2026-04-10T01:39:19Z
- **Tasks:** 2
- **Files modified:** 10

## Accomplishments
- StreamDefinition, RegisterRequest, and PipelineEngine updated with optional key_field, depends_on, and filter fields
- Keyless streams validated at registration (windowed operators rejected) and handled correctly in push (empty FeatureMap returned)
- Stream-level filter expression parsed at registration time and evaluated before operator processing
- fan_out_targets excludes keyless streams for correct fan-out behavior
- All 447 tests pass including 15 new tests (9 in pipeline.rs, 6 in protocol.rs)
- petgraph 0.8 added as dependency for upcoming DAG work

## Task Commits

Each task was committed atomically:

1. **Task 1: Write failing tests for keyless streams, type changes, and filter validation** - `b9746b2` (test)
2. **Task 2: Update types and implement keyless stream + filter logic to pass all tests** - `5437601` (feat)

## Files Created/Modified
- `Cargo.toml` - Added petgraph 0.8 dependency
- `src/engine/pipeline.rs` - StreamDefinition with Option<String> key_field, depends_on, filter; keyless validation in register(); filter + keyless handling in push(); fan_out_targets excludes keyless
- `src/server/protocol.rs` - RegisterRequest with optional key_field, depends_on, filter; convert_register_request handles optional key_field and parses filter; convert_view_register_request extracts key_field
- `src/server/tcp.rs` - Updated primary_key_field extraction to use as_deref() for Option; updated StreamDefinition constructions in tests
- `src/server/http.rs` - SerializablePipeline uses unwrap_or_default() for Option<String> key_field
- `src/main.rs` - SerializablePipeline uses unwrap_or_default() for Option<String> key_field
- `src/state/eviction.rs` - Updated StreamDefinition constructions in tests
- `tests/test_pipeline.rs` - Updated StreamDefinition constructions with new fields
- `tests/test_snapshot.rs` - Updated StreamDefinition constructions with new fields

## Decisions Made
- key_field changed to Option<String> with None = keyless, Some = keyed for maximum flexibility
- Keyless streams reject ALL windowed operators (not just count) -- consistent principle that keyless = no entity state
- Stream-level filter evaluated early in push() before key extraction -- filtered events skip all processing
- SerializablePipeline stores empty string for keyless streams' key_field (backward compat with snapshot format)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Fixed ViewDefinition key_field type mismatch**
- **Found during:** Task 2 (implementation)
- **Issue:** Python batch script inadvertently changed ViewDefinition.key_field from String to Option<String>. ViewDefinition always requires a key_field.
- **Fix:** Reverted ViewDefinition key_field back to String; updated convert_view_register_request to extract from Option with ok_or_else
- **Files modified:** src/engine/pipeline.rs, src/server/protocol.rs
- **Verification:** All tests pass
- **Committed in:** 5437601

**2. [Rule 3 - Blocking] Fixed SerializablePipeline key_field in http.rs and main.rs**
- **Found during:** Task 2 (implementation)
- **Issue:** SerializablePipeline.key_field is String but stream.key_field is now Option<String>
- **Fix:** Used unwrap_or_default() to convert Option<String> to String
- **Files modified:** src/server/http.rs, src/main.rs
- **Verification:** All tests pass including test_server integration tests
- **Committed in:** 5437601

**3. [Rule 3 - Blocking] Fixed eviction.rs StreamDefinition constructions**
- **Found during:** Task 2 (implementation)
- **Issue:** Eviction tests construct StreamDefinition with old field signature
- **Fix:** Updated all StreamDefinition constructions with Option key_field and depends_on/filter fields
- **Files modified:** src/state/eviction.rs
- **Verification:** All tests pass
- **Committed in:** 5437601

---

**Total deviations:** 3 auto-fixed (3 blocking)
**Impact on plan:** All auto-fixes necessary for compilation. No scope creep -- purely cascading type changes from the planned key_field Option<String> migration.

## Issues Encountered
None beyond the cascading type changes documented in deviations.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- StreamDefinition type foundation in place for composable pipeline
- depends_on field ready for DAG construction in Plan 03
- filter field ready for downstream pipeline triggering in Plan 02
- petgraph dependency available for topological sort

## Self-Check: PASSED

- All key files exist (pipeline.rs, protocol.rs, Cargo.toml)
- Both task commits verified (b9746b2, 5437601)
- key_field: Option<String> confirmed in pipeline.rs
- depends_on: Option<Vec<String>> confirmed in pipeline.rs
- filter: Option<Expr> confirmed in pipeline.rs
- petgraph confirmed in Cargo.toml
- 447 tests pass (401 lib + 46 integration)

---
*Phase: 07-composable-pipeline*
*Completed: 2026-04-10*
