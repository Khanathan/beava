---
phase: 18-feature-projection-and-ephemeral-schema
plan: 01
subsystem: engine
tags: [projection, ephemeral, serde, backward-compat, rust]

# Dependency graph
requires: []
provides:
  - "Projection enum (Select/Drop) with apply() filtering on FeatureMap"
  - "StreamDefinition ephemeral schema fields (ephemeral, pipeline_ttl, max_keys)"
  - "RegisterRequest 4 new serde(default) fields for backward compat"
  - "Projection filtering in push_internal and get_features paths"
affects: [18-02, python-sdk-projection, on-demand-compute]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Projection applied AFTER derives evaluate, BEFORE response -- derive-then-filter ordering"
    - "Schema-only fields (ephemeral, pipeline_ttl, max_keys) -- stored but no runtime enforcement"
    - "raw_register_json passthrough preserves new fields through snapshot round-trip"

key-files:
  created: []
  modified:
    - src/engine/pipeline.rs
    - src/server/protocol.rs
    - tests/test_pipeline.rs

key-decisions:
  - "Projection applied after derives but before views in get_features -- derives can reference any feature"
  - "select/drop mutual exclusion validated in convert_register_request, not at serde level"
  - "Ephemeral fields are schema-only -- no runtime enforcement until on-demand compute phase"

patterns-established:
  - "Projection::apply pattern for response filtering -- reusable for future per-view projections"

requirements-completed: [ENG-02, ENG-03]

# Metrics
duration: 12min
completed: 2026-04-12
---

# Phase 18 Plan 01: Feature Projection and Ephemeral Schema Summary

**Projection::Select/Drop filtering on push/get responses with 4 new backward-compatible RegisterRequest fields for ephemeral schema**

## Performance

- **Duration:** 12 min
- **Started:** 2026-04-12T23:17:47Z
- **Completed:** 2026-04-12T23:29:47Z
- **Tasks:** 2
- **Files modified:** 11

## Accomplishments
- Projection enum with Select/Drop variants filters FeatureMap in both push_internal and get_features
- RegisterRequest gains 4 new serde(default) fields -- v1.3 JSON loads without changes
- Derives evaluate correctly even when referencing features that are subsequently filtered by projection
- Snapshot round-trip preserves all new fields via raw_register_json passthrough
- 788 tests pass (629 lib + 159 integration), up from 780 pre-plan

## Task Commits

Each task was committed atomically:

1. **Task 1: Add Projection type, RegisterRequest fields, and convert_register_request integration**
   - `cc68315` (test: failing tests for Projection and RegisterRequest)
   - `ab06d61` (feat: Projection enum, new fields, push/get filtering)
2. **Task 2: Integration tests -- projection end-to-end, backward compat, snapshot round-trip**
   - `a7ee938` (test: 8 integration tests for projection, backward compat, snapshot)

## Files Created/Modified
- `src/engine/pipeline.rs` - Projection enum, StreamDefinition new fields, push/get filtering
- `src/server/protocol.rs` - ProjectionRequest struct, RegisterRequest new fields, convert_register_request projection parsing
- `tests/test_pipeline.rs` - 8 new integration tests (projection select/drop, derive ordering, backward compat, ephemeral, snapshot)
- `src/server/tcp.rs` - Updated StreamDefinition constructions with new fields
- `src/state/eviction.rs` - Updated StreamDefinition constructions with new fields
- `src/state/snapshot.rs` - Updated StreamDefinition constructions with new fields
- `tests/test_batch_primitives.rs` - Updated StreamDefinition constructions with new fields
- `tests/test_debug_ui.rs` - Updated StreamDefinition constructions with new fields
- `tests/test_push_batch.rs` - Updated StreamDefinition constructions with new fields
- `tests/test_push_coalescing.rs` - Updated StreamDefinition constructions with new fields
- `tests/test_snapshot.rs` - Updated StreamDefinition constructions with new fields
- `tests/test_incremental_snapshot.rs` - Updated StreamDefinition constructions with new fields

## Decisions Made
- Projection applied after derives but before views in get_features -- derives can reference any feature regardless of projection
- select/drop mutual exclusion validated in convert_register_request (returns TallyError), not at serde level
- Ephemeral fields are schema-only: stored on StreamDefinition but no runtime enforcement yet

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Projection enum and filtering ready for Python SDK integration in plan 18-02
- RegisterRequest backward compat verified -- safe to deploy alongside v1.3 clients
- Ephemeral fields stored but not enforced -- on-demand compute phase will add enforcement

---
*Phase: 18-feature-projection-and-ephemeral-schema*
*Completed: 2026-04-12*
