---
phase: 17-enriched-event-propagation
plan: 02
subsystem: engine
tags: [rust, pipeline, enrichment, cascade, propagation]

# Dependency graph
requires:
  - phase: 17-01
    provides: "Operator trait with enrichment parameter, resolve_field helper, EvalContext.enrichment field"
provides:
  - "Enrichment accumulator in push_with_cascade_internal"
  - "push_internal accepts enrichment_json and enrichment_fv parameters"
  - "Downstream operators and derives see upstream computed fields during cascade"
  - "Non-cascade paths pass None enrichment (zero overhead)"
affects: [17-03-PLAN]

# Tech tracking
tech-stack:
  added: []
  patterns: ["stack-local enrichment accumulator (AHashMap) threaded through cascade loop", "dual enrichment maps: serde_json::Value for operators, FeatureValue for EvalContext"]

key-files:
  created: []
  modified:
    - src/engine/pipeline.rs

key-decisions:
  - "Two enrichment maps: enrichment_json for operators (serde_json::Value), enrichment_fv for EvalContext (FeatureValue)"
  - "No-cascade fast path skips enrichment allocation entirely"
  - "Primary push always reads features when downstream exists, even in async mode (Pitfall 5)"
  - "has_further_downstream controls both ds_read_features and enrichment accumulation"
  - "Enrichment populated with both qualified (Stream.field) and unqualified names"

patterns-established:
  - "Enrichment accumulator is stack-local, never enters DashMap (C-5 compliance)"
  - "read_features=false cascade path still computes enrichment internally for downstream"

requirements-completed: [ENG-01]

# Metrics
duration: 3min
completed: 2026-04-12
---

# Phase 17 Plan 02: Enrichment Cascade Wiring Summary

**Stack-local enrichment accumulator wired through cascade execution so downstream operators and derives see upstream computed fields in a single push-through cycle**

## Performance

- **Duration:** 3 min
- **Started:** 2026-04-12T22:52:23Z
- **Completed:** 2026-04-12T22:55:41Z
- **Tasks:** 2
- **Files modified:** 1

## Accomplishments
- push_internal now accepts enrichment_json and enrichment_fv parameters, threading them to filter eval, where-clause eval, operator push, and derive eval
- push_with_cascade_internal builds stack-local enrichment accumulators populated after each upstream stream's results
- Downstream streams resolve upstream-computed fields via enrichment in both operators (resolve_field) and derives (EvalContext)
- No-cascade fast path skips all enrichment allocation for zero overhead on non-DAG pushes
- All 765 tests pass (622 lib + 143 integration)

## Task Commits

Each task was committed atomically:

1. **Task 1: Add enrichment parameter to push_internal and all its callers** - `56f2a9e` (feat)
2. **Task 2: Build enrichment accumulator in push_with_cascade_internal** - `0158052` (feat)

## Files Created/Modified
- `src/engine/pipeline.rs` - push_internal enrichment params, push_with_cascade_internal accumulator, all caller updates

## Decisions Made
- Two enrichment maps needed per RESEARCH.md Pattern 4: enrichment_json (serde_json::Value) for operators, enrichment_fv (FeatureValue) for EvalContext
- No-cascade fast path returns early without allocating enrichment maps
- Primary push always reads features when downstream exists (Pitfall 5 mitigation) even in async (read_features=false) mode
- has_further_downstream gates both ds_read_features and enrichment accumulation to avoid unnecessary work at chain terminus

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Enrichment accumulator is fully wired through cascade execution
- Plan 17-03 can now add integration tests verifying multi-stage computed features
- All existing tests pass, confirming backward compatibility

---
*Phase: 17-enriched-event-propagation*
*Completed: 2026-04-12*
