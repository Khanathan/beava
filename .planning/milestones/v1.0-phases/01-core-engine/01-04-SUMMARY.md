---
phase: 01-core-engine
plan: 04
subsystem: engine
tags: [ahashmap, state-store, pipeline-engine, push-through, derive-expressions]

# Dependency graph
requires:
  - phase: 01-core-engine (plans 01-03)
    provides: FeatureValue types, RingBuffer, CountOp/SumOp/AvgOp operators, expression parser/evaluator
provides:
  - EntityState with live operators and static features
  - StateStore mapping entity keys to EntityState via AHashMap
  - PipelineEngine with synchronous push-through orchestration
  - StreamDefinition and FeatureDef types for stream registration
  - Integration tests proving end-to-end correctness
affects: [02-server, 03-python-sdk, 04-persistence]

# Tech tracking
tech-stack:
  added: []
  patterns: [lazy-operator-instantiation, collect-then-insert-for-borrow-checker, static-overrides-live]

key-files:
  created:
    - src/engine/pipeline.rs
    - tests/test_pipeline.rs
  modified:
    - src/state/store.rs
    - src/state/mod.rs
    - src/engine/mod.rs

key-decisions:
  - "Lazy operator instantiation: operators created on first push per entity, not at registration time"
  - "Static features override live features with same name (direct writes take precedence)"
  - "Derive expressions collected into Vec before insertion to satisfy Rust borrow checker"

patterns-established:
  - "Push-through flow: extract key -> get_or_create entity -> push operators -> read operators -> eval derives -> return"
  - "Lazy init: entity live_operators populated from StreamDefinition on first push, reused on subsequent pushes"
  - "Static override: get_all_features reads operators first, then overlays static features"

requirements-completed: [ENG-01]

# Metrics
duration: 3min
completed: 2026-04-09
---

# Phase 01 Plan 04: State Store and Pipeline Engine Summary

**EntityState + StateStore with AHashMap, PipelineEngine with synchronous push-through (event -> operators -> derives -> feature map), 110 tests passing**

## Performance

- **Duration:** 3 min
- **Started:** 2026-04-09T13:51:45Z
- **Completed:** 2026-04-09T13:55:00Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments
- EntityState stores per-key live operators (Box<dyn Operator>) and static features (AHashMap) with last_event_at tracking
- StateStore maps EntityKey -> EntityState via AHashMap with get_or_create, set_static, and get_all_features (merges live + static)
- PipelineEngine orchestrates full push-through: register stream, push event with key extraction + validation, update all operators, evaluate derive expressions, return complete FeatureMap
- Integration tests prove end-to-end correctness: count/sum/avg aggregation, derive evaluation, window expiration, static feature injection, separate entity key state

## Task Commits

Each task was committed atomically:

1. **Task 1: EntityState and StateStore** - `96471ad` (feat)
2. **Task 2: PipelineEngine with push-through and integration tests** - `53a9d69` (feat)

## Files Created/Modified
- `src/state/store.rs` - EntityState, StaticFeature, StateStore with AHashMap<EntityKey, EntityState>
- `src/state/mod.rs` - Module export (already existed, unchanged)
- `src/engine/pipeline.rs` - StreamDefinition, FeatureDef, PipelineEngine with push/get_features
- `src/engine/mod.rs` - Added pipeline module export
- `tests/test_pipeline.rs` - 9 integration tests for end-to-end push-through flow

## Decisions Made
- Lazy operator instantiation: operators created from StreamDefinition on first push per entity key, not at registration time. This avoids allocating operator state for keys that never receive events.
- Static features override live features with same name per CLAUDE.md design: SET/MSET writes take precedence over computed features.
- Derive evaluation collects results into a Vec before inserting into FeatureMap to satisfy Rust borrow checker (immutable borrow of features for EvalContext vs mutable insert).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed borrow checker conflict in push derive evaluation**
- **Found during:** Task 2 (PipelineEngine implementation)
- **Issue:** Evaluating derive expressions required immutable borrow of features map (for EvalContext) while also inserting results (mutable borrow) - Rust rejects this
- **Fix:** Collect all derive results into a Vec<(String, FeatureValue)> first, then insert into features map after EvalContext is dropped
- **Files modified:** src/engine/pipeline.rs
- **Verification:** cargo test passes with 110 tests, 0 failures
- **Committed in:** 53a9d69

---

**Total deviations:** 1 auto-fixed (1 bug)
**Impact on plan:** Standard Rust borrow checker resolution. No scope creep.

## Issues Encountered
None beyond the borrow checker fix documented above.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Phase 1 core engine is complete: state store, windowed operators, expression evaluator, pipeline engine all wired together
- Ready for Phase 2 (Server): TCP protocol, PUSH/GET/SET/MSET/REGISTER commands can now call PipelineEngine::push/get_features
- Ready for Phase 4 (Persistence): EntityState needs enum-based operator wrapper for serialization (Box<dyn Operator> is not serde-compatible)
- Blocker noted: Phase 4 snapshot approach requires replacing Box<dyn Operator> with an enum for serialization

---
*Phase: 01-core-engine*
*Completed: 2026-04-09*
