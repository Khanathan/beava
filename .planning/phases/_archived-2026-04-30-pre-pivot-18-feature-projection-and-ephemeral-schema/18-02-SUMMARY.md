---
phase: 18-feature-projection-and-ephemeral-schema
plan: 02
subsystem: python-sdk
tags: [projection, select, drop, python, sdk, e2e]

# Dependency graph
requires:
  - "18-01: Projection enum, ProjectionRequest struct, push/get filtering in Rust engine"
provides:
  - "DatasetDef.select() and .drop() methods for Python SDK projection"
  - "_compile() emits projection field matching Rust ProjectionRequest serde format"
  - "E2E integration tests proving Python projection through live Rust server"
affects: [old-api-removal, on-demand-compute]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Immutable projection builder: select()/drop() return new DatasetDef, original unchanged"
    - "Function-scoped server fixture for projection E2E tests (isolates cross-stream interference)"

key-files:
  created: []
  modified:
    - python/tally/_dataset.py
    - python/tests/test_new_api.py

key-decisions:
  - "select()/drop() clone all DatasetDef fields into new instance (immutable pattern)"
  - "Function-scoped server fixture for projection E2E tests to avoid cross-stream projection interference in get_features"
  - "Unique feature name prefixes per E2E test to prevent cross-stream collision"

patterns-established:
  - "DatasetDef.select()/drop() immutable builder pattern for response filtering"

requirements-completed: [ENG-02]

# Metrics
duration: 11min
completed: 2026-04-12
---

# Phase 18 Plan 02: Python SDK Projection (select/drop) Summary

**DatasetDef.select()/drop() methods emitting projection JSON, with 7 unit tests and 3 E2E integration tests through live server**

## Performance

- **Duration:** 11 min
- **Started:** 2026-04-12T23:31:37Z
- **Completed:** 2026-04-12T23:42:37Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- DatasetDef.select() and .drop() return new DatasetDef with projection field (immutable pattern)
- _compile() emits projection JSON matching Rust ProjectionRequest serde format exactly
- 7 unit tests verify projection compilation, immutability, and field preservation
- 3 E2E integration tests verify select, drop, and derive-before-projection through live server
- 788 Rust tests pass (0 regressions), 326 Python tests pass (5 pre-existing failures unrelated)

## Task Commits

Each task was committed atomically:

1. **Task 1: Add select()/drop() to DatasetDef and emit projection in _compile() (TDD)**
   - `a923c38` (feat: select/drop methods + 7 unit tests)
2. **Task 2: E2E integration tests -- Python SDK projection through live server**
   - `683555d` (test: 3 E2E tests for select, drop, derive-with-projection)

## Files Created/Modified
- `python/tally/_dataset.py` - Added _projection attribute, select(), drop() methods, projection emission in _compile()
- `python/tests/test_new_api.py` - 7 unit tests (TestProjection class) + 3 E2E integration tests + projection_server fixture

## Decisions Made
- select()/drop() clone all DatasetDef fields into new instance, preserving immutability
- Used function-scoped server fixture for E2E tests to isolate cross-stream projection interference
- Used unique feature name prefixes (sel_, drp_, derv_) per E2E test to avoid cross-stream collision in get_features

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Function-scoped server fixture for projection E2E tests**
- **Found during:** Task 2 (E2E integration tests)
- **Issue:** Session-scoped tally_server shared across all integration tests; registering streams with projection caused cross-stream interference in get_features (projections apply globally to all features, not per-stream)
- **Fix:** Created function-scoped `projection_server` fixture that starts a fresh server per test; used unique feature name prefixes per test
- **Files modified:** python/tests/test_new_api.py
- **Verification:** All 3 E2E tests pass; full test suite unaffected
- **Committed in:** 683555d (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Necessary to work around pre-existing server-side limitation. No scope creep.

## Issues Encountered
- Cross-stream projection interference in get_features: discovered that per-stream projections in PipelineEngine::get_features apply sequentially to the entire FeatureMap, causing streams to filter each other's features. Logged as deferred item. Not caused by this plan's changes.

## User Setup Required
None - no external service configuration required.

## Known Stubs
None.

## Next Phase Readiness
- Python SDK projection fully functional: DatasetDef.select()/drop() work end-to-end
- Projection JSON format matches Rust ProjectionRequest serde -- safe for production use
- Cross-stream projection interference in get_features logged as deferred item for future fix

---
*Phase: 18-feature-projection-and-ephemeral-schema*
*Completed: 2026-04-12*
