---
phase: 19-test-migration-and-old-api-removal
plan: 03
subsystem: testing
tags: [pytest, migration, dataset, behavioral-tests, cleanup]

requires:
  - phase: 19-01
    provides: "conftest.py using new API, filter parameter on @dataset"
provides:
  - "test_dataset_behaviors.py with 28 behavioral tests ported from test_dataframe.py"
  - "test_expr.py cleaned to 11 expression string evaluation tests only"
  - "test_new_api.py cleaned of all old API imports and compat tests"
affects: [19-04]

tech-stack:
  added: []
  patterns:
    - "Behavioral test porting: categorize as portable vs API-surface-specific before migrating"

key-files:
  created:
    - python/tests/test_dataset_behaviors.py
  modified:
    - python/tests/test_dataframe.py
    - python/tests/test_expr.py
    - python/tests/test_new_api.py

key-decisions:
  - "28 behavioral tests ported (exceeds 20 minimum target); 22 DataFrame-specific tests documented as not ported"
  - "test_expr.py reduced from 51 to 11 tests -- only Literal/Ref/BinOp/UnaryOp string serialization retained"
  - "Removed TestJsonCompat (2 tests) and test_old_api_still_works from test_new_api.py -- old API compat tests no longer needed"

requirements-completed: [MIG-01]

duration: 4min
completed: 2026-04-13
---

# Phase 19 Plan 03: Test Behavioral Port and Cleanup Summary

**28 behavioral tests ported from test_dataframe.py to new @source/@dataset API; test_expr.py and test_new_api.py cleaned of all old API references**

## Performance

- **Duration:** 4 min
- **Started:** 2026-04-13T00:18:37Z
- **Completed:** 2026-04-13T00:22:59Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments
- Created test_dataset_behaviors.py with 28 behavioral tests using @source/@dataset/group_by
- Ported pipeline compilation, multi-stream derives, deduplication, filter, TTL, projection, and error handling tests
- Cleaned test_expr.py from 51 to 11 tests (removed Column, Expr, EventProxy, _FakeTable tests)
- Cleaned test_new_api.py: removed TestJsonCompat class (2 tests importing from tally._stream) and test_old_api_still_works
- Marked test_dataframe.py as DEPRECATED for deletion in Plan 04
- All 96 tests across the 3 files pass

## Task Commits

Each task was committed atomically:

1. **Task 1: Port test_dataframe.py behavioral tests to new API** - `f39c2fd` (feat)
2. **Task 2: Clean test_expr.py and test_new_api.py** - `5b59b41` (feat)

## Files Created/Modified
- `python/tests/test_dataset_behaviors.py` - 28 new behavioral tests using @source/@dataset/group_by API
- `python/tests/test_dataframe.py` - Added DEPRECATED header comment
- `python/tests/test_expr.py` - Reduced from 51 to 11 tests (expression string evaluation only)
- `python/tests/test_new_api.py` - Removed 3 old API tests (TestJsonCompat + test_old_api_still_works)

## Decisions Made
- Ported 28 of ~50 tests (the behavioral half); 22 DataFrame-specific API surface tests documented as not ported (Column overloading, JoinedTable, EventProxy, backward compat)
- test_expr.py retains Literal/Ref/BinOp/UnaryOp tests because these expression nodes are used by the server-side evaluator string format
- Replaced test_old_api_still_works with test_app_importable (App is part of new API too)

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- test_dataset_behaviors.py complete, providing behavioral coverage that survives old API deletion
- test_expr.py and test_new_api.py cleaned of all old API references
- Ready for Plan 04: old test file deletion and final count verification

---
*Phase: 19-test-migration-and-old-api-removal*
*Completed: 2026-04-13*
