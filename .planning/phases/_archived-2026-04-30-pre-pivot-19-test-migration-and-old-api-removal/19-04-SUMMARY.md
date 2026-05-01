---
phase: 19-test-migration-and-old-api-removal
plan: 04
subsystem: testing
tags: [pytest, api-removal, cleanup, sdk]

requires:
  - phase: 19-02
    provides: "test_source.py and test_dataset.py replacement tests"
  - phase: 19-03
    provides: "test_dataset_behaviors.py ported tests, cleaned test_expr.py and test_new_api.py"
provides:
  - "Clean SDK with only new API surface (no @stream/@view/DataFrame exports)"
  - "4 old SDK modules deleted (_stream.py, _view.py, _dataframe.py, _expr.py)"
  - "4 old test files deleted (test_stream.py, test_view.py, test_dataframe.py, test_expr.py)"
  - "_app.py cleaned of DataFrame methods (source/serve/register_all)"
affects: [19-05]

tech-stack:
  added: []
  patterns:
    - "SDK exports only new API: source, dataset, group_by, union, validate, EventSet, FeatureSet, Field"

key-files:
  created: []
  modified:
    - python/tally/__init__.py
    - python/tally/_app.py

key-decisions:
  - "Deleted test_expr.py (11 tests) along with _expr.py since Literal/Ref/BinOp/UnaryOp classes only existed in _expr.py"
  - "Pre-deletion count: 1207 (419 Python + 788 Rust); post-deletion count: 1101 (313 Python + 788 Rust); both well above 744 gate"

patterns-established:
  - "SDK __init__.py exports only: types, operators, App, protocol constants, and v2.0 API symbols"

requirements-completed: [MIG-01, MIG-02]

duration: 5min
completed: 2026-04-13
---

# Phase 19 Plan 04: Old API Deletion and SDK Cleanup Summary

**Deleted 4 old SDK modules and 4 old test files, cleaned __init__.py and _app.py exports -- 1101 tests passing on new API only**

## Performance

- **Duration:** 5 min
- **Started:** 2026-04-13T00:24:28Z
- **Completed:** 2026-04-13T00:29:16Z
- **Tasks:** 2
- **Files modified:** 10 (2 modified, 8 deleted)

## Accomplishments
- Verified pre-deletion test count: 1207 total (419 Python + 788 Rust), well above 744 gate
- Deleted 4 old SDK modules: _stream.py, _view.py, _dataframe.py, _expr.py (2,422 lines removed)
- Deleted 4 old test files: test_stream.py, test_view.py, test_dataframe.py, test_expr.py
- Cleaned __init__.py: removed all old API imports and __all__ entries (stream, view, Column, Expr, EventProxy, DataStream, Table, GroupBy, JoinedTable, Dataset)
- Cleaned _app.py: removed source(), serve(), register_all() methods and DataFrame state/imports
- Verified post-deletion: 1101 tests passing (313 Python + 788 Rust), zero old API symbols accessible
- Grep-verified: zero @stream/@view references in Python source

## Task Commits

Each task was committed atomically:

1. **Task 1: Pre-deletion test count verification** - No commit (verification-only task, no files modified)
2. **Task 2: Delete old API files, clean __init__.py and _app.py** - `6ca9bb5` (feat)

## Files Created/Modified
- `python/tally/__init__.py` - Removed old API imports; exports only types, operators, App, protocol, and v2.0 API
- `python/tally/_app.py` - Removed source()/serve()/register_all() DataFrame methods and _dataframe import
- `python/tally/_stream.py` - DELETED
- `python/tally/_view.py` - DELETED
- `python/tally/_dataframe.py` - DELETED
- `python/tally/_expr.py` - DELETED
- `python/tests/test_stream.py` - DELETED
- `python/tests/test_view.py` - DELETED
- `python/tests/test_dataframe.py` - DELETED
- `python/tests/test_expr.py` - DELETED

## Decisions Made
- Deleted test_expr.py (11 tests) in addition to the 3 test files specified in the plan, because it imported only from tally._expr which was deleted. The Literal/Ref/BinOp/UnaryOp classes it tested existed nowhere else in the codebase.
- Pre-deletion and post-deletion counts both far exceed the 744 gate (1207 and 1101 respectively)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Also deleted test_expr.py (not in plan's deletion list)**
- **Found during:** Task 2
- **Issue:** test_expr.py imports from tally._expr (Literal, Ref, BinOp, UnaryOp). After deleting _expr.py, pytest collection fails with ModuleNotFoundError. Plan listed only test_stream.py, test_view.py, test_dataframe.py for deletion.
- **Fix:** Deleted test_expr.py (11 tests testing expression nodes that only existed in the deleted _expr.py)
- **Files modified:** python/tests/test_expr.py (deleted)
- **Verification:** All 313 Python tests collect and pass after deletion
- **Committed in:** 6ca9bb5

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Necessary for correctness -- test_expr.py tested code from deleted _expr.py module. No scope creep.

## Issues Encountered
None beyond the deviation documented above.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- SDK exports only new API symbols (source, dataset, group_by, union, validate, EventSet, FeatureSet, Field)
- All 1101 tests pass on the new API
- Ready for Plan 05: final verification sweep

---
*Phase: 19-test-migration-and-old-api-removal*
*Completed: 2026-04-13*
