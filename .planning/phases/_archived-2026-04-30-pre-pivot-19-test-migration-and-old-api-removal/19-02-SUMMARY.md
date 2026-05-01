---
phase: 19-test-migration-and-old-api-removal
plan: 02
subsystem: testing
tags: [pytest, source, dataset, group_by, union, projection]

requires:
  - phase: 16-new-python-sdk-api
    provides: "@source, @dataset, group_by decorators and types"
  - phase: 19-01
    provides: "conftest.py using new API, filter parameter on @dataset"
provides:
  - "test_source.py: 15 tests covering @source decorator behavior"
  - "test_dataset.py: 42 tests covering @dataset, group_by, agg, derive, union, projection, TTL, filter"
  - "Old test_stream.py and test_view.py marked DEPRECATED for deletion in Plan 04"
affects: [19-03, 19-04, 19-05]

tech-stack:
  added: []
  patterns:
    - "@source creates SourceDef (keyless, no operators)"
    - "@dataset(depends_on=[...]) with group_by().agg() for keyed aggregation"
    - "Derive-only @dataset (no agg) replaces old @view pattern"

key-files:
  created:
    - python/tests/test_source.py
    - python/tests/test_dataset.py
  modified:
    - python/tests/test_stream.py
    - python/tests/test_view.py

key-decisions:
  - "57 new tests (15 source + 42 dataset) exceeds plan target of 45"
  - "Derive-only dataset with no group_by has key_field=None (view-equivalent pattern)"

patterns-established:
  - "test_source.py pattern: test SourceDef creation, compile, collect, schema"
  - "test_dataset.py pattern: test DatasetDef with group_by.agg, derives, union, projection"

requirements-completed: [MIG-01]

duration: 4min
completed: 2026-04-13
---

# Phase 19 Plan 02: Test Stream/View Rewrite Summary

**57 new tests for @source/@dataset replacing test_stream.py (33) and test_view.py (12) with expanded v2.0 API coverage**

## Performance

- **Duration:** 4 min
- **Started:** 2026-04-13T00:12:56Z
- **Completed:** 2026-04-13T00:16:58Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments
- Created test_source.py with 15 tests: decorator creation, compile/JSON, collect_registrations, EventSet schema, TTL fields
- Created test_dataset.py with 42 tests: group_by/agg, derives, view-equivalent, union, collect_registrations, TTL, filter, projection, error cases, full pipeline example
- Marked test_stream.py and test_view.py as DEPRECATED (deletion in Plan 04)
- Total test collection increased from 376 to 433

## Task Commits

Each task was committed atomically:

1. **Task 1: Create test_source.py and test_dataset.py** - `08e5d15` (feat)
2. **Task 2: Mark old test files for deletion** - `e1c50e5` (chore)

## Files Created/Modified
- `python/tests/test_source.py` - 15 tests for @source decorator (keyless source definition, naming, compile, collect, EventSet)
- `python/tests/test_dataset.py` - 42 tests for @dataset decorator (keyed agg, derives, views, union, projection, TTL, filter, errors)
- `python/tests/test_stream.py` - Added DEPRECATED comment header
- `python/tests/test_view.py` - Added DEPRECATED comment header

## Decisions Made
- Exceeded target test count (57 vs 45 minimum) to cover new v2.0 features like projection, union, and transitive dependency collection
- Derive-only datasets (no group_by.agg) compile with key_field=None, matching the old @view behavior

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
- Pre-existing test_expr.py::TestWrap::test_wrap_column failure (unrelated to this plan, not caused by changes)

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- test_source.py and test_dataset.py are complete replacement tests for old stream/view test files
- Old test files marked for deletion in Plan 04 after count verification across all migrated test files
- 433 tests collecting, ready for test_operators.py migration in Plan 03

---
*Phase: 19-test-migration-and-old-api-removal*
*Completed: 2026-04-13*
