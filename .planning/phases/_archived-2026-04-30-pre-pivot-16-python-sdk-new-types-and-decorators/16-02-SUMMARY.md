---
phase: 16-python-sdk-new-types-and-decorators
plan: 02
subsystem: sdk
tags: [python, validation, dag, exports, json-compat]

# Dependency graph
requires: [16-01]
provides:
  - "validate() pure-Python DAG validation with cycle, missing dep, type mismatch detection"
  - "All v2.0 API symbols exported from tally package (source, dataset, group_by, union, validate, EventSet, FeatureSet, Field, ValidationError)"
  - "JSON compatibility verified between old @st.stream and new @tl.dataset APIs"
  - "App.register() confirmed compatible with SourceDef/DatasetDef objects"
affects: [17-engine-enriched-propagation, 18-test-migration, 19-old-api-removal]

# Tech tracking
tech-stack:
  added: []
  patterns: ["Kahn's algorithm for topological sort cycle detection", "Pure-Python validation without server dependency"]

key-files:
  created:
    - python/tally/_validate.py
  modified:
    - python/tally/__init__.py
    - python/tally/_app.py
    - python/tests/test_new_api.py

key-decisions:
  - "Kahn's algorithm for cycle detection -- simple, O(V+E), well-understood"
  - "Type mismatch checks only when upstream has EventSet -- backward compat with schema-less sources"
  - "validate() is advisory (pure Python) -- server re-validates in Rust (threat T-16-03 accepted)"

requirements-completed: [API-06, API-07]

# Metrics
duration: 4min
completed: 2026-04-12
---

# Phase 16 Plan 02: Pipeline Validation, Exports, and JSON Compatibility Summary

**Pure-Python DAG validation (cycles, missing deps, type mismatches) with full v2.0 API exports and JSON format compatibility verified against old API**

## Performance

- **Duration:** 4 min
- **Started:** 2026-04-12T22:09:10Z
- **Completed:** 2026-04-12T22:12:50Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments
- _validate.py with ValidationError class and validate() function: Kahn's algorithm cycle detection, missing dependency checks, type mismatch detection against upstream EventSet schemas
- All v2.0 API symbols exported from tally package alongside old API (source, dataset, group_by, union, validate, EventSet, FeatureSet, Field, ValidationError)
- JSON compatibility confirmed: new @tl.dataset produces same key_field/features JSON as old @st.stream for equivalent pipelines
- App.register() confirmed compatible with SourceDef/DatasetDef via _collect_registrations() protocol
- 22 new tests (8 validation + 10 exports + 2 JSON compat + 2 integration), total 49 in test_new_api.py

## Task Commits

Each task was committed atomically:

1. **Task 1: Create _validate.py with validate() function** - `9ba80a9` (feat)
2. **Task 2: Wire exports into __init__.py, update _app.py, add JSON compat and integration tests** - `a5ec4a8` (feat)

## Files Created/Modified
- `python/tally/_validate.py` - ValidationError class, validate() function, _topological_sort() helper, _resolve_dep_names() helper
- `python/tally/__init__.py` - Added v2.0 API imports and __all__ entries (old API preserved)
- `python/tally/_app.py` - Added v2.0 API compatibility note to register() docstring
- `python/tests/test_new_api.py` - 22 new tests across TestValidate, TestExports, TestJsonCompat, TestIntegration classes

## Decisions Made
- Kahn's algorithm for cycle detection -- O(V+E) complexity, straightforward to implement and debug
- Type mismatch validation only runs when upstream source has an EventSet schema -- backward compatible with schema-less sources
- validate() is purely advisory (runs in user's Python process) -- server re-validates all RegisterRequests in Rust

## Deviations from Plan

None - plan executed exactly as written.

## Self-Check: PASSED

All 4 files exist. All 2 commit hashes verified. 49/49 tests pass.

---
*Phase: 16-python-sdk-new-types-and-decorators*
*Completed: 2026-04-12*
