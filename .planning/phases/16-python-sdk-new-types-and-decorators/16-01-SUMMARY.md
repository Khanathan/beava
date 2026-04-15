---
phase: 16-python-sdk-new-types-and-decorators
plan: 01
subsystem: sdk
tags: [python, eventset, featureset, dataclass_transform, decorator, pep681]

# Dependency graph
requires: []
provides:
  - EventSet and FeatureSet schema base classes with Field descriptors
  - "@tl.source decorator producing SourceDef with keyless stream JSON compilation"
  - "@tl.dataset decorator with group_by().agg() producing keyed stream JSON compilation"
  - "tl.union() for multi-parent depends_on flattening"
  - "App.register() compatibility via _tally_stream_name, _to_register_json(), _collect_registrations()"
affects: [16-02, 17-engine-enriched-propagation, 18-test-migration, 19-old-api-removal]

# Tech tracking
tech-stack:
  added: []
  patterns: ["dataclass_transform (PEP 681) for typed schema base classes", "__init_subclass__ field collection with dynamic __init__ generation", "SourceDef/DatasetDef compile-to-JSON pattern reusing existing OperatorBase.to_json()"]

key-files:
  created:
    - python/tally/_schema.py
    - python/tally/_source.py
    - python/tally/_dataset.py
    - python/tests/test_new_api.py
  modified: []

key-decisions:
  - "Used __init_subclass__ (not metaclass) for EventSet/FeatureSet -- simpler, no MRO issues"
  - "Dynamic __init__ via exec() for correct IDE signatures with required/optional params"
  - "SourceDef and DatasetDef are plain objects (not classes) -- decorator returns instance, not modified class"
  - "Extra derive features scanned from class body attributes beyond the 'features' GroupedDataset"

patterns-established:
  - "_compile() -> dict as the canonical compilation method; _to_register_json() delegates to it"
  - "_collect_registrations() walks depends_on tree with deduplication by name"
  - "GroupedDataset as immutable intermediate -- .agg() returns new instance"

requirements-completed: [API-01, API-02, API-03, API-04, API-05]

# Metrics
duration: 5min
completed: 2026-04-12
---

# Phase 16 Plan 01: New API Types and Decorators Summary

**EventSet/FeatureSet typed schemas with @tl.source and @tl.dataset(depends_on=[...]).group_by().agg() compiling to existing RegisterRequest JSON**

## Performance

- **Duration:** 5 min
- **Started:** 2026-04-12T22:01:57Z
- **Completed:** 2026-04-12T22:07:03Z
- **Tasks:** 3
- **Files modified:** 4

## Accomplishments
- EventSet and FeatureSet base classes with PEP 681 dataclass_transform for IDE autocomplete, Field descriptors with dtype inference, and dynamic __init__ generation
- @tl.source decorator creating SourceDef objects that compile to keyless stream JSON and are App.register() compatible
- @tl.dataset decorator with group_by().agg() pattern, UnionSource for multi-parent depends_on, and derive feature scanning from class body
- 27 unit tests covering all new types across 5 test classes

## Task Commits

Each task was committed atomically:

1. **Task 1: Create _schema.py with EventSet, FeatureSet, and Field** - `a165320` (feat)
2. **Task 2: Create _source.py with @tl.source decorator** - `827d709` (feat)
3. **Task 3: Create _dataset.py with @tl.dataset, GroupedDataset, group_by, union** - `7607e09` (feat)

## Files Created/Modified
- `python/tally/_schema.py` - EventSet, FeatureSet, Field classes with dataclass_transform and dynamic __init__
- `python/tally/_source.py` - @tl.source decorator and SourceDef class compiling to keyless stream JSON
- `python/tally/_dataset.py` - @tl.dataset decorator, DatasetDef, GroupedDataset, UnionSource, group_by/union free functions
- `python/tests/test_new_api.py` - 27 unit tests across TestSchema, TestSource, TestGroupByAgg, TestDataset, TestUnion

## Decisions Made
- Used __init_subclass__ instead of metaclass for EventSet/FeatureSet -- simpler approach, no MRO conflicts with multiple inheritance
- Dynamic __init__ via exec() for correct parameter signatures that IDEs can introspect
- SourceDef and DatasetDef are plain objects returned by decorators (not modified classes) -- cleaner than metaclass approach
- GroupedDataset.agg() returns new instance (immutable pattern) rather than mutating in place

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

- pytest was not installed in the environment -- installed via pip bootstrap (Rule 3 blocking fix, no code impact)

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- All new API types compile to the same RegisterRequest JSON format the server expects
- __init__.py exports not yet updated (deferred to plan 16-02 or later)
- Ready for plan 16-02 (pipeline validation, additional integration)

## Self-Check: PASSED

All 4 files exist. All 3 commit hashes verified. 27/27 tests pass.

---
*Phase: 16-python-sdk-new-types-and-decorators*
*Completed: 2026-04-12*
