---
phase: 03-python-sdk
plan: 02
subsystem: sdk
tags: [python, metaclass, decorators, dsl, operators, json-serialization]

# Dependency graph
requires:
  - phase: 03-python-sdk/01
    provides: Types (FeatureResult, TallyError), protocol encoding, __init__.py base
provides:
  - All 9 operator descriptor classes with JSON serialization
  - "@stream decorator with StreamMeta metaclass for feature collection"
  - "@view decorator restricted to derive/lookup operators"
  - "Full public API: import tally as st; st.count(), @st.stream(), @st.view()"
  - "_to_register_json() producing RegisterRequest dict matching Rust DTO"
affects: [03-python-sdk/03, 03-python-sdk/04]

# Tech tracking
tech-stack:
  added: []
  patterns: [metaclass-based DSL collection, operator descriptor pattern, reversed MRO mixin walk]

key-files:
  created:
    - python/tally/_operators.py
    - python/tally/_stream.py
    - python/tally/_view.py
    - python/tests/test_operators.py
    - python/tests/test_stream.py
    - python/tests/test_view.py
  modified:
    - python/tally/__init__.py

key-decisions:
  - "Operator constructors use Python's native keyword-only args for required param validation (TypeError on missing)"
  - "Lookup target is a string ref like 'MerchantActivity.chargeback_count_24h' -- cross-class attribute resolution deferred to Phase 5"
  - "Reversed MRO walk for mixin features ensures later-listed bases take precedence, class body always wins"

patterns-established:
  - "OperatorBase.to_json(name) -> dict: canonical serialization contract for all operators"
  - "StreamMeta metaclass: _tally_features, _tally_key_field, _tally_stream_name, _tally_is_view metadata"
  - "Decorator pattern: stream()/view() recreate class with StreamMeta, filtering dunder attrs from namespace"

requirements-completed: [SDK-01, SDK-02, SDK-03]

# Metrics
duration: 5min
completed: 2026-04-09
---

# Phase 03 Plan 02: Declarative DSL Layer Summary

**All 9 operator descriptor classes with JSON serialization, @stream metaclass with mixin inheritance, and @view decorator with derive/lookup restriction**

## Performance

- **Duration:** 5 min
- **Started:** 2026-04-09T16:28:45Z
- **Completed:** 2026-04-09T16:33:49Z
- **Tasks:** 2
- **Files modified:** 7

## Accomplishments
- All 9 operator classes (Count, Sum, Avg, Min, Max, DistinctCount, Last, Derive, Lookup) serialize to JSON matching Rust FeatureDefRequest schema
- StreamMeta metaclass collects operator descriptors from class body and mixin bases, supporting full CLAUDE.md VelocityMixin + AmountMixin example
- @view decorator restricts to Derive and Lookup operators with TypeError at definition time
- Full public API exported: `import tally as st; st.count(window="30m"); @st.stream(key="user_id")` works

## Task Commits

Each task was committed atomically:

1. **Task 1: Operator descriptor classes with JSON serialization tests** - `c4e36ff` (test) + `d8dff2e` (feat)
2. **Task 2: @stream and @view decorators with metaclass, mixin support** - `506383b` (test) + `757ba70` (feat)

_Note: TDD tasks have RED (test) + GREEN (feat) commits_

## Files Created/Modified
- `python/tally/_operators.py` - All 9 operator descriptor classes with OperatorBase, to_json() serialization
- `python/tally/_stream.py` - StreamMeta metaclass with mixin support, stream() decorator
- `python/tally/_view.py` - view() decorator restricted to Derive/Lookup
- `python/tally/__init__.py` - Full public API with lowercase operator aliases, decorators
- `python/tests/test_operators.py` - 42 tests covering all operator JSON output and validation
- `python/tests/test_stream.py` - 17 tests for decorator, metaclass, mixin, register JSON
- `python/tests/test_view.py` - 11 tests for view restrictions, validation, register JSON

## Decisions Made
- Operator constructors leverage Python's keyword-only argument syntax (`*` separator) to enforce required params via native TypeError -- no manual validation needed
- Lookup target stored as a plain string ("MerchantActivity.chargeback_count_24h") rather than cross-class attribute reference -- Phase 5 adds cross-key lookup resolution
- StreamMeta walks bases in reversed() order so later-listed bases take precedence for name conflicts between mixins, consistent with Python's C3 linearization expectations

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- DSL layer complete: operators, @stream, @view all working with tests
- Ready for Plan 03 (TCP client with connection management) and Plan 04 (App class with register/push/get/set/mset)
- _to_register_json() output ready to be sent via REGISTER command encoding from Plan 01

## Self-Check: PASSED

All 6 created files verified on disk. All 4 commit hashes found in git log.

---
*Phase: 03-python-sdk*
*Completed: 2026-04-09*
