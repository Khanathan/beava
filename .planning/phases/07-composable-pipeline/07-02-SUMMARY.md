---
phase: 07-composable-pipeline
plan: 02
subsystem: sdk
tags: [python, stream, keyless, depends_on, filter, composable-pipeline]

# Dependency graph
requires:
  - phase: 07-composable-pipeline plan 01
    provides: Rust RegisterRequest with optional key_field, depends_on, filter fields
provides:
  - "@st.stream() with optional key (keyless streams)"
  - "depends_on parameter for upstream dependency wiring"
  - "filter parameter for stream-level event filtering"
  - "Keyless stream validation (rejects windowed operators)"
affects: [07-composable-pipeline plan 03, 07-composable-pipeline plan 04]

# Tech tracking
tech-stack:
  added: []
  patterns: ["Keyless stream validation at class creation time (fail-fast)", "Class reference resolution to string names in JSON serialization"]

key-files:
  created: []
  modified: [python/tally/_stream.py, python/tests/test_stream.py]

key-decisions:
  - "Keyless streams reject all operators except Derive and Lookup at class creation time (TypeError)"
  - "depends_on stores class references in-memory, resolves to string names only during _to_register_json()"
  - "New JSON fields (depends_on, filter) conditionally omitted when absent for backward compatibility"

patterns-established:
  - "Fail-fast validation: invalid stream definitions raise TypeError at class creation, not at registration"
  - "Conditional JSON fields: omit keys when None to maintain backward compatibility with older servers"

requirements-completed: [PIPE-01, PIPE-02]

# Metrics
duration: 2min
completed: 2026-04-10
---

# Phase 7 Plan 2: Python SDK Keyless Streams, depends_on, and Filter Summary

**Python SDK @st.stream() extended with optional key (keyless streams), depends_on dependency wiring, and stream-level filter expressions**

## Performance

- **Duration:** 2 min
- **Started:** 2026-04-10T01:43:40Z
- **Completed:** 2026-04-10T01:45:33Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- @st.stream() key parameter now optional -- None creates a keyless stream for raw event ingestion
- depends_on parameter accepts list of stream class references, resolves to string names in JSON
- filter parameter stores expression string for stream-level event filtering
- Keyless streams validate at class creation: windowed operators (Count, Sum, etc.) raise TypeError
- Full backward compatibility: existing streams without depends_on/filter produce identical JSON

## Task Commits

Each task was committed atomically:

1. **Task 1: Write failing tests for keyless streams, depends_on, and filter** - `a5c62c5` (test)
2. **Task 2: Implement Python SDK changes** - `2e6d09e` (feat)

## Files Created/Modified
- `python/tally/_stream.py` - StreamMeta and stream() decorator updated with optional key, depends_on, filter
- `python/tests/test_stream.py` - 11 new tests across TestKeylessStream, TestDependsOn, TestStreamFilter classes

## Decisions Made
- Keyless streams reject all operators except Derive and Lookup at class creation time (fail-fast TypeError) -- matches the threat model T-07-05 mitigation for preventing invalid definitions from reaching the server
- depends_on stores live class references on _tally_depends_on, only resolving to string names during _to_register_json() -- enables IDE tooling and import validation
- New fields (depends_on, filter) conditionally omitted from JSON when None for backward compatibility with existing server protocol

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Updated test_missing_key_raises for v1.1 behavior**
- **Found during:** Task 1 (writing tests)
- **Issue:** Existing test expected @st.stream() with no key to raise TypeError, but v1.1 makes key optional for keyless streams
- **Fix:** Updated test to verify keyless stream creation instead of TypeError
- **Files modified:** python/tests/test_stream.py
- **Verification:** All 171 tests pass
- **Committed in:** a5c62c5 (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (1 bug fix)
**Impact on plan:** Necessary update to align existing test with v1.1 keyless stream behavior. No scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Python SDK now produces JSON with depends_on and filter fields matching Rust RegisterRequest from Plan 01
- Ready for Plan 03 (DAG construction) and Plan 04 (integration) to wire these together

---
*Phase: 07-composable-pipeline*
*Completed: 2026-04-10*
