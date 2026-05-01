---
phase: 19-test-migration-and-old-api-removal
plan: 01
subsystem: testing
tags: [pytest, migration, source, dataset, group_by, protocol]

requires:
  - phase: 16-new-python-sdk-api
    provides: "@source, @dataset, group_by decorators and types"
  - phase: 18-feature-projection
    provides: "select()/drop() on DatasetDef"
provides:
  - "conftest.py using import tally as tl (new API convention)"
  - "test_protocol.py collecting 44 tests (fixed broken import)"
  - "test_integration.py fully migrated to @source/@dataset (17 tests)"
  - "test_app.py fully migrated to @source/@dataset (21 tests)"
  - "filter parameter on @dataset decorator"
affects: [19-02, 19-03, 19-04, 19-05]

tech-stack:
  added: []
  patterns:
    - "Push to @source (keyless), verify features via GET on entity key"
    - "Cycle detection test uses raw register to bypass client-side DAG walk"

key-files:
  created: []
  modified:
    - python/tests/conftest.py
    - python/tests/test_protocol.py
    - python/tests/test_integration.py
    - python/tests/test_app.py
    - python/tally/_dataset.py

key-decisions:
  - "Removed TestEncodePush class (2 tests) since encode_push function was deleted in Phase 11"
  - "Push to keyless source returns empty features; tests use GET to verify downstream aggregations"
  - "Added filter parameter to @dataset decorator (was missing, blocking test migration)"

patterns-established:
  - "New API test pattern: push to @source, verify via app.get() not push response"
  - "Cycle detection tests use raw JSON registration to bypass client-side DAG walk"

requirements-completed: [MIG-01]

duration: 9min
completed: 2026-04-13
---

# Phase 19 Plan 01: Test Migration Foundation Summary

**Fixed broken test_protocol.py import, migrated conftest/integration/app tests from @st.stream to @source/@dataset -- 82 tests passing, 376 total collection**

## Performance

- **Duration:** 9 min
- **Started:** 2026-04-13T00:02:39Z
- **Completed:** 2026-04-13T00:11:15Z
- **Tasks:** 3
- **Files modified:** 5

## Accomplishments
- Fixed test_protocol.py import error (encode_push removed in Phase 11) -- 44 tests now collecting
- Migrated conftest.py from `import tally as st` to `import tally as tl`
- Migrated test_integration.py (17 tests) to @source/@dataset/group_by with zero old API references
- Migrated test_app.py (21 tests) to @source/@dataset/group_by with zero old API references
- Total Python test collection increased from 331 to 376

## Task Commits

Each task was committed atomically:

1. **Task 1: Fix test_protocol.py import and migrate conftest.py** - `50afbd2` (fix)
2. **Task 2: Migrate test_integration.py to new API** - `1852805` (feat)
3. **Task 3: Migrate test_app.py to new API** - `3f8eaf8` (feat)

## Files Created/Modified
- `python/tests/test_protocol.py` - Removed dead encode_push import and TestEncodePush class
- `python/tests/conftest.py` - Changed import to `import tally as tl`, updated App fixture
- `python/tests/test_integration.py` - Full migration to @source/@dataset/group_by (17 tests)
- `python/tests/test_app.py` - Full migration to @source/@dataset/group_by (21 tests)
- `python/tally/_dataset.py` - Added filter parameter to @dataset decorator and DatasetDef

## Decisions Made
- Removed TestEncodePush class (2 tests) rather than re-creating encode_push -- the function was intentionally removed in Phase 11 when binary encoding replaced JSON encoding on PUSH
- Tests that previously checked push_sync response features now use app.get() after push -- keyless sources return empty feature maps by design
- Added filter parameter to @dataset decorator as it was missing but needed for test_cascade_with_filter migration

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Removed TestEncodePush instead of renaming to encode_push_binary**
- **Found during:** Task 1
- **Issue:** Plan said to rename encode_push calls to encode_push_binary, but encode_push was a JSON-based encoder that no longer exists. encode_push_binary uses a completely different binary format, so the TestEncodePush tests are not applicable.
- **Fix:** Removed the 2-test TestEncodePush class entirely (dead code testing removed function)
- **Files modified:** python/tests/test_protocol.py
- **Verification:** 44 tests collect and pass
- **Committed in:** 50afbd2

**2. [Rule 3 - Blocking] Added filter parameter to @dataset decorator**
- **Found during:** Task 2
- **Issue:** test_cascade_with_filter uses stream-level filter which was only on old @st.stream API. @dataset had no filter parameter, blocking migration.
- **Fix:** Added filter parameter to dataset() decorator, DatasetDef.__init__(), _compile(), select(), and drop()
- **Files modified:** python/tally/_dataset.py
- **Verification:** test_cascade_with_filter passes with new API
- **Committed in:** 1852805

**3. [Rule 1 - Bug] Changed push assertions to use GET for keyless source pattern**
- **Found during:** Task 2
- **Issue:** Old tests asserted features from push_sync response. With new API, push targets keyless @source which returns empty features. Downstream features must be read via GET.
- **Fix:** Changed test assertions to use app.get(key) after push_sync instead of checking push response
- **Files modified:** python/tests/test_integration.py
- **Verification:** All 17 integration tests pass
- **Committed in:** 1852805

---

**Total deviations:** 3 auto-fixed (2 bugs, 1 blocking)
**Impact on plan:** All auto-fixes necessary for correctness. No scope creep. filter param addition is a natural extension of @dataset API.

## Issues Encountered
None beyond the deviations documented above.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- conftest.py now uses new API convention, unblocking all remaining test migration
- 376 tests collecting (up from 331), all green
- Remaining test files (test_stream.py, test_view.py, test_operators.py, etc.) ready for migration in plans 19-02 through 19-05

---
*Phase: 19-test-migration-and-old-api-removal*
*Completed: 2026-04-13*
