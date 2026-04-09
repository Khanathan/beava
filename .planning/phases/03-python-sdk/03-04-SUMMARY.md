---
phase: 03-python-sdk
plan: 04
subsystem: testing
tags: [python, integration-tests, subprocess, end-to-end, tcp-protocol]

# Dependency graph
requires:
  - phase: 03-python-sdk/01
    provides: Protocol encoding, FeatureResult types, exception hierarchy
  - phase: 03-python-sdk/02
    provides: Operator descriptors, @stream/@view decorators, _to_register_json()
  - phase: 03-python-sdk/03
    provides: TallyClient TCP connection, App class with register/push/get/set/mset
provides:
  - "Session-scoped pytest fixture starting live Tally server on random ports"
  - "12 end-to-end integration tests proving SDK-to-server round-trip for all commands"
  - "Wire format conformance validation (Python encodes, Rust decodes, Rust responds, Python decodes)"
  - "TALLY_TCP_PORT / TALLY_HTTP_PORT environment variable support in server binary"
affects: [04-persistence, 05-remaining-operators]

# Tech tracking
tech-stack:
  added: []
  patterns: [subprocess-managed-server-fixture, random-port-test-isolation, tcp-readiness-wait]

key-files:
  created:
    - python/tests/test_integration.py
  modified:
    - python/tests/conftest.py
    - src/main.rs

key-decisions:
  - "Added TALLY_TCP_PORT/TALLY_HTTP_PORT env vars to main.rs for test port isolation (deviation Rule 3)"
  - "Session-scoped server fixture: one server shared across all 12 integration tests for speed"
  - "Unique user_id keys per test to avoid state conflicts with session-scoped server"
  - "GET unknown key test adjusted for derive features evaluating to null on unvisited keys"

patterns-established:
  - "Integration test pattern: conftest.py builds binary, starts server, waits for TCP readiness, yields (host, tcp_port, http_port)"
  - "Test isolation via unique entity keys rather than per-test server restarts"

requirements-completed: [SDK-01, SDK-02, SDK-03, SDK-04, SDK-05, SDK-06, SDK-07]

# Metrics
duration: 3min
completed: 2026-04-09
---

# Phase 3 Plan 4: Integration Tests Summary

**12 end-to-end tests proving Python SDK correctly communicates with live Tally server across all commands: register, push, get, set, mset with correct feature computation and wire format**

## Performance

- **Duration:** 3 min
- **Started:** 2026-04-09T16:43:46Z
- **Completed:** 2026-04-09T16:47:00Z
- **Tasks:** 1
- **Files modified:** 3

## Accomplishments
- Session-scoped pytest fixture that builds the Tally binary, starts it on random ports, waits for TCP readiness, and kills it on teardown
- 12 end-to-end integration tests covering all SDK-* requirements: register+push, push accumulation, get, get unknown key, set, mset, typed feature results, derive expressions, wire conformance, multiple streams, dict access
- Full test suite: 147 Python tests (135 unit + 12 integration) + 246 Rust tests all passing
- Wire format proven correct by successful bidirectional Python<->Rust communication

## Task Commits

Each task was committed atomically:

1. **Task 1: Server fixture and integration tests** - `63cf7ad` (feat)

## Files Created/Modified
- `python/tests/conftest.py` - Session-scoped tally_server fixture with cargo build, random ports, subprocess lifecycle, TCP readiness wait; app fixture returning connected st.App
- `python/tests/test_integration.py` - 12 end-to-end tests: register_and_push, push_accumulates, get_features, get_unknown_key, set_features, mset_bulk, feature_result_types, derive_expression, wire_conformance, register_multiple_streams, push_returns_derive, feature_result_dict_access
- `src/main.rs` - Added TALLY_TCP_PORT/TALLY_HTTP_PORT environment variable support for configurable ports (defaults unchanged: 6400/6401)

## Decisions Made
- Added TALLY_TCP_PORT/TALLY_HTTP_PORT env vars to main.rs to enable random-port test isolation (server had hardcoded ports -- deviation Rule 3)
- Session-scoped server fixture for speed: one server process shared across all integration tests, using unique entity keys per test to avoid state conflicts
- GET unknown key test accounts for derive features evaluating to null on keys with no events (server returns rate=null because Transactions stream is registered with a derive feature)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Added TALLY_TCP_PORT/TALLY_HTTP_PORT environment variable support to main.rs**
- **Found during:** Task 1 (reading main.rs for port configuration)
- **Issue:** Server had hardcoded ports (6400/6401) with no configuration mechanism; integration tests require random ports for isolation
- **Fix:** Read TALLY_TCP_PORT and TALLY_HTTP_PORT env vars with fallback to defaults (6400/6401)
- **Files modified:** src/main.rs
- **Verification:** cargo build succeeds, cargo test passes (246 tests), integration tests use random ports successfully
- **Committed in:** 63cf7ad (part of task commit)

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Minimal main.rs change (3 lines) required for test isolation. Default behavior unchanged. All Rust tests still pass.

## Issues Encountered

- GET for unknown key returns `{"rate": null}` instead of `{}` because derive features evaluate even for keys with no events. Adjusted test assertion to verify no windowed aggregation features appear rather than strict empty dict.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- Full Python SDK is complete and validated: types, protocol, operators, decorators, client, App, and integration tests
- All 147 Python tests and 246 Rust tests pass
- Ready for Phase 04 (persistence) and Phase 05 (remaining operators)
- TALLY_TCP_PORT/TALLY_HTTP_PORT env vars available for future test and deployment use

## Self-Check: PASSED
