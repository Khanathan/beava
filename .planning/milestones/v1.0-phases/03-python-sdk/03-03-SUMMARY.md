---
phase: 03-python-sdk
plan: 03
subsystem: sdk
tags: [python, tcp-client, app-class, auto-reconnect, binary-protocol]

# Dependency graph
requires:
  - phase: 03-python-sdk/01
    provides: Protocol encoding (encode_frame, encode_push/get/set/mset/register), types (FeatureResult, ConnectionError, ProtocolError)
  - phase: 03-python-sdk/02
    provides: DSL layer (@stream, @view decorators, _to_register_json(), operator descriptors)
provides:
  - TallyClient TCP connection with lazy connect, auto-reconnect, frame I/O, and configurable timeout
  - App class with register/push/get/set/mset methods for full server communication
  - "Complete public API: import tally as st; app = st.App('localhost:6400')"
affects: [03-python-sdk/04]

# Tech tracking
tech-stack:
  added: []
  patterns: [mock-tcp-server-testing, lazy-connect-with-auto-reconnect, address-parsing]

key-files:
  created:
    - python/tally/_client.py
    - python/tally/_app.py
    - python/tests/test_client.py
    - python/tests/test_app.py
  modified:
    - python/tally/__init__.py

key-decisions:
  - "TallyClient auto-reconnect catches ConnectionError on send/recv, sets _sock=None, reconnects once and retries"
  - "App._parse_address uses rsplit(':',1) to handle IPv6-safe splitting; default port 6400"
  - "App._send checks STATUS_ERROR and raises ProtocolError with server message, centralizing error handling"

patterns-established:
  - "Mock TCP server pattern: threading-based with accept_count for reconnection tests"
  - "Lazy connection: _sock starts None, _ensure_connected() connects on first command"
  - "Address parsing: host:port with default port 6400"

requirements-completed: [SDK-04, SDK-05, SDK-06, SDK-07]

# Metrics
duration: 4min
completed: 2026-04-09
---

# Phase 3 Plan 3: TCP Client and App Class Summary

**TCP client with lazy connect, auto-reconnect, and timeout plus App class wiring register/push/get/set/mset with full public API export**

## Performance

- **Duration:** 4 min
- **Started:** 2026-04-09T16:36:55Z
- **Completed:** 2026-04-09T16:41:03Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments
- TallyClient with lazy TCP connection, frame-level send/receive, transparent auto-reconnect after server disconnect, and configurable socket timeout
- App class parsing host:port addresses, creating TallyClient, and exposing all 5 commands (register, push, get, set, mset) with proper error handling
- Full public API exported from tally package: `import tally as st; app = st.App("localhost:6400")` with all operators, decorators, types, and App
- 23 new tests (9 client + 14 app) with mock TCP servers, bringing total to 135 passing tests

## Task Commits

Each task was committed atomically (TDD RED then GREEN):

1. **Task 1: TCP client with frame I/O, auto-reconnect, and timeout**
   - `f0d6468` (test: failing TCP client tests -- TDD RED)
   - `5e4ec91` (feat: TCP client with auto-reconnect and timeout -- TDD GREEN)

2. **Task 2: App class with register/push/get/set/mset and __init__.py wiring**
   - `0d20dfd` (test: failing App class tests -- TDD RED)
   - `37cb11a` (feat: App class with register/push/get/set/mset and public API -- TDD GREEN)

## Files Created/Modified
- `python/tally/_client.py` - TallyClient with lazy connect, auto-reconnect, frame I/O, timeout, context manager
- `python/tally/_app.py` - App class with address parsing, register/push/get/set/mset, ProtocolError on server errors
- `python/tally/__init__.py` - Added App export to complete public API
- `python/tests/test_client.py` - 9 tests: connect, reconnect, timeout, recv_exact EOF, oversized frame, context manager
- `python/tests/test_app.py` - 14 tests: address parsing, register (single/multi/error), push, get, set, mset, exports

## Decisions Made
- TallyClient auto-reconnect catches ConnectionError during send/recv, nullifies socket, reconnects once and retries -- single retry is sufficient for server restart scenarios
- App._parse_address uses rsplit(':', 1) for safe host:port splitting with default port 6400
- App._send centralizes error handling: checks STATUS_ERROR and raises ProtocolError with decoded server message, so individual command methods stay clean

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- Full SDK public API is complete: types, protocol, operators, decorators, client, and App
- Ready for Plan 04 (integration tests against live Tally server)
- All 135 Python tests pass

## Self-Check: PASSED

All 5 files verified present. All 4 commit hashes verified in git log.

---
*Phase: 03-python-sdk*
*Completed: 2026-04-09*
