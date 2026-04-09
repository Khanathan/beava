---
phase: 06-foundation
plan: 04
subsystem: sdk
tags: [python, mget, ttl, protocol, tcp]

# Dependency graph
requires:
  - phase: 06-02
    provides: MGET server command (opcode 0x06) and per-stream entity_ttl/history_ttl in Rust
  - phase: 06-03
    provides: Event log with history_ttl configuration in RegisterRequest
provides:
  - Python SDK App.mget() for batch feature reads
  - OP_MGET constant and encode_mget wire format encoding
  - entity_ttl and history_ttl kwargs on @st.stream decorator
  - TTL fields serialized in RegisterRequest JSON to server
affects: [python-sdk, integration-tests]

# Tech tracking
tech-stack:
  added: []
  patterns: [encode_mget follows encode_mset pattern, TTL fields conditionally included in JSON]

key-files:
  created: []
  modified:
    - python/tally/_protocol.py
    - python/tally/_app.py
    - python/tally/_stream.py
    - python/tally/__init__.py
    - python/tests/test_protocol.py
    - python/tests/test_app.py
    - python/tests/test_stream.py

key-decisions:
  - "encode_mget uses simple [u32 count][u16-string key]... format matching Rust MGET handler"
  - "TTL fields conditionally omitted from JSON when None for backward compatibility"
  - "Views explicitly reject entity_ttl/history_ttl with TypeError since views have no state to evict"

patterns-established:
  - "Pattern: New opcodes follow sequential numbering (0x06 after 0x05)"
  - "Pattern: Optional fields omitted from RegisterRequest JSON when None"

requirements-completed: [OPS-01, OPS-02, ELOG-04]

# Metrics
duration: 3min
completed: 2026-04-09
---

# Phase 6 Plan 4: Python SDK MGET + TTL Support Summary

**App.mget() for batch feature reads and @st.stream entity_ttl/history_ttl for per-stream eviction control**

## Performance

- **Duration:** 3 min
- **Started:** 2026-04-09T23:52:27Z
- **Completed:** 2026-04-09T23:55:33Z
- **Tasks:** 2
- **Files modified:** 7

## Accomplishments
- MGET protocol encoding (OP_MGET = 0x06) with correct wire format matching Rust server
- App.mget(keys) returns dict[str, FeatureResult] for batch feature reads in a single round trip
- @st.stream decorator accepts entity_ttl and history_ttl kwargs, serialized to RegisterRequest JSON
- Views correctly reject TTL fields with TypeError (views have no state to evict)
- Full backward compatibility: existing code without TTL args continues to work unchanged

## Task Commits

Each task was committed atomically (TDD: test then feat):

1. **Task 1: Add MGET protocol encoding and App.mget method**
   - `bc12552` (test) - failing tests for OP_MGET, encode_mget, App.mget
   - `089bac3` (feat) - OP_MGET constant, encode_mget function, App.mget method, __init__ export
2. **Task 2: Add entity_ttl and history_ttl to @st.stream decorator**
   - `77d4362` (test) - failing tests for TTL fields on stream and view rejection
   - `8d31283` (feat) - StreamMeta accepts TTL kwargs, _to_register_json includes TTL fields

## Files Created/Modified
- `python/tally/_protocol.py` - Added OP_MGET = 0x06 constant and encode_mget() function
- `python/tally/_app.py` - Added App.mget() method importing OP_MGET and encode_mget
- `python/tally/_stream.py` - StreamMeta and stream() accept entity_ttl/history_ttl, view validation
- `python/tally/__init__.py` - Export OP_MGET in __all__
- `python/tests/test_protocol.py` - Tests for OP_MGET constant and encode_mget wire format
- `python/tests/test_app.py` - Tests for App.mget with mock server
- `python/tests/test_stream.py` - Tests for TTL fields on stream and view rejection

## Decisions Made
- encode_mget uses simple [u32 count][u16-string key]... format -- no JSON length prefix needed since MGET has no per-key payload (unlike MSET which includes JSON per entry)
- TTL fields are conditionally omitted from RegisterRequest JSON when None, ensuring backward compatibility with older servers
- Views reject entity_ttl/history_ttl at metaclass level (StreamMeta.__new__) rather than in the view() decorator, providing consistent validation regardless of how views are created

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- Phase 6 (Foundation) is now fully complete with all 4 plans executed
- Python SDK fully supports all Phase 6 features: MGET, entity_ttl, history_ttl
- Ready for Phase 7+ work (DAG execution, backfill, schema evolution, debug UI)

## Self-Check: PASSED

All 7 modified files verified present. All 4 commits verified in git log. Key content (OP_MGET, encode_mget, mget, entity_ttl, history_ttl) verified in target files.

---
*Phase: 06-foundation*
*Completed: 2026-04-09*
