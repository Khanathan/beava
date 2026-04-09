---
phase: 03-python-sdk
plan: 01
subsystem: sdk
tags: [python, tcp-protocol, binary-encoding, types]

# Dependency graph
requires:
  - phase: 02-tcp-server
    provides: Binary TCP protocol wire format (protocol.rs)
provides:
  - Installable tally Python package with hatchling build system
  - FeatureResult class with attribute access for feature maps
  - TallyError/ConnectionError/ProtocolError exception hierarchy
  - Binary protocol encoding for all 5 commands (PUSH/GET/SET/MSET/REGISTER)
  - Response frame parser with DoS protection (64MB cap)
affects: [03-python-sdk]

# Tech tracking
tech-stack:
  added: [python-3.10+, hatchling, pytest]
  patterns: [TDD-contract-first, byte-level-conformance-testing]

key-files:
  created:
    - python/pyproject.toml
    - python/tally/__init__.py
    - python/tally/_types.py
    - python/tally/_protocol.py
    - python/tests/test_types.py
    - python/tests/test_protocol.py
    - python/tests/__init__.py
    - python/tests/conftest.py
  modified: []

key-decisions:
  - "FeatureResult uses __slots__ = ('_data',) with object.__setattr__ for clean attribute access without __dict__ overhead"
  - "Type annotations on protocol constants (OP_PUSH: int = 0x01) for IDE support"
  - "parse_response raises ProtocolError on STATUS_ERROR -- callers get exception with server message"

patterns-established:
  - "TDD RED-GREEN: failing tests committed before implementation, then implementation committed when green"
  - "Byte-level conformance: protocol tests verify exact byte output against Rust server wire format"
  - "Pure stdlib: socket, struct, json only -- zero external runtime dependencies"

requirements-completed: [SDK-04]

# Metrics
duration: 4min
completed: 2026-04-09
---

# Phase 3 Plan 1: SDK Types and Protocol Summary

**Installable tally Python package with FeatureResult attribute access, exception hierarchy, and byte-level binary protocol encoding matching Rust server wire format**

## Performance

- **Duration:** 4 min
- **Started:** 2026-04-09T16:22:29Z
- **Completed:** 2026-04-09T16:26:40Z
- **Tasks:** 2
- **Files modified:** 8

## Accomplishments
- Installable `tally` Python package with pyproject.toml (hatchling build system, Python >=3.10)
- FeatureResult supporting attribute access (`features.tx_count`), dict access, `to_dict()`, `__contains__`, and `__repr__`
- TallyError > ConnectionError, ProtocolError exception hierarchy
- Binary protocol encoding for all 5 commands (PUSH, GET, SET, MSET, REGISTER) matching Rust server byte-for-byte
- Response parser with MAX_FRAME_SIZE (64MB) DoS protection and error status handling
- 42 passing tests: 16 type tests + 26 protocol conformance tests

## Task Commits

Each task was committed atomically (TDD RED then GREEN):

1. **Task 1: Project skeleton, types, and type tests**
   - `a203e95` (test: failing type tests -- TDD RED)
   - `d7bd07d` (feat: FeatureResult and exception hierarchy -- TDD GREEN)

2. **Task 2: Binary protocol encoding/decoding with byte-level conformance tests**
   - `75a6088` (test: failing protocol conformance tests -- TDD RED)
   - `cf9dfcc` (feat: binary protocol encoding/decoding -- TDD GREEN)

## Files Created/Modified
- `python/pyproject.toml` - Package config with hatchling, pytest config
- `python/tally/__init__.py` - Public re-exports: types + protocol constants
- `python/tally/_types.py` - FeatureResult, TallyError, ConnectionError, ProtocolError
- `python/tally/_protocol.py` - Binary frame encoding, string encoding, 5 command encoders, response parser
- `python/tests/__init__.py` - Test package marker
- `python/tests/conftest.py` - Empty (will be populated in Plan 03)
- `python/tests/test_types.py` - 16 tests for FeatureResult and exception hierarchy
- `python/tests/test_protocol.py` - 26 byte-level conformance tests for all protocol operations

## Decisions Made
- FeatureResult uses `__slots__ = ('_data',)` with `object.__setattr__` to store data without triggering `__getattr__`, keeping attribute access clean
- Protocol constants use type annotations (`OP_PUSH: int = 0x01`) for better IDE support
- `parse_response` raises `ProtocolError` on `STATUS_ERROR` with the server's error message -- callers handle via exception rather than checking status codes
- `encode_mset` iterates `dict.items()` directly; entry order matches Python dict insertion order (3.7+)

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- Types and protocol modules are ready for Plan 02 (operator descriptors and decorators)
- Protocol encoding is ready for Plan 03 (TCP client using encode_frame + parse_response)
- FeatureResult is ready for Plan 03 (returned from app.push() and app.get())

## Self-Check: PASSED

All 9 files verified present. All 4 commit hashes verified in git log.

---
*Phase: 03-python-sdk*
*Completed: 2026-04-09*
