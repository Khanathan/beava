---
phase: 02-tcp-server-and-binary-protocol
plan: 01
subsystem: server
tags: [tcp, binary-protocol, serde, tokio, axum, bytes]

# Dependency graph
requires:
  - phase: 01-core-engine
    provides: FeatureValue, FeatureMap, TallyError, StreamDefinition, FeatureDef, Expr, parse_expr
provides:
  - Binary frame encoding/decoding (encode_frame, parse_frame)
  - Response serialization (encode_response)
  - Protocol string read/write (u16 BE length + UTF-8)
  - Command parsing for all 5 opcodes (PUSH, GET, SET, MSET, REGISTER)
  - FeatureValue::to_json_value and feature_map_to_json for untagged JSON
  - REGISTER DTO deserialization (RegisterRequest, FeatureDefRequest)
  - DTO-to-domain conversion (convert_register_request -> StreamDefinition)
  - Duration string parsing (s, m, h, d, ms suffixes)
affects: [02-02 TCP server, 02-03 HTTP API, 03-python-sdk]

# Tech tracking
tech-stack:
  added: [tokio 1.50 (rt/net/io-util/macros/time), axum 0.8, bytes 1.11]
  patterns: [length-prefixed binary frames, u16 BE string encoding, flat DTO with serde rename for type discrimination]

key-files:
  created: [src/server/mod.rs, src/server/protocol.rs]
  modified: [Cargo.toml, Cargo.lock, src/lib.rs, src/types.rs]

key-decisions:
  - "Flat DTO struct with feature_type field (serde rename from 'type') instead of internally tagged enum -- simpler for Python SDK"
  - "Frame length = opcode + payload (everything after 4-byte header) -- standard length-prefix convention"
  - "MSET wire format: u32 count + per-entry [u16 key][u32 json_len][json_bytes] -- enables streaming parse"
  - "Default bucket granularity = window/30 clamped to 1s minimum -- matches Phase 1 convention"

patterns-established:
  - "Protocol functions are pure/synchronous (no async) -- all byte manipulation, fully unit-testable without networking"
  - "DTO-to-domain conversion at registration time: parse expressions, validate fields, reject early"
  - "TallyError::Protocol for all wire-level errors (truncation, invalid UTF-8, unknown opcodes)"

requirements-completed: [SRV-02, SRV-07]

# Metrics
duration: 5min
completed: 2026-04-09
---

# Phase 02 Plan 01: Binary Protocol Layer Summary

**Binary frame protocol with 5 command opcodes, REGISTER DTO-to-domain conversion with expression parsing, and FeatureValue untagged JSON serialization**

## Performance

- **Duration:** 5 min
- **Started:** 2026-04-09T15:02:48Z
- **Completed:** 2026-04-09T15:07:58Z
- **Tasks:** 2
- **Files modified:** 6

## Accomplishments
- Complete binary wire protocol: frame encode/decode, string protocol, all 5 command opcodes (PUSH, GET, SET, MSET, REGISTER)
- REGISTER DTO deserialization with conversion to domain types (StreamDefinition + parsed Expr ASTs)
- FeatureValue::to_json_value producing untagged JSON (1.5 not {"Float":1.5})
- Duration string parsing for all time suffixes (s, m, h, d, ms)
- 36 protocol tests + 5 types tests, all passing with zero regressions (172 total)

## Task Commits

Each task was committed atomically (TDD: test -> feat):

1. **Task 1: Frame protocol, command parsing, FeatureValue JSON**
   - `aa10da2` (test) - Failing tests for frame/string/command/JSON
   - `f5a93fe` (feat) - Implementation passing all tests
2. **Task 2: REGISTER DTO and duration parsing**
   - `32fe1a6` (test) - Failing tests for duration/DTO/conversion
   - `7bc6bb4` (feat) - Implementation passing all tests

## Files Created/Modified
- `src/server/mod.rs` - Server module with `pub mod protocol`
- `src/server/protocol.rs` - Binary protocol: frames, strings, commands, DTOs, duration parsing (~380 lines)
- `src/types.rs` - Added `to_json_value()` and `feature_map_to_json()` for untagged JSON
- `src/lib.rs` - Added `pub mod server` export
- `Cargo.toml` - Added tokio, axum, bytes dependencies
- `Cargo.lock` - Updated lock file with 39 new packages

## Decisions Made
- **Flat DTO with serde rename**: Used `FeatureDefRequest { feature_type: String }` with `#[serde(rename = "type")]` instead of internally tagged enum. Simpler for Python SDK to produce and avoids serde tagged enum edge cases.
- **Frame length semantics**: Length field (u32 BE) counts opcode + payload bytes (everything after the 4-byte header). Standard convention matching Redis RESP.
- **MSET per-entry format**: Each entry encoded as `[u16 key string][u32 json_len][json_bytes]` enabling sequential parse without pre-reading all entries.
- **Default bucket = window/30**: Consistent with Phase 1 convention. Clamped to minimum 1 second.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Added #[derive(Debug)] to Command enum**
- **Found during:** Task 1 (GREEN phase)
- **Issue:** `Command` enum lacked `Debug` derive, needed by `Result::unwrap_err()` in tests
- **Fix:** Added `#[derive(Debug)]` to `pub enum Command`
- **Files modified:** src/server/protocol.rs
- **Verification:** All tests compile and pass
- **Committed in:** f5a93fe

---

**Total deviations:** 1 auto-fixed (1 bug)
**Impact on plan:** Minor -- Debug derive is standard practice for enums. No scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Protocol layer provides all building blocks for Plan 02 (TCP server with tokio)
- Frame encode/decode, command parsing, and response serialization ready for async TCP handler
- REGISTER DTO conversion tested end-to-end with PipelineEngine::register
- FeatureValue JSON conversion ready for PUSH response serialization

---
*Phase: 02-tcp-server-and-binary-protocol*
*Completed: 2026-04-09*

## Self-Check: PASSED

- All 6 files exist (created + modified)
- All 4 task commits verified (aa10da2, f5a93fe, 32fe1a6, 7bc6bb4)
- All acceptance criteria confirmed (opcodes, functions, types, no rt-multi-thread)
