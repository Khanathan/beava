# Phase 02: Test Coverage Gaps

**Audited:** 2026-04-09
**Status:** Open — must resolve before Phase 3
**Gaps:** 13
**TDD Compliance:** Partial (02-01 strict TDD, 02-02 and 02-03 violated test-first)

---

## Security-Relevant Gaps (3)

### G-01: Frame length upper bound (64MB cap) untested
- **File:** `src/server/tcp.rs` (handle_connection, frame length check)
- **Risk:** If cap is removed during refactor, server could allocate 4GB on crafted input
- **Test:** Send frame header with length > 64MB, assert STATUS_ERROR response and connection close

### G-02: `write_string` panic on oversized input untested
- **File:** `src/server/protocol.rs:98` (assert on string > u16::MAX)
- **Risk:** Panic is only boundary protection; refactor could remove it silently
- **Test:** `#[should_panic]` test with string of length u16::MAX + 1

### G-03: TCP connection drop mid-frame untested
- **File:** `src/server/tcp.rs` (handle_connection, read_exact path)
- **Risk:** Client disconnects after length header but before payload — UnexpectedEof path untested
- **Test:** Connect, send 4-byte length header, then drop connection. Assert server handles gracefully without panic.

---

## Error-Branch Coverage Gaps (4)

### G-04: `read_string` with invalid UTF-8
- **File:** `src/server/protocol.rs:89`
- **Test:** Send 2-byte length + invalid UTF-8 bytes (0xFF 0xFE), assert TallyError::Protocol

### G-05: `convert_register_request` with unknown feature type
- **File:** `src/server/protocol.rs:366`
- **Test:** Register with `"type": "median"`, assert error

### G-06: `convert_register_request` missing required fields
- **File:** `src/server/protocol.rs:288-306`
- **Test:** count without `window`, sum without `field`, assert errors for each

### G-07: MSET with non-object payload entries silently skipped
- **File:** `src/server/tcp.rs:201`
- **Test:** MSET with mix of object and string entries, assert only objects are written

---

## Missing Unit Tests for Public API (3)

### G-08: `read_json_payload` — no direct unit test
- **File:** `src/server/protocol.rs:114`
- **Test:** Valid JSON, invalid JSON, empty buffer — assert correct parse and buffer advance

### G-09: `FeatureValue::as_f64` and `is_missing` — no unit tests
- **File:** `src/types.rs:26,35`
- **Test:** as_f64 on Float/Int/String/Missing, is_missing on all variants

### G-10: `feature_map_to_json` with Missing and String values
- **File:** `src/types.rs` (feature_map_to_json)
- **Test:** Map containing Missing (→ null) and String values, assert correct JSON output

---

## Behavioral Edge Cases (3)

### G-11: MSET with count=0 (empty MSET)
- **File:** `src/server/tcp.rs` (handle_mset)
- **Test:** Send MSET with 0 entries, assert OK response

### G-12: REGISTER duplicate stream name
- **File:** `src/engine/pipeline.rs` (register_stream)
- **Test:** Register "Transactions" twice, assert defined behavior (overwrite or error)

### G-13: Cross-connection state visibility
- **File:** `tests/test_server.rs`
- **Test:** PUSH on connection A, GET on connection B for same key, assert features visible

---

## TDD Compliance Issues

| Plan | Commit Pattern | TDD? |
|------|---------------|------|
| 02-01 Task 1 | `test(02-01)` → `feat(02-01)` | YES |
| 02-01 Task 2 | `test(02-01)` → `feat(02-01)` | YES |
| 02-02 Task 1 | `feat(02-02)` (tests bundled with impl) | NO |
| 02-03 Task 1 | `feat(02-03)` (no tests) | NO |
| 02-03 Task 2 | `test(02-03)` (tests after all impl) | NO |

**Action:** All gap closure tests (G-01 through G-13) MUST be written test-first to restore TDD compliance.

---

## Resolution Plan

Run `/gsd-plan-phase 2 --gaps` then `/gsd-execute-phase 2 --gaps-only` to create and execute gap closure plans. All 13 gaps should be resolved with test-first commits before proceeding to Phase 3.
