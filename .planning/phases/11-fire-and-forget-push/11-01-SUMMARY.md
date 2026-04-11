---
phase: 11-fire-and-forget-push
plan: 01
status: complete
date: 2026-04-11
---

# Plan 11-01 — Binary PUSH decoder + new opcodes

## Outcome

Added the binary PUSH event payload decoder (`decode_event_binary`), the new opcodes `OP_PUSH_ASYNC = 0x07` and `OP_FLUSH = 0x08`, the five type-tag constants (`TYPE_NULL..TYPE_STR`), and two new `Command` enum variants (`PushAsync`, `Flush`). `parse_command` now dispatches `OP_PUSH` through the binary decoder (replacing `read_json_payload`), dispatches `OP_PUSH_ASYNC` the same way into `Command::PushAsync`, and maps `OP_FLUSH` to `Command::Flush`.

## Key files created / modified

- `src/server/protocol.rs`
  - New constants: `OP_PUSH_ASYNC`, `OP_FLUSH`, `TYPE_NULL`, `TYPE_BOOL`, `TYPE_I64`, `TYPE_F64`, `TYPE_STR`
  - New function: `pub fn decode_event_binary(buf: &mut &[u8]) -> Result<serde_json::Value, TallyError>`
  - `Command` enum extended with `PushAsync` and `Flush`
  - `parse_command` updated: `OP_PUSH` now calls `decode_event_binary`; `OP_PUSH_ASYNC` and `OP_FLUSH` arms added
  - Existing `test_parse_command_push` rewritten to use binary fixture
  - Added helpers `build_binary_push_payload` and `build_event_only` in the test module
  - 20+ new tests: `test_decode_event_binary_*` (16), `test_parse_command_push_binary`, `test_parse_command_push_async_binary`, `test_parse_command_flush`, `test_parse_command_push_rejects_json`, `test_parse_command_unknown_opcode_still_errors`

- `src/server/tcp.rs`
  - Defensive `Command::PushAsync | Command::Flush` arm in `handle_sync_command` that returns `TallyError::Protocol`. This keeps the lib compiling in Plan 11-01 and will be replaced by Plan 11-03's dispatch-layer wiring in `handle_connection` (the new arms intercept these variants before they reach `handle_sync_command`).

## Deviations

- **Added a defensive arm in `handle_sync_command`.** The plan said "cargo check warnings about missing tcp.rs match arms are Plan 03's problem," but the missing arms cause a hard E0004 error (non-exhaustive patterns), not a warning. Added a stub arm that returns `TallyError::Protocol("must be handled by handle_connection dispatch")`. Plan 11-03 will replace this with the real dispatch wiring.

## Tests

`cargo test --lib server::protocol::tests` — 96 passed, 0 failed. Includes all pre-existing protocol tests plus the 20+ new tests from this plan.

`cargo check --lib` — clean (no errors, no new warnings).

## Notes for downstream plans

- Plan 11-02 (Python SDK) should use the same type-tag values (0x00..0x04) and the same wire layout.
- Plan 11-03 (tcp.rs) must replace the defensive arm in `handle_sync_command` with the real `handle_connection` dispatch arms (`Ok(None)` on async success, `Ok(Some(vec![]))` on Flush). The defensive arm currently returns `Err` — if the dispatch wiring is missed, PushAsync/Flush will surface errors to the client.
- Plan 11-04 (raw-TCP tests) needs to import `TYPE_*` constants via `tally::server::protocol::TYPE_*`.

## Self-Check: PASSED

- [x] `decode_event_binary` function exists and is `pub`
- [x] `Command::PushAsync` and `Command::Flush` variants defined
- [x] `parse_command` dispatches OP_PUSH / OP_PUSH_ASYNC through binary decoder and OP_FLUSH into Command::Flush
- [x] `serde_json::from_slice` no longer on OP_PUSH decode path
- [x] 16+ decode_event_binary tests + 5+ parse_command tests green
- [x] `cargo test --lib server::protocol::tests` is fully green (96 tests)
- [x] Git commit exists: `631afda` feat(11-01)
