---
phase: 11-fire-and-forget-push
plan: 04
status: complete
date: 2026-04-11
---

# Plan 11-04 — raw-TCP tests migrated to binary PUSH + Phase 11 tests

## Outcome

Rewired `tests/test_server.rs` to speak the Phase 11 binary PUSH wire format and added three new integration tests covering `OP_PUSH_ASYNC`, `OP_FLUSH`, and a malformed async PUSH.

## Key files modified

- `tests/test_server.rs`
  - `use` imports expanded to include `OP_PUSH_ASYNC`, `OP_FLUSH`, and all five `TYPE_*` constants.
  - `build_push_payload` rewritten: instead of appending `serde_json::to_vec(event)` after the stream-name header, it now emits `[u16 field_count][field...]` with one field per map entry, dispatching on JSON type to write TYPE_NULL / TYPE_BOOL / TYPE_I64 / TYPE_F64 / TYPE_STR. Nested arrays/objects panic with a clear message.
  - All 9 existing `send_frame(.., OP_PUSH, ..)` call sites automatically pick up the new wire format through the shared helper — no per-callsite changes required.
  - Three new `#[tokio::test]` functions added at the bottom of the file:
    - `test_push_async_roundtrip_then_get` — sends an `OP_PUSH_ASYNC` frame directly with `write_all` (bypassing `send_frame` so it does NOT read a response), then follows with a normal `OP_GET` via `send_frame` and asserts the feature map reflects the async push. Exercises Plan 11-03's `Ok(None)` write-elision path.
    - `test_flush_roundtrip` — sends `OP_FLUSH` with an empty body, asserts `STATUS_OK` and empty payload.
    - `test_push_async_malformed_returns_error` — sends a malformed async payload (valid stream + field_count=1 + key + type tag `0xFF`) and asserts the server responds with `STATUS_ERROR` containing "type tag" or "protocol" in the message. Proves error frames fly even on the async path.

## Deviations

- Integer vs float caveat was a no-op. All pre-existing tests use `amount: 50.0` literals, which `serde_json::json!` parses as f64. The new `build_push_payload` dispatches `Number` via `as_i64()` first, so integer-valued numbers go to TYPE_I64 and float-valued numbers go to TYPE_F64. No test required a rewrite.

## Tests

- `cargo test --test test_server` — **31 passed**, 0 failed. The 28 pre-existing tests stayed green and the 3 new Phase 11 tests joined them.
- `cargo test --test test_pipeline` — 25 passed, 0 failed.
- `cargo test --test test_snapshot` — 7 passed, 0 failed.
- `cargo test --test test_incremental_snapshot` — 6 passed, 0 failed.
- `cargo test --test test_debug_ui` — still green.

Full `cargo test` (all integration binaries at once) triggered a linker OOM due to disk-full on the build volume (4.6G /data partition, 0 free). I freed the previous release target to give 230 MB headroom; the per-test-binary runs above all compile and pass individually. Plan 11-05 will need to build `--release` from scratch for the benchmark gate — disk headroom is a known concern for that plan.

## Self-Check: PASSED

- [x] `build_push_payload` produces Phase 11 binary format
- [x] All 9 existing OP_PUSH call sites still green
- [x] `test_push_async_roundtrip_then_get` exists and passes
- [x] `test_flush_roundtrip` exists and passes
- [x] `test_push_async_malformed_returns_error` exists and passes
- [x] No existing test was marked `#[ignore]`
- [x] Git commit: `944e673` test(11-04)
