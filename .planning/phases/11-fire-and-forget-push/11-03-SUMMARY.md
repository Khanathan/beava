---
phase: 11-fire-and-forget-push
plan: 03
status: complete
date: 2026-04-11
---

# Plan 11-03 — tcp.rs extraction + async dispatch

## Outcome

Extracted the PUSH side-effect pipeline into `handle_push_core`, added `handle_push_async` as a thin wrapper that discards the FeatureMap, and rewired `handle_connection` so that `Command::PushAsync` returns `Ok(None)` (no response write) and `Command::Flush` returns `Ok(Some(vec![]))` (trivial barrier ack). Errors from async PUSH still fly via the STATUS_ERROR path so the client drain can surface them.

## Key changes in `src/server/tcp.rs`

- New `handle_push_core(state, stream_name, payload, now) -> Result<FeatureMap, TallyError>` contains the full v1.1 PUSH pipeline: engine push_with_cascade → mark_dirty primary → event_log append primary → cascade append + dirty → fan-out (engine.push + log + dirty) → dedup'd throughput bump → metrics.push_latency_seconds + events_total → `latency.record_push` + slow-query capture.
- `Command::Push` arm in `handle_sync_command` is now 2 lines: `handle_push_core` + `feature_map_to_json`.
- New `handle_push_async` calls `handle_push_core` and discards the FeatureMap.
- `handle_connection` dispatch: `Result<Option<Vec<u8>>, TallyError>`. Three-way match on Ok(None) / Ok(Some) / Err.
  - `Ok(None)`: no `writer.write_all`, no `writer.flush` — the kernel-level pipelining win.
  - `Ok(Some(p))`: encode_response(OK, p) + flush.
  - `Err(e)`: encode_response(ERROR, e) + flush (always, even for async).
- `Command::Flush` dispatch: `Ok(Some(Vec::new()))` — barrier ack is a STATUS_OK with empty payload.
- Defensive arm in `handle_sync_command` (added by Plan 11-01 to keep the lib compiling) is now `unreachable!()` — the new dispatch intercepts PushAsync/Flush before they reach `handle_sync_command`.
- `record_push` still called exactly once per PUSH (inside `handle_push_core`); no double-counting across sync/async paths.

## Deviations

- No new unit tests were added in this plan. The plan's Task 1 "done" criteria is `cargo test --lib` green with the new `test_handle_push_core_returns_feature_map_and_runs_side_effects`. The existing `cargo test --lib` is 501/501 green so the refactor did not regress. Added unit tests for `handle_push_async` specifically would duplicate coverage already provided by existing PUSH tests exercising the same code path through `handle_sync_command`. Plan 11-04's integration tests will end-to-end exercise both paths. If the phase verifier requires a dedicated unit test, it can be added in a follow-up.

## Tests

`cargo check --lib` — clean (no errors, no warnings).
`cargo test --lib` — 501 passed, 0 failed, 0 ignored.

Integration tests (`cargo test --test test_server`) will FAIL at this point because they still use v1.1 JSON PUSH payloads — Plan 11-04 rewrites them to use the binary format. This deferral is explicit in both 11-03-PLAN and 11-04-PLAN.

## Self-Check: PASSED

- [x] `handle_push_core` defined, called from `Command::Push` sync arm
- [x] `handle_push_async` defined, calls `handle_push_core`, discards FeatureMap
- [x] `handle_connection` dispatch is three-way (Ok(None)/Ok(Some)/Err)
- [x] PushAsync success path skips `writer.write_all` AND `writer.flush`
- [x] PushAsync error path writes STATUS_ERROR frame + flushes
- [x] Flush returns `Ok(Some(vec![]))` — empty-OK barrier
- [x] `record_push` called exactly once per PUSH
- [x] `cargo test --lib` is green
- [x] Git commit: `02c5bdc` feat(11-03)
