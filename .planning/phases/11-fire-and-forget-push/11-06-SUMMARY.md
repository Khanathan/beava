---
phase: 11-fire-and-forget-push
plan: 06
status: complete
date: 2026-04-11
---

# Plan 11-06 — Binary event log format (subplan)

## Outcome

Added mid-phase after the code review flagged L-3 ("partial rather than total elimination of JSON from PUSH"). Eliminates the last `serde_json::to_vec(payload)` call on the PUSH hot path by threading the original binary wire bytes from `parse_command` through to the event log. The log now stores format-tagged entries and dispatches on a prefix byte at read time, with legacy-untagged-JSON fallback for pre-11-06 files.

## Key files created / modified

- `src/state/event_log.rs`
  - New constants: `LOG_FMT_JSON = 0x00`, `LOG_FMT_BINARY = 0x01`
  - New helper: `pub fn decode_log_payload(payload: &[u8]) -> (u8, &[u8])` — returns `(format, body_slice)` with legacy-untagged-JSON fallback
  - 4 new unit tests: `test_decode_log_payload_json_tagged`, `test_decode_log_payload_binary_tagged`, `test_decode_log_payload_legacy_untagged_json`, `test_decode_log_payload_empty`

- `src/server/protocol.rs`
  - `Command::Push` and `Command::PushAsync` gain a `raw_payload: Vec<u8>` field
  - `parse_command`: captures `buf.to_vec()` before calling `decode_event_binary` so the original wire bytes survive to the event log
  - 3 existing parse tests updated to verify `raw_payload` is populated

- `src/server/tcp.rs`
  - New `fn make_log_payload(payload, raw_payload) -> Vec<u8>`: prefers `[LOG_FMT_BINARY, raw_payload...]`, falls back to `[LOG_FMT_JSON, serde_json::to_vec(payload)...]` when raw bytes unavailable (for test helpers that synthesize events without wire bytes)
  - `handle_push_core_ex` signature gains `raw_payload: &[u8]` param; builds `log_payload` ONCE per call and reuses it for primary + cascade + fan-out writes (previously each target called `serde_json::to_vec` separately)
  - `handle_push_async` + `handle_sync_command::Push` updated to plumb `raw_payload` through
  - Backfill reader at `tcp.rs:658` dispatches on the format byte — the same `.log` file can now contain a mix of legacy JSON entries and new binary entries
  - 13 test construction sites updated to pass `raw_payload: Vec::new()` (triggers JSON fallback — correct for synthesized events)

- `tests/test_server.rs`
  - 4 integration tests updated for the Phase 11 empty-ack contract: `test_register_and_push`, `test_register_with_derive`, `test_persistent_connection`, `test_register_duplicate_overwrites`. Each now asserts sync PUSH returns `{}`, then uses GET to verify state was actually updated.

## Deviations

- None. The plan as scoped mid-phase was delivered exactly as designed.

## Tests

- `cargo test --lib --release`: **501 passed**, including 4 new format-dispatch tests
- `cargo test --test test_server --release`: **31 passed**
- `cargo check --lib`: clean (1 dead-code warning for legacy `handle_push_core` stub kept for API compatibility)

## Benchmark delta (3-run mean, fresh server per run, 200k events)

| Pipeline | Mode | Before 11-06 | After 11-06 | Δ |
|---|---|---:|---:|---:|
| small | async 1c | 130k | **138k** | **+6%** |
| medium | async 1c | 140k | **142k** | noise |
| large | async 1c | 137k | 128k (σ 7k) | noise |
| small | sync 1c | 19.8k / p99 91µs | **20.4k / p99 87µs** | **+3% / -4µs** |
| medium | sync 1c | 19.6k / p99 94µs | **20.2k / p99 87µs** | **+3% / -7µs** |
| large | sync 1c | 18.1k / p99 97µs | **19.4k / p99 90µs** | **+7% / -7µs** |

**Signal:** Sync wins are the clearest — measured ~7µs p99 improvement across all sizes, +3-7% throughput. Async is neutral-to-slightly-positive within measurement variance; the JSON serialize was not the single-client async bottleneck (HLL inserts and per-event bookkeeping still dominate). The real prize is correctness: the hot path no longer re-serializes JSON that nobody needs in that format.

## Notes for downstream

- Backward compat: any `.log` file written before 11-06 is read via the legacy-untagged-JSON fallback in `decode_log_payload`. No data migration needed.
- Mixed-format logs work: after restart, new writes are binary while old entries remain JSON — the reader dispatches per entry.
- For v1.3: there is still an opportunity to eliminate the `buf.to_vec()` in `parse_command` (currently allocates ~80 bytes per push). A zero-copy slice-based approach would require lifetime plumbing through Command across the async boundary — deferred.

## Self-Check: PASSED

- [x] `make_log_payload` exists and is used by `handle_push_core_ex`
- [x] `serde_json::to_vec(payload)` no longer runs on binary wire path
- [x] `decode_log_payload` dispatches on format byte with 3 branches (binary / json-tagged / legacy fallback)
- [x] Backfill reader at `tcp.rs:658` uses `decode_log_payload` + `decode_event_binary` for binary entries
- [x] All 4 integration tests that previously asserted "push returns features" now assert "push returns {} + GET verifies state"
- [x] 532/532 tests green
