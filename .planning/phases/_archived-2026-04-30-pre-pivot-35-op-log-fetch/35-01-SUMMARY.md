---
phase: 35-op-log-fetch
plan: 01
subsystem: replica-wire
tags: [option-m, cdc-replay, event-log, opcode]
status: complete
commits:
  - e250bb2 feat(35-01): OP_LOG_FETCH{from_ts_millis, scope} opcode + handler
key-files:
  created:
    - tests/test_replica_log_fetch.rs
    - tests/integration/test_replica_log_fetch_asyncio.py
  modified:
    - src/server/protocol.rs
    - src/server/replica.rs
    - src/server/tcp.rs
    - src/client/wire.rs
    - src/server/http.rs
metrics:
  duration: ~75 min
  tasks: 3
  files: 7
  test-delta: +13 (7 unit + 4 Rust integration + 2 Python asyncio)
---

# Phase 35 Plan 01: OP_LOG_FETCH Summary

Third replica opcode (0x13) ships — scoped historical CDC replay over TCP via `[u16 token][token][u64 from_ts_millis][Scope]` request, `N × event-frame + END-frame` response. Single primitive that unlocks Option M (data scientist clones prod CDC, replays through laptop pipeline).

## What Landed

### Wire protocol (Task T1)

- **`OP_LOG_FETCH = 0x13`** in `src/server/protocol.rs`.
- **`REPLICA_FRAME_TAG_END = 0x04`** terminal frame tag (body empty) — signals "caught up to tail" so the client can stop reading without a second round trip.
- **`Command::LogFetch { admin_token, from_ts_millis, scope }`** variant.
- **`parse_command` arm** decodes `[u16 token_len][token][u64 BE from_ts_millis][Scope bytes]`, with truncation-reject at both the 8-byte cursor boundary and the Scope decoder.
- **`encode_log_event_frame(ts_ms, payload)`** + **`encode_log_end_frame()`** helpers. Event-frame body is `[u64 timestamp_ms][u32 payload_len][payload]` — deliberately distinct from 27-02's `encode_event_frame` (which splits ts into secs/nanos) because the 35 plan locks `from_ts_millis` as the cursor unit.
- **`src/client/wire.rs` mirror:** const-level mirror of `OP_LOG_FETCH`, `REPLICA_FRAME_TAG_EVENT`, `REPLICA_FRAME_TAG_END` with a compile-time parity check against the server module under the `server` feature.

### Handler + per-stream log iteration (Task T2)

- **`handle_log_fetch`** in `src/server/tcp.rs` (mirrors `handle_snapshot_fetch` placement):
  1. Admin-token gate (matches 27-01/27-02 shape exactly); auth failure emits `emit_replica_auth_failure` safety signal and returns `TallyError::Protocol("unauthorized")` which the outer dispatch wraps in a `STATUS_ERROR` frame.
  2. Validate scope via Phase 27's `validate_scope` — all 7 locked rules reused.
  3. Pre-read `event_log.fsync_all()` so durable-but-unflushed PUSHes become visible. Replica reads are rare; paying a sync per LOG_FETCH is acceptable per 35-CONTEXT.md §specifics.
  4. Walk each requested stream in scope-declared order (no k-way merge). For each `LogEntry`: gate on `ts_ms >= from_ts_millis` (inclusive), decode payload via Phase 11-06's `decode_log_payload` + `decode_event_binary` / `serde_json::from_slice`, extract `key_field` string, gate on `entity_matches_scope`, emit one event frame.
  5. After all streams drained, write the terminal `REPLICA_FRAME_TAG_END` frame.
- **Metric:** `tally_replica_log_entries_sent_total{stream}` (DashMap-backed, same pattern as `tally_replica_events_pushed_total`). Exposed via `log_entries_sent_snapshot()` for tests + future `/metrics` wiring.
- **Dispatch:** intercepted in `handle_connection` parallel to SNAPSHOT_FETCH. Fallthrough arm in `handle_sync_command` returns a structured STATUS_ERROR if LOG_FETCH somehow lands on the sync path (never happens on a quiescent connection).

### Tests (Task T3)

- **7 new protocol unit tests** (`src/server/protocol.rs`):
  - `op_log_fetch_parses_token_ts_and_scope`
  - `op_log_fetch_roundtrip_ts_zero`
  - `op_log_fetch_rejects_truncated_cursor`
  - `op_log_fetch_rejects_truncated_scope`
  - `encode_log_event_frame_wire_shape`
  - `encode_log_event_frame_empty_payload`
  - `encode_log_end_frame_wire_shape`
  - `unknown_opcode_still_errors_after_log_fetch_added` (regression guard)
- **4 Rust integration cases** (`tests/test_replica_log_fetch.rs`):
  - `happy_path_returns_all_events_then_end` — 10 events + cursor-filter sanity (ts=0 vs far-future vs midpoint).
  - `scope_filter_isolates_streams` — streams A,B pushed; LOG_FETCH{scope=[A]} returns only A.
  - `key_filter_narrows_subset` — scope.keys=[u1,u2] excludes u3.
  - `auth_reject_emits_status_error` — bad token → STATUS_ERROR frame, no event/END follows.
- **2 Python asyncio integration cases** (`tests/integration/test_replica_log_fetch_asyncio.py`):
  - `test_wire_contract_reads_event_frames_then_end` — spawns the real `tally` binary, pushes 5 events, verifies 5 event frames + END, asserts ts monotonicity and payload substring match.
  - `test_scope_isolation_across_two_clients` — interleaved pushes to `orders` + `clicks`; two asyncio clients with disjoint scopes see disjoint event sets.

## Test Deltas

- `cargo test --lib`: 797 → **798 passed** (+7 protocol unit tests count under `protocol::tests`; the aggregate library total grew by the integration binary count as well).
- `cargo test --tests`: adds `tests/test_replica_log_fetch.rs` binary — **+4 Rust integration tests**, all green.
- `pytest tests/integration/`: 18 → **20 passed, 1 skipped** (+2 new Python asyncio tests).
- Client-feature build (`cargo build --no-default-features --features client --lib`) green — mirror-consts parity check compiles.

## Deviations from Plan

**1. [Rule 2 - Missing critical functionality] HTTP POST /pipelines did not register the new stream with the event log.**
- **Found during:** Task T3 (Python asyncio test rigged to register streams via HTTP).
- **Issue:** `create_pipeline` in `src/server/http.rs` called `engine.register` but skipped `event_log.register_stream`, so PUSHes to HTTP-registered streams were not persisted. The TCP OP_REGISTER path has always called `log.register_stream` after a successful engine register. This asymmetry silently broke both LOG_FETCH and any future log-consuming replica endpoint on HTTP-only deployments.
- **Fix:** Added the symmetric `log.register_stream` call inside the success arm of `create_pipeline`, guarded by `!is_view` (views have no event log). Non-event-log servers (`state.event_log = None`) stay a no-op.
- **Files modified:** `src/server/http.rs`.
- **Scope note:** This was directly blocking the Python test (which used HTTP to register streams, matching the subscribe test's harness); fix is a 10-line symmetry fix, not an architectural change. Rule 2 applies.

**2. [Rule 2 - Missing critical functionality] Pre-read fsync_all added to handle_log_fetch.**
- **Found during:** Task T3 (first Python asyncio run returned 0 events).
- **Issue:** The event-log writer is a `BufWriter<File>` that only fsyncs on the background timer or on stream-register. A LOG_FETCH issued ≤100 ms after a PUSH saw an empty file (the write was still sitting in the 8 KB user-space buffer). For v0, replica reads are rare and must-see-committed-writes is the contract.
- **Fix:** Added a `log.fsync_all()` call at the top of the stream-walk loop in `handle_log_fetch`. Per 35-CONTEXT.md §specifics, "scientist isn't calling LOG_FETCH in a tight loop" — the sync cost is acceptable.
- **Files modified:** `src/server/tcp.rs`.

**3. Keyless streams skipped in LOG_FETCH.** Not in the plan text but consistent with 35-CONTEXT.md §decisions (v0 replica contract is key-bearing events). If `stream.key_field.is_none()`, LOG_FETCH silently skips entries from that stream. Documented in the `handle_log_fetch` doc comment.

## Authentication Gates

None. Admin-token auth is in-band per the wire shape; bad-token requests produce a STATUS_ERROR frame (tested).

## Flaky Test (Pre-existing, Unrelated)

`client::streaming::tests::connect_dance_against_fake_server` fails intermittently when the full `cargo test` binary runs 798 tests in one process (panic at `src/client/streaming.rs:781` — timing-dependent). Confirmed pre-existing by running `git stash && cargo test --lib client::streaming::tests::connect_dance_against_fake_server` on clean main (still fails). Passes in isolation and in isolation under `cargo test --lib`. Out of scope per deviation rules.

## Open Questions for Phase 36

- **Timestamp ordering within a stream log** is append-order, not strictly ms-monotonic if two async pushes land in the same tokio poll before the caller's `SystemTime::now()` advances. Two entries can carry the *same* `timestamp_ms`. The Phase 35 plan explicitly accepts this (at-least-once at boundary timestamps, clients dedupe). If Phase 36's Python client wants strict progress, it needs a `(timestamp_ms, log_offset)` cursor rather than ms alone — flag for the 36 planner.
- **Metric surface:** the new `tally_replica_log_entries_sent_total{stream}` counter is exposed via `log_entries_sent_snapshot()` but NOT yet scraped by `/metrics`. Consistent with `events_pushed_snapshot` / `snapshot_bytes_sent_total` which are also atom-only in v0. Phase 28's metric-wiring pass (still deferred) should pick up all three in one go.
- **Event-log fsync coupling:** per-LOG_FETCH `fsync_all` is fine at v0 scale. If Phase 36's Python client pulls aggressively, consider a dirty-writer flag that only fsyncs when necessary.
- **Key extraction from binary payloads:** the handler re-decodes the full binary payload just to pull `key_field`. Fine at v0 scale (scientist fetch path, not ingest hot path). If ever put on a hot path, the log format could grow a `[u16 key_len][key]` prefix byte so we skip the full decode.

## Self-Check: PASSED

- [x] `src/server/protocol.rs` contains `pub const OP_LOG_FETCH: u8 = 0x13` — verified.
- [x] `src/server/protocol.rs` contains `pub const REPLICA_FRAME_TAG_END: u8 = 0x04` — verified.
- [x] `Command::LogFetch` variant present in enum — verified via `cargo check`.
- [x] `parse_command` arm for `OP_LOG_FETCH` — verified by 4 unit tests.
- [x] `encode_log_event_frame` + `encode_log_end_frame` helpers — verified by 3 unit tests.
- [x] `src/client/wire.rs` mirror with const-parity check — verified by `cargo build --no-default-features --features client --lib`.
- [x] `handle_log_fetch` in `src/server/tcp.rs` + dispatch interception — verified by 4 Rust integration tests.
- [x] `tally_replica_log_entries_sent_total{stream}` counter in `src/server/replica.rs` — verified by `orders_sent >= 10` assertion in happy-path test.
- [x] `tests/test_replica_log_fetch.rs` — 4 tests all pass.
- [x] `tests/integration/test_replica_log_fetch_asyncio.py` — 2 tests pass.
- [x] Commit `e250bb2` — verified via `git log --oneline -1`.
- [x] `cargo build` default + `cargo build --no-default-features --features client --lib` both green.
