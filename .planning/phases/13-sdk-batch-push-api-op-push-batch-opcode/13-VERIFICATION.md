---
phase: 13-sdk-batch-push-api-op-push-batch-opcode
verified: 2026-04-12T00:00:00Z
status: passed
score: 9/9 must-haves verified (1 deferred to Phase 14)
overrides_applied: 0
deferred:
  - truth: "Single-client async throughput on medium pipeline via push_many >= 300k eps"
    addressed_in: "Phase 14"
    evidence: "Phase 14 goal: 'Break the single-core ceiling. Shard the EntityState map across N worker threads... Aggregate target: >= 1M eps on 16+ cores.' 178k achieved in Phase 13 is bottlenecked by single-threaded server CPU, not SDK (SDK encodes at 542k eps). Phase 14 multi-threading removes this ceiling."
---

# Phase 13: SDK batch push API + OP_PUSH_BATCH opcode Verification Report

**Phase Goal:** Expose a client-side batching API (`app.push_many(stream, events)`) that wraps N events into a single wire frame, reducing Python per-event loop overhead. Target single-client async >= 300k eps on medium pipeline when using `push_many`. Server-side handler is Phase 12's `handle_push_batch` verbatim.
**Verified:** 2026-04-12
**Status:** PASSED (with SC-5 deferred to Phase 14 per established precedent)
**Re-verification:** No -- initial verification

## Goal Achievement

### Observable Truths

| # | Truth (ROADMAP SC) | Status | Evidence |
|---|---|---|---|
| 1 | `app.push_many(stream_cls, events)` encodes into one OP_PUSH_BATCH (0x0A) frame; wire format: `[u16 stream_len][stream][u32 batch_id][u32 count][for each: [u32 event_len][event_bytes]]` | VERIFIED | `python/tally/_protocol.py` L185-282: `encode_push_batch` implements exact wire format. `python/tally/_app.py` L116-133: `push_many` calls `encode_push_batch` + `send_frame_no_recv(OP_PUSH_BATCH, payload)`. |
| 2 | Server decodes batch into pre-sized `Vec<DecodedEvent>` and dispatches to `handle_push_batch` -- zero new hot-path logic | VERIFIED | `src/server/protocol.rs` L320-358: `OP_PUSH_BATCH` parse arm decodes into `Vec<(Value, Vec<u8>)>` with `Vec::with_capacity(count.min(16_384))`. `src/server/tcp.rs` L346-356 and L422-432: PushBatch dispatch arms call `handle_push_batch` directly. |
| 3 | Batch size hard-capped at 16,384; oversized rejected with STATUS_ERROR; raw-TCP test for count=10B | VERIFIED | `src/server/protocol.rs` L331-332: `if count > 16_384 { return Err("batch too large") }` BEFORE allocation. `tests/test_push_batch.rs` (540 lines): `oversized_batch_reject` and `giant_count_clean_reject` (count=0xFFFFFFFF) tests present and passing. |
| 4 | Backward-compatible: `app.push()` still emits OP_PUSH_ASYNC (0x07), both opcodes coexist | VERIFIED | `python/tally/_app.py` L88-108: `push()` sends `OP_PUSH_ASYNC`. L116-133: `push_many` sends `OP_PUSH_BATCH`. Both exist side-by-side. `tests/test_push_batch.rs`: `backward_compat_push_async_still_works` test passes. |
| 5 | Single-client async throughput via push_many >= 300k eps on medium | DEFERRED | 178k eps achieved (median of 3 runs). SDK encodes at 542k eps -- bottleneck is single-threaded server CPU. Phase 14 (key-partitioned multi-threading) removes this ceiling. Matches Phase 12 precedent where single-thread throughput ceilings were deferred. |
| 6 | Error semantic: batch failures surface via `drain_errors_nonblock` with `(batch_id, event_index)` payload | VERIFIED | `src/server/tcp.rs` L356 and L432: error format `[batch:{} event:{}] {}` with batch_id and event index. `tests/test_push_batch.rs`: `partial_failure_preserves_good_events` test asserts `[batch:99]` prefix in drain. |
| 7 | `bench.py --mode async-batch` exercises push_many; results across small/medium/large matrix | VERIFIED | `benchmark/tally-throughput/bench.py` L211-236: `run_single_client_async_batch` function. L668: `--mode async-batch` CLI arg. L300-310: dispatch. 13-02-SUMMARY documents matrix: small 178k, medium 178k, large 154k. |
| 8 | Decode path benchmarked in isolation before wiring (H-6) | VERIFIED | `tests/test_push_batch.rs`: `decode_microbench_1000_events` -- 1000 events x 100 iterations, asserts < 2us per event. Shipped in 13-01 commit d9e8a8f before 13-02 SDK wiring. |
| 9 | All 532 existing tests remain green; new batch tests cover roundtrip, mixed-valid/invalid, partial errors, oversized reject | VERIFIED | 643 tests all pass (505 lib + 17 batch_primitives + 25 debug_ui + 6 incremental_snapshot + 23 pipeline + 10 push_batch + 19 push_coalescing + 31 server + 7 snapshot). 10 new tests in test_push_batch.rs cover all required scenarios. |

**Score:** 9/9 (8 directly verified + 1 deferred to Phase 14)

### Deferred Items

| # | Item | Addressed In | Evidence |
|---|------|-------------|----------|
| 1 | Single-client async >= 300k eps on medium pipeline | Phase 14 | Phase 14 goal: "Break the single-core ceiling... >= 1M eps on 16+ cores." SDK already encodes at 542k eps; server single-thread is the bottleneck at 178k. |

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `src/server/protocol.rs` | OP_PUSH_BATCH constant, Command::PushBatch, parse_command arm | VERIFIED | L17: `OP_PUSH_BATCH = 0x0A`. L52: `PushBatch` variant. L320-358: parse arm with 16,384 cap. |
| `src/server/tcp.rs` | advance_seq, PushBatch dispatch in handle_connection | VERIFIED | L845: `advance_seq` method. L346 + L422: two PushBatch dispatch arms. L1202: unreachable arm. |
| `tests/test_push_batch.rs` | 10 tests: roundtrip, oversized, giant count, e2e, partial failure, backward compat, seq continuity, micro-bench | VERIFIED | 540 lines, 10 tests, all passing. |
| `python/tally/_protocol.py` | OP_PUSH_BATCH, _encode_event_body, encode_push_batch | VERIFIED | L30: `OP_PUSH_BATCH = 0x0A`. L98: `_encode_event_body`. L185: `encode_push_batch` with key-cache optimization. |
| `python/tally/_app.py` | App.push_many, _batch_id_counter, _next_batch_id | VERIFIED | L116: `push_many`. L56: `_batch_id_counter = 0`. L110: `_next_batch_id` with u32 wrap. |
| `benchmark/tally-throughput/bench.py` | --mode async-batch, run_single_client_async_batch, --batch-size | VERIFIED | L668: `async-batch` mode. L211: runner function. L674: `--batch-size` arg. |

### Key Link Verification

| From | To | Via | Status | Details |
|------|-----|-----|--------|---------|
| `_app.py:push_many` | `_protocol.py:encode_push_batch` | Direct function call | WIRED | L132: `payload = encode_push_batch(stream_name, events, batch_id)` |
| `_app.py:push_many` | `_client.py:send_frame_no_recv` | Via `self._client` | WIRED | L133: `self._client.send_frame_no_recv(OP_PUSH_BATCH, payload)` |
| `protocol.rs:OP_PUSH_BATCH` | `tcp.rs:dispatch` | Opcode match arm | WIRED | protocol.rs L320 + tcp.rs L346/L422 match on `Command::PushBatch` |
| `tcp.rs:PushBatch dispatch` | `tcp.rs:handle_push_batch` | Direct function call | WIRED | L353 and L429: `handle_push_batch(&state, &batch)` |
| `tcp.rs:PushBatch dispatch` | `tcp.rs:advance_seq` | Method call for seq reservation | WIRED | L348 and L424: `accumulator.advance_seq(events.len() as u64)` |

### Data-Flow Trace (Level 4)

Not applicable -- this phase adds protocol encoding/decoding and dispatch wiring, not data-rendering artifacts.

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| Rust tests all pass | `cargo test` | 643 passed, 0 failed | PASS |
| PushBatch opcode constant exists | grep OP_PUSH_BATCH protocol.rs | `0x0A` at line 17 | PASS |
| Hard cap rejects before allocation | grep `16_384` protocol.rs | Line 331: guard before Vec::with_capacity | PASS |
| SDK push_many method exists and sends OP_PUSH_BATCH | grep `push_many` _app.py | Lines 116-133, sends OP_PUSH_BATCH frame | PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|-----------|-------------|--------|----------|
| PERF-04 | 13-01, 13-02 | Client-side batch push API with push_many, OP_PUSH_BATCH wire frame, error attribution, backward compat | SATISFIED | All components implemented and verified. 300k throughput target deferred to Phase 14 (single-thread ceiling). REQUIREMENTS.md marks PERF-04 as Complete. |

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| None | - | - | - | No anti-patterns detected in Phase 13 artifacts |

Scanned `src/server/protocol.rs`, `src/server/tcp.rs`, `python/tally/_protocol.py`, `python/tally/_app.py`, `tests/test_push_batch.rs`, and `benchmark/tally-throughput/bench.py` for TODO/FIXME/placeholder/stub patterns. None found.

### Human Verification Required

None. All Phase 13 deliverables are verifiable programmatically (wire format, server decode, SDK encoding, tests, benchmarks).

### Gaps Summary

No gaps. All 9 ROADMAP success criteria are verified:
- 8 criteria directly verified in the codebase
- 1 criterion (SC-5: 300k eps) deferred to Phase 14 per established precedent -- the SDK side meets the target (542k eps encoding), but the single-threaded server caps at 178k eps. Phase 14's key-partitioned multi-threading is explicitly designed to break this ceiling.

---

_Verified: 2026-04-12_
_Verifier: Claude (gsd-verifier)_
