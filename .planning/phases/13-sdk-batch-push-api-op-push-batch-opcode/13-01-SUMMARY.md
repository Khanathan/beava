---
phase: 13-sdk-batch-push-api-op-push-batch-opcode
plan: 01
subsystem: server
tags: [push-batch, opcode, protocol, decode, dispatch, micro-bench]
dependency-graph:
  requires:
    - "12-02 handle_push_batch"
    - "12-02 ConnAccumulator"
    - "12-02 PendingAsync"
  provides:
    - "protocol.OP_PUSH_BATCH (0x0A)"
    - "protocol.Command::PushBatch variant"
    - "tcp.ConnAccumulator.advance_seq"
    - "tcp.handle_connection PushBatch dispatch"
  affects:
    - "Phase 13 Plan 02 (Python SDK push_many will send OP_PUSH_BATCH frames)"
    - "Phase 14 (batch dispatch shape is the shard boundary unit)"
tech-stack:
  added: []
  patterns:
    - "per-event decode via decode_event_binary reuse (zero new serialization)"
    - "advance_seq for seq reservation without buffer insertion"
    - "[batch:{id} event:{idx}] error string prefix for attribution"
key-files:
  created:
    - tests/test_push_batch.rs
  modified:
    - src/server/protocol.rs
    - src/server/tcp.rs
decisions:
  - "Command::PushBatch stores Vec<(serde_json::Value, Vec<u8>)> not Vec<PendingAsync> -- parser has no connection context for seq assignment (Research Open Question 1)"
  - "PushBatch dispatch added to both outer sync path and tight inner loop inline handler for completeness"
  - "advance_seq and dispatch arms added in Task 1 commit because the exhaustive match on Command required handling the new variant for compilation"
metrics:
  duration: ~7min
  tasks_completed: 2
  completed_date: 2026-04-12
requirements: [PERF-04]
---

# Phase 13 Plan 01: OP_PUSH_BATCH Server-Side Decode + Dispatch Summary

Server decodes OP_PUSH_BATCH (0x0A) frames with hard cap 16,384, dispatches to Phase 12's handle_push_batch with consecutive seq assignment via advance_seq, and attributes errors with [batch:{id} event:{idx}] prefix. Decode micro-bench confirms < 2us per event.

## One-liner

OP_PUSH_BATCH (0x0A) wire decode into per-event (Value, raw) pairs, dispatched to handle_push_batch via advance_seq seq reservation, hard cap 16,384 pre-allocation guard, [batch:id event:idx] error attribution, and 10-test suite including decode micro-bench at < 2us/event.

## What Shipped

### Task 1 -- OP_PUSH_BATCH constant + Command variant + parse_command + dispatch (commit `cbb4c5c`)

**`OP_PUSH_BATCH = 0x0A`** constant in protocol.rs (after OP_FLUSH = 0x08).

**`Command::PushBatch`** variant carrying `stream_name: String`, `batch_id: u32`, `events: Vec<(serde_json::Value, Vec<u8>)>`. The events are decoded into (payload, raw_payload) pairs -- NOT PendingAsync -- because parse_command has no connection context for seq assignment.

**parse_command arm** for OP_PUSH_BATCH:
1. Read stream_name via existing `read_string`
2. Read 8 bytes: batch_id (u32 BE) + count (u32 BE)
3. Hard cap: `count > 16_384` returns `Err("batch too large")` BEFORE any allocation (T-13-01, H-7)
4. Pre-allocate `Vec::with_capacity(count.min(16_384))` (D-05 defensive)
5. Per-event: read event_len (u32 BE), validate remaining buf, slice event_bytes, clone to raw_payload, decode via `decode_event_binary` (zero new serialization), push (payload, raw_payload)

**`ConnAccumulator.advance_seq(n: u64) -> u64`** -- reserves n consecutive seq numbers from the per-connection counter and returns the base. The accumulator's internal `next_seq` advances to `base + n` so subsequent `push()` calls continue from there (D-10, Pitfall 5).

**handle_connection dispatch** -- PushBatch arm added in two locations:
1. **Outer sync dispatch path**: force-flushes accumulator (H-2), calls advance_seq, converts events to Vec<PendingAsync>, dispatches to handle_push_batch, collects errors with `[batch:{id} event:{idx}]` prefix (D-09), then `continue` (fire-and-forget).
2. **Tight inner loop inline handler**: same logic for PushBatch frames that arrive while draining BufReader during sustained async load.
3. **handle_sync_command unreachable arm**: PushBatch added alongside PushAsync/Flush.

### Task 2 -- Tests + decode micro-bench (commit `d9e8a8f`)

**tests/test_push_batch.rs** -- 540 lines, 10 tests:

**Unit tests (4):**
- `advance_seq_reserves_and_advances` -- verify seq reservation, peek, and push continuation
- `decode_roundtrip_single_event` -- 1-event batch encode/decode correctness
- `decode_roundtrip_multi_event` -- 3-event batch with different payloads, order preserved
- `oversized_batch_reject` -- count=16,385 returns "batch too large" (D-07)

**Security tests (1):**
- `giant_count_clean_reject` -- count=0xFFFFFFFF (4.2B), clean reject, no OOM (D-08, T-13-01)

**E2E tests (4):**
- `e2e_batch_dispatch_count_correct` -- 5-event batch, GET shows count=5
- `partial_failure_preserves_good_events` -- 3 events, 1 missing key_field, good events apply, drain error has [batch:99] prefix (D-09)
- `backward_compat_push_async_still_works` -- 3 OP_PUSH_ASYNC frames, count=3 (D-14)
- `batch_then_async_seq_continuity` -- batch(3) then async(2), seq counter monotonic across both

**Micro-bench (1):**
- `decode_microbench_1000_events` -- 1000 events x 100 iterations, asserts < 2us per event (H-6/D-18)

## Test Coverage

| Suite | Tests | Status |
|-------|-------|--------|
| lib | 505 | pass |
| test_batch_primitives | 17 | pass |
| test_debug_ui | 25 | pass |
| test_incremental_snapshot | 6 | pass |
| test_pipeline | 23 | pass |
| test_push_batch | **10** | **pass** |
| test_push_coalescing | 19 | pass |
| test_server | 31 | pass |
| test_snapshot | 7 | pass |
| **Grand total** | **643** | **all green** |

## Deviations from Plan

### [Rule 3 - Blocking] advance_seq and dispatch added in Task 1

- **Found during:** Task 1
- **Issue:** Adding `Command::PushBatch` variant to the enum triggered exhaustive match failures in handle_connection's sync dispatch path, tight inner loop inline handler, and handle_sync_command. The build could not succeed without handling the new variant in all match arms.
- **Fix:** Added `advance_seq`, PushBatch dispatch arms, and the unreachable arm in Task 1 instead of Task 2. This is the same code the plan specified for Task 2 Part A and Part B -- just shipped earlier due to the compilation dependency.
- **Files modified:** src/server/tcp.rs
- **Commit:** cbb4c5c

No other deviations. Plan executed as written.

## Threat Surface Scan

No new threat flags. All security mitigations from the plan's threat model are implemented:
- T-13-01: Hard cap 16,384 rejects BEFORE allocation (line 331 protocol.rs)
- T-13-02: Per-event length validation against remaining buf before slice (line 339/343 protocol.rs)
- T-13-03: Each event decoded independently via decode_event_binary (accepted risk)
- T-13-04: read_exact atomicity preserves no-partial-batch invariant (accepted risk)

## Self-Check: PASSED

- `src/server/protocol.rs` -- OP_PUSH_BATCH=0x0A, Command::PushBatch, parse_command arm with 16,384 cap: FOUND
- `src/server/tcp.rs` -- advance_seq method, PushBatch dispatch arms (2), [batch: error format: FOUND
- `tests/test_push_batch.rs` -- 540 lines, 10 tests: FOUND
- Commit `cbb4c5c`: FOUND (git log confirms)
- Commit `d9e8a8f`: FOUND (git log confirms)
- Full regression suite: 643 tests green
- Cargo.toml unchanged: zero new crates
