---
phase: 18-redis-hand-roll
plan: 18-04
subsystem: runtime-core-io-write
tags: [io-threads, write-phase, WireResponse, serialize_into, BytesMut, partial-write, off-apply-serialization]
dependency_graph:
  requires: [18-03]
  provides: [WireResponse, serialize_into, Client::output_queue, Client::write_buf, Client::write_offset, Client::enqueue_response, Client::has_write_work, Client::reset_write_buf]
  affects: [beava-runtime-core]
tech_stack:
  added: []
  patterns:
    - WireResponse enum (TcpAck/TcpError/HttpStatus/HttpStaticOk) queued by apply thread without serialization
    - serialize_into() called exclusively by I/O worker threads (off-apply)
    - BytesMut BufMut put_u32/put_u16/put_u8/put_u64 for zero-alloc TcpAck encoding (15 bytes)
    - Per-client output_queue: VecDeque<WireResponse> + write_buf: BytesMut + write_offset: usize
    - Partial-write resume: write_offset tracks bytes flushed; reset_write_buf() clears after full drain
    - Same IoPool reused for write phase work items (no second pool)
key_files:
  created:
    - crates/beava-runtime-core/tests/io_threads_write_test.rs
  modified:
    - crates/beava-runtime-core/src/response.rs
    - crates/beava-runtime-core/src/client.rs
    - crates/beava-runtime-core/src/lib.rs
decisions:
  - "OP_ACK=0x0080 chosen as response-only opcode for TcpAck; avoids collision with existing request opcodes (OP_PUSH=0x0010, OP_PUSH_SYNC=0x0011)"
  - "TcpAck wire format: [u32 len=11][u16 OP_ACK=0x0080][u8 CT_JSON=0x01][u64 lsn BE] = 15 bytes, zero allocation"
  - "output_queue: VecDeque<WireResponse> (not Mutex<VecDeque>) — per-client ownership, exclusive apply-then-I/O handoff via IoPool::join_all() Acquire barrier"
  - "serialize_into is a free function (not Client method) — signals that it must never be called on apply thread; test explicitly verifies apply_serialize_calls counter stays 0"
  - "pending_responses: VecDeque<Bytes> retained for Plan 18-01/02 backward compatibility; new code uses output_queue"
  - "Perf bench matrix deferred to Plan 18-04.5 (bench infra plan); M4 targets recorded in perf-baselines.md as informational per D-16"
metrics:
  duration: "~25 minutes"
  completed: "2026-04-25T16:18:01Z"
  tasks_completed: 3
  tasks_total: 3
  commits: 2
---

# Phase 18 Plan 04: I/O threads for writes — Summary

**One-liner:** WireResponse enum + serialize_into() off-apply serialization with BytesMut BufMut put_* ops, per-client output_queue/write_buf/write_offset for partial-write resume, and full per-tick read-apply-write lifecycle verified by IoPool dispatch tests.

## Status: COMPLETE

All 3 tasks executed with red-green TDD. 4 active tests pass. Plans 18-01/02/03 tests, Phase 6.1 glue test, and beava-server lib tests all still green.

## Tasks

### Task 4.1 — Off-thread response serialization

- RED: `e6ce2ea` — all three task test stubs (4.1, 4.2, 4.3) in `io_threads_write_test.rs`; compile failed because `WireResponse` + `serialize_into` not defined
- GREEN: `31a58d9` — `WireResponse` enum in `response.rs`; `serialize_into()` free function using `BytesMut::BufMut` (zero allocation for TcpAck); `output_queue`/`write_buf`/`write_offset` + helper methods in `client.rs`; `lib.rs` re-exports

### Task 4.2 — Per-tick lifecycle: read → apply → write

- Tests shipped in same RED/GREEN pair as 4.1 (IoPool was required to compile)
- `test_per_tick_lifecycle_read_apply_write`: 4-thread IoPool, 16 clients, records phase log `["read_dist","read_join","apply","write_dist","write_join"]` — order verified

### Task 4.3 — Partial-write resume + tail-latency stress

- `test_partial_write_resumes_next_tick`: 7 TcpAck responses (105 bytes), mock socket 17 bytes/tick, 7 ticks to drain; FIFO ordering of lsn fields verified
- `test_p99_tail_latency_under_load`: 64 clients × 500 events via 4-thread IoPool; correctness check (all frames received, no drops); elapsed logged but not gated (M4 debug build)

## Verification Gate Status

| Gate | Status | Notes |
|------|--------|-------|
| WireResponse enum defined with all variants | PASS | TcpAck/TcpError/HttpStatus/HttpStaticOk |
| serialize_into encodes TcpAck correctly (15 bytes, correct lsn) | PASS | task_4_1 test verifies frame-by-frame |
| Apply thread does zero serialization | PASS | apply_serialize_calls counter stays at 0 |
| Per-tick phase order: read→apply→write | PASS | task_4_2 phase_log assertion |
| Partial-write resume correct | PASS | task_4_3: 17 bytes/tick, 7 ticks for 105 bytes |
| FIFO ordering preserved across partial writes | PASS | task_4_3: lsn values in order 0..6 |
| 64-client stress: no dropped frames | PASS | task_4_3 p99 test |
| Phase 18-01/02/03 tests still pass | PASS | All 6 io_threads_read + event_loop_smoke green |
| Phase 6.1 glue test still passes | PASS | phase18_01_glue: 1 test green |
| beava-server lib tests still pass | PASS | 118 tests green |
| cargo clippy -D warnings | PASS | workspace clean |
| cargo fmt --all --check | PASS | clean |
| perf-baselines.md updated | PASS | Informational M4 rows appended; actual bench deferred to 18-04.5 |
| throughput-baselines.md updated | PASS | Informational target rows appended; measured numbers deferred to 18-04.5 |

## All Commits (chronological)

| Hash | Subject |
|------|---------|
| e6ce2ea | test(18-redis-hand-roll-18-04): write io thread serializes response off apply |
| 31a58d9 | feat(18-redis-hand-roll-18-04): off-thread response serialization + per-tick write phase |

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 — Bug] Tasks 4.1–4.3 combined into one RED+GREEN pair**
- Found during: Task 4.1.a RED (needed WireResponse to compile 4.2 and 4.3 tests)
- Issue: All three task tests import `WireResponse` + `serialize_into` from response.rs. They cannot compile independently — a standalone 4.1-only RED would still fail to compile the 4.2 and 4.3 modules. All test stubs were written in the single file; RED commit contains all test stubs, GREEN commit provides all implementations.
- This matches the 18-02 and 18-03 precedents.
- Commit: e6ce2ea (RED with all stubs), 31a58d9 (GREEN with all implementations)

**2. [Rule 2 — Missing functionality] WireResponse::TcpError/HttpStatus/HttpStaticOk added beyond TcpAck-only**
- Found during: Task 4.1.b GREEN (plan specified "covers all data-plane responses")
- Issue: Plan 4.1.b says "covers all data-plane responses" — implementing only TcpAck would leave the enum unusable for HTTP and error paths needed by Plans 18-05/18-06.
- Fix: Added TcpError, HttpStatus, HttpStaticOk variants with serialize_into arms.
- Files: crates/beava-runtime-core/src/response.rs
- Commit: 31a58d9

**3. [Rule 2 — Missing functionality] Client::enqueue_response / has_write_work / reset_write_buf helpers added**
- Found during: Task 4.1.b GREEN (plan says "apply enqueues WireResponse into output_queue")
- Issue: Without these helpers, callers would manipulate output_queue/write_buf directly, bypassing invariants (state transition, queue discipline).
- Fix: Added the three helpers to impl Client. No test modification needed — the invariants are documented in comments.
- Files: crates/beava-runtime-core/src/client.rs
- Commit: 31a58d9

## Known Stubs

| Stub | File | Reason |
|------|------|--------|
| EventLoop::tick() not wired to run_write_phase | event_loop.rs | Full EventLoop dispatch wiring (read → apply → write per tick) is the Plan 18-05/18-06 scope when tokio dual-path is removed. The run_write_phase function is exposed as a pub method on IoPool-level (via the tests), but EventLoop::tick() still returns raw events. |
| beava-bench --features hand-rolled-runtime write-phase sweep | beava-bench | Bench infra for the hand-rolled runtime path is Plan 18-04.5 scope. M4 informational numbers are not measured yet. |

## Known Pre-existing Issues (not caused by this plan)

**tests/phase9_smoke.rs compile errors:** Pre-existing failures that exist on v2/greenfield HEAD before Plan 18-04. Not touched or worsened by this plan. Scoped test runs use `--test io_threads_write_test` to avoid triggering them.

## Threat Flags

None — Plan 18-04 adds no new network surface. WireResponse and serialize_into operate entirely in-process on BytesMut buffers owned by the I/O worker. No new file, network, or auth paths introduced.

## Self-Check: PASSED

Files verified present:
- crates/beava-runtime-core/src/response.rs: FOUND
- crates/beava-runtime-core/src/client.rs: FOUND
- crates/beava-runtime-core/src/lib.rs: FOUND
- crates/beava-runtime-core/tests/io_threads_write_test.rs: FOUND
- .planning/phases/18-redis-hand-roll/18-04-SUMMARY.md: FOUND

Commits verified present:
- e6ce2ea: FOUND (RED — test stubs)
- 31a58d9: FOUND (GREEN — WireResponse + serialize_into + client write fields)
