---
phase: 18-redis-hand-roll
plan: 18-02
subsystem: runtime-core-wal
tags: [wal, lock-free, ring-buffer, fsync, watermark, durability]
dependency_graph:
  requires: [18-01]
  provides: [WalLsn, WalBufferRing, WalWriter, WalGlue]
  affects: [beava-runtime-core, beava-server]
tech_stack:
  added: [libc (statfs/fdatasync), beava-runtime-core wal modules]
  patterns:
    - 3-buffer state machine (ACTIVE/SEALED/FLUSHING/FREE)
    - Four-watermark LSN discipline (committed/written/synced/acked)
    - Condvar-based PerEvent waiter (wait_for_synced)
    - O_APPEND WAL file with network-FS guard (statfs)
    - Dedicated writer+fsync std::thread (beava-wal-writer)
key_files:
  created:
    - crates/beava-runtime-core/src/wal_lsn.rs
    - crates/beava-runtime-core/src/wal_buffer.rs
    - crates/beava-runtime-core/src/wal_writer.rs
    - crates/beava-server/tests/phase18_02_durability_watermarks_test.rs
    - crates/beava-server/tests/phase18_02_pingpong_test.rs
    - crates/beava-server/tests/phase18_02_inline_wal_test.rs
  modified:
    - crates/beava-runtime-core/src/lib.rs
    - crates/beava-runtime-core/Cargo.toml
    - crates/beava-server/src/runtime_core_glue.rs
    - crates/beava-server/Cargo.toml
decisions:
  - "WalGlue added to runtime_core_glue.rs (not a separate file) — keeps WAL bridge co-located with WireRequest dispatch"
  - "sealed_queue uses Mutex<VecDeque> (not crossbeam SegQueue) — avoids new workspace dep; lock taken only on seal/pop, never on per-event append hot path"
  - "seal_active enqueues even when no free buffer available — apply thread detects state != ACTIVE and waits on free_condvar"
  - "Tasks 2.3 and 2.4 share a single RED+GREEN pair — both required WalWriter::new before tests could compile, so combined into one test file"
metrics:
  duration: "~28 minutes"
  completed: "2026-04-25T15:52:30Z"
  tasks_completed: 4
  tasks_total: 4
  commits: 7
---

# Phase 18 Plan 02: Lock-free WAL with 3-buffer state machine — Summary

**One-liner:** Lock-free 3-buffer WAL ring (WalBuffer/WalBufferRing) + four-watermark LSN discipline (WalLsn) + dedicated writer/fsync thread (WalWriter) + WalGlue bridging periodic and per-event append modes — all behind the hand-rolled runtime path, zero Mutex on the per-event append hot path.

## Status: COMPLETE

All 4 tasks executed with red-green TDD. 28/28 new tests pass. Phase 6.1 durability tests (7 tests) still pass.

## Tasks

### Task 2.1 — WalLsn watermark struct

- RED: `54ff7d9` — 8 tests for four-watermark atomics + Condvar waiter wakeup
- GREEN: `22fed2d` — `wal_lsn.rs`: `committed/written/synced` AtomicU64s, `record()` apply API, `mark_written/mark_synced` writer API, `wait_for_synced` Condvar-based PerEvent blocking wait with timeout → `WaitTimeout` error

### Task 2.2 — WalBufferRing 3-buffer state machine

- RED: `cb04f67` — 13 tests: ring initialization, lock-free append, state transitions, sealed-queue handoff, buffer-full auto-seal, backpressure block
- GREEN: `bcf3eba` — `wal_buffer.rs`: `WalBuffer` (pos/lsn_lo/lsn_hi/state atomics + try_append memcpy), `WalBufferRing` (N-buffer ring, append hot path, seal_active, pop_sealed, return_to_free, free_condvar backpressure)

  **Deviation (Rule 1 — Bug):** Initial `seal_active()` returned `None` without enqueueing the buffer when no free buffer was available. `append()` then called `try_append` on the now-SEALED buffer (writing into wrong state). Fixed: `seal_active()` always enqueues to sealed_queue; `append()` checks `state != ACTIVE` before `try_append` and calls `wait_for_free_and_activate` on the backpressure path. This is the correct behavior per the architecture spec.

  **Deviation (Rule 1 — Bug):** Initial backpressure test used overflow-triggered seals to exhaust buffers, but the overflow appends themselves blocked (since no free buffers). Rewrote test to use explicit `seal_active()` calls to exhaust buffers cleanly before spawning the blocking appender thread.

### Task 2.3 + 2.4 — WalWriter thread + WalGlue integration (combined RED)

- RED: `249786c` — 7 tests covering WalWriter::new, spawn, written/synced LSN advance within tick window, WAL file creation, network-FS local-FS guard, WalGlue periodic/per-event dispatch, push-sync timeout
- GREEN: `07822ff` — `wal_writer.rs`: `WalWriter::new(dir, ring, lsn, tick_ms)` with O_APPEND file open, `is_network_fs` guard (statfs on macOS/BSD/Linux), `spawn()` into `std::thread "beava-wal-writer"`, worker loop (sleep → seal_active → drain sealed queue → write+mark_written+fdatasync+mark_synced+return_to_free); `WalGlue` struct in `runtime_core_glue.rs` with `wal_append_periodic` and `wal_append_per_event`

## Verification Gate Status

| Gate | Status | Notes |
|------|--------|-------|
| WAL append lock-free on apply thread | PASS | No Mutex on try_append hot path; Mutex only on seal/pop (slow path) |
| 3-buffer state machine correct | PASS | 13 pingpong tests including backpressure |
| Four watermarks tracked correctly | PASS | 8 watermark tests including multi-waiter |
| /push-sync waiters wake at correct LSN | PASS | wait_for_synced Condvar test |
| O_APPEND used | PASS | OpenOptions::new().create(true).append(true) |
| Network-FS guard at startup | PASS | is_network_fs via statfs; local tmpdir accepted |
| Periodic mode: no await on apply | PASS | wal_append_periodic returns at committed_lsn |
| PerEvent mode: only requesting connection blocks | PASS | wait_for_synced blocks caller; apply thread free |
| Phase 6.1 durability tests still pass | PASS | phase6_1_push_sync (5) + phase6_1_crash (2) = 7 pass |
| beava-persistence tests still pass | PASS | 7 pass |
| Clippy clean (-D warnings) | PASS | after removing .write(true) redundancy |
| Perf gate 2.1 (300-500k EPS/core M4) | MANUAL REQUIRED | Run cargo bench -p beava-bench --features hand-rolled-runtime |
| Perf gate 2.2 (WAL CPU ≤5% via samply) | MANUAL REQUIRED | Run samply profile per 18-01-perf-profile.md |

## All Commits (chronological)

| Hash | Subject |
|------|---------|
| 54ff7d9 | test(18-02): RED — WalLsn durability-watermarks test (Task 2.1) |
| 22fed2d | feat(18-02): GREEN — WalLsn four-watermark struct (Task 2.1) |
| cb04f67 | test(18-02): RED — WalBufferRing 3-buffer state machine tests (Task 2.2) |
| bcf3eba | feat(18-02): GREEN — WalBufferRing 3-buffer state machine (Task 2.2) |
| 249786c | test(18-02): RED — WalWriter + WAL glue integration tests (Tasks 2.3+2.4) |
| 07822ff | feat(18-02): GREEN — WalWriter thread + WalGlue integration (Tasks 2.3+2.4) |

## Known Stubs

| Stub | File | Reason |
|------|------|--------|
| wal_append_periodic / wal_append_per_event set registry_version=0 | runtime_core_glue.rs | WalGlue is not yet wired to AppState; registry_version comes from AppState which is wired in Plan 18-03/18-04 when the full dispatch path is connected |
| WalWriter uses single WAL segment "wal-0000000000000000.wal" | wal_writer.rs | Segment rotation (BEAVA_WAL_SEGMENT_BYTES threshold) is deferred to Plan 18-05; v0 single-file WAL sufficient for test coverage |
| WAL_BROKEN atomic flag not implemented | wal_writer.rs | Plan 18-05 adds the WAL_BROKEN flag for EIO/disk-full error signalling; currently writer logs error and continues |
| BEAVA_WAL_BUF_BYTES / BEAVA_WAL_FSYNC_TICK_MS env tunables not wired | wal_writer.rs | Constructor takes explicit tick_ms arg; env-var reading wired in Plan 18-03 server startup |

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 — Bug] seal_active skipped enqueue when no free buffer available**
- Found during: Task 2.2 GREEN (append_blocks_on_no_free_buffers test hung >60s)
- Issue: `seal_active()` returned `None` without enqueueing the sealed buffer when all other buffers were already sealed. `active_idx` continued pointing at the now-SEALED buffer. `append()` then called `try_append` on a sealed buffer (corrupt data, or inf-loop).
- Fix: `seal_active()` now always seals+enqueues regardless of free buffer availability. `append()` checks `buf.state() != BUF_STATE_ACTIVE` before calling `try_append` and redirects to `wait_for_free_and_activate` on the backpressure path.
- Files: `crates/beava-runtime-core/src/wal_buffer.rs`
- Commit: bcf3eba

**2. [Rule 1 — Bug] Backpressure test hung because overflow appends blocked before test setup complete**
- Found during: Task 2.2 GREEN (append_blocks_on_no_free_buffers ran >60s)
- Issue: Test used cascading overflow-triggered seals to exhaust all 3 buffers. The 4th overflow append itself blocked (waiting for a free buffer that would never arrive in that setup). The unblock thread was never spawned.
- Fix: Rewrote test to use explicit `seal_active()` calls to exhaust buffers, then spawn the unblock thread before the blocking `append()`.
- Files: `crates/beava-server/tests/phase18_02_pingpong_test.rs`
- Commit: bcf3eba

**3. [Rule 1 — Bug] `.write(true)` redundant with `.append(true)` (clippy -D warnings)**
- Found during: Task 2.3 GREEN (clippy pass)
- Issue: `OpenOptions::new().write(true).create(true).append(true)` triggers `clippy::suspicious_open_options` — `.write(true)` is implicit in `.append(true)` and emitting both is a common mistake.
- Fix: Removed `.write(true)`.
- Files: `crates/beava-runtime-core/src/wal_writer.rs`
- Commit: 07822ff

## Known Pre-existing Issues (not caused by this plan)

**tests/phase9_smoke.rs compile errors:** Pre-existing failures in `phase9_smoke.rs` that exist on v2/greenfield HEAD before Plan 18-02. Not touched or worsened by this plan. Run `cargo test -p beava-server --test phase9_smoke --features testing` to reproduce the pre-existing errors in isolation.

## Threat Flags

None — Plan 18-02 adds no new network surface. The WAL path is a purely local disk write. The writer thread has no network access. The `is_network_fs` guard reduces attack surface by refusing remote filesystems.

## Self-Check: PASSED

Files verified present:
- crates/beava-runtime-core/src/wal_lsn.rs: FOUND
- crates/beava-runtime-core/src/wal_buffer.rs: FOUND
- crates/beava-runtime-core/src/wal_writer.rs: FOUND
- crates/beava-server/tests/phase18_02_durability_watermarks_test.rs: FOUND
- crates/beava-server/tests/phase18_02_pingpong_test.rs: FOUND
- crates/beava-server/tests/phase18_02_inline_wal_test.rs: FOUND

Commits verified present (all 6 on v2/greenfield):
- 54ff7d9, 22fed2d, cb04f67, bcf3eba, 249786c, 07822ff: all present
