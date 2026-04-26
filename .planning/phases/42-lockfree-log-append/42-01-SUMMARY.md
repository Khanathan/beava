---
phase: 42-lockfree-log-append
plan: 01
subsystem: state/event_log + server/tcp
tags: [perf, lock-free, O_APPEND, linux, atomic-write]
dependency_graph:
  requires:
    - Phase 40 (per-stream log files)
    - Phase 41 (hot-path atomics)
  provides:
    - lock-free single-stream concurrent append
    - kernel-atomic batch writes (append_many -> one write() syscall)
  affects:
    - src/state/event_log.rs
    - src/server/tcp.rs
    - Cargo.toml (libc dep)
tech-stack:
  added: [libc (0.2)]
  patterns: [O_APPEND atomic append, OwnedFd, partial-write fallback mutex]
key-files:
  created: []
  modified:
    - src/state/event_log.rs
    - src/server/tcp.rs
    - Cargo.toml
    - Cargo.lock
decisions:
  - Replace per-stream Mutex<BufWriter<File>> with LockFreeStreamLog{fd:OwnedFd, partial_write_lock:PLMutex<()>}.
  - Hot path is libc::write() directly — no BufWriter, no userspace lock, kernel i_mutex serializes at inode level.
  - append_many concatenates all frames into a single write() (batch-atomic).
  - TCP hot path switched from per-event append to per-batch append_many so kernel syscall count matches the pre-42 BufWriter-coalesced behavior.
metrics:
  completed: 2026-04-14
  duration: ~1h
---

# Phase 42 Plan 01: Lock-free event-log append via O_APPEND Summary

**One-liner:** Replaced the per-stream `Mutex<BufWriter<File>>` on the event-log append path with `O_APPEND` + direct `libc::write()` syscalls, relying on Linux's kernel-level atomic-append guarantee; also switched the TCP batch-push path to use `append_many` so each batch is one kernel syscall.

## Commits

| Hash    | Message                                                            |
| ------- | ------------------------------------------------------------------ |
| 682b15a | feat(42-01): lock-free event-log append via O_APPEND atomic write  |
| 52e64db | perf(42-01): switch TCP deferred event-log path to append_many     |

## Files Changed

- `src/state/event_log.rs` — new `LockFreeStreamLog` type; `EventLog` uses `DashMap<String, LockFreeStreamLog>`; `append` builds one contiguous `[u32 BE len][postcard]` frame and issues one `libc::write()`; `append_many` concatenates all frames into one buffer and issues one `libc::write()`; `fsync_all` calls `libc::fdatasync(fd)` per stream; partial-write fallback takes a per-stream cold-path mutex.
- `src/server/tcp.rs` — single-stream and multi-stream batch hot paths both switched from a loop of `append()` to a single `append_many()` per group.
- `Cargo.toml` — added `libc = "0.2"`.
- `Cargo.lock` — regenerated.

## Tests

- **New:** `state::event_log::tests::parallel_appends_do_not_tear_frames` — mandatory plan test. 8 `std::thread::spawn`, 10_000 frames each, `Barrier` start, decode file: asserts exactly 80_000 valid postcard frames with correct payload length. **PASS.**
- **New:** `state::event_log::tests::parallel_append_many_preserves_batches` — 8 threads × 1_000 batches × 10 events each via `append_many`; verifies 80_000 correctly-decoded frames with correct payload length. **PASS.**
- **Existing:** `parallel_writes_to_different_streams_do_not_serialize` — still passes (different streams scale).
- **Existing `event_log` unit tests:** all 27 adapted for no-op `fsync_all` coupling (writes are visible to readers immediately under O_APPEND). 29/29 PASS.
- **Full `cargo test --lib`:** 784/784 PASS.
- **Integration `cargo test --test '*'`:** all green across all integration test binaries.
- **`scripts/check-feature-builds.sh`:** green on default / client / demo.
- **`pytest tests/integration/`:** 17 passed, 1 pre-existing unrelated env error (`test_fork_demo.py`).
- **`pytest python/tests/`:** 477/477 PASS.

## Benchmarks

All benchmarks run on the same Hetzner machine used for Phase 41, server pinned to CPUs 0–7 with `taskset`, TALLY_WORKER_THREADS=8, release build.

| Scenario                                       | Pre-Phase-42 baseline | Phase 42 result | Δ       |
| ---------------------------------------------- | --------------------: | --------------: | ------- |
| 1-proc batched `push_many(1000)`               |             553k eps  |       584k eps  | +5.6%   |
| 8-proc 1-stream batched `push_many(1000)`      |             540k eps  |       544k eps  | ~flat   |
| 8-proc 8-stream batched `push_many(1000)`      |             676k eps* |       542k eps  | regress* |

*The pre-42 676k figure was from a run with single-event (Python-capped) pushes; no pre-42 number exists for 8-proc/8-stream in batched mode with this stream setup. The number reported here is a fresh measurement; the multi-stream bench scenario uses 8 distinct streams (S0..S7) registered dynamically with the bench harness.

### Interpretation

**The lock removal is correct but the 8-proc 1-stream bench does not break through the 540k ceiling.**

This matches the plan's stop-and-report trigger: *"8-proc 1-stream bench lands between 540k and 1.5M — partial win, profile to find new bottleneck (probably cascade or DashMap contention on hot keys)."*

The previous hypothesis — that the per-stream writer `Mutex<BufWriter<File>>` was the 540k ceiling — is **refuted** by this measurement. With the writer mutex gone (verified: no userspace lock on the append path; 8-thread concurrent-append unit test proves lock-free correctness), aggregate throughput is essentially unchanged.

The bottleneck is therefore upstream of `event_log.append_many`. Candidates, in rough likelihood order, to investigate in a follow-up phase:

1. **Operator cascade on hot keys.** 8 procs × 1 stream × user_id `u_0..u_999` hashes → heavy DashMap contention in operator state for the same ~1000 keys across 8 workers.
2. **TCP accept / connection demux.** Single server-side reader per connection; if 8 connections funnel into one tokio task queue, that's the choke.
3. **Global `state.lock()` in `handle_push_batch`.** Comment at line 1207 of tcp.rs says "takes ONE state.lock() and groups events by primary stream name" — this is an obvious single-stream serialization point.
4. **serde_json parse on the hot path.** Each event is parsed to `serde_json::Value` before dispatch.

None of these were in scope for Phase 42. Strace was attempted but `ptrace` is not permitted for non-sudo in this environment, so syscall-count confirmation is deferred.

The 1-proc result (553k → 584k, +5.6%) is a modest but real win: one fewer write() syscall per batch (BufWriter's `write_all` previously split boundaries on buffer fills) and no Mutex acquire/release per append.

## Deviations from Plan

### Rule 3 — auto-fix blocking issue

**1. TCP caller sites still used per-event `append` in a loop**

- **Found during:** Task T3 (benchmark run 1).
- **Issue:** With BufWriter gone, each `append` call = one `write()` syscall. The batch path in `tcp.rs` was calling `append` once per event inside the batch loop, resulting in 1000× more syscalls per batch than the pre-42 BufWriter path (which coalesced into ~8KB flushes). Initial 1-proc bench regressed to 446k eps and 8-proc dropped to 524k.
- **Fix:** Switched both call sites in `tcp.rs` (single-stream fast path around line 1454 and multi-stream grouped path around line 1528) to a single `append_many()` per group — one buffer, one syscall.
- **Files modified:** `src/server/tcp.rs`.
- **Commit:** 52e64db.

The plan explicitly noted this was expected to be zero-change on callers *"hopefully zero changes beyond the DashMap value type"*, with a caveat that `append_many` existed but wasn't widely used. Upon profiling we confirmed the callers needed updating for the lock-free path to not regress.

### Not a deviation: benchmark outcome below target

Plan targeted >1.5M eps on 8-proc 1-stream; achieved 544k. Per the plan's own stop-and-report trigger for the 540k–1.5M band, this is a **partial win**: the lock-free swap is structurally correct and removes a real contention point, but the workload's *current* ceiling is elsewhere. Not rolled back. Profiling the next bottleneck belongs in a follow-up phase (Phase 43 candidate: operator cascade / state.lock() in handle_push_batch).

## Known Stubs

None.

## Threat Flags

None — append path already existed; new code swaps the in-memory locking discipline without introducing new trust boundaries, network surface, or schema changes.

## Self-Check: PASSED

- File `src/state/event_log.rs` modified: FOUND.
- File `src/server/tcp.rs` modified: FOUND.
- Commit `682b15a` exists: FOUND.
- Commit `52e64db` exists: FOUND.
- All library tests green (`cargo test --lib`: 784/784).
- Mandatory unit test `parallel_appends_do_not_tear_frames`: PASSED (80_000/80_000 frames decoded correctly).
- Feature-build matrix green.
- Pytest suites green (17 integration incl. unrelated pre-existing fork_demo env error; 477 python).
