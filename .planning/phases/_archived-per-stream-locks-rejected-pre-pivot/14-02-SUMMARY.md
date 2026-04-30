---
phase: 14
plan: 02
subsystem: state, server, tests
tags: [concurrency, integration-tests, multi-thread, fan-out, dashmap]
dependency-graph:
  requires:
    - "14-01 ConcurrentAppState"
    - "14-01 make_concurrent_state"
    - "14-01 per-field locking"
  provides:
    - "tests.test_concurrent (5 multi-thread integration tests)"
    - "tokio rt-multi-thread feature"
  affects:
    - "Phase 14 Plan 03 (benchmarking)"
tech-stack:
  added:
    - "tokio rt-multi-thread (for multi_thread test runtime)"
  patterns:
    - "Multi-client TCP integration test pattern with tokio::test(flavor = multi_thread, worker_threads = 4)"
    - "Fire-and-forget PUSH_ASYNC + FLUSH for high-throughput concurrent test workloads"
key-files:
  created:
    - tests/test_concurrent.rs
  modified:
    - Cargo.toml
decisions:
  - "Added rt-multi-thread to tokio features for multi-threaded test runtime; production server remains current_thread flavor"
  - "Task 1 (snapshot/eviction/event_log/HTTP adaptation) was already completed by Plan 01 Task 2 due to the PLMutex<StateStore> deviation -- no additional code changes needed"
metrics:
  duration: ~7min
  tasks_completed: 2
  completed_date: 2026-04-12
requirements: [PERF-05]
---

# Phase 14 Plan 02: Background Systems + Concurrency Tests Summary

Added 5 multi-threaded concurrency integration tests proving ConcurrentAppState correctness under parallel multi-client access. Background systems (snapshot, eviction, event log, HTTP) already worked correctly with per-field locking from Plan 01.

## One-liner

5 multi-thread integration tests (multi-stream parallel push, same-stream different-keys, concurrent push+get, fan-out under concurrency, mixed SET+PUSH) all green under 4-worker tokio runtime; 648 total tests passing.

## What Shipped

### Task 1 -- Background systems concurrent adaptation (no-op)

Plan 01 kept `PLMutex<StateStore>` instead of migrating to per-stream `DashMap<String, StreamStore>`. This means the snapshot serialization, eviction, event log fsync/compaction, and HTTP handlers were already migrated to use individual per-field locks (`state.engine.read()`, `state.store.lock()`, `state.event_log.lock()`, etc.) as part of Plan 01 Task 2. No additional code changes were needed.

Verification:
- `grep "state.lock()" src/main.rs` -- 0 matches (only comments in tcp.rs)
- `grep "state.lock()" src/server/http.rs` -- 0 matches
- Snapshot timer uses `engine.read()` + `store.lock()` + individual snapshot field locks
- Eviction timer uses `engine.read()` + `store.lock()`
- Fsync timer uses `event_log.lock()`
- Compaction timer uses `event_log.lock()` per stream with yield
- HTTP trigger_snapshot uses `engine.read()` + `store.lock()` + individual locks

### Task 2 -- Concurrency integration tests (commit `e186e01`)

Created `tests/test_concurrent.rs` with 5 tests under `tokio::test(flavor = "multi_thread", worker_threads = 4)`:

**Test 1: multi_stream_parallel_push** -- 4 TCP clients push 500 events each to 2 streams (Transactions, Logins) with different entity keys. Verifies each key's count is exactly 500. Proves: different streams + different keys = no data corruption.

**Test 2: same_stream_different_keys_concurrent** -- 4 clients push 500 events each to the same stream (Payments) for different keys. Verifies count=500 and sum=500.0 per key. Proves: entity-level concurrency within one stream works.

**Test 3: concurrent_push_and_get** -- 2 push tasks (500 events each) and 2 GET tasks (50 reads each) operate on the same entity key concurrently. All GETs return valid non-negative counts. Final count = 1000. Proves: concurrent read+write is safe.

**Test 4: fan_out_under_concurrency** -- 2 clients push 300 events each to TxFanOut (keyed on user_id) which fans out to MerchFanOut (keyed on merchant_id). Verifies both streams have correct counts for each key. Proves: cross-stream fan-out works under concurrent access.

**Test 5: set_mset_concurrent_with_push** -- One task pushes 500 live events, another writes static features via SET/MSET concurrently. GET returns both live features (count=500) and static features (lifetime_value, segment). Proves: static_store and stream_stores don't interfere.

**Cargo.toml**: Added `rt-multi-thread` to tokio features for multi-threaded test runtime.

## Test Coverage

| Suite | Count | Status |
|-------|-------|--------|
| lib | 505 | PASS |
| test_batch_primitives | 17 | PASS |
| test_push_coalescing | 19 | PASS |
| test_push_batch | 10 | PASS |
| test_server | 31 | PASS |
| test_pipeline | 23 | PASS |
| test_debug_ui | 25 | PASS |
| test_snapshot | 7 | PASS |
| test_incremental_snapshot | 6 | PASS |
| **test_concurrent** | **5** | **PASS (NEW)** |
| **Total** | **648** | **ALL PASS** |

## Deviations from Plan

### [Rule 3 - Blocking] Task 1 was already completed by Plan 01

- **Found during:** Task 1 verification.
- **Issue:** Plan 14-02 was written assuming Plan 01 would produce per-stream `DashMap<String, StreamStore>` at the top level, requiring snapshot/eviction/event_log/HTTP to be adapted for DashMap iteration. However, Plan 01's deviation kept `PLMutex<StateStore>` as the primary storage, meaning the background systems were already migrated to per-field locks during Plan 01 Task 2.
- **Fix:** Verified all acceptance criteria are met (zero `state.lock()` calls, all tests pass). No code changes needed for Task 1.
- **Files modified:** None (verification only)

### [Rule 3 - Blocking] DashMap retain pattern not applicable

- **Found during:** Task 1 acceptance criteria check.
- **Issue:** Plan acceptance criterion `grep "retain" src/state/eviction.rs returns matches` assumed per-stream DashMap eviction. Since eviction still operates through `PLMutex<StateStore>` (Plan 01 deviation), the DashMap retain pattern is not used.
- **Fix:** Eviction remains correct through the existing two-phase collect-then-apply pattern. No change needed.

## Verification

| Criterion | Result |
|-----------|--------|
| `cargo test --lib` | 505 passed |
| `cargo test --test test_concurrent` | 5 passed |
| All integration test suites | 648 total, 0 failures |
| `grep "multi_thread" tests/test_concurrent.rs` | 5 matches |
| `grep "fan_out_under_concurrency" tests/test_concurrent.rs` | 1 match |
| `grep "concurrent_push_and_get" tests/test_concurrent.rs` | 1 match |
| `grep "state.lock()" src/main.rs src/server/http.rs` (non-comment) | 0 matches |

## Self-Check: PASSED

- `tests/test_concurrent.rs` exists: FOUND
- `Cargo.toml` exists: FOUND
- Commit e186e01 reachable from HEAD: FOUND
- `multi_thread` in test_concurrent.rs: 6 matches (5 tests + 1 comment)
- `fan_out_under_concurrency` in test_concurrent.rs: FOUND
- `concurrent_push_and_get` in test_concurrent.rs: FOUND
- All 648 tests pass: VERIFIED
