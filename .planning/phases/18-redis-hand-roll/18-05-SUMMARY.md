---
phase: 18-redis-hand-roll
plan: 18-05
subsystem: io-runtime
tags: [mio, io-uring, worker-loop, per-worker, valkey8, epoll, kqueue]

# Dependency graph
requires:
  - phase: 18-13
    provides: "SPSC ring between IoPool workers and apply; established continuous-event-flow pattern replacing join_all spin barrier"
provides:
  - "IoBackend trait (MioBackend + IoUringBackend) — per-worker polling abstraction"
  - "Per-worker continuous-loop model: each worker owns its mio::Poll + Waker + disjoint client subset"
  - "Apply thread is a pure dispatcher — no client socket access, no join_all"
  - "Channel architecture: read_tx(16384) MPSC, write_tx[w](4096), new_client_tx[w](256) with waker.wake() on every send"
  - "IoUringBackend (Linux only, --features io-uring): eventfd waker, TAG_RECV/TAG_SEND/TAG_WAKER user_data scheme"
  - "WorkerHandle: start_worker<B: IoBackend>(), worker_id(), send_new_client(), waker(), stop(), join()"
affects: [18-16, 18-05.5, any plan touching serve_with_dirs or IoPool]

# Tech tracking
tech-stack:
  added:
    - "io-uring = \"=0.6.4\" (Linux only, feature-gated as `io-uring`)"
  patterns:
    - "Per-worker IoBackend ownership: each thread creates its own backend via IoBackend::new()"
    - "WakerHandle stored in WorkerHandle so apply thread can interrupt poll() at any time"
    - "TAG encoding in io_uring user_data: high 8 bits = operation type, low 56 bits = slot_idx"
    - "Zero-len RECV probe for readability notification; actual read via std::io::Read on the fd"
    - "MioBackend: WAKER_TOKEN=0 sentinel, client tokens = Token(slot_idx + 1)"

key-files:
  created:
    - "crates/beava-runtime-core/src/io_backend/mod.rs — IoBackend trait, WakerHandle, IoEvent enum"
    - "crates/beava-runtime-core/src/io_backend/mio_backend.rs — MioBackend impl"
    - "crates/beava-runtime-core/src/io_backend/io_uring.rs — IoUringBackend (Linux + feature flag)"
    - "crates/beava-runtime-core/src/io_thread_worker.rs — WorkerConfig, WorkerHandle, start_worker<B>"
    - "crates/beava-runtime-core/tests/io_backend_trait_test.rs — MioBackend trait conformance + waker test"
    - "crates/beava-runtime-core/tests/io_uring_smoke_test.rs — io_uring smoke (Linux only)"
    - "crates/beava-server/tests/phase18_05_continuous_workers_test.rs — round-robin routing, continuous loop, no write join_all"
  modified:
    - "crates/beava-runtime-core/src/lib.rs — added io_backend and io_thread_worker module declarations"
    - "crates/beava-runtime-core/Cargo.toml — io-uring feature gate, [[test]] entries"
    - "crates/beava-server/Cargo.toml — phase18_05_continuous_workers_test [[test]] entry"

key-decisions:
  - "Submodule named mio_backend (not mio) to avoid shadowing the mio crate in mod.rs namespace"
  - "WakerHandle stored in WorkerHandle: critical for send_new_client() to wake without separate Arc"
  - "io_uring RECV probe uses zero-length buffer: actual read is a std::io::Read syscall after notification (kernel compat >= 5.6)"
  - "Task 5.3 (eliminate write join_all) was already satisfied by Task 5.2b; committed as GREEN-only with timing assertion"
  - "task_5.5 (Linux Xeon HARD GATE >= 3M EPS/core) deferred: no HW available; gate explicit in plan"
  - "Default 5 IO workers (fixed); BEAVA_IO_THREADS env var override preserved"

patterns-established:
  - "IoBackend::new() called once per worker at thread startup; backend is exclusive to that thread (all methods &mut self)"
  - "Channel + Waker: apply always pairs write_tx.send() or new_client_tx.send() with waker.wake() — no polling needed"
  - "Worker loop order: drain new_client_rx -> drain write_rx -> backend.poll(1s) -> process IoEvents -> check stop flag"

requirements-completed: []

# Metrics
duration: 210min
completed: 2026-04-26
---

# Phase 18 Plan 05: Per-worker continuous loops + IoBackend trait (Valkey 8 model) Summary

**IoBackend trait + MioBackend + IoUringBackend implemented; per-worker continuous-loop model replaces per-tick IoPool; apply is now a pure dispatcher with zero client-socket access and no join_all.**

## Performance

- **Duration:** ~210 min
- **Started:** 2026-04-26T11:00:00Z
- **Completed:** 2026-04-26T14:57:00Z
- **Tasks:** 4 of 4 executed (Task 5.5 Linux Xeon deferred — no HW)
- **Files modified:** 10

## Accomplishments

- Defined `IoBackend` trait + `WakerHandle` trait + `IoEvent` enum as the per-worker polling abstraction contract
- Implemented `MioBackend` (macOS kqueue / Linux epoll) with WAKER_TOKEN=0 sentinel and HashMap-keyed client state
- Implemented `IoUringBackend` (Linux only, `--features io-uring`): eventfd waker, zero-len RECV probe, TAG-encoded user_data scheme
- Built `WorkerHandle` / `start_worker<B: IoBackend>()` with fully correct cross-thread wakeup: waker stored in `WorkerHandle` so `send_new_client()` immediately interrupts `backend.poll()`
- 5 tests across 3 test suites all green: `io_backend_trait_test` (2), `io_uring_smoke_test` (Linux-only cfg-gated), `phase18_05_continuous_workers_test` (3)
- Clippy + rustfmt clean across entire workspace

## Task Commits

Each task was committed atomically per TDD red-green discipline:

1. **Task 5.1a RED — IoBackend trait conformance** - `b1d3a4f` (test)
2. **Task 5.1b GREEN — IoBackend trait + MioBackend adapter** - `5dc3bf7` (feat)
3. **Task 5.2a RED — worker-owns-client + continuous loop** - `6e52eb7` (test)
4. **Task 5.2b GREEN — per-worker continuous loops** - `837d4fd` (feat)
5. **Task 5.3 GREEN — write phase fully async, no apply-side wait** - `751ddb4` (feat)
6. **Task 5.4a RED — io_uring backend smoke recv one frame** - `d2933d0` (test)
7. **Task 5.4b GREEN — io_uring backend Linux only** - `217c249` (feat)
8. **Chore — clippy type_complexity + rustfmt** - `4eb3fdf` (chore)

_Task 5.3 committed as GREEN-only: the write async behavior was already delivered by Task 5.2b._
_Task 5.5 (Linux Xeon >= 3M EPS gate) deferred — no HW available._

## Files Created/Modified

- `crates/beava-runtime-core/src/io_backend/mod.rs` — `IoBackend` trait, `WakerHandle`, `IoEvent`, module declarations
- `crates/beava-runtime-core/src/io_backend/mio_backend.rs` — `MioBackend` with `mio::Poll` + `mio::Waker`, WAKER_TOKEN=0 sentinel
- `crates/beava-runtime-core/src/io_backend/io_uring.rs` — `IoUringBackend` (Linux + `io-uring` feature), eventfd waker, TAG_RECV/SEND/WAKER scheme
- `crates/beava-runtime-core/src/io_thread_worker.rs` — `WorkerConfig`, `WorkerHandle`, `start_worker<B: IoBackend>()`, worker main loop
- `crates/beava-runtime-core/src/lib.rs` — added `pub mod io_backend; pub mod io_thread_worker;`
- `crates/beava-runtime-core/Cargo.toml` — `io-uring` feature + Linux-only optional dep, `[[test]]` entries
- `crates/beava-runtime-core/tests/io_backend_trait_test.rs` — `test_iobackend_trait_uniform`, `test_iobackend_waker_cross_thread_wake`
- `crates/beava-runtime-core/tests/io_uring_smoke_test.rs` — Linux+feature-gated smoke test
- `crates/beava-server/Cargo.toml` — `[[test]]` entry for phase18_05
- `crates/beava-server/tests/phase18_05_continuous_workers_test.rs` — `test_worker_owns_client_round_robin`, `test_worker_loop_processes_continuously_no_join_all`, `test_no_write_join_all_apply_doesnt_wait`

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Module name collision: `mio` submodule shadowed the `mio` crate**
- **Found during:** Task 5.1b
- **Issue:** Plan specified `pub mod mio;` but this caused `mio::net::TcpStream` in `mod.rs` to resolve to the submodule, not the external crate
- **Fix:** Renamed submodule to `mio_backend` throughout; documented in module-level comment
- **Files modified:** `src/io_backend/mod.rs`, `src/io_backend/mio_backend.rs`

**2. [Rule 1 - Bug] Worker not waking on send_new_client causing test timeout**
- **Found during:** Task 5.2b
- **Issue:** First `start_worker` impl created waker then dropped it; `WorkerHandle` had no stored waker, so `send_new_client()` couldn't interrupt `backend.poll(1s)`. Test timed out at 500ms.
- **Fix:** Store `waker: Arc<dyn WakerHandle>` in `WorkerHandle`; call `waker.wake()` after every `send_new_client()` and `stop()` call
- **Files modified:** `src/io_thread_worker.rs`

**3. [Rule 1 - Bug] `test_iobackend_trait_uniform` hanging — non-blocking not set before mio conversion**
- **Found during:** Task 5.1 test run
- **Issue:** `mio::net::TcpStream::from_std()` requires the std socket already be non-blocking; backend's `read()` called `stream.read()` in a loop that blocked waiting for data
- **Fix:** Call `stream.set_nonblocking(true)` on the accepted `std::net::TcpStream` before `mio::net::TcpStream::from_std()`; restructure test to poll in a loop rather than blocking on single call
- **Files modified:** `tests/io_backend_trait_test.rs`

**4. [Rule 2 - Clippy] `clippy::type_complexity` on test helper return type**
- **Found during:** Final clippy pass
- **Issue:** `spawn_n_workers_with_write()` returns a 3-tuple with a complex nested type; clippy `-D warnings` rejects
- **Fix:** Add `#[allow(clippy::type_complexity)]` on the helper function
- **Files modified:** `tests/phase18_05_continuous_workers_test.rs`

**5. [Rule 2 - Fmt] rustfmt diffs in io_uring.rs, mio_backend.rs, test files**
- **Found during:** Final `cargo fmt --all --check`
- **Fix:** `cargo fmt --all` applied; all diffs resolved
- **Files modified:** `src/io_backend/io_uring.rs`, `src/io_backend/mio_backend.rs`, `src/io_backend/mod.rs`, tests

### Scope Boundary — Pre-existing Test Failures (not introduced by this plan)

The following failures existed before any Plan 18-05 commits (verified via `git stash` probe):

- `test_runtime_kind_metric_mio` in `phase18_04_6_integration_test.rs`: server at `127.0.0.1:xxxxx` did not become ready within 10 seconds. Pre-existing integration test flake unrelated to per-worker architecture.
- `phase18_04_7_iopool_test` without `--features testing`: auto-discovered test fails to compile (missing feature gate). Pre-existing.

Both logged to `deferred-items.md` scope; not fixed per deviation scope boundary rule.

## Known Stubs

None. All WorkerHandle, IoBackend, and channel wiring is fully connected to real socket I/O in tests. No placeholder or hardcoded empty data flows to UI or callers.

## Threat Flags

None. No new network endpoints or auth paths introduced. IoBackend is an internal runtime abstraction not exposed over the network.

## Self-Check: PASSED

Files verified present:
- `crates/beava-runtime-core/src/io_backend/mod.rs` — FOUND
- `crates/beava-runtime-core/src/io_backend/mio_backend.rs` — FOUND
- `crates/beava-runtime-core/src/io_backend/io_uring.rs` — FOUND
- `crates/beava-runtime-core/src/io_thread_worker.rs` — FOUND
- `crates/beava-runtime-core/tests/io_backend_trait_test.rs` — FOUND
- `crates/beava-server/tests/phase18_05_continuous_workers_test.rs` — FOUND

Commits verified: b1d3a4f, 5dc3bf7, 6e52eb7, 837d4fd, 751ddb4, d2933d0, 217c249, 4eb3fdf — all present in `git log --oneline -10`.
