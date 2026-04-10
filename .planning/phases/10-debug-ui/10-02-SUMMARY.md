---
phase: 10-debug-ui
plan: 02
subsystem: infra
tags: [throughput, ewma, debug-ui, dedup, cascade, fan-out, ahash]

requires:
  - phase: 06
    provides: AppState struct wiring inside src/server/tcp.rs
  - phase: 07
    provides: push_with_cascade + get_cascade_targets + fan_out_targets APIs
provides:
  - src/server/throughput.rs ThroughputTracker module with per-stream EWMA state
  - ThroughputTracker::bump_unique dedup-safe API for primary+cascade+fan-out bump sites
  - ThroughputTracker::decay_all read-time decay for /debug/throughput
  - ThroughputTracker::snapshot read API for /debug/throughput (Plan 10-03)
  - AppState.throughput field owned by the main/tcp-handler mutex
  - Instrumentation block at the end of the Push arm of handle_sync_command
affects: [10-03, 10-04, 10-05]

tech-stack:
  added: []
  patterns:
    - Lock-once instrumentation (RESEARCH Pattern 3 option A) — the throughput bump lives inside the same AppState mutex scope that the push already holds, so there is zero new contention
    - Dedup-safe fan-out counting — every new "touch multiple streams per event" feature in this file should go through a single HashSet-deduped bump_unique call rather than naive loops across cascade_targets and fan_out_targets

key-files:
  created:
    - src/server/throughput.rs
  modified:
    - src/server/mod.rs
    - src/server/tcp.rs
    - src/main.rs

key-decisions:
  - "Option A (single-mutex bump site) over dashmap/RwLock — zero new contention on the single-threaded core, no new dependency"
  - "bump_unique builds a Vec<&str> of touched stream names at the END of the Push arm, then dedupes via a std::collections::HashSet — re-derives the exact same skip logic the fan-out loop uses so counts match the streams whose state actually changed"
  - "Fully-qualified crate::server::throughput::ThroughputTracker path in AppState field declaration to let external constructors reference the field without extra `use` lines"
  - "pending_total_for_test test-only counter via #[cfg(test)] — gives the does_not_double_count_cascade regression test a deterministic assertion target without leaking test scaffolding into the public snapshot() API"
  - "dt <= 0.0 guard on fold_event — two bumps at the same Instant leave EWMAs untouched but update last_update, avoiding division by zero that RESEARCH §Pattern 3 explicitly warned about"

patterns-established:
  - "Pattern: lock-once instrumentation. New cross-stream metrics go inside the existing AppState mutex scope in handle_sync_command rather than behind a second lock, keeping the single-threaded core contention-free."
  - "Pattern: dedup-safe multi-stream bumps. When instrumenting code paths that touch primary + cascade + fan-out streams, collect names into a Vec and pass to bump_unique (HashSet-deduped) rather than incrementing in each loop."

requirements-completed:
  - DBUI-02

duration: 10min
completed: 2026-04-10
---

# Phase 10 Plan 02: Per-Stream EWMA Throughput Tracker Summary

**ThroughputTracker module with 5s / 60s / 300s EWMAs, dedup-safe bump_unique wired into the Push arm of handle_sync_command so cascade + fan-out counts never double.**

## Performance

- **Duration:** ~10 minutes
- **Started:** 2026-04-10 (Wave 1 dispatch after Plan 10-01 completion)
- **Completed:** 2026-04-10
- **Tasks:** 2
- **Files created:** 1 (`src/server/throughput.rs`)
- **Files modified:** 3 (`src/server/mod.rs`, `src/server/tcp.rs`, `src/main.rs`)

## Accomplishments

- New `src/server/throughput.rs` module implementing `ThroughputTracker` with per-stream EWMA state over 5-second, 60-second, and 300-second time constants. Uses `ahash::AHashMap<String, StreamThroughput>` (no new dependency). Single-threaded access — all mutation goes through `&mut self` so it lives safely inside the existing `AppState` mutex.
- Dedup-safe `bump_unique` API that accepts an iterator of stream names and counts each unique name exactly once, via `std::collections::HashSet`. This is the canonical call site from the TCP handler — naive per-loop increments would double-count cascade targets.
- `decay_all(now)` method for read-time exponential decay, so the Plan 10-03 `/debug/throughput` handler will see idle streams' rates approach zero rather than staying pinned at the last bump value.
- `snapshot()` returning `Vec<(String, StreamThroughput)>` as the read API for Plan 10-03.
- Push arm of `handle_sync_command` (src/server/tcp.rs lines 283–331, immediately before `app.metrics.push_latency_seconds`) builds a `Vec<&str>` of every stream name touched by the push — primary + cascade targets + fan-out targets with the same skip logic as the fan-out loop (same-name, same-key-field, cascade-overlap, missing payload key) — and passes it to `bump_unique` in a single call.
- Six unit tests under `#[cfg(test)] mod tests` in `src/server/throughput.rs`:
  - `bump_increments_pending_and_folds_into_ewma` — two bumps 1s apart produce non-zero EWMAs
  - `decay_all_reduces_idle_streams_toward_zero` — burst + 60s idle drives 5s EWMA below 0.01 and confirms longer time-constant EWMAs decay more slowly
  - `snapshot_returns_all_tracked_streams` — three distinct stream bumps produce three snapshot entries
  - `does_not_double_count_cascade` — **the RESEARCH §Pitfall 4 + VALIDATION.md regression test**: single bump_unique call across `["Transactions", "Alerts", "FraudScore"]` increments each stream's `pending_total_for_test` by exactly 1
  - `bump_unique_deduplicates_repeated_targets` — repeated names in the iterator collapse to one bump per unique name
  - `first_bump_initializes_without_panic` — two (and three) bumps at the same `Instant` must not divide by zero

## Task Commits

1. **Task 1: Create src/server/throughput.rs with ThroughputTracker + unit tests** — `1a4ed3a` (feat)
2. **Task 2: Wire ThroughputTracker into AppState and instrument the Push arm** — `831b9c7` (feat) + `b405173` (chore: fully-qualify path to eliminate unused-import warning)

_All 6 unit tests pass on both Task 1 and the post-Task-2 re-run. Full library test suite (461 tests) passes after Task 2._

## Public API (for Plan 10-03)

```rust
// src/server/throughput.rs
pub struct ThroughputTracker { /* AHashMap<String, StreamThroughput> */ }
pub struct StreamThroughput {
    pub last_update: Option<Instant>,
    pub ewma_5s: f64,
    pub ewma_1m: f64,
    pub ewma_5m: f64,
}

impl ThroughputTracker {
    pub fn new() -> Self;
    pub fn bump(&mut self, stream_name: &str, now: Instant);
    pub fn bump_unique<'a, I: IntoIterator<Item = &'a str>>(&mut self, names: I, now: Instant);
    pub fn decay_all(&mut self, now: Instant);
    pub fn snapshot(&self) -> Vec<(String, StreamThroughput)>;
}
```

Plan 10-03's `/debug/throughput` handler should:
1. Lock `AppState`.
2. Call `app.throughput.decay_all(Instant::now())` so idle streams report declining rates.
3. Call `app.throughput.snapshot()`, drop the lock, build a JSON map `{ stream_name: { ewma_5s, ewma_1m, ewma_5m } }`.

## Insertion Point in handle_sync_command

The throughput instrumentation block lives in `src/server/tcp.rs` at the end of the `Command::Push { .. }` match arm, **immediately before** the final three lines:

```rust
let push_elapsed = push_start.elapsed();
app.metrics.push_latency_seconds = push_elapsed.as_secs_f64();
app.metrics.events_total += 1;
```

The block re-derives fan-out skip logic against `app.engine` (immutable borrow) and then calls `app.throughput.bump_unique(touched.into_iter(), now_inst)` — the only `&mut` borrow of `app.throughput` in the arm. The earlier `AppState { ref engine, ref mut store, ref mut event_log, .. } = *app;` destructure bindings are no longer in use past this point, so the compiler accepts the re-borrow without scope juggling.

## Files Created/Modified

- **`src/server/throughput.rs`** (created, ~250 lines) — ThroughputTracker module. Exponential decay formula `ewma * exp(-dt/tau) + 1/dt`. Six unit tests under `#[cfg(test)]`.
- **`src/server/mod.rs`** (modified, +1 line) — added `pub mod throughput;` after the existing three module declarations.
- **`src/server/tcp.rs`** (modified, ~50 lines added) — added `pub throughput: crate::server::throughput::ThroughputTracker,` field to `AppState`; added dedup-safe bump instrumentation block at the end of the Push arm; added `throughput: crate::server::throughput::ThroughputTracker::new(),` to the in-file `make_shared_state()` test helper.
- **`src/main.rs`** (modified, +1 line) — added `throughput: tally::server::throughput::ThroughputTracker::new(),` to the `AppState { .. }` literal.

## Decisions Made

- **Lock-once instrumentation (RESEARCH Pattern 3 option A).** The throughput bump shares the mutex scope the push handler already owns. Zero new contention, zero new dependency, fewer failure modes than a second lock.
- **Dedup via `std::collections::HashSet<&str>` inside `bump_unique`.** The N here is tiny (1 primary + cascade depth + fan-out width — realistically < 10), so a small throwaway `HashSet` is cheaper than maintaining a sorted `Vec` and dwarfed by the push itself.
- **Initial EWMA values of 0.0 for first-ever bump.** There is no inter-arrival time to measure on the first event for a stream; the second event is what establishes the baseline rate. The `bump_increments_pending_and_folds_into_ewma` test encodes this assumption explicitly.
- **`dt <= 0.0` short-circuits without panic.** RESEARCH §Pattern 3 called out the division-by-zero risk. Two bumps at the same `Instant` (bench or burst tests) leave EWMAs untouched but advance `last_update`; the next real inter-arrival time is what captures the burst.
- **`pending_total_for_test` is `#[cfg(test)]`-only.** The cascade dedup regression test needs a deterministic assertion target that is not tangled with floating-point EWMA values, but that counter must NOT ship in the public snapshot API. Gating it with `#[cfg(test)]` keeps the public surface clean.
- **Fully-qualified path for the `AppState.throughput` field.** The field uses `crate::server::throughput::ThroughputTracker` literally so external constructors in `tests/test_pipeline.rs` / `tests/test_server.rs` (which Plan 10-05 will fix) can reference the field without needing a new `use` line. The in-file test helper also uses the fully-qualified path to avoid an `unused_imports` warning on `cargo check --lib --bin tally`.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 — Blocking] Updated in-file test helper `make_shared_state` to include the new field**

- **Found during:** Task 2 re-run of `cargo test --lib throughput`
- **Issue:** After adding the `throughput` field to `AppState`, the in-file `#[cfg(test)] mod tests` helper at `src/server/tcp.rs:591` failed to compile with `error[E0063]: missing field throughput in initializer of AppState`. This is in the same file we are editing (not a separate integration test), so it blocks our own Task 1 unit-test gate from re-running and is in-scope per the deviation rules.
- **Fix:** Added `throughput: crate::server::throughput::ThroughputTracker::new(),` as the last field of the test helper's `AppState { .. }` literal.
- **Files modified:** `src/server/tcp.rs`
- **Verification:** `cargo test --lib throughput` exits 0 with all 6 tests passing; full lib suite (461 tests) passes.
- **Committed in:** `831b9c7` (Task 2 commit — the fix landed alongside the field addition because they are a single compile unit)

**2. [Rule 1 — Bug / warning] Eliminated `unused_imports` warning after fully-qualifying field path**

- **Found during:** Task 2 post-commit verification against the plan's grep-based acceptance criterion
- **Issue:** The plan's acceptance criterion specified the literal string `pub throughput: crate::server::throughput::ThroughputTracker,` for grep; the initial implementation used a short `use ThroughputTracker;` import and declared the field as `pub throughput: ThroughputTracker,`. Switching to the fully-qualified form removed the last use-site of the short name in the non-test lib path, so `cargo check --lib --bin tally` began emitting `warning: unused import: crate::server::throughput::ThroughputTracker`. Because the plan gate is "cargo check --lib --bin tally is green" with zero warnings implied, this was fixed.
- **Fix:** Deleted the now-unused `use crate::server::throughput::ThroughputTracker;` line; updated the in-file test helper to also use the fully-qualified path.
- **Files modified:** `src/server/tcp.rs`
- **Verification:** `cargo check --lib --bin tally` exits 0 with zero warnings; `cargo test --lib throughput` still exits 0 with all 6 tests passing.
- **Committed in:** `b405173` (chore commit, same plan)

---

**Total deviations:** 2 auto-fixed (1 Rule 3 blocking, 1 Rule 1 warning)
**Impact on plan:** Both fixes were required to land the plan's own gates cleanly. No scope creep — both touch only the file we were already editing.

## Issues Encountered

None beyond the two deviations above. The RESEARCH §Pitfall 4 warning about double-counting was internalized at planning time and encoded directly into the `does_not_double_count_cascade` regression test, so the cascade-dedup correctness property was proven on the very first `cargo test --lib throughput` run without a debug cycle.

## User Setup Required

None. No new environment variables, no external services, no config changes.

## Next Phase Readiness

- **Plan 10-03 can immediately consume `ThroughputTracker::snapshot()` + `decay_all()`** from the new `/debug/throughput` HTTP handler. The API is stable and ready.
- **Plans 10-04 and 10-05 (HTML + integration tests)** can reference `app.throughput` via the standard `AppState` mutex pattern with no additional wiring.
- **Known deferred work for Plan 10-05:** `tests/test_pipeline.rs` line 536 and `tests/test_server.rs` line 30 still construct `AppState { .. }` literals that are missing the `throughput` field, so `cargo test` (which compiles integration tests) fails to compile until Plan 10-05 adds the field. `tests/test_snapshot.rs` also has a pre-existing stale `backfill_complete` literal — also Plan 10-05's problem. **Plan 10-02's gate is `cargo check --lib --bin tally` (not `--tests`) by explicit design in the plan's verification section.**
- **No blockers for Wave 2 / Wave 3 dispatch.**

## Self-Check: PASSED

- `src/server/throughput.rs` — FOUND (`pub struct ThroughputTracker`, `pub fn bump_unique`, `pub fn decay_all`, `pub fn snapshot`, `const TAU_5S = 5.0`, `const TAU_1M = 60.0`, `const TAU_5M = 300.0` all present).
- `src/server/mod.rs` — FOUND (`pub mod throughput;` present).
- `src/server/tcp.rs` — FOUND (`pub throughput: crate::server::throughput::ThroughputTracker,` in AppState; `app.throughput.bump_unique` in Push arm; `let mut touched: Vec<&str>` buffer present).
- `src/main.rs` — FOUND (`throughput: tally::server::throughput::ThroughputTracker::new(),` in AppState literal).
- Commit `1a4ed3a` — FOUND in `git log --oneline`.
- Commit `831b9c7` — FOUND in `git log --oneline`.
- Commit `b405173` — FOUND in `git log --oneline`.
- `cargo check --lib --bin tally` — exits 0, zero warnings.
- `cargo test --lib throughput` — 6 of 6 tests pass including `does_not_double_count_cascade`.
- `cargo test --lib` — 461 of 461 tests pass.

---

*Phase: 10-debug-ui*
*Plan: 02*
*Completed: 2026-04-10*
