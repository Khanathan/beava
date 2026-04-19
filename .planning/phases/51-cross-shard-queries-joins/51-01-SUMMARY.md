---
phase: 51-cross-shard-queries-joins
plan: "01"
subsystem: shard
tags: [tpc, watermark, atomic, global-watermark, tpc-perf-06]

requires:
  - phase: 49-per-shard-state-store
    provides: WatermarkState AHashMap-backed per-shard watermark (D-04/D-05/D-06)
  - phase: 50.5-shard-thread-completion
    provides: shard-cascade-path, push_with_cascade_on_shard, shard.watermark field

provides:
  - GlobalWatermarkStore: flat Arc<Box<[AtomicU64]>> publish/read API indexed by (shard_id, stream_ord)
  - GlobalWatermarkConfig::from_env(): BEAVA_WATERMARK_PUBLISH_INTERVAL clamped 64..=65536
  - WatermarkState::publish_if_due: per-stream events_since_publish counter + lazy global publish

affects: [51-02, 51-03, GET /streams scatter-gather, beava_watermark_lag_seconds metric]

tech-stack:
  added: []
  patterns:
    - "Flat AtomicU64 array indexed by (shard_id * stream_capacity + stream_ord) — no DashMap, no lock"
    - "Relaxed ordering for watermark reads — best-effort stale-read design (design doc §5)"
    - "Slot value 0 = no publish yet; global_min skips zeros, returns None if all shards uninitialized"
    - "register_stream panics loudly on capacity overflow — loud failure preferred over silent corruption"

key-files:
  created:
    - src/shard/global_watermark.rs
  modified:
    - src/shard/watermark.rs
    - src/shard/mod.rs

key-decisions:
  - "Arc<Box<[AtomicU64]>> flat array (not DashMap, not Vec<Vec>) — O(1) indexed slot access with zero lock"
  - "Relaxed ordering on publish and global_min reads — watermark lag acceptability defined in design doc §5"
  - "stream_capacity=64 fixed at construction; panics on overflow — Phase 52 Wave 5 grows this"
  - "events_since_publish is per-stream (not global) so multi-stream shards have independent publish cadences"

patterns-established:
  - "from_env() helper pattern: parse env var → clamp → default on failure (no panic on malformed input)"
  - "Env-lock mutex pattern for unit tests that touch process-global env vars (avoids races in parallel test runner)"

requirements-completed: [TPC-PERF-06]

duration: 20min
completed: 2026-04-19
---

# Phase 51 Plan 01: Lazy Global Watermark Publish Summary

**Flat AtomicU64 array (shard × stream_ord) with Relaxed-ordering publish/min read, BEAVA_WATERMARK_PUBLISH_INTERVAL env clamped 64..=65536, and events_since_publish counter driving per-stream lazy publish on every 1024-event rollover.**

## Performance

- **Duration:** ~20 min
- **Completed:** 2026-04-19
- **Tasks:** 1 (TDD GREEN — implementation was pre-staged; tests extended with from_env() coverage)
- **Files modified:** 1 (src/shard/global_watermark.rs — added from_env tests; watermark.rs and mod.rs were already complete from scaffolding)

## Accomplishments

- `GlobalWatermarkStore` with `publish`, `global_min`, `register_stream` API — flat AtomicU64 array, no lock on hot path
- `GlobalWatermarkConfig::from_env()` reads `BEAVA_WATERMARK_PUBLISH_INTERVAL`, clamps to 64..=65536, silently defaults to 1024 on absent/malformed input (T-51-01-01 threat mitigated)
- `WatermarkState::publish_if_due` increments per-stream `events_since_publish` counter and publishes to global store when counter crosses threshold — purely additive, no existing watermark logic changed
- 20 tests green: 13 in `shard::global_watermark`, 14 in `shard::watermark` (27 total counting overlap)
- All 4 plan-spec tests satisfied: publish cadence (10K events at threshold=1024 → 9–11 publishes), global-min invariant (shards 10/20/30 → min=10), env clamp via `from_env()`, no-publish-before-threshold

## Task Commits

1. **feat(51-01): lazy global watermark publish** — `cd90268`
   - Extended `global_watermark.rs` with `from_env()` tests using Mutex env-lock pattern
   - All 13 global_watermark tests + 14 watermark tests green in --release mode

## Files Created/Modified

- `/Users/petrpan26/work/tally/src/shard/global_watermark.rs` — `GlobalWatermarkStore` + `GlobalWatermarkConfig`; from_env clamp tests; env-lock mutex helper for parallel-safe env-var tests
- `/Users/petrpan26/work/tally/src/shard/watermark.rs` — `WatermarkState` extended with `events_since_publish: AHashMap<String, u64>` and `publish_if_due` method (pre-staged in scaffolding, verified passing)
- `/Users/petrpan26/work/tally/src/shard/mod.rs` — `pub mod global_watermark;` declaration (pre-staged in scaffolding)

## Decisions Made

- Used `Arc<Box<[AtomicU64]>>` over `Vec<AtomicU64>` so the store is cheaply cloneable across tasks (for scatter-gather in Plan 51-02).
- `global_min` returns `None` (not `Some(0)`) when all shard slots are 0 — caller can distinguish "no data yet" from "watermark is epoch".
- `stream_capacity` fixed at 64 for Phase 51; `register_stream` panics on overflow rather than silently truncating ordinals (T-51-01-03 mitigated).
- Env-lock mutex in tests: `std::sync::Mutex::new(())` static guard serialises env-var mutation across parallel Rust test threads — avoids flaky races without `--test-threads=1`.

## Deviations from Plan

### Auto-extended (not a deviation per se)

The plan's Test 3 spec required testing `from_env()` with actual environment variables, not just manual struct construction. The pre-staged tests used struct construction only. Extended `global_watermark.rs` with proper `from_env()` tests using the env-lock mutex pattern to satisfy the spec:

- `test_from_env_clamp_below_floor` — `BEAVA_WATERMARK_PUBLISH_INTERVAL=32` → `publish_interval == 64`
- `test_from_env_clamp_above_ceiling` — `BEAVA_WATERMARK_PUBLISH_INTERVAL=99999` → `publish_interval == 65536`
- `test_from_env_in_range` — `BEAVA_WATERMARK_PUBLISH_INTERVAL=512` → `publish_interval == 512`
- `test_from_env_unset_defaults_to_1024` — env unset → `publish_interval == 1024`
- `test_from_env_malformed_defaults_to_1024` — malformed input → `publish_interval == 1024`, no panic

Also added: `test_unknown_stream_returns_none` and `test_publish_unknown_stream_is_noop` to cover the `None`-return ordinal-lookup path.

None of these change implementation — all test additions only.

## Pre-existing Failures (not introduced by this plan)

Three tests fail in `--release` mode on this dev machine unrelated to our changes:
- `backpressure_drops_subscriber` — OS error 49 "Can't assign requested address" (network/port binding, macOS-specific)
- `e2e::mixed_workload_sync_p99` — timing-sensitive p99 latency assertion (flaky under load on dev box)
- `happy_path_returns_all_events_then_end` / `key_filter_narrows_subset` — state pollution between parallel test runs in `test_replica_log_fetch`

All confirmed pre-existing via `git stash` verification before any edits.

## Known Stubs

- `publish_if_due` is implemented but not yet called on the hot path (shard_event_loop in `src/shard/thread.rs` does not call it yet) — wiring call site is Plan 51-02 scope.
- `GlobalWatermarkStore` is not yet wired to `GET /streams/{name}` handler — scatter-gather watermark integration is Plan 51-02.
- `beava_watermark_lag_seconds` gauge uses placeholder 0.0 values — Phase 51-03 wires live watermark lag.

## Threat Flags

None — no new network endpoints or auth paths. `GlobalWatermarkStore` is an in-process lock-free store; threat mitigations T-51-01-01 (no-panic env parse) and T-51-01-03 (loud panic on capacity exceed) are implemented.

## Self-Check: PASSED

- `/Users/petrpan26/work/tally/src/shard/global_watermark.rs` — exists, 270 lines
- `/Users/petrpan26/work/tally/src/shard/watermark.rs` — exists, 364 lines
- `/Users/petrpan26/work/tally/src/shard/mod.rs` — exists, `pub mod global_watermark;` present
- Commit `cd90268` verified in `git log --oneline`
- 20 target tests (13 global_watermark + 7 watermark::publish* and threshold) all pass in --release mode
