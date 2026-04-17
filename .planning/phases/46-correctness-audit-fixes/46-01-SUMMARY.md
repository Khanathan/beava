---
phase: 46-correctness-audit-fixes
plan: "01"
subsystem: deps, bench-infra, test-scaffolds, CORR-05
tags: [deps, benchmark, test-scaffolds, corr-05, wave-0]
dependency_graph:
  requires: []
  provides:
    - proptest 1.11 in dev-deps
    - arc-swap 1.9 in runtime deps
    - benchmark/fraud-pipeline/run_matrix.sh (9-cell bench driver)
    - benchmark/fraud-pipeline/compare_baseline.sh (regression gate)
    - 9 TDD-RED test scaffolds (Waves 2-5)
    - benches/dirty_swap.rs criterion scaffold
    - CORR-05 closed (test_backfill_uses_single_event_path.rs green)
  affects:
    - All Phase 46 plans that use proptest (Plans 02, 04, 06, 07)
    - Plan 07 (arc-swap runtime dep for D-21)
    - Plans 03, 07 (bench matrix gate via run_matrix.sh)
tech_stack:
  added:
    - proptest = "1.11" (dev-dep) — pinned to 1.11.0 in Cargo.lock
    - arc-swap = "1.9" (runtime dep) — pinned to 1.9.1 in Cargo.lock
  patterns:
    - Source-text grep test for CORR-05 (no server boot; reads fn body, asserts call sites)
    - TDD RED scaffold pattern with Wave-N #[ignore] markers
    - 9-cell matrix runner via explicit cell list (not nested loops)
key_files:
  created:
    - Cargo.toml (modified — deps added + [[bench]] dirty_swap)
    - Cargo.lock (updated — proptest 1.11.0, arc-swap 1.9.1 pinned)
    - benchmark/fraud-pipeline/run_matrix.sh
    - benchmark/fraud-pipeline/compare_baseline.sh
    - tests/test_batch_event_time_property.rs
    - tests/test_watermarks_per_stream_lateness.rs
    - tests/test_snapshot_lateness_migration.rs
    - tests/ship_gate.rs
    - tests/test_eviction_event_time_clock.rs
    - tests/test_fork_watermark_propagation.rs
    - tests/test_ring_buffer_drops_metric.rs
    - tests/test_snapshot_rollover_race.rs
    - tests/test_backfill_uses_single_event_path.rs
    - benches/dirty_swap.rs
decisions:
  - "proptest was NOT in tree; CONTEXT.md/SUMMARY.md claim was stale — added proptest = 1.11"
  - "humantime_serde NOT added; D-09 reuses existing parse_duration_str per RESEARCH.md Gap 4"
  - "arc-swap added to [dependencies] (runtime), not [dev-dependencies], because Plan 07 D-21 uses it in production code"
  - "compare_baseline.sh handles both flat and throughput-nested aggregate_eps JSON shapes"
  - "CORR-05 closed via source-text grep test (no production code change required)"
metrics:
  duration_minutes: ~15
  completed_date: "2026-04-17"
  tasks_completed: 3
  tasks_total: 3
  files_created: 14
  files_modified: 1
---

# Phase 46 Plan 01: Deps, Bench Shims, Test Scaffolds, CORR-05 Summary

One-liner: proptest 1.11 + arc-swap 1.9 pinned; 9-cell bench gate shims committed; 9 TDD-RED Wave 2-5 scaffolds + CORR-05 green with zero production-code change.

## Commits

| Commit | Message | Files |
|--------|---------|-------|
| `ed1ea65` | feat(46-01): add proptest+arc-swap deps and 9-cell bench shims | Cargo.toml, Cargo.lock, run_matrix.sh, compare_baseline.sh |
| `8493554` | test(46-01): scaffold 9 Phase 46 test files + dirty_swap bench (TDD RED for Waves 2-5) | 9 test files, benches/dirty_swap.rs, Cargo.toml |
| `fa14425` | test(46-01): add CORR-05 verification test (2d.i closure — run_backfill uses single-event path) | tests/test_backfill_uses_single_event_path.rs |

## Cargo.lock Pinned Versions

- `proptest = "1.11.0"` (dev-dep; source registry crates.io)
- `arc-swap = "1.9.1"` (runtime dep; source registry crates.io)

## Research Corrections Applied

### Correction 1: proptest was NOT in the tree
CONTEXT.md §"Reusable Assets" and SUMMARY.md both claimed proptest was already in dev-deps. Verified by grepping Cargo.toml and Cargo.lock: absent. Added `proptest = "1.11"` to `[dev-dependencies]`.

### Correction 2: humantime_serde is NOT needed for D-09
RESEARCH.md Gap 4 confirmed that `parse_duration_str` at `src/duration.rs:23` already handles duration strings. D-09 (Plan 04) will reuse this existing helper; `humantime_serde` was NOT added.

## Ignored Test Functions (Wave-N Audit)

| File | Test Function | Wave | Blocker |
|------|--------------|------|---------|
| test_batch_event_time_property.rs | `batch_path_equals_single_event_path` | Wave 2 | D-01/D-02: new signature |
| test_watermarks_per_stream_lateness.rs | `per_stream_override_honored` | Wave 3 | D-09/D-10: StreamDefinition field |
| test_watermarks_per_stream_lateness.rs | `absent_field_defaults_to_5s` | Wave 3 | D-09/D-10: StreamDefinition field |
| test_snapshot_lateness_migration.rs | `old_snapshot_loads_with_default_lateness` | Wave 3 | D-12: serde(default) |
| ship_gate.rs | `test_ship_gate_backfill_crash_recover` | Wave 3 | D-15: run_backfill event_time |
| test_eviction_event_time_clock.rs | `ttl_honors_event_time_not_wall_clock` | Wave 3 | D-17: eviction.rs:63 |
| test_fork_watermark_propagation.rs | `replica_batch_advances_watermark` | Wave 3 | D-19: replica_ingest_batch |
| test_ring_buffer_drops_metric.rs | `bounded_labels` | Wave 4 | D-05/D-06/D-08: counter |
| test_ring_buffer_drops_metric.rs | `counters_mutually_exclusive` | Wave 4 | D-08: mutual exclusivity |
| test_snapshot_rollover_race.rs | `busy_racer_no_lost_keys` | Wave 4 | D-21: ArcSwap dirty-set |

Total ignored: 10 test functions across 8 files.

## CORR-05 Closure

`tests/test_backfill_uses_single_event_path.rs` runs **unignored** and passes:

```
test run_backfill_uses_push_for_backfill_not_handle_push_batch ... ok
test result: ok. 1 passed; 0 failed; 0 ignored
```

Mechanism: reads `src/server/tcp.rs`, extracts the `run_backfill` function body via brace-balancing, asserts:
1. `push_for_backfill(` appears at least once.
2. `push_batch_with_cascade_no_features(` appears zero times.
3. `handle_push_batch(` appears zero times.

Zero production-code changes. If a future refactor routes backfill through the batch path, this test flips RED and forces the audit conversation.

## Verification Gates Passed

- `cargo build --release --bin beava` — green
- `cargo build --tests --release` — green
- `cargo build --benches --release` — green
- `cargo test --test test_backfill_uses_single_event_path --release` — green (1 passed)
- `bash benchmark/fraud-pipeline/compare_baseline.sh benchmark/fraud-pipeline/results/baseline/summary.json` — green (delta=+0.00%, threshold=-5.0%)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] compare_baseline.sh baseline summary.json uses nested aggregate_eps**
- **Found during:** Task 1 acceptance testing
- **Issue:** The plan's compare script template assumed flat `{ "aggregate_eps": N }` but the committed baseline at `benchmark/fraud-pipeline/results/baseline/summary.json` stores the value under `throughput.aggregate_eps`.
- **Fix:** compare_baseline.sh `extract_eps()` helper handles both flat and `throughput`-nested shapes. Also fixed `cd "$(dirname "$0")"` + relative `$1` path ordering so callers can pass relative paths from the repo root.
- **Files modified:** benchmark/fraud-pipeline/compare_baseline.sh
- **Commit:** ed1ea65

## Self-Check: PASSED

All 13 created/modified files found on disk. All 3 commits (ed1ea65, 8493554, fa14425) verified in git log.
