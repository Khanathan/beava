---
phase: 50-multi-shard-routing
plan: "08"
subsystem: benchmark-ship-gate
tags: [tpc, benchmark, metrics-parity, ship-gate, run_matrix]
dependency_graph:
  requires: [50-07]
  provides: [metrics_parity_test, run_matrix_beava_shards_support, benchmark_readme]
  affects: [benchmark/fraud-pipeline/run_matrix.sh, benchmark/50-multi-shard-routing/README.md, tests/test_metrics_parity.rs]
tech_stack:
  added: []
  patterns: [OnceLock guard for global recorder in tests, PrometheusHandle scrape in test]
key_files:
  created:
    - tests/test_metrics_parity.rs
    - benchmark/50-multi-shard-routing/README.md
  modified:
    - benchmark/fraud-pipeline/run_matrix.sh
decisions:
  - "Metrics parity test uses install_prometheus_recorder() + graceful skip if global already claimed"
  - "run_matrix.sh BEAVA_SHARDS=auto resolves to nproc/hw.physicalcpu; default=1 for regression baseline"
  - "Benchmark README provides PENDING rows for human-verify checkpoint to fill in"
metrics:
  duration_minutes: 20
  completed: "2026-04-18T16:10:00Z"
  tasks_completed: 1
  files_modified: 3
  status: "checkpoint:human-verify PENDING"
---

# Phase 50 Plan 08: Benchmark Ship-Gate + Metrics Parity Summary

One-liner: 5-test automated metrics parity suite verifies all 9 D-07 series present; run_matrix.sh updated with BEAVA_SHARDS=N/auto support; human-verify checkpoint pending for 3x EPS gate.

## What Was Built (Task 1 — Automated)

### Metrics parity test (`tests/test_metrics_parity.rs`, 5 tests)

1. `all_9_series_present_after_registration` — installs recorder, calls register_shard_metrics(2), scrapes PrometheusHandle, asserts all 9 D-07 series names present
2. `metric_name_constants_match_d07_spec` — compile-time verification of all 9 constant strings
3. `register_shard_metrics_safe_without_recorder` — no-panic guarantee at N=1 and N=4
4. `series_count_is_9` — PER_SHARD_SERIES.len()=7 + GLOBAL_SERIES.len()=2 = 9
5. `record_shard_event_no_panic` — hot-path helpers safe without recorder

All 5 pass. Commit: 0b0cfb9

### run_matrix.sh BEAVA_SHARDS support

- `BEAVA_SHARDS=auto` → `$(nproc)` on Linux, `$(sysctl -n hw.physicalcpu)` on macOS
- `BEAVA_SHARDS=1` → regression baseline (Phase 49 behavior preserved)
- Header line shows `BEAVA_SHARDS=N` in matrix run output
- Documented in script header with ship-gate criteria

### benchmark/50-multi-shard-routing/README.md

- Ship-gate criteria table (3x EPS, cross_shard_fraction, N=1 regression, metrics parity)
- 9-cell results template (PENDING rows for human-verify)
- Run instructions for auto and N=1 modes

## Pending (Task 2 — Human-Verify Checkpoint)

The following gates require manual benchmark execution:

| Gate | Target | Status |
|------|--------|--------|
| complex-c8-x8 at N=CPU_COUNT | >= 918,621 EPS | PENDING |
| cross_shard_fraction | < 0.40 | PENDING |
| N=1 regression | >= 290,897 EPS | PENDING |

Resume signal: `"approved N=K EPS=X cross_shard_fraction=Y"`

## Deviations from Plan

None — Task 1 executed exactly as written.

## Self-Check: PASSED (Task 1)

- tests/test_metrics_parity.rs: 5 tests — FOUND, all pass
- benchmark/fraud-pipeline/run_matrix.sh: BEAVA_SHARDS support — FOUND
- benchmark/50-multi-shard-routing/README.md: ship-gate template — FOUND
- Commit 0b0cfb9 — FOUND
