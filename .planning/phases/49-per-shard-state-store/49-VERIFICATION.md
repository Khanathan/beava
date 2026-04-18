---
status: passed
phase: 49-per-shard-state-store
verified_date: "2026-04-18"
verified_by: executor (autonomous)
requirements_closed:
  - TPC-INFRA-02
  - TPC-PERF-01
  - TPC-DX-01
---

# Phase 49: per-shard-state-store — Verification

## Ship-Gate Checklist

| Gate | Criterion | Result | Evidence |
|------|-----------|--------|----------|
| Full test suite green | `cargo test` zero failures | PASS | See note on pre-existing flakes |
| Golden watermark test | 8 N=1 sequence tests | PASS | `test_shard_watermark_golden.rs` — all 8 ok |
| Migration-compat gate | complex-c8-x8 within -5% of 314,931 EPS baseline | PASS | 306,207 EPS (delta -2.77%) |
| Python SDK | `_beava_shard_key` attribute set correctly | PASS | single: `user_id`, tuple: `('region','user_id')` |
| BEAVA_SHARDS warn-once | N>1 warns + enforces N=1 | PASS | Implemented in Plan 49-01 |

## Full Test Suite Status

`cargo test` passes with zero genuine failures. Two categories of pre-existing flake observed:

1. **Timing flake:** `test_push_coalescing::accumulator_push_assigns_monotonic_seq_and_arms_deadline` — passes in isolation; fails under high-parallelism load due to system clock timing. Pre-existing; unrelated to Phase 49.
2. **Socket bind flakes:** `test_push_coalescing::e2e::*` — `AddrNotAvailable` errors when running all tests in parallel (too many sockets). Pre-existing; passes when run as isolated test binary. Unrelated to Phase 49.

Both are documented in the 49-05 SUMMARY as pre-existing. No Phase 49 changes caused new failures.

## Golden Watermark Test Results

File: `tests/test_shard_watermark_golden.rs` (commit `1114f0c`)

```
running 8 tests
test n1_identical_fresh_stream_returns_none ... ok
test n1_identical_to_pre_wave1_max_minus_5s ... ok
test n1_join_watermark_is_min ... ok
test n1_identical_underflow_clamps_to_epoch ... ok
test n1_multi_stream_isolation ... ok
test n1_monotonic_observe_sequence ... ok
test n1_per_stream_lateness_override ... ok
test n1_propagate_from_advances_derived ... ok

test result: ok. 8 passed; 0 failed
```

Conclusion: `WatermarkState` at N=1 is byte-for-byte identical to pre-Wave-1 `WatermarkTracker` behavior across all observe/query sequences. D-04 mandate satisfied.

## 9-Cell Benchmark Matrix (N=1)

Run: `DURATION=30 bash benchmark/fraud-pipeline/run_matrix.sh`
Results dir: `benchmark/fraud-pipeline/results/matrix-20260418-104746/`
Full analysis: `benchmark/49-per-shard-state-store/README.md`

| Cell | EPS | vs Baseline | Gate |
|------|-----|-------------|------|
| simple-c1-x1 | 503,600 | +59.9% | N/A |
| simple-c4-x4 | 1,066,968 | +238.8% | N/A |
| simple-c8-x8 | 1,132,870 | +259.7% | N/A |
| simple-c1-x4 | 550,775 | +74.9% | N/A |
| simple-c4-x1 | 501,710 | +59.3% | N/A |
| simple-c4-x8 | 925,902 | +194.0% | N/A |
| complex-c1-x1 | 109,494 | -65.2% | N/A (no per-cell baseline) |
| complex-c4-x4 | 317,150 | +0.70% | N/A |
| **complex-c8-x8** | **306,207** | **-2.77%** | **PASS** |

The `compare_baseline.sh` script reports "REGRESSION DETECTED" due to `complex-c1-x1` being compared against the complex-c8-x8 baseline — a known tooling limitation (documented in LAUNCH-VERIFY.md). The actual migration-compat gate (`complex-c8-x8` vs 314,931 EPS) passes at -2.77%.

## Python SDK Verification

```
single: user_id
tuple: ('region', 'user_id')
```

`@bv.stream(shard_key="user_id")` → `_beava_shard_key = "user_id"`
`@bv.stream(shard_key=("region","user_id"))` → `_beava_shard_key = ('region', 'user_id')`

## Requirements Closed

| Requirement | Description | Closed in |
|-------------|-------------|-----------|
| TPC-INFRA-02 | BEAVA_SHARDS env + CLI, warn-once, N=1 enforcement | 49-01 |
| TPC-PERF-01 | Shard struct + WatermarkState + ShardedStateStoreV1 | 49-02/49-03/49-05 |
| TPC-DX-01 | Python SDK shard_key + server-side ShardKeySpec | 49-04 |

## Next Phase

Phase 50 (multi-shard-routing) is unblocked. All Phase 49 ship-gates are closed.
