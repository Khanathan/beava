---
phase: 48-shard-hint-scaffolding
plan: 02
subsystem: benchmarks
tags: [tpc, wave-0, criterion, bench, shard-hint]
dependency_graph:
  requires: [48-01]
  provides: [shard_scaffold bench, p50 baseline]
  affects: [benches/shard_scaffold.rs, Cargo.toml]
tech_stack:
  added: []
  patterns: [criterion BenchmarkGroup, Throughput::Elements(1), black_box]
key_files:
  created:
    - benches/shard_scaffold.rs
  modified:
    - Cargo.toml
decisions:
  - Used BenchmarkGroup pattern (matching hll_ops.rs) rather than simple c.bench_function
  - event values constructed outside b.iter() to measure only shard_hint_for_event call cost
metrics:
  duration: ~5 min (excl. disk-space recovery)
  completed: 2026-04-18
  tasks: 1
  files: 2
---

# Phase 48 Plan 02: Criterion Bench shard_scaffold Summary

**One-liner:** Criterion bench covering 3 event shapes; all p50 values far below 100 ns budget (6.5 ns, 12.6 ns, 5.6 ns) confirming Wave 0 is observationally inert.

## Files Created/Modified

| File | Change |
|------|--------|
| `benches/shard_scaffold.rs` | New criterion bench with `bench_shard_hint` group, 3 event shapes |
| `Cargo.toml` | Added `[[bench]] name = "shard_scaffold" harness = false` |

## Benchmark Results (dev machine — macOS, Apple Silicon)

| Bench ID | p50 (ns) | Budget | Status |
|----------|----------|--------|--------|
| `shard_hint/string_key` | 6.46 | <100 ns | PASS |
| `shard_hint/tuple_two_field_key` | 12.56 | <100 ns | PASS |
| `shard_hint/numeric_key` | 5.61 | <100 ns | PASS |

All three shapes are well within the <100 ns budget. The numeric_key path (graceful fallback, no hash) is fastest at 5.61 ns. The tuple_two_field_key path (larger JSON object, same hash path) is slightly slower at 12.56 ns due to JSON map traversal.

## Deviations from Plan

**1. [Rule 3 - Blocker] Disk full during `cargo bench --bench shard_scaffold --no-run`**
- **Found during:** Step 3 compile-check
- **Issue:** `/System/Volumes/Data` was at 100% capacity (440 GiB used of 460 GiB), preventing release build
- **Fix:** Ran `cargo clean --release` to remove 1.2 GiB of release artifacts; freed sufficient space for bench compilation
- **Impact:** No code changes; bench logic unchanged
- **Commit:** f0929d8 (bench itself unaffected)

## Self-Check: PASSED

- `benches/shard_scaffold.rs` — FOUND
- `Cargo.toml` has `name = "shard_scaffold"` — FOUND
- Commit f0929d8 — FOUND
- p50 values: all <100 ns — PASS
