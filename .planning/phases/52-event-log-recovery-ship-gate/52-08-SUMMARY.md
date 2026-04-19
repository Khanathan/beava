---
phase: 52-event-log-recovery-ship-gate
plan: "08"
subsystem: benchmark
tags: [ship-gate, pareto, zipf, criterion, tpc-perf-07]
dependency_graph:
  requires: [52-07-PLAN.md]
  provides: [pareto-c8-x8 bench cell, TPC-PERF-07 gate]
  affects: [52-09-PLAN.md, 52-10-PLAN.md]
tech_stack:
  added: []
  patterns: [criterion benchmark, inline Zipf inverse-CDF, seeded RNG, ship-gate assertion in code]
key_files:
  created:
    - benches/pareto_workload.rs
    - benchmark/pareto-c8-x8/README.md
  modified:
    - Cargo.toml
    - .github/workflows/bench-nightly.yml
decisions:
  - "Zipf property test uses n=10_000 keys / 500 samples (plan spec): theoretical top-20% fraction ~83.6% for s=1.0, threshold 75% — avoids false failure at n=100 where theoretical fraction is only 69.4%"
  - "Ship-gate assertion placed after group.finish() in bench_pareto so it runs under both cargo bench and --test mode"
  - "Zipf sampler tests exposed as criterion bench group (not #[cfg(test)]) so they run under cargo bench --bench pareto_workload -- --test"
  - "cross_shard_fraction = 0.0 is structural invariant for single-key workloads; assertion enforces architectural correctness not just measurement"
metrics:
  duration: "~15 min"
  completed: "2026-04-19"
  tasks_completed: 1
  tasks_total: 2
  files_created: 2
  files_modified: 2
---

# Phase 52 Plan 08: pareto-c8-x8 Ship-Gate Benchmark Summary

One-liner: Zipf s=1.0 Criterion cell over 10k keys with inline inverse-CDF sampler and enforced cross_shard_fraction < 0.40 assertion in code.

## Status: CHECKPOINT REACHED (Task 2 awaiting human verification)

Task 1 (automated) complete and committed at `00f9447`.
Task 2 is a `checkpoint:human-verify` gate — human must run the full ship-gate matrix and confirm all 3 criteria before 52-09 proceeds.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | pareto-c8-x8 Criterion benchmark cell + ship-gate assertion | `00f9447` | benches/pareto_workload.rs, benchmark/pareto-c8-x8/README.md, Cargo.toml, .github/workflows/bench-nightly.yml |

## Tasks Pending (checkpoint)

| Task | Name | Status |
|------|------|--------|
| 2 | Ship-gate verification run + human approval | awaiting human-verify |

## What Was Built

### benches/pareto_workload.rs

Criterion benchmark for the `pareto-c8-x8` cell:

- **Inline Zipf sampler** (`zipf_sample`): inverse-CDF method, ~20 lines, no external crate. `P(k) ∝ 1/(k+1)^s` with s=1.0 over 10,000 distinct keys.
- **Benchmark cell** `pareto-c8-x8`: 8 streams × 8x multiplier = 64 events/iter, Criterion `Throughput::Elements(64)` annotation for EPS reporting.
- **Ship-gate assertion** (in code, not just observed):
  ```rust
  assert!(
      cross_shard_fraction < 0.40,
      "Ship-gate FAILED: cross_shard_fraction={:.3} >= 0.40 (TPC-PERF-07)",
      cross_shard_fraction
  );
  ```
- **Zipf sampler tests** as criterion bench group (`zipf-sampler-tests`): Pareto property (top 20% keys ≥ 75% events), range check, determinism. Runs under `-- --test`.

### benchmark/pareto-c8-x8/README.md

Placeholder with cell spec and architecture notes. **Human must fill in actual numbers from Task 2 ship-gate run.**

### .github/workflows/bench-nightly.yml

Added `pareto-benchmark` job: `cargo bench -p beava --bench pareto_workload -- --nocapture` on ubuntu-latest, 15-minute timeout. Criterion output uploaded as artifact.

## Verification Passed (Task 1)

```
cargo bench --bench pareto_workload -- --test

Testing zipf-sampler-tests/zipf_pareto_property   Success
Testing zipf-sampler-tests/zipf_sample_in_range   Success
Testing zipf-sampler-tests/hot_key_deterministic   Success
Testing pareto-c8-x8/c8-x8-pareto/n_shards=8      Success

[pareto-c8-x8] events_total=64 events_cross_shard=0 cross_shard_fraction=0.0000
[pareto-c8-x8] Ship-gate PASSED: cross_shard_fraction=0.0000 < 0.40
```

```
grep "cross_shard_fraction < 0.40" benches/pareto_workload.rs
# → line 220: assert!(cross_shard_fraction < 0.40, ...)

grep pareto-benchmark .github/workflows/bench-nightly.yml
# → line 52: pareto-benchmark:
```

## Architecture Notes

For single-key-field streams (`user_id` only), every event routes to exactly one shard regardless of key distribution. Zipf skew creates **shard imbalance** (hot shards), not **cross-shard fan-out**. The `cross_shard_fraction` is always 0.0 for single-key workloads — the assertion enforces this invariant.

Cross-shard fraction > 0 only occurs with multi-key-field pipelines (e.g., fraud pipeline with 4 cascade keys per event). Those are tracked separately under `BEAVA_SHARD_PROBE`.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Corrected Zipf property test key count from 100 to 10,000**

- **Found during:** Task 1 test run
- **Issue:** Plan specifies "top 20% of 100 keys receives ≥75% of events" but theoretical value for n=100 s=1.0 is H(20)/H(100) ≈ 69.4% — structurally impossible to pass with correct implementation. Plan intent ("10 000 keys") was used instead.
- **Fix:** Zipf property test uses n=10,000 keys, 500 samples, top 2,000 keys. Theoretical fraction ~83.6%; passes 75% comfortably.
- **Files modified:** benches/pareto_workload.rs
- **Commit:** 00f9447

**2. [Rule 2 - Missing functionality] Zipf tests exposed as criterion bench group**

- **Found during:** Task 1 — `#[cfg(test)]` blocks in `[[bench]]` binaries don't run under `cargo test --bench`
- **Fix:** Moved verify_* functions outside `#[cfg(test)]`, exposed as `bench_zipf_sampler_tests` criterion group. Tests now run under both `cargo bench` and `-- --test`.
- **Files modified:** benches/pareto_workload.rs
- **Commit:** 00f9447

## Known Stubs

- `benchmark/pareto-c8-x8/README.md` results table is placeholder (`—` in all cells) — to be filled by human reviewer in Task 2.

## Self-Check: PASSED

- [x] `benches/pareto_workload.rs` exists
- [x] `benchmark/pareto-c8-x8/README.md` exists
- [x] Commit `00f9447` exists in git log
- [x] `cross_shard_fraction < 0.40` assertion in code (line 220)
- [x] `pareto-benchmark` job in bench-nightly.yml (line 52)
