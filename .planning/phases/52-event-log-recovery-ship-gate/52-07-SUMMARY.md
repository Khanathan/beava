---
phase: 52-event-log-recovery-ship-gate
plan: 07
subsystem: proptest, ci
tags: [proptest, parity, tpc-corr-05, sharding, ci-gate, tdd]
requirements: [TPC-CORR-05]

dependency_graph:
  requires:
    - Phase 52-05 (beava::server::replica::compute_target_shard — fork rehash path)
    - Phase 52-06 (LSN dedup + EventLog LSN tagging — not directly used but upstream context)
    - Phase 50.5-01 (push_with_cascade_on_shard in PipelineEngine — core runner primitive)
    - Phase 48 (bench-nightly.yml — extended with sharding-parity-proptest job)
  provides:
    - tests/sharding_parity.rs: integration test binary entry point (--test sharding_parity)
    - tests/proptests/mod.rs: module declaration
    - tests/proptests/sharding_parity.rs: proptest generator + runner + assert_parity
    - .github/workflows/bench-nightly.yml: job sharding-parity-proptest (10k cases, 10min nightly)
    - .github/workflows/pr.yml: job sharding-parity-smoke (50 cases, 5min PR gate)
    - TPC-CORR-05: N=1↔N=8 parity harness — hard pre-merge gate for v1.2→main
  affects:
    - Any future shard-count change — harness must be re-run at the new N

tech_stack:
  added: []
  patterns:
    - "proptest 1.11 (already in dev-dependencies) — batch_strategy(): vec(event_strategy(), 1..=500)"
    - "TestEvent { key: [a-z]{3,8}, value: i64, time_offset_secs: u32 } — bounded key space for collision"
    - "run_batch(engine, events, n_shards, stream): shard_hint_for_event % N → push_with_cascade_on_shard"
    - "run_batch_fork: compute_target_shard(upstream_n=1, downstream_n=N, hint=0) → rehash path"
    - "assert_parity: exact equality for all features; 2% HLL tolerance for distinct_* features"
    - "PROPTEST_CASES env var gates nightly (10000) vs smoke (50) vs local (default 50)"

key_files:
  created:
    - tests/sharding_parity.rs
    - tests/proptests/mod.rs
    - tests/proptests/sharding_parity.rs
  modified:
    - .github/workflows/bench-nightly.yml
    - .github/workflows/pr.yml

decisions:
  - "In-process Shard instances (not TCP server): uses push_with_cascade_on_shard directly — avoids async server lifecycle, deterministic, fast"
  - "run_batch and run_batch_fork both route via the same hash function (shard_hint_for_event vs rehash_to_shard) — parity holds because both use ahash on the same key string"
  - "HLL (DistinctCount) tolerance: 2% relative — DistinctCountOp is deterministic per-shard so actual test results show 0% divergence; tolerance is a documented allowance per T-52-07-02"
  - "8 tests total: 1 determinism + 2 filter/map + 3 agg (count/sum/hll) + 1 join + 1 fork — covers all 5 operator types per D-13"
  - "branch protection note in pr.yml job comment: sharding-parity-smoke must be added to required status checks via GitHub UI"

metrics:
  duration_minutes: 25
  completed_at: "2026-04-18T00:00:00Z"
  tasks_completed: 2
  tasks_total: 2
  files_created: 3
  files_modified: 2
---

# Phase 52 Plan 07: N=1↔N=8 Proptest Parity Harness Summary

**One-liner:** In-process proptest harness compares `push_with_cascade_on_shard` feature outputs at N=1 vs N=8 for all 5 operator types (filter, map, agg, join, fork) and is wired as a hard PR + nightly CI gate (TPC-CORR-05 D-13/D-14).

## What Was Built

### Task 1 (RED + GREEN): Proptest generator + N=1 vs N=8 runner

`tests/sharding_parity.rs` — integration test entry point (`--test sharding_parity`)

`tests/proptests/mod.rs` — `pub mod sharding_parity;`

`tests/proptests/sharding_parity.rs`:

- **`TestEvent`** — `{ key: String, value: i64, time_offset_secs: u32 }`. Key from `[a-z]{3,8}` strategy (bounded to force collisions).
- **`batch_strategy()`** — `proptest::collection::vec(event_strategy(), 1..=500)`.
- **`make_engine()`** — registers 5 streams: `filter_stream` (Count with `where_expr`), `count_stream` (Count), `sum_stream` (Sum), `distinct_stream` (DistinctCount/HLL), `derive_stream` (Last + Derive).
- **`run_batch(engine, events, n_shards, stream)`** — routes each event to `Shard[shard_hint_for_event(payload) % n]`, calls `push_with_cascade_on_shard`, collects last FeatureMap per key.
- **`run_batch_fork(engine, events, downstream_n, stream)`** — simulates N=1 upstream → N=8 downstream via `compute_target_shard(key, 1, N, 0)` (always rehashes because upstream_n != downstream_n).
- **`assert_parity(n1, n8, stream_name)`** — exact equality for all features; 2% relative tolerance for `distinct_*` features (T-52-07-02 mitigation).
- **`proptest_config()`** — reads `PROPTEST_CASES` env (default 50).

8 tests total:

| # | Name | Operator type |
|---|------|---------------|
| 1 | `test_generator_determinism` | Generator contract |
| 2 | `proptest_filter_parity` | Filter (where_expr) |
| 3 | `proptest_map_parity` | Map/Derive |
| 4 | `proptest_agg_count_parity` | Agg — Count |
| 5 | `proptest_agg_sum_parity` | Agg — Sum |
| 6 | `proptest_agg_hll_parity` | Agg — DistinctCount HLL |
| 7 | `proptest_join_parity` | Join (co-located key) |
| 8 | `proptest_fork_parity` | Fork/replica (N=1→N=8) |

### Task 2 (RED + GREEN): CI gates

`.github/workflows/bench-nightly.yml` — added job `sharding-parity-proptest`:
- `PROPTEST_CASES=10000 cargo test -p beava --test sharding_parity`
- `timeout-minutes: 10`
- Runs nightly at 02:00 UTC alongside the existing shard_scaffold criterion bench

`.github/workflows/pr.yml` — added job `sharding-parity-smoke`:
- `PROPTEST_CASES=50 cargo test -p beava --test sharding_parity`
- `timeout-minutes: 5`
- Note: must be added to required status checks in GitHub branch protection settings

## Test Results

```
PROPTEST_CASES=10 cargo test --release --test sharding_parity -- --nocapture

test proptests::sharding_parity::test_generator_determinism ... ok
test proptests::sharding_parity::proptest_filter_parity ... ok
test proptests::sharding_parity::proptest_map_parity ... ok
test proptests::sharding_parity::proptest_agg_count_parity ... ok
test proptests::sharding_parity::proptest_agg_sum_parity ... ok
test proptests::sharding_parity::proptest_agg_hll_parity ... ok
test proptests::sharding_parity::proptest_join_parity ... ok
test proptests::sharding_parity::proptest_fork_parity ... ok
test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.08s

cargo test --release (full suite): 881 lib tests ok
```

Pre-existing macOS network bind failures in `test_replica_subscribe` (OS error 49) are
environment-only, confirmed pre-existing before this plan.

## Deviations from Plan

None — plan executed exactly as written.

The one observation: `run_batch` and `run_batch_fork` both use the same underlying
hash function (`ahash` on the key string) so parity holds with 0% divergence in
practice. The 2% HLL tolerance exists as a documented safety margin per T-52-07-02
but is never triggered on the default seed set.

## Known Stubs

None — the harness is complete and wired. The one manual step (adding
`sharding-parity-smoke` to GitHub branch protection required status checks) is
documented in the pr.yml job comment and is a GitHub UI operation outside code scope.

## Threat Flags

None — all security surfaces were in the plan's threat_model:
- T-52-07-01: CI timeout — mitigated by timeout-minutes: 10 (nightly) and 5 (PR) ✓
- T-52-07-02: HLL sketch variance — mitigated by 2% relative tolerance in assert_parity ✓
- T-52-07-03: Non-reproducible failure — mitigated by same-seed determinism test (Test 1) ✓

## Self-Check: PASSED

Files verified:
- `tests/sharding_parity.rs`: exists, declares `mod proptests;` ✓
- `tests/proptests/mod.rs`: exists, declares `pub mod sharding_parity;` ✓
- `tests/proptests/sharding_parity.rs`: exists, 8 tests ✓
- `.github/workflows/bench-nightly.yml`: contains `sharding-parity-proptest` ✓
- `.github/workflows/pr.yml`: contains `sharding-parity-smoke` ✓
- Commits: 6101776 (harness), 5ee397c (CI gates) ✓
