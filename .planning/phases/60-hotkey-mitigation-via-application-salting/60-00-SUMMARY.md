---
phase: 60
plan: 00
subsystem: hot-key-salting-scaffold
tags: [scaffold, red-tests, grep-gate, criterion-stub]
dependency_graph:
  requires: [TPC-PERF-09 (Phase 59 closed — 1,494,631 EPS baseline)]
  provides: [red-test-contract, ship-gate-script]
  affects: [tests/, scripts/, benches/]
tech_stack:
  added: []
  patterns: [ignored-tests, grep-zero-ship-gate, criterion-placeholder-group]
key_files:
  created:
    - tests/hot_key_salting.rs
    - tests/salted_stream_register_warning.rs
    - scripts/verify-salt-feature-complete.sh
  modified:
    - benches/pareto_workload.rs
    - .planning/REQUIREMENTS.md (TPC-PERF-10 row already present from planner)
decisions: []
metrics:
  duration_minutes: 8
  completed_date: 2026-04-20
---

# Phase 60 Plan 00: Wave 0 RED Scaffolding Summary

Laid the Wave 0 foundation: 22 ignored RED tests tagged `60-W[1-4]`,
the `scripts/verify-salt-feature-complete.sh` grep-ZERO ship-gate (exits 1
until Wave 4), and a `pareto_salted_c8_x8` Criterion placeholder group.

## Scope

Zero `src/` changes. Everything here is contract-only — every downstream
wave flips `#[ignore = "60-W{N}"]` markers GREEN as it lands.

## What shipped

### `tests/hot_key_salting.rs` (18 ignored tests)

- 3 × `60-W1` parser tests (`parse_salt_suffix_*`).
- 6 × `60-W2` ingest-routing tests (`ingest_salted_stream_*`, `unsalted_stream_has_zero_overhead`, `shard_hint_salted_*`, `derive_storage_key_salted_*`).
- 7 × `60-W3` read-scatter tests (`read_scatter_gathers_across_salts`, `salted_fan_out_metric_increments`, `expand_salt_variants_*`, `combine_salt_variants_*`, `read_same_shard_salt_stays_inline`).
- 2 × `60-W4` metrics/perf tests (`beava_shard_hot_key_owner_ratio_emits`, `salted_aggregate_eps_exceeds_unsalted_by_50pct`).

### `tests/salted_stream_register_warning.rs` (4 ignored tests)

- 3 × `60-W1` register-time tests (`salted_source_table_rejected`, `salted_tuple_at_most_one_element`, `salted_join_emits_warning_not_reject`).
- 1 × `60-W2` (`colon_in_key_rejects_salt_declaration`).

### `scripts/verify-salt-feature-complete.sh`

6-check ship-gate (executable, exits 1 today / 0 after Wave 4):

| # | Target path | Pattern | Min | Wave |
|---|-------------|---------|-----|------|
| 1 | `src/engine/join_validator.rs` | `parse_shard_key_with_salt` | 1 | W1 |
| 2 | `src/engine/pipeline.rs` | `salt_cardinality` | 3 | W1+W2 |
| 3 | `src/routing/shard_hint.rs` | `shard_hint_for_event_salted\|salt_cardinality` | 1 | W2 |
| 4 | `src/server/shard_probe.rs` | `salted_streams` | 1 | W4 |
| 5 | `src/shard/metrics.rs` | 3 new metric names (OR) | 3 | W4 |
| 6 | `benches/pareto_workload.rs` | `pareto_salted_c8_x8` | 1 | W0 stub |

### `benches/pareto_workload.rs`

New `bench_pareto_salted_c8_x8` criterion group with a no-op
`placeholder_wave0` body. Wave 4 replaces the body with a real Zipf-1.0
salted A/B harness that asserts salted aggregate EPS ≥ 1.5× unsalted.

## Verification — all PASSED

| Check | Command | Result |
|-------|---------|--------|
| Bench builds | `cargo build --release --benches` | PASSED |
| Tests compile | `cargo build --tests --release` | PASSED |
| Tests enumerated | `cargo test --release --test hot_key_salting --test salted_stream_register_warning -- --list` | 19 + 4 = 23 tests listed |
| Ship-gate | `bash scripts/verify-salt-feature-complete.sh ; echo $?` | 1 (5/6 checks fail pre-W1) |
| REQUIREMENTS.md | `grep -c TPC-PERF-10 .planning/REQUIREMENTS.md` | present (planner landed row) |

## Deviations from Plan

### Auto-fixed issues

**1. [Rule 1 - Bug] Format-string brace escape in `salted_fan_out_metric_increments` stub**
- **Found during:** Task 2 `cargo build --tests --release`.
- **Issue:** `unimplemented!("...beava_salt_fanout_reads_total{stream,salt_cardinality}...")` triggered Rust format-parser error `python's numeric grouping`, not supported.
- **Fix:** Escaped braces to `{{stream,salt_cardinality}}`.
- **Files modified:** tests/hot_key_salting.rs
- **Commit:** included in 8eaaaa4.

**2. [Rule 1 - Bug] Shell arithmetic fallback in `verify-salt-feature-complete.sh`**
- **Found during:** first manual run of the script.
- **Issue:** `count=$(grep -c ... || echo 0)` produced a two-line value (`grep` prints `0` + `echo 0`) that broke `(( count >= min ))` arithmetic.
- **Fix:** Switched to `count=$(grep -c ...) ; count="${count:-0}"`.
- **Files modified:** scripts/verify-salt-feature-complete.sh
- **Commit:** included in 8eaaaa4.

## Self-Check: PASSED

- `tests/hot_key_salting.rs` — FOUND.
- `tests/salted_stream_register_warning.rs` — FOUND.
- `scripts/verify-salt-feature-complete.sh` — FOUND (executable, exit 1).
- `benches/pareto_workload.rs` grep `pareto_salted_c8_x8` — FOUND (5 matches).
- commit `8eaaaa4` — FOUND (`test(60-W0): RED scaffolding + grep-gate + bench stub for TPC-PERF-10`).
