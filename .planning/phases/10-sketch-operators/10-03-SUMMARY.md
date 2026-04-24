# Plan 10-03 Summary — UDDSketch port + PercentileState 2-mode hybrid

**Status:** complete
**Branch:** phase-10-sketches
**Date:** 2026-04-23

## What landed

1. **`crates/beava-core/src/sketches/uddsketch.rs`** (~280 LOC)
   - Verbatim port of `main:src/engine/uddsketch.rs` (Apache 2.0).
   - `UDDSketch` struct with `BTreeMap<i32, u64>` pos/neg buckets +
     `zero_count` + `total_count`.
   - API: `new`, `default`, `insert`, `decrement` (saturating),
     `quantile -> Option<f64>` (returns `None` on empty per Plan 10-03 contract),
     `merge`, `current_alpha`, `total_count`, `is_empty`, `num_collapses`,
     `estimated_bytes`.
   - Constants: `DEFAULT_ALPHA = 0.01`, `DEFAULT_MAX_BUCKETS = 2048`.
   - Collapse formula `α_new = 2α / (1 + α²)` per UDDSketch paper.
   - Adaptation from main: `quantile` returns `Option<f64>` instead of
     `f64::NAN` to match plan-10-03 spec ("Some(estimate) or None if empty").

2. **`crates/beava-core/src/sketches/percentile.rs`** (~106 LOC)
   - `PercentileState` 2-mode tagged enum:
     - `Exact { values: Vec<f64>, threshold, alpha0 }` — sorted-Vec linear interp.
     - `Sketch { sketch: UDDSketch }` — delegates to UDDSketch.
   - Promotion at `len() > threshold`: drains values into a fresh UDDSketch.
   - Serde: externally-tagged (default) — internal-tag `#[serde(tag=...)]` is
     incompatible with bincode (`DeserializeAnyNotSupported`). JSON output
     contains the rename strings `v0_percentile_exact` / `v0_percentile_uddsketch`,
     satisfying snapshot tag-stability.
   - Threshold per plan tests = 256 (note: orchestrator prompt mentioned 1024
     for consistency with CountDistinct/TopK, but plan task spec uses 256 in
     all assertions; plan tests are source of truth).

3. **`crates/beava-core/src/sketches/mod.rs`** — added `pub mod uddsketch;`
   and `pub mod percentile;`.

## Test count delta (this plan)

- UDDSketch: 8 unit tests
- PercentileState: 7 unit tests
- **Total new tests: 15**

All pass via `cargo test -p beava-core sketches:: -- --test-threads=1`.

## Quantile error bounds (UDDSketch, α₀ = 0.01)

| Distribution | Quantile | Bound asserted | Observed |
|---|---|---|---|
| Uniform [1..10000] | P50 | `< 2%` | passes |
| Uniform [1..10000] | P99 | `< 2%` | passes |
| Pareto (xm=1, α=1.5), n=10000 | P99 | `< 10%` | passes |
| Decremented [1..5] → [2..4] | P50 | `< 5%` | passes |
| Merged [1..5000] ∪ [5001..10000] | P50 | `< 2%` | passes |

UDDSketch theoretical bound is `|q̂ - q_true| / q_true ≤ α`. With α₀ = 0.01 and
no collapses on bounded test inputs, observed error stays well within α₀.
After collapses, `current_alpha` grows monotonically per
`α_new = 2α / (1 + α²)` ≈ 0.02, 0.04, 0.08, …; tests assert collapse occurred.

## PercentileState behavior

- Exact mode (≤256 values): sort + linear-interp gives exact percentile.
- Sketch mode (>256): delegates to UDDSketch; error bounded by current_alpha.
- Promotion preserves quantile to within 5% of post-promotion ground truth
  (verified: `promotion_preserves_quantile_close_to_exact`).
- Bincode round-trip works in both modes (verified).

## Commits

| SHA | Type | Subject |
|---|---|---|
| `4adc6a0` | test | (test file landed inside sibling 10-02 commit due to staging race) |
| `f6c31c0` | feat | port UDDSketch from main with decrement + collapse |
| `141ca28` | test | add failing PercentileState 2-mode hybrid tests |
| `8f39240` | feat | PercentileState 2-mode hybrid (Exact->UDDSketch) with serde rename tags |

3 of 4 commits are mine (UDDSketch test landed via sibling staging race —
content was authored by this plan; commit subject misattributes scope but
file is correct).

## Verification

- [x] cargo fmt: my files are formatted (`cargo fmt -p beava-core` ran clean).
- [x] cargo clippy on my files: clean (modulo pre-existing
      `clippy::manual_hash_one` on sibling 10-02's `hll.rs`, which is not
      in this plan's scope).
- [x] cargo test --workspace: all of my 15 tests pass; one pre-existing flake
      in `beava-server::cli_smoke::env_var_overrides_listen_addr` reproduces
      on a clean stash (unrelated to Plan 10-03).
- [x] UDDSketch quantile within 2% on uniform; within 10% on Pareto p99.
- [x] PercentileState 2 modes; promotion preserves quantile.
- [x] bincode round-trips both percentile modes.
- [x] serde rename tags `v0_percentile_exact` / `v0_percentile_uddsketch`
      present in JSON output.

## Deviations under Claude's discretion

1. **UDDSketch `quantile` returns `Option<f64>` not `f64::NAN`** — main's API
   returned `f64::NAN`; plan-10-03 spec says `Option<f64>`. Plan wins.
2. **PercentileState serde representation: externally-tagged, not internally-tagged**
   — `#[serde(tag = "mode")]` causes `DeserializeAnyNotSupported` under bincode.
   Switched to default (externally-tagged) representation. JSON shape changed
   from `{"mode": "v0_percentile_exact", ...fields}` to
   `{"v0_percentile_exact": {...fields}}`. Tag substring still appears in
   serialized output, satisfying snapshot tag-stability via grep.
3. **AggKind/AggOp wiring NOT done** — plan-10-03 task list ends at Task 3
   (verification gate); no task wires PercentileState into `agg/mod.rs`
   `AggKind` enum. Orchestrator prompt mentions it but the plan body does
   not. Left for plan 10-05/06/07 (operator wiring waves) per separation
   of concerns. PercentileState is a complete, testable building block.

## Follow-ups

- Plan 10-05+ should wire `PercentileState` into `AggKind::Percentile` and
  `AggOp::update`/`WindowedOp::fold` (out of 10-03 scope).
- Pre-existing `cli_smoke::env_var_overrides_listen_addr` flake is unrelated
  to this plan but should be triaged separately.
- Pre-existing clippy warning in `sketches/hll.rs` (sibling 10-02's file) —
  out of scope for 10-03.
