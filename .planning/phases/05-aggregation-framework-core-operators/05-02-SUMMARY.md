---
phase: 05-aggregation-framework-core-operators
plan: "02"
subsystem: beava-core
tags: [aggregation, where-predicate, three-valued-null, eval, tdd, sdk-agg-04]
dependency_graph:
  requires:
    - AggOp enum (05-01)
    - eval::eval + expr::parse (Phase 4)
    - Row/Value three-valued null semantics (Phase 4)
  provides:
    - evaluate_where_predicate(expr, row) -> bool (agg_where.rs)
    - AggOp::update_with_row threaded predicate apply entry point
    - WindowedOp::update_with_row threaded predicate for bucketed ops
    - AggOpDescriptor.where_expr: Option<Arc<Expr>>
  affects:
    - crates/beava-core/src/agg_where.rs (filled from placeholder)
    - crates/beava-core/src/agg_op.rs (new field + method)
    - crates/beava-core/src/agg_windowed.rs (new method)
tech_stack:
  added: []
  patterns:
    - Three-valued null drop (Bool(true) only → update; Null/false/mismatch → skip)
    - Delegate to eval::eval for bounded-depth expression evaluation
    - Ratio "gate numerator only" via existing RatioState::update(where_matched)
    - Arc<Expr> for zero-copy predicate sharing across entities
key_files:
  created: []
  modified:
    - crates/beava-core/src/agg_where.rs
    - crates/beava-core/src/agg_op.rs
    - crates/beava-core/src/agg_windowed.rs
decisions:
  - "Used eval::eval (not eval_with_depth) — eval is the public API; it already delegates to bounded-depth eval_depth internally. No new entry point needed."
  - "Ratio semantics delegated to RatioState::update(where_matched) rather than reimplementing inline — RatioState already has 'gate numerator, always increment total' logic from 05-01."
  - "AggOp::update_with_row Windowed arm delegates to WindowedOp::update_with_row passing the Arc<Expr> reference to avoid re-evaluating at the outer level — each bucket's inner AggOp evaluates the predicate via update_with_row."
metrics:
  duration_seconds: 420
  completed_date: "2026-04-23"
  tasks_completed: 2
  files_created: 0
  files_modified: 3
---

# Phase 5 Plan 02: where= Predicate Threading Through Apply Path Summary

Three-valued null predicate gate for aggregations: `evaluate_where_predicate` wraps `eval::eval` with `Bool(true) → update, everything else → skip`; threaded into `AggOp::update_with_row` and `WindowedOp::update_with_row` (SDK-AGG-04).

## What Was Built

### Task 1.a (red) — Failing tests for predicate threading

**`crates/beava-core/src/agg_where.rs`** — placeholder overwritten with:
- `evaluate_where_predicate` stub (`todo!`)
- 5 tests covering all three-valued null cases

**`crates/beava-core/src/agg_op.rs`** — extended with:
- `AggOpDescriptor.where_expr: Option<Arc<Expr>>` field
- `AggOp::update_with_row` stub (`todo!`)
- 6 tests: count true/false, sum skip, ratio gate-numerator, windowed, none-regression

**`crates/beava-core/src/agg_windowed.rs`** — extended with:
- `WindowedOp::update_with_row` stub (`todo!`)
- 1 test: windowed count with predicate drops non-matching rows

### Task 1.b (green) — Implementation

**`evaluate_where_predicate`** (agg_where.rs):
```rust
pub fn evaluate_where_predicate(expr: &Expr, row: &Row) -> bool {
    matches!(eval(expr, row), Value::Bool(true))
}
```
One-liner. Delegates to `eval::eval` which enforces 512-level recursion depth (T-05-02-01). The `Null → false` mapping is the T-05-02-02 guard — any future refactor that accidentally returns `true` for Null will be caught by `where_null_returns_false`.

**`AggOp::update_with_row`** (agg_op.rs):
- Evaluates predicate (or `true` if `where_expr=None`)
- Windowed arm delegates to `WindowedOp::update_with_row` passing the `Arc<Expr>` ref
- All other arms (including Ratio) call `self.update(row, t, field, where_matched)`
- `RatioState::update(where_matched=false)` increments `total` but not `matching` — this is the D-03 "gate numerator only" semantic, already implemented in 05-01

**`WindowedOp::update_with_row`** (agg_windowed.rs):
- Identical bucket routing + stale-reset logic as `update`
- Calls `bucket.update_with_row(row, t, field, where_expr)` on the inner `AggOp`

## Tests Added

| Module | Test name | Validates |
|---|---|---|
| agg_where | `where_bool_true_returns_true` | Bool(true) → passes |
| agg_where | `where_bool_false_returns_false` | Bool(false) → drops |
| agg_where | `where_null_returns_false` | T-05-02-02: Null → drops (not true) |
| agg_where | `where_type_mismatch_returns_false` | Str on numeric expr → Null → drops |
| agg_where | `where_missing_field_returns_false` | Missing field → Null → drops |
| agg_op | `count_with_where_true_increments` | 5 rows, 3 match → Count(3) |
| agg_op | `count_with_where_false_does_not_increment` | 5 rows, 0 match → Count(0) |
| agg_op | `sum_with_where_skips_non_matching` | [10,20,30,40,50] > 25 → sum=120 |
| agg_op | `ratio_with_where_gates_numerator_only` | 3/10 ok → ratio=0.3 (D-03) |
| agg_op | `update_with_none_where_always_updates` | None → all 5 rows counted (regression) |
| agg_windowed | `windowed_count_with_where_predicate_drops_non_matching` | 5 rows, 3 match across buckets → I64(3) |

**11 new tests** (334 beava-core + 95 beava-server = 429 workspace tests all passing).

## Deviations from Plan

### Auto-fixed Issues

None — plan executed exactly as written, with one clarification:

**Observation: eval_with_depth is not a public API.** The plan's interface spec referenced `eval::eval_with_depth` and `DEFAULT_EVAL_DEPTH`. The actual public function is `eval::eval` (which internally delegates to `eval_depth` with `MAX_EVAL_DEPTH=512`). Used `eval::eval` directly. No code impact — same bounded-depth guarantees apply.

## Known Stubs

None. All three modified files are fully implemented. No placeholder text flows to query output.

## Threat Flags

None. Pure in-memory apply-path change. No network endpoints, auth paths, or schema changes at trust boundaries introduced.

## Self-Check: PASSED

Files exist:
- `crates/beava-core/src/agg_where.rs` — FOUND (filled from placeholder)
- `crates/beava-core/src/agg_op.rs` — FOUND (where_expr field + update_with_row)
- `crates/beava-core/src/agg_windowed.rs` — FOUND (update_with_row)

Commits exist:
- `24cc7b7` — test(05-02): add failing tests for where= predicate threading
- `6a13fd5` — feat(05-02): thread where= predicate through apply path (SDK-AGG-04)

lib.rs diff:
- `git diff HEAD~1 HEAD -- crates/beava-core/src/lib.rs` → empty (lib.rs untouched by 05-02)

Gates:
- `cargo test --workspace` — 429 passed, 0 failed
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` — clean
- `cargo fmt --all --check` — clean
- `grep -q 'SDK-AGG-04' crates/beava-core/src/agg_where.rs` — match found (doc comment)
- `grep -c 'evaluate_where_predicate' crates/beava-core/src/agg_op.rs` — 1 match
