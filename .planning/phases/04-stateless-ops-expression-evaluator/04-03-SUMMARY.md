---
phase: 04-stateless-ops-expression-evaluator
plan: "03"
subsystem: evaluator
tags: [evaluator, expression, null-propagation, builtins, cast, isnull, beava-core, proptest, tdd]

requires:
  - phase: 04-01
    provides: "Value enum + Row + and/or/not_three_valued helpers"
  - phase: 04-02
    provides: "Expr AST + parse() + Pass B null-eq rewrite (BinOp(==,_,Null)→Call(isnull,[_]))"

provides:
  - "pub fn eval(expr: &Expr, row: &Row) -> Value — single deterministic evaluator entry point"
  - "pub const BUILTINS: &[BuiltinFn] — SRV-APPLY-06 table-driven extension hook"
  - "pub fn lookup_builtin(name: &str) -> Option<&'static BuiltinFn>"
  - "pub enum Arity { Fixed(usize), Variadic }"
  - "cast_eval: full conversion matrix (str/int/float/bool; Null+Bytes→Null)"
  - "isnull_eval: always Bool(true/false), never Null"

affects:
  - 04-04-op-executor (calls eval() for Filter/WithColumns/Map expressions)
  - 04-05-register-integration (calls eval() via op chain at push time)
  - 05-aggregation (BUILTINS table extended for bv.count(where=...) filter exprs)

tech-stack:
  added: []
  patterns:
    - "Recursive match on Expr enum — pure function, no shared state"
    - "BinOp and/or: short-circuit then delegate to Value::*_three_valued (no truth-table duplication)"
    - "Arithmetic: inline two-level match per op (I64×I64, F64×F64, mixed) — avoids confusing shared helper"
    - "cmp_op/cmp_eq/cmp_ne: partial_cmp returns None for NaN→Bool(false); cross-type→Null"
    - "BUILTINS: &[BuiltinFn] static slice; lookup_builtin linear scan (2 items)"

key-files:
  created:
    - crates/beava-core/src/eval.rs
    - crates/beava-core/src/expr_builtins.rs
  modified:
    - crates/beava-core/src/lib.rs

key-decisions:
  - "Rely on Plan 04-02's Pass B for (x == null) → isnull(x): eval.rs BinOp(==) is strict-null (no null-equality special case); the contract is verified by test 20 (eval_equals_null_literal_via_parser_rewrite)"
  - "I64 / I64 → I64 (truncating, not widening): avoids silent i64→f64 widening on integer division; users who want float division must cast first"
  - "i64 overflow: saturating (i64::saturating_add/sub/mul) — MAX+1=MAX, MIN-1=MIN; no panic"
  - "f64 / 0.0 → F64(+Inf) per IEEE-754 (not Null); I64 / I64(0) → Null"
  - "NaN comparisons return Bool(false): partial_cmp returns None for NaN; the None→Bool(false) branch applies only when both operands are F64 (or I64/F64 mixed); cross-type None→Null"
  - "BareIdent → Value::Str at eval time: enables cast(x, float) to deliver Value::Str('float') to cast_eval matching the builtin contract"
  - "cast Bytes → always Null: no implicit bytes-to-str without an encoding spec"
  - "BUILTINS linear scan (O(n) for n=2): no HashMap premature optimization; Phase 5+ appends to the slice"

requirements-completed:
  - SRV-APPLY-06

duration: 35min
completed: 2026-04-23
---

# Phase 4 Plan 03: Expression Evaluator + cast/isnull Builtins Summary

**Recursive `Expr × Row → Value` evaluator with SQL three-valued null propagation, i64 saturating arithmetic, IEEE-754 f64, table-driven `cast`/`isnull` builtins, and a contract test proving the Plan 04-02 parser rewrite is load-bearing — the closed loop between `bv.col(...)` SDK expressions and server-side event filtering**

## Performance

- **Duration:** ~35 min
- **Started:** 2026-04-23T12:33Z
- **Completed:** 2026-04-23T13:08Z
- **Tasks:** 2 (1.a red, 1.b green)
- **Files modified:** 3

## Accomplishments

- `pub fn eval(expr: &Expr, row: &Row) -> Value` — recursive, pure, deterministic evaluator covering all Expr variants: Field (miss→Null), Literal (Null/Bool/Int/Float/Str/BareIdent), UnaryOp (not via `not_three_valued`), BinOp (and/or short-circuit + arithmetic + comparison), Call (BUILTINS dispatch)
- `BareIdent` → `Value::Str` conversion so `cast(x, float)` delivers `Value::Str("float")` to `cast_eval` — matching the builtin contract established in 04-02's Pass A normalization
- `i64` arithmetic saturates (`i64::saturating_add/sub/mul`); integer divide-by-zero → Null; `I64/I64` → `I64` (truncating, no silent widening)
- `f64` follows IEEE-754: `1.0/0.0` → `F64(+Inf)` (not Null); NaN comparisons → `Bool(false)` via `partial_cmp` returning None
- Null propagation: `and`/`or` short-circuit first, then delegate to `Value::and/or_three_valued`; arithmetic and comparison check null before dispatch — no truth-table logic duplicated
- `BUILTINS` table with `cast` and `isnull` entries; `lookup_builtin` linear scan; `cast_eval` full conversion matrix; `isnull_eval` always returns `Bool`
- `proptest_determinism` — 256 cases, depth ≤ 3, all pass: same `(Expr, Row)` → same `Value` across two calls and a row clone
- TDD red-green commit chain: `test(04-03)` stub commit (43 FAILED) → `feat(04-03)` green commit (43+229 PASSED, 321 workspace tests zero regressions)
- beava-core stays syscall-free throughout

## The Plan 04-02 Parser-Rewrite Contract

This is the most load-bearing cross-plan coupling in Phase 4.

**What Plan 04-02 does:** `rewrite_null_eq()` in `expr.rs` transforms `BinOp("==", e, Literal::Null)` (and the commutative form) into `Call("isnull", [e])` as a post-parse pass. By the time an AST reaches `eval.rs`, no `BinOp("==")` node has a `Null` literal operand.

**What eval.rs relies on:** `BinOp("==")` in `eval_binop` is strict-null: if either evaluated operand is `Value::Null`, it returns `Value::Null` (per CONTEXT.md §D-04). There is NO null-equality special case in `eval.rs`.

**The contract test (Test 20):** `eval_equals_null_literal_via_parser_rewrite` does three things:
1. Calls `parse("(amount == null)")` and asserts the result is `Call("isnull", ...)` — proving Pass B ran.
2. Evaluates against a row where `amount` is `Value::Null` → asserts `Bool(true)`.
3. Evaluates against a row where `amount` is `Value::I64(5)` → asserts `Bool(false)`.

If Plan 04-02's `rewrite_null_eq` regresses (returns `BinOp("==")` instead of `Call("isnull", ...)`), step 2 fails with `"got Null, expected Bool(true)"` — because strict-null `BinOp("==")` returns `Null` when either operand is `Null`. The failure message is diagnostic: the fix belongs in `expr.rs`, not `eval.rs`.

## i64 Overflow and Division Policy

| Operation | Behavior | Rationale |
|-----------|----------|-----------|
| `I64 + I64` overflow | Saturate at `i64::MAX` | No panic; predictable ceiling |
| `I64 - I64` underflow | Saturate at `i64::MIN` | No panic; predictable floor |
| `I64 * I64` overflow | Saturate | Consistent with add/sub |
| `I64 / I64(0)` | `Value::Null` | Division by zero is undefined |
| `I64 / I64` (non-zero) | Truncate toward zero | Rust `/` operator default |
| `I64 + F64` | Promote to `F64` | Mixed arithmetic widens |
| `I64 / I64` (not mixed) | Stay `I64` | No silent widening |
| `F64 / F64(0.0)` | `F64(+Inf)` | IEEE-754 — not an error |
| `F64` NaN arithmetic | Propagates NaN | IEEE-754 |
| `F64` NaN comparison | `Bool(false)` | IEEE-754 total order |

## Cast Conversion Matrix

| Source | "str" | "int" | "float" | "bool" |
|--------|-------|-------|---------|--------|
| Null | Null | Null | Null | Null |
| Str | unchanged | parse or Null | parse or Null | "true"→T, "false"→F, else Null |
| I64 | fmt | unchanged | `as f64` | `≠0` → true |
| F64 | fmt | `as i64` (trunc toward 0) | unchanged | `≠0.0 && !NaN` → true |
| Bool | "true"/"false" | 1/0 | 1.0/0.0 | unchanged |
| Bytes | Null | Null | Null | Null |
| Datetime | ms.to_string() | I64(ms) | F64(ms as f64) | `ms≠0` → true |

**Note:** `Bytes → always Null` — no implicit bytes-to-str without an encoding spec.
**Note:** F64 → I64 truncates toward zero (`as i64`), not rounds. `3.9 → 3`, `-3.9 → -3`.

## Interfaces Exported to 04-04

```rust
// Single evaluator entry point
pub fn eval(expr: &Expr, row: &Row) -> Value;

// Builtin registry (extension hook for Phase 5+)
pub enum Arity { Fixed(usize), Variadic }
pub struct BuiltinFn { pub name: &'static str, pub arity: Arity, pub eval: fn(&[Value]) -> Value }
pub const BUILTINS: &[BuiltinFn];
pub fn lookup_builtin(name: &str) -> Option<&'static BuiltinFn>;
```

Plan 04-04 (op executor) calls `eval()` for every `Filter`/`WithColumns`/`Map` op step. Filter: if `eval()` returns anything other than `Bool(true)`, the row is dropped (including `Null` — per §D-04, null predicate = row dropped). WithColumns/Map: the returned `Value` is inserted into the output row under the derived column name.

## Task Commits

1. **Task 1.a (red): 43 failing evaluator + builtins tests** — `8c4ebf1` (test)
2. **Task 1.b (green): Implement evaluator + builtins** — `6029d6a` (feat)

## Files Created/Modified

- `/Users/petrpan26/work/tally/crates/beava-core/src/eval.rs` — New: `eval()` + helper functions + 22 unit tests + proptest (43 tests total, ~440 LoC)
- `/Users/petrpan26/work/tally/crates/beava-core/src/expr_builtins.rs` — New: `BUILTINS` table, `lookup_builtin`, `cast_eval`, `isnull_eval`, 13 unit tests (~275 LoC)
- `/Users/petrpan26/work/tally/crates/beava-core/src/lib.rs` — Added `pub mod eval;` and `pub mod expr_builtins;`

## Deviations from Plan

**1. [Rule 1 - Refactor] Removed unused `promote_numeric` helper skeleton**
- **Found during:** Task 1.b implementation
- **Issue:** The plan mentioned a `promote_numeric` helper but the inline two-level match per arithmetic function was cleaner and self-contained. A `promote_numeric` stub left in the file caused a `dead_code` clippy error under `-D warnings`.
- **Fix:** Removed the unused helper; arithmetic is handled inline in `arith_add/sub/mul/div`.
- **Files modified:** `eval.rs`
- **Commit:** `6029d6a`

## Known Stubs

None. All public methods are fully implemented. The cast conversion matrix covers all Value variants for all four supported target types. The evaluator handles all Expr variants without stubs or panics.

## Threat Flags

None. `eval.rs` and `expr_builtins.rs` are pure in-memory computation with no network, file system, or trust-boundary surface. The evaluator does not execute arbitrary code — it evaluates a typed AST against a row of values.

## Self-Check

### Files exist
- `crates/beava-core/src/eval.rs` — FOUND
- `crates/beava-core/src/expr_builtins.rs` — FOUND
- `crates/beava-core/src/lib.rs` — FOUND (contains `pub mod eval;` and `pub mod expr_builtins;`)

### Commits exist
- `8c4ebf1` — FOUND (`test(04-03): add failing evaluator + builtins tests + determinism proptest`)
- `6029d6a` — FOUND (`feat(04-03): implement expression evaluator + cast/isnull builtins`)

### Gate results
- `cargo test -p beava-core` — 229/229 PASSED (43 new + 186 pre-existing)
- `cargo test --workspace` — 321 tests PASSED, 0 failed
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` — CLEAN
- `cargo fmt --all --check` — CLEAN
- `proptest_determinism` — 256 cases, 0 shrinks, PASSED

## Self-Check: PASSED

## Next Phase Readiness

- Plan 04-04 (op executor): can import `beava_core::eval::eval` and call it for Filter/WithColumns/Map ops; null-predicate → row drop is Plan 04-04's responsibility
- Plan 04-05 (register integration): can validate expressions by parsing + type-checking at register time; the evaluator runtime will handle nulls/overflow
- Phase 5 (aggregation): extends `BUILTINS` by appending to the slice — no grammar or evaluator changes needed (SRV-APPLY-06 extension hook verified)

---
*Phase: 04-stateless-ops-expression-evaluator*
*Completed: 2026-04-23*
