---
phase: 4
fixed_at: 2026-04-23T00:00:00Z
review_path: .planning/phases/04-stateless-ops-expression-evaluator/04-REVIEW.md
iteration: 1
findings_in_scope: 8
fixed: 8
skipped: 0
status: all_fixed
---

# Phase 4: Code Review Fix Report

**Fixed at:** 2026-04-23
**Source review:** `.planning/phases/04-stateless-ops-expression-evaluator/04-REVIEW.md`
**Iteration:** 1

**Summary:**
- Findings in scope: 8 (CR-01, CR-02, WR-01 through WR-06)
- Fixed: 8
- Skipped: 0

## Fixed Issues

### CR-01: Missing recursion depth guard in eval()

**Files modified:** `crates/beava-core/src/eval.rs`
**Commit:** 2132d63
**Applied fix:** Added `const MAX_EVAL_DEPTH: usize = 512`. Public `eval()` now delegates to `eval_depth(..., 0)`. New `eval_depth` checks `depth > MAX_EVAL_DEPTH` and returns `Value::Null` immediately. Threaded `depth + 1` through all recursive call sites including `eval_binop` (which received a new `depth` parameter). Added test `eval_exceeds_max_depth_returns_null` building a 600-deep BinOp chain and asserting `Value::Null` is returned.

### CR-02: Division type inference bug — I64/I64 widens to F64 contrary to runtime

**Files modified:** `crates/beava-core/src/schema_propagate.rs`
**Commit:** f3dd943
**Applied fix:** Updated `infer_arithmetic_type` for `/` to return `Known(I64)` when both operands are I64 (matching the runtime `arith_div` truncating-integer behavior). Updated `resolve_null_arithmetic` for `/` to handle both `Known(I64)` and `Known(F64)` branches consistently. Updated the `infer_expr_type_arithmetic_promotion` test to assert `Known(I64)` for I64/I64 (with updated comment explaining the v1 decision) and added an F64/I64 → F64 case to confirm promotion still works when one operand is floating-point.

### WR-01: compiled_chains populated outside derivation uniqueness guard — stale entries possible

**Files modified:** `crates/beava-core/src/registry.rs`
**Commit:** 854c7a1
**Applied fix:** Moved the `compiled_chains` insertion inside the `!w.derivations.contains_key(&d.name)` branch so chains are only inserted when the corresponding descriptor is actually inserted. Collected pre-compiled chains into a `HashMap` then used `chains_map.remove(&d.name)` at the guarded insertion site; removed the old unconditional post-loop that inserted all chains regardless of deduplication.

### WR-02: OpChain::compile always reports op_index: 0 in errors

**Files modified:** `crates/beava-core/src/op_chain.rs`
**Commit:** aea5e5c
**Applied fix:** Changed `for op in ops` to `for (op_loop_idx, op) in ops.iter().enumerate()`. Both the `Filter` and `WithColumns`/`Map` error paths now emit `op_index: op_loop_idx` instead of the hardcoded `op_index: 0`.

### WR-03: Pass B null-equality rewrite misses the != operator

**Files modified:** `crates/beava-core/src/expr.rs`
**Commit:** 863efd8
**Applied fix:** Extended `rewrite_null_eq` in Pass B to handle `op == "!="`: `(x != null)` rewrites to `UnaryOp("not", Call("isnull", [x]))` and `(null != x)` rewrites symmetrically. Updated the module-level doc comment to reflect that `!=` is now rewritten. Updated test 27 (`parse_not_equal_null_not_rewritten` → `parse_not_equal_null_rewrites_to_not_isnull`) to assert the correct rewritten form, and added test 27b (`parse_null_not_equal_rewrites_to_not_isnull_commutative`) for the commutative case.

### WR-04: serde_json::to_value(...).unwrap() in register handler can panic

**Files modified:** `crates/beava-server/src/register.rs`
**Commit:** 3eec1c2
**Applied fix:** Added `fn to_json_value<T: serde::Serialize>(v: T) -> serde_json::Value` helper that calls `serde_json::to_value` and falls back to a structured error JSON on failure (logging a `tracing::error!` with `kind = "register.serialization_error"`). Replaced all 7 occurrences of `.unwrap()` on `to_value` calls with `to_json_value(...)`. Added `// infallible: ...` comments at each call site explaining why serialization is expected to succeed for that specific type.

### WR-05: Python reference eval missing test coverage for Inf/NaN cast and 0.0/0.0 division

**Files modified:** `python/tests/test_eval_reference.py`
**Commit:** 84bbfc7
**Applied fix:** Extended `test_cast_to_int` with three assertions that `cast(float("inf"), "int")`, `cast(float("-inf"), "int")`, and `cast(float("nan"), "int")` all return `None`. Added new test `test_zero_float_div_by_zero_is_nan` that evaluates `0.0 / 0.0` and asserts the result is a float `NaN` (using `math.isnan`).

### WR-06: Hypothesis SC4 proptest has no skip-rate guard — over-permissive generator masked

**Files modified:** `python/tests/test_phase4_smoke.py`
**Commit:** fab0c02
**Applied fix:** Added module-level `_sc4_skip_counter: dict[str, int]` and `_sc4_skip_lock = threading.Lock()` after `pytestmark`. Updated `test_sc4_proptest_client_server_eval_equivalence` to: increment `total` on every case, increment `skips` when the server returns non-200 at register time, call `hypothesis.note()` with current skip stats, and assert `skip_rate <= 0.50` once `total >= 10`. This ensures hypothesis cannot silently run 256 cases that all skip server-side validation, providing a coverage floor for the correctness claim.

---

_Fixed: 2026-04-23_
_Fixer: Claude (gsd-code-fixer)_
_Iteration: 1_
