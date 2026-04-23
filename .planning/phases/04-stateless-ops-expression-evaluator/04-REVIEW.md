---
phase: 04-stateless-ops-expression-evaluator
reviewed: 2026-04-23T00:00:00Z
depth: standard
files_reviewed: 18
files_reviewed_list:
  - crates/beava-core/src/row.rs
  - crates/beava-core/src/expr.rs
  - crates/beava-core/src/expr_builtins.rs
  - crates/beava-core/src/eval.rs
  - crates/beava-core/src/op_chain.rs
  - crates/beava-core/src/schema_propagate.rs
  - crates/beava-core/src/register_validate.rs
  - crates/beava-core/src/registry.rs
  - crates/beava-core/src/lib.rs
  - crates/beava-server/src/register.rs
  - crates/beava-server/src/registry_debug.rs
  - crates/beava-server/src/tcp.rs
  - python/beava/_eval_reference.py
  - python/beava/_events.py
  - python/beava/_tables.py
  - python/tests/test_sdk_ops.py
  - python/tests/test_phase4_smoke.py
  - python/tests/test_eval_reference.py
findings:
  critical: 2
  warning: 6
  info: 4
  total: 12
status: issues_found
---

# Phase 4: Code Review Report

**Reviewed:** 2026-04-23T00:00:00Z
**Depth:** standard
**Files Reviewed:** 18
**Status:** issues_found

## Summary

Phase 4 ships a recursive-descent expression parser, a SQL three-valued-null evaluator, an eight-op `OpChain` executor, schema propagation, registration Rule 10 wiring, and a Python reference evaluator for the SC4 equivalence proptest. The overall architecture is sound: ownership-consuming Row helpers satisfy SDK-OPS-09 no-in-place-mutation, the `(x == null)` → `isnull(x)` rewrite lives in Plan 04-02's parser as required, and HTTP/TCP wire error parity is correctly implemented through the shared `execute_register` core.

Two critical issues warrant attention before production use: (1) the recursive Rust evaluator has no depth guard — a crafted expression or a deeply nested SDK chain will stack-overflow at runtime, and (2) the Python reference evaluator's `_arith_div` produces `NaN` for `0.0 / 0.0` where Rust produces `NaN` via IEEE-754 but through a different code path; the SC4 proptest deliberately excludes `NaN/Inf` floats, so this divergence is never exercised, undermining the equivalence claim. Six warnings cover a `compiled_chains` cache leak on re-register, a stale `op_index: 0` hardcode in OpChain error reporting, a `!= null` semantic ambiguity not caught at register time, a schema-propagation division-type divergence between the type-inferrer and the runtime, unsafe `unwrap` in response serialization paths, and a TDD smell in the SC4 test. Four info items address dead-code suppression, magic numbers, and test coverage gaps.

---

## Critical Issues

### CR-01: No recursion depth limit in `eval()` — stack overflow on deep expressions

**File:** `crates/beava-core/src/eval.rs:47`

**Issue:** `eval()` is a plain recursive function with no depth counter or explicit stack limit. The only guard in the test suite (Test 22) verifies depth=200 does not overflow — but 200 levels of `BinOp` nodes is a modest stack; a deeply nested expression (e.g., from a bug in an SDK loop that wraps the same sub-expression 10,000 times) or a proptest shrink generating a very deep tree will cause an uncontrolled stack overflow in the server process. The parser similarly has unbounded recursion in `parse_expr` → `parse_or` → `parse_and` → … → `parse_atom` (LParen handler re-enters `parse_expr`). This is a crash-level DoS vector for any code path that evaluates user-supplied expressions at runtime (future push-path, Phase 5+ eval).

**Fix:** Add a depth counter threaded through the recursive call. Reject (return `Value::Null` or `Err`) when depth exceeds a compile-time constant (e.g., `MAX_EVAL_DEPTH = 512`). For the parser, track `paren_depth` already stored on `Parser`; additionally cap the total parse recursion depth by passing a counter through the grammar non-terminals and returning `ParseError` when the cap is hit.

```rust
// In eval.rs — replace pub fn eval with:
const MAX_EVAL_DEPTH: usize = 512;

pub fn eval(expr: &Expr, row: &Row) -> Value {
    eval_depth(expr, row, 0)
}

fn eval_depth(expr: &Expr, row: &Row, depth: usize) -> Value {
    if depth > MAX_EVAL_DEPTH {
        return Value::Null; // or return an EvalError in Phase 5
    }
    // ... existing match body, replacing recursive eval() calls with
    //     eval_depth(child, row, depth + 1)
}
```

---

### CR-02: SC4 proptest excludes NaN/Inf, masking a `_arith_div` divergence in the Python reference evaluator

**File:** `python/beava/_eval_reference.py:307-316`

**Issue:** `_arith_div` contains explicit `NaN`-producing branches for `float / 0.0` and mixed `int / 0.0`:

```python
return math.copysign(math.inf, a) if a != 0.0 else float("nan")
```

Rust's `arith_div` for `(Value::F64(x), Value::F64(y))` simply does `Value::F64(x / y)` — for `0.0 / 0.0` Rust yields `f64::NAN` (IEEE-754). For Python `0.0 / 0.0` would raise `ZeroDivisionError`, hence the special-cased `float("nan")`. These two paths agree on `NaN` as output. **However**, the SC4 hypothesis strategy (`_arb_leaf`) uses `st.floats(allow_nan=False, allow_infinity=False)` and the row strategy likewise explicitly excludes `NaN/Inf`. This means the `0.0 / 0.0` case is never generated by the proptest.

More importantly, for `int / 0.0` (mixed `I64 / F64` path in Python): the Python code returns `float("nan")` when `a == 0`, but Rust's `arith_div` for `(Value::I64(0), Value::F64(0.0))` does `Value::F64(0_i64 as f64 / 0.0_f64)` = `Value::F64(f64::NAN)`. That is actually consistent. But for `int(a) / 0.0` with `a != 0`: Python returns `math.copysign(math.inf, float(a))` but Rust returns `Value::F64(a as f64 / 0.0)` which is `±Inf`. Those also match.

The actual divergence is subtler: the SC4 proptest registers a `with_columns(out=<expr>)` derivation on the live server. The server's `schema_propagate` infers the type of a division expression `I64 / I64` as **F64** (see `infer_arithmetic_type`, line 596: "Division always widens to F64"), but the **runtime** `arith_div` for two `Value::I64` operands returns `Value::I64` (integer division, line 182–188 of `eval.rs`). This schema inference vs. runtime type contradiction means the schema propagator lies about the output type of integer division, but the SC4 proptest cannot detect it because `with_columns` output type is not checked against the schema — the proptest only checks that the **value** matches, not the type. The schema mismatch will silently produce wrong types downstream (e.g., a subsequent `Cast { type_map: {"result": "int"} }` will receive an `I64` but the propagated schema says `F64`, which is a legal cast, hiding the discrepancy).

**Fix:** Align the schema inferrer with the runtime. Change `infer_arithmetic_type` for `/` with two `I64` operands to return `Known(FieldType::I64)` (matching the runtime), **or** change the runtime to widen `I64 / I64` to `F64`. Pick one and document the v1 decision in both places consistently. The proptest should also add a case that verifies the type of the output field matches the propagated schema.

---

## Warnings

### WR-01: `compiled_chains` cache leaks on re-register of a derivation with different ops

**File:** `crates/beava-core/src/registry.rs:251-254`

**Issue:** `apply_registration` unconditionally inserts into `compiled_chains`:

```rust
for (name, chain) in compiled_chains {
    w.compiled_chains.insert(name, chain);
}
```

But derivation descriptors themselves use a `contains_key` guard (line 237) — a derivation that already exists is skipped. If a derivation is somehow re-registered (e.g., via a future admin endpoint or a schema migration tool), the stale chain from the first registration remains in `compiled_chains` while the descriptor in `w.derivations` stays at the old version. The `compiled_chain()` accessor will serve the stale chain. Currently this cannot be triggered through `POST /register` (Rule 9's diff engine prevents re-registration of changed descriptors), but `install_descriptors` — which lacks this guard — writes directly to `w.derivations` without touching `compiled_chains`. A call to `install_descriptors` with a derivation that has ops will leave `compiled_chains` empty for that derivation while the descriptor is present.

**Fix:** Tie chain insertion to the descriptor guard:

```rust
crate::registry_diff::PayloadNode::Derivation(mut d) => {
    if !w.derivations.contains_key(&d.name) {
        // ... insert descriptor ...
    }
}
// Similarly, only insert chain if derivation was new:
```

Or, simpler: move chain insertion inside the `!contains_key` branch, or clear and rebuild chains in `install_descriptors`.

---

### WR-02: `op_index: 0` hardcoded in `OpChain::compile` error paths

**File:** `crates/beava-core/src/op_chain.rs:110-115`, `128-133`

**Issue:** When `OpChain::compile` creates a `CompileError::InvalidExpr` for a failed parse it hardcodes `op_index: 0`:

```rust
vec![CompileError::InvalidExpr {
    op_index: 0,       // <-- always 0, regardless of which op failed
    parse_error: pe,
}]
```

This affects both the `Filter` branch (line 110) and the `WithColumns`/`Map` branch (line 128). The `propagate_schema` pass called first (line 101) produces correct `op_index` values in its errors, but the second compilation pass (lines 106-163) reports the wrong index. If both passes run and the second fails on op 3, the error will say `op_index=0`. The `propagation_error_to_validation` mapper in `register_validate.rs` then produces `nodes[N].ops[0].expr` regardless of which op actually has the bad expression.

Since `propagate_schema` always runs first and would also catch the parse error (and with a correct `op_index`), this bug only manifests when `propagate_schema` succeeds but the second parse call fails — an unusual window, but possible if `propagate_schema` used a different expression copy. More importantly, it sets a maintenance trap.

**Fix:** Track the op index in the compile loop and pass it to the error constructor:

```rust
for (op_loop_idx, op) in ops.iter().enumerate() {
    let cop = match op {
        OpNode::Filter { expr } => {
            let ast = expr::parse(expr).map_err(|pe| {
                vec![CompileError::InvalidExpr {
                    op_index: op_loop_idx,  // use actual index
                    parse_error: pe,
                }]
            })?;
```

---

### WR-03: `(x != null)` not rewritten at parse time; evaluates differently from `(not isnull(x))`

**File:** `crates/beava-core/src/expr.rs:39-40` (Pass B doc comment)

**Issue:** The doc comment explicitly states: "`!=` with null on either side is NOT rewritten — only `==`." At runtime, `(x != null)` reaches `eval_binop` with `op = "!="`. The null-propagation guard in `eval_binop` (line 118: `if matches!(lv, Value::Null) || matches!(rv, Value::Null)`) fires immediately and returns `Value::Null`, because `null` is the literal right-hand side. So `(x != null)` always evaluates to `Null`, even when `x` is present and non-null. This is likely unintentional for the SDK's `.isnull()` surface — users who write `(x != null)` expecting "is not null" semantics will get `Null` (which is falsy in Filter), silently dropping rows they expect to keep.

The schema propagator does not flag this usage. `register_validate` Rule 10 passes it through with no warning.

**Fix:** Either:
(a) Rewrite `BinOp("!=", e, Literal::Null)` → `UnaryOp("not", Call("isnull", [e]))` in Pass B (symmetric with the `==` rewrite), or
(b) Add a `PropagationError` or `Warning` variant that flags `!= null` in `apply_filter_schema` / `apply_with_columns_schema` and surface it as a validation hint.

The current behaviour is documented but silently incorrect for the expected use case. Option (a) is the clean fix.

---

### WR-04: `serde_json::to_value(...).unwrap()` in hot HTTP response path

**File:** `crates/beava-server/src/register.rs:245`, `254`, `261`, `288`, `308`

**Issue:** Multiple call sites in `map_outcome_to_http` use `.unwrap()` after serialising response structs to `serde_json::Value`. All of the response types (`RegisterSuccess`, `RegisterErrorBody`) are plain Rust structs with `#[derive(Serialize)]` and only primitive/String fields — serialisation of these cannot fail at runtime. However, the `serde_json::to_vec(&body).expect("...")` calls in `tcp.rs` (lines 308, 403, 411, etc.) use `.expect()` with hardcoded strings. Both patterns are acceptable in practice but mean the server panics (killing the Tokio runtime thread) if serialisation somehow fails.

Similarly, `registry_debug.rs:84` uses `serde_json::to_value(dump).unwrap()` inside the `get_registry` handler, where `dump` contains a `DerivationDescriptor` with an `Arc<OpChain>` (not serialised — correct), but also raw `serde_json::Value` fields inside `Op` members. If a future change adds a non-serialisable field to these structs, this will panic under load.

**Fix:** Use `map_err` or return a 500 from the handler instead of unwrap/expect in hot paths. For now, at minimum document the invariant with a comment clarifying why serialisation cannot fail (e.g., "// infallible: all fields are String/u64/bool"), and consider a `debug_assert!(result.is_ok())` + explicit fallback.

---

### WR-05: `_arith_div` in Python reference evaluator returns `float("nan")` for `0 / 0.0` but Rust returns `f64::NAN` via a different route — test coverage gap

**File:** `python/beava/_eval_reference.py:307`

**Issue:** For mixed `I64(0) / F64(0.0)`, the Python path at line 309-311 returns `float("nan")` via the special-case branch. Rust computes `0_i64 as f64 / 0.0_f64` which is also `NaN`. They agree. However, there is **no test** in `test_eval_reference.py` for this case (division by zero with mixed types). The SC4 proptest excludes `NaN`/`Inf` floats. This specific case can only be caught by a targeted unit test.

Additionally, `_cast_to_int` returns `None` for `math.inf` and `math.nan` inputs (line 548–551). Rust's `cast_to_int` uses `*f as i64` which for `f64::INFINITY` is UB in Rust (though in practice it saturates to `i64::MAX` or `i64::MIN` on x86). Python explicitly returns `None` to "avoid divergence" (comment on line 551). This is a documented known divergence, but the proptest cannot detect it. A targeted property test for `cast(inf_value, "int")` comparing the two implementations would lock in the agreed-upon behaviour.

**Fix:** Add unit tests in `test_eval_reference.py` for:
- `0.0 / 0.0` → both Python and Rust produce NaN
- `cast(float('inf'), 'int')` — document and assert the agreed result (Python: None, or whatever is decided)
- `cast(float('-inf'), 'int')` — same

---

### WR-06: SC4 proptest silently skips cases where server-side registration fails (`reg_resp.status_code != 200`)

**File:** `python/tests/test_phase4_smoke.py:412-420`

**Issue:** When the server rejects a `with_columns` derivation (Rule 10 schema-propagation failure — e.g., `(a and c)` where `c` is `F64` and `and` requires `Bool`), the test silently `return`s without asserting anything. The comment says "both sides produce null / None / error". But the Python reference evaluator for `(a and c)` with `a=True, c=3.0` returns `None` (non-bool → Null path in `_and_three_valued`). The server rejects the registration. The proptest records a "pass" by early return.

This means the SC4 equivalence claim covers only expressions that pass register-time schema validation — a subset of all possible ASTs. The Python reference evaluator is exercised on ALL ASTs (including semantically invalid ones), while the Rust server is only exercised on schema-valid ones. The "zero divergence across 256 cases" headline is misleading because a large fraction of the 256 cases may be silently skipped.

**Fix:** Either:
(a) Count skipped cases and fail the proptest if the skip rate exceeds a threshold (e.g., >50%), or
(b) Separate the proptest into two parts: one for schema-valid expressions (registered + evaluated) and one for schema-invalid expressions (asserting that Python returns `None` for the same inputs), or
(c) Document the skip behaviour and constrain the generator to only produce schema-valid expressions.

---

## Info

### IN-01: `parse_not` is a no-op passthrough in the grammar

**File:** `crates/beava-core/src/expr.rs:647-651`

**Issue:** `parse_not` immediately delegates to `parse_cmp()` without checking for a `not` token. The `not` keyword is only parsed inside `parse_atom` via the `LParen` branch. This is correct for the SDK's canonical grammar (which requires `(not x)` not bare `not x`), but the function name `parse_not` is misleading — it implies `not` is parsed at this level. A future contributor adding bare-unary-`not` support would add it here, unaware that it is already handled in `parse_atom`.

**Fix:** Rename `parse_not` to `parse_cmp_or_not` or add a comment: `// Note: 'not' is handled as a parenthesized prefix in parse_atom; this level is a pass-through.`

---

### IN-02: `Literal::BareIdent` is not excluded from `referenced_fields()`

**File:** `crates/beava-core/src/expr.rs:123-129`

**Issue:** `collect_fields` skips `Expr::Literal(..)` wholesale. `Literal::BareIdent` is a cast type-arg literal (e.g., `float` in `cast(amount, float)`). After Pass A normalization, the second arg to `cast` is `Literal::BareIdent("float")` wrapped in `Expr::Literal`. This is correctly excluded by the `Expr::Literal(..) => {}` arm. The code is correct. However, without a comment this is subtle — a reader might worry that `BareIdent("float")` would be mistakenly treated as a field reference if it were `Expr::Field` instead of `Expr::Literal`. The doc comment on `referenced_fields` says "Literal values (including `BareIdent`) are excluded" which covers this. No code change needed; mark this as a low-priority readability note.

**Fix:** No change required. The existing doc comment is sufficient.

---

### IN-03: `Rename` in `OpChain::apply` mutates `row.0` directly, bypassing `Row`'s owning API

**File:** `crates/beava-core/src/op_chain.rs:203-212`

**Issue:** The `Rename` branch calls `row.0.remove(old)` and `row.0.insert(new, v)` directly on the inner `BTreeMap`, bypassing `Row::renamed()`. This is correct (and efficient — avoids a clone), but it is inconsistent with the rest of `apply` which uses the owning helpers (`row.without_field`, `row.with_field`). It also means the `Rename` op is the only place that accesses `row.0` directly in this function, making the invariant harder to audit. This is particularly relevant because `Row::renamed` panics on absent-key-is-noop (it silently skips), but the direct approach also silently skips absent keys — they are equivalent.

**Fix:** Either use `Row::renamed` for consistency (with a comment that `renamed` silently no-ops on absent fields, matching the `if let Some(v)` guard already present), or extract the multi-key atomic rename into a `Row::renamed_many` helper. Low priority — both paths are correct.

---

### IN-04: `apply_registration` version bump happens even when `compiled_chains` is the only change

**File:** `crates/beava-core/src/registry.rs:215-258`

**Issue:** `apply_registration` bumps `w.version = new_version` unconditionally at line 256, even if no `PayloadNode` descriptors were actually inserted (e.g., all nodes were `already_present` and only `compiled_chains` is non-empty). In practice `apply_registration` is only called from `execute_register` when `diff.added` is non-empty (line 214 of `register.rs`), so the version bump always accompanies a real descriptor insertion. But the precondition is only documented in a comment ("Precondition: `nodes` has passed…"), not enforced. A future call site that passes `nodes=[]` and `compiled_chains=[...]` will silently bump the version with no descriptor changes.

**Fix:** Add a `debug_assert!(!nodes.is_empty() || !compiled_chains.is_empty(), "apply_registration called with empty payload")` or document the precondition as a panicking invariant in the function signature.

---

_Reviewed: 2026-04-23T00:00:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
