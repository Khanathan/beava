# RFC: Expression DSL Extension

| Field | Value |
|---|---|
| **Status** | Draft |
| **Author** | [Khanh Doan](https://github.com/Khanathan) |
| **Date** | 2026-05-18 |
| **Related Issue** | [Issue #56 (narrowed scope)](https://github.com/beava-dev/beava/issues/56) |

---

## Contents

1. [Summary](#1-summary)
2. [Motivation](#2-motivation)
3. [Goals](#3-goals)
4. [Non-goals](#4-non-goals)
5. [Detailed design](#5-detailed-design)
   - 5.1 [Builtin catalogue](#51-builtin-catalogue)
   - 5.2 [BUILTINS organization](#52-builtins-organization)
   - 5.3 [Short-circuit `if_else`](#53-short-circuit-if_else)
   - 5.4 [`is_in` — variadic only](#54-is_in--variadic-only)
   - 5.5 [Event dot-access](#55-event-dot-access)
   - 5.6 [Python footgun guards](#56-python-footgun-guards)
   - 5.7 [The `@bv.expr` decorator](#57-the-bvexpr-decorator)
6. [Implementation phases](#6-implementation-phases)
7. [Testing plan](#7-testing-plan)
   - 7.1 [Per-builtin (Phases 3–4)](#71-per-builtin-phases-34)
   - 7.2 [`@bv.expr` decorator (Phase 6)](#72-bvexpr-decorator-phase-6)
   - 7.3 [Type system (Phase 2)](#73-type-system-phase-2)
   - 7.4 [Regression](#74-regression)
   - 7.5 [End-to-end](#75-end-to-end)
8. [End-to-end use case](#8-end-to-end-use-case)
9. [Rationale and alternatives](#9-rationale-and-alternatives)
   - 9.1 [Why not new `Expr` / `Literal` variants](#91-why-not-new-expr--literal-variants)
   - 9.2 [Why defer list builtins + nested types together](#92-why-defer-list-builtins--nested-types-together)
   - 9.3 [Why defer runtime-mutable sets](#93-why-defer-runtime-mutable-sets)
   - 9.4 [The §D-04 anchor](#94-the-d-04-anchor)
10. [Drawbacks](#10-drawbacks)
11. [Future work](#11-future-work)
12. [References](#12-references)

---

## 1. Summary

This RFC lets feature engineers write streaming transforms in
idiomatic Python. A new `@bv.expr` decorator accepts plain-Python
`if`/`elif`/`else`, ternaries, `and`/`or`/`not`, `is None`, and
local variables — translating them into the existing expression
DSL. The DSL itself gains 11 v0 scalar builtins across five
categories (math, string, time, hashing, conditional / null;
the Predicates category from the full catalogue defers entirely
to good-first-issues since `&` / `|` cover the v0 needs) — see
§5.1 for the v0 list plus the queue — and event dot-access
(`e.email` for `bv.col("email")`).
No AST or grammar growth; everything reuses existing extension
points.

## 2. Motivation

Today's DSL stops at comparisons, arithmetic, and three builtins
(`cast`, `isnull`, `quadkey`). Real use cases routinely reach for 
log/clip/abs, lower/contains/starts_with, hour-of-day, conditional 
branching, etc — none of which the current surface supports. Users
route around the gap by defining features in offline Python
pipelines, losing exactly the low-latency windowed aggregations
the streaming server was built to provide. This RFC closes that
gap with the smallest possible surface change: a Python-first
decorator on top of a builtin catalogue extension, both designed
to make future extension easier.

## 3. Goals

1. Ship one function per category of builtins in 5.1 below.
2. Make future builtins a bounded mechanical change: one
   `BuiltinFn` row (name + arity + eval + `infer` fn pointer)
   + one Python sugar method. No `match fn_name` anywhere.
3. Keep wire format and AST shape locked.
4. Provide pandas/Polars-style ergonomics: method chaining,
   dot-access on events.
5. Support plain-Python `if/elif`/ternary/`and`/`or`/`not`
   inside expression defs.
6. Add `CONTRIBUTING-OPS.md` walking one full op contribution.

## 4. Non-goals

- **Full `@bv.expr` per #56** (type-annotation schema checking,
  loops, recursion, subprocess fallback). 
- **Runtime-mutable sets** (`is_in_set` w/ server-managed sets).
  Different problem class.
- **New `Expr` / `Literal` variants and grammar growth.** No
  `Expr::If`, `Expr::Let`, `Expr::Regex`, no `Literal::List`, no
  `[` / `]` lexer tokens. `is_in` uses the existing variadic
  call form (§5.4).
- **List builtins (`split`, `at`), `[i]` indexing, and nested /
  composite types.** Deferred to a follow-up issue. Needs
  `FieldType::List<T>` + `InferredType` extension + `Value::Map`
  policy + `_Expr.__getitem__` desugar; pulling them in couples
  several independent cross-cutting type-system decisions. v0
  ships zero list-returning builtins.
- **`Value::Map` indexing**. Folded into the list / nested-types
  follow-up.
- **Python-side register-time type checking**.

## 5. Detailed design

### 5.1 Builtin catalogue

This RFC proposes the full catalogue below; the rest become
good-first-issues citing `CONTRIBUTING-OPS.md` from Phase 7 so
new contributors have a templated extension path.

| Category | Functions |
|---|---|
| Math | `abs`, `sign`, `log`, `log1p`, `log10`, `exp`, `sqrt`, `pow`, `floor`, `ceil`, `round`, `mod`, `clip` |
| String | `lower`, `upper`, `length`, `contains`, `starts_with`, `ends_with`, `substr`, `replace`, `concat` |
| Conditional / null | `if_else`, `coalesce`, `fill_null`, `is_in` |
| Predicates | `any_of`, `all_of` |
| Time | `hour_of_day`, `day_of_week`, `month`, `day_of_month` |
| Hashing | `hash`, `hash_mod` |

**v0 ships** (11 builtins — every one exercised by the §8
canonical example, plus `if_else` as a load-bearing rewrite
target for the `@bv.expr` if/elif/ternary lowerings, plus
`hour_of_day` as a Time category representative not used by the
example):

- **Math**: `log1p`, `clip`
- **String**: `lower`, `contains`, `starts_with`, `ends_with`, `replace`
- **Conditional / null**: `if_else`, `is_in`
- **Time**: `hour_of_day`
- **Hashing**: `hash_mod`

The **Predicates** category (`any_of` / `all_of`) defers
entirely to good-first-issues — `&` / `|` (and their `and` / `or`
sugar inside `@bv.expr`) cover the v0 use cases; flat variadic
predicates earn their keep only at 3+ args.

Each builtin pins one of four null rules in its doc comment:
**strict-propagating** (math, time), **null-eating** (`fill_null`,
`coalesce`), **null-aware predicate** (`contains`, `is_in`),
**short-circuit** (`if_else`). Non-bool inputs follow §D-04
runtime-tolerant: return `Null`, never panic.

#### Generalizing per-builtin typechecking

Today each builtin's register-time type rules live in a hardcoded 
`match fn_name` arm in `infer_call_type`. This RFC moves them onto the
`BuiltinFn` row via an `infer` fn pointer, so builtins with the
same type signature share one helper instead of each getting its
own match arm.

`BuiltinFn` carries an `infer` fn pointer; `infer_call_type`
arity-checks, infers args, then dispatches through `builtin.infer`.

```rust
pub struct BuiltinFn {
    pub name:  &'static str,
    pub arity: Arity,
    pub eval:  fn(&[Value]) -> Value,
    pub infer: fn(arg_types: &[InferredType], args: &[Expr])
                 -> Result<InferredType, InferError>,
}
```

The `args: &[Expr]` second parameter exists only for `cast`,
whose inference reads the AST literal name. Every other builtin
ignores it.

Shared helpers in `builtins/_inference.rs`:

| Helper | Used by |
|---|---|
| `unary_str_to_str` | `lower`, `upper` |
| `unary_str_to_i64` | `length` |
| `unary_numeric_same` | `abs`, `sign`, `floor`, `ceil`, `round` |
| `unary_numeric_to_f64` | `log`, `log1p`, `log10`, `exp`, `sqrt` |
| `binary_numeric_to_f64` | `pow`, `mod` |
| `string_search_to_bool` | `contains`, `starts_with`, `ends_with` |
| `polymorphic_var0_unify` | `coalesce`, `fill_null` |
| `any_to_bool` | `isnull` |

New Builtins with type signature not matching existing helpers can use
Inline-fn primitives:
`require_arg_types`, `require_arg_class`, `unify_var0_strict`,
`unify_var0_with_class`, `read_literal_type_name`.

Two-line inline example:

```rust
fn day_diff_infer(arg_types: &[InferredType], _: &[Expr])
    -> Result<InferredType, InferError>
{
    require_arg_types(arg_types, &[FieldType::Datetime, FieldType::Datetime])?;
    Ok(InferredType::Known(FieldType::I64))
}
```

If more builtins share the same new type signature, more shared helpers can be added.

**Unification** (`if_else`, `coalesce`, `fill_null`) is **strict
equality** — no numeric promotion. `if_else(c, 1, 2.0)` and
`coalesce(int_field, 0.0)` register-fail with `TypeMismatch`;
users wrap with `cast(...)`. `NullLiteral` is the permitted hole;
all-null fallback → `FieldType::Str`. Relaxing later is additive.

**Coverage is type-system-enforced**: function-pointer field
has no default, so missing `infer` fails compilation. The
catch-all arm (which also misclassifies `quadkey` today) is
deleted.

### 5.2 BUILTINS organization

Split per-category files (`builtins/{math,string,time,cond,hash}.rs`),
each exporting `pub const X_BUILTINS: &[BuiltinFn]`. `lookup_builtin`
chains them with `.or_else(...)`. Compile-time test asserts
no-duplicate names. Categorization is source-organization only —
never on `Expr::Call`, never on the wire.

### 5.3 Short-circuit `if_else`

`if_else(cond, then, else)` evaluates `cond`, then runs **only
one** of `then` / `else` per row. The inactive arm is skipped,
not just discarded. This avoids spurious null propagation
(e.g. `bv.if_else(denom != 0, num / denom, 0.0)` — the
division would otherwise turn the whole result into `Null` on
`denom == 0`) and saves per-row work when the inactive arm is
expensive.

**Mechanism.** The AST node is a normal `Call("if_else", [cond, then, else])`.
The Rust `eval_depth` checks `fn_name == "if_else"` before its
generic eager-eval Call arm and evaluates exactly one of the
two arms — same pattern already used for `and` / `or` in
`eval_binop`. The `BUILTINS` table still has an entry
(`Arity::Fixed(3)`, inline `infer` validating `cond: Bool` +
strict-unifying the arms, defensive eager `eval` fn) so
removing the special-case would regress perf, not correctness.

**Null cond contract.** `if_else(Null, a, b)` → `Null` per
§D-04; neither arm runs. SQL's `CASE WHEN NULL` falls through
to `ELSE`, silently masking missing data; Polars returns `Null`
for the same reason.

**Python surface.** Two equivalent forms:

```python
bv.if_else(c, a, b)                  # function form
bv.when(c).then(a).otherwise(b)      # builder form
```

Both lower to `_Call("if_else", (c, a, b))`. `.otherwise()` is
**required** — the builder returned by `.then(a)` is
intentionally not an `_Expr`; incomplete chains raise
`when_missing_otherwise`.

### 5.4 `is_in` — variadic only

`is_in(x, "US", "CA", "DE")` on the wire — existing variadic
call form, no grammar or AST change. Python SDK accepts **both
forms equivalently**:

```python
bv.is_in(col, "US", "CA", "DE")        # direct variadic
bv.is_in(col, ["US", "CA", "DE"])      # convenience list, unpacked at serialization
```

Both produce the same wire output. The list form exists so
programmatic construction reads naturally
(`bv.is_in(col, *known_countries)` also works but a literal
Python list is the idiomatic alternative). `None` as a positional
arg or inside the list raises
`RegistrationError(code="is_in_null_element")` at descriptor-build
time — SQL's `x IN (NULL)` is surprising; point at `isnull(x)`.

Readable-literal-list syntax (`is_in(x, [...])` parsed on the
wire) is deferred with the list / nested-types follow-up (§4) —
adding `Literal::List` then is strictly additive: the variadic
form keeps working.

### 5.5 Event dot-access

`__getattr__` on `_ChainMixin` lets `e.email` work. Guarded
against dunder/private names (`name.startswith("_")` →
`AttributeError`) so introspection doesn't get intercepted.

Trade-off: typos (`e.eamil`) become register-time "field not in
schema" errors. Matches pandas/Polars.

### 5.6 Python footgun guards

Three `_Expr` dunders raise `TypeError`:
- `__bool__` — blocks ternary `a if cond else b`, `x and y`,
  `x or y` outside `@bv.expr`. Points at `bv.if_else`/`&`/`|`.
- `__iter__` — blocks for-loops and any other iteration context.
- `__len__` — blocks `len()`.

Polars guards `pl.Expr` the same way. Non-optional — without
them, every silent-first-branch ternary becomes a silent bug.

### 5.7 The `@bv.expr` decorator

Narrow Python→`_Expr` translator. Rewrites five constructs;
rejects everything else.

**Decoration semantics**:

- **Rewrite at decoration time.** When Python executes the
  `@bv.expr` line, the decorator reads the function source via
  `inspect.getsource`, parses it with `ast.parse`, applies the
  five rewrites below, validates the result (rejecting unsupported
  constructs with structured `RegistrationError`s at the user's
  `def` site), then `compile()`s the transformed AST and rebinds
  the function name to the rewritten code. Cost paid once at
  import / definition time; errors surface where the user writes
  the function, never deep in registration or evaluation.

- **Returns a callable, not a static `_Expr`.** The decorated
  name is a function; invoking it runs the rewritten body and
  returns a fresh `_Expr` tree. The body re-runs on every call —
  the same `@bv.expr` can be reused with different `_Expr`
  arguments at different call sites (e.g. `email_token(e.email)`
  in one event chain and `email_token(e.alt_email)` in another).
  No tree-caching layer.

- **`_Expr` arguments pass through; literals are coerced lazily**
  at call time. Any non-`_Expr` argument (`int`, `float`, `str`,
  `bool`, `None`) is wrapped in `_Literal` before the body
  executes, so `email_token(e.email)` and `email_token("hi")` both
  work — inside the body, the bound parameter is always an
  `_Expr`. Anything outside this set (lists, dataclasses, non-DSL
  objects) reaches the body unwrapped and hits the usual `_Expr`
  operator errors.

- **Type annotations are decorative**, not enforced — consistent
  with §4's no-Python-typecheck stance. They serve only as
  developer documentation and IDE hinting.

- **`@bv.expr` calling another `@bv.expr` is plain function
  composition.** The inner returns an `_Expr` tree; the outer uses
  it like any other subexpression. No special-case inlining
  needed — the resulting wire tree is identical to what hand-
  written nested calls would produce.

**Accepted**:

1. **`if`/`elif`/`else`** where every branch ends `return <expr>`.
   → nested `bv.if_else(...)`.

   ```python
   @bv.expr
   def dwell_bucket(dwell_ms):
       if   dwell_ms < 1000:  return 0
       elif dwell_ms < 5000:  return 1
       elif dwell_ms < 30000: return 2
       else:                  return 3
   ```

2. **Ternary `a if cond else b`** → `_Call("if_else", (cond, a, b))`.
   Bare-Python ternary outside `@bv.expr` raises `TypeError` per
   §5.6 — asymmetry is intentional (loud error or correct code,
   never silent miscompile).

3. **Local assignments** before the terminal return —
   inline-substituted into the lowered `_Expr` tree (no
   `Expr::Let`, no evaluator environment frame). The translator
   maintains a `dict[str, _Expr]`; each `ast.Name` reference is
   replaced with the bound subtree at rewrite time.

   ```python
   @bv.expr
   def risk(c):
       country_risk = bv.if_else(bv.is_in(c.country, "NG", "RO"), 5, 0)
       amount_risk  = bv.log1p(c.amount) * 2
       return country_risk + amount_risk
   ```

   **Scope rule**: assignments are only accepted at the
   function-body top level, before the terminal `return` or
   `if`/`elif`/`else` chain. Per #1, every if-branch is
   single-expression-`return`-only — no per-branch assigns. The
   pattern below is **rejected** (`expr_unsupported_if_branch`):

   ```python
   if x > 0:
       y = x * 2          # per-branch assign — rejected
       return y + 1
   else:
       return 0
   ```

   Workaround: lift the binding to the top level, or fold it
   into the returned expression.

   **Sequential reassignment** (`x = …; x = x + 1`) is allowed
   — the dict is updated in place; later RHS sees the prior
   binding.

   **Rejected forms** (each with a structured error):
   - Tuple / list unpacking (`a, b = …`) — `expr_bad_assign_target`.
   - Attribute / subscript targets (`c.x = …`, `c[0] = …`) —
     `expr_bad_assign_target` ("row mutation not allowed").
   - Augmented assigns (`x += 1`) — `expr_unsupported_python_op`
     ("use `x = x + 1`").
   - Reference before assignment — `expr_unknown_name`.
   - **Convergent branch-local bindings** — a name assigned only
     in some if-branches and read after the if. Rejected with
     `expr_branch_local_binding`, pointer at `bv.if_else(c, a, b)`.
     The one case that would genuinely need `Expr::Let`; the
     workaround pushes the branching down to a single
     `bv.if_else` and keeps the binding at top level.

   **Trade-off vs `Expr::Let`**: inline substitution duplicates
   the bound subtree per reference. A 200-char subexpression
   referenced 8 times grows the wire payload from ~400 B to ~1.6 KB
   — bounded and paid only at registration; scalar-per-row eval
   cost is dwarfed by deserialize and per-event allocation.
   `Expr::Let` promotion is §11 future work, triggered by
   register-payload >100 KB from duplicated subexprs. (Most likely not an issue 
   in most use cases unless subexpression is ridiculously large and referenced
   many times)

4. **`is None` / `is not None` checks** — four accepted shapes:
   `x is None`, `x is not None`, `None is x`, `None is not x`.
   The `None`-side operand is identified positionally (not by
   argument order), so all four lower to the same node:

   | Source | Rewritten to |
   |---|---|
   | `x is None`, `None is x` | `_Call("isnull", (x,))` |
   | `x is not None`, `None is not x` | `_UnaryOp("not", _Call("isnull", (x,)))` |

   Reason: `is` is Python object-identity; on `_Expr` instances
   `email is None` is unconditionally `False` without inspecting
   the operand — silent miscompile if unrewritten. All other
   `is` / `is not` shapes (e.g. `x is y`, `x is True`,
   `x is some_var`) remain **rejected** at decoration time with
   `expr_unsupported_python_op`.

5. **`and` / `or` / `not`** → `&` / `|` / `~` at the AST-rewrite
   layer (i.e. before Python executes the body, so `__bool__`
   never fires). The full pipeline for `a and b`:

   ```
   Python source:       a and b
   Rewritten AST:       a & b                          (BoolOp → BitAnd)
   Operator overload:   _Expr.__and__(a, b)
   SDK node:            _BinOp("and", a, b)
   Wire string:         (a and b)                       (keyword form)
   Rust AST:            BinOp { op: "and", … }
   ```

   The wire keeps the keyword form — `&` is purely a Python-side
   bridge, never visible on the wire. Same applies to `|` → `or`
   and `~x` → `(not x)`. See the operator-translation table in
   `CLAUDE.md` for the full mapping.

   Outside `@bv.expr`, bare `and`/`or`/`not` still raises via
   `__bool__` (§5.6). Same loud-error-or-correct-code asymmetry
   as ternary.

**Rejected at decoration time** (with structured `RegistrationError`
+ source line): `for`/`while`, nested defs/classes/lambdas,
`try`/`except`/`with`/`raise`, unpacking, augmented assigns,
attribute/subscript targets, `import`, `is`/`is not` on non-`None`
operands.

**Always-safe usage**: `@bv.expr` is a no-op on rewrite-free
function bodies but still provides the decoration-time validation
and call-time parameter coercion described above. Apply
uniformly — there is no penalty for decorating an expression-only
function.


## 6. Implementation phases

Each phase gated by `check.sh` green.

1. **Python prep**: `_Call(name, args)` + `_BareIdent`;
   `__bool__`/`__iter__`/`__len__` guards; `_ChainMixin.__getattr__`.
2. **Rust prep**: split `BUILTINS` per-category; add
   `Arity::AtLeast(usize)` (for `coalesce` shape); add `infer`
   fn pointer to `BuiltinFn`; create `_inference.rs` (helpers +
   primitives); backfill `infer` on `cast`/`isnull`/`quadkey`
   (`cast` exercises `read_literal_type_name`, dogfood for the
   confusing primitive); replace `infer_call_type`'s match with
   fn-pointer dispatch; add name-uniqueness test. Coverage is
   type-system-enforced.
3. **Level 0 (fixed-arity) v0 builtins**: the 10 fixed-arity
   entries from §5.1's v0 list (`log1p`, `clip`, `lower`,
   `contains`, `starts_with`, `ends_with`, `replace`, `if_else`,
   `hour_of_day`, `hash_mod`). Per builtin: eval, row, sugar,
   tests. **No `schema_propagate.rs` edit per builtin.**
4. **Level 1 (variadic) v0 builtins**: the 1 variadic from
   §5.1's v0 list — `is_in` (`AtLeast(2)`). Remaining variadics
   (`coalesce`, `any_of`, `all_of`, `concat`) queued as
   good-first-issues per §5.1.
5. **`if_else` short-circuit**: special-case in `eval_depth`;
   defensive eval fn; `bv.when().then().otherwise()` sugar.
6. **`@bv.expr`**: add `and`/`or`/`not`
   rewriter.
7. **`CONTRIBUTING-OPS.md`**: walk one full op contribution end
   to end (e.g. adding `math.log1p`) — choice of `infer` helper
   vs inline primitive, `eval` fn, Python sugar method, null-rule
   selection, test set. Cite Phase 3 / 4 builtins as concrete
   dogfood references so first-time contributors have a
   templated PR pattern.
8. **Docs / regeneration**: `__all__` updates; website per
   `SOURCE-OF-TRUTH.md`.
9. **Out-of-scope catalog**: §4 + plan.

Step-level detail in [`implementationplan.md`](./implementationplan.md).

## 7. Testing plan

Tests are gated by `check.sh` at every phase boundary (§6). The
following standard applies.

### 7.1 Per-builtin (Phases 3–4)

Each builtin (eval fn + `BuiltinFn` row + Python sugar) ships
with four tests in the per-category test module:

- **Arity**: wrong-arg-count register-fails with the documented
  structured error (`aggregation_invalid_arity` or `TypeMismatch`
  for zero-arg under `AtLeast(n)`).
- **Eval truth-table**: 3–5 representative inputs covering normal
  values, boundary cases (e.g. `clip` at exact bounds), and edge
  values (`I64::MAX`, `F64::NAN`, empty string).
- **Null-rule conformance**: verifies the documented null
  behavior — strict-propagating / null-eating / null-aware
  predicate / short-circuit per §5.1.
- **SDK round-trip**: Python `_Expr` → wire string → Rust
  `parse` → identical Rust `Expr` AST. Pins the wire form
  documented in the Python sugar's docstring.

### 7.2 `@bv.expr` decorator (Phase 6)

Two test modules under `python/tests/v0/test_expr_translator/`:

- **Accepted**: one test per rewrite rule (§5.7 #1–#5), each
  asserting the lowered `_Expr` tree matches the expected nested
  `_Call` / `_BinOp` structure. Plus integration cases combining
  multiple rules (the §8 canonical example reused as a fixture).
- **Rejected**: one test per error code in the rejection list
  (`expr_unsupported_python_op`, `expr_bad_assign_target`,
  `expr_branch_local_binding`, `expr_unsupported_if_branch`, …),
  asserting both the code and the source line in the error
  message.

Plus decoration-time validation runs once per `@bv.expr`;
call-time literal coercion (`int` / `str` / `bool` / `None` →
`_Literal`); sequential reassignment threads the binding dict;
nested `@bv.expr` composition produces the expected wire tree.

### 7.3 Type system (Phase 2)

- **Helper-table tests** in `crates/beava-core/src/builtins/_inference.rs`:
  each shared helper (`unary_str_to_str`,
  `polymorphic_var0_unify`, etc.) gets at least one positive
  (well-typed → expected `InferredType`) and one negative
  (mistyped → `TypeMismatch`) test.
- **Unification corner cases**: `polymorphic_var0_unify` rejects
  `I64`/`F64` mixing, accepts `NullLiteral` as a hole, falls
  back to `FieldType::Str` when all bindings are null.
- **Name uniqueness across category tables**: compile-time test
  walks all category slices and asserts no name collisions.
- `infer` field coverage is type-system-enforced (no default;
  missing `infer` is a compile error) — no runtime test needed.

### 7.4 Regression

- Existing builtins (`cast`, `isnull`, `quadkey`) — `quadkey` in
  particular — type correctly under fn-pointer dispatch where
  they previously fell through the deleted catch-all arm.
- Wire-format round-trip stability: every expression in the
  current `python/tests/v0` corpus continues to parse and
  evaluate identically.

### 7.5 End-to-end

The §8 canonical example registers successfully against a v0
server, exercises every accepted `@bv.expr` rewrite plus every
v0 builtin from §5.1, and produces the documented aggregations
against a synthetic Click event stream.

## 8. End-to-end use case

```python
import beava as bv

@bv.event
class Click:
    user_id: str
    email: str | None
    country: str
    referrer: str
    amount_usd: float
    dwell_ms: int
    ts: int

# §5.7 #4 (is None) + hashing builtin + string.lower
@bv.expr
def email_bucket(email: str | None):
    if email is None:
        return 0
    return bv.hash_mod(email.lower(), 1024)

# §5.7 #2 (ternary) + §5.7 #5 (and/not rewrite) + string predicates
@bv.expr
def is_external_secure(url: str):
    return 1 if url.starts_with("https://") and not url.contains("internal.") else 0

# §5.7 #1 (if/elif/else) + #3 (local assigns) + #5 (and/or)
# + math.log1p + math.clip + is_in + nested bv.if_else
@bv.expr
def risk_score(amount_usd: float, dwell_ms: int, country: str):
    log_amount  = bv.log1p(amount_usd)
    short_dwell = bv.clip(dwell_ms, 0, 1_000)
    geo_bonus   = bv.if_else(
        bv.is_in(country.lower(), "ru", "kp", "ir"),
        3.0,
        0.0,
    )
    if   log_amount > 6.0 and short_dwell < 200: return geo_bonus + 5.0
    elif log_amount > 4.0 or  short_dwell < 500: return geo_bonus + 2.0
    else:                                        return geo_bonus

# §5.7 #3 (local assign) + #2 (ternary) + #5 (or)
# + method chaining (lower → replace → replace, then chained predicate)
# + string.ends_with + string.contains
@bv.expr
def is_suspicious_referrer(referrer: str):
    clean = referrer.lower().replace("https://", "").replace("http://", "")
    return 1 if clean.ends_with(".xyz") or clean.ends_with(".tk") or clean.contains("bit.ly") else 0

# §5.5 event dot-access (e.email rather than bv.col("email"))
def ClickFeatures(e: Click):
    e = e.with_columns(
        email_bkt      = email_bucket(e.email),
        secure_ext     = is_external_secure(e.referrer),
        risk           = risk_score(e.amount_usd, e.dwell_ms, e.country),
        suspicious_ref = is_suspicious_referrer(e.referrer),
    )
    return e.group_by("user_id").agg(
        clicks_24h            = bv.count(window="24h"),
        distinct_emails_24h   = bv.n_unique("email_bkt", window="24h"),
        risky_clicks_1h       = bv.count(where=bv.col("risk") >= 5.0, window="1h"),
        avg_amount_24h        = bv.mean("amount_usd", window="24h"),
        suspicious_refs_24h   = bv.sum("suspicious_ref", window="24h"),
        secure_clicks_24h     = bv.sum("secure_ext", window="24h"),
    )
```

Each function targets a distinct combination of accepted rewrites
and builtin families so the example doubles as a coverage
checklist. Construct-level walkthrough, register-time type-check
trace, and edge-case behavior table live in
[`canonexample2.md`](./canonexample2.md).

## 9. Rationale and alternatives

### 9.1 Why not new `Expr` / `Literal` variants

Every `match Expr` / `match Literal` arm pays for new variants
forever (~30 sites: eval, schema, serde, parser tests). 
`Call` + special-cased names in the evaluator + the existing variadic
call form cover every operator and literal shape this RFC needs.
Zero AST growth keeps the entire change inside the function-pointer
dispatch surface §5.1 establishes — easier to review, no
parser/lexer touch, no wire-grammar change.


### 9.2 Why defer list builtins + nested types together

`split` / `at` / `[i]` indexing need `FieldType::List<T>` +
`InferredType` extension. `Value::Map` indexing needs a parallel
decision for object-typed fields with a different null-rule
shape. Pulling these in couples several cross-cutting
type-system trades — easier to design and review them in one
dedicated follow-up than to ship a v0 sidestep that constrains
the eventual shape.

### 9.3 Why defer runtime-mutable sets

Different primitive (not a generalization). Needs server state,
admin API, persistence, versioning. Own design pass.

### 9.4 The §D-04 anchor

Beava already has a runtime-tolerant convention: non-bool /
non-null → `Null`, never panic. Referenced in `row.rs:146,174,198`,
`eval.rs:133`, `op_chain.rs:184`. New builtins reference it
rather than re-litigate.

## 10. Drawbacks

- **`@bv.expr` rejection error quality is load-bearing**. Needs
  concrete pointer messages, not generic "unsupported".
- **`is_in` wire form is verbose for large allow-lists**
  (`is_in(country, "US", "CA", "DE", ...)`). Readable `[...]`
  syntax waits for the list / nested-types follow-up.
- **No list / map access in v0.** Common idioms like
  `email.split("@")[1]` are not expressible until the follow-up
  RFC ships. Users wanting domain-from-email today must
  pre-extract in event-producer code or wait.


## 11. Future work

Triggers for revisiting §4 deferrals:

- **Full `@bv.expr`**: concrete user request for type-annotation
  schema checking / loops, or 3+ rejection errors/week in tooling.
- **Runtime-mutable sets**: production deployment needing
  denylist freshness >1 register cycle.
- **List builtins + nested / composite types + literal-list
  syntax** (`split`, `at`, `[i]` indexing; `Value::Map` indexing;
  `FieldType::List<T>` + `InferredType` extension;
  `Literal::List` + `[` / `]` lexer tokens; `is_in(x, [...])`
  on the wire): when the first concrete user need surfaces —
  domain extraction, JSON-field access, large allow-list
  ergonomics. Promotion is **strictly additive** to v0
  semantics: no currently-valid expression changes meaning.
- **`Let` AST variant**: register-payload >100 KB from duplicated
  subexprs, or frequent convergent branch-local binding requests.

## 12. References

- Issue #56 — original proposal
- ADR-002 — Polars-style aggregation naming
- `CONTEXT.md §D-04` — runtime-tolerant null convention
- `CLAUDE.md`, `SOURCE-OF-TRUTH.md`
