---
phase: 04-stateless-ops-expression-evaluator
plan: "02"
subsystem: parser
tags: [parser, expression, ast, recursive-descent, span-tracking, column-error, beava-core, proptest, null-rewrite, sdk-col-04]

requires:
  - phase: 04-01
    provides: "Value enum — Literal AST nodes store compatible scalar types"
  - phase: 03-python-sdk-skeleton-decorators-expression-dsl
    provides: "Canonical parenthesized grammar locked in _col.py (D-08)"

provides:
  - "pub fn parse(source: &str) -> Result<Expr, ParseError> — full recursive-descent parser"
  - "Expr enum: Field/Literal/BinOp/UnaryOp/Call with byte-offset Span on every node"
  - "Literal enum: Null/Bool/Int/Float/Str/BareIdent"
  - "ParseError { col: usize, reason: String } — 1-indexed column-pointing errors"
  - "Span { start: usize, end: usize } — byte-offset spans"
  - "Expr::referenced_fields() -> BTreeSet<String> — field-reference collector for schema validation"
  - "Expr::span() -> Span — top-level span accessor"
  - "Post-parse Pass A: cast(x, float) second-arg Field → Literal::BareIdent normalization"
  - "Post-parse Pass B: (x == null) / (null == x) → Call('isnull', [x]) rewrite"

affects:
  - 04-03-evaluator (takes Expr, produces Value; relies on Pass B having run — no null-eq special-case in eval)
  - 04-04-schema-propagation (walks Expr via referenced_fields to validate field refs against schema)
  - 04-05-register-integration (calls parse() on every Filter/WithColumns/Map expr; surfaces ParseError.col in 400 path)

tech-stack:
  added: []
  patterns:
    - "Hand-rolled tokenizer (inline Lexer struct) with byte-by-byte scan and Span tracking"
    - "paren_depth guard: binary ops only consumed when paren_depth > 0; enforces SDK invariant that every binary op is parenthesized"
    - "Two-pass post-parse normalization: Pass A (cast BareIdent), Pass B (null-eq rewrite)"
    - "proptest arb_sdk_expr strategy mirrors Python _col.py AST for SDK-equivalence property test"

key-files:
  created:
    - crates/beava-core/src/expr.rs
  modified:
    - crates/beava-core/src/lib.rs
    - .gitignore

key-decisions:
  - "paren_depth guard enforces parenthesization invariant at parse time — bare 'a + b' → Err, not silent BinOp; this prevents grammar drift between SDK and server"
  - "Negative literals handled at Atom level: leading '-' followed by IntLit/FloatLit emits Literal::Int(-N)/Float(-N) to match Python repr() output"
  - "Pass B rewrite is bottom-up (recurse children first) so nested null-eq inside and/or/call-args is rewritten before the parent sees it"
  - "!= null is intentionally NOT rewritten — only == null; 'is not null' must be written as (not isnull(x))"
  - "cast second-arg is normalized in Pass A (not at lex time) — identifiers parse as Fields normally; post-parse inspection of Cast's arg[1] converts it to BareIdent"
  - "proptest-regressions/ added to .gitignore — generated at test time, not part of source"

requirements-completed:
  - SDK-COL-07

duration: 45min
completed: 2026-04-23
---

# Phase 4 Plan 02: Recursive-Descent Parser + Null-Equality Rewrite Summary

**Hand-rolled recursive-descent parser for the canonical parenthesized bv.col grammar, producing a typed Expr AST with byte-offset spans, column-pointing ParseError, SDK-equivalence proptest, and (x == null) → isnull(x) post-parse rewrite — the central gatekeeper for Plans 04-03 through 04-06**

## Performance

- **Duration:** ~45 min
- **Started:** 2026-04-23
- **Completed:** 2026-04-23
- **Tasks:** 2 (1.a red, 1.b green)
- **Files modified:** 3

## Accomplishments

- Full recursive-descent parser with 7 non-terminal functions (`parse_or/and/not/cmp/add/mul/atom`) and inline tokenizer scanning byte-by-byte with Span tracking
- `paren_depth` guard enforces the SDK's parenthesization invariant: bare `a + b` is rejected with `col 3: unexpected token` while `(a + b)` parses correctly
- Negative literals at Atom level: leading `-` followed by `IntLit`/`FloatLit` emits `Literal::Int(-N)` or `Literal::Float(-N)`, matching Python's `repr(-7) = "-7"` output
- Single-quoted string unescaping: `\'` → `'`, `\\` → `\`
- **Pass A normalization**: `cast(amount, float)` — the type-arg identifier `float` parses as `Expr::Field` then is rewritten post-parse to `Literal::BareIdent("float")`
- **Pass B null-equality rewrite**: `(x == null)` and `(null == x)` both rewrite to `Call("isnull", [x])` recursively (bottom-up); `!=` null is intentionally left as `BinOp("!=", _, Null)`
- `referenced_fields() -> BTreeSet<String>`: walks the AST collecting `Expr::Field` names; `BareIdent` excluded; survives the null-eq rewrite (test 29)
- `span() -> Span`: returns the node's byte-offset span
- TDD red-green commit chain: `test(04-02)` stub commit (30 FAILED) → `feat(04-02)` green commit (30 PASSED, 278 workspace tests zero regressions)

## Grammar Coverage Checklist

| Grammar Rule | Test Fixture | Test Name |
|---|---|---|
| `Expr := OrExpr` | `"((a > 0) and (b < 5))"` | `parse_nested_and_or` |
| `OrExpr: or` | `"((a > 0) or (b < 5))"` | `proptest_sdk_strings_parse` |
| `AndExpr: and` | `"((a > 0) and (b < 5))"` | `parse_nested_and_or` |
| `NotExpr: not` | `"(not flag)"` | `parse_unary_not` |
| `CmpExpr: >` | `"(amount > 100)"` | `parse_binary_comparison` |
| `CmpExpr: >=, <, <=, ==, !=` | all via proptest | `proptest_sdk_strings_parse` |
| `AddExpr: +` | `"(a + b)"` | `parse_binary_arithmetic` |
| `AddExpr: -` | `"(0 - 7)"` | `parse_integer_literal` |
| `MulExpr: *` | via proptest | `proptest_sdk_strings_parse` |
| `MulExpr: /` | via proptest | `proptest_sdk_strings_parse` |
| `Atom: ( Expr )` | `"(amount > 100)"` | `parse_binary_comparison` |
| `Atom: Call` | `"cast(amount, float)"` | `parse_call_cast` |
| `Atom: Ident` | `"amount"` | `parse_bare_field` |
| `Atom: Literal` | `"42"`, `"null"`, `"true"` | `parse_integer_literal` etc. |
| `Call: empty arglist` | `"noop()"` | `parse_empty_arglist` |
| `Ident: dotted` | `"Stream.x"` | `parse_qualified_field` |
| `Literal: negative number` | `"-7"`, `"-0.5"` | `parse_integer_literal`, `parse_float_literal` |
| `Literal: single-quoted string` | `"'hello world'"`, `"'it\\'s'"` | `parse_string_literal_with_escapes` |
| `Literal: bool/null` | `"true"`, `"false"`, `"null"` | `parse_bool_literals`, `parse_null_literal` |

## Proptest

- **Strategy**: `arb_sdk_expr(depth=4)` generates `SdkExpr` trees mirroring Python `_col.py` AST
- **`to_expr_string()`**: implemented identically to Python (every binary op parenthesized, string escaping via `replace('\\', "\\\\").replace('\'', "\\'")`)
- **Cases**: proptest default 256 cases; all passed in green commit with 0 shrinks
- **Covered shapes**: Field (4 variants), Literal (5 types), BinOp (12 ops), UnaryNot, CallIsnull, CallCast (4 type args)
- **Regression tracking**: `proptest-regressions/` added to `.gitignore` (generated at test time)

## Cast Second-Arg Normalization (Pass A)

- `cast(amount, float)` — the grammar's `Ident` production parses `float` as `Expr::Field { name: "float" }`
- After `parse_expr()` succeeds, `normalize_cast()` walks the tree; for any `Call("cast", [_, Expr::Field { name }])`, rewrites `args[1]` to `Expr::Literal(Literal::BareIdent(name), span)`
- This is post-parse (not at lex time) so the normal expression pipeline handles cast's first arg as any expression
- Test 12 (`parse_call_cast`) verifies the output is `BareIdent("float")`, not `Field("float")`

## Null-Equality Rewrite (Pass B)

- **Location**: `fn rewrite_null_eq(expr: Expr) -> Expr` in `expr.rs`, called after Pass A
- **Trigger**: `BinOp { op: "==", left, right }` where either `left` or `right` is `Expr::Literal(Literal::Null, _)`
- **Output**: `Call { fn_name: "isnull", args: vec![non_null_side], span: original_binop_span }`
- **Commutativity**: both `(x == null)` and `(null == x)` rewrite to `isnull(x)` (tests 22, 23)
- **Recursion**: bottom-up — children are rewritten before parents, so `((amount == null) and ...)` works (test 26)
- **NOT rewritten**: `(x != null)` remains as `BinOp("!=", Field("x"), Literal::Null)` (test 27)
- **Degenerate case**: `(null == null)` → `Call("isnull", [Literal::Null])` — eval of `isnull(null)` is `Bool(true)` (test 28)
- **Field preservation**: `(amount == null).referenced_fields()` still returns `{"amount"}` after rewrite (test 29)
- **Rationale**: `eval.rs` BinOp("==") stays strict-null per CONTEXT.md §D-04; this rewrite folds the SDK-COL-04 `.isnull()` sugar into the parser, so the evaluator never sees `BinOp("==", _, Null)`

## Interfaces Exported to 04-03/04-04

```rust
// Span — byte offsets into source
pub struct Span { pub start: usize, pub end: usize }

// ParseError — 1-indexed column with human-readable reason
pub struct ParseError { pub col: usize, pub reason: String }

// Literal scalar variants
pub enum Literal { Null, Bool(bool), Int(i64), Float(f64), Str(String), BareIdent(String) }

// Expression AST — every node carries a Span
pub enum Expr {
    Field { name: String, span: Span },
    Literal(Literal, Span),
    BinOp { op: String, left: Box<Expr>, right: Box<Expr>, span: Span },
    UnaryOp { op: String, operand: Box<Expr>, span: Span },
    Call { fn_name: String, args: Vec<Expr>, span: Span },
}

impl Expr {
    pub fn span(&self) -> Span { ... }
    pub fn referenced_fields(&self) -> BTreeSet<String> { ... }
}

// Entry point
pub fn parse(source: &str) -> Result<Expr, ParseError> { ... }
```

**Plan 04-03 dependencies:**
- `Expr` enum — pattern-matched by the evaluator
- `Literal` variants — converted to `Value` by the evaluator
- Pass B has already run — evaluator never sees `BinOp("==", _, Literal::Null)`

**Plan 04-04 dependencies:**
- `Expr::referenced_fields()` — used to validate field refs against propagated schema
- `ParseError { col }` — surfaced in register-time 400 responses as `"col N: ..."`

## Task Commits

1. **Task 1.a (red): 30 failing parser tests** — `7ce78bd` (test)
2. **Task 1.b (green): Full parser implementation** — `57349d1` (feat)

## Files Created/Modified

- `/Users/petrpan26/work/tally/crates/beava-core/src/expr.rs` — New module: Expr/Literal/Span/ParseError types, Lexer, Parser, normalize_cast, rewrite_null_eq, 30 unit tests + proptest (~1000 LoC)
- `/Users/petrpan26/work/tally/crates/beava-core/src/lib.rs` — Added `pub mod expr;` alphabetically between `defaults` and `op_node`
- `/Users/petrpan26/work/tally/.gitignore` — Added `proptest-regressions/` exclusion

## Decisions Made

- **`paren_depth` guard**: tracks unclosed `(` nesting; `parse_add/mul/cmp/or/and` only consume binary operators when `paren_depth > 0`. This is the cleanest way to enforce the SDK invariant at parse time without a separate validation pass.
- **Negative literals at Atom level**: leading `-` in Atom position, followed by a number token, emits a negative `Literal`. This matches `repr(-7)` = `"-7"` from Python. If `-` is not followed by a number, it errors (no unary minus on non-literals).
- **Post-parse normalization order**: Pass A (cast BareIdent) runs before Pass B (null-eq rewrite). Order doesn't matter for correctness here (they touch different patterns) but Pass A first is consistent with "structural normalization before semantic rewriting."
- **Bottom-up rewrite_null_eq**: recurse into children first, then check for null-eq at the current node. This ensures nested patterns like `((amount == null) and ...)` are rewritten correctly.

## Deviations from Plan

None — plan executed exactly as specified. The `paren_depth` mechanism for enforcing the parenthesization invariant was the executor's choice of implementation (the plan specified the *behavior* without prescribing the mechanism).

## Known Stubs

None. All public methods are fully implemented. `parse()`, `referenced_fields()`, and `span()` are all complete.

## Threat Flags

None. `expr.rs` is a pure in-memory parser with no network, file system, or trust-boundary surface. String inputs are parsed but not executed; the parser does not `eval()` anything — that's Plan 04-03's job.

## Self-Check

### Files exist

- `crates/beava-core/src/expr.rs` — FOUND
- `crates/beava-core/src/lib.rs` — FOUND (contains `pub mod expr;`)
- `.gitignore` — FOUND (contains `proptest-regressions/`)

### Commits exist

- `7ce78bd` — test(04-02) red commit
- `57349d1` — feat(04-02) green commit

### Gate results

- `cargo test -p beava-core expr::tests` — 30/30 PASSED
- `cargo test --workspace` — 278 tests PASSED, 0 failed
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` — CLEAN
- `cargo fmt --all --check` — CLEAN
- Proptest `proptest_sdk_strings_parse` — 256+ cases, 0 shrinks, PASSED

## Self-Check: PASSED

## Next Phase Readiness

- Plan 04-03 (evaluator): can import `beava_core::expr::{Expr, Literal, Span}` and match on AST variants; `BinOp("==")` never has a `Null` operand (Pass B ensures this)
- Plan 04-04 (schema propagation): can call `expr.referenced_fields()` to validate field references against the current schema after each op
- Plan 04-05 (register integration): calls `parse(op.expr)` for Filter/WithColumns/Map ops, maps `ParseError.col` to the `"col N: ..."` 400 response path
- Plan 04-06 (acceptance): can run end-to-end register → push → query with expressions parsed by this module

---
*Phase: 04-stateless-ops-expression-evaluator*
*Completed: 2026-04-23*
