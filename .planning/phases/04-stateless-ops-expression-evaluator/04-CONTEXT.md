# Phase 4: Stateless ops + expression evaluator (server-side) - Context

**Gathered:** 2026-04-23
**Status:** Ready for planning
**Mode:** Interactive discuss under `/gsd-autonomous --interactive`
**Depends on:** Phase 2.5 (wire + opcodes), Phase 3 (SDK produces canonical expression strings)

<domain>
## Phase Boundary

Server-side execution of two things:

1. **Expression parser + evaluator** — Parse the canonical parenthesized string produced by `bv.col(...)` (v1's grammar, locked in Phase 3 D-08) into a typed AST. Evaluate the AST against a per-event `Row` producing a `Value`. Uniform grammar for filter predicates, `with_columns` derivations, and (Phase 5+) aggregation-spec `where=` clauses.
2. **Stateless op execution** — For each registered `Derivation` whose `ops` chain contains `Filter / Select / Drop / Rename / WithColumns / Map / Cast / Fillna`, execute the chain on each event BEFORE any aggregations see it. Op chain composes left-to-right; schema propagates through each step.

Scope (12 REQs: SDK-OPS-01..10, SRV-APPLY-06, SRV-APPLY-07):
- Expression parser with column-pointing error messages at register time
- Expression evaluator for the v1 grammar + `cast(x, 'type')` + `isnull(x)` builtin calls
- Row data model: `BTreeMap<String, Value>`, Value as tagged enum with SQL-style three-valued null logic
- 8 stateless ops execute server-side against a `Row`
- Schema propagation: register-time server computes the derived schema after each op in the chain (not just the last one)
- Register-time type checking: syntax + field-existence + basic operator type compatibility; runtime handles nulls + overflow
- `OpChain::execute(row)` entry point that runs all ops in sequence, returning `Option<Row>` (None = filter dropped it)
- Proptest correctness: random predicate + random event → truth-table match between SDK's client-side eval (if implemented) and server-side eval. Minimum: random expression + random row → deterministic evaluator output

Out of scope (deferred):
- Aggregation execution — Phase 5
- `filter`-inside-aggregation (`bv.count(where=...)`) — Phase 5; Phase 4 parser must be extensible (function-call hook) so Phase 5 adds it without grammar surgery
- WAL / idempotency around push — Phase 6
- Join ops — Phase 12
- MessagePack payload — Phase 6
- UDF / plugin expressions — out of v0

</domain>

<decisions>
## Implementation Decisions

### D-01: Hand-rolled recursive-descent parser with Span tracking

- New module `crates/beava-core/src/expr.rs` (or `expression.rs` — whichever matches v1 Rust `src/engine/expression.rs` conventions if they're informative)
- Grammar (from v1 `_col.py` — DO NOT change, server and SDK match):
  - `Expr = OrExpr`
  - `OrExpr = AndExpr ( 'or' AndExpr )*`
  - `AndExpr = NotExpr ( 'and' NotExpr )*`
  - `NotExpr = 'not' NotExpr | CmpExpr`
  - `CmpExpr = AddExpr ( ('>'|'>='|'<'|'<='|'=='|'!=') AddExpr )?`
  - `AddExpr = MulExpr ( ('+'|'-') MulExpr )*`
  - `MulExpr = Atom ( ('*'|'/') Atom )*`
  - `Atom = '(' Expr ')' | Call | Ident | Literal`
  - `Call = Ident '(' ArgList ')'`
  - `ArgList = Expr ( ',' Expr )* | ε`
  - `Literal = Number | SingleQuotedString | 'true' | 'false' | 'null'`
  - `Ident = [A-Za-z_][A-Za-z0-9_]* ( '.' [A-Za-z_][A-Za-z0-9_]* )?` (supports `Stream.field` for future scoping)
- AST: `Expr` enum with variants for each node; every node carries `span: Span { start: usize, end: usize }` (byte offsets into the source)
- Parser signature: `pub fn parse(source: &str) -> Result<Expr, ParseError>` where `ParseError { col: usize, reason: String }`
- Pure function, no I/O, no state. Easy to unit-test.
- ~300-400 LoC when done; use `Pratt` flavoring for `CmpExpr`/`AddExpr`/`MulExpr` if cleaner.

### D-02: Column-pointing parse errors

- `ParseError` structure serialized into the HTTP/TCP 400 response:
  ```json
  {
    "error": {
      "code": "invalid_expression",
      "path": "nodes[2].ops[1].expr",
      "reason": "col 14: expected ')' but found '>'"
    },
    "registry_version": N
  }
  ```
- `col` is 1-indexed byte offset from start of the expression string
- Users get `grep -n "(amount > "` → jump to character
- Register-time validation (register_validate.rs) calls `parse(expr)` for every `Filter`/`WithColumns`/`Map` op's `expr` field; accumulates errors fail-soft

### D-03: Row + Value data model

- New module `crates/beava-core/src/row.rs`:
  ```rust
  #[derive(Debug, Clone, PartialEq)]
  pub enum Value {
      Null,
      Str(String),
      I64(i64),
      F64(f64),       // NaN handled via is_nan() guards in Ord operations
      Bool(bool),
      Bytes(Vec<u8>),
      Datetime(i64),  // ms since epoch (matches event_time convention)
  }

  #[derive(Debug, Clone, PartialEq)]
  pub struct Row(pub BTreeMap<String, Value>);

  impl Row {
      pub fn get(&self, field: &str) -> Option<&Value> { ... }
      pub fn insert(&mut self, field: &str, v: Value) { ... }
      pub fn drop(&mut self, field: &str) { ... }
      pub fn rename(&mut self, old: &str, new: &str) { ... }
  }
  ```
- BTreeMap for deterministic iteration order (matters for Phase 5 aggregation keys and replay)
- Heap-allocated; Phase 13 perf pass can optimize if needed
- `Value` ↔ `FieldType` mapping: explicit table in `Value::type_of()`
- NaN for F64: comparisons return false, arithmetic propagates NaN. Null semantics take precedence (null vs NaN: null wins; `f64::NAN + null = null`)

### D-04: SQL-style three-valued null logic

- `null + anything` = `null`
- `null * 0` = `null` (not 0)
- `null == null` = `null` (NOT true; use `isnull(x)` to test)
- `null > anything` = `null`
- `filter((col > 100))` when col is null → row DROPPED (null predicate ≠ true)
- `true and null` = `null`
- `false and null` = `false` (short-circuit)
- `true or null` = `true` (short-circuit)
- `false or null` = `null`
- `not null` = `null`
- `.fillna(default)` replaces null with the default
- `isnull(x)` always returns `Bool(true/false)` — never null

This matches SQL, pandas (`.fillna` semantics), and polars. Industry-standard.

### D-05: Hybrid register-time vs runtime type checking

**At register time** (in `register_validate.rs` extension for Phase 4):
- Parse the expression (D-01)
- Walk the AST: every `Ident` → verify it exists in the current schema (after prior ops in the chain)
- Operator type compatibility table:
  - Arithmetic (`+ - * /`): both operands must be numeric (i64 or f64); result promotes i64+f64 → f64
  - Comparison (`> >= < <= == !=`): operands must be comparable (same or promotable type); result is `bool`
  - Boolean (`and or not`): operands must be bool (or null, which is always allowed); result is bool
  - `cast(x, 'type')`: x may be any type; 'type' is string literal `'str'|'int'|'float'|'bool'`. At register: validate 'type' is in the set; validate x's type has a legal cast to the target (e.g., `cast(str, 'bool')` is rejected).
  - `isnull(x)`: x may be any type; returns bool
- If any rule fails → `invalid_expression` 400 with `path: "nodes[N].ops[M].expr"` + reason
- Register-time does NOT check: overflow, runtime type of f64 NaN, `cast` failure at value level (e.g., `cast('abc', 'float')` — Rust's parse fails at runtime)

**At runtime** (evaluator):
- Executes against actual `Row` values
- Null propagation per D-04
- Overflow on arithmetic: i64 saturates; f64 follows IEEE-754 (NaN/Inf)
- Cast failures: return `Value::Null` (don't crash the row — drop it via filter if user cares)

### D-06: Server-side schema propagation at register time

- New module or extension: `crates/beava-core/src/schema_propagate.rs` (or add to `schema.rs`)
- Entry point: `pub fn propagate_schema(upstream_schemas: &[Schema], ops: &[OpNode]) -> Result<Schema, ValidationError>`
- For each op in sequence, transform the current schema:
  - `Filter { expr }`: schema unchanged (type-check expr produces bool)
  - `Select { fields }`: keep only listed fields; error if any field not in schema
  - `Drop { fields }`: remove listed fields; error if any field not in schema
  - `Rename { mapping }`: apply the mapping; error on collision
  - `WithColumns { exprs }` / `Map { exprs }`: for each `(name, expr)`, type-check expr against current schema, add `name: inferred_type` (overwrites if already present)
  - `Cast { type_map }`: for each `(field, type)`, validate field exists + type is castable, replace field's type in schema
  - `Fillna { defaults }`: for each field in defaults, clear the `optional_fields` flag on that field
  - `GroupBy` / `Join` / `Union`: Phase 5/12 — for Phase 4, reject or pass through unchanged (these ops aren't in Phase 4 scope; we see them in the DAG but don't propagate through them yet)
- Returned `Schema` is the derived output schema; also stored server-side (used by Phase 5 aggregations + downstream derivations)
- Replaces Phase 2's "trust client-supplied derived schema" for op chains; Phase 2 still trusts for aggregation/join schemas pending Phase 5/12
- Does not mutate the descriptor — returns an updated Schema that the register endpoint stores alongside

### D-07: Function-call syntax as extension hook

- `cast(x, 'type_literal')` and `isnull(x)` are the Phase 4 builtins
- Grammar production `Call = Ident '(' ArgList ')'` is the extension point; Phase 5+ adds more builtins without grammar changes
- Builtin dispatch via a table:
  ```rust
  // crates/beava-core/src/expr_builtins.rs
  pub struct BuiltinFn {
      pub name: &'static str,
      pub arity: Arity,  // Fixed(n), Variadic
      pub type_check: fn(arg_types: &[FieldType]) -> Result<FieldType, TypeError>,
      pub eval: fn(args: &[Value]) -> Value,
  }

  pub const BUILTINS: &[BuiltinFn] = &[
      BuiltinFn { name: "cast", arity: Arity::Fixed(2), type_check: cast_check, eval: cast_eval },
      BuiltinFn { name: "isnull", arity: Arity::Fixed(1), type_check: isnull_check, eval: isnull_eval },
  ];
  ```
- Phase 5 extension: add `bv.count(where=filter_expr)` parses the filter expression the same way; aggregation-spec parameters are expressions too
- Phase 4 rejects unknown function names at register time: `unknown_function 'foo' at col N`

### D-08: No broad-builtin set in Phase 4

- Ship `cast` + `isnull` only. These cover every Phase 4 REQ.
- Explicitly NOT in Phase 4: `len`, `lower`, `upper`, `now`, `coalesce`, `abs`, `floor`, `ceil`, etc. Each has its own tests, its own type rules, its own edge cases. Add them as downstream phases need them (or in Phase 13 if v0 user feedback demands)
- SDK-COL-04 says `.isnull()` sugars to `(x == null)` client-side → actually re-reading: SDK-COL-04 in REQUIREMENTS.md defines `.isnull()` produces `(x == null)` expression. So Phase 4 server may not even see `isnull(x)` as a function call — only the `==` form. RESOLUTION: support BOTH at the server (the SDK canonical form is `(x == null)` but users can also emit `isnull(x)` via raw expressions or future SDK methods; cheap to handle both; `(x == null)` is the primary path through evaluator)

### Claude's Discretion

- Exact module file naming (`expr.rs` vs `expression.rs` — prefer the shorter)
- Pratt vs pure recursive-descent inside the parser — equivalent output, pick what reads cleaner
- Whether `Value::F64(f64)` implements `Eq` (standard Rust answer: no — NaN). Implement custom `PartialEq` and guard ordering operations
- `Span` representation (byte offsets vs line/column) — byte offsets are fine for single-line expressions; line info matters only if we ever support multi-line
- Tokenizer: separate module or inline in parser — for a grammar this size, inlined tokenizer is cleaner
- Proptest strategy shape — `(arb_expr, arb_row)` → `evaluator.eval(expr, row)` terminates deterministically; specific type-compatibility invariants documented per-op
- Whether to ship a `bench/` harness for the evaluator — Phase 13 owns perf; Phase 4 has correctness proptests only

### Folded Todos

None.

</decisions>

<canonical_refs>
## Canonical References

### Locked wire + grammar contracts
- `.planning/phases/03-python-sdk-skeleton-decorators-expression-dsl/03-CONTEXT.md` §D-08 — canonical parenthesized grammar (server Phase 4 parses EXACTLY what SDK emits)
- `.planning/phases/02-sources-registry-version-bumps/02-CONTEXT.md` §Key locked wire contracts — register JSON shape; ops carry `expr: String`
- `crates/beava-core/src/op_node.rs` — OpNode::Filter/WithColumns/Map already carry `expr: String`; Phase 4 makes those strings meaningful
- `crates/beava-core/src/register_validate.rs` — extension point for expression parsing + schema propagation
- `crates/beava-core/src/schema.rs` — FieldType enum; Value enum mirrors it

### Project-level
- `.planning/PROJECT.md` §Key Decisions — Python SDK canonical authoring; expression DSL; devex-first naming (all apply to error messages too)
- `.planning/REQUIREMENTS.md` §SDK-OPS, §SRV-APPLY — 12 REQs in Phase 4 scope

### v1 reference (read-only)
- `git show main:python/beava/_col.py` — grammar definition (the Python side of the contract)
- `git show main:src/engine/expression.rs` — v1's Rust parser (reference for the recursive-descent shape; v2 re-implements but can borrow structural patterns)

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable assets
- `OpNode` enum (op_node.rs): Filter/Select/Drop/Rename/WithColumns/Map/Cast/Fillna/GroupBy/Join/Union variants already exist — Phase 4 only needs to execute the first 8
- `EventSchema`/`TableSchema`/`DerivedSchema` + `FieldType` enum: reuse for type propagation; `Value` enum mirrors `FieldType`
- `register_validate.rs`: extension point for expression parsing + schema propagation; error format already uses `{code, path, reason}`
- `defaults.rs`: defaults module exists; Phase 4 may add `DEFAULT_ROW_VALUE_CAP` or similar

### Established patterns
- `thiserror` for library errors, `anyhow` for HTTP boundary
- `tracing` with structured `kind` field
- `parking_lot::RwLock` for registry; evaluator is stateless so no locking concerns
- `serde` round-trip tests for any wire type

### Integration points
- Register endpoint (`register.rs::execute_register` after Phase 2.5's refactor): adds a step "parse expressions + propagate schema" to the validation pass
- Future push handler (Phase 6): calls `OpChain::execute(row)` after WAL-acking the event; Phase 4 exposes the function but no push path exercises it yet — Phase 4 smoke uses a direct function-call test harness
- Phase 5 aggregation: builds on Row + Value + schema-propagation infrastructure

</code_context>

<specifics>
## Specific Ideas

- **Proptest the evaluator against a simple reference**: for deterministic expressions with random i64 rows, compare evaluator output to a direct Rust calculation. Covers `(a + b) > c` with wraparound, null propagation, casts.
- **Parser error messages are UX** — if `(amount > ` trails off, reason should say `col 12: expected expression after '>'` not `unexpected EOF`. Catch EOF at expected-expression boundaries.
- **BTreeMap<String, Value>** iteration order is stable — Phase 5 depends on this for aggregation-key determinism.
- **Boolean short-circuit** is REQUIRED to match SQL (avoids evaluating `b` in `false and b`). Important for future `where=` that might have expensive checks.
- **Cast errors at runtime return Null, not panic** — `cast('abc', 'int')` produces Null Value; a filter on null-dropping can remove those rows. No exceptions up the stack.

</specifics>

<deferred>
## Deferred Ideas

- **Broader builtin set** (`len`, `lower`, `upper`, `now`, `coalesce`, `abs`, `floor`, `ceil`, `substring`, `trim`, date math) — add per-phase as needs emerge or in Phase 13 if v0 users request
- **User-defined functions / plugin hooks** — out of v0 per PROJECT.md
- **Multi-line expressions** — grammar stays single-line; no need in v0
- **Query-language alternative to bv.col** — SQL parser etc. out of v0
- **Zero-copy row views** — Phase 13 perf pass
- **Batch evaluator** — single-row evaluator for v0 (matches push's single-row semantics)
- **Aggregation-spec expressions (`bv.count(where=...)`)** — Phase 5
- **Join predicate expressions** — Phase 12

</deferred>

---

*Phase: 04-stateless-ops-expression-evaluator*
*Context gathered: 2026-04-23*
*Discuss mode: interactive (gsd-autonomous --interactive)*
*Depends on Phase 2.5 (wire; executor in flight) + Phase 3 (SDK DSL; planning pending)*
