---
phase: "04"
plan: "04"
subsystem: beava-core
tags: [op-chain, schema-propagation, filter, select, drop, rename, with_columns, map, cast, fillna, sdk-col-07, srv-apply-07]
dependency_graph:
  requires: ["04-01", "04-02", "04-03"]
  provides: ["OpChain::compile", "OpChain::apply", "propagate_schema", "PropagationError"]
  affects: ["04-05-register-wire", "04-06-end-to-end"]
tech_stack:
  added: []
  patterns: ["fail-soft error collection", "InferredType NullLiteral polymorphism", "compile-time expression caching"]
key_files:
  created:
    - crates/beava-core/src/op_chain.rs
    - crates/beava-core/src/schema_propagate.rs
  modified:
    - crates/beava-core/src/lib.rs
key_decisions:
  - "InferredType::NullLiteral for polymorphic null handling in type inference"
  - "propagate_schema returns (Schema, Vec<Schema>) — per-step snapshots for Phase 5"
  - "fail-soft error collection: all errors gathered, carry-forward best-effort schema"
  - "Division always widens to F64 at type-inference time (mirrors eval.rs runtime)"
  - "Filter short-circuits at apply time: None returned immediately, skipping all remaining ops"
  - "Bytes type is isolated: no casts in or out at register time"
metrics:
  duration_minutes: 35
  completed_date: "2026-04-23"
  tasks_completed: 2
  files_changed: 3
  tests_added: 43
  workspace_test_total: 272
---

# Phase 04 Plan 04: OpChain::compile + apply Summary

**One-liner:** Op-chain executor with compile-time schema propagation and SDK-COL-07 field-reference validation for all 8 stateless ops (Filter/Select/Drop/Rename/WithColumns/Map/Cast/Fillna).

## What Was Built

Two new `beava-core` modules wiring together Plans 04-01/02/03 into a register-time compiler + per-row executor:

**`schema_propagate.rs`** — Register-time schema derivation:
- `Schema` neutral transport type (structurally identical to Event/Table/DerivedSchema)
- `InferredType` enum: `Known(FieldType)` | `NullLiteral` — polymorphic null handling
- `PropagationError`: FieldMissing, TypeMismatch, RenameCollision, InvalidExpr, UnsupportedOp
- `propagate_schema(input, ops) -> Result<(Schema, Vec<Schema>), Vec<PropagationError>>` — fail-soft
- `infer_expr_type(expr, schema)` — public entry point for expression type inference
- Per-op schema transforms: Filter (no-op), Select (keep), Drop (remove), Rename (atomic), WithColumns/Map (add/overwrite), Cast (type replace), Fillna (clear optional flag)
- GroupBy/Join/Union → UnsupportedOp (Phase 5/12 deferred)

**`op_chain.rs`** — Compile-once, apply-many executor:
- `OpChain::compile(input_schema, &[OpNode]) -> Result<(OpChain, Schema), Vec<CompileError>>`
  - Calls `propagate_schema` first (full validation)
  - Parses and caches `Expr` ASTs for Filter/WithColumns/Map
  - Materialises `CastTarget` enum from type strings
  - Converts `serde_json::Value` defaults to `Value` for Fillna
- `OpChain::apply(&self, row: Row) -> Option<Row>`
  - Filter short-circuits: `Bool(true)` keeps, anything else drops (returns `None`)
  - All other ops return `Some(updated_row)`
  - Cast delegates to `lookup_builtin("cast")` at runtime (Null on failure)

## Key Design Decisions

### 1. InferredType::NullLiteral — Polymorphic null handling

Rather than a `FieldType::Null` variant, type inference uses an `InferredType` wrapper:
- `Known(FieldType)` — concrete type known at register time
- `NullLiteral` — null literal is polymorphic; compatible with any type in comparisons and arithmetic

This lets `(amount > null)` type-check without error — the null propagates at runtime per §D-04.

### 2. fail-soft error collection

`propagate_schema` collects all errors across all ops before returning `Err(Vec<PropagationError>)`. Each op applies a best-effort carry-forward:
- Select with unknown field: skip that field in the synthesized schema
- Drop with unknown field: record error, continue with unchanged schema
- Cast with bad target: record error, leave type unchanged
- WithColumns with parse error: record error, skip that field

This mirrors `register_validate.rs`'s existing pattern and gives users all errors in one response.

### 3. Filter short-circuit in apply

`OpChain::apply` returns `None` immediately when a Filter drops a row. This is correct SQL semantics (null predicate drops the row) and also efficient — no subsequent ops run on a dropped row.

### 4. Division widens to F64 at type-inference time

Per the plan spec (mirroring `python/beava/_col.py::infer_output_type`): `I64 / I64 → F64` at the type-inference level, even though the runtime evaluator performs integer division for `I64/I64`. This means `Cast{{is_div: "int"}}` after a division expression will type-check correctly at register time. The type inference is register-time contract; runtime follows eval.rs semantics.

### 5. Legal-cast matrix (Bytes is isolated)

| Source   | str | int | float | bool |
|----------|-----|-----|-------|------|
| Str      | ✓   | ✓   | ✓     | ✓    |
| I64      | ✓   | ✓   | ✓     | ✓    |
| F64      | ✓   | ✓   | ✓     | ✓    |
| Bool     | ✓   | ✓   | ✓     | ✓    |
| Datetime | ✓   | ✓   | ✓     | ✓    |
| Bytes    | ✗   | ✗   | ✗     | ✗    |

Bytes has no implicit encoding spec → all casts rejected at register time (TypeMismatch).

## SDK-COL-07 Shared Ownership

SDK-COL-07 ("Schema-reference validation at register time") is satisfied across three plans:

| Plan | Contribution |
|------|-------------|
| 04-02 (parser) | `referenced_fields()` collector on `Expr` — enumerates all field names used in an expression |
| **04-04 (this)** | `check_referenced_fields()` in `propagate_schema` — validates every field against the current per-step schema; `FieldMissing { op_index, field }` error with pinpointed location |
| 04-05 (register wire) | Surfaces `PropagationError::FieldMissing` as HTTP/TCP `invalid_expression` 400 with `path: "nodes[N].ops[M]"` |

The requirement is only end-to-end satisfied after Plan 04-05 lands the wire-error surface.

## Interfaces Exported to Plan 04-05

```rust
// schema_propagate.rs
pub fn propagate_schema(input: &Schema, ops: &[OpNode])
    -> Result<(Schema, Vec<Schema>), Vec<PropagationError>>;
pub enum PropagationError { FieldMissing, TypeMismatch, RenameCollision, InvalidExpr, UnsupportedOp }
pub struct Schema { pub fields: BTreeMap<String, FieldType>, pub optional_fields: Vec<String> }
impl Schema { pub fn from_event/from_table/from_derived/into_derived }

// op_chain.rs
pub struct OpChain;
pub use PropagationError as CompileError;
impl OpChain {
    pub fn compile(input_schema: &Schema, ops: &[OpNode])
        -> Result<(OpChain, Schema), Vec<CompileError>>;
    pub fn apply(&self, row: Row) -> Option<Row>;
}
```

Plan 04-05 calls `OpChain::compile` inside `execute_register` to validate the op chain and compute the derived schema; stores the `OpChain` in the descriptor for use by Plan 04-06's push handler.

## Test Results

- **43 new tests** added (28 schema_propagate + 15 op_chain)
- **272 total workspace tests** — 0 failures
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` — clean
- `cargo fmt --all --check` — clean

## Commits

| Hash | Message |
|------|---------|
| `test(04-04)` | add failing op-chain + schema-propagation tests |
| `5bdbbea` | feat(04-04): implement op-chain compile+apply + schema propagator + SDK-COL-07 |

## Deviations from Plan

None — plan executed exactly as written.

## Self-Check: PASSED

- op_chain.rs: FOUND
- schema_propagate.rs: FOUND
- 04-04-SUMMARY.md: FOUND
- RED commit `73d2561` (test(04-04)): FOUND
- GREEN commit `5bdbbea` (feat(04-04)): FOUND
- 272/272 tests pass, 0 failures
