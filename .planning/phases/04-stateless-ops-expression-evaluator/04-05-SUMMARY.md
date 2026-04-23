---
phase: "04"
plan: "05"
subsystem: beava-server/register-pipeline
tags: [expression-validation, op-chain, schema-propagation, rule10, register, tcp-parity]
dependency_graph:
  requires: [04-02, 04-03, 04-04]
  provides: [expression-validation-on-register, op-chain-caching, invalid-expression-wire-code]
  affects: [beava-core/register_validate, beava-core/registry, beava-server/register, beava-server/tcp]
tech_stack:
  added: [tracing dependency in beava-core]
  patterns: [rule10-validate-expressions, into_parts-decomposition, error-code-to-wire-str, propagated-schema-D06]
key_files:
  created: []
  modified:
    - crates/beava-core/src/register_validate.rs
    - crates/beava-core/src/registry.rs
    - crates/beava-core/Cargo.toml
    - crates/beava-server/src/register.rs
    - crates/beava-server/src/registry_debug.rs
    - crates/beava-server/src/tcp.rs
decisions:
  - "error_code_to_wire_str() centralises HTTP+TCP wire-code dispatch; both transports call the same function"
  - "UnsupportedOp (GroupBy/Join/Union) treated as pass-through warning (tracing::warn), not error, per Phase 4 scope"
  - "apply_registration signature extended to (nodes, compiled_chains, propagated_schemas) — no separate method"
  - "tracing added to beava-core dependencies to support Rule 10 UnsupportedOp warn call"
metrics:
  duration_minutes: 35
  completed_date: "2026-04-23"
  tasks_completed: 5
  files_modified: 6
---

# Phase 4 Plan 05: Register Pipeline — Rule 10 Expression Validation + Op-Chain Caching Summary

Rule 10 expression validation wired into the POST /register pipeline: OpChain::compile validates every derivation's ops against propagated schemas at register time, compiled chains cached in RegistryInner, and HTTP+TCP both surface errors as `"invalid_expression"`.

## What Was Built

### Task 1 (red) — Failing integration tests
Commit `ac51ed8`: 6 failing tests in `beava-server/src/register.rs`:
- Test 11: bad Filter expression → 400 with `code="invalid_expression"`
- Test 12: unknown field in filter → 400 with `code="invalid_expression"`
- Test 13: invalid cast target → 400 with `code="invalid_expression"`
- Test 14: valid chained ops → 200; propagated schema visible via GET /registry
- Test 15: compiled chain cached in `registry.compiled_chain("D")` after register
- Test 16: TCP frame with bad filter → OP_ERROR_RESPONSE with `code="invalid_expression"`

### Task 2 (green, Step 1–2) — Rule 10 implementation in `register_validate.rs`
- Added `ErrorCode` variants: `InvalidExpression`, `UnknownFieldReference`, `SchemaPropagationFailure`, `InvalidCastTarget`, `UnsupportedOpInPhase4`
- Changed `ValidatedPayload` from newtype to struct with `nodes`, `compiled_chains`, `propagated_schemas`
- Added `into_parts()` decomposition method; `into_inner()` kept as backward-compat alias
- `validate_expressions()` function: topological batch propagation, upstream schema resolution, `OpChain::compile`, fail-soft error collection
- Helper functions: `resolve_upstream_schema()`, `union_schemas()`, `propagation_error_to_validation()`
- UnsupportedOp ops (GroupBy/Join/Union) treated as pass-through (tracing::warn), not error
- 10 Rule 10 unit tests added and passing

### Task 3 (green, Step 3) — `Registry::apply_registration` extended
- Signature extended: `(nodes, compiled_chains, propagated_schemas)`
- Server-propagated schemas overwrite client-supplied derivation schemas (CONTEXT D-06)
- Compiled chains installed atomically into `RegistryInner.compiled_chains`
- `registry.compiled_chain(name)` accessor exposed for tests and future push-path

### Task 4 (green, Step 4) — `execute_register` uses `into_parts()`
- `validated.into_inner()` replaced with `validated.into_parts()` destructuring
- `compiled_chains` and `propagated_schemas` passed through to `apply_registration`

### Task 5 (green, Step 5) — Error code mapping on both transports
- `error_code_to_wire_str(ErrorCode) -> &'static str` helper in register.rs (pub(crate))
- Maps `InvalidExpression | UnknownFieldReference | SchemaPropagationFailure | InvalidCastTarget` → `"invalid_expression"`
- All other codes → `"invalid_registration"` (structural rules 1-9)
- HTTP `map_outcome_to_http` and TCP `handle_register` both call this function

## Test Results

| Suite | Before | After |
|-------|--------|-------|
| beava-core | 272 pass | 282 pass (+10 Rule 10 unit tests) |
| beava-server | 85 pass, 6 fail | 91 pass, 0 fail |
| **Total** | **357 pass, 6 fail** | **373 pass, 0 fail** |

## Commits

| Commit | Type | Description |
|--------|------|-------------|
| `ac51ed8` | test(04-05) | Add 6 failing integration tests (red) |
| `b2b69db` | feat(04-05) | Wire Rule 10 into register pipeline (green) |

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] `tracing` crate not in beava-core dependencies**
- **Found during:** Task 2 (green)
- **Issue:** `validate_expressions()` calls `tracing::warn!` for UnsupportedOp pass-through, but beava-core had no tracing dependency
- **Fix:** Added `tracing = { workspace = true }` to `crates/beava-core/Cargo.toml`
- **Files modified:** `crates/beava-core/Cargo.toml`
- **Commit:** `b2b69db`

**2. [Rule 1 - Bug] Unused `BTreeMap` import after ValidatedPayload struct change**
- **Found during:** Task 2 (green) — first compile attempt
- **Issue:** Import remained from earlier draft; clippy would have rejected it
- **Fix:** Removed `BTreeMap` from the `use std::collections::...` import line
- **Files modified:** `crates/beava-core/src/register_validate.rs`
- **Commit:** `b2b69db`

**3. [Rule 3 - Blocking] `registry_debug.rs` called `apply_registration` with old 1-arg signature**
- **Found during:** Task 3 (green) — workspace build after apply_registration signature change
- **Issue:** Test helpers in registry_debug.rs used the old single-argument form
- **Fix:** Updated both calls to `apply_registration(nodes, vec![], vec![])`
- **Files modified:** `crates/beava-server/src/registry_debug.rs`
- **Commit:** `b2b69db`

## Known Stubs

None — all six integration tests exercise real OpChain::compile paths. No hardcoded empty values flow to observable outputs.

## Threat Flags

None — no new network endpoints, auth paths, or trust-boundary schema changes introduced. Rule 10 is a pure register-time validation layer.

## Self-Check

Files exist check:
- `crates/beava-core/src/register_validate.rs` — FOUND
- `crates/beava-core/src/registry.rs` — FOUND
- `crates/beava-server/src/register.rs` — FOUND
- `crates/beava-server/src/tcp.rs` — FOUND

Commits exist check:
- `ac51ed8` (red) — FOUND
- `b2b69db` (green) — FOUND

## Self-Check: PASSED
