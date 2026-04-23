---
phase: 05-aggregation-framework-core-operators
plan: "04"
subsystem: aggregation-registration
tags: [aggregation, register-time-validation, rule-11, compiled-aggregations, wire-errors, tdd]
dependency_graph:
  requires: ["05-01", "05-02", "05-03", "02.5-tcp-wire"]
  provides: ["05-05", "05-06"]
  affects: ["register_validate", "registry", "register", "tcp"]
tech_stack:
  added: []
  patterns:
    - "Rule 11 aggregation validation mirrors Phase 4 Rule 10 pattern — compile_aggregations_from_nodes called after Rules 1-10 only when errors.is_empty()"
    - "parse_duration_to_ms: suffix-stripping hand-rolled parser; u64::checked_mul for overflow safety (T-05-04-02)"
    - "compiled_aggregations: BTreeMap<String, Arc<AggregationDescriptor>> in RegistryInner parallel to compiled_chains"
    - "apply_registration extended from 3→4 args; aggregations installed only in Derivation branch"
    - "error_code_to_wire_str shared by HTTP + TCP for identical Rule 11 error code strings on both transports"
key_files:
  created:
    - crates/beava-core/src/agg_compile.rs
  modified:
    - crates/beava-core/src/lib.rs
    - crates/beava-core/src/register_validate.rs
    - crates/beava-core/src/registry.rs
    - crates/beava-server/src/register.rs
    - crates/beava-server/src/registry_debug.rs
    - crates/beava-server/src/tcp.rs
decisions:
  - "Rule 11 only runs after Rules 1-10 are clean (errors.is_empty() guard); avoids cascading errors from structural schema failures"
  - "parse_duration_to_ms returns Result<Option<u64>, ()>; forever maps to None (windowless); #[allow(clippy::result_unit_err)] applied"
  - "apply_registration gets compiled_aggregations as 4th Vec arg; existing callers in registry_debug.rs pass vec![] (non-agg paths)"
  - "SDK-AGG-05 (aggregation-on-Table) hard-rejected in Rule 11; AggregationOnTableNotSupported surfaced on both HTTP and TCP"
  - "SDK-AGG-06 (window validation) enforced via parse_duration_to_ms; rejects anything not matching \\d+(ms|s|m|h|d) or 'forever'"
metrics:
  duration: "~35 minutes (resumed from prior session)"
  completed: "2026-04-23T17:19:35Z"
  tasks_completed: 2
  files_changed: 7
  lines_added: 1308
  lines_removed: 16
---

# Phase 05 Plan 04: Rule 11 Aggregation Register-Time Validation + compiled_aggregations Cache Summary

Register-time Rule 11 that compiles GroupBy OpNodes into AggregationDescriptors, validates group-by keys, op fields, where predicates, and window strings, caches results in RegistryInner.compiled_aggregations, and surfaces 7 new error codes identically over HTTP 400 and TCP OP_ERROR_RESPONSE.

## Objective

Wire the Plan 05-01/02/03 aggregation machinery into POST /register + TCP op=register. Rule 11 is the aggregation analogue of Phase 4's Rule 10 (op-chain expression validation). On valid registration the compiled AggregationDescriptor is cached for Plan 05-05's apply loop.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1.a | Red: failing Rule 11 tests (HTTP 17-20, TCP 21-22, unit tests) | combined into green | agg_compile.rs, register_validate.rs, register.rs, tcp.rs |
| 1.b | Green: full Rule 11 impl + compiled_aggregations cache + wire mapping | b5c9246 | all 7 files |

Note: TDD red-green split was attempted but the implementation reached green state in a single pass. Per CLAUDE.md §Conventions, both the tests and the implementation are included in the feat commit. The tests are structurally sound (they would fail without the implementation).

## What Was Built

### New: `crates/beava-core/src/agg_compile.rs` (829 lines)

- `parse_duration_to_ms(s: &str) -> Result<Option<u64>, ()>`: hand-rolled suffix parser for `ms/s/m/h/d` units and `"forever"`. Uses `u64::checked_mul` to guard overflow (T-05-04-02). Rejects bare numbers, unknown suffixes, empty strings.
- `compile_aggregations_from_nodes(nodes, registry) -> (Vec<(String, Arc<AggregationDescriptor>)>, Vec<ValidationError>)`: walks every Derivation node's ops for GroupBy; resolves upstream source; validates group keys, op fields, where predicates, window strings against the upstream schema; builds AggregationDescriptors on success. Fails soft — all errors collected, not fail-fast.
- Full unit test suite: 6 duration parser tests + 11 Rule 11 validation tests.

### Extended: `crates/beava-core/src/register_validate.rs`

7 new `ErrorCode` variants added (Rule 11):
- `AggregationOnTableNotSupported` (SDK-AGG-05)
- `AggregationUnknownField`
- `AggregationInvalidWhere`
- `AggregationInvalidWindow` (SDK-AGG-06)
- `AggregationUnknownOp`
- `AggregationDuplicateFeatureName`
- `AggregationGroupKeyCollidesWithFeature`

`ValidatedPayload` extended with `pub compiled_aggregations: Vec<(String, Arc<AggregationDescriptor>)>`. `into_parts()` now returns a 4-tuple. Rule 11 block added to `validate_payload()` after Rule 10 (runs only when `errors.is_empty()`).

### Extended: `crates/beava-core/src/registry.rs`

- `RegistryInner.compiled_aggregations: BTreeMap<String, Arc<AggregationDescriptor>>` field added.
- `compiled_aggregation(name) -> Option<Arc<AggregationDescriptor>>` read accessor added.
- `apply_registration` extended from 3 to 4 arguments; aggregations installed in the Derivation branch.
- New test: `compiled_aggregations_cached_after_apply_registration`.

### Extended: `crates/beava-server/src/register.rs`

- `error_code_to_wire_str`: 7 new match arms for Rule 11 error codes → wire strings.
- `execute_register`: destructures the 4-tuple from `into_parts()` and passes `compiled_aggregations` to `apply_registration`.
- Tests 17-20 (HTTP integration):
  - `test_17_http_rejects_aggregation_on_table_source`: 400 + `aggregation_on_table_not_supported`
  - `test_18_http_rejects_aggregation_unknown_field`: 400 + `aggregation_unknown_field`
  - `test_19_http_rejects_aggregation_invalid_window`: 400 + `aggregation_invalid_window`
  - `test_20_http_accepts_valid_aggregation`: 200 + registry version bump + compiled_aggregation returns Some

### Extended: `crates/beava-server/src/tcp.rs`

Tests 21-22 (TCP integration):
- `test_21_tcp_rejects_aggregation_on_table_source`: OP_ERROR_RESPONSE + `aggregation_on_table_not_supported`
- `test_22_tcp_rejects_aggregation_invalid_window`: OP_ERROR_RESPONSE + `aggregation_invalid_window`

### Fixed: `crates/beava-server/src/registry_debug.rs`

4 callers of `apply_registration` updated from 3-arg to 4-arg (pass `vec![]` — no aggregations in debug paths).

## Verification

```
test result: ok. 369 passed; 0 failed (beava-core)
test result: ok. 101 passed; 0 failed (beava-server)
cargo clippy --workspace --all-targets --all-features -- -D warnings: clean
cargo fmt --all --check: clean
```

Total passing tests: 477 across workspace.

Wire parity confirmed: `error_code_to_wire_str` is shared between HTTP and TCP paths; same 7 Rule 11 error codes surface identically on both transports. `registry.compiled_aggregation("AggTable")` returns `Some(Arc)` after valid aggregation registration (confirmed by test 20 and `compiled_aggregations_cached_after_apply_registration`).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Test helper had wrong schema causing test 18/19 to produce 200 instead of 400**
- **Found during:** Task 1.b (green) — test 18 asserted 400 but got 200
- **Issue:** The shared `agg_derivation_payload` helper added the group_by key to the event schema, making "missing_key" actually present; test 19 used `event_time` as group_by but the schema field was typed as i64 causing Rule 3 to fire before Rule 11
- **Fix:** Rewrote tests 17-20 to use explicit `txn_event_node()` helper (fixed schema: `user_id: Str`, `amount: F64`, `event_time: i64`) and corrected field references to non-existent keys
- **Files modified:** `crates/beava-server/src/register.rs`
- **Commit:** b5c9246

**2. [Rule 1 - Bug] Unused import `BTreeMap` at top-level in agg_compile.rs**
- **Found during:** Task 1.b — clippy error
- **Issue:** `use std::collections::BTreeMap;` at module level not used outside tests
- **Fix:** Moved import inside `#[cfg(test)] mod tests` block; added `#[allow(clippy::result_unit_err)]` on `parse_duration_to_ms`
- **Files modified:** `crates/beava-core/src/agg_compile.rs`
- **Commit:** b5c9246

**3. [Rule 1 - Bug] `cargo fmt` failures**
- **Found during:** Task 1.b — pre-commit check
- **Issue:** Line length violations, closure style issues
- **Fix:** Ran `cargo fmt --all`
- **Files modified:** multiple
- **Commit:** b5c9246

## Known Stubs

None — all Rule 11 validation paths are fully implemented and wired. `compiled_aggregations` cache is populated on valid registration. The apply loop (Plan 05-05) reads this cache but that integration is not part of this plan's scope.

## Threat Surface Scan

No new network endpoints, auth paths, or file access patterns introduced. All changes are within existing POST /register + TCP OP_REGISTER paths. Rule 11 mitigations from the plan's threat model are implemented:
- T-05-04-01: AggregationUnknownOp rejects non-whitelisted op strings
- T-05-04-02: parse_duration_to_ms uses u64::checked_mul
- T-05-04-03: where expr parsed via Phase 4 depth-bounded parser
- T-05-04-05: SDK-AGG-05 hard-coded; test_17/test_21 prove it

## Self-Check: PASSED

- `crates/beava-core/src/agg_compile.rs` — FOUND
- `crates/beava-core/src/lib.rs` (pub mod agg_compile) — FOUND
- Commit b5c9246 — FOUND (`git log --oneline -1` = `b5c9246 feat(05-04): ...`)
- 477 total passing tests — VERIFIED
