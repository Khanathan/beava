---
phase: 05-aggregation-framework-core-operators
plan: "03"
subsystem: beava-core
tags: [aggregation, schema-propagation, fail-soft, tdd, type-inference]
dependency_graph:
  requires:
    - output_type_for (05-01, agg_op.rs)
    - AggOpDescriptor / AggKind (05-01, agg_op.rs)
    - Schema / DerivedSchema (schema_propagate.rs, schema.rs)
  provides:
    - AggregationDescriptor struct (agg_descriptor.rs)
    - NamedAggOp struct (agg_descriptor.rs)
    - propagate_aggregation_schema (agg_schema.rs)
    - AggSchemaError enum (agg_schema.rs)
  affects:
    - Plan 05-04: calls propagate_aggregation_schema at register time (Rule 11)
    - Plan 05-05: uses source_node_name for apply-loop event routing
    - Plan 05-06: uses feature_name for query-time lookup
tech_stack:
  added: []
  patterns:
    - Fail-soft error collection (Vec<AggSchemaError>) matching schema_propagate pattern
    - HashSet deduplication for group-key set + seen-feature-name set
    - Pre-call field-existence validation for F64-returning ops (Sum/Avg/Variance/StdDev)
      before delegating to output_type_for (which only validates Min/Max field presence)
key_files:
  created: []
  modified:
    - crates/beava-core/src/agg_descriptor.rs
    - crates/beava-core/src/agg_schema.rs
decisions:
  - "Field-existence check for Sum/Avg/Variance/StdDev added in propagator (not in output_type_for): output_type_for returns F64 unconditionally for those ops; propagate_aggregation_schema adds a pre-call field check so sum(field='nonexistent') surfaces FeatureTypeError at register time."
  - "lib.rs NOT touched — Plan 05-01 pre-declared both pub mod agg_descriptor and pub mod agg_schema; this plan overwrites file bodies only."
  - "GroupKeyCollidesWithFeature uses HashSet of valid (found) group keys only — if a group key was itself missing, its name is not in group_key_set, so a feature named the same as a missing key will not trigger this error (correct: the GroupKeyMissing error already covers it)."
metrics:
  duration_seconds: 180
  completed_date: "2026-04-23"
  tasks_completed: 2
  files_created: 0
  files_modified: 2
---

# Phase 5 Plan 03: AggregationDescriptor + Aggregation Schema Propagator Summary

`AggregationDescriptor` + `NamedAggOp` structs + `propagate_aggregation_schema` with fail-soft error collection for group-key validation, feature deduplication, collision detection, and per-op type inference via `output_type_for`.

## What Was Built

### Task 1.a (red) — Failing tests: `2a7834d`

**`crates/beava-core/src/agg_descriptor.rs`** (overwrite of 05-01 placeholder)

```rust
pub struct NamedAggOp {
    pub feature_name: String,
    pub descriptor: AggOpDescriptor,
}

pub struct AggregationDescriptor {
    pub node_name: String,
    pub source_node_name: String,
    pub group_keys: Vec<String>,
    pub features: Vec<NamedAggOp>,
}
```

2 tests: `named_aggop_new_constructs_cleanly`, `aggregation_descriptor_records_source_node_name`.

**`crates/beava-core/src/agg_schema.rs`** (overwrite of 05-01 placeholder)

`AggSchemaError` enum (5 variants) + `propagate_aggregation_schema` as `todo!()` stub + 14 failing tests.

### Task 1.b (green) — Implementation: `bf0ffb9`

`propagate_aggregation_schema` implementation:

**Step 1 — Group key validation (SDK-AGG-01):**
- Iterates `descriptor.group_keys`; any key absent from `upstream.fields` → `GroupKeyMissing`
- Valid keys inserted into output `fields` with inherited type; added to `group_key_set` HashSet

**Step 2 — Feature validation + type inference (SDK-AGG-03):**
- `DuplicateFeatureName` via `seen_feature_names` HashSet (`!insert` = duplicate)
- `GroupKeyCollidesWithFeature` when feature name is in `group_key_set` (T-05-03-01)
- Pre-call field-existence check for Sum/Avg/Variance/StdDev with `desc.field.is_some()` — surfaces `FeatureTypeError { field_missing: Some(name) }` before `output_type_for` (which returns F64 unconditionally for these ops)
- `output_type_for` call for Min/Max field resolution + all final type assignments

**Fail-soft:** all errors collected; `Err(errors)` only returned at the end if `!errors.is_empty()`.

## Tests Added

| Test | Validates |
|---|---|
| `named_aggop_new_constructs_cleanly` | NamedAggOp Debug + Clone |
| `aggregation_descriptor_records_source_node_name` | AggregationDescriptor fields accessible |
| `schema_includes_group_keys_with_upstream_types` | D-05 key type inheritance |
| `count_feature_infers_i64` | Count → I64 (SDK-AGG-03) |
| `sum_feature_infers_f64` | Sum → F64 (SDK-AGG-03) |
| `avg_feature_infers_f64` | Avg → F64 (SDK-AGG-03) |
| `min_feature_preserves_field_type` | Min → upstream type (SDK-AGG-03) |
| `variance_feature_infers_f64` | Variance → F64 (SDK-AGG-03) |
| `stddev_feature_infers_f64` | StdDev → F64 (SDK-AGG-03) |
| `ratio_feature_infers_f64` | Ratio → F64 (SDK-AGG-03) |
| `unknown_group_key_returns_error` | SDK-AGG-01 GroupKeyMissing |
| `missing_field_for_sum_returns_error` | FeatureTypeError for field-based ops |
| `duplicate_feature_names_rejected` | DuplicateFeatureName |
| `feature_name_collides_with_group_key_rejected` | T-05-03-01 GroupKeyCollidesWithFeature |
| `fail_soft_collects_all_errors` | Fail-soft: 2 missing keys + 1 missing field collected |
| `multiple_features_all_inferred_independently` | Multi-feature output schema shape |

**16 new tests.** Workspace total: 452 passed, 0 failed.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing critical functionality] Added field-existence validation for Sum/Avg/Variance/StdDev in propagator**
- **Found during:** Task 1.b (first test run — `missing_field_for_sum_returns_error` + `fail_soft_collects_all_errors` failed)
- **Issue:** `output_type_for` returns `Ok(F64)` unconditionally for Sum/Avg/Variance/StdDev — it does not check whether `desc.field` refers to an existing upstream column (only Min/Max need the field type so only they validate it). The plan's test `missing_field_for_sum_returns_error` expected `FeatureTypeError` for `sum(field="nonexistent")`, but got `Ok` schema.
- **Fix:** Added a pre-call field-existence check in `propagate_aggregation_schema` for the four F64-returning ops before delegating to `output_type_for`. Missing field → `FeatureTypeError { field_missing: Some(name) }` + `continue` (skips `output_type_for` call). Does not change `output_type_for` itself (that function's contract is intentionally type-only for those ops).
- **Files modified:** `crates/beava-core/src/agg_schema.rs`
- **Commit:** `bf0ffb9`

## Known Stubs

None. Both modules are fully implemented. No placeholder values or TODO comments flow to any output.

## Threat Flags

None. This plan introduces no network endpoints, auth paths, file access patterns, or schema changes at trust boundaries. Pure in-process Rust type arithmetic.

## Self-Check: PASSED

Files exist:
- `crates/beava-core/src/agg_descriptor.rs` — FOUND (overwritten from placeholder)
- `crates/beava-core/src/agg_schema.rs` — FOUND (overwritten from placeholder)

Commits exist:
- `2a7834d` — test(05-03): add failing tests for AggregationDescriptor + aggregation schema propagator
- `bf0ffb9` — feat(05-03): implement aggregation schema propagator with fail-soft error collection

lib.rs verification:
- `git diff HEAD -- crates/beava-core/src/lib.rs` → empty (untouched by 05-03)

Gates:
- `cargo test --workspace` — 452 passed, 0 failed
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` — clean
- `cargo fmt --all --check` — clean
- `grep -c output_type_for agg_schema.rs` — 9 (≥ 1)
- SDK-AGG-01 referenced in agg_schema.rs — confirmed
- SDK-AGG-03 referenced in agg_schema.rs — confirmed
