//! Aggregation schema propagator: upstream schema + AggregationDescriptor → DerivedSchema.
//!
//! `propagate_aggregation_schema` computes the output `DerivedSchema` for a
//! `group_by().agg()` aggregation at register time. Per D-05:
//!   - Group keys inherit their types from the upstream schema.
//!   - Each named feature gets its type from `output_type_for(upstream, desc)`.
//!
//! Fail-soft: all validation errors are collected (same pattern as
//! `schema_propagate::propagate_schema`). Returns `Err(Vec<AggSchemaError>)` if
//! any violations are found.
//!
//! # Requirements traceability
//! - SDK-AGG-01: group_by keys validated against upstream schema
//! - SDK-AGG-03: output type inferred per operator via `output_type_for`

use std::collections::BTreeMap;

use crate::agg_descriptor::AggregationDescriptor;
use crate::agg_op::{output_type_for, AggKind, AggTypeError};
use crate::schema::DerivedSchema;
use crate::schema_propagate::Schema;

// ─── AggSchemaError ──────────────────────────────────────────────────────────

/// Validation error produced by `propagate_aggregation_schema`.
///
/// All variants are collected (fail-soft); callers receive a `Vec<AggSchemaError>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggSchemaError {
    /// A group_by key is not present in the upstream schema.
    GroupKeyMissing { key: String },
    /// Two features share the same `feature_name`.
    DuplicateFeatureName { name: String },
    /// A feature's operator failed type inference (missing/required field).
    FeatureTypeError {
        feature: String,
        kind: AggKind,
        /// `Some(field)` when the named field is absent; `None` when the op
        /// requires a field but none was specified.
        field_missing: Option<String>,
    },
    /// A feature name collides with a group_by key, which would silently
    /// overwrite the key column (T-05-03-01 mitigation).
    GroupKeyCollidesWithFeature { name: String },
}

// ─── propagate_aggregation_schema ────────────────────────────────────────────

/// Derive the aggregation's output `DerivedSchema` from the upstream schema.
///
/// Validation (fail-soft — all errors collected):
/// 1. Every `group_key` must exist in `upstream.fields`.
/// 2. Every `feature_name` must be unique within the feature list.
/// 3. No `feature_name` may collide with a `group_key`.
/// 4. `output_type_for(upstream, &feature.descriptor)` must succeed for each
///    feature (validates field existence for Sum/Avg/Min/Max/Variance/StdDev).
///
/// On success: returns `DerivedSchema` with group-key columns (inherited types)
/// followed by feature columns (inferred types). `optional_fields` is empty in
/// v0 — all output columns are treated as non-null at the schema level.
///
/// # SDK-AGG-01, SDK-AGG-03
pub fn propagate_aggregation_schema(
    upstream: &Schema,
    descriptor: &AggregationDescriptor,
) -> Result<DerivedSchema, Vec<AggSchemaError>> {
    let mut errors: Vec<AggSchemaError> = Vec::new();
    let mut fields: BTreeMap<String, crate::schema::FieldType> = BTreeMap::new();

    // ── Step 1: Validate group keys + copy their types into output ────────────
    // SDK-AGG-01: every group_by key must exist in upstream schema.
    let mut group_key_set: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for key in &descriptor.group_keys {
        match upstream.fields.get(key.as_str()) {
            Some(ty) => {
                fields.insert(key.clone(), *ty);
                group_key_set.insert(key.as_str());
            }
            None => {
                errors.push(AggSchemaError::GroupKeyMissing { key: key.clone() });
            }
        }
    }

    // ── Step 2: Validate features + infer their output types ──────────────────
    // SDK-AGG-03: type inferred via output_type_for per operator.
    // T-05-03-01: feature name collision with a group key is rejected.
    let mut seen_feature_names: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for feat in &descriptor.features {
        // Duplicate feature name?
        if !seen_feature_names.insert(feat.feature_name.as_str()) {
            errors.push(AggSchemaError::DuplicateFeatureName {
                name: feat.feature_name.clone(),
            });
            continue;
        }

        // Feature name collides with group key? (T-05-03-01 mitigation)
        if group_key_set.contains(feat.feature_name.as_str()) {
            errors.push(AggSchemaError::GroupKeyCollidesWithFeature {
                name: feat.feature_name.clone(),
            });
            continue;
        }

        // For ops that consume a named field (Sum, Avg, Variance, StdDev), verify
        // the field exists in the upstream schema. `output_type_for` only does this
        // check for Min/Max (since they need the upstream type); we replicate the
        // field-existence check here for the F64-returning ops so that
        // `sum(field="nonexistent")` surfaces a FeatureTypeError at register time.
        let field_check_kinds = [
            AggKind::Sum,
            AggKind::Avg,
            AggKind::Variance,
            AggKind::StdDev,
        ];
        if field_check_kinds.contains(&feat.descriptor.kind) {
            match &feat.descriptor.field {
                None => {
                    // Field is optional for Sum/Avg/Variance/StdDev in the descriptor
                    // (none = whole-row semantics deferred to v1). No error in v0.
                }
                Some(field_name) => {
                    if !upstream.fields.contains_key(field_name.as_str()) {
                        errors.push(AggSchemaError::FeatureTypeError {
                            feature: feat.feature_name.clone(),
                            kind: feat.descriptor.kind,
                            field_missing: Some(field_name.clone()),
                        });
                        continue;
                    }
                }
            }
        }

        // Type inference via output_type_for (SDK-AGG-03).
        // At this point field-existence is already validated above for F64 ops;
        // output_type_for handles Min/Max field resolution independently.
        match output_type_for(upstream, &feat.descriptor) {
            Ok(ty) => {
                fields.insert(feat.feature_name.clone(), ty);
            }
            Err(AggTypeError::FieldMissing { field }) => {
                errors.push(AggSchemaError::FeatureTypeError {
                    feature: feat.feature_name.clone(),
                    kind: feat.descriptor.kind,
                    field_missing: Some(field),
                });
            }
            Err(AggTypeError::FieldRequired { kind }) => {
                errors.push(AggSchemaError::FeatureTypeError {
                    feature: feat.feature_name.clone(),
                    kind,
                    field_missing: None,
                });
            }
        }
    }

    if !errors.is_empty() {
        return Err(errors);
    }

    // Optional-fields: aggregation output is non-null for group keys (null-keyed
    // events are dropped at apply time in Plan 05-05). Feature values MAY be null
    // for avg/min/max/variance/stddev/ratio when no matching rows, but the SCHEMA
    // contract in v0 is "field present, non-null" — apply loop returns Null for
    // empty-state queries at runtime, which is distinct from a schema-level null.
    Ok(DerivedSchema {
        fields,
        optional_fields: Vec::new(),
    })
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agg_descriptor::{AggregationDescriptor, NamedAggOp};
    use crate::agg_op::{AggKind, AggOpDescriptor};
    use crate::schema::FieldType;
    use std::collections::BTreeMap;

    // ── Fixtures ──────────────────────────────────────────────────────────────

    fn schema_with(pairs: &[(&str, FieldType)]) -> Schema {
        let mut fields = BTreeMap::new();
        for (k, v) in pairs {
            fields.insert(k.to_string(), *v);
        }
        Schema {
            fields,
            optional_fields: vec![],
        }
    }

    fn op_desc(kind: AggKind, field: Option<&str>) -> AggOpDescriptor {
        AggOpDescriptor {
            kind,
            field: field.map(|s| s.to_string()),
            window_ms: None,
            where_expr: None,

            ext: Default::default(),
        }
    }

    fn named(feature_name: &str, kind: AggKind, field: Option<&str>) -> NamedAggOp {
        NamedAggOp {
            feature_name: feature_name.to_string(),
            descriptor: op_desc(kind, field),
        }
    }

    fn agg(source: &str, group_keys: &[&str], features: Vec<NamedAggOp>) -> AggregationDescriptor {
        AggregationDescriptor {
            node_name: "output_table".to_string(),
            source_node_name: source.to_string(),
            group_keys: group_keys.iter().map(|s| s.to_string()).collect(),
            features,
        }
    }

    fn assert_ok(r: Result<DerivedSchema, Vec<AggSchemaError>>) -> DerivedSchema {
        match r {
            Ok(s) => s,
            Err(errs) => panic!("expected Ok, got errors: {errs:?}"),
        }
    }

    fn assert_err(r: Result<DerivedSchema, Vec<AggSchemaError>>) -> Vec<AggSchemaError> {
        match r {
            Err(errs) => errs,
            Ok(s) => panic!("expected Err, got schema: {s:?}"),
        }
    }

    // ── schema_includes_group_keys_with_upstream_types ────────────────────────

    /// Output schema contains group key with upstream type + feature column.
    #[test]
    fn schema_includes_group_keys_with_upstream_types() {
        let upstream = schema_with(&[("user_id", FieldType::Str), ("amount", FieldType::F64)]);
        let desc = agg(
            "txn",
            &["user_id"],
            vec![named("cnt", AggKind::Count, None)],
        );
        let schema = assert_ok(propagate_aggregation_schema(&upstream, &desc));

        assert_eq!(
            schema.fields.get("user_id"),
            Some(&FieldType::Str),
            "group key user_id should inherit Str from upstream"
        );
        assert_eq!(
            schema.fields.get("cnt"),
            Some(&FieldType::I64),
            "count feature should be I64"
        );
        assert_eq!(schema.fields.len(), 2, "exactly 2 output columns");
    }

    // ── count_feature_infers_i64 ──────────────────────────────────────────────

    #[test]
    fn count_feature_infers_i64() {
        let upstream = schema_with(&[("user_id", FieldType::Str)]);
        let desc = agg(
            "txn",
            &["user_id"],
            vec![named("cnt", AggKind::Count, None)],
        );
        let schema = assert_ok(propagate_aggregation_schema(&upstream, &desc));
        assert_eq!(schema.fields.get("cnt"), Some(&FieldType::I64));
    }

    // ── sum_feature_infers_f64 ────────────────────────────────────────────────

    #[test]
    fn sum_feature_infers_f64() {
        let upstream = schema_with(&[("user_id", FieldType::Str), ("amount", FieldType::F64)]);
        let desc = agg(
            "txn",
            &["user_id"],
            vec![named("total", AggKind::Sum, Some("amount"))],
        );
        let schema = assert_ok(propagate_aggregation_schema(&upstream, &desc));
        assert_eq!(schema.fields.get("total"), Some(&FieldType::F64));
    }

    // ── avg_feature_infers_f64 ────────────────────────────────────────────────

    #[test]
    fn avg_feature_infers_f64() {
        let upstream = schema_with(&[("user_id", FieldType::Str), ("amount", FieldType::F64)]);
        let desc = agg(
            "txn",
            &["user_id"],
            vec![named("avg_amt", AggKind::Avg, Some("amount"))],
        );
        let schema = assert_ok(propagate_aggregation_schema(&upstream, &desc));
        assert_eq!(schema.fields.get("avg_amt"), Some(&FieldType::F64));
    }

    // ── min_feature_preserves_field_type ─────────────────────────────────────

    /// min inherits the upstream field's type (F64 stays F64, I64 stays I64).
    #[test]
    fn min_feature_preserves_field_type() {
        let upstream = schema_with(&[
            ("user_id", FieldType::Str),
            ("amount", FieldType::F64),
            ("event_time", FieldType::I64),
        ]);

        // min(amount:F64) → F64
        let desc_f = agg(
            "txn",
            &["user_id"],
            vec![named("min_amt", AggKind::Min, Some("amount"))],
        );
        let schema_f = assert_ok(propagate_aggregation_schema(&upstream, &desc_f));
        assert_eq!(schema_f.fields.get("min_amt"), Some(&FieldType::F64));

        // min(event_time:I64) → I64
        let desc_i = agg(
            "txn",
            &["user_id"],
            vec![named("min_t", AggKind::Min, Some("event_time"))],
        );
        let schema_i = assert_ok(propagate_aggregation_schema(&upstream, &desc_i));
        assert_eq!(schema_i.fields.get("min_t"), Some(&FieldType::I64));
    }

    // ── variance_feature_infers_f64 ───────────────────────────────────────────

    #[test]
    fn variance_feature_infers_f64() {
        let upstream = schema_with(&[("user_id", FieldType::Str), ("amount", FieldType::F64)]);
        let desc = agg(
            "txn",
            &["user_id"],
            vec![named("var_amt", AggKind::Variance, Some("amount"))],
        );
        let schema = assert_ok(propagate_aggregation_schema(&upstream, &desc));
        assert_eq!(schema.fields.get("var_amt"), Some(&FieldType::F64));
    }

    // ── stddev_feature_infers_f64 ─────────────────────────────────────────────

    #[test]
    fn stddev_feature_infers_f64() {
        let upstream = schema_with(&[("user_id", FieldType::Str), ("amount", FieldType::F64)]);
        let desc = agg(
            "txn",
            &["user_id"],
            vec![named("sd_amt", AggKind::StdDev, Some("amount"))],
        );
        let schema = assert_ok(propagate_aggregation_schema(&upstream, &desc));
        assert_eq!(schema.fields.get("sd_amt"), Some(&FieldType::F64));
    }

    // ── ratio_feature_infers_f64 ──────────────────────────────────────────────

    #[test]
    fn ratio_feature_infers_f64() {
        let upstream = schema_with(&[("user_id", FieldType::Str)]);
        let desc = agg(
            "txn",
            &["user_id"],
            vec![named("r_ok", AggKind::Ratio, None)],
        );
        let schema = assert_ok(propagate_aggregation_schema(&upstream, &desc));
        assert_eq!(schema.fields.get("r_ok"), Some(&FieldType::F64));
    }

    // ── unknown_group_key_returns_error ───────────────────────────────────────

    /// SDK-AGG-01: unknown group key → GroupKeyMissing error.
    #[test]
    fn unknown_group_key_returns_error() {
        let upstream = schema_with(&[("user_id", FieldType::Str)]);
        let desc = agg(
            "txn",
            &["nonexistent"],
            vec![named("cnt", AggKind::Count, None)],
        );
        let errs = assert_err(propagate_aggregation_schema(&upstream, &desc));
        assert!(
            errs.iter().any(|e| matches!(
                e,
                AggSchemaError::GroupKeyMissing { key }
                if key == "nonexistent"
            )),
            "expected GroupKeyMissing{{nonexistent}}, got {errs:?}"
        );
    }

    // ── missing_field_for_sum_returns_error ───────────────────────────────────

    /// SDK-AGG-03: sum on unknown field → FeatureTypeError.
    #[test]
    fn missing_field_for_sum_returns_error() {
        let upstream = schema_with(&[("user_id", FieldType::Str)]);
        let desc = agg(
            "txn",
            &["user_id"],
            vec![named("total", AggKind::Sum, Some("nonexistent_field"))],
        );
        let errs = assert_err(propagate_aggregation_schema(&upstream, &desc));
        assert!(
            errs.iter().any(|e| matches!(
                e,
                AggSchemaError::FeatureTypeError {
                    feature,
                    kind: AggKind::Sum,
                    ..
                }
                if feature == "total"
            )),
            "expected FeatureTypeError for sum on missing field, got {errs:?}"
        );
    }

    // ── duplicate_feature_names_rejected ─────────────────────────────────────

    #[test]
    fn duplicate_feature_names_rejected() {
        let upstream = schema_with(&[("user_id", FieldType::Str)]);
        let desc = agg(
            "txn",
            &["user_id"],
            vec![
                named("cnt", AggKind::Count, None),
                named("cnt", AggKind::Count, None), // duplicate
            ],
        );
        let errs = assert_err(propagate_aggregation_schema(&upstream, &desc));
        assert!(
            errs.iter().any(|e| matches!(
                e,
                AggSchemaError::DuplicateFeatureName { name }
                if name == "cnt"
            )),
            "expected DuplicateFeatureName{{cnt}}, got {errs:?}"
        );
    }

    // ── feature_name_collides_with_group_key_rejected ─────────────────────────

    /// T-05-03-01: feature named same as a group key → GroupKeyCollidesWithFeature.
    #[test]
    fn feature_name_collides_with_group_key_rejected() {
        let upstream = schema_with(&[("user_id", FieldType::Str)]);
        let desc = agg(
            "txn",
            &["user_id"],
            vec![named("user_id", AggKind::Count, None)], // collides with group key
        );
        let errs = assert_err(propagate_aggregation_schema(&upstream, &desc));
        assert!(
            errs.iter().any(|e| matches!(
                e,
                AggSchemaError::GroupKeyCollidesWithFeature { name }
                if name == "user_id"
            )),
            "expected GroupKeyCollidesWithFeature{{user_id}}, got {errs:?}"
        );
    }

    // ── fail_soft_collects_all_errors ─────────────────────────────────────────

    /// Two unknown group keys + one unknown field → all three errors collected.
    #[test]
    fn fail_soft_collects_all_errors() {
        let upstream = schema_with(&[("user_id", FieldType::Str)]);
        let desc = AggregationDescriptor {
            node_name: "out".to_string(),
            source_node_name: "txn".to_string(),
            group_keys: vec!["missing_key1".to_string(), "missing_key2".to_string()],
            features: vec![named("total", AggKind::Sum, Some("no_such_field"))],
        };
        let errs = assert_err(propagate_aggregation_schema(&upstream, &desc));
        assert!(
            errs.len() >= 3,
            "expected at least 3 errors (2 missing keys + 1 missing field), got {errs:?}"
        );
        // Both missing keys reported
        assert!(
            errs.iter().any(|e| matches!(
                e,
                AggSchemaError::GroupKeyMissing { key } if key == "missing_key1"
            )),
            "missing_key1 not reported in {errs:?}"
        );
        assert!(
            errs.iter().any(|e| matches!(
                e,
                AggSchemaError::GroupKeyMissing { key } if key == "missing_key2"
            )),
            "missing_key2 not reported in {errs:?}"
        );
        // Missing sum field reported
        assert!(
            errs.iter().any(|e| matches!(
                e,
                AggSchemaError::FeatureTypeError { feature, .. } if feature == "total"
            )),
            "FeatureTypeError for total not reported in {errs:?}"
        );
    }

    // ── multiple_features_all_inferred_independently ──────────────────────────

    /// Multiple features each get their own independently inferred type.
    #[test]
    fn multiple_features_all_inferred_independently() {
        let upstream = schema_with(&[("user_id", FieldType::Str), ("amount", FieldType::F64)]);
        let desc = agg(
            "txn",
            &["user_id"],
            vec![
                named("cnt", AggKind::Count, None),
                named("total", AggKind::Sum, Some("amount")),
                named("n_declined", AggKind::Count, None),
            ],
        );
        let schema = assert_ok(propagate_aggregation_schema(&upstream, &desc));
        assert_eq!(schema.fields.get("user_id"), Some(&FieldType::Str));
        assert_eq!(schema.fields.get("cnt"), Some(&FieldType::I64));
        assert_eq!(schema.fields.get("total"), Some(&FieldType::F64));
        assert_eq!(schema.fields.get("n_declined"), Some(&FieldType::I64));
        assert_eq!(schema.fields.len(), 4);
    }
}
