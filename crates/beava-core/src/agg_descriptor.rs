//! AggregationDescriptor + NamedAggOp: register-time aggregation shape.
//!
//! This module provides the structural descriptors that model an
//! `Event.group_by(*keys).agg(**features)` aggregation at the beava-core level.
//!
//! Used by:
//! - Plan 05-03 (this plan): `propagate_aggregation_schema` in `agg_schema.rs`
//! - Plan 05-04: `RegistryInner.compiled_aggregations` caches `Arc<AggregationDescriptor>`
//! - Plan 05-05: `source_node_name` routes incoming events to the right aggregation
//! - Plan 05-06: feature lookup by name
//!
//! # Requirements traceability
//! - SDK-AGG-01: group_by keys validated against upstream schema (enforced in agg_schema.rs)
//! - SDK-AGG-03: feature output type inference via `output_type_for` (enforced in agg_schema.rs)

use crate::agg_op::AggOpDescriptor;

// ─── NamedAggOp ──────────────────────────────────────────────────────────────

/// One named aggregation feature within an `AggregationDescriptor`.
///
/// Maps a user-visible feature name (e.g., `"cnt_5m"`) to the operator
/// descriptor that drives it (`AggOpDescriptor`).
#[derive(Debug, Clone)]
pub struct NamedAggOp {
    /// User-visible feature name — must be unique within the aggregation.
    pub feature_name: String,
    /// Operator descriptor (kind, field, window_ms, where_expr).
    pub descriptor: AggOpDescriptor,
}

// ─── AggregationDescriptor ───────────────────────────────────────────────────

/// Register-time descriptor for one `group_by().agg()` aggregation.
///
/// Captures everything the apply loop and HTTP query endpoint need:
/// - Which event source to watch (`source_node_name`)
/// - Which keys to group by (`group_keys`)
/// - Which features to compute (`features`)
///
/// The schema produced by this aggregation is computed by
/// `agg_schema::propagate_aggregation_schema`. Plan 05-04 caches
/// `Arc<AggregationDescriptor>` in `RegistryInner.compiled_aggregations`.
#[derive(Debug, Clone)]
pub struct AggregationDescriptor {
    /// Derivation node name — the Table this aggregation produces.
    pub node_name: String,
    /// Upstream event node name — the stream the apply loop watches.
    pub source_node_name: String,
    /// Keys to group by; must all exist in the upstream event schema.
    pub group_keys: Vec<String>,
    /// Ordered named features; `feature_name` must be unique within the list
    /// and must not collide with any `group_key`.
    pub features: Vec<NamedAggOp>,
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agg_op::{AggKind, AggOpDescriptor};

    fn count_desc() -> AggOpDescriptor {
        AggOpDescriptor {
            kind: AggKind::Count,
            field: None,
            window_ms: None,
            where_expr: None,
            half_life_ms: None,
            sub_window_ms: None,
            sigma: None,
        }
    }

    // ── named_aggop_new_constructs_cleanly ────────────────────────────────────

    /// NamedAggOp carries feature_name + descriptor; Debug and Clone work.
    #[test]
    fn named_aggop_new_constructs_cleanly() {
        let op = NamedAggOp {
            feature_name: "cnt_5m".to_string(),
            descriptor: count_desc(),
        };
        // Clone round-trip
        let cloned = op.clone();
        assert_eq!(cloned.feature_name, "cnt_5m");
        assert_eq!(cloned.descriptor.kind, AggKind::Count);
        // Debug must not panic
        let _ = format!("{:?}", op);
    }

    // ── aggregation_descriptor_records_source_node_name ───────────────────────

    /// AggregationDescriptor exposes node_name + source_node_name as accessible fields.
    #[test]
    fn aggregation_descriptor_records_source_node_name() {
        let desc = AggregationDescriptor {
            node_name: "user_stats".to_string(),
            source_node_name: "transactions".to_string(),
            group_keys: vec!["user_id".to_string()],
            features: vec![NamedAggOp {
                feature_name: "cnt".to_string(),
                descriptor: count_desc(),
            }],
        };
        assert_eq!(desc.node_name, "user_stats");
        assert_eq!(desc.source_node_name, "transactions");
        assert_eq!(desc.group_keys, vec!["user_id"]);
        assert_eq!(desc.features.len(), 1);
        assert_eq!(desc.features[0].feature_name, "cnt");
        // Clone round-trip
        let cloned = desc.clone();
        assert_eq!(cloned.source_node_name, "transactions");
        // Debug must not panic
        let _ = format!("{:?}", desc);
    }
}
