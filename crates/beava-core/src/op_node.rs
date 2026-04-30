//! OpNode enum: the 9 transformation operators that can appear in a derivation's `ops` list.
//!
//! Phase 2 stores these verbatim — no execution, no expression parsing.
//! Phase 4 evaluates Filter/Select/etc. server-side.
//! Phase 5 resolves GroupBy.agg.op against the operator catalogue.
//!
//! **Phase 12.6 (2026-04-30):** `Join` (with its `JoinType` enum) and `Union`
//! variants permanently removed per `project_redis_shaped_no_event_time_ever`
//! commitment. Reviving either requires explicit user override + a new ADR.
//! The register-time validator emits structured error codes
//! (`feature_removed_no_joins_v0` / `feature_removed_no_unions_v0`) for stale
//! Python SDK fixtures or hand-rolled JSON DAGs that include `{"op":"join"}`
//! / `{"op":"union"}` — see `register_validate::pre_check_removed_ops`.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// ─── Supporting types ─────────────────────────────────────────────────────────

/// Aggregation spec: an operator name (Phase 5 validates against catalogue) plus
/// operator-specific params stored as opaque JSON.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggSpec {
    pub op: String,
    /// Per-operator params; Phase 2 treats this as opaque JSON.
    #[serde(default)]
    pub params: serde_json::Value,
}

// ─── OpNode ───────────────────────────────────────────────────────────────────

/// A single transformation step in a derivation pipeline.
///
/// Uses serde's internally-tagged representation with `"op"` as the tag field
/// and `snake_case` variant names (e.g., `"with_columns"`, `"group_by"`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum OpNode {
    /// Keep rows where `expr` evaluates to true. Phase 4 parses/evaluates.
    Filter { expr: String },

    /// Project to the named fields only.
    Select { fields: Vec<String> },

    /// Remove the named fields.
    Drop { fields: Vec<String> },

    /// Rename fields by the provided mapping (old_name → new_name).
    Rename { mapping: BTreeMap<String, String> },

    /// Add/replace columns via expressions (alias: `with_columns`).
    WithColumns { exprs: BTreeMap<String, String> },

    /// Alias for `WithColumns` — same wire shape.
    Map { exprs: BTreeMap<String, String> },

    /// Cast field types; `type_map` maps field_name → target type string.
    Cast { type_map: BTreeMap<String, String> },

    /// Fill null values with defaults. `defaults` maps field_name → fill value.
    Fillna {
        defaults: BTreeMap<String, serde_json::Value>,
    },

    /// Group by keys and apply aggregations. Phase 5 executes.
    GroupBy {
        keys: Vec<String>,
        agg: BTreeMap<String, AggSpec>,
    },
    // Phase 12.6 (2026-04-30): Join + Union variants permanently removed
    // per project_redis_shaped_no_event_time_ever. JSON-prelude shim
    // `register_validate::pre_check_removed_ops` rejects stale fixtures
    // with structured error codes feature_removed_no_joins_v0 /
    // feature_removed_no_unions_v0.
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Test 1: Filter round-trip
    #[test]
    fn round_trip_filter() {
        let op = OpNode::Filter {
            expr: "(amount > 500)".to_string(),
        };
        let json_str = serde_json::to_string(&op).unwrap();
        assert_eq!(json_str, r#"{"op":"filter","expr":"(amount > 500)"}"#);
        let back: OpNode = serde_json::from_str(&json_str).unwrap();
        assert_eq!(back, op);
    }

    // Test 2: Select and Drop round-trip
    #[test]
    fn round_trip_select_drop() {
        let sel = OpNode::Select {
            fields: vec!["a".to_string(), "b".to_string()],
        };
        let j = serde_json::to_string(&sel).unwrap();
        let back: OpNode = serde_json::from_str(&j).unwrap();
        assert_eq!(back, sel);

        let drop_op = OpNode::Drop {
            fields: vec!["c".to_string()],
        };
        let j2 = serde_json::to_string(&drop_op).unwrap();
        let back2: OpNode = serde_json::from_str(&j2).unwrap();
        assert_eq!(back2, drop_op);
    }

    // Test 3: Rename, WithColumns, Map, Cast, Fillna round-trips
    #[test]
    fn round_trip_rename_with_columns_map_cast_fillna() {
        let rename = OpNode::Rename {
            mapping: {
                let mut m = BTreeMap::new();
                m.insert("old".to_string(), "new".to_string());
                m
            },
        };
        let j = serde_json::to_string(&rename).unwrap();
        let back: OpNode = serde_json::from_str(&j).unwrap();
        assert_eq!(back, rename);

        let wc = OpNode::WithColumns {
            exprs: {
                let mut m = BTreeMap::new();
                m.insert("is_big".to_string(), "(amount > 500)".to_string());
                m
            },
        };
        let j = serde_json::to_string(&wc).unwrap();
        let back: OpNode = serde_json::from_str(&j).unwrap();
        assert_eq!(back, wc);

        let map_op = OpNode::Map {
            exprs: {
                let mut m = BTreeMap::new();
                m.insert("cents".to_string(), "(amount * 100)".to_string());
                m
            },
        };
        let j = serde_json::to_string(&map_op).unwrap();
        let back: OpNode = serde_json::from_str(&j).unwrap();
        assert_eq!(back, map_op);

        let cast_op = OpNode::Cast {
            type_map: {
                let mut m = BTreeMap::new();
                m.insert("amount".to_string(), "f64".to_string());
                m
            },
        };
        let j = serde_json::to_string(&cast_op).unwrap();
        let back: OpNode = serde_json::from_str(&j).unwrap();
        assert_eq!(back, cast_op);

        // Fillna with string default
        let fillna_str = OpNode::Fillna {
            defaults: {
                let mut m = BTreeMap::new();
                m.insert("category".to_string(), json!("unknown"));
                m
            },
        };
        let j = serde_json::to_string(&fillna_str).unwrap();
        let back: OpNode = serde_json::from_str(&j).unwrap();
        assert_eq!(back, fillna_str);

        // Fillna with numeric default
        let fillna_num = OpNode::Fillna {
            defaults: {
                let mut m = BTreeMap::new();
                m.insert("score".to_string(), json!(0));
                m
            },
        };
        let j = serde_json::to_string(&fillna_num).unwrap();
        let back: OpNode = serde_json::from_str(&j).unwrap();
        assert_eq!(back, fillna_num);
    }

    // Test 4: GroupBy round-trip
    #[test]
    fn round_trip_group_by() {
        let op = OpNode::GroupBy {
            keys: vec!["user_id".to_string()],
            agg: {
                let mut m = BTreeMap::new();
                m.insert(
                    "cnt".to_string(),
                    AggSpec {
                        op: "count".to_string(),
                        params: json!({}),
                    },
                );
                m.insert(
                    "sum_amt".to_string(),
                    AggSpec {
                        op: "sum".to_string(),
                        params: json!({"field": "amount"}),
                    },
                );
                m
            },
        };
        let j = serde_json::to_string(&op).unwrap();
        let back: OpNode = serde_json::from_str(&j).unwrap();
        assert_eq!(back, op);
    }

    // Tests 5 (Join round-trip) and 6 (Union round-trip) deleted in Phase 12.6
    // Plan 04 — joins and unions permanently removed per
    // project_redis_shaped_no_event_time_ever (2026-04-30). The new
    // join_op_unknown_variant_after_phase_12_6_removal and
    // union_op_unknown_variant_after_phase_12_6_removal tests below pin the
    // post-removal contract.

    // Test 7: Unknown op variant is rejected
    #[test]
    fn unknown_op_rejected() {
        let result: Result<OpNode, _> = serde_json::from_str(r#"{"op":"delete","fields":[]}"#);
        assert!(result.is_err(), "expected Err for unknown op 'delete'");
        let msg = result.unwrap_err().to_string().to_lowercase();
        assert!(
            msg.contains("unknown variant") || msg.contains("delete"),
            "error should mention 'delete' or 'unknown variant', got: {msg}"
        );
    }

    // Test 8: Filter missing expr is rejected
    #[test]
    fn filter_missing_expr_rejected() {
        let result: Result<OpNode, _> = serde_json::from_str(r#"{"op":"filter"}"#);
        assert!(result.is_err(), "expected Err for filter without expr");
        let msg = result.unwrap_err().to_string().to_lowercase();
        assert!(
            msg.contains("expr") || msg.contains("missing field"),
            "error should mention 'expr', got: {msg}"
        );
    }

    // Test 9: GroupBy missing keys is rejected
    #[test]
    fn group_by_missing_keys_rejected() {
        let result: Result<OpNode, _> = serde_json::from_str(r#"{"op":"group_by","agg":{}}"#);
        assert!(result.is_err(), "expected Err for group_by without keys");
        let msg = result.unwrap_err().to_string().to_lowercase();
        assert!(
            msg.contains("keys") || msg.contains("missing field"),
            "error should mention 'keys', got: {msg}"
        );
    }

    // Test 10: AggSpec missing op field is rejected inside GroupBy
    #[test]
    fn agg_spec_missing_op_rejected() {
        let result: Result<OpNode, _> = serde_json::from_str(
            r#"{"op":"group_by","keys":["user_id"],"agg":{"cnt":{"params":{}}}}"#,
        );
        assert!(
            result.is_err(),
            "expected Err when AggSpec missing 'op' field"
        );
    }

    // Test 11: AggSpec params defaults to Null when absent
    #[test]
    fn agg_spec_params_default() {
        let agg: AggSpec = serde_json::from_str(r#"{"op":"count"}"#).unwrap();
        assert_eq!(agg.op, "count");
        assert!(
            agg.params.is_null(),
            "params should default to null, got: {:?}",
            agg.params
        );
    }

    // Test 12 (join_type required-field rejection) deleted in Phase 12.6 Plan
    // 04 — joins permanently removed per project_redis_shaped_no_event_time_ever
    // (2026-04-30). The post-removal serde-rejection contract is pinned by
    // join_op_unknown_variant_after_phase_12_6_removal below.

    // ── Phase 12.6 Plan 04: Join/Union variant removal ────────────────────────
    //
    // After Plan 04 lands, OpNode has 9 variants (Filter, Select, Drop, Rename,
    // WithColumns, Map, Cast, Fillna, GroupBy). Join/Union are PERMANENTLY
    // removed per project_redis_shaped_no_event_time_ever (2026-04-30).
    //
    // These two tests assert the deletion at the serde/enum level. They are
    // RED while the variants are alive (serde successfully deserializes
    // {"op":"join", ...}); they go GREEN as soon as the variants are deleted
    // and serde returns "unknown variant `join`" / "unknown variant `union`".

    // Test 13: {"op":"join"} payload must NOT deserialize as OpNode (post-removal).
    #[test]
    fn join_op_unknown_variant_after_phase_12_6_removal() {
        let result: Result<OpNode, _> =
            serde_json::from_str(r#"{"op":"join","other":"E","on":["x"],"join_type":"inner"}"#);
        assert!(
            result.is_err(),
            "post-Phase-12.6 OpNode must not accept {{op:join}}; \
             got Ok({:?}) — joins are permanently removed",
            result.ok()
        );
        let msg = result.unwrap_err().to_string().to_lowercase();
        assert!(
            msg.contains("unknown variant") || msg.contains("join"),
            "expected 'unknown variant' or 'join' in serde error, got: {msg}"
        );
    }

    // Test 14: {"op":"union"} payload must NOT deserialize as OpNode (post-removal).
    #[test]
    fn union_op_unknown_variant_after_phase_12_6_removal() {
        let result: Result<OpNode, _> = serde_json::from_str(r#"{"op":"union","others":["E2"]}"#);
        assert!(
            result.is_err(),
            "post-Phase-12.6 OpNode must not accept {{op:union}}; \
             got Ok({:?}) — unions are permanently removed",
            result.ok()
        );
        let msg = result.unwrap_err().to_string().to_lowercase();
        assert!(
            msg.contains("unknown variant") || msg.contains("union"),
            "expected 'unknown variant' or 'union' in serde error, got: {msg}"
        );
    }
}
