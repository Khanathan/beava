//! Per-aggregation, per-entity state storage.
//!
//! # Overview
//!
//! `AggStateTable` maps `EntityKey → Vec<AggOp>` where each slot in the `Vec`
//! corresponds (in order) to `AggregationDescriptor::features`.
//!
//! ## Key design invariants (D-06 determinism)
//!
//! - Uses `BTreeMap` (never Hash...Map) so iteration order is stable across
//!   invocations — required for WAL replay determinism (SC4).
//! - `EntityKey` is a canonically-ordered vec of `(group_key_name, value_string)`
//!   pairs; keys are in `group_keys` declaration order, not alphabetical order.
//! - Null or missing group-key values produce `None` from `EntityKey::from_row`,
//!   causing the apply loop to drop the event for that aggregation.
//!
//! # Value → String canonicalization table
//!
//! | `Value` variant    | canonical string                     | notes                            |
//! |--------------------|--------------------------------------|----------------------------------|
//! | `Str(s)`           | `s` as-is                            | keys are plain identifiers        |
//! | `I64(n)`           | `n.to_string()`                      | e.g. "42"                        |
//! | `F64(f)`           | `format!("{:?}", f)`                 | Rust Debug repr; deterministic   |
//! | `Bool(b)`          | `b.to_string()`                      | "true" / "false"                 |
//! | `Datetime(ms)`     | `ms.to_string()`                     | epoch-ms as decimal              |
//! | `Bytes(_)`         | → return `None` (event dropped)      | bytes are not sane keys in v0    |
//! | `Null`             | → return `None` (event dropped)      | null group-key means no entity   |

use std::collections::BTreeMap;

use crate::agg_descriptor::AggregationDescriptor;
use crate::agg_op::AggOp;
use crate::row::{Row, Value};
use serde::{Deserialize, Serialize};

// ─── EntityKey ────────────────────────────────────────────────────────────────

/// Stable entity identifier for per-aggregation state lookup.
///
/// A vec of `(group_key_name, canonical_value_string)` pairs in declaration
/// order (the order of `AggregationDescriptor::group_keys`).  Implements `Ord`
/// so it can be used as a `BTreeMap` key without hashing.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EntityKey(pub Vec<(String, String)>);

impl EntityKey {
    /// Build an `EntityKey` from a `Row` by extracting the fields named in
    /// `group_keys` and converting each to its canonical string.
    ///
    /// Returns `None` if any group-key field is absent or `Value::Null` — the
    /// apply loop must drop the event for this aggregation in that case.
    ///
    /// `Bytes` values also produce `None` (not sane entity keys in v0; see
    /// module doc for the full canonicalization table).
    pub fn from_row(group_keys: &[String], row: &Row) -> Option<EntityKey> {
        let mut pairs = Vec::with_capacity(group_keys.len());
        for key in group_keys {
            let canonical = match row.get(key) {
                None => return None,                  // missing field → drop
                Some(Value::Null) => return None,     // null field → drop
                Some(Value::Bytes(_)) => return None, // bytes not sane as key → drop
                Some(Value::Str(s)) => s.clone(),
                Some(Value::I64(n)) => n.to_string(),
                Some(Value::F64(f)) => format!("{:?}", f),
                Some(Value::Bool(b)) => b.to_string(),
                Some(Value::Datetime(ms)) => ms.to_string(),
            };
            pairs.push((key.clone(), canonical));
        }
        Some(EntityKey(pairs))
    }
}

// ─── AggStateTable ────────────────────────────────────────────────────────────

/// Per-aggregation state store: maps `EntityKey → Vec<AggOp>` (one slot per
/// feature in `AggregationDescriptor::features`).
///
/// The outer `BTreeMap` keeps entity entries in deterministic order (D-06).
pub struct AggStateTable {
    pub entities: BTreeMap<EntityKey, Vec<AggOp>>,
}

impl AggStateTable {
    /// Create an empty table.
    pub fn new() -> Self {
        AggStateTable {
            entities: BTreeMap::new(),
        }
    }

    /// Look up the per-entity `Vec<AggOp>` for `key`.  If the key is new,
    /// initialise a fresh `Vec` with one `AggOp::new` per feature in `descriptor`.
    ///
    /// Returns a mutable reference to the entity row so the apply loop can call
    /// `update_with_row` on each slot.
    pub fn get_or_init(
        &mut self,
        key: &EntityKey,
        descriptor: &AggregationDescriptor,
    ) -> &mut Vec<AggOp> {
        self.entities.entry(key.clone()).or_insert_with(|| {
            descriptor
                .features
                .iter()
                .map(|f| AggOp::new(&f.descriptor))
                .collect()
        })
    }

    /// Query feature `feature_index` for entity `key` at `query_time_ms`.
    ///
    /// Returns `None` if the key is not present or the index is out of range.
    pub fn query_feature(
        &self,
        key: &EntityKey,
        feature_index: usize,
        query_time_ms: i64,
    ) -> Option<Value> {
        self.entities
            .get(key)
            .and_then(|ops| ops.get(feature_index))
            .map(|op| op.query(query_time_ms))
    }

    /// Return the number of distinct entities in this table.
    pub fn entity_count(&self) -> usize {
        self.entities.len()
    }
}

impl Default for AggStateTable {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agg_descriptor::{AggregationDescriptor, NamedAggOp};
    use crate::agg_op::{AggKind, AggOpDescriptor};
    use crate::row::{Row, Value};

    // ── Helpers ──────────────────────────────────────────────────────────────

    fn count_op_desc() -> AggOpDescriptor {
        AggOpDescriptor {
            kind: AggKind::Count,
            field: None,
            window_ms: None,
            where_expr: None,
        }
    }

    fn sum_op_desc(field: &str) -> AggOpDescriptor {
        AggOpDescriptor {
            kind: AggKind::Sum,
            field: Some(field.to_string()),
            window_ms: None,
            where_expr: None,
        }
    }

    fn make_descriptor(
        node_name: &str,
        source: &str,
        keys: &[&str],
        features: &[(&str, AggOpDescriptor)],
    ) -> AggregationDescriptor {
        AggregationDescriptor {
            node_name: node_name.to_string(),
            source_node_name: source.to_string(),
            group_keys: keys.iter().map(|k| k.to_string()).collect(),
            features: features
                .iter()
                .map(|(name, d)| NamedAggOp {
                    feature_name: name.to_string(),
                    descriptor: d.clone(),
                })
                .collect(),
        }
    }

    // ── EntityKey tests ───────────────────────────────────────────────────────

    /// T01: group_keys=["user_id","merchant_id"], row has both → correct EntityKey.
    #[test]
    fn entity_key_from_row_extracts_group_keys_in_order() {
        let keys = vec!["user_id".to_string(), "merchant_id".to_string()];
        let row = Row::new()
            .with_field("user_id", Value::Str("a".to_string()))
            .with_field("merchant_id", Value::Str("m1".to_string()))
            .with_field("amount", Value::F64(10.0));

        let ek = EntityKey::from_row(&keys, &row).expect("should succeed");
        assert_eq!(
            ek,
            EntityKey(vec![
                ("user_id".to_string(), "a".to_string()),
                ("merchant_id".to_string(), "m1".to_string()),
            ])
        );
    }

    /// T02: Null group-key value → None (event dropped).
    #[test]
    fn entity_key_from_row_returns_none_on_null_field() {
        let keys = vec!["user_id".to_string()];
        let row = Row::new().with_field("user_id", Value::Null);
        assert!(
            EntityKey::from_row(&keys, &row).is_none(),
            "Null group-key value must produce None"
        );
    }

    /// T03: Missing group-key field → None (event dropped).
    #[test]
    fn entity_key_from_row_returns_none_on_missing_field() {
        let keys = vec!["user_id".to_string()];
        let row = Row::new(); // no fields at all
        assert!(
            EntityKey::from_row(&keys, &row).is_none(),
            "Missing group-key field must produce None"
        );
    }

    /// T04: I64(42) and F64(42.0) canonicalize to distinct strings — no collisions
    /// between integer and float entity keys.
    #[test]
    fn entity_key_normalises_numeric_values_deterministically() {
        let keys = vec!["id".to_string()];

        let row_i64 = Row::new().with_field("id", Value::I64(42));
        let row_f64 = Row::new().with_field("id", Value::F64(42.0));

        let ek_i = EntityKey::from_row(&keys, &row_i64).expect("I64 key");
        let ek_f = EntityKey::from_row(&keys, &row_f64).expect("F64 key");

        // They must NOT collide.
        assert_ne!(
            ek_i, ek_f,
            "I64(42) and F64(42.0) must canonicalize to distinct strings"
        );

        // Each must be deterministic (same call twice → same result).
        let ek_i2 = EntityKey::from_row(&keys, &row_i64).expect("I64 key again");
        assert_eq!(ek_i, ek_i2, "EntityKey must be deterministic");
    }

    /// T05: Bytes value → None (not a sane entity key in v0).
    #[test]
    fn entity_key_returns_none_for_bytes_value() {
        let keys = vec!["id".to_string()];
        let row = Row::new().with_field("id", Value::Bytes(vec![0x01, 0x02]));
        assert!(
            EntityKey::from_row(&keys, &row).is_none(),
            "Bytes group-key value must produce None"
        );
    }

    // ── AggStateTable tests ───────────────────────────────────────────────────

    /// T06: get_or_init on a new key creates a Vec with correct arity.
    #[test]
    fn agg_state_table_get_or_init_creates_row_of_correct_arity() {
        let desc = make_descriptor(
            "MyAgg",
            "Txn",
            &["user_id"],
            &[
                ("cnt", count_op_desc()),
                ("total", sum_op_desc("amount")),
                ("cnt2", count_op_desc()),
            ],
        );

        let mut table = AggStateTable::new();
        let key = EntityKey(vec![("user_id".to_string(), "alice".to_string())]);

        let row = table.get_or_init(&key, &desc);
        assert_eq!(
            row.len(),
            3,
            "entity row must have one slot per feature (3 features)"
        );
    }

    /// T07: get_or_init returns the same (now-mutated) slice on repeated calls.
    #[test]
    fn agg_state_table_get_or_init_returns_existing_on_repeat() {
        let desc = make_descriptor("A", "S", &["user_id"], &[("cnt", count_op_desc())]);
        let mut table = AggStateTable::new();
        let key = EntityKey(vec![("user_id".to_string(), "alice".to_string())]);

        // First call: creates the row.
        {
            let row = table.get_or_init(&key, &desc);
            // Mutate via update to give it a non-default state.
            row[0].update(
                &Row::new(),
                0,
                None,
                true, // where_matched
            );
        }

        // Second call: same key → must find the existing (mutated) row.
        {
            let row2 = table.get_or_init(&key, &desc);
            // Count should be 1 (from the update above), not 0 (re-initialised).
            assert_eq!(
                row2[0].query(0),
                Value::I64(1),
                "get_or_init must return the existing row, not re-initialise it"
            );
        }
    }

    /// T08: Five distinct keys pushed → entity_count == 5.
    #[test]
    fn agg_state_table_entity_count_counts_distinct_keys() {
        let desc = make_descriptor("A", "S", &["user_id"], &[("cnt", count_op_desc())]);
        let mut table = AggStateTable::new();

        for i in 0..5 {
            let key = EntityKey(vec![("user_id".to_string(), i.to_string())]);
            table.get_or_init(&key, &desc);
        }

        assert_eq!(table.entity_count(), 5);
    }

    /// T09: query_feature returns the value from the underlying AggOp.
    #[test]
    fn agg_state_table_query_feature_returns_value() {
        let desc = make_descriptor("A", "S", &["user_id"], &[("cnt", count_op_desc())]);
        let mut table = AggStateTable::new();
        let key = EntityKey(vec![("user_id".to_string(), "alice".to_string())]);

        // Push 3 events by mutating the entity row directly.
        {
            let row = table.get_or_init(&key, &desc);
            for _ in 0..3 {
                row[0].update(&Row::new(), 0, None, true);
            }
        }

        let val = table.query_feature(&key, 0, 0);
        assert_eq!(val, Some(Value::I64(3)), "query_feature must return I64(3)");
    }

    /// T10: query_feature returns None for an unknown key.
    #[test]
    fn agg_state_table_query_feature_returns_none_for_unknown_key() {
        let desc = make_descriptor("A", "S", &["user_id"], &[("cnt", count_op_desc())]);
        let table = AggStateTable::new();
        let key = EntityKey(vec![("user_id".to_string(), "unknown".to_string())]);

        // Never inserted key.
        let _ = desc; // suppress unused warning
        let val = table.query_feature(&key, 0, 0);
        assert!(val.is_none(), "unknown key must return None");
    }

    /// T11 (D-06 grep guard): file uses BTreeMap; must NOT use HashMap.
    #[test]
    fn agg_state_table_uses_btreemap() {
        let src = include_str!("agg_state_table.rs");

        assert!(
            src.contains("BTreeMap"),
            "agg_state_table.rs must use BTreeMap for deterministic iteration (D-06)"
        );

        // The source of THIS file must not contain "HashMap" outside of test comments.
        // We split the forbidden pattern to avoid triggering the very check we write.
        let forbidden = ["Hash", "Map"].concat();
        // Only the test module references it (in this comment) — the production code must not.
        // Count occurrences in non-test (pre-#[cfg(test)]) portion:
        let test_marker = "#[cfg(test)]";
        let production_src = src.split(test_marker).next().unwrap_or("");
        assert!(
            !production_src.contains(forbidden.as_str()),
            "agg_state_table.rs production code must not use HashMap (D-06 determinism)"
        );
    }
}
