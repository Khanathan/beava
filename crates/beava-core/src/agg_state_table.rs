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
//!   invocations — required for WAL replay determinism (SC4). Plan 18-11
//!   Task 11.6 will swap this to `hashbrown` with iter-sorted snapshot.
//! - `EntityKey` (Plan 18-11 D-5) is a SmallVec of
//!   `(group_key_name, native_Value)` pairs in declaration order
//!   (the order of `AggregationDescriptor::group_keys`).
//!   No string canonicalization on the hot path — Value variants discriminate
//!   I64/F64/Str/Bool/Datetime natively.
//! - Null or missing group-key values produce `None` from `EntityKey::from_row`,
//!   causing the apply loop to drop the event for that aggregation.
//!
//! # Value variants accepted in EntityKey
//!
//! | `Value` variant    | inclusion         | notes                            |
//! |--------------------|-------------------|----------------------------------|
//! | `Str(CompactStr)`  | included          | inline-storage for ≤24 bytes     |
//! | `I64(n)`           | included          | distinct from F64 by variant     |
//! | `F64(f)`           | included          | NaN handled via total_cmp        |
//! | `Bool(b)`          | included          |                                  |
//! | `Datetime(ms)`     | included          |                                  |
//! | `Bytes(_)`         | → `None` (drop)   | bytes are not sane keys in v0    |
//! | `Null`             | → `None` (drop)   | null group-key means no entity   |
//! | `Json/List/Map`    | → `None` (drop)   | structured outputs aren't keys   |

use std::hash::{Hash, Hasher};

use crate::agg_descriptor::AggregationDescriptor;
use crate::agg_op::AggOp;
use crate::row::{Row, Value};
use compact_str::CompactString;
use fxhash::FxBuildHasher;
use hashbrown::hash_map::RawEntryMut;
use hashbrown::HashMap;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

// ─── EntityKey ────────────────────────────────────────────────────────────────

/// Stable entity identifier for per-aggregation state lookup.
///
/// Plan 18-11 D-5: SmallVec inline storage of `(CompactString, Value)` pairs
/// in declaration order (the order of `AggregationDescriptor::group_keys`).
/// Most aggregations group by 1-2 keys → inline storage, zero heap alloc on
/// the hot path.
///
/// Implements `Hash + Eq + PartialOrd + Ord` over the SmallVec contents so it
/// can serve as both a HashMap key (Plan 18-11 D-4) and a sortable key for
/// snapshot determinism (Plan 18-11 D-8).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityKey(pub SmallVec<[(CompactString, Value); 2]>);

impl PartialEq for EntityKey {
    fn eq(&self, other: &Self) -> bool {
        if self.0.len() != other.0.len() {
            return false;
        }
        self.0
            .iter()
            .zip(other.0.iter())
            .all(|((ak, av), (bk, bv))| ak == bk && entity_value_eq(av, bv))
    }
}

impl Eq for EntityKey {}

impl Hash for EntityKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash length first, then each (key, value) pair via hash_entity_value.
        self.0.len().hash(state);
        for (k, v) in &self.0 {
            k.hash(state);
            hash_entity_value(v, state);
        }
    }
}

impl PartialOrd for EntityKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for EntityKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Lex order over the (key, value) sequence.
        for ((ak, av), (bk, bv)) in self.0.iter().zip(other.0.iter()) {
            match ak.cmp(bk) {
                std::cmp::Ordering::Equal => {}
                ord => return ord,
            }
            match cmp_entity_value(av, bv) {
                std::cmp::Ordering::Equal => {}
                ord => return ord,
            }
        }
        self.0.len().cmp(&other.0.len())
    }
}

/// Equality over the Value variants permitted inside an EntityKey.
/// Cross-variant comparisons return false. F64 NaN compares equal to
/// itself here (entity-key context — we want determinism, not IEEE-754).
fn entity_value_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Str(x), Value::Str(y)) => x == y,
        (Value::I64(x), Value::I64(y)) => x == y,
        (Value::F64(x), Value::F64(y)) => x.to_bits() == y.to_bits(),
        (Value::Bool(x), Value::Bool(y)) => x == y,
        (Value::Datetime(x), Value::Datetime(y)) => x == y,
        // Cross-variant or unsupported (Bytes/Null/Json/List/Map shouldn't
        // appear here — from_row drops them) → false.
        _ => false,
    }
}

/// Hash a Value as an EntityKey component. Variant tag + payload — must
/// agree with `entity_value_eq` (Eq–Hash contract).
fn hash_entity_value<H: Hasher>(v: &Value, state: &mut H) {
    match v {
        Value::Str(s) => {
            0u8.hash(state);
            s.as_str().hash(state);
        }
        Value::I64(n) => {
            1u8.hash(state);
            n.hash(state);
        }
        Value::F64(f) => {
            2u8.hash(state);
            f.to_bits().hash(state);
        }
        Value::Bool(b) => {
            3u8.hash(state);
            b.hash(state);
        }
        Value::Datetime(ms) => {
            4u8.hash(state);
            ms.hash(state);
        }
        // Variants below shouldn't appear in EntityKey (from_row drops them);
        // hash them as a sentinel for total robustness.
        _ => {
            255u8.hash(state);
        }
    }
}

/// Total ordering over Value variants permitted inside an EntityKey. Variant
/// discrimination first (Str < I64 < F64 < Bool < Datetime), then payload
/// order. F64 uses `total_cmp` so NaN sorts deterministically.
fn cmp_entity_value(a: &Value, b: &Value) -> std::cmp::Ordering {
    fn rank(v: &Value) -> u8 {
        match v {
            Value::Str(_) => 0,
            Value::I64(_) => 1,
            Value::F64(_) => 2,
            Value::Bool(_) => 3,
            Value::Datetime(_) => 4,
            _ => 255,
        }
    }
    match rank(a).cmp(&rank(b)) {
        std::cmp::Ordering::Equal => match (a, b) {
            (Value::Str(x), Value::Str(y)) => x.cmp(y),
            (Value::I64(x), Value::I64(y)) => x.cmp(y),
            (Value::F64(x), Value::F64(y)) => x.total_cmp(y),
            (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
            (Value::Datetime(x), Value::Datetime(y)) => x.cmp(y),
            _ => std::cmp::Ordering::Equal,
        },
        ord => ord,
    }
}

impl EntityKey {
    /// Build an `EntityKey` from a `Row` by extracting the fields named in
    /// `group_keys` and canonicalising each to a `Value::Str(CompactString)`.
    ///
    /// Returns `None` if any group-key field is absent or carries a
    /// non-key-safe Value variant (Null / Bytes / Json / List / Map).
    ///
    /// **Canonicalization (preserved from pre-Plan-18-11 behaviour for URL
    /// query compat):** all group-key values are stringified into
    /// CompactString and wrapped in `Value::Str`. This keeps query-side URL
    /// parsing (which always sees strings) consistent with apply-side
    /// EntityKey construction. I64(42) and F64(42.0) canonicalize to
    /// distinct strings ("42" vs "42.0") just as before.
    pub fn from_row(group_keys: &[String], row: &Row) -> Option<EntityKey> {
        let mut pairs: SmallVec<[(CompactString, Value); 2]> =
            SmallVec::with_capacity(group_keys.len());
        for key in group_keys {
            let canonical: CompactString = match row.get(key) {
                None => return None,                  // missing field → drop
                Some(Value::Null) => return None,     // null field → drop
                Some(Value::Bytes(_)) => return None, // bytes not sane as key → drop
                Some(Value::Json(_)) => return None,  // Json not sane as group key
                // Phase 11 (D-01): structured outputs (List/Map) are never
                // legal as group-by keys — drop the event for this aggregation.
                Some(Value::List(_)) | Some(Value::Map(_)) => return None,
                Some(Value::Str(s)) => s.clone(),
                Some(Value::I64(n)) => n.to_string().into(),
                Some(Value::F64(f)) => format!("{:?}", f).into(),
                Some(Value::Bool(b)) => b.to_string().into(),
                Some(Value::Datetime(ms)) => ms.to_string().into(),
            };
            pairs.push((CompactString::from(key.as_str()), Value::Str(canonical)));
        }
        Some(EntityKey(pairs))
    }
}

// ─── AggStateTable ────────────────────────────────────────────────────────────

/// Per-aggregation state store: maps `EntityKey → Vec<AggOp>` (one slot per
/// feature in `AggregationDescriptor::features`).
///
/// **Plan 18-11 D-4:** uses `hashbrown::HashMap` with `FxBuildHasher` for
/// O(1) lookup on the apply hot path. FxBuildHasher is non-cryptographic
/// and ~3× faster than the default SipHasher for short keys — safe here
/// because the apply path is single-process, single-writer (no DoS attack
/// surface).
///
/// **Plan 18-11 D-8 — snapshot determinism:** the implicit BTreeMap
/// ordering is replaced by an explicit `iter_sorted()` method that materializes
/// a sorted Vec at snapshot-write time. Hot path stays O(1); the sort cost
/// (O(N log N)) lands once per snapshot (cold path).
///
/// Hot-path lookup uses `raw_entry_mut().from_key(key)` so the borrowed
/// `&EntityKey` doesn't need to be cloned on lookup; only the cold-path
/// vacant insert clones into the map.
pub struct AggStateTable {
    pub entities: HashMap<EntityKey, Vec<AggOp>, FxBuildHasher>,
}

impl AggStateTable {
    /// Create an empty table.
    pub fn new() -> Self {
        AggStateTable {
            entities: HashMap::with_hasher(FxBuildHasher::default()),
        }
    }

    /// Look up the per-entity `Vec<AggOp>` for `key`. If the key is new,
    /// initialise a fresh `Vec` with one `AggOp::new` per feature in `descriptor`.
    ///
    /// Plan 18-11 D-4: uses `raw_entry_mut().from_key(key)` so the lookup
    /// doesn't clone the key. Only the cold-path vacant insert clones.
    /// Returns a mutable reference to the entity row so the apply loop can
    /// call `update_with_row` on each slot.
    pub fn get_or_init(
        &mut self,
        key: &EntityKey,
        descriptor: &AggregationDescriptor,
    ) -> &mut Vec<AggOp> {
        match self.entities.raw_entry_mut().from_key(key) {
            RawEntryMut::Occupied(entry) => entry.into_mut(),
            RawEntryMut::Vacant(slot) => {
                let new_row: Vec<AggOp> = descriptor
                    .features
                    .iter()
                    .map(|f| AggOp::new(&f.descriptor))
                    .collect();
                let (_, v) = slot.insert(key.clone(), new_row);
                v
            }
        }
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

    /// Plan 18-11 D-8: iterate entries in EntityKey-sorted order. Used by
    /// the snapshot writer + debug routes to preserve D-06 determinism
    /// despite the underlying HashMap's unordered iteration. Hot path
    /// (per-push apply) uses HashMap O(1); sort cost lands once per
    /// snapshot (cold path).
    pub fn iter_sorted(&self) -> impl Iterator<Item = (&EntityKey, &Vec<AggOp>)> {
        let mut entries: Vec<(&EntityKey, &Vec<AggOp>)> = self.entities.iter().collect();
        entries.sort_by(|(a, _), (b, _)| a.cmp(b));
        entries.into_iter()
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

    /// Build a single-key EntityKey from a user_id string (test helper).
    fn make_user_key(user_id: &str) -> EntityKey {
        let pair: (CompactString, Value) = ("user_id".into(), Value::Str(user_id.into()));
        EntityKey(SmallVec::from_buf_and_len(
            [pair, ("".into(), Value::Null)],
            1,
        ))
    }

    /// Same but for I64 user_id.
    fn make_user_key_from_i64(n: i64) -> EntityKey {
        let pair: (CompactString, Value) = ("user_id".into(), Value::Str(n.to_string().into()));
        EntityKey(SmallVec::from_buf_and_len(
            [pair, ("".into(), Value::Null)],
            1,
        ))
    }

    fn count_op_desc() -> AggOpDescriptor {
        AggOpDescriptor {
            kind: AggKind::Count,
            field: None,
            window_ms: None,
            where_expr: None,
            n: None,
            half_life_ms: None,
            sub_window_ms: None,
            sigma: None,
            sketch_params: None,
            ext: Default::default(),
        }
    }

    fn sum_op_desc(field: &str) -> AggOpDescriptor {
        AggOpDescriptor {
            kind: AggKind::Sum,
            field: Some(field.to_string()),
            window_ms: None,
            where_expr: None,
            n: None,
            half_life_ms: None,
            sub_window_ms: None,
            sigma: None,
            sketch_params: None,
            ext: Default::default(),
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
            .with_field("user_id", Value::Str("a".into()))
            .with_field("merchant_id", Value::Str("m1".into()))
            .with_field("amount", Value::F64(10.0));

        let ek = EntityKey::from_row(&keys, &row).expect("should succeed");
        // Plan 18-11 D-5: EntityKey carries native (CompactString, Value) pairs.
        let expected: SmallVec<[(CompactString, Value); 2]> = SmallVec::from_buf([
            ("user_id".into(), Value::Str("a".into())),
            ("merchant_id".into(), Value::Str("m1".into())),
        ]);
        assert_eq!(ek, EntityKey(expected));
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
        let key = make_user_key("alice");

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
        let key = make_user_key("alice");

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
            let key = make_user_key_from_i64(i);
            table.get_or_init(&key, &desc);
        }

        assert_eq!(table.entity_count(), 5);
    }

    /// T09: query_feature returns the value from the underlying AggOp.
    #[test]
    fn agg_state_table_query_feature_returns_value() {
        let desc = make_descriptor("A", "S", &["user_id"], &[("cnt", count_op_desc())]);
        let mut table = AggStateTable::new();
        let key = make_user_key("alice");

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
        let key = make_user_key("unknown");

        // Never inserted key.
        let _ = desc; // suppress unused warning
        let val = table.query_feature(&key, 0, 0);
        assert!(val.is_none(), "unknown key must return None");
    }

    /// T11 (Plan 18-11 D-8 replacement of D-06 grep guard):
    /// Snapshot determinism is now preserved by `iter_sorted` + sort-on-write
    /// instead of by BTreeMap-implicit ordering. This test asserts the new
    /// invariant: two state tables built from the same input event sequence
    /// must yield byte-identical iter_sorted output regardless of HashMap
    /// insertion order.
    #[test]
    fn agg_state_table_iter_sorted_byte_identical_for_same_inputs() {
        let desc = make_descriptor("A", "S", &["user_id"], &[("cnt", count_op_desc())]);
        let users = ["alice", "bob", "carol", "dan"];

        // Build two tables — order of inserts is the same; HashMap may pick
        // different bucket layouts but iter_sorted must yield the same order.
        let mut t1 = AggStateTable::new();
        let mut t2 = AggStateTable::new();
        for u in users {
            t1.get_or_init(&make_user_key(u), &desc);
            t2.get_or_init(&make_user_key(u), &desc);
        }

        let view1: Vec<&EntityKey> = t1.iter_sorted().map(|(k, _)| k).collect();
        let view2: Vec<&EntityKey> = t2.iter_sorted().map(|(k, _)| k).collect();
        assert_eq!(view1, view2, "iter_sorted must be deterministic");

        // Sorted yields the lex order: alice, bob, carol, dan.
        let names: Vec<String> = view1
            .iter()
            .map(|k| match &k.0[0].1 {
                Value::Str(s) => s.to_string(),
                _ => panic!("expected Value::Str"),
            })
            .collect();
        assert_eq!(names, vec!["alice", "bob", "carol", "dan"]);
    }

    // ── Plan 18-11 Task 11.3: EntityKey SmallVec + CompactString + Value ─────

    /// Plan 18-11 D-5: EntityKey backing storage is `SmallVec<[(CompactString, Value); 2]>`
    /// (was `Vec<(String, String)>`). Most aggregations group by 1-2 keys → inline storage
    /// → zero heap alloc on construction.
    ///
    /// This is a compile-fail RED until the storage type is changed.
    #[test]
    fn entity_key_smallvec_inline() {
        use compact_str::CompactString;
        use smallvec::SmallVec;

        // Construction with 1 group key — uses inline SmallVec storage (no heap).
        let pair: (CompactString, Value) = ("user_id".into(), Value::Str("alice".into()));
        let inline_storage: SmallVec<[(CompactString, Value); 2]> =
            SmallVec::from_buf_and_len([pair, ("".into(), Value::Null)], 1);
        let ek = EntityKey(inline_storage);

        // Spilled-to-heap check: SmallVec exposes spilled() — true when over inline cap.
        assert!(
            !ek.0.spilled(),
            "1-key EntityKey must use inline SmallVec storage (no heap)"
        );
        assert_eq!(ek.0.len(), 1);

        // 2 group keys also fit inline.
        let pair_a: (CompactString, Value) = ("user_id".into(), Value::Str("a".into()));
        let pair_b: (CompactString, Value) = ("merchant_id".into(), Value::Str("m1".into()));
        let two: SmallVec<[(CompactString, Value); 2]> = SmallVec::from_buf([pair_a, pair_b]);
        let ek2 = EntityKey(two);
        assert!(!ek2.0.spilled(), "2-key EntityKey must use inline storage");
    }

    // ── Plan 18-11 Task 11.6: AggStateTable HashMap + raw_entry_mut + iter_sorted ──

    /// AggStateTable.entities is a hashbrown::HashMap with FxBuildHasher
    /// (Plan 18-11 D-4). Production type is exposed so callers can rely on
    /// raw_entry_mut on the hot path. Compile-fail RED until the swap.
    #[test]
    fn agg_state_table_uses_hashbrown_hashmap_with_fx_hasher() {
        // Type-name string check — the assertion must reference the new type.
        let table: AggStateTable = AggStateTable::new();
        let typename = std::any::type_name_of_val(&table.entities);
        assert!(
            typename.contains("hashbrown") && typename.contains("HashMap"),
            "AggStateTable.entities must be hashbrown::HashMap, got {}",
            typename
        );
        // FxHashBuilder presence (the third type parameter of HashMap).
        assert!(
            typename.contains("Fx") || typename.contains("fxhash"),
            "AggStateTable.entities must use FxBuildHasher, got {}",
            typename
        );
    }

    /// iter_sorted returns entries in EntityKey-sorted order regardless of
    /// HashMap insertion order (snapshot determinism per Plan 18-11 D-8).
    #[test]
    fn agg_state_table_iter_sorted_is_deterministic() {
        let desc = make_descriptor("A", "S", &["user_id"], &[("cnt", count_op_desc())]);
        let mut table = AggStateTable::new();

        // Insert in a non-sorted order.
        for u in ["zebra", "alice", "monkey", "bob"] {
            let key = make_user_key(u);
            table.get_or_init(&key, &desc);
        }

        // iter_sorted yields lex-ordered EntityKeys.
        let sorted_users: Vec<String> = table
            .iter_sorted()
            .map(|(k, _)| match &k.0[0].1 {
                Value::Str(s) => s.to_string(),
                other => panic!("expected Value::Str, got {:?}", other),
            })
            .collect();
        assert_eq!(sorted_users, vec!["alice", "bob", "monkey", "zebra"]);
    }

    /// EntityKey::from_row canonicalises each group-key value to a
    /// `Value::Str(CompactString)` (pre-Plan-18-11 behaviour preserved for
    /// URL-query compat). I64(42) and F64(42.0) canonicalise to distinct
    /// strings ("42" vs "42.0") so they remain non-colliding entity keys.
    #[test]
    fn entity_key_from_row_yields_canonicalised_str_pairs() {
        let keys = vec!["user_id".to_string()];
        let row_i64 = Row::new().with_field("user_id", Value::I64(42));
        let row_f64 = Row::new().with_field("user_id", Value::F64(42.0));

        let ek_i = EntityKey::from_row(&keys, &row_i64).expect("I64 EntityKey");
        let ek_f = EntityKey::from_row(&keys, &row_f64).expect("F64 EntityKey");

        // Distinct keys via stringified canonical ("42" vs "42.0").
        assert_ne!(ek_i, ek_f);

        match &ek_i.0[0].1 {
            Value::Str(s) if s.as_str() == "42" => {}
            other => panic!("expected Value::Str(\"42\") in EntityKey, got {:?}", other),
        }
        match &ek_f.0[0].1 {
            Value::Str(s) if s.as_str() == "42.0" => {}
            other => panic!(
                "expected Value::Str(\"42.0\") in EntityKey, got {:?}",
                other
            ),
        }
    }
}
