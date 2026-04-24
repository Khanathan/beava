//! Row + Value data model for Phase 4 stateless ops and expression evaluation.
//!
//! Implements the shapes defined in CONTEXT.md §D-03 and the SQL three-valued
//! null logic specified in CONTEXT.md §D-04.
//!
//! # Key design choices
//! - `Row` wraps a `BTreeMap<String, Value>` for deterministic iteration order
//!   (Phase 5 aggregation-key stability depends on this).
//! - `Value::F64` uses a custom `PartialEq` that treats NaN as never equal.
//! - Boolean helpers (`and_three_valued`, `or_three_valued`, `not_three_valued`)
//!   implement the full SQL null truth table including short-circuit semantics.
//! - Row mutation helpers (`with_field`, `without_field`, `renamed`) consume
//!   `self` and return the updated `Row`. This satisfies SDK-OPS-09: derivation
//!   op steps construct a new `Row` per step rather than mutating shared state.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// ─── Value ────────────────────────────────────────────────────────────────────

/// A dynamically-typed scalar value used in Row fields and expression results.
///
/// Mirrors `FieldType` one-to-one (see `type_of()`). `Null` has no `FieldType`
/// equivalent and signals absence/unknown per SQL three-valued logic (§D-04).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Null,
    Str(String),
    I64(i64),
    /// NaN-safe: two `F64(NaN)` values are never equal (see `PartialEq` impl).
    F64(f64),
    Bool(bool),
    Bytes(Vec<u8>),
    /// Milliseconds since Unix epoch — matches the `event_time` convention.
    Datetime(i64),
    /// Phase 11 (D-01): ordered list of values used as an aggregation output
    /// (e.g. `most_recent_n`, `reservoir_sample`). Never appears in event/table
    /// rows — only as the output of `AggOp::query`. `type_of()` → None.
    List(Vec<Value>),
    /// Phase 11 (D-01): keyed map of values used as a structured aggregation
    /// output (e.g. `histogram`, `event_type_mix`). Never appears in
    /// event/table rows — only as the output of `AggOp::query`. `type_of()` → None.
    Map(BTreeMap<String, Value>),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            // NaN is never equal to anything, including itself (IEEE-754).
            (Value::F64(a), Value::F64(b)) => !a.is_nan() && !b.is_nan() && a == b,
            (Value::Str(a), Value::Str(b)) => a == b,
            (Value::I64(a), Value::I64(b)) => a == b,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Bytes(a), Value::Bytes(b)) => a == b,
            (Value::Datetime(a), Value::Datetime(b)) => a == b,
            // Phase 11: List + Map recurse element-wise (BTreeMap iteration is
            // ordered + deterministic; PartialEq on Vec is positional).
            (Value::List(a), Value::List(b)) => a == b,
            (Value::Map(a), Value::Map(b)) => a == b,
            // Cross-variant comparisons are always false.
            _ => false,
        }
    }
}

impl Value {
    /// Returns the corresponding `FieldType` for non-Null values, or `None` for `Null`.
    pub fn type_of(&self) -> Option<crate::schema::FieldType> {
        use crate::schema::FieldType;
        match self {
            Value::Null => None,
            Value::Str(_) => Some(FieldType::Str),
            Value::I64(_) => Some(FieldType::I64),
            Value::F64(_) => Some(FieldType::F64),
            Value::Bool(_) => Some(FieldType::Bool),
            Value::Bytes(_) => Some(FieldType::Bytes),
            Value::Datetime(_) => Some(FieldType::Datetime),
            // Phase 11: structured outputs are not representable as a
            // FieldType — they only appear as aggregation outputs, never in
            // event/table rows.
            Value::List(_) => None,
            Value::Map(_) => None,
        }
    }

    /// SQL three-valued AND.
    ///
    /// Truth table (§D-04):
    /// - `false AND null  = false`   (short-circuit)
    /// - `null  AND false = false`   (short-circuit)
    /// - `true  AND true  = true`
    /// - `true  AND null  = null`
    /// - `null  AND true  = null`
    /// - `null  AND null  = null`
    /// - Non-bool/non-null operands → `Null` (runtime-tolerant per §D-04)
    pub fn and_three_valued(&self, other: &Self) -> Self {
        match (self, other) {
            // Short-circuit: false on either side always yields false.
            (Value::Bool(false), Value::Bool(_))
            | (Value::Bool(false), Value::Null)
            | (Value::Bool(_), Value::Bool(false))
            | (Value::Null, Value::Bool(false)) => Value::Bool(false),
            // Both true.
            (Value::Bool(true), Value::Bool(true)) => Value::Bool(true),
            // At least one null, no short-circuit false → null.
            (Value::Null, Value::Null)
            | (Value::Null, Value::Bool(true))
            | (Value::Bool(true), Value::Null) => Value::Null,
            // Any non-bool/non-null operand → Null (runtime-tolerant).
            _ => Value::Null,
        }
    }

    /// SQL three-valued OR.
    ///
    /// Truth table (§D-04):
    /// - `true  OR null  = true`   (short-circuit)
    /// - `null  OR true  = true`   (short-circuit)
    /// - `false OR false = false`
    /// - `false OR null  = null`
    /// - `null  OR false = null`
    /// - `null  OR null  = null`
    /// - Non-bool/non-null operands → `Null` (runtime-tolerant per §D-04)
    pub fn or_three_valued(&self, other: &Self) -> Self {
        match (self, other) {
            // Short-circuit: true on either side always yields true.
            (Value::Bool(true), Value::Bool(_))
            | (Value::Bool(true), Value::Null)
            | (Value::Bool(_), Value::Bool(true))
            | (Value::Null, Value::Bool(true)) => Value::Bool(true),
            // Both false.
            (Value::Bool(false), Value::Bool(false)) => Value::Bool(false),
            // At least one null, no short-circuit true → null.
            (Value::Null, Value::Null)
            | (Value::Null, Value::Bool(false))
            | (Value::Bool(false), Value::Null) => Value::Null,
            // Any non-bool/non-null operand → Null (runtime-tolerant).
            _ => Value::Null,
        }
    }

    /// SQL three-valued NOT.
    ///
    /// - `NOT true  = false`
    /// - `NOT false = true`
    /// - `NOT null  = null`
    /// - Non-bool/non-null → `Null` (runtime-tolerant per §D-04)
    pub fn not_three_valued(&self) -> Self {
        match self {
            Value::Bool(b) => Value::Bool(!b),
            Value::Null => Value::Null,
            _ => Value::Null,
        }
    }
}

// ─── Row ──────────────────────────────────────────────────────────────────────

/// A named-field bag of `Value`s backed by a `BTreeMap` for deterministic
/// iteration order.
///
/// All "mutation" helpers consume `self` and return a new `Row`. This is the
/// owning API contract required by SDK-OPS-09: stateless op steps must not
/// mutate a shared upstream row.
#[derive(Debug, Clone, PartialEq)]
pub struct Row(pub BTreeMap<String, Value>);

impl Row {
    /// Creates an empty Row.
    pub fn new() -> Self {
        Row(BTreeMap::new())
    }

    /// Returns a reference to the value for `field`, or `None` if absent.
    pub fn get(&self, field: &str) -> Option<&Value> {
        self.0.get(field)
    }

    /// Consumes `self`, inserts or overwrites `field` with `value`, and returns
    /// the updated `Row`.
    ///
    /// SDK-OPS-09: callers that need to preserve the upstream `Row` must
    /// `.clone()` before calling this method. Derivation op steps construct a
    /// fresh copy per step and do not share mutable state.
    pub fn with_field(mut self, field: &str, value: Value) -> Self {
        self.0.insert(field.to_string(), value);
        self
    }

    /// Consumes `self`, removes `field` (no-op if absent), and returns the
    /// updated `Row`.
    pub fn without_field(mut self, field: &str) -> Self {
        self.0.remove(field);
        self
    }

    /// Consumes `self`, renames `old` to `new` (preserving the value), and
    /// returns the updated `Row`. If `old` is absent this is a no-op. If `new`
    /// already exists it is overwritten.
    pub fn renamed(mut self, old: &str, new: &str) -> Self {
        if let Some(value) = self.0.remove(old) {
            self.0.insert(new.to_string(), value);
        }
        self
    }

    /// Returns an iterator over `(field, value)` pairs in BTreeMap order.
    pub fn iter(&self) -> std::collections::btree_map::Iter<'_, String, Value> {
        self.0.iter()
    }

    /// Returns the number of fields in this Row.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if this Row contains no fields.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Default for Row {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::FieldType;

    // Test 1: Value::type_of maps every non-Null variant to the expected FieldType;
    // Null returns None.
    #[test]
    fn value_type_of_maps_every_variant_to_fieldtype() {
        assert_eq!(Value::Null.type_of(), None);
        assert_eq!(Value::Str("x".into()).type_of(), Some(FieldType::Str));
        assert_eq!(Value::I64(1).type_of(), Some(FieldType::I64));
        assert_eq!(Value::F64(1.0).type_of(), Some(FieldType::F64));
        assert_eq!(Value::Bool(true).type_of(), Some(FieldType::Bool));
        assert_eq!(Value::Bytes(vec![]).type_of(), Some(FieldType::Bytes));
        assert_eq!(Value::Datetime(0).type_of(), Some(FieldType::Datetime));
    }

    // Test 2: NaN is never equal to itself.
    #[test]
    fn value_partialeq_nan_is_never_equal() {
        let a = Value::F64(f64::NAN);
        let b = Value::F64(f64::NAN);
        assert_ne!(a, b, "NaN != NaN per IEEE-754 (guarded in PartialEq)");
    }

    // Test 3: Structural equality — Value::Null == Value::Null at the Rust level.
    // SQL "null == null" yields Value::Null (tested through and/or helpers), but
    // the Rust PartialEq on Value treats Null structurally.
    #[test]
    fn value_partialeq_null_equals_null_structurally() {
        assert_eq!(
            Value::Null,
            Value::Null,
            "Value::Null must structurally equal itself"
        );
    }

    // Test 4: and_three_valued truth table.
    #[test]
    fn and_three_valued_truth_table() {
        let t = Value::Bool(true);
        let f = Value::Bool(false);
        let n = Value::Null;

        assert_eq!(t.and_three_valued(&t), Value::Bool(true));
        assert_eq!(t.and_three_valued(&f), Value::Bool(false));
        assert_eq!(t.and_three_valued(&n), Value::Null);
        assert_eq!(f.and_three_valued(&n), Value::Bool(false)); // short-circuit
        assert_eq!(n.and_three_valued(&f), Value::Bool(false)); // short-circuit
        assert_eq!(n.and_three_valued(&n), Value::Null);
        assert_eq!(n.and_three_valued(&t), Value::Null);
        assert_eq!(f.and_three_valued(&t), Value::Bool(false));
    }

    // Test 5: or_three_valued truth table.
    #[test]
    fn or_three_valued_truth_table() {
        let t = Value::Bool(true);
        let f = Value::Bool(false);
        let n = Value::Null;

        assert_eq!(t.or_three_valued(&n), Value::Bool(true)); // short-circuit
        assert_eq!(n.or_three_valued(&t), Value::Bool(true)); // short-circuit
        assert_eq!(f.or_three_valued(&n), Value::Null);
        assert_eq!(n.or_three_valued(&f), Value::Null);
        assert_eq!(n.or_three_valued(&n), Value::Null);
        assert_eq!(f.or_three_valued(&f), Value::Bool(false));
        assert_eq!(t.or_three_valued(&t), Value::Bool(true));
        assert_eq!(t.or_three_valued(&f), Value::Bool(true)); // short-circuit on left
    }

    // Test 6: not_three_valued truth table.
    #[test]
    fn not_three_valued_truth_table() {
        assert_eq!(Value::Bool(true).not_three_valued(), Value::Bool(false));
        assert_eq!(Value::Bool(false).not_three_valued(), Value::Bool(true));
        assert_eq!(Value::Null.not_three_valued(), Value::Null);
    }

    // Test 7: Non-bool operands to and/or return Null (runtime tolerance per §D-04).
    #[test]
    fn and_or_reject_non_bool_operands() {
        let i = Value::I64(1);
        let t = Value::Bool(true);
        let n = Value::Null;

        // i64 operand to AND → Null
        assert_eq!(i.and_three_valued(&t), Value::Null);
        assert_eq!(t.and_three_valued(&i), Value::Null);
        assert_eq!(i.and_three_valued(&n), Value::Null);

        // i64 operand to OR → Null
        assert_eq!(i.or_three_valued(&t), Value::Null);
        assert_eq!(t.or_three_valued(&i), Value::Null);
    }

    // Test 8: Row::new() produces an empty Row.
    #[test]
    fn row_new_is_empty() {
        let r = Row::new();
        assert_eq!(r.len(), 0);
        assert!(r.is_empty());
    }

    // Test 9: with_field consumes self and returns updated Row; original binding
    // cannot be used afterward (ownership transferred). Verify via a second alias.
    // SDK-OPS-09: owning API, no shared mutable state.
    #[test]
    fn row_with_field_returns_new_row_without_mutating_source() {
        let r1 = Row::new();
        let r2 = r1.with_field("x", Value::I64(7));
        // r1 has been consumed; r2 has "x"
        assert_eq!(r2.get("x"), Some(&Value::I64(7)));
        // Aliasing test: clone r2 before modifying to show originals are unaffected
        let r3 = r2.clone();
        let r4 = r3.with_field("y", Value::I64(99));
        // r2 still only has "x"
        assert_eq!(r2.get("y"), None);
        assert_eq!(r4.get("x"), Some(&Value::I64(7)));
        assert_eq!(r4.get("y"), Some(&Value::I64(99)));
    }

    // Test 10: without_field removes the named field.
    #[test]
    fn row_without_field_removes_field() {
        let r = Row::new()
            .with_field("x", Value::I64(1))
            .with_field("y", Value::I64(2));
        let r2 = r.without_field("x");
        assert_eq!(r2.get("x"), None);
        assert_eq!(r2.get("y"), Some(&Value::I64(2)));
    }

    // Test 11: renamed swaps key while preserving value; renaming absent key is a no-op.
    #[test]
    fn row_renamed_swaps_key_preserving_value() {
        let r = Row::new().with_field("a", Value::I64(5));
        let r2 = r.renamed("a", "b");
        assert_eq!(r2.get("b"), Some(&Value::I64(5)));
        assert_eq!(r2.get("a"), None);

        // Renaming absent field is a no-op
        let r3 = r2.renamed("no_such_field", "z");
        assert_eq!(r3.get("z"), None);
        assert_eq!(r3.get("b"), Some(&Value::I64(5)));
    }

    // Test 12: iter yields keys in BTreeMap sorted order regardless of insertion order.
    #[test]
    fn row_iter_order_is_deterministic() {
        let r = Row::new()
            .with_field("c", Value::I64(3))
            .with_field("a", Value::I64(1))
            .with_field("b", Value::I64(2));
        let keys: Vec<&str> = r.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, vec!["a", "b", "c"]);
    }

    // ── Phase 11: Value::List + Value::Map for structured outputs (D-01) ──────

    /// Phase 11 — D-01: Value::List variant exists and round-trips through serde.
    #[test]
    fn value_list_round_trips_serde() {
        let v = Value::List(vec![Value::I64(1), Value::I64(2), Value::I64(3)]);
        let s = serde_json::to_string(&v).expect("serialize List");
        let v2: Value = serde_json::from_str(&s).expect("deserialize List");
        assert_eq!(v, v2);
    }

    /// Phase 11 — D-01: Value::Map variant round-trips deterministically (BTreeMap).
    #[test]
    fn value_map_round_trips_serde() {
        let mut m = BTreeMap::new();
        m.insert("a".to_string(), Value::I64(10));
        m.insert("b".to_string(), Value::F64(2.5));
        let v = Value::Map(m);
        let s = serde_json::to_string(&v).expect("serialize Map");
        let v2: Value = serde_json::from_str(&s).expect("deserialize Map");
        assert_eq!(v, v2);
    }

    /// PartialEq on Value::List compares element-wise.
    #[test]
    fn value_list_partialeq_elementwise() {
        let a = Value::List(vec![Value::I64(1), Value::Str("x".into())]);
        let b = Value::List(vec![Value::I64(1), Value::Str("x".into())]);
        let c = Value::List(vec![Value::I64(1), Value::Str("y".into())]);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    /// PartialEq on Value::Map matches when same keys + same values.
    #[test]
    fn value_map_partialeq_keys_and_values() {
        let mut m1 = BTreeMap::new();
        m1.insert("k".to_string(), Value::I64(1));
        let mut m2 = BTreeMap::new();
        m2.insert("k".to_string(), Value::I64(1));
        let mut m3 = BTreeMap::new();
        m3.insert("k".to_string(), Value::I64(2));
        assert_eq!(Value::Map(m1.clone()), Value::Map(m2));
        assert_ne!(Value::Map(m1), Value::Map(m3));
    }

    /// Cross-variant comparison: List vs Map must be false.
    #[test]
    fn value_list_vs_map_cross_variant_false() {
        let l = Value::List(vec![]);
        let m = Value::Map(BTreeMap::new());
        assert_ne!(l, m);
    }

    /// type_of returns None for List/Map (no FieldType representation; outputs only).
    #[test]
    fn value_list_map_type_of_is_none() {
        assert_eq!(Value::List(vec![]).type_of(), None);
        assert_eq!(Value::Map(BTreeMap::new()).type_of(), None);
    }
}
