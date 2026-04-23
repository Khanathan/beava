//! Register-time schema propagation for stateless op chains.
//!
//! Walks `(input_schema, &[OpNode])` and derives the output schema after each
//! op, returning:
//!   - The final output schema.
//!   - A per-step `Vec<Schema>` snapshot (schema after op `i`).
//!   - Accumulated `Vec<PropagationError>` (fail-soft: collects all errors).
//!
//! SDK-COL-07 field-reference resolution lives here: `Filter`, `WithColumns`,
//! and `Map` ops call `referenced_fields()` on each expression and verify every
//! field is present in the current per-step schema.  Unknown field → `FieldMissing`.
//!
//! This module intentionally has no `serde` or I/O dependencies — it is a pure
//! in-process Rust library.

// RED commit: implementation stubs — dead_code/unused_imports expected until green.
#![allow(dead_code, unused_imports)]

use std::collections::BTreeMap;

use crate::expr::{self, ParseError};
use crate::op_node::OpNode;
use crate::schema::{DerivedSchema, EventSchema, FieldType, TableSchema};

// ─── Neutral Schema ───────────────────────────────────────────────────────────

/// Transport schema used inside the propagator.
///
/// Structurally identical to `EventSchema` / `TableSchema` / `DerivedSchema`
/// but type-erased so the propagator does not need to know which descriptor
/// kind it is operating on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub fields: BTreeMap<String, FieldType>,
    pub optional_fields: Vec<String>,
}

impl Schema {
    pub fn new() -> Self {
        Schema {
            fields: BTreeMap::new(),
            optional_fields: Vec::new(),
        }
    }

    pub fn from_event(s: &EventSchema) -> Self {
        Schema {
            fields: s.fields.clone(),
            optional_fields: s.optional_fields.clone(),
        }
    }

    pub fn from_table(s: &TableSchema) -> Self {
        Schema {
            fields: s.fields.clone(),
            optional_fields: s.optional_fields.clone(),
        }
    }

    pub fn from_derived(s: &DerivedSchema) -> Self {
        Schema {
            fields: s.fields.clone(),
            optional_fields: s.optional_fields.clone(),
        }
    }

    pub fn into_derived(self) -> DerivedSchema {
        DerivedSchema {
            fields: self.fields,
            optional_fields: self.optional_fields,
        }
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self::new()
    }
}

// ─── InferredType ────────────────────────────────────────────────────────────

/// Wraps a `FieldType` with a special `NullLiteral` variant for polymorphic
/// null-literal type checking.
///
/// `Null` literals are compatible with any type during binary-op type
/// inference — they merely propagate to the other operand's type (or stay
/// ambiguous if both operands are null).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InferredType {
    Known(FieldType),
    NullLiteral,
}

// ─── PropagationError ────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum PropagationError {
    /// An expression references a field not present in the schema at that step.
    FieldMissing { op_index: usize, field: String },
    /// An expression has a type incompatibility (e.g., bool op on non-bool).
    TypeMismatch { op_index: usize, reason: String },
    /// A Rename op would produce a collision in the output schema.
    RenameCollision { op_index: usize, new: String },
    /// An expression string failed to parse.
    InvalidExpr {
        op_index: usize,
        parse_error: ParseError,
    },
    /// An op is not supported in Phase 4 (GroupBy / Join / Union).
    UnsupportedOp { op_index: usize, op: &'static str },
}

// ─── Entry point ─────────────────────────────────────────────────────────────

/// Propagate `input` schema through `ops`, returning `(final_schema, per_step_schemas)`.
///
/// `per_step_schemas[i]` is the schema **after** applying `ops[i]`.
///
/// Fail-soft: all errors are collected; on error the propagator applies a
/// best-effort carry-forward so subsequent ops still get a schema to type-check
/// against.
///
/// Returns `Err(errors)` if any errors were found; `Ok(...)` if clean.
pub fn propagate_schema(
    _input: &Schema,
    _ops: &[OpNode],
) -> Result<(Schema, Vec<Schema>), Vec<PropagationError>> {
    todo!("propagate_schema not yet implemented")
}

// ─── Per-op schema logic ─────────────────────────────────────────────────────

fn apply_filter_schema(
    _op_index: usize,
    _expr_src: &str,
    _schema: &Schema,
    _errors: &mut Vec<PropagationError>,
) {
    todo!("apply_filter_schema not yet implemented")
}

fn apply_select_schema(
    _op_index: usize,
    _fields: &[String],
    _current: &mut Schema,
    _errors: &mut Vec<PropagationError>,
) {
    todo!("apply_select_schema not yet implemented")
}

fn apply_drop_schema(
    _op_index: usize,
    _fields: &[String],
    _current: &mut Schema,
    _errors: &mut Vec<PropagationError>,
) {
    todo!("apply_drop_schema not yet implemented")
}

fn apply_rename_schema(
    _op_index: usize,
    _mapping: &BTreeMap<String, String>,
    _current: &mut Schema,
    _errors: &mut Vec<PropagationError>,
) {
    todo!("apply_rename_schema not yet implemented")
}

fn apply_with_columns_schema(
    _op_index: usize,
    _exprs: &BTreeMap<String, String>,
    _current: &mut Schema,
    _errors: &mut Vec<PropagationError>,
) {
    todo!("apply_with_columns_schema not yet implemented")
}

fn apply_cast_schema(
    _op_index: usize,
    _type_map: &BTreeMap<String, String>,
    _current: &mut Schema,
    _errors: &mut Vec<PropagationError>,
) {
    todo!("apply_cast_schema not yet implemented")
}

fn apply_fillna_schema(
    _op_index: usize,
    _defaults: &BTreeMap<String, serde_json::Value>,
    _current: &mut Schema,
    _errors: &mut Vec<PropagationError>,
) {
    todo!("apply_fillna_schema not yet implemented")
}

// ─── Type inference helpers ───────────────────────────────────────────────────

/// Public entry point for expression type inference (used in tests).
///
/// Returns `Err(PropagationError)` on the first type error found.
pub fn infer_expr_type(
    _expr: &crate::expr::Expr,
    _schema: &Schema,
) -> Result<InferredType, PropagationError> {
    todo!("infer_expr_type not yet implemented")
}

/// Internal recursive type inference.
///
/// Returns `None` when an error has been pushed (callers treat None as "errored").
fn infer_expr_type_inner(
    _op_index: usize,
    _expr: &crate::expr::Expr,
    _schema: &Schema,
    _errors: &mut Vec<PropagationError>,
) -> Option<InferredType> {
    todo!("infer_expr_type_inner not yet implemented")
}

fn infer_binop_type(
    _op_index: usize,
    _op: &str,
    _left: &crate::expr::Expr,
    _right: &crate::expr::Expr,
    _schema: &Schema,
    _errors: &mut Vec<PropagationError>,
) -> Option<InferredType> {
    todo!("infer_binop_type not yet implemented")
}

fn infer_arithmetic_type(
    _op_index: usize,
    _op: &str,
    _lt: &InferredType,
    _rt: &InferredType,
    _errors: &mut Vec<PropagationError>,
) -> Option<InferredType> {
    todo!("infer_arithmetic_type not yet implemented")
}

fn resolve_null_arithmetic(_op: &str, _other: &InferredType) -> Option<InferredType> {
    todo!("resolve_null_arithmetic not yet implemented")
}

fn infer_call_type(
    _op_index: usize,
    _fn_name: &str,
    _args: &[crate::expr::Expr],
    _schema: &Schema,
    _errors: &mut Vec<PropagationError>,
) -> Option<InferredType> {
    todo!("infer_call_type not yet implemented")
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Parse a cast target string into a `FieldType`.
pub fn parse_cast_target(s: &str) -> Option<FieldType> {
    match s {
        "str" => Some(FieldType::Str),
        "int" => Some(FieldType::I64),
        "float" => Some(FieldType::F64),
        "bool" => Some(FieldType::Bool),
        _ => None,
    }
}

/// Legal cast matrix. See CONTEXT.md §D-05.
///
/// | Source    | str | int | float | bool |
/// |-----------|-----|-----|-------|------|
/// | Str       | ✓   | ✓   | ✓     | ✓    |
/// | I64       | ✓   | ✓   | ✓     | ✓    |
/// | F64       | ✓   | ✓   | ✓     | ✓    |
/// | Bool      | ✓   | ✓   | ✓     | ✓    |
/// | Datetime  | ✓   | ✓   | ✓     | ✓    |
/// | Bytes     | ✗   | ✗   | ✗     | ✗    |
pub fn is_cast_legal(source: FieldType, target: FieldType) -> bool {
    // Bytes is isolated — no casts in or out.
    if source == FieldType::Bytes {
        return false;
    }
    // All other source types can be cast to str/int/float/bool
    // (may fail at runtime for Str→int/float, but legal at register time).
    matches!(
        target,
        FieldType::Str | FieldType::I64 | FieldType::F64 | FieldType::Bool
    )
}

fn is_numeric(ft: &FieldType) -> bool {
    matches!(ft, FieldType::I64 | FieldType::F64)
}

fn is_bool_compatible(it: &InferredType) -> bool {
    matches!(
        it,
        InferredType::Known(FieldType::Bool) | InferredType::NullLiteral
    )
}

fn types_are_comparable(lt: &InferredType, rt: &InferredType) -> bool {
    match (lt, rt) {
        // Null is compatible with anything.
        (InferredType::NullLiteral, _) | (_, InferredType::NullLiteral) => true,
        (InferredType::Known(l), InferredType::Known(r)) => {
            // Same type is always comparable.
            if l == r {
                return true;
            }
            // Numeric cross-type (I64 ↔ F64) is comparable.
            is_numeric_ft(*l) && is_numeric_ft(*r)
        }
    }
}

fn is_numeric_ft(ft: FieldType) -> bool {
    matches!(ft, FieldType::I64 | FieldType::F64)
}

fn check_referenced_fields(
    op_index: usize,
    ast: &crate::expr::Expr,
    schema: &Schema,
    errors: &mut Vec<PropagationError>,
) {
    for field in ast.referenced_fields() {
        if !schema.fields.contains_key(field.as_str()) {
            errors.push(PropagationError::FieldMissing { op_index, field });
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::op_node::OpNode;
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
            optional_fields: Vec::new(),
        }
    }

    fn schema_with_opt(pairs: &[(&str, FieldType)], opt: &[&str]) -> Schema {
        let mut s = schema_with(pairs);
        s.optional_fields = opt.iter().map(|f| f.to_string()).collect();
        s
    }

    fn assert_no_errors<T>(r: Result<T, Vec<PropagationError>>) -> T {
        match r {
            Ok(v) => v,
            Err(errs) => panic!("expected Ok, got errors: {errs:?}"),
        }
    }

    fn assert_errors(
        r: Result<(Schema, Vec<Schema>), Vec<PropagationError>>,
    ) -> Vec<PropagationError> {
        match r {
            Err(errs) => errs,
            Ok(_) => panic!("expected Err, got Ok"),
        }
    }

    // ── Test 1: Filter preserves schema ──────────────────────────────────────

    #[test]
    fn prop_filter_preserves_schema() {
        let input = schema_with(&[("amount", FieldType::F64), ("id", FieldType::Str)]);
        let ops = vec![OpNode::Filter {
            expr: "(amount > 0)".to_string(),
        }];
        let (final_schema, per_step) = assert_no_errors(propagate_schema(&input, &ops));
        assert_eq!(
            final_schema.fields, input.fields,
            "filter must not change schema fields"
        );
        assert_eq!(per_step.len(), 1);
        assert_eq!(per_step[0].fields, input.fields);
    }

    // ── Test 2: Filter with unknown field errors ──────────────────────────────

    #[test]
    fn prop_filter_expr_unknown_field_errors() {
        let input = schema_with(&[("amount", FieldType::F64)]);
        let ops = vec![OpNode::Filter {
            expr: "(missing > 0)".to_string(),
        }];
        let errs = assert_errors(propagate_schema(&input, &ops));
        assert!(
            errs.iter().any(|e| matches!(
                e,
                PropagationError::FieldMissing { op_index: 0, field }
                if field == "missing"
            )),
            "expected FieldMissing for 'missing', got {errs:?}"
        );
    }

    // ── Test 3: Filter with parse error ──────────────────────────────────────

    #[test]
    fn prop_filter_expr_parse_error_errors() {
        let input = schema_with(&[("amount", FieldType::F64)]);
        let ops = vec![OpNode::Filter {
            expr: "(amount > ".to_string(),
        }];
        let errs = assert_errors(propagate_schema(&input, &ops));
        assert!(
            errs.iter()
                .any(|e| matches!(e, PropagationError::InvalidExpr { op_index: 0, .. })),
            "expected InvalidExpr at op_index=0, got {errs:?}"
        );
    }

    // ── Test 4: Select keeps listed fields ────────────────────────────────────

    #[test]
    fn prop_select_keeps_listed_fields_in_order() {
        let input = schema_with(&[
            ("a", FieldType::Str),
            ("b", FieldType::I64),
            ("c", FieldType::F64),
        ]);
        let ops = vec![OpNode::Select {
            fields: vec!["b".to_string(), "a".to_string()],
        }];
        let (final_schema, _) = assert_no_errors(propagate_schema(&input, &ops));
        // BTreeMap sorts alphabetically; assert key set equality.
        assert!(final_schema.fields.contains_key("a"));
        assert!(final_schema.fields.contains_key("b"));
        assert!(
            !final_schema.fields.contains_key("c"),
            "c should be dropped"
        );
        assert_eq!(final_schema.fields.len(), 2);
    }

    // ── Test 5: Select unknown field errors ───────────────────────────────────

    #[test]
    fn prop_select_unknown_field_errors() {
        let input = schema_with(&[("a", FieldType::Str)]);
        let ops = vec![OpNode::Select {
            fields: vec!["missing".to_string()],
        }];
        let errs = assert_errors(propagate_schema(&input, &ops));
        assert!(
            errs.iter().any(|e| matches!(
                e,
                PropagationError::FieldMissing { op_index: 0, field }
                if field == "missing"
            )),
            "expected FieldMissing for 'missing', got {errs:?}"
        );
    }

    // ── Test 6: Drop removes fields ───────────────────────────────────────────

    #[test]
    fn prop_drop_removes_fields() {
        let input = schema_with(&[
            ("a", FieldType::Str),
            ("b", FieldType::I64),
            ("c", FieldType::F64),
        ]);
        let ops = vec![OpNode::Drop {
            fields: vec!["b".to_string()],
        }];
        let (final_schema, _) = assert_no_errors(propagate_schema(&input, &ops));
        assert!(!final_schema.fields.contains_key("b"));
        assert!(final_schema.fields.contains_key("a"));
        assert!(final_schema.fields.contains_key("c"));
    }

    // ── Test 7: Drop unknown field errors ─────────────────────────────────────

    #[test]
    fn prop_drop_unknown_field_errors() {
        let input = schema_with(&[("a", FieldType::Str)]);
        let ops = vec![OpNode::Drop {
            fields: vec!["missing".to_string()],
        }];
        let errs = assert_errors(propagate_schema(&input, &ops));
        assert!(
            errs.iter().any(|e| matches!(
                e,
                PropagationError::FieldMissing { op_index: 0, field }
                if field == "missing"
            )),
            "expected FieldMissing for 'missing', got {errs:?}"
        );
    }

    // ── Test 8: Rename swaps keys ──────────────────────────────────────────────

    #[test]
    fn prop_rename_swaps_keys() {
        let input = schema_with_opt(&[("a", FieldType::Str), ("b", FieldType::I64)], &["a"]);
        let mut mapping = BTreeMap::new();
        mapping.insert("a".to_string(), "x".to_string());
        let ops = vec![OpNode::Rename { mapping }];
        let (final_schema, _) = assert_no_errors(propagate_schema(&input, &ops));
        assert!(
            final_schema.fields.contains_key("x"),
            "x should exist after rename"
        );
        assert!(
            !final_schema.fields.contains_key("a"),
            "a should be gone after rename"
        );
        assert!(
            final_schema.fields.contains_key("b"),
            "b should be unchanged"
        );
        // optional_fields should track the rename
        assert!(
            final_schema.optional_fields.contains(&"x".to_string()),
            "optional status of 'a' should transfer to 'x'"
        );
    }

    // ── Test 9: Rename collision errors ───────────────────────────────────────

    #[test]
    fn prop_rename_collision_errors() {
        let input = schema_with(&[("a", FieldType::Str), ("b", FieldType::I64)]);
        let mut mapping = BTreeMap::new();
        mapping.insert("a".to_string(), "b".to_string());
        let ops = vec![OpNode::Rename { mapping }];
        let errs = assert_errors(propagate_schema(&input, &ops));
        assert!(
            errs.iter().any(|e| matches!(
                e,
                PropagationError::RenameCollision { op_index: 0, new }
                if new == "b"
            )),
            "expected RenameCollision for 'b', got {errs:?}"
        );
    }

    // ── Test 10: Rename unknown field errors ──────────────────────────────────

    #[test]
    fn prop_rename_unknown_field_errors() {
        let input = schema_with(&[("a", FieldType::Str)]);
        let mut mapping = BTreeMap::new();
        mapping.insert("missing".to_string(), "x".to_string());
        let ops = vec![OpNode::Rename { mapping }];
        let errs = assert_errors(propagate_schema(&input, &ops));
        assert!(
            errs.iter().any(|e| matches!(
                e,
                PropagationError::FieldMissing { op_index: 0, field }
                if field == "missing"
            )),
            "expected FieldMissing for 'missing', got {errs:?}"
        );
    }

    // ── Test 11: WithColumns adds derived field ────────────────────────────────

    #[test]
    fn prop_with_columns_adds_derived_field() {
        let input = schema_with(&[("amount", FieldType::F64)]);
        let mut exprs = BTreeMap::new();
        exprs.insert("is_big".to_string(), "(amount > 500)".to_string());
        let ops = vec![OpNode::WithColumns { exprs }];
        let (final_schema, _) = assert_no_errors(propagate_schema(&input, &ops));
        assert_eq!(
            final_schema.fields.get("is_big"),
            Some(&FieldType::Bool),
            "is_big should be Bool (comparison op)"
        );
        assert!(
            final_schema.fields.contains_key("amount"),
            "amount should remain"
        );
    }

    // ── Test 12: WithColumns overwrites existing field ────────────────────────

    #[test]
    fn prop_with_columns_overwrites_existing() {
        let input = schema_with(&[("amount", FieldType::F64)]);
        let mut exprs = BTreeMap::new();
        exprs.insert("amount".to_string(), "cast(amount, int)".to_string());
        let ops = vec![OpNode::WithColumns { exprs }];
        let (final_schema, _) = assert_no_errors(propagate_schema(&input, &ops));
        assert_eq!(
            final_schema.fields.get("amount"),
            Some(&FieldType::I64),
            "amount should be I64 after cast-overwrite"
        );
    }

    // ── Test 13: WithColumns expr type mismatch errors ────────────────────────

    #[test]
    fn prop_with_columns_expr_type_mismatch_errors() {
        let input = schema_with(&[("amount", FieldType::F64)]);
        let mut exprs = BTreeMap::new();
        // "amount and true" — boolean op on non-bool (amount is F64)
        exprs.insert("bad".to_string(), "(amount and true)".to_string());
        let ops = vec![OpNode::WithColumns { exprs }];
        let errs = assert_errors(propagate_schema(&input, &ops));
        assert!(
            errs.iter()
                .any(|e| matches!(e, PropagationError::TypeMismatch { op_index: 0, .. })),
            "expected TypeMismatch at op_index=0, got {errs:?}"
        );
    }

    // ── Test 14: Map is alias of WithColumns ──────────────────────────────────

    #[test]
    fn prop_map_is_alias_of_with_columns() {
        let input = schema_with(&[("a", FieldType::F64)]);

        let mut exprs_wc = BTreeMap::new();
        exprs_wc.insert("b".to_string(), "(a + 1)".to_string());
        let ops_wc = vec![OpNode::WithColumns { exprs: exprs_wc }];

        let mut exprs_map = BTreeMap::new();
        exprs_map.insert("b".to_string(), "(a + 1)".to_string());
        let ops_map = vec![OpNode::Map { exprs: exprs_map }];

        let (s_wc, _) = assert_no_errors(propagate_schema(&input, &ops_wc));
        let (s_map, _) = assert_no_errors(propagate_schema(&input, &ops_map));

        assert_eq!(
            s_wc.fields.get("b"),
            s_map.fields.get("b"),
            "Map and WithColumns must produce the same schema for identical exprs"
        );
    }

    // ── Test 15: Cast replaces field type ─────────────────────────────────────

    #[test]
    fn prop_cast_replaces_field_type() {
        let input = schema_with(&[("amount", FieldType::Str)]);
        let mut type_map = BTreeMap::new();
        type_map.insert("amount".to_string(), "float".to_string());
        let ops = vec![OpNode::Cast { type_map }];
        let (final_schema, _) = assert_no_errors(propagate_schema(&input, &ops));
        assert_eq!(
            final_schema.fields.get("amount"),
            Some(&FieldType::F64),
            "amount should be F64 after cast to float"
        );
    }

    // ── Test 16: Cast unknown target errors ───────────────────────────────────

    #[test]
    fn prop_cast_unknown_target_errors() {
        let input = schema_with(&[("amount", FieldType::F64)]);
        let mut type_map = BTreeMap::new();
        type_map.insert("amount".to_string(), "decimal".to_string());
        let ops = vec![OpNode::Cast { type_map }];
        let errs = assert_errors(propagate_schema(&input, &ops));
        assert!(
            errs.iter()
                .any(|e| matches!(e, PropagationError::TypeMismatch { op_index: 0, .. })),
            "expected TypeMismatch for unknown cast target, got {errs:?}"
        );
    }

    // ── Test 17: Cast unknown field errors ────────────────────────────────────

    #[test]
    fn prop_cast_unknown_field_errors() {
        let input = schema_with(&[("amount", FieldType::F64)]);
        let mut type_map = BTreeMap::new();
        type_map.insert("missing".to_string(), "int".to_string());
        let ops = vec![OpNode::Cast { type_map }];
        let errs = assert_errors(propagate_schema(&input, &ops));
        assert!(
            errs.iter().any(|e| matches!(
                e,
                PropagationError::FieldMissing { op_index: 0, field }
                if field == "missing"
            )),
            "expected FieldMissing for 'missing', got {errs:?}"
        );
    }

    // ── Test 18: Fillna clears optional ───────────────────────────────────────

    #[test]
    fn prop_fillna_keeps_field_clears_optional() {
        let input = schema_with_opt(&[("amount", FieldType::F64)], &["amount"]);
        let mut defaults = BTreeMap::new();
        defaults.insert("amount".to_string(), serde_json::json!(0.0));
        let ops = vec![OpNode::Fillna { defaults }];
        let (final_schema, _) = assert_no_errors(propagate_schema(&input, &ops));
        assert_eq!(final_schema.fields.get("amount"), Some(&FieldType::F64));
        assert!(
            !final_schema.optional_fields.contains(&"amount".to_string()),
            "amount should no longer be optional after fillna"
        );
    }

    // ── Test 19: Fillna unknown field errors ──────────────────────────────────

    #[test]
    fn prop_fillna_unknown_field_errors() {
        let input = schema_with(&[("amount", FieldType::F64)]);
        let mut defaults = BTreeMap::new();
        defaults.insert("missing".to_string(), serde_json::json!(0));
        let ops = vec![OpNode::Fillna { defaults }];
        let errs = assert_errors(propagate_schema(&input, &ops));
        assert!(
            errs.iter().any(|e| matches!(
                e,
                PropagationError::FieldMissing { op_index: 0, field }
                if field == "missing"
            )),
            "expected FieldMissing for 'missing', got {errs:?}"
        );
    }

    // ── Test 20: Chained ops propagate ────────────────────────────────────────

    #[test]
    fn prop_chained_ops_propagate() {
        let input = schema_with(&[("amount", FieldType::F64)]);
        let mut with_exprs = BTreeMap::new();
        with_exprs.insert("is_big".to_string(), "(amount > 500)".to_string());
        let mut cast_map = BTreeMap::new();
        cast_map.insert("is_big".to_string(), "int".to_string());

        let ops = vec![
            OpNode::Filter {
                expr: "(amount > 0)".to_string(),
            },
            OpNode::WithColumns { exprs: with_exprs },
            OpNode::Cast { type_map: cast_map },
        ];

        let (final_schema, per_step) = assert_no_errors(propagate_schema(&input, &ops));

        assert_eq!(per_step.len(), 3, "must have 3 per-step schemas");
        // After op[2] (Cast): is_big should be I64
        assert_eq!(
            final_schema.fields.get("is_big"),
            Some(&FieldType::I64),
            "is_big should be I64 after cast"
        );
        assert_eq!(
            per_step[2].fields.get("is_big"),
            Some(&FieldType::I64),
            "per_step[2]['is_big'] should be I64"
        );
        assert_eq!(final_schema.fields.get("amount"), Some(&FieldType::F64));
    }

    // ── Test 21: Passthrough ops unsupported ──────────────────────────────────

    #[test]
    fn prop_passthrough_ops_unsupported() {
        let input = schema_with(&[("amount", FieldType::F64)]);

        // GroupBy
        let ops_gb = vec![OpNode::GroupBy {
            keys: vec!["amount".to_string()],
            agg: BTreeMap::new(),
        }];
        let errs = assert_errors(propagate_schema(&input, &ops_gb));
        assert!(
            errs.iter().any(|e| matches!(
                e,
                PropagationError::UnsupportedOp {
                    op_index: 0,
                    op: "group_by"
                }
            )),
            "expected UnsupportedOp(group_by), got {errs:?}"
        );

        // Join
        let ops_join = vec![OpNode::Join {
            other: "other_stream".to_string(),
            on: vec!["amount".to_string()],
            within_ms: None,
            join_type: crate::op_node::JoinType::Inner,
        }];
        let errs2 = assert_errors(propagate_schema(&input, &ops_join));
        assert!(
            errs2.iter().any(|e| matches!(
                e,
                PropagationError::UnsupportedOp {
                    op_index: 0,
                    op: "join"
                }
            )),
            "expected UnsupportedOp(join), got {errs2:?}"
        );

        // Union
        let ops_union = vec![OpNode::Union {
            others: vec!["other_stream".to_string()],
        }];
        let errs3 = assert_errors(propagate_schema(&input, &ops_union));
        assert!(
            errs3.iter().any(|e| matches!(
                e,
                PropagationError::UnsupportedOp {
                    op_index: 0,
                    op: "union"
                }
            )),
            "expected UnsupportedOp(union), got {errs3:?}"
        );
    }

    // ── Test 22: Fail-soft collects multiple errors ───────────────────────────

    #[test]
    fn prop_fail_soft_collects_multiple_errors() {
        let input = schema_with(&[("amount", FieldType::F64)]);
        let ops = vec![
            OpNode::Select {
                fields: vec!["missing1".to_string()],
            },
            OpNode::Drop {
                fields: vec!["missing2".to_string()],
            },
        ];
        let errs = assert_errors(propagate_schema(&input, &ops));
        assert!(
            errs.len() >= 2,
            "expected at least 2 errors (one per op), got {errs:?}"
        );
        // First error is op_index=0.
        assert!(
            errs.iter()
                .any(|e| matches!(e, PropagationError::FieldMissing { op_index: 0, .. })),
            "expected op_index=0 error, got {errs:?}"
        );
        // Second error is op_index=1.
        assert!(
            errs.iter()
                .any(|e| matches!(e, PropagationError::FieldMissing { op_index: 1, .. })),
            "expected op_index=1 error, got {errs:?}"
        );
    }

    // ── Tests 23–28: infer_expr_type helpers ──────────────────────────────────

    fn parse_and_infer(src: &str, schema: &Schema) -> Result<InferredType, PropagationError> {
        let ast = crate::expr::parse(src).expect("should parse in test");
        infer_expr_type(&ast, schema)
    }

    // ── Test 23: Arithmetic promotion ─────────────────────────────────────────

    #[test]
    fn infer_expr_type_arithmetic_promotion() {
        let schema = schema_with(&[("a", FieldType::I64), ("b", FieldType::F64)]);

        // I64 + I64 → I64
        let s = schema_with(&[("x", FieldType::I64), ("y", FieldType::I64)]);
        let r = parse_and_infer("(x + y)", &s).expect("should not error");
        assert_eq!(
            r,
            InferredType::Known(FieldType::I64),
            "I64+I64 should be I64"
        );

        // I64 + F64 → F64
        let r2 = parse_and_infer("(a + b)", &schema).expect("should not error");
        assert_eq!(
            r2,
            InferredType::Known(FieldType::F64),
            "I64+F64 should be F64"
        );

        // I64 / I64 → F64 (division widens)
        let s2 = schema_with(&[("x", FieldType::I64), ("y", FieldType::I64)]);
        let r3 = parse_and_infer("(x / y)", &s2).expect("should not error");
        assert_eq!(
            r3,
            InferredType::Known(FieldType::F64),
            "I64/I64 should widen to F64"
        );
    }

    // ── Test 24: Comparison is Bool ───────────────────────────────────────────

    #[test]
    fn infer_expr_type_comparison_is_bool() {
        let s = schema_with(&[("x", FieldType::I64), ("y", FieldType::F64)]);
        let r = parse_and_infer("(x > y)", &s).expect("should not error");
        assert_eq!(r, InferredType::Known(FieldType::Bool));

        let r2 = parse_and_infer("(x == y)", &s).expect("should not error");
        assert_eq!(r2, InferredType::Known(FieldType::Bool));
    }

    // ── Test 25: Boolean requires Bool operands ───────────────────────────────

    #[test]
    fn infer_expr_type_boolean_requires_bool() {
        let s = schema_with(&[("flag", FieldType::Bool), ("amount", FieldType::I64)]);

        // Bool and Bool → OK
        let r = parse_and_infer("(flag and true)", &s).expect("should not error");
        assert_eq!(r, InferredType::Known(FieldType::Bool));

        // I64 and Bool → TypeMismatch
        let r2 = parse_and_infer("(amount and flag)", &s);
        assert!(
            r2.is_err(),
            "expected TypeMismatch for I64 and Bool, got {r2:?}"
        );
    }

    // ── Test 26: isnull is Bool ───────────────────────────────────────────────

    #[test]
    fn infer_expr_type_isnull_is_bool() {
        let s = schema_with(&[("amount", FieldType::I64)]);
        let r = parse_and_infer("isnull(amount)", &s).expect("should not error");
        assert_eq!(r, InferredType::Known(FieldType::Bool));
    }

    // ── Test 27: cast returns target type ────────────────────────────────────

    #[test]
    fn infer_expr_type_cast_returns_target() {
        let s = schema_with(&[("amount", FieldType::I64)]);
        let r = parse_and_infer("cast(amount, float)", &s).expect("should not error");
        assert_eq!(r, InferredType::Known(FieldType::F64));

        // Unknown target type → error
        let r2 = parse_and_infer("cast(amount, 'blob')", &s);
        assert!(
            r2.is_err(),
            "expected TypeMismatch for unknown cast target 'blob', got {r2:?}"
        );
    }

    // ── Test 28: unknown function errors ──────────────────────────────────────

    #[test]
    fn infer_expr_type_unknown_function_errors() {
        let s = schema_with(&[("amount", FieldType::I64)]);
        let r = parse_and_infer("foo(amount)", &s);
        assert!(
            r.is_err(),
            "expected TypeMismatch for unknown function 'foo', got {r:?}"
        );
        match r {
            Err(PropagationError::TypeMismatch { reason, .. }) => {
                assert!(
                    reason.contains("foo"),
                    "error reason should mention 'foo', got: {reason}"
                );
            }
            other => panic!("expected TypeMismatch, got {other:?}"),
        }
    }
}
