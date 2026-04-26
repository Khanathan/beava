//! Where-predicate evaluator for aggregation apply path.
//!
//! # Requirements traceability
//! - SDK-AGG-04: optional `where=` predicate on every core aggregation
//!
//! D-03: three-valued null drop — `Bool(true)` → update; everything else
//! (`Bool(false)`, `Null`, type mismatch, missing field) → skip the update.
//!
//! Reuses Phase 4's `eval::eval` (bounded-depth evaluator with D-06 determinism
//! guarantees). No new expression machinery is introduced here.

use crate::eval::eval;
use crate::expr::Expr;
use crate::row::{Row, Value};

/// Evaluate the `where` predicate against `row`.
///
/// Returns `true` iff the predicate evaluates to `Value::Bool(true)`.
/// Any other outcome — `Bool(false)`, `Null`, type mismatch, missing field —
/// returns `false`. This is the three-valued null "drop" rule from D-03:
/// ambiguous or null predicates conservatively skip the aggregation update.
///
/// Delegates to `eval::eval` which enforces a bounded recursion depth (512),
/// preventing adversarial expressions from overflowing the call stack (T-05-02-01).
/// The `Null → false` mapping is explicitly tested as `where_null_returns_false`
/// to guard against future refactors silently inverting semantics (T-05-02-02).
///
/// # SDK-AGG-04
pub fn evaluate_where_predicate(expr: &Expr, row: &Row) -> bool {
    matches!(eval(expr, row), Value::Bool(true))
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::parse;
    use crate::row::{Row, Value};

    fn row_with_f64(field: &str, v: f64) -> Row {
        Row::new().with_field(field, Value::F64(v))
    }

    fn row_with_null(field: &str) -> Row {
        Row::new().with_field(field, Value::Null)
    }

    fn row_with_str(field: &str, s: &str) -> Row {
        Row::new().with_field(field, Value::Str(s.into()))
    }

    // ── T-05-02-02 guard: Null → false (three-valued null drop) ──────────────

    /// Bool(true) → predicate passes.
    #[test]
    fn where_bool_true_returns_true() {
        let expr = parse("(amount > 100)").expect("should parse");
        let row = row_with_f64("amount", 150.0);
        assert!(
            evaluate_where_predicate(&expr, &row),
            "amount=150 > 100 should return true"
        );
    }

    /// Bool(false) → predicate drops.
    #[test]
    fn where_bool_false_returns_false() {
        let expr = parse("(amount > 100)").expect("should parse");
        let row = row_with_f64("amount", 50.0);
        assert!(
            !evaluate_where_predicate(&expr, &row),
            "amount=50 > 100 should return false"
        );
    }

    /// Null result (null field) → three-valued null drop → false.
    /// T-05-02-02: this test is the guard that Null evaluates to FALSE (not TRUE).
    #[test]
    fn where_null_returns_false() {
        let expr = parse("(amount > 100)").expect("should parse");
        let row = row_with_null("amount");
        assert!(
            !evaluate_where_predicate(&expr, &row),
            "amount=Null > 100 should return false (three-valued null drop)"
        );
    }

    /// Type mismatch → eval returns Null → drop → false.
    #[test]
    fn where_type_mismatch_returns_false() {
        let expr = parse("(amount > 100)").expect("should parse");
        let row = row_with_str("amount", "oops");
        assert!(
            !evaluate_where_predicate(&expr, &row),
            "amount=Str(oops) > 100 should return false (type mismatch → Null → drop)"
        );
    }

    /// Missing field → eval returns Null → drop → false.
    #[test]
    fn where_missing_field_returns_false() {
        let expr = parse("(nonexistent > 0)").expect("should parse");
        let row = Row::new(); // empty row
        assert!(
            !evaluate_where_predicate(&expr, &row),
            "missing field should return false (field miss → Null → drop)"
        );
    }
}
