//! Conditional builtins.
//!
//! Per-category file under the PR 3 BUILTINS split (RFC-001 §5.2).
//! `isnull` stays in `super::mod` (polymorphic null check that predates the
//! split). `is_in` is deferred until the first variadic consumer ships.
//!
//! # Builtins in this file (PR 4)
//!
//! | Name       | Arity    | Eval signature                    | Null rule             | Infer                        |
//! |------------|----------|-----------------------------------|-----------------------|------------------------------|
//! | `if_else`  | Fixed(3) | `(Bool, T, T) → T`               | null cond → Null;     | `if_else_infer` (one-off)    |
//! |            |          |                                   | branch value nullable |                              |
//!
//! `if_else` is the only builtin with a short-circuit rule. `eval_depth` in
//! `eval.rs` special-cases it before the generic eager-Call arm: it evaluates
//! the condition first, then evaluates exactly one branch. This eval fn
//! receives already-evaluated values and just dispatches on the condition —
//! it is correct on its own, but the short-circuit in `eval_depth` is what
//! prevents the inactive branch from running at all.

use super::_inference::{infer_same_type, InferError};
use crate::row::Value;
use crate::schema::FieldType;
use crate::schema_propagate::InferredType;

// ─── if_else ─────────────────────────────────────────────────────────────────

/// Choose which `if_else` branch a condition selects — the single source of
/// branch-selection truth.
///
/// Returns the argument index to return, or `None` when the result is `Null`:
/// - `Bool(true)`  → `Some(1)` (then-branch)
/// - `Bool(false)` → `Some(2)` (else-branch)
/// - `Null` / any other type → `None` (unknown or non-Bool condition → `Null`;
///   register-time inference rejects non-Bool conditions, so the non-Bool case
///   is defensive)
///
/// Both the eager path ([`if_else_eval`]) and the short-circuit path
/// (`BuiltinFn::eval_lazy` in `mod.rs`) consult this, so the two cannot diverge.
pub(super) fn if_else_select_branch(cond: &Value) -> Option<usize> {
    match cond {
        Value::Bool(true) => Some(1),
        Value::Bool(false) => Some(2),
        _ => None,
    }
}

/// Eager reference evaluation of `if_else(cond, then_val, else_val)`.
///
/// Picks the branch chosen by [`if_else_select_branch`] from already-evaluated
/// argument values.
///
/// # Short-circuit (this fn is the eager fallback, not the live path)
/// `if_else` short-circuits in `eval.rs::eval_depth` via `BuiltinFn::eval_lazy`:
/// only the selected branch is ever evaluated. This fn receives values that
/// have *already* been evaluated, so once `eval_lazy` is wired it never runs in
/// production — it stays as the eager reference (and the `BuiltinFn::eval` arm
/// the closed enum requires). It shares `if_else_select_branch` with the lazy
/// path, so they cannot drift.
///
/// Eager and short-circuit are **observably identical** here, because beava
/// expressions are pure (no side effects) and eval is total (div-by-zero →
/// `Null`/`Inf`, overflow saturates, depth-capped, no loops). The short-circuit
/// is therefore a *performance* optimization (it avoids computing the unused
/// branch — matters for nested `if`/`elif` chains), not a correctness fix.
///
/// # Flip-trigger
/// The equivalence holds only while eval is pure + total. The day a partial or
/// effectful builtin lands (one that can trap, loop, or have a side effect),
/// eager evaluation of the untaken branch becomes a correctness bug and the
/// short-circuit becomes load-bearing.
///
/// # Arity
/// Fixed(3). Wrong arity → `Null` (defensive; register-time catches).
pub(super) fn if_else_eval(args: &[Value]) -> Value {
    if args.len() != 3 {
        return Value::Null;
    }
    if_else_select_branch(&args[0]).map_or(Value::Null, |i| args[i].clone())
}

/// Register-time inference for `if_else(cond, then_val, else_val)`.
///
/// - `cond` must be `Bool`. Non-Bool condition → `TypeMismatch` at arg 0.
/// - `then_val` and `else_val` must be the same type. Mixed types → `Unify`
///   error. `NullLiteral` in either branch is accepted as a hole (the concrete
///   type from the other branch wins). Both `NullLiteral` → falls back to
///   `Known(Str)` per the documented `infer_same_type` default.
pub(super) fn if_else_infer(arg_types: &[InferredType]) -> Result<InferredType, InferError> {
    if arg_types.len() != 3 {
        return Err(InferError::Arity {
            expected: 3,
            got: arg_types.len(),
        });
    }
    // cond must be Bool (NullLiteral accepted per wildcard rule).
    match &arg_types[0] {
        InferredType::NullLiteral | InferredType::Known(FieldType::Bool) => {}
        got => {
            return Err(InferError::TypeMismatch {
                arg_idx: 0,
                expected: "Bool",
                got: got.clone(),
            });
        }
    }
    // The two branches must unify to the same concrete type.
    infer_same_type(arg_types, &[1, 2])
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::Value;
    use crate::schema::FieldType;
    use crate::schema_propagate::InferredType;

    // ── if_else_eval ──────────────────────────────────────────────────────────

    /// if_else takes exactly three args. Any other count gives Null.
    #[test]
    fn if_else_arity_wrong_count_errors() {
        assert_eq!(if_else_eval(&[]), Value::Null);
        assert_eq!(if_else_eval(&[Value::Bool(true)]), Value::Null);
        assert_eq!(
            if_else_eval(&[Value::Bool(true), Value::I64(1)]),
            Value::Null
        );
        // 4 args
        assert_eq!(
            if_else_eval(&[
                Value::Bool(true),
                Value::I64(1),
                Value::I64(2),
                Value::I64(3),
            ]),
            Value::Null
        );
    }

    /// Four cases covering the three condition states and a type-mismatch
    /// on the condition. Pins which branch is returned in each case.
    #[test]
    fn if_else_eval_truth_table() {
        // true → then-branch
        assert_eq!(
            if_else_eval(&[Value::Bool(true), Value::I64(1), Value::I64(2)]),
            Value::I64(1)
        );
        // false → else-branch
        assert_eq!(
            if_else_eval(&[Value::Bool(false), Value::I64(1), Value::I64(2)]),
            Value::I64(2)
        );
        // works with non-numeric branch values
        assert_eq!(
            if_else_eval(&[
                Value::Bool(true),
                Value::Str("yes".into()),
                Value::Str("no".into()),
            ]),
            Value::Str("yes".into())
        );
        // non-Bool condition → Null (defensive; register-time rejects this)
        assert_eq!(
            if_else_eval(&[Value::I64(1), Value::I64(10), Value::I64(20)]),
            Value::Null
        );
    }

    /// Null condition → Null. The selected branch carrying a Null value also
    /// produces Null — that's the branch value, not spurious propagation.
    #[test]
    fn if_else_null_rule_matches_doc() {
        // Null condition → Null regardless of branches
        assert_eq!(
            if_else_eval(&[Value::Null, Value::I64(1), Value::I64(2)]),
            Value::Null
        );
        // The selected branch's value can itself be Null — that's returned as-is
        assert_eq!(
            if_else_eval(&[Value::Bool(true), Value::Null, Value::I64(2)]),
            Value::Null
        );
        assert_eq!(
            if_else_eval(&[Value::Bool(false), Value::I64(1), Value::Null]),
            Value::Null
        );
        // A real result when cond is concrete and branches are non-null
        assert_ne!(
            if_else_eval(&[Value::Bool(true), Value::I64(42), Value::I64(0)]),
            Value::Null
        );
    }

    // ── if_else_infer ─────────────────────────────────────────────────────────

    /// if_else_infer rejects wrong arity at register time.
    #[test]
    fn if_else_infer_wrong_arity_errors() {
        // Too few
        assert!(matches!(
            if_else_infer(&[]),
            Err(InferError::Arity {
                expected: 3,
                got: 0
            })
        ));
        assert!(matches!(
            if_else_infer(&[InferredType::Known(FieldType::Bool)]),
            Err(InferError::Arity {
                expected: 3,
                got: 1
            })
        ));
        // Too many
        assert!(matches!(
            if_else_infer(&[
                InferredType::Known(FieldType::Bool),
                InferredType::Known(FieldType::I64),
                InferredType::Known(FieldType::I64),
                InferredType::Known(FieldType::I64),
            ]),
            Err(InferError::Arity {
                expected: 3,
                got: 4
            })
        ));
    }

    /// Valid inputs: Bool cond + matching branch types. The output type
    /// is the unified branch type, not the cond type.
    #[test]
    fn if_else_infer_typecheck() {
        // (Bool, I64, I64) → I64
        let args_i64 = [
            InferredType::Known(FieldType::Bool),
            InferredType::Known(FieldType::I64),
            InferredType::Known(FieldType::I64),
        ];
        assert_eq!(
            if_else_infer(&args_i64),
            Ok(InferredType::Known(FieldType::I64))
        );
        // (Bool, F64, F64) → F64 (output tracks branch type, not fixed to I64)
        let args_f64 = [
            InferredType::Known(FieldType::Bool),
            InferredType::Known(FieldType::F64),
            InferredType::Known(FieldType::F64),
        ];
        assert_eq!(
            if_else_infer(&args_f64),
            Ok(InferredType::Known(FieldType::F64))
        );
        // NullLiteral in one branch is a hole — the concrete branch wins
        let args_null_then = [
            InferredType::Known(FieldType::Bool),
            InferredType::NullLiteral,
            InferredType::Known(FieldType::I64),
        ];
        assert_eq!(
            if_else_infer(&args_null_then),
            Ok(InferredType::Known(FieldType::I64))
        );
        let args_null_else = [
            InferredType::Known(FieldType::Bool),
            InferredType::Known(FieldType::F64),
            InferredType::NullLiteral,
        ];
        assert_eq!(
            if_else_infer(&args_null_else),
            Ok(InferredType::Known(FieldType::F64))
        );
        // Non-Bool cond → TypeMismatch at arg 0
        let args_bad_cond = [
            InferredType::Known(FieldType::I64),
            InferredType::Known(FieldType::I64),
            InferredType::Known(FieldType::I64),
        ];
        assert!(matches!(
            if_else_infer(&args_bad_cond),
            Err(InferError::TypeMismatch { arg_idx: 0, .. })
        ));
        let args_str_cond = [
            InferredType::Known(FieldType::Str),
            InferredType::Known(FieldType::I64),
            InferredType::Known(FieldType::I64),
        ];
        assert!(matches!(
            if_else_infer(&args_str_cond),
            Err(InferError::TypeMismatch { arg_idx: 0, .. })
        ));
        // Mixed branch types → Unify error
        // if_else(c, 1, 2.0) makes no sense — can't return either I64 or F64
        let args_mixed = [
            InferredType::Known(FieldType::Bool),
            InferredType::Known(FieldType::I64),
            InferredType::Known(FieldType::F64),
        ];
        assert!(matches!(
            if_else_infer(&args_mixed),
            Err(InferError::Unify { .. })
        ));
    }
}
