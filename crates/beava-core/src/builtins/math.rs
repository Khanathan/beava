//! Math builtins.
//!
//! Per-category file under the PR 3 BUILTINS split (RFC-001 §5.2). Holds
//! `*_eval` free fns and one-off `*_infer` fns; the `BuiltinFn` enum and
//! its five `match self` methods stay centralized in `super::mod`.
//!
//! # Builtins in this file (PR 3)
//!
//! | Name    | Arity    | Eval signature       | Null rule          | Infer                       |
//! |---------|----------|----------------------|--------------------|-----------------------------|
//! | `log1p` | Fixed(1) | `Numeric → F64`      | strict-propagating | `numeric_to_f64`      |
//! | `clip`  | Fixed(3) | `(Num, Num, Num) → same as arg0` | strict-propagating | `clip_infer` (one-off)      |

use super::_inference::{infer_same_type, require_arg_class, InferError, TypeClass};
use crate::row::Value;
use crate::schema_propagate::InferredType;

// ─── log1p ────────────────────────────────────────────────────────────────────

/// Evaluate `log1p(x)`.
///
/// Returns `Value::F64((x as f64).ln_1p())`. Computes `ln(1 + x)` with
/// extra precision near zero (matches `f64::ln_1p`).
///
/// # Null rule
/// Strict-propagating: `log1p(null) → Null`.
///
/// # Arity
/// Fixed(1). Wrong arity → `Null` (defensive; register-time catches).
pub(super) fn log1p_eval(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }

    let arg: f64 = match &args[0] {
        Value::F64(v) => *v,
        Value::I64(v) => *v as f64,
        Value::Null => return Value::Null, // strict-propagating null rule
        _ => return Value::Null,           // non-numeric input → Null (defensive)
    };
    Value::F64(arg.ln_1p())
}

// ─── clip ─────────────────────────────────────────────────────────────────────

/// Evaluate `clip(x, lo, hi)`.
///
/// Returns `x` clamped to `[lo, hi]`. Preserves the input numeric type
/// (`I64 → I64`, `F64 → F64`).
///
/// # Null rule
/// Strict-propagating in all three args. NaN in any arg → `Null`
/// (eval-tolerant per CONTEXT.md §D-04).
///
/// # Arity
/// Fixed(3). Wrong arity → `Null` (defensive; register-time catches).
pub(super) fn clip_eval(args: &[Value]) -> Value {
    if args.len() != 3 {
        return Value::Null;
    }

    if args.iter().any(|v| matches!(v, Value::Null)) {
        return Value::Null; // strict-propagating null rule
    }

    let at_least_one_float = args.iter().any(|v| matches!(v, Value::F64(_)));

    if at_least_one_float {
        let to_f64 = |v: &Value| match v {
            Value::F64(f) => Some(*f),
            Value::I64(i) => Some(*i as f64),
            _ => None,
        };
        let (Some(x), Some(lo), Some(hi)) = (to_f64(&args[0]), to_f64(&args[1]), to_f64(&args[2]))
        else {
            return Value::Null; // non-numeric input → Null (defensive)
        };

        if x.is_nan() || lo.is_nan() || hi.is_nan() {
            return Value::Null; // NaN in any arg → Null (eval-tolerant)
        }
        Value::F64(x.clamp(lo, hi))
    } else {
        let to_i64 = |v: &Value| match v {
            Value::I64(i) => Some(*i),
            _ => None,
        };
        let (Some(x), Some(lo), Some(hi)) = (to_i64(&args[0]), to_i64(&args[1]), to_i64(&args[2]))
        else {
            return Value::Null; // non-numeric input → Null (defensive)
        };
        Value::I64(x.clamp(lo, hi))
    }
}

/// Register-time inference for `clip(x, lo, hi)`.
///
/// All three args must be the same `Numeric` type (all `I64` or all `F64`).
/// Mixed `I64`/`F64` is rejected: `clip_eval` preserves `arg0`'s concrete
/// runtime type (`I64 → I64`, `F64 → F64`), so allowing mixed bounds would
/// produce a runtime value whose type contradicts the registered schema.
/// This matches Polars, which requires clip bounds to share the input dtype.
/// `NullLiteral` is exempt per the wildcard rule.
pub(super) fn clip_infer(arg_types: &[InferredType]) -> Result<InferredType, InferError> {
    // Each arg must be numeric (require_arg_class), and all three must be the
    // *same* concrete numeric type (infer_same_type) — mixed I64/F64 is
    // rejected. The unified type is discarded; clip's result tracks arg0.
    require_arg_class(
        arg_types,
        &[TypeClass::Numeric, TypeClass::Numeric, TypeClass::Numeric],
    )?;
    infer_same_type(arg_types, &[0, 1, 2])?;
    Ok(arg_types[0].clone())
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::Value;
    use crate::schema::FieldType;
    use crate::schema_propagate::InferredType;

    // ── log1p ─────────────────────────────────────────────────────────────────

    /// log1p takes one number. Calling it with zero or two args must
    /// give back Null instead of crashing.
    #[test]
    fn log1p_arity_wrong_count_errors() {
        assert_eq!(log1p_eval(&[]), Value::Null);
        assert_eq!(log1p_eval(&[Value::F64(1.0), Value::F64(2.0)]), Value::Null);
    }

    /// Three example inputs that pin what log1p computes: `ln(1 + x)`.
    #[test]
    fn log1p_eval_truth_table() {
        // log1p(0) = ln(1) = 0
        assert_eq!(log1p_eval(&[Value::F64(0.0)]), Value::F64(0.0));
        // log1p(1) = ln(2) ≈ 0.6931...
        assert_eq!(log1p_eval(&[Value::F64(1.0)]), Value::F64(f64::ln_1p(1.0)));
        // I64 input promotes to F64
        assert_eq!(log1p_eval(&[Value::I64(0)]), Value::F64(0.0));
    }

    /// Confirms two things:
    ///   1. log1p(null) gives back null (strict-propagating null rule).
    ///   2. log1p of a real number does NOT give back null.
    #[test]
    fn log1p_null_rule_matches_doc() {
        assert_eq!(log1p_eval(&[Value::Null]), Value::Null);
        assert_ne!(log1p_eval(&[Value::F64(1.0)]), Value::Null);
    }

    /// log1p's type rule: takes a number, gives back a float. The actual
    /// check is in the shared helper `numeric_to_f64` (already
    /// tested elsewhere). This test pins that the helper still does what
    /// log1p needs — so if anyone changes the helper, we notice.
    #[test]
    fn log1p_infer_typecheck() {
        use super::super::_inference::numeric_to_f64;
        assert_eq!(
            numeric_to_f64(&[InferredType::Known(FieldType::F64)]),
            Ok(InferredType::Known(FieldType::F64))
        );
        assert_eq!(
            numeric_to_f64(&[InferredType::Known(FieldType::I64)]),
            Ok(InferredType::Known(FieldType::F64))
        );
        assert!(numeric_to_f64(&[InferredType::Known(FieldType::Str)]).is_err());
    }

    // ── clip ──────────────────────────────────────────────────────────────────

    /// clip takes exactly three args (value, low, high). Any other count
    /// must give Null instead of crashing.
    #[test]
    fn clip_arity_wrong_count_errors() {
        assert_eq!(clip_eval(&[]), Value::Null);
        assert_eq!(clip_eval(&[Value::I64(5)]), Value::Null);
        assert_eq!(clip_eval(&[Value::I64(5), Value::I64(0)]), Value::Null);
        // 4 args: also wrong
        assert_eq!(
            clip_eval(&[Value::I64(5), Value::I64(0), Value::I64(10), Value::I64(99),]),
            Value::Null
        );
    }

    /// Five example inputs that pin clip's behavior: inside the range
    /// returns the value unchanged; below the low bound returns the low
    /// bound; above the high bound returns the high bound; exact
    /// boundary returns the boundary; float input stays float.
    #[test]
    fn clip_eval_truth_table() {
        // Inside range: returns x unchanged
        assert_eq!(
            clip_eval(&[Value::I64(5), Value::I64(0), Value::I64(10)]),
            Value::I64(5)
        );
        // Below low bound: returns lo
        assert_eq!(
            clip_eval(&[Value::I64(-3), Value::I64(0), Value::I64(10)]),
            Value::I64(0)
        );
        // Above high bound: returns hi
        assert_eq!(
            clip_eval(&[Value::I64(99), Value::I64(0), Value::I64(10)]),
            Value::I64(10)
        );
        // Exact boundary (lo)
        assert_eq!(
            clip_eval(&[Value::I64(0), Value::I64(0), Value::I64(10)]),
            Value::I64(0)
        );
        // F64 preserves type
        assert_eq!(
            clip_eval(&[Value::F64(2.5), Value::F64(0.0), Value::F64(1.0)]),
            Value::F64(1.0)
        );
    }

    /// If ANY of the three args is null, clip gives back null. Also if
    /// any arg is NaN (a broken float), clip gives back null instead of
    /// returning nonsense. The last assertion confirms a valid input
    /// gives back a non-null result.
    #[test]
    fn clip_null_rule_matches_doc() {
        // Strict in all three args
        assert_eq!(
            clip_eval(&[Value::Null, Value::I64(0), Value::I64(10)]),
            Value::Null
        );
        assert_eq!(
            clip_eval(&[Value::I64(5), Value::Null, Value::I64(10)]),
            Value::Null
        );
        assert_eq!(
            clip_eval(&[Value::I64(5), Value::I64(0), Value::Null]),
            Value::Null
        );
        // NaN in any arg → Null (eval-tolerant)
        assert_eq!(
            clip_eval(&[Value::F64(f64::NAN), Value::F64(0.0), Value::F64(10.0)]),
            Value::Null
        );
        // Non-null sanity: valid input → non-null result.
        assert_ne!(
            clip_eval(&[Value::I64(5), Value::I64(0), Value::I64(10)]),
            Value::Null
        );
    }

    /// clip's type rule: takes three numbers, gives back the same number
    /// type as the first arg (int in → int out, float in → float out).
    /// Also confirms that giving a non-number (like a string) is
    /// rejected at register time, not at runtime.
    #[test]
    fn clip_infer_typecheck() {
        let args = [
            InferredType::Known(FieldType::I64),
            InferredType::Known(FieldType::I64),
            InferredType::Known(FieldType::I64),
        ];
        assert_eq!(clip_infer(&args), Ok(InferredType::Known(FieldType::I64)));
        // F64 input → F64 output (identity)
        let args_f = [
            InferredType::Known(FieldType::F64),
            InferredType::Known(FieldType::F64),
            InferredType::Known(FieldType::F64),
        ];
        assert_eq!(clip_infer(&args_f), Ok(InferredType::Known(FieldType::F64)));
        // Non-numeric arg → TypeMismatch
        let args_bad = [
            InferredType::Known(FieldType::Str),
            InferredType::Known(FieldType::I64),
            InferredType::Known(FieldType::I64),
        ];
        assert!(clip_infer(&args_bad).is_err());
        // Mixed I64/F64 → Unify error. eval preserves arg0's type, so
        // clip(i64_col, 0.0, 10.0) would infer I64 but produce F64 at
        // runtime — contradicting the schema. Reject at register time.
        let args_mixed = [
            InferredType::Known(FieldType::I64),
            InferredType::Known(FieldType::F64),
            InferredType::Known(FieldType::F64),
        ];
        assert!(clip_infer(&args_mixed).is_err());
    }
}
