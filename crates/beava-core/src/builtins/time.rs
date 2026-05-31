//! Time builtins.
//!
//! Per-category file under the PR 3 BUILTINS split (RFC-001 §5.2). Holds
//! `*_eval` free fns and one-off `*_infer` fns; the `BuiltinFn` enum and
//! its five `match self` methods stay centralized in `super::mod`.
//!
//! # Builtins in this file (PR 3)
//!
//! | Name          | Arity    | Eval signature        | Null rule          | Infer                  |
//! |---------------|----------|-----------------------|--------------------|------------------------|
//! | `hour_of_day` | Fixed(1) | `Datetime → I64`      | strict-propagating | `hour_of_day_infer`    |

use super::_inference::{require_arg_types, InferError};
use crate::row::Value;
use crate::schema::FieldType;
use crate::schema_propagate::InferredType;

// ─── hour_of_day ──────────────────────────────────────────────────────────────

/// Evaluate `hour_of_day(dt)`.
///
/// Returns `Value::I64(0..=23)` — the hour-of-day component of a
/// `Datetime` value (milliseconds since epoch, UTC).
///
/// # Null rule
/// Strict-propagating: `hour_of_day(null) → Null`.
///
/// # Arity
/// Fixed(1). Wrong arity → `Null` (defensive; register-time catches).
pub(super) fn hour_of_day_eval(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    match args[0] {
        Value::Datetime(ms) => {
            // ms is epoch millis UTC. div_euclid/rem_euclid handles pre-epoch (negative) timestamps.
            Value::I64(ms.div_euclid(3_600_000).rem_euclid(24))
        }
        Value::Null => Value::Null,
        _ => Value::Null,
    }
}

/// Register-time inference for `hour_of_day(dt)`.
///
/// One `Datetime` arg, returns `I64`.
pub(super) fn hour_of_day_infer(arg_types: &[InferredType]) -> Result<InferredType, InferError> {
    require_arg_types(arg_types, &[FieldType::Datetime])?;
    Ok(InferredType::Known(FieldType::I64))
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::Value;
    use crate::schema::FieldType;
    use crate::schema_propagate::InferredType;

    /// hour_of_day takes one timestamp. Giving zero or two args must
    /// give back Null instead of crashing.
    #[test]
    fn hour_of_day_arity_wrong_count_errors() {
        assert_eq!(hour_of_day_eval(&[]), Value::Null);
        assert_eq!(
            hour_of_day_eval(&[Value::Datetime(0), Value::Datetime(0)]),
            Value::Null
        );
    }

    /// Three timestamps that pin the hour extraction:
    ///   - midnight Jan 1 1970 → hour 0
    ///   - 23:00 same day → hour 23
    ///   - 13:30 same day → hour 13
    #[test]
    fn hour_of_day_eval_truth_table() {
        // 1970-01-01 00:00:00 UTC → hour 0
        assert_eq!(hour_of_day_eval(&[Value::Datetime(0)]), Value::I64(0));
        // 1970-01-01 23:00:00 UTC = 23 * 3600 * 1000 ms → hour 23
        assert_eq!(
            hour_of_day_eval(&[Value::Datetime(23 * 3600 * 1000)]),
            Value::I64(23)
        );
        // 1970-01-01 13:30:00 UTC = (13*3600 + 1800) * 1000 → hour 13
        assert_eq!(
            hour_of_day_eval(&[Value::Datetime((13 * 3600 + 1800) * 1000)]),
            Value::I64(13)
        );
    }

    /// hour_of_day(null) → null (always). A real timestamp must NOT
    /// give back null.
    #[test]
    fn hour_of_day_null_rule() {
        assert_eq!(hour_of_day_eval(&[Value::Null]), Value::Null);
        assert_ne!(hour_of_day_eval(&[Value::Datetime(0)]), Value::Null);
    }

    /// hour_of_day's type rule: takes a timestamp, gives back an int.
    /// Passing something other than a timestamp (like an int or string)
    /// is rejected at register time.
    #[test]
    fn hour_of_day_infer_typecheck() {
        assert_eq!(
            hour_of_day_infer(&[InferredType::Known(FieldType::Datetime)]),
            Ok(InferredType::Known(FieldType::I64))
        );
        // Non-Datetime → TypeMismatch
        assert!(hour_of_day_infer(&[InferredType::Known(FieldType::I64)]).is_err());
        assert!(hour_of_day_infer(&[InferredType::Known(FieldType::Str)]).is_err());
    }
}
