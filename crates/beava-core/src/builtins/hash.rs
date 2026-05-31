//! Hash + geo-hash builtins.
//!
//! Per-category file under the PR 3 BUILTINS split (RFC-001 §5.2). Holds
//! `*_eval` free fns and one-off `*_infer` fns; the `BuiltinFn` enum and
//! its five `match self` methods stay centralized in `super::mod`.
//!
//! # Builtins in this file
//!
//! | Name       | Arity    | Eval signature              | Null rule          | Infer              | PR  |
//! |------------|----------|-----------------------------|--------------------|--------------------|-----|
//! | `quadkey`  | Fixed(3) | `(Num, Num, Num) → I64`     | strict (any null → Null) | `quadkey_infer` | PR 1 |
//! | `hash_mod` | Fixed(2) | `(Any, Num) → I64`          | strict-propagating | `hash_mod_infer`   | PR 3 |
//!
//! `quadkey` moved here from `super::mod` in PR 3 Step 2 — no clear
//! category, but geohash is the closest sibling to hashing.

use super::_inference::{require_arg_class, InferError, TypeClass};
use crate::row::Value;
use crate::schema::FieldType;
use crate::schema_propagate::InferredType;

// ─── quadkey ─────────────────────────────────────────────────────────────────

/// Evaluate `quadkey(lat, lon, zoom)`.
///
/// Returns a deterministic `Value::I64` cell-id using a simplified-Mercator
/// formula (NOT RFC slippy-tile — no external tile dependency required).
///
/// # Formula
///
/// ```text
/// n   = 1 << zoom                         (tiles per axis)
/// row = floor((sin(lat_clamped_rad) + 1) / 2 * n)
/// col = floor((lon + 180) / 360 * n)
/// cell_id = col * n + row.clamp(0, n-1)
/// ```
///
/// # Null / range rules
/// - Any `Null` argument → `Null`.
/// - `zoom` outside `1..=24` → `Null`.
/// - `lat` is clamped to `[-85.05112878, 85.05112878]` (Web-Mercator bounds).
pub(super) fn quadkey_eval(args: &[Value]) -> Value {
    if args.len() != 3 {
        return Value::Null;
    }
    let lat = match &args[0] {
        Value::F64(v) => *v,
        Value::I64(v) => *v as f64,
        _ => return Value::Null,
    };
    let lon = match &args[1] {
        Value::F64(v) => *v,
        Value::I64(v) => *v as f64,
        _ => return Value::Null,
    };
    let zoom = match &args[2] {
        Value::I64(v) if (1..=24).contains(v) => *v,
        _ => return Value::Null,
    };
    let n = 1i64 << zoom;
    let lat_clamped = lat.clamp(-85.051_128_78, 85.051_128_78);
    let row = ((lat_clamped.to_radians().sin() + 1.0) / 2.0 * (n as f64)).floor() as i64;
    let col = ((lon + 180.0) / 360.0 * (n as f64)).floor() as i64;
    Value::I64(
        col.saturating_mul(n)
            .saturating_add(row.clamp(0, n.saturating_sub(1))),
    )
}

/// Register-time inference for `quadkey(lat, lon, zoom)`.
///
/// All three args typed as `Numeric` (I64 or F64); `NullLiteral` accepted
/// per the wildcard rule. Returns `I64`. Note: zoom is lenient at register
/// time — runtime requires strict `I64` in `1..=24` and returns `Null`
/// otherwise (matches existing `quadkey_eval` behavior).
pub(super) fn quadkey_infer(arg_types: &[InferredType]) -> Result<InferredType, InferError> {
    require_arg_class(
        arg_types,
        &[TypeClass::Numeric, TypeClass::Numeric, TypeClass::Numeric],
    )?;
    Ok(InferredType::Known(FieldType::I64))
}

// ─── hash_mod ─────────────────────────────────────────────────────────────────

/// Evaluate `hash_mod(x, m)`.
///
/// Returns `Value::I64(hash(x) % m)`. Used for bucketing high-cardinality
/// fields into fixed-size feature spaces (e.g. `hash_mod(email, 1024)`).
///
/// # Null rule
/// Strict-propagating: any `Null` arg → `Null`. `m <= 0` → `Null` (avoids
/// modulo-by-zero / negative-modulo footgun).
///
/// # Arity
/// Fixed(2). Wrong arity → `Null` (defensive; register-time catches).
pub(super) fn hash_mod_eval(args: &[Value]) -> Value {
    if args.len() != 2 {
        return Value::Null;
    }
    if args.iter().any(|v| matches!(v, Value::Null)) {
        return Value::Null;
    }
    let m: i64 = match &args[1] {
        Value::I64(v) => *v,
        _ => return Value::Null,
    };
    if m <= 0 {
        return Value::Null; // guard against modulo-by-zero / negative-modulo
    }
    let hash = crate::agg_state::hash_value_for_hll(&args[0]);
    Value::I64((hash % m as u64) as i64)
}

/// Register-time inference for `hash_mod(x, m)`.
///
/// First arg is `Any` (hashable scalar). Second arg must be strictly `I64` —
/// fractional bucket counts make no semantic sense and are rejected at
/// register time rather than silently truncated.
pub(super) fn hash_mod_infer(arg_types: &[InferredType]) -> Result<InferredType, InferError> {
    if arg_types.len() != 2 {
        return Err(InferError::Arity {
            expected: 2,
            got: arg_types.len(),
        });
    }
    match &arg_types[1] {
        InferredType::NullLiteral | InferredType::Known(FieldType::I64) => {}
        got => {
            return Err(InferError::TypeMismatch {
                arg_idx: 1,
                expected: "I64",
                got: got.clone(),
            });
        }
    }
    Ok(InferredType::Known(FieldType::I64))
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::Value;
    use crate::schema::FieldType;
    use crate::schema_propagate::InferredType;

    // ── quadkey (carried over from mod.rs; behavior unchanged) ────────────────

    /// A valid lat/lon/zoom must produce an integer cell-id. Sanity check
    /// that moving quadkey from mod.rs to hash.rs didn't break anything.
    #[test]
    fn quadkey_returns_i64_for_valid_args() {
        let r = quadkey_eval(&[Value::F64(40.0), Value::F64(-74.0), Value::I64(7)]);
        assert!(matches!(r, Value::I64(_)));
    }

    /// quadkey's zoom must be between 1 and 24. Anything outside (0, 25,
    /// negative, huge) gets back Null instead of returning bogus tile ids.
    #[test]
    fn quadkey_zoom_out_of_range_is_null() {
        // zoom=0 below the 1..=24 range
        assert_eq!(
            quadkey_eval(&[Value::F64(40.0), Value::F64(-74.0), Value::I64(0)]),
            Value::Null
        );
        // zoom=25 above the range
        assert_eq!(
            quadkey_eval(&[Value::F64(40.0), Value::F64(-74.0), Value::I64(25)]),
            Value::Null
        );
    }

    /// If ANY of the three args is null, quadkey gives back null.
    /// One test for each position to make sure none are accidentally
    /// special-cased.
    #[test]
    fn quadkey_null_arg_is_null() {
        assert_eq!(
            quadkey_eval(&[Value::Null, Value::F64(-74.0), Value::I64(7)]),
            Value::Null
        );
        assert_eq!(
            quadkey_eval(&[Value::F64(40.0), Value::Null, Value::I64(7)]),
            Value::Null
        );
        assert_eq!(
            quadkey_eval(&[Value::F64(40.0), Value::F64(-74.0), Value::Null]),
            Value::Null
        );
    }

    /// quadkey's type rule: takes three numbers, gives back an int.
    /// Passing a string is rejected at register time, not at runtime.
    #[test]
    fn quadkey_infer_typecheck() {
        let args = [
            InferredType::Known(FieldType::F64),
            InferredType::Known(FieldType::F64),
            InferredType::Known(FieldType::I64),
        ];
        assert_eq!(
            quadkey_infer(&args),
            Ok(InferredType::Known(FieldType::I64))
        );
        // Non-numeric → TypeMismatch
        let args_bad = [
            InferredType::Known(FieldType::Str),
            InferredType::Known(FieldType::F64),
            InferredType::Known(FieldType::I64),
        ];
        assert!(quadkey_infer(&args_bad).is_err());
    }

    // ── hash_mod ──────────────────────────────────────────────────────────────

    /// hash_mod takes exactly two args (value, bucket_count). Any other
    /// count must give Null instead of crashing.
    #[test]
    fn hash_mod_arity_wrong_count_errors() {
        assert_eq!(hash_mod_eval(&[]), Value::Null);
        assert_eq!(hash_mod_eval(&[Value::I64(42)]), Value::Null);
        assert_eq!(
            hash_mod_eval(&[Value::I64(42), Value::I64(1024), Value::I64(99)]),
            Value::Null
        );
    }

    /// Two things this pins:
    ///   1. SHAPE — the result is an int between 0 and the bucket count.
    ///      Doesn't pin a specific hash number because the impl is free
    ///      to pick which hash library to use.
    ///   2. DETERMINISM — same input must always give the same bucket.
    ///      If hash_mod returned a random number, two events with the
    ///      same email would land in different buckets and break feature
    ///      computation entirely.
    #[test]
    fn hash_mod_eval_truth_table() {
        let r = hash_mod_eval(&[Value::Str("hello".into()), Value::I64(1024)]);
        match r {
            Value::I64(n) => assert!((0..1024).contains(&n), "result {n} out of [0, 1024)"),
            _ => panic!("expected I64, got {r:?}"),
        }
        // Determinism: same input → same output
        let r1 = hash_mod_eval(&[Value::Str("hello".into()), Value::I64(1024)]);
        let r2 = hash_mod_eval(&[Value::Str("hello".into()), Value::I64(1024)]);
        assert_eq!(r1, r2);
    }

    /// Three things this pins:
    ///   1. null in either arg → null.
    ///   2. bucket_count <= 0 → null. Without this guard, the impl would
    ///      crash with "divide by zero" or give a meaningless negative
    ///      bucket id.
    ///   3. Valid input must NOT give null.
    #[test]
    fn hash_mod_null_rule_matches_doc() {
        // Strict in both args
        assert_eq!(hash_mod_eval(&[Value::Null, Value::I64(1024)]), Value::Null);
        assert_eq!(hash_mod_eval(&[Value::I64(42), Value::Null]), Value::Null);
        // m <= 0 → Null (modulo-by-zero / negative-modulo footgun)
        assert_eq!(hash_mod_eval(&[Value::I64(42), Value::I64(0)]), Value::Null);
        assert_eq!(
            hash_mod_eval(&[Value::I64(42), Value::I64(-1)]),
            Value::Null
        );
        // Non-null sanity: valid input → non-null result.
        assert_ne!(
            hash_mod_eval(&[Value::Str("hello".into()), Value::I64(1024)]),
            Value::Null
        );
    }

    /// hash_mod's type rule: takes anything for the first arg, but m must be
    /// strictly I64 — fractional bucket counts are rejected at register time,
    /// not silently truncated at eval time.
    #[test]
    fn hash_mod_infer_typecheck() {
        // (Str, I64) → I64
        let args_str = [
            InferredType::Known(FieldType::Str),
            InferredType::Known(FieldType::I64),
        ];
        assert_eq!(
            hash_mod_infer(&args_str),
            Ok(InferredType::Known(FieldType::I64))
        );
        // Any first arg accepted
        let args_bool = [
            InferredType::Known(FieldType::Bool),
            InferredType::Known(FieldType::I64),
        ];
        assert_eq!(
            hash_mod_infer(&args_bool),
            Ok(InferredType::Known(FieldType::I64))
        );
        // Str m → TypeMismatch
        let args_str_m = [
            InferredType::Known(FieldType::Str),
            InferredType::Known(FieldType::Str),
        ];
        assert!(hash_mod_infer(&args_str_m).is_err());
        // F64 m → TypeMismatch (fractional bucket count makes no sense)
        let args_f64_m = [
            InferredType::Known(FieldType::Str),
            InferredType::Known(FieldType::F64),
        ];
        assert!(hash_mod_infer(&args_f64_m).is_err());
    }
}
