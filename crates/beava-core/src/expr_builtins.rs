//! Builtin function registry for the Phase 4 expression evaluator.
//!
//! # Design (CONTEXT.md §D-07, §D-08)
//!
//! Builtins are stored in a static `BUILTINS` table. Adding a new builtin in
//! Phase 5+ requires only appending one `BuiltinFn` entry — no grammar or
//! evaluator dispatch changes needed (SRV-APPLY-06 extension hook).
//!
//! Phase 4 ships two builtins:
//! - `cast(value, type_str)` — converts `Value` to target type; returns `Value::Null` on failure.
//! - `isnull(value)` — always returns `Bool(true/false)`, never `Null`.
//!
//! # Cast policy decisions (CONTEXT.md §D-05)
//!
//! - **Arity check**: wrong argument count → `Null` (register-time catches; runtime is defensive).
//! - **Null input**: `cast(null, any)` → `Null` (all targets).
//! - **Unknown target type**: → `Null`.
//! - **f64 → i64**: `as i64` (truncate toward zero, Rust default). Documents the truncation choice.
//! - **str → int/float**: `str.parse::<i64/f64>()` — fails → `Null`, not panic.
//! - **Bytes**: no implicit bytes-to-str without an encoding spec → `Null`.

use crate::row::Value;

// ─── Arity ────────────────────────────────────────────────────────────────────

/// Argument count constraint for a builtin function.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Arity {
    /// Exactly `n` arguments required.
    Fixed(usize),
    /// Any number of arguments (including zero).
    Variadic,
}

// ─── BuiltinFn ────────────────────────────────────────────────────────────────

/// A single entry in the builtin function table.
pub struct BuiltinFn {
    /// Function name as it appears in expressions (e.g. `"cast"`, `"isnull"`).
    pub name: &'static str,
    /// Expected argument count.
    pub arity: Arity,
    /// Evaluation function called by the evaluator after arguments have been
    /// evaluated to `Value`s.
    pub eval: fn(&[Value]) -> Value,
}

// ─── BUILTINS table ───────────────────────────────────────────────────────────

/// Static table of Phase 4 builtins.
///
/// Phase 5+ extension: append a new `BuiltinFn` here. No grammar or evaluator
/// dispatch surgery required — `lookup_builtin` performs a linear scan.
pub const BUILTINS: &[BuiltinFn] = &[
    BuiltinFn {
        name: "cast",
        arity: Arity::Fixed(2),
        eval: cast_eval,
    },
    BuiltinFn {
        name: "isnull",
        arity: Arity::Fixed(1),
        eval: isnull_eval,
    },
];

// ─── Lookup ───────────────────────────────────────────────────────────────────

/// Returns the `BuiltinFn` entry for `name`, or `None` if unknown.
///
/// Linear scan over the (currently 2-item) `BUILTINS` table.  O(n) is fine
/// at the current scale; a `HashMap` would be premature.
#[allow(unused_variables)]
pub fn lookup_builtin(name: &str) -> Option<&'static BuiltinFn> {
    todo!()
}

// ─── cast ─────────────────────────────────────────────────────────────────────

/// Evaluate `cast(value, type_str)`.
///
/// `args[0]` is the value to cast; `args[1]` is `Value::Str(type_str)` (the
/// evaluator converts `Literal::BareIdent` → `Value::Str` before dispatch, so
/// `cast(x, float)` arrives as `[eval(x), Value::Str("float")]`).
///
/// Returns `Value::Null` on any error: wrong arity, unknown type, null input,
/// or parse failure.
#[allow(unused_variables)]
fn cast_eval(args: &[Value]) -> Value {
    todo!()
}

// ─── isnull ───────────────────────────────────────────────────────────────────

/// Evaluate `isnull(value)`.
///
/// Always returns `Bool(true/false)` — never `Null`.
#[allow(unused_variables)]
fn isnull_eval(args: &[Value]) -> Value {
    todo!()
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::Value;

    // ── Lookup tests ──────────────────────────────────────────────────────────

    #[test]
    fn lookup_builtin_returns_cast() {
        let b = lookup_builtin("cast").expect("cast must be in BUILTINS");
        assert_eq!(b.name, "cast");
        assert_eq!(b.arity, Arity::Fixed(2));
    }

    #[test]
    fn lookup_builtin_returns_isnull() {
        let b = lookup_builtin("isnull").expect("isnull must be in BUILTINS");
        assert_eq!(b.name, "isnull");
        assert_eq!(b.arity, Arity::Fixed(1));
    }

    #[test]
    fn lookup_builtin_unknown_returns_none() {
        assert!(lookup_builtin("foo").is_none());
        assert!(lookup_builtin("").is_none());
        assert!(lookup_builtin("COUNT").is_none());
    }

    // ── isnull tests ──────────────────────────────────────────────────────────

    #[test]
    fn isnull_of_null_is_bool_true() {
        assert_eq!(isnull_eval(&[Value::Null]), Value::Bool(true));
    }

    #[test]
    fn isnull_of_i64_is_bool_false() {
        assert_eq!(isnull_eval(&[Value::I64(42)]), Value::Bool(false));
        assert_eq!(isnull_eval(&[Value::I64(0)]), Value::Bool(false));
        assert_eq!(isnull_eval(&[Value::I64(-1)]), Value::Bool(false));
    }

    #[test]
    fn isnull_of_str_is_bool_false() {
        assert_eq!(
            isnull_eval(&[Value::Str("hello".to_string())]),
            Value::Bool(false)
        );
        assert_eq!(
            isnull_eval(&[Value::Str(String::new())]),
            Value::Bool(false)
        );
    }

    // ── cast tests ────────────────────────────────────────────────────────────

    // cast(I64(7), Str("float")) → F64(7.0)
    #[test]
    fn cast_int_to_float_ok() {
        assert_eq!(
            cast_eval(&[Value::I64(7), Value::Str("float".to_string())]),
            Value::F64(7.0)
        );
    }

    // cast(Str("42"), Str("int")) → I64(42)
    #[test]
    fn cast_str_to_int_parses_numeric() {
        assert_eq!(
            cast_eval(&[Value::Str("42".to_string()), Value::Str("int".to_string())]),
            Value::I64(42)
        );
    }

    // cast(Str("abc"), Str("int")) → Null (parse failure)
    #[test]
    fn cast_str_to_int_nonnumeric_is_null() {
        assert_eq!(
            cast_eval(&[Value::Str("abc".to_string()), Value::Str("int".to_string())]),
            Value::Null
        );
    }

    // cast(Null, Str("int")) → Null
    #[test]
    fn cast_null_any_is_null() {
        assert_eq!(
            cast_eval(&[Value::Null, Value::Str("int".to_string())]),
            Value::Null
        );
        assert_eq!(
            cast_eval(&[Value::Null, Value::Str("str".to_string())]),
            Value::Null
        );
        assert_eq!(
            cast_eval(&[Value::Null, Value::Str("float".to_string())]),
            Value::Null
        );
        assert_eq!(
            cast_eval(&[Value::Null, Value::Str("bool".to_string())]),
            Value::Null
        );
    }

    // cast(I64(1), Str("blob")) → Null (unknown target type)
    #[test]
    fn cast_unknown_type_is_null() {
        assert_eq!(
            cast_eval(&[Value::I64(1), Value::Str("blob".to_string())]),
            Value::Null
        );
        assert_eq!(
            cast_eval(&[Value::I64(1), Value::Str("bytes".to_string())]),
            Value::Null
        );
    }

    // cast(F64(3.9), Str("int")) → I64(3)  (truncate toward zero, Rust `as i64`)
    #[test]
    fn cast_float_to_int_truncates() {
        // Truncation toward zero: 3.9 → 3, -3.9 → -3
        assert_eq!(
            cast_eval(&[Value::F64(3.9), Value::Str("int".to_string())]),
            Value::I64(3)
        );
        assert_eq!(
            cast_eval(&[Value::F64(-3.9), Value::Str("int".to_string())]),
            Value::I64(-3)
        );
    }

    // cast(Bool(true), Str("int")) → I64(1); cast(Bool(false), Str("int")) → I64(0)
    #[test]
    fn cast_bool_to_int() {
        assert_eq!(
            cast_eval(&[Value::Bool(true), Value::Str("int".to_string())]),
            Value::I64(1)
        );
        assert_eq!(
            cast_eval(&[Value::Bool(false), Value::Str("int".to_string())]),
            Value::I64(0)
        );
    }

    // cast(I64(42), Str("str")) → Str("42")
    #[test]
    fn cast_int_to_str() {
        assert_eq!(
            cast_eval(&[Value::I64(42), Value::Str("str".to_string())]),
            Value::Str("42".to_string())
        );
    }

    // cast(I64(1)) [wrong arity] → Null  (defensive; register-time catches)
    #[test]
    fn cast_arity_wrong_is_null() {
        assert_eq!(cast_eval(&[Value::I64(1)]), Value::Null);
        assert_eq!(cast_eval(&[]), Value::Null);
        assert_eq!(
            cast_eval(&[
                Value::I64(1),
                Value::Str("int".to_string()),
                Value::Str("extra".to_string())
            ]),
            Value::Null
        );
    }
}
