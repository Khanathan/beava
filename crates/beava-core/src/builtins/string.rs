//! String builtins.
//!
//! Per-category file under the PR 3 BUILTINS split (RFC-001 §5.2). Holds
//! `*_eval` free fns and their unit tests; the `BuiltinFn` enum and its
//! five `match self` methods stay centralized in `super::mod`.
//!
//! Inference for each string builtin reuses a shared helper from
//! `super::_inference` (`str_to_str`, `str_to_i64`,
//! `string_search_to_bool`). `replace` is the lone one-off — three
//! `Str` args, no shared helper — so it gets a `replace_infer` here.
//!
//! # Builtins in this file (PR 3)
//!
//! | Name          | Arity      | Eval signature              | Null rule              | Infer                   |
//! |---------------|------------|-----------------------------|------------------------|-------------------------|
//! | `lower`       | Fixed(1)   | `Str → Str`                 | strict-propagating     | `str_to_str`      |
//! | `length`      | Fixed(1)   | `Str → I64` (codepoints)    | strict-propagating     | `str_to_i64`      |
//! | `contains`    | Fixed(2)   | `(Str, Str) → Bool`         | null-aware predicate   | `string_search_to_bool` |
//! | `starts_with` | Fixed(2)   | `(Str, Str) → Bool`         | null-aware predicate   | `string_search_to_bool` |
//! | `ends_with`   | Fixed(2)   | `(Str, Str) → Bool`         | null-aware predicate   | `string_search_to_bool` |
//! | `replace`     | Fixed(3)   | `(Str, Str, Str) → Str`     | strict-propagating     | `replace_infer` (one-off) |

use super::_inference::{require_arg_types, InferError};
use crate::row::Value;
use crate::schema::FieldType;
use crate::schema_propagate::InferredType;

// ─── lower ────────────────────────────────────────────────────────────────────

/// Evaluate `lower(s)`.
///
/// Returns `s.to_lowercase()` (Unicode-aware via `str::to_lowercase`).
///
/// # Null rule
/// Strict-propagating: `lower(null) → Null`.
///
/// # Arity
/// Fixed(1). Wrong arity → `Null` (defensive; register-time catches).
pub(super) fn lower_eval(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    match &args[0] {
        Value::Str(s) => Value::Str(s.to_lowercase()),
        Value::Null => Value::Null,
        _ => Value::Null, // register-time infer guarantees Str; defensive only
    }
}

// ─── length ───────────────────────────────────────────────────────────────────
/// Note: Might become obsolete once we support nested types and support len() in python
///
/// Evaluate `length(s)`.
///
/// Returns `Value::I64(s.chars().count() as i64)` — codepoint count, not
/// byte count. Pinned in tests; matches Python's `len("héllo") == 5`.
///
/// # Null rule
/// Strict-propagating: `length(null) → Null`.
///
/// # Arity
/// Fixed(1). Wrong arity → `Null` (defensive; register-time catches).
pub(super) fn length_eval(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    match &args[0] {
        // Codepoint count, not byte count: "héllo" → 5, not 6. Matches Python's len().
        Value::Str(s) => Value::I64(s.chars().count() as i64),
        Value::Null => Value::Null,
        _ => Value::Null, // register-time infer guarantees Str; defensive only
    }
}

// ─── contains ─────────────────────────────────────────────────────────────────

/// Evaluate `contains(haystack, needle)`.
///
/// Returns `Bool(haystack.contains(needle))`.
///
/// # Null rule
/// Null-aware predicate: any `Null` arg → `Null` (not `Bool(false)`).
/// Matches SQL `LIKE` semantics where `null LIKE _ → null`.
///
/// # Arity
/// Fixed(2). Wrong arity → `Null` (defensive; register-time catches).
pub(super) fn contains_eval(args: &[Value]) -> Value {
    if args.len() != 2 {
        return Value::Null;
    }
    match (&args[0], &args[1]) {
        (Value::Str(haystack), Value::Str(needle)) => {
            Value::Bool(haystack.contains(needle.as_str()))
        }
        (Value::Null, _) | (_, Value::Null) => Value::Null,
        _ => Value::Null,
    }
}

// ─── starts_with ──────────────────────────────────────────────────────────────

/// Evaluate `starts_with(haystack, needle)`.
///
/// Returns `Bool(haystack.starts_with(needle))`.
///
/// # Null rule
/// Null-aware predicate: any `Null` arg → `Null`.
///
/// # Arity
/// Fixed(2). Wrong arity → `Null` (defensive; register-time catches).
pub(super) fn starts_with_eval(args: &[Value]) -> Value {
    if args.len() != 2 {
        return Value::Null;
    }
    match (&args[0], &args[1]) {
        (Value::Str(haystack), Value::Str(prefix)) => {
            Value::Bool(haystack.starts_with(prefix.as_str()))
        }
        (Value::Null, _) | (_, Value::Null) => Value::Null,
        _ => Value::Null,
    }
}

// ─── ends_with ────────────────────────────────────────────────────────────────

/// Evaluate `ends_with(haystack, needle)`.
///
/// Returns `Bool(haystack.ends_with(needle))`.
///
/// # Null rule
/// Null-aware predicate: any `Null` arg → `Null`.
///
/// # Arity
/// Fixed(2). Wrong arity → `Null` (defensive; register-time catches).
pub(super) fn ends_with_eval(args: &[Value]) -> Value {
    if args.len() != 2 {
        return Value::Null;
    }
    match (&args[0], &args[1]) {
        (Value::Str(haystack), Value::Str(suffix)) => {
            Value::Bool(haystack.ends_with(suffix.as_str()))
        }
        (Value::Null, _) | (_, Value::Null) => Value::Null,
        _ => Value::Null,
    }
}

// ─── replace ──────────────────────────────────────────────────────────────────

/// Evaluate `replace(s, old, new)`.
///
/// Returns `Value::Str(s.replace(old, new))` — replaces every non-overlapping
/// occurrence of `old` with `new`.
///
/// # Null rule
/// Strict-propagating in all three args.
///
/// # Arity
/// Fixed(3). Wrong arity → `Null` (defensive; register-time catches).
pub(super) fn replace_eval(args: &[Value]) -> Value {
    if args.len() != 3 {
        return Value::Null;
    }
    if args.iter().any(|v| matches!(v, Value::Null)) {
        return Value::Null;
    }
    match (&args[0], &args[1], &args[2]) {
        (Value::Str(s), Value::Str(old), Value::Str(new)) => {
            Value::Str(s.replace(old.as_str(), new.as_str()).into())
        }
        _ => Value::Null,
    }
}

/// Register-time inference for `replace(s, old, new)`.
///
/// All three args `Str`; returns `Str`. One-off because the shared
/// helpers cap at two `Str` args.
pub(super) fn replace_infer(arg_types: &[InferredType]) -> Result<InferredType, InferError> {
    require_arg_types(arg_types, &[FieldType::Str, FieldType::Str, FieldType::Str])?;
    Ok(InferredType::Known(FieldType::Str))
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::super::_inference::{str_to_i64, str_to_str, string_search_to_bool};
    use super::*;
    use crate::row::Value;
    use crate::schema::FieldType;
    use crate::schema_propagate::InferredType;

    // ── lower ─────────────────────────────────────────────────────────────────

    /// lower takes one string. Other arg counts give back Null instead
    /// of crashing.
    #[test]
    fn lower_arity_wrong_count_errors() {
        assert_eq!(lower_eval(&[]), Value::Null);
        assert_eq!(
            lower_eval(&[Value::Str("a".into()), Value::Str("b".into())]),
            Value::Null
        );
    }

    /// Three examples that pin lower's behavior: a normal string,
    /// an empty string (must stay empty, not crash), and a Unicode
    /// uppercase letter (must lowercase to its accented form, not
    /// drop the accent).
    #[test]
    fn lower_eval_truth_table() {
        assert_eq!(
            lower_eval(&[Value::Str("Hello".into())]),
            Value::Str("hello".into())
        );
        // Empty string → empty string
        assert_eq!(lower_eval(&[Value::Str("".into())]), Value::Str("".into()));
        // Unicode: ÉLLO → éllo
        assert_eq!(
            lower_eval(&[Value::Str("ÉLLO".into())]),
            Value::Str("éllo".into())
        );
    }

    /// lower(null) → null. A real string must NOT give back null.
    #[test]
    fn lower_null_rule_matches_doc() {
        assert_eq!(lower_eval(&[Value::Null]), Value::Null);
        assert_ne!(lower_eval(&[Value::Str("a".into())]), Value::Null);
    }

    /// lower's type rule: takes a string, gives back a string. Passing
    /// a non-string (like an int) is rejected at register time.
    #[test]
    fn lower_infer_typecheck() {
        assert_eq!(
            str_to_str(&[InferredType::Known(FieldType::Str)]),
            Ok(InferredType::Known(FieldType::Str))
        );
        assert!(str_to_str(&[InferredType::Known(FieldType::I64)]).is_err());
    }

    // ── length ────────────────────────────────────────────────────────────────

    /// length takes one string. Other arg counts give back Null.
    #[test]
    fn length_arity_wrong_count_errors() {
        assert_eq!(length_eval(&[]), Value::Null);
        assert_eq!(
            length_eval(&[Value::Str("a".into()), Value::Str("b".into())]),
            Value::Null
        );
    }

    /// Pins what "length" actually counts: USER-VISIBLE characters, not
    /// underlying bytes. "héllo" looks like 5 characters to a user even
    /// though it takes 6 bytes to store (the é needs 2 bytes). An emoji
    /// counts as 1 character. This matches Python's `len()` so users
    /// don't get surprises crossing the SDK boundary.
    #[test]
    fn length_eval_truth_table() {
        assert_eq!(length_eval(&[Value::Str("hello".into())]), Value::I64(5));
        assert_eq!(length_eval(&[Value::Str("".into())]), Value::I64(0));
        // "héllo" = 5 codepoints, 6 bytes (é = 2 UTF-8 bytes)
        assert_eq!(length_eval(&[Value::Str("héllo".into())]), Value::I64(5));
        // Surrogate-pair emoji counts as 1 codepoint
        assert_eq!(length_eval(&[Value::Str("👋".into())]), Value::I64(1));
    }

    /// length(null) → null. A real string must NOT give back null.
    #[test]
    fn length_null_rule_matches_doc() {
        assert_eq!(length_eval(&[Value::Null]), Value::Null);
        assert_ne!(length_eval(&[Value::Str("a".into())]), Value::Null);
    }

    /// length's type rule: takes a string, gives back an int. Non-string
    /// input rejected at register time.
    #[test]
    fn length_infer_typecheck() {
        assert_eq!(
            str_to_i64(&[InferredType::Known(FieldType::Str)]),
            Ok(InferredType::Known(FieldType::I64))
        );
        assert!(str_to_i64(&[InferredType::Known(FieldType::I64)]).is_err());
    }

    // ── contains ──────────────────────────────────────────────────────────────

    /// contains takes exactly two strings (haystack, needle). Other arg
    /// counts give back Null.
    #[test]
    fn contains_arity_wrong_count_errors() {
        assert_eq!(contains_eval(&[]), Value::Null);
        assert_eq!(contains_eval(&[Value::Str("hello".into())]), Value::Null);
        assert_eq!(
            contains_eval(&[
                Value::Str("hello".into()),
                Value::Str("ell".into()),
                Value::Str("extra".into()),
            ]),
            Value::Null
        );
    }

    /// Four cases that pin contains: a match → true, a non-match →
    /// false, empty needle (matches anything, including empty haystack),
    /// and empty haystack with non-empty needle → false. These four
    /// catch the most common off-by-one bugs people write.
    #[test]
    fn contains_eval_truth_table() {
        assert_eq!(
            contains_eval(&[Value::Str("hello".into()), Value::Str("ell".into())]),
            Value::Bool(true)
        );
        assert_eq!(
            contains_eval(&[Value::Str("hello".into()), Value::Str("xyz".into())]),
            Value::Bool(false)
        );
        // Empty needle matches everything
        assert_eq!(
            contains_eval(&[Value::Str("hello".into()), Value::Str("".into())]),
            Value::Bool(true)
        );
        // Empty haystack: only empty needle matches
        assert_eq!(
            contains_eval(&[Value::Str("".into()), Value::Str("a".into())]),
            Value::Bool(false)
        );
    }

    /// If EITHER arg is null, contains gives back null — NOT false.
    /// This matters: `where contains(email, "@")` on rows where email
    /// is missing must skip the row, not include it as if it didn't
    /// match.
    #[test]
    fn contains_null_rule_matches_doc() {
        assert_eq!(
            contains_eval(&[Value::Null, Value::Str("x".into())]),
            Value::Null
        );
        assert_eq!(
            contains_eval(&[Value::Str("x".into()), Value::Null]),
            Value::Null
        );
        assert_ne!(
            contains_eval(&[Value::Str("hello".into()), Value::Str("ell".into())]),
            Value::Null
        );
    }

    /// contains's type rule: takes two strings, gives back a bool. A
    /// non-string arg is rejected at register time.
    #[test]
    fn contains_infer_typecheck() {
        let args = [
            InferredType::Known(FieldType::Str),
            InferredType::Known(FieldType::Str),
        ];
        assert_eq!(
            string_search_to_bool(&args),
            Ok(InferredType::Known(FieldType::Bool))
        );
        let bad = [
            InferredType::Known(FieldType::I64),
            InferredType::Known(FieldType::Str),
        ];
        assert!(string_search_to_bool(&bad).is_err());
    }

    // ── starts_with ───────────────────────────────────────────────────────────

    /// starts_with takes exactly two strings. Other arg counts give Null.
    #[test]
    fn starts_with_arity_wrong_count_errors() {
        assert_eq!(starts_with_eval(&[]), Value::Null);
        assert_eq!(starts_with_eval(&[Value::Str("a".into())]), Value::Null);
    }

    /// Three cases: a URL that starts with https:// → true; a similar
    /// URL with only http:// → false (catches the easy mistake of
    /// matching a prefix that's ALSO a prefix of something else);
    /// empty needle always matches.
    #[test]
    fn starts_with_eval_truth_table() {
        assert_eq!(
            starts_with_eval(&[
                Value::Str("https://example.com".into()),
                Value::Str("https://".into())
            ]),
            Value::Bool(true)
        );
        assert_eq!(
            starts_with_eval(&[
                Value::Str("http://example.com".into()),
                Value::Str("https://".into())
            ]),
            Value::Bool(false)
        );
        // Empty needle always true
        assert_eq!(
            starts_with_eval(&[Value::Str("hello".into()), Value::Str("".into())]),
            Value::Bool(true)
        );
    }

    /// Same null rule as contains: ANY null → null (not false). A real
    /// match must NOT give back null.
    #[test]
    fn starts_with_null_rule_matches_doc() {
        assert_eq!(
            starts_with_eval(&[Value::Null, Value::Str("x".into())]),
            Value::Null
        );
        assert_eq!(
            starts_with_eval(&[Value::Str("x".into()), Value::Null]),
            Value::Null
        );
        assert_ne!(
            starts_with_eval(&[Value::Str("hello".into()), Value::Str("hel".into())]),
            Value::Null
        );
    }

    /// Same type rule as contains (shares the helper). Re-checked here
    /// so a future change to the helper that breaks starts_with shows
    /// up against this builtin's name, not contains's.
    #[test]
    fn starts_with_infer_typecheck() {
        let args = [
            InferredType::Known(FieldType::Str),
            InferredType::Known(FieldType::Str),
        ];
        assert_eq!(
            string_search_to_bool(&args),
            Ok(InferredType::Known(FieldType::Bool))
        );
    }

    // ── ends_with ─────────────────────────────────────────────────────────────

    /// ends_with takes exactly two strings. Other arg counts give Null.
    #[test]
    fn ends_with_arity_wrong_count_errors() {
        assert_eq!(ends_with_eval(&[]), Value::Null);
        assert_eq!(ends_with_eval(&[Value::Str("a".into())]), Value::Null);
    }

    /// Three cases: a filename ending in .png → true; same filename
    /// asked about .jpg → false; empty needle always matches. Common
    /// file-extension pattern.
    #[test]
    fn ends_with_eval_truth_table() {
        assert_eq!(
            ends_with_eval(&[Value::Str("file.png".into()), Value::Str(".png".into())]),
            Value::Bool(true)
        );
        assert_eq!(
            ends_with_eval(&[Value::Str("file.png".into()), Value::Str(".jpg".into())]),
            Value::Bool(false)
        );
        assert_eq!(
            ends_with_eval(&[Value::Str("hello".into()), Value::Str("".into())]),
            Value::Bool(true)
        );
    }

    /// Same null rule as contains/starts_with: ANY null → null. Real
    /// match must NOT give back null.
    #[test]
    fn ends_with_null_rule_matches_doc() {
        assert_eq!(
            ends_with_eval(&[Value::Null, Value::Str("x".into())]),
            Value::Null
        );
        assert_eq!(
            ends_with_eval(&[Value::Str("x".into()), Value::Null]),
            Value::Null
        );
        assert_ne!(
            ends_with_eval(&[Value::Str("file.png".into()), Value::Str(".png".into())]),
            Value::Null
        );
    }

    /// Same type rule as contains/starts_with (shared helper). Pinned
    /// here too so a regression names ends_with directly.
    #[test]
    fn ends_with_infer_typecheck() {
        let args = [
            InferredType::Known(FieldType::Str),
            InferredType::Known(FieldType::Str),
        ];
        assert_eq!(
            string_search_to_bool(&args),
            Ok(InferredType::Known(FieldType::Bool))
        );
    }

    // ── replace ───────────────────────────────────────────────────────────────

    /// replace takes exactly three strings (haystack, old, new). Any
    /// other count gives back Null.
    #[test]
    fn replace_arity_wrong_count_errors() {
        assert_eq!(replace_eval(&[]), Value::Null);
        assert_eq!(replace_eval(&[Value::Str("a".into())]), Value::Null);
        assert_eq!(
            replace_eval(&[Value::Str("a".into()), Value::Str("b".into())]),
            Value::Null
        );
        // 4 args
        assert_eq!(
            replace_eval(&[
                Value::Str("a".into()),
                Value::Str("b".into()),
                Value::Str("c".into()),
                Value::Str("d".into()),
            ]),
            Value::Null
        );
    }

    /// Three cases: a normal replace (one occurrence); a no-match input
    /// (returns the original unchanged, doesn't crash); and a multi-
    /// occurrence input where the impl must replace EVERY match, not
    /// just the first. The last case is the common "I forgot the
    /// global flag" bug.
    #[test]
    fn replace_eval_truth_table() {
        assert_eq!(
            replace_eval(&[
                Value::Str("hello world".into()),
                Value::Str("world".into()),
                Value::Str("rust".into()),
            ]),
            Value::Str("hello rust".into())
        );
        // No match: unchanged
        assert_eq!(
            replace_eval(&[
                Value::Str("hello".into()),
                Value::Str("xyz".into()),
                Value::Str("abc".into()),
            ]),
            Value::Str("hello".into())
        );
        // All occurrences replaced
        assert_eq!(
            replace_eval(&[
                Value::Str("aaa".into()),
                Value::Str("a".into()),
                Value::Str("bb".into()),
            ]),
            Value::Str("bbbbbb".into())
        );
    }

    /// If ANY of the three args is null, replace gives back null. One
    /// test for each position so we know no slot is accidentally
    /// special-cased.
    #[test]
    fn replace_null_rule_matches_doc() {
        assert_eq!(
            replace_eval(&[Value::Null, Value::Str("a".into()), Value::Str("b".into())]),
            Value::Null
        );
        assert_eq!(
            replace_eval(&[Value::Str("a".into()), Value::Null, Value::Str("b".into())]),
            Value::Null
        );
        assert_eq!(
            replace_eval(&[Value::Str("a".into()), Value::Str("b".into()), Value::Null,]),
            Value::Null
        );
        assert_ne!(
            replace_eval(&[
                Value::Str("a".into()),
                Value::Str("a".into()),
                Value::Str("b".into()),
            ]),
            Value::Null
        );
    }

    /// replace's type rule: takes three strings, gives back a string.
    /// A non-string arg is rejected at register time. This is the only
    /// string builtin whose infer doesn't fit a shared helper — it
    /// needs three string args and the helpers cap at two.
    #[test]
    fn replace_infer_typecheck() {
        let args = [
            InferredType::Known(FieldType::Str),
            InferredType::Known(FieldType::Str),
            InferredType::Known(FieldType::Str),
        ];
        assert_eq!(
            replace_infer(&args),
            Ok(InferredType::Known(FieldType::Str))
        );
        // Non-Str arg → TypeMismatch
        let bad = [
            InferredType::Known(FieldType::I64),
            InferredType::Known(FieldType::Str),
            InferredType::Known(FieldType::Str),
        ];
        assert!(replace_infer(&bad).is_err());
    }
}
