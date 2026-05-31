//! Builtin function registry for the expression evaluator.
//!
//! # Design
//!
//! Builtins are a closed Rust enum `BuiltinFn` — one variant per builtin.
//! Adding a new builtin is one variant + arms in the five `match self`
//! methods (`name`, `from_name`, `arity`, `eval`, `infer`), plus an arm in
//! `eval_lazy` (lazy builtins return `Some`; eager ones join its explicit
//! `None` list). The compiler enforces exhaustiveness — no `_ =>` fallback
//! arm, so a missing handler is a hard compile error.
//!
//! # File layout (PR 3 BUILTINS split, RFC-001 §5.2)
//!
//! - This file (`mod.rs`) — `BuiltinFn` enum + six `match self` methods
//!   (the five above + `eval_lazy`, the short-circuit hook) + `isnull_eval`
//!   (small, polymorphic, no clear category) + `cast_eval` (called from
//!   `eval.rs`'s `Expr::Cast` arm, not via the enum).
//!
//! - `math.rs`, `string.rs`, `time.rs`, `cond.rs`, `hash.rs` — per-category
//!   `*_eval` free fns and one-off `*_infer` fns.
//! - `_inference.rs` — shared inference helpers, `InferError`, `TypeClass`.
//!
//! # Builtins reachable via the enum (PR 3 v0 set)
//!
//! Math: `log1p`, `clip` — bodies in `math.rs`.
//! Time: `hour_of_day` — body in `time.rs`.
//! Hash: `quadkey`, `hash_mod` — bodies in `hash.rs`.
//! String: `lower`, `length`, `contains`, `starts_with`, `ends_with`, `replace` — bodies in `string.rs`.
//! Cond: `isnull` — body inline in this file (polymorphic null check, no category);
//! `if_else` — lazy/short-circuit builtin, body in `cond.rs` (see `eval_lazy`).
//!
//! `cast(value, type)` is NOT a `BuiltinFn` variant — it's `Expr::Cast`,
//! a dedicated AST node (RFC-001 §5.1). The parser detects `cast(...)`
//! before reaching `BuiltinFn::from_name` and routes directly to
//! `Expr::Cast`; `cast_eval` is the `pub(crate)` free fn the evaluator's
//! `Expr::Cast` arm calls.
//!
//! # Cast policy decisions (CONTEXT.md §D-05)
//!
//! - **Arity check**: wrong argument count → `Null` (register-time catches; runtime is defensive).
//! - **Null input**: `cast(null, any)` → `Null` (all targets).
//! - **Unknown target type**: → `Null`.
//! - **f64 → i64**: `as i64` (truncate toward zero, Rust default). Documents the truncation choice.
//! - **str → int/float**: `str.parse::<i64/f64>()` — fails → `Null`, not panic.
//! - **Bytes**: no implicit bytes-to-str without an encoding spec → `Null`.

pub(crate) mod _inference;

// Per-category files — PR 3 BUILTINS split (RFC-001 §5.2). Each holds the
// `*_eval` free fns (and one-off `*_infer` fns) for its category. The
// `BuiltinFn` enum and its `match self` methods stay in this module.
mod cond;
mod hash;
mod math;
mod string;
mod time;

use crate::builtins::_inference::{numeric_to_f64, string_search_to_bool};
use crate::builtins::cond::{if_else_eval, if_else_infer, if_else_select_branch};
use crate::builtins::hash::{hash_mod_eval, hash_mod_infer, quadkey_eval, quadkey_infer};
use crate::builtins::math::{clip_eval, clip_infer, log1p_eval};
use crate::builtins::string::{
    contains_eval, ends_with_eval, length_eval, lower_eval, replace_eval, replace_infer,
    starts_with_eval,
};
use crate::builtins::time::{hour_of_day_eval, hour_of_day_infer};
use crate::expr::Expr;
use crate::row::Value;
use crate::schema_propagate::InferredType;
use _inference::{any_to_bool, str_to_i64, str_to_str, InferError};

// ─── Arity ────────────────────────────────────────────────────────────────────

/// Argument count constraint for a builtin function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arity {
    /// Exactly `n` arguments required.
    Fixed(usize),
    /// Any number of arguments (including zero).
    Variadic,
}

// ─── BuiltinFn ────────────────────────────────────────────────────────────────

/// Closed enum of builtin functions.
///
/// Each variant has arms in the `match self` methods below; the compiler
/// enforces exhaustiveness. To add a builtin: add a variant + one arm in each
/// of `name`, `from_name`, `arity`, `eval`, `infer`, and `eval_lazy` (eager
/// builtins join `eval_lazy`'s `None` list; lazy ones return `Some`).
///
/// `Copy + Hash + Eq` so callers can use `BuiltinFn` values freely
/// (hash-map keys, equality checks, copy across boundaries) without
/// borrow ceremony.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BuiltinFn {
    /// `isnull(value)` — always returns `Bool(true/false)`, never `Null`.
    IsNull,
    /// `quadkey(lat, lon, zoom)` — geo cell ID. `lat`/`lon` numeric;
    /// `zoom` typed as numeric at register time (runtime requires
    /// strict `I64` in `1..=24`, otherwise `Null`).
    Quadkey,
    Log1p,
    Clip,
    HourOfDay,
    HashMod,
    Lower,
    Length,
    Contains,
    StartsWith,
    EndsWith,
    Replace,
    /// `if_else(cond, then, else)` — the only **lazy** builtin: it
    /// short-circuits in `eval.rs::eval_depth` via `eval_lazy`, evaluating the
    /// condition and exactly one branch. `cond` must be `Bool`; the two
    /// branches must share a type. Body in `cond.rs`.
    IfElse,
    // Note: `cast` is NOT a variant here. It's `Expr::Cast`, a dedicated
    // AST node (RFC-001 §5.1). The parser detects `cast(...)` before
    // reaching `BuiltinFn::from_name` and routes directly to Expr::Cast.
    // `cast_eval` remains in this file as a pub(crate) free fn that the
    // evaluator's Expr::Cast arm calls.
}

impl BuiltinFn {
    /// Wire-format name. `const fn` because it's a pure lookup.
    pub const fn name(self) -> &'static str {
        match self {
            Self::IsNull => "isnull",
            Self::Quadkey => "quadkey",
            Self::Lower => "lower",
            Self::Log1p => "log1p",
            Self::Clip => "clip",
            Self::HourOfDay => "hour_of_day",
            Self::HashMod => "hash_mod",
            Self::Length => "length",
            Self::Contains => "contains",
            Self::StartsWith => "starts_with",
            Self::EndsWith => "ends_with",
            Self::Replace => "replace",
            Self::IfElse => "if_else",
        }
    }

    /// Parse a wire-format name. `None` for unknown names.
    ///
    /// Note: `from_name("cast")` returns `None` by design — `cast` is an
    /// `Expr::Cast` AST variant, not a `BuiltinFn`. The parser detects
    /// the `"cast"` token before reaching this function.
    pub fn from_name(s: &str) -> Option<Self> {
        match s {
            "isnull" => Some(Self::IsNull),
            "quadkey" => Some(Self::Quadkey),
            "lower" => Some(Self::Lower),
            "log1p" => Some(Self::Log1p),
            "clip" => Some(Self::Clip),
            "hour_of_day" => Some(Self::HourOfDay),
            "hash_mod" => Some(Self::HashMod),
            "length" => Some(Self::Length),
            "contains" => Some(Self::Contains),
            "starts_with" => Some(Self::StartsWith),
            "ends_with" => Some(Self::EndsWith),
            "replace" => Some(Self::Replace),
            "if_else" => Some(Self::IfElse),
            _ => None,
        }
    }

    /// Argument count constraint.
    pub const fn arity(self) -> Arity {
        match self {
            Self::IsNull => Arity::Fixed(1),
            Self::Quadkey => Arity::Fixed(3),
            Self::Lower => Arity::Fixed(1),
            Self::Log1p => Arity::Fixed(1),
            Self::Clip => Arity::Fixed(3),
            Self::HourOfDay => Arity::Fixed(1),
            Self::HashMod => Arity::Fixed(2),
            Self::Length => Arity::Fixed(1),
            Self::Contains => Arity::Fixed(2),
            Self::StartsWith => Arity::Fixed(2),
            Self::EndsWith => Arity::Fixed(2),
            Self::Replace => Arity::Fixed(3),
            Self::IfElse => Arity::Fixed(3),
        }
    }

    /// Per-event evaluator dispatch. Called after each arg has been
    /// evaluated to a `Value`.
    pub fn eval(self, args: &[Value]) -> Value {
        match self {
            Self::IsNull => isnull_eval(args),
            Self::Quadkey => quadkey_eval(args),
            Self::Lower => lower_eval(args),
            Self::Log1p => log1p_eval(args),
            Self::Clip => clip_eval(args),
            Self::HourOfDay => hour_of_day_eval(args),
            Self::HashMod => hash_mod_eval(args),
            Self::Length => length_eval(args),
            Self::Contains => contains_eval(args),
            Self::StartsWith => starts_with_eval(args),
            Self::EndsWith => ends_with_eval(args),
            Self::Replace => replace_eval(args),
            // Eager reference path. In production `if_else` is intercepted by
            // `eval_lazy` (short-circuit) before reaching here; this arm stays
            // as the eager fallback the closed enum requires. See `cond.rs`.
            Self::IfElse => if_else_eval(args),
        }
    }

    /// Lazy/short-circuit dispatch. Eager builtins return `None` — the caller
    /// (`eval.rs::eval_depth`) then evaluates all args and calls [`Self::eval`].
    /// Lazy builtins evaluate only the args they need via `eval_arg` and return
    /// `Some(result)`.
    ///
    /// `if_else` is the only lazy builtin today: it evaluates the condition,
    /// then exactly one branch. Selection is shared with the eager path via
    /// `cond::if_else_select_branch`, so the two cannot drift. The `eval_arg`
    /// closure is injected by the caller, so this module never depends on the
    /// evaluator (no module cycle); `impl Fn` is monomorphized — zero-cost.
    ///
    /// Eager builtins are listed explicitly (no `_ =>` fallback), matching the
    /// file's exhaustiveness convention: adding a builtin forces a conscious
    /// eager (`None`) vs lazy (`Some`) decision, which is correctness-relevant
    /// once a non-total builtin exists (see `cond::if_else_eval` flip-trigger).
    pub(crate) fn eval_lazy(
        self,
        args: &[Expr],
        eval_arg: impl Fn(&Expr) -> Value,
    ) -> Option<Value> {
        match self {
            Self::IfElse => {
                debug_assert_eq!(
                    args.len(),
                    3,
                    "if_else arity is Fixed(3), enforced at parse/register time"
                );
                let cond = eval_arg(&args[0]);
                Some(if_else_select_branch(&cond).map_or(Value::Null, |i| eval_arg(&args[i])))
            }
            _ => None,
        }
    }

    /// Register-time type inference.
    ///
    /// Single-arg signature: no `&[Expr]` parameter, because no
    /// value-shaped builtin needs AST access. (Cast was the only one
    /// that ever did, and it's an `Expr::Cast` variant now.)
    pub fn infer(self, arg_types: &[InferredType]) -> Result<InferredType, InferError> {
        match self {
            Self::IsNull => any_to_bool(arg_types),
            Self::Quadkey => quadkey_infer(arg_types),
            Self::Log1p => numeric_to_f64(arg_types),
            Self::Clip => clip_infer(arg_types),
            Self::HourOfDay => hour_of_day_infer(arg_types),
            Self::HashMod => hash_mod_infer(arg_types),
            Self::Lower => str_to_str(arg_types),
            Self::Length => str_to_i64(arg_types),
            Self::Contains => string_search_to_bool(arg_types),
            Self::StartsWith => string_search_to_bool(arg_types),
            Self::EndsWith => string_search_to_bool(arg_types),
            Self::Replace => replace_infer(arg_types),
            Self::IfElse => if_else_infer(arg_types),
        }
    }

    /// All variants. Used by the name↔variant round-trip test and any
    /// future iteration need (testing, doc generation).
    #[cfg(test)]
    pub const fn all() -> &'static [BuiltinFn] {
        &[
            Self::IsNull,
            Self::Quadkey,
            Self::Log1p,
            Self::Clip,
            Self::HourOfDay,
            Self::HashMod,
            Self::Lower,
            Self::Length,
            Self::Contains,
            Self::StartsWith,
            Self::EndsWith,
            Self::Replace,
            Self::IfElse,
        ]
    }
}

impl std::fmt::Display for BuiltinFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
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
///
/// # Cast conversion matrix (CONTEXT.md §D-05)
///
/// | Source     | "str"          | "int"             | "float"           | "bool"               |
/// |------------|----------------|-------------------|-------------------|----------------------|
/// | Null       | Null           | Null              | Null              | Null                 |
/// | Str        | unchanged      | parse or Null     | parse or Null     | "true"→T,"false"→F   |
/// | I64        | fmt            | unchanged         | as f64            | ≠0 → true            |
/// | F64        | fmt            | as i64 (trunc)    | unchanged         | ≠0.0&&!NaN→true      |
/// | Bool       | "true"/"false" | 1 / 0             | 1.0 / 0.0         | unchanged            |
/// | Bytes      | Null           | Null              | Null              | Null                 |
/// | Datetime   | i64.to_string  | I64(ms)           | F64(ms as f64)    | ms≠0→true            |
pub(crate) fn cast_eval(args: &[Value]) -> Value {
    // Arity guard: must be exactly 2 args.
    if args.len() != 2 {
        return Value::Null;
    }

    // Target type comes as Value::Str (evaluator converts BareIdent → Str).
    let target = match &args[1] {
        Value::Str(s) => s.as_str(),
        _ => return Value::Null,
    };

    // Null input → always Null regardless of target.
    if matches!(args[0], Value::Null) {
        return Value::Null;
    }

    match target {
        "str" => cast_to_str(&args[0]),
        "int" => cast_to_int(&args[0]),
        "float" => cast_to_float(&args[0]),
        "bool" => cast_to_bool(&args[0]),
        _ => Value::Null,
    }
}

fn cast_to_str(v: &Value) -> Value {
    match v {
        Value::Null => Value::Null,
        Value::Str(s) => Value::Str(s.clone()),
        Value::I64(n) => Value::Str(n.to_string().into()),
        Value::F64(f) => Value::Str(f.to_string().into()),
        Value::Bool(b) => Value::Str((if *b { "true" } else { "false" }).into()),
        Value::Bytes(_) => Value::Null, // no implicit bytes→str without encoding spec
        Value::Datetime(ms) => Value::Str(ms.to_string().into()),
        Value::Json(_) => Value::Null,
        Value::List(_) | Value::Map(_) => Value::Null,
    }
}

fn cast_to_int(v: &Value) -> Value {
    match v {
        Value::Null => Value::Null,
        Value::I64(n) => Value::I64(*n),
        Value::F64(f) => Value::I64(*f as i64), // truncate toward zero
        Value::Bool(b) => Value::I64(if *b { 1 } else { 0 }),
        Value::Str(s) => s.parse::<i64>().map(Value::I64).unwrap_or(Value::Null),
        Value::Bytes(_) => Value::Null,
        Value::Datetime(ms) => Value::I64(*ms),
        Value::Json(_) => Value::Null,
        Value::List(_) | Value::Map(_) => Value::Null,
    }
}

fn cast_to_float(v: &Value) -> Value {
    match v {
        Value::Null => Value::Null,
        Value::I64(n) => Value::F64(*n as f64),
        Value::F64(f) => Value::F64(*f),
        Value::Bool(b) => Value::F64(if *b { 1.0 } else { 0.0 }),
        Value::Str(s) => s.parse::<f64>().map(Value::F64).unwrap_or(Value::Null),
        Value::Bytes(_) => Value::Null,
        Value::Datetime(ms) => Value::F64(*ms as f64),
        Value::Json(_) => Value::Null,
        Value::List(_) | Value::Map(_) => Value::Null,
    }
}

fn cast_to_bool(v: &Value) -> Value {
    match v {
        Value::Null => Value::Null,
        Value::Bool(b) => Value::Bool(*b),
        Value::I64(n) => Value::Bool(*n != 0),
        Value::F64(f) => Value::Bool(*f != 0.0 && !f.is_nan()),
        Value::Str(s) => match s.as_str() {
            "true" => Value::Bool(true),
            "false" => Value::Bool(false),
            _ => Value::Null,
        },
        Value::Bytes(_) => Value::Null,
        Value::Datetime(ms) => Value::Bool(*ms != 0),
        Value::Json(_) => Value::Null,
        Value::List(_) | Value::Map(_) => Value::Null,
    }
}

// ─── isnull ───────────────────────────────────────────────────────────────────

/// Evaluate `isnull(value)`.
///
/// Always returns `Bool(true/false)` — never `Null`.
fn isnull_eval(args: &[Value]) -> Value {
    // Arity guard: must be exactly 1 arg (defensive; register-time catches).
    if args.len() != 1 {
        return Value::Null;
    }
    Value::Bool(matches!(args[0], Value::Null))
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::Value;
    use crate::schema::FieldType;
    use crate::schema_propagate::InferredType;

    // ── Lookup tests ──────────────────────────────────────────────────────────

    #[test]
    fn from_name_returns_none_for_cast() {
        // Cast is NOT a BuiltinFn variant — it's Expr::Cast. The parser
        // routes "cast" through a special path before reaching from_name.
        // If anyone calls from_name("cast") directly, they get None.
        assert!(BuiltinFn::from_name("cast").is_none());
    }

    #[test]
    fn from_name_returns_isnull() {
        let b = BuiltinFn::from_name("isnull").expect("isnull must be a BuiltinFn variant");
        assert_eq!(b, BuiltinFn::IsNull);
        assert_eq!(b.name(), "isnull");
        assert_eq!(b.arity(), Arity::Fixed(1));
    }

    #[test]
    fn from_name_returns_quadkey() {
        let b = BuiltinFn::from_name("quadkey").expect("quadkey must be a BuiltinFn variant");
        assert_eq!(b, BuiltinFn::Quadkey);
        assert_eq!(b.name(), "quadkey");
        assert_eq!(b.arity(), Arity::Fixed(3));
    }

    #[test]
    fn from_name_unknown_returns_none() {
        assert!(BuiltinFn::from_name("foo").is_none());
        assert!(BuiltinFn::from_name("").is_none());
        assert!(BuiltinFn::from_name("COUNT").is_none());
    }

    // ── Exhaustiveness guard for BuiltinFn::all() ─────────────────────────────

    #[test]
    fn all_is_exhaustive() {
        // The match below has no wildcard arm, so the compiler rejects a new
        // variant that isn't listed here. After updating the match, also add
        // the variant to `all()` and increment the expected count — if you
        // do one but not the other, the assertion catches it at test time.
        for &b in BuiltinFn::all() {
            let _ = match b {
                BuiltinFn::IsNull
                | BuiltinFn::Quadkey
                | BuiltinFn::Log1p
                | BuiltinFn::Clip
                | BuiltinFn::HourOfDay
                | BuiltinFn::HashMod
                | BuiltinFn::Lower
                | BuiltinFn::Length
                | BuiltinFn::Contains
                | BuiltinFn::StartsWith
                | BuiltinFn::EndsWith
                | BuiltinFn::Replace
                | BuiltinFn::IfElse => b,
            };
        }
        assert_eq!(
            BuiltinFn::all().len(),
            13,
            "update all() and this match/count when adding a variant"
        );
    }

    // ── Name ↔ variant round-trip (permanent guard against mirror drift) ─────

    #[test]
    fn name_from_name_roundtrip() {
        for &b in BuiltinFn::all() {
            assert_eq!(
                BuiltinFn::from_name(b.name()),
                Some(b),
                "round-trip failed for {b:?}: name() = {:?}, from_name returned different variant",
                b.name()
            );
        }
    }

    // ── Display ───────────────────────────────────────────────────────────────

    #[test]
    fn display_emits_wire_name() {
        assert_eq!(format!("{}", BuiltinFn::IsNull), "isnull");
        assert_eq!(format!("{}", BuiltinFn::Quadkey), "quadkey");
    }

    // ── BuiltinFn::eval dispatch ──────────────────────────────────────────────

    #[test]
    fn enum_eval_dispatches_to_underlying_fns() {
        // Spot-check that match-arm dispatch in BuiltinFn::eval reaches the
        // same fn that the per-fn tests below verify in detail. Cast no
        // longer goes through the enum (see Expr::Cast in eval.rs); the
        // pub(crate) cast_eval fn is tested in detail below.
        assert_eq!(BuiltinFn::IsNull.eval(&[Value::Null]), Value::Bool(true));
        let r = BuiltinFn::Quadkey.eval(&[Value::F64(40.0), Value::F64(-74.0), Value::I64(7)]);
        assert!(matches!(r, Value::I64(_)));
    }

    // ── BuiltinFn::infer dispatch ─────────────────────────────────────────────

    #[test]
    fn enum_infer_dispatches_to_helpers() {
        // IsNull → any_to_bool → Bool
        assert_eq!(
            BuiltinFn::IsNull.infer(&[InferredType::Known(FieldType::I64)]),
            Ok(InferredType::Known(FieldType::Bool))
        );
        // Quadkey → quadkey_infer → I64
        let quadkey_args = [
            InferredType::Known(FieldType::F64),
            InferredType::Known(FieldType::F64),
            InferredType::Known(FieldType::I64),
        ];
        assert_eq!(
            BuiltinFn::Quadkey.infer(&quadkey_args),
            Ok(InferredType::Known(FieldType::I64))
        );
    }

    // ── eval_lazy (short-circuit hook) ────────────────────────────────────────

    /// The defining short-circuit test: with a false condition, `if_else`
    /// must evaluate the condition and the else-branch, and **never** the
    /// then-branch. The injected closure records which args it's asked to
    /// evaluate, which is the only way to prove the untaken branch was
    /// skipped — a value-only assertion can't, because beava's eager path
    /// would discard the untaken branch's value and return the same result.
    #[test]
    fn eval_lazy_if_else_skips_untaken_branch() {
        use crate::expr::Span;
        use std::cell::RefCell;

        let field = |name: &str| Expr::Field {
            name: name.to_string(),
            span: Span { start: 0, end: 0 },
        };
        let args = [field("cond"), field("then"), field("else")];

        let evaluated = RefCell::new(Vec::<String>::new());
        let result = BuiltinFn::IfElse.eval_lazy(&args, |e| {
            let Expr::Field { name, .. } = e else {
                return Value::Null;
            };
            evaluated.borrow_mut().push(name.clone());
            match name.as_str() {
                "cond" => Value::Bool(false), // → else-branch wins
                "then" => Value::I64(99),
                "else" => Value::I64(7),
                _ => Value::Null,
            }
        });

        assert_eq!(result, Some(Value::I64(7)));
        let log = evaluated.borrow();
        assert!(
            log.contains(&"cond".to_string()),
            "condition must be evaluated"
        );
        assert!(
            log.contains(&"else".to_string()),
            "selected branch must be evaluated"
        );
        assert!(
            !log.contains(&"then".to_string()),
            "untaken (then) branch must NOT be evaluated — short-circuit broken; got {log:?}"
        );
    }

    /// Eager builtins opt out of the hook (return `None`) so the caller falls
    /// back to the collect-all-args + `eval` path.
    #[test]
    fn eval_lazy_eager_builtins_return_none() {
        let args: [Expr; 0] = [];
        assert!(BuiltinFn::IsNull
            .eval_lazy(&args, |_| Value::Null)
            .is_none());
        assert!(BuiltinFn::Log1p.eval_lazy(&args, |_| Value::Null).is_none());
        assert!(BuiltinFn::Replace
            .eval_lazy(&args, |_| Value::Null)
            .is_none());
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
            isnull_eval(&[Value::Str("hello".into())]),
            Value::Bool(false)
        );
        assert_eq!(
            isnull_eval(&[Value::Str(compact_str::CompactString::new(""))]),
            Value::Bool(false)
        );
    }

    // ── cast tests ────────────────────────────────────────────────────────────

    // cast(I64(7), Str("float")) → F64(7.0)
    #[test]
    fn cast_int_to_float_ok() {
        assert_eq!(
            cast_eval(&[Value::I64(7), Value::Str("float".into())]),
            Value::F64(7.0)
        );
    }

    // cast(Str("42"), Str("int")) → I64(42)
    #[test]
    fn cast_str_to_int_parses_numeric() {
        assert_eq!(
            cast_eval(&[Value::Str("42".into()), Value::Str("int".into())]),
            Value::I64(42)
        );
    }

    // cast(Str("abc"), Str("int")) → Null (parse failure)
    #[test]
    fn cast_str_to_int_nonnumeric_is_null() {
        assert_eq!(
            cast_eval(&[Value::Str("abc".into()), Value::Str("int".into())]),
            Value::Null
        );
    }

    // cast(Null, Str("int")) → Null
    #[test]
    fn cast_null_any_is_null() {
        assert_eq!(
            cast_eval(&[Value::Null, Value::Str("int".into())]),
            Value::Null
        );
        assert_eq!(
            cast_eval(&[Value::Null, Value::Str("str".into())]),
            Value::Null
        );
        assert_eq!(
            cast_eval(&[Value::Null, Value::Str("float".into())]),
            Value::Null
        );
        assert_eq!(
            cast_eval(&[Value::Null, Value::Str("bool".into())]),
            Value::Null
        );
    }

    // cast(I64(1), Str("blob")) → Null (unknown target type)
    #[test]
    fn cast_unknown_type_is_null() {
        assert_eq!(
            cast_eval(&[Value::I64(1), Value::Str("blob".into())]),
            Value::Null
        );
        assert_eq!(
            cast_eval(&[Value::I64(1), Value::Str("bytes".into())]),
            Value::Null
        );
    }

    // cast(F64(3.9), Str("int")) → I64(3)  (truncate toward zero, Rust `as i64`)
    #[test]
    fn cast_float_to_int_truncates() {
        // Truncation toward zero: 3.9 → 3, -3.9 → -3
        assert_eq!(
            cast_eval(&[Value::F64(3.9), Value::Str("int".into())]),
            Value::I64(3)
        );
        assert_eq!(
            cast_eval(&[Value::F64(-3.9), Value::Str("int".into())]),
            Value::I64(-3)
        );
    }

    // cast(Bool(true), Str("int")) → I64(1); cast(Bool(false), Str("int")) → I64(0)
    #[test]
    fn cast_bool_to_int() {
        assert_eq!(
            cast_eval(&[Value::Bool(true), Value::Str("int".into())]),
            Value::I64(1)
        );
        assert_eq!(
            cast_eval(&[Value::Bool(false), Value::Str("int".into())]),
            Value::I64(0)
        );
    }

    // cast(I64(42), Str("str")) → Str("42")
    #[test]
    fn cast_int_to_str() {
        assert_eq!(
            cast_eval(&[Value::I64(42), Value::Str("str".into())]),
            Value::Str("42".into())
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
                Value::Str("int".into()),
                Value::Str("extra".into())
            ]),
            Value::Null
        );
    }
}
