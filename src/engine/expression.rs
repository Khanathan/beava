//! Expression parser (winnow Pratt) and evaluator.
//!
//! Expressions are parsed at pipeline registration time into an AST (`Expr`),
//! then evaluated at event time by walking the AST with an `EvalContext`.
//! This keeps Python out of the hot path.

use serde::{Deserialize, Serialize};
use winnow::ascii::{digit1, space0};
use winnow::combinator::{alt, delimited, expression, opt, preceded, separated, Infix, Prefix};
use winnow::error::ContextError;
use winnow::prelude::*;
use winnow::token::{literal, take_while};

use crate::error::TallyError;

type PResult<T> = winnow::Result<T>;

/// AST node for parsed expressions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expr {
    Literal(f64),
    StringLit(String),
    FieldAccess(FieldRef),
    BinaryOp {
        op: BinOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnOp,
        operand: Box<Expr>,
    },
    FnCall {
        name: String,
        args: Vec<Expr>,
    },
}

/// Field reference types for expression field access.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FieldRef {
    /// Bare field name: "tx_count_30m"
    Local(String),
    /// Qualified: "Transactions.tx_count_30m"
    Qualified(String, String),
    /// Event field: "_event.amount"
    Event(String),
}

/// Binary operators.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Gt,
    Lt,
    Gte,
    Lte,
    Eq,
    Neq,
    And,
    Or,
}

/// Unary operators.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum UnOp {
    Not,
    Neg,
}

// ---------------------------------------------------------------------------
// winnow Pratt parser implementation
// ---------------------------------------------------------------------------

/// Check if a character is valid as an identifier continuation (alphanumeric or _).
fn is_ident_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// Parse an identifier: [a-zA-Z_][a-zA-Z0-9_]*
fn parse_ident(input: &mut &str) -> PResult<String> {
    let first = take_while(1, |c: char| c.is_alphabetic() || c == '_').parse_next(input)?;
    let rest = take_while(0.., |c: char| c.is_alphanumeric() || c == '_').parse_next(input)?;
    Ok(format!("{}{}", first, rest))
}

/// Parse a number literal (integer or float) into Expr::Literal.
fn parse_number(input: &mut &str) -> PResult<Expr> {
    let int_part = digit1.parse_next(input)?;
    let frac_part = opt(preceded(literal("."), digit1)).parse_next(input)?;
    let s = match frac_part {
        Some(frac) => format!("{}.{}", int_part, frac),
        None => format!("{}.0", int_part),
    };
    // unwrap is safe: digit1 + optional ".digit1" always produces a valid f64
    let val: f64 = s.parse().unwrap();
    Ok(Expr::Literal(val))
}

/// Parse a single-quoted string literal: 'hello'
fn parse_string_lit(input: &mut &str) -> PResult<Expr> {
    literal("'").parse_next(input)?;
    let content = take_while(0.., |c: char| c != '\'').parse_next(input)?;
    literal("'").parse_next(input)?;
    Ok(Expr::StringLit(content.to_string()))
}

/// Parse a function call: name(arg1, arg2, ...)
/// Must be tried BEFORE parse_field_ref since identifiers overlap.
fn parse_fn_call(input: &mut &str) -> PResult<Expr> {
    let name = parse_ident.parse_next(input)?;
    space0.parse_next(input)?;
    literal("(").parse_next(input)?;
    space0.parse_next(input)?;
    let args: Vec<Expr> = opt(separated(1.., parse_full_expr, (space0, literal(","), space0)))
        .parse_next(input)?
        .unwrap_or_default();
    space0.parse_next(input)?;
    literal(")").parse_next(input)?;
    Ok(Expr::FnCall { name, args })
}

/// Keywords that cannot be bare field names (they are operators, not identifiers).
const KEYWORDS: &[&str] = &["and", "or", "not"];

/// Parse a field reference: _event.field, Stream.field, or bare field_name.
/// Rejects standalone keywords (and, or, not) so the Pratt prefix/infix parsers
/// can handle them instead.
fn parse_field_ref(input: &mut &str) -> PResult<Expr> {
    let checkpoint = input.checkpoint();
    let first = parse_ident.parse_next(input)?;
    // If the identifier is a keyword AND not followed by '.', reject it so the
    // Pratt parser can try the prefix/infix branches instead.
    if KEYWORDS.contains(&first.as_str()) {
        // Check if followed by dot (qualified access like `not_a_keyword.field` won't match
        // here because `not_a_keyword` != "not"). But `not.field` would be weird --
        // we still reject bare keywords without a dot.
        if !input.starts_with('.') {
            input.reset(&checkpoint);
            return Err(ContextError::new());
        }
    }
    if let Some(_dot) = opt(literal(".")).parse_next(input)? {
        let second = parse_ident.parse_next(input)?;
        if first == "_event" {
            Ok(Expr::FieldAccess(FieldRef::Event(second)))
        } else {
            Ok(Expr::FieldAccess(FieldRef::Qualified(first, second)))
        }
    } else {
        Ok(Expr::FieldAccess(FieldRef::Local(first)))
    }
}

/// Parse a parenthesized sub-expression: (expr)
fn parse_paren(input: &mut &str) -> PResult<Expr> {
    delimited(
        (literal("("), space0),
        parse_full_expr,
        (space0, literal(")")),
    )
    .parse_next(input)
}

/// Parse an operand (atom) for the Pratt parser.
fn parse_operand(input: &mut &str) -> PResult<Expr> {
    preceded(
        space0,
        alt((
            parse_number,
            parse_string_lit,
            parse_fn_call,
            parse_field_ref,
            parse_paren,
        )),
    )
    .parse_next(input)
}

/// Match a keyword only if followed by a non-identifier character (Pitfall 5).
/// Prevents "and_count" from being parsed as keyword "and" + "_count".
fn keyword<'a>(kw: &'static str) -> impl Parser<&'a str, &'a str, ContextError> {
    move |input: &mut &'a str| {
        let matched = literal(kw).parse_next(input)?;
        // Peek at the next char -- if it's an identifier continuation, backtrack.
        if let Some(c) = input.chars().next() {
            if is_ident_char(c) {
                return Err(ContextError::new());
            }
        }
        Ok(matched)
    }
}

// Infix fold fn type alias for readability.
type InfixFoldFn = fn(&mut &str, Expr, Expr) -> Result<Expr, ContextError>;
type PrefixFoldFn = fn(&mut &str, Expr) -> Result<Expr, ContextError>;

// --- Infix fold functions (fn pointers as required by winnow Prefix/Infix) ---
fn fold_or(_: &mut &str, a: Expr, b: Expr) -> Result<Expr, ContextError> {
    Ok(Expr::BinaryOp { op: BinOp::Or, left: Box::new(a), right: Box::new(b) })
}
fn fold_and(_: &mut &str, a: Expr, b: Expr) -> Result<Expr, ContextError> {
    Ok(Expr::BinaryOp { op: BinOp::And, left: Box::new(a), right: Box::new(b) })
}
fn fold_gt(_: &mut &str, a: Expr, b: Expr) -> Result<Expr, ContextError> {
    Ok(Expr::BinaryOp { op: BinOp::Gt, left: Box::new(a), right: Box::new(b) })
}
fn fold_lt(_: &mut &str, a: Expr, b: Expr) -> Result<Expr, ContextError> {
    Ok(Expr::BinaryOp { op: BinOp::Lt, left: Box::new(a), right: Box::new(b) })
}
fn fold_gte(_: &mut &str, a: Expr, b: Expr) -> Result<Expr, ContextError> {
    Ok(Expr::BinaryOp { op: BinOp::Gte, left: Box::new(a), right: Box::new(b) })
}
fn fold_lte(_: &mut &str, a: Expr, b: Expr) -> Result<Expr, ContextError> {
    Ok(Expr::BinaryOp { op: BinOp::Lte, left: Box::new(a), right: Box::new(b) })
}
fn fold_eq(_: &mut &str, a: Expr, b: Expr) -> Result<Expr, ContextError> {
    Ok(Expr::BinaryOp { op: BinOp::Eq, left: Box::new(a), right: Box::new(b) })
}
fn fold_neq(_: &mut &str, a: Expr, b: Expr) -> Result<Expr, ContextError> {
    Ok(Expr::BinaryOp { op: BinOp::Neq, left: Box::new(a), right: Box::new(b) })
}
fn fold_add(_: &mut &str, a: Expr, b: Expr) -> Result<Expr, ContextError> {
    Ok(Expr::BinaryOp { op: BinOp::Add, left: Box::new(a), right: Box::new(b) })
}
fn fold_sub(_: &mut &str, a: Expr, b: Expr) -> Result<Expr, ContextError> {
    Ok(Expr::BinaryOp { op: BinOp::Sub, left: Box::new(a), right: Box::new(b) })
}
fn fold_mul(_: &mut &str, a: Expr, b: Expr) -> Result<Expr, ContextError> {
    Ok(Expr::BinaryOp { op: BinOp::Mul, left: Box::new(a), right: Box::new(b) })
}
fn fold_div(_: &mut &str, a: Expr, b: Expr) -> Result<Expr, ContextError> {
    Ok(Expr::BinaryOp { op: BinOp::Div, left: Box::new(a), right: Box::new(b) })
}

// --- Prefix fold functions ---
fn fold_not(_: &mut &str, a: Expr) -> Result<Expr, ContextError> {
    Ok(Expr::UnaryOp { op: UnOp::Not, operand: Box::new(a) })
}
fn fold_neg(_: &mut &str, a: Expr) -> Result<Expr, ContextError> {
    Ok(Expr::UnaryOp { op: UnOp::Neg, operand: Box::new(a) })
}

/// Parse infix operators with binding power.
/// Split into two alt groups (max ~9 per tuple) to stay within winnow's Alt impl bounds.
fn parse_infix_op<'a>(input: &mut &'a str) -> PResult<Infix<&'a str, Expr, ContextError>> {
    preceded(
        space0,
        alt((
            // Group 1: comparison + boolean operators
            alt((
                literal(">=").value(Infix::Left(3, fold_gte as InfixFoldFn)),
                literal("<=").value(Infix::Left(3, fold_lte as InfixFoldFn)),
                literal("==").value(Infix::Left(3, fold_eq as InfixFoldFn)),
                literal("!=").value(Infix::Left(3, fold_neq as InfixFoldFn)),
                literal(">").value(Infix::Left(3, fold_gt as InfixFoldFn)),
                literal("<").value(Infix::Left(3, fold_lt as InfixFoldFn)),
                keyword("or").value(Infix::Left(1, fold_or as InfixFoldFn)),
                keyword("and").value(Infix::Left(2, fold_and as InfixFoldFn)),
            )),
            // Group 2: arithmetic operators
            alt((
                literal("+").value(Infix::Left(5, fold_add as InfixFoldFn)),
                literal("-").value(Infix::Left(5, fold_sub as InfixFoldFn)),
                literal("*").value(Infix::Left(7, fold_mul as InfixFoldFn)),
                literal("/").value(Infix::Left(7, fold_div as InfixFoldFn)),
            )),
        )),
    )
    .parse_next(input)
}

/// Parse prefix operators with binding power.
fn parse_prefix_op<'a>(input: &mut &'a str) -> PResult<Prefix<&'a str, Expr, ContextError>> {
    preceded(
        space0,
        alt((
            keyword("not").value(Prefix(10, fold_not as PrefixFoldFn)),
            literal("-").value(Prefix(10, fold_neg as PrefixFoldFn)),
        )),
    )
    .parse_next(input)
}

/// The full expression parser using winnow's Pratt combinator.
fn parse_full_expr(input: &mut &str) -> PResult<Expr> {
    expression(parse_operand)
        .prefix(parse_prefix_op)
        .infix(parse_infix_op)
        .parse_next(input)
}

// ---------------------------------------------------------------------------
// Expression evaluator
// ---------------------------------------------------------------------------

use crate::types::FeatureValue;

/// Context for evaluating expressions. Provides feature values and event data.
pub struct EvalContext<'a> {
    /// Features for the current entity (from all streams).
    /// Key format: "feature_name" for local, "StreamName.feature_name" for qualified.
    pub features: &'a ahash::AHashMap<String, FeatureValue>,
    /// The current event JSON (for _event.field access).
    pub event: Option<&'a serde_json::Value>,
}

impl<'a> EvalContext<'a> {
    /// Resolve a field reference to its current value.
    pub fn resolve_field(&self, field_ref: &FieldRef) -> FeatureValue {
        match field_ref {
            FieldRef::Local(name) => {
                self.features.get(name).cloned().unwrap_or(FeatureValue::Missing)
            }
            FieldRef::Qualified(stream, field) => {
                let key = format!("{}.{}", stream, field);
                self.features.get(&key).cloned().unwrap_or(FeatureValue::Missing)
            }
            FieldRef::Event(field) => match self.event {
                Some(ev) => match ev.get(field) {
                    Some(serde_json::Value::Number(n)) => {
                        if let Some(i) = n.as_i64() {
                            FeatureValue::Int(i)
                        } else if let Some(f) = n.as_f64() {
                            FeatureValue::Float(f)
                        } else {
                            FeatureValue::Missing
                        }
                    }
                    Some(serde_json::Value::String(s)) => FeatureValue::String(s.clone()),
                    Some(serde_json::Value::Bool(b)) => FeatureValue::Int(if *b { 1 } else { 0 }),
                    _ => FeatureValue::Missing,
                },
                None => FeatureValue::Missing,
            },
        }
    }
}

/// Evaluate an expression AST against a context, returning a FeatureValue.
///
/// Called per-event for derive/where expressions. The AST is pre-parsed at
/// registration time, so this just walks the tree.
pub fn eval(expr: &Expr, ctx: &EvalContext) -> FeatureValue {
    match expr {
        Expr::Literal(f) => FeatureValue::Float(*f),
        Expr::StringLit(s) => FeatureValue::String(s.clone()),
        Expr::FieldAccess(field_ref) => ctx.resolve_field(field_ref),
        Expr::BinaryOp { op, left, right } => {
            let l = eval(left, ctx);
            let r = eval(right, ctx);
            eval_binary(*op, l, r)
        }
        Expr::UnaryOp { op, operand } => {
            let val = eval(operand, ctx);
            eval_unary(*op, val)
        }
        Expr::FnCall { name, args } => eval_fn_call(name, args, ctx),
    }
}

/// Guard: if f64 result is NaN or infinite, return Missing (defense-in-depth, ENG-08).
fn guard_float(val: f64) -> FeatureValue {
    if val.is_nan() || val.is_infinite() {
        FeatureValue::Missing
    } else {
        FeatureValue::Float(val)
    }
}

/// Evaluate a binary operation with Missing propagation (SQL NULL semantics).
fn eval_binary(op: BinOp, left: FeatureValue, right: FeatureValue) -> FeatureValue {
    // String equality/inequality: handled before Missing check for string-specific ops.
    if matches!(op, BinOp::Eq | BinOp::Neq) {
        // Allow String == String and String != String
        if let (FeatureValue::String(ref a), FeatureValue::String(ref b)) = (&left, &right) {
            return match op {
                BinOp::Eq => FeatureValue::Int(if a == b { 1 } else { 0 }),
                BinOp::Neq => FeatureValue::Int(if a != b { 1 } else { 0 }),
                _ => unreachable!(),
            };
        }
    }

    // Missing propagation: any Missing input -> Missing output (Pitfall 6).
    if left.is_missing() || right.is_missing() {
        return FeatureValue::Missing;
    }

    // String in arithmetic/comparison (except equality handled above) -> Missing.
    if matches!(left, FeatureValue::String(_)) || matches!(right, FeatureValue::String(_)) {
        return FeatureValue::Missing;
    }

    match op {
        // Arithmetic: Int + Int -> Int; if either Float -> Float.
        BinOp::Add => match (&left, &right) {
            (FeatureValue::Int(a), FeatureValue::Int(b)) => FeatureValue::Int(a + b),
            _ => guard_float(left.as_f64().unwrap() + right.as_f64().unwrap()),
        },
        BinOp::Sub => match (&left, &right) {
            (FeatureValue::Int(a), FeatureValue::Int(b)) => FeatureValue::Int(a - b),
            _ => guard_float(left.as_f64().unwrap() - right.as_f64().unwrap()),
        },
        BinOp::Mul => match (&left, &right) {
            (FeatureValue::Int(a), FeatureValue::Int(b)) => FeatureValue::Int(a * b),
            _ => guard_float(left.as_f64().unwrap() * right.as_f64().unwrap()),
        },
        BinOp::Div => {
            // Division by zero -> Missing (ENG-08).
            let r = right.as_f64().unwrap();
            if r == 0.0 {
                return FeatureValue::Missing;
            }
            // Division always promotes to Float.
            guard_float(left.as_f64().unwrap() / r)
        }

        // Comparison: returns Int(1) for true, Int(0) for false.
        BinOp::Gt => {
            let (a, b) = (left.as_f64().unwrap(), right.as_f64().unwrap());
            FeatureValue::Int(if a > b { 1 } else { 0 })
        }
        BinOp::Lt => {
            let (a, b) = (left.as_f64().unwrap(), right.as_f64().unwrap());
            FeatureValue::Int(if a < b { 1 } else { 0 })
        }
        BinOp::Gte => {
            let (a, b) = (left.as_f64().unwrap(), right.as_f64().unwrap());
            FeatureValue::Int(if a >= b { 1 } else { 0 })
        }
        BinOp::Lte => {
            let (a, b) = (left.as_f64().unwrap(), right.as_f64().unwrap());
            FeatureValue::Int(if a <= b { 1 } else { 0 })
        }
        BinOp::Eq => {
            // Numeric equality (string equality handled above).
            let (a, b) = (left.as_f64().unwrap(), right.as_f64().unwrap());
            FeatureValue::Int(if (a - b).abs() < f64::EPSILON { 1 } else { 0 })
        }
        BinOp::Neq => {
            let (a, b) = (left.as_f64().unwrap(), right.as_f64().unwrap());
            FeatureValue::Int(if (a - b).abs() >= f64::EPSILON { 1 } else { 0 })
        }

        // Boolean: and/or operate on Int(0)/Int(1). Missing -> Missing (Pitfall 6).
        BinOp::And => {
            let a = left.as_f64().unwrap();
            let b = right.as_f64().unwrap();
            FeatureValue::Int(if a != 0.0 && b != 0.0 { 1 } else { 0 })
        }
        BinOp::Or => {
            let a = left.as_f64().unwrap();
            let b = right.as_f64().unwrap();
            FeatureValue::Int(if a != 0.0 || b != 0.0 { 1 } else { 0 })
        }
    }
}

/// Evaluate a unary operation.
fn eval_unary(op: UnOp, val: FeatureValue) -> FeatureValue {
    if val.is_missing() {
        return FeatureValue::Missing;
    }
    match op {
        UnOp::Not => match &val {
            FeatureValue::Int(i) => FeatureValue::Int(if *i == 0 { 1 } else { 0 }),
            FeatureValue::Float(f) => FeatureValue::Int(if *f == 0.0 { 1 } else { 0 }),
            _ => FeatureValue::Missing,
        },
        UnOp::Neg => match &val {
            FeatureValue::Int(i) => FeatureValue::Int(-i),
            FeatureValue::Float(f) => FeatureValue::Float(-f),
            _ => FeatureValue::Missing,
        },
    }
}

/// Evaluate a builtin function call.
fn eval_fn_call(name: &str, args: &[Expr], ctx: &EvalContext) -> FeatureValue {
    match name {
        "abs" => {
            if args.len() != 1 {
                return FeatureValue::Missing;
            }
            let val = eval(&args[0], ctx);
            if val.is_missing() {
                return FeatureValue::Missing;
            }
            match val {
                FeatureValue::Int(i) => FeatureValue::Int(i.abs()),
                FeatureValue::Float(f) => FeatureValue::Float(f.abs()),
                _ => FeatureValue::Missing,
            }
        }
        "min" => {
            if args.len() != 2 {
                return FeatureValue::Missing;
            }
            let a = eval(&args[0], ctx);
            let b = eval(&args[1], ctx);
            if a.is_missing() || b.is_missing() {
                return FeatureValue::Missing;
            }
            let af = a.as_f64().unwrap();
            let bf = b.as_f64().unwrap();
            FeatureValue::Float(af.min(bf))
        }
        "max" => {
            if args.len() != 2 {
                return FeatureValue::Missing;
            }
            let a = eval(&args[0], ctx);
            let b = eval(&args[1], ctx);
            if a.is_missing() || b.is_missing() {
                return FeatureValue::Missing;
            }
            let af = a.as_f64().unwrap();
            let bf = b.as_f64().unwrap();
            FeatureValue::Float(af.max(bf))
        }
        "now" => {
            use std::time::{SystemTime, UNIX_EPOCH};
            let secs = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs_f64();
            FeatureValue::Float(secs)
        }
        _ => FeatureValue::Missing,
    }
}

/// Parse an expression string into an AST.
///
/// Called at pipeline registration time, not per-event.
pub fn parse_expr(input: &str) -> Result<Expr, TallyError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(TallyError::Parse("empty expression".into()));
    }
    let mut remaining = trimmed;
    let expr = parse_full_expr
        .parse_next(&mut remaining)
        .map_err(|e| TallyError::Parse(format!("failed to parse expression '{}': {}", input, e)))?;
    // Ensure the entire input was consumed.
    let leftover = remaining.trim();
    if !leftover.is_empty() {
        return Err(TallyError::Parse(format!(
            "unexpected trailing input '{}' in expression '{}'",
            leftover, input
        )));
    }
    Ok(expr)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ======================== Parser Tests ========================

    #[test]
    fn test_parse_number_literal_float() {
        let expr = parse_expr("42.5").unwrap();
        assert_eq!(expr, Expr::Literal(42.5));
    }

    #[test]
    fn test_parse_number_literal_integer() {
        let expr = parse_expr("42").unwrap();
        assert_eq!(expr, Expr::Literal(42.0));
    }

    #[test]
    fn test_parse_field_local() {
        let expr = parse_expr("field_name").unwrap();
        assert_eq!(
            expr,
            Expr::FieldAccess(FieldRef::Local("field_name".into()))
        );
    }

    #[test]
    fn test_parse_field_qualified() {
        let expr = parse_expr("Stream.field").unwrap();
        assert_eq!(
            expr,
            Expr::FieldAccess(FieldRef::Qualified("Stream".into(), "field".into()))
        );
    }

    #[test]
    fn test_parse_field_event() {
        let expr = parse_expr("_event.amount").unwrap();
        assert_eq!(
            expr,
            Expr::FieldAccess(FieldRef::Event("amount".into()))
        );
    }

    #[test]
    fn test_parse_binary_add() {
        let expr = parse_expr("a + b").unwrap();
        assert_eq!(
            expr,
            Expr::BinaryOp {
                op: BinOp::Add,
                left: Box::new(Expr::FieldAccess(FieldRef::Local("a".into()))),
                right: Box::new(Expr::FieldAccess(FieldRef::Local("b".into()))),
            }
        );
    }

    #[test]
    fn test_parse_precedence_mul_over_add() {
        // a + b * c  =>  Add(a, Mul(b, c))
        let expr = parse_expr("a + b * c").unwrap();
        assert_eq!(
            expr,
            Expr::BinaryOp {
                op: BinOp::Add,
                left: Box::new(Expr::FieldAccess(FieldRef::Local("a".into()))),
                right: Box::new(Expr::BinaryOp {
                    op: BinOp::Mul,
                    left: Box::new(Expr::FieldAccess(FieldRef::Local("b".into()))),
                    right: Box::new(Expr::FieldAccess(FieldRef::Local("c".into()))),
                }),
            }
        );
    }

    #[test]
    fn test_parse_boolean_with_comparison() {
        // a > 10 and b < 5
        let expr = parse_expr("a > 10 and b < 5").unwrap();
        assert_eq!(
            expr,
            Expr::BinaryOp {
                op: BinOp::And,
                left: Box::new(Expr::BinaryOp {
                    op: BinOp::Gt,
                    left: Box::new(Expr::FieldAccess(FieldRef::Local("a".into()))),
                    right: Box::new(Expr::Literal(10.0)),
                }),
                right: Box::new(Expr::BinaryOp {
                    op: BinOp::Lt,
                    left: Box::new(Expr::FieldAccess(FieldRef::Local("b".into()))),
                    right: Box::new(Expr::Literal(5.0)),
                }),
            }
        );
    }

    #[test]
    fn test_parse_parentheses() {
        // (a + b) * c  =>  Mul(Add(a, b), c)
        let expr = parse_expr("(a + b) * c").unwrap();
        assert_eq!(
            expr,
            Expr::BinaryOp {
                op: BinOp::Mul,
                left: Box::new(Expr::BinaryOp {
                    op: BinOp::Add,
                    left: Box::new(Expr::FieldAccess(FieldRef::Local("a".into()))),
                    right: Box::new(Expr::FieldAccess(FieldRef::Local("b".into()))),
                }),
                right: Box::new(Expr::FieldAccess(FieldRef::Local("c".into()))),
            }
        );
    }

    #[test]
    fn test_parse_unary_not() {
        let expr = parse_expr("not x").unwrap();
        assert_eq!(
            expr,
            Expr::UnaryOp {
                op: UnOp::Not,
                operand: Box::new(Expr::FieldAccess(FieldRef::Local("x".into()))),
            }
        );
    }

    #[test]
    fn test_parse_unary_neg() {
        let expr = parse_expr("-x").unwrap();
        assert_eq!(
            expr,
            Expr::UnaryOp {
                op: UnOp::Neg,
                operand: Box::new(Expr::FieldAccess(FieldRef::Local("x".into()))),
            }
        );
    }

    #[test]
    fn test_parse_fn_call_abs() {
        let expr = parse_expr("abs(x)").unwrap();
        assert_eq!(
            expr,
            Expr::FnCall {
                name: "abs".into(),
                args: vec![Expr::FieldAccess(FieldRef::Local("x".into()))],
            }
        );
    }

    #[test]
    fn test_parse_fn_call_min_two_args() {
        let expr = parse_expr("min(a, b)").unwrap();
        assert_eq!(
            expr,
            Expr::FnCall {
                name: "min".into(),
                args: vec![
                    Expr::FieldAccess(FieldRef::Local("a".into())),
                    Expr::FieldAccess(FieldRef::Local("b".into())),
                ],
            }
        );
    }

    #[test]
    fn test_parse_string_literal() {
        let expr = parse_expr("'hello'").unwrap();
        assert_eq!(expr, Expr::StringLit("hello".into()));
    }

    #[test]
    fn test_parse_keyword_prefix_field_and_count() {
        // "and_count" should be a field name, NOT keyword "and" + "_count"
        let expr = parse_expr("and_count").unwrap();
        assert_eq!(
            expr,
            Expr::FieldAccess(FieldRef::Local("and_count".into()))
        );
    }

    #[test]
    fn test_parse_keyword_prefix_field_not_fraud() {
        // "not_fraud" should be a field name, NOT keyword "not" + "_fraud"
        let expr = parse_expr("not_fraud").unwrap();
        assert_eq!(
            expr,
            Expr::FieldAccess(FieldRef::Local("not_fraud".into()))
        );
    }

    #[test]
    fn test_parse_empty_input_returns_error() {
        let result = parse_expr("");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_incomplete_expression_returns_error() {
        let result = parse_expr("a +");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_fn_call_now_no_args() {
        let expr = parse_expr("now()").unwrap();
        assert_eq!(
            expr,
            Expr::FnCall {
                name: "now".into(),
                args: vec![],
            }
        );
    }

    #[test]
    fn test_parse_fn_call_max_two_args() {
        let expr = parse_expr("max(a, b)").unwrap();
        assert_eq!(
            expr,
            Expr::FnCall {
                name: "max".into(),
                args: vec![
                    Expr::FieldAccess(FieldRef::Local("a".into())),
                    Expr::FieldAccess(FieldRef::Local("b".into())),
                ],
            }
        );
    }

    #[test]
    fn test_parse_all_comparison_ops() {
        for (op_str, op) in [
            (">=", BinOp::Gte),
            ("<=", BinOp::Lte),
            ("==", BinOp::Eq),
            ("!=", BinOp::Neq),
            (">", BinOp::Gt),
            ("<", BinOp::Lt),
        ] {
            let input = format!("a {} b", op_str);
            let expr = parse_expr(&input).unwrap();
            assert_eq!(
                expr,
                Expr::BinaryOp {
                    op,
                    left: Box::new(Expr::FieldAccess(FieldRef::Local("a".into()))),
                    right: Box::new(Expr::FieldAccess(FieldRef::Local("b".into()))),
                },
                "Failed for operator: {}",
                op_str,
            );
        }
    }

    #[test]
    fn test_parse_or_operator() {
        let expr = parse_expr("a or b").unwrap();
        assert_eq!(
            expr,
            Expr::BinaryOp {
                op: BinOp::Or,
                left: Box::new(Expr::FieldAccess(FieldRef::Local("a".into()))),
                right: Box::new(Expr::FieldAccess(FieldRef::Local("b".into()))),
            }
        );
    }

    #[test]
    fn test_parse_string_equality_in_expression() {
        // status == 'failed'
        let expr = parse_expr("status == 'failed'").unwrap();
        assert_eq!(
            expr,
            Expr::BinaryOp {
                op: BinOp::Eq,
                left: Box::new(Expr::FieldAccess(FieldRef::Local("status".into()))),
                right: Box::new(Expr::StringLit("failed".into())),
            }
        );
    }

    // ======================== Evaluator Tests ========================

    use crate::types::FeatureValue;

    /// Helper: parse + eval with given features.
    fn eval_with(input: &str, pairs: &[(&str, FeatureValue)]) -> FeatureValue {
        let expr = parse_expr(input).unwrap();
        let features: ahash::AHashMap<String, FeatureValue> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();
        let ctx = EvalContext {
            features: &features,
            event: None,
        };
        eval(&expr, &ctx)
    }

    #[test]
    fn test_eval_literal_float() {
        let result = eval_with("42.5", &[]);
        assert_eq!(result, FeatureValue::Float(42.5));
    }

    #[test]
    fn test_eval_string_literal() {
        let result = eval_with("'hello'", &[]);
        assert_eq!(result, FeatureValue::String("hello".into()));
    }

    #[test]
    fn test_eval_field_found() {
        let result = eval_with("tx_count", &[("tx_count", FeatureValue::Int(5))]);
        assert_eq!(result, FeatureValue::Int(5));
    }

    #[test]
    fn test_eval_field_missing() {
        let result = eval_with("unknown", &[]);
        assert_eq!(result, FeatureValue::Missing);
    }

    #[test]
    fn test_eval_add_int_int() {
        let result = eval_with("a + b", &[
            ("a", FeatureValue::Int(3)),
            ("b", FeatureValue::Int(4)),
        ]);
        assert_eq!(result, FeatureValue::Int(7));
    }

    #[test]
    fn test_eval_add_int_float() {
        let result = eval_with("a + b", &[
            ("a", FeatureValue::Int(3)),
            ("b", FeatureValue::Float(4.5)),
        ]);
        assert_eq!(result, FeatureValue::Float(7.5));
    }

    #[test]
    fn test_eval_sub_float() {
        let result = eval_with("a - b", &[
            ("a", FeatureValue::Float(10.0)),
            ("b", FeatureValue::Float(3.0)),
        ]);
        assert_eq!(result, FeatureValue::Float(7.0));
    }

    #[test]
    fn test_eval_mul_int_int() {
        let result = eval_with("a * b", &[
            ("a", FeatureValue::Int(2)),
            ("b", FeatureValue::Int(3)),
        ]);
        assert_eq!(result, FeatureValue::Int(6));
    }

    #[test]
    fn test_eval_div_float() {
        let result = eval_with("a / b", &[
            ("a", FeatureValue::Float(10.0)),
            ("b", FeatureValue::Float(3.0)),
        ]);
        match result {
            FeatureValue::Float(f) => assert!((f - 10.0 / 3.0).abs() < 1e-10),
            other => panic!("Expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_eval_div_by_zero_float_returns_missing() {
        let result = eval_with("a / b", &[
            ("a", FeatureValue::Float(10.0)),
            ("b", FeatureValue::Float(0.0)),
        ]);
        assert_eq!(result, FeatureValue::Missing);
    }

    #[test]
    fn test_eval_div_by_zero_int_returns_missing() {
        let result = eval_with("a / b", &[
            ("a", FeatureValue::Int(10)),
            ("b", FeatureValue::Int(0)),
        ]);
        assert_eq!(result, FeatureValue::Missing);
    }

    #[test]
    fn test_eval_missing_propagation_in_arithmetic() {
        let result = eval_with("a + b", &[
            ("a", FeatureValue::Missing),
            ("b", FeatureValue::Int(5)),
        ]);
        assert_eq!(result, FeatureValue::Missing);
    }

    #[test]
    fn test_eval_gt_true() {
        let result = eval_with("a > 10", &[("a", FeatureValue::Int(15))]);
        assert_eq!(result, FeatureValue::Int(1));
    }

    #[test]
    fn test_eval_gt_false() {
        let result = eval_with("a > 10", &[("a", FeatureValue::Int(5))]);
        assert_eq!(result, FeatureValue::Int(0));
    }

    #[test]
    fn test_eval_string_eq_true() {
        let result = eval_with("a == 'US'", &[("a", FeatureValue::String("US".into()))]);
        assert_eq!(result, FeatureValue::Int(1));
    }

    #[test]
    fn test_eval_string_eq_false() {
        let result = eval_with("a == 'US'", &[("a", FeatureValue::String("UK".into()))]);
        assert_eq!(result, FeatureValue::Int(0));
    }

    #[test]
    fn test_eval_comparison_with_missing() {
        let result = eval_with("a > b", &[("a", FeatureValue::Missing)]);
        assert_eq!(result, FeatureValue::Missing);
    }

    #[test]
    fn test_eval_and_true_true() {
        let result = eval_with("a and b", &[
            ("a", FeatureValue::Int(1)),
            ("b", FeatureValue::Int(1)),
        ]);
        assert_eq!(result, FeatureValue::Int(1));
    }

    #[test]
    fn test_eval_and_true_false() {
        let result = eval_with("a and b", &[
            ("a", FeatureValue::Int(1)),
            ("b", FeatureValue::Int(0)),
        ]);
        assert_eq!(result, FeatureValue::Int(0));
    }

    #[test]
    fn test_eval_and_with_missing() {
        let result = eval_with("a and b", &[
            ("a", FeatureValue::Missing),
            ("b", FeatureValue::Int(1)),
        ]);
        assert_eq!(result, FeatureValue::Missing);
    }

    #[test]
    fn test_eval_not_zero() {
        let result = eval_with("not a", &[("a", FeatureValue::Int(0))]);
        assert_eq!(result, FeatureValue::Int(1));
    }

    #[test]
    fn test_eval_not_one() {
        let result = eval_with("not a", &[("a", FeatureValue::Int(1))]);
        assert_eq!(result, FeatureValue::Int(0));
    }

    #[test]
    fn test_eval_not_missing() {
        let result = eval_with("not a", &[("a", FeatureValue::Missing)]);
        assert_eq!(result, FeatureValue::Missing);
    }

    #[test]
    fn test_eval_abs() {
        let result = eval_with("abs(a)", &[("a", FeatureValue::Float(-5.0))]);
        assert_eq!(result, FeatureValue::Float(5.0));
    }

    #[test]
    fn test_eval_min_two_args() {
        let result = eval_with("min(a, b)", &[
            ("a", FeatureValue::Float(3.0)),
            ("b", FeatureValue::Float(7.0)),
        ]);
        assert_eq!(result, FeatureValue::Float(3.0));
    }

    #[test]
    fn test_eval_max_two_args() {
        let result = eval_with("max(a, b)", &[
            ("a", FeatureValue::Float(3.0)),
            ("b", FeatureValue::Float(7.0)),
        ]);
        assert_eq!(result, FeatureValue::Float(7.0));
    }

    #[test]
    fn test_eval_event_field() {
        let expr = parse_expr("_event.amount").unwrap();
        let features = ahash::AHashMap::new();
        let event = serde_json::json!({"amount": 50.0});
        let ctx = EvalContext {
            features: &features,
            event: Some(&event),
        };
        assert_eq!(eval(&expr, &ctx), FeatureValue::Float(50.0));
    }

    #[test]
    fn test_eval_qualified_field() {
        let result = eval_with("Stream.field", &[
            ("Stream.field", FeatureValue::Int(42)),
        ]);
        assert_eq!(result, FeatureValue::Int(42));
    }

    #[test]
    fn test_eval_string_plus_int_returns_missing() {
        let result = eval_with("a + b", &[
            ("a", FeatureValue::String("hello".into())),
            ("b", FeatureValue::Int(5)),
        ]);
        assert_eq!(result, FeatureValue::Missing);
    }

    #[test]
    fn test_eval_nan_returns_missing() {
        // f64::MAX + f64::MAX overflows to infinity
        let result = eval_with("a + b", &[
            ("a", FeatureValue::Float(f64::MAX)),
            ("b", FeatureValue::Float(f64::MAX)),
        ]);
        assert_eq!(result, FeatureValue::Missing);
    }

    #[test]
    fn test_eval_neg_unary() {
        let result = eval_with("-a", &[("a", FeatureValue::Float(5.0))]);
        assert_eq!(result, FeatureValue::Float(-5.0));
    }

    #[test]
    fn test_eval_neg_int() {
        let result = eval_with("-a", &[("a", FeatureValue::Int(3))]);
        assert_eq!(result, FeatureValue::Int(-3));
    }

    #[test]
    fn test_eval_now_returns_float() {
        let result = eval_with("now()", &[]);
        match result {
            FeatureValue::Float(f) => assert!(f > 1_000_000_000.0, "now() should return Unix timestamp"),
            other => panic!("Expected Float from now(), got {:?}", other),
        }
    }
}
