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
}
