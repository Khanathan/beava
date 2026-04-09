//! Expression parser (winnow Pratt) and evaluator.
//!
//! Expressions are parsed at pipeline registration time into an AST (`Expr`),
//! then evaluated at event time by walking the AST with an `EvalContext`.
//! This keeps Python out of the hot path.

use serde::{Deserialize, Serialize};

use crate::error::TallyError;

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

/// Parse an expression string into an AST.
///
/// Called at pipeline registration time, not per-event.
pub fn parse_expr(_input: &str) -> Result<Expr, TallyError> {
    Err(TallyError::Parse("not yet implemented".into()))
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
