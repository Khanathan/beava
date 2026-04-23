//! Recursive-descent parser for the canonical parenthesized expression grammar
//! emitted by `python/beava/_col.py::to_expr_string()`.
//!
//! # Grammar (locked — Phase 3 D-08; Phase 4 D-01)
//!
//! ```text
//! Expr      := OrExpr
//! OrExpr    := AndExpr ( 'or' AndExpr )*
//! AndExpr   := NotExpr ( 'and' NotExpr )*
//! NotExpr   := 'not' NotExpr | CmpExpr
//! CmpExpr   := AddExpr ( ('>'|'>='|'<'|'<='|'=='|'!=') AddExpr )?
//! AddExpr   := MulExpr ( ('+'|'-') MulExpr )*
//! MulExpr   := Atom ( ('*'|'/') Atom )*
//! Atom      := '(' Expr ')' | Call | Ident | Literal
//! Call      := Ident '(' ArgList ')'
//! ArgList   := Expr ( ',' Expr )* | ε
//! Literal   := Number | SingleQuotedString | 'true' | 'false' | 'null'
//! Ident     := [A-Za-z_][A-Za-z0-9_]* ( '.' [A-Za-z_][A-Za-z0-9_]* )?
//! ```
//!
//! # Post-parse AST normalization
//!
//! After parsing, two normalization passes run in order before `parse()` returns:
//!
//! **Pass A — cast bare-identifier normalization**: every `Call("cast", args)` whose
//! second argument parses as `Expr::Field { name }` is rewritten in-place to
//! `Expr::Literal(Literal::BareIdent(name))`. This lets `cast(amount, float)` flow
//! through the normal expression pipeline (identifiers parse as Fields) and
//! produce the `BareIdent` that the evaluator expects.
//!
//! **Pass B — null-equality rewrite (SDK-COL-04 compatibility)**: every
//! `BinOp("==", e, Literal::Null)` and `BinOp("==", Literal::Null, e)` is rewritten
//! recursively (bottom-up) to `Call("isnull", [e])`. Rationale: the Python SDK's
//! `.isnull()` emits `(x == null)`, but `CONTEXT.md §D-04` requires that
//! `BinOp("==")` in the evaluator stays strict-null (null == anything → Null).
//! Folding the rewrite here means `eval.rs` never needs to special-case `== null`,
//! and `.isnull()` always produces a deterministic `Bool`.
//!
//! **`!=` with null on either side is NOT rewritten** — only `==`. Users who want
//! "is not null" should write `(not isnull(x))`.

use std::collections::BTreeSet;

// ─── Public types ─────────────────────────────────────────────────────────────

/// Byte-offset span into the source string (`start..end`, exclusive end).
/// `col` for error reporting is `start + 1` (1-indexed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

/// Parse error with 1-indexed column number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    /// 1-indexed byte offset of the offending character.
    pub col: usize,
    /// Human-readable reason; always prefixed `"col N: ..."`.
    pub reason: String,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.reason)
    }
}

/// Scalar literal variants.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    /// Bare identifier used as the type argument to `cast(x, float)`.
    /// Treated as a literal (not a field reference) by `referenced_fields`.
    BareIdent(String),
}

/// The expression AST produced by `parse()`.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Column reference, e.g. `amount` or `Stream.x`.
    Field { name: String, span: Span },
    /// Scalar constant.
    Literal(Literal, Span),
    /// Binary operation, e.g. `(a > b)`, `(a and b)`.
    BinOp {
        op: String,
        left: Box<Expr>,
        right: Box<Expr>,
        span: Span,
    },
    /// Unary operation — currently only `not`.
    UnaryOp {
        op: String,
        operand: Box<Expr>,
        span: Span,
    },
    /// Function call, e.g. `cast(x, float)`, `isnull(x)`.
    Call {
        fn_name: String,
        args: Vec<Expr>,
        span: Span,
    },
}

impl Expr {
    /// Returns the byte-offset span of this node in the original source.
    pub fn span(&self) -> Span {
        todo!()
    }

    /// Collects every `Expr::Field` name referenced anywhere in this subtree
    /// into a sorted set. Literal values (including `BareIdent`) are excluded.
    pub fn referenced_fields(&self) -> BTreeSet<String> {
        todo!()
    }
}

// ─── Entry point ──────────────────────────────────────────────────────────────

/// Parse `source` into an `Expr` AST.
///
/// Returns `Err(ParseError)` on any syntax error. The error's `col` is
/// 1-indexed into `source`; the `reason` string is human-readable and always
/// begins with `"col N: "`.
///
/// Post-parse normalization passes are applied before returning:
/// - Pass A: cast's second-arg `Field` rewritten to `Literal::BareIdent`.
/// - Pass B: `(x == null)` / `(null == x)` rewritten to `Call("isnull", [x])`.
pub fn parse(_source: &str) -> Result<Expr, ParseError> {
    todo!()
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Helpers (some used only in green-phase tests; allow dead_code for red commit) ──

    #[allow(dead_code)]
    fn field(name: &str, start: usize, end: usize) -> Expr {
        Expr::Field {
            name: name.to_string(),
            span: Span { start, end },
        }
    }

    #[allow(dead_code)]
    fn lit_int(n: i64, start: usize, end: usize) -> Expr {
        Expr::Literal(Literal::Int(n), Span { start, end })
    }

    #[allow(dead_code)]
    fn lit_float(f: f64, start: usize, end: usize) -> Expr {
        Expr::Literal(Literal::Float(f), Span { start, end })
    }

    #[allow(dead_code)]
    fn lit_null(start: usize, end: usize) -> Expr {
        Expr::Literal(Literal::Null, Span { start, end })
    }

    #[allow(dead_code)]
    fn lit_bool(b: bool, start: usize, end: usize) -> Expr {
        Expr::Literal(Literal::Bool(b), Span { start, end })
    }

    #[allow(dead_code)]
    fn lit_str(s: &str, start: usize, end: usize) -> Expr {
        Expr::Literal(Literal::Str(s.to_string()), Span { start, end })
    }

    #[allow(dead_code)]
    fn binop(op: &str, left: Expr, right: Expr, start: usize, end: usize) -> Expr {
        Expr::BinOp {
            op: op.to_string(),
            left: Box::new(left),
            right: Box::new(right),
            span: Span { start, end },
        }
    }

    #[allow(dead_code)]
    fn unaryop(op: &str, operand: Expr, start: usize, end: usize) -> Expr {
        Expr::UnaryOp {
            op: op.to_string(),
            operand: Box::new(operand),
            span: Span { start, end },
        }
    }

    #[allow(dead_code)]
    fn call(fn_name: &str, args: Vec<Expr>, start: usize, end: usize) -> Expr {
        Expr::Call {
            fn_name: fn_name.to_string(),
            args,
            span: Span { start, end },
        }
    }

    // ── Test 1: bare field ────────────────────────────────────────────────────

    #[test]
    fn parse_bare_field() {
        let expr = parse("amount").expect("should parse bare field");
        assert!(
            matches!(&expr, Expr::Field { name, span } if name == "amount" && span.start == 0 && span.end == 6),
            "got {expr:?}"
        );
    }

    // ── Test 2: qualified field ───────────────────────────────────────────────

    #[test]
    fn parse_qualified_field() {
        let expr = parse("Stream.x").expect("should parse qualified field");
        assert!(
            matches!(&expr, Expr::Field { name, .. } if name == "Stream.x"),
            "got {expr:?}"
        );
    }

    // ── Test 3: null literal ──────────────────────────────────────────────────

    #[test]
    fn parse_null_literal() {
        let expr = parse("null").expect("should parse null literal");
        assert!(
            matches!(&expr, Expr::Literal(Literal::Null, _)),
            "got {expr:?}"
        );
    }

    // ── Test 4: bool literals ─────────────────────────────────────────────────

    #[test]
    fn parse_bool_literals() {
        let t = parse("true").expect("should parse true");
        assert!(
            matches!(&t, Expr::Literal(Literal::Bool(true), _)),
            "got {t:?}"
        );
        let f = parse("false").expect("should parse false");
        assert!(
            matches!(&f, Expr::Literal(Literal::Bool(false), _)),
            "got {f:?}"
        );
    }

    // ── Test 5: integer literals (positive + negative) ────────────────────────

    #[test]
    fn parse_integer_literal() {
        // Positive integer
        let pos = parse("42").expect("should parse 42");
        assert!(
            matches!(&pos, Expr::Literal(Literal::Int(42), _)),
            "got {pos:?}"
        );
        // Negative literal — the Python SDK emits `repr(-7)` which is `-7`
        let neg = parse("-7").expect("should parse -7");
        assert!(
            matches!(&neg, Expr::Literal(Literal::Int(-7), _)),
            "got {neg:?}"
        );
        // Parenthesized subtraction (also accepted)
        let sub = parse("(0 - 7)").expect("should parse (0 - 7)");
        assert!(
            matches!(&sub, Expr::BinOp { op, .. } if op == "-"),
            "got {sub:?}"
        );
    }

    // ── Test 6: float literals (positive + negative) ──────────────────────────

    #[test]
    fn parse_float_literal() {
        // Use 2.5 (exact in binary float; not an approximation of a named constant)
        let pos = parse("2.5").expect("should parse 2.5");
        assert!(
            matches!(&pos, Expr::Literal(Literal::Float(f), _) if *f == 2.5_f64),
            "got {pos:?}"
        );
        let neg = parse("-0.5").expect("should parse -0.5");
        assert!(
            matches!(&neg, Expr::Literal(Literal::Float(f), _) if *f == -0.5_f64),
            "got {neg:?}"
        );
    }

    // ── Test 7: string literals with escapes ──────────────────────────────────

    #[test]
    fn parse_string_literal_with_escapes() {
        // Plain string
        let plain = parse("'hello world'").expect("should parse plain string");
        assert!(
            matches!(&plain, Expr::Literal(Literal::Str(s), _) if s == "hello world"),
            "got {plain:?}"
        );
        // Escaped apostrophe: `'it\'s'` (10 bytes: ' i t \ ' s ')
        let apos = parse(r"'it\'s'").expect("should parse escaped apostrophe");
        assert!(
            matches!(&apos, Expr::Literal(Literal::Str(s), _) if s == "it's"),
            "got {apos:?}"
        );
        // Escaped backslash: `'a\\b'` → "a\b" (one backslash)
        let bs = parse(r"'a\\b'").expect("should parse escaped backslash");
        assert!(
            matches!(&bs, Expr::Literal(Literal::Str(s), _) if s == r"a\b"),
            "got {bs:?}"
        );
    }

    // ── Test 8: binary comparison ─────────────────────────────────────────────

    #[test]
    fn parse_binary_comparison() {
        let expr = parse("(amount > 100)").expect("should parse binary comparison");
        match &expr {
            Expr::BinOp {
                op,
                left,
                right,
                span,
            } => {
                assert_eq!(op, ">");
                assert!(matches!(left.as_ref(), Expr::Field { name, .. } if name == "amount"));
                assert!(matches!(
                    right.as_ref(),
                    Expr::Literal(Literal::Int(100), _)
                ));
                assert_eq!(span.start, 0);
                assert_eq!(span.end, 14);
            }
            _ => panic!("expected BinOp, got {expr:?}"),
        }
    }

    // ── Test 9: binary arithmetic ─────────────────────────────────────────────

    #[test]
    fn parse_binary_arithmetic() {
        let expr = parse("(a + b)").expect("should parse binary arithmetic");
        assert!(
            matches!(&expr, Expr::BinOp { op, left, right, .. }
                if op == "+" &&
                   matches!(left.as_ref(), Expr::Field { name, .. } if name == "a") &&
                   matches!(right.as_ref(), Expr::Field { name, .. } if name == "b")),
            "got {expr:?}"
        );
    }

    // ── Test 10: nested and/or ────────────────────────────────────────────────

    #[test]
    fn parse_nested_and_or() {
        let expr = parse("((a > 0) and (b < 5))").expect("should parse nested and/or");
        match &expr {
            Expr::BinOp {
                op, left, right, ..
            } => {
                assert_eq!(op, "and");
                assert!(matches!(left.as_ref(), Expr::BinOp { op, .. } if op == ">"));
                assert!(matches!(right.as_ref(), Expr::BinOp { op, .. } if op == "<"));
            }
            _ => panic!("expected BinOp('and'), got {expr:?}"),
        }
    }

    // ── Test 11: unary not ────────────────────────────────────────────────────

    #[test]
    fn parse_unary_not() {
        let expr = parse("(not flag)").expect("should parse unary not");
        match &expr {
            Expr::UnaryOp { op, operand, .. } => {
                assert_eq!(op, "not");
                assert!(matches!(operand.as_ref(), Expr::Field { name, .. } if name == "flag"));
            }
            _ => panic!("expected UnaryOp('not'), got {expr:?}"),
        }
    }

    // ── Test 12: call cast ────────────────────────────────────────────────────

    #[test]
    fn parse_call_cast() {
        // cast(amount, float) → after Pass A normalization, second arg is BareIdent
        let expr = parse("cast(amount, float)").expect("should parse cast call");
        match &expr {
            Expr::Call { fn_name, args, .. } => {
                assert_eq!(fn_name, "cast");
                assert_eq!(args.len(), 2);
                assert!(matches!(&args[0], Expr::Field { name, .. } if name == "amount"));
                assert!(
                    matches!(&args[1], Expr::Literal(Literal::BareIdent(n), _) if n == "float"),
                    "expected BareIdent('float'), got {:?}",
                    &args[1]
                );
            }
            _ => panic!("expected Call('cast'), got {expr:?}"),
        }
    }

    // ── Test 13: call isnull ──────────────────────────────────────────────────

    #[test]
    fn parse_call_isnull() {
        let expr = parse("isnull(amount)").expect("should parse isnull call");
        match &expr {
            Expr::Call { fn_name, args, .. } => {
                assert_eq!(fn_name, "isnull");
                assert_eq!(args.len(), 1);
                assert!(matches!(&args[0], Expr::Field { name, .. } if name == "amount"));
            }
            _ => panic!("expected Call('isnull'), got {expr:?}"),
        }
    }

    // ── Test 14: empty arglist ────────────────────────────────────────────────

    #[test]
    fn parse_empty_arglist() {
        // Grammar allows empty arglists; semantics (unknown builtins) are 04-03's concern.
        let expr = parse("noop()").expect("should parse empty arglist");
        match &expr {
            Expr::Call { fn_name, args, .. } => {
                assert_eq!(fn_name, "noop");
                assert!(args.is_empty(), "expected no args, got {args:?}");
            }
            _ => panic!("expected Call('noop'), got {expr:?}"),
        }
    }

    // ── Test 15: rejects empty input ──────────────────────────────────────────

    #[test]
    fn parse_rejects_empty_input() {
        let err = parse("").expect_err("empty input should fail");
        assert_eq!(err.col, 1, "error col should be 1 for empty input");
        let reason_lc = err.reason.to_lowercase();
        assert!(
            reason_lc.contains("expected") || reason_lc.contains("empty"),
            "reason should mention 'expected' or 'empty', got: {:?}",
            err.reason
        );
    }

    // ── Test 16: rejects trailing tokens ─────────────────────────────────────

    #[test]
    fn parse_rejects_trailing_tokens() {
        // "a b" — 'b' starts at byte 2 (0-indexed) → col 3 (1-indexed)
        let err = parse("a b").expect_err("trailing token should fail");
        assert_eq!(err.col, 3, "col should point at 'b' (byte 2 + 1)");
        let reason_lc = err.reason.to_lowercase();
        assert!(
            reason_lc.contains("unexpected") || reason_lc.contains("trailing"),
            "reason should mention unexpected/trailing, got: {:?}",
            err.reason
        );
    }

    // ── Test 17: rejects unclosed paren ───────────────────────────────────────

    #[test]
    fn parse_rejects_unclosed_paren() {
        let err = parse("(amount > 100").expect_err("unclosed paren should fail");
        // col should be at or past the end of input; reason must mention ')'
        assert!(
            err.reason.contains("')'") || err.reason.contains(")"),
            "reason should mention ')', got: {:?}",
            err.reason
        );
    }

    // ── Test 18: rejects bare binop ───────────────────────────────────────────

    #[test]
    fn parse_rejects_bare_binop() {
        // "a + b" — SDK always parenthesizes; bare binary ops are rejected.
        // '+' starts at byte 2 → col 3.
        let err = parse("a + b").expect_err("bare binary op should fail");
        // The '+' at byte 2 produces trailing content after parsing 'a'
        assert!(
            err.col >= 3,
            "error col should be ≥ 3 (at the operator), got {}",
            err.col
        );
    }

    // ── Test 19: rejects unknown trailing char ────────────────────────────────

    #[test]
    fn parse_rejects_unknown_trailing_char() {
        // "a $ b" — '$' is at byte 2 → col 3
        let err = parse("a $ b").expect_err("unknown char should fail");
        assert_eq!(err.col, 3, "col should point at '$'");
        assert!(
            err.reason.contains('$'),
            "reason should mention '$', got: {:?}",
            err.reason
        );
    }

    // ── Test 20: referenced_fields collects all fields ────────────────────────

    #[test]
    fn referenced_fields_collects_all() {
        let expr =
            parse("((amount > 100) and (isnull(merchant_id)))").expect("should parse compound");
        let fields = expr.referenced_fields();
        let expected: BTreeSet<String> = ["amount", "merchant_id"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(
            fields, expected,
            "expected {{amount, merchant_id}}, got {fields:?}"
        );
    }

    // ── Test 21: span points inside outer expr ────────────────────────────────

    #[test]
    fn span_points_inside_outer_expr() {
        // "((a > 0) and (b < 5))"
        //  0123456789012345678901
        //  ^       ^    ^    ^  ^
        //  0       8    13   17 21
        // Inner left BinOp "(a > 0)" spans bytes 1..8
        // Outer BinOp spans bytes 0..21
        let expr = parse("((a > 0) and (b < 5))").expect("should parse");
        match &expr {
            Expr::BinOp { span, left, .. } => {
                assert_eq!(span.start, 0, "outer span.start");
                assert_eq!(span.end, 21, "outer span.end");
                match left.as_ref() {
                    Expr::BinOp {
                        span: inner_span, ..
                    } => {
                        assert_eq!(inner_span.start, 1, "inner left span.start");
                        assert_eq!(inner_span.end, 8, "inner left span.end");
                    }
                    _ => panic!("expected inner BinOp for left, got {left:?}"),
                }
            }
            _ => panic!("expected outer BinOp, got {expr:?}"),
        }
    }

    // ── Test 22: (x == null) → isnull(x) (right-side null) ───────────────────

    #[test]
    fn parse_equal_null_rewrites_to_isnull_call_right() {
        let expr = parse("(x == null)").expect("should parse (x == null)");
        assert!(
            matches!(&expr, Expr::Call { fn_name, .. } if fn_name == "isnull"),
            "expected Call('isnull', ...) after rewrite, got {expr:?}"
        );
        match &expr {
            Expr::Call { fn_name, args, .. } => {
                assert_eq!(fn_name, "isnull");
                assert_eq!(args.len(), 1);
                assert!(
                    matches!(&args[0], Expr::Field { name, .. } if name == "x"),
                    "expected Field('x'), got {:?}",
                    &args[0]
                );
            }
            _ => unreachable!(),
        }
    }

    // ── Test 23: (null == x) → isnull(x) (left-side null, commutative) ───────

    #[test]
    fn parse_equal_null_rewrites_to_isnull_call_left() {
        let expr = parse("(null == x)").expect("should parse (null == x)");
        assert!(
            matches!(&expr, Expr::Call { fn_name, .. } if fn_name == "isnull"),
            "expected Call('isnull', ...) after commutative rewrite, got {expr:?}"
        );
        match &expr {
            Expr::Call { fn_name, args, .. } => {
                assert_eq!(fn_name, "isnull");
                assert_eq!(args.len(), 1);
                assert!(
                    matches!(&args[0], Expr::Field { name, .. } if name == "x"),
                    "expected Field('x'), got {:?}",
                    &args[0]
                );
            }
            _ => unreachable!(),
        }
    }

    // ── Test 24: dotted field path preserved through rewrite ──────────────────

    #[test]
    fn parse_equal_null_rewrite_preserves_field_path() {
        let expr = parse("(Stream.field == null)").expect("should parse dotted field with null");
        assert!(
            matches!(&expr, Expr::Call { fn_name, .. } if fn_name == "isnull"),
            "expected isnull call, got {expr:?}"
        );
        match &expr {
            Expr::Call { args, .. } => {
                assert!(
                    matches!(&args[0], Expr::Field { name, .. } if name == "Stream.field"),
                    "expected Field('Stream.field'), got {:?}",
                    &args[0]
                );
            }
            _ => unreachable!(),
        }
    }

    // ── Test 25: sub-expression preserved through rewrite ─────────────────────

    #[test]
    fn parse_equal_null_rewrite_with_nested_expr() {
        // ((amount + 1) == null) → isnull(BinOp("+", Field("amount"), Int(1)))
        let expr = parse("((amount + 1) == null)").expect("should parse nested expr with null");
        assert!(
            matches!(&expr, Expr::Call { fn_name, .. } if fn_name == "isnull"),
            "expected isnull call, got {expr:?}"
        );
        match &expr {
            Expr::Call { args, .. } => {
                assert!(
                    matches!(&args[0], Expr::BinOp { op, .. } if op == "+"),
                    "expected BinOp('+') as isnull arg, got {:?}",
                    &args[0]
                );
            }
            _ => unreachable!(),
        }
    }

    // ── Test 26: rewrite applies recursively inside and/or ────────────────────

    #[test]
    fn parse_equal_null_inside_and_or_rewrites() {
        // ((amount == null) and (merchant_id == 'X'))
        // → BinOp("and", Call("isnull", [Field("amount")]), BinOp("==", Field("merchant_id"), Str("X")))
        let expr = parse("((amount == null) and (merchant_id == 'X'))")
            .expect("should parse compound with null");
        match &expr {
            Expr::BinOp {
                op, left, right, ..
            } => {
                assert_eq!(op, "and");
                assert!(
                    matches!(left.as_ref(), Expr::Call { fn_name, .. } if fn_name == "isnull"),
                    "left should be isnull call, got {left:?}"
                );
                assert!(
                    matches!(right.as_ref(), Expr::BinOp { op, .. } if op == "=="),
                    "right should remain BinOp('=='), got {right:?}"
                );
            }
            _ => panic!("expected BinOp('and'), got {expr:?}"),
        }
    }

    // ── Test 27: != null is NOT rewritten ─────────────────────────────────────

    #[test]
    fn parse_not_equal_null_not_rewritten() {
        // (x != null) should remain as BinOp("!=", Field("x"), Literal::Null)
        let expr = parse("(x != null)").expect("should parse (x != null)");
        assert!(
            matches!(&expr, Expr::BinOp { op, right, .. }
                if op == "!=" && matches!(right.as_ref(), Expr::Literal(Literal::Null, _))),
            "expected BinOp('!=', _, Null) — != with null must NOT be rewritten, got {expr:?}"
        );
    }

    // ── Test 28: (null == null) → isnull(null) ────────────────────────────────

    #[test]
    fn parse_equal_null_literal_both_sides_rewrites_to_isnull_of_null() {
        // Degenerate: both sides null → isnull(null)
        let expr = parse("(null == null)").expect("should parse (null == null)");
        assert!(
            matches!(&expr, Expr::Call { fn_name, .. } if fn_name == "isnull"),
            "expected isnull call, got {expr:?}"
        );
        match &expr {
            Expr::Call { args, .. } => {
                assert_eq!(args.len(), 1);
                assert!(
                    matches!(&args[0], Expr::Literal(Literal::Null, _)),
                    "expected Literal::Null inside isnull, got {:?}",
                    &args[0]
                );
            }
            _ => unreachable!(),
        }
    }

    // ── Test 29: referenced_fields after null-equality rewrite ────────────────

    #[test]
    fn parse_equal_null_referenced_fields() {
        let expr = parse("(amount == null)").expect("should parse (amount == null)");
        let fields = expr.referenced_fields();
        let expected: BTreeSet<String> = ["amount"].iter().map(|s| s.to_string()).collect();
        assert_eq!(
            fields, expected,
            "rewrite must not drop field references; expected {{amount}}, got {fields:?}"
        );
    }

    // ── Test 30: proptest — SDK strings always parse ──────────────────────────

    use proptest::prelude::*;

    /// Mirror of Python's `_col.py` AST for proptest generation.
    #[derive(Debug, Clone)]
    enum SdkExpr {
        Field(String),
        LitNull,
        LitBool(bool),
        LitInt(i64),
        LitFloat(f64),
        LitStr(String),
        BinOp(String, Box<SdkExpr>, Box<SdkExpr>),
        UnaryNot(Box<SdkExpr>),
        CallIsnull(Box<SdkExpr>),
        CallCast(Box<SdkExpr>, String),
    }

    impl SdkExpr {
        fn to_expr_string(&self) -> String {
            match self {
                SdkExpr::Field(name) => name.clone(),
                SdkExpr::LitNull => "null".to_string(),
                SdkExpr::LitBool(b) => if *b { "true" } else { "false" }.to_string(),
                SdkExpr::LitInt(n) => n.to_string(),
                SdkExpr::LitFloat(f) => {
                    // Mimic Python repr() for floats — always includes decimal point
                    let s = format!("{f}");
                    if s.contains('.') || s.contains('e') {
                        s
                    } else {
                        format!("{s}.0")
                    }
                }
                SdkExpr::LitStr(s) => {
                    let escaped = s.replace('\\', "\\\\").replace('\'', "\\'");
                    format!("'{escaped}'")
                }
                SdkExpr::BinOp(op, l, r) => {
                    format!("({} {op} {})", l.to_expr_string(), r.to_expr_string())
                }
                SdkExpr::UnaryNot(e) => format!("(not {})", e.to_expr_string()),
                SdkExpr::CallIsnull(e) => format!("isnull({})", e.to_expr_string()),
                SdkExpr::CallCast(e, ty) => {
                    format!("cast({}, {ty})", e.to_expr_string())
                }
            }
        }
    }

    fn arb_field() -> impl Strategy<Value = SdkExpr> {
        prop_oneof![
            Just(SdkExpr::Field("a".to_string())),
            Just(SdkExpr::Field("b".to_string())),
            Just(SdkExpr::Field("amount".to_string())),
            Just(SdkExpr::Field("Stream.x".to_string())),
        ]
    }

    fn arb_literal() -> impl Strategy<Value = SdkExpr> {
        prop_oneof![
            Just(SdkExpr::LitNull),
            any::<bool>().prop_map(SdkExpr::LitBool),
            // Restrict to i32 range to avoid repr differences with very large i64
            any::<i32>().prop_map(|n| SdkExpr::LitInt(n as i64)),
            // Use well-behaved floats (no NaN/Inf)
            (-1000.0f64..1000.0f64).prop_map(SdkExpr::LitFloat),
            // Strings with printable ASCII only (no control chars that complicate escaping)
            "[a-zA-Z0-9 _/]*".prop_map(SdkExpr::LitStr),
        ]
    }

    fn arb_sdk_expr(depth: u32) -> impl Strategy<Value = SdkExpr> {
        let leaf = prop_oneof![arb_field(), arb_literal()];
        leaf.prop_recursive(depth, 64, 4, move |inner| {
            let bin_ops = vec![
                "+", "-", "*", "/", ">", ">=", "<", "<=", "==", "!=", "and", "or",
            ];
            prop_oneof![
                // BinOp: pick a random op
                (0usize..bin_ops.len(), inner.clone(), inner.clone()).prop_map(
                    move |(idx, l, r)| {
                        SdkExpr::BinOp(bin_ops[idx].to_string(), Box::new(l), Box::new(r))
                    }
                ),
                // UnaryNot
                inner.clone().prop_map(|e| SdkExpr::UnaryNot(Box::new(e))),
                // isnull call
                inner.clone().prop_map(|e| SdkExpr::CallIsnull(Box::new(e))),
                // cast call — type arg is one of the known cast types
                (
                    inner.clone(),
                    prop_oneof![Just("int"), Just("float"), Just("str"), Just("bool"),]
                )
                    .prop_map(|(e, ty)| SdkExpr::CallCast(Box::new(e), ty.to_string())),
            ]
        })
    }

    proptest! {
        #[test]
        fn proptest_sdk_strings_parse(sdk in arb_sdk_expr(4)) {
            let s = sdk.to_expr_string();
            let result = parse(&s);
            prop_assert!(
                result.is_ok(),
                "SDK-generated string failed to parse: {:?}\nError: {:?}",
                s,
                result.err()
            );
        }
    }
}
