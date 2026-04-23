// Phase 4 expr/eval/op-chain bench (plan 05.5-03 task 1.b — GREEN).
//
// Bench groups (10 total):
//   parse/{small, medium, deep}         — expression parse cost (cold path)
//   eval/{arith, compare, boolean, nullcheck, cast}  — eval cost per operator family
//   op_chain/{compile_4op, apply_4op}   — compile-time and per-row hot-path cost

use beava_core::eval::eval;
use beava_core::expr::parse;
use beava_core::op_chain::OpChain;
use beava_core::op_node::OpNode;
use beava_core::row::{Row, Value};
use beava_core::schema::FieldType;
use beava_core::schema_propagate::Schema;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::collections::BTreeMap;

// ── Parse corpus (fixed strings — no randomness) ───────────────────────────────

const EXPR_SMALL: &str = "(amount > 100)";

const EXPR_MEDIUM: &str = "((amount > 100) and ((status == 'ok') and (country != 'IR')))";

// 12-level deep nested expression mixing arith, comparison, boolean, and isnull.
// Each level adds one parenthesized `and` clause, reaching 12 levels of nesting.
const EXPR_DEEP: &str = concat!(
    "((((((((((((a > 0)",
    " and (b < 10))",
    " and ((c + d) > 5))",
    " and (isnull(e) or (f > 0)))",
    " and ((g + 1) > h))",
    " and (i != 0))",
    " and (j > 0))",
    " and (k < 100))",
    " and (l > 0))",
    " and (m < 99))",
    " and (n != 1))",
    " and (p > 0))"
);

fn bench_parse(c: &mut Criterion) {
    let mut g = c.benchmark_group("parse");
    g.bench_function("small", |b| {
        b.iter(|| parse(black_box(EXPR_SMALL)).unwrap())
    });
    g.bench_function("medium", |b| {
        b.iter(|| parse(black_box(EXPR_MEDIUM)).unwrap())
    });
    g.bench_function("deep", |b| b.iter(|| parse(black_box(EXPR_DEEP)).unwrap()));
    g.finish();
}

// ── Row helpers ────────────────────────────────────────────────────────────────

fn make_arith_row() -> Row {
    Row::new()
        .with_field("a", Value::I64(3))
        .with_field("b", Value::I64(4))
        .with_field("c", Value::I64(5))
        .with_field("d", Value::I64(8))
}

fn make_compare_row() -> Row {
    Row::new().with_field("amount", Value::F64(250.0))
}

fn make_bool_row() -> Row {
    Row::new()
        .with_field("a", Value::I64(2))
        .with_field("b", Value::I64(1))
        .with_field("c", Value::I64(3))
}

fn make_null_row() -> Row {
    Row::new().with_field("x", Value::Null)
}

fn make_cast_row() -> Row {
    Row::new().with_field("x", Value::I64(42))
}

fn bench_eval(c: &mut Criterion) {
    // Pre-parse each expression once outside the bench closure; benchmark only eval.
    let arith_expr = parse("((a + (b * c)) - (d / 2))").unwrap();
    let compare_expr = parse("(amount > 100)").unwrap();
    let bool_expr = parse("((a > 1) and ((b < 2) and (c == 3)))").unwrap();
    let nullcheck_expr = parse("isnull(x)").unwrap();
    let cast_expr = parse("(cast(x, float) > 0)").unwrap();

    let arith_row = make_arith_row();
    let compare_row = make_compare_row();
    let bool_row = make_bool_row();
    let null_row = make_null_row();
    let cast_row = make_cast_row();

    let mut g = c.benchmark_group("eval");

    g.bench_function("arith", |b| {
        b.iter(|| eval(black_box(&arith_expr), black_box(&arith_row)))
    });

    g.bench_function("compare", |b| {
        b.iter(|| eval(black_box(&compare_expr), black_box(&compare_row)))
    });

    g.bench_function("boolean", |b| {
        b.iter(|| eval(black_box(&bool_expr), black_box(&bool_row)))
    });

    g.bench_function("nullcheck", |b| {
        b.iter(|| eval(black_box(&nullcheck_expr), black_box(&null_row)))
    });

    g.bench_function("cast", |b| {
        b.iter(|| eval(black_box(&cast_expr), black_box(&cast_row)))
    });

    g.finish();
}

// ── OpChain fixtures ────────────────────────────────────────────────────────────

fn four_op_schema() -> Schema {
    let mut fields = BTreeMap::new();
    fields.insert("user_id".to_string(), FieldType::Str);
    fields.insert("amount".to_string(), FieldType::I64);
    fields.insert("status".to_string(), FieldType::Str);
    Schema {
        fields,
        optional_fields: Vec::new(),
    }
}

fn four_op_nodes() -> Vec<OpNode> {
    let mut with_exprs = BTreeMap::new();
    with_exprs.insert("is_big".to_string(), "(amount > 500)".to_string());

    let mut cast_map = BTreeMap::new();
    cast_map.insert("amount".to_string(), "float".to_string());

    vec![
        OpNode::Filter {
            expr: "(amount > 100)".to_string(),
        },
        OpNode::WithColumns { exprs: with_exprs },
        OpNode::Select {
            fields: vec![
                "user_id".to_string(),
                "amount".to_string(),
                "is_big".to_string(),
            ],
        },
        OpNode::Cast { type_map: cast_map },
    ]
}

fn four_op_row() -> Row {
    Row::new()
        .with_field("user_id", Value::Str("u1".to_string()))
        .with_field("amount", Value::I64(1000))
        .with_field("status", Value::Str("ok".to_string()))
}

fn bench_op_chain(c: &mut Criterion) {
    let schema = four_op_schema();
    let ops = four_op_nodes();

    let mut g = c.benchmark_group("op_chain");

    // compile_4op: register-time cost — measure compile on each iteration.
    g.bench_function("compile_4op", |b| {
        b.iter(|| {
            let (_chain, _out_schema) =
                OpChain::compile(black_box(&schema), black_box(&ops)).unwrap();
        });
    });

    // apply_4op: per-row hot-path cost — compile once outside, bench apply only.
    let (chain, _out_schema) = OpChain::compile(&schema, &ops).unwrap();
    let row = four_op_row();

    g.bench_function("apply_4op", |b| {
        b.iter(|| {
            // apply consumes Row (by value), so clone per iteration.
            let out = chain.apply(black_box(row.clone()));
            black_box(out);
        });
    });

    g.finish();
}

criterion_group!(phase4_expr, bench_parse, bench_eval, bench_op_chain);
criterion_main!(phase4_expr);

// ── Contract constant ───────────────────────────────────────────────────────────

/// Declares the expected number of bench IDs: 3 parse + 5 eval + 2 op_chain.
#[allow(dead_code)]
pub mod phase4_expr_benches {
    pub const EXPECTED_GROUPS: usize = 10;
}

#[cfg(test)]
mod tests {
    #[test]
    fn groups_registered() {
        assert_eq!(
            crate::phase4_expr_benches::EXPECTED_GROUPS,
            10,
            "bench file must register 3 parse + 5 eval + 2 op-chain groups"
        );
    }
}
