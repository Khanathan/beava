//! Microbench: `if_else` short-circuit (lazy) vs eager evaluation.
//!
//! `eval` (production) short-circuits `if_else` — it evaluates the condition
//! and exactly one branch. `eval_eager` (test-only twin, identical machinery
//! minus the short-circuit hook) evaluates every branch at every level. Both
//! return the same value (eval is pure + total); the only difference is wasted
//! work on untaken branches.
//!
//! The win is only visible when untaken branches are expensive, so we generate
//! a *balanced* `if_else` tree where BOTH branches recurse: nesting `depth`
//! drives the eager cost exponentially (2^depth leaves), while lazy walks one
//! root->leaf path (depth conditions + 1 leaf). `leaf_complexity` sets the
//! arithmetic size at each leaf.
//!
//! Run: `cargo bench -p beava-bench --bench if_else_eager_vs_lazy`

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

use beava_core::eval::{eval, eval_eager};
use beava_core::expr;
use beava_core::row::{Row, Value};

/// A leaf branch: left-deep arithmetic of size `c`, e.g. `((x * 2 + 1) * 2 + 1)`.
fn leaf(c: usize) -> String {
    let mut s = "x".to_string();
    for _ in 0..c {
        s = format!("({s} * 2 + 1)");
    }
    s
}

/// A balanced `if_else` tree of nesting `depth`; both branches recurse, so
/// eager evaluates all 2^depth leaves while lazy walks one path. `ctr` varies
/// the per-node condition threshold so the tree isn't trivially uniform.
fn tree(depth: usize, c: usize, ctr: &mut i64) -> String {
    if depth == 0 {
        return leaf(c);
    }
    *ctr += 1;
    let k = *ctr % 7;
    let then_ = tree(depth - 1, c, ctr);
    let else_ = tree(depth - 1, c, ctr);
    // Condition is parenthesized to match beava's wire form for a compound
    // condition: `if_else((x > k), then, else)`.
    format!("if_else((x > {k}), {then_}, {else_})")
}

fn bench(crit: &mut Criterion) {
    let row = Row::new().with_field("x", Value::I64(3));

    // (label, nesting_depth, leaf_complexity) — tune freely.
    let cases = [
        ("simple_flat", 1usize, 1usize),   // average / simple both branches
        ("moderate_nested", 4, 4),         // moderately nested, complex both branches
        ("highly_nested", 8, 8),           // highly complex nested both branches
    ];

    let mut g = crit.benchmark_group("if_else_eager_vs_lazy");
    for (label, depth, c) in cases {
        let mut ctr = 0i64;
        let src = tree(depth, c, &mut ctr);
        let e = expr::parse(&src).expect("generated expression must parse");

        // Sanity: both paths agree on the value (perf, not correctness, differs).
        assert_eq!(eval(&e, &row), eval_eager(&e, &row));

        g.bench_with_input(BenchmarkId::new("lazy", label), &e, |b, e| {
            b.iter(|| black_box(eval(black_box(e), black_box(&row))));
        });
        g.bench_with_input(BenchmarkId::new("eager", label), &e, |b, e| {
            b.iter(|| black_box(eval_eager(black_box(e), black_box(&row))));
        });
    }
    g.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
