// D-22: micro-bench for ArcSwap<DashSet<String>> mark_dirty overhead.
// Compile-only until Phase 46 Wave 4 (D-21) exposes StateStore::mark_dirty
// through the new ArcSwap<DashSet<String>> container.
//
// Regression ceiling: <2% delta on the 9-cell bench (D-03 gate).
// Run via: cargo bench --bench dirty_swap

use arc_swap::ArcSwap;
use criterion::{criterion_group, criterion_main, Criterion};
use dashmap::DashSet;
use std::sync::Arc;

/// Smoke bench: verify arc-swap is in the dependency tree and ArcSwap<DashSet>
/// load overhead is measurable.  Real StateStore::mark_dirty bench follows
/// once D-21 lands.
fn bench_arc_swap_load(c: &mut Criterion) {
    let set = ArcSwap::new(Arc::new(DashSet::<String>::new()));
    c.bench_function("arc_swap_load_smoke", |b| {
        b.iter(|| {
            let guard = set.load();
            std::hint::black_box(&*guard);
        })
    });
}

/// Smoke bench: insert a key into the loaded DashSet under an ArcSwap guard.
/// Proxy for the mark_dirty hot path once D-21 lands.
fn bench_arc_swap_insert(c: &mut Criterion) {
    let set = ArcSwap::new(Arc::new(DashSet::<String>::new()));
    let key = "entity::12345".to_string();
    c.bench_function("arc_swap_insert_smoke", |b| {
        b.iter(|| {
            let guard = set.load();
            guard.insert(std::hint::black_box(key.clone()));
        })
    });
}

criterion_group!(benches, bench_arc_swap_load, bench_arc_swap_insert);
criterion_main!(benches);
