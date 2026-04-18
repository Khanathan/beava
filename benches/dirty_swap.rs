// D-22: micro-bench for ArcSwap<DashSet<String>> mark_dirty overhead.
// Regression ceiling: <2% delta on the 9-cell bench (D-03 gate).
// Run via: cargo bench --bench dirty_swap

use beava::state::store::StateStore;
use criterion::{criterion_group, criterion_main, Criterion};

/// Steady-state mark_dirty: repeatedly mark the same key.
/// After the first call in a snapshot cycle, the per-entity dirty_gen fast-path
/// short-circuits and the ArcSwap guard is never acquired.  This bench measures
/// the pure fast-path cost (two relaxed atomic loads + early return).
fn bench_mark_dirty_steady_state(c: &mut Criterion) {
    let store = StateStore::new();
    // Prime the entity so the per-entity dirty_gen path is active.
    let _ = store.get_or_create_entity("k0");
    c.bench_function("mark_dirty_steady_state", |b| {
        b.iter(|| {
            store.mark_dirty("k0");
        })
    });
}

/// Distinct-key mark_dirty: each iteration inserts a new key.
/// The per-entity dirty_gen fast-path cannot short-circuit (no entity exists),
/// so every call goes through ArcSwap::load() + DashSet::insert().
/// Measures the ArcSwap Guard overhead + DashSet insert cost.
fn bench_mark_dirty_distinct(c: &mut Criterion) {
    let store = StateStore::new();
    let mut i = 0u64;
    c.bench_function("mark_dirty_distinct", |b| {
        b.iter(|| {
            store.mark_dirty(&format!("k{}", i));
            i = i.wrapping_add(1);
        })
    });
}

/// take_dirty_and_advance_gen on an empty set: measures the pure atomic-swap
/// + fetch_add cost with no DashSet population work.
fn bench_take_and_advance_empty(c: &mut Criterion) {
    let store = StateStore::new();
    c.bench_function("take_dirty_and_advance_gen_empty", |b| {
        b.iter(|| {
            let _ = store.take_dirty_and_advance_gen();
        })
    });
}

criterion_group!(
    benches,
    bench_mark_dirty_steady_state,
    bench_mark_dirty_distinct,
    bench_take_and_advance_empty
);
criterion_main!(benches);
