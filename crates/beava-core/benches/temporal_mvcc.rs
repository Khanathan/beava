// Phase 11.5 temporal MVCC store hot-path bench (plan 11.5-01 Task 6).
//
// Groups:
//   temporal_store_upsert/depth_{1,10,100,1000} — pure TemporalStore::upsert
//   temporal_store_as_of_lookup/depth_{1,10,100,1000} — lookup_at_lsn walking the chain
//
// First baseline for the Phase 11.5 MVCC store. Per CLAUDE.md §Performance
// Discipline, the bench's existence is the deliverable — no prior baseline
// to compare against.

use beava_core::row::{Row, Value};
use beava_core::temporal::TemporalStore;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

fn make_row(score: i64) -> Row {
    Row::new()
        .with_field("score", Value::I64(score))
        .with_field("note", Value::Str("bench".into()))
}

fn build_store_with_chain(depth: u64) -> (TemporalStore, Vec<u8>) {
    let mut store = TemporalStore::new();
    let key: Vec<u8> = b"k1".to_vec();
    for lsn in 1..=depth {
        store.upsert(key.clone(), lsn, make_row(lsn as i64), lsn * 10);
    }
    (store, key)
}

fn bench_upsert(c: &mut Criterion) {
    let mut group = c.benchmark_group("temporal_store_upsert");
    for depth in [1u64, 10, 100, 1000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(depth),
            &depth,
            |b, &depth| {
                // Pre-fill so each iter inserts at chain length ~depth.
                b.iter_batched(
                    || build_store_with_chain(depth),
                    |(mut store, key)| {
                        store.upsert(
                            black_box(key.clone()),
                            black_box(depth + 1),
                            black_box(make_row((depth + 1) as i64)),
                            black_box((depth + 1) * 10),
                        );
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_as_of_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("temporal_store_as_of_lookup");
    for depth in [1u64, 10, 100, 1000] {
        let (store, key) = build_store_with_chain(depth);
        // Sanity check: the fixture is well-formed.
        assert!(store.lookup_at_lsn(&key, depth).is_some());
        group.bench_with_input(
            BenchmarkId::from_parameter(depth),
            &depth,
            |b, &depth| {
                b.iter(|| {
                    let v = store.lookup_at_lsn(black_box(&key), black_box(depth / 2 + 1));
                    black_box(v);
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_upsert, bench_as_of_lookup);
criterion_main!(benches);
