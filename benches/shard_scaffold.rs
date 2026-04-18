//! Criterion micro-bench: `shard_hint_for_event` overhead per event shape.
//!
//! Ship-gate (D-06): p50 <100 ns per invocation on the reference box.
//! Run: `cargo bench --bench shard_scaffold`
//!
//! Three shapes exercised:
//!   1. `string_key`           — single string primary key (common case)
//!   2. `tuple_two_field_key`  — two-field group-by key, first field hashed
//!   3. `numeric_key`          — integer primary key (graceful-fallback path)
//!
//! Nightly CI saves the output to `benchmark/shard_scaffold/README.md`.
//! Do NOT inline SPSC roundtrip here — deferred to Wave 1 (D-08).

use beava::routing::shard_hint_for_event;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkGroup, Criterion, Throughput};
use criterion::measurement::WallTime;
use serde_json::json;

fn run_shape(
    group: &mut BenchmarkGroup<WallTime>,
    id: &str,
    event: serde_json::Value,
    key_field: Option<&'static str>,
) {
    group.bench_function(id, |b| {
        b.iter(|| {
            let hint = shard_hint_for_event(black_box(&event), black_box(key_field));
            black_box(hint)
        });
    });
}

fn bench_shard_hint(c: &mut Criterion) {
    let mut group = c.benchmark_group("shard_hint");
    // Throughput annotation: 1 event per iteration — allows criterion to report ns/event.
    group.throughput(Throughput::Elements(1));

    run_shape(
        &mut group,
        "string_key",
        json!({"user_id": "user-0001"}),
        Some("user_id"),
    );

    run_shape(
        &mut group,
        "tuple_two_field_key",
        json!({"region": "us-east", "user_id": "user-0001"}),
        Some("region"),
    );

    run_shape(
        &mut group,
        "numeric_key",
        json!({"id": 42_u64}),
        Some("id"),
    );

    group.finish();
}

criterion_group!(benches, bench_shard_hint);
criterion_main!(benches);
