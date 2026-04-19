//! Phase 53-01 spike gate bench: fjall 2.11 vs AHashMap read-modify-write.
//!
//! **RED scaffold** — this file is the TDD RED commit for plan 53-01 Task 1.
//! The fjall bench group intentionally references `fjall_rmw_one_step` which
//! is NOT yet defined, so `cargo bench --bench fjall_spike --no-run` fails
//! with `cannot find function`. The GREEN commit replaces this file with a
//! fully-compiling bench.
//!
//! Measures (per plan 53-01 D-05):
//!   1. AHashMap<String, SerializableEntityState> entry().or_default() + mutate baseline
//!   2. fjall::PartitionHandle read-modify-write cycle on the same payload shape
//!   3. Postcard-encoded EntityState byte-size distribution (p50/p95/p99/max)
//!
//! The gate passes if fjall is within −25% of AHashMap on (1) vs (2), and
//! p95 postcard size ≤ 4 KB (fjall default block_size).
//!
//! Run: `cargo bench --bench fjall_spike`

#![allow(dead_code)] // RED scaffold — symbols wired up in GREEN commit.

use ahash::AHashMap;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkGroup, Criterion, Throughput};
use criterion::measurement::WallTime;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use std::time::SystemTime;
use tempfile::TempDir;

use beava::state::snapshot::{SerializableEntityState, SerializableStreamEntityState};
use beava::state::store::StaticFeature;
use beava::types::FeatureValue;

/// N keys inserted during warm-up (populates both baselines to steady state).
const N_KEYS: usize = 1_000;
/// M read-modify-write operations per iteration of the hot-path bench.
const N_OPS: usize = 100;
/// Seed for the synthetic key / payload generator — deterministic across runs.
const SEED: u64 = 0x53_01_FJALL_5P1KE;

/// Seeded generator for `[a-z]{3,8}` keys matching `sharding_parity.rs` key shape.
fn gen_key(rng: &mut StdRng) -> String {
    let len = rng.gen_range(3..=8);
    (0..len)
        .map(|_| (b'a' + rng.gen_range(0..26)) as char)
        .collect()
}

/// Build a representative hot-path EntityState: 3 static_features + 1 stream with
/// 2 operator entries. Matches common-case shape per 53-CONTEXT §specifics.
fn gen_entity(rng: &mut StdRng) -> SerializableEntityState {
    let now = SystemTime::now();
    let static_features = vec![
        (
            "country".to_string(),
            StaticFeature { value: FeatureValue::String("US".into()), updated_at: now },
        ),
        (
            "tier".to_string(),
            StaticFeature { value: FeatureValue::Int(rng.gen_range(1..=3)), updated_at: now },
        ),
        (
            "score".to_string(),
            StaticFeature { value: FeatureValue::Float(rng.gen_range(0.0..1.0)), updated_at: now },
        ),
    ];
    // We do not construct real OperatorState values — they require engine
    // constructors out of scope for this spike. Streams stays empty; the
    // static_features load dominates the postcard size for the common case.
    SerializableEntityState {
        streams: Vec::new(),
        static_features,
        table_rows: Vec::new(),
    }
}

/// Mutate one static_feature field in-place (simulates hot-path write).
fn mutate_entity(entity: &mut SerializableEntityState, now: SystemTime, new_score: f64) {
    for (name, feat) in entity.static_features.iter_mut() {
        if name == "score" {
            feat.value = FeatureValue::Float(new_score);
            feat.updated_at = now;
            return;
        }
    }
}

/// Pre-generated workload: keys + initial entities + per-op (key, new_score) stream.
struct Workload {
    keys: Vec<String>,
    entities: Vec<SerializableEntityState>,
    ops: Vec<(String, f64)>,
}

fn gen_workload() -> Workload {
    let mut rng = StdRng::seed_from_u64(SEED);
    let keys: Vec<String> = (0..N_KEYS).map(|_| gen_key(&mut rng)).collect();
    let entities: Vec<SerializableEntityState> = (0..N_KEYS).map(|_| gen_entity(&mut rng)).collect();
    let ops: Vec<(String, f64)> = (0..N_OPS)
        .map(|_| {
            let idx = rng.gen_range(0..N_KEYS);
            (keys[idx].clone(), rng.gen_range(0.0..1.0))
        })
        .collect();
    Workload { keys, entities, ops }
}

/// Baseline: AHashMap entry().or_default() + in-place mutate.
fn bench_ahashmap_baseline(c: &mut Criterion) {
    let workload = gen_workload();
    let mut group = c.benchmark_group("fjall_spike::ahashmap_baseline");
    group.throughput(Throughput::Elements(N_OPS as u64));
    group.bench_function("rmw_100_ops", |b| {
        b.iter_batched(
            || {
                let mut map: AHashMap<String, SerializableEntityState> = AHashMap::new();
                for (k, e) in workload.keys.iter().zip(workload.entities.iter()) {
                    map.insert(k.clone(), e.clone());
                }
                map
            },
            |mut map| {
                let now = SystemTime::now();
                for (k, new_score) in &workload.ops {
                    let entity = map.entry(k.clone()).or_default();
                    mutate_entity(entity, now, *new_score);
                    black_box(&*entity);
                }
                black_box(map);
            },
            criterion::BatchSize::SmallInput,
        );
    });
    group.finish();
}

/// fjall read-modify-write: get → postcard decode → mutate → postcard encode → insert.
fn bench_fjall_rmw(c: &mut Criterion) {
    let workload = gen_workload();
    let mut group = c.benchmark_group("fjall_spike::fjall_rmw");
    group.throughput(Throughput::Elements(N_OPS as u64));
    group.bench_function("rmw_100_ops", |b| {
        b.iter_batched(
            || {
                // Scaffold: fjall_rmw_one_step is intentionally undefined in the RED commit.
                // GREEN commit defines it to do the get/decode/mutate/encode/insert cycle.
                let tmp = TempDir::new().expect("tempdir");
                let cfg = fjall::Config::new(tmp.path().join("fjall"))
                    .fsync_ms(None)
                    .cache_size(32 * 1024 * 1024);
                let ks = cfg.open().expect("open keyspace");
                let part = ks
                    .open_partition("shard-0", fjall::PartitionCreateOptions::default())
                    .expect("open partition");
                for (k, e) in workload.keys.iter().zip(workload.entities.iter()) {
                    let bytes = postcard::to_stdvec(e).expect("postcard");
                    part.insert(k.as_bytes(), bytes).expect("fjall insert");
                }
                ks.persist(fjall::PersistMode::SyncData).expect("persist");
                (tmp, part)
            },
            |(_tmp, part)| {
                let now = SystemTime::now();
                for (k, new_score) in &workload.ops {
                    fjall_rmw_one_step(black_box(&part), black_box(k), black_box(now), black_box(*new_score));
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });
    group.finish();
}

/// Prints the postcard-encoded size histogram over 10_000 synthetic entities.
fn bench_postcard_sizes(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(SEED ^ 0xDEAD_BEEF);
    let mut sizes: Vec<usize> = (0..10_000)
        .map(|_| {
            let e = gen_entity(&mut rng);
            postcard::to_stdvec(&e).unwrap().len()
        })
        .collect();
    sizes.sort_unstable();
    let p = |pct: f64| sizes[((sizes.len() as f64 - 1.0) * pct) as usize];
    eprintln!(
        "postcard_size_p50={} p95={} p99={} max={} n={}",
        p(0.50),
        p(0.95),
        p(0.99),
        sizes.last().copied().unwrap_or(0),
        sizes.len()
    );
    // Dummy bench group so criterion_main accepts the function.
    let mut group = c.benchmark_group("fjall_spike::postcard_sizes");
    group.bench_function("noop_histogram_dump", |b| b.iter(|| black_box(sizes.len())));
    group.finish();
    // Silence unused BenchmarkGroup type import
    let _: fn(&mut BenchmarkGroup<WallTime>) = |_| {};
}

criterion_group!(benches, bench_ahashmap_baseline, bench_fjall_rmw, bench_postcard_sizes);
criterion_main!(benches);
