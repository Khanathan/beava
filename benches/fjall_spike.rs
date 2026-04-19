//! Phase 53-01 spike gate bench: fjall 2.11 vs AHashMap read-modify-write.
//!
//! **GREEN** — plan 53-01 Task 1 Step 2. Compiles, runs, and produces the
//! numbers that feed the spike gate report.
//!
//! Measures (per plan 53-01 D-05):
//!   1. `fjall_spike::ahashmap_baseline::rmw_100_ops` — AHashMap
//!      `entry().or_default()` + in-place mutate baseline.
//!   2. `fjall_spike::fjall_rmw::rmw_100_ops` — `fjall::PartitionHandle`
//!      read-modify-write cycle: `partition.get` → `postcard::from_bytes`
//!      → mutate one `static_features` field → `postcard::to_stdvec` →
//!      `partition.insert`.
//!   3. `fjall_spike::postcard_sizes::noop_histogram_dump` — carrier
//!      bench whose SETUP emits the p50/p95/p99/max byte-size histogram
//!      of 10 000 synthetic `SerializableEntityState` values on stderr.
//!
//! Both `ahashmap_baseline` and `fjall_rmw` measure exactly `N_OPS` mutations
//! per iteration after pre-populating `N_KEYS` entries. Throughput is annotated
//! so Criterion reports ns/op directly.
//!
//! The gate passes if fjall is within −25% of AHashMap on (1) vs (2), and
//! p95 postcard size ≤ 4 KB (fjall default block_size).
//!
//! **Determinism:** `fsync_ms(None)` disables background fsync on the fjall
//! keyspace; all writes land in the memtable only. Microbench stays on the
//! memtable hot path — this is deliberately *optimistic* vs production
//! (`fsync_ms(5)`), so the −25% tolerance absorbs the real fsync cost.
//!
//! **Scope boundary:** This bench does NOT touch `src/state/` or
//! `src/shard/store.rs`. It uses fjall directly plus the existing
//! `SerializableEntityState` + `postcard` round-trip that Plan 53-02 will
//! wire into the production shard write path.
//!
//! Run: `cargo bench --bench fjall_spike`
//! Quick: `cargo bench --bench fjall_spike -- --measurement-time 5 --sample-size 30`

use ahash::AHashMap;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use std::time::SystemTime;
use tempfile::TempDir;

use beava::state::snapshot::SerializableEntityState;
use beava::state::store::StaticFeature;
use beava::types::FeatureValue;

/// N keys inserted during warm-up (populates both baselines to steady state).
const N_KEYS: usize = 1_000;
/// M read-modify-write operations per iteration of the hot-path bench.
const N_OPS: usize = 100;
/// Seed for the synthetic key / payload generator — deterministic across runs.
/// Value-space: hex-only so the literal is a valid `u64`.
const SEED: u64 = 0x5301_FA11_5E1C_0000;

/// `SerializableEntityState` does not derive `Default`. Build one by hand so the
/// AHashMap baseline can use `entry(k).or_insert_with(default_entity)` without
/// pulling in `beava::state::store::EntityState` (whose `AtomicU64` would
/// invalidate the postcard round-trip this bench measures).
fn default_entity() -> SerializableEntityState {
    SerializableEntityState {
        streams: Vec::new(),
        static_features: Vec::new(),
        table_rows: Vec::new(),
    }
}

/// Seeded generator for `[a-z]{3,8}` keys matching `sharding_parity.rs` key shape.
fn gen_key(rng: &mut StdRng) -> String {
    let len = rng.gen_range(3..=8);
    (0..len)
        .map(|_| (b'a' + rng.gen_range(0..26)) as char)
        .collect()
}

/// Build a representative hot-path EntityState: 3 static_features.
/// Matches common-case shape per 53-CONTEXT §specifics. Streams/table_rows
/// stay empty — the real hot-path value size is dominated by static_features
/// in the TPC architecture (streams serialize only when operator state
/// advances, which in this spike we don't exercise).
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
    SerializableEntityState {
        streams: Vec::new(),
        static_features,
        table_rows: Vec::new(),
    }
}

/// Mutate the `score` static_feature in-place (simulates the common hot-path write).
fn mutate_entity(entity: &mut SerializableEntityState, now: SystemTime, new_score: f64) {
    for (name, feat) in entity.static_features.iter_mut() {
        if name == "score" {
            feat.value = FeatureValue::Float(new_score);
            feat.updated_at = now;
            return;
        }
    }
    // If "score" missing (shouldn't happen for gen_entity output, but keeps
    // the mutation path sound if ever called on a default_entity).
    entity
        .static_features
        .push(("score".to_string(), StaticFeature {
            value: FeatureValue::Float(new_score),
            updated_at: now,
        }));
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

/// One fjall read-modify-write step: `get` → `postcard::from_bytes` → mutate
/// `score` → `postcard::to_stdvec` → `insert`. Mirrors the production hot-path
/// shape Plan 53-02 will wire into `StoreView::Sharded` (per 53-RESEARCH
/// Pattern 2).
fn fjall_rmw_one_step(
    part: &fjall::PartitionHandle,
    key: &str,
    now: SystemTime,
    new_score: f64,
) {
    let mut entity: SerializableEntityState = match part.get(key.as_bytes()).ok().flatten() {
        Some(bytes) => postcard::from_bytes(&bytes).unwrap_or_else(|_| default_entity()),
        None => default_entity(),
    };
    mutate_entity(&mut entity, now, new_score);
    let encoded = postcard::to_stdvec(&entity).expect("postcard encode EntityState");
    part.insert(key.as_bytes(), encoded).expect("fjall insert");
}

/// Baseline: AHashMap `entry().or_insert_with(default_entity)` + in-place mutate.
/// No (de)serialization — matches today's pre-Phase-53 in-memory hot path.
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
                    let entity = map.entry(k.clone()).or_insert_with(default_entity);
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

/// fjall read-modify-write: full cycle per op on a pre-populated partition.
///
/// The per-iteration SETUP tears down and rebuilds the keyspace in a fresh
/// `TempDir` so each measurement starts from a warm memtable but a cold
/// OS page cache, isolating the fjall-side cost from run-to-run noise.
/// `fsync_ms(None)` disables the background fsync thread.
fn bench_fjall_rmw(c: &mut Criterion) {
    let workload = gen_workload();
    let mut group = c.benchmark_group("fjall_spike::fjall_rmw");
    group.throughput(Throughput::Elements(N_OPS as u64));
    group.bench_function("rmw_100_ops", |b| {
        b.iter_batched(
            || {
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
                // Hold the TempDir alive for the duration of the bench iter;
                // fjall keeps file handles inside.
                (tmp, ks, part)
            },
            |(_tmp, _ks, part)| {
                let now = SystemTime::now();
                for (k, new_score) in &workload.ops {
                    fjall_rmw_one_step(
                        black_box(&part),
                        black_box(k),
                        black_box(now),
                        black_box(*new_score),
                    );
                }
                black_box(part);
            },
            criterion::BatchSize::SmallInput,
        );
    });
    group.finish();
}

/// Emit the postcard-encoded size histogram for 10 000 synthetic entities on
/// stderr (Criterion swallows stdout). The carrier bench is a no-op `iter`
/// so the histogram only runs once, at setup time.
fn bench_postcard_sizes(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(SEED ^ 0xDEAD_BEEF);
    let mut sizes: Vec<usize> = (0..10_000)
        .map(|_| {
            let e = gen_entity(&mut rng);
            postcard::to_stdvec(&e).expect("postcard encode").len()
        })
        .collect();
    sizes.sort_unstable();
    let pct = |p: f64| sizes[((sizes.len() as f64 - 1.0) * p) as usize];
    eprintln!(
        "postcard_size_p50={} p95={} p99={} max={} n={}",
        pct(0.50),
        pct(0.95),
        pct(0.99),
        sizes.last().copied().unwrap_or(0),
        sizes.len()
    );

    let mut group = c.benchmark_group("fjall_spike::postcard_sizes");
    group.bench_function("noop_histogram_dump", |b| {
        b.iter(|| black_box(sizes.len()))
    });
    group.finish();
}

criterion_group!(benches, bench_ahashmap_baseline, bench_fjall_rmw, bench_postcard_sizes);
criterion_main!(benches);
