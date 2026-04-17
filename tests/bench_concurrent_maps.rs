//! Concurrent-hashmap shootout for Beava's entity-lookup-and-mutate pattern.
//!
//! The pprof profile of `handle_push_batch` showed 66% of CPU in
//! `DashMap::lock_exclusive`. Before attempting an architectural fix, test
//! whether ANOTHER concurrent-hashmap crate would remove this bottleneck on
//! the same access pattern. If a swap wins, it's a one-commit fix; if not,
//! the bottleneck is the mutation pattern itself, not DashMap.
//!
//! Workload: 8 OS threads each run a loop of
//!   `map.get_or_create(key).mutate()`. Key is drawn from the same Zipf
//! distribution as profile_ingest (10K keys, α=1.2). The "EntityState"
//! is a small struct (u64 counter + Vec<(String, u64)> for "operator
//! state") — enough work per mutation that lock hold time is comparable
//! to the real server, not artificially brief.
//!
//! Primitives tested:
//!   - dashmap::DashMap           (current implementation)
//!   - scc::HashMap               (non-blocking entry API)
//!   - papaya::HashMap            (seqlock reads, batched writes)
//!   - flurry::HashMap            (Java ConcurrentHashMap port)
//!   - parking_lot::RwLock<HashMap>  (single big RwLock)
//!   - parking_lot::Mutex<HashMap>   (single big Mutex)
//!
//! Run:
//!   cargo test --release --test bench_concurrent_maps \
//!     -- --nocapture --ignored concurrent_map_shootout

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

// --- shared test harness ---

#[derive(Default, Clone)]
struct Entity {
    counter: u64,
    ops: Vec<(String, u64)>,
}

impl Entity {
    /// Simulate per-entity work that DashMap would do under a held RefMut:
    /// a counter bump, plus a small amount of mutation in an owned Vec.
    /// The Vec push + string allocation approximates operator-state updates.
    #[inline]
    fn mutate(&mut self) {
        self.counter = self.counter.wrapping_add(1);
        if self.ops.len() < 4 {
            self.ops.push(("count_1h".into(), self.counter));
        } else {
            self.ops[0].1 = self.counter;
        }
    }
}

fn zipf_key(rng_state: &mut u64, n: u64) -> String {
    *rng_state = rng_state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    let u = (*rng_state >> 33) as f64 / (1u64 << 31) as f64;
    let alpha = 1.2_f64;
    let rank = ((u * (n as f64).powf(1.0 - alpha) + (1.0 - u)).powf(1.0 / (1.0 - alpha)))
        .max(1.0)
        .min(n as f64) as u64;
    format!("u{:06}", rank)
}

const N_WORKERS: usize = 8;
const DURATION_S: u64 = 3;
const N_KEYS: u64 = 10_000;

/// Drive a closure on N_WORKERS threads for DURATION_S, count total ops.
fn drive<F>(label: &str, body: F) -> u64
where
    F: Fn(u64) + Send + Sync + 'static + Clone,
{
    let stop = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let mut handles = Vec::with_capacity(N_WORKERS);
    for tid in 0..N_WORKERS {
        let stop = stop.clone();
        let total = total.clone();
        let body = body.clone();
        handles.push(thread::spawn(move || {
            let mut local: u64 = 0;
            while !stop.load(Ordering::Relaxed) {
                body(tid as u64);
                local += 1;
            }
            total.fetch_add(local, Ordering::Relaxed);
        }));
    }
    thread::sleep(Duration::from_secs(DURATION_S));
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    let elapsed = start.elapsed();
    let ops = total.load(Ordering::Relaxed);
    let ops_per_sec = ops as f64 / elapsed.as_secs_f64();
    let ns_per = 1e9 / (ops_per_sec / N_WORKERS as f64);
    println!(
        "{:<36}  {:>12.0} ops/s  ({:>6.0} ns/op)  across {} threads",
        label, ops_per_sec, ns_per, N_WORKERS
    );
    ops
}

// --- DashMap ---
fn bench_dashmap() {
    let map: Arc<dashmap::DashMap<String, Entity>> = Arc::new(dashmap::DashMap::new());
    let label = "dashmap::DashMap";
    let m = map.clone();
    drive(label, move |tid| {
        let mut rng = tid.wrapping_mul(0x9E3779B97F4A7C15);
        let key = zipf_key(&mut rng, N_KEYS);
        let mut e = m.entry(key).or_default();
        e.value_mut().mutate();
    });
}

// --- scc::HashMap ---
fn bench_scc() {
    let map: Arc<scc::HashMap<String, Entity>> = Arc::new(scc::HashMap::new());
    let label = "scc::HashMap";
    let m = map.clone();
    drive(label, move |tid| {
        let mut rng = tid.wrapping_mul(0x9E3779B97F4A7C15);
        let key = zipf_key(&mut rng, N_KEYS);
        // scc 3.x sync API is entry_sync (blocking).
        m.entry_sync(key).or_default().get_mut().mutate();
    });
}

// --- papaya::HashMap ---
fn bench_papaya() {
    let map: Arc<papaya::HashMap<String, std::sync::Mutex<Entity>>> =
        Arc::new(papaya::HashMap::new());
    let label = "papaya::HashMap (+entity Mutex)";
    let m = map.clone();
    // papaya's reads are lock-free but it returns shared references to
    // values — you can't get &mut through the map. To mutate, we wrap
    // each entity in its own Mutex. This is actually a test of the
    // "short critical section" pattern: papaya's map lookup is
    // lock-free, then we lock only the individual entity.
    drive(label, move |tid| {
        let mut rng = tid.wrapping_mul(0x9E3779B97F4A7C15);
        let key = zipf_key(&mut rng, N_KEYS);
        let guard = m.guard();
        if let Some(entity_mutex) = m.get(&key, &guard) {
            entity_mutex.lock().unwrap().mutate();
        } else {
            // Insert + mutate on first sight.
            m.insert(key, std::sync::Mutex::new(Entity::default()), &guard);
        }
    });
}

// --- flurry::HashMap ---
fn bench_flurry() {
    let map: Arc<flurry::HashMap<String, std::sync::Mutex<Entity>>> =
        Arc::new(flurry::HashMap::new());
    let label = "flurry::HashMap (+entity Mutex)";
    let m = map.clone();
    drive(label, move |tid| {
        let mut rng = tid.wrapping_mul(0x9E3779B97F4A7C15);
        let key = zipf_key(&mut rng, N_KEYS);
        let guard = m.guard();
        if let Some(entity_mutex) = m.get(&key, &guard) {
            entity_mutex.lock().unwrap().mutate();
        } else {
            m.insert(key, std::sync::Mutex::new(Entity::default()), &guard);
        }
    });
}

// --- single parking_lot::RwLock<HashMap> ---
fn bench_rwlock() {
    use std::collections::HashMap;
    let map: Arc<parking_lot::RwLock<HashMap<String, parking_lot::Mutex<Entity>>>> =
        Arc::new(parking_lot::RwLock::new(HashMap::new()));
    let label = "RwLock<HashMap> (+entity Mutex)";
    let m = map.clone();
    drive(label, move |tid| {
        let mut rng = tid.wrapping_mul(0x9E3779B97F4A7C15);
        let key = zipf_key(&mut rng, N_KEYS);
        // Fast path: read-lock, lookup, mutate-in-entity-mutex.
        let r = m.read();
        if let Some(entity_mutex) = r.get(&key) {
            entity_mutex.lock().mutate();
            return;
        }
        drop(r);
        // Slow path: write-lock, insert.
        m.write()
            .entry(key)
            .or_insert_with(|| parking_lot::Mutex::new(Entity::default()))
            .lock()
            .mutate();
    });
}

// --- single parking_lot::Mutex<HashMap> ---
fn bench_mutex() {
    use std::collections::HashMap;
    let map: Arc<parking_lot::Mutex<HashMap<String, Entity>>> =
        Arc::new(parking_lot::Mutex::new(HashMap::new()));
    let label = "Mutex<HashMap> (coarse)";
    let m = map.clone();
    drive(label, move |tid| {
        let mut rng = tid.wrapping_mul(0x9E3779B97F4A7C15);
        let key = zipf_key(&mut rng, N_KEYS);
        let mut g = m.lock();
        g.entry(key).or_default().mutate();
    });
}

// --- sharded Mutex<HashMap> (DIY N-way sharding with full control) ---
fn bench_sharded_mutex() {
    use std::collections::HashMap;
    use std::hash::{BuildHasher, Hasher};
    const N_SHARDS: usize = 64;
    let shards: Vec<parking_lot::Mutex<HashMap<String, Entity>>> =
        (0..N_SHARDS).map(|_| parking_lot::Mutex::new(HashMap::new())).collect();
    let map: Arc<Vec<parking_lot::Mutex<HashMap<String, Entity>>>> = Arc::new(shards);
    let hasher_state = ahash::RandomState::with_seeds(1, 2, 3, 4);
    let label = "sharded-64 Mutex<HashMap>";
    let m = map.clone();
    drive(label, move |tid| {
        let mut rng = tid.wrapping_mul(0x9E3779B97F4A7C15);
        let key = zipf_key(&mut rng, N_KEYS);
        let mut h = hasher_state.build_hasher();
        std::hash::Hash::hash(&key, &mut h);
        let shard = (h.finish() as usize) % N_SHARDS;
        let mut g = m[shard].lock();
        g.entry(key).or_default().mutate();
    });
}

// ============================================================
// Part 2: does per-operator DashMap unlock pipeline parallelism?
// ============================================================
//
// Real Beava pushes touch N_OPS operators per event (count_1h, sum_1h,
// p99_1h, distinct, etc.). Current design: all N_OPS updates happen
// under ONE shard lock (the entity's RefMut). Proposal: split operators
// into N_OPS separate DashMaps keyed by entity_key, so two threads
// pushing to the same entity can pipeline — Thread A updates count_1h
// while Thread B starts updating sum_1h.
//
// Per-event cost: N_OPS lock acquisitions instead of 1. Trade-off is
// whether pipeline parallelism outweighs the extra acquisition overhead.

/// Number of operators updated per "event" (approximating a Beava
/// stream with count_1h + sum_1h + distinct_1h + min_1h + max_1h + ...).
const N_OPS: usize = 8;

#[derive(Default, Clone)]
struct OpState {
    value: u64,
    buf: Vec<u8>,
}

impl OpState {
    #[inline]
    fn push(&mut self, v: u64) {
        self.value = self.value.wrapping_add(v);
        // Simulate the ~1-3 µs of real work operator.push does
        // (HLL hash, bucket advance, sketch update).
        if self.buf.len() < 16 {
            self.buf.extend_from_slice(&v.to_le_bytes());
        } else {
            self.buf[0..8].copy_from_slice(&v.to_le_bytes());
        }
    }
}

/// Entity with N_OPS inline operators — the current Beava layout.
#[derive(Default, Clone)]
struct EntityWithOps {
    ops: [OpState; N_OPS],
}

// --- MONOLITHIC: one DashMap<key, EntityWithOps>, update all N_OPS under one lock ---
fn bench_monolithic_entity() {
    let map: Arc<dashmap::DashMap<String, EntityWithOps>> = Arc::new(dashmap::DashMap::new());
    let label = "MONOLITHIC: DashMap<key, EntityWithOps>";
    let m = map.clone();
    drive(label, move |tid| {
        let mut rng = tid.wrapping_mul(0x9E3779B97F4A7C15);
        let key = zipf_key(&mut rng, N_KEYS);
        let mut e = m.entry(key).or_default();
        // All N_OPS updates under one held RefMut (current Beava behavior)
        for i in 0..N_OPS {
            e.value_mut().ops[i].push(1);
        }
    });
}

// --- PER-OPERATOR DashMaps: N_OPS separate maps, one lock per op ---
fn bench_per_operator() {
    // One DashMap per operator slot.
    let maps: Arc<Vec<dashmap::DashMap<String, OpState>>> =
        Arc::new((0..N_OPS).map(|_| dashmap::DashMap::new()).collect());
    let label = "PER-OP: Vec<DashMap<key, OpState>>";
    let m = maps.clone();
    drive(label, move |tid| {
        let mut rng = tid.wrapping_mul(0x9E3779B97F4A7C15);
        let key = zipf_key(&mut rng, N_KEYS);
        // Separate lookup + acquire per operator — N_OPS lock pairs,
        // but other threads can interleave at operator granularity.
        for i in 0..N_OPS {
            let mut e = m[i].entry(key.clone()).or_default();
            e.value_mut().push(1);
        }
    });
}

// --- Composite-key single DashMap: DashMap<(key, op_idx), OpState> ---
// Different operators for the same entity hash to different shards.
// Avoids the Vec<DashMap> overhead but still gets per-op locking.
fn bench_composite_key_dashmap() {
    let map: Arc<dashmap::DashMap<(String, u8), OpState>> = Arc::new(dashmap::DashMap::new());
    let label = "COMPOSITE-KEY: DashMap<(key, op), OpState>";
    let m = map.clone();
    drive(label, move |tid| {
        let mut rng = tid.wrapping_mul(0x9E3779B97F4A7C15);
        let key = zipf_key(&mut rng, N_KEYS);
        for i in 0..N_OPS {
            let mut e = m.entry((key.clone(), i as u8)).or_default();
            e.value_mut().push(1);
        }
    });
}

// --- Per-operator with Arc<str> key (avoid clone overhead) ---
fn bench_per_operator_arc_str() {
    let maps: Arc<Vec<dashmap::DashMap<Arc<str>, OpState>>> =
        Arc::new((0..N_OPS).map(|_| dashmap::DashMap::new()).collect());
    let label = "PER-OP (Arc<str>): Vec<DashMap<Arc<str>, OpState>>";
    let m = maps.clone();
    drive(label, move |tid| {
        let mut rng = tid.wrapping_mul(0x9E3779B97F4A7C15);
        let key: Arc<str> = Arc::from(zipf_key(&mut rng, N_KEYS).as_str());
        for i in 0..N_OPS {
            let mut e = m[i].entry(key.clone()).or_default();
            e.value_mut().push(1);
        }
    });
}

#[test]
#[ignore]
fn concurrent_map_shootout() {
    println!(
        "\n=== concurrent-hashmap shootout ===\n{} threads, {} s, {} distinct keys, Zipf α=1.2\n",
        N_WORKERS, DURATION_S, N_KEYS
    );
    bench_dashmap();
    bench_scc();
    bench_papaya();
    bench_flurry();
    bench_rwlock();
    bench_sharded_mutex();
    bench_mutex();

    println!(
        "\n=== per-operator layout shootout (N_OPS={} per event) ===\n",
        N_OPS
    );
    bench_monolithic_entity();
    bench_per_operator();
    bench_composite_key_dashmap();
    bench_per_operator_arc_str();
    println!();
}
