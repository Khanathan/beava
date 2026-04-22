//! In-process pprof-rs profiler for the server ingest hot path.
//!
//! Replaces the flaky samply-subprocess-TCP-driver-SIGINT dance with a
//! self-contained harness that:
//!   1. Builds a fraud-pipeline-shaped `SharedState` (4 streams, 3 fan-out
//!      keys — same shape as benchmark/fraud-pipeline 'complex' mode).
//!   2. Spawns 8 OS threads that sustain PUSH load via `handle_push_batch`
//!      for ~8 seconds at ~300 K aggregate EPS.
//!   3. Collects pprof-rs samples at 997 Hz across ALL threads.
//!   4. Writes `/tmp/beava_ingest.flamegraph.svg` for visual inspection
//!      AND `/tmp/beava_ingest.top.txt` for a text dump of top hot
//!      functions sorted by self-samples.
//!
//! Run with:
//!   cargo test --release --test profile_ingest -- --nocapture --ignored
//!
//! Reading the output:
//! - `top.txt` shows which symbols burned the most CPU. Lines near the
//!   top are the bottleneck. Look for entries like:
//!   - `dashmap::...` — DashMap shard lock contention
//!   - `parking_lot::...park` — RwLock/Mutex wait
//!   - `libc::write` / `__write_nocancel` — event log syscall (but we
//!     have event_log disabled in this bench so shouldn't appear)
//!   - `core::sync::atomic::...` — cache-line bouncing on hot atomics
//!
//!   - SVG: load in a browser for the interactive flamegraph.
//!
//! Gated by `#[ignore]` so regular `cargo test` doesn't spend 8 s on it.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use serde_json::json;

use beava::engine::pipeline::{FeatureDef, PipelineEngine, StreamDefinition};
use beava::server::tcp::{handle_push_batch, make_concurrent_state_full, BackfillTracker, PendingAsync, SharedState};

fn count_stream(name: &str, key_field: &str) -> StreamDefinition {
    StreamDefinition {
        name: name.into(),
        key_field: Some(key_field.into()),
        group_by_keys: None,
        features: vec![(
            "count_1h".into(),
            FeatureDef::Count {
                window: Duration::from_secs(3600),
                bucket: Duration::from_secs(60),
                where_expr: None,
                backfill: false,
            },
        )],
        depends_on: None,
        filter: None,
        entity_ttl: None,
        history_ttl: None,
        projection: None,
        ephemeral: None,
        pipeline_ttl: None,
        max_keys: None,
        watermark_lateness: None,
        shard_key: None,
        salt: None,
    }
}

/// Build a state with N shards AND spawn the shard threads so
/// `handle_push_batch` actually routes through the SPSC→shard→fjall path.
///
/// Wave-4 removed the N=1 legacy bypass, so `handle_push_batch` now
/// requires `state.shard_handles` to be populated. Without
/// `spawn_shard_threads`, every send returns `ShardOverload` / dropped
/// and `r.is_ok()` is false for every event → 0 EPS.
fn make_state_with_shards(n_shards: u16) -> SharedState {
    // Use BEAVA_DATA_DIR so per-shard fjall partitions land in an
    // ephemeral tempdir rather than clobbering a user workspace.
    let tmp_data = tempfile::tempdir().unwrap();
    std::env::set_var("BEAVA_DATA_DIR", tmp_data.path());
    // Intentionally leak the TempDir: shard threads reference it for the
    // lifetime of the test and dropping it mid-run would rm -rf the
    // fjall partitions out from under them.
    Box::leak(Box::new(tmp_data));

    let mut engine = PipelineEngine::new();
    // Mirrors benchmark/fraud-pipeline 'complex' topology: primary stream
    // keyed on user_id, plus three fan-out tables keyed on independent
    // fields that appear in every event. Every PUSH touches ~4 shards —
    // this is the actual server shape we're trying to profile.
    engine
        .register(count_stream("Transactions", "user_id"))
        .unwrap();
    engine
        .register(count_stream("MerchantSummary", "merchant_id"))
        .unwrap();
    engine
        .register(count_stream("DeviceSummary", "device_id"))
        .unwrap();
    engine
        .register(count_stream("IPSummary", "ip_address"))
        .unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let state = make_concurrent_state_full(
        engine,
        None, // event_log off — we want pure in-memory hot path
        tmp.path().join("snap"),
        Arc::new(BackfillTracker::default()),
        false, // snapshot disabled
        false, // event_log disabled
        None,
        false,
        n_shards,
    );
    // Leak `tmp` for the same reason as tmp_data above.
    Box::leak(Box::new(tmp));

    // Phase 54-05: spawn_shard_threads populates state.shard_handles.
    // Without this, handle_push_batch drops every event.
    let handles =
        beava::shard::thread::spawn_shard_threads(n_shards.into(), 65_536, state.clone(), None);
    *state.shard_handles.write() = handles;
    state
}

fn make_state() -> SharedState {
    // Post-Wave-4: drive through 8 shards to match the fraud-pipeline
    // MODE=complex N=8 workload shape that baseline-N8-complex.json
    // captures. At N=1 the profile would under-sample the scatter-gather
    // + SPSC-transit costs that dominate post-refactor.
    make_state_with_shards(8)
}

/// Key generator. Defaults to Zipfian-1.2 (pathological hot-key).
/// Set `BENCH_UNIFORM_KEYS=1` to use uniform random keys — this is the
/// diagnostic: if lock_exclusive drops significantly under uniform
/// distribution, the 66% contention is hot-shard-specific, not
/// an architectural problem.
fn zipf_key(rng_state: &mut u64, prefix: &str, n: u64) -> String {
    *rng_state = rng_state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    let u = (*rng_state >> 33) as f64 / (1u64 << 31) as f64;
    // Uniform keys when BENCH_UNIFORM_KEYS=1. Cached via OnceLock to avoid
    // per-event env lookups.
    use std::sync::OnceLock;
    static UNIFORM: OnceLock<bool> = OnceLock::new();
    let uniform =
        *UNIFORM.get_or_init(|| std::env::var("BENCH_UNIFORM_KEYS").ok().as_deref() == Some("1"));
    let rank = if uniform {
        (u * n as f64) as u64 + 1
    } else {
        let alpha = 1.2_f64;
        ((u * (n as f64).powf(1.0 - alpha) + (1.0 - u)).powf(1.0 / (1.0 - alpha)))
            .max(1.0)
            .min(n as f64) as u64
    };
    format!("{}{:06}", prefix, rank)
}

fn synth_batch(thread_id: u64, seq_start: u64, n_events: usize) -> Vec<PendingAsync> {
    let mut rng = thread_id.wrapping_mul(0x9E3779B97F4A7C15);
    let now = SystemTime::now();
    let mut out = Vec::with_capacity(n_events);
    for i in 0..n_events {
        let user_id = zipf_key(&mut rng, "u", 10_000);
        let merchant_id = zipf_key(&mut rng, "m", 2_000);
        let device_id = zipf_key(&mut rng, "d", 5_000);
        let ip_address = zipf_key(&mut rng, "ip", 8_000);
        let payload = json!({
            "user_id": user_id,
            "merchant_id": merchant_id,
            "device_id": device_id,
            "ip_address": ip_address,
            "amount": (i % 100) as f64,
            "country": "US",
            "status": "success",
            "currency": "USD",
        });
        out.push(PendingAsync::new(
            seq_start + i as u64,
            "Transactions".into(),
            payload,
            Vec::new(),
            now,
        ));
    }
    out
}

#[test]
#[ignore]
fn profile_ingest_hot_path() {
    let state = make_state();
    const N_WORKERS: usize = 8;
    const DURATION_S: u64 = 8;
    const BATCH_SIZE: usize = 1000;

    // Start pprof at 997 Hz across all threads.
    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(250)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .expect("pprof start");

    let total_events = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let start = Instant::now();

    // Spawn N_WORKERS native OS threads hammering handle_push_batch. No
    // tokio in play — this isolates the server-side hot path from any
    // runtime scheduling overhead. Each thread owns its RNG + seq counter
    // so allocations interleave the way the real accept+spawn path does.
    let mut handles = Vec::with_capacity(N_WORKERS);
    for tid in 0..N_WORKERS {
        let state = state.clone();
        let total_events = total_events.clone();
        let stop = stop.clone();
        handles.push(thread::spawn(move || {
            let mut seq: u64 = tid as u64 * 1_000_000_000;
            let mut local_events: usize = 0;
            while !stop.load(Ordering::Relaxed) {
                let batch = synth_batch(tid as u64, seq, BATCH_SIZE);
                seq = seq.wrapping_add(BATCH_SIZE as u64);
                let results = handle_push_batch(&state, &batch);
                for r in &results {
                    if r.is_ok() {
                        local_events += 1;
                    }
                }
            }
            total_events.fetch_add(local_events, Ordering::Relaxed);
        }));
    }

    thread::sleep(Duration::from_secs(DURATION_S));
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    let elapsed = start.elapsed();
    let total = total_events.load(Ordering::Relaxed);
    let eps = total as f64 / elapsed.as_secs_f64();
    eprintln!(
        "\n=== workload ===\n  threads={} duration={:.2}s events={} → {:.0} EPS ({:.0} EPS/thread)",
        N_WORKERS,
        elapsed.as_secs_f64(),
        total,
        eps,
        eps / N_WORKERS as f64,
    );

    // Build report + flamegraph.
    let report = guard.report().build().expect("pprof report");

    let flame_path = "/tmp/beava_ingest.flamegraph.svg";
    {
        let f = std::fs::File::create(flame_path).unwrap();
        report.flamegraph(f).expect("flamegraph write");
    }
    eprintln!("\n=== flamegraph ===\n  {}", flame_path);

    // Dump text top-N by self-samples. pprof-rs's Report.data is a
    // HashMap<Frames, usize> where the Frames key is a stack trace and the
    // usize is the sample count for that exact stack. We aggregate by the
    // LEAF frame name — that's what shows which function burned the most
    // CPU on-CPU time.
    use std::collections::HashMap;
    let mut leaf_counts: HashMap<String, i64> = HashMap::new();
    let mut inclusive_counts: HashMap<String, i64> = HashMap::new();
    let mut total_samples: i64 = 0;
    for (frames, &count) in report.data.iter() {
        let count = count as i64;
        total_samples += count;
        if let Some(frame) = frames.frames.first() {
            if let Some(leaf) = frame.first() {
                let leaf: &pprof::Symbol = leaf;
                *leaf_counts.entry(leaf.name()).or_insert(0) += count;
            }
        }
        // Inclusive: every distinct symbol in the stack gets credited.
        let mut seen_in_stack: std::collections::HashSet<String> = std::collections::HashSet::new();
        for frame in &frames.frames {
            for sym in frame {
                let sym: &pprof::Symbol = sym;
                let name = sym.name();
                if seen_in_stack.insert(name.clone()) {
                    *inclusive_counts.entry(name).or_insert(0) += count;
                }
            }
        }
    }
    let mut leaf: Vec<(String, i64)> = leaf_counts.into_iter().collect();
    leaf.sort_by(|a, b| b.1.cmp(&a.1));
    let mut incl: Vec<(String, i64)> = inclusive_counts.into_iter().collect();
    incl.sort_by(|a, b| b.1.cmp(&a.1));

    let mut text_report = String::new();
    text_report.push_str(&format!(
        "# Beava ingest profile\n\nWorkload: {} threads, {:.2}s, {} events, {:.0} EPS total.\nSamples: {}\n\n",
        N_WORKERS, elapsed.as_secs_f64(), total, eps, total_samples
    ));
    text_report.push_str("## Top 40 leaf functions (self-samples, on-CPU time)\n\n");
    text_report.push_str("```\n");
    text_report.push_str(&format!(
        "{:>7}  {:>6}   {}\n",
        "samples", "self %", "function"
    ));
    text_report.push_str(&format!("{:-<7}  {:-<6}   {:-<80}\n", "", "", ""));
    for (name, count) in leaf.iter().take(40) {
        let pct = 100.0 * *count as f64 / total_samples.max(1) as f64;
        text_report.push_str(&format!("{:>7}  {:>5.1}%  {}\n", count, pct, name));
    }
    text_report.push_str("```\n\n## Top 40 functions by inclusive samples (time in this function OR anything it called)\n\n");
    text_report.push_str("```\n");
    text_report.push_str(&format!(
        "{:>7}  {:>6}   {}\n",
        "samples", "incl %", "function"
    ));
    text_report.push_str(&format!("{:-<7}  {:-<6}   {:-<80}\n", "", "", ""));
    for (name, count) in incl.iter().take(40) {
        let pct = 100.0 * *count as f64 / total_samples.max(1) as f64;
        text_report.push_str(&format!("{:>7}  {:>5.1}%  {}\n", count, pct, name));
    }
    let text_path = "/tmp/beava_ingest.top.txt";
    std::fs::write(text_path, &text_report).unwrap();
    eprintln!("  {}", text_path);

    // Also echo top 20 leaf functions inline so `cargo test --nocapture`
    // shows them without needing to open the file.
    eprintln!("\n=== top 20 leaf functions (self-samples) ===");
    for (name, count) in leaf.iter().take(20) {
        let pct = 100.0 * *count as f64 / total_samples.max(1) as f64;
        eprintln!("  {:>5.1}%  ({:>6} samples)  {}", pct, count, name);
    }
    eprintln!("\n=== top 20 by inclusive samples ===");
    for (name, count) in incl.iter().take(20) {
        let pct = 100.0 * *count as f64 / total_samples.max(1) as f64;
        eprintln!("  {:>5.1}%  ({:>6} samples)  {}", pct, count, name);
    }
}

/// Thread-per-core simulation: each thread owns its own isolated StateStore
/// and processes only keys in its partition. No DashMap sharing, no
/// cross-thread channels, no locks on the entity-state hot path. This is
/// the upper bound for what TPC could deliver — it measures whether the
/// 66% lock_exclusive ceiling in the shared-state design is actually
/// architectural or just contention-shaped.
///
/// Caveat: does NOT model cross-shard reshuffle cost (channel sends for
/// fan-out). The fraud pipeline has 100% cross-shard fan-out, so the real
/// TPC number for that shape would be this number minus ~800-2000 ns/event
/// of channel overhead (per earlier research).
#[test]
#[ignore]
fn profile_ingest_thread_per_core_simulation() {
    const N_WORKERS: usize = 8;
    const DURATION_S: u64 = 8;
    const BATCH_SIZE: usize = 1000;

    // One isolated state per worker — perfect partitioning, zero sharing.
    let states: Vec<SharedState> = (0..N_WORKERS).map(|_| make_state()).collect();

    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(250)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .expect("pprof start");

    let total_events = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let start = Instant::now();

    let mut handles = Vec::with_capacity(N_WORKERS);
    for (tid, state_entry) in states.iter().enumerate().take(N_WORKERS) {
        let state = state_entry.clone();
        let total_events = total_events.clone();
        let stop = stop.clone();
        handles.push(thread::spawn(move || {
            let mut seq: u64 = tid as u64 * 1_000_000_000;
            let mut local_events: usize = 0;
            while !stop.load(Ordering::Relaxed) {
                let batch = synth_batch(tid as u64, seq, BATCH_SIZE);
                seq = seq.wrapping_add(BATCH_SIZE as u64);
                let results = handle_push_batch(&state, &batch);
                for r in &results {
                    if r.is_ok() {
                        local_events += 1;
                    }
                }
            }
            total_events.fetch_add(local_events, Ordering::Relaxed);
        }));
    }

    thread::sleep(Duration::from_secs(DURATION_S));
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    let elapsed = start.elapsed();
    let total = total_events.load(Ordering::Relaxed);
    let eps = total as f64 / elapsed.as_secs_f64();
    eprintln!(
        "\n=== TPC simulation ===\n  threads={} (isolated states) duration={:.2}s events={} → {:.0} EPS ({:.0} EPS/thread)",
        N_WORKERS,
        elapsed.as_secs_f64(),
        total,
        eps,
        eps / N_WORKERS as f64,
    );

    let report = guard.report().build().expect("pprof report");
    use std::collections::HashMap;
    let mut leaf_counts: HashMap<String, i64> = HashMap::new();
    let mut total_samples: i64 = 0;
    for (frames, &count) in report.data.iter() {
        let count = count as i64;
        total_samples += count;
        if let Some(frame) = frames.frames.first() {
            if let Some(leaf) = frame.first() {
                let leaf: &pprof::Symbol = leaf;
                *leaf_counts.entry(leaf.name()).or_insert(0) += count;
            }
        }
    }
    let mut leaf: Vec<(String, i64)> = leaf_counts.into_iter().collect();
    leaf.sort_by(|a, b| b.1.cmp(&a.1));
    eprintln!("\n=== TPC top 15 leaf functions ===");
    for (name, count) in leaf.iter().take(15) {
        let pct = 100.0 * *count as f64 / total_samples.max(1) as f64;
        eprintln!("  {:>5.1}%  ({:>6} samples)  {}", pct, count, name);
    }
}
