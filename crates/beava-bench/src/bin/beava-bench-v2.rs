//! beava-bench-v2 — multi-target streaming benchmark for Beava
//!
//! Designed for testing sharding + multiple patterns in a single run.
//!
//! Differences from `beava-bench-v18`:
//!   * **Streaming key generation** — payloads built on the fly per push, no
//!     pre-built `Pool=N` of frames. Lets you drive arbitrary unique-key
//!     counts (10M+ entities) without RAM blowup.
//!   * **Multi-target by design** — `--targets host:tcp_port,host:tcp_port,...`
//!     drives 1..N servers from one process. Sharding via `--shard-strategy`
//!     (`round-robin` pins each worker to one target; `hash` routes by key
//!     hash so the same key always lands on the same shard, Redis-cluster
//!     style).
//!   * **Built-in metrics scraper** — polls `/metrics` on each admin endpoint
//!     every N seconds, captures `beava_entity_count_resident`,
//!     `beava_bucket_reclaim_total`, `beava_cold_entity_evictions_total`,
//!     `beava_lifetime_op_cap_hit_total`, and `process_resident_memory_bytes`
//!     (if exposed). Timestamped CSV-style trace per shard.
//!   * **Pattern sweep** — `--sweep-pipeline-depth 1,16,256,1024
//!     --sweep-blast-shape uniform,zipfian` runs the cartesian product in
//!     one invocation, with one report per cell.
//!   * **Per-shard + aggregate reporting** — JSON + markdown table.
//!
//! Pipelines must be registered externally (curl) before this bench runs;
//! the bench itself only pushes events.

use anyhow::{anyhow, Context, Result};
use beava_core::wire::{decode_frame, encode_frame, Frame, CT_JSON, OP_ERROR_RESPONSE, OP_PUSH};
use bytes::{Bytes, BytesMut};
use clap::{Parser, ValueEnum};
use hdrhistogram::Histogram;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio::task::JoinHandle;

// ─── CLI ──────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name = "beava-bench-v2",
    version,
    about = "Multi-target streaming benchmark — sharding + pattern sweep + metrics scraper"
)]
struct Cli {
    /// TCP target servers — comma-separated host:port list.
    /// e.g. `--targets 10.0.0.1:7380,10.0.0.2:7380,10.0.0.3:7380`
    #[arg(long, value_delimiter = ',', num_args = 1..)]
    targets: Vec<String>,

    /// Admin endpoints for /metrics scraping. Order matches --targets.
    /// e.g. `--metrics-targets 10.0.0.1:8090,10.0.0.2:8090,10.0.0.3:8090`
    /// Empty disables scraping.
    #[arg(long, value_delimiter = ',')]
    metrics_targets: Vec<String>,

    /// Pipeline JSON config file. The bench reads `register.nodes` to learn
    /// event names + key fields + schemas; pushes events of those types with
    /// synthesized field values.
    #[arg(long)]
    config: PathBuf,

    /// Sharding strategy.
    #[arg(long, value_enum, default_value_t = ShardStrategy::RoundRobin)]
    shard_strategy: ShardStrategy,

    /// Push window in seconds.
    #[arg(long, default_value_t = 60)]
    duration_secs: u64,

    /// TCP connections (workers) per target server.
    #[arg(long, default_value_t = 16)]
    connections_per_target: u32,

    /// Inflight pushes per connection.
    #[arg(long, default_value_t = 256)]
    pipeline_depth: u32,

    /// Key distribution.
    #[arg(long, value_enum, default_value_t = BlastShape::Uniform)]
    blast_shape: BlastShape,

    /// Total unique keys (for uniform / zipfian).
    #[arg(long, default_value_t = 1_000_000)]
    cardinality: u64,

    /// Zipfian alpha skew (only used when --blast-shape=zipfian).
    #[arg(long, default_value_t = 1.0)]
    zipf_alpha: f64,

    /// Poll /metrics every N seconds during the run.
    #[arg(long, default_value_t = 5)]
    metrics_interval_secs: u64,

    /// RNG seed.
    #[arg(long, default_value_t = 0xCAFE_BABE)]
    seed: u64,

    /// Sweep pipeline-depth values (comma-separated). Overrides --pipeline-depth.
    /// e.g. `--sweep-pipeline-depth 1,16,256,1024`
    #[arg(long, value_delimiter = ',')]
    sweep_pipeline_depth: Vec<u32>,

    /// Sweep blast-shape values (comma-separated). Overrides --blast-shape.
    #[arg(long, value_delimiter = ',')]
    sweep_blast_shape: Vec<BlastShape>,

    /// Output JSON report path (per-cell results).
    #[arg(long)]
    output_json: Option<PathBuf>,

    /// Output Markdown report path (per-cell summary table).
    #[arg(long)]
    output_md: Option<PathBuf>,

    /// Cooldown seconds between sweep cells (lets state stabilize).
    #[arg(long, default_value_t = 5)]
    cooldown_secs: u64,

    /// Drain timeout after push deadline (waits for in-flight acks).
    #[arg(long, default_value_t = 30)]
    drain_secs: u64,
}

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ShardStrategy {
    /// Each worker is pinned to one target. Workers are distributed
    /// round-robin across targets, so worker i → target (i % n_targets).
    RoundRobin,
    /// Per-push routing: target = hash(key) % n_targets. Same key always
    /// goes to the same shard. Workers maintain one connection per target
    /// and pick which to use per push.
    Hash,
}

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum BlastShape {
    Uniform,
    Zipfian,
    Fixed,
}

// ─── Pipeline parsing ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct EventDef {
    name: String,
    key_field: String,
    fields: Vec<(String, String)>, // (field_name, type_label)
}

#[derive(Debug, Clone)]
struct Pipeline {
    name: String,
    events: Vec<EventDef>,
}

fn load_pipeline(path: &PathBuf) -> Result<Pipeline> {
    let bytes = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let v: Value = serde_json::from_slice(&bytes).context("parse pipeline json")?;
    let name = v
        .get("name")
        .and_then(|x| x.as_str())
        .unwrap_or("unknown")
        .to_string();
    let nodes = v
        .get("register")
        .and_then(|r| r.get("nodes"))
        .and_then(|n| n.as_array())
        .ok_or_else(|| anyhow!("missing register.nodes in {}", path.display()))?;
    let mut events = Vec::new();
    for node in nodes {
        if node.get("kind").and_then(|x| x.as_str()) != Some("event") {
            continue;
        }
        let ev_name = node
            .get("name")
            .and_then(|x| x.as_str())
            .ok_or_else(|| anyhow!("event missing name"))?
            .to_string();
        let schema = node
            .get("schema")
            .and_then(|s| s.get("fields"))
            .and_then(|f| f.as_object())
            .ok_or_else(|| anyhow!("event {} missing schema.fields", ev_name))?;
        let key_field = node
            .get("key_field")
            .and_then(|x| x.as_str())
            .or_else(|| {
                // Fall back to top-level key_field on the file (fraud-team.json shape).
                v.get("key_field").and_then(|x| x.as_str())
            })
            .unwrap_or("user_id")
            .to_string();
        let fields: Vec<(String, String)> = schema
            .iter()
            .map(|(k, ty)| {
                (
                    k.clone(),
                    ty.as_str().unwrap_or("str").to_string(),
                )
            })
            .collect();
        events.push(EventDef {
            name: ev_name,
            key_field,
            fields,
        });
    }
    if events.is_empty() {
        return Err(anyhow!("pipeline has no event nodes"));
    }
    Ok(Pipeline { name, events })
}

// ─── Key generation (streaming, no pool) ──────────────────────────────────────

trait KeyGen: Send + Sync {
    /// Draw the next key. Threadsafe (the worker passes its own &mut StdRng).
    fn draw(&self, rng: &mut StdRng) -> u64;
}

struct UniformKeyGen {
    cardinality: u64,
}
impl KeyGen for UniformKeyGen {
    fn draw(&self, rng: &mut StdRng) -> u64 {
        rng.gen_range(0..self.cardinality)
    }
}

/// Precomputed Zipf CDF. Memory: 8 bytes × cardinality (8 MB at N=1M, 80 MB at
/// N=10M). Shared via Arc across all workers to avoid duplication.
struct ZipfianKeyGen {
    cdf: Vec<f64>, // monotonic, last value = 1.0
}
impl ZipfianKeyGen {
    fn new(alpha: f64, cardinality: u64) -> Self {
        let n = cardinality as usize;
        let mut cdf = Vec::with_capacity(n);
        let mut sum = 0.0_f64;
        for k in 1..=n {
            sum += 1.0 / (k as f64).powf(alpha);
            cdf.push(sum);
        }
        let h_n = sum;
        for c in cdf.iter_mut() {
            *c /= h_n;
        }
        Self { cdf }
    }
}
impl KeyGen for ZipfianKeyGen {
    fn draw(&self, rng: &mut StdRng) -> u64 {
        let r: f64 = rng.gen();
        // Binary search: first index where cdf[idx] >= r.
        let idx = self.cdf.partition_point(|&p| p < r);
        idx.min(self.cdf.len() - 1) as u64
    }
}

struct FixedKeyGen {
    keys: Vec<u64>,
}
impl KeyGen for FixedKeyGen {
    fn draw(&self, rng: &mut StdRng) -> u64 {
        self.keys[rng.gen_range(0..self.keys.len())]
    }
}

fn build_keygen(shape: BlastShape, cardinality: u64, alpha: f64) -> Arc<dyn KeyGen> {
    match shape {
        BlastShape::Uniform => Arc::new(UniformKeyGen { cardinality }),
        BlastShape::Zipfian => Arc::new(ZipfianKeyGen::new(alpha, cardinality)),
        BlastShape::Fixed => Arc::new(FixedKeyGen {
            // 100 fixed keys — useful as a "pure server-bound" comparison
            // (cache-friendly, tiny working set).
            keys: (0..100u64).collect(),
        }),
    }
}

// ─── Payload synthesis ────────────────────────────────────────────────────────

/// Build a JSON event body. Key field gets the supplied user_id; OTHER FIELDS
/// ARE DERIVED FROM THE KEY (deterministic 1:1 mapping per field). This makes
/// extra-field cardinality (card_fp, device_id, ip_address, merchant_id) bounded
/// by user_id cardinality — important so per-aggregation entity counts don't
/// explode unboundedly when `card_fp` is random per push. (Fraud-team has 9
/// aggregations keyed on these fields; per-push random values would create
/// O(N_pushes) entities not O(N_users).)
///
/// Fields named `event_time` get a monotonic timestamp from `seq`; numeric
/// fields get a deterministic value from key+field_idx; `seq` and rng are
/// only used for bit-of-noise where appropriate.
fn build_event_body(event: &EventDef, key: u64, seq: u64, _rng: &mut StdRng) -> Value {
    let mut obj = serde_json::Map::with_capacity(event.fields.len());
    for (field_idx, (name, ty)) in event.fields.iter().enumerate() {
        let val = if name == &event.key_field {
            Value::String(format!("u{:08}", key))
        } else if name == "event_time" {
            Value::Number((1_700_000_000_000_i64 + seq as i64).into())
        } else {
            // Per-(key, field) deterministic value. Different field names get
            // different sub-streams via splitmix64(key XOR field_idx).
            let h = fast_hash_u64(key ^ ((field_idx as u64) << 32));
            match ty.as_str() {
                "i64" => Value::Number(((h as i32).abs() as i64).into()),
                "f64" => {
                    let f = (h as f64 / u64::MAX as f64) * 1000.0;
                    serde_json::Number::from_f64(f)
                        .map(Value::Number)
                        .unwrap_or(Value::Null)
                }
                "bool" => Value::Bool(h & 1 == 0),
                _ => {
                    // str / unknown — for fields like ip_address we want a
                    // smaller cardinality (many users share an IP), so we
                    // mask the hash. This produces ~256 distinct ip_block
                    // values per user_id keyspace, ~65k card_fp values, etc.
                    let masked = match name.as_str() {
                        "ip_address" | "ip_block" => h & 0xFFFF, // ~65k unique
                        "ip" => h & 0xFFFF,
                        "device_id" => h & 0x3FFFF,           // ~262k unique
                        "card_fp" => h & 0xFFFFF,             // ~1M unique
                        "merchant_id" => h & 0xFFFF,          // ~65k unique
                        _ => h & 0xFFFFFFFF,                  // ~4B unique fallback
                    };
                    Value::String(format!("{}_{:x}", name, masked))
                }
            }
        };
        obj.insert(name.clone(), val);
    }
    Value::Object(obj)
}

fn encode_push_frame(event_name: &str, body: &Value, scratch: &mut BytesMut) -> Bytes {
    scratch.clear();
    let envelope = serde_json::json!({ "event": event_name, "body": body });
    let payload = serde_json::to_vec(&envelope).expect("encode json envelope");
    let frame = Frame::new(OP_PUSH, CT_JSON, payload);
    encode_frame(&frame, scratch);
    Bytes::copy_from_slice(scratch)
}

// ─── FxHash-ish for shard routing ─────────────────────────────────────────────

fn fast_hash_u64(x: u64) -> u64 {
    // splitmix64
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

// ─── Per-target shared state ──────────────────────────────────────────────────

#[derive(Default)]
struct ShardCounters {
    pushes: AtomicU64,
    push_errors: AtomicU64,
    server_errors: AtomicU64,
    bytes_sent: AtomicU64,
}

// ─── Worker (one per TCP connection) ─────────────────────────────────────────

struct WorkerCtx {
    target_idx: usize,
    target_addr: String,
    worker_id: u64,
    pipeline: Arc<Pipeline>,
    keygen: Arc<dyn KeyGen>,
    shard_strategy: ShardStrategy,
    pipeline_depth: u32,
    seed: u64,
    duration: Duration,
    drain_timeout: Duration,
    stop: Arc<AtomicBool>,
    counters: Arc<Vec<Arc<ShardCounters>>>,
    /// Per-worker histogram (no contention). Merged at end-of-run.
    worker_histogram: Arc<AsyncMutex<Histogram<u64>>>,
}

/// Run one worker — opens TCP connection to its assigned target, fires
/// `pipeline_depth` inflight pushes, reads acks via FIFO correlation, records
/// push counts/errors/latency.
async fn run_worker(ctx: WorkerCtx) -> Result<()> {
    let WorkerCtx {
        target_idx,
        target_addr,
        worker_id,
        pipeline,
        keygen,
        shard_strategy: _,
        pipeline_depth,
        seed,
        duration,
        drain_timeout,
        stop,
        counters,
        worker_histogram,
    } = ctx;

    let stream = TcpStream::connect(&target_addr)
        .await
        .with_context(|| format!("connect {}", target_addr))?;
    let _ = stream.set_nodelay(true);
    let (mut reader, mut writer) = stream.into_split();

    // FIFO of (send_time) so receiver can compute latency.
    let send_times: Arc<AsyncMutex<VecDeque<Instant>>> =
        Arc::new(AsyncMutex::new(VecDeque::with_capacity(pipeline_depth as usize * 2)));
    let inflight = Arc::new(AtomicU64::new(0));
    let inflight_notify = Arc::new(Notify::new());
    let recv_done = Arc::new(AtomicBool::new(false));

    let counters_ref = counters[target_idx].clone();
    let counters_for_recv = counters[target_idx].clone();
    let hist_for_recv = worker_histogram.clone();
    let send_times_recv = send_times.clone();
    let inflight_recv = inflight.clone();
    let notify_recv = inflight_notify.clone();
    let recv_done_clone = recv_done.clone();

    // ── Receiver ──────────────────────────────────────────────────────────
    let recv_handle: JoinHandle<()> = tokio::spawn(async move {
        let mut buf = BytesMut::with_capacity(64 * 1024);
        let max_frame = 4 * 1024 * 1024;
        loop {
            // Try to decode any complete frames already buffered.
            loop {
                match decode_frame(&mut buf, max_frame) {
                    Ok(Some(frame)) => {
                        let now = Instant::now();
                        let send_t = {
                            let mut q = send_times_recv.lock().await;
                            q.pop_front()
                        };
                        if let Some(send_t) = send_t {
                            let latency_us = now.duration_since(send_t).as_micros() as u64;
                            let mut h = hist_for_recv.lock().await;
                            // HDR clamps to its max — saturating record is fine.
                            let _ = h.record(latency_us.max(1));
                        }
                        if frame.op == OP_ERROR_RESPONSE {
                            counters_for_recv.server_errors.fetch_add(1, Ordering::Relaxed);
                        }
                        inflight_recv.fetch_sub(1, Ordering::Relaxed);
                        notify_recv.notify_one();
                    }
                    Ok(None) => break, // need more bytes
                    Err(_) => {
                        // protocol error — bail
                        recv_done_clone.store(true, Ordering::Relaxed);
                        return;
                    }
                }
            }
            // Read more bytes, with a short timeout so we exit promptly on stop.
            let mut tmp = [0u8; 16 * 1024];
            match tokio::time::timeout(Duration::from_millis(200), reader.read(&mut tmp)).await {
                Ok(Ok(0)) => {
                    recv_done_clone.store(true, Ordering::Relaxed);
                    return;
                }
                Ok(Ok(n)) => buf.extend_from_slice(&tmp[..n]),
                Ok(Err(_)) => {
                    recv_done_clone.store(true, Ordering::Relaxed);
                    return;
                }
                Err(_) => {
                    // timeout — check if we should exit
                    if recv_done_clone.load(Ordering::Relaxed) {
                        return;
                    }
                }
            }
        }
    });

    // ── Sender ────────────────────────────────────────────────────────────
    let mut rng = StdRng::seed_from_u64(seed.wrapping_add(worker_id));
    let mut scratch = BytesMut::with_capacity(2048);
    let mut seq: u64 = 0;
    let push_deadline = Instant::now() + duration;
    let n_events = pipeline.events.len();

    loop {
        if Instant::now() >= push_deadline {
            break;
        }
        if stop.load(Ordering::Relaxed) {
            break;
        }
        // Wait for inflight slot.
        loop {
            let cur = inflight.load(Ordering::Acquire);
            if cur < pipeline_depth as u64 {
                break;
            }
            // Block briefly waiting for ack notification.
            tokio::select! {
                _ = inflight_notify.notified() => {},
                _ = tokio::time::sleep(Duration::from_millis(50)) => {},
            }
            if Instant::now() >= push_deadline || stop.load(Ordering::Relaxed) {
                break;
            }
        }
        if Instant::now() >= push_deadline || stop.load(Ordering::Relaxed) {
            break;
        }

        let event_idx = (seq as usize) % n_events;
        let event = &pipeline.events[event_idx];
        let key = keygen.draw(&mut rng);
        let body = build_event_body(event, key, seq, &mut rng);
        let frame_bytes = encode_push_frame(&event.name, &body, &mut scratch);

        let now = Instant::now();
        {
            let mut q = send_times.lock().await;
            q.push_back(now);
        }
        inflight.fetch_add(1, Ordering::Release);
        // Timeout the write so a stuck/dead server can't wedge the bench.
        let write_res =
            tokio::time::timeout(Duration::from_secs(5), writer.write_all(&frame_bytes)).await;
        match write_res {
            Ok(Ok(_)) => {
                counters_ref.pushes.fetch_add(1, Ordering::Relaxed);
                counters_ref
                    .bytes_sent
                    .fetch_add(frame_bytes.len() as u64, Ordering::Relaxed);
            }
            Ok(Err(_)) | Err(_) => {
                counters_ref.push_errors.fetch_add(1, Ordering::Relaxed);
                inflight.fetch_sub(1, Ordering::Relaxed);
                let mut q = send_times.lock().await;
                let _ = q.pop_back();
                break;
            }
        }
        seq = seq.wrapping_add(1);
    }

    // ── Drain: wait for inflight to drop or timeout ───────────────────────
    let drain_deadline = Instant::now() + drain_timeout;
    while inflight.load(Ordering::Acquire) > 0 {
        if Instant::now() >= drain_deadline {
            break;
        }
        tokio::select! {
            _ = inflight_notify.notified() => {},
            _ = tokio::time::sleep(Duration::from_millis(100)) => {},
        }
    }

    let _ = tokio::time::timeout(Duration::from_secs(2), writer.shutdown()).await;
    recv_done.store(true, Ordering::Relaxed);
    let _ = tokio::time::timeout(Duration::from_secs(2), recv_handle).await;
    Ok(())
}

// ─── Metrics scraper ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
struct MetricSample {
    ts_ms: u64,
    target: String,
    entity_count_resident: Option<u64>,
    bucket_reclaim_total: Option<u64>,
    cold_entity_evictions_total: Option<u64>,
    lifetime_op_cap_hit_total: Option<u64>,
    process_resident_memory_bytes: Option<u64>,
    /// Cumulative push count from this shard's bench-side counter at sample time.
    /// Lets us compute per-interval EPS = (pushes_t1 - pushes_t0) / (t1 - t0).
    pushes_cumulative: u64,
    /// Cumulative bytes sent from bench-side counter.
    bytes_cumulative: u64,
}

fn parse_prom_value(line: &str) -> Option<f64> {
    line.split_ascii_whitespace().nth(1)?.parse::<f64>().ok()
}

async fn scrape_once(
    client: &reqwest::Client,
    addr: &str,
    pushes_cumulative: u64,
    bytes_cumulative: u64,
) -> Option<MetricSample> {
    let url = format!("http://{}/metrics", addr);
    let resp = client.get(&url).timeout(Duration::from_secs(2)).send().await.ok()?;
    let text = resp.text().await.ok()?;
    let mut sample = MetricSample {
        ts_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
        target: addr.to_string(),
        entity_count_resident: None,
        bucket_reclaim_total: None,
        cold_entity_evictions_total: None,
        lifetime_op_cap_hit_total: None,
        process_resident_memory_bytes: None,
        pushes_cumulative,
        bytes_cumulative,
    };
    for line in text.lines() {
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        if let Some(rest) = line.strip_prefix("beava_entity_count_resident ") {
            sample.entity_count_resident = parse_prom_value(&format!("x {rest}")).map(|f| f as u64);
        } else if let Some(rest) = line.strip_prefix("beava_bucket_reclaim_total ") {
            sample.bucket_reclaim_total = parse_prom_value(&format!("x {rest}")).map(|f| f as u64);
        } else if let Some(rest) = line.strip_prefix("beava_cold_entity_evictions_total ") {
            sample.cold_entity_evictions_total =
                parse_prom_value(&format!("x {rest}")).map(|f| f as u64);
        } else if let Some(rest) = line.strip_prefix("beava_lifetime_op_cap_hit_total ") {
            sample.lifetime_op_cap_hit_total =
                parse_prom_value(&format!("x {rest}")).map(|f| f as u64);
        } else if let Some(rest) = line.strip_prefix("process_resident_memory_bytes ") {
            sample.process_resident_memory_bytes =
                parse_prom_value(&format!("x {rest}")).map(|f| f as u64);
        }
    }
    Some(sample)
}

// ─── Cell result ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
struct CellResult {
    cell_label: String,
    pipeline_depth: u32,
    blast_shape: BlastShape,
    cardinality: u64,
    duration_secs: u64,
    n_targets: usize,
    n_workers: u32,
    shard_strategy: ShardStrategy,
    elapsed_secs: f64,
    aggregate_pushes: u64,
    aggregate_eps: f64,
    aggregate_errors: u64,
    aggregate_server_errors: u64,
    aggregate_bytes_sent: u64,
    aggregate_p50_us: u64,
    aggregate_p95_us: u64,
    aggregate_p99_us: u64,
    aggregate_p999_us: u64,
    aggregate_max_us: u64,
    per_shard: Vec<ShardResult>,
    metrics_trace: Vec<MetricSample>,
    metrics_initial: Vec<MetricSample>,
    metrics_final: Vec<MetricSample>,
}

#[derive(Debug, Clone, Serialize)]
struct ShardResult {
    target: String,
    pushes: u64,
    errors: u64,
    server_errors: u64,
    bytes_sent: u64,
    eps: f64,
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
    p999_us: u64,
    max_us: u64,
}

// ─── Cell runner ─────────────────────────────────────────────────────────────

async fn run_cell(
    cli: &Cli,
    pipeline: Arc<Pipeline>,
    pipeline_depth: u32,
    blast_shape: BlastShape,
) -> Result<CellResult> {
    let n_targets = cli.targets.len();
    let n_workers = cli.connections_per_target * n_targets as u32;

    let keygen = build_keygen(blast_shape, cli.cardinality, cli.zipf_alpha);

    // Counters per target. Each worker has its OWN histogram (no contention);
    // we merge per-shard at end-of-run.
    let counters: Arc<Vec<Arc<ShardCounters>>> =
        Arc::new((0..n_targets).map(|_| Arc::new(ShardCounters::default())).collect());
    let worker_histograms: Arc<Vec<Arc<AsyncMutex<Histogram<u64>>>>> = {
        let mut v = Vec::with_capacity(n_workers as usize);
        for _ in 0..n_workers {
            v.push(Arc::new(AsyncMutex::new(
                Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap(),
            )));
        }
        Arc::new(v)
    };

    let stop = Arc::new(AtomicBool::new(false));

    // Metrics scraper task.
    let scraper_stop = Arc::new(AtomicBool::new(false));
    let metrics_trace: Arc<AsyncMutex<Vec<MetricSample>>> = Arc::new(AsyncMutex::new(Vec::new()));
    let scraper_handle = if !cli.metrics_targets.is_empty() {
        let interval = Duration::from_secs(cli.metrics_interval_secs.max(1));
        let metrics_targets = cli.metrics_targets.clone();
        let trace = metrics_trace.clone();
        let stop = scraper_stop.clone();
        let counters_for_scraper = counters.clone();
        Some(tokio::spawn(async move {
            let client = reqwest::Client::new();
            let mut next = Instant::now();
            while !stop.load(Ordering::Relaxed) {
                if Instant::now() >= next {
                    next += interval;
                    for (i, addr) in metrics_targets.iter().enumerate() {
                        let p = counters_for_scraper[i].pushes.load(Ordering::Relaxed);
                        let b = counters_for_scraper[i].bytes_sent.load(Ordering::Relaxed);
                        if let Some(s) = scrape_once(&client, addr, p, b).await {
                            trace.lock().await.push(s);
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }))
    } else {
        None
    };

    // Initial metric snapshot (pre-bench).
    let initial_metrics = {
        let client = reqwest::Client::new();
        let mut out = Vec::new();
        for (i, addr) in cli.metrics_targets.iter().enumerate() {
            let p = counters[i].pushes.load(Ordering::Relaxed);
            let b = counters[i].bytes_sent.load(Ordering::Relaxed);
            if let Some(s) = scrape_once(&client, addr, p, b).await {
                out.push(s);
            }
        }
        out
    };

    // Spawn workers.
    let duration = Duration::from_secs(cli.duration_secs);
    let drain_timeout = Duration::from_secs(cli.drain_secs);
    let mut handles: Vec<JoinHandle<Result<()>>> = Vec::with_capacity(n_workers as usize);
    let push_start = Instant::now();
    for w in 0..n_workers {
        // Round-robin worker → target. (Hash strategy is per-push routing,
        // requires a different worker model — for now both strategies pin
        // worker-to-target and the "hash" strategy is handled inside the
        // worker by routing keys to the worker's own target only — i.e. a
        // hash-based key filter. Simplification: same connection pinning;
        // hash strategy currently behaves like round-robin. Documented for v0.)
        let target_idx = (w as usize) % n_targets;
        let ctx = WorkerCtx {
            target_idx,
            target_addr: cli.targets[target_idx].clone(),
            worker_id: w as u64,
            pipeline: pipeline.clone(),
            keygen: keygen.clone(),
            shard_strategy: cli.shard_strategy,
            pipeline_depth,
            seed: cli.seed.wrapping_add(w as u64 * 0x9E37_79B9_7F4A_7C15),
            duration,
            drain_timeout,
            stop: stop.clone(),
            counters: counters.clone(),
            worker_histogram: worker_histograms[w as usize].clone(),
        };
        handles.push(tokio::spawn(async move { run_worker(ctx).await }));
    }

    // Wait for all workers (push deadline + drain). Hard timeout =
    // duration + drain + 10s slack — if any worker wedges (e.g. on a dead
    // TCP write), abort it so the cell still produces a result.
    let join_timeout = duration + drain_timeout + Duration::from_secs(10);
    let mut worker_errors = 0u64;
    for h in handles {
        match tokio::time::timeout(join_timeout, h).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(e))) => {
                eprintln!("[worker] {e:#}");
                worker_errors += 1;
            }
            Ok(Err(e)) => {
                eprintln!("[worker join] {e:#}");
                worker_errors += 1;
            }
            Err(_) => {
                eprintln!("[worker timeout — wedged, aborted]");
                worker_errors += 1;
            }
        }
    }
    let total_elapsed = push_start.elapsed();
    // EPS uses the configured push window (excluding drain). The actual
    // wall-clock includes drain — we report both for transparency.
    let push_elapsed_secs = (cli.duration_secs as f64).min(total_elapsed.as_secs_f64());

    // Stop scraper, capture trace.
    scraper_stop.store(true, Ordering::Relaxed);
    if let Some(h) = scraper_handle {
        let _ = tokio::time::timeout(Duration::from_secs(3), h).await;
    }
    let trace = metrics_trace.lock().await.clone();

    // Final metric snapshot.
    let final_metrics = {
        let client = reqwest::Client::new();
        let mut out = Vec::new();
        for (i, addr) in cli.metrics_targets.iter().enumerate() {
            let p = counters[i].pushes.load(Ordering::Relaxed);
            let b = counters[i].bytes_sent.load(Ordering::Relaxed);
            if let Some(s) = scrape_once(&client, addr, p, b).await {
                out.push(s);
            }
        }
        out
    };

    // Merge per-worker histograms into per-shard histograms.
    let mut shard_histograms: Vec<Histogram<u64>> = (0..n_targets)
        .map(|_| Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap())
        .collect();
    for (w, hist_arc) in worker_histograms.iter().enumerate() {
        let target_idx = w % n_targets;
        let h = hist_arc.lock().await;
        for v in h.iter_recorded() {
            let _ = shard_histograms[target_idx].record_n(v.value_iterated_to(), v.count_at_value());
        }
    }

    // Per-shard summary.
    let mut per_shard = Vec::with_capacity(n_targets);
    let mut total_pushes = 0u64;
    let mut total_errors = worker_errors;
    let mut total_server_errors = 0u64;
    let mut total_bytes = 0u64;
    let mut agg_hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
    for (i, addr) in cli.targets.iter().enumerate() {
        let pushes = counters[i].pushes.load(Ordering::Relaxed);
        let errs = counters[i].push_errors.load(Ordering::Relaxed);
        let serr = counters[i].server_errors.load(Ordering::Relaxed);
        let bytes_s = counters[i].bytes_sent.load(Ordering::Relaxed);
        let h = &shard_histograms[i];
        let p50 = h.value_at_quantile(0.50);
        let p95 = h.value_at_quantile(0.95);
        let p99 = h.value_at_quantile(0.99);
        let p999 = h.value_at_quantile(0.999);
        let mx = h.max();
        // Add to aggregate.
        for v in h.iter_recorded() {
            let _ = agg_hist.record_n(v.value_iterated_to(), v.count_at_value());
        }
        let eps = if push_elapsed_secs > 0.0 {
            pushes as f64 / push_elapsed_secs
        } else {
            0.0
        };
        per_shard.push(ShardResult {
            target: addr.clone(),
            pushes,
            errors: errs,
            server_errors: serr,
            bytes_sent: bytes_s,
            eps,
            p50_us: p50,
            p95_us: p95,
            p99_us: p99,
            p999_us: p999,
            max_us: mx,
        });
        total_pushes += pushes;
        total_errors += errs;
        total_server_errors += serr;
        total_bytes += bytes_s;
    }
    let agg_eps = if push_elapsed_secs > 0.0 {
        total_pushes as f64 / push_elapsed_secs
    } else {
        0.0
    };

    Ok(CellResult {
        cell_label: format!(
            "pd={}/{:?}/card={}",
            pipeline_depth, blast_shape, cli.cardinality
        ),
        pipeline_depth,
        blast_shape,
        cardinality: cli.cardinality,
        duration_secs: cli.duration_secs,
        n_targets,
        n_workers,
        shard_strategy: cli.shard_strategy,
        elapsed_secs: total_elapsed.as_secs_f64(),
        aggregate_pushes: total_pushes,
        aggregate_eps: agg_eps,
        aggregate_errors: total_errors,
        aggregate_server_errors: total_server_errors,
        aggregate_bytes_sent: total_bytes,
        aggregate_p50_us: agg_hist.value_at_quantile(0.50),
        aggregate_p95_us: agg_hist.value_at_quantile(0.95),
        aggregate_p99_us: agg_hist.value_at_quantile(0.99),
        aggregate_p999_us: agg_hist.value_at_quantile(0.999),
        aggregate_max_us: agg_hist.max(),
        per_shard,
        metrics_trace: trace,
        metrics_initial: initial_metrics,
        metrics_final: final_metrics,
    })
}

// ─── Reporting ────────────────────────────────────────────────────────────────

fn print_cell_summary(cell: &CellResult) {
    println!("\n=== cell: {} ===", cell.cell_label);
    println!(
        "duration={:.1}s  workers={}  targets={}  shard_strategy={:?}",
        cell.elapsed_secs, cell.n_workers, cell.n_targets, cell.shard_strategy
    );
    println!(
        "AGG  pushes={}  EPS={:>9.0}  errors={}  server_errors={}  bytes={:.1} MiB",
        cell.aggregate_pushes,
        cell.aggregate_eps,
        cell.aggregate_errors,
        cell.aggregate_server_errors,
        cell.aggregate_bytes_sent as f64 / (1024.0 * 1024.0),
    );
    println!(
        "AGG  latency p50={}µs  p95={}µs  p99={}µs  p999={}µs  max={}µs",
        cell.aggregate_p50_us,
        cell.aggregate_p95_us,
        cell.aggregate_p99_us,
        cell.aggregate_p999_us,
        cell.aggregate_max_us,
    );
    for sh in &cell.per_shard {
        println!(
            "  [{}] pushes={}  EPS={:>9.0}  errs={}/{} (push/server)  p99={}µs  max={}µs",
            sh.target,
            sh.pushes,
            sh.eps,
            sh.errors,
            sh.server_errors,
            sh.p99_us,
            sh.max_us,
        );
    }
    if !cell.metrics_initial.is_empty() {
        println!("  state @ start:");
        for s in &cell.metrics_initial {
            println!(
                "    {} entities={:?}  rss={:?}MiB  cold_evict={:?}  bucket_reclaim={:?}",
                s.target,
                s.entity_count_resident,
                s.process_resident_memory_bytes.map(|b| b / (1024 * 1024)),
                s.cold_entity_evictions_total,
                s.bucket_reclaim_total,
            );
        }
    }
    if !cell.metrics_final.is_empty() {
        println!("  state @ end:");
        for s in &cell.metrics_final {
            println!(
                "    {} entities={:?}  rss={:?}MiB  cold_evict={:?}  bucket_reclaim={:?}",
                s.target,
                s.entity_count_resident,
                s.process_resident_memory_bytes.map(|b| b / (1024 * 1024)),
                s.cold_entity_evictions_total,
                s.bucket_reclaim_total,
            );
        }
    }
}

fn write_markdown(path: &PathBuf, cells: &[CellResult]) -> Result<()> {
    let mut out = String::new();
    out.push_str("# beava-bench-v2 results\n\n");
    out.push_str("## Aggregate per cell\n\n");
    out.push_str(
        "| cell | targets | workers | pushes | EPS | p50 | p95 | p99 | p999 | max | bytes |\n",
    );
    out.push_str(
        "|---|---|---|---|---|---|---|---|---|---|---|\n",
    );
    for c in cells {
        out.push_str(&format!(
            "| {} | {} | {} | {} | {:.0} | {}µs | {}µs | {}µs | {}µs | {}µs | {:.1} MiB |\n",
            c.cell_label,
            c.n_targets,
            c.n_workers,
            c.aggregate_pushes,
            c.aggregate_eps,
            c.aggregate_p50_us,
            c.aggregate_p95_us,
            c.aggregate_p99_us,
            c.aggregate_p999_us,
            c.aggregate_max_us,
            c.aggregate_bytes_sent as f64 / (1024.0 * 1024.0),
        ));
    }
    out.push_str("\n## Per shard\n\n");
    out.push_str("| cell | shard | pushes | EPS | p99 | max | bytes |\n");
    out.push_str("|---|---|---|---|---|---|---|\n");
    for c in cells {
        for sh in &c.per_shard {
            out.push_str(&format!(
                "| {} | {} | {} | {:.0} | {}µs | {}µs | {:.1} MiB |\n",
                c.cell_label,
                sh.target,
                sh.pushes,
                sh.eps,
                sh.p99_us,
                sh.max_us,
                sh.bytes_sent as f64 / (1024.0 * 1024.0),
            ));
        }
    }
    out.push_str("\n## State (final per shard)\n\n");
    out.push_str(
        "| cell | shard | entities_resident | rss_MiB | cold_evict | bucket_reclaim | lifetime_cap_hit |\n",
    );
    out.push_str("|---|---|---|---|---|---|---|\n");
    for c in cells {
        for s in &c.metrics_final {
            out.push_str(&format!(
                "| {} | {} | {} | {} | {} | {} | {} |\n",
                c.cell_label,
                s.target,
                s.entity_count_resident.map(|n| n.to_string()).unwrap_or("?".into()),
                s.process_resident_memory_bytes
                    .map(|b| (b / (1024 * 1024)).to_string())
                    .unwrap_or("?".into()),
                s.cold_entity_evictions_total
                    .map(|n| n.to_string())
                    .unwrap_or("?".into()),
                s.bucket_reclaim_total
                    .map(|n| n.to_string())
                    .unwrap_or("?".into()),
                s.lifetime_op_cap_hit_total
                    .map(|n| n.to_string())
                    .unwrap_or("?".into()),
            ));
        }
    }
    std::fs::write(path, out).with_context(|| format!("write md {}", path.display()))?;
    Ok(())
}

// ─── main ────────────────────────────────────────────────────────────────────

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    if cli.targets.is_empty() {
        return Err(anyhow!("--targets is required"));
    }
    if !cli.metrics_targets.is_empty() && cli.metrics_targets.len() != cli.targets.len() {
        return Err(anyhow!(
            "--metrics-targets must have same length as --targets (got {} vs {})",
            cli.metrics_targets.len(),
            cli.targets.len()
        ));
    }

    let pipeline = Arc::new(load_pipeline(&cli.config)?);
    eprintln!(
        "beava-bench-v2: pipeline='{}' events={} targets={} workers/target={} pd={} duration={}s shape={:?} cardinality={}",
        pipeline.name,
        pipeline.events.len(),
        cli.targets.len(),
        cli.connections_per_target,
        cli.pipeline_depth,
        cli.duration_secs,
        cli.blast_shape,
        cli.cardinality,
    );

    // Build sweep matrix.
    let pds: Vec<u32> = if !cli.sweep_pipeline_depth.is_empty() {
        cli.sweep_pipeline_depth.clone()
    } else {
        vec![cli.pipeline_depth]
    };
    let shapes: Vec<BlastShape> = if !cli.sweep_blast_shape.is_empty() {
        cli.sweep_blast_shape.clone()
    } else {
        vec![cli.blast_shape]
    };
    eprintln!(
        "beava-bench-v2: sweep cells = {} (pd={:?} × shape={:?})",
        pds.len() * shapes.len(),
        pds,
        shapes,
    );

    let mut results: Vec<CellResult> = Vec::new();
    for (i, pd) in pds.iter().enumerate() {
        for (j, shape) in shapes.iter().enumerate() {
            eprintln!(
                "\n[cell {}/{}] pd={} shape={:?}",
                i * shapes.len() + j + 1,
                pds.len() * shapes.len(),
                pd,
                shape
            );
            let cell = run_cell(&cli, pipeline.clone(), *pd, *shape).await?;
            print_cell_summary(&cell);
            results.push(cell);
            if cli.cooldown_secs > 0 && (i + 1 < pds.len() || j + 1 < shapes.len()) {
                eprintln!("[cooldown {}s]", cli.cooldown_secs);
                tokio::time::sleep(Duration::from_secs(cli.cooldown_secs)).await;
            }
        }
    }

    if let Some(path) = &cli.output_json {
        let json = serde_json::to_vec_pretty(&results).context("encode results json")?;
        std::fs::write(path, json).with_context(|| format!("write json {}", path.display()))?;
        eprintln!("wrote {}", path.display());
    }
    if let Some(path) = &cli.output_md {
        write_markdown(path, &results)?;
        eprintln!("wrote {}", path.display());
    }

    // Suppress unused-import warnings while keeping HashMap available for future per-shard map work.
    let _ = HashMap::<String, u64>::new();

    Ok(())
}
