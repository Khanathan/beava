//! beava-bench — end-to-end throughput harness for the Beava server.
//!
//! Drives a live `beava` server (HTTP + TCP) via in-process `TestServer` for
//! N seconds at saturation, capturing:
//! - Sustained EPS (events / wall-time)
//! - P50/P95/P99 push latency (HDR histogram, 1µs precision)
//! - P99 batch-get latency (sampled every second over the run)
//! - Peak RSS (sampled every 500ms via sysinfo)
//!
//! Pipeline configs live in `configs/{small,medium,large}.json`. Two
//! transports supported: `http` (reqwest) and `tcp` (beava-server's
//! TcpClient). Results emit as a markdown table row to stdout — copy/paste
//! into `.planning/throughput-baselines.md`.
//!
//! Usage:
//! ```text
//! cargo run -p beava-bench --release -- \
//!     --pipeline small --transport http --duration-secs 60
//! ```

use anyhow::{Context, Result};
use beava_server::testing::{TcpClient, TestServer, TestServerBuilder};
use bytes::Bytes;
use clap::{Parser, ValueEnum};
use hdrhistogram::Histogram;
use serde_json::Value;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex as AsyncMutex;

const KEY_SPACE: u64 = 100_000;

#[derive(Parser, Debug)]
#[command(version, about = "Beava end-to-end throughput harness", long_about = None)]
struct Cli {
    /// Pipeline config name (small, medium, large) OR explicit path to a
    /// JSON file. Defaults to looking up under `configs/{name}.json`.
    #[arg(long, default_value = "small")]
    pipeline: String,

    /// Transport to drive the server with.
    #[arg(long, value_enum, default_value_t = Transport::Http)]
    transport: Transport,

    /// Wall-time duration in seconds to drive at saturation.
    #[arg(long, default_value_t = 60)]
    duration_secs: u64,

    /// Number of parallel client workers pushing events. Defaults to
    /// min(8, num_cpus).
    #[arg(long)]
    parallel: Option<usize>,

    /// Random seed for the workload (reproducible event/key streams).
    #[arg(long, default_value_t = 0xCAFE_BABE_u64)]
    seed: u64,

    /// How often to sample batch /get latency in milliseconds.
    #[arg(long, default_value_t = 1000)]
    get_sample_interval_ms: u64,

    /// How many keys to include in each batch /get sample.
    #[arg(long, default_value_t = 100)]
    get_batch_keys: usize,

    /// Suppress markdown ledger row output (only print human summary).
    #[arg(long)]
    no_ledger: bool,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum Transport {
    Http,
    Tcp,
}

impl Transport {
    fn label(self) -> &'static str {
        match self {
            Transport::Http => "http",
            Transport::Tcp => "tcp",
        }
    }
}

#[derive(Debug, serde::Deserialize)]
struct PipelineConfig {
    name: String,
    #[allow(dead_code)]
    description: String,
    register: Value,
    event_name: String,
    features: Vec<String>,
    key_field: String,
    extra_fields: serde_json::Map<String, Value>,
}

fn load_pipeline(name_or_path: &str) -> Result<PipelineConfig> {
    let path = PathBuf::from(name_or_path);
    let resolved = if path.is_file() {
        path
    } else {
        let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
        PathBuf::from(manifest)
            .join("configs")
            .join(format!("{name_or_path}.json"))
    };
    let bytes = std::fs::read(&resolved)
        .with_context(|| format!("read pipeline config {}", resolved.display()))?;
    let cfg: PipelineConfig = serde_json::from_slice(&bytes)
        .with_context(|| format!("parse pipeline config {}", resolved.display()))?;
    Ok(cfg)
}

fn hw_class_string() -> String {
    let uname = std::process::Command::new("uname")
        .arg("-sr")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().replace(' ', "-"))
        .unwrap_or_else(|| "unknown".to_string());
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    format!("{uname} / {cpus} cores")
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("beava_bench=info,warn")),
        )
        .init();
    let cli = Cli::parse();
    let pipeline = load_pipeline(&cli.pipeline)?;

    let parallel = cli
        .parallel
        .unwrap_or_else(|| std::cmp::min(8, num_cpus_or_default()));

    eprintln!(
        "beava-bench: pipeline={} transport={} duration_secs={} parallel={} seed={} get_sample_ms={} get_batch_keys={}",
        pipeline.name,
        cli.transport.label(),
        cli.duration_secs,
        parallel,
        cli.seed,
        cli.get_sample_interval_ms,
        cli.get_batch_keys,
    );

    // Spawn the in-process server with per-run tempdirs.
    let wal = tempfile::tempdir()?;
    let snap = tempfile::tempdir()?;
    let ts = TestServerBuilder::new()
        .dev_endpoints(true)
        .wal_dir(wal.path().to_path_buf())
        .snapshot_dir(snap.path().to_path_buf())
        .fsync_interval_ms(2)
        .spawn()
        .await
        .context("spawn TestServer")?;

    // Register the pipeline.
    register_pipeline(&ts, &pipeline.register).await?;

    // Pre-warm: push 100 events serially so the WAL has something committed
    // before we time the run.
    for i in 0..100_u64 {
        push_one_http(&ts, &pipeline, i, &mut sampling_rng(cli.seed ^ i)).await?;
    }

    // Run the workload.
    let result = run_workload(&ts, &pipeline, &cli, parallel).await?;

    let report = format_report(&pipeline, cli.transport, &cli, &result);
    if !cli.no_ledger {
        println!("{}", report.ledger_row);
    }
    eprintln!("\n=== beava-bench summary ===\n{}\n", report.human);

    ts.shutdown().await.context("shutdown TestServer")?;
    Ok(())
}

fn num_cpus_or_default() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

fn sampling_rng(seed: u64) -> rand::rngs::StdRng {
    rand::SeedableRng::seed_from_u64(seed)
}

async fn register_pipeline(ts: &TestServer, register_payload: &Value) -> Result<()> {
    let resp = ts.post_json("/register", register_payload).await?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    anyhow::ensure!(
        status.is_success(),
        "register failed: status={status} body={body}"
    );
    Ok(())
}

fn make_event_payload(pipeline: &PipelineConfig, seq: u64, rng: &mut rand::rngs::StdRng) -> Value {
    use rand::Rng;
    let key_idx: u64 = rng.gen_range(0..KEY_SPACE);
    let mut obj = serde_json::Map::new();
    obj.insert(
        pipeline.key_field.clone(),
        Value::String(format!("k{key_idx:08}")),
    );
    obj.insert(
        "event_time".into(),
        Value::Number((1_000_000 + seq as i64).into()),
    );
    for (field, ty) in &pipeline.extra_fields {
        let v = match ty.as_str().unwrap_or("f64") {
            "f64" => serde_json::json!(rng.gen_range(0.0..1000.0)),
            "i64" => serde_json::json!(rng.gen_range(0_i64..1_000_000)),
            "str" => serde_json::json!(format!("s{}", rng.gen_range(0..1000))),
            _ => serde_json::json!(0),
        };
        obj.insert(field.clone(), v);
    }
    Value::Object(obj)
}

async fn push_one_http(
    ts: &TestServer,
    pipeline: &PipelineConfig,
    seq: u64,
    rng: &mut rand::rngs::StdRng,
) -> Result<()> {
    let body = make_event_payload(pipeline, seq, rng);
    let resp = ts
        .post_json(&format!("/push/{}", pipeline.event_name), &body)
        .await?;
    anyhow::ensure!(
        resp.status().is_success(),
        "pre-warm push failed: status={}",
        resp.status()
    );
    Ok(())
}

#[derive(Debug)]
struct WorkloadResult {
    sustained_eps: f64,
    push_p50_us: u64,
    push_p95_us: u64,
    push_p99_us: u64,
    push_count: u64,
    push_errors: u64,
    get_p99_us: u64,
    get_samples: u64,
    peak_rss_mb: u64,
    elapsed: Duration,
}

async fn run_workload(
    ts: &TestServer,
    pipeline: &PipelineConfig,
    cli: &Cli,
    parallel: usize,
) -> Result<WorkloadResult> {
    let stop = Arc::new(AtomicBool::new(false));
    let pushes = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let push_hist: Arc<AsyncMutex<Histogram<u64>>> = Arc::new(AsyncMutex::new(
        Histogram::new_with_bounds(1, 60_000_000, 3)?,
    ));

    let deadline = Instant::now() + Duration::from_secs(cli.duration_secs);

    // RSS sampler — shell out to `ps` for cross-platform simplicity. macOS
    // and Linux both report RSS in kilobytes via `ps -o rss= -p <pid>`.
    let stop_rss = Arc::clone(&stop);
    let peak_rss = Arc::new(AtomicU64::new(0));
    let peak_rss_clone = Arc::clone(&peak_rss);
    let pid = std::process::id();
    let rss_task = tokio::spawn(async move {
        loop {
            if stop_rss.load(Ordering::Relaxed) {
                break;
            }
            if let Ok(out) = std::process::Command::new("ps")
                .args(["-o", "rss=", "-p", &pid.to_string()])
                .output()
            {
                if let Ok(s) = std::str::from_utf8(&out.stdout) {
                    if let Ok(rss_kb) = s.trim().parse::<u64>() {
                        let rss_mb = rss_kb / 1024;
                        let prev = peak_rss_clone.load(Ordering::Relaxed);
                        if rss_mb > prev {
                            peak_rss_clone.store(rss_mb, Ordering::Relaxed);
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });

    // Batch-get latency sampler.
    let stop_get = Arc::clone(&stop);
    let get_hist: Arc<AsyncMutex<Histogram<u64>>> = Arc::new(AsyncMutex::new(
        Histogram::new_with_bounds(1, 60_000_000, 3)?,
    ));
    let get_hist_clone = Arc::clone(&get_hist);
    let get_samples_counter = Arc::new(AtomicU64::new(0));
    let get_samples_clone = Arc::clone(&get_samples_counter);
    let get_url = format!("{}/get", ts.base_url());
    let features_clone = pipeline.features.clone();
    let get_interval_ms = cli.get_sample_interval_ms;
    let get_batch_keys = cli.get_batch_keys;
    let get_seed = cli.seed;
    let get_task = tokio::spawn(async move {
        use rand::Rng;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("client");
        let mut rng = sampling_rng(get_seed.wrapping_add(0xDEAD));
        loop {
            if stop_get.load(Ordering::Relaxed) {
                break;
            }
            let keys: Vec<String> = (0..get_batch_keys)
                .map(|_| {
                    let key_idx: u64 = rng.gen_range(0..KEY_SPACE);
                    format!("k{key_idx:08}")
                })
                .collect();
            let body = serde_json::json!({"keys": keys, "features": features_clone});
            let start = Instant::now();
            let resp = client
                .post(&get_url)
                .header("Content-Type", "application/json")
                .body(body.to_string())
                .send()
                .await;
            if let Ok(r) = resp {
                if r.status().is_success() {
                    let elapsed_us = start.elapsed().as_micros() as u64;
                    let _ = r.bytes().await;
                    let mut h = get_hist_clone.lock().await;
                    let _ = h.record(elapsed_us.max(1));
                    drop(h);
                    get_samples_clone.fetch_add(1, Ordering::Relaxed);
                }
            }
            tokio::time::sleep(Duration::from_millis(get_interval_ms)).await;
        }
    });

    // Spawn N parallel push workers.
    let mut workers = Vec::with_capacity(parallel);
    for worker_id in 0..parallel {
        let stop = Arc::clone(&stop);
        let pushes = Arc::clone(&pushes);
        let errors = Arc::clone(&errors);
        let push_hist = Arc::clone(&push_hist);
        let pipeline_name = pipeline.event_name.clone();
        let key_field = pipeline.key_field.clone();
        let extra = pipeline.extra_fields.clone();
        let seed = cli.seed.wrapping_add(worker_id as u64 * 0x9E37);
        let url = format!("{}/push/{}", ts.base_url(), pipeline_name);
        let tcp_addr = ts.tcp_addr();
        let transport = cli.transport;
        let pipeline_for_payload = PipelineConfig {
            name: pipeline.name.clone(),
            description: pipeline.description.clone(),
            register: pipeline.register.clone(),
            event_name: pipeline.event_name.clone(),
            features: pipeline.features.clone(),
            key_field,
            extra_fields: extra,
        };

        workers.push(tokio::spawn(async move {
            run_push_worker(
                worker_id,
                seed,
                stop,
                pushes,
                errors,
                push_hist,
                pipeline_for_payload,
                transport,
                url,
                tcp_addr,
                deadline,
            )
            .await
        }));
    }

    let start = Instant::now();
    // Wait for the deadline.
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    stop.store(true, Ordering::Relaxed);

    for w in workers {
        let _ = w.await;
    }
    let _ = get_task.await;
    let _ = rss_task.await;

    let elapsed = start.elapsed();
    let push_count = pushes.load(Ordering::Relaxed);
    let push_errors = errors.load(Ordering::Relaxed);
    let push_h = push_hist.lock().await;
    let get_h = get_hist.lock().await;
    let push_p50 = push_h.value_at_quantile(0.5);
    let push_p95 = push_h.value_at_quantile(0.95);
    let push_p99 = push_h.value_at_quantile(0.99);
    let get_p99 = if !get_h.is_empty() {
        get_h.value_at_quantile(0.99)
    } else {
        0
    };

    Ok(WorkloadResult {
        sustained_eps: push_count as f64 / elapsed.as_secs_f64(),
        push_p50_us: push_p50,
        push_p95_us: push_p95,
        push_p99_us: push_p99,
        push_count,
        push_errors,
        get_p99_us: get_p99,
        get_samples: get_samples_counter.load(Ordering::Relaxed),
        peak_rss_mb: peak_rss.load(Ordering::Relaxed),
        elapsed,
    })
}

#[allow(clippy::too_many_arguments)]
async fn run_push_worker(
    _worker_id: usize,
    seed: u64,
    stop: Arc<AtomicBool>,
    pushes: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
    push_hist: Arc<AsyncMutex<Histogram<u64>>>,
    pipeline: PipelineConfig,
    transport: Transport,
    http_url: String,
    tcp_addr: Option<std::net::SocketAddr>,
    deadline: Instant,
) {
    let mut rng = sampling_rng(seed);
    match transport {
        Transport::Http => {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .pool_max_idle_per_host(8)
                .build()
                .expect("reqwest client");
            let mut seq = 0_u64;
            while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
                let body = make_event_payload(&pipeline, seq, &mut rng);
                let start = Instant::now();
                let r = client
                    .post(&http_url)
                    .header("Content-Type", "application/json")
                    .body(body.to_string())
                    .send()
                    .await;
                let elapsed_us = start.elapsed().as_micros() as u64;
                match r {
                    Ok(resp) => {
                        if resp.status().is_success() {
                            pushes.fetch_add(1, Ordering::Relaxed);
                            let mut h = push_hist.lock().await;
                            let _ = h.record(elapsed_us.max(1));
                        } else {
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                        let _ = resp.bytes().await;
                    }
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                seq = seq.wrapping_add(1);
            }
        }
        Transport::Tcp => {
            let addr = match tcp_addr {
                Some(a) => a,
                None => {
                    eprintln!("TCP transport requested but TestServer has no TCP addr");
                    return;
                }
            };
            let mut client = match TcpClient::connect(addr).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("TcpClient::connect failed: {e}");
                    return;
                }
            };
            // OP_PUSH frame: send raw JSON event with op = 0x10 (matches TCP wire spec)
            // Using OP_PUSH constant defined in beava-core wire module.
            use beava_core::wire::{CT_JSON, OP_PUSH};
            let mut seq = 0_u64;
            while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
                let body = make_event_payload(&pipeline, seq, &mut rng);
                let payload = serde_json::json!({
                    "event_name": pipeline.event_name,
                    "body": body,
                });
                let payload_bytes = serde_json::to_vec(&payload).unwrap();
                let start = Instant::now();
                match client
                    .send_raw(OP_PUSH, CT_JSON, Bytes::from(payload_bytes))
                    .await
                {
                    Ok(resp) => {
                        let elapsed_us = start.elapsed().as_micros() as u64;
                        // Phase 2.5/7.5 truth: OP_PUSH on the TCP wire is
                        // currently reserved (returns OP_ERROR_RESPONSE with
                        // code "op_not_implemented"). Treat any non-OP_PUSH
                        // response as an error so the EPS reading reflects
                        // pushes-actually-applied, not bytes-bounced. Once
                        // Phase 8+ implements TCP OP_PUSH, this branch will
                        // start succeeding and the bench can capture real
                        // TCP push throughput.
                        if resp.op == OP_PUSH {
                            pushes.fetch_add(1, Ordering::Relaxed);
                            let mut h = push_hist.lock().await;
                            let _ = h.record(elapsed_us.max(1));
                        } else {
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(_e) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                        // Try to reconnect on persistent errors.
                        if let Ok(c) = TcpClient::connect(addr).await {
                            client = c;
                        }
                    }
                }
                seq = seq.wrapping_add(1);
            }
        }
    }
}

struct Report {
    ledger_row: String,
    human: String,
}

fn format_report(
    pipeline: &PipelineConfig,
    transport: Transport,
    cli: &Cli,
    r: &WorkloadResult,
) -> Report {
    let date = current_utc_date();
    let phase = "7.5";
    let commit = git_short_sha().unwrap_or_else(|| "unknown".to_string());
    let notes = if r.push_errors > 0 {
        format!("errors={}", r.push_errors)
    } else {
        String::new()
    };
    let ledger_row = format!(
        "| {phase} | {date} | {pipeline} | {transport} | {eps:.0} | {p50} | {p95} | {p99} | {gp99} | {rss} | {commit} | {notes} |",
        pipeline = pipeline.name,
        transport = transport.label(),
        eps = r.sustained_eps,
        p50 = r.push_p50_us,
        p95 = r.push_p95_us,
        p99 = r.push_p99_us,
        gp99 = r.get_p99_us,
        rss = r.peak_rss_mb,
    );
    let human = format!(
        "pipeline:        {}\n\
         transport:       {}\n\
         duration_secs:   {}\n\
         parallel:        {}\n\
         pushes:          {}\n\
         push_errors:     {}\n\
         sustained_eps:   {:.0}\n\
         push p50/p95/p99: {} / {} / {} µs\n\
         get_samples:     {}\n\
         get p99:         {} µs\n\
         peak_rss_mb:     {}\n\
         elapsed:         {:?}\n\
         hw-class:        {}\n\
         commit:          {}\n",
        pipeline.name,
        transport.label(),
        cli.duration_secs,
        cli.parallel.unwrap_or(0),
        r.push_count,
        r.push_errors,
        r.sustained_eps,
        r.push_p50_us,
        r.push_p95_us,
        r.push_p99_us,
        r.get_samples,
        r.get_p99_us,
        r.peak_rss_mb,
        r.elapsed,
        hw_class_string(),
        commit,
    );
    Report { ledger_row, human }
}

fn current_utc_date() -> String {
    use std::time::SystemTime;
    let secs = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    // Naive date conversion: YYYY-MM-DD.
    // Avoids pulling chrono into the harness.
    let days = secs / 86400;
    // Reference epoch = 1970-01-01 (Thursday).
    let (y, m, d) = days_to_ymd(days as i64);
    format!("{y:04}-{m:02}-{d:02}")
}

fn days_to_ymd(days_since_epoch: i64) -> (i32, u32, u32) {
    // Algorithm: civil_from_days from
    // http://howardhinnant.github.io/date_algorithms.html
    let z = days_since_epoch + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = y + (if m <= 2 { 1 } else { 0 });
    (y as i32, m as u32, d as u32)
}

fn git_short_sha() -> Option<String> {
    let output = std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let s = String::from_utf8(output.stdout).ok()?.trim().to_string();
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}
