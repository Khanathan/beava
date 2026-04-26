//! beava-bench-v18 — standalone throughput harness binding ServerV18 directly.
//!
//! Boots `ServerV18::bind()` + `serve_with_dirs()` directly (NOT TestServer),
//! then drives it at saturation for N seconds. Captures:
//! - Sustained EPS (events / wall-time)
//! - P50/P95/P99 push latency (HDR histogram, 1µs precision)
//! - P99 batch-get latency (sampled every second)
//! - Peak RSS (sampled every 500ms via `ps`)
//!
//! Usage:
//! ```text
//! ./target/release/beava-bench-v18 \
//!     --pipeline small --transport tcp --duration-secs 10 --parallel 16 --no-ledger
//! ```

use anyhow::{Context, Result};
use beava_core::wire::{CT_JSON, CT_MSGPACK, OP_PUSH};
use beava_server::server::ServerV18;
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
#[command(
    version,
    about = "Beava v18 standalone throughput harness (ServerV18 direct)",
    long_about = None
)]
struct Cli {
    /// Pipeline config name (small, medium, large) OR explicit JSON file path.
    #[arg(long, default_value = "small")]
    pipeline: String,

    /// Transport to use.
    #[arg(long, value_enum, default_value_t = Transport::Tcp)]
    transport: Transport,

    /// Wire format for TCP pushes (json or msgpack). HTTP always uses JSON.
    #[arg(long, value_enum, default_value_t = WireFormat::Json)]
    wire_format: WireFormat,

    /// Wall-time duration in seconds.
    #[arg(long, default_value_t = 60)]
    duration_secs: u64,

    /// Number of parallel push workers. Defaults to min(8, num_cpus).
    #[arg(long)]
    parallel: Option<usize>,

    /// Random seed.
    #[arg(long, default_value_t = 0xCAFE_BABE_u64)]
    seed: u64,

    /// How often to sample batch /get latency (ms).
    #[arg(long, default_value_t = 1000)]
    get_sample_interval_ms: u64,

    /// Keys per batch /get sample.
    #[arg(long, default_value_t = 100)]
    get_batch_keys: usize,

    /// Suppress markdown ledger row; only print human summary.
    #[arg(long)]
    no_ledger: bool,

    /// TCP pipeline depth — caps inflight pushes per worker connection.
    /// In burst mode (--continuous-pipeline=false), each batch sends N
    /// pushes back-to-back then reads N acks. In continuous mode (default),
    /// this is the inflight semaphore size: sender keeps up to N pushes
    /// in-flight concurrently with receiver. Default 1 (request-response
    /// per event). HTTP transport ignores this.
    #[arg(long, default_value_t = 1)]
    pipeline_depth: usize,

    /// TCP continuous pipelining (default true) — split sender/receiver
    /// tasks with a semaphore-gated inflight queue. Eliminates the burst-
    /// mode sawtooth (apply thread idles between batches while the bench
    /// is reading N acks then re-sending N events) and produces constant
    /// load on the apply thread. Set `--continuous-pipeline=false` to
    /// fall back to the burst pattern. HTTP transport ignores this.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    continuous_pipeline: bool,
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

#[derive(Copy, Clone, Debug, ValueEnum)]
enum WireFormat {
    Json,
    Msgpack,
}

impl WireFormat {
    fn label(self) -> &'static str {
        match self {
            WireFormat::Json => "json",
            WireFormat::Msgpack => "msgpack",
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
    serde_json::from_slice(&bytes)
        .with_context(|| format!("parse pipeline config {}", resolved.display()))
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

fn num_cpus_or_default() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

fn sampling_rng(seed: u64) -> rand::rngs::StdRng {
    rand::SeedableRng::seed_from_u64(seed)
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

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new("beava_bench_v18=info,warn")
            }),
        )
        .init();

    let cli = Cli::parse();
    let pipeline = load_pipeline(&cli.pipeline)?;
    let parallel = cli
        .parallel
        .unwrap_or_else(|| std::cmp::min(8, num_cpus_or_default()));

    eprintln!(
        "beava-bench-v18: pipeline={} transport={} wire_format={} duration_secs={} parallel={} seed={} get_sample_ms={} get_batch_keys={}",
        pipeline.name,
        cli.transport.label(),
        cli.wire_format.label(),
        cli.duration_secs,
        parallel,
        cli.seed,
        cli.get_sample_interval_ms,
        cli.get_batch_keys,
    );

    // Bind ServerV18 directly — no TestServer.
    let any: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let sv18 = ServerV18::bind(any, any, any)
        .await
        .context("ServerV18::bind")?;

    let http_addr = sv18.http_addr();
    let tcp_addr = sv18.tcp_addr();

    eprintln!(
        "beava-bench-v18: bound http={} tcp={} admin={}",
        http_addr,
        tcp_addr,
        sv18.admin_addr()
    );

    // Serve on a tokio task with a oneshot shutdown signal.
    let wal_dir = tempfile::tempdir().context("wal tempdir")?;
    let snap_dir = tempfile::tempdir().context("snap tempdir")?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let wal_path = wal_dir.path().to_path_buf();
    let snap_path = snap_dir.path().to_path_buf();
    let serve_task = tokio::spawn(async move {
        sv18.serve_with_dirs(
            async move {
                let _ = shutdown_rx.await;
            },
            wal_path,
            snap_path,
        )
        .await
    });

    // Wait for the server to start accepting.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Register the pipeline via HTTP (register is always HTTP regardless of transport).
    let http_base = format!("http://{}", http_addr);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .pool_max_idle_per_host(parallel + 4)
        .build()
        .context("build reqwest client")?;

    let reg_resp = client
        .post(format!("{http_base}/register"))
        .header("Content-Type", "application/json")
        .body(pipeline.register.to_string())
        .send()
        .await
        .context("register request")?;
    let reg_status = reg_resp.status();
    let reg_body = reg_resp.text().await.unwrap_or_default();
    anyhow::ensure!(
        reg_status.is_success(),
        "register failed: status={reg_status} body={reg_body}"
    );
    eprintln!("beava-bench-v18: registered pipeline OK ({})", reg_status);

    // Pre-warm: 100 events serially before timed run.
    {
        let mut rng = sampling_rng(cli.seed ^ 0xDEAD);
        for i in 0..100_u64 {
            let body = make_event_payload(&pipeline, i, &mut rng);
            let _ = client
                .post(format!("{http_base}/push/{}", pipeline.event_name))
                .header("Content-Type", "application/json")
                .body(body.to_string())
                .send()
                .await;
        }
    }
    eprintln!("beava-bench-v18: pre-warm done");

    // Run the workload.
    let result = run_workload(
        http_addr,
        tcp_addr,
        &pipeline,
        &cli,
        parallel,
        Arc::new(client.clone()),
        cli.wire_format,
    )
    .await?;

    // Print results.
    let report = format_report(&pipeline, cli.transport, cli.wire_format, &cli, &result);
    if !cli.no_ledger {
        println!("{}", report.ledger_row);
    }
    eprintln!("\n=== beava-bench-v18 summary ===\n{}\n", report.human);

    // Shutdown.
    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(5), serve_task).await;

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
    http_addr: std::net::SocketAddr,
    tcp_addr: std::net::SocketAddr,
    pipeline: &PipelineConfig,
    cli: &Cli,
    parallel: usize,
    http_client: Arc<reqwest::Client>,
    wire_format: WireFormat,
) -> Result<WorkloadResult> {
    let stop = Arc::new(AtomicBool::new(false));
    let pushes = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let push_hist: Arc<AsyncMutex<Histogram<u64>>> = Arc::new(AsyncMutex::new(
        Histogram::new_with_bounds(1, 60_000_000, 3)?,
    ));

    let deadline = Instant::now() + Duration::from_secs(cli.duration_secs);

    // RSS sampler.
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
    let get_url = format!("http://{}/get", http_addr);
    let features_clone = pipeline.features.clone();
    let get_interval_ms = cli.get_sample_interval_ms;
    let get_batch_keys = cli.get_batch_keys;
    let get_seed = cli.seed;
    let get_client = Arc::clone(&http_client);
    let get_task = tokio::spawn(async move {
        use rand::Rng;
        let mut rng = sampling_rng(get_seed.wrapping_add(0xDEAD));
        loop {
            if stop_get.load(Ordering::Relaxed) {
                break;
            }
            let keys: Vec<String> = (0..get_batch_keys)
                .map(|_| format!("k{:08}", rng.gen_range(0..KEY_SPACE)))
                .collect();
            let body = serde_json::json!({"keys": keys, "features": features_clone});
            let start = Instant::now();
            let resp = get_client
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
        let pipeline_clone = PipelineConfig {
            name: pipeline.name.clone(),
            description: pipeline.description.clone(),
            register: pipeline.register.clone(),
            event_name: pipeline.event_name.clone(),
            features: pipeline.features.clone(),
            key_field: pipeline.key_field.clone(),
            extra_fields: pipeline.extra_fields.clone(),
        };
        let seed = cli.seed.wrapping_add(worker_id as u64 * 0x9E37);
        let http_url = format!("http://{}/push/{}", http_addr, pipeline.event_name);
        let transport = cli.transport;
        let wf = wire_format;
        let client = Arc::clone(&http_client);
        let pipeline_depth = cli.pipeline_depth.max(1);
        let continuous_pipeline = cli.continuous_pipeline;

        workers.push(tokio::spawn(async move {
            run_push_worker(
                worker_id,
                seed,
                stop,
                pushes,
                errors,
                push_hist,
                pipeline_clone,
                transport,
                wf,
                http_url,
                tcp_addr,
                deadline,
                client,
                pipeline_depth,
                continuous_pipeline,
            )
            .await;
        }));
    }

    let start = Instant::now();
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

    Ok(WorkloadResult {
        sustained_eps: push_count as f64 / elapsed.as_secs_f64(),
        push_p50_us: push_h.value_at_quantile(0.5),
        push_p95_us: push_h.value_at_quantile(0.95),
        push_p99_us: push_h.value_at_quantile(0.99),
        push_count,
        push_errors,
        get_p99_us: if !get_h.is_empty() {
            get_h.value_at_quantile(0.99)
        } else {
            0
        },
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
    wire_format: WireFormat,
    http_url: String,
    tcp_addr: std::net::SocketAddr,
    deadline: Instant,
    http_client: Arc<reqwest::Client>,
    pipeline_depth: usize,
    continuous_pipeline: bool,
) {
    // Plan 18-12 follow-up: continuous pipelining for TCP.
    // When enabled (default), the TCP path uses a split sender/receiver
    // pattern with a semaphore-gated inflight queue, eliminating the
    // burst-mode sawtooth (apply-thread idles between batches). HTTP and
    // burst-mode TCP keep their existing single-task loop.
    if matches!(transport, Transport::Tcp) && continuous_pipeline {
        run_tcp_continuous_push_worker(
            seed,
            stop,
            pushes,
            errors,
            push_hist,
            pipeline,
            wire_format,
            tcp_addr,
            deadline,
            pipeline_depth,
        )
        .await;
        return;
    }

    let mut rng = sampling_rng(seed);
    match transport {
        Transport::Http => {
            let mut seq = 0_u64;
            while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
                let body = make_event_payload(&pipeline, seq, &mut rng);
                let start = Instant::now();
                let r = http_client
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
            use beava_core::wire::Frame;
            use beava_server::testing::TcpClient;
            let mut client = match TcpClient::connect(tcp_addr).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("TcpClient::connect failed: {e}");
                    return;
                }
            };
            let mut seq = 0_u64;
            let pdepth = pipeline_depth.max(1);
            // Reused across batches; capacity matches pdepth so first-batch
            // alloc is the only one. Holds per-event send timestamps for
            // FIFO-paired real-latency measurement.
            let mut send_times: Vec<Instant> = Vec::with_capacity(pdepth);
            while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
                // ─── Build N frames + send all of them, then read N acks ─────
                send_times.clear();
                let mut send_err = false;
                for _ in 0..pdepth {
                    if stop.load(Ordering::Relaxed) || Instant::now() >= deadline {
                        break;
                    }
                    let body = make_event_payload(&pipeline, seq, &mut rng);
                    let (ct, payload_bytes) = match wire_format {
                        WireFormat::Json => {
                            let envelope = serde_json::json!({
                                "event": pipeline.event_name,
                                "body": body,
                            });
                            (CT_JSON, serde_json::to_vec(&envelope).unwrap())
                        }
                        WireFormat::Msgpack => {
                            use serde::Serialize;
                            #[derive(Serialize)]
                            struct Envelope<'a> {
                                event: &'a str,
                                body: &'a serde_json::Value,
                            }
                            let envelope = Envelope {
                                event: &pipeline.event_name,
                                body: &body,
                            };
                            (CT_MSGPACK, rmp_serde::to_vec_named(&envelope).unwrap())
                        }
                    };
                    let frame = Frame {
                        op: OP_PUSH,
                        content_type: ct,
                        payload: Bytes::from(payload_bytes),
                    };
                    // Capture per-event send timestamp BEFORE write_frame so
                    // p50/p95/p99 reflect real per-event wall-clock latency
                    // (matches continuous-mode measurement). Previously this
                    // path recorded `batch_total / N` (amortized CPU-time-per-
                    // event), which under-reported real latency by ~190× at
                    // pdepth=256 saturation.
                    let send_ts = Instant::now();
                    if client.write_frame(&frame).await.is_err() {
                        send_err = true;
                        break;
                    }
                    send_times.push(send_ts);
                    seq = seq.wrapping_add(1);
                }
                let sent = send_times.len();
                if sent == 0 {
                    if send_err {
                        if let Ok(c) = TcpClient::connect(tcp_addr).await {
                            client = c;
                        }
                    }
                    continue;
                }
                // ─── Read N acks (strict FIFO order) ─────────────────────────
                match client.read_n_frames(sent).await {
                    Ok(frames) => {
                        // Frames arrive in send order (TCP wire is strict
                        // FIFO, no request_id). Pair each ack with its
                        // matching send-timestamp for real per-event latency.
                        let mut h = push_hist.lock().await;
                        for (f, send_ts) in frames.iter().zip(send_times.iter()) {
                            if f.op == OP_PUSH {
                                pushes.fetch_add(1, Ordering::Relaxed);
                                let elapsed_us = send_ts.elapsed().as_micros() as u64;
                                let _ = h.record(elapsed_us.max(1));
                            } else {
                                errors.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        drop(h);
                    }
                    Err(_e) => {
                        errors.fetch_add(sent as u64, Ordering::Relaxed);
                        if let Ok(c) = TcpClient::connect(tcp_addr).await {
                            client = c;
                        }
                    }
                }
            }
        }
    }
}

/// Continuous pipelining TCP worker. Splits the TcpStream into independent
/// read/write halves and drives them with two cooperating tasks gated by a
/// semaphore.
///
/// **Why:** Burst mode (`send_n → read_n → send_n → ...`) leaves the apply
/// thread idle while the bench reads N acks then re-encodes N pushes, producing
/// a sawtooth load profile. Continuous mode keeps `pipeline_depth` pushes
/// always-in-flight: the sender writes whenever a permit is available; the
/// receiver decodes acks and returns permits as fast as the server can
/// respond. The apply thread sees constant pressure.
///
/// **Per-event latency** is captured via an unbounded mpsc<Instant> queue
/// from sender to receiver. Each ack pops the matching send-timestamp and
/// records `now() - send_ts` into the histogram. FIFO ordering is the wire
/// contract (Redis-style strict-FIFO, no request_id), so the timestamp queue
/// pairs correctly with acks one-to-one.
#[allow(clippy::too_many_arguments)]
async fn run_tcp_continuous_push_worker(
    seed: u64,
    stop: Arc<AtomicBool>,
    pushes: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
    push_hist: Arc<AsyncMutex<Histogram<u64>>>,
    pipeline: PipelineConfig,
    wire_format: WireFormat,
    tcp_addr: std::net::SocketAddr,
    deadline: Instant,
    pipeline_depth: usize,
) {
    use beava_core::wire::{decode_frame, encode_frame, Frame};
    use bytes::BytesMut;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::sync::Semaphore;

    let pdepth = pipeline_depth.max(1);
    let stream = match TcpStream::connect(tcp_addr).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("continuous_pipeline TcpStream::connect failed: {e}");
            return;
        }
    };
    let _ = stream.set_nodelay(true);
    let (mut read_half, write_half) = tokio::io::split(stream);

    let sem = Arc::new(Semaphore::new(pdepth));
    let (ts_tx, mut ts_rx) = tokio::sync::mpsc::unbounded_channel::<Instant>();

    // ─── Sender task ──────────────────────────────────────────────────────
    let sender_stop = Arc::clone(&stop);
    let sender_errors = Arc::clone(&errors);
    let sender_sem = Arc::clone(&sem);
    let sender_pipeline = PipelineConfig {
        name: pipeline.name.clone(),
        description: pipeline.description.clone(),
        register: pipeline.register.clone(),
        event_name: pipeline.event_name.clone(),
        features: pipeline.features.clone(),
        key_field: pipeline.key_field.clone(),
        extra_fields: pipeline.extra_fields.clone(),
    };
    let sender_handle = tokio::spawn(async move {
        let mut rng = sampling_rng(seed);
        let mut seq = 0_u64;
        let mut write_half = write_half;
        let mut buf = BytesMut::with_capacity(4 * 1024);
        loop {
            if sender_stop.load(Ordering::Relaxed) || Instant::now() >= deadline {
                break;
            }
            // Acquire one inflight slot. `acquire_owned` returns a permit
            // bound to the sem Arc. We `forget()` it: the receiver is the
            // one that gives the permit back via `add_permits(1)` once the
            // ack arrives. Without `forget`, the permit would be returned
            // here on drop and the gate would have no effect.
            let permit = match Arc::clone(&sender_sem).acquire_owned().await {
                Ok(p) => p,
                Err(_) => break, // semaphore closed
            };
            permit.forget();

            let body = make_event_payload(&sender_pipeline, seq, &mut rng);
            let (ct, payload_bytes) = match wire_format {
                WireFormat::Json => {
                    let envelope = serde_json::json!({
                        "event": sender_pipeline.event_name,
                        "body": body,
                    });
                    (CT_JSON, serde_json::to_vec(&envelope).unwrap())
                }
                WireFormat::Msgpack => {
                    use serde::Serialize;
                    #[derive(Serialize)]
                    struct Envelope<'a> {
                        event: &'a str,
                        body: &'a serde_json::Value,
                    }
                    let envelope = Envelope {
                        event: &sender_pipeline.event_name,
                        body: &body,
                    };
                    (CT_MSGPACK, rmp_serde::to_vec_named(&envelope).unwrap())
                }
            };
            let frame = Frame {
                op: OP_PUSH,
                content_type: ct,
                payload: Bytes::from(payload_bytes),
            };
            buf.clear();
            encode_frame(&frame, &mut buf);

            let send_ts = Instant::now();
            if write_half.write_all(&buf).await.is_err() {
                sender_errors.fetch_add(1, Ordering::Relaxed);
                break;
            }
            // Notify receiver of the send-timestamp for this in-flight ack.
            // unbounded_send only fails if receiver dropped the channel; in
            // that case the receiver task has exited and we should too.
            if ts_tx.send(send_ts).is_err() {
                break;
            }
            seq = seq.wrapping_add(1);
        }
        // Drop ts_tx to signal EOF to the receiver after we've written
        // everything.
        drop(ts_tx);
    });

    // ─── Receiver loop (this task) ────────────────────────────────────────
    //
    // Latency batching: burst mode locks `push_hist` ONCE per N-event batch
    // and records N values inside the lock. Continuous mode would otherwise
    // lock once per event — 256× more lock ops at pd=256, contending with
    // the other 15 workers' receivers. We mirror burst's batching by
    // accumulating elapsed_us into a local Vec and flushing every
    // HIST_FLUSH_BATCH records (or on shutdown) in a single lock.
    let mut read_buf = BytesMut::with_capacity(8 * 1024);
    const MAX_FRAME_BYTES: u32 = 16 * 1024 * 1024;
    const HIST_FLUSH_BATCH: usize = 64;
    let mut latency_batch: Vec<u64> = Vec::with_capacity(HIST_FLUSH_BATCH);

    async fn flush_latencies(push_hist: &AsyncMutex<Histogram<u64>>, latency_batch: &mut Vec<u64>) {
        if latency_batch.is_empty() {
            return;
        }
        let mut h = push_hist.lock().await;
        for us in latency_batch.iter() {
            let _ = h.record(*us);
        }
        drop(h);
        latency_batch.clear();
    }

    'recv: loop {
        // Drain any frames already buffered.
        loop {
            match decode_frame(&mut read_buf, MAX_FRAME_BYTES) {
                Ok(Some(f)) => {
                    // Pair the ack with the matching send-timestamp.
                    let send_ts = match ts_rx.recv().await {
                        Some(t) => t,
                        None => break 'recv, // sender finished + drained
                    };
                    let elapsed_us = send_ts.elapsed().as_micros() as u64;
                    if f.op == OP_PUSH {
                        pushes.fetch_add(1, Ordering::Relaxed);
                        latency_batch.push(elapsed_us.max(1));
                        if latency_batch.len() >= HIST_FLUSH_BATCH {
                            flush_latencies(&push_hist, &mut latency_batch).await;
                        }
                    } else {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                    // Release the inflight slot.
                    sem.add_permits(1);
                }
                Ok(None) => break, // need more bytes
                Err(_e) => {
                    errors.fetch_add(1, Ordering::Relaxed);
                    break 'recv;
                }
            }
        }
        if stop.load(Ordering::Relaxed) || Instant::now() >= deadline {
            break;
        }
        match read_half.read_buf(&mut read_buf).await {
            Ok(0) => break, // peer closed
            Ok(_) => continue,
            Err(_) => break,
        }
    }

    // Flush any tail-batch latencies before exiting.
    flush_latencies(&push_hist, &mut latency_batch).await;

    // Wait for the sender to finish before returning so the worker doesn't
    // outlive its sender task (avoids Tokio "task stopped" warnings).
    let _ = sender_handle.await;
}

struct Report {
    ledger_row: String,
    human: String,
}

fn format_report(
    pipeline: &PipelineConfig,
    transport: Transport,
    wire_format: WireFormat,
    cli: &Cli,
    r: &WorkloadResult,
) -> Report {
    let date = current_utc_date();
    let commit = git_short_sha().unwrap_or_else(|| "unknown".to_string());
    let transport_label = format!("{}/{}", transport.label(), wire_format.label());
    let notes = if r.push_errors > 0 {
        format!("errors={}", r.push_errors)
    } else {
        String::new()
    };
    let ledger_row = format!(
        "| 18 | {date} | {pipeline} | {transport} | {parallel} | {duration}s | {eps:.0} | {p50} | {p95} | {p99} | {gp99} | {rss} | {commit} | {notes} |",
        pipeline = pipeline.name,
        transport = transport_label,
        parallel = cli.parallel.unwrap_or(0),
        duration = cli.duration_secs,
        eps = r.sustained_eps,
        p50 = r.push_p50_us,
        p95 = r.push_p95_us,
        p99 = r.push_p99_us,
        gp99 = r.get_p99_us,
        rss = r.peak_rss_mb,
    );
    let human = format!(
        "pipeline:         {}\n\
         transport:        {}\n\
         wire_format:      {}\n\
         duration_secs:    {}\n\
         parallel:         {}\n\
         pushes:           {}\n\
         push_errors:      {}\n\
         sustained_eps:    {:.0}\n\
         push p50/p95/p99: {} / {} / {} µs\n\
         get_samples:      {}\n\
         get p99:          {} µs\n\
         peak_rss_mb:      {}\n\
         elapsed:          {:?}\n\
         hw-class:         {}\n\
         commit:           {}\n",
        pipeline.name,
        transport.label(),
        wire_format.label(),
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
    let days = secs / 86400;
    let (y, m, d) = days_to_ymd(days as i64);
    format!("{y:04}-{m:02}-{d:02}")
}

fn days_to_ymd(days_since_epoch: i64) -> (i32, u32, u32) {
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
