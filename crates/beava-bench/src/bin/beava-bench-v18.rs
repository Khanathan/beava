//! Standalone throughput harness binding `ServerV18` directly.
//!
//! Boots `ServerV18::bind()` + `serve_with_dirs()` (NOT `TestServer`), then
//! drives it at saturation for N seconds (or N events, when `--total-events`
//! is set). Captures:
//! - Sustained EPS (events / wall-time)
//! - P50 / P95 / P99 push latency (HDR histogram, 1µs precision)
//! - P99 batch-get latency (sampled every second)
//! - Peak RSS (sampled every 500 ms via `ps`)
//!
//! Blast modes:
//! - `--total-events N` — fixed-event blast using the [`blast_shape`]
//!   pre-encoded frame pool. `--duration-secs` becomes a safety upper bound
//!   only; the receiver flips `stop` and closes the sender semaphore as soon
//!   as `acks >= N`. A hard-cap counter on the sender prevents over-pushing
//!   past `N`; run end reports the invariant tuple
//!   `{requested, pushed, acked}` (asserted equal when `--total-events` is
//!   set).
//! - `--blast-shape={fixed,uniform,zipfian,mixed}` — distribution that the
//!   Pool=N builder samples.
//! - `--isolation-mode` — adds `wall_clock_ms` / `send_drain_ms` /
//!   `ack_lag_ms = wall_clock - send_drain` columns. Pool-build time is
//!   excluded from `wall_clock_ms` via a `tokio::sync::Barrier::new(parallel + 1)`
//!   synchronization point.
//!
//! Usage:
//! ```text
//! ./target/release/beava-bench-v18 \
//!     --pipeline small --transport tcp --duration-secs 10 --parallel 16 --no-ledger
//!
//! ./target/release/beava-bench-v18 \
//!     --total-events 1_000_000 --blast-shape zipfian --transport tcp \
//!     --wire-format msgpack --pipeline small --duration-secs 30 \
//!     --parallel 16 --pipeline-depth 1024 --no-ledger --isolation-mode
//! ```
//!
//! [`blast_shape`]: beava_bench::blast_shape

use anyhow::{Context, Result};
use beava_bench::blast_shape::{build_pool_timed, BlastShape, BlastShapeConfig, PipelineConfig};
use beava_core::wire::{CT_JSON, OP_GET_MULTI, OP_GET_RESPONSE, OP_PUSH};
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

    /// Wall-time duration in seconds. Becomes a safety upper bound only when
    /// `--total-events` is set.
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

    /// Number of parallel back-to-back /get worker tasks. Each worker holds
    /// its own `TcpClient` connection (TCP transport) or reqwest client
    /// (HTTP transport) and issues /get requests as fast as the server
    /// responds — no sleep between requests. Use this to measure read
    /// throughput, not just sampled latency. 0 (default) disables parallel
    /// read workers; the `--get-sample-interval-ms` sampler still runs.
    #[arg(long, default_value_t = 0)]
    read_workers: usize,

    /// Suppress markdown ledger row; only print human summary.
    #[arg(long)]
    no_ledger: bool,

    /// Connect to an existing remote beava server instead of binding our
    /// own ServerV18. Format: `host:http_port,host:tcp_port` (e.g.
    /// `127.0.0.1:8080,127.0.0.1:8081`). When set, the bench skips
    /// ServerV18::bind, /register (assumes pipeline already registered),
    /// and pre-warm; just runs the workload. Used to measure server-side
    /// throughput with multiple bench client processes pointing at one
    /// server (escapes per-process glibc mmap_lock contention).
    #[arg(long)]
    remote_addr: Option<String>,

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

    /// Send a fixed total number of events instead of running for
    /// `--duration-secs`. When set, the bench reports `wall_clock` to push N
    /// events end-to-end; `--duration-secs` becomes a safety upper bound only.
    #[arg(long)]
    total_events: Option<u64>,

    /// Blast shape — distribution that the Pool=N is built from.
    #[arg(long, value_enum, default_value_t = BlastShapeArg::Fixed)]
    blast_shape: BlastShapeArg,

    /// Zipfian alpha skew (only used when --blast-shape=zipfian). Default 1.0.
    #[arg(long, default_value_t = 1.0)]
    zipf_alpha: f64,

    /// Cardinality K for uniform/zipfian shapes. Default 1_000_000.
    #[arg(long, default_value_t = 1_000_000)]
    cardinality: u64,

    /// Number of distinct event names for --blast-shape=mixed. Default 3.
    #[arg(long, default_value_t = 3)]
    mixed_event_count: usize,

    /// Isolation mode — print wall_clock_ms / send_drain_ms / ack_lag_ms columns.
    #[arg(long, default_value_t = false)]
    isolation_mode: bool,

    /// IoPool worker count override.
    ///
    /// Defaults to `max(2, available_parallelism / 4)` (Redis-style ratio).
    /// On virtualized cloud servers with ≥ 16 vCPUs the auto-default can
    /// underperform: the Hetzner sweep on 2026-04-28 showed `iot=4` is the
    /// worst config for fraud-team-shaped workloads (~10% lift available at
    /// `iot=2` or `iot=8`). Try `--io-threads 2` for feature-heavy workloads
    /// or `--io-threads 8` for thin shapes if the auto-default underperforms
    /// on your box. Equivalent to setting `BEAVA_IO_THREADS=N`.
    #[arg(long)]
    io_threads: Option<usize>,
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

#[derive(Copy, Clone, Debug, ValueEnum)]
enum BlastShapeArg {
    Fixed,
    Uniform,
    Zipfian,
    Mixed,
}

impl BlastShapeArg {
    fn label(self) -> &'static str {
        match self {
            BlastShapeArg::Fixed => "fixed",
            BlastShapeArg::Uniform => "uniform",
            BlastShapeArg::Zipfian => "zipfian",
            BlastShapeArg::Mixed => "mixed",
        }
    }
    fn to_blast_shape(self, cli: &Cli) -> BlastShape {
        match self {
            BlastShapeArg::Fixed => BlastShape::Fixed,
            BlastShapeArg::Uniform => BlastShape::Uniform {
                cardinality: cli.cardinality,
            },
            BlastShapeArg::Zipfian => BlastShape::Zipfian {
                alpha: cli.zipf_alpha,
                cardinality: cli.cardinality,
            },
            BlastShapeArg::Mixed => BlastShape::Mixed {
                event_count: cli.mixed_event_count,
            },
        }
    }
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

    // `--io-threads N` overrides the IoPool worker count by setting
    // BEAVA_IO_THREADS, which `default_io_threads()` reads inside
    // `run_mio_event_loop`. Must land BEFORE `ServerV18::bind` so the apply
    // thread sees it when it spawns the per-worker continuous-loop pool.
    if let Some(n) = cli.io_threads {
        std::env::set_var("BEAVA_IO_THREADS", n.to_string());
        eprintln!("beava-bench-v18: --io-threads {n} → BEAVA_IO_THREADS={n}");
    }

    let pipeline = load_pipeline(&cli.pipeline)?;
    let parallel = cli
        .parallel
        .unwrap_or_else(|| std::cmp::min(8, num_cpus_or_default()));

    eprintln!(
        "beava-bench-v18: pipeline={} transport={} wire_format={} duration_secs={} parallel={} seed={} get_sample_ms={} get_batch_keys={} blast_shape={} total_events={:?}",
        pipeline.name,
        cli.transport.label(),
        cli.wire_format.label(),
        cli.duration_secs,
        parallel,
        cli.seed,
        cli.get_sample_interval_ms,
        cli.get_batch_keys,
        cli.blast_shape.label(),
        cli.total_events,
    );

    // Bind a local ServerV18, or connect to an existing remote one. Remote
    // mode is the "many bench clients, one server" pattern used to escape
    // per-process glibc mmap_lock contention that caps in-process push EPS.
    let (http_addr, tcp_addr, _local_server_handles) = if let Some(remote) = &cli.remote_addr {
        let parts: Vec<&str> = remote.split(',').collect();
        anyhow::ensure!(
            parts.len() == 2,
            "--remote-addr expects \"host:http_port,host:tcp_port\", got {:?}",
            remote
        );
        let http: std::net::SocketAddr = parts[0]
            .parse()
            .with_context(|| format!("parse remote http addr {:?}", parts[0]))?;
        let tcp: std::net::SocketAddr = parts[1]
            .parse()
            .with_context(|| format!("parse remote tcp addr {:?}", parts[1]))?;
        eprintln!(
            "beava-bench-v18: REMOTE mode http={} tcp={} (skipping bind/register/prewarm)",
            http, tcp
        );
        (http, tcp, None)
    } else {
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

        // Hold dirs + shutdown so they outlive the bench (drop closes them).
        (
            http_addr,
            tcp_addr,
            Some((wal_dir, snap_dir, shutdown_tx, serve_task)),
        )
    };

    // Wait for the server to start accepting.
    tokio::time::sleep(Duration::from_millis(150)).await;

    let http_base = format!("http://{}", http_addr);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .pool_max_idle_per_host(parallel + 4)
        .build()
        .context("build reqwest client")?;

    if cli.remote_addr.is_none() {
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
        eprintln!("beava-bench-v18: pre-warm done");
    } else {
        eprintln!("beava-bench-v18: REMOTE mode — skipping pipeline register + pre-warm (assumed already done)");
    }

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

    let report = format_report(&pipeline, cli.transport, cli.wire_format, &cli, &result);
    if !cli.no_ledger {
        println!("{}", report.ledger_row);
    }
    eprintln!("\n=== beava-bench-v18 summary ===\n{}\n", report.human);

    // Invariant tuple {requested, pushed, acked}: printed unconditionally so
    // smoke tests can grep for it; asserted equal when `--total-events` is
    // set. `pushes` is incremented by the receiver per ack (so
    // `push_count == acked`); `pushes_cap` tracks issued frames before
    // `write_all` and is not reused here.
    let requested = cli.total_events.unwrap_or(0);
    let pushed = result.push_count;
    let acked = pushed;
    eprintln!(
        "beava-bench-v18: invariant_tuple requested={requested} pushed={pushed} acked={acked}",
    );
    if cli.total_events.is_some() {
        anyhow::ensure!(
            requested == pushed,
            "{{requested, pushed}} mismatch — measurement error: requested={requested} pushed={pushed} acked={acked}",
        );
    }

    if cli.isolation_mode {
        let wall_clock_ms = result.elapsed.as_millis() as u64;
        let send_drain_ms = result.send_drain_ms;
        let ack_lag_ms = wall_clock_ms.saturating_sub(send_drain_ms);
        eprintln!(
            "beava-bench-v18: isolation_mode wall_clock_ms={wall_clock_ms} send_drain_ms={send_drain_ms} ack_lag_ms={ack_lag_ms}",
        );
    }

    if let Some((_wal_dir, _snap_dir, shutdown_tx, serve_task)) = _local_server_handles {
        let _ = shutdown_tx.send(());
        let _ = tokio::time::timeout(Duration::from_secs(5), serve_task).await;
    }

    Ok(())
}

#[derive(Debug)]
struct WorkloadResult {
    /// `push_count / elapsed.as_secs_f64()` — the wire-rate the run
    /// produced. NOT necessarily a sustained rate: when the run was capped
    /// early (typically by `--total-events`), this is a *burst* rate, and
    /// `format_report` switches the human-report label from `sustained_eps:`
    /// to `burst_eps:` accordingly (Plan 13.7.6-27 Bug 2). The field name
    /// is kept for API stability; the label renders the methodology.
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
    /// `last_send_ts - first_send_ts` across all workers, in milliseconds.
    /// Zero when no sends were captured (e.g., legacy duration-only mode where
    /// the timestamps are not wired). Populated on every continuous + burst
    /// run; the smoke test reads it via the `send_drain_ms` field.
    send_drain_ms: u64,
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

    // When --total-events is set, --duration-secs is a safety upper bound
    // only; raise the floor to 1h so an unbounded runaway is impossible.
    let effective_duration_secs = if cli.total_events.is_some() {
        cli.duration_secs.max(3600)
    } else {
        cli.duration_secs
    };
    let deadline = Instant::now() + Duration::from_secs(effective_duration_secs);

    // Hard-cap counter: sender uses `fetch_add(1, Relaxed) >= cap` before
    // `write_all` so `{requested, pushed, acked}` stay equal at run end.
    let pushes_cap = Arc::new(AtomicU64::new(0));
    let total_events_cap: Option<u64> = cli.total_events;
    // Per-worker first/last send timestamps for --isolation-mode.
    let first_send_ts: Arc<AsyncMutex<Option<Instant>>> = Arc::new(AsyncMutex::new(None));
    let last_send_ts: Arc<AsyncMutex<Option<Instant>>> = Arc::new(AsyncMutex::new(None));
    // Barrier of size (parallel + 1): workers await AFTER pool-build, BEFORE
    // first push; the main task awaits as the (parallel + 1)-th party THEN
    // sets `start`. Excludes pool-build time from wall_clock_ms.
    let pool_ready_barrier = Arc::new(tokio::sync::Barrier::new(parallel + 1));

    // Mixed-event-name list: harvest distinct event names from
    // pipeline.register["nodes"] where node["kind"] == "event"; if fewer than
    // mixed_event_count distinct events, pad with synthetic "e1", "e2", ...
    let mixed_event_names: Vec<String> = {
        let mut names: Vec<String> = pipeline
            .register
            .get("nodes")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter(|n| n.get("kind").and_then(|k| k.as_str()) == Some("event"))
                    .filter_map(|n| n.get("name").and_then(|s| s.as_str()).map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        if names.is_empty() {
            names.push(pipeline.event_name.clone());
        }
        let want = cli.mixed_event_count.max(1);
        let original_len = names.len();
        while names.len() < want {
            let i = names.len();
            names.push(format!("e{i}"));
        }
        if names.len() > want {
            names.truncate(want);
        }
        if matches!(cli.blast_shape, BlastShapeArg::Mixed) && original_len < cli.mixed_event_count {
            eprintln!(
                "warning: --blast-shape=mixed but pipeline has only {} distinct events; padding to {} with synthetic names",
                original_len, cli.mixed_event_count
            );
        }
        names
    };
    let blast_shape = cli.blast_shape.to_blast_shape(cli);

    // Background-task stop signal: `tokio::sync::watch` lets each sampler
    // race its sleep against the stop signal in `tokio::select!` and exit
    // within select-poll latency of `stop`, not up to a full interval past.
    // Required for honest wall_clock_ms measurement at any N — see the
    // `bench_wallclock_capture_order` regression test.
    let (bg_stop_tx, _) = tokio::sync::watch::channel::<()>(());

    let peak_rss = Arc::new(AtomicU64::new(0));
    let peak_rss_clone = Arc::clone(&peak_rss);
    let pid = std::process::id();
    let mut rss_stop_rx = bg_stop_tx.subscribe();
    let rss_task = tokio::spawn(async move {
        loop {
            // Race the stop signal against the 500 ms sample interval so the
            // task exits within `select!` poll latency of `stop`, not up to
            // a full 500 ms after.
            tokio::select! {
                biased;
                _ = rss_stop_rx.changed() => break,
                _ = tokio::time::sleep(Duration::from_millis(500)) => {
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
                }
            }
        }
    });

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
    let mut get_stop_rx = bg_stop_tx.subscribe();
    let get_transport = cli.transport;
    let get_tcp_addr = tcp_addr;
    let get_task = tokio::spawn(async move {
        use rand::Rng;
        let mut rng = sampling_rng(get_seed.wrapping_add(0xDEAD));

        // TCP path drives /get over the framed-TCP OP_GET_MULTI opcode (same
        // multi-key + multi-feature body shape as HTTP /get); HTTP path keeps
        // the reqwest client. Both share the same apply thread server-side.
        let mut tcp_client: Option<beava_server::testing::TcpClient> =
            if matches!(get_transport, Transport::Tcp) {
                match beava_server::testing::TcpClient::connect(get_tcp_addr).await {
                    Ok(c) => Some(c),
                    Err(e) => {
                        eprintln!("get_task TcpClient::connect failed: {e}");
                        None
                    }
                }
            } else {
                None
            };

        loop {
            // Race the stop signal against the get-interval sleep — without
            // this the task lingers up to `get_interval_ms` (default 1000)
            // past `stop`, contaminating wall_clock_ms for any N where the
            // genuine bench time is shorter than the sleep cycle.
            tokio::select! {
                biased;
                _ = get_stop_rx.changed() => break,
                _ = tokio::time::sleep(Duration::from_millis(get_interval_ms)) => {
                    let keys: Vec<String> = (0..get_batch_keys)
                        .map(|_| format!("k{:08}", rng.gen_range(0..KEY_SPACE)))
                        .collect();
                    let body =
                        serde_json::json!({"keys": keys, "features": features_clone});

                    let start = Instant::now();

                    let ok = match (get_transport, tcp_client.as_mut()) {
                        (Transport::Tcp, Some(client)) => {
                            // OP_GET_MULTI body is JSON
                            // {"keys": [...], "features": [...]}; success
                            // returns OP_GET_RESPONSE, failure returns
                            // OP_ERROR_RESPONSE.
                            let payload = Bytes::from(body.to_string().into_bytes());
                            match client.send_raw(OP_GET_MULTI, CT_JSON, payload).await {
                                Ok(frame) => frame.op == OP_GET_RESPONSE,
                                Err(_) => {
                                    // Reconnect on transport error; skip this sample.
                                    if let Ok(c) = beava_server::testing::TcpClient::connect(get_tcp_addr).await {
                                        tcp_client = Some(c);
                                    }
                                    false
                                }
                            }
                        }
                        _ => {
                            // HTTP, or TCP-with-no-client.
                            match get_client
                                .post(&get_url)
                                .header("Content-Type", "application/json")
                                .body(body.to_string())
                                .send()
                                .await
                            {
                                Ok(r) if r.status().is_success() => {
                                    let _ = r.bytes().await;
                                    true
                                }
                                _ => false,
                            }
                        }
                    };

                    if ok {
                        let elapsed_us = start.elapsed().as_micros() as u64;
                        let mut h = get_hist_clone.lock().await;
                        let _ = h.record(elapsed_us.max(1));
                        drop(h);
                        get_samples_clone.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    });

    // Parallel back-to-back read workers (read-throughput mode). Each worker
    // holds its own TcpClient/reqwest client and issues /get requests as fast
    // as the server responds — no sleep between requests. Aggregate reads
    // across all workers count toward `get_samples_counter` (the total
    // completed-read count under this mode) and `get_hist` (aggregated
    // latency histogram).
    let read_workers_count = cli.read_workers;
    let mut read_worker_handles: Vec<tokio::task::JoinHandle<()>> =
        Vec::with_capacity(read_workers_count);
    for read_worker_id in 0..read_workers_count {
        let read_hist = Arc::clone(&get_hist);
        let read_samples = Arc::clone(&get_samples_counter);
        let read_seed = cli
            .seed
            .wrapping_add(0xBADC0FEE)
            .wrapping_add(read_worker_id as u64 * 0x9E37);
        let read_features = pipeline.features.clone();
        let read_batch_keys = cli.get_batch_keys;
        let read_transport = cli.transport;
        let read_tcp_addr = tcp_addr;
        let read_get_url = format!("http://{}/get", http_addr);
        let read_http_client = Arc::clone(&http_client);
        let read_stop_rx = bg_stop_tx.subscribe();

        read_worker_handles.push(tokio::spawn(async move {
            use rand::Rng;
            let mut rng = sampling_rng(read_seed);

            let mut tcp_client: Option<beava_server::testing::TcpClient> =
                if matches!(read_transport, Transport::Tcp) {
                    match beava_server::testing::TcpClient::connect(read_tcp_addr).await {
                        Ok(c) => Some(c),
                        Err(e) => {
                            eprintln!(
                                "read_worker {read_worker_id} TcpClient::connect failed: {e}"
                            );
                            None
                        }
                    }
                } else {
                    None
                };

            loop {
                if read_stop_rx.has_changed().unwrap_or(false) {
                    break;
                }

                let keys: Vec<String> = (0..read_batch_keys)
                    .map(|_| format!("k{:08}", rng.gen_range(0..KEY_SPACE)))
                    .collect();
                let body = serde_json::json!({"keys": keys, "features": read_features});

                let start = Instant::now();
                let ok = match (read_transport, tcp_client.as_mut()) {
                    (Transport::Tcp, Some(client)) => {
                        let payload = Bytes::from(body.to_string().into_bytes());
                        match client.send_raw(OP_GET_MULTI, CT_JSON, payload).await {
                            Ok(frame) => frame.op == OP_GET_RESPONSE,
                            Err(_) => {
                                if let Ok(c) =
                                    beava_server::testing::TcpClient::connect(read_tcp_addr).await
                                {
                                    tcp_client = Some(c);
                                }
                                false
                            }
                        }
                    }
                    _ => match read_http_client
                        .post(&read_get_url)
                        .header("Content-Type", "application/json")
                        .body(body.to_string())
                        .send()
                        .await
                    {
                        Ok(r) if r.status().is_success() => {
                            let _ = r.bytes().await;
                            true
                        }
                        _ => false,
                    },
                };

                if ok {
                    let elapsed_us = start.elapsed().as_micros() as u64;
                    let mut h = read_hist.lock().await;
                    let _ = h.record(elapsed_us.max(1));
                    drop(h);
                    read_samples.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    let mut workers = Vec::with_capacity(parallel);
    for worker_id in 0..parallel {
        let stop = Arc::clone(&stop);
        let pushes = Arc::clone(&pushes);
        let pushes_cap_w = Arc::clone(&pushes_cap);
        let errors = Arc::clone(&errors);
        let push_hist = Arc::clone(&push_hist);
        let pipeline_clone = pipeline.clone();
        let mixed_event_names_w = mixed_event_names.clone();
        let first_send_ts_w = Arc::clone(&first_send_ts);
        let last_send_ts_w = Arc::clone(&last_send_ts);
        let pool_barrier_w = Arc::clone(&pool_ready_barrier);
        let seed = cli.seed.wrapping_add(worker_id as u64 * 0x9E37);
        let http_url = format!("http://{}/push/{}", http_addr, pipeline.event_name);
        let transport = cli.transport;
        let wf = wire_format;
        let client = Arc::clone(&http_client);
        let pipeline_depth = cli.pipeline_depth.max(1);
        let continuous_pipeline = cli.continuous_pipeline;
        let total_cap = total_events_cap;
        let bs = blast_shape;

        workers.push(tokio::spawn(async move {
            run_push_worker(
                worker_id,
                seed,
                stop,
                pushes,
                pushes_cap_w,
                total_cap,
                errors,
                push_hist,
                pipeline_clone,
                bs,
                mixed_event_names_w,
                transport,
                wf,
                http_url,
                tcp_addr,
                deadline,
                client,
                pipeline_depth,
                continuous_pipeline,
                first_send_ts_w,
                last_send_ts_w,
                pool_barrier_w,
            )
            .await;
        }));
    }

    // Wait for ALL parallel workers to finish pool-build (or signal "no pool
    // to build" for the HTTP transport) before any worker starts pushing.
    // wall_clock_ms (set after barrier release) excludes pool-build time
    // uniformly across workers.
    pool_ready_barrier.wait().await;
    // wall_clock_ms timer starts NOW; excludes per-worker pool-build time.
    let start = Instant::now();
    while Instant::now() < deadline {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    stop.store(true, Ordering::Relaxed);

    for w in workers {
        let _ = w.await;
    }

    // Capture wall_clock_ms BEFORE awaiting `get_task` / `rss_task`. With
    // their old raw `loop { sleep }` form, awaiting them would inflate
    // elapsed by up to `get_interval_ms` + 500 ms at any N where the genuine
    // bench time is shorter than those sleep cycles. Background tasks now
    // use `tokio::select!` so they exit promptly when signalled, but the
    // elapsed capture stays before the awaits as a structural guard
    // (regression-pinned by `tests/bench_wallclock_capture_order.rs`).
    let elapsed = start.elapsed();

    // Signal background tasks to stop and join them for clean shutdown.
    let _ = bg_stop_tx.send(());
    let _ = get_task.await;
    for h in read_worker_handles {
        let _ = h.await;
    }
    let _ = rss_task.await;
    let push_count = pushes.load(Ordering::Relaxed);
    let push_errors = errors.load(Ordering::Relaxed);
    let push_h = push_hist.lock().await;
    let get_h = get_hist.lock().await;

    // Compute send_drain_ms from the per-worker first/last timestamps.
    let send_drain_ms: u64 = {
        let f = first_send_ts.lock().await;
        let l = last_send_ts.lock().await;
        match (*f, *l) {
            (Some(fst), Some(lst)) if lst >= fst => lst.duration_since(fst).as_millis() as u64,
            _ => 0,
        }
    };

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
        send_drain_ms,
    })
}

#[allow(clippy::too_many_arguments)]
async fn run_push_worker(
    _worker_id: usize,
    seed: u64,
    stop: Arc<AtomicBool>,
    pushes: Arc<AtomicU64>,
    pushes_cap: Arc<AtomicU64>,
    total_cap: Option<u64>,
    errors: Arc<AtomicU64>,
    push_hist: Arc<AsyncMutex<Histogram<u64>>>,
    pipeline: PipelineConfig,
    blast_shape: BlastShape,
    mixed_event_names: Vec<String>,
    transport: Transport,
    wire_format: WireFormat,
    http_url: String,
    tcp_addr: std::net::SocketAddr,
    deadline: Instant,
    http_client: Arc<reqwest::Client>,
    pipeline_depth: usize,
    continuous_pipeline: bool,
    first_send_ts: Arc<AsyncMutex<Option<Instant>>>,
    last_send_ts: Arc<AsyncMutex<Option<Instant>>>,
    pool_ready_barrier: Arc<tokio::sync::Barrier>,
) {
    // Continuous pipelining for TCP: when enabled (default), the TCP path
    // uses a split sender / receiver pattern with a semaphore-gated inflight
    // queue, eliminating the burst-mode sawtooth (apply-thread idles between
    // batches). HTTP and burst-mode TCP keep their existing single-task loop.
    if matches!(transport, Transport::Tcp) && continuous_pipeline {
        run_tcp_continuous_push_worker(
            seed,
            stop,
            pushes,
            pushes_cap,
            total_cap,
            errors,
            push_hist,
            pipeline,
            blast_shape,
            mixed_event_names,
            wire_format,
            tcp_addr,
            deadline,
            pipeline_depth,
            first_send_ts,
            last_send_ts,
            pool_ready_barrier,
        )
        .await;
        return;
    }

    if matches!(transport, Transport::Tcp) {
        // Burst-mode TCP using Pool=N + receiver-flips-stop pattern.
        run_tcp_burst_push_worker(
            seed,
            stop,
            pushes,
            pushes_cap,
            total_cap,
            errors,
            push_hist,
            pipeline,
            blast_shape,
            mixed_event_names,
            wire_format,
            tcp_addr,
            deadline,
            pipeline_depth,
            first_send_ts,
            last_send_ts,
            pool_ready_barrier,
        )
        .await;
        return;
    }

    // HTTP transport — no Pool=N (HTTP path is rarely the headline number).
    // Still wait on the barrier so wall_clock_ms is consistent with TCP runs.
    pool_ready_barrier.wait().await;

    let mut rng = sampling_rng(seed);
    let mut seq = 0_u64;
    let mut local_first: Option<Instant> = None;
    let mut local_last: Option<Instant> = None;
    while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
        // Hard cap before send so {requested, pushed, acked} stay equal.
        if let Some(cap) = total_cap {
            let prev = pushes_cap.fetch_add(1, Ordering::Relaxed);
            if prev >= cap {
                pushes_cap.fetch_sub(1, Ordering::Relaxed);
                break;
            }
        }
        let body = make_event_payload(&pipeline, seq, &mut rng);
        let start = Instant::now();
        if local_first.is_none() {
            local_first = Some(start);
        }
        let r = http_client
            .post(&http_url)
            .header("Content-Type", "application/json")
            .body(body.to_string())
            .send()
            .await;
        let elapsed_us = start.elapsed().as_micros() as u64;
        local_last = Some(Instant::now());
        match r {
            Ok(resp) => {
                if resp.status().is_success() {
                    let new_count = pushes.fetch_add(1, Ordering::Relaxed) + 1;
                    let mut h = push_hist.lock().await;
                    let _ = h.record(elapsed_us.max(1));
                    drop(h);
                    if let Some(cap) = total_cap {
                        if new_count >= cap {
                            stop.store(true, Ordering::Release);
                            break;
                        }
                    }
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
    publish_send_window(local_first, local_last, &first_send_ts, &last_send_ts).await;
    // Suppress unused-warnings for fields HTTP doesn't read.
    let _ = blast_shape;
    let _ = mixed_event_names;
}

/// Update the workload-wide `first_send_ts` / `last_send_ts` slots based on
/// this worker's locally-captured first/last send. Used for `send_drain_ms`
/// computation in `--isolation-mode`.
async fn publish_send_window(
    local_first: Option<Instant>,
    local_last: Option<Instant>,
    first_send_ts: &AsyncMutex<Option<Instant>>,
    last_send_ts: &AsyncMutex<Option<Instant>>,
) {
    if let Some(t) = local_first {
        let mut slot = first_send_ts.lock().await;
        if slot.map_or(true, |s| t < s) {
            *slot = Some(t);
        }
    }
    if let Some(t) = local_last {
        let mut slot = last_send_ts.lock().await;
        if slot.map_or(true, |s| t > s) {
            *slot = Some(t);
        }
    }
}

/// Build a Pool=N for this worker. Pool size is `total_cap` when set, otherwise
/// 1024 (legacy duration-only mode treats Pool=N as a small precomputed buffer
/// the sender cycles through). Returns `None` and prints a warning on builder
/// error so the worker can exit cleanly.
fn build_worker_pool(
    pipeline: &PipelineConfig,
    blast_shape: BlastShape,
    mixed_event_names: &[String],
    wire_format: WireFormat,
    seed: u64,
    total_cap: Option<u64>,
) -> Option<(Vec<Bytes>, Duration)> {
    let pool_n: u64 = total_cap.unwrap_or(1024);
    let names_ref: Vec<&str> = mixed_event_names.iter().map(String::as_str).collect();
    let pool_cfg = BlastShapeConfig {
        pipeline,
        event_names_for_mixed: &names_ref,
        wire_format: match wire_format {
            WireFormat::Json => beava_bench::blast_shape::WireFormat::Json,
            WireFormat::Msgpack => beava_bench::blast_shape::WireFormat::Msgpack,
        },
        seed,
    };
    match build_pool_timed(blast_shape, &pool_cfg, pool_n) {
        Ok(p) => Some(p),
        Err(e) => {
            eprintln!("build_pool_timed failed: {e}");
            None
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_tcp_burst_push_worker(
    seed: u64,
    stop: Arc<AtomicBool>,
    pushes: Arc<AtomicU64>,
    pushes_cap: Arc<AtomicU64>,
    total_cap: Option<u64>,
    errors: Arc<AtomicU64>,
    push_hist: Arc<AsyncMutex<Histogram<u64>>>,
    pipeline: PipelineConfig,
    blast_shape: BlastShape,
    mixed_event_names: Vec<String>,
    wire_format: WireFormat,
    tcp_addr: std::net::SocketAddr,
    deadline: Instant,
    pipeline_depth: usize,
    first_send_ts: Arc<AsyncMutex<Option<Instant>>>,
    last_send_ts: Arc<AsyncMutex<Option<Instant>>>,
    pool_ready_barrier: Arc<tokio::sync::Barrier>,
) {
    use beava_core::wire::Frame;
    use beava_server::testing::TcpClient;

    // Build Pool=N at worker startup. Pool-building time is NOT measured in
    // wall_clock_ms — wall_clock starts AFTER the barrier.
    let (pool, _pool_build_dur) = match build_worker_pool(
        &pipeline,
        blast_shape,
        &mixed_event_names,
        wire_format,
        seed,
        total_cap,
    ) {
        Some(p) => p,
        None => {
            // Wait on the barrier so other workers + the main task don't deadlock.
            pool_ready_barrier.wait().await;
            return;
        }
    };

    // Synchronize all workers BEFORE first push.
    pool_ready_barrier.wait().await;

    let mut client = match TcpClient::connect(tcp_addr).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("TcpClient::connect failed: {e}");
            return;
        }
    };
    let pdepth = pipeline_depth.max(1);
    let mut send_times: Vec<Instant> = Vec::with_capacity(pdepth);
    let mut idx: u64 = 0;
    let pool_len = pool.len() as u64;
    let mut local_first: Option<Instant> = None;
    let mut local_last: Option<Instant> = None;

    'outer: while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
        // Build N frames + send all of them, then drain N acks.
        send_times.clear();
        let mut send_err = false;
        for _ in 0..pdepth {
            if stop.load(Ordering::Relaxed) || Instant::now() >= deadline {
                break;
            }
            // Hard cap BEFORE write so {requested, pushed, acked} stay equal.
            if let Some(cap) = total_cap {
                let prev = pushes_cap.fetch_add(1, Ordering::Relaxed);
                if prev >= cap {
                    pushes_cap.fetch_sub(1, Ordering::Relaxed);
                    break;
                }
            }
            let frame_bytes = pool[(idx % pool_len) as usize].clone();
            idx = idx.wrapping_add(1);
            // Pool entries are full encoded frames `[len][op][ct][payload]`.
            // TcpClient::write_frame re-encodes from a Frame struct, so we
            // decode just enough to get `(op, ct, payload)` back and let it
            // re-encode. ~50 ns per call is "free" relative to the apply hot
            // path and keeps the public TcpClient API intact.
            let (op, ct, payload) = match decode_pool_frame(&frame_bytes) {
                Some(t) => t,
                None => {
                    errors.fetch_add(1, Ordering::Relaxed);
                    send_err = true;
                    break;
                }
            };
            let frame = Frame {
                op,
                content_type: ct,
                payload,
            };
            let send_ts = Instant::now();
            if local_first.is_none() {
                local_first = Some(send_ts);
            }
            if client.write_frame(&frame).await.is_err() {
                send_err = true;
                break;
            }
            send_times.push(send_ts);
            local_last = Some(Instant::now());
        }
        let sent = send_times.len();
        if sent == 0 {
            if send_err {
                if let Ok(c) = TcpClient::connect(tcp_addr).await {
                    client = c;
                }
                continue;
            }
            // No work issued AND no error — must be that the cap was reached
            // before any push in this batch. Exit so we don't spin while
            // waiting for the receiver to flip `stop` (other workers may
            // still be reading their own in-flight acks).
            if total_cap.is_some() {
                break 'outer;
            }
            continue;
        }
        // Read N acks (strict FIFO order — Redis-style wire contract).
        match client.read_n_frames(sent).await {
            Ok(frames) => {
                let mut h = push_hist.lock().await;
                for (f, send_ts) in frames.iter().zip(send_times.iter()) {
                    if f.op == OP_PUSH {
                        let new_count = pushes.fetch_add(1, Ordering::Relaxed) + 1;
                        let elapsed_us = send_ts.elapsed().as_micros() as u64;
                        let _ = h.record(elapsed_us.max(1));
                        // When ack count crosses the cap, flip stop. No
                        // semaphore in burst mode; just store + break.
                        if let Some(cap) = total_cap {
                            if new_count >= cap {
                                stop.store(true, Ordering::Release);
                                drop(h);
                                break 'outer;
                            }
                        }
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

    publish_send_window(local_first, local_last, &first_send_ts, &last_send_ts).await;
}

/// Decode the leading `[u32 len][u16 op][u8 ct][payload]` from a pre-encoded
/// pool entry into `(op, content_type, Bytes payload)`. Wire is big-endian
/// (matches `beava_core::wire::encode_frame`). Returns `None` on a malformed
/// frame — the worker bumps `errors` in that case.
fn decode_pool_frame(buf: &Bytes) -> Option<(u16, u8, Bytes)> {
    if buf.len() < 7 {
        return None;
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if buf.len() < 4 + len {
        return None;
    }
    let op = u16::from_be_bytes([buf[4], buf[5]]);
    let ct = buf[6];
    let payload_start = 7;
    let payload_end = 4 + len;
    if payload_end < payload_start {
        return None;
    }
    let payload = buf.slice(payload_start..payload_end);
    Some((op, ct, payload))
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
    pushes_cap: Arc<AtomicU64>,
    total_cap: Option<u64>,
    errors: Arc<AtomicU64>,
    push_hist: Arc<AsyncMutex<Histogram<u64>>>,
    pipeline: PipelineConfig,
    blast_shape: BlastShape,
    mixed_event_names: Vec<String>,
    wire_format: WireFormat,
    tcp_addr: std::net::SocketAddr,
    deadline: Instant,
    pipeline_depth: usize,
    first_send_ts: Arc<AsyncMutex<Option<Instant>>>,
    last_send_ts: Arc<AsyncMutex<Option<Instant>>>,
    pool_ready_barrier: Arc<tokio::sync::Barrier>,
) {
    use beava_core::wire::decode_frame;
    use bytes::BytesMut;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::sync::Semaphore;

    let pdepth = pipeline_depth.max(1);

    // Build Pool=N at worker startup. Pool-building time is NOT measured in
    // wall_clock_ms — wall_clock starts AFTER the pool-ready barrier.
    let (pool, _pool_build_dur) = match build_worker_pool(
        &pipeline,
        blast_shape,
        &mixed_event_names,
        wire_format,
        seed,
        total_cap,
    ) {
        Some(p) => p,
        None => {
            // Wait on the barrier so other workers + the main task don't deadlock.
            pool_ready_barrier.wait().await;
            return;
        }
    };

    // Synchronize all workers BEFORE first push.
    pool_ready_barrier.wait().await;

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

    let sender_stop = Arc::clone(&stop);
    let sender_errors = Arc::clone(&errors);
    let sender_sem = Arc::clone(&sem);
    let sender_pushes_cap = Arc::clone(&pushes_cap);
    let sender_total_cap = total_cap;
    let sender_first_send_ts = Arc::clone(&first_send_ts);
    let sender_last_send_ts = Arc::clone(&last_send_ts);
    let sender_pool = pool;
    let sender_handle = tokio::spawn(async move {
        let mut write_half = write_half;
        let mut idx: u64 = 0;
        let pool_len = sender_pool.len() as u64;
        let mut local_first: Option<Instant> = None;
        let mut local_last: Option<Instant> = None;
        loop {
            if sender_stop.load(Ordering::Relaxed) || Instant::now() >= deadline {
                break;
            }
            // Hard cap — break BEFORE write_all so {requested, pushed, acked}
            // stay equal.
            if let Some(cap) = sender_total_cap {
                let prev = sender_pushes_cap.fetch_add(1, Ordering::Relaxed);
                if prev >= cap {
                    sender_pushes_cap.fetch_sub(1, Ordering::Relaxed);
                    break;
                }
            }
            // Acquire one inflight slot. `acquire_owned` returns a permit
            // bound to the sem Arc. We `forget()` it: the receiver is the
            // one that gives the permit back via `add_permits(1)` once the
            // ack arrives. Without `forget`, the permit would be returned
            // here on drop and the gate would have no effect.
            let permit = match Arc::clone(&sender_sem).acquire_owned().await {
                Ok(p) => p,
                Err(_) => break, // sender_sem.close() from receiver — exit
            };
            permit.forget();

            let frame_bytes = &sender_pool[(idx % pool_len) as usize];
            idx = idx.wrapping_add(1);

            let send_ts = Instant::now();
            if local_first.is_none() {
                local_first = Some(send_ts);
            }
            if write_half.write_all(frame_bytes).await.is_err() {
                sender_errors.fetch_add(1, Ordering::Relaxed);
                break;
            }
            local_last = Some(Instant::now());
            // Notify receiver of the send-timestamp for this in-flight ack.
            // unbounded_send only fails if receiver dropped the channel; in
            // that case the receiver task has exited and we should too.
            if ts_tx.send(send_ts).is_err() {
                break;
            }
        }
        // Drop ts_tx to signal EOF to the receiver after we've written
        // everything.
        drop(ts_tx);
        publish_send_window(
            local_first,
            local_last,
            &sender_first_send_ts,
            &sender_last_send_ts,
        )
        .await;
    });

    // Receiver loop (this task). Burst mode locks `push_hist` ONCE per
    // N-event batch and records N values inside the lock. Continuous mode
    // would otherwise lock once per event — 256× more lock ops at pd=256,
    // contending with the other 15 workers' receivers. Mirror burst's
    // batching by accumulating elapsed_us into a local Vec and flushing
    // every `HIST_FLUSH_BATCH` records (or on shutdown) in a single lock.
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
                        let new_count = pushes.fetch_add(1, Ordering::Relaxed) + 1;
                        latency_batch.push(elapsed_us.max(1));
                        if latency_batch.len() >= HIST_FLUSH_BATCH {
                            flush_latencies(&push_hist, &mut latency_batch).await;
                        }
                        // Release the inflight slot.
                        sem.add_permits(1);
                        // When ack count crosses the cap, flip stop AND close
                        // the semaphore so the sender (blocked on
                        // `acquire_owned().await`) returns `Err` and exits.
                        if let Some(cap) = total_cap {
                            if new_count >= cap {
                                stop.store(true, Ordering::Release);
                                sem.close();
                                break 'recv;
                            }
                        }
                    } else {
                        errors.fetch_add(1, Ordering::Relaxed);
                        sem.add_permits(1);
                    }
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
        // Race the socket read against a periodic wake so we re-check `stop`
        // even if no bytes arrive. Without this, a receiver whose sender has
        // exited (sender drops `ts_tx`, all in-flight acks already drained)
        // can park indefinitely on `read_buf.await` while the global `stop`
        // is already true.
        let read_fut = read_half.read_buf(&mut read_buf);
        tokio::pin!(read_fut);
        let n = tokio::select! {
            r = &mut read_fut => match r {
                Ok(0) => break, // peer closed
                Ok(n) => n,
                Err(_) => break,
            },
            _ = tokio::time::sleep(Duration::from_millis(50)) => {
                if stop.load(Ordering::Relaxed) || Instant::now() >= deadline {
                    break;
                }
                continue;
            }
        };
        let _ = n;
    }

    // Flush any tail-batch latencies before exiting.
    flush_latencies(&push_hist, &mut latency_batch).await;

    // Wake up the sender if it's parked on `acquire_owned().await`. The cap-
    // cross path inside the 'recv loop already calls `sem.close()` (line ~1515)
    // before breaking — but every OTHER exit path (deadline fired with `stop`
    // set by the main task; socket EOF; decode error; 50 ms tick + stop) leaves
    // the sender potentially parked on a permit that will never become
    // available because we won't be calling `add_permits(1)` again. Without
    // this close, the trailing `sender_handle.await` below deadlocks: receiver
    // waits for sender, sender waits for permit, neither ever fires.
    //
    // 13.7.6-27 root cause: pre-fix, `--duration-secs N` (no --total-events)
    // hung at 0.1 % CPU for 14+ minutes before being killed; the cap-cross
    // path was the only branch that closed the semaphore, so the
    // deadline-only path always deadlocked when the sender happened to be
    // awaiting a permit at the moment `stop` flipped.
    //
    // Idempotent — if already closed (cap-cross path), this is a no-op.
    sem.close();

    // Wait for the sender to finish before returning so the worker doesn't
    // outlive its sender task (avoids tokio "task stopped" warnings).
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
    // Burst-vs-sustained label discipline (Plan 13.7.6-27 Bug 2).
    //
    // Pre-fix, the `sustained_eps:` label was emitted unconditionally — even
    // when `--total-events N` capped the run before `--duration-secs` elapsed.
    // At ~600K EPS, `--total-events 1_000_000 --duration-secs 60` finished in
    // ~1.5 s (well below the 60 s deadline), so the reported number was a
    // 1.5 s burst rate mislabelled as a 60 s sustained rate. Most committed
    // numbers in `.planning/throughput-baselines.md` through Phase 13 are
    // affected — Plan 13.7.6-28 will re-baseline using true sustained runs.
    //
    // Rule: when `elapsed >= duration_secs * 0.95`, the run actually spent
    // its full deadline window saturating the apply thread → emit
    // `sustained_eps:`. Otherwise the run was capped early (most commonly
    // by `--total-events`) → emit `burst_eps:`. The label itself surfaces
    // the methodology distinction so a reader can tell at a glance whether
    // the number is a sustained or a burst measurement.
    let elapsed_secs = r.elapsed.as_secs_f64();
    let duration_secs = cli.duration_secs as f64;
    let is_sustained_run = elapsed_secs >= duration_secs * 0.95;
    let eps_label = if is_sustained_run {
        "sustained_eps:"
    } else {
        "burst_eps:    "
    };
    let mut human = format!(
        "pipeline:         {}\n\
         transport:        {}\n\
         wire_format:      {}\n\
         duration_secs:    {}\n\
         parallel:         {}\n\
         pushes:           {}\n\
         push_errors:      {}\n\
         {} {:.0}\n\
         push p50/p95/p99: {} / {} / {} µs\n\
         get_samples:      {}\n\
         read_workers:     {}\n\
         read_throughput:  {:.0} reads/sec\n\
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
        eps_label,
        r.sustained_eps,
        r.push_p50_us,
        r.push_p95_us,
        r.push_p99_us,
        r.get_samples,
        cli.read_workers,
        r.get_samples as f64 / r.elapsed.as_secs_f64(),
        r.get_p99_us,
        r.peak_rss_mb,
        r.elapsed,
        hw_class_string(),
        commit,
    );
    if cli.isolation_mode {
        let wall_clock_ms = r.elapsed.as_millis() as u64;
        let send_drain_ms = r.send_drain_ms;
        let ack_lag_ms = wall_clock_ms.saturating_sub(send_drain_ms);
        human.push_str(&format!(
            "blast_shape:      {}\n\
             total_events:    {:?}\n\
             wall_clock_ms:    {}\n\
             send_drain_ms:    {}\n\
             ack_lag_ms:       {}\n",
            cli.blast_shape.label(),
            cli.total_events,
            wall_clock_ms,
            send_drain_ms,
            ack_lag_ms,
        ));
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that decode_pool_frame parses the same encoding produced by
    /// `beava_core::wire::encode_frame`.
    #[test]
    fn decode_pool_frame_parses_encoded_frame() {
        use beava_core::wire::{encode_frame, Frame, CT_JSON};
        use bytes::BytesMut;
        let frame = Frame {
            op: OP_PUSH,
            content_type: CT_JSON,
            payload: Bytes::from_static(b"{\"event\":\"Txn\",\"body\":{}}"),
        };
        let mut buf = BytesMut::new();
        encode_frame(&frame, &mut buf);
        let bytes = buf.freeze();
        let (op, ct, payload) = decode_pool_frame(&bytes).expect("decode");
        assert_eq!(op, OP_PUSH);
        assert_eq!(ct, CT_JSON);
        assert_eq!(&payload[..], b"{\"event\":\"Txn\",\"body\":{}}");
    }

    #[test]
    fn decode_pool_frame_rejects_truncated() {
        let bytes = Bytes::from_static(&[0u8, 0, 0, 0, 0, 0]); // <7 bytes
        assert!(decode_pool_frame(&bytes).is_none());
    }

    /// `--io-threads N` must parse into `Cli::io_threads = Some(N)` so users
    /// can override the auto-default IoPool worker count without setting the
    /// `BEAVA_IO_THREADS` env var.
    ///
    /// Rationale: the Hetzner sweep on 2026-04-28 (16-vCPU vServer) showed
    /// the auto-default `iot=4` is the worst tested config for fraud-team
    /// workloads, with ~10% lift available at `iot=2` or `iot=8`. Exposing
    /// the CLI knob lets users tune without env-var ceremony.
    #[test]
    fn cli_parses_io_threads_flag() {
        let cli = Cli::try_parse_from(["beava-bench-v18", "--io-threads", "7"])
            .expect("parses --io-threads 7");
        assert_eq!(
            cli.io_threads,
            Some(7),
            "--io-threads 7 must parse to Some(7)"
        );
    }

    #[test]
    fn cli_io_threads_defaults_to_none() {
        let cli = Cli::try_parse_from(["beava-bench-v18"]).expect("parses with no args");
        assert!(
            cli.io_threads.is_none(),
            "no --io-threads flag means auto-default (BEAVA_IO_THREADS env or formula)"
        );
    }
}
