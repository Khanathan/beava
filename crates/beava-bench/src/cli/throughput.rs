//! Throughput mode — production benchmark surface.
//!
//! Plan 13.7.6-32 reverses Plan 13.7.6-24's strip: `--parallel N` is restored,
//! the v18 production harness is now reachable via `beava-bench throughput`,
//! and the standalone `beava-bench-v18` binary is gone. The flag shape mirrors
//! v18's exactly so the throughput-baselines.md ledger reproduces 1-for-1
//! against the unified binary.
//!
//! Smoke-test runs are still cheap (set `--parallel 1 --duration-secs 1` for
//! a quick sanity check). The production headline numbers (Plan 13.7.6-28's
//! 660K EPS sustained / 60 s on small/tcp Apple-M4) reproduce via
//! `beava-bench throughput --parallel 16 --pipeline small --transport tcp
//! --wire-format msgpack --duration-secs 60 --pipeline-depth 1024 --no-ledger`.

use anyhow::Result;
use clap::Args;

use crate::harness::production::{self, BlastShapeArg, ProductionConfig, Transport, WireFormat};

/// Throughput-subcommand CLI args. Mirrors `beava-bench-v18`'s pre-Plan-32 CLI
/// shape verbatim so the consolidated binary reproduces ledger numbers without
/// flag-rename ceremony. See `harness::production::ProductionConfig` for the
/// downstream struct.
#[derive(Debug, Args, Clone)]
pub struct ThroughputArgs {
    /// Pipeline config name (small, medium, large, fraud-team, ...) OR an
    /// explicit JSON file path.
    #[arg(long, default_value = "small")]
    pub pipeline: String,

    /// Transport to use.
    #[arg(long, value_enum, default_value_t = Transport::Tcp)]
    pub transport: Transport,

    /// Wire format for TCP pushes (json or msgpack). HTTP always uses JSON.
    #[arg(long, value_enum, default_value_t = WireFormat::Json)]
    pub wire_format: WireFormat,

    /// Wall-time duration in seconds. Becomes a safety upper bound only when
    /// `--total-events` is set.
    #[arg(long, default_value_t = 60)]
    pub duration_secs: u64,

    /// Number of parallel push workers. Defaults to min(8, num_cpus). Set
    /// `--parallel 1` for a single-threaded smoke run.
    #[arg(long)]
    pub parallel: Option<usize>,

    /// Random seed.
    #[arg(long, default_value_t = 0xCAFE_BABE_u64)]
    pub seed: u64,

    /// How often to sample batch /get latency (ms).
    #[arg(long, default_value_t = 1000)]
    pub get_sample_interval_ms: u64,

    /// Keys per batch /get sample.
    #[arg(long, default_value_t = 100)]
    pub get_batch_keys: usize,

    /// Number of parallel back-to-back /get worker tasks. Each worker holds
    /// its own `TcpClient` connection (TCP transport) or reqwest client
    /// (HTTP transport) and issues /get requests as fast as the server
    /// responds — no sleep between requests. Use this to measure read
    /// throughput, not just sampled latency.
    #[arg(long, default_value_t = 0)]
    pub read_workers: usize,

    /// Suppress markdown ledger row; only print human summary.
    #[arg(long)]
    pub no_ledger: bool,

    /// Connect to an existing remote beava server instead of binding our
    /// own ServerV18. Format: `host:http_port,host:tcp_port`.
    #[arg(long)]
    pub remote_addr: Option<String>,

    /// TCP pipeline depth — caps inflight pushes per worker connection.
    #[arg(long, default_value_t = 1)]
    pub pipeline_depth: usize,

    /// TCP continuous pipelining (default true). Set `--continuous-pipeline=false`
    /// to fall back to the burst pattern. HTTP transport ignores this.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub continuous_pipeline: bool,

    /// Send a fixed total number of events instead of running for
    /// `--duration-secs`.
    #[arg(long)]
    pub total_events: Option<u64>,

    /// Blast shape — distribution that the pre-encoded frame Pool=N is built
    /// from.
    #[arg(long, value_enum, default_value_t = BlastShapeArg::Fixed)]
    pub blast_shape: BlastShapeArg,

    /// Zipfian alpha skew (only used when --blast-shape=zipfian).
    #[arg(long, default_value_t = 1.0)]
    pub zipf_alpha: f64,

    /// Cardinality K for uniform/zipfian shapes.
    #[arg(long, default_value_t = 1_000_000)]
    pub cardinality: u64,

    /// Number of distinct event names for --blast-shape=mixed.
    #[arg(long, default_value_t = 3)]
    pub mixed_event_count: usize,

    /// Isolation mode — print wall_clock_ms / send_drain_ms / ack_lag_ms columns.
    #[arg(long, default_value_t = false)]
    pub isolation_mode: bool,

    /// IoPool worker count override (sets BEAVA_IO_THREADS for ServerV18 to
    /// pick up at boot). See harness::production for the per-hw-class default.
    #[arg(long)]
    pub io_threads: Option<usize>,
}

impl From<ThroughputArgs> for ProductionConfig {
    fn from(args: ThroughputArgs) -> Self {
        ProductionConfig {
            pipeline: args.pipeline,
            transport: args.transport,
            wire_format: args.wire_format,
            duration_secs: args.duration_secs,
            parallel: args.parallel,
            seed: args.seed,
            get_sample_interval_ms: args.get_sample_interval_ms,
            get_batch_keys: args.get_batch_keys,
            read_workers: args.read_workers,
            no_ledger: args.no_ledger,
            remote_addr: args.remote_addr,
            pipeline_depth: args.pipeline_depth,
            continuous_pipeline: args.continuous_pipeline,
            total_events: args.total_events,
            blast_shape: args.blast_shape,
            zipf_alpha: args.zipf_alpha,
            cardinality: args.cardinality,
            mixed_event_count: args.mixed_event_count,
            isolation_mode: args.isolation_mode,
            io_threads: args.io_threads,
        }
    }
}

pub fn run_throughput(args: ThroughputArgs) -> Result<()> {
    let cfg: ProductionConfig = args.into();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(production::run_production(&cfg))
}
