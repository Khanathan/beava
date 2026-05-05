//! `beava bench <mode>` CLI — the polished subcommand surface.
//!
//! Standalone bench binaries (`beava-bench-legacy`, `beava-bench-v18`,
//! `beava-bench-v2`) live alongside this one for the deeper harnesses used by
//! `.planning/throughput-baselines.md` runs.

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(
    name = "beava-bench",
    version,
    about = "Beava performance benchmark suite"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Subcommands>,
}

#[derive(Subcommand, Debug)]
enum Subcommands {
    /// Throughput mode (acks=1 best-effort EPS).
    Throughput(beava_bench::cli::throughput::ThroughputArgs),
    /// Mixed read+write ratio mode.
    Mixed(beava_bench::cli::mixed::MixedArgs),
    /// Memory mode (RSS / per-entity overhead).
    Memory(beava_bench::cli::memory::MemoryArgs),
    /// Fsync mode (acks=all per-push fsync wait latency).
    Fsync(beava_bench::cli::fsync::FsyncArgs),
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("beava_bench=info,warn")),
        )
        .init();
    let cli = Cli::parse();
    match cli.command {
        Some(Subcommands::Throughput(args)) => beava_bench::cli::throughput::run_throughput(args),
        Some(Subcommands::Mixed(args)) => beava_bench::cli::mixed::run_mixed(args),
        Some(Subcommands::Memory(args)) => beava_bench::cli::memory::run_memory(args),
        Some(Subcommands::Fsync(args)) => beava_bench::cli::fsync::run_fsync(args),
        None => beava_bench::cli::interactive::run_interactive(),
    }
}
