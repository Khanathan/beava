//! Beava bench CLI — Phase 13.5 Plan 08.
//!
//! Polished `beava bench <mode>` subcommand surface. The legacy in-process
//! throughput harness lives at `src/bin/beava-bench-legacy.rs` (one milestone
//! deprecation window). The Phase 19 alternates `beava-bench-v18` and
//! `beava-bench-v2` are also retained.

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "beava-bench", version, about = "Beava performance benchmark suite")]
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
    /// Fsync mode (acks=all per-push fsync wait latency) — D-03 2026-05-03 amendment.
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
