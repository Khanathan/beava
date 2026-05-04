//! Phase 13.5 Plan 08 — fsync mode (acks=all per-push fsync wait latency).
//!
//! Per D-03 2026-05-03 amendment: production fraud users running `push_sync`
//! need to validate per-push fsync latency stays under the CLAUDE.md 10ms P99
//! budget. Phase 6.1 only had a microbench; this gives real-workload
//! validation. The harness wraps `force_snapshot_now()` after each push as a
//! proxy for the group-commit watermark crossing.

use anyhow::Result;
use clap::Args;

use crate::cli::output::{dispatch_output, effective_format, BenchResult, OutputFormat};
use crate::harness;

#[derive(Debug, Args, Clone)]
pub struct FsyncArgs {
    #[arg(long)]
    pub workload: String,
    #[arg(long)]
    pub size: Option<String>,
    #[arg(long, default_value = "60s")]
    pub duration: String,
    #[arg(long, default_value = "16")]
    pub parallel: u32,
    #[arg(long, value_enum, default_value_t = OutputFormat::Human)]
    pub format: OutputFormat,
    #[arg(long, conflicts_with_all = ["format", "markdown"])]
    pub json: bool,
    #[arg(long, conflicts_with_all = ["format", "json"])]
    pub markdown: bool,
    #[arg(long)]
    pub append: Option<std::path::PathBuf>,
    #[arg(long, default_value_t = false)]
    pub yes: bool,
}

pub fn run_fsync(args: FsyncArgs) -> Result<()> {
    let duration = humantime::parse_duration(&args.duration).map_err(|e| {
        anyhow::anyhow!("invalid --duration {:?}: {}", args.duration, e)
    })?;
    print_estimate(&args.workload, args.size.as_deref());
    let result: BenchResult = run_async(harness::run_fsync_acks_all(
        &args.workload,
        args.size.as_deref(),
        duration,
        args.parallel,
    ))?;
    let fmt = effective_format(args.json, args.markdown, args.format);
    dispatch_output(&result, fmt, args.append.as_deref())?;
    Ok(())
}

fn print_estimate(workload: &str, size: Option<&str>) {
    if let Some(s) = size {
        if let Ok(est) = crate::cli::estimator::estimate_memory(workload, s) {
            crate::cli::estimator::print_estimate_to_stderr(&est);
        }
    }
}

fn run_async<F: std::future::Future<Output = Result<BenchResult>>>(fut: F) -> Result<BenchResult> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(fut)
}
