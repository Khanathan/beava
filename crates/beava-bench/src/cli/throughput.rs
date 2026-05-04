//! Phase 13.5 Plan 08 — throughput mode (acks=1 best-effort EPS).

use anyhow::Result;
use clap::Args;

use crate::cli::output::{dispatch_output, effective_format, BenchResult, OutputFormat};
use crate::harness;

#[derive(Debug, Args, Clone)]
pub struct ThroughputArgs {
    /// Workload preset: small | medium | large | fraud | adtech | ecommerce.
    #[arg(long)]
    pub workload: String,
    /// Optional pipeline size override when --workload is a dataset.
    #[arg(long)]
    pub size: Option<String>,
    /// Duration in humantime form: 30s | 60s | 2m | 5m | 1h.
    #[arg(long, default_value = "60s")]
    pub duration: String,
    /// Number of concurrent push workers (default 16).
    #[arg(long, default_value = "16")]
    pub parallel: u32,
    /// Output format.
    #[arg(long, value_enum, default_value_t = OutputFormat::Human)]
    pub format: OutputFormat,
    /// JSON shorthand for --format=json.
    #[arg(long, conflicts_with_all = ["format", "markdown"])]
    pub json: bool,
    /// Markdown shorthand.
    #[arg(long, conflicts_with_all = ["format", "json"])]
    pub markdown: bool,
    /// Append-mode ledger path (JSONL).
    #[arg(long)]
    pub append: Option<std::path::PathBuf>,
    /// Skip interactive prompts (Plan 10 walkthrough); also used by smoke tests.
    #[arg(long, default_value_t = false)]
    pub yes: bool,
}

pub fn run_throughput(args: ThroughputArgs) -> Result<()> {
    let duration = humantime::parse_duration(&args.duration).map_err(|e| {
        anyhow::anyhow!("invalid --duration {:?}: {}", args.duration, e)
    })?;

    print_estimate(&args.workload, args.size.as_deref());

    let result: BenchResult = run_async(harness::run_throughput_acks_one(
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
