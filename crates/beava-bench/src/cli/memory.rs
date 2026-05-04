//! Phase 13.5 Plan 08 — memory mode (RSS / per-entity overhead reporting).

use anyhow::Result;
use clap::Args;

use crate::cli::output::{dispatch_output, effective_format, BenchResult, OutputFormat};
use crate::harness;

#[derive(Debug, Args, Clone)]
pub struct MemoryArgs {
    #[arg(long)]
    pub workload: String,
    #[arg(long)]
    pub size: Option<String>,
    /// Number of entities to load.
    #[arg(long, default_value = "100000")]
    pub entities: u64,
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

pub fn run_memory(args: MemoryArgs) -> Result<()> {
    print_estimate(&args.workload, args.size.as_deref());
    let result: BenchResult = run_async(harness::run_memory(
        &args.workload,
        args.size.as_deref(),
        args.entities,
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
