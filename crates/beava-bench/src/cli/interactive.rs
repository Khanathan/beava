//! Interactive walkthrough — Phase 13.5 Plan 10.
//!
//! Invoked when `beava-bench` is called with no subcommand. Prompts the user
//! for: mode → workload → (size if dataset) → duration|entities → confirm.

use anyhow::Result;
use inquire::{Confirm, Select, Text};

use crate::cli::output::OutputFormat;

pub fn run_interactive() -> Result<()> {
    println!("Beava bench — interactive walkthrough");
    println!("(use `--yes` on a subcommand to skip these prompts)\n");

    let mode = Select::new(
        "Which mode?",
        vec!["throughput", "mixed", "memory", "fsync"],
    )
    .with_help_message(
        "throughput=acks=1 EPS, mixed=read+write, memory=RSS-only, fsync=acks=all per-push latency",
    )
    .prompt()?;

    let workload = Select::new(
        "Which workload?",
        vec!["adtech", "fraud", "ecommerce", "small", "medium", "large"],
    )
    .with_help_message(
        "adtech/fraud/ecommerce are realistic shapes; small/medium/large are synthetic",
    )
    .prompt()?;

    let size: Option<String> = if !["small", "medium", "large"].contains(&workload) {
        Some(
            Select::new("What size?", vec!["small", "medium", "large"])
                .prompt()?
                .to_string(),
        )
    } else {
        None
    };

    let duration_or_entities = if mode == "memory" {
        Text::new("How many entities to load?")
            .with_default("100000")
            .prompt()?
    } else {
        Text::new("Duration (e.g., 30s, 60s, 2m)?")
            .with_default("60s")
            .prompt()?
    };

    if let Some(s) = &size {
        if let Ok(est) = crate::cli::estimator::estimate_memory(workload, s) {
            crate::cli::estimator::print_estimate_to_stderr(&est);
        }
    }

    let confirm = Confirm::new("Run now?").with_default(true).prompt()?;
    if !confirm {
        println!("Aborted.");
        return Ok(());
    }

    match mode {
        "throughput" => {
            let args = crate::cli::throughput::ThroughputArgs {
                workload: workload.into(),
                size,
                duration: duration_or_entities,
                parallel: 16,
                format: OutputFormat::Human,
                json: false,
                markdown: false,
                append: None,
                yes: true,
            };
            crate::cli::throughput::run_throughput(args)
        }
        "fsync" => {
            let args = crate::cli::fsync::FsyncArgs {
                workload: workload.into(),
                size,
                duration: duration_or_entities,
                parallel: 16,
                format: OutputFormat::Human,
                json: false,
                markdown: false,
                append: None,
                yes: true,
            };
            crate::cli::fsync::run_fsync(args)
        }
        "mixed" => {
            let args = crate::cli::mixed::MixedArgs {
                workload: workload.into(),
                size,
                duration: duration_or_entities,
                read_write_ratio: "70/30".into(),
                parallel: 16,
                format: OutputFormat::Human,
                json: false,
                markdown: false,
                append: None,
                yes: true,
            };
            crate::cli::mixed::run_mixed(args)
        }
        "memory" => {
            let entities: u64 = duration_or_entities.trim().parse().unwrap_or(100_000);
            let args = crate::cli::memory::MemoryArgs {
                workload: workload.into(),
                size,
                entities,
                format: OutputFormat::Human,
                json: false,
                markdown: false,
                append: None,
                yes: true,
            };
            crate::cli::memory::run_memory(args)
        }
        _ => unreachable!(),
    }
}
