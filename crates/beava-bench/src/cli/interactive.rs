//! Interactive walkthrough — invoked when `beava-bench` is called with no
//! subcommand. Prompts the user for: mode → workload → (size if dataset) →
//! duration | entities → confirm.

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
            // Plan 13.7.6-32 reverses Plan 13.7.6-24's strip and merges v18's
            // production harness into `beava-bench throughput`. The interactive
            // walkthrough drives the production harness with sensible defaults;
            // `--parallel = None` means harness::production picks min(8, ncpus).
            // Workload selection (small/medium/large/fraud/adtech/ecommerce)
            // maps to the corresponding pipeline config under configs/.
            let pipeline = match workload {
                "fraud" => "fraud-team".to_string(),
                "adtech" => "medium-with-sketches".to_string(),
                "ecommerce" => "large-with-sketches".to_string(),
                other => other.to_string(),
            };
            let duration_secs = parse_duration_to_secs(&duration_or_entities).unwrap_or(60);
            let args = crate::cli::throughput::ThroughputArgs {
                pipeline,
                transport: crate::harness::production::Transport::Tcp,
                wire_format: crate::harness::production::WireFormat::Msgpack,
                duration_secs,
                parallel: None,
                seed: 0xCAFE_BABE,
                get_sample_interval_ms: 1000,
                get_batch_keys: 100,
                read_workers: 0,
                no_ledger: true,
                remote_addr: None,
                pipeline_depth: 1024,
                continuous_pipeline: true,
                total_events: None,
                blast_shape: crate::harness::production::BlastShapeArg::Zipfian,
                zipf_alpha: 1.0,
                cardinality: 10_000,
                mixed_event_count: 3,
                isolation_mode: false,
                io_threads: None,
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

/// Parse `30s` / `60s` / `2m` / `5m` / `1h` style duration strings into integer
/// seconds. Returns `None` on a malformed string so the caller can fall back
/// to a default (60 s for the throughput interactive default).
fn parse_duration_to_secs(s: &str) -> Option<u64> {
    let parsed = humantime::parse_duration(s.trim()).ok()?;
    Some(parsed.as_secs())
}
