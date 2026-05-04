//! Phase 13.5 Plan 08 — shared output struct + 4 formatters
//! (human / JSON / markdown / append).
//!
//! All 4 mode modules return a [`BenchResult`] which is then formatted via
//! [`dispatch_output`]. Schema is pinned at v0; future schema bumps must be
//! gated by a new `schema_version`.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct BenchResult {
    pub schema_version: u32,
    pub mode: String,
    pub workload: String,
    pub duration_ms: u64,
    pub events_pushed: u64,
    pub events_per_sec: f64,
    pub p50_us: Option<u64>,
    pub p99_us: Option<u64>,
    pub p999_us: Option<u64>,
    pub rss_mb_max: Option<u64>,
    pub bytes_per_entity_p99: Option<u64>,
    pub fsync_p50_us: Option<u64>,
    pub fsync_p99_us: Option<u64>,
    pub fsync_p999_us: Option<u64>,
    pub git_sha: String,
    pub timestamp_utc: String,
}

impl BenchResult {
    /// Build an empty result with sensible defaults filled in (mode/workload
    /// + git_sha + timestamp). Caller fills in measurement fields.
    pub fn new(mode: &str, workload: &str) -> Self {
        Self {
            schema_version: 1,
            mode: mode.to_string(),
            workload: workload.to_string(),
            duration_ms: 0,
            events_pushed: 0,
            events_per_sec: 0.0,
            p50_us: None,
            p99_us: None,
            p999_us: None,
            rss_mb_max: None,
            bytes_per_entity_p99: None,
            fsync_p50_us: None,
            fsync_p99_us: None,
            fsync_p999_us: None,
            git_sha: capture_git_sha(),
            timestamp_utc: capture_timestamp(),
        }
    }
}

#[derive(Clone, Copy, Debug, clap::ValueEnum, Default, PartialEq, Eq)]
pub enum OutputFormat {
    #[default]
    Human,
    Json,
    Markdown,
}

pub fn print_human(r: &BenchResult) {
    println!("=== beava bench {} — {} ===", r.mode, r.workload);
    println!("  duration:        {} ms", r.duration_ms);
    println!("  events pushed:   {}", r.events_pushed);
    println!("  events/sec:      {:.1}", r.events_per_sec);
    if let Some(p50) = r.p50_us {
        println!("  p50 latency:     {} us", p50);
    }
    if let Some(p99) = r.p99_us {
        println!("  p99 latency:     {} us", p99);
    }
    if let Some(p999) = r.p999_us {
        println!("  p999 latency:    {} us", p999);
    }
    if let Some(rss) = r.rss_mb_max {
        println!("  rss max:         {} MB", rss);
    }
    if let Some(bpe) = r.bytes_per_entity_p99 {
        println!("  bytes/entity P99: {}", bpe);
    }
    if r.mode == "fsync" {
        println!("  --- fsync (acks=all) ---");
        if let Some(p) = r.fsync_p50_us {
            println!("  fsync p50:       {} us", p);
        }
        if let Some(p) = r.fsync_p99_us {
            println!("  fsync p99:       {} us", p);
        }
        if let Some(p) = r.fsync_p999_us {
            println!("  fsync p999:      {} us", p);
        }
    }
}

pub fn print_json(r: &BenchResult) {
    println!("{}", serde_json::to_string_pretty(r).unwrap());
}

pub fn print_markdown(r: &BenchResult) {
    println!("| metric | value |");
    println!("|--------|-------|");
    println!("| mode | {} |", r.mode);
    println!("| workload | {} |", r.workload);
    println!("| duration_ms | {} |", r.duration_ms);
    println!("| events_per_sec | {:.1} |", r.events_per_sec);
    if let Some(p99) = r.p99_us {
        println!("| p99_us | {} |", p99);
    }
    if let Some(rss) = r.rss_mb_max {
        println!("| rss_mb_max | {} |", rss);
    }
    if r.mode == "fsync" {
        if let Some(p) = r.fsync_p99_us {
            println!("| fsync_p99_us | {} |", p);
        }
    }
}

pub fn append_ledger(r: &BenchResult, path: &Path) -> anyhow::Result<()> {
    let mut f = OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(f, "{}", serde_json::to_string(r)?)?;
    Ok(())
}

pub fn dispatch_output(
    r: &BenchResult,
    fmt: OutputFormat,
    append: Option<&Path>,
) -> anyhow::Result<()> {
    match fmt {
        OutputFormat::Human => print_human(r),
        OutputFormat::Json => print_json(r),
        OutputFormat::Markdown => print_markdown(r),
    }
    if let Some(p) = append {
        append_ledger(r, p)?;
    }
    Ok(())
}

pub fn effective_format(json: bool, markdown: bool, default: OutputFormat) -> OutputFormat {
    if json {
        OutputFormat::Json
    } else if markdown {
        OutputFormat::Markdown
    } else {
        default
    }
}

fn capture_git_sha() -> String {
    std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".into())
}

fn capture_timestamp() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("epoch:{secs}")
}
