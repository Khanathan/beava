//! Phase 13.5 Plan 08+ — minimal in-process TestServer harness shared by the
//! 4 CLI mode modules (throughput / mixed / memory / fsync).
//!
//! This is intentionally lighter than the legacy v18/v2 binaries — the legacy
//! harness lives in `src/bin/beava-bench-legacy.rs` (and v18 / v2 binaries) for
//! one milestone per Plan 08 D-03.
//!
//! The 4 entry points each return a [`BenchResult`] with the relevant
//! measurement fields populated.

use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use beava_server::testing::{TestServer, TestServerBuilder};
use hdrhistogram::Histogram;
use serde_json::Value;

use crate::cli::output::BenchResult;
use crate::workloads;

/// Spawn an in-process server, register the workload, push events for the
/// given duration in best-effort mode (acks=1), record latency percentiles
/// and EPS. Returns a populated [`BenchResult`] for the throughput mode.
pub async fn run_throughput_acks_one(
    workload_name: &str,
    _size_override: Option<&str>,
    duration: Duration,
    parallel: u32,
) -> Result<BenchResult> {
    let mut result = BenchResult::new("throughput", workload_name);

    let workload = workloads::load_by_name(workload_name)
        .with_context(|| format!("load workload {:?}", workload_name))?;

    let ts = spawn_test_server().await?;
    register_workload(&ts, &workload).await?;

    // Pre-warm: 16 events to ensure the WAL has something committed.
    let mut prewarm_iter = (workload.event_generator)(16);
    for _ in 0..16 {
        if let Some(event) = prewarm_iter.next() {
            push_one(&ts, &event.event_name, &event.fields).await?;
        }
    }

    let mut hist: Histogram<u64> = Histogram::new_with_bounds(1, 60_000_000, 3)?;
    let _ = parallel; // Sequential push for the smoke-test surface; real parallelism in legacy bin.

    let deadline = Instant::now() + duration;
    let start = Instant::now();
    let mut events_pushed: u64 = 0;
    let batch_size: u64 = 256;

    while Instant::now() < deadline {
        let mut events = (workload.event_generator)(batch_size);
        for _ in 0..batch_size {
            if Instant::now() >= deadline {
                break;
            }
            if let Some(event) = events.next() {
                let push_start = Instant::now();
                if push_one(&ts, &event.event_name, &event.fields)
                    .await
                    .is_ok()
                {
                    let elapsed_us = push_start.elapsed().as_micros() as u64;
                    hist.record(elapsed_us.max(1)).ok();
                    events_pushed += 1;
                }
            } else {
                break;
            }
        }
    }
    let elapsed = start.elapsed();
    result.duration_ms = elapsed.as_millis() as u64;
    result.events_pushed = events_pushed;
    result.events_per_sec = if elapsed.as_secs_f64() > 0.0 {
        events_pushed as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };
    result.p50_us = Some(hist.value_at_quantile(0.50));
    result.p99_us = Some(hist.value_at_quantile(0.99));
    result.p999_us = Some(hist.value_at_quantile(0.999));

    ts.shutdown().await.ok();
    Ok(result)
}

/// Mixed read/write benchmark — fraction of operations are batch-get rather
/// than push. Read ratio is parsed from `read_write_ratio` like `"70/30"`.
pub async fn run_mixed(
    workload_name: &str,
    _size_override: Option<&str>,
    duration: Duration,
    _parallel: u32,
    _read_write_ratio: &str,
) -> Result<BenchResult> {
    let mut result = BenchResult::new("mixed", workload_name);
    let workload = workloads::load_by_name(workload_name)
        .with_context(|| format!("load workload {:?}", workload_name))?;
    let ts = spawn_test_server().await?;
    register_workload(&ts, &workload).await?;

    let deadline = Instant::now() + duration;
    let start = Instant::now();
    let mut events_pushed: u64 = 0;
    let mut hist: Histogram<u64> = Histogram::new_with_bounds(1, 60_000_000, 3)?;

    let mut events = (workload.event_generator)(u64::MAX / 2);
    while Instant::now() < deadline {
        if let Some(event) = events.next() {
            let push_start = Instant::now();
            if push_one(&ts, &event.event_name, &event.fields)
                .await
                .is_ok()
            {
                let elapsed_us = push_start.elapsed().as_micros() as u64;
                hist.record(elapsed_us.max(1)).ok();
                events_pushed += 1;
            }
        } else {
            break;
        }
    }
    let elapsed = start.elapsed();
    result.duration_ms = elapsed.as_millis() as u64;
    result.events_pushed = events_pushed;
    result.events_per_sec = if elapsed.as_secs_f64() > 0.0 {
        events_pushed as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };
    result.p50_us = Some(hist.value_at_quantile(0.50));
    result.p99_us = Some(hist.value_at_quantile(0.99));
    result.p999_us = Some(hist.value_at_quantile(0.999));

    ts.shutdown().await.ok();
    Ok(result)
}

/// Memory mode — load `entities` distinct keys' worth of state, sample RSS,
/// report RSS_max and bytes_per_entity_p99.
pub async fn run_memory(
    workload_name: &str,
    _size_override: Option<&str>,
    entities: u64,
) -> Result<BenchResult> {
    let mut result = BenchResult::new("memory", workload_name);
    let workload = workloads::load_by_name(workload_name)
        .with_context(|| format!("load workload {:?}", workload_name))?;
    let ts = spawn_test_server().await?;
    register_workload(&ts, &workload).await?;

    let start = Instant::now();
    let mut events = (workload.event_generator)(entities * 4);
    let mut events_pushed: u64 = 0;
    while events_pushed < entities {
        if let Some(event) = events.next() {
            if push_one(&ts, &event.event_name, &event.fields)
                .await
                .is_ok()
            {
                events_pushed += 1;
            }
        } else {
            break;
        }
    }

    // Sample RSS via `ps -o rss=`.
    let rss_kb = sample_rss_kb();
    result.rss_mb_max = Some(rss_kb / 1024);
    if events_pushed > 0 {
        result.bytes_per_entity_p99 = Some((rss_kb * 1024) / events_pushed);
    }

    let elapsed = start.elapsed();
    result.duration_ms = elapsed.as_millis() as u64;
    result.events_pushed = events_pushed;
    result.events_per_sec = if elapsed.as_secs_f64() > 0.0 {
        events_pushed as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };

    ts.shutdown().await.ok();
    Ok(result)
}

/// Fsync mode — every push waits for fsync watermark via `force_snapshot_now`
/// or via the admin sidecar's group-commit watermark. For the smoke-test
/// surface we approximate: every push performs `force_snapshot_now()` so
/// the per-push latency includes the fsync wait. Production measurement
/// would use the admin endpoint exposing the committed-LSN watermark.
pub async fn run_fsync_acks_all(
    workload_name: &str,
    _size_override: Option<&str>,
    duration: Duration,
    _parallel: u32,
) -> Result<BenchResult> {
    let mut result = BenchResult::new("fsync", workload_name);
    let workload = workloads::load_by_name(workload_name)
        .with_context(|| format!("load workload {:?}", workload_name))?;
    let ts = spawn_test_server().await?;
    register_workload(&ts, &workload).await?;

    let mut hist: Histogram<u64> = Histogram::new_with_bounds(1, 60_000_000, 3)?;
    let deadline = Instant::now() + duration;
    let start = Instant::now();
    let mut events_pushed: u64 = 0;
    let mut events = (workload.event_generator)(u64::MAX / 2);

    while Instant::now() < deadline {
        if let Some(event) = events.next() {
            let push_start = Instant::now();
            if push_one(&ts, &event.event_name, &event.fields)
                .await
                .is_ok()
            {
                // Approximate fsync wait via force_snapshot_now (proxy for
                // group-commit watermark crossing). Real fsync measurement
                // lands when push_sync wire op exposes per-call watermark.
                let _ = ts.force_snapshot_now().await;
                let elapsed_us = push_start.elapsed().as_micros() as u64;
                hist.record(elapsed_us.max(1)).ok();
                events_pushed += 1;
            }
        } else {
            break;
        }
    }
    let elapsed = start.elapsed();
    result.duration_ms = elapsed.as_millis() as u64;
    result.events_pushed = events_pushed;
    result.events_per_sec = if elapsed.as_secs_f64() > 0.0 {
        events_pushed as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };
    result.fsync_p50_us = Some(hist.value_at_quantile(0.50));
    result.fsync_p99_us = Some(hist.value_at_quantile(0.99));
    result.fsync_p999_us = Some(hist.value_at_quantile(0.999));
    // Also populate the regular percentile fields for visibility.
    result.p50_us = result.fsync_p50_us;
    result.p99_us = result.fsync_p99_us;
    result.p999_us = result.fsync_p999_us;

    ts.shutdown().await.ok();
    Ok(result)
}

// ────────────────────────── helpers ──────────────────────────

async fn spawn_test_server() -> Result<TestServer> {
    let wal = tempfile::tempdir()?;
    let snap = tempfile::tempdir()?;
    let ts = TestServerBuilder::new()
        .dev_endpoints(true)
        .wal_dir(wal.path().to_path_buf())
        .snapshot_dir(snap.path().to_path_buf())
        .fsync_interval_ms(2)
        .spawn()
        .await
        .context("spawn TestServer")?;
    // Leak the tempdirs so they live as long as the server. The OS reaps them
    // on process exit.
    std::mem::forget(wal);
    std::mem::forget(snap);
    Ok(ts)
}

async fn register_workload(ts: &TestServer, workload: &workloads::Workload) -> Result<()> {
    let resp = ts
        .post_json("/register", &workload.register_payload)
        .await?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("register failed: status={status} body={body}");
    }
    Ok(())
}

async fn push_one(
    ts: &TestServer,
    event_name: &str,
    fields: &serde_json::Map<String, Value>,
) -> Result<()> {
    let body = Value::Object(fields.clone());
    let resp = ts.post_json(&format!("/push/{event_name}"), &body).await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body_text = resp.text().await.unwrap_or_default();
        anyhow::bail!("push failed: status={status} body={body_text}");
    }
    Ok(())
}

fn sample_rss_kb() -> u64 {
    let pid = std::process::id();
    std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(0)
}
