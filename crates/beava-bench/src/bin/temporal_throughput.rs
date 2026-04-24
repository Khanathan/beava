//! Phase 11.5 Plan 01 Task 7 — temporal-table throughput micro-harness.
//!
//! Drives an in-process `TestServer` over HTTP with a temporal-table workload:
//! - 70% `POST /push-table/merch` (upsert)
//! - 25% `GET /table/merch?key=...` (point read)
//!  -  5% `POST /retract` (undo a recent upsert)
//!
//! Emits a markdown row to stdout describing EPS / latency per op. This is
//! the *first* table-write throughput baseline — there is no prior number to
//! compare against. Phase 12 (joins consuming `as_of=`) will compare.
//!
//! macOS hw-class fsync ceiling (~7.4 ms F_FULLSYNC) bounds upsert EPS,
//! mirroring the Phase 7.5 push baseline (~990 EPS).
//!
//! Run:
//! ```text
//! cargo run -p beava-bench --release --bin temporal_throughput -- --duration-secs 10 --parallel 8
//! ```

use anyhow::{Context, Result};
use beava_server::testing::TestServerBuilder;
use clap::Parser;
use hdrhistogram::Histogram;
use serde_json::json;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex as AsyncMutex;

const KEY_SPACE: u64 = 1_000;

#[derive(Parser, Debug)]
struct Cli {
    #[arg(long, default_value_t = 10)]
    duration_secs: u64,
    #[arg(long, default_value_t = 8)]
    parallel: usize,
    #[arg(long, default_value_t = 0xC0FF_EE_u64)]
    seed: u64,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    eprintln!(
        "temporal_throughput: duration_secs={} parallel={} seed={}",
        cli.duration_secs, cli.parallel, cli.seed
    );

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

    // Register temporal table.
    let client = reqwest::Client::new();
    let reg = json!({
        "nodes": [{
            "kind": "table",
            "name": "merch",
            "primary_key": ["k"],
            "schema": {"fields": {"k": "str", "v": "i64"}, "optional_fields": []},
            "mode": "upsert",
            "temporal": true,
            "retention_ms": 3_600_000_u64
        }]
    });
    let r = client
        .post(format!("{}/register", ts.base_url()))
        .json(&reg)
        .send()
        .await?;
    anyhow::ensure!(
        r.status().is_success(),
        "register failed: {}",
        r.text().await.unwrap_or_default()
    );

    // Pre-warm so retract has targets to hit.
    let recent_lsns: Arc<AsyncMutex<Vec<u64>>> =
        Arc::new(AsyncMutex::new(Vec::with_capacity(1024)));
    for i in 0..50_u64 {
        let body = json!({"k": format!("m{}", i % KEY_SPACE), "v": i as i64});
        let resp: serde_json::Value = client
            .post(format!("{}/push-table/merch", ts.base_url()))
            .json(&body)
            .send()
            .await?
            .json()
            .await?;
        if let Some(lsn) = resp.get("ack_lsn").and_then(|v| v.as_u64()) {
            recent_lsns.lock().await.push(lsn);
        }
    }

    let stop = Arc::new(AtomicBool::new(false));
    let upsert_count = Arc::new(AtomicU64::new(0));
    let read_count = Arc::new(AtomicU64::new(0));
    let retract_count = Arc::new(AtomicU64::new(0));
    let upsert_hist = Arc::new(AsyncMutex::new(Histogram::<u64>::new(3).unwrap()));
    let read_hist = Arc::new(AsyncMutex::new(Histogram::<u64>::new(3).unwrap()));
    let retract_hist = Arc::new(AsyncMutex::new(Histogram::<u64>::new(3).unwrap()));

    let base_url = ts.base_url().to_string();
    let start = Instant::now();
    let mut handles = Vec::with_capacity(cli.parallel);
    for worker_id in 0..cli.parallel {
        let stop = stop.clone();
        let base = base_url.clone();
        let upsert_count = upsert_count.clone();
        let read_count = read_count.clone();
        let retract_count = retract_count.clone();
        let upsert_hist = upsert_hist.clone();
        let read_hist = read_hist.clone();
        let retract_hist = retract_hist.clone();
        let recent_lsns = recent_lsns.clone();
        let seed = cli.seed.wrapping_add(worker_id as u64);
        handles.push(tokio::spawn(async move {
            let client = reqwest::Client::new();
            let mut counter: u64 = 0;
            while !stop.load(Ordering::Relaxed) {
                counter = counter.wrapping_add(1);
                let mix = (seed.wrapping_add(counter).wrapping_mul(2862933555777941757)) % 100;
                let key_idx = counter % KEY_SPACE;
                if mix < 70 {
                    // upsert
                    let body = json!({"k": format!("m{}", key_idx), "v": counter as i64});
                    let t = Instant::now();
                    let r = client
                        .post(format!("{}/push-table/merch", base))
                        .json(&body)
                        .send()
                        .await;
                    let elapsed_us = t.elapsed().as_micros() as u64;
                    if let Ok(resp) = r {
                        if resp.status().is_success() {
                            upsert_count.fetch_add(1, Ordering::Relaxed);
                            upsert_hist.lock().await.record(elapsed_us).ok();
                            if let Ok(v) = resp.json::<serde_json::Value>().await {
                                if let Some(lsn) = v.get("ack_lsn").and_then(|v| v.as_u64()) {
                                    let mut g = recent_lsns.lock().await;
                                    if g.len() >= 1024 {
                                        g.remove(0);
                                    }
                                    g.push(lsn);
                                }
                            }
                        }
                    }
                } else if mix < 95 {
                    // read
                    let t = Instant::now();
                    let r = client
                        .get(format!("{}/table/merch?key=m{}", base, key_idx))
                        .send()
                        .await;
                    let elapsed_us = t.elapsed().as_micros() as u64;
                    if let Ok(resp) = r {
                        if resp.status().is_success() || resp.status() == 404 {
                            read_count.fetch_add(1, Ordering::Relaxed);
                            read_hist.lock().await.record(elapsed_us).ok();
                        }
                    }
                } else {
                    // retract a recent lsn
                    let target = {
                        let g = recent_lsns.lock().await;
                        g.last().copied()
                    };
                    if let Some(lsn) = target {
                        let t = Instant::now();
                        let r = client
                            .post(format!("{}/retract", base))
                            .json(&json!({"event_id": lsn}))
                            .send()
                            .await;
                        let elapsed_us = t.elapsed().as_micros() as u64;
                        if let Ok(resp) = r {
                            // Accept 200 or 409 (already-retracted under contention).
                            if resp.status().is_success() || resp.status() == 409 {
                                retract_count.fetch_add(1, Ordering::Relaxed);
                                retract_hist.lock().await.record(elapsed_us).ok();
                            }
                        }
                    }
                }
            }
        }));
    }

    tokio::time::sleep(Duration::from_secs(cli.duration_secs)).await;
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.await;
    }
    let elapsed = start.elapsed();

    let upserts = upsert_count.load(Ordering::Relaxed);
    let reads = read_count.load(Ordering::Relaxed);
    let retracts = retract_count.load(Ordering::Relaxed);
    let secs = elapsed.as_secs_f64();

    let upsert_h = upsert_hist.lock().await;
    let read_h = read_hist.lock().await;
    let retract_h = retract_hist.lock().await;

    println!(
        "| temporal-fraud | http | upsert  | {:.0} | {:.2} | {:.2} | first table-write baseline |",
        upserts as f64 / secs,
        upsert_h.value_at_quantile(0.99) as f64 / 1000.0,
        upsert_h.value_at_quantile(0.50) as f64 / 1000.0,
    );
    println!(
        "| temporal-fraud | http | read    | {:.0} | {:.2} | {:.2} | first temporal-read baseline |",
        reads as f64 / secs,
        read_h.value_at_quantile(0.99) as f64 / 1000.0,
        read_h.value_at_quantile(0.50) as f64 / 1000.0,
    );
    println!(
        "| temporal-fraud | http | retract | {:.0} | {:.2} | {:.2} | first retract baseline |",
        retracts as f64 / secs,
        retract_h.value_at_quantile(0.99) as f64 / 1000.0,
        retract_h.value_at_quantile(0.50) as f64 / 1000.0,
    );
    eprintln!(
        "elapsed={:.2}s upserts={} reads={} retracts={}",
        secs, upserts, reads, retracts
    );

    ts.shutdown().await.ok();
    Ok(())
}
