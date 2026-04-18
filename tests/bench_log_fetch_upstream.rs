//! In-process benchmark for the upstream `handle_log_fetch` hot path.
//!
//! The replica-side ingest path is already at 1.3 M EPS (see
//! `bench_replica_ingest_raw.rs`). The fork-replay end-to-end number shows
//! ~436 K EPS and ~10.5 s catchup for 5 M events. The ~6.7 s gap has to
//! live somewhere upstream or in the TCP parse path. This bench isolates
//! the upstream cost by replaying exactly what `handle_log_fetch` does to
//! a 5 M-entry event-log file, but writing to an in-memory sink instead of
//! a TCP socket. It decomposes the upstream wall-clock into:
//!
//!   (1) `EventLog::read_entries`  — open file, BufReader, postcard-decode
//!        every frame, allocate every payload Vec, push into growing Vec.
//!   (2) filter + encode + write loop — per-entry decode-payload, key
//!        extraction, scope-check, `encode_log_event_frame` allocation,
//!        write to the sink.
//!
//! Together these bound what the upstream half of a fork catchup can do.
//!
//! Run with:
//!   cargo test --release --test bench_log_fetch_upstream \
//!     -- --nocapture --ignored upstream_log_fetch_bench

use std::io::Write as IoWrite;
use std::time::{Duration, Instant};

use serde_json::json;

use beava::server::protocol::{encode_log_event_frame, Scope};
use beava::state::event_log::{decode_log_payload, EventLog, LOG_FMT_JSON};

fn make_log(tmp: &std::path::Path) -> EventLog {
    let event_log = EventLog::new(tmp.to_path_buf()).unwrap();
    event_log.register_stream("events", None).unwrap();
    event_log
}

fn wrap_json(v: &serde_json::Value) -> Vec<u8> {
    let body = serde_json::to_vec(v).unwrap();
    let mut out = Vec::with_capacity(1 + body.len());
    out.push(LOG_FMT_JSON);
    out.extend_from_slice(&body);
    out
}

fn seed_events_to_log(log: &EventLog, n: usize, entities: usize) {
    let base_ts: u64 = 1_700_000_000_000;
    // One giant append_many call per ~5000 events to speed up seeding.
    let chunk = 5000usize;
    let mut buf: Vec<Vec<u8>> = Vec::with_capacity(chunk);
    let mut times: Vec<std::time::SystemTime> = Vec::with_capacity(chunk);
    for i in 0..n {
        let uid = format!("u{}", i % entities);
        let ts_ms = base_ts + (i as u64) * 1_000;
        let event_time = std::time::UNIX_EPOCH + Duration::from_millis(ts_ms);
        let wrapped = wrap_json(&json!({"user_id": uid, "amount": (i % 37) as i64}));
        buf.push(wrapped);
        times.push(event_time);
        if buf.len() == chunk {
            let refs: Vec<&[u8]> = buf.iter().map(|v| v.as_slice()).collect();
            log.append_many_with_ts("events", &refs, &times).unwrap();
            buf.clear();
            times.clear();
        }
    }
    if !buf.is_empty() {
        let refs: Vec<&[u8]> = buf.iter().map(|v| v.as_slice()).collect();
        log.append_many_with_ts("events", &refs, &times).unwrap();
    }
    log.fsync_all().unwrap();
}

fn fmt_eps(n: usize, elapsed: Duration) -> String {
    let eps = n as f64 / elapsed.as_secs_f64();
    let ns_per = elapsed.as_nanos() as f64 / n as f64;
    format!(
        "{:.3} s  ({:.0} EPS, {:.0} ns/event)",
        elapsed.as_secs_f64(),
        eps,
        ns_per
    )
}

#[test]
#[ignore]
fn upstream_log_fetch_bench() {
    let n: usize = std::env::var("BENCH_EVENTS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5_000_000);
    let entities: usize = std::env::var("BENCH_ENTITIES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000);

    eprintln!("upstream log-fetch bench: N={} entities={}", n, entities);

    let tmp = tempfile::tempdir().unwrap();
    let log = make_log(tmp.path());

    eprintln!("seeding {} events to on-disk log...", n);
    let t_seed = Instant::now();
    seed_events_to_log(&log, n, entities);
    let seed_elapsed = t_seed.elapsed();
    eprintln!("seed wall: {}", fmt_eps(n, seed_elapsed));

    // File size sanity check.
    let log_path = tmp.path().join("events.log");
    let log_size = std::fs::metadata(&log_path).map(|m| m.len()).unwrap_or(0);
    eprintln!(
        "log file: {} bytes ({:.1} MiB), {:.0} B/event",
        log_size,
        log_size as f64 / 1_048_576.0,
        log_size as f64 / n as f64
    );

    // ========================================================================
    // Phase (1): read_entries alone
    // ========================================================================
    eprintln!("\n[1] EventLog::read_entries — parse postcard stream, allocate payload Vecs");
    let t_read = Instant::now();
    let entries = log.read_entries("events").unwrap();
    let read_elapsed = t_read.elapsed();
    eprintln!(
        "    entries: {} · {}",
        entries.len(),
        fmt_eps(n, read_elapsed)
    );

    // ========================================================================
    // Phase (2): filter + encode + write loop (sink = Vec<u8>)
    // ========================================================================
    eprintln!("\n[2] filter + encode_log_event_frame + write-to-sink");
    let kf: Option<String> = Some("user_id".into());
    let scope = Scope {
        streams: vec!["events".into()],
        keys: None,
        key_prefix: Some("u".into()),
        pull: "all".into(),
    };
    let stream_arr = ["events"];
    use std::time::UNIX_EPOCH;

    // BufWriter-backed Vec<u8> mimics BufWriter<OwnedWriteHalf> on the wire
    // side without the async + TCP overhead.
    let mut sink: Vec<u8> = Vec::with_capacity((log_size as usize).saturating_mul(2));
    let t_loop = Instant::now();
    let mut emitted: usize = 0;
    let mut dropped: usize = 0;

    for entry in &entries {
        // Timestamp gate (inclusive) — from_ts_millis=0 here, so every
        // entry passes.
        let _ts_ms = match entry.timestamp.duration_since(UNIX_EPOCH) {
            Ok(d) => d.as_millis().min(u64::MAX as u128) as u64,
            Err(_) => 0,
        };
        if let Some(kf_name) = &kf {
            let (fmt, body) = decode_log_payload(&entry.payload);
            use beava::state::event_log::{LOG_FMT_BINARY, LOG_FMT_JSON};
            let event_value: serde_json::Value = match fmt {
                LOG_FMT_BINARY => {
                    let mut buf = body;
                    match beava::server::protocol::decode_event_binary(&mut buf) {
                        Ok(v) => v,
                        Err(_) => {
                            dropped += 1;
                            continue;
                        }
                    }
                }
                LOG_FMT_JSON => match serde_json::from_slice(body) {
                    Ok(v) => v,
                    Err(_) => {
                        dropped += 1;
                        continue;
                    }
                },
                _ => {
                    dropped += 1;
                    continue;
                }
            };
            let key = match event_value.get(kf_name.as_str()) {
                Some(serde_json::Value::String(s)) if !s.is_empty() => s.as_str(),
                _ => {
                    dropped += 1;
                    continue;
                }
            };
            if !beava::server::replica::entity_matches_scope(&stream_arr, key, &scope) {
                dropped += 1;
                continue;
            }
        }

        let ts_ms = match entry.timestamp.duration_since(UNIX_EPOCH) {
            Ok(d) => d.as_millis().min(u64::MAX as u128) as u64,
            Err(_) => 0,
        };
        let frame = encode_log_event_frame(ts_ms, &entry.payload);
        sink.write_all(&frame).unwrap();
        emitted += 1;
    }
    let loop_elapsed = t_loop.elapsed();
    eprintln!(
        "    emitted: {} (dropped {}), sink: {:.1} MiB · {}",
        emitted,
        dropped,
        sink.len() as f64 / 1_048_576.0,
        fmt_eps(emitted, loop_elapsed),
    );

    // ========================================================================
    // Summary
    // ========================================================================
    let total = read_elapsed + loop_elapsed;
    eprintln!("\n=== upstream log-fetch summary ===");
    eprintln!("read_entries    : {}", fmt_eps(n, read_elapsed));
    eprintln!("filter+encode   : {}", fmt_eps(emitted, loop_elapsed));
    eprintln!("combined upstream work: {}", fmt_eps(emitted, total));
    eprintln!(
        "upstream/replica-ingest ratio: at current replica ingest 1.32M EPS, a fork-replay\n\
         catchup of {} events is bounded below by max(upstream={:.3}s, replica={:.3}s).",
        emitted,
        total.as_secs_f64(),
        emitted as f64 / 1_320_000.0,
    );
}
