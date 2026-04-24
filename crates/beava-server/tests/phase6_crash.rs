//! Phase 6 Plan 04: subprocess-based crash UAT.
//!
//! `wal_kill_before_fsync_drops_event`: spawn the phase6_crash_probe with a
//! fsync interval so large that no coalesce ever fires; issue a /push with a
//! short client-side timeout; SIGKILL on timeout; reopen WAL → zero Event
//! records.
//!
//! `wal_kill_after_ack_preserves_event`: same probe binary, default fsync
//! interval; push completes with 200; SIGKILL child after ACK; reopen WAL → at
//! least one Event record.

#![cfg(feature = "testing")]

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn probe_bin() -> &'static str {
    env!("CARGO_BIN_EXE_phase6_crash_probe")
}

fn read_port_from_stdout(child: &mut std::process::Child) -> u16 {
    let stdout = child.stdout.take().expect("probe stdout");
    let reader = BufReader::new(stdout);
    let start = Instant::now();
    for line in reader.lines() {
        let line = line.expect("read line");
        if let Some(rest) = line.strip_prefix("PORT=") {
            let port: u16 = rest.trim().parse().expect("parse port");
            return port;
        }
        if start.elapsed() > Duration::from_secs(5) {
            panic!("probe did not print PORT= within 5s");
        }
    }
    panic!("probe stdout closed before PORT=");
}

fn sigkill(pid: u32) {
    unsafe {
        libc::kill(pid as i32, libc::SIGKILL);
    }
}

fn wait_for_exit(child: &mut std::process::Child, timeout: Duration) {
    let start = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) => {
                if start.elapsed() > timeout {
                    let _ = child.kill();
                    panic!("child did not exit within {timeout:?}");
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(e) => panic!("try_wait: {e}"),
        }
    }
}

fn count_wal_event_records(wal_dir: &std::path::Path) -> usize {
    match beava_persistence::WalReader::read_all(wal_dir) {
        Ok(records) => records
            .iter()
            .filter(|r| matches!(r.record_type, beava_persistence::RecordType::Event))
            .count(),
        Err(_) => 0,
    }
}

#[test]
fn wal_kill_before_fsync_drops_event() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let wal_dir = tmp.path().to_path_buf();

    let mut child = Command::new(probe_bin())
        .env("BEAVA_WAL_DIR", &wal_dir)
        // Make fsync never fire within the test window.
        .env("BEAVA_WAL_FSYNC_INTERVAL_MS", "999999999")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn probe");

    let port = read_port_from_stdout(&mut child);
    let pid = child.id();
    let url = format!("http://127.0.0.1:{port}/push/Test");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    // Fire the push with a short client-side timeout; the handler will never
    // resolve because the fsync worker never flushes.
    let _ = rt.block_on(async {
        reqwest::Client::builder()
            .timeout(Duration::from_millis(200))
            .build()
            .unwrap()
            .post(&url)
            .header("Content-Type", "application/json")
            .body(
                serde_json::to_vec(&serde_json::json!({
                    "user_id": "alice",
                    "amount": 1.0,
                    "event_time": 1_000_000,
                }))
                .unwrap(),
            )
            .send()
            .await
    });

    // Kill before fsync.
    sigkill(pid);
    wait_for_exit(&mut child, Duration::from_secs(2));

    let count = count_wal_event_records(&wal_dir);
    assert_eq!(
        count, 0,
        "kill-before-fsync must leave zero Event records on disk"
    );
}

#[test]
fn wal_kill_after_ack_preserves_event() {
    // Phase 6.1 update: the default `/push` is now Periodic-mode
    // (Kafka acks=1) — ACK does NOT imply fsync. The strict-mode
    // contract that Phase 6 SC1 captures (ACK ⇒ durable) now lives
    // on `/push-sync`. The probe binary serves both endpoints; we
    // route this test at the strict one.
    let tmp = tempfile::tempdir().expect("tempdir");
    let wal_dir = tmp.path().to_path_buf();

    let mut child = Command::new(probe_bin())
        .env("BEAVA_WAL_DIR", &wal_dir)
        .env("BEAVA_WAL_FSYNC_INTERVAL_MS", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn probe");

    let port = read_port_from_stdout(&mut child);
    let pid = child.id();
    let url = format!("http://127.0.0.1:{port}/push-sync/Test");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let resp_status = rt.block_on(async {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap()
            .post(&url)
            .header("Content-Type", "application/json")
            .body(
                serde_json::to_vec(&serde_json::json!({
                    "user_id": "bob",
                    "amount": 9.0,
                    "event_time": 2_000_000,
                }))
                .unwrap(),
            )
            .send()
            .await
            .expect("send")
            .status()
            .as_u16()
    });
    assert_eq!(resp_status, 200, "push must 200 before we kill");

    // Now SIGKILL — the data is already fsynced.
    sigkill(pid);
    wait_for_exit(&mut child, Duration::from_secs(2));

    let count = count_wal_event_records(&wal_dir);
    assert!(
        count >= 1,
        "kill-after-ACK must preserve the event record (got {count})"
    );
}
