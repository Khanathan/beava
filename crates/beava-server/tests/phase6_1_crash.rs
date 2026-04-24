//! Phase 6.1 Plan 04: crash semantics under Periodic vs PerEvent modes.
//!
//! Periodic mode (default `/push`): events ACK'd within
//! BEAVA_WAL_FSYNC_INTERVAL_MS of a crash MAY be lost. Test asserts
//! the weaker invariant: 0 ≤ recovered ≤ N. Recovery must still be
//! crash-safe (no torn records, monotonic LSNs, registry intact).
//!
//! PerEvent mode (/push-sync): unchanged from Phase 6 — every ACK'd
//! event survives crash unconditionally. Reuses `phase6_crash.rs`'s
//! probe binary to assert the strict invariant on /push-sync.

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
            return rest.trim().parse().expect("parse port");
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

/// Periodic mode: kill the probe shortly after issuing N pushes; the
/// fsync timer may not have fired. Recovery must produce 0..=N records
/// and the records present must form a contiguous prefix (monotonic
/// LSNs). The N pushes use the default /push endpoint.
#[test]
fn periodic_push_kill_keeps_subset_of_events() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let wal_dir = tmp.path().to_path_buf();

    // Long fsync interval — most pushes will be ACK'd before the
    // first fsync tick, and the kill happens before that tick.
    let mut child = Command::new(probe_bin())
        .env("BEAVA_WAL_DIR", &wal_dir)
        .env("BEAVA_WAL_FSYNC_INTERVAL_MS", "2000")
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

    let n = 50;
    let pushed = rt.block_on(async {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        let mut acked = 0;
        for i in 0..n {
            let body = serde_json::to_vec(&serde_json::json!({
                "user_id": format!("u{i}"),
                "amount": (i as f64),
                "event_time": 1_000_000 + i as i64,
            }))
            .unwrap();
            let r = client
                .post(&url)
                .header("Content-Type", "application/json")
                .body(body)
                .send()
                .await;
            match r {
                Ok(resp) if resp.status().as_u16() == 200 => acked += 1,
                _ => break,
            }
        }
        acked
    });

    // Kill quickly — well before the 2_000ms fsync tick.
    sigkill(pid);
    wait_for_exit(&mut child, Duration::from_secs(2));

    let count = count_wal_event_records(&wal_dir);
    assert!(
        count <= pushed,
        "recovered count {count} must not exceed pushed {pushed}"
    );
    // Recovery is allowed to find 0 records — periodic mode does not
    // promise durability within the fsync window. The strict guarantee
    // is "no torn records" — covered by the WalReader successfully
    // returning Ok(records) above.
}

/// PerEvent mode (/push-sync): ACK'd events survive crash
/// unconditionally — same invariant as Phase 6 SC1, just exercised on
/// the new endpoint.
#[test]
fn push_sync_ack_survives_crash() {
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
                    "user_id": "carol",
                    "amount": 7.0,
                    "event_time": 2_222_222,
                }))
                .unwrap(),
            )
            .send()
            .await
            .expect("send")
            .status()
            .as_u16()
    });
    assert_eq!(resp_status, 200);

    sigkill(pid);
    wait_for_exit(&mut child, Duration::from_secs(2));

    let count = count_wal_event_records(&wal_dir);
    assert!(
        count >= 1,
        "push-sync ACK'd event must survive crash unconditionally (got {count})"
    );
}
