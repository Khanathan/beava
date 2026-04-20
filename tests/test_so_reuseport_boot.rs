//! Phase 50.5-02 Task 1 (RED) — Linux-only test asserting that
//! `bind_reuseport_tcp` is invoked from the boot path when BEAVA_SHARDS>1.
//!
//! Validation row: 50.5-02-01
//! Requirement: TPC-PERF-02 — Linux per-shard SO_REUSEPORT accept loops.
//!
//! Today (before Task 2): run_tcp_server_with_listener uses a single
//! TcpListener::bind, so only 1 listener socket exists on the port. This test
//! MUST FAIL until Task 2 wires bind_reuseport_tcp into the boot path.
//!
//! After Task 2: 4 listener sockets exist on the same port when BEAVA_SHARDS=4.
//!
//! Gate: #[cfg(target_os = "linux")] — not compiled on macOS.

// Only compiled + run on Linux.
#![cfg(target_os = "linux")]

use std::sync::Arc;
use std::time::Duration;

use beava::engine::pipeline::PipelineEngine;
use beava::server::tcp::{make_concurrent_state_default_store, BackfillTracker};
const TEST_ADMIN: &str = "test-admin-50-5-02-reuseport";

// ---------------------------------------------------------------------------
// Helper: count LISTEN sockets on a given port via /proc/net/tcp
// ---------------------------------------------------------------------------

/// Parse /proc/net/tcp and count LISTEN (state=0A) entries for the given port.
///
/// /proc/net/tcp format (hex fields, space-separated):
///   sl  local_address rem_address st tx_queue:rx_queue tr:when retrnsmt uid timeout inode
///   local_address = "XXXXXXXX:PPPP" (little-endian IP : port in hex)
///
/// Falls back to 0 if /proc/net/tcp is not accessible.
fn count_listen_sockets_on_port(port: u16) -> usize {
    let port_hex = format!("{:04X}", port);
    let content = match std::fs::read_to_string("/proc/net/tcp") {
        Ok(c) => c,
        Err(_) => {
            eprintln!("[test_so_reuseport_boot] /proc/net/tcp not readable — falling back to 0");
            return 0;
        }
    };
    let mut count = 0usize;
    for line in content.lines().skip(1) {
        let cols: Vec<&str> = line.split_whitespace().collect();
        if cols.len() < 4 {
            continue;
        }
        // cols[1] = local_address "XXXXXXXX:PPPP"
        // cols[3] = state (0A = TCP_LISTEN)
        let local = cols[1];
        let state = cols[3];
        if state == "0A" {
            // Extract port from local_address (after the colon)
            if let Some(p) = local.split(':').nth(1) {
                if p == port_hex {
                    count += 1;
                }
            }
        }
    }
    count
}

// ---------------------------------------------------------------------------
// Test: bind_reuseport_invoked_by_boot_path
// ---------------------------------------------------------------------------

/// Start a server with 4 shards and assert that 4 listener sockets are bound
/// to the same TCP port (SO_REUSEPORT distributes across N listeners).
///
/// MUST FAIL before Task 2: only 1 socket exists (plain TcpListener::bind).
/// PASSES after Task 2: N sockets = BEAVA_SHARDS = 4.
#[tokio::test]
async fn bind_reuseport_invoked_by_boot_path() {
    const N_SHARDS: u16 = 4;

    let state = make_concurrent_state_default_store(
        PipelineEngine::new(),
        None,
        std::path::PathBuf::from("/tmp/beava-test-reuseport-boot.snapshot"),
        Arc::new(BackfillTracker::default()),
        false,
        false,
        Some(TEST_ADMIN.to_string()),
        false,
        N_SHARDS,
    );

    // Register shard metrics and spawn shard threads first (as run_tcp_server does).
    let shard_count = N_SHARDS as usize;
    let inbox_size = beava::shard::thread::inbox_size_from_env();
    let handles = beava::shard::thread::spawn_shard_threads(shard_count, inbox_size, state.clone());
    *state.shard_handles.write() = handles;
    beava::server::shard_probe::init_route_counters(shard_count);
    beava::metrics::install_prometheus_recorder();
    beava::shard::metrics::register_shard_metrics(shard_count);

    // Bind on a random port — after Task 2, this should create N=4 SO_REUSEPORT sockets.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let srv_state = state.clone();
    tokio::spawn(async move {
        let _ = beava::server::tcp::run_tcp_server_with_listener(listener, srv_state).await;
    });

    // Give the server time to spawn all per-shard accept loops.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Count how many LISTEN sockets are on this port via /proc/net/tcp.
    let socket_count = count_listen_sockets_on_port(port);

    // MUST FAIL before Task 2 (only 1 socket); PASSES after Task 2 (4 sockets).
    assert_eq!(
        socket_count, N_SHARDS as usize,
        "Expected {} SO_REUSEPORT listener sockets on port {} (one per shard), \
         but found {}. run_tcp_server_with_listener must call bind_reuseport_tcp \
         N times on Linux when shard_count > 1 (Task 2 not yet landed).",
        N_SHARDS, port, socket_count
    );
}
