//! Phase 58 Wave 0 RED: asserts that at `BEAVA_SHARDS=N`, the boot path
//! produces N observable listener endpoints (TPC-PERF-08 / D-A1 + D-B1).
//!
//! Platform split:
//!   Linux (`cfg(target_os = "linux")`): asserts N LISTEN sockets are bound
//!     to the same test port via SO_REUSEPORT. Flips GREEN at Wave 1.
//!     Reuses the `/proc/net/tcp` parse pattern established in
//!     `tests/test_so_reuseport_boot.rs` (Phase 50.5-02 Task 1).
//!   macOS (`cfg(not(target_os = "linux"))`): asserts
//!     `ConcurrentAppState.accept_threads_spawned_total == N` — the
//!     field is an always-on `AtomicU64` (Phase 58 Wave 0 adds it),
//!     bumped once per shard by Wave 2's dedicated-accept-thread spawner.
//!     Pre-Wave-2 reads 0 → RED.
//!
//! Ignore markers:
//!   - Linux test: `#[ignore = "58-W1"]` (Linux Wave 1 per-shard listener).
//!   - macOS test: `#[ignore = "58-W2"]` (macOS Wave 2 accept-thread spawn).
//!
//! Invocation:
//!   cargo test --release --test per_shard_listener_smoke -- --ignored
//!
//! Today (pre-Wave-1) both tests fail:
//!   - Linux: only 1 LISTEN socket (single `TcpListener::bind` at N=1 fallback,
//!     or the existing Phase 50 SO_REUSEPORT path wasn't extended for the TCP
//!     PUSH port — which is exactly what Wave 1 fixes).
//!   - macOS: counter is 0; Wave 2 wires it to N.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use beava::engine::pipeline::PipelineEngine;
use beava::server::tcp::{make_concurrent_state_full, BackfillTracker};

const TEST_ADMIN: &str = "test-admin-58-00-per-shard-listener";
const N_SHARDS: u16 = 4;

// ---------------------------------------------------------------------------
// Linux half: count LISTEN sockets on the test port in /proc/net/tcp.
// ---------------------------------------------------------------------------

/// Parse /proc/net/tcp and count LISTEN (state=0A) entries on `port`.
/// Helper copied verbatim from `tests/test_so_reuseport_boot.rs` (Phase
/// 50.5-02 Task 1 pattern). Factoring it out to `tests/common/` is
/// yak-shaving for a Wave-0 RED test; duplicated intentionally.
#[cfg(target_os = "linux")]
fn count_listen_sockets_on_port(port: u16) -> usize {
    let port_hex = format!("{:04X}", port);
    let content = match std::fs::read_to_string("/proc/net/tcp") {
        Ok(c) => c,
        Err(_) => {
            eprintln!("[per_shard_listener_smoke] /proc/net/tcp not readable — falling back to 0");
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
// Linux: N shards ⇒ N LISTEN sockets (SO_REUSEPORT).
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "guardrail-opens-real-tcp-socket; run with --ignored"]
async fn n_shards_produces_n_listeners_linux() {
    let state = make_concurrent_state_full(
        PipelineEngine::new(),
        None,
        std::path::PathBuf::from("/tmp/beava-test-58-00-linux-listener.snapshot"),
        Arc::new(BackfillTracker::default()),
        false,
        false,
        Some(TEST_ADMIN.to_string()),
        false,
        N_SHARDS,
    );

    let shard_count = N_SHARDS as usize;
    let inbox_size = beava::shard::thread::inbox_size_from_env();

    // Phase 58-01 Task 2: bind an ephemeral port UP FRONT, then pass the
    // address as `accept_cfg` to `spawn_shard_threads` so every shard binds
    // its own SO_REUSEPORT socket on that port. This flips the Linux half of
    // the RED smoke test GREEN — `count_listen_sockets_on_port` observes
    // N LISTEN sockets bound to the same port.
    //
    // The `probe_listener` bound here is just to discover a free port; we
    // drop it BEFORE spawning the shard threads because SO_REUSEPORT with a
    // plain (non-REUSEPORT) prior bind on the same port behaves
    // inconsistently across kernels. The shard threads then each create
    // their own SO_REUSEPORT sockets on the same port.
    let probe_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = probe_listener.local_addr().unwrap().port();
    drop(probe_listener);

    let accept_addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let accept_cfg = Some(beava::shard::thread::PerShardAcceptCfg {
        accept_addr,
        max_conns_per_shard: 256,
    });
    let handles = beava::shard::thread::spawn_shard_threads(
        shard_count,
        inbox_size,
        state.clone(),
        accept_cfg,
    );
    *state.shard_handles.write() = handles;
    beava::server::shard_probe::init_route_counters(shard_count);
    beava::metrics::install_prometheus_recorder();
    beava::shard::metrics::register_shard_metrics(shard_count);

    // No top-level listener needed: the shard threads own their SO_REUSEPORT
    // sockets. Bind a dummy listener for `run_tcp_server_with_listener` —
    // on Linux the function drops it and becomes `pending`.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();

    let srv_state = state.clone();
    tokio::spawn(async move {
        let _ = beava::server::tcp::run_tcp_server_with_listener(listener, srv_state).await;
    });

    // Give the server time to bring per-shard accept loops online.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let socket_count = count_listen_sockets_on_port(port);

    // RED until Wave 1 lands per-shard SO_REUSEPORT binding for the PUSH
    // port. Pre-Wave-1 count is typically 1 (single top-level listener).
    assert_eq!(
        socket_count, N_SHARDS as usize,
        "TPC-PERF-08 D-A1 gate FAIL: expected {} SO_REUSEPORT listener sockets on \
         port {} (one per shard), found {}. Wave 1 must bind N per-shard \
         listeners via `bind_reuseport_tcp`.",
        N_SHARDS, port, socket_count
    );
}

// ---------------------------------------------------------------------------
// macOS: N shards ⇒ N dedicated accept threads.
// ---------------------------------------------------------------------------

#[cfg(not(target_os = "linux"))]
#[tokio::test]
async fn n_shards_produces_n_accept_threads_macos() {
    // Phase 58-02 D-B2 escape-hatch: when `BEAVA_SHARDS_SINGLE_LISTENER=1`,
    // the fallback spawns exactly ONE accept thread (counter == 1, not N).
    // Skip the D-B1 N-counter assertion in that mode rather than falsely
    // failing — this mirrors Wave 2 plan §<behavior> bullet:
    // "the macOS smoke test is SKIPPED in this mode".
    let single_listener = std::env::var("BEAVA_SHARDS_SINGLE_LISTENER")
        .ok()
        .and_then(|s| s.parse::<u8>().ok())
        .map(|n| n != 0)
        .unwrap_or(false);
    if single_listener {
        eprintln!(
            "[58-02] D-B2 fallback active (BEAVA_SHARDS_SINGLE_LISTENER=1) — \
             skipping D-B1 per-shard-accept-thread assertion"
        );
        return;
    }

    let state = make_concurrent_state_full(
        PipelineEngine::new(),
        None,
        std::path::PathBuf::from("/tmp/beava-test-58-00-macos-listener.snapshot"),
        Arc::new(BackfillTracker::default()),
        false,
        false,
        Some(TEST_ADMIN.to_string()),
        false,
        N_SHARDS,
    );

    let shard_count = N_SHARDS as usize;
    let inbox_size = beava::shard::thread::inbox_size_from_env();
    // Phase 58-02 Task 2: macOS accept threads spawn AFTER shard handles are
    // installed (to avoid the boot-race where a client could connect before
    // `state.shard_handles.write()` runs). We replicate `run_tcp_server`'s
    // ordering here instead of calling it directly — the test binds its own
    // loopback ephemeral port rather than relying on a shared public addr.
    let handles = beava::shard::thread::spawn_shard_threads(
        shard_count,
        inbox_size,
        state.clone(),
        None,
    );
    *state.shard_handles.write() = handles;
    beava::server::shard_probe::init_route_counters(shard_count);
    beava::metrics::install_prometheus_recorder();
    beava::shard::metrics::register_shard_metrics(shard_count);

    // Pre-bind a loopback ephemeral port and drop it so the macOS per-shard
    // accept threads can bind their own SO_REUSEPORT sockets on that port.
    // (macOS BSD-style REUSEPORT permits the new binds even if a prior
    // non-REUSEPORT listener briefly held the port.)
    let probe_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = probe_listener.local_addr().unwrap().port();
    drop(probe_listener);

    let accept_addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let max_conns = beava::shard::thread::max_conns_per_shard_from_env();
    let _accept_threads = beava::server::tcp::spawn_macos_per_shard_accept_threads(
        accept_addr,
        shard_count,
        state.clone(),
        max_conns,
    )
    .expect("macOS per-shard accept thread bind");

    // Give the OS time for each accept thread's `fetch_add` bump to land
    // before we observe the counter. The threads bump before entering the
    // blocking `accept()` call, so 100 ms is ample on a loaded CI box.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let threads_spawned = state.accept_threads_spawned_total.load(Ordering::Relaxed);

    assert_eq!(
        threads_spawned as u64, N_SHARDS as u64,
        "TPC-PERF-08 D-B1 gate FAIL: expected {} dedicated macOS accept threads \
         (accept_threads_spawned_total=N at BEAVA_SHARDS=N), found {}. Wave 2 \
         must spawn one `std::thread` per shard running a blocking \
         `TcpListener::accept` loop and bump this counter exactly once per shard.",
        N_SHARDS, threads_spawned
    );
}
