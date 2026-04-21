//! Phase 59-00 Wave 0 — TPC-PERF-09 D-B1 / D-B2 RED contract (flips GREEN at Wave 2).
//!
//! Phase 59 Wave 2 adds `OP_NEGOTIATE_WIRE_FORMAT = 0x18` (D-B1). Wave 0
//! plants this test as a latent failure — today the server returns
//! `STATUS_ERROR "unknown opcode 0x18"` because the opcode isn't defined.
//! Wave 2 registers the opcode + capability bits; this test flips GREEN.
//!
//! Request wire (D-B1):
//!   `[u32 BE frame_len=7][u8 opcode=0x18][u32 BE client_cap_bits][u16 BE client_version_tag]`
//!
//! Response wire on success:
//!   `[u32 BE resp_len=7][u8 STATUS_OK][u32 BE server_cap_bits][u16 BE server_version_tag]`
//!
//! Capability bits: `WIRE_BINARY_PASSTHROUGH = 1 << 0`. Phase 59 sets
//! bit 0 = 1 in the server reply.
//!
//! Marked `#[ignore = "59-W2"]` per D-D1.
//!
//! Test command: `cargo test --release --test wire_negotiation_handshake -- --ignored`.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use beava::engine::pipeline::PipelineEngine;
use beava::server::tcp::{make_concurrent_state_full, BackfillTracker, SharedState};

const TEST_ADMIN: &str = "test-admin-59-00-wire-negotiate";
/// Phase 59 D-B1: `OP_NEGOTIATE_WIRE_FORMAT`. Wave 2 adds this as a
/// public const in `beava::server::protocol`; Wave 0 hardcodes the value
/// so this test can compile pre-Wave-2.
const OP_NEGOTIATE_WIRE_FORMAT: u8 = 0x18;
/// Phase 59 D-B1: capability bit for `WIRE_BINARY_PASSTHROUGH`.
const WIRE_BINARY_PASSTHROUGH: u32 = 1 << 0;
/// Phase 59 D-B1: client version tag = 2 (Phase-59 SDK speaks v2).
const CLIENT_VERSION_TAG: u16 = 2;

fn build_state_n_shards(tag: &str, n_shards: u16) -> SharedState {
    let state = make_concurrent_state_full(
        PipelineEngine::new(),
        None,
        std::path::PathBuf::from(format!(
            "/tmp/beava-test-59-00-wire-negotiate-{tag}.snapshot"
        )),
        Arc::new(BackfillTracker::default()),
        false,
        false,
        Some(TEST_ADMIN.to_string()),
        false,
        n_shards,
    );

    let handles =
        beava::shard::thread::spawn_shard_threads(n_shards as usize, 65_536, state.clone(), None);
    *state.shard_handles.write() = handles;
    beava::server::shard_probe::init_route_counters(n_shards as usize);
    beava::metrics::install_prometheus_recorder();
    beava::shard::metrics::register_shard_metrics(n_shards as usize);
    state
}

/// Send an `OP_NEGOTIATE_WIRE_FORMAT` handshake and parse the server reply.
/// Returns (status, server_cap_bits, server_version_tag).
async fn negotiate(
    addr: std::net::SocketAddr,
    client_cap_bits: u32,
    client_version: u16,
) -> (u8, u32, u16) {
    let mut conn = TcpStream::connect(addr).await.unwrap();

    // Request payload: [u32 BE client_cap_bits][u16 BE client_version_tag]
    let mut payload = Vec::with_capacity(6);
    payload.extend_from_slice(&client_cap_bits.to_be_bytes());
    payload.extend_from_slice(&client_version.to_be_bytes());

    let total_len = (1 + payload.len()) as u32;
    conn.write_u32(total_len).await.unwrap();
    conn.write_u8(OP_NEGOTIATE_WIRE_FORMAT).await.unwrap();
    conn.write_all(&payload).await.unwrap();
    conn.flush().await.unwrap();

    let resp_len = conn.read_u32().await.unwrap() as usize;
    let status = conn.read_u8().await.unwrap();
    let mut body = vec![0u8; resp_len - 1];
    if !body.is_empty() {
        conn.read_exact(&mut body).await.unwrap();
    }

    // On STATUS_OK, body is [u32 BE server_cap_bits][u16 BE server_version_tag]
    if status == 0x00 && body.len() >= 6 {
        let server_bits = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
        let server_ver = u16::from_be_bytes([body[4], body[5]]);
        (status, server_bits, server_ver)
    } else {
        (status, 0, 0)
    }
}

/// TPC-PERF-09 D-B1: `OP_NEGOTIATE_WIRE_FORMAT` echoes server capability
/// bits, server version ≥ 2, and sets `WIRE_BINARY_PASSTHROUGH` = 1.
///
/// Pre-Wave-2: server returns `STATUS_ERROR` for opcode 0x18 → test FAILS
/// (the connection may even close after the error, so the first assert
/// on `status == STATUS_OK` triggers).
/// Post-Wave-2: GREEN — server advertises binary-passthrough support.
#[tokio::test]
async fn op_negotiate_wire_format_round_trips_capability_bits() {
    let state = build_state_n_shards("round_trip", 2);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let srv_state = state.clone();
    tokio::spawn(async move {
        let _ = beava::server::tcp::run_tcp_server_with_listener(listener, srv_state).await;
    });
    tokio::time::sleep(Duration::from_millis(30)).await;

    let (status, server_bits, server_ver) =
        negotiate(addr, WIRE_BINARY_PASSTHROUGH, CLIENT_VERSION_TAG).await;

    assert_eq!(
        status, 0x00,
        "TPC-PERF-09 D-B1: OP_NEGOTIATE_WIRE_FORMAT must return STATUS_OK \
         (got 0x{:02x}). Pre-Wave-2 HEAD expected to fail here with \
         STATUS_ERROR 'unknown opcode 0x18' — that's the RED contract.",
        status
    );

    assert!(
        server_bits & WIRE_BINARY_PASSTHROUGH != 0,
        "TPC-PERF-09 D-B1: server reply must set WIRE_BINARY_PASSTHROUGH \
         (bit 0) in server_cap_bits (got 0x{:08x}).",
        server_bits
    );

    assert!(
        server_ver >= CLIENT_VERSION_TAG,
        "TPC-PERF-09 D-B1: server version tag must be ≥ 2 \
         (client sent {CLIENT_VERSION_TAG}, got {server_ver})."
    );
}
