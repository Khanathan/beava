//! Phase 54-00 Task 3 — Replica ingest routing RED test (drives Wave 1 Task 3).
//!
//! Protects against the "silent regression" flagged in 54-RESEARCH.md Risk #3:
//! when Wave 1 rewires the N=1 hot path through the shard thread,
//! `PipelineEngine::push_internal_on_shard` will be the mutation path — but
//! currently it has NO `notify_subscribers` call. Without a parallel hook on
//! the shard path, every live `OP_SUBSCRIBE` session goes silent.
//!
//! **Scope deviation from plan text:** The plan proposes testing at N=1, but
//! at Phase 53 HEAD N=1 still uses the LEGACY `push_with_cascade_no_features`
//! path which DOES call `notify_subscribers` (pipeline.rs:1198). So an N=1
//! test would pass today. The real-today RED condition is at N>1, where the
//! shard path IS live: a subscriber registered for shard-owned keys NEVER
//! sees events because `push_internal_on_shard` (pipeline.rs:1939) skips the
//! notify hook. We test at N=2. After Wave 1 deletes the legacy path, this
//! test also guards N=1.
//!
//! Test command: `cargo test --release --test replica_ingest_routing`.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use beava::engine::pipeline::{FeatureDef, PipelineEngine, StreamDefinition};
use beava::server::protocol::Scope;
use beava::server::replica::{ReplicaEvent, SubscriberRegistry};
use beava::server::signals::SignalRegistry;
use beava::server::tcp::{make_concurrent_state_full, BackfillTracker, SharedState};
const TEST_ADMIN: &str = "test-admin-54-00-replica-routing";

use std::sync::OnceLock;
static REGISTRY_MAP: OnceLock<
    std::sync::Mutex<std::collections::HashMap<String, Arc<SubscriberRegistry>>>,
> = OnceLock::new();

fn build_two_shard_state(tag: &str) -> SharedState {
    let state = make_concurrent_state_full(
        PipelineEngine::new(),
        None,
        std::path::PathBuf::from(format!("/tmp/beava-test-54-00-replica-{tag}.snapshot")),
        Arc::new(BackfillTracker::default()),
        false,
        false,
        Some(TEST_ADMIN.to_string()),
        false,
        2,
    );

    state
        .engine
        .write()
        .register(StreamDefinition {
            name: "replica_stream".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
            features: vec![(
                "count_1h".into(),
                FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                    backfill: false,
                },
            )],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        })
        .unwrap();

    // Build and wire a SubscriberRegistry.
    let signals = SignalRegistry::new_default().into_shared();
    let registry = Arc::new(SubscriberRegistry::new(signals));
    state
        .engine
        .write()
        .install_subscribers(Arc::clone(&registry));

    *state.shard_handles.write() =
        beava::shard::thread::spawn_shard_threads(2, 65_536, state.clone(), None);
    beava::server::shard_probe::init_route_counters(2);

    beava::metrics::install_prometheus_recorder();
    beava::shard::metrics::register_shard_metrics(2);

    REGISTRY_MAP
        .get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
        .lock()
        .unwrap()
        .insert(tag.to_string(), registry);

    state
}

fn get_registry(tag: &str) -> Arc<SubscriberRegistry> {
    REGISTRY_MAP
        .get()
        .unwrap()
        .lock()
        .unwrap()
        .get(tag)
        .cloned()
        .expect("registry registered for this tag")
}

fn scope_for(streams: &[&str]) -> Scope {
    Scope {
        streams: streams.iter().map(|s| s.to_string()).collect(),
        keys: None,
        key_prefix: None,
        pull: "eager".to_string(),
    }
}

/// At N>1, a push that transits the shard-thread path MUST fire
/// `notify_subscribers` so live `OP_SUBSCRIBE` sessions receive the event.
///
/// Phase 53 HEAD: FAILS. `push_internal_on_shard` (src/engine/pipeline.rs:1939)
/// has NO `notify_subscribers` call. Subscribers silently miss every event at
/// N>1.
///
/// Phase 54-01 Task 3 (Wave 1 GREEN): PASSES. The notify hook is mirrored on
/// the shard path (parallel to `push_internal`'s hook at pipeline.rs:1198).
#[tokio::test]
async fn replica_push_fires_notify_on_shard_path() {
    let tag = "shard_notify";
    let state = build_two_shard_state(tag);
    let registry = get_registry(tag);

    // Register a subscriber session scoped to `replica_stream`.
    let (tx, mut rx) = mpsc::channel::<ReplicaEvent>(64);
    let _conn_id = registry.register(scope_for(&["replica_stream"]), tx);

    // Push 20 events with diverse user_ids so both shards receive some.
    let now = std::time::SystemTime::now();
    for i in 0..20u32 {
        let user_id = format!("repu_{:04}", i);
        let payload = serde_json::json!({ "user_id": user_id, "amount": i });

        // At N>1, handle_push_core_ex routes to the shard via SPSC.
        let _ = beava::server::tcp::handle_push_core_ex(
            &state,
            "replica_stream",
            &payload,
            &[],
            now,
            false,
            None,
        );
    }

    // Give the shard threads time to drain SPSC inboxes and process events.
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Drain the subscriber channel with a bounded timeout.
    let deadline = tokio::time::Instant::now() + Duration::from_millis(800);
    let mut received: Vec<ReplicaEvent> = Vec::new();
    while received.len() < 20 && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(50), rx.recv()).await {
            Ok(Some(ev)) => received.push(ev),
            Ok(None) => break,
            Err(_) => {} // timeout; retry until deadline
        }
    }

    assert!(
        !received.is_empty(),
        "TPC-ARCH-01 (replica silent-regression guard): 0 of 20 events reached the \
         subscriber session. At N=2, the shard-thread mutation path \
         (src/engine/pipeline.rs::push_internal_on_shard) does NOT call \
         `notify_subscribers` — live OP_SUBSCRIBE sessions silently miss every \
         event. Wave 1 plan 54-01 Task 3 must port the notify hook to the shard \
         path (parallel to push_internal's hook at pipeline.rs:1198)."
    );
}

// ===========================================================================
// Phase 58-03 (Wave 3) — Replica ingest rides the per-shard accept topology
// ===========================================================================
//
// Wave 1 (Linux SO_REUSEPORT per-shard accept) + Wave 2 (macOS dedicated
// std::thread per shard) established a unified per-shard TCP accept
// topology on the TCP PUSH port. Replica connections — OP_LOG_FETCH and
// OP_SUBSCRIBE — share that port, so they inherit the same accept path
// "for free" via the single `handle_connection` opcode-dispatch table.
//
// These tests GUARDRAIL against a future regression where an executor
// splits the accept path ("primary PUSH" vs "replica") or carves out a
// dedicated replica listener. They assert:
//
//   (a) At BEAVA_SHARDS=4, N per-shard listeners / N accept threads exist
//       (Linux: 4 LISTEN sockets via /proc/net/tcp; macOS:
//       `accept_threads_spawned_total == 4`).
//   (b) A TCP client issuing a replica opcode (OP_LOG_FETCH) connects
//       successfully to that per-shard listener set, completes the
//       auth+handshake, and receives the END frame back — proving the
//       replica ingress opcode dispatched through the same per-shard
//       accept path as primary PUSH, with no separate listener.
//
// Invocation:
//   cargo test --release --test replica_ingest_routing -- --ignored

const TEST_ADMIN_W3: &str = "test-admin-58-03-replica-per-shard";
const N_SHARDS_W3: usize = 4;

/// Build an N=4-shard SharedState with a registered keyed stream named
/// `replica_stream_w3` so OP_LOG_FETCH validates the scope. No subscriber
/// is registered (Wave 3 is accept-topology-shape, not notify-path).
fn build_four_shard_state_w3(tag: &str) -> SharedState {
    let state = make_concurrent_state_full(
        PipelineEngine::new(),
        None,
        std::path::PathBuf::from(format!("/tmp/beava-test-58-03-{tag}.snapshot")),
        Arc::new(BackfillTracker::default()),
        false,
        false,
        Some(TEST_ADMIN_W3.to_string()),
        false,
        N_SHARDS_W3 as u16,
    );

    state
        .engine
        .write()
        .register(StreamDefinition {
            name: "replica_stream_w3".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
            features: vec![(
                "count_1h".into(),
                FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                    backfill: false,
                },
            )],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        })
        .unwrap();

    state
}

/// Build an OP_LOG_FETCH request frame (byte layout mirrors the private
/// `build_log_fetch_frame` in `src/server/replica_client.rs`:
///   [u32 BE total_len][u8 OP_LOG_FETCH][u16 BE token_len][token]
///   [u64 BE from_ts_millis][Scope bytes via client::wire::write_scope]
fn build_log_fetch_frame(token: &str, from_ts_millis: u64, streams: &[&str]) -> Vec<u8> {
    use beava::client::wire::{write_scope, Scope as ClientScope, OP_LOG_FETCH};

    let scope = ClientScope {
        streams: streams.iter().map(|s| s.to_string()).collect(),
        keys: None,
        key_prefix: None,
        // v0 only implements pull="all" (see server::protocol::validate_scope);
        // "eager" is a protocol-reserved future value.
        pull: "all".to_string(),
    };

    let mut payload = Vec::new();
    let token_bytes = token.as_bytes();
    payload.extend_from_slice(&(token_bytes.len() as u16).to_be_bytes());
    payload.extend_from_slice(token_bytes);
    payload.extend_from_slice(&from_ts_millis.to_be_bytes());
    write_scope(&mut payload, &scope);

    let total_len = (1 + payload.len()) as u32;
    let mut frame = Vec::with_capacity(4 + total_len as usize);
    frame.extend_from_slice(&total_len.to_be_bytes());
    frame.push(OP_LOG_FETCH);
    frame.extend_from_slice(&payload);
    frame
}

/// Send an OP_LOG_FETCH request and drain the response frames until the
/// terminal END frame (tag 0x04) is observed. Returns the count of EVENT
/// frames (tag 0x03) seen. Any STATUS_ERROR envelope or I/O error is
/// propagated as an `Err`.
async fn send_log_fetch_and_drain(
    addr: std::net::SocketAddr,
    token: &str,
    streams: &[&str],
) -> Result<usize, String> {
    use beava::client::wire::{REPLICA_FRAME_TAG_END, REPLICA_FRAME_TAG_EVENT};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .map_err(|e| format!("tcp connect: {e}"))?;
    let frame = build_log_fetch_frame(token, 0, streams);
    stream
        .write_all(&frame)
        .await
        .map_err(|e| format!("write log_fetch: {e}"))?;

    let mut events = 0usize;
    loop {
        let frame_len = stream
            .read_u32()
            .await
            .map_err(|e| format!("read frame_len: {e}"))?
            as usize;
        if frame_len == 0 || frame_len > 64 * 1024 * 1024 {
            return Err(format!("invalid frame_len: {frame_len}"));
        }
        let tag = stream
            .read_u8()
            .await
            .map_err(|e| format!("read tag: {e}"))?;
        let body_len = frame_len - 1;
        let mut body = vec![0u8; body_len];
        if body_len > 0 {
            stream
                .read_exact(&mut body)
                .await
                .map_err(|e| format!("read body: {e}"))?;
        }
        match tag {
            REPLICA_FRAME_TAG_EVENT => {
                events += 1;
            }
            REPLICA_FRAME_TAG_END => {
                return Ok(events);
            }
            other => {
                let body_str = String::from_utf8_lossy(&body);
                return Err(format!(
                    "unexpected frame tag 0x{other:02x} body={body_str:?} (body_len={body_len})"
                ));
            }
        }
    }
}

/// Count LISTEN sockets on `port` by parsing /proc/net/tcp. Mirrors the
/// helper in `tests/per_shard_listener_smoke.rs` / `tests/test_so_reuseport_boot.rs`.
#[cfg(target_os = "linux")]
fn count_listen_sockets_on_port_w3(port: u16) -> usize {
    let port_hex = format!("{:04X}", port);
    let content = match std::fs::read_to_string("/proc/net/tcp") {
        Ok(c) => c,
        Err(_) => {
            eprintln!("[58-03] /proc/net/tcp not readable — falling back to 0");
            return 0;
        }
    };
    let mut count = 0usize;
    for line in content.lines().skip(1) {
        let cols: Vec<&str> = line.split_whitespace().collect();
        if cols.len() < 4 {
            continue;
        }
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
// Linux — replica OP_LOG_FETCH lands on per-shard SO_REUSEPORT listener set.
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "58-W3"]
async fn replica_ingest_lands_on_per_shard_accept_linux_at_n4() {
    use std::sync::atomic::Ordering;

    let state = build_four_shard_state_w3("linux");

    // Bind an ephemeral loopback port up front, then drop it so the shards
    // can each bind their own SO_REUSEPORT socket on that port (mirrors the
    // Wave-1 smoke test harness).
    let probe_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = probe_listener.local_addr().unwrap().port();
    drop(probe_listener);

    let accept_addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let accept_cfg = Some(beava::shard::thread::PerShardAcceptCfg {
        accept_addr,
        max_conns_per_shard: 256,
    });

    let inbox_size = beava::shard::thread::inbox_size_from_env();
    let handles = beava::shard::thread::spawn_shard_threads(
        N_SHARDS_W3,
        inbox_size,
        state.clone(),
        accept_cfg,
    );
    *state.shard_handles.write() = handles;
    beava::server::shard_probe::init_route_counters(N_SHARDS_W3);
    beava::metrics::install_prometheus_recorder();
    beava::shard::metrics::register_shard_metrics(N_SHARDS_W3);

    // Give per-shard accept loops time to bind + begin accepting.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // (a) N listener sockets present (Wave 1 topology, reused by replica).
    let socket_count = count_listen_sockets_on_port_w3(port);
    assert_eq!(
        socket_count, N_SHARDS_W3,
        "TPC-PERF-08 Wave 3: expected {} per-shard SO_REUSEPORT listeners on \
         port {} (replica ingest shares the primary-PUSH accept topology), \
         found {}. A separate replica listener or a missing shard bind would \
         surface here.",
        N_SHARDS_W3, port, socket_count
    );

    // accept_threads_spawned_total is also bumped on Linux (Wave 1 bumps it
    // at listener install).
    let threads_bumped = state.accept_threads_spawned_total.load(Ordering::Relaxed);
    assert_eq!(
        threads_bumped as usize, N_SHARDS_W3,
        "accept_threads_spawned_total expected {} (per-shard install), got {}",
        N_SHARDS_W3, threads_bumped
    );

    // (b) Replica OP_LOG_FETCH connects successfully and receives END frame.
    // Empty log ⇒ zero event frames then END. This proves the replica-ingress
    // opcode transited the same per-shard accept path as primary PUSH.
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let event_count = send_log_fetch_and_drain(addr, TEST_ADMIN_W3, &["replica_stream_w3"])
        .await
        .expect("OP_LOG_FETCH via per-shard accept");
    assert_eq!(
        event_count, 0,
        "empty log should yield zero event frames before END; got {event_count}"
    );
}

// ---------------------------------------------------------------------------
// macOS — replica OP_LOG_FETCH lands on dedicated-thread-per-shard accept.
// ---------------------------------------------------------------------------

#[cfg(not(target_os = "linux"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "58-W3"]
async fn replica_ingest_lands_on_per_shard_accept_macos_at_n4() {
    use std::sync::atomic::Ordering;

    // D-B2 fallback: when BEAVA_SHARDS_SINGLE_LISTENER=1, the single-accept
    // spawner bumps the counter exactly once (not N). The D-B1 per-shard
    // assertion doesn't apply — skip with an informative eprintln, mirroring
    // tests/per_shard_listener_smoke.rs.
    let single_listener = std::env::var("BEAVA_SHARDS_SINGLE_LISTENER")
        .ok()
        .and_then(|s| s.parse::<u8>().ok())
        .map(|n| n != 0)
        .unwrap_or(false);
    if single_listener {
        eprintln!(
            "[58-03] BEAVA_SHARDS_SINGLE_LISTENER=1 — D-B2 fallback active; \
             skipping D-B1 per-shard-accept-thread assertion for replica path"
        );
        return;
    }

    let state = build_four_shard_state_w3("macos");
    let inbox_size = beava::shard::thread::inbox_size_from_env();
    let handles =
        beava::shard::thread::spawn_shard_threads(N_SHARDS_W3, inbox_size, state.clone(), None);
    *state.shard_handles.write() = handles;
    beava::server::shard_probe::init_route_counters(N_SHARDS_W3);
    beava::metrics::install_prometheus_recorder();
    beava::shard::metrics::register_shard_metrics(N_SHARDS_W3);

    // Bind+drop a loopback ephemeral port, then spawn the macOS per-shard
    // accept threads on it (replicates run_tcp_server's ordering — handles
    // install first, accept threads second, listener bind race-free).
    let probe_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = probe_listener.local_addr().unwrap().port();
    drop(probe_listener);

    let accept_addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let max_conns = beava::shard::thread::max_conns_per_shard_from_env();
    let _accept_threads = beava::server::tcp::spawn_macos_per_shard_accept_threads(
        accept_addr,
        N_SHARDS_W3,
        state.clone(),
        max_conns,
    )
    .expect("macOS per-shard accept thread bind");

    // Give each accept thread time to bump the counter before observing.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // (a) N dedicated accept threads (Wave 2 topology, reused by replica).
    let threads_spawned = state.accept_threads_spawned_total.load(Ordering::Relaxed);
    assert_eq!(
        threads_spawned as usize, N_SHARDS_W3,
        "TPC-PERF-08 Wave 3: expected {} dedicated macOS accept threads \
         (replica ingest shares the primary-PUSH accept topology), got {}. \
         A separate replica accept thread or a missing shard spawn would \
         surface here.",
        N_SHARDS_W3, threads_spawned
    );

    // (b) Replica OP_LOG_FETCH connects successfully and receives END frame.
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let event_count = send_log_fetch_and_drain(addr, TEST_ADMIN_W3, &["replica_stream_w3"])
        .await
        .expect("OP_LOG_FETCH via per-shard accept");
    assert_eq!(
        event_count, 0,
        "empty log should yield zero event frames before END; got {event_count}"
    );
}
