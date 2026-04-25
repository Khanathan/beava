//! Phase 18 Plan 10 — parse-stage optimization integration tests.
//!
//! Asserts that:
//! - dispatch_push_sync routes msgpack bodies through `Row::Deserialize`
//!   directly (no `JsonValue` intermediate).
//! - dispatch_push_sync routes JSON bodies through `Row::Deserialize` directly.
//! - Resulting Row contents are exactly the same as Plan 18-09.
//!
//! The "no JsonValue allocation" claim is gated by the microbench (Task 10.4).
//! Here we assert correctness only — the function signature change (returning
//! `Row` directly from sonic_rs/rmp_serde) is the contract.

use beava_core::wire::{CT_JSON, CT_MSGPACK, OP_PUSH};
use bytes::Bytes;
use serde_json::json;

static SERVER_SERIALIZER_10: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Wait until the hand-rolled HTTP server at `addr` accepts connections.
async fn wait_for_http_10(addr: std::net::SocketAddr) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(500))
        .build()
        .expect("reqwest client");
    loop {
        match client.get(format!("http://{}/ping", addr)).send().await {
            Ok(_) => return,
            Err(_) => {
                if tokio::time::Instant::now() >= deadline {
                    panic!("hand-rolled HTTP server at {} did not become ready", addr);
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }
    }
}

fn small_pipeline_register() -> serde_json::Value {
    json!({
        "nodes": [
            {
                "kind": "event",
                "name": "TxnEvent",
                "schema": {
                    "fields": {"user_id": "str", "amount": "f64", "event_time": "i64"},
                    "optional_fields": []
                },
                "event_time_field": "event_time"
            },
            {
                "kind": "derivation",
                "name": "TxnAgg",
                "output_kind": "table",
                "upstreams": ["TxnEvent"],
                "ops": [
                    {
                        "op": "group_by",
                        "keys": ["user_id"],
                        "agg": {"cnt": {"op": "count", "params": {}}}
                    }
                ],
                "schema": {
                    "fields": {"user_id": "str", "cnt": "i64"},
                    "optional_fields": []
                },
                "table_primary_key": ["user_id"]
            }
        ]
    })
}

fn make_msgpack_envelope(event_name: &str, body: &serde_json::Value) -> Vec<u8> {
    use serde::Serialize;
    #[derive(Serialize)]
    struct Envelope<'a> {
        event: &'a str,
        body: &'a serde_json::Value,
    }
    rmp_serde::to_vec_named(&Envelope {
        event: event_name,
        body,
    })
    .expect("msgpack serialize envelope")
}

async fn send_msgpack_push_tcp(
    tcp_addr: std::net::SocketAddr,
    event_name: &str,
    body: &serde_json::Value,
) -> beava_core::wire::Frame {
    use beava_server::testing::TcpClient;
    let envelope_bytes = make_msgpack_envelope(event_name, body);
    let mut client = TcpClient::connect(tcp_addr)
        .await
        .expect("TcpClient connect");
    client
        .send_raw(OP_PUSH, CT_MSGPACK, Bytes::from(envelope_bytes))
        .await
        .expect("send_raw msgpack push")
}

async fn send_json_push_tcp(
    tcp_addr: std::net::SocketAddr,
    event_name: &str,
    body: &serde_json::Value,
) -> beava_core::wire::Frame {
    use beava_server::testing::TcpClient;
    let envelope = json!({"event": event_name, "body": body});
    let envelope_bytes = serde_json::to_vec(&envelope).expect("json envelope");
    let mut client = TcpClient::connect(tcp_addr)
        .await
        .expect("TcpClient connect");
    client
        .send_raw(OP_PUSH, CT_JSON, Bytes::from(envelope_bytes))
        .await
        .expect("send_raw json push")
}

/// Push two msgpack events; verify Row contents survive the no-JsonValue path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_dispatch_push_sync_no_jsonvalue_intermediate_msgpack() {
    {
        let _g = SERVER_SERIALIZER_10.lock().unwrap();
    }
    let any: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let sv18 = beava_server::server::ServerV18::bind(any, any, any)
        .await
        .expect("ServerV18::bind");

    let http_addr = sv18.http_addr();
    let tcp_addr = sv18.tcp_addr();

    let wal_dir = tempfile::tempdir().expect("wal dir");
    let snap_dir = tempfile::tempdir().expect("snap dir");
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let wp = wal_dir.path().to_path_buf();
    let sp = snap_dir.path().to_path_buf();
    let serve_task = tokio::spawn(async move {
        sv18.serve_with_dirs(
            async {
                let _ = shutdown_rx.await;
            },
            wp,
            sp,
        )
        .await
    });

    wait_for_http_10(http_addr).await;

    let http = reqwest::Client::new();
    let reg_resp = http
        .post(format!("http://{}/register", http_addr))
        .header("Content-Type", "application/json")
        .json(&small_pipeline_register())
        .send()
        .await
        .expect("register");
    assert!(reg_resp.status().is_success());

    // Push two msgpack events for the same user.
    for et in [1_000_000i64, 2_000_000] {
        let body = json!({"user_id": "u_msgpack", "amount": 42.0, "event_time": et});
        let resp_frame = send_msgpack_push_tcp(tcp_addr, "TxnEvent", &body).await;
        assert_eq!(resp_frame.op, OP_PUSH, "msgpack push must succeed");
    }

    // Verify count = 2 — proves Row.fields included user_id correctly via the
    // direct Row deserialize path (no JsonValue intermediate).
    let get_resp = http
        .get(format!("http://{}/get/cnt/u_msgpack", http_addr))
        .send()
        .await
        .expect("get");
    assert!(get_resp.status().is_success(), "GET status");
    let v: serde_json::Value = get_resp.json().await.expect("json");
    assert_eq!(
        v["value"],
        json!(2),
        "msgpack push aggregations must increment correctly"
    );

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), serve_task).await;
}

/// Push two JSON events; verify Row contents survive the no-JsonValue path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_dispatch_push_sync_no_jsonvalue_intermediate_json() {
    {
        let _g = SERVER_SERIALIZER_10.lock().unwrap();
    }
    let any: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let sv18 = beava_server::server::ServerV18::bind(any, any, any)
        .await
        .expect("ServerV18::bind");

    let http_addr = sv18.http_addr();
    let tcp_addr = sv18.tcp_addr();

    let wal_dir = tempfile::tempdir().expect("wal dir");
    let snap_dir = tempfile::tempdir().expect("snap dir");
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let wp = wal_dir.path().to_path_buf();
    let sp = snap_dir.path().to_path_buf();
    let serve_task = tokio::spawn(async move {
        sv18.serve_with_dirs(
            async {
                let _ = shutdown_rx.await;
            },
            wp,
            sp,
        )
        .await
    });

    wait_for_http_10(http_addr).await;

    let http = reqwest::Client::new();
    let reg_resp = http
        .post(format!("http://{}/register", http_addr))
        .header("Content-Type", "application/json")
        .json(&small_pipeline_register())
        .send()
        .await
        .expect("register");
    assert!(reg_resp.status().is_success());

    for et in [1_000_000i64, 2_000_000] {
        let body = json!({"user_id": "u_json", "amount": 99.0, "event_time": et});
        let resp_frame = send_json_push_tcp(tcp_addr, "TxnEvent", &body).await;
        assert_eq!(resp_frame.op, OP_PUSH, "json push must succeed");
    }

    let get_resp = http
        .get(format!("http://{}/get/cnt/u_json", http_addr))
        .send()
        .await
        .expect("get");
    assert!(get_resp.status().is_success());
    let v: serde_json::Value = get_resp.json().await.expect("json");
    assert_eq!(
        v["value"],
        json!(2),
        "json push aggregations must increment correctly"
    );

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), serve_task).await;
}

/// Negative-marker test (best-effort): walk the apply_shard.rs source for
/// references to `JsonValue::from_*` constructors or `serde_json::from_slice<JsonValue>`
/// in dispatch_push_sync. The hot path must not allocate JsonValue anymore.
///
/// This is a textual check (cargo expand would be nicer but the build cost is
/// prohibitive for a unit test). Failing patterns are documented per the plan
/// (D-3): no `JsonValue` indirection in `dispatch_push_sync`.
#[test]
fn test_apply_shard_dispatch_push_sync_no_jsonvalue_construct() {
    let src = std::fs::read_to_string("src/apply_shard.rs").expect("read apply_shard.rs");
    // Find the dispatch_push_sync function body.
    let start = src
        .find("fn dispatch_push_sync(")
        .expect("dispatch_push_sync defined in apply_shard.rs");
    // Take ~200 lines as the body window (the function is about that long).
    let body = &src[start..src.len().min(start + 12_000)];

    // The function MUST NOT pull in `serde_json::Value as JsonValue`
    // any longer (the `let parsed: JsonValue` from Plan 18-09 is gone).
    assert!(
        !body.contains("let parsed: JsonValue"),
        "dispatch_push_sync still has the `let parsed: JsonValue` line — \
         Plan 18-10 D-3 requires direct Row deserialize"
    );
    assert!(
        !body.contains("rmp_serde::from_slice::<JsonValue>"),
        "dispatch_push_sync still uses rmp_serde::from_slice::<JsonValue> — \
         must use rmp_serde::from_slice::<Row> per Plan 18-10 D-3"
    );

    // Sanity: confirm the file actually contains the new direct-Row path.
    assert!(
        body.contains("Row")
            && (body.contains("rmp_serde::from_slice::<Row>")
                || body.contains("from_slice::<Row>")
                || body.contains(": Row =")),
        "dispatch_push_sync must deserialise into Row directly per Plan 18-10 D-3 \
         (snippet: ...{}...)",
        &body[..body.len().min(800)]
    );
}
