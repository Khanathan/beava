//! Phase 2.5 acceptance gate — proves all 8 ROADMAP Phase 2.5 success
//! criteria end-to-end over real HTTP + TCP sockets via TestServer.

use beava_core::wire::{
    decode_frame, encode_frame, Frame, CT_JSON, CT_MSGPACK, OP_ERROR_RESPONSE, OP_PUSH, OP_REGISTER,
};
use beava_server::testing::{TcpClient, TestServer, TestServerBuilder};
use bytes::{Bytes, BytesMut};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn valid_event_payload() -> Value {
    json!({
        "nodes": [{
            "kind": "event",
            "name": "Transaction",
            "schema": {
                "fields": {"card_id": "str", "amount": "f64", "event_time": "i64"},
                "optional_fields": []
            },
            "event_time_field": "event_time"
        }]
    })
}

fn event_node_named(name: &str) -> Value {
    json!({
        "kind": "event",
        "name": name,
        "schema": {
            "fields": {"event_time": "i64", "x": "f64"},
            "optional_fields": []
        },
        "event_time_field": "event_time"
    })
}

// ─── Criterion 1: both listeners bind at startup ──────────────────────────────

#[tokio::test]
async fn criterion_1_both_listeners_bind_at_startup() {
    let ts = TestServer::spawn().await.expect("spawn");

    // HTTP is reachable
    let resp = ts
        .post_json("/register", &json!({"nodes": []}))
        .await
        .expect("http post");
    assert_eq!(resp.status().as_u16(), 200);

    // TCP is bound and ping works
    assert!(
        ts.tcp_addr().is_some(),
        "TCP listener should be bound by default"
    );
    let mut c = ts.tcp_client().await.expect("tcp client");
    let pong = c.ping().await.expect("ping");
    assert!(pong.get("server_version").is_some());

    ts.shutdown().await.expect("shutdown");
}

// ─── Criterion 2: frame codec round-trip ──────────────────────────────────────

#[tokio::test]
async fn criterion_2_frame_codec_roundtrip() {
    // Belt-and-suspenders check in integration-test land. The full proptest
    // (256 cases, arbitrary op/ct/payload) lives in beava-core::wire::tests.
    let f = Frame {
        op: OP_REGISTER,
        content_type: CT_JSON,
        payload: Bytes::from(vec![1u8, 2, 3, 4, 5]),
    };
    let mut buf = BytesMut::new();
    encode_frame(&f, &mut buf);
    let out = decode_frame(&mut buf, 16 * 1024 * 1024).unwrap().unwrap();
    assert_eq!(out, f);
    assert_eq!(buf.len(), 0, "buf fully drained");
}

// ─── Criterion 3: ping returns server_version + registry_version ──────────────

#[tokio::test]
async fn criterion_3_ping_returns_registry_version() {
    let ts = TestServer::spawn().await.expect("spawn");
    let mut c = ts.tcp_client().await.expect("tcp client");

    let pong = c.ping().await.expect("ping");
    assert_eq!(pong["server_version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(pong["registry_version"], 0);

    // After a successful register, registry_version bumps.
    ts.post_json("/register", &valid_event_payload())
        .await
        .expect("http register");
    let pong2 = c.ping().await.expect("ping 2");
    assert_eq!(pong2["registry_version"], 1);

    ts.shutdown().await.expect("shutdown");
}

// ─── Criterion 4: TCP-register equivalent to HTTP-register ────────────────────

#[tokio::test]
async fn criterion_4_tcp_register_equivalent_to_http_register() {
    // Two fresh servers, identical body. Both start at v=0, so responses are
    // expected to be byte-structurally equal.
    let ts_http = TestServer::spawn().await.expect("spawn http");
    let ts_tcp = TestServer::spawn().await.expect("spawn tcp");

    let body = valid_event_payload();

    let http_resp = ts_http
        .post_json("/register", &body)
        .await
        .expect("http post");
    assert_eq!(http_resp.status().as_u16(), 200);
    let http_body: Value = http_resp.json().await.expect("http json");

    let mut c = ts_tcp.tcp_client().await.expect("tcp client");
    let (op, tcp_body) = c.register_json(body).await.expect("tcp register");
    assert_eq!(op, OP_REGISTER);

    assert_eq!(
        http_body, tcp_body,
        "TCP response should equal HTTP response structurally (both fresh servers, same body)"
    );

    ts_http.shutdown().await.expect("http shutdown");
    ts_tcp.shutdown().await.expect("tcp shutdown");
}

// ─── Criterion 5: unimplemented opcode → op_not_implemented; conn stays open ──

#[tokio::test]
async fn criterion_5_unimplemented_opcode_returns_error_connection_stays_open() {
    let ts = TestServer::spawn().await.expect("spawn");
    let mut c = ts.tcp_client().await.expect("tcp client");

    // Reserved opcode (OP_PUSH is reserved for Phase 6)
    let resp = c
        .send_raw(OP_PUSH, CT_JSON, Bytes::new())
        .await
        .expect("send push");
    assert_eq!(resp.op, OP_ERROR_RESPONSE);
    let body: Value = serde_json::from_slice(&resp.payload).unwrap();
    assert_eq!(body["error"]["code"], "op_not_implemented");
    let msg = body["error"]["message"].as_str().unwrap();
    assert!(msg.contains("push"), "msg: {msg}");
    assert!(msg.contains("Phase 6"), "msg: {msg}");

    // Connection still works — send a ping
    let pong = c.ping().await.expect("ping after error");
    assert!(pong.get("server_version").is_some());

    // Unknown opcode also keeps connection alive
    let resp = c
        .send_raw(0x4242, CT_JSON, Bytes::new())
        .await
        .expect("send unknown");
    assert_eq!(resp.op, OP_ERROR_RESPONSE);
    let body: Value = serde_json::from_slice(&resp.payload).unwrap();
    assert_eq!(body["error"]["code"], "unknown_op");

    let pong = c.ping().await.expect("ping after unknown op");
    assert!(pong.get("server_version").is_some());

    ts.shutdown().await.expect("shutdown");
}

// ─── Criterion 6: pipelined registers return in order ─────────────────────────

#[tokio::test]
async fn criterion_6_pipelined_registers_return_in_order() {
    let ts = TestServer::spawn().await.expect("spawn");
    let mut c = ts.tcp_client().await.expect("tcp client");

    // Write 3 register frames without awaiting responses.
    for name in ["EventA", "EventB", "EventC"] {
        let body = serde_json::to_vec(&json!({"nodes": [event_node_named(name)]})).unwrap();
        c.write_frame(&Frame {
            op: OP_REGISTER,
            content_type: CT_JSON,
            payload: Bytes::from(body),
        })
        .await
        .expect("write");
    }

    // Read three responses in order; version bumps monotonically.
    let frames = c.read_n_frames(3).await.expect("read 3");
    let expectations = [("EventA", 1u64), ("EventB", 2), ("EventC", 3)];
    for (i, (name, expected_version)) in expectations.iter().enumerate() {
        assert_eq!(frames[i].op, OP_REGISTER);
        let body: Value = serde_json::from_slice(&frames[i].payload).unwrap();
        assert_eq!(body["status"], "ok");
        assert_eq!(body["registry_version"], *expected_version);
        assert_eq!(body["added"][0], *name);
    }

    ts.shutdown().await.expect("shutdown");
}

// ─── Criterion 7: oversize frame → frame_too_large + connection closes ────────

#[tokio::test]
async fn criterion_7_oversize_frame_closes_connection() {
    let ts = TestServerBuilder::new()
        .tcp_max_frame_bytes(1024)
        .spawn()
        .await
        .expect("spawn");

    let tcp_addr = ts.tcp_addr().unwrap();
    let stream = tokio::net::TcpStream::connect(tcp_addr).await.unwrap();
    let (mut read_half, mut write_half) = stream.into_split();

    // Declared length = 9999 (way over 1024 + 3 limit). We only need to write
    // enough bytes for the decoder to see the length prefix.
    let mut bogus = BytesMut::new();
    bogus.extend_from_slice(&9999u32.to_be_bytes());
    bogus.extend_from_slice(&[0u8, 0, 1]); // op + ct (dummy)
    write_half.write_all(&bogus).await.unwrap();

    // Read error frame
    let mut read_buf = BytesMut::new();
    let err_frame = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(f) = decode_frame(&mut read_buf, 16 * 1024 * 1024).unwrap() {
                return f;
            }
            let n = read_half.read_buf(&mut read_buf).await.unwrap();
            if n == 0 {
                panic!("server closed before error frame");
            }
        }
    })
    .await
    .expect("read frame within 2s");

    assert_eq!(err_frame.op, OP_ERROR_RESPONSE);
    let body: Value = serde_json::from_slice(&err_frame.payload).unwrap();
    assert_eq!(body["error"]["code"], "frame_too_large");
    assert_eq!(body["error"]["limit"], 1024u64 + 3);

    // Next read must observe EOF within 500ms.
    let n = tokio::time::timeout(
        Duration::from_millis(500),
        read_half.read_buf(&mut read_buf),
    )
    .await
    .expect("eof within 500ms")
    .expect("read");
    assert_eq!(n, 0, "server should close after frame_too_large");

    drop(write_half);
    drop(read_half);
    ts.shutdown().await.expect("shutdown");
}

// ─── Criterion 8: graceful shutdown drains in-flight handlers ─────────────────

#[tokio::test]
async fn criterion_8_graceful_shutdown_drains_inflight() {
    let ts = TestServer::spawn().await.expect("spawn");
    let tcp_addr = ts.tcp_addr().unwrap();
    let mut c = TcpClient::connect(tcp_addr).await.expect("connect");

    // Write a register frame.
    let payload = serde_json::to_vec(&valid_event_payload()).unwrap();
    c.write_frame(&Frame {
        op: OP_REGISTER,
        content_type: CT_JSON,
        payload: Bytes::from(payload),
    })
    .await
    .expect("write register");

    // Give the server a tiny window to start processing the register before
    // we trigger shutdown. This makes the test deterministic: the handler is
    // now past the outer select guard and is inside dispatch.await (which does
    // not observe cancel until it finishes). The server MUST then complete
    // the response before closing.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Kick off shutdown.
    let shutdown_task = tokio::spawn(async move { ts.shutdown().await });

    // The register response should still arrive before the connection closes.
    let resp = tokio::time::timeout(Duration::from_secs(2), c.read_one_frame())
        .await
        .expect("read within 2s")
        .expect("decode");
    assert_eq!(
        resp.op, OP_REGISTER,
        "register response should arrive before shutdown"
    );
    let body: Value = serde_json::from_slice(&resp.payload).unwrap();
    assert_eq!(body["status"], "ok");

    // After that, next read should EOF.
    let eof = tokio::time::timeout(
        Duration::from_secs(2),
        c.read_or_eof(Duration::from_secs(1)),
    )
    .await
    .expect("eof-or-frame within 2s")
    .unwrap();
    assert!(eof.is_none(), "server should close after drain");

    shutdown_task.await.expect("join").expect("shutdown ok");
}

// ─── Bonus: HTTP-only mode still works (tcp.enabled=false) ────────────────────

#[tokio::test]
async fn bonus_tcp_disabled_leaves_http_working() {
    let ts = TestServerBuilder::new()
        .tcp_enabled(false)
        .spawn()
        .await
        .expect("spawn");

    assert!(ts.tcp_addr().is_none(), "TCP should not be bound");

    let resp = ts
        .post_json("/register", &valid_event_payload())
        .await
        .expect("http post");
    assert_eq!(resp.status().as_u16(), 200);
    let body: Value = resp.json().await.expect("json");
    assert_eq!(body["registry_version"], 1);

    ts.shutdown().await.expect("shutdown");
}

// ─── Bonus: MessagePack content-type returns unsupported, conn stays open ─────

#[tokio::test]
async fn bonus_msgpack_register_returns_unsupported_ct_connection_stays_open() {
    let ts = TestServer::spawn().await.expect("spawn");
    let mut c = ts.tcp_client().await.expect("tcp client");

    let resp = c
        .send_raw(OP_REGISTER, CT_MSGPACK, Bytes::new())
        .await
        .expect("send msgpack");
    assert_eq!(resp.op, OP_ERROR_RESPONSE);
    let body: Value = serde_json::from_slice(&resp.payload).unwrap();
    assert_eq!(body["error"]["code"], "unsupported_content_type");

    // Connection still works
    let pong = c.ping().await.expect("ping");
    assert_eq!(pong["registry_version"], 0);

    ts.shutdown().await.expect("shutdown");
}
