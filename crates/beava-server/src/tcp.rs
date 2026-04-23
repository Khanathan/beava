//! TCP wire listener + accept loop + per-connection handler dispatch (Phase 2.5).
//!
//! Accept model: one tokio task spawns per connection on the SAME `current_thread`
//! runtime that drives HTTP. Each connection runs a strict-FIFO loop:
//! read one frame → dispatch → write response frame → repeat.
//! Pipelining works because the client can send N frames without waiting;
//! the server reads/dispatches/writes one at a time, preserving order
//! (Redis RESP model).
//!
//! Error handling:
//! - `op_not_implemented` / `unknown_op` / `unsupported_content_type` →
//!   write error frame; connection stays open (SDK may pipeline many ops).
//! - `frame_too_large` / `malformed_frame` → write error frame, then close.

use anyhow::Context;
use beava_core::registry::Registry;
use beava_core::wire::{
    decode_frame, encode_frame, opcode_name, reserved_phase, Frame, FrameError, CT_JSON,
    CT_MSGPACK, OP_ERROR_RESPONSE, OP_PING, OP_REGISTER,
};
use bytes::{Bytes, BytesMut};
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::register::{execute_register, RegisterOutcome, RegisterPayload};

/// Handle to a bound TCP listener plus its max_frame_bytes for the accept loop.
pub struct TcpListenerHandle {
    listener: TcpListener,
    local_addr: SocketAddr,
    max_frame_bytes: u32,
}

impl std::fmt::Debug for TcpListenerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpListenerHandle")
            .field("local_addr", &self.local_addr)
            .field("max_frame_bytes", &self.max_frame_bytes)
            .finish()
    }
}

impl TcpListenerHandle {
    /// Bind a TCP listener on (host, port). Port 0 asks the OS for an ephemeral port.
    pub async fn bind(host: &str, port: u16, max_frame_bytes: u32) -> Result<Self, std::io::Error> {
        let addr_str = format!("{}:{}", host, port);
        let addr: SocketAddr = addr_str
            .parse()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        tracing::info!(
            target: "beava.tcp",
            kind = "tcp.listener_bound",
            addr = %local_addr,
            max_frame_bytes,
            "TCP listener bound"
        );
        Ok(Self {
            listener,
            local_addr,
            max_frame_bytes,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

/// Accept-loop task body. Runs until `cancel` is tripped, then drains in-flight
/// per-connection tasks.
pub(crate) async fn accept_loop(
    handle: TcpListenerHandle,
    registry: Arc<Registry>,
    cancel: CancellationToken,
) {
    let TcpListenerHandle {
        listener,
        local_addr: _,
        max_frame_bytes,
    } = handle;

    let connections_tracker = TaskTracker::new();

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                tracing::info!(target: "beava.tcp", "TCP accept loop cancelled; draining");
                break;
            }
            accept_res = listener.accept() => {
                match accept_res {
                    Ok((stream, peer)) => {
                        tracing::info!(
                            target: "beava.tcp",
                            kind = "tcp.connection_accepted",
                            peer = %peer,
                            "TCP connection accepted"
                        );
                        let reg = Arc::clone(&registry);
                        let cancel_child = cancel.clone();
                        let mfb = max_frame_bytes;
                        connections_tracker.spawn(async move {
                            if let Err(e) = handle_connection(stream, reg, cancel_child, mfb).await {
                                tracing::warn!(
                                    target: "beava.tcp",
                                    kind = "tcp.handler_error",
                                    error = %e,
                                    "connection handler exited with error"
                                );
                            }
                        });
                    }
                    Err(e) => {
                        tracing::warn!(
                            target: "beava.tcp",
                            kind = "tcp.accept_error",
                            error = %e,
                            "accept failed; continuing"
                        );
                    }
                }
            }
        }
    }

    // Drain: stop accepting new tasks, wait for in-flight to finish.
    connections_tracker.close();
    connections_tracker.wait().await;
    tracing::info!(target: "beava.tcp", "TCP accept loop drained");
}

/// Per-connection read→dispatch→write loop. Strict FIFO.
pub(crate) async fn handle_connection<S>(
    mut stream: S,
    registry: Arc<Registry>,
    cancel: CancellationToken,
    max_frame_bytes: u32,
) -> anyhow::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut read_buf = BytesMut::with_capacity(8 * 1024);
    let mut write_buf = BytesMut::with_capacity(8 * 1024);

    loop {
        // Try to decode a frame already in the buffer.
        match decode_frame(&mut read_buf, max_frame_bytes) {
            Ok(Some(frame)) => {
                tracing::trace!(
                    target: "beava.tcp",
                    kind = "tcp.frame_received",
                    op = format!("{:#06x}", frame.op),
                    content_type = format!("{:#04x}", frame.content_type),
                    payload_len = frame.payload.len(),
                    "frame received"
                );
                let response = dispatch(&registry, frame).await;
                write_buf.clear();
                encode_frame(&response, &mut write_buf);
                stream
                    .write_all(&write_buf)
                    .await
                    .context("write response frame")?;
                // Loop: try another frame from the buffer first.
                continue;
            }
            Ok(None) => {
                // Need more bytes — fall through to read.
            }
            Err(FrameError::TooLarge {
                declared_len,
                limit,
            }) => {
                tracing::warn!(
                    target: "beava.tcp",
                    kind = "tcp.frame_error",
                    error = "too_large",
                    declared_len,
                    limit,
                    "frame too large; writing error and closing connection"
                );
                let err_frame = build_error_frame(
                    &registry,
                    "frame_too_large",
                    json!({"limit": limit, "declared": declared_len}),
                );
                write_buf.clear();
                encode_frame(&err_frame, &mut write_buf);
                let _ = stream.write_all(&write_buf).await;
                let _ = stream.shutdown().await;
                tracing::debug!(
                    target: "beava.tcp",
                    kind = "tcp.connection_closed",
                    reason = "frame_too_large"
                );
                return Ok(());
            }
            Err(FrameError::LengthUnderflow { declared_len }) => {
                tracing::warn!(
                    target: "beava.tcp",
                    kind = "tcp.frame_error",
                    error = "length_underflow",
                    declared_len,
                    "malformed frame; writing error and closing connection"
                );
                let err_frame = build_error_frame(
                    &registry,
                    "malformed_frame",
                    json!({
                        "reason": "declared length cannot cover op+content_type",
                        "declared": declared_len,
                    }),
                );
                write_buf.clear();
                encode_frame(&err_frame, &mut write_buf);
                let _ = stream.write_all(&write_buf).await;
                let _ = stream.shutdown().await;
                tracing::debug!(
                    target: "beava.tcp",
                    kind = "tcp.connection_closed",
                    reason = "malformed_frame"
                );
                return Ok(());
            }
        }

        // Read more bytes (or cancel).
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                tracing::debug!(
                    target: "beava.tcp",
                    kind = "tcp.connection_closed",
                    reason = "shutdown"
                );
                let _ = stream.shutdown().await;
                return Ok(());
            }
            read_res = stream.read_buf(&mut read_buf) => {
                match read_res {
                    Ok(0) => {
                        if !read_buf.is_empty() {
                            tracing::debug!(
                                target: "beava.tcp",
                                kind = "tcp.connection_closed",
                                reason = "truncated",
                                buffered_bytes = read_buf.len(),
                                "client closed mid-frame"
                            );
                        } else {
                            tracing::debug!(
                                target: "beava.tcp",
                                kind = "tcp.connection_closed",
                                reason = "client_close"
                            );
                        }
                        return Ok(());
                    }
                    Ok(_n) => { /* loop back and try to decode */ }
                    Err(e) => return Err(e).context("stream read"),
                }
            }
        }
    }
}

/// Map an inbound frame to an outbound frame.
async fn dispatch(registry: &Arc<Registry>, frame: Frame) -> Frame {
    match frame.op {
        OP_PING => handle_ping(registry, &frame).await,
        OP_REGISTER => handle_register(registry, &frame).await,
        op if reserved_phase(op).is_some() => {
            // Known but reserved (push / push_sync / … / mset).
            let name = opcode_name(op).unwrap_or("<unnamed>");
            let phase = reserved_phase(op).unwrap_or("<unknown>");
            build_error_frame(
                registry,
                "op_not_implemented",
                json!({
                    "message": format!("opcode {:#06x} ({}) reserved for {}", op, name, phase),
                }),
            )
        }
        op => {
            // Unknown opcode (not in the table at all).
            build_error_frame(
                registry,
                "unknown_op",
                json!({"message": format!("opcode {:#06x}", op)}),
            )
        }
    }
}

async fn handle_ping(registry: &Arc<Registry>, _frame: &Frame) -> Frame {
    let body = json!({
        "server_version": env!("CARGO_PKG_VERSION"),
        "registry_version": registry.version(),
    });
    let bytes = serde_json::to_vec(&body).expect("serialize ping");
    Frame {
        op: OP_PING,
        content_type: CT_JSON,
        payload: Bytes::from(bytes),
    }
}

async fn handle_register(registry: &Arc<Registry>, frame: &Frame) -> Frame {
    // Content-type policy: register requires JSON in v0.
    if frame.content_type == CT_MSGPACK {
        return build_error_frame(
            registry,
            "unsupported_content_type",
            json!({"reason": "MessagePack payload encoding ships in Phase 6 / 12"}),
        );
    }
    if frame.content_type != CT_JSON {
        return build_error_frame(
            registry,
            "unsupported_content_type",
            json!({"reason": format!("unknown content_type {:#04x}", frame.content_type)}),
        );
    }

    // Parse JSON → RegisterPayload
    let payload: RegisterPayload = match serde_json::from_slice(&frame.payload) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(
                target: "beava.tcp",
                kind = "register.parse_error",
                reason = %e,
                "malformed register payload over TCP"
            );
            return build_error_frame(
                registry,
                "invalid_registration",
                json!({"path": "<body>", "reason": e.to_string()}),
            );
        }
    };

    let outcome = execute_register(registry, payload).await;
    match outcome {
        RegisterOutcome::Success {
            version,
            registered_descriptors,
            added,
            already_present,
        } => build_success_frame(
            OP_REGISTER,
            json!({
                "status": "ok",
                "registry_version": version,
                "registered_descriptors": registered_descriptors,
                "added": added,
                "already_present": already_present,
            }),
        ),
        RegisterOutcome::EmptyPayload { version } => build_success_frame(
            OP_REGISTER,
            json!({
                "status": "ok",
                "registry_version": version,
                "registered_descriptors": [],
                "added": [],
                "already_present": [],
            }),
        ),
        RegisterOutcome::Noop {
            version,
            registered_descriptors,
            already_present,
        } => build_success_frame(
            OP_REGISTER,
            json!({
                "status": "ok",
                "registry_version": version,
                "registered_descriptors": registered_descriptors,
                "added": [],
                "already_present": already_present,
            }),
        ),
        RegisterOutcome::ValidationFailed {
            version,
            first_error_code,
            first_error_path,
            first_error_reason,
            ..
        } => {
            let wire_code = crate::register::error_code_to_wire_str(first_error_code);
            Frame {
                op: OP_ERROR_RESPONSE,
                content_type: CT_JSON,
                payload: Bytes::from(
                    serde_json::to_vec(&json!({
                        "error": {
                            "code": wire_code,
                            "path": first_error_path,
                            "reason": first_error_reason,
                        },
                        "registry_version": version,
                    }))
                    .expect("serialize validation error"),
                ),
            }
        }
        RegisterOutcome::Conflict {
            version,
            added,
            changed,
        } => Frame {
            op: OP_ERROR_RESPONSE,
            content_type: CT_JSON,
            payload: Bytes::from(
                serde_json::to_vec(&json!({
                    "error": {
                        "code": "registration_conflict",
                        "message": "Registration would change or remove existing descriptors",
                        "diff": {"added": added, "removed": [], "changed": changed},
                    },
                    "registry_version": version,
                }))
                .expect("serialize conflict error"),
            ),
        },
    }
}

fn build_success_frame(op: u16, body: Value) -> Frame {
    Frame {
        op,
        content_type: CT_JSON,
        payload: Bytes::from(serde_json::to_vec(&body).expect("serialize success")),
    }
}

fn build_error_frame(registry: &Arc<Registry>, code: &str, extra: Value) -> Frame {
    let mut err_obj = json!({"code": code});
    if let Some(obj) = extra.as_object() {
        for (k, v) in obj {
            err_obj[k] = v.clone();
        }
    }
    let body = json!({
        "error": err_obj,
        "registry_version": registry.version(),
    });
    Frame {
        op: OP_ERROR_RESPONSE,
        content_type: CT_JSON,
        payload: Bytes::from(serde_json::to_vec(&body).expect("serialize error")),
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use beava_core::wire::{OP_PUSH, OP_PUSH_SYNC};
    use std::time::Duration;
    use tokio::net::TcpStream;

    // ─── TcpListenerHandle ────────────────────────────────────────────────────

    #[tokio::test]
    async fn tcp_listener_handle_binds_to_port_zero() {
        let h = TcpListenerHandle::bind("127.0.0.1", 0, 1024)
            .await
            .expect("bind");
        let addr = h.local_addr();
        assert_eq!(addr.ip().to_string(), "127.0.0.1");
        assert_ne!(addr.port(), 0);
        // Verify it accepts a connection.
        let _stream = TcpStream::connect(addr).await.expect("connect");
        // Don't bother accepting; we just proved the listener is live.
    }

    #[tokio::test]
    async fn tcp_listener_handle_bind_invalid_host_fails() {
        let err = TcpListenerHandle::bind("not.a.host", 0, 1024).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn accept_loop_drains_on_cancel() {
        let reg = Arc::new(Registry::new());
        let h = TcpListenerHandle::bind("127.0.0.1", 0, 4096)
            .await
            .expect("bind");
        let addr = h.local_addr();
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let task = tokio::spawn(accept_loop(h, reg, cancel_clone));

        // Open one connection
        let _conn = TcpStream::connect(addr).await.expect("connect");

        // Cancel; accept_loop should drain and return
        cancel.cancel();
        tokio::time::timeout(Duration::from_millis(500), task)
            .await
            .expect("drain within 500ms")
            .expect("task join");
    }

    // ─── Helper: run one frame via tokio::io::duplex ──────────────────────────

    async fn run_one_frame(req: Frame, registry: Arc<Registry>, max_frame_bytes: u32) -> Frame {
        let (client, server) = tokio::io::duplex(64 * 1024);
        let cancel = CancellationToken::new();
        let handler = tokio::spawn(handle_connection(
            server,
            registry,
            cancel.clone(),
            max_frame_bytes,
        ));

        // Write request
        let mut client = client;
        let mut buf = BytesMut::new();
        encode_frame(&req, &mut buf);
        client.write_all(&buf).await.expect("write req");

        // Read response
        let mut read_buf = BytesMut::with_capacity(8 * 1024);
        let response = loop {
            if let Some(f) = decode_frame(&mut read_buf, 16 * 1024 * 1024).expect("decode") {
                break f;
            }
            let n = client.read_buf(&mut read_buf).await.expect("read");
            if n == 0 {
                panic!("server closed before sending response");
            }
        };

        // Close client; wait for handler to finish.
        drop(client);
        let _ = tokio::time::timeout(Duration::from_secs(1), handler).await;
        response
    }

    fn event_node_json(name: &str, fields: &[(&str, &str)], etf: &str) -> serde_json::Value {
        let fields_map: serde_json::Map<String, serde_json::Value> = fields
            .iter()
            .map(|(k, v)| (k.to_string(), serde_json::Value::String(v.to_string())))
            .collect();
        serde_json::json!({
            "kind": "event",
            "name": name,
            "schema": {"fields": fields_map, "optional_fields": []},
            "event_time_field": etf,
        })
    }

    fn valid_register_body() -> Vec<u8> {
        let payload = serde_json::json!({
            "nodes": [event_node_json(
                "Transaction",
                &[("event_time", "i64"), ("amount", "f64")],
                "event_time"
            )]
        });
        serde_json::to_vec(&payload).unwrap()
    }

    // ─── Handlers ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn handle_ping_returns_server_and_registry_version() {
        let reg = Arc::new(Registry::new());
        let req = Frame::new(OP_PING, CT_JSON, Bytes::new());
        let resp = run_one_frame(req, reg, 1024 * 1024).await;
        assert_eq!(resp.op, OP_PING);
        assert_eq!(resp.content_type, CT_JSON);
        let body: Value = serde_json::from_slice(&resp.payload).unwrap();
        assert_eq!(body["server_version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(body["registry_version"], 0);
    }

    #[tokio::test]
    async fn handle_register_valid_event_matches_http_shape() {
        let reg = Arc::new(Registry::new());
        let req = Frame::new(OP_REGISTER, CT_JSON, Bytes::from(valid_register_body()));
        let resp = run_one_frame(req, reg.clone(), 1024 * 1024).await;
        assert_eq!(resp.op, OP_REGISTER);
        assert_eq!(resp.content_type, CT_JSON);
        let body: Value = serde_json::from_slice(&resp.payload).unwrap();
        assert_eq!(body["status"], "ok");
        assert_eq!(body["registry_version"], 1);
        assert_eq!(body["added"], serde_json::json!(["Transaction"]));
        assert_eq!(reg.version(), 1);
    }

    #[tokio::test]
    async fn handle_register_validation_failure_uses_error_response_opcode() {
        let reg = Arc::new(Registry::new());
        let bad = serde_json::json!({
            "nodes": [{
                "kind": "event",
                "name": "A",
                "schema": {"fields": {"x": "f64"}, "optional_fields": []},
                "event_time_field": "ts"  // not in schema
            }]
        });
        let req = Frame::new(
            OP_REGISTER,
            CT_JSON,
            Bytes::from(serde_json::to_vec(&bad).unwrap()),
        );
        let resp = run_one_frame(req, reg, 1024 * 1024).await;
        assert_eq!(resp.op, OP_ERROR_RESPONSE);
        let body: Value = serde_json::from_slice(&resp.payload).unwrap();
        assert_eq!(body["error"]["code"], "invalid_registration");
        assert!(!body["error"]["path"].as_str().unwrap().is_empty());
    }

    #[tokio::test]
    async fn handle_register_conflict_uses_error_response_opcode() {
        let reg = Arc::new(Registry::new());

        // First register: valid
        let payload = serde_json::json!({
            "nodes": [event_node_json("A", &[("event_time", "i64"), ("amount", "f64")], "event_time")]
        });
        let req = Frame::new(
            OP_REGISTER,
            CT_JSON,
            Bytes::from(serde_json::to_vec(&payload).unwrap()),
        );
        let _ = run_one_frame(req, reg.clone(), 1024 * 1024).await;

        // Second register: conflicting schema
        let payload2 = serde_json::json!({
            "nodes": [event_node_json("A", &[("event_time", "i64"), ("amount", "i64")], "event_time")]
        });
        let req2 = Frame::new(
            OP_REGISTER,
            CT_JSON,
            Bytes::from(serde_json::to_vec(&payload2).unwrap()),
        );
        let resp = run_one_frame(req2, reg, 1024 * 1024).await;
        assert_eq!(resp.op, OP_ERROR_RESPONSE);
        let body: Value = serde_json::from_slice(&resp.payload).unwrap();
        assert_eq!(body["error"]["code"], "registration_conflict");
        assert!(!body["error"]["diff"]["changed"]
            .as_array()
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn handle_register_msgpack_content_type_returns_unsupported() {
        let reg = Arc::new(Registry::new());
        let req = Frame::new(OP_REGISTER, CT_MSGPACK, Bytes::new());
        let resp = run_one_frame(req, reg.clone(), 1024 * 1024).await;
        assert_eq!(resp.op, OP_ERROR_RESPONSE);
        let body: Value = serde_json::from_slice(&resp.payload).unwrap();
        assert_eq!(body["error"]["code"], "unsupported_content_type");
        assert!(body["error"]["reason"]
            .as_str()
            .unwrap()
            .to_lowercase()
            .contains("messagepack"));
        assert_eq!(reg.version(), 0);
    }

    #[tokio::test]
    async fn handle_register_malformed_json_returns_invalid_registration() {
        let reg = Arc::new(Registry::new());
        let req = Frame::new(OP_REGISTER, CT_JSON, Bytes::from(b"{\"nodes\": [".to_vec()));
        let resp = run_one_frame(req, reg, 1024 * 1024).await;
        assert_eq!(resp.op, OP_ERROR_RESPONSE);
        let body: Value = serde_json::from_slice(&resp.payload).unwrap();
        assert_eq!(body["error"]["code"], "invalid_registration");
        assert_eq!(body["error"]["path"], "<body>");
    }

    #[tokio::test]
    async fn reserved_opcode_returns_op_not_implemented() {
        let reg = Arc::new(Registry::new());
        let req = Frame::new(OP_PUSH, CT_JSON, Bytes::new());
        let resp = run_one_frame(req, reg, 1024 * 1024).await;
        assert_eq!(resp.op, OP_ERROR_RESPONSE);
        let body: Value = serde_json::from_slice(&resp.payload).unwrap();
        assert_eq!(body["error"]["code"], "op_not_implemented");
        let msg = body["error"]["message"].as_str().unwrap();
        assert!(msg.contains("push"));
        assert!(msg.contains("Phase 6"));
    }

    #[tokio::test]
    async fn reserved_opcode_push_sync_phase_12() {
        let reg = Arc::new(Registry::new());
        let req = Frame::new(OP_PUSH_SYNC, CT_JSON, Bytes::new());
        let resp = run_one_frame(req, reg, 1024 * 1024).await;
        assert_eq!(resp.op, OP_ERROR_RESPONSE);
        let body: Value = serde_json::from_slice(&resp.payload).unwrap();
        assert_eq!(body["error"]["code"], "op_not_implemented");
        let msg = body["error"]["message"].as_str().unwrap();
        assert!(msg.contains("push_sync"));
        assert!(msg.contains("Phase 12"));
    }

    #[tokio::test]
    async fn unknown_opcode_returns_unknown_op() {
        let reg = Arc::new(Registry::new());
        let req = Frame::new(0x4242, CT_JSON, Bytes::new());
        let resp = run_one_frame(req, reg, 1024 * 1024).await;
        assert_eq!(resp.op, OP_ERROR_RESPONSE);
        let body: Value = serde_json::from_slice(&resp.payload).unwrap();
        assert_eq!(body["error"]["code"], "unknown_op");
        assert!(body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("0x4242"));
    }

    // ─── Frame-level errors ───────────────────────────────────────────────────

    #[tokio::test]
    async fn frame_too_large_returns_error_and_closes_connection() {
        let reg = Arc::new(Registry::new());
        let (client, server) = tokio::io::duplex(64 * 1024);
        let cancel = CancellationToken::new();
        let handler = tokio::spawn(handle_connection(server, reg, cancel.clone(), 1024));

        // Send a frame with declared_len = 9999 (oversized).
        let mut client = client;
        let mut bogus = BytesMut::new();
        bogus.extend_from_slice(&9999u32.to_be_bytes());
        bogus.extend_from_slice(&[0u8; 7]); // enough bytes to start decoding
        client.write_all(&bogus).await.expect("write bogus");

        // Read error frame
        let mut read_buf = BytesMut::new();
        let err_frame = loop {
            if let Some(f) = decode_frame(&mut read_buf, 16 * 1024 * 1024).expect("decode") {
                break f;
            }
            let n = client.read_buf(&mut read_buf).await.expect("read");
            if n == 0 {
                panic!("server closed before error frame");
            }
        };
        assert_eq!(err_frame.op, OP_ERROR_RESPONSE);
        let body: Value = serde_json::from_slice(&err_frame.payload).unwrap();
        assert_eq!(body["error"]["code"], "frame_too_large");

        // Next read should return EOF (server closed).
        let n = tokio::time::timeout(Duration::from_millis(500), client.read_buf(&mut read_buf))
            .await
            .expect("read within 500ms")
            .expect("read");
        assert_eq!(n, 0, "server should close after frame_too_large");

        drop(client);
        let _ = tokio::time::timeout(Duration::from_secs(1), handler).await;
    }

    #[tokio::test]
    async fn malformed_frame_length_underflow_closes_connection() {
        let reg = Arc::new(Registry::new());
        let (client, server) = tokio::io::duplex(64 * 1024);
        let cancel = CancellationToken::new();
        let handler = tokio::spawn(handle_connection(server, reg, cancel.clone(), 1024));

        // declared_len = 2 (underflow)
        let mut client = client;
        let mut bogus = BytesMut::new();
        bogus.extend_from_slice(&2u32.to_be_bytes());
        bogus.extend_from_slice(&[0u8, 0]);
        client.write_all(&bogus).await.expect("write");

        let mut read_buf = BytesMut::new();
        let err_frame = loop {
            if let Some(f) = decode_frame(&mut read_buf, 16 * 1024 * 1024).expect("decode") {
                break f;
            }
            let n = client.read_buf(&mut read_buf).await.expect("read");
            if n == 0 {
                panic!("server closed too early");
            }
        };
        assert_eq!(err_frame.op, OP_ERROR_RESPONSE);
        let body: Value = serde_json::from_slice(&err_frame.payload).unwrap();
        assert_eq!(body["error"]["code"], "malformed_frame");

        let n = tokio::time::timeout(Duration::from_millis(500), client.read_buf(&mut read_buf))
            .await
            .expect("eof within 500ms")
            .expect("read");
        assert_eq!(n, 0);

        drop(client);
        let _ = tokio::time::timeout(Duration::from_secs(1), handler).await;
    }

    // ─── Pipelining ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn pipelined_three_pings_return_in_order() {
        let reg = Arc::new(Registry::new());
        let (client, server) = tokio::io::duplex(64 * 1024);
        let cancel = CancellationToken::new();
        let handler = tokio::spawn(handle_connection(server, reg, cancel.clone(), 1024 * 1024));

        // Write 3 pings
        let mut client = client;
        let mut buf = BytesMut::new();
        for _ in 0..3 {
            encode_frame(&Frame::new(OP_PING, CT_JSON, Bytes::new()), &mut buf);
        }
        client.write_all(&buf).await.expect("write pings");

        // Read 3 responses
        let mut read_buf = BytesMut::new();
        for _ in 0..3 {
            let resp = loop {
                if let Some(f) = decode_frame(&mut read_buf, 16 * 1024 * 1024).unwrap() {
                    break f;
                }
                let n = client.read_buf(&mut read_buf).await.unwrap();
                assert!(n > 0);
            };
            assert_eq!(resp.op, OP_PING);
        }

        drop(client);
        let _ = tokio::time::timeout(Duration::from_secs(1), handler).await;
    }

    #[tokio::test]
    async fn pipelined_two_registers_serial() {
        let reg = Arc::new(Registry::new());
        let (client, server) = tokio::io::duplex(64 * 1024);
        let cancel = CancellationToken::new();
        let handler = tokio::spawn(handle_connection(
            server,
            reg.clone(),
            cancel.clone(),
            1024 * 1024,
        ));

        let a = serde_json::json!({
            "nodes": [event_node_json("A", &[("event_time", "i64"), ("x", "f64")], "event_time")]
        });
        let ab = serde_json::json!({
            "nodes": [
                event_node_json("A", &[("event_time", "i64"), ("x", "f64")], "event_time"),
                event_node_json("B", &[("event_time", "i64"), ("y", "f64")], "event_time"),
            ]
        });

        let mut client = client;
        let mut buf = BytesMut::new();
        encode_frame(
            &Frame::new(
                OP_REGISTER,
                CT_JSON,
                Bytes::from(serde_json::to_vec(&a).unwrap()),
            ),
            &mut buf,
        );
        encode_frame(
            &Frame::new(
                OP_REGISTER,
                CT_JSON,
                Bytes::from(serde_json::to_vec(&ab).unwrap()),
            ),
            &mut buf,
        );
        client.write_all(&buf).await.unwrap();

        let mut read_buf = BytesMut::new();
        let resp_a = loop {
            if let Some(f) = decode_frame(&mut read_buf, 16 * 1024 * 1024).unwrap() {
                break f;
            }
            let n = client.read_buf(&mut read_buf).await.unwrap();
            assert!(n > 0);
        };
        let body_a: Value = serde_json::from_slice(&resp_a.payload).unwrap();
        assert_eq!(body_a["registry_version"], 1);
        assert_eq!(body_a["added"], serde_json::json!(["A"]));

        let resp_ab = loop {
            if let Some(f) = decode_frame(&mut read_buf, 16 * 1024 * 1024).unwrap() {
                break f;
            }
            let n = client.read_buf(&mut read_buf).await.unwrap();
            assert!(n > 0);
        };
        let body_ab: Value = serde_json::from_slice(&resp_ab.payload).unwrap();
        assert_eq!(body_ab["registry_version"], 2);
        assert_eq!(body_ab["added"], serde_json::json!(["B"]));
        assert_eq!(body_ab["already_present"], serde_json::json!(["A"]));

        drop(client);
        let _ = tokio::time::timeout(Duration::from_secs(1), handler).await;
    }

    // ─── Connection lifecycle ─────────────────────────────────────────────────

    #[tokio::test]
    async fn client_eof_between_frames_returns_ok() {
        let reg = Arc::new(Registry::new());
        let (client, server) = tokio::io::duplex(64 * 1024);
        let cancel = CancellationToken::new();
        let handler = tokio::spawn(handle_connection(server, reg, cancel.clone(), 1024 * 1024));

        let mut client = client;
        let mut buf = BytesMut::new();
        encode_frame(&Frame::new(OP_PING, CT_JSON, Bytes::new()), &mut buf);
        client.write_all(&buf).await.unwrap();

        // Read the response then drop the client.
        let mut read_buf = BytesMut::new();
        let _resp = loop {
            if let Some(f) = decode_frame(&mut read_buf, 16 * 1024 * 1024).unwrap() {
                break f;
            }
            let n = client.read_buf(&mut read_buf).await.unwrap();
            assert!(n > 0);
        };
        drop(client);
        let result = tokio::time::timeout(Duration::from_millis(500), handler)
            .await
            .expect("handler exit within 500ms")
            .expect("join");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn cancel_during_idle_connection_closes() {
        let reg = Arc::new(Registry::new());
        let (_client, server) = tokio::io::duplex(64 * 1024);
        let cancel = CancellationToken::new();
        let handler = tokio::spawn(handle_connection(server, reg, cancel.clone(), 1024 * 1024));

        cancel.cancel();
        let result = tokio::time::timeout(Duration::from_millis(500), handler)
            .await
            .expect("handler exit within 500ms")
            .expect("join");
        assert!(result.is_ok());
    }
}
