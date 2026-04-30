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
    CT_MSGPACK, OP_ERROR_RESPONSE, OP_PING, OP_PUSH, OP_REGISTER,
};
use bytes::{Bytes, BytesMut};
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::push::{execute_push, PushOutcome};
use crate::register::{execute_register, RegisterOutcome, RegisterPayload};
use crate::AppState;

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

/// Accept-loop task body — registry-only entry point (used by ping/register
/// unit tests that don't need the full WAL/apply pipeline). Production
/// servers use `accept_loop_with_app` so OP_PUSH works.
#[allow(dead_code)]
pub(crate) async fn accept_loop(
    handle: TcpListenerHandle,
    registry: Arc<Registry>,
    cancel: CancellationToken,
) {
    accept_loop_inner(handle, registry, None, cancel).await
}

/// Accept-loop task body — full AppState entry point (Phase 8+). Routes
/// `OP_PUSH` frames through the shared `execute_push` function so both
/// transports honor the same WAL fsync + idem-cache + apply-loop semantics.
pub async fn accept_loop_with_app(
    handle: TcpListenerHandle,
    app: Arc<AppState>,
    cancel: CancellationToken,
) {
    let registry = Arc::clone(&app.dev_agg.registry);
    accept_loop_inner(handle, registry, Some(app), cancel).await
}

async fn accept_loop_inner(
    handle: TcpListenerHandle,
    registry: Arc<Registry>,
    app: Option<Arc<AppState>>,
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
                        let app_clone = app.clone();
                        let cancel_child = cancel.clone();
                        let mfb = max_frame_bytes;
                        connections_tracker.spawn(async move {
                            if let Err(e) = handle_connection_inner(stream, reg, app_clone, cancel_child, mfb).await {
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

/// Per-connection read→dispatch→write loop. Strict FIFO. Registry-only entry
/// (test-only — production goes through `accept_loop_with_app`).
#[allow(dead_code)]
pub(crate) async fn handle_connection<S>(
    stream: S,
    registry: Arc<Registry>,
    cancel: CancellationToken,
    max_frame_bytes: u32,
) -> anyhow::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    handle_connection_inner(stream, registry, None, cancel, max_frame_bytes).await
}

async fn handle_connection_inner<S>(
    mut stream: S,
    registry: Arc<Registry>,
    app: Option<Arc<AppState>>,
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
                let response = dispatch(&registry, app.as_ref(), frame).await;
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
async fn dispatch(registry: &Arc<Registry>, app: Option<&Arc<AppState>>, frame: Frame) -> Frame {
    match frame.op {
        OP_PING => handle_ping(registry, &frame).await,
        OP_REGISTER => handle_register(registry, &frame).await,
        OP_PUSH => match app {
            Some(a) => handle_push(a, &frame).await,
            None => build_error_frame(
                registry,
                "op_not_implemented",
                json!({
                    "message": "opcode 0x0010 (push) requires AppState (use accept_loop_with_app)"
                }),
            ),
        },
        op if reserved_phase(op).is_some() => {
            // Known but reserved (push_sync / push_many / … / mset).
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

/// Handle an OP_PUSH frame.
///
/// Wire format (CT_JSON in v0; MessagePack reserved): the payload is a JSON
/// object `{"event": "<name>", "body": {...event fields...}}`. Routes through
/// the shared `execute_push` for parity with the HTTP `POST /push/{event}`
/// path. On success returns an OP_PUSH frame with `PushAck` body; on dedupe
/// replay sets `idempotent_replay: true` and returns the cached body. Errors
/// emit OP_ERROR_RESPONSE with the same code strings the HTTP path uses.
async fn handle_push(app: &Arc<AppState>, frame: &Frame) -> Frame {
    let registry = &app.dev_agg.registry;

    // Content-type policy: JSON only in v0 (mirrors HTTP).
    if frame.content_type == CT_MSGPACK {
        return build_error_frame(
            registry,
            "unsupported_content_type",
            json!({"reason": "MessagePack payload encoding ships in Phase 12"}),
        );
    }
    if frame.content_type != CT_JSON {
        return build_error_frame(
            registry,
            "unsupported_content_type",
            json!({"reason": format!("unknown content_type {:#04x}", frame.content_type)}),
        );
    }

    // Parse the envelope: {"event": "...", "body": {...}}
    #[derive(serde::Deserialize)]
    struct PushEnvelope {
        event: String,
        body: serde_json::Value,
    }

    let envelope: PushEnvelope = match serde_json::from_slice(&frame.payload) {
        Ok(e) => e,
        Err(e) => {
            return build_error_frame(
                registry,
                "invalid_event",
                json!({"path": "<envelope>", "reason": e.to_string()}),
            );
        }
    };

    let body_bytes = match serde_json::to_vec(&envelope.body) {
        Ok(b) => b,
        Err(e) => {
            return build_error_frame(
                registry,
                "invalid_event",
                json!({"path": "<body>", "reason": e.to_string()}),
            );
        }
    };

    match execute_push(
        app,
        &envelope.event,
        &body_bytes,
        beava_persistence::SyncMode::Periodic,
    )
    .await
    {
        PushOutcome::Ok { response_bytes, .. } => Frame {
            op: OP_PUSH,
            content_type: CT_JSON,
            payload: response_bytes,
        },
        PushOutcome::IdempotentReplay {
            cached_response_bytes,
        } => {
            // Re-encode with idempotent_replay=true flag flipped. The cached
            // bytes come from the original ACK (which had
            // idempotent_replay=false), so we patch the JSON.
            let patched = match patch_idempotent_replay_flag(&cached_response_bytes) {
                Some(b) => b,
                None => cached_response_bytes,
            };
            Frame {
                op: OP_PUSH,
                content_type: CT_JSON,
                payload: patched,
            }
        }
        PushOutcome::Error {
            http_status: _,
            code,
            registry_version: _,
        } => build_error_frame(registry, code, json!({})),
    }
}

/// Re-serialize a `PushAck` JSON blob with `idempotent_replay: true`. Returns
/// `None` if the blob can't be parsed (defensive — caller falls back to the
/// raw cached bytes).
fn patch_idempotent_replay_flag(cached: &Bytes) -> Option<Bytes> {
    let mut v: Value = serde_json::from_slice(cached).ok()?;
    if let Some(obj) = v.as_object_mut() {
        obj.insert("idempotent_replay".to_string(), Value::Bool(true));
    }
    Some(Bytes::from(serde_json::to_vec(&v).ok()?))
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
        RegisterOutcome::WalUnavailable { version } => Frame {
            op: OP_ERROR_RESPONSE,
            content_type: CT_JSON,
            payload: Bytes::from(
                serde_json::to_vec(&json!({
                    "error": {
                        "code": "wal_unavailable",
                        "reason": "WAL append for registry bump failed; registry not mutated"
                    },
                    "registry_version": version,
                }))
                .expect("serialize wal_unavailable error"),
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

    fn event_node_json(name: &str, fields: &[(&str, &str)], _etf: &str) -> serde_json::Value {
        let fields_map: serde_json::Map<String, serde_json::Value> = fields
            .iter()
            .map(|(k, v)| (k.to_string(), serde_json::Value::String(v.to_string())))
            .collect();
        serde_json::json!({
            "kind": "event",
            "name": name,
            "schema": {"fields": fields_map, "optional_fields": []},
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
        // Plan 12.6-06 D-03 hard rip: pre-pivot this used a payload with a
        // missing event_time_field schema slot to trigger validation failure.
        // Post-pivot use a bad-pattern name instead — same goal: validator
        // emits an error, dispatch encodes via OP_ERROR_RESPONSE.
        let reg = Arc::new(Registry::new());
        let bad = serde_json::json!({
            "nodes": [{
                "kind": "event",
                "name": "1bad", // digit-leading: NameBadPattern
                "schema": {"fields": {"x": "f64"}, "optional_fields": []},
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
    async fn op_push_without_app_state_returns_op_not_implemented() {
        // Legacy `accept_loop` path (no AppState) cannot route OP_PUSH to the
        // apply loop. Production wires use `accept_loop_with_app` (see commit
        // 48e09fd, Phase 8 folded scope) which dispatches OP_PUSH through
        // `execute_push`. End-to-end success path lives in
        // `tests/phase8_tcp_push.rs`.
        let reg = Arc::new(Registry::new());
        let req = Frame::new(OP_PUSH, CT_JSON, Bytes::new());
        let resp = run_one_frame(req, reg, 1024 * 1024).await;
        assert_eq!(resp.op, OP_ERROR_RESPONSE);
        let body: Value = serde_json::from_slice(&resp.payload).unwrap();
        assert_eq!(body["error"]["code"], "op_not_implemented");
        let msg = body["error"]["message"].as_str().unwrap();
        assert!(msg.contains("push"));
        assert!(msg.contains("AppState"));
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

    // ─── Plan 05-04: Rule 11 aggregation validation tests (TCP) ──────────────

    fn table_node_json(name: &str, pk: &str) -> serde_json::Value {
        serde_json::json!({
            "kind": "table",
            "name": name,
            "primary_key": [pk],
            "schema": {"fields": {pk: "str"}, "optional_fields": []},
            "mode": "upsert"
        })
    }

    /// Test 21: TCP register frame with aggregation on Table source →
    /// OP_ERROR_RESPONSE with code="aggregation_on_table_not_supported"
    #[tokio::test]
    async fn test_21_tcp_rejects_aggregation_on_table_source() {
        use crate::testing::TestServerBuilder;

        let ts = TestServerBuilder::new()
            .dev_endpoints(false)
            .spawn()
            .await
            .expect("spawn test server");

        let payload = serde_json::json!({
            "nodes": [
                table_node_json("Merchants", "merchant_id"),
                {
                    "kind": "derivation",
                    "name": "AggTable",
                    "output_kind": "table",
                    "upstreams": ["Merchants"],
                    "ops": [{
                        "op": "group_by",
                        "keys": ["merchant_id"],
                        "agg": {"cnt": {"op": "count", "params": {}}}
                    }],
                    "schema": {"fields": {"merchant_id": "str", "cnt": "i64"}, "optional_fields": []},
                    "table_primary_key": ["merchant_id"]
                }
            ]
        });

        let mut tcp = ts.tcp_client().await.expect("tcp connect");
        let (resp_op, body) = tcp.register_json(payload).await.expect("tcp register");

        assert_eq!(
            resp_op, OP_ERROR_RESPONSE,
            "expected OP_ERROR_RESPONSE, got op={resp_op:#06x}, body: {body:#}"
        );
        assert_eq!(
            body["error"]["code"], "aggregation_on_table_not_supported",
            "TCP must use 'aggregation_on_table_not_supported' code, body: {body:#}"
        );

        ts.shutdown().await.expect("shutdown");
    }

    /// Test 22: TCP register frame with invalid window string →
    /// OP_ERROR_RESPONSE with code="aggregation_invalid_window"
    #[tokio::test]
    async fn test_22_tcp_rejects_aggregation_invalid_window() {
        use crate::testing::TestServerBuilder;

        let ts = TestServerBuilder::new()
            .dev_endpoints(false)
            .spawn()
            .await
            .expect("spawn test server");

        let payload = serde_json::json!({
            "nodes": [
                event_node_json(
                    "Txn",
                    &[("event_time", "i64"), ("user_id", "str"), ("amount", "f64")],
                    "event_time"
                ),
                {
                    "kind": "derivation",
                    "name": "AggTable",
                    "output_kind": "table",
                    "upstreams": ["Txn"],
                    "ops": [{
                        "op": "group_by",
                        "keys": ["user_id"],
                        "agg": {"cnt": {"op": "count", "params": {"window": "5seconds"}}}
                    }],
                    "schema": {"fields": {"user_id": "str", "cnt": "i64"}, "optional_fields": []},
                    "table_primary_key": ["user_id"]
                }
            ]
        });

        let mut tcp = ts.tcp_client().await.expect("tcp connect");
        let (resp_op, body) = tcp.register_json(payload).await.expect("tcp register");

        assert_eq!(
            resp_op, OP_ERROR_RESPONSE,
            "expected OP_ERROR_RESPONSE, got op={resp_op:#06x}, body: {body:#}"
        );
        assert_eq!(
            body["error"]["code"], "aggregation_invalid_window",
            "TCP must use 'aggregation_invalid_window' code, body: {body:#}"
        );

        ts.shutdown().await.expect("shutdown");
    }
}
