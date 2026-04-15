//! Phase 31-01: shared handshake helpers for the replica wire protocol.
//!
//! Both the historical path (`client::clone::run_clone`, Phase 28-04) and the
//! streaming path (`client::streaming::StreamingClient::connect`, this phase)
//! need to:
//!   1. open a TCP connection to the tally server,
//!   2. send an admin-token + `Scope` preamble framed under an opcode
//!      (`OP_SNAPSHOT_FETCH` or `OP_SUBSCRIBE`),
//!   3. for the snapshot case, read the header + payload frames and decode
//!      a `BaseSnapshotState`.
//!
//! Phase 31-01 factors those out here so the streaming dance reuses the
//! snapshot-fetch codec verbatim. Historical `run_clone` is refactored to
//! call `fetch_snapshot` without behavior change.
//!
//! Wire layouts mirror `src/server/protocol.rs`; see `src/client/wire.rs`
//! for the duplicated `Scope` codec and the frame-tag constants.

use crate::client::wire::{
    write_scope, Scope, OP_SNAPSHOT_FETCH, REPLICA_FRAME_TAG_HEADER, REPLICA_FRAME_TAG_PAYLOAD,
};
use crate::state::snapshot::BaseSnapshotState;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Phase 27-02 live-subscribe opcode. Mirrors
/// `crate::server::protocol::OP_SUBSCRIBE`.
pub const OP_SUBSCRIBE: u8 = 0x11;

/// Phase 27-02 per-event frame tag. Mirrors
/// `crate::server::protocol::REPLICA_FRAME_TAG_EVENT`.
pub const REPLICA_FRAME_TAG_EVENT: u8 = 0x03;

/// Defence-in-depth protocol limit for snapshot frames. Matches
/// `client::clone::SNAPSHOT_HARD_LIMIT_BYTES`.
const SNAPSHOT_HARD_LIMIT_BYTES: u32 = 1024 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("io: {0}")]
    Io(String),
    #[error("protocol: {0}")]
    Protocol(String),
    #[error("server error: {0}")]
    ServerError(String),
    #[error("decode: {0}")]
    Decode(String),
    #[error("unauthorized")]
    Unauthorized,
}

impl From<std::io::Error> for SessionError {
    fn from(e: std::io::Error) -> Self {
        SessionError::Io(e.to_string())
    }
}

/// Build the request frame for `OP_SNAPSHOT_FETCH` / `OP_SUBSCRIBE`.
///
/// Wire shape: `[u32 BE total_len][u8 opcode][u16-string token][scope-bytes]`.
fn build_request_frame(opcode: u8, token: &str, scope: &Scope) -> Vec<u8> {
    let mut payload = Vec::new();
    let token_bytes = token.as_bytes();
    assert!(
        token_bytes.len() <= u16::MAX as usize,
        "admin token too long for u16 prefix"
    );
    payload.extend_from_slice(&(token_bytes.len() as u16).to_be_bytes());
    payload.extend_from_slice(token_bytes);
    write_scope(&mut payload, scope);
    let total_len: u32 = (1 + payload.len()) as u32;
    let mut frame = Vec::with_capacity(4 + total_len as usize);
    frame.extend_from_slice(&total_len.to_be_bytes());
    frame.push(opcode);
    frame.extend_from_slice(&payload);
    frame
}

/// Phase 31-01: write the `OP_SUBSCRIBE` handshake to `stream`.
///
/// The caller is responsible for all subsequent reads (event frames or
/// STATUS_ERROR). Unlike `fetch_snapshot`, this helper does NOT await a
/// terminal response — the subscribe socket stays open and the server
/// pushes per-event frames indefinitely.
pub async fn subscribe_handshake(
    stream: &mut TcpStream,
    token: &str,
    scope: &Scope,
) -> Result<(), SessionError> {
    let frame = build_request_frame(OP_SUBSCRIBE, token, scope);
    stream.write_all(&frame).await?;
    stream.flush().await?;
    Ok(())
}

/// Phase 31-01: extracted snapshot-fetch round-trip. Factored out of
/// `client::clone::try_once` so the streaming path reuses the exact wire
/// codec.
///
/// Returns `(snapshot_taken_at, BaseSnapshotState)` on success. On an
/// unauthorized response the server writes a STATUS_ERROR frame sharing
/// `REPLICA_FRAME_TAG_HEADER`'s tag; this helper detects that case and
/// returns `SessionError::Unauthorized` when the message begins with
/// "unauthorized", `ServerError(msg)` otherwise.
pub async fn fetch_snapshot(
    stream: &mut TcpStream,
    token: &str,
    scope: &Scope,
) -> Result<(SystemTime, BaseSnapshotState), SessionError> {
    // 1. write the request frame.
    let frame = build_request_frame(OP_SNAPSHOT_FETCH, token, scope);
    stream.write_all(&frame).await?;
    stream.flush().await?;

    // 2. read header frame length + tag.
    let header_len = read_u32(stream).await?;
    if header_len == 0 || header_len > SNAPSHOT_HARD_LIMIT_BYTES {
        return Err(SessionError::Protocol(format!(
            "header frame length out of range: {}",
            header_len
        )));
    }
    let header_tag = read_u8(stream).await?;
    let body_len = (header_len - 1) as usize;
    let mut body = vec![0u8; body_len];
    stream.read_exact(&mut body).await?;

    if header_tag != REPLICA_FRAME_TAG_HEADER {
        return Err(SessionError::Protocol(format!(
            "unexpected tag 0x{:02x} in first frame",
            header_tag
        )));
    }
    if body_len != 12 {
        // STATUS_ERROR — shared tag per `server::protocol.rs`.
        let err_msg = String::from_utf8_lossy(&body).to_string();
        if err_msg.contains("unauthorized") {
            return Err(SessionError::Unauthorized);
        }
        return Err(SessionError::ServerError(err_msg));
    }
    let secs = u64::from_be_bytes([
        body[0], body[1], body[2], body[3], body[4], body[5], body[6], body[7],
    ]);
    let nanos = u32::from_be_bytes([body[8], body[9], body[10], body[11]]);
    let snapshot_taken_at = UNIX_EPOCH
        .checked_add(Duration::new(secs, nanos))
        .unwrap_or(UNIX_EPOCH);

    // 3. read payload frame.
    let payload_len = read_u32(stream).await?;
    if payload_len == 0 || payload_len > SNAPSHOT_HARD_LIMIT_BYTES {
        return Err(SessionError::Protocol(format!(
            "payload frame length out of range: {}",
            payload_len
        )));
    }
    let payload_tag = read_u8(stream).await?;
    if payload_tag != REPLICA_FRAME_TAG_PAYLOAD {
        return Err(SessionError::Protocol(format!(
            "unexpected payload tag 0x{:02x}",
            payload_tag
        )));
    }
    let mut payload_buf = vec![0u8; (payload_len - 1) as usize];
    stream.read_exact(&mut payload_buf).await?;
    let snapshot: BaseSnapshotState = postcard::from_bytes(&payload_buf)
        .map_err(|e| SessionError::Decode(format!("postcard decode: {}", e)))?;
    Ok((snapshot_taken_at, snapshot))
}

/// Phase 27-02 event-frame decode.
///
/// Wire shape (matches `server::protocol::encode_event_frame`):
///   `[u32 BE frame_len][u8 tag=0x03]
///    [u64 BE ts_secs][u32 BE ts_nanos]
///    [u32 BE payload_len][payload_bytes]`
///
/// Reads the frame length prefix + body; returns the decoded
/// `(timestamp, payload_bytes)` tuple. The plan's Option K invariant: no
/// `seq` field is propagated — only `timestamp` + payload. If Phase 27
/// ever adds a `seq` prefix inside the body we ignore it at decode time.
pub async fn read_event_frame(
    stream: &mut TcpStream,
) -> Result<(SystemTime, Vec<u8>), SessionError> {
    let frame_len = read_u32(stream).await?;
    if frame_len == 0 || frame_len > SNAPSHOT_HARD_LIMIT_BYTES {
        return Err(SessionError::Protocol(format!(
            "event frame length out of range: {}",
            frame_len
        )));
    }
    let mut body = vec![0u8; frame_len as usize];
    stream.read_exact(&mut body).await?;
    if body.is_empty() {
        return Err(SessionError::Protocol("event frame empty body".into()));
    }
    let tag = body[0];
    if tag != REPLICA_FRAME_TAG_EVENT {
        return Err(SessionError::Protocol(format!(
            "unexpected event tag 0x{:02x}",
            tag
        )));
    }
    // body[1..9] secs, [9..13] nanos, [13..17] payload_len, [17..] payload.
    if body.len() < 17 {
        return Err(SessionError::Protocol("event frame truncated".into()));
    }
    let secs = u64::from_be_bytes([
        body[1], body[2], body[3], body[4], body[5], body[6], body[7], body[8],
    ]);
    let nanos = u32::from_be_bytes([body[9], body[10], body[11], body[12]]);
    let payload_len = u32::from_be_bytes([body[13], body[14], body[15], body[16]]) as usize;
    if body.len() < 17 + payload_len {
        return Err(SessionError::Protocol(format!(
            "event frame payload truncated: expected {}, got {}",
            payload_len,
            body.len() - 17
        )));
    }
    let timestamp = UNIX_EPOCH
        .checked_add(Duration::new(secs, nanos as u32))
        .unwrap_or(UNIX_EPOCH);
    let payload = body[17..17 + payload_len].to_vec();
    Ok((timestamp, payload))
}

async fn read_u32(stream: &mut TcpStream) -> Result<u32, SessionError> {
    let mut b = [0u8; 4];
    stream.read_exact(&mut b).await?;
    Ok(u32::from_be_bytes(b))
}

async fn read_u8(stream: &mut TcpStream) -> Result<u8, SessionError> {
    let mut b = [0u8; 1];
    stream.read_exact(&mut b).await?;
    Ok(b[0])
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    fn sample_scope() -> Scope {
        Scope {
            streams: vec!["Txn".into()],
            keys: None,
            key_prefix: None,
            pull: "all".into(),
        }
    }

    #[test]
    fn build_subscribe_request_frame_shape() {
        let frame = build_request_frame(OP_SUBSCRIBE, "tok", &sample_scope());
        let total_len = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]) as usize;
        assert_eq!(total_len, frame.len() - 4);
        assert_eq!(frame[4], OP_SUBSCRIBE);
        // token len prefix u16 BE = 3
        assert_eq!(&frame[5..7], &3u16.to_be_bytes());
        assert_eq!(&frame[7..10], b"tok");
    }

    #[test]
    fn build_snapshot_fetch_frame_uses_opcode_0x12() {
        let frame = build_request_frame(OP_SNAPSHOT_FETCH, "", &sample_scope());
        assert_eq!(frame[4], OP_SNAPSHOT_FETCH);
    }

    #[tokio::test]
    async fn subscribe_handshake_writes_full_frame() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut len_buf = [0u8; 4];
            sock.read_exact(&mut len_buf).await.unwrap();
            let total_len = u32::from_be_bytes(len_buf);
            let mut body = vec![0u8; total_len as usize];
            sock.read_exact(&mut body).await.unwrap();
            assert_eq!(body[0], OP_SUBSCRIBE);
            // u16 token len
            let tok_len = u16::from_be_bytes([body[1], body[2]]) as usize;
            assert_eq!(&body[3..3 + tok_len], b"tokABC");
        });
        let mut stream = TcpStream::connect(addr).await.unwrap();
        subscribe_handshake(&mut stream, "tokABC", &sample_scope())
            .await
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn read_event_frame_decodes_wire_layout() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            // Use the server codec so we're byte-identical to prod.
            let frame = crate::server::protocol::encode_event_frame(
                UNIX_EPOCH + Duration::from_secs(100) + Duration::from_nanos(42),
                b"{\"x\":1}",
            );
            sock.write_all(&frame).await.unwrap();
            sock.flush().await.unwrap();
        });
        let mut sock = TcpStream::connect(addr).await.unwrap();
        let (ts, payload) = read_event_frame(&mut sock).await.unwrap();
        assert_eq!(
            ts,
            UNIX_EPOCH + Duration::from_secs(100) + Duration::from_nanos(42)
        );
        assert_eq!(&payload, b"{\"x\":1}");
        server.await.unwrap();
    }
}
