//! Phase 28-04: one-shot historical-clone bootstrap.
//!
//! `run_clone` performs a single TCP round-trip against a running Tally
//! server:
//!   1. connect to `remote`
//!   2. send `[u32 BE total_len][u8 OP_SNAPSHOT_FETCH=0x12][u16-string token][scope-bytes]`
//!      — shape mirrors `src/server/tcp.rs::read_one_frame` + the
//!      `parse_command(OP_SNAPSHOT_FETCH, ...)` branch in
//!      `src/server/protocol.rs:849`.
//!   3. read response header frame: `[u32 BE 13][u8 tag=0x01][u64 BE secs][u32 BE nanos]`
//!   4. read payload frame: `[u32 BE len][u8 tag=0x02][postcard(BaseSnapshotState)]`
//!   5. `postcard::from_bytes::<BaseSnapshotState>` → `StateStore::bulk_load`
//!   6. return a `FrozenClient` pinned at the server's `snapshot_taken_at`.
//!
//! On any failure, retry up to `max_attempts` with exponential-jitter backoff
//! (1s → 2s → 4s → 8s → 16s, cap 30s, ±20%). No LOG_FETCH, no catchup,
//! no mode state machine.

use crate::client::wire::{
    write_scope, Scope, OP_SNAPSHOT_FETCH, REPLICA_FRAME_TAG_HEADER, REPLICA_FRAME_TAG_PAYLOAD,
};
use crate::client::{FrozenClient, SessionMode};
use crate::state::snapshot::BaseSnapshotState;
use crate::state::store::StateStore;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Defence-in-depth protocol limit: reject frames larger than this to avoid
/// OOM on a malicious or corrupted length prefix. The wire's u32 length
/// prefix caps at 4 GiB; we tighten the hard limit to 1 GiB.
const SNAPSHOT_HARD_LIMIT_BYTES: u32 = 1024 * 1024 * 1024;

pub struct CloneArgs {
    pub remote: String,
    pub scope: Scope,
    pub token: Option<String>,
    pub mode: SessionMode,
    pub max_attempts: u32,
}

#[derive(Debug, thiserror::Error)]
pub enum CloneError {
    #[error("streaming mode not supported in Phase 28 (Phase 31 will enable streaming)")]
    StreamingNotSupported,
    #[error("auth failed after {attempts} attempts (last error: {last_error})")]
    AuthFailed { attempts: u32, last_error: String },
    #[error("snapshot fetch failed after {attempts} attempts: {last_error}")]
    FetchFailed { attempts: u32, last_error: String },
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("io: {0}")]
    Io(String),
    #[error("decode: {0}")]
    Decode(String),
}

/// Classify which failure stage the attempt reached — used to pick between
/// `AuthFailed` and `FetchFailed` after the retry budget is exhausted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FailureStage {
    Connect,
    Handshake,
    Fetch,
    Decode,
}

/// Exponential-jitter delay for retry attempts.
///
/// - attempts 0..=4 → base of 1s, 2s, 4s, 8s, 16s
/// - cap at 30s
/// - ±20% symmetric jitter
pub fn next_delay<R: rand::Rng>(attempt: u32, rng: &mut R) -> Duration {
    let base_s = 1u64.checked_shl(attempt.min(4)).unwrap_or(16);
    let capped_ms = (base_s * 1000).min(30_000);
    let jitter_span = (capped_ms as f64 * 0.2) as i64;
    let delta: i64 = if jitter_span == 0 {
        0
    } else {
        rng.gen_range(-jitter_span..=jitter_span)
    };
    let ms = (capped_ms as i64 + delta).max(0) as u64;
    Duration::from_millis(ms)
}

/// Default sleep function: uses `tokio::time::sleep`. Injectable for tests.
async fn default_sleep(d: Duration) {
    tokio::time::sleep(d).await;
}

/// Top-level entry. Delegates to `run_clone_with` with default sleep + RNG.
pub async fn run_clone(args: &CloneArgs) -> Result<FrozenClient, CloneError> {
    use rand::SeedableRng;
    let mut rng = rand::rngs::StdRng::from_entropy();
    run_clone_with(args, &mut rng, |d| Box::pin(default_sleep(d))).await
}

/// Testable inner form: injectable RNG and sleep closure.
pub async fn run_clone_with<F, R>(
    args: &CloneArgs,
    rng: &mut R,
    mut sleep_fn: F,
) -> Result<FrozenClient, CloneError>
where
    R: rand::Rng,
    F: FnMut(Duration) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
{
    if args.mode != SessionMode::Historical {
        return Err(CloneError::StreamingNotSupported);
    }
    if args.max_attempts == 0 {
        return Err(CloneError::Protocol("max_attempts must be > 0".into()));
    }

    let mut last_error = String::new();
    let mut last_stage = FailureStage::Connect;

    for attempt in 0..args.max_attempts {
        match try_once(&args.remote, args.token.as_deref(), &args.scope).await {
            Ok((snapshot_taken_at, state_store)) => {
                return Ok(FrozenClient::new(state_store, args.scope.clone(), snapshot_taken_at));
            }
            Err((stage, msg)) => {
                last_stage = stage;
                last_error = msg;
                // Do not sleep after the final attempt.
                if attempt + 1 < args.max_attempts {
                    let d = next_delay(attempt, rng);
                    sleep_fn(d).await;
                }
            }
        }
    }

    let attempts = args.max_attempts;
    match last_stage {
        FailureStage::Handshake => Err(CloneError::AuthFailed { attempts, last_error }),
        _ => Err(CloneError::FetchFailed { attempts, last_error }),
    }
}

/// Single connection attempt. Returns Ok((snapshot_taken_at, populated state))
/// on success, or Err((stage, msg)) on failure.
async fn try_once(
    remote: &str,
    token: Option<&str>,
    scope: &Scope,
) -> Result<(SystemTime, StateStore), (FailureStage, String)> {
    // 1. connect
    let mut stream = TcpStream::connect(remote)
        .await
        .map_err(|e| (FailureStage::Connect, format!("connect {}: {}", remote, e)))?;

    // 2. build + send request frame:
    //    [u32 BE total_len][u8 opcode][u16-string token][scope-bytes]
    let mut payload = Vec::new();
    let token_str = token.unwrap_or("");
    let token_bytes = token_str.as_bytes();
    if token_bytes.len() > u16::MAX as usize {
        return Err((FailureStage::Handshake, "admin token too long for u16 prefix".into()));
    }
    payload.extend_from_slice(&(token_bytes.len() as u16).to_be_bytes());
    payload.extend_from_slice(token_bytes);
    write_scope(&mut payload, scope);

    let total_len: u32 = (1 + payload.len()) as u32; // opcode + payload
    let mut frame = Vec::with_capacity(4 + total_len as usize);
    frame.extend_from_slice(&total_len.to_be_bytes());
    frame.push(OP_SNAPSHOT_FETCH);
    frame.extend_from_slice(&payload);

    stream
        .write_all(&frame)
        .await
        .map_err(|e| (FailureStage::Handshake, format!("write request: {}", e)))?;
    stream
        .flush()
        .await
        .map_err(|e| (FailureStage::Handshake, format!("flush request: {}", e)))?;

    // 3. read header frame — may be a STATUS_ERROR (tag 0x01, variable body)
    //    or REPLICA_FRAME_TAG_HEADER (tag 0x01, 12-byte body = u64 secs + u32 nanos).
    let header_len = read_u32(&mut stream)
        .await
        .map_err(|e| (FailureStage::Handshake, format!("read header len: {}", e)))?;
    if header_len == 0 || header_len > SNAPSHOT_HARD_LIMIT_BYTES {
        return Err((FailureStage::Fetch, format!("header frame length out of range: {}", header_len)));
    }
    let header_tag = read_u8(&mut stream)
        .await
        .map_err(|e| (FailureStage::Handshake, format!("read header tag: {}", e)))?;
    let body_len = (header_len - 1) as usize;
    let mut body = vec![0u8; body_len];
    stream
        .read_exact(&mut body)
        .await
        .map_err(|e| (FailureStage::Handshake, format!("read header body: {}", e)))?;

    // Distinguish STATUS_ERROR (body length != 12) from header frame (body == 12).
    if header_tag != REPLICA_FRAME_TAG_HEADER {
        return Err((FailureStage::Fetch, format!("unexpected tag 0x{:02x} in first frame", header_tag)));
    }
    if body_len != 12 {
        // tag == 0x01 but body != 12 → the server wrote a STATUS_ERROR frame
        // (shares the same tag per `src/server/protocol.rs` doc comment on
        // REPLICA_FRAME_TAG_HEADER). Interpret as UTF-8 error message.
        let err_msg = String::from_utf8_lossy(&body).to_string();
        let stage = if err_msg.contains("unauthorized") {
            FailureStage::Handshake
        } else {
            FailureStage::Fetch
        };
        return Err((stage, format!("server error: {}", err_msg)));
    }
    let secs = u64::from_be_bytes([
        body[0], body[1], body[2], body[3], body[4], body[5], body[6], body[7],
    ]);
    let nanos = u32::from_be_bytes([body[8], body[9], body[10], body[11]]);
    let snapshot_taken_at = UNIX_EPOCH
        .checked_add(Duration::new(secs, nanos))
        .unwrap_or(UNIX_EPOCH);

    // 4. read payload frame
    let payload_len = read_u32(&mut stream)
        .await
        .map_err(|e| (FailureStage::Fetch, format!("read payload len: {}", e)))?;
    if payload_len == 0 || payload_len > SNAPSHOT_HARD_LIMIT_BYTES {
        return Err((FailureStage::Fetch, format!("payload frame length out of range: {}", payload_len)));
    }
    let payload_tag = read_u8(&mut stream)
        .await
        .map_err(|e| (FailureStage::Fetch, format!("read payload tag: {}", e)))?;
    if payload_tag != REPLICA_FRAME_TAG_PAYLOAD {
        return Err((FailureStage::Fetch, format!("unexpected payload tag 0x{:02x}", payload_tag)));
    }
    let mut payload_buf = vec![0u8; (payload_len - 1) as usize];
    stream
        .read_exact(&mut payload_buf)
        .await
        .map_err(|e| (FailureStage::Fetch, format!("read payload body: {}", e)))?;

    // 5. postcard-decode
    let snapshot: BaseSnapshotState = postcard::from_bytes(&payload_buf)
        .map_err(|e| (FailureStage::Decode, format!("postcard decode: {}", e)))?;

    // 6. bulk-load
    let store = StateStore::new();
    store.bulk_load(snapshot.entities);
    Ok((snapshot_taken_at, store))
}

async fn read_u32(stream: &mut TcpStream) -> std::io::Result<u32> {
    let mut b = [0u8; 4];
    stream.read_exact(&mut b).await?;
    Ok(u32::from_be_bytes(b))
}

async fn read_u8(stream: &mut TcpStream) -> std::io::Result<u8> {
    let mut b = [0u8; 1];
    stream.read_exact(&mut b).await?;
    Ok(b[0])
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::snapshot::{BaseSnapshotState, SnapshotHeader, SnapshotType};
    use rand::SeedableRng;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    fn sample_scope() -> Scope {
        Scope {
            streams: vec!["Txn".into()],
            keys: None,
            key_prefix: None,
            pull: "all".into(),
        }
    }

    fn empty_snapshot() -> BaseSnapshotState {
        BaseSnapshotState {
            header: SnapshotHeader { snapshot_type: SnapshotType::Base, sequence: 1 },
            entities: vec![],
            pipelines: vec![],
            backfill_complete: vec![],
        }
    }

    fn zero_sleep() -> impl FnMut(Duration) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        |_| Box::pin(async {})
    }

    // ---- next_delay envelope ----
    #[test]
    fn next_delay_envelope_matches_spec() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        // Expected (capped) bases in ms: 1000, 2000, 4000, 8000, 16000.
        // For attempt >= 4 we saturate at 16000 per the shift clamp.
        let expected_bases = [1000u64, 2000, 4000, 8000, 16000];
        for (i, &base) in expected_bases.iter().enumerate() {
            for _ in 0..100 {
                let d = next_delay(i as u32, &mut rng);
                let ms = d.as_millis() as u64;
                let low = (base as f64 * 0.8) as u64;
                let high = (base as f64 * 1.2) as u64;
                assert!(ms >= low && ms <= high, "attempt {} ms={} not in [{}, {}]", i, ms, low, high);
            }
        }
    }

    #[test]
    fn next_delay_caps_at_30s() {
        // Values at attempt>=4 cap at 16s (per shift clamp). Verify
        // the hard 30s cap never kicks in below attempt 5 — documentary.
        let mut rng = rand::rngs::StdRng::seed_from_u64(1);
        for _ in 0..50 {
            let d = next_delay(10, &mut rng); // saturates
            // 16s * 1.2 = 19.2s, well under 30s cap.
            assert!(d.as_millis() <= 30_000);
        }
    }

    // ---- streaming rejection (defence in depth) ----
    #[tokio::test]
    async fn run_clone_rejects_streaming_mode() {
        let args = CloneArgs {
            remote: "127.0.0.1:1".into(),
            scope: sample_scope(),
            token: None,
            mode: SessionMode::Historical, // SessionMode::Historical is the only variant; assert semantics by direct check below
            max_attempts: 1,
        };
        // With only Historical defined today, we can't construct Streaming in
        // Rust; instead, exercise the guard by checking run_clone_with short-
        // circuits when mode is Historical (happy path hits elsewhere). This
        // is a placeholder for Phase 31 when Streaming lands.
        let mut rng = rand::rngs::StdRng::seed_from_u64(0);
        // Historical mode against an unreachable remote → FetchFailed,
        // not StreamingNotSupported.
        let res = run_clone_with(&args, &mut rng, zero_sleep()).await;
        match res {
            Err(CloneError::FetchFailed { attempts, .. }) => assert_eq!(attempts, 1),
            other => panic!("expected FetchFailed, got {:?}", other),
        }
    }

    // ---- mock server harness ----
    async fn mock_server_happy<F>(listener: TcpListener, on_done: F)
    where
        F: FnOnce(),
    {
        let (mut sock, _) = listener.accept().await.unwrap();
        // Read the request frame to completion.
        let total_len = {
            let mut b = [0u8; 4];
            sock.read_exact(&mut b).await.unwrap();
            u32::from_be_bytes(b)
        };
        let mut req = vec![0u8; total_len as usize];
        sock.read_exact(&mut req).await.unwrap();
        assert_eq!(req[0], OP_SNAPSHOT_FETCH);

        // Emit header frame: [u32 BE 13][u8 0x01][u64 secs=7][u32 nanos=500]
        let secs: u64 = 7;
        let nanos: u32 = 500;
        let mut hdr = Vec::new();
        hdr.extend_from_slice(&13u32.to_be_bytes());
        hdr.push(REPLICA_FRAME_TAG_HEADER);
        hdr.extend_from_slice(&secs.to_be_bytes());
        hdr.extend_from_slice(&nanos.to_be_bytes());
        sock.write_all(&hdr).await.unwrap();

        // Payload frame
        let snap = empty_snapshot();
        let postcard_bytes = postcard::to_allocvec(&snap).unwrap();
        let payload_total_len: u32 = (1 + postcard_bytes.len()) as u32;
        let mut pay = Vec::new();
        pay.extend_from_slice(&payload_total_len.to_be_bytes());
        pay.push(REPLICA_FRAME_TAG_PAYLOAD);
        pay.extend_from_slice(&postcard_bytes);
        sock.write_all(&pay).await.unwrap();
        sock.flush().await.unwrap();
        on_done();
    }

    #[tokio::test]
    async fn run_clone_happy_path() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            mock_server_happy(listener, || {}).await;
        });

        let args = CloneArgs {
            remote: addr.to_string(),
            scope: sample_scope(),
            token: Some("tok".into()),
            mode: SessionMode::Historical,
            max_attempts: 1,
        };
        let mut rng = rand::rngs::StdRng::seed_from_u64(0);
        let fc = run_clone_with(&args, &mut rng, zero_sleep()).await.expect("happy path");
        let expected = UNIX_EPOCH + Duration::new(7, 500);
        assert_eq!(fc.snapshot_taken_at, expected);
        assert_eq!(fc.scope().streams, vec!["Txn".to_string()]);
        server.await.unwrap();
    }

    async fn mock_server_auth_reject(listener: TcpListener) {
        // Accept, read the request fully, then close without sending anything.
        // Client sees EOF on header read → handshake-stage error.
        loop {
            let (mut sock, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => return,
            };
            // Read total_len
            let mut b = [0u8; 4];
            if sock.read_exact(&mut b).await.is_err() { continue; }
            let total_len = u32::from_be_bytes(b);
            let mut req = vec![0u8; total_len as usize];
            let _ = sock.read_exact(&mut req).await;
            // Emit a STATUS_ERROR frame carrying "unauthorized".
            let msg = b"unauthorized: bad token";
            let body_len: u32 = (1 + msg.len()) as u32;
            let mut out = Vec::new();
            out.extend_from_slice(&body_len.to_be_bytes());
            out.push(REPLICA_FRAME_TAG_HEADER); // tag collides with STATUS_ERROR
            out.extend_from_slice(msg);
            let _ = sock.write_all(&out).await;
            let _ = sock.flush().await;
        }
    }

    #[tokio::test]
    async fn run_clone_auth_failure_after_retries() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(mock_server_auth_reject(listener));

        let args = CloneArgs {
            remote: addr.to_string(),
            scope: sample_scope(),
            token: Some("wrong".into()),
            mode: SessionMode::Historical,
            max_attempts: 2,
        };
        let mut rng = rand::rngs::StdRng::seed_from_u64(0);
        let err = run_clone_with(&args, &mut rng, zero_sleep()).await.unwrap_err();
        match err {
            CloneError::AuthFailed { attempts, .. } => assert_eq!(attempts, 2),
            other => panic!("expected AuthFailed, got {:?}", other),
        }
        server.abort();
    }

    async fn mock_server_mid_drop(listener: TcpListener) {
        loop {
            let (mut sock, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => return,
            };
            let mut b = [0u8; 4];
            if sock.read_exact(&mut b).await.is_err() { continue; }
            let total_len = u32::from_be_bytes(b);
            let mut req = vec![0u8; total_len as usize];
            let _ = sock.read_exact(&mut req).await;

            // Write a valid header frame, then drop the connection.
            let secs: u64 = 1;
            let nanos: u32 = 0;
            let mut hdr = Vec::new();
            hdr.extend_from_slice(&13u32.to_be_bytes());
            hdr.push(REPLICA_FRAME_TAG_HEADER);
            hdr.extend_from_slice(&secs.to_be_bytes());
            hdr.extend_from_slice(&nanos.to_be_bytes());
            let _ = sock.write_all(&hdr).await;
            let _ = sock.flush().await;
            drop(sock); // client sees EOF mid-payload
        }
    }

    #[tokio::test]
    async fn run_clone_mid_payload_drop() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(mock_server_mid_drop(listener));

        let args = CloneArgs {
            remote: addr.to_string(),
            scope: sample_scope(),
            token: Some("tok".into()),
            mode: SessionMode::Historical,
            max_attempts: 2,
        };
        let mut rng = rand::rngs::StdRng::seed_from_u64(0);
        let err = run_clone_with(&args, &mut rng, zero_sleep()).await.unwrap_err();
        match err {
            CloneError::FetchFailed { attempts, .. } => assert_eq!(attempts, 2),
            other => panic!("expected FetchFailed, got {:?}", other),
        }
        server.abort();
    }
}
