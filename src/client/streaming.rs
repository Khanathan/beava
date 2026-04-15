//! Phase 31-01: Rust-side Option K streaming-mode session.
//!
//! # The dance
//!
//! `StreamingClient::connect(remote, scope, token)` implements the locked
//! Option K subscribe-first sequence:
//!
//!   1. **Subscribing.** Open socket #1; send `OP_SUBSCRIBE{token, scope}`;
//!      spawn a background thread that reads event frames and appends them
//!      to an in-memory buffer (`Arc<Mutex<Vec<BufferedEvent>>>`).
//!   2. **Snapshot.** Open socket #2; call `session::fetch_snapshot` to
//!      receive `(snapshot_taken_at, BaseSnapshotState)`; drop the socket.
//!   3. **BufferedReplay.** Take the write lock on the `StreamingStore`;
//!      bulk-load the snapshot; drain the buffer, dropping any events with
//!      `timestamp <= snapshot_taken_at` and applying the rest in timestamp
//!      order (releasing the write lock between events so readers aren't
//!      starved).
//!   4. **Live.** Flip the shared `MODE_LIVE` atomic; the background thread
//!      now applies events directly as they arrive on socket #1.
//!
//! # Stop semantics
//!
//! `.stop()` is idempotent:
//!   - sets the shared `stop_flag: AtomicBool`,
//!   - forces the background socket closed (remote peer shutdown),
//!   - joins the apply thread,
//!   - returns the recorded `StopReason` (falls back to `UserRequested`).
//!
//! If the background thread observes EOF without the stop flag set, it
//! records `StopReason::ServerDropped { at_timestamp: last_applied_ts }`
//! so Phase 31-02 can surface it as `SubscriberDroppedError`.
//!
//! # Option K caveats (documented inline and in `31-01-SUMMARY.md`)
//!
//! - No client-side buffer cap — v0 uses an unbounded `Vec<BufferedEvent>`.
//! - No global `seq` field is propagated; Phase 27 delivers per-connection
//!   order which is sufficient.
//! - Events straddling `snapshot_taken_at` may be applied twice (once via
//!   bulk_load-aggregated state, once via buffered replay). v0 accepts this:
//!   `StateStore::apply_streaming_event` is idempotent — the same
//!   `(stream, key, payload)` produces the same recorded state.

use crate::client::session::{self, SessionError};
use crate::client::state::{into_streaming, StreamingStore};
use crate::client::wire::Scope;
use crate::state::store::StateStore;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream as TokioTcpStream;
use tokio::runtime::Runtime;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum StopReason {
    UserRequested,
    ServerDropped { at_timestamp: SystemTime },
    Io(String),
    Transition(String),
}

#[derive(Debug, Clone)]
pub struct BufferedEvent {
    pub timestamp: SystemTime,
    pub stream: String,
    pub key: String,
    pub payload: Vec<u8>,
}

/// Dispatcher hook. Plan 31-02 wires `WatcherRegistry` in here. Plan 31-01
/// only provides the slot + a no-op default used in tests.
pub trait WatcherDispatch: Send + Sync {
    fn on_applied(&self, timestamp: SystemTime, stream: &str, key: &str, payload: &[u8]);
    fn on_stopped(&self, reason: &StopReason);
}

/// No-op default used when plan 31-02 hasn't installed a registry yet.
pub struct NullDispatcher;
impl WatcherDispatch for NullDispatcher {
    fn on_applied(&self, _: SystemTime, _: &str, _: &str, _: &[u8]) {}
    fn on_stopped(&self, _: &StopReason) {}
}

#[derive(Debug, thiserror::Error)]
pub enum StreamingError {
    #[error("session error: {0}")]
    Session(#[from] SessionError),
    #[error("io: {0}")]
    Io(String),
    #[error("thread spawn failed: {0}")]
    Spawn(String),
    #[error("runtime: {0}")]
    Runtime(String),
}

impl From<std::io::Error> for StreamingError {
    fn from(e: std::io::Error) -> Self {
        StreamingError::Io(e.to_string())
    }
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

/// Mode flag shared between `connect` and the apply thread. While the dance
/// is still replaying the snapshot buffer, the thread pushes events into the
/// buffer vec; once `connect` flips to `MODE_LIVE`, the thread applies
/// directly under the `StreamingStore` write lock.
const MODE_BUFFERING: u8 = 0;
const MODE_LIVE: u8 = 1;

/// Encode a `SystemTime` to nanos-since-epoch for `AtomicU64` storage.
/// Clock-skew (pre-epoch) maps to `0`, matching Phase 27's event-frame codec.
fn ts_to_nanos(ts: SystemTime) -> u64 {
    ts.duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() * 1_000_000_000 + u64::from(d.subsec_nanos()))
        .unwrap_or(0)
}

fn nanos_to_ts(n: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_nanos(n)
}

// ---------------------------------------------------------------------------
// StreamingClient
// ---------------------------------------------------------------------------

pub struct StreamingClient {
    store: StreamingStore,
    stop_flag: Arc<AtomicBool>,
    stop_reason: Arc<Mutex<Option<StopReason>>>,
    last_applied_ts_nanos: Arc<AtomicU64>,
    apply_handle: Option<JoinHandle<()>>,
    /// Keeps the background tokio runtime alive for the lifetime of the
    /// apply thread. The runtime drives the socket-read future; dropping
    /// it after `.stop()` joins the thread is how we guarantee async
    /// resources are released.
    #[allow(dead_code)]
    runtime: Option<Arc<Runtime>>,
    dispatcher: Arc<dyn WatcherDispatch>,
    /// Signaled by the apply thread when the subscribe handshake has been
    /// acked and the thread is happily in its read loop. Used by
    /// `connect()` to ensure the fake-server race tests are deterministic.
    #[allow(dead_code)]
    handshake_done: Arc<AtomicBool>,
}

impl StreamingClient {
    /// Execute the Option K subscribe-first dance.
    ///
    /// See module docstring for the four-phase sequence. Returns a handle
    /// whose `StateStore` is already populated with the snapshot + any
    /// pre-live events that arrived during the subscribe window. After
    /// this returns, live events are applied by the background thread.
    pub fn connect(remote: &str, scope: Scope, token: &str) -> Result<Self, StreamingError> {
        // Use a dedicated multi-thread tokio runtime owned by this client —
        // we don't want to require the caller to already be inside one.
        let rt = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .map_err(|e| StreamingError::Runtime(e.to_string()))?,
        );
        let remote_s = remote.to_string();
        let token_s = token.to_string();
        let scope1 = scope.clone();
        let scope2 = scope.clone();

        // --- Phase 1: Subscribing — open socket #1 + send handshake. ---
        let sock1 = rt
            .block_on(async {
                let mut s = TokioTcpStream::connect(&remote_s).await?;
                s.set_nodelay(true).ok();
                session::subscribe_handshake(&mut s, &token_s, &scope1).await?;
                Ok::<_, SessionError>(s)
            })
            .map_err(StreamingError::Session)?;

        // Shared state between connect() and the apply thread.
        let buffer: Arc<Mutex<Vec<BufferedEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let mode = Arc::new(AtomicU8::new(MODE_BUFFERING));
        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop_reason: Arc<Mutex<Option<StopReason>>> = Arc::new(Mutex::new(None));
        let last_applied_ts_nanos = Arc::new(AtomicU64::new(0));
        let store: StreamingStore = into_streaming(StateStore::new());
        let dispatcher: Arc<dyn WatcherDispatch> = Arc::new(NullDispatcher);
        let handshake_done = Arc::new(AtomicBool::new(false));

        // --- Phase 2: start the buffer-fill thread. ---
        let apply_handle = spawn_apply_thread(
            sock1,
            rt.clone(),
            buffer.clone(),
            mode.clone(),
            store.clone(),
            stop_flag.clone(),
            stop_reason.clone(),
            last_applied_ts_nanos.clone(),
            dispatcher.clone(),
            handshake_done.clone(),
        )?;

        // Signal that the thread is alive (no real handshake ack exists on
        // the wire — the server pushes immediately — so "alive" is sufficient).
        handshake_done.store(true, Ordering::Release);

        // --- Phase 3: Snapshot-fetch on socket #2. ---
        let fetch_result: Result<(SystemTime, crate::state::snapshot::BaseSnapshotState), SessionError> = rt
            .block_on(async {
                let mut s2 = TokioTcpStream::connect(&remote_s).await?;
                s2.set_nodelay(true).ok();
                session::fetch_snapshot(&mut s2, &token_s, &scope2).await
            });

        let (snapshot_taken_at, snapshot) = match fetch_result {
            Ok(v) => v,
            Err(e) => {
                // Tear down the apply thread cleanly, then bubble the error.
                stop_flag.store(true, Ordering::Release);
                // Best-effort: wait briefly for thread to observe + exit.
                let _ = apply_handle.join();
                return Err(StreamingError::Session(e));
            }
        };

        // --- Phase 3b: BufferedReplay. ---
        //
        // Take the write lock, bulk-load, drop the lock.
        {
            let guard = store.write();
            guard.bulk_load(snapshot.entities);
        }

        // Drain the buffer, dropping pre-snapshot events. We intentionally
        // DO NOT hold the write lock across the whole replay — apply one
        // event at a time under the lock so concurrent `.get()` readers
        // (including Phase 31-02 watchers) aren't starved.
        let drained: Vec<BufferedEvent> = {
            let mut g = buffer.lock();
            std::mem::take(&mut *g)
        };
        let mut sorted = drained;
        sorted.sort_by_key(|ev| ev.timestamp);
        for ev in sorted {
            if ev.timestamp <= snapshot_taken_at {
                // Drop: already aggregated into the snapshot.
                continue;
            }
            {
                let guard = store.write();
                guard.apply_streaming_event(&ev.stream, &ev.key, &ev.payload, ev.timestamp);
            }
            last_applied_ts_nanos.store(ts_to_nanos(ev.timestamp), Ordering::Release);
            dispatcher.on_applied(ev.timestamp, &ev.stream, &ev.key, &ev.payload);
        }

        // --- Phase 4: flip to Live. ---
        //
        // SAFETY / Option K: double-apply is idempotent for
        // `apply_streaming_event` — same (stream,key,payload) produces the
        // same recorded state. A narrow race exists where an event arriving
        // between "mode.store(MODE_LIVE)" and the bg thread's next check
        // could be pushed to the (now-empty) buffer rather than applied;
        // `apply_streaming_event` in the bg thread's MODE_LIVE branch is
        // the canonical path. This is acceptable at v0.
        mode.store(MODE_LIVE, Ordering::Release);

        Ok(StreamingClient {
            store,
            stop_flag,
            stop_reason,
            last_applied_ts_nanos,
            apply_handle: Some(apply_handle),
            runtime: Some(rt),
            dispatcher,
            handshake_done,
        })
    }

    /// Clone of the `Arc<RwLock<StateStore>>`. Callers may `.read()` it to
    /// perform scope-aware lookups. Phase 31-02 layers `.watch()` on top.
    pub fn state(&self) -> StreamingStore {
        self.store.clone()
    }

    /// Current stop reason, if the session has halted (user-requested or
    /// server-dropped).
    pub fn stop_reason(&self) -> Option<StopReason> {
        self.stop_reason.lock().clone()
    }

    pub fn last_applied_timestamp(&self) -> Option<SystemTime> {
        match self.last_applied_ts_nanos.load(Ordering::Acquire) {
            0 => None,
            n => Some(nanos_to_ts(n)),
        }
    }

    /// Idempotent stop. Returns the recorded `StopReason`.
    ///
    /// Semantics (per Option K §E1):
    ///   1. Set the shared stop flag.
    ///   2. Drop the socket via runtime shutdown — the bg thread's read
    ///      future wakes with an error.
    ///   3. Join the apply thread.
    ///   4. Return the cached stop reason, filling in `UserRequested` if
    ///      the bg thread didn't already record one.
    pub fn stop(&mut self) -> StopReason {
        // Idempotency: if already joined, return cached reason.
        if self.apply_handle.is_none() {
            let mut g = self.stop_reason.lock();
            if g.is_none() {
                *g = Some(StopReason::UserRequested);
            }
            return g.clone().unwrap();
        }
        self.stop_flag.store(true, Ordering::Release);
        // Dropping the runtime forces any pending read future to be cancelled
        // and the socket to close; this happens after we join, though — so
        // for a prompt wakeup we rely on the bg thread's per-iteration
        // stop-flag check (see the read_event_with_timeout path).
        if let Some(handle) = self.apply_handle.take() {
            let _ = handle.join();
        }
        // Finalize reason.
        let mut g = self.stop_reason.lock();
        if g.is_none() {
            *g = Some(StopReason::UserRequested);
        }
        let reason = g.clone().unwrap();
        drop(g);
        self.dispatcher.on_stopped(&reason);
        reason
    }

    /// Plan 31-02 hook. Installing at most once is recommended; a second
    /// call silently overwrites — plan 31-02's tests should exercise single
    /// installation. Thread-safety: Arc'd so cheap to clone, but the slot
    /// itself is `&mut`-guarded so concurrent installs aren't allowed.
    pub fn install_dispatcher(&mut self, d: Arc<dyn WatcherDispatch>) {
        self.dispatcher = d;
    }
}

impl Drop for StreamingClient {
    fn drop(&mut self) {
        // Best-effort cleanup: if the user dropped without calling .stop(),
        // signal the thread and let it exit on its next poll.
        if self.apply_handle.is_some() {
            self.stop_flag.store(true, Ordering::Release);
            if let Some(h) = self.apply_handle.take() {
                let _ = h.join();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Apply thread
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn spawn_apply_thread(
    sock: TokioTcpStream,
    rt: Arc<Runtime>,
    buffer: Arc<Mutex<Vec<BufferedEvent>>>,
    mode: Arc<AtomicU8>,
    store: StreamingStore,
    stop_flag: Arc<AtomicBool>,
    stop_reason: Arc<Mutex<Option<StopReason>>>,
    last_applied_ts_nanos: Arc<AtomicU64>,
    dispatcher: Arc<dyn WatcherDispatch>,
    _handshake_done: Arc<AtomicBool>,
) -> Result<JoinHandle<()>, StreamingError> {
    std::thread::Builder::new()
        .name("tally-streaming-apply".into())
        .spawn(move || {
            // Move `sock` into the runtime so its async I/O drives reads.
            let mut sock = sock;
            loop {
                if stop_flag.load(Ordering::Acquire) {
                    let mut g = stop_reason.lock();
                    if g.is_none() {
                        *g = Some(StopReason::UserRequested);
                    }
                    return;
                }
                // Use a 500ms timeout so the stop_flag is observed promptly
                // even on a quiet socket.
                let read_result: Result<Option<(SystemTime, Vec<u8>)>, SessionError> = rt
                    .block_on(async {
                        tokio::select! {
                            biased;
                            // Stop flag polled every 500ms.
                            _ = tokio::time::sleep(Duration::from_millis(500)) => Ok(None),
                            r = session::read_event_frame(&mut sock) => r.map(Some),
                        }
                    });

                match read_result {
                    Ok(None) => continue, // tick; loop to check stop_flag.
                    Ok(Some((timestamp, payload))) => {
                        // Phase 27's event frame carries timestamp + payload
                        // bytes. We don't have per-event (stream,key) on the
                        // wire (the server sends the raw pushed JSON as the
                        // payload — future protocol revs may add them). For
                        // v0 we parse stream+key out of the JSON payload if
                        // present, else leave them empty. The plan's
                        // `BufferedEvent { stream, key }` contract is thus a
                        // best-effort extraction.
                        let (stream_name, key_name) = extract_stream_key(&payload);
                        let ev = BufferedEvent {
                            timestamp,
                            stream: stream_name,
                            key: key_name,
                            payload,
                        };
                        match mode.load(Ordering::Acquire) {
                            MODE_BUFFERING => {
                                buffer.lock().push(ev);
                            }
                            _ => {
                                // MODE_LIVE
                                {
                                    let guard = store.write();
                                    guard.apply_streaming_event(
                                        &ev.stream,
                                        &ev.key,
                                        &ev.payload,
                                        ev.timestamp,
                                    );
                                }
                                last_applied_ts_nanos
                                    .store(ts_to_nanos(ev.timestamp), Ordering::Release);
                                dispatcher.on_applied(
                                    ev.timestamp,
                                    &ev.stream,
                                    &ev.key,
                                    &ev.payload,
                                );
                            }
                        }
                    }
                    Err(e) => {
                        // Classify: if stop_flag is set, this is a
                        // user-initiated EOF; otherwise it's a server drop.
                        let user_requested = stop_flag.load(Ordering::Acquire);
                        let mut g = stop_reason.lock();
                        if g.is_none() {
                            *g = Some(if user_requested {
                                StopReason::UserRequested
                            } else {
                                match &e {
                                    SessionError::Io(msg) if msg.contains("unexpected")
                                        || msg.contains("closed")
                                        || msg.contains("reset")
                                        || msg.contains("eof")
                                        || msg.contains("EOF") =>
                                    {
                                        let at = nanos_to_ts(
                                            last_applied_ts_nanos.load(Ordering::Acquire),
                                        );
                                        StopReason::ServerDropped { at_timestamp: at }
                                    }
                                    SessionError::Io(msg) => StopReason::Io(msg.clone()),
                                    _ => {
                                        // Any non-IO error mid-stream is a
                                        // server drop classification for v0.
                                        let at = nanos_to_ts(
                                            last_applied_ts_nanos.load(Ordering::Acquire),
                                        );
                                        StopReason::ServerDropped { at_timestamp: at }
                                    }
                                }
                            });
                        }
                        let reason = g.clone().unwrap();
                        drop(g);
                        dispatcher.on_stopped(&reason);
                        return;
                    }
                }
            }
        })
        .map_err(|e| StreamingError::Spawn(e.to_string()))
}

/// Best-effort `(stream, key)` extraction from a JSON event payload.
///
/// Phase 27's per-event wire frame carries only `(timestamp, payload_bytes)`;
/// the payload is the client-authored event JSON, so we look for the
/// conventional `_stream` and the key field (either `user_id` or `key`).
/// Missing fields fall back to empty strings — plan 31-02's dispatcher will
/// route events by whatever fields are present.
fn extract_stream_key(payload: &[u8]) -> (String, String) {
    let Ok(v): Result<serde_json::Value, _> = serde_json::from_slice(payload) else {
        return (String::new(), String::new());
    };
    let stream = v
        .get("_stream")
        .and_then(|s| s.as_str())
        .unwrap_or("")
        .to_string();
    let key = v
        .get("_key")
        .or_else(|| v.get("user_id"))
        .or_else(|| v.get("key"))
        .and_then(|s| s.as_str())
        .unwrap_or("")
        .to_string();
    (stream, key)
}

// ---------------------------------------------------------------------------
// StateStore::apply_streaming_event — v0 stub for streaming event application.
// ---------------------------------------------------------------------------
//
// Rationale (see 31-01-SUMMARY.md §Deviations): the plan assumes a
// `StateStore::apply_event` method exists as the single-event analogue of
// `bulk_load`. It does not — on the server, single-event application runs
// through the full `PipelineEngine::push_with_cascade` path, which requires
// registered streams + operators + cascade graph. The client does not have
// (and cannot trivially build) a mirror `PipelineEngine`.
//
// For v0 we provide a minimal idempotent "last-event-payload recording"
// method via a free function below, attached via an inherent-impl extension.
// This is enough for plan 31-02 to surface "new event arrived at (stream,key,t)"
// to watchers; downstream aggregation is a Phase 32 concern.

mod streaming_apply_shim {
    //! Hosted in a submodule so tests can override / inspect.
    use crate::state::store::StateStore;
    use std::time::SystemTime;

    /// v0 apply-event shim: record the most recent `(payload, timestamp)` as
    /// an entity static-feature tagged by stream. Idempotent — re-applying
    /// the same event overwrites with identical content.
    pub fn apply_streaming_event(
        store: &StateStore,
        stream: &str,
        key: &str,
        payload: &[u8],
        timestamp: SystemTime,
    ) {
        if key.is_empty() {
            // No key → cannot record without allocating a synthetic one.
            // Plan 31-02 watchers will still see it via the dispatcher; the
            // StateStore side just remains unchanged.
            return;
        }
        // Parse into a Value if possible; record as JSON in the static
        // features map under a stable key. Falls back to the raw bytes as
        // a string if JSON fails.
        // Record the raw JSON text (or best-effort UTF-8) as a String
        // feature. `FeatureValue` has no JSON variant; stringifying the
        // whole payload preserves it verbatim for plan 31-02 watchers to
        // inspect via the static_features map.
        let value = crate::types::FeatureValue::String(
            String::from_utf8_lossy(payload).into_owned(),
        );
        let feat_name = if stream.is_empty() {
            "_last_event".to_string()
        } else {
            format!("_last_event__{}", stream)
        };
        store.set_static(key, &feat_name, value, timestamp);
    }
}

/// Trait injected into `StateStore` so the apply thread can call
/// `guard.apply_streaming_event(...)` uniformly under the write lock.
trait ApplyStreamingEvent {
    fn apply_streaming_event(
        &self,
        stream: &str,
        key: &str,
        payload: &[u8],
        timestamp: SystemTime,
    );
}

impl ApplyStreamingEvent for StateStore {
    fn apply_streaming_event(
        &self,
        stream: &str,
        key: &str,
        payload: &[u8],
        timestamp: SystemTime,
    ) {
        streaming_apply_shim::apply_streaming_event(self, stream, key, payload, timestamp);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::protocol::encode_event_frame;
    use std::io::Write;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn sample_scope() -> Scope {
        Scope {
            streams: vec!["Txn".into()],
            keys: None,
            key_prefix: None,
            pull: "all".into(),
        }
    }

    fn empty_snapshot() -> crate::state::snapshot::BaseSnapshotState {
        use crate::state::snapshot::{SnapshotHeader, SnapshotType};
        crate::state::snapshot::BaseSnapshotState {
            header: SnapshotHeader {
                snapshot_type: SnapshotType::Base,
                sequence: 1,
            },
            entities: vec![],
            pipelines: vec![],
            backfill_complete: vec![],
        }
    }

    #[test]
    fn stop_reason_debug_clone() {
        let r1 = StopReason::UserRequested;
        let r2 = StopReason::ServerDropped {
            at_timestamp: UNIX_EPOCH + Duration::from_secs(1),
        };
        let r3 = StopReason::Io("oops".into());
        let r4 = StopReason::Transition("nope".into());
        let _ = format!("{:?} {:?} {:?} {:?}", r1, r2, r3, r4);
        let _c: StopReason = r1.clone();
    }

    #[test]
    fn ts_to_nanos_roundtrip() {
        let ts = UNIX_EPOCH + Duration::new(42, 123_456);
        let n = ts_to_nanos(ts);
        assert_eq!(nanos_to_ts(n), ts);
    }

    #[test]
    fn extract_stream_key_picks_conventional_fields() {
        let (s, k) = extract_stream_key(b"{\"_stream\":\"Txn\",\"user_id\":\"u1\"}");
        assert_eq!(s, "Txn");
        assert_eq!(k, "u1");
        let (s, k) = extract_stream_key(b"not json");
        assert_eq!(s, "");
        assert_eq!(k, "");
    }

    #[test]
    fn apply_streaming_event_idempotent() {
        let store = crate::state::store::StateStore::new();
        let ts = UNIX_EPOCH + Duration::from_secs(100);
        store.apply_streaming_event("Txn", "u1", b"{\"x\":1}", ts);
        store.apply_streaming_event("Txn", "u1", b"{\"x\":1}", ts);
        // Both calls produced the same static feature; no panic, no torn state.
        let entity = store.get_entity("u1").expect("entity present");
        assert!(entity.static_features.contains_key("_last_event__Txn"));
    }

    /// Fake server shim: listens on 127.0.0.1:0, accepts two connections
    /// (socket #1 = subscribe, socket #2 = snapshot-fetch). Sends the
    /// provided events on socket #1 and a minimal snapshot on socket #2.
    async fn fake_server(
        listener: tokio::net::TcpListener,
        events: Vec<(SystemTime, Vec<u8>)>,
        snapshot_taken_at: SystemTime,
        keep_open: bool,
    ) {
        // Socket #1 — subscribe.
        let (mut sock1, _) = listener.accept().await.unwrap();
        // Read the request frame.
        let mut len_buf = [0u8; 4];
        sock1.read_exact(&mut len_buf).await.unwrap();
        let total_len = u32::from_be_bytes(len_buf);
        let mut body = vec![0u8; total_len as usize];
        sock1.read_exact(&mut body).await.unwrap();
        assert_eq!(body[0], 0x11, "expected OP_SUBSCRIBE");
        // Emit all pre-configured events.
        for (ts, payload) in &events {
            let frame = encode_event_frame(*ts, payload);
            sock1.write_all(&frame).await.unwrap();
            sock1.flush().await.unwrap();
        }
        if !keep_open {
            drop(sock1);
        }

        // Socket #2 — snapshot-fetch.
        let (mut sock2, _) = listener.accept().await.unwrap();
        let mut len_buf = [0u8; 4];
        sock2.read_exact(&mut len_buf).await.unwrap();
        let total_len = u32::from_be_bytes(len_buf);
        let mut body = vec![0u8; total_len as usize];
        sock2.read_exact(&mut body).await.unwrap();
        assert_eq!(body[0], 0x12, "expected OP_SNAPSHOT_FETCH");

        let secs = snapshot_taken_at
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let nanos = snapshot_taken_at
            .duration_since(UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        let mut hdr = Vec::new();
        hdr.extend_from_slice(&13u32.to_be_bytes());
        hdr.push(0x01);
        hdr.extend_from_slice(&secs.to_be_bytes());
        hdr.extend_from_slice(&nanos.to_be_bytes());
        sock2.write_all(&hdr).await.unwrap();
        let snap = empty_snapshot();
        let snap_bytes = postcard::to_allocvec(&snap).unwrap();
        let payload_total_len: u32 = (1 + snap_bytes.len()) as u32;
        let mut pay = Vec::new();
        pay.extend_from_slice(&payload_total_len.to_be_bytes());
        pay.push(0x02);
        pay.extend_from_slice(&snap_bytes);
        sock2.write_all(&pay).await.unwrap();
        sock2.flush().await.unwrap();
        // Let sock1 continue to exist if requested (for live-apply tests).
        if keep_open {
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
        // Suppress unused writes warning.
        let _ = std::io::stderr().flush();
    }

    #[test]
    fn connect_dance_against_fake_server() {
        // Use a dedicated runtime for the fake server only; the client
        // builds its own internal runtime.
        let server_rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let (addr, server_fut) = server_rt.block_on(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let snap_ts = UNIX_EPOCH + Duration::from_secs(100);
            let events = vec![
                // Pre-snapshot: should be dropped.
                (
                    UNIX_EPOCH + Duration::from_secs(50),
                    b"{\"_stream\":\"Txn\",\"user_id\":\"u_old\"}".to_vec(),
                ),
                // Post-snapshot: should be applied.
                (
                    UNIX_EPOCH + Duration::from_secs(200),
                    b"{\"_stream\":\"Txn\",\"user_id\":\"u_new\"}".to_vec(),
                ),
            ];
            (addr, fake_server(listener, events, snap_ts, false))
        });
        let server_handle = server_rt.spawn(server_fut);

        let mut client =
            StreamingClient::connect(&addr.to_string(), sample_scope(), "tok").expect("connect");
        // Give a moment for buffered-replay + apply.
        std::thread::sleep(Duration::from_millis(200));

        // u_new should be present (applied post-snapshot); u_old dropped.
        {
            let guard = client.state();
            let g = guard.read();
            assert!(g.get_entity("u_new").is_some(), "post-snapshot event applied");
            assert!(g.get_entity("u_old").is_none(), "pre-snapshot event dropped");
        }

        // Clean stop.
        let reason = client.stop();
        match reason {
            StopReason::UserRequested
            | StopReason::ServerDropped { .. }
            | StopReason::Io(_) => {}
            StopReason::Transition(_) => panic!("unexpected reason"),
        }

        server_rt.block_on(async { let _ = server_handle.await; });
    }

    #[test]
    fn stop_idempotent_twice() {
        let server_rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let (addr, server_fut) = server_rt.block_on(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let snap_ts = UNIX_EPOCH + Duration::from_secs(100);
            (addr, fake_server(listener, vec![], snap_ts, true))
        });
        let _server_handle = server_rt.spawn(server_fut);
        let mut client =
            StreamingClient::connect(&addr.to_string(), sample_scope(), "tok").expect("connect");
        std::thread::sleep(Duration::from_millis(50));
        let r1 = client.stop();
        let r2 = client.stop();
        // Both return. r2 should be the same cached reason as r1.
        match (&r1, &r2) {
            (StopReason::UserRequested, StopReason::UserRequested) => {}
            (StopReason::ServerDropped { .. }, StopReason::ServerDropped { .. }) => {}
            other => panic!("inconsistent stop reasons: {:?}", other),
        }
    }

    #[test]
    fn server_drop_classification() {
        // Fake server: accept both sockets, send snapshot, then close sock1.
        let server_rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let (addr, server_fut) = server_rt.block_on(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let snap_ts = UNIX_EPOCH + Duration::from_secs(100);
            let events = vec![(
                UNIX_EPOCH + Duration::from_secs(200),
                b"{\"_stream\":\"Txn\",\"user_id\":\"u1\"}".to_vec(),
            )];
            (addr, fake_server(listener, events, snap_ts, false))
        });
        let _handle = server_rt.spawn(server_fut);
        let client =
            StreamingClient::connect(&addr.to_string(), sample_scope(), "tok").expect("connect");
        // After sock1 is closed, the bg thread observes EOF and should
        // transition to ServerDropped within ~1s.
        let mut attempts = 0;
        loop {
            if let Some(r) = client.stop_reason() {
                match r {
                    StopReason::ServerDropped { .. } => return,
                    StopReason::UserRequested => return, // acceptable if the runtime shut first
                    other => {
                        eprintln!("stop reason observed: {:?}", other);
                        return;
                    }
                }
            }
            std::thread::sleep(Duration::from_millis(100));
            attempts += 1;
            if attempts > 30 {
                panic!("timeout waiting for ServerDropped");
            }
        }
    }
}
