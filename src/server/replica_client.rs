//! Phase 36-01: replica-mode boot client.
//!
//! When `beava` is launched with `--replica-from HOST:PORT`, this module
//! drives the CDC replay loop:
//!
//!   1. Connect to the upstream cluster.
//!   2. Issue `OP_LOG_FETCH{from_ts_millis, scope}` (Phase 35-01).
//!   3. For every `REPLICA_FRAME_TAG_EVENT` frame, call
//!      [`crate::server::tcp::replica_ingest`] so the event flows through
//!      the same local ingest path a PUSH would take (event log + pipelines).
//!   4. When `REPLICA_FRAME_TAG_END` arrives, fire the `catchup_done`
//!      oneshot so `main.rs` unblocks the TCP + HTTP listener bind.
//!   5. Open a second connection, issue `OP_SUBSCRIBE{scope}`, and keep
//!      applying live events forever.
//!   6. On SUBSCRIBE EOF: reconnect with LOG_FETCH cursored at
//!      `state.replica_last_applied_ts_ms` and then re-SUBSCRIBE.
//!
//! Retry policy per `36-CONTEXT.md §failure policy`:
//!   * LOG_FETCH boot failure: exp-backoff 1s→30s with ±20% jitter, 5
//!     attempts; exhaustion → fatal error propagated to `main.rs`.
//!   * SUBSCRIBE drop: same backoff, up to 10 consecutive failures in
//!     one minute → fatal.
//!
//! The caller (main.rs) is responsible for deciding what "fatal" means
//! (graceful shutdown); this module just returns `Err`.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;

use crate::client::wire::{
    write_scope, Scope as ClientScope, OP_LOG_FETCH, REPLICA_FRAME_TAG_END, REPLICA_FRAME_TAG_EVENT,
};
use crate::server::tcp::{replica_ingest, replica_ingest_batch, SharedState};

/// Max events accumulated before a forced flush in the catchup batch loop.
/// Chosen to keep the batch's log-payload frame under the 1 MiB
/// `O_APPEND`-atomic guard in `event_log::append_many_with_ts` for
/// typical event sizes (~200-800 bytes), and to amortize the engine
/// read lock + syscall cost over enough events that per-event overhead
/// drops into single-digit microseconds. Empirically tuned via
/// benchmark/fork-replay.
const REPLICA_BATCH_FLUSH_SIZE: usize = 1000;

/// Phase 27-02 subscribe opcode — mirrors `src/client/session.rs::OP_SUBSCRIBE`.
const OP_SUBSCRIBE: u8 = 0x11;

/// Max single-frame size allowed from the upstream. Matches the
/// conservative 1 GiB bound used in `client::session`; defence-in-depth
/// against a hostile upstream advertising a giant length prefix.
const HARD_FRAME_LIMIT: u32 = 1024 * 1024 * 1024;

/// Phase 36-01 replica-mode boot configuration.
///
/// Assembled from CLI flags (see `src/main.rs::parse_replica_boot_config`)
/// and handed to `ReplicaClient::new`.
#[derive(Debug, Clone)]
pub struct ReplicaBootConfig {
    /// Upstream cluster endpoint, `HOST:PORT`.
    pub remote: String,
    /// CDC cursor — all events with `timestamp_ms >= since_millis` are pulled.
    pub since_millis: u64,
    /// Streams to replicate.
    pub streams: Vec<String>,
    /// Optional key filter (mutually exclusive with `key_prefix`).
    pub keys: Option<Vec<String>>,
    /// Optional key-prefix filter.
    pub key_prefix: Option<String>,
    /// Admin bearer token for the upstream.
    pub token: String,
    /// When true, TCP+HTTP listeners wait for catchup-done before binding.
    pub block_until_catchup: bool,
    /// Optional REGISTER-JSON payload path to seed pipelines before catchup.
    pub pipeline_file: Option<std::path::PathBuf>,
    /// Phase 44-01: sorted ascending list of unix-millis timestamps at which
    /// the historical-catchup loop should snapshot per-scope-key feature
    /// state. Empty = no historical extraction (default behavior).
    ///
    /// Semantics: maintain a cursor `i = 0`; before applying event E with
    /// `E.ts_ms`, while `i < n && E.ts_ms > extract_at_millis[i]`, snapshot
    /// current state and `i += 1`. After LOG_FETCH END, snapshot any
    /// remaining entries against the final state.
    pub extract_at_millis: Vec<u64>,
}

impl ReplicaBootConfig {
    /// Build the `Scope` this config scopes the replica wire to.
    pub fn to_scope(&self) -> ClientScope {
        ClientScope {
            streams: self.streams.clone(),
            keys: self.keys.clone(),
            key_prefix: self.key_prefix.clone(),
            pull: "all".into(),
        }
    }
}

/// Replica-client error surface. Callers in `main.rs` log + shut down.
#[derive(Debug, thiserror::Error)]
pub enum ReplicaError {
    #[error("connect failed: {0}")]
    ConnectFailed(String),
    #[error("io: {0}")]
    Io(String),
    #[error("protocol: {0}")]
    Protocol(String),
    #[error("unauthorized")]
    Unauthorized,
    #[error("ingest failed: {0}")]
    IngestFailed(String),
    #[error("retry exhausted: {attempt} attempts against {target}")]
    RetryExhausted { target: String, attempt: u32 },
}

impl From<std::io::Error> for ReplicaError {
    fn from(e: std::io::Error) -> Self {
        ReplicaError::Io(e.to_string())
    }
}

/// Runtime handle for the replica boot loop.
pub struct ReplicaClient {
    pub config: ReplicaBootConfig,
    pub app: SharedState,
    /// Fires once when the initial LOG_FETCH catchup has hit END.
    /// `main.rs` awaits this before binding listeners when
    /// `--replica-block-until-catchup=true` (the default).
    pub catchup_done_tx: Option<oneshot::Sender<()>>,
}

impl ReplicaClient {
    pub fn new(
        config: ReplicaBootConfig,
        app: SharedState,
        catchup_done_tx: oneshot::Sender<()>,
    ) -> Self {
        Self {
            config,
            app,
            catchup_done_tx: Some(catchup_done_tx),
        }
    }

    /// Run the boot + tail loop. Returns `Err` on fatal retry-exhaustion
    /// (caller should shut the process down).
    pub async fn run(mut self) -> Result<(), ReplicaError> {
        // 1. Initial catchup via LOG_FETCH.
        self.run_log_fetch_with_retry(self.config.since_millis, /*max_attempts=*/ 5)
            .await?;

        // 2. Fire catchup-done so main.rs can bind listeners.
        if let Some(tx) = self.catchup_done_tx.take() {
            let _ = tx.send(());
        }
        // Intentional: startup status (Phase 47 audit)
        eprintln!(
            "replica caught up to ts_ms={}; opening listeners",
            self.app.replica_last_applied_ts_ms.load(Ordering::Relaxed)
        );

        // 3. SUBSCRIBE tail loop. Per 36-CONTEXT.md §failure policy:
        //    10 consecutive failures in 1 min → fatal.
        let mut consecutive_failures: u32 = 0;
        let mut failure_window_start = Instant::now();
        loop {
            let result = self.run_subscribe_once().await;
            match result {
                Ok(()) => {
                    // Clean EOF — treat as a drop, reconnect.
                    // Intentional: operational status (Phase 47 audit)
                    eprintln!("replica SUBSCRIBE stream closed; reconnecting");
                }
                Err(e) => {
                    // Intentional: operational error (Phase 47 audit)
                    eprintln!("replica SUBSCRIBE error: {}; reconnecting", e);
                }
            }
            // Track consecutive failures in a rolling 60s window.
            let now = Instant::now();
            if now.duration_since(failure_window_start) > Duration::from_secs(60) {
                consecutive_failures = 0;
                failure_window_start = now;
            }
            consecutive_failures += 1;
            if consecutive_failures >= 10 {
                // Intentional: fatal operational error (Phase 47 audit)
                eprintln!(
                    "replica FATAL: {} consecutive SUBSCRIBE failures within 1 minute",
                    consecutive_failures
                );
                return Err(ReplicaError::RetryExhausted {
                    target: self.config.remote.clone(),
                    attempt: consecutive_failures,
                });
            }

            // Before re-SUBSCRIBE, re-catchup from last_applied_ts_ms via LOG_FETCH
            // so we don't miss events that landed during the drop window.
            let cursor = self.app.replica_last_applied_ts_ms.load(Ordering::Relaxed);
            if let Err(e) = self
                .run_log_fetch_with_retry(cursor, /*max_attempts=*/ 5)
                .await
            {
                // Intentional: operational error (Phase 47 audit)
                eprintln!("replica re-catchup LOG_FETCH failed: {}; continuing", e);
            }

            // Backoff before reconnect.
            let delay = backoff_delay(consecutive_failures);
            tokio::time::sleep(delay).await;
        }
    }

    /// Perform LOG_FETCH + retry-with-backoff. Applies events via
    /// `replica_ingest`; returns when END frame is received.
    async fn run_log_fetch_with_retry(
        &mut self,
        from_ts_millis: u64,
        max_attempts: u32,
    ) -> Result<(), ReplicaError> {
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            match self.run_log_fetch_once(from_ts_millis).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    // Intentional: operational error (Phase 47 audit)
                    eprintln!("replica LOG_FETCH attempt {} failed: {}", attempt, e);
                    if attempt >= max_attempts {
                        return Err(ReplicaError::RetryExhausted {
                            target: self.config.remote.clone(),
                            attempt,
                        });
                    }
                    let delay = backoff_delay(attempt);
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    /// Single LOG_FETCH attempt.
    async fn run_log_fetch_once(&mut self, from_ts_millis: u64) -> Result<(), ReplicaError> {
        let mut stream = TcpStream::connect(&self.config.remote)
            .await
            .map_err(|e| ReplicaError::ConnectFailed(format!("{}: {}", self.config.remote, e)))?;

        // Write the OP_LOG_FETCH request frame.
        let frame =
            build_log_fetch_frame(&self.config.token, from_ts_millis, &self.config.to_scope());
        stream.write_all(&frame).await?;
        stream.flush().await?;

        // Phase 44-01: historical-extract cursor. Tracks the next
        // `extract_at_millis[i]` threshold we haven't yet crossed. Before
        // applying each event E with ts_ms, advance the cursor + snapshot
        // for every threshold strictly less than E.ts_ms.
        let mut extract_cursor: usize = 0;
        let extract_at = &self.config.extract_at_millis.clone();

        // Batch accumulator for the catchup hot path. Flushed via
        // `replica_ingest_batch` at `REPLICA_BATCH_FLUSH_SIZE` events, at
        // END-frame, and **before** any extract_at snapshot so the snapshot
        // sees the correct state.
        let mut pending: Vec<(String, u64, Vec<u8>)> = Vec::with_capacity(REPLICA_BATCH_FLUSH_SIZE);

        // Lightweight profiling: accumulate wall-clock spent in each phase
        // of the catchup loop. Printed to stderr on END-frame so the
        // benchmark harness + human reader can see where the 11.5 s goes.
        // Guarded by BEAVA_REPLICA_PROFILE=1 so production replicas don't
        // pay the (tiny) Instant::now overhead — but we take that cost per
        // event intentionally during measurement runs.
        let profile = std::env::var("BEAVA_REPLICA_PROFILE").ok().as_deref() == Some("1");
        let loop_start = std::time::Instant::now();
        let mut t_read_ns: u128 = 0;
        let mut t_parse_ns: u128 = 0;
        let mut t_resolve_ns: u128 = 0;
        let mut t_apply_ns: u128 = 0;
        let mut n_events: u64 = 0;
        let mut n_batches: u64 = 0;
        let mut total_bytes: u64 = 0;

        // Reused frame-body buffer: previously every iteration allocated a
        // fresh `vec![0u8; frame_len]` — 5M allocs over a 5M-event catchup.
        // Resizing this Vec in place lets the allocator reuse the same heap
        // region across the hot loop.
        let mut body: Vec<u8> = Vec::with_capacity(1024);

        // Read response frames until END (or STATUS_ERROR).
        loop {
            let t_read0 = if profile {
                Some(std::time::Instant::now())
            } else {
                None
            };
            let frame_len = read_u32_be(&mut stream).await?;
            if frame_len == 0 || frame_len > HARD_FRAME_LIMIT {
                return Err(ReplicaError::Protocol(format!(
                    "log-fetch frame_len out of range: {}",
                    frame_len
                )));
            }
            body.resize(frame_len as usize, 0);
            stream.read_exact(&mut body).await?;
            if let Some(t0) = t_read0 {
                t_read_ns += t0.elapsed().as_nanos();
                total_bytes += (4 + body.len()) as u64;
            }
            if body.is_empty() {
                return Err(ReplicaError::Protocol("log-fetch frame empty".into()));
            }
            let tag = body[0];
            match tag {
                t if t == REPLICA_FRAME_TAG_END => {
                    // Flush any remaining buffered events before the final
                    // extract_at snapshots so "state-at-end-of-log" is
                    // correct.
                    if !pending.is_empty() {
                        let t_apply0 = if profile {
                            Some(std::time::Instant::now())
                        } else {
                            None
                        };
                        replica_ingest_batch(&self.app, &pending)
                            .map_err(|e| ReplicaError::IngestFailed(e.to_string()))?;
                        if let Some(t0) = t_apply0 {
                            t_apply_ns += t0.elapsed().as_nanos();
                            n_batches += 1;
                        }
                        pending.clear();
                    }
                    // Phase 44-01: snapshot any remaining extract_at entries
                    // that were never crossed during replay. They capture
                    // the state-as-of end-of-log, which is the correct
                    // semantics for "extract_at T in the future" — replay
                    // finishes before T, state-at-end is state-at-T.
                    while extract_cursor < extract_at.len() {
                        self.snapshot_extract(extract_at[extract_cursor]);
                        extract_cursor += 1;
                    }
                    if profile {
                        // Intentional: profile instrumentation (Phase 47 audit)
                        let loop_total = loop_start.elapsed().as_nanos();
                        let other_ns = loop_total
                            .saturating_sub(t_read_ns + t_parse_ns + t_resolve_ns + t_apply_ns);
                        let per_event_ns = if n_events > 0 {
                            (loop_total / n_events as u128) as u64
                        } else {
                            0
                        };
                        // Intentional: profile instrumentation (Phase 47 audit)
                        eprintln!(
                            "[replica-profile] LOG_FETCH loop summary:\n  \
                             events={} batches={} bytes={} wall={:.3}s ({:.0} EPS, {} ns/event, {:.1} MiB/s)\n  \
                             net read   : {:.3}s ({:.1}%)\n  \
                             frame parse: {:.3}s ({:.1}%)\n  \
                             resolve    : {:.3}s ({:.1}%)\n  \
                             apply_batch: {:.3}s ({:.1}%)\n  \
                             other      : {:.3}s ({:.1}%)",
                            n_events, n_batches, total_bytes,
                            loop_total as f64 / 1e9,
                            if loop_total > 0 { (n_events as u128 * 1_000_000_000) as f64 / loop_total as f64 } else { 0.0 },
                            per_event_ns,
                            if loop_total > 0 { (total_bytes as f64) / (loop_total as f64 / 1e9) / (1024.0 * 1024.0) } else { 0.0 },
                            t_read_ns as f64 / 1e9,    100.0 * t_read_ns as f64 / loop_total.max(1) as f64,
                            t_parse_ns as f64 / 1e9,   100.0 * t_parse_ns as f64 / loop_total.max(1) as f64,
                            t_resolve_ns as f64 / 1e9, 100.0 * t_resolve_ns as f64 / loop_total.max(1) as f64,
                            t_apply_ns as f64 / 1e9,   100.0 * t_apply_ns as f64 / loop_total.max(1) as f64,
                            other_ns as f64 / 1e9,     100.0 * other_ns as f64 / loop_total.max(1) as f64,
                        );
                    }
                    return Ok(());
                }
                t if t == REPLICA_FRAME_TAG_EVENT => {
                    let t_parse0 = if profile {
                        Some(std::time::Instant::now())
                    } else {
                        None
                    };
                    // body = [tag][u64 ts_ms][u32 payload_len][payload]
                    if body.len() < 1 + 8 + 4 {
                        return Err(ReplicaError::Protocol(
                            "event frame header truncated".into(),
                        ));
                    }
                    let ts_ms = u64::from_be_bytes([
                        body[1], body[2], body[3], body[4], body[5], body[6], body[7], body[8],
                    ]);
                    let payload_len =
                        u32::from_be_bytes([body[9], body[10], body[11], body[12]]) as usize;
                    if body.len() < 13 + payload_len {
                        return Err(ReplicaError::Protocol(format!(
                            "event frame payload truncated: expected {}, got {}",
                            payload_len,
                            body.len() - 13
                        )));
                    }
                    let payload = &body[13..13 + payload_len];
                    if let Some(t0) = t_parse0 {
                        t_parse_ns += t0.elapsed().as_nanos();
                    }
                    // Phase 44-01: snapshot-before-apply for every cursor
                    // threshold strictly less than this event's ts_ms.
                    // Must flush the pending batch first so the snapshot
                    // sees the state produced by all prior events.
                    while extract_cursor < extract_at.len() && ts_ms > extract_at[extract_cursor] {
                        if !pending.is_empty() {
                            let t_apply0 = if profile {
                                Some(std::time::Instant::now())
                            } else {
                                None
                            };
                            replica_ingest_batch(&self.app, &pending)
                                .map_err(|e| ReplicaError::IngestFailed(e.to_string()))?;
                            if let Some(t0) = t_apply0 {
                                t_apply_ns += t0.elapsed().as_nanos();
                                n_batches += 1;
                            }
                            pending.clear();
                        }
                        self.snapshot_extract(extract_at[extract_cursor]);
                        extract_cursor += 1;
                    }
                    // Resolve the stream up-front (same logic single-event
                    // apply_event uses) so the batch path has everything
                    // it needs without re-acquiring the engine lock.
                    let t_res0 = if profile {
                        Some(std::time::Instant::now())
                    } else {
                        None
                    };
                    let stream_name = self.resolve_stream_for_event(payload)?;
                    if let Some(t0) = t_res0 {
                        t_resolve_ns += t0.elapsed().as_nanos();
                    }
                    pending.push((stream_name, ts_ms, payload.to_vec()));
                    n_events += 1;
                    if pending.len() >= REPLICA_BATCH_FLUSH_SIZE {
                        let t_apply0 = if profile {
                            Some(std::time::Instant::now())
                        } else {
                            None
                        };
                        replica_ingest_batch(&self.app, &pending)
                            .map_err(|e| ReplicaError::IngestFailed(e.to_string()))?;
                        if let Some(t0) = t_apply0 {
                            t_apply_ns += t0.elapsed().as_nanos();
                            n_batches += 1;
                        }
                        pending.clear();
                    }
                }
                0x01 => {
                    // STATUS_ERROR (shared tag value) — body[1..] is the msg.
                    let msg = String::from_utf8_lossy(&body[1..]).to_string();
                    if msg.contains("unauthorized") {
                        return Err(ReplicaError::Unauthorized);
                    }
                    return Err(ReplicaError::Protocol(format!("upstream error: {}", msg)));
                }
                other => {
                    return Err(ReplicaError::Protocol(format!(
                        "unexpected log-fetch tag 0x{:02x}",
                        other
                    )));
                }
            }
        }
    }

    /// Open an OP_SUBSCRIBE connection and apply event frames forever.
    /// Returns Ok(()) on clean EOF (caller decides whether to reconnect).
    async fn run_subscribe_once(&mut self) -> Result<(), ReplicaError> {
        let mut stream = TcpStream::connect(&self.config.remote)
            .await
            .map_err(|e| ReplicaError::ConnectFailed(format!("{}: {}", self.config.remote, e)))?;

        let frame = build_subscribe_frame(&self.config.token, &self.config.to_scope());
        stream.write_all(&frame).await?;
        stream.flush().await?;

        // Event frames: shape is `[u32 len][u8 tag=0x03][u64 secs][u32 nanos]
        // [u32 payload_len][payload]`. Note: OP_SUBSCRIBE uses (secs, nanos)
        // whereas OP_LOG_FETCH uses ts_ms directly. We convert to ms here.
        loop {
            let frame_len = match read_u32_be(&mut stream).await {
                Ok(n) => n,
                Err(_) => {
                    // Connection closed.
                    return Ok(());
                }
            };
            if frame_len == 0 || frame_len > HARD_FRAME_LIMIT {
                return Err(ReplicaError::Protocol(format!(
                    "subscribe frame_len out of range: {}",
                    frame_len
                )));
            }
            let mut body = vec![0u8; frame_len as usize];
            stream.read_exact(&mut body).await?;
            if body.is_empty() {
                return Err(ReplicaError::Protocol("subscribe frame empty".into()));
            }
            let tag = body[0];
            if tag == 0x01 {
                // STATUS_ERROR
                let msg = String::from_utf8_lossy(&body[1..]).to_string();
                if msg.contains("unauthorized") {
                    return Err(ReplicaError::Unauthorized);
                }
                return Err(ReplicaError::Protocol(format!("upstream error: {}", msg)));
            }
            if tag != REPLICA_FRAME_TAG_EVENT {
                return Err(ReplicaError::Protocol(format!(
                    "unexpected subscribe tag 0x{:02x}",
                    tag
                )));
            }
            if body.len() < 1 + 8 + 4 + 4 {
                return Err(ReplicaError::Protocol("subscribe frame truncated".into()));
            }
            let secs = u64::from_be_bytes([
                body[1], body[2], body[3], body[4], body[5], body[6], body[7], body[8],
            ]);
            let nanos = u32::from_be_bytes([body[9], body[10], body[11], body[12]]);
            let payload_len = u32::from_be_bytes([body[13], body[14], body[15], body[16]]) as usize;
            if body.len() < 17 + payload_len {
                return Err(ReplicaError::Protocol("subscribe payload truncated".into()));
            }
            let ts_ms = secs
                .saturating_mul(1_000)
                .saturating_add(nanos as u64 / 1_000_000);
            let payload = &body[17..17 + payload_len];
            self.apply_event(ts_ms, payload)?;
        }
    }

    /// Phase 44-01: capture a per-scope-key snapshot of computed features
    /// into `state.extracted_history[ts_ms]`. Called from the historical
    /// catchup loop just before applying an event whose ts crosses a
    /// configured `--replica-extract-at` threshold.
    ///
    /// Scope iteration: prefers `config.keys` if non-empty, else falls back
    /// to every entity currently in the StateStore. Keys with no features
    /// yet are skipped (consistent with the "missing key → None" semantics).
    fn snapshot_extract(&self, ts_ms: u64) {
        let now = std::time::SystemTime::now();
        let keys: Vec<String> = match &self.config.keys {
            Some(ks) if !ks.is_empty() => ks.clone(),
            _ => self.app.store.entity_keys(),
        };
        // Outer get-or-insert is lock-free per DashMap; inner inserts are
        // per-key and rare (one write per key per extract_at threshold).
        let inner = self.app.extracted_history.entry(ts_ms).or_default();
        for key in keys {
            let feats = self.app.store.get_all_features(&key, now);
            if feats.is_empty() {
                continue;
            }
            // FeatureMap → serde_json::Value::Object keyed by feature name.
            let mut obj = serde_json::Map::new();
            for (fname, fval) in feats.iter() {
                obj.insert(fname.clone(), fval.to_json_value());
            }
            inner.insert(key, serde_json::Value::Object(obj));
        }
    }

    /// Route a single event through the local ingest path.
    fn apply_event(&self, ts_ms: u64, raw_payload: &[u8]) -> Result<(), ReplicaError> {
        // We need the stream name. For LOG_FETCH/SUBSCRIBE frames the body
        // is the on-wire "log payload" = `[fmt_byte][TLV body]`, which does
        // NOT carry the stream name. The stream is implicit — 36-01 replicas
        // subscribe scope-scoped, so we need to figure out which stream each
        // event belongs to.
        //
        // For v0 we take the simplest approach: if there is exactly one
        // stream in the replica scope, every event is attributed to it.
        // If there are multiple streams, we look at the event payload's
        // declared key_field across each candidate stream — whichever
        // stream's `key_field` is present in the decoded payload wins.
        // This is consistent with the Phase 35-01 log-fetch key-extraction
        // path in `handle_log_fetch`.
        let stream_name = self.resolve_stream_for_event(raw_payload)?;

        replica_ingest(&self.app, &stream_name, ts_ms, raw_payload)
            .map_err(|e| ReplicaError::IngestFailed(e.to_string()))
    }

    /// Decode the event enough to find which stream it belongs to.
    ///
    /// If the replica scope has a single stream, trust the scope: every
    /// event on the socket must belong to that stream. Otherwise, decode
    /// the payload and match against each candidate stream's `key_field`.
    fn resolve_stream_for_event(&self, raw_payload: &[u8]) -> Result<String, ReplicaError> {
        if self.config.streams.len() == 1 {
            return Ok(self.config.streams[0].clone());
        }
        // Multi-stream scope: decode + look up key_field for each candidate.
        use crate::state::event_log::{decode_log_payload, LOG_FMT_BINARY, LOG_FMT_JSON};
        let (fmt, body) = decode_log_payload(raw_payload);
        let event_value: serde_json::Value = match fmt {
            LOG_FMT_BINARY => {
                let mut buf = body;
                crate::server::protocol::decode_event_binary(&mut buf)
                    .map_err(|e| ReplicaError::Protocol(format!("decode: {}", e)))?
            }
            LOG_FMT_JSON => serde_json::from_slice(body)
                .map_err(|e| ReplicaError::Protocol(format!("json decode: {}", e)))?,
            _ => return Err(ReplicaError::Protocol("unknown log payload format".into())),
        };
        // Attribute to first stream whose key_field is present as a String.
        let engine = self.app.engine.read();
        for s in &self.config.streams {
            if let Some(def) = engine.get_stream(s) {
                if let Some(kf) = &def.key_field {
                    if let Some(serde_json::Value::String(_)) = event_value.get(kf.as_str()) {
                        return Ok(s.clone());
                    }
                }
            }
        }
        // Fallback: first configured stream.
        Ok(self.config.streams[0].clone())
    }
}

/// Build the OP_LOG_FETCH request frame. Wire shape mirrors
/// `src/server/protocol.rs::parse_command::OP_LOG_FETCH`.
fn build_log_fetch_frame(token: &str, from_ts_millis: u64, scope: &ClientScope) -> Vec<u8> {
    let mut payload = Vec::new();
    let token_bytes = token.as_bytes();
    assert!(token_bytes.len() <= u16::MAX as usize);
    payload.extend_from_slice(&(token_bytes.len() as u16).to_be_bytes());
    payload.extend_from_slice(token_bytes);
    payload.extend_from_slice(&from_ts_millis.to_be_bytes());
    write_scope(&mut payload, scope);
    let total_len = (1 + payload.len()) as u32;
    let mut frame = Vec::with_capacity(4 + total_len as usize);
    frame.extend_from_slice(&total_len.to_be_bytes());
    frame.push(OP_LOG_FETCH);
    frame.extend_from_slice(&payload);
    frame
}

/// Build the OP_SUBSCRIBE request frame — mirrors
/// `src/client/session.rs::build_request_frame`.
fn build_subscribe_frame(token: &str, scope: &ClientScope) -> Vec<u8> {
    let mut payload = Vec::new();
    let token_bytes = token.as_bytes();
    assert!(token_bytes.len() <= u16::MAX as usize);
    payload.extend_from_slice(&(token_bytes.len() as u16).to_be_bytes());
    payload.extend_from_slice(token_bytes);
    write_scope(&mut payload, scope);
    let total_len = (1 + payload.len()) as u32;
    let mut frame = Vec::with_capacity(4 + total_len as usize);
    frame.extend_from_slice(&total_len.to_be_bytes());
    frame.push(OP_SUBSCRIBE);
    frame.extend_from_slice(&payload);
    frame
}

async fn read_u32_be(stream: &mut TcpStream) -> Result<u32, ReplicaError> {
    let mut b = [0u8; 4];
    stream.read_exact(&mut b).await?;
    Ok(u32::from_be_bytes(b))
}

/// Exponential backoff with ±20% jitter, clamped to `[1s, 30s]`.
/// `attempt` starts at 1.
fn backoff_delay(attempt: u32) -> Duration {
    use rand::Rng;
    let base_ms: u64 = 1_000u64.saturating_mul(1u64 << attempt.min(6));
    let clamped = base_ms.clamp(1_000, 30_000);
    let jitter = rand::thread_rng().gen_range(-0.2f64..0.2f64);
    let with_jitter = (clamped as f64 * (1.0 + jitter)).max(100.0) as u64;
    Duration::from_millis(with_jitter)
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn config_single() -> ReplicaBootConfig {
        ReplicaBootConfig {
            remote: "127.0.0.1:1".into(),
            since_millis: 0,
            streams: vec!["orders".into()],
            keys: None,
            key_prefix: None,
            token: "tok".into(),
            block_until_catchup: true,
            pipeline_file: None,
            extract_at_millis: Vec::new(),
        }
    }

    #[test]
    fn to_scope_maps_fields() {
        let mut c = config_single();
        c.keys = Some(vec!["k1".into()]);
        let s = c.to_scope();
        assert_eq!(s.streams, vec!["orders"]);
        assert_eq!(s.keys.as_deref(), Some(&["k1".into()][..]));
        assert_eq!(s.key_prefix, None);
        assert_eq!(s.pull, "all");
    }

    #[test]
    fn build_log_fetch_frame_shape() {
        let scope = config_single().to_scope();
        let frame = build_log_fetch_frame("tokABC", 12345, &scope);
        // [u32 total_len][u8 opcode][u16 token_len][token][u64 ts_ms][scope...]
        let total_len = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]) as usize;
        assert_eq!(total_len, frame.len() - 4);
        assert_eq!(frame[4], OP_LOG_FETCH);
        let token_len = u16::from_be_bytes([frame[5], frame[6]]) as usize;
        assert_eq!(token_len, 6);
        assert_eq!(&frame[7..7 + 6], b"tokABC");
        let ts_ms = u64::from_be_bytes([
            frame[13], frame[14], frame[15], frame[16], frame[17], frame[18], frame[19], frame[20],
        ]);
        assert_eq!(ts_ms, 12345);
    }

    #[test]
    fn build_subscribe_frame_uses_op_subscribe() {
        let scope = config_single().to_scope();
        let frame = build_subscribe_frame("t", &scope);
        assert_eq!(frame[4], OP_SUBSCRIBE);
    }

    #[test]
    fn backoff_delay_within_bounds() {
        // Every attempt in [1..=10] lands inside [0.8s, 36s] (jitter band).
        for attempt in 1u32..=10 {
            let d = backoff_delay(attempt);
            let ms = d.as_millis();
            assert!(ms >= 800, "attempt {}: {}ms below floor", attempt, ms);
            assert!(ms <= 40_000, "attempt {}: {}ms above ceiling", attempt, ms);
        }
    }

    // ---- Mock upstream for full LOG_FETCH loop ----

    async fn serve_fake_log_fetch(listener: tokio::net::TcpListener, event_frames: Vec<Vec<u8>>) {
        let (mut sock, _) = listener.accept().await.unwrap();
        // Drain the request frame (we don't validate in the mock).
        let mut len_buf = [0u8; 4];
        sock.read_exact(&mut len_buf).await.unwrap();
        let total_len = u32::from_be_bytes(len_buf) as usize;
        let mut req = vec![0u8; total_len];
        sock.read_exact(&mut req).await.unwrap();
        // Write all event frames + END.
        for f in event_frames {
            sock.write_all(&f).await.unwrap();
        }
        let end = {
            let mut v = Vec::new();
            v.extend_from_slice(&1u32.to_be_bytes());
            v.push(REPLICA_FRAME_TAG_END);
            v
        };
        sock.write_all(&end).await.unwrap();
        sock.flush().await.unwrap();
    }

    #[tokio::test]
    async fn log_fetch_once_reads_end_frame_only() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve_fake_log_fetch(listener, vec![]));

        let engine = crate::engine::pipeline::PipelineEngine::new();
        let state = crate::server::tcp::make_concurrent_state(
            engine,
            crate::state::store::StateStore::new(),
            None,
            std::path::PathBuf::from("/tmp/__replica_test.snap"),
            std::sync::Arc::new(crate::server::tcp::BackfillTracker::default()),
            false,
            false,
        );

        let mut cfg = config_single();
        cfg.remote = addr.to_string();
        let (tx, _rx) = oneshot::channel();
        let mut client = ReplicaClient::new(cfg, state, tx);
        client.run_log_fetch_once(0).await.expect("END frame");
    }
}
