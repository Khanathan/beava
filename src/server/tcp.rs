//! TCP server: listener, connection handler, command dispatch, MSET cooperative yielding.
//!
//! SharedState wraps PipelineEngine + StateStore in Arc<Mutex<AppState>>.
//! Synchronous commands (PUSH, GET, SET, REGISTER) lock, process, unlock with no .await.
//! MSET releases the lock between 1024-key chunks and calls yield_now().
//!
//! Phase 12: server-side async push coalescing (PERF-03). Per-connection
//! `ConnAccumulator` buffers `OP_PUSH_ASYNC` frames up to N=64 or a 200µs
//! deadline and then dispatches them through `handle_push_batch` under a
//! single state lock. See .planning/phases/12-server-side-async-push-coalescing
//! CONTEXT.md decisions D-01..D-20 and pitfalls C-2/C-7/H-2.

// Phase 12 C-7 gate: holding a std::MutexGuard across an .await inside
// handle_connection (or anywhere else reached by its call graph) is a
// compile-time error. Do NOT remove this without a documented deviation.
#![deny(clippy::await_holding_lock)]

use std::sync::Arc;
use std::time::SystemTime;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use parking_lot::{Mutex as PLMutex, RwLock};

use crate::engine::pipeline::PipelineEngine;
use crate::error::BeavaError;
use crate::server::protocol::{self, Command, STATUS_ERROR, STATUS_OK};
use crate::state::event_log::{EventLog, LogEntry};
use crate::state::store::StateStore;
use crate::types::{feature_map_to_json, FeatureValue};

/// Operational metrics exposed via GET /metrics.
/// Updated in-place by command handlers.
#[derive(Debug, Default)]
pub struct Metrics {
    pub events_total: u64,
    pub push_latency_seconds: f64, // Last observed PUSH latency
    pub snapshot_duration_ms: u64,
    /// Phase 15: number of snapshot cycles skipped because a previous write was
    /// still in progress.
    pub snapshots_skipped: u64,
    /// Phase 25-02: per-stream history-log compaction counter. Bumped once per
    /// successful compaction that removed ≥1 entry. Exposed as
    /// `beava_history_compacted_total{stream}`.
    pub history_compacted_total: std::collections::HashMap<String, u64>,
    /// Phase 25-02: per-stream backfill-miss counter. Bumped when a backfill
    /// read returned an entry set that straddled the compaction floor (some
    /// events are guaranteed missing). Exposed as
    /// `beava_history_backfill_misses_total{stream}`.
    pub history_backfill_misses_total: std::collections::HashMap<String, u64>,
    /// Phase 25-02: largest observed backfill span per stream in seconds.
    /// Exposed as `beava_max_backfill_span_seen{stream}`.
    pub max_backfill_span_seen: std::collections::HashMap<String, u64>,
}

/// Status of a single backfill task.
#[derive(Debug)]
pub struct BackfillStatus {
    pub stream: String,
    pub features: Vec<String>,
    pub total_events: usize,
    pub processed_events: Arc<AtomicUsize>,
    pub started_at: SystemTime,
    pub completed_at: std::sync::Mutex<Option<SystemTime>>,
}

/// Tracks all active and recently completed backfill tasks.
#[derive(Debug, Default)]
pub struct BackfillTracker {
    pub tasks: std::sync::Mutex<Vec<Arc<BackfillStatus>>>,
}

/// Phase 14: Concurrent application state (D-01). Replaces the global
/// `Arc<Mutex<AppState>>`. Each field is independently lockable — no single
/// lock serializes all connections.
///
/// - `engine`: `RwLock` — many concurrent reads on hot path (PUSH/GET),
///   write only on REGISTER (D-04).
/// - `store`: `PLMutex` — separate from engine, metrics, throughput, latency.
///   Handlers that need the store acquire only this lock.
/// - `event_log`: `Option<Arc<EventLog>>` — set once at startup, never
///   replaced. `EventLog` itself uses `DashMap<stream, Mutex<BufWriter>>`
///   so different streams write in parallel (Phase 40).
/// - `metrics`, `throughput`, `latency`: each behind their own small `PLMutex`.
/// - `snapshot_*`, `backfill_*`: each behind their own small `PLMutex`.
pub struct ConcurrentAppState {
    /// Stream definitions — RwLock: many concurrent reads on hot path,
    /// write only on REGISTER (D-04).
    pub engine: RwLock<PipelineEngine>,

    /// Entity + static feature state — DashMap provides per-key concurrency,
    /// no outer lock needed.
    pub store: StateStore,

    /// Optional event log. `Arc<EventLog>` because the log uses interior
    /// mutability (per-stream DashMap + per-writer mutex, Phase 40). Set once
    /// at startup, never replaced — no outer lock needed.
    pub event_log: Option<Arc<EventLog>>,

    /// Operational metrics — small independent lock.
    pub metrics: PLMutex<Metrics>,

    /// Snapshot file path (immutable after startup).
    pub snapshot_path: std::path::PathBuf,

    /// Phase 9: Snapshot coordination — small independent lock.
    pub snapshot_cycle: PLMutex<u64>,
    pub snapshot_seq: PLMutex<u64>,
    pub last_base_seq: PLMutex<u64>,
    pub previous_base_seq: PLMutex<u64>,

    /// Backfill tracking.
    pub backfill_tracker: Arc<BackfillTracker>,
    pub backfill_complete: PLMutex<HashSet<(String, String)>>,

    /// Phase 10 DBUI-02: per-stream EWMA throughput tracker.
    pub throughput: PLMutex<crate::server::throughput::ThroughputTracker>,

    /// Phase 10.2 DBUI-07: per-command and per-stream latency histograms.
    pub latency: PLMutex<crate::server::latency::LatencyTracker>,

    /// Whether snapshot persistence is enabled (BEAVA_SNAPSHOT env var).
    pub snapshot_enabled: bool,

    /// Whether the event log is enabled (BEAVA_EVENT_LOG env var).
    pub event_log_enabled: bool,

    /// Phase 15: cycle guard — true while a snapshot write is in progress.
    /// Prevents overlapping writes when the timer fires faster than I/O completes.
    pub snapshot_in_progress: AtomicBool,

    /// Phase 20: optional bearer token for admin HTTP routes. Loaded from
    /// `BEAVA_ADMIN_TOKEN` at startup. If `None`, non-loopback admin requests
    /// are always rejected (403).
    pub admin_token: Option<String>,

    /// Phase 20: instant the server started. Used by `/public/stats` to
    /// report uptime.
    pub started_at: std::time::Instant,

    /// Phase 20: bounded ring buffer of the most recent PUSH events for the
    /// public `/public/recent-events` endpoint.
    ///
    /// Phase 41-01 T1: gated behind `feature = "demo"` — the ring's per-push
    /// `PLMutex` insert was ~12% of futex time in the 8-proc 8-stream bench.
    /// Default server build omits this field + its push site + its endpoint.
    #[cfg(feature = "demo")]
    pub recent_events: PLMutex<RecentEventsRing>,

    /// Phase 20: when true, `GET /` serves the public demo page; when false
    /// it serves the debug UI. Set via `--public-mode` / `BEAVA_PUBLIC_MODE`.
    pub public_mode: bool,

    /// Phase 25-02: per-Table eviction tracker — bloom filter of recently-
    /// evicted keys plus per-Table eviction and eviction-then-reinit counters.
    /// Drives the `/debug/config-recommendations` endpoint and
    /// `beava_ttl_evictions_total` / `beava_ttl_eviction_then_reinit_total`
    /// on `/metrics`.
    pub eviction_tracker: Arc<crate::state::eviction_tracker::EvictionTracker>,

    /// Phase 25-02: shared signal bus backing `/debug/warnings`. All warning
    /// sources (REGISTER failures, snapshot failures, memory pressure,
    /// late-drop rate, perf SLO, config recommendations from 25-03) emit
    /// into this registry via [`crate::server::signals::SignalRegistry::record`].
    /// In-memory only; restart loses history. See `src/server/signals.rs`.
    pub signals: crate::server::signals::SharedRegistry,

    /// Phase 27-02: process-wide replica-subscriber registry. Lock-free
    /// DashMap<conn_id, ReplicaSession> — the ingest hook iterates this
    /// registry on every successful push and the TCP dispatcher inserts /
    /// removes entries for live `OP_SUBSCRIBE` sessions. Shared with the
    /// pipeline engine via `PipelineEngine::install_subscribers`.
    pub subscriber_registry: Arc<crate::server::replica::SubscriberRegistry>,

    /// Phase 36-01: when `true`, this server is running as a replica (boot
    /// launched with `--replica-from ...`). Gates local PUSH handling
    /// (rejected) and flips ingest routing so CDC-replay events bypass the
    /// subscriber-notify hook. See `src/server/replica_client.rs`.
    pub replica_mode: AtomicBool,

    /// Phase 36-01: monotonic `last_applied_event_timestamp_ms` for the
    /// replica client's SUBSCRIBE reconnect cursor. Updated by every
    /// successful `replica_ingest` call; read by `ReplicaClient::run` when
    /// it needs to issue a resume LOG_FETCH after a SUBSCRIBE drop. When
    /// `replica_mode` is false, this value is unused and stays at 0.
    pub replica_last_applied_ts_ms: std::sync::atomic::AtomicU64,

    /// Phase 41-01 T2: lock-free per-push event counter. Replaces the
    /// `events_total` field that used to live inside `Metrics` behind a
    /// `PLMutex`. Bumped on every successful PUSH (single + batch paths);
    /// read by `/metrics` and `/public/stats`. Other `Metrics` fields are
    /// rarely written and stay behind the mutex.
    pub events_total: std::sync::atomic::AtomicU64,

    /// Phase 45-04 A5: per-protocol event counters for the dual-emit
    /// `beava_events_total{proto="http"|"tcp"}` labeled series. Bumped by HTTP
    /// handlers (`events_http`) and TCP push sites (`events_tcp`). The unlabeled
    /// `events_total` is preserved for backward compat and equals their sum.
    /// TODO(gh-TBD): remove unlabeled beava_events_total and these fields if
    /// dashboards have migrated to the labeled series — tracked for v1.0-launch follow-up.
    pub events_http: std::sync::atomic::AtomicU64,
    /// Phase 45-04 A5: TCP-originated event counter. See `events_http` doc.
    pub events_tcp: std::sync::atomic::AtomicU64,

    /// Phase 41-01 T2: last observed PUSH latency, in nanoseconds. Replaces
    /// `Metrics::push_latency_seconds` which used to live inside the mutex
    /// and was rewritten on every successful PUSH. Stored as nanos to keep
    /// a `u64` atomic; `/metrics` divides by 1e9 for the seconds output.
    pub last_push_latency_nanos: std::sync::atomic::AtomicU64,

    /// Phase 41-01 T3: lock-free rolling-window EPS counter used on the
    /// hot PUSH path and exposed via `/metrics` and `/public/stats` as the
    /// global 5-second EPS rate. Replaces the per-push bump into the
    /// `PLMutex<ThroughputTracker>`. The per-stream EWMA tracker
    /// (`self.throughput`) stays live for `/debug/throughput` but is no
    /// longer touched by the hot path.
    pub atomic_throughput: crate::server::throughput::AtomicThroughput,

    /// Phase 41-01 T4: sampling counter for the latency histogram. Every
    /// PUSH bumps this `Relaxed`; only 1-in-`LATENCY_SAMPLE_STRIDE`
    /// actually locks `self.latency` and records. Histogram shape is
    /// preserved; lock-acquire rate drops ~94%.
    pub latency_sample_counter: std::sync::atomic::AtomicU64,

    /// Phase 44-01: historical-extraction registry. Populated by
    /// `ReplicaClient::snapshot_extract` during historical catchup when
    /// `--replica-extract-at T1,T2,...` is set. Outer key is the requested
    /// unix-millis timestamp (sorted ascending by the client); inner map
    /// is `entity_key -> {feature_name: feature_value}` JSON. Exposed via
    /// `GET /extracts`. Empty for non-replica servers and for replicas
    /// launched without `--replica-extract-at`.
    pub extracted_history: dashmap::DashMap<u64, dashmap::DashMap<String, serde_json::Value>>,
}

/// Phase 41-01 T4: only every Nth PUSH records into the latency histogram.
/// Preserves p50/p99 shape while reducing per-push lock acquisitions by
/// ~(N-1)/N. Must be a power of two for the `& (STRIDE-1)` fast-path, but
/// `% STRIDE` is also fine and keeps the code readable.
pub const LATENCY_SAMPLE_STRIDE: u64 = 16;

/// Phase 20: a single entry in the `/public/recent-events` feed.
///
/// Phase 41-01 T1: gated behind `feature = "demo"`.
#[cfg(feature = "demo")]
#[derive(Debug, Clone)]
pub struct RecentEvent {
    pub ts_ms: u64,
    pub stream: String,
    pub key: String,
    pub payload_preview: String,
}

/// Phase 20: bounded in-memory ring of the last `CAPACITY` PUSH events. Used
/// by the public read-only `/public/recent-events` endpoint so the demo page
/// has something alive to render without exposing the full event log.
///
/// Phase 41-01 T1: gated behind `feature = "demo"`.
#[cfg(feature = "demo")]
pub struct RecentEventsRing {
    buf: std::collections::VecDeque<RecentEvent>,
    capacity: usize,
}

#[cfg(feature = "demo")]
impl RecentEventsRing {
    pub const CAPACITY: usize = 100;
    pub const PAYLOAD_PREVIEW_MAX: usize = 200;

    pub fn new() -> Self {
        Self {
            buf: std::collections::VecDeque::with_capacity(Self::CAPACITY),
            capacity: Self::CAPACITY,
        }
    }

    /// Push a new event, evicting the oldest if at capacity.
    pub fn push(&mut self, ev: RecentEvent) {
        if self.buf.len() == self.capacity {
            self.buf.pop_front();
        }
        self.buf.push_back(ev);
    }

    /// Return up to `limit` most-recent events, newest first.
    pub fn snapshot(&self, limit: usize) -> Vec<RecentEvent> {
        let n = limit.min(self.buf.len());
        self.buf.iter().rev().take(n).cloned().collect()
    }
}

#[cfg(feature = "demo")]
impl Default for RecentEventsRing {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared state handle for concurrent connection handlers.
/// Phase 14: `Arc<ConcurrentAppState>` — each field independently lockable.
pub type SharedState = Arc<ConcurrentAppState>;

/// Helper: create a `SharedState` with the given initial values.
/// Replaces the old `Arc::new(Mutex::new(AppState { ... }))` pattern.
pub fn make_concurrent_state(
    engine: PipelineEngine,
    store: StateStore,
    event_log: Option<EventLog>,
    snapshot_path: std::path::PathBuf,
    backfill_tracker: Arc<BackfillTracker>,
    snapshot_enabled: bool,
    event_log_enabled: bool,
) -> SharedState {
    make_concurrent_state_full(
        engine,
        store,
        event_log,
        snapshot_path,
        backfill_tracker,
        snapshot_enabled,
        event_log_enabled,
        None,
        false,
    )
}

/// Phase 20: full constructor that accepts the admin token and public-mode
/// flag. The legacy `make_concurrent_state` delegates here with `None`/`false`
/// so existing callers keep working.
#[allow(clippy::too_many_arguments)]
pub fn make_concurrent_state_full(
    engine: PipelineEngine,
    store: StateStore,
    event_log: Option<EventLog>,
    snapshot_path: std::path::PathBuf,
    backfill_tracker: Arc<BackfillTracker>,
    snapshot_enabled: bool,
    event_log_enabled: bool,
    admin_token: Option<String>,
    public_mode: bool,
) -> SharedState {
    let signals = crate::server::signals::SignalRegistry::new_default().into_shared();
    let subscriber_registry = Arc::new(crate::server::replica::SubscriberRegistry::new(
        signals.clone(),
    ));
    // Phase 27-02: wire the registry into the pipeline engine so the ingest
    // hot path can fire `notify_subscribers` without needing a reference
    // through the server AppState.
    let mut engine = engine;
    engine.install_subscribers(Arc::clone(&subscriber_registry));
    Arc::new(ConcurrentAppState {
        engine: RwLock::new(engine),
        store,
        event_log: event_log.map(Arc::new),
        metrics: PLMutex::new(Metrics::default()),
        snapshot_path,
        snapshot_cycle: PLMutex::new(0),
        snapshot_seq: PLMutex::new(1),
        last_base_seq: PLMutex::new(0),
        previous_base_seq: PLMutex::new(0),
        backfill_tracker,
        backfill_complete: PLMutex::new(HashSet::new()),
        throughput: PLMutex::new(crate::server::throughput::ThroughputTracker::new()),
        latency: PLMutex::new(crate::server::latency::LatencyTracker::new()),
        snapshot_enabled,
        event_log_enabled,
        snapshot_in_progress: AtomicBool::new(false),
        admin_token,
        started_at: std::time::Instant::now(),
        #[cfg(feature = "demo")]
        recent_events: PLMutex::new(RecentEventsRing::new()),
        public_mode,
        eviction_tracker: Arc::new(crate::state::eviction_tracker::EvictionTracker::new()),
        signals,
        subscriber_registry,
        replica_mode: AtomicBool::new(false),
        replica_last_applied_ts_ms: std::sync::atomic::AtomicU64::new(0),
        events_total: std::sync::atomic::AtomicU64::new(0),
        events_http: std::sync::atomic::AtomicU64::new(0), // Phase 45-04 A5
        events_tcp: std::sync::atomic::AtomicU64::new(0),  // Phase 45-04 A5
        last_push_latency_nanos: std::sync::atomic::AtomicU64::new(0),
        atomic_throughput: crate::server::throughput::AtomicThroughput::new(),
        latency_sample_counter: std::sync::atomic::AtomicU64::new(0),
        extracted_history: dashmap::DashMap::new(),
    })
}

/// Start the TCP server on the given address. Loops forever accepting connections.
pub async fn run_tcp_server(addr: &str, state: SharedState) -> Result<(), std::io::Error> {
    let listener = TcpListener::bind(addr).await?;
    run_tcp_server_with_listener(listener, state).await
}

/// Start the TCP server from a pre-bound listener (for tests with random ports).
pub async fn run_tcp_server_with_listener(
    listener: TcpListener,
    state: SharedState,
) -> Result<(), std::io::Error> {
    loop {
        let (stream, _addr) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(_e) = handle_connection(stream, state).await {
                // Connection closed or error -- debug log only
            }
        });
    }
}

/// Public wrapper for handle_connection, for integration tests.
pub async fn handle_connection_public(
    stream: TcpStream,
    state: SharedState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    handle_connection(stream, state).await
}

/// Handle a single persistent TCP connection: read frames in a loop,
/// dispatch commands.
///
/// Phase 12 rewrite:
/// - The read loop is now a `tokio::select! { biased; read | deadline }`.
/// - `OP_PUSH_ASYNC` frames accumulate into a stack-local `ConnAccumulator`.
/// - The accumulator is flushed via `handle_push_batch` when:
///   (a) it hits `BATCH_SIZE` events,
///   (b) its deadline elapses (200µs armed on first buffered event),
///   (c) any non-async opcode arrives (sync force-flush, pitfall H-2),
///   (d) the client disconnects.
/// - Per-event errors from the batch path are surfaced on the NEXT sync
///   call (or on clean disconnect), in per-connection `seq` order (C-2).
/// - The `BufWriter` I-3 invariant is preserved: every byte written is
///   followed by an explicit `flush` in the same loop iteration, except
///   for the zero-byte async-push success path.
async fn handle_connection(
    stream: TcpStream,
    state: SharedState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (reader, writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut writer = BufWriter::new(writer);

    let mut accumulator = ConnAccumulator::new();
    // Per-connection drain queue for async push errors. (seq, err_string).
    // Sorted by seq and flushed before the next sync response (D-13/D-14).
    let mut pending_drain: Vec<(u64, String)> = Vec::new();

    // Helper: flush accumulator batch, collect errors into drain queue.
    #[inline(always)]
    fn flush_batch_to_drain(
        state: &SharedState,
        accumulator: &mut ConnAccumulator,
        pending_drain: &mut Vec<(u64, String)>,
    ) {
        let batch = accumulator.drain();
        let results = handle_push_batch(state, &batch);
        let n_ok = results.iter().filter(|r| r.is_ok()).count() as u64;
        // Phase 45-04 A5: TCP async-batch path — bump labeled counter.
        if n_ok > 0 {
            state
                .events_tcp
                .fetch_add(n_ok, std::sync::atomic::Ordering::Relaxed);
        }
        for (ev, res) in batch.iter().zip(results.iter()) {
            if let Err(err) = res {
                pending_drain.push((ev.seq, err.to_string()));
            }
        }
    }

    // Helper: read one frame, parse command, return it. Returns None on
    // clean disconnect (UnexpectedEof).
    async fn read_one_frame(
        reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
    ) -> Result<Option<(usize, Command)>, Box<dyn std::error::Error + Send + Sync>> {
        let len = match reader.read_u32().await {
            Ok(len) => len as usize,
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        if len == 0 || len > 64 * 1024 * 1024 {
            return Err(format!("invalid frame length: {}", len).into());
        }
        let opcode = reader.read_u8().await?;
        let payload_len = len - 1;
        let mut payload = vec![0u8; payload_len];
        if payload_len > 0 {
            reader.read_exact(&mut payload).await?;
        }
        let cmd = protocol::parse_command(opcode, &payload)
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
        Ok(Some((len, cmd)))
    }

    loop {
        // ================================================================
        // PHASE 1: read frames in a tight loop (no select! overhead).
        //
        // When the accumulator is empty, we do a blocking read_u32 — no
        // deadline to race against. When the accumulator has frames, we
        // keep reading in a tight loop until either (a) the accumulator
        // is full, (b) we get a non-async command, or (c) the BufReader's
        // internal buffer is empty (meaning the next read would have to
        // wait for the OS, at which point we fall through to the select!
        // deadline path).
        //
        // This eliminates the select! macro overhead from the hot path
        // under sustained single-client async load, where the TCP receive
        // buffer typically has multiple frames queued.
        // ================================================================

        // First frame: always a blocking read (no deadline needed when
        // accumulator is empty, or we just flushed a batch).
        let frame = if accumulator.is_empty() {
            // No deadline to race — read directly.
            match read_one_frame(&mut reader).await {
                Ok(Some(frame)) => frame,
                Ok(None) => return Ok(()), // clean disconnect, accumulator empty
                Err(e) => {
                    // Frame error (invalid length, parse error, etc.) — send
                    // error response and close connection, matching the
                    // original inline error handling behavior.
                    let resp = protocol::encode_response(STATUS_ERROR, e.to_string().as_bytes());
                    writer.write_all(&resp).await?;
                    writer.flush().await?;
                    return Ok(());
                }
            }
        } else {
            // Accumulator has frames — use select! to race read vs deadline.
            let deadline_opt = accumulator.deadline();

            enum FrameOrDeadline {
                Frame(Result<Option<(usize, Command)>, Box<dyn std::error::Error + Send + Sync>>),
                Deadline,
            }

            let next = tokio::select! {
                biased;
                read_result = read_one_frame(&mut reader) => {
                    FrameOrDeadline::Frame(read_result)
                }
                _ = async {
                    match deadline_opt {
                        Some(d) => tokio::time::sleep_until(d).await,
                        None => std::future::pending::<()>().await,
                    }
                }, if deadline_opt.is_some() => {
                    FrameOrDeadline::Deadline
                }
            };

            match next {
                FrameOrDeadline::Deadline => {
                    flush_batch_to_drain(&state, &mut accumulator, &mut pending_drain);
                    continue;
                }
                FrameOrDeadline::Frame(Ok(Some(frame))) => frame,
                FrameOrDeadline::Frame(Ok(None)) => {
                    // Clean disconnect with buffered frames — flush first.
                    flush_batch_to_drain(&state, &mut accumulator, &mut pending_drain);
                    flush_drain(&mut writer, &mut pending_drain).await?;
                    return Ok(());
                }
                FrameOrDeadline::Frame(Err(e)) => {
                    // Frame error (invalid length, etc.) — flush accumulator,
                    // send error, close connection.
                    if !accumulator.is_empty() {
                        flush_batch_to_drain(&state, &mut accumulator, &mut pending_drain);
                    }
                    let resp = protocol::encode_response(STATUS_ERROR, e.to_string().as_bytes());
                    writer.write_all(&resp).await?;
                    writer.flush().await?;
                    return Ok(());
                }
            }
        };

        // Process the frame we just read.
        let (_len, cmd) = frame;

        // Phase 12 H-2: any non-async opcode force-flushes the
        // accumulator BEFORE the sync handler runs, so the sync
        // response observes all buffered async mutations.
        let is_async_push = matches!(&cmd, Command::PushAsync { .. });
        if !is_async_push && !accumulator.is_empty() {
            flush_batch_to_drain(&state, &mut accumulator, &mut pending_drain);
        }

        // Async push: accumulate, maybe auto-flush at BATCH_SIZE.
        if let Command::PushAsync {
            stream_name,
            payload,
            raw_payload,
        } = cmd
        {
            accumulator.push(stream_name, payload, raw_payload, SystemTime::now());
            if accumulator.is_full() {
                flush_batch_to_drain(&state, &mut accumulator, &mut pending_drain);
            }

            // PHASE 2: tight inner loop — keep reading async frames from
            // the BufReader without going through select!. Under sustained
            // load the BufReader's internal buffer typically contains many
            // queued frames; reading them here avoids select! overhead per
            // frame. We break out when: accumulator is full (flush first),
            // BufReader buffer is empty (need to wait for OS → fall through
            // to select! path on next outer loop iteration), or a non-async
            // frame arrives (process via the outer loop's sync path).
            while !accumulator.is_full() {
                // Check if the BufReader has data in its internal buffer.
                // If the buffer is empty, the next read_u32 would block
                // waiting for the OS, so we break out to the select! path
                // which can race against the deadline.
                if reader.buffer().len() < 4 {
                    break;
                }

                // We have data — read the next frame directly (no select!).
                match read_one_frame(&mut reader).await {
                    Ok(Some((
                        _len,
                        Command::PushAsync {
                            stream_name,
                            payload,
                            raw_payload,
                        },
                    ))) => {
                        accumulator.push(stream_name, payload, raw_payload, SystemTime::now());
                    }
                    Ok(Some(frame)) => {
                        // Non-async frame — flush accumulator, then process
                        // it in the outer loop's sync path. We can't easily
                        // "put it back" so we handle inline.
                        flush_batch_to_drain(&state, &mut accumulator, &mut pending_drain);

                        // Process the non-async command inline (same logic
                        // as the sync dispatch below).
                        let (_len2, cmd2) = frame;
                        // Fall through to handle below by re-entering the
                        // command processing. For simplicity, we break and
                        // handle in the next iteration — but we'd lose the
                        // frame. Instead, handle inline:
                        let cmd_start = std::time::Instant::now();
                        let is_mset = matches!(&cmd2, Command::Mset { .. });
                        let response: Result<Option<Vec<u8>>, BeavaError> = match cmd2 {
                            Command::Mset { entries } => {
                                handle_mset(entries, &state).await.map(Some)
                            }
                            Command::Flush => Ok(Some(Vec::new())),
                            Command::PushAsync { .. } => unreachable!(),
                            Command::PushBatch {
                                stream_name,
                                batch_id,
                                events,
                            } => {
                                // Accumulator already flushed above (line 331).
                                let base_seq = accumulator.advance_seq(events.len() as u64);
                                let now = SystemTime::now();
                                let batch: Vec<PendingAsync> = events
                                    .into_iter()
                                    .enumerate()
                                    .map(|(i, (payload, raw_payload))| {
                                        PendingAsync::new(
                                            base_seq + i as u64,
                                            stream_name.clone(),
                                            payload,
                                            raw_payload,
                                            now,
                                        )
                                    })
                                    .collect();
                                let results = handle_push_batch(&state, &batch);
                                // Phase 45-04 A5: TCP MSET-batch path — bump labeled counter.
                                let n_ok_tcp = results.iter().filter(|r| r.is_ok()).count() as u64;
                                if n_ok_tcp > 0 {
                                    state
                                        .events_tcp
                                        .fetch_add(n_ok_tcp, std::sync::atomic::Ordering::Relaxed);
                                }
                                for (i, (ev, res)) in batch.iter().zip(results.iter()).enumerate() {
                                    if let Err(err) = res {
                                        pending_drain.push((
                                            ev.seq,
                                            format!("[batch:{} event:{}] {}", batch_id, i, err),
                                        ));
                                    }
                                }
                                // Fire-and-forget: no response frame.
                                break;
                            }
                            other => handle_sync_command(other, &state).map(Some),
                        };
                        if is_mset {
                            let mset_us = cmd_start.elapsed().as_secs_f64() * 1_000_000.0;
                            let mut latency = state.latency.lock();
                            latency.record_command(
                                crate::server::latency::CommandKind::Mset,
                                mset_us,
                                std::time::Instant::now(),
                            );
                            if latency.slow_queries_would_accept(
                                crate::server::latency::CommandKind::Mset,
                                mset_us,
                            ) {
                                latency.maybe_record_slow(
                                    crate::server::latency::CommandKind::Mset,
                                    None,
                                    mset_us,
                                    String::new(),
                                );
                            }
                        }
                        flush_drain(&mut writer, &mut pending_drain).await?;
                        match response {
                            Ok(Some(resp_bytes)) => {
                                let resp = protocol::encode_response(STATUS_OK, &resp_bytes);
                                writer.write_all(&resp).await?;
                                writer.flush().await?;
                            }
                            Ok(None) => {}
                            Err(e) => {
                                let resp = protocol::encode_response(
                                    STATUS_ERROR,
                                    e.to_string().as_bytes(),
                                );
                                writer.write_all(&resp).await?;
                                writer.flush().await?;
                            }
                        }
                        break;
                    }
                    Ok(None) => {
                        // Clean disconnect while reading tight loop.
                        if !accumulator.is_empty() {
                            flush_batch_to_drain(&state, &mut accumulator, &mut pending_drain);
                        }
                        flush_drain(&mut writer, &mut pending_drain).await?;
                        return Ok(());
                    }
                    Err(e) => {
                        if !accumulator.is_empty() {
                            flush_batch_to_drain(&state, &mut accumulator, &mut pending_drain);
                        }
                        let resp =
                            protocol::encode_response(STATUS_ERROR, e.to_string().as_bytes());
                        writer.write_all(&resp).await?;
                        writer.flush().await?;
                        return Ok(());
                    }
                }
            }

            // If accumulator became full during tight loop, flush it.
            if accumulator.is_full() {
                flush_batch_to_drain(&state, &mut accumulator, &mut pending_drain);
            }
            continue;
        }

        // Phase 27-01: OP_SNAPSHOT_FETCH has a two-frame response shape
        // (header + payload), so it bypasses the STATUS_OK envelope used
        // by every other sync command. Handle it inline and continue the
        // read loop — the handler writes both response frames itself and
        // may also write a single STATUS_ERROR envelope on validation /
        // auth failure.
        if let Command::SnapshotFetch { admin_token, scope } = cmd {
            flush_drain(&mut writer, &mut pending_drain).await?;
            if let Err(e) = handle_snapshot_fetch(&mut writer, &admin_token, scope, &state).await {
                let resp = protocol::encode_response(STATUS_ERROR, e.to_string().as_bytes());
                writer.write_all(&resp).await?;
                writer.flush().await?;
            }
            continue;
        }

        // Phase 27-02: OP_SUBSCRIBE takes ownership of the connection for
        // the lifetime of the subscription (per user direction §1). Any
        // accumulated async-push errors are flushed first, then
        // `handle_subscribe` runs the subscribe loop inline (select over
        // notification-drain and reader-EOF). When it returns, the
        // connection is done — we return from `handle_connection` rather
        // than re-entering the main read loop.
        if let Command::Subscribe { admin_token, scope } = cmd {
            flush_drain(&mut writer, &mut pending_drain).await?;
            handle_subscribe(&mut reader, &mut writer, &admin_token, scope, &state).await?;
            return Ok(());
        }

        // Phase 35-01: OP_LOG_FETCH has a multi-frame response shape
        // (N × event frame + one END frame), so it bypasses the
        // STATUS_OK envelope just like SNAPSHOT_FETCH. Handle inline and
        // continue the read loop; the handler writes all response frames
        // itself, and on auth/validation failure emits a STATUS_ERROR
        // envelope (no event/end frames follow).
        if let Command::LogFetch {
            admin_token,
            from_ts_millis,
            scope,
        } = cmd
        {
            flush_drain(&mut writer, &mut pending_drain).await?;
            if let Err(e) =
                handle_log_fetch(&mut writer, &admin_token, from_ts_millis, scope, &state).await
            {
                let resp = protocol::encode_response(STATUS_ERROR, e.to_string().as_bytes());
                writer.write_all(&resp).await?;
                writer.flush().await?;
            }
            continue;
        }

        // Sync dispatch path. cmd is one of Mset/Flush/other.
        let cmd_start = std::time::Instant::now();
        let is_mset = matches!(&cmd, Command::Mset { .. });
        let response: Result<Option<Vec<u8>>, BeavaError> = match cmd {
            Command::Mset { entries } => handle_mset(entries, &state).await.map(Some),
            Command::Flush => Ok(Some(Vec::new())),
            Command::PushAsync { .. } => unreachable!("handled above"),
            Command::PushBatch {
                stream_name,
                batch_id,
                events,
            } => {
                // Accumulator already force-flushed above (H-2).
                let base_seq = accumulator.advance_seq(events.len() as u64);
                let now = SystemTime::now();
                let batch: Vec<PendingAsync> = events
                    .into_iter()
                    .enumerate()
                    .map(|(i, (payload, raw_payload))| {
                        PendingAsync::new(
                            base_seq + i as u64,
                            stream_name.clone(),
                            payload,
                            raw_payload,
                            now,
                        )
                    })
                    .collect();
                let results = handle_push_batch(&state, &batch);
                // Phase 45-04 A5: TCP async-batch path (second site) — bump labeled counter.
                let n_ok_tcp2 = results.iter().filter(|r| r.is_ok()).count() as u64;
                if n_ok_tcp2 > 0 {
                    state
                        .events_tcp
                        .fetch_add(n_ok_tcp2, std::sync::atomic::Ordering::Relaxed);
                }
                for (i, (ev, res)) in batch.iter().zip(results.iter()).enumerate() {
                    if let Err(err) = res {
                        pending_drain
                            .push((ev.seq, format!("[batch:{} event:{}] {}", batch_id, i, err)));
                    }
                }
                // Fire-and-forget: no response frame. Continue loop.
                continue;
            }
            other => handle_sync_command(other, &state).map(Some),
        };
        // Phase 10.2: record MSET latency AFTER async completion,
        // in a separate lock. No guard held across the .await above.
        if is_mset {
            let mset_us = cmd_start.elapsed().as_secs_f64() * 1_000_000.0;
            let mut latency = state.latency.lock();
            latency.record_command(
                crate::server::latency::CommandKind::Mset,
                mset_us,
                std::time::Instant::now(),
            );
            if latency.slow_queries_would_accept(crate::server::latency::CommandKind::Mset, mset_us)
            {
                latency.maybe_record_slow(
                    crate::server::latency::CommandKind::Mset,
                    None,
                    mset_us,
                    String::new(),
                );
            }
        }

        // D-13: drain async errors BEFORE the sync response, in
        // per-connection seq order. flush_drain sorts and flushes.
        flush_drain(&mut writer, &mut pending_drain).await?;

        // Write response (Phase 11 three-way match).
        //
        // I-3: BufWriter flush invariant.
        //
        // Every byte we write is followed by an explicit flush
        // before we loop back to the read arm. Ok(None) is the
        // zero-byte async-push success path (unreachable here —
        // async pushes are accumulated above), Ok(Some) and Err
        // both flush. flush_drain above also ends in flush().
        match response {
            Ok(None) => { /* unreachable on the sync path */ }
            Ok(Some(payload)) => {
                let resp = protocol::encode_response(STATUS_OK, &payload);
                writer.write_all(&resp).await?;
                writer.flush().await?;
            }
            Err(e) => {
                let resp = protocol::encode_response(STATUS_ERROR, e.to_string().as_bytes());
                writer.write_all(&resp).await?;
                writer.flush().await?;
            }
        }
    }
}

/// Flush queued async-push errors to the writer in per-connection seq order
/// (D-13) and clear the queue. Writes nothing and returns immediately if the
/// queue is empty. Every write is followed by an explicit flush so the
/// BufWriter I-3 invariant is preserved.
async fn flush_drain(
    writer: &mut BufWriter<tokio::net::tcp::OwnedWriteHalf>,
    pending: &mut Vec<(u64, String)>,
) -> std::io::Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    pending.sort_by_key(|(seq, _)| *seq);
    for (_seq, msg) in pending.drain(..) {
        let resp = protocol::encode_response(STATUS_ERROR, msg.as_bytes());
        writer.write_all(&resp).await?;
    }
    writer.flush().await?;
    Ok(())
}

/// Phase 36-01: replica-mode ingest entry point.
///
/// Accepts an event pulled from the upstream `OP_LOG_FETCH` / `OP_SUBSCRIBE`
/// stream and routes it through the exact same side-effect pipeline a local
/// PUSH would take: `push_with_cascade_no_features` (operators tick, state
/// advances), event-log append (so chained replicas can re-LOG_FETCH), and
/// dirty marking for incremental snapshots.
///
/// Differences from a local PUSH:
///   * No admin-auth check (events are pre-authenticated at the upstream).
///   * No `notify_subscribers` call — in replica mode `main.rs` does NOT
///     install the subscriber_registry into the engine, so the hook inside
///     `push_internal` is already a no-op. This is the "thin approach"
///     documented in 36-01-PLAN.md §stop-and-report to avoid factoring
///     `push_with_cascade_internal` (300+ lines).
///   * Bumps `beava_replica_events_ingested_total{stream}` instead of the
///     normal accept counter / throughput tracker (replicated events are
///     not locally-accepted traffic — operators need a separate signal).
///
/// `event_time` is the upstream's `timestamp_ms`; we use it verbatim as
/// the "now" parameter so operator bucket routing matches the source
/// cluster. Also updates `state.replica_last_applied_ts_ms` atomically
/// on success so the SUBSCRIBE reconnect path can cursor-resume.
pub fn replica_ingest(
    state: &SharedState,
    stream_name: &str,
    ts_ms: u64,
    raw_payload: &[u8],
) -> Result<(), BeavaError> {
    use crate::state::event_log::{decode_log_payload, LOG_FMT_BINARY, LOG_FMT_JSON};
    // Decode the log-payload wrapper (format byte + body) that the upstream
    // persisted. The body is either binary-tagged (OP_PUSH binary wire) or
    // JSON-tagged (HTTP POST path) — in both cases we need a `serde_json::Value`
    // for `handle_push_core_ex` (it's the interface every push path uses).
    let (fmt, body) = decode_log_payload(raw_payload);
    let payload_value: serde_json::Value = match fmt {
        LOG_FMT_BINARY => {
            let mut buf = body;
            protocol::decode_event_binary(&mut buf).map_err(|e| {
                BeavaError::Protocol(format!("replica_ingest: binary decode: {}", e))
            })?
        }
        LOG_FMT_JSON => serde_json::from_slice(body)
            .map_err(|e| BeavaError::Protocol(format!("replica_ingest: json decode: {}", e)))?,
        other => {
            return Err(BeavaError::Protocol(format!(
                "replica_ingest: unknown log fmt byte 0x{:02x}",
                other
            )));
        }
    };

    // Reconstitute the event-time marker. On the source cluster the writer
    // stored `SystemTime::now()` at append; we use the wire `ts_ms` so the
    // replica's watermark progresses in lock-step with upstream.
    let event_time = std::time::UNIX_EPOCH + std::time::Duration::from_millis(ts_ms);

    // For the log-append inside handle_push_core_ex we pass `raw_payload = body`
    // (the un-wrapped binary/JSON body) — make_log_payload re-wraps. This
    // mirrors the sync OP_PUSH path, which feeds `raw_payload` straight from
    // the wire frame into make_log_payload.
    let _features = handle_push_core_ex(
        state,
        stream_name,
        &payload_value,
        // raw_payload_for_relog: pass-through if binary, empty if JSON (legacy).
        if fmt == LOG_FMT_BINARY { body } else { &[] },
        event_time,
        false, // no feature read on replica ingest (async-mode semantics)
    )?;

    // Bump per-stream replica-ingest counter.
    crate::server::replica::bump_replica_events_ingested(stream_name);

    // Advance the SUBSCRIBE reconnect cursor. Relaxed is fine: the reconnect
    // path only needs eventual-consistency and we accept a duplicate at the
    // reconnect boundary by design (see 36-CONTEXT.md §failure policy).
    state
        .replica_last_applied_ts_ms
        .fetch_max(ts_ms, Ordering::Relaxed);
    Ok(())
}

/// Replica-side batch ingest. Semantically equivalent to calling
/// `replica_ingest` N times, but amortizes:
///
/// - the engine read lock (held once for the whole batch),
/// - `store.mark_dirty` (one `mark_dirty_many` call per touched stream),
/// - `event_log.append` (one `append_many_with_ts` per touched stream — one
///   `libc::write()` syscall per stream instead of N),
/// - the per-stream `beava_replica_events_ingested_total` counter bump (one
///   `fetch_add(n)` per stream instead of N `fetch_add(1)`),
/// - the `replica_last_applied_ts_ms` fetch_max (once per batch).
///
/// Per-event `event_time` semantics are **preserved**: each event's operator
/// bucket routing and `LogEntry.timestamp` use `UNIX_EPOCH + ts_ms`, so
/// a downstream fork reading this replica's log via `handle_log_fetch`
/// observes the same ts_ms stream upstream would have emitted.
///
/// Bails on the first per-event error, but flushes log + dirty state for
/// every successfully-applied event before returning. Returns the count
/// of events applied.
pub fn replica_ingest_batch(
    state: &SharedState,
    events: &[(String, u64, Vec<u8>)],
) -> Result<usize, BeavaError> {
    use crate::state::event_log::{decode_log_payload, LOG_FMT_BINARY, LOG_FMT_JSON};

    if events.is_empty() {
        return Ok(0);
    }

    let engine = state.engine.read();
    let store = &state.store;

    // Per-stream accumulators for end-of-batch flush.
    let mut per_stream_log: ahash::AHashMap<String, (Vec<Vec<u8>>, Vec<SystemTime>)> =
        ahash::AHashMap::new();
    let mut per_stream_dirty: ahash::AHashMap<String, Vec<String>> = ahash::AHashMap::new();
    let mut per_stream_counts: ahash::AHashMap<String, u64> = ahash::AHashMap::new();
    let mut max_ts_ms: u64 = 0;
    let mut n_ok: usize = 0;
    let mut first_err: Option<BeavaError> = None;

    // Cache fan_out_targets once; it doesn't change mid-batch under the
    // read lock.
    let fan_out_all = engine.fan_out_targets();

    'outer: for (stream_name, ts_ms, raw_payload) in events {
        let (fmt, body) = decode_log_payload(raw_payload);
        let payload_value: serde_json::Value = match fmt {
            LOG_FMT_BINARY => {
                let mut buf = body;
                match protocol::decode_event_binary(&mut buf) {
                    Ok(v) => v,
                    Err(e) => {
                        first_err = Some(BeavaError::Protocol(format!(
                            "replica_ingest_batch: binary decode: {}",
                            e
                        )));
                        break 'outer;
                    }
                }
            }
            LOG_FMT_JSON => match serde_json::from_slice(body) {
                Ok(v) => v,
                Err(e) => {
                    first_err = Some(BeavaError::Protocol(format!(
                        "replica_ingest_batch: json decode: {}",
                        e
                    )));
                    break 'outer;
                }
            },
            other => {
                first_err = Some(BeavaError::Protocol(format!(
                    "replica_ingest_batch: unknown log fmt byte 0x{:02x}",
                    other
                )));
                break 'outer;
            }
        };

        let event_time = SystemTime::UNIX_EPOCH + std::time::Duration::from_millis(*ts_ms);

        // Primary + cascade push (no features — replica semantics).
        if let Err(e) =
            engine.push_with_cascade_no_features(stream_name, &payload_value, store, event_time)
        {
            first_err = Some(e);
            break 'outer;
        }
        // D-19 / CORR-08: advance the replica's watermark per event so downstream
        // table-cascade γ-propagation fires. Mirrors the live-ingest call at
        // tcp.rs:1750. Atomic fetch_max on AtomicU64 — ~5 ns/call.
        engine.watermarks.observe(stream_name, event_time);

        // Build the log payload once; reused across primary + cascade + fan-out.
        let log_body = if fmt == LOG_FMT_BINARY { body } else { &[] };
        let log_payload = make_log_payload(&payload_value, log_body);

        // Primary: queue log + dirty.
        let primary_key_field: Option<String> = engine
            .get_stream(stream_name)
            .and_then(|s| s.key_field.clone());
        if let Some(ref kf) = primary_key_field {
            if let Some(serde_json::Value::String(k)) = payload_value.get(kf.as_str()) {
                if !k.is_empty() {
                    per_stream_dirty
                        .entry(stream_name.clone())
                        .or_default()
                        .push(k.clone());
                }
            }
        }
        let entry = per_stream_log.entry(stream_name.clone()).or_default();
        entry.0.push(log_payload.clone());
        entry.1.push(event_time);
        *per_stream_counts.entry(stream_name.clone()).or_insert(0) += 1;

        // Cascade targets: log + dirty keys (mirror handle_push_core_ex).
        let cascade_targets = engine.get_cascade_targets(stream_name);
        for ds_name in &cascade_targets {
            if let Some(d) = engine.get_stream(ds_name) {
                let (should_log, dirty_key) = match &d.key_field {
                    Some(kf) => match payload_value.get(kf.as_str()) {
                        Some(serde_json::Value::String(k)) if !k.is_empty() => {
                            (true, Some(k.clone()))
                        }
                        _ => (false, None),
                    },
                    None => (true, None),
                };
                if should_log {
                    let ds_entry = per_stream_log.entry(ds_name.clone()).or_default();
                    ds_entry.0.push(log_payload.clone());
                    ds_entry.1.push(event_time);
                }
                if let Some(k) = dirty_key {
                    per_stream_dirty.entry(ds_name.clone()).or_default().push(k);
                }
            }
        }

        // Fan-out: filter targets the same way handle_push_core_ex does.
        for (target_name, target_key_field) in &fan_out_all {
            if target_name == stream_name {
                continue;
            }
            if primary_key_field.as_deref() == Some(target_key_field.as_str()) {
                continue;
            }
            if cascade_targets.iter().any(|ct| ct == target_name) {
                continue;
            }
            if let Some(serde_json::Value::String(k)) = payload_value.get(target_key_field.as_str())
            {
                if !k.is_empty() {
                    let _ = engine.push_no_features(target_name, &payload_value, store, event_time);
                    per_stream_dirty
                        .entry(target_name.clone())
                        .or_default()
                        .push(k.clone());
                    let tgt_entry = per_stream_log.entry(target_name.clone()).or_default();
                    tgt_entry.0.push(log_payload.clone());
                    tgt_entry.1.push(event_time);
                }
            }
        }

        if *ts_ms > max_ts_ms {
            max_ts_ms = *ts_ms;
        }
        n_ok += 1;
    }

    // Flush dirty keys per stream (one `dirty_keys` mutex acquisition each).
    for (_stream, keys) in &per_stream_dirty {
        store.mark_dirty_many(keys.iter().map(|s| s.as_str()));
    }

    // Flush event-log per stream via the per-timestamp batch writer.
    // One `libc::write()` syscall per stream instead of N.
    if let Some(ref log) = state.event_log {
        for (stream_name, (bodies, timestamps)) in &per_stream_log {
            let refs: Vec<&[u8]> = bodies.iter().map(|v| v.as_slice()).collect();
            let _ = log.append_many_with_ts(stream_name, &refs, timestamps);
        }
    }

    // Per-stream replica counter bump (one fetch_add per stream).
    for (stream_name, n) in &per_stream_counts {
        crate::server::replica::bump_replica_events_ingested_by(stream_name, *n);
    }

    // Advance reconnect cursor to the highest applied ts.
    if n_ok > 0 {
        state
            .replica_last_applied_ts_ms
            .fetch_max(max_ts_ms, Ordering::Relaxed);
    }

    // Bump events_total once per batch too — matches the sync-batch path's
    // per-event semantics when converted back (N events applied).
    state
        .events_total
        .fetch_add(n_ok as u64, std::sync::atomic::Ordering::Relaxed);
    state.atomic_throughput.bump(n_ok as u64);

    match first_err {
        Some(e) if n_ok == 0 => Err(e),
        Some(e) => {
            // Partial success: some events applied + flushed; return the
            // error so the caller can reconnect-and-resume from
            // replica_last_applied_ts_ms. We've committed N in-memory + log
            // state so resume is consistent.
            Err(e)
        }
        None => Ok(n_ok),
    }
}

/// Core PUSH side-effect pipeline shared by sync and async push paths (Phase 11).
///
/// Performs the full PUSH work under a single lock acquisition:
/// - engine.push_with_cascade
/// - mark_dirty for the primary key
/// - event log append (primary + cascade)
/// - fan-out (engine.push + log + dirty)
/// - dedup'd throughput bump across all touched streams
/// - push_latency_seconds metric + events_total
/// - Phase 10.2 latency histogram + slow-query capture
///
/// Returns the computed FeatureMap. The sync PUSH arm converts it to JSON;
/// the async PUSH wrapper discards it.
#[allow(dead_code)] // Phase 12: legacy single-event wrapper; kept for symmetry.
fn handle_push_core(
    state: &SharedState,
    stream_name: &str,
    payload: &serde_json::Value,
    now: SystemTime,
) -> Result<crate::types::FeatureMap, BeavaError> {
    handle_push_core_ex(state, stream_name, payload, &[], now, true)
}

/// Build the event-log payload bytes for a push.
///
/// Plan 11-06: if we have the original binary wire bytes (`raw_payload`
/// non-empty), prefix them with `LOG_FMT_BINARY` and return — zero JSON
/// work on the hot path. If we don't have raw bytes (legacy code path,
/// e.g. test helpers that construct a `Command::Push` by hand), fall
/// back to serializing the decoded `serde_json::Value` and prefix with
/// `LOG_FMT_JSON`.
fn make_log_payload(payload: &serde_json::Value, raw_payload: &[u8]) -> Vec<u8> {
    use crate::state::event_log::{LOG_FMT_BINARY, LOG_FMT_JSON};
    if !raw_payload.is_empty() {
        let mut out = Vec::with_capacity(1 + raw_payload.len());
        out.push(LOG_FMT_BINARY);
        out.extend_from_slice(raw_payload);
        out
    } else {
        let json_bytes = serde_json::to_vec(payload).unwrap_or_default();
        let mut out = Vec::with_capacity(1 + json_bytes.len());
        out.push(LOG_FMT_JSON);
        out.extend_from_slice(&json_bytes);
        out
    }
}

pub fn handle_push_core_ex(
    state: &SharedState,
    stream_name: &str,
    payload: &serde_json::Value,
    raw_payload: &[u8],
    now: SystemTime,
    read_features: bool,
) -> Result<crate::types::FeatureMap, BeavaError> {
    let push_start = std::time::Instant::now();
    let engine = state.engine.read();
    let store = &state.store;

    // Phase 14 fix: Do NOT acquire event_log lock during entity state work.
    // Entity state mutations use DashMap (lock-free per key). Event log
    // append is deferred to after all state work completes so the event_log
    // lock does not serialize concurrent connections.

    // Cascade-aware push: handles topological cascade through depends_on chains.
    // Async path (read_features=false) skips HLL/derive reads for ~140x speedup
    // on large pipelines — see pipeline.rs push_no_features doc.
    let features = if read_features {
        engine.push_with_cascade(stream_name, payload, store, now)?
    } else {
        engine.push_with_cascade_no_features(stream_name, payload, store, now)?
    };

    // Mark primary key dirty for incremental snapshots (OPS-03).
    if let Some(stream_def) = engine.get_stream(stream_name) {
        if let Some(ref kf) = stream_def.key_field {
            if let Some(serde_json::Value::String(key_val)) = payload.get(kf.as_str()) {
                if !key_val.is_empty() {
                    store.mark_dirty(key_val);
                }
            }
        }
    }

    // Fan-out: push to other streams whose key_field exists in the event.
    let cascade_targets = engine.get_cascade_targets(stream_name);
    let primary_key_field = engine
        .get_stream(stream_name)
        .and_then(|s| s.key_field.as_deref());
    let targets = engine.fan_out_targets();
    // Collect fan-out stream names for event log (need to know which streams were touched)
    let mut fan_out_logged: Vec<&str> = Vec::new();
    for (target_name, target_key_field) in &targets {
        if target_name == stream_name {
            continue;
        }
        if primary_key_field == Some(target_key_field.as_str()) {
            continue;
        }
        if cascade_targets.iter().any(|ct| ct == target_name) {
            continue;
        }
        if let Some(serde_json::Value::String(key_val)) = payload.get(target_key_field.as_str()) {
            if !key_val.is_empty() {
                // PERF: honor async read_features flag for fan-out targets.
                let _ = if read_features {
                    engine.push(target_name, payload, store, now)
                } else {
                    engine.push_no_features(target_name, payload, store, now)
                };
                store.mark_dirty(key_val);
                fan_out_logged.push(target_name);
            }
        }
    }

    // Mark cascade target keys dirty for incremental snapshots (OPS-03).
    for ds_name in &cascade_targets {
        if let Some(d) = engine.get_stream(ds_name) {
            if let Some(ref kf) = d.key_field {
                if let Some(serde_json::Value::String(key_val)) = payload.get(kf.as_str()) {
                    if !key_val.is_empty() {
                        store.mark_dirty(key_val);
                    }
                }
            }
        }
    }

    // Phase 40: no outer lock — `EventLog` has per-stream interior locks.
    // Different streams (primary + cascade + fan-out) all acquire only their
    // own writer mutex, so fan-out I/O is parallel across streams.
    if let Some(ref log) = state.event_log {
        let log_payload = make_log_payload(payload, raw_payload);
        // Primary stream
        let _ = log.append(stream_name, &log_payload, now);
        // Cascade targets (T-07-10)
        for ds_name in &cascade_targets {
            let should_log = match engine.get_stream(ds_name) {
                Some(d) => match &d.key_field {
                    Some(kf) => {
                        matches!(payload.get(kf.as_str()), Some(serde_json::Value::String(s)) if !s.is_empty())
                    }
                    None => true,
                },
                None => false,
            };
            if should_log {
                let _ = log.append(ds_name, &log_payload, now);
            }
        }
        // Fan-out targets
        for target_name in &fan_out_logged {
            let _ = log.append(target_name, &log_payload, now);
        }
    }
    // Phase 41-01 T3: lock-free atomic ring counter (used by /metrics
    // and /public/stats). The old code built a `touched` vector of
    // primary + cascade + fan-out targets and fed it into the per-stream
    // EWMA `ThroughputTracker`; that tracker is no longer part of the hot
    // path. We just bump the global EPS ring once per successful PUSH —
    // cascade/fan-out event counts are still visible via /metrics
    // (beava_events_total counts successful PUSHes, not derived events).
    // Per-stream EPS visibility on `/debug/throughput` becomes admin-path
    // only; see 41-01-SUMMARY.md for the trade-off.
    state.atomic_throughput.bump(1);

    let push_elapsed = push_start.elapsed();
    // Phase 41-01 T2: lock-free counter + last-latency gauge.
    state
        .events_total
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    state.last_push_latency_nanos.store(
        push_elapsed.as_nanos().min(u64::MAX as u128) as u64,
        std::sync::atomic::Ordering::Relaxed,
    );

    // Phase 20: capture the event in the recent-events ring for the public
    // read-only feed. Bounded at RecentEventsRing::CAPACITY — O(1) write.
    //
    // Phase 41-01 T1: gated behind `feature = "demo"`. Default server build
    // omits this call entirely.
    #[cfg(feature = "demo")]
    record_recent_event(state, stream_name, payload, now);

    // Phase 10.2: record latency into histogram tracker
    // Phase 41-01 T4: sampled 1-in-LATENCY_SAMPLE_STRIDE to drop ~94% of
    // per-push latency-mutex acquisitions. Histogram shape preserved.
    let push_us = push_elapsed.as_secs_f64() * 1_000_000.0;
    let sample_n = state
        .latency_sample_counter
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if sample_n.is_multiple_of(LATENCY_SAMPLE_STRIDE) {
        let mut latency = state.latency.lock();
        latency.record_push(stream_name, push_us, std::time::Instant::now());
        if latency.slow_queries_would_accept(crate::server::latency::CommandKind::Push, push_us) {
            let key_preview = engine
                .get_stream(stream_name)
                .and_then(|s| s.key_field.clone())
                .and_then(|kf| {
                    payload.get(&kf).and_then(|v| v.as_str()).map(|s| {
                        let mut kp = s.to_string();
                        kp.truncate(32);
                        kp
                    })
                })
                .unwrap_or_default();
            latency.maybe_record_slow(
                crate::server::latency::CommandKind::Push,
                Some(stream_name),
                push_us,
                key_preview,
            );
        }
    }

    Ok(features)
}

// ============================================================
// Phase 12: per-connection async push coalescing
// ============================================================
//
// ConnAccumulator buffers OP_PUSH_ASYNC frames on the per-connection task
// stack. When it hits BATCH_SIZE events or its deadline elapses, the whole
// batch is handed to `handle_push_batch`, which takes ONE state.lock() and
// groups events by primary stream name, issuing exactly one
// `engine.push_batch_with_cascade_no_features` + one `event_log.append_many`
// + one `store.mark_dirty_many` per group. Decisions D-01..D-20 in
// 12-CONTEXT.md. Pitfalls C-2/C-7/H-2 in 12-RESEARCH.md.

/// A single async push frame buffered inside a connection's accumulator.
/// `seq` is the per-connection monotonic ordering stamp attached at
/// accumulate time so that drain errors surface in push order regardless
/// of stream-grouping reshuffles inside `handle_push_batch` (pitfall C-2).
#[derive(Debug)]
pub struct PendingAsync {
    pub seq: u64,
    pub stream_name: String,
    pub payload: serde_json::Value,
    pub raw_payload: Vec<u8>,
    pub now: SystemTime,
}

impl PendingAsync {
    /// Test/integration constructor — lets external tests build a batch
    /// for `handle_push_batch` without having to go through the full
    /// per-connection accumulator plumbing.
    pub fn new(
        seq: u64,
        stream_name: String,
        payload: serde_json::Value,
        raw_payload: Vec<u8>,
        now: SystemTime,
    ) -> Self {
        Self {
            seq,
            stream_name,
            payload,
            raw_payload,
            now,
        }
    }
}

/// Coalescing parameters locked in 12-CONTEXT.md (D-01 / D-02).
pub const BATCH_SIZE: usize = 64;
pub const BATCH_DEADLINE_US: u64 = 200;

/// Stack-local per-connection accumulator. Never on AppState (D-15).
pub struct ConnAccumulator {
    buf: Vec<PendingAsync>,
    next_seq: u64,
    deadline: Option<tokio::time::Instant>,
}

impl Default for ConnAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnAccumulator {
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(BATCH_SIZE),
            next_seq: 0,
            deadline: None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.buf.len() >= BATCH_SIZE
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn next_seq_peek(&self) -> u64 {
        self.next_seq
    }

    pub fn deadline(&self) -> Option<tokio::time::Instant> {
        self.deadline
    }

    /// Push one async frame. Assigns the next monotonic seq. Arms the
    /// deadline if this is the first frame since the last drain (D-03 —
    /// deadline is an absolute `tokio::time::Instant`, NOT a
    /// `sleep(duration)` that would hit the 1ms timer wheel floor).
    pub fn push(
        &mut self,
        stream_name: String,
        payload: serde_json::Value,
        raw_payload: Vec<u8>,
        now: SystemTime,
    ) {
        let seq = self.next_seq;
        self.next_seq += 1;
        if self.buf.is_empty() {
            self.deadline = Some(
                tokio::time::Instant::now() + std::time::Duration::from_micros(BATCH_DEADLINE_US),
            );
        }
        self.buf.push(PendingAsync {
            seq,
            stream_name,
            payload,
            raw_payload,
            now,
        });
    }

    /// Reserve `n` consecutive seq numbers for a client-side batch that
    /// bypasses the accumulator buffer. Returns the base seq; the batch
    /// events get seqs [base, base+1, ..., base+n-1]. The accumulator's
    /// internal counter advances to base+n so subsequent push() calls
    /// continue from there. (Phase 13 D-10, Pitfall 5 in RESEARCH.md)
    pub fn advance_seq(&mut self, n: u64) -> u64 {
        let base = self.next_seq;
        self.next_seq += n;
        base
    }

    /// Drain all buffered frames and reset the deadline.
    /// `next_seq` is NOT reset — it is per-connection monotonic for the
    /// lifetime of the connection (D-12).
    ///
    /// The internal buffer is drained in-place and a new Vec with the same
    /// pre-allocated capacity is swapped in, avoiding a fresh heap allocation
    /// on the next batch cycle.
    pub fn drain(&mut self) -> Vec<PendingAsync> {
        self.deadline = None;
        let mut taken = Vec::with_capacity(BATCH_SIZE);
        std::mem::swap(&mut self.buf, &mut taken);
        taken
    }
}

/// Synchronous batch dispatch: ONE `state.lock()`, ONE
/// `push_batch_with_cascade_no_features` per stream group. Returns
/// per-event Results in INPUT order.
///
/// D-05..D-08: stream grouping happens BEFORE the lock; critical section
/// is strictly synchronous; no `.await` inside. Cascade and fan-out are
/// preserved via the Wave 1 `push_batch_with_cascade_no_features` primitive
/// (NOT the primary-only `push_batch_no_features`).
///
/// Allocation-optimized: avoids intermediate Vec<Vec<u8>> for log payloads
/// and Vec<String> for dirty keys by issuing per-event append/mark_dirty
/// calls inline (still under the same single lock). The lock-amortization
/// benefit is preserved; only the intermediate collection allocations are
/// eliminated.
pub fn handle_push_batch(
    state: &SharedState,
    batch: &[PendingAsync],
) -> Vec<Result<(), BeavaError>> {
    if batch.is_empty() {
        return Vec::new();
    }
    // Phase 36-01: replica mode rejects async-batch PUSH alongside sync PUSH.
    if state.replica_mode.load(Ordering::Relaxed) {
        return batch
            .iter()
            .map(|_| {
                Err(BeavaError::Protocol(
                    "replica mode: local PUSH disabled".into(),
                ))
            })
            .collect();
    }

    // Cross-shard probe: if `BEAVA_SHARD_PROBE=N` is set, enumerate each
    // event's touched keys (primary + cascade + fan-out) and record the
    // distinct-shard count. Zero-cost when disabled (single atomic read).
    if crate::server::shard_probe::is_enabled() {
        let engine = state.engine.read();
        let fan_out_all = engine.fan_out_targets();
        for ev in batch {
            let stream_name = ev.stream_name.as_str();
            let cascade_targets = engine.get_cascade_targets(stream_name);
            let primary_kf = engine
                .get_stream(stream_name)
                .and_then(|s| s.key_field.clone());
            // Collect key-string views touched by this event.
            let mut keys: Vec<&str> = Vec::with_capacity(8);
            if let Some(ref kf) = primary_kf {
                if let Some(serde_json::Value::String(k)) = ev.payload.get(kf.as_str()) {
                    if !k.is_empty() {
                        keys.push(k.as_str());
                    }
                }
            }
            for ds_name in &cascade_targets {
                if let Some(d) = engine.get_stream(ds_name) {
                    if let Some(ref kf) = d.key_field {
                        if let Some(serde_json::Value::String(k)) = ev.payload.get(kf.as_str()) {
                            if !k.is_empty() {
                                keys.push(k.as_str());
                            }
                        }
                    }
                }
            }
            for (target_name, target_key_field) in &fan_out_all {
                if target_name == stream_name {
                    continue;
                }
                if primary_kf.as_deref() == Some(target_key_field.as_str()) {
                    continue;
                }
                if cascade_targets.iter().any(|ct| ct == target_name) {
                    continue;
                }
                if let Some(serde_json::Value::String(k)) =
                    ev.payload.get(target_key_field.as_str())
                {
                    if !k.is_empty() {
                        keys.push(k.as_str());
                    }
                }
            }
            crate::server::shard_probe::record_event(&keys);
        }
    }

    // Phase 43 T1: measure total server-side batch processing time so the
    // PUSH histogram on /debug/latency reflects the batch path (OP_PUSH_BATCH
    // and OP_PUSH_ASYNC accumulator flushes). Previously only the OP_PUSH
    // single-event hot path recorded latency, so production traffic — which
    // runs ~100% through this function — reported count=0 forever.
    let batch_start = std::time::Instant::now();

    // Fast path: check if all events target the same stream (common case
    // under single-client sustained load). Avoids the grouping Vec entirely.
    let all_same_stream = batch.len() == 1
        || batch[1..]
            .iter()
            .all(|ev| ev.stream_name == batch[0].stream_name);

    // Result slots in input order, pre-filled with Ok. Per-event errors
    // from the cascade-aware batch primitive are scattered back to their
    // input positions below.
    let mut results: Vec<Result<(), BeavaError>> = (0..batch.len()).map(|_| Ok(())).collect();

    // Phase 14 fix: engine read lock only. Store is accessed via DashMap
    // (no lock needed). Event log lock is deferred to AFTER all entity state
    // work so it does not serialize concurrent connections.
    let engine = state.engine.read();
    let store = &state.store;

    if all_same_stream {
        // Single-stream fast path: no grouping needed, no index indirection.
        let stream_name = batch[0].stream_name.as_str();
        let key_field: Option<&str> = engine
            .get_stream(stream_name)
            .and_then(|s| s.key_field.as_deref());

        // Phase 24-04: per-event event-time parse + late-drop gate. Each
        // event gets its own `_event_time` (or wall-clock fallback); the
        // stream's watermark is checked BEFORE the batch call so late
        // events never reach the operator bucket router.
        //
        // D-01 (CORR-01): kept now holds (&Value, SystemTime) pairs so that
        // push_batch_with_cascade_no_features receives a per-event event-time.
        // The old `min_event_time` collapse (which destroyed per-event precision
        // and was the 2a bug) is removed entirely.
        let mut kept: Vec<(&serde_json::Value, SystemTime)> = Vec::with_capacity(batch.len());
        let mut kept_idxs: Vec<usize> = Vec::with_capacity(batch.len());
        for (idx, ev) in batch.iter().enumerate() {
            let et = crate::engine::event_time::parse_event_time(&ev.payload, ev.now);
            let wm = engine.watermarks.watermark(stream_name);
            if let Some(wm) = wm {
                if et < wm {
                    // OBS-02: late-drop gate fires here — the event never
                    // reaches the ring-buffer bucket router, so
                    // beava_ring_buffer_drops_total and
                    // beava_late_events_dropped_total are mutually exclusive.
                    engine.late_drops.increment(stream_name);
                    continue;
                }
            }
            engine.watermarks.observe(stream_name, et);
            kept.push((&ev.payload, et));
            kept_idxs.push(idx);
        }

        let per_event = engine.push_batch_with_cascade_no_features(stream_name, &kept, store);

        // Scatter errors to result slots.
        for (k_idx, res) in per_event.into_iter().enumerate() {
            if let Err(e) = res {
                let orig = kept_idxs[k_idx];
                results[orig] = Err(e);
            }
        }

        // Batched dirty marking: one `dirty_keys` mutex acquisition per
        // batch instead of per event. Simple pipelines have sub-µs per-event
        // compute, so a global lock taken N times per batch dominated CPU
        // under concurrent clients (throughput flat past ~4 processes).
        if let Some(kf) = key_field {
            store.mark_dirty_many(batch.iter().enumerate().filter_map(|(idx, ev)| {
                if results[idx].is_err() {
                    return None;
                }
                match ev.payload.get(kf) {
                    Some(serde_json::Value::String(k)) if !k.is_empty() => Some(k.as_str()),
                    _ => None,
                }
            }));
        }

        // Deferred event log append. Phase 42: batch into a single
        // `append_many` so the whole batch becomes one `O_APPEND` `write()`
        // syscall — batch-atomic, lock-free, one kernel transition.
        // D-01: use the wall-clock arrival time of the first batch event as
        // the log-entry timestamp — this is only used for event-log ordering,
        // not for bucket routing (which is now per-event inside the primitive).
        if let Some(ref log) = state.event_log {
            let mut payloads: Vec<Vec<u8>> = Vec::with_capacity(batch.len());
            for (idx, ev) in batch.iter().enumerate() {
                if results[idx].is_ok() {
                    payloads.push(make_log_payload(&ev.payload, &ev.raw_payload));
                }
            }
            if !payloads.is_empty() {
                let refs: Vec<&[u8]> = payloads.iter().map(|v| v.as_slice()).collect();
                let _ = log.append_many(stream_name, &refs, batch[0].now);
            }
        }
    } else {
        // Multi-stream path: group by stream name BEFORE processing (D-05).
        let mut groups: Vec<(&str, Vec<usize>)> = Vec::with_capacity(4);
        for (idx, ev) in batch.iter().enumerate() {
            let name = ev.stream_name.as_str();
            if let Some((_, ids)) = groups.iter_mut().find(|(n, _)| *n == name) {
                ids.push(idx);
            } else {
                groups.push((name, vec![idx]));
            }
        }

        for (stream_name, indices) in &groups {
            let key_field: Option<&str> = engine
                .get_stream(stream_name)
                .and_then(|s| s.key_field.as_deref());

            // Phase 24-04: per-event late-drop gate for this stream group.
            //
            // D-01 (CORR-01): kept now holds (&Value, SystemTime) pairs so
            // push_batch_with_cascade_no_features receives a per-event
            // event-time. The old `min_et` collapse is removed entirely.
            let mut kept: Vec<(&serde_json::Value, SystemTime)> = Vec::with_capacity(indices.len());
            let mut kept_orig: Vec<usize> = Vec::with_capacity(indices.len());
            for &i in indices {
                let et =
                    crate::engine::event_time::parse_event_time(&batch[i].payload, batch[i].now);
                let wm = engine.watermarks.watermark(stream_name);
                if let Some(wm) = wm {
                    if et < wm {
                        engine.late_drops.increment(stream_name);
                        continue;
                    }
                }
                engine.watermarks.observe(stream_name, et);
                kept.push((&batch[i].payload, et));
                kept_orig.push(i);
            }

            let per_event = engine.push_batch_with_cascade_no_features(stream_name, &kept, store);

            for (k_idx, res) in per_event.into_iter().enumerate() {
                if let Err(e) = res {
                    results[kept_orig[k_idx]] = Err(e);
                }
            }

            // Batched dirty marking: one mutex acquisition per stream group
            // instead of per event (see single-stream branch for rationale).
            if let Some(kf) = key_field {
                store.mark_dirty_many(indices.iter().filter_map(|&i| {
                    if results[i].is_err() {
                        return None;
                    }
                    match batch[i].payload.get(kf) {
                        Some(serde_json::Value::String(k)) if !k.is_empty() => Some(k.as_str()),
                        _ => None,
                    }
                }));
            }
        }

        // Deferred event log append. Phase 42: use `append_many` so the
        // whole per-stream group becomes one `O_APPEND` `write()` syscall
        // — batch-atomic and lock-free.
        //
        // We materialize the `Vec<u8>` payloads per group into owned buffers
        // (postcard wire format is keyed from `make_log_payload`) and then
        // pass borrowed slice references to `append_many`. All events in a
        // group share the same `batch[i].now` of the first successful entry
        // — historically `append_many` took a single `now`; we preserve that
        // by using the first event's timestamp per group. (Batched pushes
        // are microseconds apart; event-time / watermark handles ordering
        // semantically.)
        if let Some(ref log) = state.event_log {
            for (stream_name, indices) in &groups {
                // Gather successful events' log payloads.
                let mut payloads: Vec<Vec<u8>> = Vec::with_capacity(indices.len());
                let mut group_now: Option<std::time::SystemTime> = None;
                for &i in indices {
                    if results[i].is_ok() {
                        if group_now.is_none() {
                            group_now = Some(batch[i].now);
                        }
                        payloads.push(make_log_payload(&batch[i].payload, &batch[i].raw_payload));
                    }
                }
                if payloads.is_empty() {
                    continue;
                }
                let refs: Vec<&[u8]> = payloads.iter().map(|v| v.as_slice()).collect();
                let _ = log.append_many(stream_name, &refs, group_now.unwrap());
            }
        }
    }

    // Phase 41-01 T2: lock-free events counter + T3 atomic throughput
    // ring. One fetch_add per batch — no `state.metrics.lock()` here.
    let batch_len = batch.len() as u64;
    state
        .events_total
        .fetch_add(batch_len, std::sync::atomic::Ordering::Relaxed);
    state.atomic_throughput.bump(batch_len);

    // Phase 43 T1: record server-side PUSH latency. We divide total batch
    // elapsed by batch.len() to produce a per-event microsecond number
    // directly comparable to the OP_PUSH single-event path. The stream
    // attribution is the common-stream name when all events target one
    // stream (99%+ of production batches); mixed-stream batches attribute
    // to "_mixed" rather than skewing a real stream's percentile.
    //
    // Cost: one mutex acquisition per batch (~100ns). At 300 batches/sec
    // (60s × 300K eps / 1000-event batches) that is 60µs/min — negligible
    // vs the ~333µs/batch processing cost.
    let batch_elapsed_us = batch_start.elapsed().as_secs_f64() * 1_000_000.0;
    let per_event_us = batch_elapsed_us / batch.len() as f64;
    let stream_attr: &str = if all_same_stream {
        batch[0].stream_name.as_str()
    } else {
        "_mixed"
    };
    {
        let mut latency = state.latency.lock();
        latency.record_push(stream_attr, per_event_us, std::time::Instant::now());
    }

    // Phase 20: record each event in the recent-events ring (bounded).
    //
    // Phase 41-01 T1: gated behind `feature = "demo"`. Default server build
    // skips the per-event ring insert entirely.
    #[cfg(feature = "demo")]
    for (idx, ev) in batch.iter().enumerate() {
        if results[idx].is_ok() {
            record_recent_event(state, &ev.stream_name, &ev.payload, ev.now);
        }
    }

    results
}

/// Phase 20: push a single event into the public recent-events ring.
/// Extracts the key (if the primary stream has one) and truncates the payload
/// to `RecentEventsRing::PAYLOAD_PREVIEW_MAX` characters.
///
/// Phase 41-01 T1: gated behind `feature = "demo"` along with its callers.
#[cfg(feature = "demo")]
fn record_recent_event(
    state: &SharedState,
    stream_name: &str,
    payload: &serde_json::Value,
    now: SystemTime,
) {
    let ts_ms = now
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let key = {
        let engine = state.engine.read();
        engine
            .get_stream(stream_name)
            .and_then(|s| s.key_field.clone())
            .and_then(|kf| {
                payload
                    .get(kf.as_str())
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .unwrap_or_default()
    };
    let mut preview = serde_json::to_string(payload).unwrap_or_default();
    if preview.len() > RecentEventsRing::PAYLOAD_PREVIEW_MAX {
        // Truncate at char boundary to avoid splitting a UTF-8 codepoint.
        let mut cut = RecentEventsRing::PAYLOAD_PREVIEW_MAX;
        while !preview.is_char_boundary(cut) && cut > 0 {
            cut -= 1;
        }
        preview.truncate(cut);
    }
    let ev = RecentEvent {
        ts_ms,
        stream: stream_name.to_string(),
        key,
        payload_preview: preview,
    };
    state.recent_events.lock().push(ev);
}

// Phase 12 Task 2: `handle_push_async` removed. All async pushes now flow
// through the per-connection `ConnAccumulator` → `handle_push_batch` path in
// `handle_connection`. The batch path is the only async push path.

/// Handle synchronous commands: lock, process, unlock. No .await while locked.
fn handle_sync_command(cmd: Command, state: &SharedState) -> Result<Vec<u8>, BeavaError> {
    let now = SystemTime::now();
    match cmd {
        Command::Push {
            stream_name,
            payload,
            raw_payload,
        } => {
            // Phase 36-01: replica mode rejects client-originated PUSH.
            // Events must come from the configured upstream via the replica
            // client loop. Scientists wanting synthetic events need a
            // separate helper flag (deferred — see 36-CONTEXT.md §deferred).
            if state.replica_mode.load(Ordering::Relaxed) {
                return Err(BeavaError::Protocol(
                    "replica mode: local PUSH disabled".into(),
                ));
            }
            // Phase 24-04: parse `_event_time` from payload (falls back to
            // wall-clock `now` if absent / unparseable). The parsed
            // event-time is (a) checked against the stream's current
            // watermark for late-drop, (b) recorded as the stream's
            // latest observation, (c) used as the "now" for operator
            // bucket routing in the push-through pipeline.
            let event_time = crate::engine::event_time::parse_event_time(&payload, now);
            {
                let engine = state.engine.read();
                let wm = engine.watermarks.watermark(&stream_name);
                if let Some(wm) = wm {
                    if event_time < wm {
                        // Late event — drop silently with counter increment.
                        engine.late_drops.increment(&stream_name);
                        return Ok(feature_map_to_json(&crate::types::FeatureMap::new()));
                    }
                }
                engine.watermarks.observe(&stream_name, event_time);
            }
            // PERF: sync push path also skips feature read + derive eval. The
            // response is a synchronous ack ({}) confirming the event was
            // processed — callers that need features should use OP_GET after.
            // Plan 11-06: raw_payload goes to the event log directly.
            let _features = handle_push_core_ex(
                state,
                &stream_name,
                &payload,
                &raw_payload,
                event_time,
                false,
            )?;
            // Phase 45-04 A5: TCP sync single-event path — bump labeled counter.
            state.events_tcp.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(feature_map_to_json(&crate::types::FeatureMap::new()))
        }
        Command::Get { key } => {
            let get_start = std::time::Instant::now();
            let engine = state.engine.read();
            let features = engine.get_features(&key, &state.store, now);
            let result = feature_map_to_json(&features);
            drop(engine);
            // Phase 10.2: record GET latency
            let get_us = get_start.elapsed().as_secs_f64() * 1_000_000.0;
            let mut latency = state.latency.lock();
            latency.record_command(
                crate::server::latency::CommandKind::Get,
                get_us,
                std::time::Instant::now(),
            );
            if latency.slow_queries_would_accept(crate::server::latency::CommandKind::Get, get_us) {
                let mut kp = key.clone();
                kp.truncate(32);
                latency.maybe_record_slow(
                    crate::server::latency::CommandKind::Get,
                    None,
                    get_us,
                    kp,
                );
            }
            Ok(result)
        }
        Command::Set { key, payload } => {
            let set_start = std::time::Instant::now();
            {
                // payload is a JSON object; iterate its key-value pairs
                if let serde_json::Value::Object(map) = payload {
                    // Phase 23-03: empty-object SET is a tombstone — remove
                    // all static features for the key and fire Table↔Table
                    // cascade with tombstoned=true. Non-empty SET is an
                    // upsert; cascade fires with tombstoned=false.
                    let tombstoned = map.is_empty();
                    if tombstoned {
                        state.store.tombstone_static(&key);
                    } else {
                        for (feat_name, val) in map {
                            let fv = json_to_feature_value(val);
                            state.store.set_static(&key, &feat_name, fv, now);
                        }
                    }
                    // Mark entity key dirty for incremental snapshots (OPS-03)
                    state.store.mark_dirty(&key);

                    // Phase 23-03: cascade into Table↔Table join outputs. We
                    // don't know which input Table fired the SET (protocol is
                    // key-only), so we cascade for every registered Table
                    // that has TT-join downstreams. The engine resolves the
                    // right per-side marker internally.
                    {
                        let engine = state.engine.read();
                        let input_tables: Vec<String> = engine
                            .list_streams()
                            .filter_map(|s| {
                                // Only iterate Tables (key_field Some). We
                                // can't cheaply know which table the SET
                                // targets, so fan out to all input tables
                                // that participate in a TT-join.
                                if s.key_field.is_some() {
                                    Some(s.name.clone())
                                } else {
                                    None
                                }
                            })
                            .collect();
                        for input_table in input_tables {
                            let _ = engine.cascade_table_upsert(
                                &input_table,
                                &key,
                                tombstoned,
                                &state.store,
                                now,
                            );
                        }
                    }
                } else {
                    return Err(BeavaError::Protocol(
                        "SET payload must be a JSON object".into(),
                    ));
                }
            }
            // Phase 10.2: record SET latency
            let set_us = set_start.elapsed().as_secs_f64() * 1_000_000.0;
            let mut latency = state.latency.lock();
            latency.record_command(
                crate::server::latency::CommandKind::Set,
                set_us,
                std::time::Instant::now(),
            );
            if latency.slow_queries_would_accept(crate::server::latency::CommandKind::Set, set_us) {
                let mut kp = key.clone();
                kp.truncate(32);
                latency.maybe_record_slow(
                    crate::server::latency::CommandKind::Set,
                    None,
                    set_us,
                    kp,
                );
            }
            Ok(vec![])
        }
        Command::Register { payload } => {
            // Phase 25-02: track the pipeline name so we can emit a safety
            // signal on failure. Extracted up-front from the payload (best
            // effort — falls back to "unknown" if the JSON is malformed).
            let pipeline_name_for_signal = payload
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();
            let raw_json = payload.clone();

            let result = (|| -> Result<Vec<u8>, BeavaError> {
            // Plan 22-04: v0 REGISTER dispatch. v0 payloads always carry a top-level
            // `kind` field ("stream" | "table"). v2.0 payloads never had one — detect
            // by the presence of `kind` and route to the v0 translator → existing
            // PipelineEngine.register via the v0→v2 bridge.
            let is_v0 = raw_json.get("kind").is_some();
            if is_v0 {
                let v0_bytes = serde_json::to_vec(&raw_json).map_err(|e| {
                    BeavaError::Protocol(format!("v0 REGISTER: re-serialize failed: {}", e))
                })?;
                let parsed = crate::engine::register::V0RegisterPayload::parse(&v0_bytes)?;
                let stream_def = match &parsed {
                    crate::engine::register::V0RegisterPayload::Source(desc) => {
                        crate::engine::register::v0_source_to_stream_def(desc)?
                    }
                    crate::engine::register::V0RegisterPayload::Aggregation(desc) => {
                        crate::engine::register::v0_aggregation_to_stream_def(desc)?
                    }
                    crate::engine::register::V0RegisterPayload::Join(desc) => {
                        // Phase 23-01: Stream↔Table enrichment. Look up the
                        // left source's field schema from its previously-
                        // stored raw register JSON so the translator can
                        // partition output fields into left vs right.
                        // Phase 23-03: also supply a source_meta_lookup for
                        // table_table key validation.
                        let engine_ref = state.engine.read();
                        let lookup = |name: &str| -> Option<Vec<String>> {
                            engine_ref.get_raw_register_json(name).and_then(|j| {
                                j.get("fields")
                                    .and_then(|f| f.as_object())
                                    .map(|m| m.keys().cloned().collect())
                            })
                        };
                        // Phase 47: complex return type is required for the join-meta API;
                        // a type alias would not simplify the call site.
                        #[allow(clippy::type_complexity)]
                        let meta_lookup =
                            |name: &str| -> Option<(Vec<String>, Vec<(String, String)>)> {
                                let j = engine_ref.get_raw_register_json(name)?;
                                // Derive key fields.
                                let keys: Vec<String> = if let Some(kf) = j
                                    .get("key_fields")
                                    .and_then(|v| v.as_array())
                                {
                                    kf.iter()
                                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                        .collect()
                                } else if let Some(k) =
                                    j.get("key_field").and_then(|v| v.as_str())
                                {
                                    vec![k.to_string()]
                                } else {
                                    Vec::new()
                                };
                                // Derive (field_name, type) tuples. Field spec
                                // has `{"type": "...", "optional": bool}`;
                                // `type` may be absent in which case we use "".
                                let fields: Vec<(String, String)> = j
                                    .get("fields")
                                    .and_then(|f| f.as_object())
                                    .map(|m| {
                                        m.iter()
                                            .map(|(n, spec)| {
                                                let t = spec
                                                    .get("type")
                                                    .and_then(|v| v.as_str())
                                                    .unwrap_or("")
                                                    .to_string();
                                                (n.clone(), t)
                                            })
                                            .collect()
                                    })
                                    .unwrap_or_default();
                                Some((keys, fields))
                            };
                        let sd = crate::engine::register::v0_join_to_stream_def_with_meta(
                            desc,
                            Some(&lookup),
                            Some(&meta_lookup),
                        )?;
                        drop(engine_ref);
                        sd
                    }
                    crate::engine::register::V0RegisterPayload::StatelessChain(_)
                    | crate::engine::register::V0RegisterPayload::Union(_) => {
                        return Err(BeavaError::Protocol(format!(
                            "v0 REGISTER: descriptor kind '{}' not yet wired for end-to-end \
                             execution (Phase 23 lands joins/stateless-chains/union)",
                            parsed.descriptor_kind()
                        )));
                    }
                };
                let def_name = stream_def.name.clone();
                let mut engine = state.engine.write();
                let diff = engine.register(stream_def)?;
                // Track for event log (flow preserved from v2.0 path).
                let history_ttl = engine.get_stream(&def_name).and_then(|s| s.history_ttl);
                if let Some(ref log) = state.event_log {
                    let _ = log.register_stream(&def_name, history_ttl);
                }
                engine.store_raw_register_json(&def_name, raw_json);
                let diff_json = serde_json::json!({
                    "status": "ok",
                    "kind": "v0",
                    "name": def_name,
                    "added": diff.added,
                    "removed": diff.removed,
                    "backfilling": diff.backfilling,
                });
                return Ok(serde_json::to_vec(&diff_json).unwrap());
            }

            let req: protocol::RegisterRequest = serde_json::from_value(payload)
                .map_err(|e| BeavaError::Protocol(format!("invalid register payload: {}", e)))?;
            let def_name = req.name.clone();
            let is_view = req.definition_type.as_deref() == Some("view");
            // REGISTER needs engine write lock (D-04).
            let mut engine = state.engine.write();
            if is_view {
                let view_def = protocol::convert_view_register_request(req)?;
                engine.register_view(view_def)?;
                engine.store_raw_register_json(&def_name, raw_json);
                Ok(vec![])
            } else {
                let stream_def = protocol::convert_register_request(req)?;
                let diff = engine.register(stream_def)?;
                // Register stream with event log for persistence
                let history_ttl = engine.get_stream(&def_name).and_then(|s| s.history_ttl);
                if let Some(ref log) = state.event_log {
                    let _ = log.register_stream(&def_name, history_ttl);
                }
                engine.store_raw_register_json(&def_name, raw_json);

                // If there are features to backfill, spawn async task (SCHM-03)
                if !diff.backfilling.is_empty() {
                    // Flush event log to ensure all events are readable
                    if let Some(ref log) = state.event_log {
                        let _ = log.fsync_all();
                    }
                    // Read event log entries for this stream
                    let entries = state
                        .event_log
                        .as_ref()
                        .map(|log| log.read_entries(&def_name).unwrap_or_default())
                        .unwrap_or_default();
                    let backfill_features = diff.backfilling.clone();

                    if !entries.is_empty() {
                        let status = Arc::new(BackfillStatus {
                            stream: def_name.clone(),
                            features: backfill_features.clone(),
                            total_events: entries.len(),
                            processed_events: Arc::new(AtomicUsize::new(0)),
                            started_at: SystemTime::now(),
                            completed_at: std::sync::Mutex::new(None),
                        });
                        state
                            .backfill_tracker
                            .tasks
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .push(Arc::clone(&status));

                        let state_clone = state.clone();
                        // Drop engine write lock before spawning (no lock across .await)
                        drop(engine);
                        tokio::spawn(run_backfill(
                            state_clone,
                            def_name.clone(),
                            backfill_features,
                            entries,
                            status,
                        ));

                        // Return diff JSON summary (SCHM-01/02)
                        let diff_json = serde_json::json!({
                            "status": "ok",
                            "added": diff.added,
                            "removed": diff.removed,
                            "backfilling": diff.backfilling,
                        });
                        return Ok(serde_json::to_vec(&diff_json).unwrap());
                    }
                }

                // Return diff JSON summary (SCHM-01/02)
                let diff_json = serde_json::json!({
                    "status": "ok",
                    "added": diff.added,
                    "removed": diff.removed,
                    "backfilling": diff.backfilling,
                });
                Ok(serde_json::to_vec(&diff_json).unwrap())
            }
            })();
            // Phase 25-02: on REGISTER error, emit a safety signal so it
            // surfaces on /debug/warnings. The signal id is stable per
            // pipeline (`register.failure.{name}`) so repeated failures
            // dedupe rather than spam.
            if let Err(ref e) = result {
                crate::server::signals::emit_register_failure(
                    &state.signals,
                    &pipeline_name_for_signal,
                    &format!("{}", e),
                );
            }
            result
        }
        Command::Mget { keys } => {
            let engine = state.engine.read();
            let mut result = serde_json::Map::new();
            for key in &keys {
                let features = engine.get_features(key, &state.store, now);
                let feature_json = feature_map_to_json(&features);
                // Parse the JSON bytes back to a Value for nesting
                let mut value: serde_json::Value = serde_json::from_slice(&feature_json)
                    .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));
                // T-06-03 mitigation: Strip feature names containing "." to avoid
                // leaking internal StreamName.feature qualified names to clients
                if let serde_json::Value::Object(ref mut map) = value {
                    map.retain(|k, _| !k.contains('.'));
                }
                result.insert(key.clone(), value);
            }
            Ok(serde_json::to_vec(&serde_json::Value::Object(result)).unwrap())
        }
        Command::PushTable {
            table_name,
            key,
            fields,
        } => handle_push_table(state, &table_name, &key, fields, now),
        Command::DeleteTable { table_name, key } => {
            handle_delete_table(state, &table_name, &key, now)
        }
        Command::GetMulti { table_names, key } => {
            handle_get_multi(state, &table_names, &key, now)
        }
        Command::ReservedNotImplemented { op_name } => Err(BeavaError::NotImplemented(format!(
            "{} reserved in v0; not implemented",
            op_name
        ))),
        Command::Mset { .. } => unreachable!("MSET handled separately"),
        // I-2: PushAsync and Flush are intercepted in `handle_connection`
        // BEFORE this function is called — see the three-way match on
        // `command` around the `handle_push_core` / `Command::Flush`
        // arms. They can only land here if that dispatch invariant is
        // violated by a refactor; the `unreachable!` protects against
        // that regression (panic in debug, UB-free abort in release).
        Command::PushAsync { .. } | Command::Flush | Command::PushBatch { .. } => {
            unreachable!("PushAsync/Flush/PushBatch handled by handle_connection dispatch")
        }
        // Phase 27-01: OP_SNAPSHOT_FETCH is normally intercepted in
        // `handle_connection` and dispatched to `handle_snapshot_fetch`,
        // which emits a header + payload frame pair instead of a single
        // STATUS_OK envelope. Landing here means the connection-level
        // interception was skipped (e.g. the inner tight-loop sync
        // dispatch after an async burst). Return STATUS_ERROR rather
        // than panicking — the client will see a structured refusal and
        // can retry on a quiescent connection.
        Command::SnapshotFetch { .. } => Err(BeavaError::Protocol(
            "OP_SNAPSHOT_FETCH not supported on this dispatch path (mix with async pushes not allowed)".into(),
        )),
        // Phase 27-02: same reasoning as SnapshotFetch above. Subscribe
        // takes ownership of the connection and is dispatched at the
        // outer loop in `handle_connection`. Landing here means the mix
        // invariant was violated — return a structured error.
        Command::Subscribe { .. } => Err(BeavaError::Protocol(
            "OP_SUBSCRIBE not supported on this dispatch path (connection ownership conflict)".into(),
        )),
        // Phase 35-01: OP_LOG_FETCH is intercepted in `handle_connection`
        // and dispatched to `handle_log_fetch`, which emits a stream of
        // event frames followed by a terminal END frame instead of a
        // single STATUS_OK envelope. Landing here means the outer
        // interception was skipped — return STATUS_ERROR.
        Command::LogFetch { .. } => Err(BeavaError::Protocol(
            "OP_LOG_FETCH not supported on this dispatch path (mix with async pushes not allowed)".into(),
        )),
    }
}

/// Phase 24-02: Dispatch for OP_PUSH_TABLE.
///
/// Flow:
/// 1. Validate the table is registered as `kind=table`; reject unknown name.
/// 2. Convert JSON fields → AHashMap<String, FeatureValue>.
/// 3. Call `StateStore::upsert_table_row`.
/// 4. Fire the Phase 23 TT-cascade hook with tombstoned=false. Plan 03
///    reworks the cascade internals to read from `table_rows` directly;
///    plan 02 just keeps the hook live.
///
/// `now` is wall-clock here. Phase 24-04 will surgically replace this with
/// `_event_time` parsing once watermarks land.
fn handle_push_table(
    state: &SharedState,
    table_name: &str,
    key: &str,
    fields_json: serde_json::Value,
    now: SystemTime,
) -> Result<Vec<u8>, BeavaError> {
    {
        let engine = state.engine.read();
        if !engine.has_registered_table(table_name) {
            return Err(BeavaError::Protocol(format!(
                "unknown table: {}",
                table_name
            )));
        }
    }

    let map = match fields_json {
        serde_json::Value::Object(m) => m,
        _ => {
            return Err(BeavaError::Protocol(
                "OP_PUSH_TABLE fields payload must be a JSON object".into(),
            ))
        }
    };

    // Phase 24-04: parse `_event_time` (stripped from the stored fields —
    // it's metadata, not a column) before converting to FeatureValues.
    // Late-drop: Tables track their own watermark off _event_time.
    let fields_value = serde_json::Value::Object(map);
    let event_time = crate::engine::event_time::parse_event_time(&fields_value, now);
    {
        let engine = state.engine.read();
        let wm = engine.watermarks.watermark(table_name);
        if let Some(wm) = wm {
            if event_time < wm {
                engine.late_drops.increment(table_name);
                return Ok(Vec::new());
            }
        }
        engine.watermarks.observe(table_name, event_time);
    }
    let map = match fields_value {
        serde_json::Value::Object(m) => m,
        _ => unreachable!(),
    };
    let mut fields: ahash::AHashMap<String, FeatureValue> = ahash::AHashMap::new();
    for (k, v) in map {
        if k == crate::engine::event_time::EVENT_TIME_FIELD {
            continue;
        }
        fields.insert(k, json_to_feature_value(v));
    }

    // Phase 25-02: eviction-then-reinit detection. If this (table, key) was
    // evicted recently by TTL, the tracker's bloom will hit and the reinit
    // counter bumps automatically. This is ONLY meaningful when the row did
    // NOT already exist in state — if the row is still live, the upsert is
    // just an update. So we check for existence BEFORE upsert.
    let pre_existed = state.store.get_table_row(key, table_name).is_some();
    if !pre_existed {
        state.eviction_tracker.check_reinit(table_name, key);
    }

    state
        .store
        .upsert_table_row(key, table_name, fields, event_time);
    state.store.mark_dirty(key);

    // Keep Phase 23 TT cascade hook alive. Plan 03 will rework the cascade
    // internals to consume `table_rows` rather than `static_features`.
    {
        let engine = state.engine.read();
        let _ = engine.cascade_table_upsert(table_name, key, false, &state.store, event_time);
    }

    Ok(Vec::new())
}

/// Phase 24-02: Dispatch for OP_DELETE_TABLE. Symmetric with `handle_push_table`.
///
/// Phase 24-04: advances the Table's watermark off `_event_time` if the
/// DELETE payload carried one. The opcode wire format (Phase 24-02)
/// doesn't include a JSON fields payload, so delete's event-time is
/// always wall-clock — the code path is here so future protocol
/// expansion (delete-with-metadata) has the hook in place.
fn handle_delete_table(
    state: &SharedState,
    table_name: &str,
    key: &str,
    now: SystemTime,
) -> Result<Vec<u8>, BeavaError> {
    {
        let engine = state.engine.read();
        if !engine.has_registered_table(table_name) {
            return Err(BeavaError::Protocol(format!(
                "unknown table: {}",
                table_name
            )));
        }
    }

    let event_time = now;
    {
        let engine = state.engine.read();
        let wm = engine.watermarks.watermark(table_name);
        if let Some(wm) = wm {
            if event_time < wm {
                engine.late_drops.increment(table_name);
                return Ok(Vec::new());
            }
        }
        engine.watermarks.observe(table_name, event_time);
    }

    state.store.tombstone_table_row(key, table_name, event_time);
    state.store.mark_dirty(key);

    {
        let engine = state.engine.read();
        let _ = engine.cascade_table_upsert(table_name, key, true, &state.store, event_time);
    }

    Ok(Vec::new())
}

/// Phase 25-01: Dispatch for OP_GET_MULTI.
///
/// Assembles a per-table null-collapsed feature vector for a single entity
/// key in one TCP round-trip.
///
/// Behaviour contract (see 25-01-PLAN.md):
///
/// 1. Validate EVERY requested `table_name` is registered as a Table BEFORE
///    any state read. The first unknown name aborts with `BeavaError::Protocol`
///    (maps to `STATUS_ERROR`) — no partial state read, no partial response
///    (T-25-01-03 tampering mitigation).
/// 2. For each registered `name`, project `state.store.collect_table_row_view`.
///    Never-seen, tombstoned, and empty-registered collapse to `null`.
/// 3. Serialize the response as a JSON object `{name: row|null, ...}` in
///    request order (serde_json::Map preserves insertion order behind the
///    `preserve_order` feature; falling back to string-sorted order is
///    acceptable since the `keys()` iterator on the Python side does not
///    promise insertion order across serde_json without the feature).
///    To guarantee request-order serialization regardless of serde_json
///    feature flags, we build the JSON by hand.
fn handle_get_multi(
    state: &SharedState,
    table_names: &[String],
    key: &str,
    now: SystemTime,
) -> Result<Vec<u8>, BeavaError> {
    // (1) Validate every table is registered under engine read lock.
    {
        let engine = state.engine.read();
        for name in table_names {
            if !engine.has_registered_table(name) {
                return Err(BeavaError::Protocol(format!("unknown table: {}", name)));
            }
        }
    }

    // (2) Project each table's row view; null-collapse per spec.
    // Build the JSON body by hand so the response keys serialize in the
    // request order the client gave us — this is independent of whether
    // serde_json was built with `preserve_order`.
    let mut body = Vec::<u8>::with_capacity(64 + 32 * table_names.len());
    body.push(b'{');
    for (i, name) in table_names.iter().enumerate() {
        if i > 0 {
            body.push(b',');
        }
        // Encode the key as a JSON string via serde_json to handle escaping.
        let key_json = serde_json::to_vec(name)
            .map_err(|e| BeavaError::Protocol(format!("GET_MULTI key serialize: {}", e)))?;
        body.extend_from_slice(&key_json);
        body.push(b':');
        match state.store.collect_table_row_view(key, name, now) {
            Some(row) => {
                let row_bytes = serde_json::to_vec(&row)
                    .map_err(|e| BeavaError::Protocol(format!("GET_MULTI row serialize: {}", e)))?;
                body.extend_from_slice(&row_bytes);
            }
            None => body.extend_from_slice(b"null"),
        }
    }
    body.push(b'}');
    Ok(body)
}

/// Cooperative backfill: reads event log entries, pushes to new operators in 64-event chunks.
/// On completion, adds (stream, feature) pairs to backfill_complete set for persistence.
/// Clears existing operator state for backfill features before replay to ensure idempotent restart.
pub async fn run_backfill(
    state: SharedState,
    stream_name: String,
    feature_names: Vec<String>,
    entries: Vec<LogEntry>,
    status: Arc<BackfillStatus>,
) {
    // Clear any existing operator state for backfill features (idempotent restart).
    // This ensures a re-run after crash produces the same result as a fresh run.
    {
        let keys: Vec<String> = state.store.entity_keys();
        for key in &keys {
            if let Some(mut entity) = state.store.get_entity_mut(key) {
                if let Some(stream_state) = entity.streams.get_mut(&stream_name) {
                    stream_state
                        .operators
                        .retain(|(name, _)| !feature_names.contains(name));
                }
            }
        }
    }

    // Phase 25-02: detect backfill miss — the configured history_ttl is
    // wider than what the log still holds. Compare the oldest entry's
    // timestamp against (now - history_ttl); if the oldest surviving entry
    // is younger than the cutoff, compaction has trimmed the head of the
    // log and this backfill is guaranteed incomplete.
    if let (Some(earliest), Some(history_ttl)) = (
        entries.iter().map(|e| e.timestamp).min(),
        state
            .engine
            .read()
            .get_stream(&stream_name)
            .and_then(|s| s.history_ttl),
    ) {
        let now_ts = SystemTime::now();
        let window_floor = now_ts
            .checked_sub(history_ttl)
            .unwrap_or(std::time::UNIX_EPOCH);
        if earliest > window_floor {
            let mut m = state.metrics.lock();
            *m.history_backfill_misses_total
                .entry(stream_name.clone())
                .or_insert(0) += 1;
            // Track max observed backfill span (seconds).
            if let Some(latest) = entries.iter().map(|e| e.timestamp).max() {
                let span = latest
                    .duration_since(earliest)
                    .unwrap_or(std::time::Duration::ZERO)
                    .as_secs();
                let cur = m
                    .max_backfill_span_seen
                    .entry(stream_name.clone())
                    .or_insert(0);
                if span > *cur {
                    *cur = span;
                }
            }
        }
    }

    let total = entries.len();
    for (chunk_idx, chunk) in entries.chunks(64).enumerate() {
        {
            let engine = state.engine.read();
            for entry in chunk {
                // Plan 11-06: dispatch on log payload format byte.
                // LOG_FMT_BINARY → decode binary wire format.
                // LOG_FMT_JSON or legacy (no prefix) → serde_json::from_slice.
                use crate::state::event_log::{decode_log_payload, LOG_FMT_BINARY, LOG_FMT_JSON};
                let (fmt, body) = decode_log_payload(&entry.payload);
                let event: serde_json::Value = match fmt {
                    LOG_FMT_BINARY => {
                        let mut buf = body;
                        match crate::server::protocol::decode_event_binary(&mut buf) {
                            Ok(v) => v,
                            Err(_) => continue, // Skip malformed entries (T-08-08)
                        }
                    }
                    LOG_FMT_JSON => match serde_json::from_slice(body) {
                        Ok(v) => v,
                        Err(_) => continue,
                    },
                    _ => continue,
                };
                // D-15 / CORR-06: bucket the replayed event by its payload _event_time,
                // falling back to entry.timestamp only when the payload has no _event_time.
                // This matches live-ingest semantics exactly so crash-replay produces
                // bit-identical feature values.
                let event_time =
                    crate::engine::event_time::parse_event_time(&event, entry.timestamp);
                let _ = engine.push_for_backfill(
                    &stream_name,
                    &event,
                    &state.store,
                    event_time, // was: entry.timestamp (D-15 fix)
                    &feature_names,
                );
                // Mark entity key dirty for incremental snapshots (OPS-03)
                if let Some(stream_def) = engine.get_stream(&stream_name) {
                    if let Some(ref kf) = stream_def.key_field {
                        if let Some(serde_json::Value::String(key_val)) = event.get(kf.as_str()) {
                            if !key_val.is_empty() {
                                state.store.mark_dirty(key_val);
                            }
                        }
                    }
                }
            }
        } // Engine read lock released before yield
          // Update progress
        let processed = std::cmp::min((chunk_idx + 1) * 64, total);
        status.processed_events.store(processed, Ordering::Relaxed);
        tokio::task::yield_now().await; // Cooperative yield (SCHM-04)
    }
    // Mark complete in tracker
    *status
        .completed_at
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = Some(SystemTime::now());
    // Persist completion markers so restart detects this backfill as done
    {
        let mut backfill_complete = state.backfill_complete.lock();
        for feat in &feature_names {
            backfill_complete.insert((stream_name.clone(), feat.clone()));
        }
    }
}

/// Convert a serde_json::Value to FeatureValue for SET/MSET writes.
fn json_to_feature_value(v: serde_json::Value) -> FeatureValue {
    match v {
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                FeatureValue::Int(i)
            } else if let Some(f) = n.as_f64() {
                FeatureValue::Float(f)
            } else {
                FeatureValue::Missing
            }
        }
        serde_json::Value::String(s) => FeatureValue::String(s),
        serde_json::Value::Null => FeatureValue::Missing,
        serde_json::Value::Bool(b) => FeatureValue::Int(if b { 1 } else { 0 }),
        _ => FeatureValue::Missing, // Arrays/objects -> Missing
    }
}

/// Phase 27-01: dispatch for `OP_SNAPSHOT_FETCH`.
///
/// Flow (locked by `27-01-PLAN.md` + user direction on admin-token shape):
///
/// 1. **Admin-token gate.** If `state.admin_token` is `Some(expected)`,
///    the client-provided `admin_token` must match exactly. If the server
///    has no admin token configured, reject (the replica wire is admin-
///    only in v0; a misconfigured server refuses rather than leaking
///    snapshot bytes to anonymous clients). Failure → `BeavaError::Auth`.
/// 2. **Capture `snapshot_taken_at = SystemTime::now()`.** Response-only;
///    never persisted.
/// 3. **Collect known streams.** From `state.engine.read().list_streams()`.
/// 4. **Validate scope.** `protocol::validate_scope` runs all seven locked
///    rejection rules. Failure → `BeavaError::Protocol`.
/// 5. **Acquire the base snapshot.** Read the on-disk snapshot file via
///    `load_snapshot_file`. If the snapshot file is missing / corrupt,
///    synthesize an empty `BaseSnapshotState` so that a fresh server with
///    no snapshot still returns a valid (empty) response.
/// 6. **Filter in-memory.** `replica::filter_base_snapshot`.
/// 7. **Serialize filtered state with postcard** and emit the header +
///    payload frame pair. Increment
///    `beava_replica_snapshot_bytes_sent_total` by payload length.
async fn handle_snapshot_fetch(
    writer: &mut BufWriter<tokio::net::tcp::OwnedWriteHalf>,
    admin_token: &str,
    scope: protocol::Scope,
    state: &SharedState,
) -> Result<(), BeavaError> {
    // (1) Admin-token gate — wire-level bearer check (no loopback bypass on
    // the replica path; admin-only always). Empty expected token (`Some("")`)
    // is not a supported configuration — treat as unauthenticated.
    let authed = matches!(
        state.admin_token.as_deref(),
        Some(expected) if !expected.is_empty() && expected == admin_token
    );
    if !authed {
        return Err(BeavaError::Protocol("unauthorized".into()));
    }

    // (2) Capture response-only timestamp.
    let snapshot_taken_at = SystemTime::now();

    // (3) Known-streams lookup from the pipeline registry.
    let known: std::collections::HashSet<String> = {
        let engine = state.engine.read();
        engine.list_streams().map(|s| s.name.clone()).collect()
    };

    // (4) Validate — all seven rules.
    if let Err(e) = protocol::validate_scope(&scope, &known) {
        return Err(BeavaError::Protocol(format!("invalid scope: {}", e)));
    }

    // (5) Acquire the base snapshot. Missing / corrupt file → empty.
    let base = load_base_snapshot_for_fetch(state);

    // (6) Filter in-memory.
    let filtered = crate::server::replica::filter_base_snapshot(&base, &scope);

    // (7) Serialize + emit header + payload frames.
    let payload_bytes = postcard::to_allocvec(&filtered)
        .map_err(|e| BeavaError::Protocol(format!("snapshot serialize failed: {}", e)))?;

    // Header frame body: [u64 BE ts_secs][u32 BE ts_nanos]. `encode_frame`
    // adds the 4-byte length prefix + 1-byte tag (reused as "opcode" here —
    // the framing shape is identical to every other TCP frame).
    let (ts_secs, ts_nanos) = match snapshot_taken_at.duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => (d.as_secs(), d.subsec_nanos()),
        // Clock running backwards (extremely unlikely) — emit zeros.
        Err(_) => (0u64, 0u32),
    };
    let mut header_body = Vec::with_capacity(12);
    header_body.extend_from_slice(&ts_secs.to_be_bytes());
    header_body.extend_from_slice(&ts_nanos.to_be_bytes());
    let header_frame = protocol::encode_frame(protocol::REPLICA_FRAME_TAG_HEADER, &header_body);
    writer
        .write_all(&header_frame)
        .await
        .map_err(|e| BeavaError::Protocol(format!("write snapshot header frame failed: {}", e)))?;

    let payload_frame = protocol::encode_frame(protocol::REPLICA_FRAME_TAG_PAYLOAD, &payload_bytes);
    writer
        .write_all(&payload_frame)
        .await
        .map_err(|e| BeavaError::Protocol(format!("write snapshot payload frame failed: {}", e)))?;
    writer
        .flush()
        .await
        .map_err(|e| BeavaError::Protocol(format!("flush snapshot response failed: {}", e)))?;

    // (7c) Metric bump.
    crate::server::replica::record_snapshot_bytes_sent(payload_bytes.len() as u64);
    Ok(())
}

/// Phase 27-02: live-subscribe handler.
///
/// Takes ownership of the connection for the whole subscription lifetime
/// (user direction §1). Flow:
///
/// 1. Admin-token gate (same shape as `handle_snapshot_fetch`). Failure
///    emits a `safety/error` signal and a `STATUS_ERROR` frame; returns.
/// 2. Collect known streams, run `validate_scope`. Rejection →
///    `STATUS_ERROR` frame + return.
/// 3. Create a bounded `mpsc::channel<ReplicaEvent>(10_000)` (user
///    direction §A1). Register the session in
///    `state.subscriber_registry`; the ingest hook on the pipeline
///    engine (Task 2) now observes this subscriber.
/// 4. `tokio::select!` in a loop between:
///       - `rx.recv()` → encode event frame and write to the socket.
///         Write failure = disconnect; break out.
///       - a reader-EOF probe (`reader.fill_buf()` returning 0 bytes or
///         erroring) → client closed the socket; break out.
/// 5. On exit: `drop_subscriber(conn_id, "disconnect")`. The registry
///    drop also closes the Sender, so if the loop exited via reader-EOF
///    (not channel-closed) the residual queued events are silently
///    discarded, which is intentional — disconnect trumps delivery.
///
/// This function does NOT spawn the drain task on a separate tokio task
/// (plan action wording notwithstanding) — running it inline keeps the
/// `BufReader` / `BufWriter` halves together without unsafe ownership
/// juggling across tasks, and the outer `handle_connection` already runs
/// per-connection on its own tokio task, so we inherit the isolation.
async fn handle_subscribe(
    reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: &mut BufWriter<tokio::net::tcp::OwnedWriteHalf>,
    admin_token: &str,
    scope: protocol::Scope,
    state: &SharedState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // (1) Admin-token gate (same shape as snapshot-fetch; empty expected
    // token is not a supported configuration).
    let authed = matches!(
        state.admin_token.as_deref(),
        Some(expected) if !expected.is_empty() && expected == admin_token
    );
    if !authed {
        // Peer address — the OwnedReadHalf doesn't expose it; we record
        // "unknown" for the signal and rely on operators to correlate via
        // the signal's first_seen timestamp.
        crate::server::signals::emit_replica_auth_failure(&state.signals, "unknown");
        let resp = protocol::encode_response(STATUS_ERROR, b"unauthorized");
        writer.write_all(&resp).await?;
        writer.flush().await?;
        return Ok(());
    }

    // (2) Known-streams + validate_scope.
    let known: std::collections::HashSet<String> = {
        let engine = state.engine.read();
        engine.list_streams().map(|s| s.name.clone()).collect()
    };
    if let Err(e) = protocol::validate_scope(&scope, &known) {
        let msg = format!("invalid scope: {}", e);
        let resp = protocol::encode_response(STATUS_ERROR, msg.as_bytes());
        writer.write_all(&resp).await?;
        writer.flush().await?;
        return Ok(());
    }

    // (3) Register session.
    let (tx, mut rx) = tokio::sync::mpsc::channel::<crate::server::replica::ReplicaEvent>(
        crate::server::replica::SUBSCRIBER_CHANNEL_CAPACITY,
    );
    let conn_id = state.subscriber_registry.register(scope, tx);

    // (4) Drain loop. We use a tokio::select! over:
    //   - rx.recv(): next queued event, encode + write + flush.
    //   - reader.read_u8(): any byte from the client. SUBSCRIBE doesn't
    //     accept any further frames, so:
    //       Ok(_) = protocol violation → drop and close.
    //       Err(UnexpectedEof) = clean disconnect → drop and close.
    //       Err(other)         = network error → drop and close.
    use tokio::io::AsyncReadExt;

    loop {
        tokio::select! {
            biased;
            maybe_ev = rx.recv() => {
                match maybe_ev {
                    Some(ev) => {
                        let frame = protocol::encode_event_frame(ev.timestamp, &ev.payload);
                        if writer.write_all(&frame).await.is_err() {
                            break;
                        }
                        if writer.flush().await.is_err() {
                            break;
                        }
                    }
                    None => {
                        // Registry dropped our sender (e.g. backpressure
                        // drop on a notify). Exit the loop; the drop
                        // counter was already bumped by drop_subscriber.
                        return Ok(());
                    }
                }
            }
            read_result = reader.read_u8() => {
                // Any activity from the client is a disconnect (clean EOF
                // or a protocol violation — either way we're done).
                let _ = read_result;
                break;
            }
        }
    }

    // (5) Clean-up. Safe to call even if the session was already removed
    // (drop_subscriber is idempotent).
    state
        .subscriber_registry
        .drop_subscriber(conn_id, "disconnect");
    Ok(())
}

/// Phase 35-01: dispatch for `OP_LOG_FETCH`.
///
/// Flow:
///
/// 1. **Admin-token gate** (same shape as `handle_snapshot_fetch` /
///    `handle_subscribe`). Empty expected token is not a supported
///    configuration — treat as unauthenticated. Failure emits a
///    `safety/error` signal via `emit_replica_auth_failure` and returns
///    `BeavaError::Protocol("unauthorized")`; the caller wraps it in a
///    STATUS_ERROR frame.
/// 2. **Collect known streams** from `state.engine.read().list_streams()`.
/// 3. **Validate scope** via `protocol::validate_scope` — all seven
///    locked rejection rules from Phase 27.
/// 4. **Walk each requested stream's log file.** For each stream in
///    `scope.streams` (in scope-declared order — no cross-stream merge):
///      a. Call `event_log.read_entries(stream)` under a short lock hold
///         (the per-stream log is read end-to-end; v0 accepts the
///         memory cost per 35-CONTEXT.md §specifics).
///      b. For each `LogEntry`:
///         - Gate on `entry.timestamp.duration_since(UNIX_EPOCH).as_millis()
///           >= from_ts_millis` (inclusive; boundary duplicates
///           acceptable per the opcode doc-comment).
///         - Decode the payload (JSON or binary tagged, via
///           `decode_log_payload` → `decode_event_binary` /
///           `serde_json::from_slice`) to extract the key via the
///           stream's `key_field`. Malformed entries are skipped silently
///           (T-08-08 stance — same as the backfill path in `handle_backfill`).
///         - For keyed streams, gate on `entity_matches_scope(&[stream],
///           key, &scope)`. Keyless streams (no `key_field`) are not
///           supported here in v0 — their entries are skipped because
///           `entity_matches_scope` with a zero-length key + key filters
///           is ill-defined and 27-CONTEXT locked key-bearing events
///           only for replica delivery.
///         - Write one `encode_log_event_frame(ts_ms, raw_payload)` frame
///           and bump `beava_replica_log_entries_sent_total{stream}`.
/// 5. **Terminal frame:** after all streams drained, write one
///    `encode_log_end_frame()` (tag 0x04, empty body) so the client
///    knows the response is complete.
///
/// I/O failures during writes are propagated as `BeavaError::Protocol`.
// clippy's doc_overindented_list_items heuristic doesn't agree with the
// nested numbered/lettered/bulleted structure above; the rendering in
// rustdoc is correct as-is.
#[allow(clippy::doc_overindented_list_items)]
async fn handle_log_fetch(
    writer: &mut BufWriter<tokio::net::tcp::OwnedWriteHalf>,
    admin_token: &str,
    from_ts_millis: u64,
    scope: protocol::Scope,
    state: &SharedState,
) -> Result<(), BeavaError> {
    // (1) Admin-token gate.
    let authed = matches!(
        state.admin_token.as_deref(),
        Some(expected) if !expected.is_empty() && expected == admin_token
    );
    if !authed {
        crate::server::signals::emit_replica_auth_failure(&state.signals, "unknown");
        return Err(BeavaError::Protocol("unauthorized".into()));
    }

    // (2) Known-streams lookup + per-stream key_field snapshot. We lock the
    // engine once, snapshot the key_field for every requested stream, and
    // drop the lock before doing any log I/O.
    let known: std::collections::HashSet<String>;
    let key_fields: std::collections::HashMap<String, Option<String>>;
    {
        let engine = state.engine.read();
        known = engine.list_streams().map(|s| s.name.clone()).collect();
        key_fields = scope
            .streams
            .iter()
            .map(|s| {
                let kf = engine.get_stream(s).and_then(|d| d.key_field.clone());
                (s.clone(), kf)
            })
            .collect();
    }

    // (3) Validate scope.
    if let Err(e) = protocol::validate_scope(&scope, &known) {
        return Err(BeavaError::Protocol(format!("invalid scope: {}", e)));
    }

    // (4) Flush the event-log writer before reading, so LOG_FETCH sees
    // every durable entry the background fsync timer hasn't yet fired on.
    // Without this, recent PUSHes can sit in the BufWriter's in-memory
    // buffer and `read_entries` (which opens the file fresh) misses them.
    // Replica reads are relatively rare — paying a sync on each LOG_FETCH
    // is acceptable for v0 and matches the "scientist isn't calling this
    // in a tight loop" note in 35-CONTEXT.md §specifics.
    if let Some(ref log) = state.event_log {
        let _ = log.fsync_all();
    }

    // (5) Walk each stream's log file.
    use crate::state::event_log::{decode_log_payload, LOG_FMT_BINARY, LOG_FMT_JSON};
    use std::time::UNIX_EPOCH;

    // Frame-write batching: instead of `writer.write_all(&frame).await` per
    // event (5 M awaits on a 5 M-event LOG_FETCH), accumulate framed bytes
    // into this per-call buffer and flush only when it crosses
    // `LOG_FETCH_FLUSH_BYTES`. Cuts tokio yields by ~3000× on the big-log
    // path. See bench_log_fetch_upstream.rs for the profile that motivated
    // this — the CPU work was already sub-300 ns/event; most of the 1.19 s
    // "filter+encode+write-to-sink" time at 5 M events was actually the
    // per-event `write_all` async machinery.
    const LOG_FETCH_FLUSH_BYTES: usize = 256 * 1024;
    let mut out_buf: Vec<u8> = Vec::with_capacity(LOG_FETCH_FLUSH_BYTES + 1024);

    for stream_name in &scope.streams {
        // Phase 40: `read_entries` uses `&self` and opens the file fresh;
        // no lock on `state.event_log` needed.
        let entries: Vec<crate::state::event_log::LogEntry> = match &state.event_log {
            Some(log) => log.read_entries(stream_name).unwrap_or_default(),
            None => Vec::new(),
        };

        let kf = key_fields.get(stream_name).cloned().flatten();
        let stream_arr = [stream_name.as_str()];
        let mut frames_this_stream: u64 = 0;

        for entry in entries {
            // Timestamp gate (inclusive).
            let ts_ms = match entry.timestamp.duration_since(UNIX_EPOCH) {
                Ok(d) => d.as_millis().min(u64::MAX as u128) as u64,
                Err(_) => 0, // pre-epoch: treat as ts=0 (admits everything)
            };
            if ts_ms < from_ts_millis {
                continue;
            }

            // Key extraction + scope filter.
            //
            // Keyed streams: extract the per-event key from the declared
            // key_field and apply the full (stream + keys + key_prefix) scope
            // filter. Drop events missing or malformed at their key field.
            //
            // Keyless streams (v0: @bv.stream has no `key=` arg) AND caller
            // supplied `keys`/`key_prefix`: fall back to searching every
            // string field in the decoded payload for a match. This lets
            // scope-filtered forks work against the common pattern where the
            // stream carries an entity field (e.g. `user_id`) that its
            // downstream @bv.table uses as its key. Without this fallback,
            // fork(..., keys=[...]) against a keyless stream would emit
            // zero events — see the bench regression in 2026-04.
            match &kf {
                Some(kf_name) => {
                    let (fmt, body) = decode_log_payload(&entry.payload);
                    let event_value: serde_json::Value = match fmt {
                        LOG_FMT_BINARY => {
                            let mut buf = body;
                            match protocol::decode_event_binary(&mut buf) {
                                Ok(v) => v,
                                Err(_) => continue, // skip malformed (T-08-08)
                            }
                        }
                        LOG_FMT_JSON => match serde_json::from_slice(body) {
                            Ok(v) => v,
                            Err(_) => continue,
                        },
                        _ => continue,
                    };
                    let key = match event_value.get(kf_name.as_str()) {
                        Some(serde_json::Value::String(s)) if !s.is_empty() => s.as_str(),
                        _ => continue,
                    };
                    if !crate::server::replica::entity_matches_scope(&stream_arr, key, &scope) {
                        continue;
                    }
                }
                None => {
                    if scope.keys.is_some() || scope.key_prefix.is_some() {
                        // Try the any-string-field fallback. Decode the
                        // event body, scan string fields, and emit if ANY
                        // matches the scope. This is a scope-OVER-approximation:
                        // we may emit events that look matching on a
                        // non-key field, which is harmless because the
                        // replica re-filters at extraction time.
                        let (fmt, body) = decode_log_payload(&entry.payload);
                        let event_value: Option<serde_json::Value> = match fmt {
                            LOG_FMT_BINARY => {
                                let mut buf = body;
                                protocol::decode_event_binary(&mut buf).ok()
                            }
                            LOG_FMT_JSON => serde_json::from_slice(body).ok(),
                            _ => None,
                        };
                        let any_match = event_value
                            .as_ref()
                            .and_then(|v| v.as_object())
                            .map(|obj| {
                                obj.values().any(|val| match val.as_str() {
                                    Some(s) if !s.is_empty() => {
                                        crate::server::replica::entity_matches_scope(
                                            &stream_arr,
                                            s,
                                            &scope,
                                        )
                                    }
                                    _ => false,
                                })
                            })
                            .unwrap_or(false);
                        if !any_match {
                            continue;
                        }
                    }
                    // Stream overlap already guaranteed by the outer loop.
                }
            }

            // Emit event frame by appending directly to the per-call batch
            // buffer (no intermediate `Vec<u8>` allocation from
            // `encode_log_event_frame`, no per-event tokio yield).
            // Frame layout: [u32 body_len][u8 tag][u64 ts_ms][u32 payload_len][payload]
            let body_len = 1 + 8 + 4 + entry.payload.len();
            out_buf.extend_from_slice(&(body_len as u32).to_be_bytes());
            out_buf.push(protocol::REPLICA_FRAME_TAG_EVENT);
            out_buf.extend_from_slice(&ts_ms.to_be_bytes());
            out_buf.extend_from_slice(&(entry.payload.len() as u32).to_be_bytes());
            out_buf.extend_from_slice(&entry.payload);
            frames_this_stream += 1;
            if out_buf.len() >= LOG_FETCH_FLUSH_BYTES {
                writer.write_all(&out_buf).await.map_err(|e| {
                    BeavaError::Protocol(format!("write log-fetch event batch failed: {}", e))
                })?;
                out_buf.clear();
            }
        }

        // Amortized per-stream counter bump — single DashMap lookup per
        // LOG_FETCH instead of one per event.
        crate::server::replica::bump_log_entries_sent_by(stream_name, frames_this_stream);
    }

    // Flush whatever is left in the batch buffer before the END frame.
    if !out_buf.is_empty() {
        writer.write_all(&out_buf).await.map_err(|e| {
            BeavaError::Protocol(format!("write log-fetch event batch tail failed: {}", e))
        })?;
        out_buf.clear();
    }

    // (6) Terminal END frame.
    let end = protocol::encode_log_end_frame();
    writer
        .write_all(&end)
        .await
        .map_err(|e| BeavaError::Protocol(format!("write log-fetch end frame failed: {}", e)))?;
    writer
        .flush()
        .await
        .map_err(|e| BeavaError::Protocol(format!("flush log-fetch response failed: {}", e)))?;
    Ok(())
}

/// Phase 27-01 helper: read the current base snapshot off disk.
///
/// Source-of-truth lookup order (matches `src/main.rs::load_incremental_snapshots`
/// for startup — we want the replica endpoint to see what recovery would see):
///   1. Highest-sequence `beava.snapshot.base.*` in `snapshot_path`'s parent
///      directory (the Phase 9+ layout production actually writes).
///   2. The file at `snapshot_path` itself, if it exists (legacy single-blob
///      layout + a convenient test escape hatch).
///   3. An empty `BaseSnapshotState` otherwise. Missing / corrupt / Delta
///      files all fall through to empty. This keeps the wire contract intact
///      on fresh servers and servers that haven't taken their first base
///      snapshot yet.
///
/// Note: this deliberately does NOT apply delta snapshots on top of the base.
/// The v0 replica contract is "ship the latest persisted base"; delta-aware
/// replication is out of scope for 27-01 per `27-01-PLAN.md §objective`.
fn load_base_snapshot_for_fetch(state: &SharedState) -> crate::state::snapshot::BaseSnapshotState {
    use crate::state::snapshot::{
        load_snapshot_file, BaseSnapshotState, SnapshotFile, SnapshotHeader, SnapshotType,
    };
    let empty = || BaseSnapshotState {
        header: SnapshotHeader {
            snapshot_type: SnapshotType::Base,
            sequence: 0,
        },
        entities: vec![],
        pipelines: vec![],
        backfill_complete: vec![],
    };

    // (1) Scan parent dir for `beava.snapshot.base.*` — pick the highest seq.
    let snap_dir = state
        .snapshot_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    let mut best: Option<(u64, std::path::PathBuf)> = None;
    if let Ok(entries) = std::fs::read_dir(snap_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy().into_owned();
            if let Some(seq_str) = name_str.strip_prefix("beava.snapshot.base.") {
                if let Ok(seq) = seq_str.parse::<u64>() {
                    match &best {
                        Some((cur, _)) if *cur >= seq => {}
                        _ => best = Some((seq, entry.path())),
                    }
                }
            }
        }
    }
    if let Some((_, path)) = best {
        if let Ok(bytes) = std::fs::read(&path) {
            if let Some(SnapshotFile::Base(b)) = load_snapshot_file(&bytes) {
                return b;
            }
        }
    }

    // (2) Legacy / test: read `snapshot_path` directly.
    if let Ok(bytes) = std::fs::read(&state.snapshot_path) {
        if let Some(SnapshotFile::Base(b)) = load_snapshot_file(&bytes) {
            return b;
        }
    }

    empty()
}

/// Handle MSET with cooperative yielding: process 1024-key chunks, yield between.
async fn handle_mset(
    entries: Vec<(String, serde_json::Value)>,
    state: &SharedState,
) -> Result<Vec<u8>, BeavaError> {
    let now = SystemTime::now();
    for chunk in entries.chunks(1024) {
        for (key, payload) in chunk {
            if let serde_json::Value::Object(map) = payload {
                for (feat_name, val) in map {
                    let fv = json_to_feature_value(val.clone());
                    state.store.set_static(key, feat_name, fv, now);
                }
                // Mark entity key dirty once per chunk iteration (OPS-03)
                state.store.mark_dirty(key);
            }
            // Skip non-object payloads silently (defensive)
        }
        tokio::task::yield_now().await;
    }
    Ok(vec![])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::pipeline::{FeatureDef, StreamDefinition};
    use std::time::Duration;

    /// Helper: create shared state with empty engine + store.
    /// Phase 14: creates Arc<ConcurrentAppState> instead of Arc<Mutex<AppState>>.
    fn make_shared_state() -> SharedState {
        make_concurrent_state(
            PipelineEngine::new(),
            StateStore::new(),
            None,
            std::path::PathBuf::from("test.snapshot"),
            Arc::new(BackfillTracker::default()),
            true,
            false,
        )
    }

    /// Helper: register a simple stream with count and sum features.
    fn register_tx_stream(state: &SharedState) {
        let stream = StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
            features: vec![
                (
                    "tx_count_1h".into(),
                    FeatureDef::Count {
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                        where_expr: None,
                        backfill: false,
                    },
                ),
                (
                    "tx_sum_1h".into(),
                    FeatureDef::Sum {
                        field: "amount".into(),
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                        optional: false,
                        where_expr: None,
                        backfill: false,
                    },
                ),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
        };
        state.engine.write().register(stream).unwrap();
    }

    // --- ConcurrentAppState type tests ---

    #[test]
    fn test_app_state_wraps_engine_and_store() {
        let state = make_shared_state();
        assert_eq!(state.engine.read().stream_count(), 0);
        assert_eq!(state.store.entity_count(), 0);
    }

    #[test]
    fn test_shared_state_is_arc_concurrent() {
        // Verify SharedState is Arc<ConcurrentAppState> by cloning
        let state: SharedState = make_shared_state();
        let state2 = state.clone();
        drop(state2); // Would fail if not Arc
        let _engine = state.engine.read(); // Would fail if not RwLock
    }

    // --- PUSH command tests ---

    #[test]
    fn test_push_registered_stream_returns_empty_ack() {
        // Phase 11 read-skip: sync push returns an empty feature map as an
        // ack-only response. Callers that need features must use OP_GET.
        let state = make_shared_state();
        register_tx_stream(&state);

        let cmd = Command::Push {
            stream_name: "Transactions".into(),
            payload: serde_json::json!({"user_id": "u123", "amount": 50.0}),
            raw_payload: Vec::new(),
        };
        let result = handle_sync_command(cmd, &state);
        assert!(result.is_ok());

        let bytes = result.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json, serde_json::json!({}));

        // Verify the underlying state WAS updated even though we didn't return it.
        let get_cmd = Command::Get { key: "u123".into() };
        let get_bytes = handle_sync_command(get_cmd, &state).unwrap();
        let get_json: serde_json::Value = serde_json::from_slice(&get_bytes).unwrap();
        assert_eq!(get_json["tx_count_1h"], 1);
        assert_eq!(get_json["tx_sum_1h"], 50.0);
    }

    #[test]
    fn test_push_unregistered_stream_returns_error() {
        let state = make_shared_state();
        let cmd = Command::Push {
            stream_name: "NonExistent".into(),
            payload: serde_json::json!({"user_id": "u123"}),
            raw_payload: Vec::new(),
        };
        let result = handle_sync_command(cmd, &state);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("unknown stream"));
    }

    // --- GET command tests ---

    #[test]
    fn test_get_existing_key_returns_features() {
        let state = make_shared_state();
        register_tx_stream(&state);

        // Push an event first
        let push_cmd = Command::Push {
            stream_name: "Transactions".into(),
            payload: serde_json::json!({"user_id": "u123", "amount": 50.0}),
            raw_payload: Vec::new(),
        };
        handle_sync_command(push_cmd, &state).unwrap();

        // GET should return features
        let get_cmd = Command::Get { key: "u123".into() };
        let result = handle_sync_command(get_cmd, &state);
        assert!(result.is_ok());

        let bytes = result.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["tx_count_1h"], 1);
        assert_eq!(json["tx_sum_1h"], 50.0);
    }

    #[test]
    fn test_get_unknown_key_returns_empty_json() {
        let state = make_shared_state();
        let cmd = Command::Get {
            key: "nonexistent".into(),
        };
        let result = handle_sync_command(cmd, &state);
        assert!(result.is_ok());
        let bytes = result.unwrap();
        assert_eq!(bytes, b"{}");
    }

    // --- SET command tests ---

    #[test]
    fn test_set_writes_static_features() {
        let state = make_shared_state();
        let cmd = Command::Set {
            key: "u123".into(),
            payload: serde_json::json!({"lifetime_value": 4500.0, "segment": "high_value"}),
        };
        let result = handle_sync_command(cmd, &state);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty()); // Empty payload on success

        // Verify the features were written
        let entity = state.store.get_entity("u123").unwrap();
        assert_eq!(
            entity.static_features.get("segment").unwrap().value,
            FeatureValue::String("high_value".into())
        );
    }

    #[test]
    fn test_set_non_object_payload_returns_error() {
        let state = make_shared_state();
        let cmd = Command::Set {
            key: "u123".into(),
            payload: serde_json::json!("not an object"),
        };
        let result = handle_sync_command(cmd, &state);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("SET payload must be a JSON object"));
    }

    // --- REGISTER command tests ---

    #[test]
    fn test_register_valid_stream() {
        let state = make_shared_state();
        let cmd = Command::Register {
            payload: serde_json::json!({
                "name": "Logins",
                "key_field": "user_id",
                "features": [
                    {"name": "login_count_1h", "type": "count", "window": "1h"}
                ]
            }),
        };
        let result = handle_sync_command(cmd, &state);
        assert!(result.is_ok());
        // REGISTER now returns diff JSON for streams (SCHM-01/02)
        let response_bytes = result.unwrap();
        assert!(!response_bytes.is_empty());
        let diff_json: serde_json::Value = serde_json::from_slice(&response_bytes).unwrap();
        assert_eq!(diff_json["status"], "ok");
        assert!(diff_json["added"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("login_count_1h")));
        assert!(diff_json["removed"].as_array().unwrap().is_empty());

        let engine = state.engine.read();
        assert_eq!(engine.stream_count(), 1);
        assert!(engine.get_stream("Logins").is_some());
    }

    #[test]
    fn test_register_invalid_json_returns_error() {
        let state = make_shared_state();
        // Missing required "name" field
        let cmd = Command::Register {
            payload: serde_json::json!({"features": []}),
        };
        let result = handle_sync_command(cmd, &state);
        assert!(result.is_err());
    }

    // --- MSET tests ---

    #[tokio::test]
    async fn test_mset_processes_entries() {
        let state = make_shared_state();
        let entries = vec![
            ("u123".to_string(), serde_json::json!({"score": 0.95})),
            ("u456".to_string(), serde_json::json!({"score": 0.5})),
        ];
        let result = handle_mset(entries, &state).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());

        assert_eq!(state.store.entity_count(), 2);
    }

    #[tokio::test]
    async fn test_mset_yields_between_chunks() {
        let state = make_shared_state();
        // Create > 1024 entries to ensure chunking happens
        let entries: Vec<(String, serde_json::Value)> = (0..2050)
            .map(|i| (format!("user_{}", i), serde_json::json!({"v": i})))
            .collect();
        let result = handle_mset(entries, &state).await;
        assert!(result.is_ok());

        assert_eq!(state.store.entity_count(), 2050);
    }

    // --- json_to_feature_value tests ---

    #[test]
    fn test_json_to_feature_value_int() {
        let v = json_to_feature_value(serde_json::json!(42));
        assert_eq!(v, FeatureValue::Int(42));
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_json_to_feature_value_float() {
        let v = json_to_feature_value(serde_json::json!(3.14));
        assert_eq!(v, FeatureValue::Float(3.14));
    }

    #[test]
    fn test_json_to_feature_value_string() {
        let v = json_to_feature_value(serde_json::json!("hello"));
        assert_eq!(v, FeatureValue::String("hello".into()));
    }

    #[test]
    fn test_json_to_feature_value_null() {
        let v = json_to_feature_value(serde_json::Value::Null);
        assert_eq!(v, FeatureValue::Missing);
    }

    #[test]
    fn test_json_to_feature_value_bool_true() {
        let v = json_to_feature_value(serde_json::json!(true));
        assert_eq!(v, FeatureValue::Int(1));
    }

    #[test]
    fn test_json_to_feature_value_bool_false() {
        let v = json_to_feature_value(serde_json::json!(false));
        assert_eq!(v, FeatureValue::Int(0));
    }

    #[test]
    fn test_json_to_feature_value_array_becomes_missing() {
        let v = json_to_feature_value(serde_json::json!([1, 2, 3]));
        assert_eq!(v, FeatureValue::Missing);
    }

    #[tokio::test]
    async fn test_mset_skips_non_object_entries() {
        let state = make_shared_state();
        let entries = vec![
            ("k1".to_string(), serde_json::json!({"score": 1})),
            ("k2".to_string(), serde_json::json!("not an object")), // string, not object
            ("k3".to_string(), serde_json::json!({"score": 3})),
        ];
        let result = handle_mset(entries, &state).await;
        assert!(result.is_ok());

        // k1 and k3 should be written (object payloads)
        assert!(
            state.store.get_entity("k1").is_some(),
            "k1 should be written"
        );
        assert!(
            state.store.get_entity("k3").is_some(),
            "k3 should be written"
        );
        // k2 should NOT be written (non-object payload was skipped)
        assert!(
            state.store.get_entity("k2").is_none(),
            "k2 should be skipped (non-object)"
        );
    }

    // --- panic recovery test ---
    // DashMap + parking_lot do not poison on panic (unlike std::sync::Mutex),
    // so this test verifies that the state is still usable after a panic.

    #[test]
    fn test_no_poisoning_after_panic() {
        let state = make_shared_state();
        // Attempt a panic inside an engine write lock scope.
        let state2 = state.clone();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _engine = state2.engine.write();
            panic!("intentional panic inside parking_lot lock");
        }));
        assert!(result.is_err()); // Panic was caught

        // State is still usable — no poisoning.
        let cmd = Command::Get { key: "test".into() };
        let result = handle_sync_command(cmd, &state);
        assert!(result.is_ok());
    }

    // --- Fan-out tests ---

    /// Helper: register MerchantActivity stream keyed by merchant_id.
    fn register_merchant_stream(state: &SharedState) {
        let stream = StreamDefinition {
            name: "MerchantActivity".into(),
            key_field: Some("merchant_id".into()),
            group_by_keys: None,
            features: vec![(
                "merchant_tx_count_1h".into(),
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
        };
        state.engine.write().register(stream).unwrap();
    }

    #[test]
    fn test_fan_out_push_updates_secondary_stream() {
        let state = make_shared_state();
        register_tx_stream(&state);
        register_merchant_stream(&state);

        // Push event with both user_id and merchant_id
        let cmd = Command::Push {
            stream_name: "Transactions".into(),
            payload: serde_json::json!({
                "user_id": "u123",
                "merchant_id": "m456",
                "amount": 50.0
            }),

            raw_payload: Vec::new(),
        };
        let result = handle_sync_command(cmd, &state);
        assert!(result.is_ok());

        // Verify merchant entity was created via fan-out
        let merchant_features = state
            .store
            .get_all_features("m456", std::time::SystemTime::now());
        assert_eq!(
            merchant_features.get("merchant_tx_count_1h"),
            Some(&FeatureValue::Int(1)),
            "fan-out should have pushed to MerchantActivity for m456"
        );
    }

    #[test]
    fn test_fan_out_push_ack_is_empty_but_state_updated() {
        // Phase 11 read-skip: sync push returns an empty ack. Fan-out still
        // runs (MerchantActivity is updated), but neither the primary nor the
        // fan-out target's features are materialized in the response.
        let state = make_shared_state();
        register_tx_stream(&state);
        register_merchant_stream(&state);

        let cmd = Command::Push {
            stream_name: "Transactions".into(),
            payload: serde_json::json!({
                "user_id": "u123",
                "merchant_id": "m456",
                "amount": 50.0
            }),

            raw_payload: Vec::new(),
        };
        let result = handle_sync_command(cmd, &state).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();

        // PUSH response is now an empty ack — neither primary nor fan-out features.
        assert_eq!(json, serde_json::json!({}));

        // But the fan-out still happened: MerchantActivity state was updated.
        let merchant_features = state
            .store
            .get_all_features("m456", std::time::SystemTime::now());
        assert_eq!(
            merchant_features.get("merchant_tx_count_1h"),
            Some(&FeatureValue::Int(1)),
            "fan-out should still push to MerchantActivity"
        );
    }

    #[test]
    fn test_fan_out_skips_primary_stream() {
        let state = make_shared_state();
        register_tx_stream(&state);
        register_merchant_stream(&state);

        // Push to Transactions -- should push once, not twice
        let cmd = Command::Push {
            stream_name: "Transactions".into(),
            payload: serde_json::json!({
                "user_id": "u123",
                "merchant_id": "m456",
                "amount": 50.0
            }),

            raw_payload: Vec::new(),
        };
        handle_sync_command(cmd, &state).unwrap();

        // user count should be 1 (pushed once, not twice)
        let user_features = state
            .store
            .get_all_features("u123", std::time::SystemTime::now());
        assert_eq!(
            user_features.get("tx_count_1h"),
            Some(&FeatureValue::Int(1))
        );
    }

    #[test]
    fn test_fan_out_skips_streams_without_key_in_event() {
        let state = make_shared_state();
        register_tx_stream(&state);
        register_merchant_stream(&state);

        // Push event WITHOUT merchant_id -- should NOT fan out to MerchantActivity
        let cmd = Command::Push {
            stream_name: "Transactions".into(),
            payload: serde_json::json!({
                "user_id": "u123",
                "amount": 50.0
            }),

            raw_payload: Vec::new(),
        };
        handle_sync_command(cmd, &state).unwrap();

        // Merchant entity should NOT exist
        assert!(
            state.store.get_entity("m456").is_none(),
            "no fan-out without key field"
        );
    }

    #[test]
    fn test_fan_out_skips_empty_key_value() {
        let state = make_shared_state();
        register_tx_stream(&state);
        register_merchant_stream(&state);

        // Push event with empty merchant_id
        let cmd = Command::Push {
            stream_name: "Transactions".into(),
            payload: serde_json::json!({
                "user_id": "u123",
                "merchant_id": "",
                "amount": 50.0
            }),

            raw_payload: Vec::new(),
        };
        handle_sync_command(cmd, &state).unwrap();

        // Should not create entity for empty key
        assert_eq!(
            state.store.entity_count(),
            1,
            "only u123 entity, not empty merchant"
        );
    }

    #[test]
    fn test_get_after_fan_out_returns_both_streams() {
        let state = make_shared_state();
        register_tx_stream(&state);
        register_merchant_stream(&state);

        // Push with fan-out
        let cmd = Command::Push {
            stream_name: "Transactions".into(),
            payload: serde_json::json!({
                "user_id": "u123",
                "merchant_id": "m456",
                "amount": 50.0
            }),

            raw_payload: Vec::new(),
        };
        handle_sync_command(cmd, &state).unwrap();

        // GET for user should return Transactions features
        let get_cmd = Command::Get { key: "u123".into() };
        let result = handle_sync_command(get_cmd, &state).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["tx_count_1h"], 1);

        // GET for merchant should return MerchantActivity features
        let get_cmd = Command::Get { key: "m456".into() };
        let result = handle_sync_command(get_cmd, &state).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["merchant_tx_count_1h"], 1);
    }

    // --- MGET command tests ---

    #[test]
    fn test_mget_returns_nested_json_for_known_and_unknown_keys() {
        let state = make_shared_state();
        register_tx_stream(&state);

        // Push event for u123
        let push_cmd = Command::Push {
            stream_name: "Transactions".into(),
            payload: serde_json::json!({"user_id": "u123", "amount": 50.0}),
            raw_payload: Vec::new(),
        };
        handle_sync_command(push_cmd, &state).unwrap();

        // MGET for known key u123 and unknown key u999
        let mget_cmd = Command::Mget {
            keys: vec!["u123".into(), "u999".into()],
        };
        let result = handle_sync_command(mget_cmd, &state).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();

        // u123 should have features
        assert_eq!(json["u123"]["tx_count_1h"], 1);
        assert_eq!(json["u123"]["tx_sum_1h"], 50.0);

        // u999 should have empty object
        assert_eq!(json["u999"], serde_json::json!({}));
    }

    #[test]
    fn test_mget_empty_keys_returns_empty_object() {
        let state = make_shared_state();
        let mget_cmd = Command::Mget { keys: vec![] };
        let result = handle_sync_command(mget_cmd, &state).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json, serde_json::json!({}));
    }

    #[test]
    fn test_mget_strips_qualified_feature_names() {
        let state = make_shared_state();
        register_tx_stream(&state);

        // Push event
        let push_cmd = Command::Push {
            stream_name: "Transactions".into(),
            payload: serde_json::json!({"user_id": "u123", "amount": 50.0}),
            raw_payload: Vec::new(),
        };
        handle_sync_command(push_cmd, &state).unwrap();

        // MGET should not contain "Transactions.tx_count_1h" etc.
        let mget_cmd = Command::Mget {
            keys: vec!["u123".into()],
        };
        let result = handle_sync_command(mget_cmd, &state).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();

        // Should have unqualified names
        assert!(json["u123"]["tx_count_1h"].is_number());
        // Should NOT have qualified names
        let obj = json["u123"].as_object().unwrap();
        for key in obj.keys() {
            assert!(
                !key.contains('.'),
                "MGET response should not contain qualified name: {}",
                key
            );
        }
    }

    #[test]
    fn test_end_to_end_register_push_get_with_views() {
        let state = make_shared_state();
        register_tx_stream(&state);
        register_merchant_stream(&state);

        // Register a view via REGISTER command
        let register_view_cmd = Command::Register {
            payload: serde_json::json!({
                "name": "UserRisk",
                "key_field": "user_id",
                "type": "view",
                "features": [{
                    "name": "tx_velocity",
                    "type": "derive",
                    "expr": "Transactions.tx_count_1h / 1"
                }]
            }),
        };
        handle_sync_command(register_view_cmd, &state).unwrap();

        // Push events
        for _ in 0..3 {
            let cmd = Command::Push {
                stream_name: "Transactions".into(),
                payload: serde_json::json!({
                    "user_id": "u1",
                    "merchant_id": "m1",
                    "amount": 10.0
                }),

                raw_payload: Vec::new(),
            };
            handle_sync_command(cmd, &state).unwrap();
        }

        // GET for user should include Transactions features + view features
        let get_cmd = Command::Get { key: "u1".into() };
        let result = handle_sync_command(get_cmd, &state).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["tx_count_1h"], 3, "should have 3 transactions");
        assert_eq!(json["tx_velocity"], 3.0, "view derive should be 3/1=3.0");

        // GET for merchant should include MerchantActivity features (from fan-out)
        let get_cmd = Command::Get { key: "m1".into() };
        let result = handle_sync_command(get_cmd, &state).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(
            json["merchant_tx_count_1h"], 3,
            "fan-out should have 3 merchant events"
        );
    }
}
