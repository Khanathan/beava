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

use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::engine::pipeline::PipelineEngine;
use crate::error::TallyError;
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
}

/// Status of a single backfill task.
#[derive(Debug)]
pub struct BackfillStatus {
    pub stream: String,
    pub features: Vec<String>,
    pub total_events: usize,
    pub processed_events: Arc<AtomicUsize>,
    pub started_at: SystemTime,
    pub completed_at: Mutex<Option<SystemTime>>,
}

/// Tracks all active and recently completed backfill tasks.
#[derive(Debug, Default)]
pub struct BackfillTracker {
    pub tasks: Mutex<Vec<Arc<BackfillStatus>>>,
}

/// Application state: engine + store + metrics.
pub struct AppState {
    pub engine: PipelineEngine,
    pub store: StateStore,
    pub metrics: Metrics,
    /// Snapshot file path. Single source of truth for both periodic and manual snapshot triggers.
    pub snapshot_path: std::path::PathBuf,
    /// Optional event log for persisting raw events to per-stream log files.
    /// None if event log initialization failed or is disabled.
    pub event_log: Option<EventLog>,
    /// Tracks active and completed backfill tasks for /debug/backfill endpoint.
    pub backfill_tracker: Arc<BackfillTracker>,
    /// Persistent set of (stream_name, feature_name) pairs that have completed backfill.
    /// Written to snapshot for crash recovery. On restart, features with backfill=true
    /// that are NOT in this set are re-run (idempotent restart per CONTEXT.md locked decision).
    pub backfill_complete: HashSet<(String, String)>,
    /// Phase 9: Current snapshot cycle number. Incremented after each successful
    /// snapshot write. When cycle % full_snapshot_interval == 0 the periodic
    /// timer writes a full base instead of a delta.
    pub snapshot_cycle: u64,
    /// Phase 9: Next sequence number for snapshot files. Derived from disk on
    /// startup (max existing sequence + 1).
    pub snapshot_seq: u64,
    /// Phase 9 WR-02: Sequence number of the most recently written base
    /// snapshot. Used to stamp delta headers with the correct `base_seq` so
    /// downstream tooling and recovery-time validation have a trustworthy
    /// pointer back to the base a delta was built against.
    pub last_base_seq: u64,
    /// Phase 9 WR-03: Sequence number of the base snapshot that was current
    /// BEFORE the most recent base write. Used by `cleanup_old_snapshots` to
    /// keep the previous base on disk as a fallback in case the new base
    /// turns out to be unreadable on startup.
    pub previous_base_seq: u64,
    /// Phase 10 DBUI-02: per-stream EWMA throughput tracker. Updated once per
    /// unique stream per successful PUSH (primary + cascade + fan-out with
    /// HashSet dedup -- see handle_sync_command Push arm). Read by the
    /// /debug/throughput handler in src/server/http.rs.
    pub throughput: crate::server::throughput::ThroughputTracker,
    /// Phase 10.2 DBUI-07: per-command and per-stream latency histograms.
    pub latency: crate::server::latency::LatencyTracker,
}

/// Shared state handle for concurrent connection handlers.
pub type SharedState = Arc<Mutex<AppState>>;

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
///     (a) it hits `BATCH_SIZE` events,
///     (b) its deadline elapses (200µs armed on first buffered event),
///     (c) any non-async opcode arrives (sync force-flush, pitfall H-2),
///     (d) the client disconnects.
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
        if let Command::PushAsync { stream_name, payload, raw_payload } = cmd {
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
                    Ok(Some((_len, Command::PushAsync { stream_name, payload, raw_payload }))) => {
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
                        let response: Result<Option<Vec<u8>>, TallyError> = match cmd2 {
                            Command::Mset { entries } => handle_mset(entries, &state).await.map(Some),
                            Command::Flush => Ok(Some(Vec::new())),
                            Command::PushAsync { .. } => unreachable!(),
                            Command::PushBatch { stream_name, batch_id, events } => {
                                // Accumulator already flushed above (line 331).
                                let base_seq = accumulator.advance_seq(events.len() as u64);
                                let now = SystemTime::now();
                                let batch: Vec<PendingAsync> = events.into_iter().enumerate().map(|(i, (payload, raw_payload))| {
                                    PendingAsync::new(base_seq + i as u64, stream_name.clone(), payload, raw_payload, now)
                                }).collect();
                                let results = handle_push_batch(&state, &batch);
                                for (i, (ev, res)) in batch.iter().zip(results.iter()).enumerate() {
                                    if let Err(err) = res {
                                        pending_drain.push((ev.seq, format!("[batch:{} event:{}] {}", batch_id, i, err)));
                                    }
                                }
                                // Fire-and-forget: no response frame.
                                break;
                            }
                            other => handle_sync_command(other, &state).map(Some),
                        };
                        if is_mset {
                            let mset_us = cmd_start.elapsed().as_secs_f64() * 1_000_000.0;
                            let mut app2 = state.lock().unwrap_or_else(|e| e.into_inner());
                            app2.latency.record_command(crate::server::latency::CommandKind::Mset, mset_us, std::time::Instant::now());
                            if app2.latency.slow_queries_would_accept(crate::server::latency::CommandKind::Mset, mset_us) {
                                app2.latency.maybe_record_slow(crate::server::latency::CommandKind::Mset, None, mset_us, String::new());
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
                                let resp = protocol::encode_response(STATUS_ERROR, e.to_string().as_bytes());
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
                        let resp = protocol::encode_response(STATUS_ERROR, e.to_string().as_bytes());
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

        // Sync dispatch path. cmd is one of Mset/Flush/other.
        let cmd_start = std::time::Instant::now();
        let is_mset = matches!(&cmd, Command::Mset { .. });
        let response: Result<Option<Vec<u8>>, TallyError> = match cmd {
            Command::Mset { entries } => handle_mset(entries, &state).await.map(Some),
            Command::Flush => Ok(Some(Vec::new())),
            Command::PushAsync { .. } => unreachable!("handled above"),
            Command::PushBatch { stream_name, batch_id, events } => {
                // Accumulator already force-flushed above (H-2).
                let base_seq = accumulator.advance_seq(events.len() as u64);
                let now = SystemTime::now();
                let batch: Vec<PendingAsync> = events.into_iter().enumerate().map(|(i, (payload, raw_payload))| {
                    PendingAsync::new(base_seq + i as u64, stream_name.clone(), payload, raw_payload, now)
                }).collect();
                let results = handle_push_batch(&state, &batch);
                for (i, (ev, res)) in batch.iter().zip(results.iter()).enumerate() {
                    if let Err(err) = res {
                        pending_drain.push((ev.seq, format!("[batch:{} event:{}] {}", batch_id, i, err)));
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
            let mut app = state.lock().unwrap_or_else(|e| e.into_inner());
            app.latency.record_command(crate::server::latency::CommandKind::Mset, mset_us, std::time::Instant::now());
            if app.latency.slow_queries_would_accept(crate::server::latency::CommandKind::Mset, mset_us) {
                app.latency.maybe_record_slow(crate::server::latency::CommandKind::Mset, None, mset_us, String::new());
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
) -> Result<crate::types::FeatureMap, TallyError> {
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

fn handle_push_core_ex(
    state: &SharedState,
    stream_name: &str,
    payload: &serde_json::Value,
    raw_payload: &[u8],
    now: SystemTime,
    read_features: bool,
) -> Result<crate::types::FeatureMap, TallyError> {
    let push_start = std::time::Instant::now();
    let mut app = state.lock().unwrap_or_else(|e| e.into_inner());
    let AppState {
        ref engine,
        ref mut store,
        ref mut event_log,
        ..
    } = *app;

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

    // Append event to primary stream's event log (ELOG-01, ELOG-02, ELOG-03).
    // Plan 11-06: writes raw binary wire bytes directly — no JSON serialize
    // on the hot path. Event log readers dispatch on the format byte.
    // Build once, reuse for primary + cascade + fan-out writes.
    let log_payload = if event_log.is_some() {
        make_log_payload(payload, raw_payload)
    } else {
        Vec::new()
    };
    if let Some(ref mut log) = event_log {
        let _ = log.append(stream_name, &log_payload, now);
    }

    // Append event to downstream (cascade) streams' event logs (T-07-10)
    let cascade_targets = engine.get_cascade_targets(stream_name);
    if let Some(ref mut log) = event_log {
        for ds_name in &cascade_targets {
            let should_log = match engine.get_stream(ds_name) {
                Some(d) => match &d.key_field {
                    Some(kf) => matches!(payload.get(kf.as_str()), Some(serde_json::Value::String(s)) if !s.is_empty()),
                    None => true,
                },
                None => false,
            };
            if should_log {
                let _ = log.append(ds_name, &log_payload, now);
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

    // Fan-out: push to other streams whose key_field exists in the event.
    let primary_key_field = engine.get_stream(stream_name).and_then(|s| s.key_field.as_deref());
    let targets = engine.fan_out_targets();
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
                // Without this, fan-out calls engine.push (defaults to
                // read_features=true) and still pays the HLL read cost on
                // downstream streams — measured ~50x regression on `bench.py
                // large` where 3 HLLs are spread across 3 streams via fan-out.
                let _ = if read_features {
                    engine.push(target_name, payload, store, now)
                } else {
                    engine.push_no_features(target_name, payload, store, now)
                };
                store.mark_dirty(key_val);
                if let Some(ref mut log) = event_log {
                    // Reuse the single log_payload built earlier (same bytes
                    // for every target — the format prefix + payload is
                    // stream-agnostic).
                    let _ = log.append(target_name, &log_payload, now);
                }
            }
        }
    }

    // DBUI-02: throughput bump across unique touched streams.
    {
        let now_inst = std::time::Instant::now();
        let primary_key_field_for_tp = app
            .engine
            .get_stream(stream_name)
            .and_then(|s| s.key_field.clone());
        let tp_targets_all = app.engine.fan_out_targets();
        let mut touched: Vec<&str> =
            Vec::with_capacity(1 + cascade_targets.len() + tp_targets_all.len());
        touched.push(stream_name);
        for ds in &cascade_targets {
            touched.push(ds.as_str());
        }
        for (target_name, target_key_field) in &tp_targets_all {
            if target_name == stream_name {
                continue;
            }
            if primary_key_field_for_tp.as_deref() == Some(target_key_field.as_str()) {
                continue;
            }
            if cascade_targets.iter().any(|ct| ct == target_name) {
                continue;
            }
            let key_present = matches!(
                payload.get(target_key_field.as_str()),
                Some(serde_json::Value::String(s)) if !s.is_empty()
            );
            if !key_present {
                continue;
            }
            touched.push(target_name.as_str());
        }
        app.throughput.bump_unique(touched.into_iter(), now_inst);
    }

    let push_elapsed = push_start.elapsed();
    app.metrics.push_latency_seconds = push_elapsed.as_secs_f64();
    app.metrics.events_total += 1;

    // Phase 10.2: record latency into histogram tracker
    let push_us = push_elapsed.as_secs_f64() * 1_000_000.0;
    app.latency.record_push(stream_name, push_us, std::time::Instant::now());
    if app.latency.slow_queries_would_accept(crate::server::latency::CommandKind::Push, push_us) {
        let key_preview = app.engine.get_stream(stream_name)
            .and_then(|s| s.key_field.clone())
            .and_then(|kf| payload.get(&kf).and_then(|v| v.as_str()).map(|s| {
                let mut kp = s.to_string();
                kp.truncate(32);
                kp
            }))
            .unwrap_or_default();
        app.latency.maybe_record_slow(
            crate::server::latency::CommandKind::Push,
            Some(stream_name),
            push_us,
            key_preview,
        );
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
        Self { seq, stream_name, payload, raw_payload, now }
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
                tokio::time::Instant::now()
                    + std::time::Duration::from_micros(BATCH_DEADLINE_US),
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
) -> Vec<Result<(), TallyError>> {
    if batch.is_empty() {
        return Vec::new();
    }

    // Fast path: check if all events target the same stream (common case
    // under single-client sustained load). Avoids the grouping Vec entirely.
    let all_same_stream = batch.len() == 1
        || batch[1..].iter().all(|ev| ev.stream_name == batch[0].stream_name);

    // Result slots in input order, pre-filled with Ok. Per-event errors
    // from the cascade-aware batch primitive are scattered back to their
    // input positions below.
    let mut results: Vec<Result<(), TallyError>> =
        (0..batch.len()).map(|_| Ok(())).collect();

    // Single lock acquisition for the whole batch (D-06). The guard lives
    // only for the synchronous body below — clippy::await_holding_lock
    // enforces that at compile time.
    let mut app = state.lock().unwrap_or_else(|e| e.into_inner());
    let AppState {
        ref engine,
        ref mut store,
        ref mut event_log,
        ..
    } = *app;

    if all_same_stream {
        // Single-stream fast path: no grouping needed, no index indirection.
        let stream_name = batch[0].stream_name.as_str();
        let key_field: Option<&str> = engine
            .get_stream(stream_name)
            .and_then(|s| s.key_field.as_deref());

        // Build event refs inline (stack-friendly for typical batch sizes).
        let events_refs: Vec<&serde_json::Value> =
            batch.iter().map(|ev| &ev.payload).collect();

        let now = batch[0].now;

        let per_event = engine.push_batch_with_cascade_no_features(
            stream_name,
            &events_refs,
            store,
            now,
        );

        // Scatter errors to result slots.
        for (idx, res) in per_event.into_iter().enumerate() {
            if let Err(e) = res {
                results[idx] = Err(e);
            }
        }

        // Per-event event log append — avoids intermediate Vec<Vec<u8>>
        // allocation. Each call reuses the writer lookup from EventLog's
        // internal HashMap (one HashMap probe per call, amortized by branch
        // prediction on the repeated stream name).
        if let Some(ref mut log) = event_log {
            for (idx, ev) in batch.iter().enumerate() {
                if results[idx].is_ok() {
                    let lp = make_log_payload(&ev.payload, &ev.raw_payload);
                    let _ = log.append(stream_name, &lp, now);
                }
            }
        }

        // Per-event dirty marking — avoids intermediate Vec<String>.
        if let Some(kf) = key_field {
            for (idx, ev) in batch.iter().enumerate() {
                if results[idx].is_ok() {
                    if let Some(serde_json::Value::String(key_val)) = ev.payload.get(kf) {
                        if !key_val.is_empty() {
                            store.mark_dirty(key_val);
                        }
                    }
                }
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

            let events_refs: Vec<&serde_json::Value> =
                indices.iter().map(|&i| &batch[i].payload).collect();

            let now = indices
                .iter()
                .map(|&i| batch[i].now)
                .min()
                .unwrap_or_else(SystemTime::now);

            let per_event = engine.push_batch_with_cascade_no_features(
                stream_name,
                &events_refs,
                store,
                now,
            );

            for (slot_idx, res) in indices.iter().zip(per_event.into_iter()) {
                if let Err(e) = res {
                    results[*slot_idx] = Err(e);
                }
            }

            // Per-event log append (avoids Vec<Vec<u8>> allocation).
            if let Some(ref mut log) = event_log {
                for &i in indices {
                    if results[i].is_ok() {
                        let lp = make_log_payload(&batch[i].payload, &batch[i].raw_payload);
                        let _ = log.append(stream_name, &lp, now);
                    }
                }
            }

            // Per-event dirty marking (avoids Vec<String> allocation).
            if let Some(kf) = key_field {
                for &i in indices {
                    if results[i].is_ok() {
                        if let Some(serde_json::Value::String(key_val)) = batch[i].payload.get(kf) {
                            if !key_val.is_empty() {
                                store.mark_dirty(key_val);
                            }
                        }
                    }
                }
            }
        }
    }

    // Metrics bump once per batch — matches the per-event increment the
    // legacy handle_push_core path performs, but amortized.
    app.metrics.events_total += batch.len() as u64;

    results
}

// Phase 12 Task 2: `handle_push_async` removed. All async pushes now flow
// through the per-connection `ConnAccumulator` → `handle_push_batch` path in
// `handle_connection`. The batch path is the only async push path.

/// Handle synchronous commands: lock, process, unlock. No .await while locked.
fn handle_sync_command(cmd: Command, state: &SharedState) -> Result<Vec<u8>, TallyError> {
    let now = SystemTime::now();
    match cmd {
        Command::Push {
            stream_name,
            payload,
            raw_payload,
        } => {
            // PERF: sync push path also skips feature read + derive eval. The
            // response is a synchronous ack ({}) confirming the event was
            // processed — callers that need features should use OP_GET after.
            // This removes the HLL read cost from the sync hot path too, at
            // the cost of breaking the v1.1 push_sync "returns features"
            // contract. Clients relying on that contract must migrate to
            // push() + get() or accept an empty map.
            // Plan 11-06: raw_payload goes to the event log directly.
            let _features = handle_push_core_ex(state, &stream_name, &payload, &raw_payload, now, false)?;
            Ok(feature_map_to_json(&crate::types::FeatureMap::new()))
        }
        Command::Get { key } => {
            let get_start = std::time::Instant::now();
            let mut app = state.lock().unwrap_or_else(|e| e.into_inner());
            let AppState {
                ref engine,
                ref mut store,
                ..
            } = *app;
            let features = engine.get_features(&key, store, now);
            let result = feature_map_to_json(&features);
            // Phase 10.2: record GET latency
            let get_us = get_start.elapsed().as_secs_f64() * 1_000_000.0;
            app.latency.record_command(crate::server::latency::CommandKind::Get, get_us, std::time::Instant::now());
            if app.latency.slow_queries_would_accept(crate::server::latency::CommandKind::Get, get_us) {
                let mut kp = key.clone();
                kp.truncate(32);
                app.latency.maybe_record_slow(crate::server::latency::CommandKind::Get, None, get_us, kp);
            }
            Ok(result)
        }
        Command::Set { key, payload } => {
            let set_start = std::time::Instant::now();
            let mut app = state.lock().unwrap_or_else(|e| e.into_inner());
            // payload is a JSON object; iterate its key-value pairs
            if let serde_json::Value::Object(map) = payload {
                for (feat_name, val) in map {
                    let fv = json_to_feature_value(val);
                    app.store.set_static(&key, &feat_name, fv, now);
                }
                // Mark entity key dirty for incremental snapshots (OPS-03)
                app.store.mark_dirty(&key);
            } else {
                return Err(TallyError::Protocol(
                    "SET payload must be a JSON object".into(),
                ));
            }
            // Phase 10.2: record SET latency
            let set_us = set_start.elapsed().as_secs_f64() * 1_000_000.0;
            app.latency.record_command(crate::server::latency::CommandKind::Set, set_us, std::time::Instant::now());
            if app.latency.slow_queries_would_accept(crate::server::latency::CommandKind::Set, set_us) {
                let mut kp = key.clone();
                kp.truncate(32);
                app.latency.maybe_record_slow(crate::server::latency::CommandKind::Set, None, set_us, kp);
            }
            Ok(vec![])
        }
        Command::Register { payload } => {
            let raw_json = payload.clone();
            let req: protocol::RegisterRequest = serde_json::from_value(payload)
                .map_err(|e| TallyError::Protocol(format!("invalid register payload: {}", e)))?;
            let def_name = req.name.clone();
            let is_view = req.definition_type.as_deref() == Some("view");
            let mut app = state.lock().unwrap_or_else(|e| e.into_inner());
            if is_view {
                let view_def = protocol::convert_view_register_request(req)?;
                app.engine.register_view(view_def)?;
                app.engine.store_raw_register_json(&def_name, raw_json);
                Ok(vec![])
            } else {
                let stream_def = protocol::convert_register_request(req)?;
                let diff = app.engine.register(stream_def)?;
                // Register stream with event log for persistence
                let history_ttl = app.engine.get_stream(&def_name)
                    .and_then(|s| s.history_ttl);
                if let Some(ref mut log) = app.event_log {
                    let _ = log.register_stream(&def_name, history_ttl);
                }
                app.engine.store_raw_register_json(&def_name, raw_json);

                // If there are features to backfill, spawn async task (SCHM-03)
                if !diff.backfilling.is_empty() {
                    // Flush event log to ensure all events are readable
                    if let Some(ref mut log) = app.event_log {
                        let _ = log.fsync_all();
                    }
                    // Read event log entries for this stream
                    let entries = app.event_log.as_ref()
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
                            completed_at: Mutex::new(None),
                        });
                        app.backfill_tracker.tasks.lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .push(Arc::clone(&status));

                        let state_clone = state.clone();
                        tokio::spawn(run_backfill(
                            state_clone,
                            def_name.clone(),
                            backfill_features,
                            entries,
                            status,
                        ));
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
        }
        Command::Mget { keys } => {
            let mut app = state.lock().unwrap_or_else(|e| e.into_inner());
            let AppState {
                ref engine,
                ref mut store,
                ..
            } = *app;
            let mut result = serde_json::Map::new();
            for key in &keys {
                let features = engine.get_features(key, store, now);
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
    }
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
        let mut app = state.lock().unwrap_or_else(|e| e.into_inner());
        let keys: Vec<String> = app.store.entity_keys().collect();
        for key in &keys {
            if let Some(entity) = app.store.get_entity_mut(key) {
                if let Some(stream_state) = entity.streams.get_mut(&stream_name) {
                    stream_state.operators.retain(|(name, _)| !feature_names.contains(name));
                }
            }
        }
    }

    let total = entries.len();
    for (chunk_idx, chunk) in entries.chunks(64).enumerate() {
        {
            let mut app = state.lock().unwrap_or_else(|e| e.into_inner());
            let AppState {
                ref engine,
                ref mut store,
                ..
            } = *app;
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
                let _ = engine.push_for_backfill(
                    &stream_name,
                    &event,
                    store,
                    entry.timestamp, // Event timestamp for determinism (SCHM-05)
                    &feature_names,
                );
                // Mark entity key dirty for incremental snapshots (OPS-03)
                if let Some(stream_def) = engine.get_stream(&stream_name) {
                    if let Some(ref kf) = stream_def.key_field {
                        if let Some(serde_json::Value::String(key_val)) = event.get(kf.as_str()) {
                            if !key_val.is_empty() {
                                store.mark_dirty(key_val);
                            }
                        }
                    }
                }
            }
        } // Lock released before yield
        // Update progress
        let processed = std::cmp::min((chunk_idx + 1) * 64, total);
        status.processed_events.store(processed, Ordering::Relaxed);
        tokio::task::yield_now().await; // Cooperative yield (SCHM-04)
    }
    // Mark complete in tracker
    *status.completed_at.lock().unwrap_or_else(|e| e.into_inner()) = Some(SystemTime::now());
    // Persist completion markers so restart detects this backfill as done
    {
        let mut app = state.lock().unwrap_or_else(|e| e.into_inner());
        for feat in &feature_names {
            app.backfill_complete.insert((stream_name.clone(), feat.clone()));
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

/// Handle MSET with cooperative yielding: process 1024-key chunks, yield between.
async fn handle_mset(
    entries: Vec<(String, serde_json::Value)>,
    state: &SharedState,
) -> Result<Vec<u8>, TallyError> {
    let now = SystemTime::now();
    for chunk in entries.chunks(1024) {
        {
            let mut app = state.lock().unwrap_or_else(|e| e.into_inner());
            for (key, payload) in chunk {
                if let serde_json::Value::Object(map) = payload {
                    for (feat_name, val) in map {
                        let fv = json_to_feature_value(val.clone());
                        app.store.set_static(key, feat_name, fv, now);
                    }
                    // Mark entity key dirty once per chunk iteration (OPS-03)
                    app.store.mark_dirty(key);
                }
                // Skip non-object payloads silently (defensive)
            }
        } // Lock released before yield
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
    fn make_shared_state() -> SharedState {
        Arc::new(Mutex::new(AppState {
            engine: PipelineEngine::new(),
            store: StateStore::new(),
            metrics: Metrics::default(),
            snapshot_path: std::path::PathBuf::from("test.snapshot"),
            event_log: None,
            backfill_tracker: Arc::new(BackfillTracker::default()),
            backfill_complete: HashSet::new(),
            snapshot_cycle: 0,
            snapshot_seq: 1,
            last_base_seq: 0,
            previous_base_seq: 0,
            throughput: crate::server::throughput::ThroughputTracker::new(),
            latency: crate::server::latency::LatencyTracker::new(),
        }))
    }

    /// Helper: register a simple stream with count and sum features.
    fn register_tx_stream(state: &SharedState) {
        let stream = StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
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
        };
        let mut app = state.lock().unwrap();
        app.engine.register(stream).unwrap();
    }

    // --- AppState and SharedState type tests ---

    #[test]
    fn test_app_state_wraps_engine_and_store() {
        let state = make_shared_state();
        let app = state.lock().unwrap();
        assert_eq!(app.engine.stream_count(), 0);
        assert_eq!(app.store.entity_count(), 0);
    }

    #[test]
    fn test_shared_state_is_arc_mutex() {
        // Verify SharedState is Arc<Mutex<AppState>> by cloning
        let state: SharedState = make_shared_state();
        let state2 = state.clone();
        drop(state2); // Would fail if not Arc
        let _app = state.lock().unwrap(); // Would fail if not Mutex
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
        let get_cmd = Command::Get {
            key: "u123".into(),
        };
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
        let app = state.lock().unwrap();
        let entity = app.store.get_entity("u123").unwrap();
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
        assert!(diff_json["added"].as_array().unwrap().contains(&serde_json::json!("login_count_1h")));
        assert!(diff_json["removed"].as_array().unwrap().is_empty());

        let app = state.lock().unwrap();
        assert_eq!(app.engine.stream_count(), 1);
        assert!(app.engine.get_stream("Logins").is_some());
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
            (
                "u123".to_string(),
                serde_json::json!({"score": 0.95}),
            ),
            (
                "u456".to_string(),
                serde_json::json!({"score": 0.5}),
            ),
        ];
        let result = handle_mset(entries, &state).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());

        let app = state.lock().unwrap();
        assert_eq!(app.store.entity_count(), 2);
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

        let app = state.lock().unwrap();
        assert_eq!(app.store.entity_count(), 2050);
    }

    // --- json_to_feature_value tests ---

    #[test]
    fn test_json_to_feature_value_int() {
        let v = json_to_feature_value(serde_json::json!(42));
        assert_eq!(v, FeatureValue::Int(42));
    }

    #[test]
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

        let app = state.lock().unwrap();
        // k1 and k3 should be written (object payloads)
        assert!(app.store.get_entity("k1").is_some(), "k1 should be written");
        assert!(app.store.get_entity("k3").is_some(), "k3 should be written");
        // k2 should NOT be written (non-object payload was skipped)
        assert!(
            app.store.get_entity("k2").is_none(),
            "k2 should be skipped (non-object)"
        );
    }

    // --- Mutex poisoning recovery test ---

    #[test]
    fn test_poisoned_mutex_recovery() {
        let state = make_shared_state();
        // Poison the mutex by panicking inside a lock
        let state2 = state.clone();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _app = state2.lock().unwrap();
            panic!("intentional panic to poison mutex");
        }));
        assert!(result.is_err()); // Panic was caught

        // Should still be able to use the state via unwrap_or_else recovery
        let cmd = Command::Get {
            key: "test".into(),
        };
        let result = handle_sync_command(cmd, &state);
        assert!(result.is_ok());
    }

    // --- Fan-out tests ---

    /// Helper: register MerchantActivity stream keyed by merchant_id.
    fn register_merchant_stream(state: &SharedState) {
        let stream = StreamDefinition {
            name: "MerchantActivity".into(),
            key_field: Some("merchant_id".into()),
            features: vec![
                (
                    "merchant_tx_count_1h".into(),
                    FeatureDef::Count {
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                        where_expr: None,
                        backfill: false,
                    },
                ),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
        };
        let mut app = state.lock().unwrap();
        app.engine.register(stream).unwrap();
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
        let mut app = state.lock().unwrap();
        let merchant_features = app.store.get_all_features("m456", std::time::SystemTime::now());
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
        let mut app = state.lock().unwrap();
        let merchant_features = app.store.get_all_features("m456", std::time::SystemTime::now());
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
        let mut app = state.lock().unwrap();
        let user_features = app.store.get_all_features("u123", std::time::SystemTime::now());
        assert_eq!(user_features.get("tx_count_1h"), Some(&FeatureValue::Int(1)));
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
        let app = state.lock().unwrap();
        assert!(app.store.get_entity("m456").is_none(), "no fan-out without key field");
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
        let app = state.lock().unwrap();
        assert_eq!(app.store.entity_count(), 1, "only u123 entity, not empty merchant");
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
            assert!(!key.contains('.'), "MGET response should not contain qualified name: {}", key);
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
        assert_eq!(json["merchant_tx_count_1h"], 3, "fan-out should have 3 merchant events");
    }
}
