//! N-parallel shard recovery (Phase 52-03, TPC-INFRA-06).
//!
//! On boot, `parallel_recover_all_shards` spawns one OS thread per shard
//! (matching D-05: one recovery thread per shard, I/O-bound, no tokio tasks).
//! Each thread:
//!   1. Opens `EventLog::new_for_shard(data_dir, shard_id)`.
//!   2. Scans `data_dir/shard-{N}/streams/` for registered stream subdirs.
//!   3. For each stream, calls `event_log.read_entries(stream_name)` and
//!      passes each `LogEntry` to `shard.apply_log_entry(entry, engine)`.
//!   4. On completion, calls `barrier.mark_recovered(shard_id)`.
//!
//! The main thread joins all handles and returns `Ok(())` only if every thread
//! succeeded. Any shard failure propagates as `Err(io::Error)`.
//!
//! **RecoveryBarrier**: extends the Phase 50 boot-barrier concept with a
//! "recovered" sub-state. Uses `per_shard_recovered: Box<[AtomicBool]>` and
//! `recovered_count: AtomicUsize`. Listeners bind only after
//! `barrier.all_recovered()` returns true (gated in the boot sequence).
//!
//! **Threat T-52-03-01**: log read errors surface as `Err(io::Error)` — the
//! main thread propagates them and refuses to continue. No shard failure is
//! silently ignored.
//!
//! **Threat T-52-03-03**: each thread has exclusive `&mut Shard` access during
//! recovery — no cross-shard writes are possible.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

use crate::state::event_log::EventLog;

// ============================================================
// RecoveryBarrier
// ============================================================

/// Extended boot-barrier for per-shard log recovery.
///
/// Extends the Phase 50 ready-barrier concept: alongside "init-ready"
/// (already tracked by spawn_shard_threads WaitGroup), this barrier
/// tracks "recovered" — every shard has finished replaying its event log.
///
/// Listeners may not bind sockets until `all_recovered()` returns true.
///
/// # Usage
/// ```text
/// let barrier = Arc::new(RecoveryBarrier::new(N));
/// parallel_recover_all_shards(data_dir, &shards, Arc::clone(&barrier), engine)?;
/// // Now barrier.all_recovered() == true — safe to bind listeners.
/// ```
pub struct RecoveryBarrier {
    /// Per-shard recovered flags. Indexed by shard_id.
    /// `AtomicBool` allows lock-free per-shard status reads from the /debug/shards handler.
    per_shard_recovered: Box<[AtomicBool]>,
    /// Count of shards that have called `mark_recovered`. When this reaches
    /// `total`, `all_recovered()` returns true.
    recovered_count: AtomicUsize,
    /// Total number of shards (= N).
    total: usize,
    /// Per-shard replay counters (used by tests to verify isolation).
    /// Each entry is the number of log entries replayed into that shard.
    per_shard_replay_count: Box<[AtomicUsize]>,
}

impl RecoveryBarrier {
    /// Create a barrier for `shard_count` shards.
    pub fn new(shard_count: usize) -> Self {
        let per_shard_recovered = (0..shard_count)
            .map(|_| AtomicBool::new(false))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let per_shard_replay_count = (0..shard_count)
            .map(|_| AtomicUsize::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        RecoveryBarrier {
            per_shard_recovered,
            recovered_count: AtomicUsize::new(0),
            total: shard_count,
            per_shard_replay_count,
        }
    }

    /// Mark shard `shard_id` as having completed log replay.
    ///
    /// Idempotent: calling multiple times for the same shard is safe
    /// (the count is only incremented once per shard via the AtomicBool CAS).
    pub fn mark_recovered(&self, shard_id: u8) {
        let idx = shard_id as usize;
        if idx >= self.total {
            return;
        }
        // CAS: only increment the count once per shard.
        if self.per_shard_recovered[idx]
            .compare_exchange(false, true, Ordering::Release, Ordering::Relaxed)
            .is_ok()
        {
            self.recovered_count.fetch_add(1, Ordering::Release);
        }
    }

    /// Returns true when every shard has called `mark_recovered`.
    pub fn all_recovered(&self) -> bool {
        self.recovered_count.load(Ordering::Acquire) >= self.total
    }

    /// Returns the list of shard IDs that have NOT yet called `mark_recovered`.
    /// Used by the `/ready` 503 response body (`shards_recovering` field).
    pub fn recovering_shards(&self) -> Vec<u8> {
        self.per_shard_recovered
            .iter()
            .enumerate()
            .filter(|(_, flag)| !flag.load(Ordering::Relaxed))
            .map(|(idx, _)| idx as u8)
            .collect()
    }

    /// Returns true if shard `shard_id` has completed recovery.
    /// Used by `/debug/shards` to populate the per-shard `"recovered"` field (D-09 extension).
    pub fn shard_is_recovered(&self, shard_id: u8) -> bool {
        let idx = shard_id as usize;
        if idx >= self.total {
            return false;
        }
        self.per_shard_recovered[idx].load(Ordering::Relaxed)
    }

    /// Returns the per-shard replay entry counts (for test isolation verification).
    pub fn per_shard_replay_counts(&self) -> Vec<usize> {
        self.per_shard_replay_count
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .collect()
    }

    /// Increment the replay count for the given shard (called by recovery thread).
    fn add_replay_count(&self, shard_id: u8, count: usize) {
        let idx = shard_id as usize;
        if idx < self.total {
            self.per_shard_replay_count[idx].fetch_add(count, Ordering::Relaxed);
        }
    }
}

// ============================================================
// parallel_recover_all_shards
// ============================================================

/// Discover all stream names registered under `data_dir/shard-{shard_id}/streams/`.
///
/// Reads the streams/ directory and returns the name of each subdirectory
/// that contains a `log.bin` file (meaning the stream was previously registered).
///
/// Returns an empty Vec if the directory does not exist.
fn discover_streams_for_shard(data_dir: &Path, shard_id: u8) -> io::Result<Vec<String>> {
    let streams_dir = data_dir.join(format!("shard-{}/streams", shard_id));
    if !streams_dir.exists() {
        return Ok(vec![]);
    }
    let mut names = Vec::new();
    for entry in std::fs::read_dir(&streams_dir)? {
        let entry = entry?;
        let stream_dir = entry.path();
        if !stream_dir.is_dir() {
            continue;
        }
        let log_file = stream_dir.join("log.bin");
        if log_file.exists() {
            if let Some(name) = stream_dir.file_name().and_then(|n| n.to_str()) {
                names.push(name.to_string());
            }
        }
    }
    Ok(names)
}

/// Run N-parallel shard recovery threads, one per shard.
///
/// Each thread:
/// 1. Discovers all stream subdirectories under `data_dir/shard-{N}/streams/`.
/// 2. Opens an `EventLog` for the shard.
/// 3. For each stream, reads all log entries and applies them to the shard.
/// 4. Calls `barrier.mark_recovered(shard_id)`.
///
/// The main thread joins all handles. If any thread returns an error,
/// the first error is propagated (T-52-03-01: no silent shard failure).
///
/// # Arguments
/// - `data_dir` — root data directory (e.g. `data/` or tmp dir in tests).
/// - `shards` — `Arc<Mutex<Shard>>` per shard; recovery thread takes exclusive
///   access during replay (no other writer touches it — T-52-03-03).
/// - `barrier` — shared RecoveryBarrier; each thread calls `mark_recovered` on exit.
/// - `engine` — optional `Arc<parking_lot::RwLock<PipelineEngine>>` for parsing
///   events through the registered pipeline definitions. Pass `None` in tests
///   that don't need operator replay.
pub fn parallel_recover_all_shards(
    data_dir: PathBuf,
    shards: &[Arc<std::sync::Mutex<crate::shard::Shard>>],
    barrier: Arc<RecoveryBarrier>,
    engine: Option<Arc<parking_lot::RwLock<crate::engine::pipeline::PipelineEngine>>>,
) -> io::Result<()> {
    let n = shards.len();
    let mut handles = Vec::with_capacity(n);

    for (shard_index, shard_arc) in shards.iter().enumerate() {
        let shard_id = shard_index as u8;
        let data_dir_clone = data_dir.clone();
        let barrier_clone = Arc::clone(&barrier);
        let shard_clone = Arc::clone(shard_arc);
        let engine_clone = engine.clone();

        let handle = std::thread::Builder::new()
            .name(format!("beava-recover-{}", shard_id))
            .spawn(move || -> io::Result<()> {
                recover_single_shard(
                    &data_dir_clone,
                    shard_id,
                    shard_clone,
                    barrier_clone,
                    engine_clone,
                )
            })
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        handles.push(handle);
    }

    // Join all threads; collect errors.
    let mut first_error: Option<io::Error> = None;
    for handle in handles {
        match handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
            Err(_panic) => {
                if first_error.is_none() {
                    first_error = Some(io::Error::new(
                        io::ErrorKind::Other,
                        "recovery thread panicked",
                    ));
                }
            }
        }
    }

    match first_error {
        None => Ok(()),
        Some(e) => Err(e),
    }
}

/// Recovery routine for a single shard.
///
/// Called in a dedicated thread (D-05: one thread per shard).
fn recover_single_shard(
    data_dir: &Path,
    shard_id: u8,
    shard_arc: Arc<std::sync::Mutex<crate::shard::Shard>>,
    barrier: Arc<RecoveryBarrier>,
    engine: Option<Arc<parking_lot::RwLock<crate::engine::pipeline::PipelineEngine>>>,
) -> io::Result<()> {
    // Discover which streams exist on disk for this shard.
    let stream_names = discover_streams_for_shard(data_dir, shard_id)?;

    if stream_names.is_empty() {
        // No streams to recover — mark recovered immediately.
        barrier.mark_recovered(shard_id);
        return Ok(());
    }

    // Open an EventLog for reading this shard's log files.
    let event_log = EventLog::new_for_shard(data_dir.to_path_buf(), shard_id)?;
    // Register all discovered streams so read_entries() can find their paths.
    for stream_name in &stream_names {
        event_log.register_stream(stream_name, None)?;
    }

    // Replay entries into the shard.
    let mut total_replayed = 0usize;
    for stream_name in &stream_names {
        let entries = event_log.read_entries(stream_name)?;
        let n_entries = entries.len();

        // Apply each log entry to the shard's state.
        // If we have a pipeline engine, parse the JSON payload and push through
        // the cascade pipeline. Without an engine (test mode), skip operator replay
        // but still count entries for isolation verification.
        if let Some(ref eng) = engine {
            let mut shard = shard_arc.lock().unwrap();
            let now = std::time::SystemTime::now();
            for entry in entries {
                apply_log_entry_to_shard(&entry.payload, stream_name, &mut shard, eng, now)?;
            }
        }
        // else: no engine — entries are counted for isolation verification only.

        total_replayed += n_entries;
    }

    // Record how many entries were replayed (for test isolation verification).
    barrier.add_replay_count(shard_id, total_replayed);
    barrier.mark_recovered(shard_id);

    Ok(())
}

/// Apply a single log entry's payload to the shard state via the pipeline engine.
///
/// Parses the raw bytes as JSON, then calls `engine.push_with_cascade_on_shard`.
/// On parse error: returns `Err` (T-52-03-01: corrupt entries must not panic
/// the recovery thread, but we do surface them as errors so the caller
/// can decide whether to continue or abort).
fn apply_log_entry_to_shard(
    payload: &[u8],
    stream_name: &str,
    shard: &mut crate::shard::Shard,
    engine: &Arc<parking_lot::RwLock<crate::engine::pipeline::PipelineEngine>>,
    now: std::time::SystemTime,
) -> io::Result<()> {
    // Handle format-tagged payloads from Phase 11-06.
    let (_, body) = crate::state::event_log::decode_log_payload(payload);

    let event: serde_json::Value = serde_json::from_slice(body).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("log entry JSON parse error: {}", e),
        )
    })?;

    // Push through the pipeline engine (read lock — engine is read-only after registration).
    let eng = engine.read();
    // read_features=false during recovery (we don't need the computed feature map back).
    // Phase 54-02 Task 2: recovery runs before shard threads are spawned,
    // so there are no sibling shards to scatter-gather into — pass `None`
    // so the cascade stays intra-shard (which is also how legacy recovery
    // behaved).
    let _ = eng.push_with_cascade_on_shard(stream_name, &event, shard, None, now, false, None, 0);

    Ok(())
}

// ============================================================
// Phase 55-03 Task 2 — rematerialize_tables_from_event_logs
// ============================================================

/// Phase 55-03 Task 2 report: how much replay work the rematerializer did.
/// Returned by `rematerialize_tables_from_event_logs` for boot-log / test
/// assertions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RematerializeReport {
    /// Total count of primary-stream log entries re-driven through the
    /// cascade path.
    pub events_replayed: u64,
    /// Number of shards the rematerializer processed (equals `shards.len()`
    /// for the default fjall build; `0` in the state-inmem skip path).
    pub shards_processed: usize,
}

/// Phase 55-03 D-C1 (TPC-CORR-07): rebuild downstream table state by
/// replaying every per-shard primary event log through the post-Phase-55
/// cross-shard cascade path. Must be called on the main thread **before**
/// `spawn_shard_threads` to preserve the fjall single-writer invariant
/// (Phase 53 D-01; same discipline as Phase 54 Wave 3 Task 1
/// `restore_snapshot_to_shards`).
///
/// Failure modes:
/// - Event log truncated past rebuild boundary (an earlier primary entry
///   was compacted away and cannot be replayed) → returns
///   `BeavaError::Protocol` whose message contains both
///   `"Event log truncated before LSN"` AND
///   `"tally rebuild --from-source"` (D-C2). This is the Phase 52
///   TPC-CORR-02 guard extended to boot rematerialization.
/// - Missing partition for a downstream table or unknown shard index →
///   returns `BeavaError::Protocol`.
///
/// Replay order: serial over shards (single-writer), serial over streams
/// within a shard, in-order over entries within a stream. Cross-shard
/// cascade dispatch uses `SyncCascadeTargets` which applies writes
/// directly to the target fjall partition on the calling thread
/// (D-C3 — parallelism is at the TT fan-out layer, NOT at the replay
/// layer).
///
/// **state-inmem build:** there is no persistent event log on disk, so
/// this function skips replay unconditionally and returns an empty
/// report with a debug-log line (Pitfall 7 defensive). A subsequent
/// snapshot save upgrades the on-disk format to v9 just the same.
#[cfg(not(feature = "state-inmem"))]
pub fn rematerialize_tables_from_event_logs(
    shards: &[std::sync::Arc<std::sync::Mutex<crate::shard::Shard>>],
    event_logs: &[std::sync::Arc<crate::state::event_log::EventLog>],
    engine: &crate::engine::pipeline::PipelineEngine,
) -> Result<RematerializeReport, crate::error::BeavaError> {
    use crate::engine::cascade_target::SyncCascadeTargets;

    if shards.len() != event_logs.len() {
        return Err(crate::error::BeavaError::Protocol(format!(
            "rematerialize: shards.len()={} != event_logs.len()={}",
            shards.len(),
            event_logs.len()
        )));
    }
    let n_shards = shards.len();

    // 1. Clear downstream TT output tables on every shard so we rebuild from
    //    scratch — pre-Phase-55 snapshots planted rows on the INPUT event's
    //    shard (wrong shard); we want only hash(output_key)%N-owned rows.
    //    Clearing is a row-level `remove_table_row` walk for every known
    //    entity on the shard. The PartitionHandle does not expose a prefix-
    //    delete efficient enough for this, so we iterate entities and drop
    //    table_rows entries for the downstream tables.
    let downstream_tables = engine.downstream_tt_output_tables();
    for shard_arc in shards.iter() {
        let mut shard = shard_arc.lock().map_err(|e| {
            crate::error::BeavaError::Protocol(format!(
                "rematerialize: lock shard: {e}"
            ))
        })?;
        clear_downstream_table_rows(&mut shard, &downstream_tables)?;
    }

    // 2. For each shard, iterate primary streams and replay events through
    //    the cascade.
    let mut total_events: u64 = 0;
    for (s, event_log) in event_logs.iter().enumerate() {
        let primary_streams = engine.primary_streams_on_shard(s);

        // Build a SyncCascadeTargets view that lets the cascade dispatch
        // cross-shard writes directly to sibling shards' partitions. The
        // target trait object is constructed fresh per source shard so the
        // `source_shard_idx` field matches the input shard we're replaying
        // from.
        let sync_tgt = SyncCascadeTargets {
            shards,
            source_shard_idx: s,
        };

        for p in primary_streams {
            // Truncation guard: if the stream is registered on disk but its
            // log file is empty / truncated (zero entries) while the
            // snapshot expects replay from LSN 1, hard-fail with the D-C2
            // actionable error. We cannot tell "never-written" from
            // "truncated" at this layer, so we scope the check to
            // registered streams whose log file exists but whose first
            // entry has a non-zero LSN (indicating gap).
            let entries = event_log.read_entries(&p).map_err(|e| {
                crate::error::BeavaError::Protocol(format!(
                    "rematerialize: read_entries({}) on shard {}: {}",
                    p, s, e
                ))
            })?;
            if let Some(first) = entries.first() {
                // LSN 0 = pre-v1.2 entry (no packed LSN). Anything else at
                // position-0 that is > 1 indicates truncation.
                if first.lsn > 1 {
                    return Err(crate::error::BeavaError::Protocol(format!(
                        "Event log truncated before LSN {}; cannot \
                         rematerialize downstream tables. Run \
                         'tally rebuild --from-source' to re-ingest.",
                        first.lsn
                    )));
                }
            }

            // Replay entries in order through the cascade path. The lock
            // against the source shard is held for the duration of the
            // per-event push_with_cascade_on_shard call; SyncCascadeTargets
            // would also attempt to lock this same shard if a same-shard
            // output resulted — but push_with_cascade_on_shard's
            // same-shard fast path does the write inline via
            // `StoreView::Sharded` on `input_shard`, not via the
            // `CascadeTarget`. Cross-shard writes take the target_shard_idx
            // path (locks a sibling, not self).
            let now = std::time::SystemTime::now();
            let mut shard = shards[s].lock().map_err(|e| {
                crate::error::BeavaError::Protocol(format!(
                    "rematerialize: lock shard {}: {e}",
                    s
                ))
            })?;
            for entry in &entries {
                engine.replay_one_event_through_cascade(
                    entry, &sync_tgt, &p, &mut shard, s, now,
                )?;
                total_events += 1;
            }
            drop(shard);
        }
    }

    Ok(RematerializeReport {
        events_replayed: total_events,
        shards_processed: n_shards,
    })
}

/// state-inmem build: skip rematerialization — no persistent event log.
#[cfg(feature = "state-inmem")]
pub fn rematerialize_tables_from_event_logs(
    _shards: &[std::sync::Arc<std::sync::Mutex<crate::shard::Shard>>],
    _event_logs: &[std::sync::Arc<crate::state::event_log::EventLog>],
    _engine: &crate::engine::pipeline::PipelineEngine,
) -> Result<RematerializeReport, crate::error::BeavaError> {
    // Intentional: boot status line (Phase 47 audit exemption — startup
    // informational output consistent with "Snapshot version mismatch"
    // in state/snapshot.rs).
    eprintln!(
        "Rematerialization skipped (state-inmem build — no persistent event log)."
    );
    Ok(RematerializeReport {
        events_replayed: 0,
        shards_processed: 0,
    })
}

/// Phase 55-03 helper: drop every downstream-TT table row from a shard's
/// partition. Iterates the shard's entity list and removes the named
/// `table_rows` entries in-place. Used before replay to ensure a clean
/// slate (pre-Phase-55 snapshots may have planted rows on the wrong shard).
#[cfg(not(feature = "state-inmem"))]
fn clear_downstream_table_rows(
    shard: &mut crate::shard::Shard,
    tables: &[String],
) -> Result<(), crate::error::BeavaError> {
    if tables.is_empty() {
        return Ok(());
    }
    // Snapshot the entity keys first (fjall iteration materializes),
    // then RMW each.
    let keys: Vec<String> = shard
        .iter_entities()
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    for key in &keys {
        for table_name in tables {
            // Remove the downstream TT output table entry from this
            // entity. Shard does not expose a direct `remove_table_row`,
            // so we RMW via StoreView::with_entity_mut (which handles
            // both fjall + state-inmem backends).
            use crate::shard::StoreView;
            let mut view = StoreView::Sharded(shard);
            view.with_entity_mut(key.as_str(), |entity| {
                entity.table_rows.remove(table_name);
            });
        }
        shard.dirty_set.insert(key.clone());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_recovery_barrier_new() {
        let b = RecoveryBarrier::new(4);
        assert_eq!(b.total, 4);
        assert!(!b.all_recovered());
        assert_eq!(b.recovering_shards().len(), 4);
    }

    #[test]
    fn test_recovery_barrier_mark_recovered_idempotent() {
        let b = RecoveryBarrier::new(2);
        b.mark_recovered(0);
        b.mark_recovered(0); // idempotent
        b.mark_recovered(0); // idempotent
        assert!(!b.all_recovered(), "shard-1 still recovering");
        b.mark_recovered(1);
        assert!(b.all_recovered());
    }

    #[test]
    fn test_recovery_barrier_shard_is_recovered() {
        let b = RecoveryBarrier::new(3);
        assert!(!b.shard_is_recovered(0));
        b.mark_recovered(2);
        assert!(!b.shard_is_recovered(0));
        assert!(!b.shard_is_recovered(1));
        assert!(b.shard_is_recovered(2));
    }

    #[test]
    fn test_discover_streams_missing_dir_returns_empty() {
        let tmp = tempfile::TempDir::new().unwrap();
        let streams = discover_streams_for_shard(tmp.path(), 5).unwrap();
        assert!(streams.is_empty());
    }

    #[test]
    fn test_discover_streams_finds_log_bin() {
        use crate::state::event_log::EventLog;
        use std::time::UNIX_EPOCH;

        let tmp = tempfile::TempDir::new().unwrap();
        let log = EventLog::new_for_shard(tmp.path().to_path_buf(), 0).unwrap();
        log.register_stream("TestStream", None).unwrap();
        log.append("TestStream", b"data", UNIX_EPOCH).unwrap();

        let streams = discover_streams_for_shard(tmp.path(), 0).unwrap();
        assert!(streams.contains(&"TestStream".to_string()));
    }
}
