use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tally::engine::pipeline::PipelineEngine;
use tally::server::http::run_http_server;
use tally::server::protocol::{
    convert_register_request, convert_view_register_request, RegisterRequest,
};
use tally::server::tcp::{
    make_concurrent_state, run_backfill, run_tcp_server, BackfillStatus, BackfillTracker,
    SharedState,
};
use tally::state::event_log::EventLog;
use tally::state::eviction::evict_expired_keys;
use tally::state::snapshot::{
    load_legacy_v5, load_snapshot_file, save_base_snapshot, save_delta_snapshot, BaseSnapshotState,
    DeltaSnapshotState, SerializablePipeline, SnapshotFile, SnapshotHeader, SnapshotState,
    SnapshotType,
};
use tally::state::store::StateStore;

/// Local enum used by the periodic snapshot timer to pass a fully-prepared
/// snapshot payload (base or delta) into the blocking serialization task.
enum SnapshotData {
    Base(BaseSnapshotState),
    Delta(DeltaSnapshotState),
}

fn main() {
    let worker_threads: usize = std::env::var("TALLY_WORKER_THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(worker_threads);
    builder.enable_all();
    let runtime = builder.build().expect("failed to build tokio runtime");
    eprintln!("Worker threads: {}", worker_threads);
    runtime.block_on(async_main());
}

async fn async_main() {
    let tcp_port = std::env::var("TALLY_TCP_PORT").unwrap_or_else(|_| "6400".into());
    let http_port = std::env::var("TALLY_HTTP_PORT").unwrap_or_else(|_| "6401".into());
    let snapshot_path = PathBuf::from(
        std::env::var("TALLY_SNAPSHOT_PATH").unwrap_or_else(|_| "tally.snapshot".into()),
    );
    let ttl_multiplier: u32 = std::env::var("TALLY_TTL_MULTIPLIER")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);

    let event_log_enabled = std::env::var("TALLY_EVENT_LOG")
        .map(|v| v != "false" && v != "0")
        .unwrap_or(true);
    let snapshot_enabled = std::env::var("TALLY_SNAPSHOT")
        .map(|v| v != "false" && v != "0")
        .unwrap_or(true);

    let tcp_addr = format!("0.0.0.0:{}", tcp_port);
    let http_addr = format!("0.0.0.0:{}", http_port);

    // Initialize event log directory (skip if disabled)
    let event_log = if event_log_enabled {
        let event_log_dir =
            PathBuf::from(std::env::var("TALLY_DATA_DIR").unwrap_or_else(|_| ".".into()))
                .join("events");
        EventLog::new(event_log_dir).map(Some).unwrap_or_else(|e| {
            eprintln!("Failed to initialize event log: {}", e);
            None
        })
    } else {
        eprintln!("Event log: disabled");
        None
    };

    // Phase 14: ConcurrentAppState with per-field locking.
    let state: SharedState = make_concurrent_state(
        PipelineEngine::new(),
        StateStore::new(),
        event_log,
        snapshot_path.clone(),
        Arc::new(BackfillTracker::default()),
        snapshot_enabled,
        event_log_enabled,
    );

    // Phase 9: how often to write a full base snapshot. Every Nth cycle is a
    // base, all other cycles are deltas. Default 10 (= one base per ~5 minutes
    // at the default 30s interval).
    let full_snapshot_interval: u64 = std::env::var("TALLY_FULL_SNAPSHOT_INTERVAL")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);

    // Load snapshot on startup -- incremental recovery (OPS-04).
    // Skip if snapshots are disabled.
    let recovery = if snapshot_enabled {
        let snap_dir_startup = snapshot_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .to_path_buf();
        load_incremental_snapshots(&snap_dir_startup, &snapshot_path)
    } else {
        eprintln!("Snapshots: disabled");
        None
    };
    if let Some((snapshot_state, next_seq, loaded_base_seq)) = recovery {
        *state.snapshot_seq.lock() = next_seq;
        *state.last_base_seq.lock() = loaded_base_seq;
        *state.previous_base_seq.lock() = 0;

        // Restore entity state
        state.store.restore_from_snapshot(snapshot_state.entities);
        // Clear any dirty/deleted tracking
        state.store.clear_dirty();
        let _ = state.store.take_deleted();

        // Re-register pipelines from stored JSON
        {
            let mut engine = state.engine.write();
            for pipeline in snapshot_state.pipelines {
                let parsed: Result<serde_json::Value, _> =
                    serde_json::from_str(&pipeline.raw_register_json);
                if let Ok(json_val) = parsed {
                    let req: Result<RegisterRequest, _> = serde_json::from_value(json_val.clone());
                    if let Ok(req) = req {
                        let def_name = req.name.clone();
                        let is_view = req.definition_type.as_deref() == Some("view");
                        let registered: Result<(), tally::error::TallyError> = if is_view {
                            convert_view_register_request(req)
                                .and_then(|view_def| engine.register_view(view_def))
                        } else {
                            convert_register_request(req)
                                .and_then(|stream_def| engine.register(stream_def).map(|_diff| ()))
                        };
                        if registered.is_ok() {
                            engine.store_raw_register_json(&def_name, json_val);
                            // Register stream with event log for persistence
                            if !is_view {
                                let history_ttl =
                                    engine.get_stream(&def_name).and_then(|s| s.history_ttl);
                                let mut event_log = state.event_log.lock();
                                if let Some(ref mut log) = *event_log {
                                    let _ = log.register_stream(&def_name, history_ttl);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Restore backfill_complete markers from snapshot
        {
            let mut bc = state.backfill_complete.lock();
            for (stream, feature) in &snapshot_state.backfill_complete {
                bc.insert((stream.clone(), feature.clone()));
            }
        }

        // Phase 9 WR-05: one-shot GC pass.
        {
            let engine = state.engine.read();
            let valid_features = engine.valid_features_map();
            state.store.gc_invalid_operators(&valid_features);
        }

        // Detect incomplete backfills
        let mut incomplete_backfills: Vec<(String, Vec<String>)> = Vec::new();
        {
            let engine = state.engine.read();
            let bc = state.backfill_complete.lock();
            for stream in engine.list_streams() {
                let missing: Vec<String> = stream
                    .features
                    .iter()
                    .filter(|(_, def)| tally::engine::pipeline::get_backfill_flag(def))
                    .filter(|(name, _)| !bc.contains(&(stream.name.clone(), name.clone())))
                    .map(|(name, _)| name.clone())
                    .collect();
                if !missing.is_empty() {
                    incomplete_backfills.push((stream.name.clone(), missing));
                }
            }
        }

        eprintln!("Loaded snapshot (next_seq={})", next_seq);

        // Spawn backfill tasks for incomplete backfills
        for (stream_name, features) in incomplete_backfills {
            let entries = {
                let event_log = state.event_log.lock();
                event_log
                    .as_ref()
                    .map(|log| log.read_entries(&stream_name).unwrap_or_default())
                    .unwrap_or_default()
            };
            if !entries.is_empty() {
                let status = Arc::new(BackfillStatus {
                    stream: stream_name.clone(),
                    features: features.clone(),
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
                eprintln!(
                    "Resuming incomplete backfill for {} features: {:?}",
                    stream_name, features
                );
                tokio::spawn(run_backfill(
                    state.clone(),
                    stream_name,
                    features,
                    entries,
                    status,
                ));
            }
        }
    }

    let tcp_state = state.clone();
    let tcp_handle = tokio::spawn(async move {
        if let Err(e) = run_tcp_server(&tcp_addr, tcp_state).await {
            eprintln!("TCP server error: {}", e);
        }
    });

    let http_state = state.clone();
    let http_handle = tokio::spawn(async move {
        if let Err(e) = run_http_server(&http_addr, http_state).await {
            eprintln!("HTTP server error: {}", e);
        }
    });

    // Periodic incremental snapshot timer (PERS-01, PERS-04, OPS-03).
    // Skip if snapshots are disabled.
    if snapshot_enabled {
        let snap_state = state.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            interval.tick().await; // First tick completes immediately -- skip it
            loop {
                interval.tick().await;

                // Phase 15: cycle guard — skip if a previous snapshot write is
                // still in progress (from either the timer or a manual trigger).
                if snap_state
                    .snapshot_in_progress
                    .compare_exchange(
                        false,
                        true,
                        std::sync::atomic::Ordering::AcqRel,
                        std::sync::atomic::Ordering::Acquire,
                    )
                    .is_err()
                {
                    snap_state.metrics.lock().snapshots_skipped += 1;
                    eprintln!("Snapshot cycle skipped: previous write still in progress");
                    continue;
                }
                // RAII guard clears the flag even on panic.
                struct SnapGuard(SharedState);
                impl Drop for SnapGuard {
                    fn drop(&mut self) {
                        self.0
                            .snapshot_in_progress
                            .store(false, std::sync::atomic::Ordering::Release);
                    }
                }
                let _guard = SnapGuard(snap_state.clone());

                // Decide base vs delta, clone the required state, and advance
                // the cycle counter — using individual locks.
                let prepared: Option<(SnapshotData, u64, bool, PathBuf, u64)> = {
                    let engine = snap_state.engine.read();
                    let store = &snap_state.store;
                    let cycle = *snap_state.snapshot_cycle.lock();
                    let seq = *snap_state.snapshot_seq.lock();
                    let is_full = cycle.is_multiple_of(full_snapshot_interval);
                    let valid_features = engine.valid_features_map();
                    let snap_dir = snap_state
                        .snapshot_path
                        .parent()
                        .unwrap_or_else(|| std::path::Path::new("."))
                        .to_path_buf();

                    let last_base_seq_for_delta = *snap_state.last_base_seq.lock();
                    if is_full {
                        // Full base snapshot -- clone everything.
                        let entities = store.clone_for_snapshot_with_gc(&valid_features);
                        let mut pipelines: Vec<SerializablePipeline> = engine
                            .list_streams()
                            .filter_map(|stream| {
                                engine.get_raw_register_json(&stream.name).map(|json| {
                                    SerializablePipeline {
                                        name: stream.name.clone(),
                                        key_field: stream.key_field.clone().unwrap_or_default(),
                                        raw_register_json: serde_json::to_string(json)
                                            .unwrap_or_default(),
                                    }
                                })
                            })
                            .collect();
                        for view in engine.list_views() {
                            if let Some(json) = engine.get_raw_register_json(&view.name) {
                                pipelines.push(SerializablePipeline {
                                    name: view.name.clone(),
                                    key_field: view.key_field.clone(),
                                    raw_register_json: serde_json::to_string(json)
                                        .unwrap_or_default(),
                                });
                            }
                        }
                        let backfill_complete: Vec<(String, String)> = snap_state
                            .backfill_complete
                            .lock()
                            .iter()
                            .cloned()
                            .collect();
                        // Clear tracking
                        store.clear_dirty();
                        let _ = store.take_deleted();

                        let base = BaseSnapshotState {
                            header: SnapshotHeader {
                                snapshot_type: SnapshotType::Base,
                                sequence: seq,
                            },
                            entities,
                            pipelines,
                            backfill_complete,
                        };
                        *snap_state.snapshot_cycle.lock() = cycle + 1;
                        *snap_state.snapshot_seq.lock() = seq + 1;
                        let prev_base = *snap_state.last_base_seq.lock();
                        *snap_state.previous_base_seq.lock() = prev_base;
                        *snap_state.last_base_seq.lock() = seq;
                        Some((SnapshotData::Base(base), seq, true, snap_dir, prev_base))
                    } else {
                        // Delta -- clone only dirty entities.
                        let changed = store.clone_dirty_for_snapshot_with_gc(&valid_features);
                        let deleted = store.take_deleted();
                        store.clear_dirty();

                        if changed.is_empty() && deleted.is_empty() {
                            *snap_state.snapshot_cycle.lock() = cycle + 1;
                            None
                        } else {
                            let delta = DeltaSnapshotState {
                                header: SnapshotHeader {
                                    snapshot_type: SnapshotType::Delta {
                                        base_seq: last_base_seq_for_delta,
                                    },
                                    sequence: seq,
                                },
                                changed_entities: changed,
                                deleted_keys: deleted,
                            };
                            *snap_state.snapshot_cycle.lock() = cycle + 1;
                            *snap_state.snapshot_seq.lock() = seq + 1;
                            Some((SnapshotData::Delta(delta), seq, false, snap_dir, 0))
                        }
                    }
                };

                let (snapshot_data, seq, is_full, snap_dir, prev_base_seq_for_cleanup) =
                    match prepared {
                        Some(p) => p,
                        None => continue, // No changes this cycle
                    };

                // Serialize on blocking thread pool
                let snap_start = std::time::Instant::now();
                let result = tokio::task::spawn_blocking(move || {
                    let (bytes, filename) = match snapshot_data {
                        SnapshotData::Base(base) => {
                            let bytes = save_base_snapshot(&base)
                                .map_err(std::io::Error::other)?;
                            let filename = format!("tally.snapshot.base.{:010}", seq);
                            Ok::<(Vec<u8>, String), std::io::Error>((bytes, filename))
                        }
                        SnapshotData::Delta(delta) => {
                            let bytes = save_delta_snapshot(&delta)
                                .map_err(std::io::Error::other)?;
                            let filename = format!("tally.snapshot.delta.{:010}", seq);
                            Ok((bytes, filename))
                        }
                    }?;
                    let file_path = snap_dir.join(&filename);
                    let tmp_path = snap_dir.join(format!("{}.tmp", filename));
                    {
                        use std::fs::OpenOptions;
                        use std::io::Write;
                        let mut f = OpenOptions::new()
                            .create(true)
                            .write(true)
                            .truncate(true)
                            .open(&tmp_path)?;
                        f.write_all(&bytes)?;
                        f.sync_all()?;
                    }
                    std::fs::rename(&tmp_path, &file_path)?;
                    if let Ok(dir) = std::fs::File::open(&snap_dir) {
                        let _ = dir.sync_all();
                    }
                    if is_full {
                        let cutoff = if prev_base_seq_for_cleanup == 0 {
                            seq
                        } else {
                            prev_base_seq_for_cleanup
                        };
                        cleanup_old_snapshots(&snap_dir, cutoff);
                    }
                    Ok::<usize, std::io::Error>(bytes.len())
                })
                .await;
                match result {
                    Ok(Ok(size)) => {
                        let snap_elapsed = snap_start.elapsed();
                        snap_state.metrics.lock().snapshot_duration_ms =
                            snap_elapsed.as_millis() as u64;
                        eprintln!(
                            "Snapshot saved ({} bytes, {}ms, {})",
                            size,
                            snap_elapsed.as_millis(),
                            if is_full { "base" } else { "delta" },
                        );
                    }
                    Ok(Err(e)) => eprintln!("Snapshot write failed: {}", e),
                    Err(e) => eprintln!("Snapshot task panicked: {}", e),
                }
            }
        });
    } // if snapshot_enabled

    // Periodic eviction timer (PERS-05)
    let evict_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        interval.tick().await; // Skip first immediate tick
        loop {
            interval.tick().await;
            let now = std::time::SystemTime::now();
            let engine = evict_state.engine.read();
            let evicted = evict_expired_keys(&evict_state.store, &engine, now, ttl_multiplier);
            if evicted > 0 {
                eprintln!("Evicted {} expired keys", evicted);
            }
        }
    });

    // Periodic event log fsync timer (ELOG-04: 1-second interval, Redis everysec pattern)
    // Skip if event log is disabled.
    if event_log_enabled {
        let fsync_state = state.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.tick().await; // Skip first immediate tick
            loop {
                interval.tick().await;
                let result = {
                    let mut event_log = fsync_state.event_log.lock();
                    if let Some(ref mut log) = *event_log {
                        log.fsync_all()
                    } else {
                        Ok(())
                    }
                };
                if let Err(e) = result {
                    eprintln!("Event log fsync failed: {}", e);
                }
            }
        });
    } // if event_log_enabled

    // Periodic event log compaction timer (ELOG-05: 60-second interval)
    // Skip if event log is disabled.
    if event_log_enabled {
        let compact_state = state.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            interval.tick().await; // Skip first immediate tick
            loop {
                interval.tick().await;
                let now = SystemTime::now();
                // Get list of streams to compact
                let streams_to_compact: Vec<String> = {
                    let event_log = compact_state.event_log.lock();
                    if let Some(ref log) = *event_log {
                        log.registered_streams().map(String::from).collect()
                    } else {
                        vec![]
                    }
                };
                // Compact each stream (re-acquires lock per stream for cooperative yielding)
                for stream_name in &streams_to_compact {
                    {
                        let mut event_log = compact_state.event_log.lock();
                        if let Some(ref mut log) = *event_log {
                            match log.compact_stream(stream_name, now) {
                                Ok(removed) if removed > 0 => {
                                    eprintln!(
                                        "Compacted {}: removed {} expired entries",
                                        stream_name, removed
                                    );
                                }
                                Err(e) => {
                                    eprintln!("Compaction failed for {}: {}", stream_name, e);
                                }
                                _ => {}
                            }
                        }
                    }
                    // Yield between streams for cooperative scheduling
                    tokio::task::yield_now().await;
                }
            }
        });
    } // if event_log_enabled

    // Log ephemeral mode if both persistence mechanisms are disabled
    if !snapshot_enabled && !event_log_enabled {
        eprintln!("Running in ephemeral mode (no persistence)");
    }

    tokio::select! {
        _ = tcp_handle => {},
        _ = http_handle => {},
    }
}

// ================ Phase 9: Incremental Snapshot Helpers ================

/// Remove snapshot files whose sequence is strictly less than the current
/// base's sequence.
fn cleanup_old_snapshots(dir: &Path, current_base_seq: u64) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        let seq_opt = name_str
            .strip_prefix("tally.snapshot.base.")
            .or_else(|| name_str.strip_prefix("tally.snapshot.delta."));
        if let Some(seq_str) = seq_opt {
            if let Ok(seq) = seq_str.parse::<u64>() {
                if seq < current_base_seq {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
    }
}

/// Scan the snapshot directory and load the latest base + subsequent deltas.
pub(crate) fn load_incremental_snapshots(
    snap_dir: &Path,
    legacy_path: &Path,
) -> Option<(SnapshotState, u64, u64)> {
    let mut bases: Vec<(u64, PathBuf)> = Vec::new();
    let mut deltas: Vec<(u64, PathBuf)> = Vec::new();

    if let Ok(entries) = std::fs::read_dir(snap_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy().into_owned();
            if let Some(seq_str) = name_str.strip_prefix("tally.snapshot.base.") {
                if let Ok(seq) = seq_str.parse::<u64>() {
                    bases.push((seq, entry.path()));
                }
            } else if let Some(seq_str) = name_str.strip_prefix("tally.snapshot.delta.") {
                if let Ok(seq) = seq_str.parse::<u64>() {
                    deltas.push((seq, entry.path()));
                }
            }
        }
    }

    bases.sort_by_key(|(seq, _)| *seq);

    let loaded = bases.iter().rev().find_map(|(seq, path)| {
        let bytes = std::fs::read(path).ok()?;
        match load_snapshot_file(&bytes)? {
            SnapshotFile::Base(b) => Some((*seq, b)),
            _ => None,
        }
    });

    if let Some((base_seq, base)) = loaded {
        let store = StateStore::new();
        store.restore_from_snapshot(base.entities.clone());

        let mut applicable: Vec<(u64, PathBuf)> = deltas
            .into_iter()
            .filter(|(seq, _)| *seq > base_seq)
            .collect();
        applicable.sort_by_key(|(seq, _)| *seq);

        let mut max_seq = base_seq;
        for (seq, delta_path) in &applicable {
            let bytes = match std::fs::read(delta_path) {
                Ok(b) => b,
                Err(_) => continue,
            };
            match load_snapshot_file(&bytes) {
                Some(SnapshotFile::Delta(delta)) => {
                    store.apply_delta(delta.changed_entities, delta.deleted_keys);
                    if *seq > max_seq {
                        max_seq = *seq;
                    }
                }
                _ => continue,
            }
        }

        let state = SnapshotState {
            entities: store.clone_for_snapshot(),
            pipelines: base.pipelines,
            backfill_complete: base.backfill_complete,
        };
        return Some((state, max_seq + 1, base_seq));
    }

    if legacy_path.exists() {
        let bytes = std::fs::read(legacy_path).ok()?;
        let legacy = load_legacy_v5(&bytes)?;
        eprintln!("Loaded legacy v5 snapshot from {}", legacy_path.display());
        return Some((legacy, 1, 0));
    }

    None
}
