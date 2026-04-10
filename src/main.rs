use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicUsize;
use std::time::{Duration, SystemTime};

use tally::engine::pipeline::PipelineEngine;
use tally::server::http::run_http_server;
use tally::server::protocol::{RegisterRequest, convert_register_request, convert_view_register_request};
use tally::server::tcp::{AppState, BackfillStatus, BackfillTracker, Metrics, run_backfill, run_tcp_server};
use tally::state::event_log::EventLog;
use tally::state::eviction::evict_expired_keys;
use tally::state::snapshot::{
    BaseSnapshotState, DeltaSnapshotState, SerializablePipeline, SnapshotFile, SnapshotHeader,
    SnapshotState, SnapshotType, load_legacy_v5, load_snapshot_file, save_base_snapshot,
    save_delta_snapshot,
};
use tally::state::store::StateStore;

/// Local enum used by the periodic snapshot timer to pass a fully-prepared
/// snapshot payload (base or delta) into the blocking serialization task.
enum SnapshotData {
    Base(BaseSnapshotState),
    Delta(DeltaSnapshotState),
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let tcp_port = std::env::var("TALLY_TCP_PORT").unwrap_or_else(|_| "6400".into());
    let http_port = std::env::var("TALLY_HTTP_PORT").unwrap_or_else(|_| "6401".into());
    let snapshot_path = PathBuf::from(
        std::env::var("TALLY_SNAPSHOT_PATH").unwrap_or_else(|_| "tally.snapshot".into()),
    );
    let ttl_multiplier: u32 = std::env::var("TALLY_TTL_MULTIPLIER")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);

    let tcp_addr = format!("0.0.0.0:{}", tcp_port);
    let http_addr = format!("0.0.0.0:{}", http_port);

    // Initialize event log directory
    let event_log_dir = PathBuf::from(
        std::env::var("TALLY_DATA_DIR").unwrap_or_else(|_| ".".into()),
    ).join("events");
    let event_log = EventLog::new(event_log_dir)
        .map(Some)
        .unwrap_or_else(|e| {
            eprintln!("Failed to initialize event log: {}", e);
            None
        });

    let state = Arc::new(Mutex::new(AppState {
        engine: PipelineEngine::new(),
        store: StateStore::new(),
        metrics: Metrics::default(),
        snapshot_path: snapshot_path.clone(),
        event_log,
        backfill_tracker: Arc::new(BackfillTracker::default()),
        backfill_complete: HashSet::new(),
        snapshot_cycle: 0,
        snapshot_seq: 1,
    }));

    // Phase 9: how often to write a full base snapshot. Every Nth cycle is a
    // base, all other cycles are deltas. Default 10 (= one base per ~5 minutes
    // at the default 30s interval).
    let full_snapshot_interval: u64 = std::env::var("TALLY_FULL_SNAPSHOT_INTERVAL")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);

    // Load snapshot on startup -- incremental recovery (OPS-04).
    // Scans the snapshot directory for v6 base+delta files, merges them into
    // a single state, and falls back to the legacy v5 single-file snapshot if
    // no v6 files are found. Updates snapshot_seq to max_seen + 1 so the next
    // timer tick continues the numbering.
    let snap_dir_startup = snapshot_path.parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .to_path_buf();
    let recovery = load_incremental_snapshots(&snap_dir_startup, &snapshot_path);
    if let Some((snapshot_state, next_seq)) = recovery {
        let mut app = state.lock().unwrap_or_else(|e| e.into_inner());
        app.snapshot_seq = next_seq;
        // Restore entity state
        app.store.restore_from_snapshot(snapshot_state.entities);
        // Clear any dirty/deleted tracking that restore_from_snapshot may have
        // introduced (belt-and-suspenders: apply_delta explicitly avoids
        // touching the tracking sets, but a fresh start is cleaner).
        app.store.clear_dirty();
        let _ = app.store.take_deleted();
        // Re-register pipelines from stored JSON
        for pipeline in snapshot_state.pipelines {
            let parsed: Result<serde_json::Value, _> =
                serde_json::from_str(&pipeline.raw_register_json);
            if let Ok(json_val) = parsed {
                let req: Result<RegisterRequest, _> =
                    serde_json::from_value(json_val.clone());
                if let Ok(req) = req {
                    let def_name = req.name.clone();
                    let is_view = req.definition_type.as_deref() == Some("view");
                    let registered: Result<(), tally::error::TallyError> = if is_view {
                        convert_view_register_request(req)
                            .and_then(|view_def| app.engine.register_view(view_def))
                    } else {
                        convert_register_request(req)
                            .and_then(|stream_def| app.engine.register(stream_def).map(|_diff| ()))
                    };
                    if registered.is_ok() {
                        app.engine.store_raw_register_json(&def_name, json_val);
                        // Register stream with event log for persistence
                        if !is_view {
                            let history_ttl = app.engine.get_stream(&def_name)
                                .and_then(|s| s.history_ttl);
                            if let Some(ref mut log) = app.event_log {
                                let _ = log.register_stream(&def_name, history_ttl);
                            }
                        }
                    }
                }
            }
        }
        // Restore backfill_complete markers from snapshot
        for (stream, feature) in &snapshot_state.backfill_complete {
            app.backfill_complete.insert((stream.clone(), feature.clone()));
        }

        // Detect incomplete backfills: features with backfill=true that are not in
        // the backfill_complete set. Re-spawn backfill for them (idempotent restart
        // per CONTEXT.md locked decision). Operators are deterministic so replay
        // from start produces the same result.
        let mut incomplete_backfills: Vec<(String, Vec<String>)> = Vec::new();
        for stream in app.engine.list_streams() {
            let missing: Vec<String> = stream.features.iter()
                .filter(|(_, def)| tally::engine::pipeline::get_backfill_flag(def))
                .filter(|(name, _)| !app.backfill_complete.contains(&(stream.name.clone(), name.clone())))
                .map(|(name, _)| name.clone())
                .collect();
            if !missing.is_empty() {
                incomplete_backfills.push((stream.name.clone(), missing));
            }
        }

        eprintln!("Loaded snapshot (next_seq={})", next_seq);

        // Must drop the lock before spawning backfill tasks
        drop(app);

        // Spawn backfill tasks for incomplete backfills (outside lock)
        for (stream_name, features) in incomplete_backfills {
            let entries = {
                let app = state.lock().unwrap_or_else(|e| e.into_inner());
                app.event_log.as_ref()
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
                    completed_at: Mutex::new(None),
                });
                {
                    let app = state.lock().unwrap_or_else(|e| e.into_inner());
                    app.backfill_tracker.tasks.lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .push(Arc::clone(&status));
                }
                eprintln!("Resuming incomplete backfill for {} features: {:?}", stream_name, features);
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
    // Writes a delta every 30s by default, with a full base every
    // full_snapshot_interval cycles. No-op cycles (no dirty, no deleted)
    // are skipped entirely. Old files are cleaned up after each base.
    let snap_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        interval.tick().await; // First tick completes immediately -- skip it
        loop {
            interval.tick().await;

            // Decide base vs delta, clone the required state, and advance
            // the cycle counter -- all under a single lock acquisition so
            // the dirty set cannot race with new events.
            let prepared: Option<(SnapshotData, u64, bool, PathBuf)> = {
                let mut app = snap_state.lock().unwrap_or_else(|e| e.into_inner());
                let cycle = app.snapshot_cycle;
                let seq = app.snapshot_seq;
                let is_full = cycle % full_snapshot_interval == 0;
                let valid_features = app.engine.valid_features_map();
                let snap_dir = app.snapshot_path.parent()
                    .unwrap_or_else(|| std::path::Path::new("."))
                    .to_path_buf();

                if is_full {
                    // Full base snapshot -- clone everything.
                    let entities = app.store.clone_for_snapshot_with_gc(&valid_features);
                    let mut pipelines: Vec<SerializablePipeline> = app
                        .engine
                        .list_streams()
                        .filter_map(|stream| {
                            app.engine
                                .get_raw_register_json(&stream.name)
                                .map(|json| SerializablePipeline {
                                    name: stream.name.clone(),
                                    key_field: stream.key_field.clone().unwrap_or_default(),
                                    raw_register_json: serde_json::to_string(json)
                                        .unwrap_or_default(),
                                })
                        })
                        .collect();
                    for view in app.engine.list_views() {
                        if let Some(json) = app.engine.get_raw_register_json(&view.name) {
                            pipelines.push(SerializablePipeline {
                                name: view.name.clone(),
                                key_field: view.key_field.clone(),
                                raw_register_json: serde_json::to_string(json)
                                    .unwrap_or_default(),
                            });
                        }
                    }
                    let backfill_complete: Vec<(String, String)> =
                        app.backfill_complete.iter().cloned().collect();
                    // Clear tracking -- a full base supersedes any pending delta.
                    app.store.clear_dirty();
                    let _ = app.store.take_deleted();

                    let base = BaseSnapshotState {
                        header: SnapshotHeader {
                            snapshot_type: SnapshotType::Base,
                            sequence: seq,
                        },
                        entities,
                        pipelines,
                        backfill_complete,
                    };
                    app.snapshot_cycle += 1;
                    app.snapshot_seq += 1;
                    Some((SnapshotData::Base(base), seq, true, snap_dir))
                } else {
                    // Delta -- clone only dirty entities.
                    let changed = app.store.clone_dirty_for_snapshot_with_gc(&valid_features);
                    let deleted = app.store.take_deleted();
                    app.store.clear_dirty();

                    // No-change cycle: advance cycle counter and skip the
                    // write entirely. Seq stays put so the next written file
                    // has no gap.
                    if changed.is_empty() && deleted.is_empty() {
                        app.snapshot_cycle += 1;
                        None
                    } else {
                        let delta = DeltaSnapshotState {
                            header: SnapshotHeader {
                                snapshot_type: SnapshotType::Delta {
                                    base_seq: seq.saturating_sub(1),
                                },
                                sequence: seq,
                            },
                            changed_entities: changed,
                            deleted_keys: deleted,
                        };
                        app.snapshot_cycle += 1;
                        app.snapshot_seq += 1;
                        Some((SnapshotData::Delta(delta), seq, false, snap_dir))
                    }
                }
            };

            let (snapshot_data, seq, is_full, snap_dir) = match prepared {
                Some(p) => p,
                None => continue, // No changes this cycle
            };

            // Serialize on blocking thread pool (PERS-04: does not block event loop)
            let snap_start = std::time::Instant::now();
            let result = tokio::task::spawn_blocking(move || {
                let (bytes, filename) = match snapshot_data {
                    SnapshotData::Base(base) => {
                        let bytes = save_base_snapshot(&base)
                            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
                        let filename = format!("tally.snapshot.base.{:010}", seq);
                        Ok::<(Vec<u8>, String), std::io::Error>((bytes, filename))
                    }
                    SnapshotData::Delta(delta) => {
                        let bytes = save_delta_snapshot(&delta)
                            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
                        let filename = format!("tally.snapshot.delta.{:010}", seq);
                        Ok((bytes, filename))
                    }
                }?;
                let file_path = snap_dir.join(&filename);
                let tmp_path = file_path.with_extension("tmp");
                std::fs::write(&tmp_path, &bytes)?;
                std::fs::rename(&tmp_path, &file_path)?;
                // After a successful base write, delete old snapshot files
                // whose sequence is strictly less than the current base's.
                if is_full {
                    cleanup_old_snapshots(&snap_dir, seq);
                }
                Ok::<usize, std::io::Error>(bytes.len())
            })
            .await;
            match result {
                Ok(Ok(size)) => {
                    let snap_elapsed = snap_start.elapsed();
                    {
                        let mut app = snap_state.lock().unwrap_or_else(|e| e.into_inner());
                        app.metrics.snapshot_duration_ms = snap_elapsed.as_millis() as u64;
                    }
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

    // Periodic eviction timer (PERS-05)
    let evict_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        interval.tick().await; // Skip first immediate tick
        loop {
            interval.tick().await;
            let now = std::time::SystemTime::now();
            let mut app = evict_state.lock().unwrap_or_else(|e| e.into_inner());
            let AppState {
                ref engine,
                ref mut store,
                ..
            } = *app;
            let evicted = evict_expired_keys(store, engine, now, ttl_multiplier);
            if evicted > 0 {
                eprintln!("Evicted {} expired keys", evicted);
            }
        }
    });

    // Periodic event log fsync timer (ELOG-04: 1-second interval, Redis everysec pattern)
    let fsync_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.tick().await; // Skip first immediate tick
        loop {
            interval.tick().await;
            let result = {
                let mut app = fsync_state.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(ref mut log) = app.event_log {
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

    // Periodic event log compaction timer (ELOG-05: 60-second interval)
    let compact_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        interval.tick().await; // Skip first immediate tick
        loop {
            interval.tick().await;
            let now = SystemTime::now();
            // Get list of streams to compact
            let streams_to_compact: Vec<String> = {
                let app = compact_state.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(ref log) = app.event_log {
                    log.registered_streams().map(String::from).collect()
                } else {
                    vec![]
                }
            };
            // Compact each stream (re-acquires lock per stream for cooperative yielding)
            for stream_name in &streams_to_compact {
                {
                    let mut app = compact_state.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(ref mut log) = app.event_log {
                        match log.compact_stream(stream_name, now) {
                            Ok(removed) if removed > 0 => {
                                eprintln!("Compacted {}: removed {} expired entries", stream_name, removed);
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

    tokio::select! {
        _ = tcp_handle => {},
        _ = http_handle => {},
    }
}

// ================ Phase 9: Incremental Snapshot Helpers ================

/// Remove snapshot files whose sequence is strictly less than the current
/// base's sequence. Runs after a successful base write. Silently ignores
/// filesystem errors (bounded-effort cleanup; snapshot correctness does not
/// depend on it).
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

/// Scan the snapshot directory and load the latest base + subsequent deltas
/// into a single `SnapshotState`. Falls back to the legacy v5 single-file
/// format at `legacy_path` if no v6 files are found. Returns the merged
/// state plus the next sequence number to use for future writes.
///
/// Returns `None` if nothing loadable exists.
pub(crate) fn load_incremental_snapshots(
    snap_dir: &Path,
    legacy_path: &Path,
) -> Option<(SnapshotState, u64)> {
    // Step 1: Scan directory for v6 snapshot files.
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

    // Step 2: Find latest base by sequence.
    bases.sort_by_key(|(seq, _)| *seq);

    if let Some((base_seq, base_path)) = bases.last().cloned() {
        // Load base bytes.
        let bytes = std::fs::read(&base_path).ok()?;
        let base = match load_snapshot_file(&bytes)? {
            SnapshotFile::Base(b) => b,
            // A file named "tally.snapshot.base.*" that decodes as a delta
            // is a corruption signal -- bail out and start fresh rather
            // than silently loading the wrong thing.
            _ => return None,
        };

        // Build a scratch store, restore base entities, then apply deltas in
        // monotonic sequence order.
        let mut store = StateStore::new();
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
                Err(_) => continue, // Skip unreadable files
            };
            match load_snapshot_file(&bytes) {
                Some(SnapshotFile::Delta(delta)) => {
                    store.apply_delta(delta.changed_entities, delta.deleted_keys);
                    if *seq > max_seq {
                        max_seq = *seq;
                    }
                }
                // Skip files that claim to be deltas but fail to decode,
                // or are mis-named bases (corruption-tolerant recovery).
                _ => continue,
            }
        }

        let state = SnapshotState {
            entities: store.clone_for_snapshot(),
            pipelines: base.pipelines,
            backfill_complete: base.backfill_complete,
        };
        return Some((state, max_seq + 1));
    }

    // Step 3: Fall back to legacy v5 single-file snapshot.
    if legacy_path.exists() {
        let bytes = std::fs::read(legacy_path).ok()?;
        let legacy = load_legacy_v5(&bytes)?;
        eprintln!("Loaded legacy v5 snapshot from {}", legacy_path.display());
        return Some((legacy, 1));
    }

    None
}
