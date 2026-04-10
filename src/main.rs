use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicUsize;
use std::time::{Duration, SystemTime};

use tally::engine::pipeline::PipelineEngine;
use tally::server::http::run_http_server;
use tally::server::protocol::{RegisterRequest, convert_register_request, convert_view_register_request};
use tally::server::tcp::{AppState, BackfillStatus, BackfillTracker, Metrics, run_backfill, run_tcp_server};
use tally::state::event_log::EventLog;
use tally::state::eviction::evict_expired_keys;
use tally::state::snapshot::{SerializablePipeline, SnapshotState, load_snapshot, save_snapshot};
use tally::state::store::StateStore;

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
    }));

    // Load snapshot on startup (PERS-03)
    if snapshot_path.exists() {
        match std::fs::read(&snapshot_path) {
            Ok(bytes) => match load_snapshot(&bytes) {
                Some(snapshot_state) => {
                    let mut app = state.lock().unwrap_or_else(|e| e.into_inner());
                    // Restore entity state
                    app.store.restore_from_snapshot(snapshot_state.entities);
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

                    eprintln!("Loaded snapshot from {}", snapshot_path.display());

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
                None => {
                    eprintln!("Snapshot incompatible or corrupt, starting fresh");
                }
            },
            Err(e) => {
                eprintln!("Failed to read snapshot file: {}", e);
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

    // Periodic snapshot timer (PERS-01, PERS-04)
    let snap_state = state.clone();
    let snap_path = snapshot_path.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        interval.tick().await; // First tick completes immediately -- skip it
        loop {
            interval.tick().await;
            // Clone state under lock (brief hold)
            let snapshot_data = {
                let app = snap_state.lock().unwrap_or_else(|e| e.into_inner());
                let valid_features = app.engine.valid_features_map();
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
                // Also include view definitions in the snapshot
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
                let backfill_complete: Vec<(String, String)> = app.backfill_complete.iter().cloned().collect();
                SnapshotState {
                    entities,
                    pipelines,
                    backfill_complete,
                }
            };
            // Serialize on blocking thread pool (PERS-04: does not block event loop)
            // Capture start time for snapshot_duration_ms metric (Plan 03 wires this)
            let snap_start = std::time::Instant::now();
            let path = snap_path.clone();
            let result = tokio::task::spawn_blocking(move || {
                let bytes = save_snapshot(&snapshot_data)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
                let tmp_path = path.with_extension("tmp");
                std::fs::write(&tmp_path, &bytes)?;
                std::fs::rename(&tmp_path, &path)?;
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
                    eprintln!("Snapshot saved ({} bytes, {}ms)", size, snap_elapsed.as_millis());
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
