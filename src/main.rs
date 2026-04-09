use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tally::engine::pipeline::PipelineEngine;
use tally::server::http::run_http_server;
use tally::server::protocol::{RegisterRequest, convert_register_request};
use tally::server::tcp::{AppState, Metrics, run_tcp_server};
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

    let state = Arc::new(Mutex::new(AppState {
        engine: PipelineEngine::new(),
        store: StateStore::new(),
        metrics: Metrics::default(),
        snapshot_path: snapshot_path.clone(),
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
                                let stream_name = req.name.clone();
                                if let Ok(stream_def) = convert_register_request(req) {
                                    let _ = app.engine.register(stream_def);
                                    app.engine.store_raw_register_json(&stream_name, json_val);
                                }
                            }
                        }
                    }
                    eprintln!("Loaded snapshot from {}", snapshot_path.display());
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
                let entities = app.store.clone_for_snapshot();
                let pipelines: Vec<SerializablePipeline> = app
                    .engine
                    .list_streams()
                    .filter_map(|stream| {
                        app.engine
                            .get_raw_register_json(&stream.name)
                            .map(|json| SerializablePipeline {
                                name: stream.name.clone(),
                                key_field: stream.key_field.clone(),
                                raw_register_json: serde_json::to_string(json)
                                    .unwrap_or_default(),
                            })
                    })
                    .collect();
                SnapshotState {
                    entities,
                    pipelines,
                }
            };
            // Serialize on blocking thread pool (PERS-04: does not block event loop)
            // Capture start time for snapshot_duration_ms metric (Plan 03 wires this)
            let snap_start = std::time::Instant::now();
            let path = snap_path.clone();
            let result = tokio::task::spawn_blocking(move || {
                let bytes = save_snapshot(&snapshot_data);
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

    tokio::select! {
        _ = tcp_handle => {},
        _ = http_handle => {},
    }
}
