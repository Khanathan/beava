//! HTTP management API: health, pipeline CRUD, metrics, debug, snapshot endpoints.
//!
//! Runs on a separate port (default 6401) from the TCP hot path.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use tokio::net::TcpListener;
use std::time::SystemTime;

use super::tcp::SharedState;
use crate::server::protocol::{convert_register_request, RegisterRequest};

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}

async fn list_pipelines(State(state): State<SharedState>) -> Json<serde_json::Value> {
    let app = state.lock().unwrap_or_else(|e| e.into_inner());
    let names: Vec<String> = app.engine.list_streams().map(|s| s.name.clone()).collect();
    Json(serde_json::json!({"pipelines": names}))
}

async fn get_pipeline(
    State(state): State<SharedState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let app = state.lock().unwrap_or_else(|e| e.into_inner());
    match app.engine.get_stream(&name) {
        Some(stream) => {
            let features: Vec<serde_json::Value> = stream
                .features
                .iter()
                .map(|(fname, def)| match def {
                    crate::engine::pipeline::FeatureDef::Count { window, bucket, .. } => {
                        serde_json::json!({"name": fname, "type": "count", "window_secs": window.as_secs(), "bucket_secs": bucket.as_secs()})
                    }
                    crate::engine::pipeline::FeatureDef::Sum {
                        field,
                        window,
                        bucket,
                        optional,
                        ..
                    } => {
                        serde_json::json!({"name": fname, "type": "sum", "field": field, "window_secs": window.as_secs(), "bucket_secs": bucket.as_secs(), "optional": optional})
                    }
                    crate::engine::pipeline::FeatureDef::Avg {
                        field,
                        window,
                        bucket,
                        optional,
                        ..
                    } => {
                        serde_json::json!({"name": fname, "type": "avg", "field": field, "window_secs": window.as_secs(), "bucket_secs": bucket.as_secs(), "optional": optional})
                    }
                    crate::engine::pipeline::FeatureDef::Min {
                        field,
                        window,
                        bucket,
                        optional,
                        ..
                    } => {
                        serde_json::json!({"name": fname, "type": "min", "field": field, "window_secs": window.as_secs(), "bucket_secs": bucket.as_secs(), "optional": optional})
                    }
                    crate::engine::pipeline::FeatureDef::Max {
                        field,
                        window,
                        bucket,
                        optional,
                        ..
                    } => {
                        serde_json::json!({"name": fname, "type": "max", "field": field, "window_secs": window.as_secs(), "bucket_secs": bucket.as_secs(), "optional": optional})
                    }
                    crate::engine::pipeline::FeatureDef::Last {
                        field,
                        optional,
                    } => {
                        serde_json::json!({"name": fname, "type": "last", "field": field, "optional": optional})
                    }
                    crate::engine::pipeline::FeatureDef::Derive { .. } => {
                        serde_json::json!({"name": fname, "type": "derive"})
                    }
                })
                .collect();
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "name": stream.name,
                    "key_field": stream.key_field,
                    "features": features,
                })),
            )
                .into_response()
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("pipeline '{}' not found", name)})),
        )
            .into_response(),
    }
}

async fn create_pipeline(
    State(state): State<SharedState>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let req: RegisterRequest = match serde_json::from_value(body.clone()) {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": format!("invalid request: {}", e)})),
            )
                .into_response()
        }
    };
    let stream_name = req.name.clone();
    let stream_def = match convert_register_request(req) {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": format!("{}", e)})),
            )
                .into_response()
        }
    };
    let mut app = state.lock().unwrap_or_else(|e| e.into_inner());
    match app.engine.register(stream_def) {
        Ok(()) => {
            // Store raw JSON for snapshot persistence (same as TCP REGISTER handler)
            app.engine.store_raw_register_json(&stream_name, body);
            (
                StatusCode::OK,
                Json(serde_json::json!({"status": "ok"})),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": format!("{}", e)})),
        )
            .into_response(),
    }
}

async fn delete_pipeline(
    State(state): State<SharedState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let mut app = state.lock().unwrap_or_else(|e| e.into_inner());
    if app.engine.remove_stream(&name) {
        (
            StatusCode::OK,
            Json(serde_json::json!({"status": "ok"})),
        )
            .into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("pipeline '{}' not found", name)})),
        )
            .into_response()
    }
}

async fn metrics_endpoint(State(state): State<SharedState>) -> impl IntoResponse {
    let app = state.lock().unwrap_or_else(|e| e.into_inner());
    let keys_total = app.store.entity_count();
    let events_total = app.metrics.events_total;
    let push_latency = app.metrics.push_latency_seconds;
    let snapshot_duration = app.metrics.snapshot_duration_ms as f64 / 1000.0;
    let memory_bytes = keys_total * 2048; // Rough estimate: ~2KB per entity with operators

    let body = format!(
        "# HELP tally_keys_total Number of entity keys in memory\n\
         # TYPE tally_keys_total gauge\n\
         tally_keys_total {}\n\
         # HELP tally_events_total Total events processed\n\
         # TYPE tally_events_total counter\n\
         tally_events_total {}\n\
         # HELP tally_push_latency_seconds Last observed PUSH latency\n\
         # TYPE tally_push_latency_seconds gauge\n\
         tally_push_latency_seconds {}\n\
         # HELP tally_snapshot_duration_seconds Last snapshot write duration\n\
         # TYPE tally_snapshot_duration_seconds gauge\n\
         tally_snapshot_duration_seconds {}\n\
         # HELP tally_memory_bytes Estimated memory usage\n\
         # TYPE tally_memory_bytes gauge\n\
         tally_memory_bytes {}\n",
        keys_total, events_total, push_latency, snapshot_duration, memory_bytes,
    );
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        body,
    )
}

async fn debug_key(
    State(state): State<SharedState>,
    Path(key): Path<String>,
) -> impl IntoResponse {
    let mut app = state.lock().unwrap_or_else(|e| e.into_inner());
    let now = SystemTime::now();
    // First check if entity exists
    let entity_exists = app.store.get_entity(&key).is_some();
    if !entity_exists {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("key '{}' not found", key)})),
        )
            .into_response();
    }
    // Collect debug info from entity (immutable borrow)
    let (live_ops, static_feats, last_event_at) = {
        let entity = app.store.get_entity(&key).unwrap();
        let live_ops: Vec<serde_json::Value> = entity
            .live_operators
            .iter()
            .map(|(name, op)| {
                serde_json::json!({
                    "name": name,
                    "state": format!("{:?}", op),
                })
            })
            .collect();
        let static_feats: serde_json::Map<String, serde_json::Value> = entity
            .static_features
            .iter()
            .map(|(k, v)| (k.clone(), v.value.to_json_value()))
            .collect();
        let last_event_at = entity.last_event_at.map(|t| {
            t.duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        });
        (live_ops, static_feats, last_event_at)
    };
    // Now get computed features (mutable borrow for window advancement)
    let features = app.store.get_all_features(&key, now);
    let feature_json: serde_json::Map<String, serde_json::Value> = features
        .iter()
        .map(|(k, v)| (k.clone(), v.to_json_value()))
        .collect();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "key": key,
            "live_operators": live_ops,
            "static_features": static_feats,
            "computed_features": feature_json,
            "last_event_at": last_event_at,
        })),
    )
        .into_response()
}

async fn debug_memory(State(state): State<SharedState>) -> Json<serde_json::Value> {
    let app = state.lock().unwrap_or_else(|e| e.into_inner());
    Json(serde_json::json!({
        "entity_count": app.store.entity_count(),
        "stream_count": app.engine.stream_count(),
        "estimated_bytes": app.store.entity_count() * 2048,
    }))
}

async fn trigger_snapshot(State(state): State<SharedState>) -> impl IntoResponse {
    let snapshot_data = {
        let app = state.lock().unwrap_or_else(|e| e.into_inner());
        let entities = app.store.clone_for_snapshot();
        // Populate pipelines from engine -- same pattern as periodic snapshot timer in Plan 02
        let pipelines: Vec<crate::state::snapshot::SerializablePipeline> = app
            .engine
            .list_streams()
            .filter_map(|stream| {
                app.engine.get_raw_register_json(&stream.name).map(|json| {
                    crate::state::snapshot::SerializablePipeline {
                        name: stream.name.clone(),
                        key_field: stream.key_field.clone(),
                        raw_register_json: serde_json::to_string(json).unwrap_or_default(),
                    }
                })
            })
            .collect();
        crate::state::snapshot::SnapshotState {
            entities,
            pipelines,
        }
    };
    let path = {
        let app = state.lock().unwrap_or_else(|e| e.into_inner());
        app.snapshot_path.clone()
    };
    // Capture start time for snapshot_duration_ms metric
    let snap_start = std::time::Instant::now();
    let result = tokio::task::spawn_blocking(move || {
        let bytes = crate::state::snapshot::save_snapshot(&snapshot_data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let tmp_path = path.with_extension("tmp");
        std::fs::write(&tmp_path, &bytes)?;
        std::fs::rename(&tmp_path, &path)?;
        Ok::<usize, std::io::Error>(bytes.len())
    })
    .await;
    match result {
        Ok(Ok(size)) => {
            // Write snapshot duration metric so GET /metrics reports non-zero tally_snapshot_duration_seconds
            let snap_elapsed = snap_start.elapsed();
            {
                let mut app = state.lock().unwrap_or_else(|e| e.into_inner());
                app.metrics.snapshot_duration_ms = snap_elapsed.as_millis() as u64;
            }
            (
                StatusCode::OK,
                Json(serde_json::json!({"status": "ok", "bytes": size, "duration_ms": snap_elapsed.as_millis() as u64})),
            )
                .into_response()
        }
        Ok(Err(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": format!("{}", e)})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": format!("{}", e)})),
        )
            .into_response(),
    }
}

pub fn build_router(state: SharedState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/pipelines", get(list_pipelines).post(create_pipeline))
        .route(
            "/pipelines/{name}",
            get(get_pipeline).delete(delete_pipeline),
        )
        .route("/metrics", get(metrics_endpoint))
        .route("/debug/key/{key}", get(debug_key))
        .route("/debug/memory", get(debug_memory))
        .route("/snapshot", post(trigger_snapshot))
        .with_state(state)
}

/// Start the HTTP management server on the given address.
pub async fn run_http_server(addr: &str, state: SharedState) -> Result<(), std::io::Error> {
    let app = build_router(state);
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
}

/// Start the HTTP management server from a pre-bound listener (for tests).
pub async fn run_http_server_with_listener(
    listener: TcpListener,
    state: SharedState,
) -> Result<(), std::io::Error> {
    let app = build_router(state);
    axum::serve(listener, app)
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
}
