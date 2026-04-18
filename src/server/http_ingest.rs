//! HTTP ingest + read endpoints (Phase 45).
//!
//! Handlers here are thin wrappers over `crate::server::tcp::handle_push_core_ex`
//! and `crate::server::tcp::handle_push_batch`. Do not duplicate ingest logic.

use std::time::Duration;

use axum::{
    extract::{DefaultBodyLimit, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use axum_extra::json_lines::JsonLines;
use serde::Deserialize;
use serde_json::json;
use tokio_stream::StreamExt as _;
use tower::ServiceBuilder;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;

use crate::server::tcp::SharedState;

const DEFAULT_MAX_BODY_BYTES: usize = 16 * 1024 * 1024; // D-05: 16 MiB
const DEFAULT_TIMEOUT_SECS: u64 = 30;

fn max_body_bytes() -> usize {
    std::env::var("BEAVA_HTTP_MAX_BODY")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_BODY_BYTES)
}

/// Register the six Phase 45 ingest + read routes onto the provided admin and
/// public routers. Returns the updated (public, admin) pair. Caller must attach
/// `require_loopback_or_token` to the admin router AFTER this fn returns
/// (matches `http.rs:1565-1568` pattern).
///
/// `public_mode = true` mounts /features/* and /streams[/*] on the public
/// router (per D-03 / HTTP-07). When false, reads stay admin-only.
///
/// # Middleware ordering (Pitfall 15)
///
/// `DefaultBodyLimit::disable()` MUST precede `RequestBodyLimitLayer::new`
/// otherwise axum's per-extractor 2 MiB cap silently applies first. The
/// `ServiceBuilder` stack is attached to write routes via `.layer(ingest_layers)`
/// BEFORE `.route_layer(require_loopback_or_token)` is applied by the caller,
/// preventing any auth-bypass on future route additions.
pub fn register_ingest_routes(
    public_router: Router<SharedState>,
    admin_router: Router<SharedState>,
    public_mode: bool,
) -> (Router<SharedState>, Router<SharedState>) {
    // Build the body-limit + timeout layer stack. Per Phase-level Pitfall A
    // `DefaultBodyLimit::disable()` MUST precede `RequestBodyLimitLayer::new`
    // otherwise axum's per-extractor 2 MiB cap silently applies.
    let ingest_layers = ServiceBuilder::new()
        .layer(DefaultBodyLimit::disable())
        .layer(RequestBodyLimitLayer::new(max_body_bytes()))
        // tower-http 0.6: TimeoutLayer::new is deprecated; use with_status_code
        // so the response is a proper 408 Request Timeout instead of 500.
        // Note: with_status_code takes (status_code, duration) order.
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(DEFAULT_TIMEOUT_SECS),
        ));

    // Writes: always admin-mounted.
    let admin_router = admin_router
        .route("/push/{stream}", post(http_push_single))
        .route("/push-batch/{stream}", post(http_push_batch))
        .route("/push/{stream}/ndjson", post(http_push_ndjson))
        .layer(ingest_layers);

    // Reads: public if public_mode, else admin.
    let read_routes: Router<SharedState> = Router::new()
        .route("/features/{key}", get(http_get_features))
        .route("/streams", get(http_list_streams))
        .route("/streams/{name}", get(http_get_stream));

    if public_mode {
        (public_router.merge(read_routes), admin_router)
    } else {
        (public_router, admin_router.merge(read_routes))
    }
}

// -------- Query param structs --------

#[derive(Debug, Deserialize)]
pub(crate) struct SyncQuery {
    #[serde(default)]
    pub sync: Option<u8>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub(crate) struct TableQuery {
    pub table: Option<String>,
}

// -------- Error mapping helper --------

fn map_err_to_response(e: crate::error::BeavaError) -> axum::response::Response {
    use crate::error::BeavaError;
    // BeavaError variants on HEAD: Parse, Type, Window, Expression, Protocol, NotImplemented.
    // Stream-not-found surfaces as BeavaError::Protocol("unknown stream: {name}").
    // Map that variant to a structured 400 envelope clients can detect by code.
    match e {
        BeavaError::Protocol(ref msg) if msg.contains("unknown stream") => (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "ok": false,
                "error": { "code": "stream_not_registered", "message": format!("{e}") }
            })),
        )
            .into_response(),
        _ => (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "ok": false,
                "error": { "code": "schema_error", "message": format!("{e}") }
            })),
        )
            .into_response(),
    }
}

// -------- Write handlers (Wave 1 — 45-03) --------

/// `POST /push/{stream}` — single-event ingest (HTTP-01).
///
/// Body: a single JSON object (arbitrary fields + optional `_event_time`).
///
/// Query:
/// - `?sync=1` — in-memory drain via `read_features=true` (orchestrator A7).
///   Durable fsync deferred to Phase 46.
///
/// Response (200): `{"ok": true}`
/// Response (400): `{"ok": false, "error": {"code": "schema_error"|"stream_not_registered", ...}}`
/// Response (413): returned by `RequestBodyLimitLayer` before handler runs.
async fn http_push_single(
    State(state): State<SharedState>,
    Path(stream): Path<String>,
    Query(q): Query<SyncQuery>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    use std::time::SystemTime;

    // Parse from raw Bytes so schema errors produce our D-11 structured envelope
    // instead of axum's default plain-text 400.
    let payload: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "ok": false,
                    "error": { "code": "schema_error", "message": format!("invalid JSON body: {e}") }
                })),
            )
                .into_response();
        }
    };

    // D-17: `?sync=1` → in-memory drain fast-path (A7).
    let read_features = matches!(q.sync, Some(1));
    let now = SystemTime::now();

    // TPC-INFRA-01 (Wave 0): shard hint computed at HTTP ingest entry point.
    // At N=1: always 0. Discarded — routing wired in Wave 1 (Phase 49).
    {
        let engine_guard = state.engine.read();
        let key_field_ref = engine_guard
            .get_stream(&stream)
            .and_then(|s| s.key_field.as_deref());
        let _shard_hint: u32 =
            crate::routing::shard_hint_for_event(&payload, key_field_ref);
    }

    match crate::server::tcp::handle_push_core_ex(
        &state,
        &stream,
        &payload,
        &body,
        now,
        read_features,
    ) {
        Ok(_fm) => {
            // Phase 45-04 A5: HTTP single-event path — bump labeled counter.
            // events_total is already bumped inside handle_push_core_ex.
            state
                .events_http
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            (StatusCode::OK, Json(json!({"ok": true}))).into_response()
        }
        Err(e) => map_err_to_response(e),
    }
}

/// `POST /push-batch/{stream}` — JSON-array batch ingest (HTTP-02).
///
/// Body: a JSON array of event objects. Max size enforced by 16 MiB body limit.
///
/// Per-event `_event_time` is captured individually and stored in
/// `PendingAsync.now`. `handle_push_batch` reads `batch[i].now` at
/// `tcp.rs:1715` for per-event late-drop gating and watermark advance.
///
/// **Phase 46 handoff:** When Phase 46 flips `push_batch_with_cascade_no_features`
/// to accept `&[(&Value, SystemTime)]` directly, the wrapping in PendingAsync
/// becomes the direct pass-through — zero HTTP-handler changes needed.
///
/// Response (D-12 summary-only):
/// `{"ok": true, "data": {"accepted": N, "rejected": M, "first_error": null|{...}}}`
async fn http_push_batch(
    State(state): State<SharedState>,
    Path(stream): Path<String>,
    Query(_q): Query<SyncQuery>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    use std::time::SystemTime;

    let events: Vec<serde_json::Value> = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "ok": false,
                    "error": { "code": "schema_error", "message": format!("invalid JSON array: {e}") }
                })),
            )
                .into_response();
        }
    };

    if events.is_empty() {
        return (
            StatusCode::OK,
            Json(json!({
                "ok": true,
                "data": { "accepted": 0, "rejected": 0, "first_error": null }
            })),
        )
            .into_response();
    }

    let wall = SystemTime::now();
    let mut batch: Vec<crate::server::tcp::PendingAsync> = Vec::with_capacity(events.len());
    for (seq, payload) in events.into_iter().enumerate() {
        // Per-event event-time capture (Gap 8 / Phase 46 handoff):
        // handle_push_batch picks up PendingAsync.now per-event at tcp.rs:1715
        // for watermark gating. Phase 46 flips engine primitive — no change here.
        let et = crate::engine::event_time::parse_event_time(&payload, wall);
        let raw = serde_json::to_vec(&payload).unwrap_or_default();
        batch.push(crate::server::tcp::PendingAsync {
            seq: seq as u64,
            stream_name: stream.clone(),
            payload,
            raw_payload: raw,
            now: et,
        });
    }

    // TPC-INFRA-01 (Wave 0): shard hint computed per event at HTTP batch entry point.
    // At N=1: always 0. Discarded — routing wired in Wave 1 (Phase 49).
    {
        let engine_guard = state.engine.read();
        for pending in &batch {
            let key_field_ref = engine_guard
                .get_stream(&pending.stream_name)
                .and_then(|s| s.key_field.as_deref());
            let _shard_hint: u32 =
                crate::routing::shard_hint_for_event(&pending.payload, key_field_ref);
        }
    }

    let results = crate::server::tcp::handle_push_batch(&state, &batch);
    let mut accepted = 0usize;
    let mut rejected = 0usize;
    let mut first_error: Option<serde_json::Value> = None;
    for r in results {
        match r {
            Ok(()) => accepted += 1,
            Err(e) => {
                rejected += 1;
                if first_error.is_none() {
                    first_error = Some(json!({"code": "schema_error", "message": format!("{e}")}));
                }
            }
        }
    }

    // Phase 45-04 A5: HTTP batch path — bump labeled counter.
    // events_total is already bumped inside handle_push_batch.
    if accepted > 0 {
        state
            .events_http
            .fetch_add(accepted as u64, std::sync::atomic::Ordering::Relaxed);
    }

    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "data": {
                "accepted": accepted,
                "rejected": rejected,
                "first_error": first_error,
            }
        })),
    )
        .into_response()
}

/// `POST /push/{stream}/ndjson` — NDJSON streaming ingest (HTTP-03, D-06).
///
/// Uses `axum_extra::json_lines::JsonLines<serde_json::Value>` — line-by-line
/// parse, no full-array allocation (Pitfall 7 mitigation).
///
/// Events are flushed to `handle_push_batch` in chunks of 1000 to bound
/// memory for large backfills.
///
/// Malformed lines are counted as `rejected`; stream is NOT aborted.
///
/// Response (D-13 summary-only):
/// `{"ok": true, "data": {"accepted": N, "rejected": M, "chunks": C, "first_error": null|{...}}}`
async fn http_push_ndjson(
    State(state): State<SharedState>,
    Path(stream): Path<String>,
    mut stream_body: JsonLines<serde_json::Value>,
) -> impl IntoResponse {
    use std::time::SystemTime;

    const CHUNK_SIZE: usize = 1000;

    let wall = SystemTime::now();
    let mut batch: Vec<crate::server::tcp::PendingAsync> = Vec::with_capacity(CHUNK_SIZE);
    let mut accepted = 0usize;
    let mut rejected = 0usize;
    let mut chunks = 0usize;
    let mut first_error: Option<serde_json::Value> = None;
    let mut seq_counter: u64 = 0;

    // Flush helper: drain batch into handle_push_batch and accumulate stats.
    macro_rules! flush_batch {
        () => {
            if !batch.is_empty() {
                chunks += 1;
                let results = crate::server::tcp::handle_push_batch(&state, &batch);
                for r in results {
                    match r {
                        Ok(()) => accepted += 1,
                        Err(e) => {
                            rejected += 1;
                            if first_error.is_none() {
                                first_error = Some(
                                    json!({"code": "schema_error", "message": format!("{e}")}),
                                );
                            }
                        }
                    }
                }
                batch.clear();
            }
        };
    }

    while let Some(line) = stream_body.next().await {
        match line {
            Ok(payload) => {
                let et = crate::engine::event_time::parse_event_time(&payload, wall);
                let raw = serde_json::to_vec(&payload).unwrap_or_default();
                batch.push(crate::server::tcp::PendingAsync {
                    seq: seq_counter,
                    stream_name: stream.clone(),
                    payload,
                    raw_payload: raw,
                    now: et,
                });
                seq_counter += 1;
                if batch.len() >= CHUNK_SIZE {
                    flush_batch!();
                }
            }
            Err(e) => {
                rejected += 1;
                if first_error.is_none() {
                    first_error = Some(
                        json!({"code": "schema_error", "message": format!("ndjson line parse: {e}")}),
                    );
                }
            }
        }
    }
    // Flush final partial chunk.
    flush_batch!();

    // Phase 45-04 A5: HTTP ndjson path — bump labeled counter.
    // events_total is already bumped inside handle_push_batch per chunk.
    if accepted > 0 {
        state
            .events_http
            .fetch_add(accepted as u64, std::sync::atomic::Ordering::Relaxed);
    }

    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "data": {
                "accepted": accepted,
                "rejected": rejected,
                "chunks": chunks,
                "first_error": first_error,
            }
        })),
    )
        .into_response()
}

async fn http_get_features(
    State(state): State<SharedState>,
    Path(key): Path<String>,
    Query(q): Query<TableQuery>,
) -> impl IntoResponse {
    use std::time::SystemTime;
    let now = SystemTime::now();

    // Fast existence check first — avoids computing features for unknown keys.
    if state.store.get_entity(&key).is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "ok": false,
                "error": { "code": "key_not_found", "message": format!("no entity for key {key}") }
            })),
        )
            .into_response();
    }

    // Walk all features for this key. The `FeatureMap` uses flat keys; features
    // registered with stream-prefixed names ("stream.feature") are grouped by the
    // prefix. Features without a dot land under a table whose key equals the full
    // feature name.
    let features = state.store.get_all_features(&key, now);

    let mut tables: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
    for (fq_name, val) in features.iter() {
        let (table, feat) = fq_name.split_once('.').unwrap_or((fq_name.as_str(), ""));
        if let Some(ref filter) = q.table {
            if table != filter {
                continue;
            }
        }
        let entry = tables
            .entry(table.to_string())
            .or_insert_with(|| serde_json::Value::Object(Default::default()));
        if let serde_json::Value::Object(ref mut m) = entry {
            m.insert(feat.to_string(), val.to_json_value());
        }
    }

    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "data": { "key": key, "tables": tables }
        })),
    )
        .into_response()
}

async fn http_list_streams(State(state): State<SharedState>) -> impl IntoResponse {
    use std::time::UNIX_EPOCH;
    let engine = state.engine.read();
    let mut streams: Vec<serde_json::Value> = Vec::new();
    for def in engine.list_streams() {
        let wm = engine.watermarks.watermark(&def.name);
        let wm_ms: Option<u64> = wm
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as u64);
        streams.push(json!({
            "name": def.name,
            "watermark_ms": wm_ms,
        }));
    }
    (
        StatusCode::OK,
        Json(json!({ "ok": true, "data": { "streams": streams } })),
    )
        .into_response()
}

async fn http_get_stream(
    State(state): State<SharedState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    use std::time::UNIX_EPOCH;
    let engine = state.engine.read();
    let def = match engine.get_stream(&name) {
        Some(d) => d,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({
                    "ok": false,
                    "error": {
                        "code": "stream_not_found",
                        "message": format!("stream {name} not registered")
                    }
                })),
            )
                .into_response();
        }
    };
    let wm = engine.watermarks.watermark(&name);
    let wm_ms: Option<u64> = wm
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as u64);
    // feature type: use Debug repr of FeatureDef variant — polished in Phase 47.
    let features: Vec<serde_json::Value> = def
        .features
        .iter()
        .map(|(fname, fdef)| {
            json!({
                "name": fname,
                "type": format!("{:?}", fdef),
            })
        })
        .collect();
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "data": {
                "name": def.name,
                "watermark_ms": wm_ms,
                "features": features,
            }
        })),
    )
        .into_response()
}
