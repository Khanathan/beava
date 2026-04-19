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
        // Phase 50-06 (D-10, TPC-CORR-03): tuple shard_key field missing → 400.
        BeavaError::ShardKeyMissing { ref missing } => (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "ok": false,
                "error": {
                    "code": "shard_key_missing",
                    "missing": missing,
                    "message": format!("{e}"),
                }
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
        None, // no per-connection intern cache on the HTTP ingest path
    ) {
        Ok(_fm) => {
            // Phase 45-04 A5: HTTP single-event path — bump labeled counter.
            // events_total is already bumped inside handle_push_core_ex.
            state
                .events_http
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // Phase 50-04: record_shard_event now called inside handle_push_core_ex
            // with the real shard_index — no duplicate call needed here.
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
    use std::sync::atomic::Ordering;
    use std::time::UNIX_EPOCH;

    // Phase 51-02: increment cross-shard fanout counter (TPC-PERF-05).
    crate::server::http::CROSS_SHARD_FANOUT_LIST_STREAMS.fetch_add(1, Ordering::Relaxed);

    // Phase 51-02: acquire global watermark read lock once for the whole listing.
    // Uncontended on hot path — publish/global_min only touch the AtomicU64 array.
    let gw = state.global_watermark.read();

    let engine = state.engine.read();

    // Use scatter_gather to fan out to all N shards (Wave 1: N=1, synchronous).
    // collect stream names via engine (all shards hold identical StreamDefinition).
    let stream_names: Vec<String> = engine.list_streams().map(|s| s.name.clone()).collect();
    // Phase 53-03 (D-03): shard count under state-inmem comes from the
    // legacy `sharded_store` field; under default (fjall) read BEAVA_SHARDS
    // directly (Plan 03B will route via `ConcurrentAppState.shard_partitions`).
    #[cfg(feature = "state-inmem")]
    let n_shards = {
        let ss = state.sharded_store.lock().expect("sharded_store mutex poisoned");
        crate::shard::traits::ShardedStateStore::shard_count(&*ss) as usize
    };
    #[cfg(not(feature = "state-inmem"))]
    let n_shards = crate::state::store::read_beava_shards() as usize;
    // scatter_gather deduplicates names across N shards (all identical at Wave 1).
    let merged_names = crate::routing::scatter::scatter_gather(
        n_shards,
        |_shard_id| stream_names.clone(),
        crate::routing::scatter::merge_stream_lists,
    );

    let mut streams: Vec<serde_json::Value> = Vec::new();
    for name in &merged_names {
        // Shard-local legacy watermark (ms resolution, lateness-adjusted).
        let wm = engine.wm_watermark(name);
        let wm_ms: Option<u64> = wm
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as u64);
        // Global watermark: min across all shard atomics (ns, no lateness).
        let global_wm_ns: Option<u64> = gw.global_min(name);
        streams.push(json!({
            "name": name,
            "watermark_ms": wm_ms,
            "watermark_ns": global_wm_ns,
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
    let wm = engine.wm_watermark(&name);
    let wm_ms: Option<u64> = wm
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as u64);
    // Phase 51-02: global watermark min from GlobalWatermarkStore (all shards, atomic reads).
    // Read lock is uncontended on the hot path (publish/global_min use AtomicU64 internally).
    let global_wm_ns: Option<u64> = state.global_watermark.read().global_min(&name);
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
                "watermark_ns": global_wm_ns,
                "features": features,
            }
        })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::pipeline::PipelineEngine;
    use crate::server::tcp::{make_concurrent_state_full, BackfillTracker};
    use crate::shard::global_watermark::GlobalWatermarkStore;
    use crate::state::store::StateStore;
    use axum::body::Body;
    use axum::http::Request;
    use std::sync::Arc;
    use tower::ServiceExt;

    fn test_state() -> crate::server::tcp::SharedState {
        make_concurrent_state_full(
            PipelineEngine::new(),
            StateStore::new(),
            None,
            std::path::PathBuf::from("/tmp/beava-test-http-ingest"),
            Arc::new(BackfillTracker::default()),
            false,
            false,
            None,
            false,
            1,
        )
    }

    // -----------------------------------------------------------------------
    // Test 1: ConcurrentAppState exposes a global_watermark field.
    //
    // TDD RED: ConcurrentAppState does not yet have global_watermark → compile error.
    // GREEN: field is parking_lot::RwLock<GlobalWatermarkStore>, readable via .read().
    // -----------------------------------------------------------------------
    #[test]
    fn test_global_watermark_field_on_state() {
        let state = test_state();
        // global_watermark field must exist and be readable
        let store = state.global_watermark.read();
        // global_min for an unknown stream returns None (not panic)
        assert_eq!(store.global_min("nonexistent"), None);
    }

    // -----------------------------------------------------------------------
    // Test 2: global_min returns min across 3 shards (integration invariant).
    // N=3 shards publish watermarks 10/20/30 → global_min == 10.
    // -----------------------------------------------------------------------
    #[test]
    fn test_global_watermark_min_across_3_shards() {
        let mut store = GlobalWatermarkStore::new(3, 16);
        store.register_stream("txn");

        store.publish(0, "txn", 10);
        store.publish(1, "txn", 20);
        store.publish(2, "txn", 30);

        assert_eq!(
            store.global_min("txn"),
            Some(10),
            "global min must be the minimum across all shards"
        );
    }

    // -----------------------------------------------------------------------
    // Test 3: GET /streams/{name} returns watermark_ns from GlobalWatermarkStore.
    //
    // TDD RED: handler does not yet read from state.global_watermark.
    // GREEN: handler calls state.global_watermark.read().global_min(&name).
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn test_get_stream_returns_watermark_ns() {
        use axum::Router;

        let state = test_state();
        // Register stream in engine using StreamDefinition directly.
        {
            let mut engine = state.engine.write();
            let _ = engine.register(crate::engine::pipeline::StreamDefinition {
                name: "orders".to_string(),
                key_field: Some("id".to_string()),
                group_by_keys: None,
                features: vec![],
                depends_on: None,
                filter: None,
                entity_ttl: None,
                history_ttl: None,
                projection: None,
                ephemeral: None,
                pipeline_ttl: None,
                max_keys: None,
                watermark_lateness: None,
                shard_key: None,
            });
        }

        // Register stream in global watermark store and publish a value.
        {
            state.global_watermark.write().register_stream("orders");
            state.global_watermark.read().publish(0, "orders", 12345_u64);
        }

        let (pub_r, admin_r) = register_ingest_routes(Router::new(), Router::new(), false);
        let app = pub_r.merge(admin_r).with_state(state);

        let req = Request::builder()
            .method("GET")
            .uri("/streams/orders")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        // watermark_ns must be present and equal the published value
        assert_eq!(
            json["data"]["watermark_ns"],
            serde_json::Value::Number(serde_json::Number::from(12345u64)),
            "watermark_ns must come from global watermark store global_min"
        );
    }

    // -----------------------------------------------------------------------
    // Test 4: publish_if_due integrates with GlobalWatermarkStore.
    // After threshold events, shard publishes to global store.
    // -----------------------------------------------------------------------
    #[test]
    fn test_publish_if_due_integrates_with_global_store() {
        use crate::shard::watermark::WatermarkState;
        use std::time::{Duration, UNIX_EPOCH};

        let mut store = GlobalWatermarkStore::new(1, 16);
        store.register_stream("events");

        let mut wm = WatermarkState::new();
        let t = UNIX_EPOCH + Duration::from_nanos(999_000);
        wm.observe("events", t);

        // 1023 events — no publish yet
        for _ in 0..1023 {
            wm.publish_if_due("events", &store, 0, 1024);
        }
        assert_eq!(store.global_min("events"), None, "no publish before threshold");

        // 1024th event — publish fires
        let result = wm.publish_if_due("events", &store, 0, 1024);
        assert!(result.is_some(), "publish must occur on threshold crossing");
        assert_eq!(
            store.global_min("events"),
            Some(999_000),
            "global min must match the published shard watermark"
        );
    }
}
