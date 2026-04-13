//! HTTP management API: health, pipeline CRUD, metrics, debug, snapshot endpoints.
//!
//! Runs on a separate port (default 6401) from the TCP hot path.
//! Phase 14: All handlers use individual field locks from ConcurrentAppState
//! instead of a single global Mutex<AppState>.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use std::sync::atomic::Ordering;
use std::time::SystemTime;
use tokio::net::TcpListener;

use super::tcp::SharedState;
use crate::server::protocol::{convert_register_request, RegisterRequest};
use crate::server::ui::{ui_index, ui_static};

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}

async fn list_pipelines(State(state): State<SharedState>) -> Json<serde_json::Value> {
    let engine = state.engine.read();
    let names: Vec<String> = engine.list_streams().map(|s| s.name.clone()).collect();
    Json(serde_json::json!({"pipelines": names}))
}

async fn get_pipeline(
    State(state): State<SharedState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let engine = state.engine.read();
    match engine.get_stream(&name) {
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
                        ..
                    } => {
                        serde_json::json!({"name": fname, "type": "last", "field": field, "optional": optional})
                    }
                    crate::engine::pipeline::FeatureDef::DistinctCount {
                        field,
                        window,
                        bucket,
                        optional,
                        ..
                    } => {
                        serde_json::json!({"name": fname, "type": "distinct_count", "field": field, "window_secs": window.as_secs(), "bucket_secs": bucket.as_secs(), "optional": optional})
                    }
                    crate::engine::pipeline::FeatureDef::Stddev {
                        field,
                        window,
                        bucket,
                        optional,
                        ..
                    } => {
                        serde_json::json!({"name": fname, "type": "stddev", "field": field, "window_secs": window.as_secs(), "bucket_secs": bucket.as_secs(), "optional": optional})
                    }
                    crate::engine::pipeline::FeatureDef::Percentile {
                        field,
                        quantile,
                        window,
                        bucket,
                        optional,
                        ..
                    } => {
                        serde_json::json!({"name": fname, "type": "percentile", "field": field, "quantile": quantile, "window_secs": window.as_secs(), "bucket_secs": bucket.as_secs(), "optional": optional})
                    }
                    crate::engine::pipeline::FeatureDef::Derive { .. } => {
                        serde_json::json!({"name": fname, "type": "derive"})
                    }
                    crate::engine::pipeline::FeatureDef::Lag { field, n, optional, .. } => {
                        serde_json::json!({"name": fname, "type": "lag", "field": field, "n": n, "optional": optional})
                    }
                    crate::engine::pipeline::FeatureDef::Ema { field, half_life_secs, optional, .. } => {
                        serde_json::json!({"name": fname, "type": "ema", "field": field, "half_life_secs": half_life_secs, "optional": optional})
                    }
                    crate::engine::pipeline::FeatureDef::LastN { field, n, optional, .. } => {
                        serde_json::json!({"name": fname, "type": "last_n", "field": field, "n": n, "optional": optional})
                    }
                    crate::engine::pipeline::FeatureDef::First { field, optional, .. } => {
                        serde_json::json!({"name": fname, "type": "first", "field": field, "optional": optional})
                    }
                    crate::engine::pipeline::FeatureDef::ExactMin { field, window, bucket, optional, .. } => {
                        serde_json::json!({"name": fname, "type": "exact_min", "field": field, "window_secs": window.as_secs(), "bucket_secs": bucket.as_secs(), "optional": optional})
                    }
                    crate::engine::pipeline::FeatureDef::ExactMax { field, window, bucket, optional, .. } => {
                        serde_json::json!({"name": fname, "type": "exact_max", "field": field, "window_secs": window.as_secs(), "bucket_secs": bucket.as_secs(), "optional": optional})
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
    let def_name = req.name.clone();
    let is_view = req.definition_type.as_deref() == Some("view");
    let mut engine = state.engine.write();
    let result: Result<(), crate::error::TallyError> = if is_view {
        match crate::server::protocol::convert_view_register_request(req) {
            Ok(view_def) => engine.register_view(view_def),
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": format!("{}", e)})),
                )
                    .into_response()
            }
        }
    } else {
        match convert_register_request(req) {
            Ok(stream_def) => engine.register(stream_def).map(|_diff| ()),
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": format!("{}", e)})),
                )
                    .into_response()
            }
        }
    };
    match result {
        Ok(()) => {
            engine.store_raw_register_json(&def_name, body);
            (StatusCode::OK, Json(serde_json::json!({"status": "ok"}))).into_response()
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
    let mut engine = state.engine.write();
    if engine.remove_stream(&name) {
        // Also deregister from event log
        let mut event_log = state.event_log.lock();
        if let Some(ref mut log) = *event_log {
            let _ = log.deregister_stream(&name);
        }
        (StatusCode::OK, Json(serde_json::json!({"status": "ok"}))).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("pipeline '{}' not found", name)})),
        )
            .into_response()
    }
}

async fn metrics_endpoint(State(state): State<SharedState>) -> impl IntoResponse {
    let keys_total = state.store.entity_count();
    let metrics = state.metrics.lock();
    let events_total = metrics.events_total;
    let push_latency = metrics.push_latency_seconds;
    let snapshot_duration = metrics.snapshot_duration_ms as f64 / 1000.0;
    let snapshots_skipped = metrics.snapshots_skipped;
    drop(metrics);
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
         tally_memory_bytes {}\n\
         # HELP tally_snapshots_skipped_total Snapshot cycles skipped due to in-progress write\n\
         # TYPE tally_snapshots_skipped_total counter\n\
         tally_snapshots_skipped_total {}\n",
        keys_total, events_total, push_latency, snapshot_duration, memory_bytes, snapshots_skipped,
    );
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        body,
    )
}

async fn debug_key(State(state): State<SharedState>, Path(key): Path<String>) -> impl IntoResponse {
    let store = &state.store;
    let now = SystemTime::now();
    // First check if entity exists
    let entity_exists = store.get_entity(&key).is_some();
    if !entity_exists {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("key '{}' not found", key)})),
        )
            .into_response();
    }
    // Collect debug info from entity (immutable borrow)
    let (live_ops, static_feats, last_event_at, total_estimated_bytes) = {
        let entity = store.get_entity(&key).unwrap();
        // Collect operators from all streams
        let mut live_ops: Vec<serde_json::Value> = Vec::new();
        let mut total_estimated_bytes: u64 = 0;
        for (stream_name, stream_state) in &entity.streams {
            for (name, op) in &stream_state.operators {
                let op_bytes = op.estimated_bytes() as u64;
                total_estimated_bytes += op_bytes;
                let mut entry = serde_json::json!({
                    "name": name,
                    "stream": stream_name,
                    "operator_type": op.operator_type_name(),
                    "estimated_bytes": op_bytes,
                    "state": format!("{:?}", op),
                });
                let buckets = op.num_buckets();
                if buckets > 0 {
                    entry["num_buckets"] = serde_json::json!(buckets);
                }
                live_ops.push(entry);
            }
        }
        let static_feats: serde_json::Map<String, serde_json::Value> = entity
            .static_features
            .iter()
            .map(|(k, v)| (k.clone(), v.value.to_json_value()))
            .collect();
        // Use the most recent last_event_at across all streams
        let last_event_at = entity
            .streams
            .values()
            .filter_map(|s| s.last_event_at)
            .max()
            .map(|t: SystemTime| {
                t.duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
            });
        (live_ops, static_feats, last_event_at, total_estimated_bytes)
    };
    // Now get computed features
    let features = store.get_all_features(&key, now);
    let feature_json: serde_json::Map<String, serde_json::Value> = features
        .iter()
        .map(|(k, v): (&String, &crate::types::FeatureValue)| (k.clone(), v.to_json_value()))
        .collect();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "key": key,
            "live_operators": live_ops,
            "static_features": static_feats,
            "computed_features": feature_json,
            "last_event_at": last_event_at,
            "estimated_bytes": total_estimated_bytes,
        })),
    )
        .into_response()
}

/// GET /debug/topology — Stream/view DAG for the Debug UI Topology tab.
///
/// Emits nodes for every registered stream AND every view (RESEARCH §Pitfall 7)
/// plus two kinds of edges: `cascade` edges for `depends_on` upstream links on
/// streams, and `lookup` edges for `ViewFeatureDef::Lookup` features on views.
/// Returns the cached topological order so the frontend can render nodes in
/// stable execution order without re-running toposort in JavaScript.
///
/// Lock discipline: acquires engine read lock, reads/clones everything it
/// needs, and returns `Json(...)` without any `.await` between lock and return.
async fn debug_topology(State(state): State<SharedState>) -> Json<serde_json::Value> {
    let engine = state.engine.read();

    let mut nodes: Vec<serde_json::Value> = Vec::new();
    let mut edges: Vec<serde_json::Value> = Vec::new();

    // Emit a node per registered stream. Include key_field (may be null for
    // keyless streams), the list of feature names, and depends_on for the
    // cascade DAG.
    for s in engine.list_streams() {
        let feature_names: Vec<&str> = s.features.iter().map(|(n, _)| n.as_str()).collect();

        // Phase 10.1 DBUI-06: pass through raw register JSON features as the
        // `operators` field so the drill-in panel can render per-stream operator
        // details without a new endpoint or a FeatureDef AST serializer.
        let operators: Vec<serde_json::Value> = engine
            .get_raw_register_json(&s.name)
            .and_then(|raw| raw.get("features"))
            .and_then(|f| f.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|feat| {
                        let mut out = serde_json::Map::new();
                        if let Some(n) = feat.get("name") {
                            out.insert("name".into(), n.clone());
                        }
                        // Rename `type` -> `op` in the output for frontend readability.
                        if let Some(t) = feat.get("type") {
                            out.insert("op".into(), t.clone());
                        }
                        if let Some(w) = feat.get("window") {
                            out.insert("window".into(), w.clone());
                        }
                        if let Some(b) = feat.get("bucket") {
                            out.insert("bucket".into(), b.clone());
                        }
                        if let Some(fld) = feat.get("field") {
                            out.insert("field".into(), fld.clone());
                        }
                        if let Some(wh) = feat.get("where") {
                            out.insert("where".into(), wh.clone());
                        }
                        if let Some(e) = feat.get("expr") {
                            out.insert("expr".into(), e.clone());
                        }
                        if let Some(o) = feat.get("optional") {
                            out.insert("optional".into(), o.clone());
                        }
                        if let Some(bf) = feat.get("backfill") {
                            out.insert("backfill".into(), bf.clone());
                        }
                        // Lookup-only fields (present when type == "lookup")
                        if let Some(on) = feat.get("on") {
                            out.insert("on".into(), on.clone());
                        }
                        if let Some(tg) = feat.get("target") {
                            out.insert("target".into(), tg.clone());
                        }
                        serde_json::Value::Object(out)
                    })
                    .collect()
            })
            .unwrap_or_default();

        nodes.push(serde_json::json!({
            "name": s.name,
            "kind": "stream",
            "key_field": s.key_field,
            "features": feature_names,              // UNCHANGED -- Phase 10 test compat
            "operators": operators,                 // NEW -- Phase 10.1 additive (DBUI-06)
            "depends_on": s.depends_on.clone().unwrap_or_default(),
        }));
        // Cascade edges: upstream -> downstream (this stream).
        for dep in s.depends_on.clone().unwrap_or_default() {
            edges.push(serde_json::json!({
                "from": dep,
                "to": s.name,
                "kind": "cascade",
            }));
        }
    }

    // Emit a node per registered view. Views have a String key_field (not
    // Option), no depends_on field, and derive edges from their Lookup
    // features.
    for v in engine.list_views() {
        let feature_names: Vec<&str> = v.features.iter().map(|(n, _)| n.as_str()).collect();

        let operators: Vec<serde_json::Value> = engine
            .get_raw_register_json(&v.name)
            .and_then(|raw| raw.get("features"))
            .and_then(|f| f.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|feat| {
                        let mut out = serde_json::Map::new();
                        if let Some(n) = feat.get("name") {
                            out.insert("name".into(), n.clone());
                        }
                        if let Some(t) = feat.get("type") {
                            out.insert("op".into(), t.clone());
                        }
                        if let Some(e) = feat.get("expr") {
                            out.insert("expr".into(), e.clone());
                        }
                        if let Some(on) = feat.get("on") {
                            out.insert("on".into(), on.clone());
                        }
                        if let Some(tg) = feat.get("target") {
                            out.insert("target".into(), tg.clone());
                        }
                        serde_json::Value::Object(out)
                    })
                    .collect()
            })
            .unwrap_or_default();

        nodes.push(serde_json::json!({
            "name": v.name,
            "kind": "view",
            "key_field": v.key_field,
            "features": feature_names,              // UNCHANGED
            "operators": operators,                 // NEW -- Phase 10.1 additive (DBUI-06)
            "depends_on": Vec::<String>::new(),
        }));
        // Lookup edges: the view depends on each target_stream it looks up.
        for (_fname, fdef) in &v.features {
            if let crate::engine::pipeline::ViewFeatureDef::Lookup { target_stream, .. } = fdef {
                edges.push(serde_json::json!({
                    "from": target_stream,
                    "to": v.name,
                    "kind": "lookup",
                }));
            }
        }
    }

    // Topological order already cached on the engine (Phase 7).
    let topo_order: Vec<String> = engine.get_topo_order().to_vec();

    Json(serde_json::json!({
        "nodes": nodes,
        "edges": edges,
        "topo_order": topo_order,
    }))
}

/// GET /debug/throughput — Per-stream EWMA message rates for the Streams tab.
async fn debug_throughput(State(state): State<SharedState>) -> Json<serde_json::Value> {
    let mut throughput = state.throughput.lock();
    let now_inst = std::time::Instant::now();
    throughput.decay_all(now_inst);
    let streams: Vec<serde_json::Value> = throughput
        .snapshot()
        .into_iter()
        .map(|(name, s)| {
            serde_json::json!({
                "name": name,
                "ewma_5s": s.ewma_5s,
                "ewma_1m": s.ewma_1m,
                "ewma_5m": s.ewma_5m,
            })
        })
        .collect();
    Json(serde_json::json!({
        "streams": streams,
    }))
}

/// Phase 10.2 DBUI-07: per-command and per-stream latency histograms.
async fn debug_latency(State(state): State<SharedState>) -> Json<serde_json::Value> {
    let latency = state.latency.lock();
    let now = std::time::Instant::now();
    Json(latency.to_json(now))
}

/// Per-stream memory accumulator used by `debug_memory`.
#[derive(Default)]
struct StreamMemoryStats {
    key_count: u64,
    total_bytes: u64,
    /// Operator type -> (count of operators across all keys, total bytes, bucket count if uniform)
    operator_types: ahash::AHashMap<&'static str, OperatorTypeStats>,
    /// Per-feature detail: feature_name -> (operator_type, num_buckets, total_bytes across keys)
    features: ahash::AHashMap<String, FeatureMemoryStats>,
}

#[derive(Default, Clone)]
struct OperatorTypeStats {
    count: u64,
    total_bytes: u64,
}

#[derive(Default, Clone)]
struct FeatureMemoryStats {
    operator_type: &'static str,
    num_buckets: usize,
    total_bytes: u64,
    key_count: u64,
}

/// GET /debug/memory — Memory rollup + per-stream breakdown.
///
/// Returns fine-grained, per-operator-type memory estimates based on actual
/// operator state rather than hardcoded per-key estimates.
async fn debug_memory(State(state): State<SharedState>) -> Json<serde_json::Value> {
    let store = &state.store;
    let engine = state.engine.read();

    // Accumulate per-stream stats by iterating all entity state
    let mut stream_stats: ahash::AHashMap<String, StreamMemoryStats> = ahash::AHashMap::new();
    let mut total_static_bytes: u64 = 0;
    let mut total_static_features: u64 = 0;

    let keys: Vec<String> = store.entity_keys();
    for key in &keys {
        if let Some(entity) = store.get_entity(key) {
            for (stream_name, stream_state) in &entity.streams {
                let stats = stream_stats
                    .entry(stream_name.clone())
                    .or_default();
                stats.key_count += 1;

                for (feature_name, op) in &stream_state.operators {
                    let op_bytes = op.estimated_bytes() as u64;
                    let op_type = op.operator_type_name();
                    let buckets = op.num_buckets();

                    stats.total_bytes += op_bytes;

                    let type_stats = stats
                        .operator_types
                        .entry(op_type)
                        .or_default();
                    type_stats.count += 1;
                    type_stats.total_bytes += op_bytes;

                    let feat_stats = stats
                        .features
                        .entry(feature_name.clone())
                        .or_default();
                    feat_stats.operator_type = op_type;
                    feat_stats.num_buckets = buckets;
                    feat_stats.total_bytes += op_bytes;
                    feat_stats.key_count += 1;
                }
            }

            // Account for static features
            let sf_count = entity.static_features.len() as u64;
            total_static_features += sf_count;
            // Estimate ~128 bytes per static feature (FeatureValue + timestamp + key overhead)
            total_static_bytes += sf_count * 128;
        }
    }

    // Build per-stream JSON
    let mut per_stream: Vec<serde_json::Value> = Vec::new();
    let mut grand_total_bytes: u64 = 0;

    for s in engine.list_streams() {
        let stats = stream_stats.get(&s.name);
        let key_count = stats.map_or(0, |s| s.key_count);
        let estimated_bytes = stats.map_or(0, |s| s.total_bytes);
        grand_total_bytes += estimated_bytes;

        // Per-operator-type breakdown
        let operator_breakdown: Vec<serde_json::Value> = stats
            .map(|s| {
                let mut items: Vec<_> = s.operator_types.iter().collect();
                items.sort_by_key(|(name, _)| *name);
                items
                    .iter()
                    .map(|(name, ts)| {
                        serde_json::json!({
                            "type": name,
                            "count": ts.count,
                            "total_bytes": ts.total_bytes,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        // Per-feature breakdown with bucket counts
        let feature_details: Vec<serde_json::Value> = stats
            .map(|s| {
                let mut items: Vec<_> = s.features.iter().collect();
                items.sort_by(|(a, _), (b, _)| a.cmp(b));
                items
                    .iter()
                    .map(|(name, fs)| {
                        let mut obj = serde_json::json!({
                            "name": name,
                            "operator_type": fs.operator_type,
                            "total_bytes": fs.total_bytes,
                            "key_count": fs.key_count,
                        });
                        if fs.num_buckets > 0 {
                            obj["num_buckets"] = serde_json::json!(fs.num_buckets);
                            if fs.key_count > 0 {
                                obj["avg_bytes_per_key"] =
                                    serde_json::json!(fs.total_bytes / fs.key_count);
                            }
                        }
                        obj
                    })
                    .collect()
            })
            .unwrap_or_default();

        let per_entity_avg = if key_count > 0 {
            estimated_bytes / key_count
        } else {
            0
        };

        per_stream.push(serde_json::json!({
            "name": s.name,
            "kind": "stream",
            "key_count": key_count,
            "estimated_bytes": estimated_bytes,
            "per_entity_avg_bytes": per_entity_avg,
            "operator_breakdown": operator_breakdown,
            "features": feature_details,
        }));
    }

    for v in engine.list_views() {
        per_stream.push(serde_json::json!({
            "name": v.name,
            "kind": "view",
            "key_count": 0,
            "estimated_bytes": 0,
        }));
    }

    grand_total_bytes += total_static_bytes;

    let entity_count = store.entity_count();
    let per_entity_avg = if entity_count > 0 {
        grand_total_bytes / entity_count as u64
    } else {
        0
    };

    Json(serde_json::json!({
        "entity_count": entity_count,
        "stream_count": engine.stream_count(),
        "estimated_bytes": grand_total_bytes,
        "per_entity_avg_bytes": per_entity_avg,
        "static_features": {
            "count": total_static_features,
            "estimated_bytes": total_static_bytes,
        },
        "per_stream": per_stream,
    }))
}

/// Query parameters for `POST /snapshot`.
#[derive(Debug, serde::Deserialize, Default)]
struct SnapshotQuery {
    /// If true, wait for the snapshot to complete before responding.
    #[serde(default)]
    wait: Option<bool>,
    /// Maximum time (ms) to wait when `wait=true`. Returns 408 on timeout.
    #[serde(default)]
    timeout_ms: Option<u64>,
}

async fn trigger_snapshot(
    State(state): State<SharedState>,
    Query(params): Query<SnapshotQuery>,
) -> impl IntoResponse {
    if !state.snapshot_enabled {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "snapshots disabled"})),
        )
            .into_response();
    }

    // Phase 15: cycle guard — reject if a snapshot is already in progress.
    if state
        .snapshot_in_progress
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error": "snapshot cycle already in progress"})),
        )
            .into_response();
    }

    // RAII guard to clear the flag even on panic/early return.
    struct SnapshotGuard(SharedState);
    impl Drop for SnapshotGuard {
        fn drop(&mut self) {
            self.0.snapshot_in_progress.store(false, Ordering::Release);
        }
    }
    let _guard = SnapshotGuard(state.clone());

    // Manual trigger always writes a full v6 base snapshot.
    let (snapshot_data, seq, snap_dir) = {
        let engine = state.engine.read();
        let store = &state.store;
        let seq = *state.snapshot_seq.lock();
        let valid_features = engine.valid_features_map();
        let entities = store.clone_for_snapshot_with_gc(&valid_features);
        // Populate pipelines from engine
        let mut pipelines: Vec<crate::state::snapshot::SerializablePipeline> = engine
            .list_streams()
            .filter_map(|stream| {
                engine.get_raw_register_json(&stream.name).map(|json| {
                    crate::state::snapshot::SerializablePipeline {
                        name: stream.name.clone(),
                        key_field: stream.key_field.clone().unwrap_or_default(),
                        raw_register_json: serde_json::to_string(json).unwrap_or_default(),
                    }
                })
            })
            .collect();
        // Also include view definitions in the snapshot
        for view in engine.list_views() {
            if let Some(json) = engine.get_raw_register_json(&view.name) {
                pipelines.push(crate::state::snapshot::SerializablePipeline {
                    name: view.name.clone(),
                    key_field: view.key_field.clone(),
                    raw_register_json: serde_json::to_string(json).unwrap_or_default(),
                });
            }
        }
        let backfill_complete: Vec<(String, String)> =
            state.backfill_complete.lock().iter().cloned().collect();
        // Manual trigger clears dirty/deleted tracking
        store.clear_dirty();
        let _ = store.take_deleted();
        *state.snapshot_seq.lock() = seq + 1;
        // Phase 9 WR-01 (re-review): keep the manual path symmetric with the
        // periodic timer.
        {
            let mut last_base = state.last_base_seq.lock();
            let mut prev_base = state.previous_base_seq.lock();
            *prev_base = *last_base;
            *last_base = seq;
        }

        let snap_dir = state
            .snapshot_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .to_path_buf();

        let base = crate::state::snapshot::BaseSnapshotState {
            header: crate::state::snapshot::SnapshotHeader {
                snapshot_type: crate::state::snapshot::SnapshotType::Base,
                sequence: seq,
            },
            entities,
            pipelines,
            backfill_complete,
        };
        (base, seq, snap_dir)
    };
    // Capture start time for snapshot_duration_ms metric
    let snap_start = std::time::Instant::now();
    let write_fut = tokio::task::spawn_blocking(move || {
        let bytes = crate::state::snapshot::save_base_snapshot(&snapshot_data)
            .map_err(std::io::Error::other)?;
        let filename = format!("tally.snapshot.base.{:010}", seq);
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
        Ok::<usize, std::io::Error>(bytes.len())
    });

    // Phase 15: if wait=true, optionally apply a timeout.
    let wait = params.wait.unwrap_or(false);
    let result = if wait {
        if let Some(timeout_ms) = params.timeout_ms {
            match tokio::time::timeout(std::time::Duration::from_millis(timeout_ms), write_fut)
                .await
            {
                Ok(inner) => inner,
                Err(_) => {
                    return (
                        StatusCode::REQUEST_TIMEOUT,
                        Json(serde_json::json!({"error": "snapshot timed out"})),
                    )
                        .into_response();
                }
            }
        } else {
            write_fut.await
        }
    } else {
        write_fut.await
    };

    match result {
        Ok(Ok(size)) => {
            let snap_elapsed = snap_start.elapsed();
            state.metrics.lock().snapshot_duration_ms = snap_elapsed.as_millis() as u64;
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "status": "ok",
                    "bytes": size,
                    "duration_ms": snap_elapsed.as_millis() as u64,
                })),
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

async fn debug_backfill(State(state): State<SharedState>) -> Json<serde_json::Value> {
    let tasks = state
        .backfill_tracker
        .tasks
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let task_list: Vec<serde_json::Value> = tasks
        .iter()
        .map(|t| {
            let processed = t
                .processed_events
                .load(std::sync::atomic::Ordering::Relaxed);
            let completed = t
                .completed_at
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .map(|_| true)
                .unwrap_or(false);
            serde_json::json!({
                "stream": t.stream,
                "features": t.features,
                "total_events": t.total_events,
                "processed_events": processed,
                "completed": completed,
                "status": if completed { "completed" } else { "running" },
            })
        })
        .collect();
    Json(serde_json::json!({
        "backfill_tasks": task_list,
    }))
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
        .route("/debug/backfill", get(debug_backfill))
        .route("/debug/topology", get(debug_topology)) // NEW (DBUI-01)
        .route("/debug/throughput", get(debug_throughput)) // NEW (DBUI-02)
        .route("/debug/latency", get(debug_latency)) // NEW (DBUI-07)
        .route("/snapshot", post(trigger_snapshot))
        .route("/", get(ui_index)) // NEW (DBUI-05)
        .route("/static/{*file}", get(ui_static)) // NEW (DBUI-05)
        .with_state(state)
}

/// Start the HTTP management server on the given address.
pub async fn run_http_server(addr: &str, state: SharedState) -> Result<(), std::io::Error> {
    let app = build_router(state);
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .await
        .map_err(std::io::Error::other)
}

/// Start the HTTP management server from a pre-bound listener (for tests).
pub async fn run_http_server_with_listener(
    listener: TcpListener,
    state: SharedState,
) -> Result<(), std::io::Error> {
    let app = build_router(state);
    axum::serve(listener, app)
        .await
        .map_err(std::io::Error::other)
}

// ======================== Phase 15 Tests ========================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::pipeline::PipelineEngine;
    use crate::server::tcp::{make_concurrent_state, BackfillTracker};
    use crate::state::store::StateStore;
    use std::sync::Arc;

    fn test_state() -> SharedState {
        make_concurrent_state(
            PipelineEngine::new(),
            StateStore::new(),
            None,
            std::path::PathBuf::from("/tmp/tally-test-snapshot"),
            Arc::new(BackfillTracker::default()),
            true,
            false,
        )
    }

    #[test]
    fn test_snapshot_cycle_guard_prevents_overlap() {
        let state = test_state();

        // Simulate first snapshot starting
        assert!(state
            .snapshot_in_progress
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire,)
            .is_ok());

        // Second attempt should fail
        assert!(state
            .snapshot_in_progress
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire,)
            .is_err());

        // After clearing, it should succeed again
        state.snapshot_in_progress.store(false, Ordering::Release);
        assert!(state
            .snapshot_in_progress
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire,)
            .is_ok());
    }

    #[test]
    fn test_snapshot_guard_raii_clears_flag() {
        let state = test_state();

        {
            struct SnapGuard(SharedState);
            impl Drop for SnapGuard {
                fn drop(&mut self) {
                    self.0.snapshot_in_progress.store(false, Ordering::Release);
                }
            }
            state.snapshot_in_progress.store(true, Ordering::Release);
            let _guard = SnapGuard(state.clone());
            assert!(state.snapshot_in_progress.load(Ordering::Acquire));
            // _guard drops here
        }
        // Flag should be cleared
        assert!(!state.snapshot_in_progress.load(Ordering::Acquire));
    }

    #[test]
    fn test_snapshots_skipped_metric_increments() {
        let state = test_state();
        assert_eq!(state.metrics.lock().snapshots_skipped, 0);
        state.metrics.lock().snapshots_skipped += 1;
        assert_eq!(state.metrics.lock().snapshots_skipped, 1);
    }

    #[tokio::test]
    async fn test_snapshot_trigger_returns_409_when_in_progress() {
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        let state = test_state();
        // Simulate in-progress snapshot
        state.snapshot_in_progress.store(true, Ordering::Release);

        let app = build_router(state);
        let req = Request::builder()
            .method("POST")
            .uri("/snapshot")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_snapshot_trigger_returns_404_when_disabled() {
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        let state = make_concurrent_state(
            PipelineEngine::new(),
            StateStore::new(),
            None,
            std::path::PathBuf::from("/tmp/tally-test-snapshot"),
            Arc::new(BackfillTracker::default()),
            false, // snapshots disabled
            false,
        );

        let app = build_router(state);
        let req = Request::builder()
            .method("POST")
            .uri("/snapshot")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_metrics_includes_snapshots_skipped() {
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        let state = test_state();
        state.metrics.lock().snapshots_skipped = 42;

        let app = build_router(state);
        let req = Request::builder()
            .method("GET")
            .uri("/metrics")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(
            text.contains("tally_snapshots_skipped_total 42"),
            "metrics body: {}",
            text
        );
    }
}
