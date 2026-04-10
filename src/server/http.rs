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
use crate::server::ui::{ui_index, ui_static};

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
    let def_name = req.name.clone();
    let is_view = req.definition_type.as_deref() == Some("view");
    let mut app = state.lock().unwrap_or_else(|e| e.into_inner());
    let result: Result<(), crate::error::TallyError> = if is_view {
        match crate::server::protocol::convert_view_register_request(req) {
            Ok(view_def) => app.engine.register_view(view_def),
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
            Ok(stream_def) => app.engine.register(stream_def).map(|_diff| ()),
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
            app.engine.store_raw_register_json(&def_name, body);
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
        // Also deregister from event log
        if let Some(ref mut log) = app.event_log {
            let _ = log.deregister_stream(&name);
        }
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
        // Collect operators from all streams
        let mut live_ops: Vec<serde_json::Value> = Vec::new();
        for (stream_name, stream_state) in &entity.streams {
            for (name, op) in &stream_state.operators {
                live_ops.push(serde_json::json!({
                    "name": name,
                    "stream": stream_name,
                    "state": format!("{:?}", op),
                }));
            }
        }
        let static_feats: serde_json::Map<String, serde_json::Value> = entity
            .static_features
            .iter()
            .map(|(k, v)| (k.clone(), v.value.to_json_value()))
            .collect();
        // Use the most recent last_event_at across all streams
        let last_event_at = entity.streams.values()
            .filter_map(|s| s.last_event_at)
            .max()
            .map(|t| {
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

/// GET /debug/topology — Stream/view DAG for the Debug UI Topology tab.
///
/// Emits nodes for every registered stream AND every view (RESEARCH §Pitfall 7)
/// plus two kinds of edges: `cascade` edges for `depends_on` upstream links on
/// streams, and `lookup` edges for `ViewFeatureDef::Lookup` features on views.
/// Returns the cached topological order so the frontend can render nodes in
/// stable execution order without re-running toposort in JavaScript.
///
/// Lock discipline (RESEARCH §Pitfall 3): acquires the AppState mutex,
/// reads/clones everything it needs, and returns `Json(...)` without any
/// `.await` between lock and return.
async fn debug_topology(State(state): State<SharedState>) -> Json<serde_json::Value> {
    let app = state.lock().unwrap_or_else(|e| e.into_inner());

    let mut nodes: Vec<serde_json::Value> = Vec::new();
    let mut edges: Vec<serde_json::Value> = Vec::new();

    // Emit a node per registered stream. Include key_field (may be null for
    // keyless streams), the list of feature names, and depends_on for the
    // cascade DAG.
    for s in app.engine.list_streams() {
        let feature_names: Vec<&str> = s.features.iter().map(|(n, _)| n.as_str()).collect();

        // Phase 10.1 DBUI-06: pass through raw register JSON features as the
        // `operators` field so the drill-in panel can render per-stream operator
        // details without a new endpoint or a FeatureDef AST serializer.
        // RESEARCH Pattern 8 -- pass-through from `raw_register_jsons`.
        // Snapshot-restored streams that pre-date raw_register_jsons return None
        // here -- emit `operators: []` as a tolerant fallback (RESEARCH Pitfall 7).
        let operators: Vec<serde_json::Value> = app
            .engine
            .get_raw_register_json(&s.name)
            .and_then(|raw| raw.get("features"))
            .and_then(|f| f.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|feat| {
                        let mut out = serde_json::Map::new();
                        if let Some(n) = feat.get("name") { out.insert("name".into(), n.clone()); }
                        // Rename `type` -> `op` in the output for frontend readability.
                        if let Some(t) = feat.get("type") { out.insert("op".into(), t.clone()); }
                        if let Some(w) = feat.get("window") { out.insert("window".into(), w.clone()); }
                        if let Some(b) = feat.get("bucket") { out.insert("bucket".into(), b.clone()); }
                        if let Some(fld) = feat.get("field") { out.insert("field".into(), fld.clone()); }
                        if let Some(wh) = feat.get("where") { out.insert("where".into(), wh.clone()); }
                        if let Some(e) = feat.get("expr") { out.insert("expr".into(), e.clone()); }
                        if let Some(o) = feat.get("optional") { out.insert("optional".into(), o.clone()); }
                        if let Some(bf) = feat.get("backfill") { out.insert("backfill".into(), bf.clone()); }
                        // Lookup-only fields (present when type == "lookup")
                        if let Some(on) = feat.get("on") { out.insert("on".into(), on.clone()); }
                        if let Some(tg) = feat.get("target") { out.insert("target".into(), tg.clone()); }
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
    // features. This MUST include views -- RESEARCH §Pitfall 7 warns that
    // forgetting list_views() breaks the topology tab's purple view nodes.
    for v in app.engine.list_views() {
        let feature_names: Vec<&str> = v.features.iter().map(|(n, _)| n.as_str()).collect();

        // Phase 10.1 DBUI-06: same pass-through as stream nodes, but views
        // only have derive and lookup features (no window/field/where/bucket/
        // optional/backfill), so the projection is reduced.
        let operators: Vec<serde_json::Value> = app
            .engine
            .get_raw_register_json(&v.name)
            .and_then(|raw| raw.get("features"))
            .and_then(|f| f.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|feat| {
                        let mut out = serde_json::Map::new();
                        if let Some(n) = feat.get("name") { out.insert("name".into(), n.clone()); }
                        if let Some(t) = feat.get("type") { out.insert("op".into(), t.clone()); }
                        if let Some(e) = feat.get("expr") { out.insert("expr".into(), e.clone()); }
                        if let Some(on) = feat.get("on") { out.insert("on".into(), on.clone()); }
                        if let Some(tg) = feat.get("target") { out.insert("target".into(), tg.clone()); }
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

    // Topological order already cached on the engine (Phase 7). Return it so
    // the frontend can render nodes in a stable, execution-order sequence
    // without re-running toposort in JavaScript.
    let topo_order: Vec<String> = app.engine.get_topo_order().to_vec();

    Json(serde_json::json!({
        "nodes": nodes,
        "edges": edges,
        "topo_order": topo_order,
    }))
}

/// GET /debug/throughput — Per-stream EWMA message rates for the Streams tab.
///
/// Calls `ThroughputTracker::decay_all(Instant::now())` BEFORE `snapshot()` so
/// idle streams report declining rates even when no recent push drove an
/// update. Returns `{streams: [{name, ewma_5s, ewma_1m, ewma_5m}, ...]}`.
///
/// Lock discipline (RESEARCH §Pitfall 3): acquires the AppState mutex with
/// `&mut` for `decay_all`, performs both calls synchronously, and returns
/// `Json(...)` without any `.await` while holding the guard.
async fn debug_throughput(State(state): State<SharedState>) -> Json<serde_json::Value> {
    let mut app = state.lock().unwrap_or_else(|e| e.into_inner());
    // Decay every stream's EWMAs to "now" before snapshotting so idle streams
    // report declining rates even with no recent push to drive the update.
    let now_inst = std::time::Instant::now();
    app.throughput.decay_all(now_inst);
    let streams: Vec<serde_json::Value> = app
        .throughput
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
///
/// Lock discipline: acquires the AppState mutex, calls `to_json()` synchronously,
/// returns `Json(...)` without any `.await` while holding the guard.
async fn debug_latency(State(state): State<SharedState>) -> Json<serde_json::Value> {
    let app = state.lock().unwrap_or_else(|e| e.into_inner());
    let now = std::time::Instant::now();
    Json(app.latency.to_json(now))
}

/// GET /debug/memory — Memory rollup + per-stream breakdown.
///
/// Additively extended in Plan 10-03 (RESEARCH §Pitfall 8): the original three
/// rollup fields (`entity_count`, `stream_count`, `estimated_bytes`) are
/// preserved for backward compatibility, and a new `per_stream` array emits
/// one entry per registered stream AND per registered view with
/// `{name, kind, key_count, estimated_bytes}`.
///
/// Key count per stream is computed by iterating every entity and tallying
/// which streams have a `StreamEntityState` under each. Views always report
/// `key_count: 0` / `estimated_bytes: 0` since views hold no operator state.
async fn debug_memory(State(state): State<SharedState>) -> Json<serde_json::Value> {
    let app = state.lock().unwrap_or_else(|e| e.into_inner());

    // Per-stream breakdown: iterate every entity, tally how many have state
    // under each stream name, and apply the same naive 2 KB/key estimator
    // the existing rollup uses. Views are reported separately with their
    // known key count of zero since they hold no operator state.
    let mut per_stream_counts: ahash::AHashMap<String, u64> = ahash::AHashMap::new();
    let keys: Vec<String> = app.store.entity_keys().collect();
    for key in &keys {
        if let Some(entity) = app.store.get_entity(key) {
            for stream_name in entity.streams.keys() {
                *per_stream_counts.entry(stream_name.clone()).or_insert(0) += 1;
            }
        }
    }

    // Emit per_stream entries in the order streams are registered (stable
    // across calls for a given registration set).
    let mut per_stream: Vec<serde_json::Value> = Vec::new();
    for s in app.engine.list_streams() {
        let key_count = per_stream_counts.get(&s.name).copied().unwrap_or(0);
        per_stream.push(serde_json::json!({
            "name": s.name,
            "kind": "stream",
            "key_count": key_count,
            "estimated_bytes": key_count * 2048,
        }));
    }
    for v in app.engine.list_views() {
        per_stream.push(serde_json::json!({
            "name": v.name,
            "kind": "view",
            "key_count": 0,
            "estimated_bytes": 0,
        }));
    }

    // Preserve the existing three top-level fields (Phase 6 callers and any
    // hand-crafted curl scripts still read them). The `per_stream` array is
    // an additive extension (DBUI-04).
    //
    // Phase 10 review IN-04: bind entity_count once to avoid walking the
    // state map twice in the same handler (matches the `keys` binding
    // pattern above).
    let entity_count = app.store.entity_count();
    Json(serde_json::json!({
        "entity_count": entity_count,
        "stream_count": app.engine.stream_count(),
        "estimated_bytes": entity_count * 2048,
        "per_stream": per_stream,
    }))
}

async fn trigger_snapshot(State(state): State<SharedState>) -> impl IntoResponse {
    // Manual trigger always writes a full v6 base snapshot. This keeps the
    // manual path simple and debuggable: no delta chain to reason about, no
    // dependency on prior sequence state. Filenames follow the same
    // tally.snapshot.base.{seq} convention as the periodic timer.
    let (snapshot_data, seq, snap_dir) = {
        let mut app = state.lock().unwrap_or_else(|e| e.into_inner());
        let seq = app.snapshot_seq;
        let valid_features = app.engine.valid_features_map();
        let entities = app.store.clone_for_snapshot_with_gc(&valid_features);
        // Populate pipelines from engine -- same pattern as periodic snapshot timer
        let mut pipelines: Vec<crate::state::snapshot::SerializablePipeline> = app
            .engine
            .list_streams()
            .filter_map(|stream| {
                app.engine.get_raw_register_json(&stream.name).map(|json| {
                    crate::state::snapshot::SerializablePipeline {
                        name: stream.name.clone(),
                        key_field: stream.key_field.clone().unwrap_or_default(),
                        raw_register_json: serde_json::to_string(json).unwrap_or_default(),
                    }
                })
            })
            .collect();
        // Also include view definitions in the snapshot
        for view in app.engine.list_views() {
            if let Some(json) = app.engine.get_raw_register_json(&view.name) {
                pipelines.push(crate::state::snapshot::SerializablePipeline {
                    name: view.name.clone(),
                    key_field: view.key_field.clone(),
                    raw_register_json: serde_json::to_string(json).unwrap_or_default(),
                });
            }
        }
        let backfill_complete: Vec<(String, String)> =
            app.backfill_complete.iter().cloned().collect();
        // Manual trigger clears dirty/deleted tracking since the full base
        // supersedes any pending delta.
        app.store.clear_dirty();
        let _ = app.store.take_deleted();
        app.snapshot_seq += 1;
        // Phase 9 WR-01 (re-review): keep the manual path symmetric with the
        // periodic timer in src/main.rs:289-293. Advance `last_base_seq` and
        // roll the previous one into `previous_base_seq` so that:
        //   1. The next periodic delta stamps the correct base_seq in its
        //      header (instead of pointing at a stale pre-manual base).
        //   2. The WR-03 fallback policy keeps the pre-manual base as the
        //      "previous" candidate rather than skipping the manual one.
        let prev_base = app.last_base_seq;
        app.previous_base_seq = prev_base;
        app.last_base_seq = seq;

        let snap_dir = app.snapshot_path.parent()
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
    let result = tokio::task::spawn_blocking(move || {
        let bytes = crate::state::snapshot::save_base_snapshot(&snapshot_data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let filename = format!("tally.snapshot.base.{:010}", seq);
        let file_path = snap_dir.join(&filename);
        // Phase 9 CR-01: unique tmp filename per (type, seq) to avoid races
        // between concurrent snapshot writers. `with_extension("tmp")` would
        // drop the sequence number and produce a shared tmp file that the
        // periodic timer could clobber.
        let tmp_path = snap_dir.join(format!("{}.tmp", filename));
        // Phase 9 WR-01: fsync the tmp file before rename so we don't end up
        // with a zero-byte snapshot after a power loss.
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
        // Fsync the directory so the rename itself is durable.
        if let Ok(dir) = std::fs::File::open(&snap_dir) {
            let _ = dir.sync_all();
        }
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

async fn debug_backfill(State(state): State<SharedState>) -> Json<serde_json::Value> {
    let app = state.lock().unwrap_or_else(|e| e.into_inner());
    let tasks = app.backfill_tracker.tasks.lock()
        .unwrap_or_else(|e| e.into_inner());
    let task_list: Vec<serde_json::Value> = tasks.iter().map(|t| {
        let processed = t.processed_events.load(std::sync::atomic::Ordering::Relaxed);
        let completed = t.completed_at.lock()
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
    }).collect();
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
