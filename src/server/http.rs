//! HTTP management API: health, pipeline CRUD, metrics, debug, snapshot endpoints.
//!
//! Runs on a separate port (default 6401) from the TCP hot path.
//! Phase 14: All handlers use individual field locks from ConcurrentAppState
//! instead of a single global Mutex<AppState>.

use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderName, HeaderValue, StatusCode},
    middleware,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;
use tokio::net::TcpListener;

use super::tcp::SharedState;
use crate::server::auth::require_loopback_or_token;
use crate::server::protocol::{convert_register_request, RegisterRequest};
use crate::server::ui::{ui_index, ui_static, UiAssets};

/// Phase 20: CORS headers applied to every `/public/*` response so the launch
/// blog (and other third-party sites) can fetch live metrics cross-origin.
/// Caddy may override this at the edge — that's intentional.
fn cors_headers() -> [(HeaderName, HeaderValue); 1] {
    [(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    )]
}

/// `GET /health` — process-is-alive probe (TPC-INFRA-06).
///
/// Returns HTTP 200 with `{"status":"alive"}` unconditionally — from process
/// start through recovery and steady-state operation. This endpoint is NEVER
/// gated on recovery state (that is `/ready`'s job). Orchestrators use
/// `/health` to distinguish "process running" from "process dead".
async fn health() -> impl IntoResponse {
    (axum::http::StatusCode::OK, Json(serde_json::json!({"status": "alive"})))
}

/// `GET /ready` — readiness probe gated on per-shard log recovery (TPC-INFRA-06, Phase 52-03).
///
/// Returns HTTP 503 with `{"status":"recovering","shards_recovering":[...]}` while
/// any shard is still replaying its event log. Returns HTTP 200 with
/// `{"status":"ready"}` only when `recovery_barrier.all_recovered()` is true
/// (or when no recovery barrier exists — fresh install / event-log disabled).
///
/// The `shards_recovering` list in the 503 body allows probes to report
/// per-shard progress without polling `/debug/shards`.
async fn ready(State(state): State<SharedState>) -> impl IntoResponse {
    let all_recovered = state
        .recovery_barrier
        .as_ref()
        .map(|b| b.all_recovered())
        .unwrap_or(true); // No barrier → treat as recovered (no recovery needed).

    if all_recovered {
        (
            axum::http::StatusCode::OK,
            Json(serde_json::json!({"status": "ready"})),
        )
    } else {
        let shards_recovering: Vec<u8> = state
            .recovery_barrier
            .as_ref()
            .map(|b| b.recovering_shards())
            .unwrap_or_default();
        (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "status": "recovering",
                "shards_recovering": shards_recovering,
            })),
        )
    }
}

/// Phase 37-01: `/debug/ready` — legacy readiness endpoint (kept for backward compat).
/// Returns 200 unconditionally once routable — in replica/fork mode the HTTP
/// listener only binds after the catchup-done oneshot fires.
async fn debug_ready(State(state): State<SharedState>) -> Json<serde_json::Value> {
    let replica_mode = state
        .replica_mode
        .load(std::sync::atomic::Ordering::Relaxed);
    Json(serde_json::json!({
        "ready": true,
        "replica_mode": replica_mode,
    }))
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
                    crate::engine::pipeline::FeatureDef::EnrichFromTable { right_table, on, join_type, right_fields } => {
                        serde_json::json!({"name": fname, "type": "enrich_from_table", "right_table": right_table, "on": on, "join_type": format!("{:?}", join_type), "right_fields": right_fields})
                    }
                    crate::engine::pipeline::FeatureDef::StreamStreamJoin { left_stream, right_stream, on, within_ms, join_type, left_fields, right_fields } => {
                        serde_json::json!({"name": fname, "type": "stream_stream_join", "left_stream": left_stream, "right_stream": right_stream, "on": on, "within_ms": within_ms, "join_type": format!("{:?}", join_type), "left_fields": left_fields, "right_fields": right_fields})
                    }
                    crate::engine::pipeline::FeatureDef::TableTableJoin { left_table, right_table, on, join_type, left_fields, right_fields } => {
                        serde_json::json!({"name": fname, "type": "table_table_join", "left_table": left_table, "right_table": right_table, "on": on, "join_type": format!("{:?}", join_type), "left_fields": left_fields, "right_fields": right_fields})
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
    let result: Result<(), crate::error::BeavaError> = if is_view {
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
            // Phase 35-01 parity fix: mirror the TCP OP_REGISTER path and
            // register the new stream with the event log so PUSH events to
            // this stream persist to disk. Without this hook, HTTP-registered
            // streams were invisible to OP_LOG_FETCH (and to any other
            // replay/backfill consumer of the event log). This is symmetric
            // with the TCP `handle_sync_command::OP_REGISTER` arm that has
            // always called `log.register_stream` after a successful engine
            // registration.
            if !is_view {
                let history_ttl = engine.get_stream(&def_name).and_then(|s| s.history_ttl);
                if let Some(ref log) = state.event_log {
                    let _ = log.register_stream(&def_name, history_ttl);
                }
                // Phase 50-06 (D-11/D-12): warn if stream has no shard_key at N>1.
                let no_shard_key = engine
                    .get_stream(&def_name)
                    .map(|s| s.shard_key.is_none())
                    .unwrap_or(false);
                if no_shard_key {
                    let shard_count = state.shard_handles.read().len();
                    crate::server::signals::emit_shard_key_missing_warning(
                        &state.signals,
                        &def_name,
                        shard_count,
                    );
                }
            }
            (StatusCode::OK, Json(serde_json::json!({"status": "ok"}))).into_response()
        }
        Err(e) => {
            // Phase 25-02: emit a safety signal so failed registrations
            // surface on /debug/warnings.
            drop(engine);
            crate::server::signals::emit_register_failure(
                &state.signals,
                &def_name,
                &format!("{}", e),
            );
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": format!("{}", e)})),
            )
                .into_response()
        }
    }
}

async fn delete_pipeline(
    State(state): State<SharedState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let mut engine = state.engine.write();
    if engine.remove_stream(&name) {
        // Also deregister from event log
        if let Some(ref log) = state.event_log {
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
    // Phase 54-04 Pass A2: scatter-gather `keys_total` across all
    // shards instead of reading the global StateStore. Each shard
    // returns its approximate (O(1)) key count via SPSC; shards that
    // are DOWN or backpressured contribute 0 so the endpoint stays
    // available during partial failures (matches the `keys_owned`
    // gauge behaviour). Clone handles before awaiting to release the
    // `shard_handles` RwLock (Pass A1 pattern).
    let keys_total: usize = {
        let handle_clones: Vec<crate::shard::thread::ShardHandle> = {
            let handles = state.shard_handles.read();
            handles
                .iter()
                .map(crate::shard::thread::clone_handle)
                .collect()
        };
        let mut total = 0usize;
        for h in &handle_clones {
            match crate::shard::thread::entity_count_via_shard(h).await {
                Ok(n) => total = total.saturating_add(n),
                // Shard DOWN / inbox full / channel closed: skip so
                // the metrics endpoint stays up during degraded mode.
                Err(_) => {}
            }
        }
        total
    };
    // Phase 41-01 T2: events_total + last-push latency are now lock-free
    // atomics on AppState. Only the rare-write fields stay behind the mutex.
    let events_total = state
        .events_total
        .load(std::sync::atomic::Ordering::Relaxed);
    // Phase 45-04 A5: per-protocol labeled counters for dual-emit transition.
    let events_http = state.events_http.load(std::sync::atomic::Ordering::Relaxed);
    let events_tcp = state.events_tcp.load(std::sync::atomic::Ordering::Relaxed);
    let push_latency = state
        .last_push_latency_nanos
        .load(std::sync::atomic::Ordering::Relaxed) as f64
        / 1_000_000_000.0;
    let metrics = state.metrics.lock();
    let snapshot_duration = metrics.snapshot_duration_ms as f64 / 1000.0;
    let snapshots_skipped = metrics.snapshots_skipped;
    drop(metrics);
    let memory_bytes = keys_total * 2048; // Rough estimate: ~2KB per entity with operators

    // Phase 20 TRAC-07: current EPS (5s EWMA summed across streams) and p99
    // PUSH latency taken from the Phase 10.2 rolling histogram. Both read a
    // snapshot of their respective trackers; no mutation.
    //
    // Phase 41-01 T3: current_eps now reads the lock-free atomic ring.
    let current_eps = state.atomic_throughput.eps_5s();
    let now_inst = std::time::Instant::now();
    let p99_push_us = state.latency.lock().push_percentile_us(99.0, now_inst);
    // Prometheus convention: seconds, not microseconds.
    let p99_push_seconds = p99_push_us / 1_000_000.0;

    let mut body = format!(
        "# HELP beava_keys_total Number of entity keys in memory\n\
         # TYPE beava_keys_total gauge\n\
         beava_keys_total {}\n\
         # HELP beava_events_total Total events ingested (sum of all protocols; unlabeled for backward compat; labeled series will replace in v1.1)\n\
         # TYPE beava_events_total counter\n\
         beava_events_total {}\n\
         beava_events_total{{proto=\"http\"}} {}\n\
         beava_events_total{{proto=\"tcp\"}} {}\n\
         # HELP beava_push_latency_seconds Last observed PUSH latency\n\
         # TYPE beava_push_latency_seconds gauge\n\
         beava_push_latency_seconds {}\n\
         # HELP beava_push_latency_p99_seconds Rolling p99 PUSH latency (5 min window)\n\
         # TYPE beava_push_latency_p99_seconds gauge\n\
         beava_push_latency_p99_seconds {}\n\
         # HELP beava_current_eps Events per second (5s EWMA, all streams)\n\
         # TYPE beava_current_eps gauge\n\
         beava_current_eps {}\n\
         # HELP beava_snapshot_duration_seconds Last snapshot write duration\n\
         # TYPE beava_snapshot_duration_seconds gauge\n\
         beava_snapshot_duration_seconds {}\n\
         # HELP beava_memory_bytes Estimated memory usage\n\
         # TYPE beava_memory_bytes gauge\n\
         beava_memory_bytes {}\n\
         # HELP beava_snapshots_skipped_total Snapshot cycles skipped due to in-progress write\n\
         # TYPE beava_snapshots_skipped_total counter\n\
         beava_snapshots_skipped_total {}\n",
        keys_total,
        events_total,           // TODO(gh-TBD): remove unlabeled beava_events_total emission — tracked for v1.0-launch follow-up
        events_http,
        events_tcp,
        push_latency,
        p99_push_seconds,
        current_eps,
        snapshot_duration,
        memory_bytes,
        snapshots_skipped,
    );

    // Phase 24-04: per-stream late-drop counter. Label cardinality is
    // bounded by registered streams (T-24-04-05).
    body.push_str(
        "# HELP beava_late_events_dropped_total Events dropped for arriving with \
         event_time older than the stream's current watermark\n\
         # TYPE beava_late_events_dropped_total counter\n",
    );
    {
        let engine = state.engine.read();
        let drops = engine.late_drops.snapshot();
        for (stream, count) in drops {
            // Basic label-value escaping: backslashes and double quotes per
            // Prometheus exposition grammar.
            let escaped = stream.replace('\\', "\\\\").replace('"', "\\\"");
            body.push_str(&format!(
                "beava_late_events_dropped_total{{stream=\"{}\"}} {}\n",
                escaped, count
            ));
        }
    }

    // OBS-01 / Phase 46-06: ring-buffer drop counter with three-variant
    // reason label. Cardinality is bounded: stream × operator_kind × reason
    // where operator_kind and reason are compile-time enums.
    // OBS-02: mutually exclusive with beava_late_events_dropped_total — the
    // late-drop gate in tcp.rs fires `continue` before the event reaches the
    // ring-buffer bucket router.
    body.push_str(
        "# HELP beava_ring_buffer_drops_total Events rejected by the sliding-window \
         ring buffer, labelled by reason (too_old | too_new | pre_epoch)\n\
         # TYPE beava_ring_buffer_drops_total counter\n",
    );
    {
        let engine = state.engine.read();
        let rb_escape = |s: &str| s.replace('\\', "\\\\").replace('"', "\\\"");
        let drops = engine.ring_buffer_drops.snapshot();
        for ((stream, operator_kind, reason), count) in drops {
            body.push_str(&format!(
                "beava_ring_buffer_drops_total{{stream=\"{}\",operator_kind=\"{}\",reason=\"{}\"}} {}\n",
                rb_escape(&stream),
                rb_escape(&operator_kind),
                reason.as_label(),
                count
            ));
        }
    }

    // Phase 25-02: TTL eviction + history retention counters.
    let escape = |s: &str| s.replace('\\', "\\\\").replace('"', "\\\"");
    body.push_str(
        "# HELP beava_ttl_evictions_total TTL-triggered evictions per Table\n\
         # TYPE beava_ttl_evictions_total counter\n",
    );
    for (table, count) in state.eviction_tracker.evictions_snapshot() {
        body.push_str(&format!(
            "beava_ttl_evictions_total{{table=\"{}\"}} {}\n",
            escape(&table),
            count
        ));
    }
    body.push_str(
        "# HELP beava_ttl_eviction_then_reinit_total Keys evicted by TTL then \
         re-observed within the bloom window (signal: TTL too short)\n\
         # TYPE beava_ttl_eviction_then_reinit_total counter\n",
    );
    for (table, count) in state.eviction_tracker.reinits_snapshot() {
        body.push_str(&format!(
            "beava_ttl_eviction_then_reinit_total{{table=\"{}\"}} {}\n",
            escape(&table),
            count
        ));
    }
    body.push_str(
        "# HELP beava_bloom_memory_bytes Memory held by per-Table eviction bloom filters\n\
         # TYPE beava_bloom_memory_bytes gauge\n",
    );
    body.push_str(&format!(
        "beava_bloom_memory_bytes {}\n",
        state.eviction_tracker.memory_bytes()
    ));
    {
        let m = state.metrics.lock();
        body.push_str(
            "# HELP beava_history_compacted_total History-log compactions per stream that removed ≥1 entry\n\
             # TYPE beava_history_compacted_total counter\n",
        );
        for (stream, count) in m.history_compacted_total.iter() {
            body.push_str(&format!(
                "beava_history_compacted_total{{stream=\"{}\"}} {}\n",
                escape(stream),
                count
            ));
        }
        body.push_str(
            "# HELP beava_history_backfill_misses_total Backfill requests whose window \
             straddled the compaction floor\n\
             # TYPE beava_history_backfill_misses_total counter\n",
        );
        for (stream, count) in m.history_backfill_misses_total.iter() {
            body.push_str(&format!(
                "beava_history_backfill_misses_total{{stream=\"{}\"}} {}\n",
                escape(stream),
                count
            ));
        }
        body.push_str(
            "# HELP beava_max_backfill_span_seen Largest observed backfill span per stream (seconds)\n\
             # TYPE beava_max_backfill_span_seen gauge\n",
        );
        for (stream, span) in m.max_backfill_span_seen.iter() {
            body.push_str(&format!(
                "beava_max_backfill_span_seen{{stream=\"{}\"}} {}\n",
                escape(stream),
                span
            ));
        }
    }
    // -------------------------------------------------------------
    // Phase 27-01 + 27-02: replica metric surface.
    //   - beava_replica_snapshot_bytes_sent_total  (counter, 27-01)
    //   - beava_replica_subscriptions_active       (gauge,   27-02)
    //   - beava_replica_events_pushed_total{stream}(counter, 27-02)
    //   - beava_replica_subscribers_dropped_total{reason}(counter, 27-02)
    // -------------------------------------------------------------
    body.push_str(
        "# HELP beava_replica_snapshot_bytes_sent_total Total bytes written as \
         OP_SNAPSHOT_FETCH payload-frame bodies\n\
         # TYPE beava_replica_snapshot_bytes_sent_total counter\n",
    );
    body.push_str(&format!(
        "beava_replica_snapshot_bytes_sent_total {}\n",
        crate::server::replica::snapshot_bytes_sent_total()
    ));
    body.push_str(
        "# HELP beava_replica_subscriptions_active Currently-active OP_SUBSCRIBE sessions\n\
         # TYPE beava_replica_subscriptions_active gauge\n",
    );
    body.push_str(&format!(
        "beava_replica_subscriptions_active {}\n",
        state.subscriber_registry.active_count()
    ));
    body.push_str(
        "# HELP beava_replica_events_pushed_total Events delivered over \
         OP_SUBSCRIBE sockets, per stream\n\
         # TYPE beava_replica_events_pushed_total counter\n",
    );
    for (stream, count) in crate::server::replica::events_pushed_snapshot() {
        body.push_str(&format!(
            "beava_replica_events_pushed_total{{stream=\"{}\"}} {}\n",
            escape(&stream),
            count
        ));
    }
    body.push_str(
        "# HELP beava_replica_subscribers_dropped_total OP_SUBSCRIBE subscribers \
         dropped, labelled by reason\n\
         # TYPE beava_replica_subscribers_dropped_total counter\n",
    );
    for (reason, count) in crate::server::replica::subscribers_dropped_snapshot() {
        body.push_str(&format!(
            "beava_replica_subscribers_dropped_total{{reason=\"{}\"}} {}\n",
            reason, count
        ));
    }

    // Phase 51-02: cross-shard scatter-gather fanout counters.
    body.push_str(
        "# HELP beava_cross_shard_fanout_total Scatter-gather operations dispatched \
         across shards, labelled by op\n\
         # TYPE beava_cross_shard_fanout_total counter\n",
    );
    body.push_str(&format!(
        "beava_cross_shard_fanout_total{{op=\"list_streams\"}} {}\n",
        CROSS_SHARD_FANOUT_LIST_STREAMS.load(Ordering::Relaxed)
    ));

    // Phase 50-01 (D-06): Append metrics-exporter-prometheus scrape output in parallel
    // with the hand-rolled text above. If no metrics recorder is installed (e.g. in tests
    // that don't call install_prometheus_recorder), this is a no-op.
    if let Some(handle) = crate::metrics::handle() {
        let prom_output = handle.scrape();
        if !prom_output.is_empty() {
            body.push('\n');
            body.push_str(&prom_output);
        }
    }

    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        body,
    )
}

// ========================================================================
// Phase 20: Public read-only surface (TRAC-04).
// ========================================================================

/// Query parameters for `GET /public/recent-events`.
#[cfg(feature = "demo")]
#[derive(Debug, serde::Deserialize, Default)]
struct RecentEventsQuery {
    /// Maximum number of events to return. Clamped to [1, 100]; default 20.
    limit: Option<usize>,
}

/// `GET /public/features/{key}` — return the computed feature map for one key.
///
/// SECURITY (TRAC-04): response MUST NOT expose operator internal state —
/// no bucket arrays, no HLL bitmaps, no operator type names. We only include
/// feature NAME -> VALUE (scalar / string / number / null). Contrast with
/// `/debug/key/{key}` which is admin-gated and exposes everything.
async fn public_features(
    State(state): State<SharedState>,
    Path(key): Path<String>,
) -> impl IntoResponse {
    let _ = SystemTime::now(); // now is computed per-shard; kept here for parity

    // Phase 54-03 Task 3: read features via the owner shard's SPSC inbox
    // instead of reading `state.store` directly. Existence + feature-map
    // come back in a single ShardResult::GetOk so 404 is race-free.
    let shard_count = state.shard_handles.read().len();
    let shard_idx = shard_index_for_key(&key, shard_count);
    let handle_clone = {
        let handles = state.shard_handles.read();
        match handles.get(shard_idx) {
            Some(h) => crate::shard::thread::ShardHandle {
                shard_index: h.shard_index,
                is_down: std::sync::Arc::clone(&h.is_down),
                inbox_tx: h.inbox_tx.clone(),
            },
            None => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    cors_headers(),
                    Json(serde_json::json!({"error": "no shards registered"})),
                )
                    .into_response();
            }
        }
    };

    match crate::shard::thread::get_features_via_shard(&handle_clone, key.clone()).await {
        Ok((false, _)) => (
            StatusCode::NOT_FOUND,
            cors_headers(),
            Json(serde_json::json!({"error": "key not found"})),
        )
            .into_response(),
        Ok((true, features)) => {
            let feature_json: serde_json::Map<String, serde_json::Value> = features
                .iter()
                .map(|(k, v)| (k.clone(), v.to_json_value()))
                .collect();
            (
                StatusCode::OK,
                cors_headers(),
                Json(serde_json::json!({
                    "key": key,
                    "features": feature_json,
                })),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            cors_headers(),
            Json(serde_json::json!({"error": format!("shard dispatch: {:?}", e)})),
        )
            .into_response(),
    }
}

/// Phase 54-03 Task 3: compute the owner shard for a bare key string.
/// Mirrors the routing used by `shard_hint_for_event` (ahash of the key),
/// but takes the key directly (no JSON payload extraction).
pub(crate) fn shard_index_for_key(key: &str, shard_count: usize) -> usize {
    if shard_count <= 1 {
        return 0;
    }
    use std::hash::{Hash, Hasher};
    let mut hasher = ahash::AHasher::default();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % shard_count
}

/// `GET /public/recent-events?limit=N` — tail of the in-memory recent-events
/// ring. Defaults to 20 events, clamped to the ring capacity (100).
///
/// Phase 41-01 T1: gated behind `feature = "demo"`. Default server build
/// does not compile this handler; the `/public/recent-events` route is
/// also not registered, so requests return 404.
#[cfg(feature = "demo")]
async fn public_recent_events(
    State(state): State<SharedState>,
    Query(q): Query<RecentEventsQuery>,
) -> impl IntoResponse {
    let limit = q
        .limit
        .unwrap_or(20)
        .clamp(1, crate::server::tcp::RecentEventsRing::CAPACITY);
    let events = state.recent_events.lock().snapshot(limit);
    let events_json: Vec<serde_json::Value> = events
        .into_iter()
        .map(|e| {
            serde_json::json!({
                "ts": e.ts_ms,
                "stream": e.stream,
                "key": e.key,
                "payload_preview": e.payload_preview,
            })
        })
        .collect();
    (
        StatusCode::OK,
        cors_headers(),
        Json(serde_json::json!({"events": events_json})),
    )
        .into_response()
}

/// `GET /public/stats` — aggregate counters for the demo page tiles.
async fn public_stats(State(state): State<SharedState>) -> impl IntoResponse {
    // Phase 54-04 Pass A2: scatter-gather `keys_total` across all
    // shards (mirror of `metrics_endpoint`). Per-shard failures
    // degrade to 0 so the public demo tiles keep rendering even
    // during a shard outage. Done before locking the parking_lot
    // `latency` mutex so no `!Send` guard can possibly cross the
    // await point.
    let keys_total: usize = {
        let handle_clones: Vec<crate::shard::thread::ShardHandle> = {
            let handles = state.shard_handles.read();
            handles
                .iter()
                .map(crate::shard::thread::clone_handle)
                .collect()
        };
        let mut total = 0usize;
        for h in &handle_clones {
            if let Ok(n) = crate::shard::thread::entity_count_via_shard(h).await {
                total = total.saturating_add(n);
            }
        }
        total
    };
    // Phase 41-01 T2+T3: lock-free reads.
    let events_total = state
        .events_total
        .load(std::sync::atomic::Ordering::Relaxed);
    let current_eps = state.atomic_throughput.eps_5s();
    let now_inst = std::time::Instant::now();
    let latency = state.latency.lock();
    let p99_push_us = latency.push_percentile_us(99.0, now_inst);
    let p50_push_us = latency.push_percentile_us(50.0, now_inst);
    drop(latency);
    let uptime_seconds = state.started_at.elapsed().as_secs();
    (
        StatusCode::OK,
        cors_headers(),
        Json(serde_json::json!({
            "events_total":    events_total,
            "current_eps":     current_eps,
            "p99_push_us":     p99_push_us,
            "p50_push_us":     p50_push_us,
            "uptime_seconds":  uptime_seconds,
            "keys_total":      keys_total,
        })),
    )
        .into_response()
}

/// `GET /` dispatch: public demo page when `public_mode=true`, debug UI
/// otherwise. Keeps the existing `/static/*` handler unchanged — both pages
/// load their assets from the same embed root.
async fn root_dispatch(State(state): State<SharedState>) -> axum::response::Response {
    if state.public_mode {
        match UiAssets::get("demo.html") {
            Some(content) => {
                let body = String::from_utf8_lossy(&content.data).to_string();
                (
                    StatusCode::OK,
                    [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
                    body,
                )
                    .into_response()
            }
            None => (StatusCode::NOT_FOUND, "demo.html not embedded").into_response(),
        }
    } else {
        ui_index().await
    }
}

async fn debug_key(State(state): State<SharedState>, Path(key): Path<String>) -> impl IntoResponse {
    // Phase 54-04 Pass A6a: `AppState.store` deleted. The per-key debug view
    // needs a shard-lookup round-trip (`ShardOp::DebugKey`) which lands in
    // Pass A7 / Pass B. Until then return a stub body — the endpoint stays
    // live so operators can still inspect watermarks + stream ordinals, but
    // per-entity fields are empty.
    let _now = SystemTime::now();
    let live_ops: Vec<serde_json::Value> = Vec::new();
    let static_feats: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
    let last_event_at: Option<u64> = None;
    let total_estimated_bytes: u64 = 0;
    let feature_json: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
    // Phase 24-04: per-stream watermarks visible on /debug/key/:key.
    // The watermark map is stream-level state (not per-entity) but it's
    // the most useful place to surface it for ad-hoc debugging, alongside
    // the other per-key snapshots.
    let watermarks_json: serde_json::Map<String, serde_json::Value> = {
        let engine = state.engine.read();
        let mut out = serde_json::Map::new();
        for (stream, max) in engine.wm_iter_streams() {
            if let Some(wm) = engine.wm_watermark(&stream) {
                let ms = wm
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                let max_ms = max
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                out.insert(
                    stream.clone(),
                    serde_json::json!({
                        "watermark_ms":    ms,
                        "observed_max_ms": max_ms,
                    }),
                );
            }
        }
        out
    };
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "key": key,
            "live_operators": live_ops,
            "static_features": static_feats,
            "computed_features": feature_json,
            "last_event_at": last_event_at,
            "estimated_bytes": total_estimated_bytes,
            "watermarks": watermarks_json,
        })),
    )
        .into_response()
}

/// Phase 24-04: `GET /debug/streams/{name}` — surface the full
/// per-stream watermark state. 404 when the stream has not been
/// observed (either unregistered or no events yet).
async fn debug_stream(
    State(state): State<SharedState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let engine = state.engine.read();
    let observed_max = match engine.wm_observed_max(&name) {
        Some(m) => m,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "error": format!("stream '{}' has no watermark state", name),
                })),
            )
                .into_response();
        }
    };
    let wm = engine.wm_watermark(&name).unwrap_or(observed_max);
    let last_event = engine.wm_last_event_time(&name).unwrap_or(observed_max);
    let late_drops = engine.late_drops.get(&name);

    let to_ms = |t: std::time::SystemTime| -> u64 {
        t.duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    };
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "name":                 name,
            "watermark_ms":         to_ms(wm),
            "observed_max_ms":      to_ms(observed_max),
            "last_event_time_ms":   to_ms(last_event),
            "lateness_seconds":     crate::engine::event_time::WATERMARK_LATENESS.as_secs(),
            "late_events_dropped":  late_drops,
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

/// GET /debug/pprof?secs=N — CPU sample profile via pprof-rs. Writes a
/// flamegraph SVG to /tmp/beava-pprof-{now}.svg and returns its path. N
/// defaults to 10. Sampling frequency fixed at 997 Hz (common prime).
#[cfg(feature = "pprof-endpoint")]
async fn debug_pprof(Query(params): Query<std::collections::HashMap<String, String>>) -> Json<serde_json::Value> {
    let secs: u64 = params.get("secs").and_then(|s| s.parse().ok()).unwrap_or(10);
    let freq: i32 = 997;
    let guard = match pprof::ProfilerGuardBuilder::default()
        .frequency(freq)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
    {
        Ok(g) => g,
        Err(e) => {
            return Json(serde_json::json!({"error": format!("ProfilerGuard build failed: {}", e)}));
        }
    };
    tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
    let report = match guard.report().build() {
        Ok(r) => r,
        Err(e) => {
            return Json(serde_json::json!({"error": format!("report build failed: {}", e)}));
        }
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let svg_path = format!("/tmp/beava-pprof-{}.svg", now);
    let txt_path = format!("/tmp/beava-pprof-{}.txt", now);
    let flamegraph_result = {
        let f = std::fs::File::create(&svg_path);
        match f {
            Ok(f) => report.flamegraph(f).map_err(|e| e.to_string()),
            Err(e) => Err(e.to_string()),
        }
    };
    let top_lines: Vec<String> = {
        let mut entries: Vec<(String, isize)> = report
            .data
            .iter()
            .map(|(frames, count)| {
                // frames.frames: &Vec<Vec<Symbol>> — each Vec<Symbol> is inlined
                // frame chain; take the innermost Symbol of the top stack frame.
                let name = frames
                    .frames
                    .first()
                    .and_then(|chain: &Vec<pprof::Symbol>| chain.first())
                    .map(|sym: &pprof::Symbol| sym.name())
                    .unwrap_or_else(|| "<unknown>".to_string());
                (name, *count as isize)
            })
            .collect();
        entries.sort_by(|a, b| b.1.cmp(&a.1));
        entries.into_iter().take(40).map(|(n, c)| format!("{:>8}  {}", c, n)).collect()
    };
    let _ = std::fs::write(&txt_path, top_lines.join("\n"));
    Json(serde_json::json!({
        "secs": secs,
        "frequency_hz": freq,
        "flamegraph": svg_path,
        "top40_txt": txt_path,
        "flamegraph_result": match flamegraph_result { Ok(_) => "ok".to_string(), Err(e) => e },
        "sample_count": report.data.values().map(|c| *c as usize).sum::<usize>(),
        "top5": top_lines.iter().take(5).cloned().collect::<Vec<_>>(),
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

async fn debug_shard_probe(State(_state): State<SharedState>) -> Json<serde_json::Value> {
    let snap = crate::server::shard_probe::snapshot();
    Json(serde_json::to_value(&snap).unwrap_or(serde_json::Value::Null))
}

/// Phase 51-02: cross-shard fanout counter incremented by /streams handler.
/// Exported via /metrics as beava_cross_shard_fanout_total{op="list_streams"}.
pub(crate) static CROSS_SHARD_FANOUT_LIST_STREAMS: AtomicU64 = AtomicU64::new(0);

/// Phase 51-03: `GET /debug/shards` — per-shard diagnostics (D-09 schema).
async fn debug_shards(State(state): State<SharedState>) -> Json<serde_json::Value> {
    use crate::server::shard_probe::{collect_shard_diagnostics, HotShardConfig};
    let config = HotShardConfig::from_env();
    let report = collect_shard_diagnostics(&state, &config);
    Json(serde_json::to_value(&report).unwrap_or(serde_json::Value::Null))
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
///
/// Phase 54-04 Pass A6a: the `AppState.store` DashMap is gone. The per-entity
/// scatter-gather over shards (`ShardOp::MemoryStats`) lands in Pass A7 / Pass
/// B. Until then, per-stream stats reflect an empty accumulator on the default
/// (fjall) build; the route keeps responding so dashboards don't 404.
async fn debug_memory(State(state): State<SharedState>) -> Json<serde_json::Value> {
    let engine = state.engine.read();

    // Accumulate per-stream stats by iterating all entity state.
    // Phase 54-04 Pass A6a: DashMap iteration removed — empty accumulator.
    let stream_stats: ahash::AHashMap<String, StreamMemoryStats> = ahash::AHashMap::new();
    let total_static_bytes: u64 = 0;
    let total_static_features: u64 = 0;

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

    // Phase 54-04 Pass A6a: `AppState.store` deleted; entity_count needs a
    // shard-fan-out (Pass A7). Report 0 until then.
    let entity_count: usize = 0;
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
    //
    // Phase 54-04 Pass A6a: `AppState.store` deleted. Entity dump migrates to a
    // per-shard scatter-gather (`ShardOp::SnapshotEntities`) in Pass A7 / Pass
    // B; until then the manual snapshot writes an empty `entities` vec so the
    // endpoint keeps responding without corrupting on-disk state.
    let (snapshot_data, seq, snap_dir) = {
        let engine = state.engine.read();
        let seq = *state.snapshot_seq.lock();
        let entities: Vec<(String, crate::state::snapshot::SerializableEntityState)> = Vec::new();
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
                schema_version: 9,
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
        let filename = format!("beava.snapshot.base.{:010}", seq);
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

/// Phase 25-02: `GET /debug/config-recommendations` — surface
/// TTL/history_ttl suggestions derived from observed eviction-then-reinit
/// signals. Admin-gated (loopback-or-token).
///
/// Wire schema (see `25-CONTEXT.md §Suggestion engine`):
/// `{ "generated_at": RFC3339, "observation_window": "7d",
///    "recommendations": [ConfigRecommendation, ...] }`
async fn debug_config_recommendations(State(state): State<SharedState>) -> Json<serde_json::Value> {
    let engine = state.engine.read();
    let recs = crate::engine::recommend::recommend_config(&engine, &state.eviction_tracker);
    drop(engine);
    let now_rfc3339 = {
        let secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        format_rfc3339_utc(secs)
    };
    Json(serde_json::json!({
        "generated_at": now_rfc3339,
        "observation_window": "7d",
        "recommendations": recs,
    }))
}

/// Phase 25-02: `GET /debug/warnings` — unified severity-sorted feed of
/// live signals from every warning source (REGISTER / snapshot /
/// memory-pressure / late-drop / perf / config). Admin-gated.
///
/// Query params:
///   - `category=config|data_quality|operational|safety|performance` —
///     optional filter; invalid values return the full unfiltered list.
///
/// Response shape (frozen per `25-CONTEXT.md §decisions`):
/// ```json
/// { "generated_at": RFC3339,
///   "observation_window": "7d",
///   "warnings": [ {id, severity, category, title, detail, action?,
///                  first_seen, last_seen, evidence}, ... ] }
/// ```
async fn debug_warnings(
    State(state): State<SharedState>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Json<serde_json::Value> {
    let category = params
        .get("category")
        .and_then(|s| crate::server::signals::Category::parse(s));
    let now = std::time::SystemTime::now();
    // Age out stale entries before snapshotting.
    state.signals.write().age_out(now);
    let warnings = state.signals.read().snapshot_sorted(now, category);
    // Phase 56 D-C1: structured `cross_shard_joins` sibling field surfaced
    // alongside the flat `warnings` signal feed. Back-compat with Phase 51
    // tests that treat `warnings` as a flat array is preserved — the new
    // field is additive. Each warning also appears in the `warnings` feed
    // as a `Category::Safety` / `Severity::Warning` signal (see
    // `emit_cross_shard_join_warning`).
    let cross_shard_joins = state.signals.read().cross_shard_joins_snapshot();
    // Phase 57 Wave 3 (TPC-CORR-10): surface the retraction-beyond-history
    // warning array as a sibling field (same shape as cross_shard_joins).
    let retraction_beyond_history =
        state.signals.read().retraction_beyond_history_snapshot();
    let now_rfc3339 = {
        let secs = now
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        format_rfc3339_utc(secs)
    };
    Json(serde_json::json!({
        "generated_at": now_rfc3339,
        "observation_window": "7d",
        "warnings": warnings,
        "cross_shard_joins": cross_shard_joins,
        "retraction_beyond_history": retraction_beyond_history,
    }))
}

/// Minimal RFC3339 / ISO 8601 formatter for a UTC unix-seconds timestamp.
/// Produces e.g. `2026-04-14T21:59:53Z`. Avoids a chrono dependency.
fn format_rfc3339_utc(secs: u64) -> String {
    // Gregorian conversion (standard algorithm).
    let days = (secs / 86400) as i64;
    let sod = (secs % 86400) as u32;
    let h = sod / 3600;
    let m = (sod % 3600) / 60;
    let s = sod % 60;
    // Days since 1970-01-01 → y-m-d via Howard Hinnant's algorithm.
    let z = days + 719_468;
    let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m_cal = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = y + if m_cal <= 2 { 1 } else { 0 };
    format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", y, m_cal, d, h, m, s)
}

/// Phase 44-01: `GET /extracts` — return the historical-extraction registry
/// captured during replica catchup (see `--replica-extract-at` /
/// `beava fork --extract-at`).
///
/// Response shape (timestamps ISO-8601 UTC, sorted ascending):
/// ```json
/// {
///   "extracts": {
///     "2026-03-01T10:00:00Z": {"u1": {"count": 1, "total": 10.0}, ...},
///     "2026-03-15T10:00:00Z": {...}
///   }
/// }
/// ```
/// Empty object when no extractions were requested or replay has not yet
/// captured any.
async fn debug_extracts(State(state): State<SharedState>) -> Json<serde_json::Value> {
    // Phase 54-03 Task 2: `extracted_history` is now a single RwLock<AHashMap>,
    // collect into a Vec so we can sort by timestamp (AHashMap iteration
    // order is unspecified).
    let mut entries: Vec<(u64, serde_json::Map<String, serde_json::Value>)> = {
        let guard = state.extracted_history.read();
        guard
            .iter()
            .map(|(ts, inner)| {
                let mut key_map = serde_json::Map::with_capacity(inner.len());
                for (k, v) in inner.iter() {
                    key_map.insert(k.clone(), v.clone());
                }
                (*ts, key_map)
            })
            .collect()
    };
    entries.sort_by_key(|(ts, _)| *ts);

    let mut out = serde_json::Map::with_capacity(entries.len());
    for (ts, key_map) in entries {
        // Same ISO-8601 formatter the warnings/config-recs endpoints use —
        // the v0 shape operates in whole seconds; sub-second precision is
        // dropped (extract_at granularity is defined in whole seconds for
        // the demo, see docs/data-scientist-demo.md).
        let secs = ts / 1000;
        let iso = format_rfc3339_utc(secs);
        out.insert(iso, serde_json::Value::Object(key_map));
    }
    Json(serde_json::json!({"extracts": serde_json::Value::Object(out)}))
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

/// Phase 20: assemble the full HTTP router by MERGING a public sub-router with
/// an admin sub-router, the latter gated by `require_loopback_or_token`.
///
/// Public (ungated) routes:
///   - `GET /health`, `GET /metrics`
///   - `GET /public/features/{key}`, `GET /public/recent-events`, `GET /public/stats`
///   - `GET /`, `GET /static/{*file}`
///
/// Admin (gated) routes:
///   - `GET|POST /pipelines`, `GET|DELETE /pipelines/{name}`
///   - `POST /snapshot`
///   - `GET /debug/{key,memory,backfill,topology,throughput,latency}`
pub fn build_router(state: SharedState) -> Router {
    // Phase 41-01 T1: the `/public/recent-events` route + handler are only
    // compiled under `--features demo`. Default builds return 404 — the
    // route is simply not registered.
    let public_router = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/debug/ready", get(debug_ready))
        .route("/metrics", get(metrics_endpoint))
        .route("/public/features/{key}", get(public_features))
        .route("/public/stats", get(public_stats))
        .route("/", get(root_dispatch))
        .route("/static/{*file}", get(ui_static));
    #[cfg(feature = "demo")]
    let public_router = public_router.route("/public/recent-events", get(public_recent_events));

    let admin_router = Router::new()
        .route("/pipelines", get(list_pipelines).post(create_pipeline))
        .route(
            "/pipelines/{name}",
            get(get_pipeline).delete(delete_pipeline),
        )
        .route("/debug/key/{key}", get(debug_key))
        .route("/debug/streams/{name}", get(debug_stream))
        .route("/debug/memory", get(debug_memory))
        .route("/debug/backfill", get(debug_backfill))
        .route("/extracts", get(debug_extracts))
        .route(
            "/debug/config-recommendations",
            get(debug_config_recommendations),
        )
        .route("/debug/warnings", get(debug_warnings))
        .route("/debug/topology", get(debug_topology))
        .route("/debug/throughput", get(debug_throughput));

    #[cfg(feature = "pprof-endpoint")]
    let admin_router = admin_router.route("/debug/pprof", get(debug_pprof));

    let admin_router = admin_router
        .route("/debug/latency", get(debug_latency))
        .route("/debug/shard_probe", get(debug_shard_probe))
        .route("/debug/shards", get(debug_shards))
        .route("/snapshot", post(trigger_snapshot));

    // Phase 45: register HTTP ingest + read routes.
    // public_mode toggles read-route placement (D-03 / HTTP-07).
    // MUST be called BEFORE .route_layer(require_loopback_or_token) so that
    // the auth layer covers the new write routes (Pitfall 15 prevention).
    let public_mode = state.public_mode;
    let (public_router, admin_router) = crate::server::http_ingest::register_ingest_routes(
        public_router,
        admin_router,
        public_mode,
    );

    let admin_router = admin_router.route_layer(middleware::from_fn_with_state(
        state.clone(),
        require_loopback_or_token,
    ));

    public_router.merge(admin_router).with_state(state)
}

/// Phase 50-05 (D-09): build a per-shard axum Router.
///
/// Identical to `build_router` — same middleware stack including
/// `require_loopback_or_token` applied in the same position. The shard_index
/// parameter is reserved for future per-shard state routing; currently the
/// full SharedState is shared across shards (Wave 2 transition period).
///
/// On Linux, one AxumServerSet instance is created per shard with its own
/// SO_REUSEPORT socket. On macOS, a single server serves all shards (D-04).
pub fn build_shard_router(state: SharedState, _shard_index: usize) -> Router {
    // D-09: identical middleware stack on every per-shard router.
    // require_loopback_or_token MUST be applied — no auth regression.
    build_router(state)
}

/// Start the HTTP management server on the given address.
///
/// Phase 20: uses `into_make_service_with_connect_info::<SocketAddr>()` so the
/// `ConnectInfo<SocketAddr>` extractor in `require_loopback_or_token` works at
/// runtime. Tests that exercise the router via `oneshot` inject this extension
/// manually (see `tests/test_admin_auth.rs`).
pub async fn run_http_server(addr: &str, state: SharedState) -> Result<(), std::io::Error> {
    let app = build_router(state);
    let listener = TcpListener::bind(addr).await?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .map_err(std::io::Error::other)
}

/// Start the HTTP management server from a pre-bound listener (for tests).
pub async fn run_http_server_with_listener(
    listener: TcpListener,
    state: SharedState,
) -> Result<(), std::io::Error> {
    let app = build_router(state);
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .map_err(std::io::Error::other)
}

// ======================== Phase 15 Tests ========================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::pipeline::PipelineEngine;
    use crate::server::tcp::{make_concurrent_state, BackfillTracker};
    use std::sync::Arc;

    fn test_state() -> SharedState {
        make_concurrent_state(
            PipelineEngine::new(),
            None,
            std::path::PathBuf::from("/tmp/beava-test-snapshot"),
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

    /// Phase 20: admin routes now require a `ConnectInfo<SocketAddr>` extension
    /// (populated by axum via `into_make_service_with_connect_info`). Tests
    /// using `oneshot` must insert one manually. Loopback peers bypass the
    /// token check, keeping the test setup minimal.
    fn inject_loopback(req: &mut axum::http::Request<axum::body::Body>) {
        use axum::extract::ConnectInfo;
        use std::net::SocketAddr;
        let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
        req.extensions_mut().insert(ConnectInfo(addr));
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
        let mut req = Request::builder()
            .method("POST")
            .uri("/snapshot")
            .body(Body::empty())
            .unwrap();
        inject_loopback(&mut req);

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
            None,
            std::path::PathBuf::from("/tmp/beava-test-snapshot"),
            Arc::new(BackfillTracker::default()),
            false, // snapshots disabled
            false,
        );

        let app = build_router(state);
        let mut req = Request::builder()
            .method("POST")
            .uri("/snapshot")
            .body(Body::empty())
            .unwrap();
        inject_loopback(&mut req);

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
            text.contains("beava_snapshots_skipped_total 42"),
            "metrics body: {}",
            text
        );
    }
}
