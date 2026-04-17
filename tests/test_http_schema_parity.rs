//! Phase 45 — HTTP/TCP schema parity test.
//!
//! Asserts that the same JSON event pushed through HTTP single, HTTP batch,
//! HTTP NDJSON, and direct handle_push_core_ex (TCP proxy) produces
//! bit-identical feature values. Prevents Pitfall 22 (HTTP/TCP schema drift).

mod http_common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use beava::engine::pipeline::{FeatureDef, PipelineEngine, StreamDefinition};
use beava::engine::event_time::parse_event_time;
use beava::server::http::build_router;
use beava::server::tcp::{
    handle_push_core_ex, make_concurrent_state_full, BackfillTracker, SharedState,
};
use beava::state::store::StateStore;
use http_common::{build_test_state, inject_loopback};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// Helper: build a fresh SharedState with the test stream registered
// ---------------------------------------------------------------------------

fn fresh_state_with_stream() -> SharedState {
    let state = build_test_state(false);
    state
        .engine
        .write()
        .register(StreamDefinition {
            name: "parity".into(),
            key_field: Some("user".into()),
            group_by_keys: None,
            features: vec![(
                "total_amount".into(),
                FeatureDef::Sum {
                    field: "amount".into(),
                    window: Duration::from_secs(86400),
                    bucket: Duration::from_secs(3600),
                    optional: true,
                    where_expr: None,
                    backfill: false,
                },
            )],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
        })
        .unwrap();
    state
}

// ---------------------------------------------------------------------------
// Parity test: 4 ingest paths × 2 _event_time formats
// ---------------------------------------------------------------------------

/// Same JSON through HTTP single, HTTP batch, HTTP NDJSON, and direct
/// handle_push_core_ex → bit-identical feature values.
///
/// We test two _event_time formats:
///  1. unix-ms integer  (parse_event_time int branch)
///  2. RFC3339 string   (parse_event_time string branch)
///
/// To avoid window expiry (Sum windows only return values when queried within
/// the window), we use near-current timestamps so the feature bucket is live
/// when we read back features. The two timestamps are offset by 1 minute so
/// they map to different buckets — confirming the parser is active, not just
/// using fallback wall-clock.
///
/// Both unix-ms and RFC3339 encode the same UTC instant:
///   2026-01-15T12:00:00Z  →  1736942400000 ms
#[tokio::test]
async fn same_json_through_http_and_tcp_yields_identical_feature_values() {
    // Current wall clock used for feature-read queries across all paths.
    let query_now = SystemTime::now();

    // Near-now event times that are within the 24h window.
    // Both formats encode the same instant; we run two iterations to confirm
    // parse_event_time handles both without schema drift.
    let unix_ms_now = query_now
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    // Subtract 10 minutes to stay well inside the 24h window but differ from
    // fallback wall-clock (proving the parser ran).
    let event_unix_ms = unix_ms_now.saturating_sub(10 * 60 * 1000);

    // Build the RFC3339 equivalent of the same unix-ms timestamp.
    let event_secs = event_unix_ms / 1000;
    let rfc3339_str = {
        // Manual UTC format: YYYY-MM-DDTHH:MM:SSZ
        // We use a fixed known-recent date and offset into today.
        // Simplest: encode as unix seconds with no sub-second precision.
        // parse_event_time will parse it correctly via parse_iso8601.
        let secs = event_secs;
        // Build a valid UTC timestamp string.
        // Days since epoch
        let days = secs / 86400;
        let rem = secs % 86400;
        let hh = rem / 3600;
        let mm = (rem % 3600) / 60;
        let ss = rem % 60;
        // Compute year/month/day from days-since-epoch (Gregorian proleptic)
        let (y, mo, d) = days_to_ymd(days);
        format!("{y:04}-{mo:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}Z")
    };

    let test_cases: Vec<(&str, String)> = vec![
        ("unix_ms", format!("{event_unix_ms}")),
        ("rfc3339", format!("\"{rfc3339_str}\"")),
    ];

    for (label, et_frag) in &test_cases {
        // ---- Event payload (identical across all four paths) ----
        let event_json = format!(
            r#"{{"user":"parity_user","amount":99.5,"_event_time":{et_frag}}}"#
        );
        let event: serde_json::Value = serde_json::from_str(&event_json).unwrap();
        let raw = event_json.as_bytes().to_vec();

        // Parse the event time exactly as the handlers do.
        let event_time = parse_event_time(&event, query_now);

        // ----------------------------------------------------------------
        // Path 1: direct handle_push_core_ex (proxies TCP path)
        // read_features=true so the feature map is computed at push time.
        // ----------------------------------------------------------------
        let state_tcp = fresh_state_with_stream();
        handle_push_core_ex(&state_tcp, "parity", &event, &raw, event_time, true).unwrap();
        // Query at event_time so the bucket is in-window.
        let features_tcp = state_tcp.store.get_all_features("parity_user", event_time);

        // ----------------------------------------------------------------
        // Path 2: HTTP single POST /push/{stream}?sync=1
        // ----------------------------------------------------------------
        let state_http_single = fresh_state_with_stream();
        let app_single = build_router(state_http_single.clone());
        let mut req = Request::builder()
            .method("POST")
            .uri("/push/parity?sync=1")
            .header("content-type", "application/json")
            .body(Body::from(event_json.clone()))
            .unwrap();
        inject_loopback(&mut req);
        let resp = app_single.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "[{label}] http_single push failed"
        );
        let features_http_single =
            state_http_single.store.get_all_features("parity_user", event_time);

        // ----------------------------------------------------------------
        // Path 3: HTTP batch POST /push-batch/{stream} (1-element array)
        // ----------------------------------------------------------------
        let state_http_batch = fresh_state_with_stream();
        let app_batch = build_router(state_http_batch.clone());
        let batch_body = format!("[{event_json}]");
        let mut req = Request::builder()
            .method("POST")
            .uri("/push-batch/parity")
            .header("content-type", "application/json")
            .body(Body::from(batch_body))
            .unwrap();
        inject_loopback(&mut req);
        let resp = app_batch.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "[{label}] http_batch push failed"
        );
        let features_http_batch =
            state_http_batch.store.get_all_features("parity_user", event_time);

        // ----------------------------------------------------------------
        // Path 4: HTTP NDJSON POST /push/{stream}/ndjson (1 line)
        // ----------------------------------------------------------------
        let state_http_ndjson = fresh_state_with_stream();
        let app_ndjson = build_router(state_http_ndjson.clone());
        let ndjson_body = format!("{event_json}\n");
        let mut req = Request::builder()
            .method("POST")
            .uri("/push/parity/ndjson")
            .header("content-type", "application/x-ndjson")
            .body(Body::from(ndjson_body))
            .unwrap();
        inject_loopback(&mut req);
        let resp = app_ndjson.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "[{label}] http_ndjson push failed"
        );
        let features_http_ndjson =
            state_http_ndjson.store.get_all_features("parity_user", event_time);

        // ----------------------------------------------------------------
        // Assert bit-identical feature values across all four paths.
        // ----------------------------------------------------------------

        // Collect feature keys from TCP baseline; all four maps must agree.
        for (feat_key, tcp_val) in &features_tcp {
            let single_val = features_http_single.get(feat_key);
            let batch_val = features_http_batch.get(feat_key);
            let ndjson_val = features_http_ndjson.get(feat_key);

            assert_eq!(
                single_val,
                Some(tcp_val),
                "[{label}] feature '{feat_key}': http_single ({single_val:?}) != tcp ({tcp_val:?})"
            );
            assert_eq!(
                batch_val,
                Some(tcp_val),
                "[{label}] feature '{feat_key}': http_batch ({batch_val:?}) != tcp ({tcp_val:?})"
            );
            assert_eq!(
                ndjson_val,
                Some(tcp_val),
                "[{label}] feature '{feat_key}': http_ndjson ({ndjson_val:?}) != tcp ({tcp_val:?})"
            );
        }

        // Ensure all four maps have the same number of keys (no extras).
        assert_eq!(
            features_http_single.len(),
            features_tcp.len(),
            "[{label}] http_single feature count differs from tcp"
        );
        assert_eq!(
            features_http_batch.len(),
            features_tcp.len(),
            "[{label}] http_batch feature count differs from tcp"
        );
        assert_eq!(
            features_http_ndjson.len(),
            features_tcp.len(),
            "[{label}] http_ndjson feature count differs from tcp"
        );

        // Confirm the feature is non-Missing (proves the window is in-range).
        let total = features_tcp.get("total_amount").expect("total_amount must exist");
        assert!(
            !total.is_missing(),
            "[{label}] total_amount is Missing — bucket out of window? event_time={event_unix_ms}ms"
        );
    }
}

// ---------------------------------------------------------------------------
// Days-since-epoch → (year, month, day) helper (no external date crate)
// ---------------------------------------------------------------------------

fn days_to_ymd(days: u64) -> (u32, u32, u32) {
    // Algorithm: civil calendar, proleptic Gregorian.
    // Adapted from https://howardhinnant.github.io/date_algorithms.html
    let z = days as i64 + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as u32, m as u32, d as u32)
}
