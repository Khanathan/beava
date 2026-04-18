//! Phase 45 — HTTP-03: NDJSON streaming ingest tests.
//!
//! Wave 1 (45-03): live handler via axum_extra::JsonLines.

mod http_common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use beava::engine::pipeline::{FeatureDef, PipelineEngine, StreamDefinition};
use beava::server::http::build_router;
use beava::server::tcp::{make_concurrent_state_full, BackfillTracker, SharedState};
use beava::state::store::StateStore;
use http_common::{build_test_state, inject_loopback};
use std::sync::Arc;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn register_events_stream(state: &SharedState) {
    state
        .engine
        .write()
        .register(StreamDefinition {
            name: "events".into(),
            key_field: Some("user".into()),
            group_by_keys: None,
            features: vec![(
                "total_amount".into(),
                FeatureDef::Sum {
                    field: "amount".into(),
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
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
            watermark_lateness: None,
        })
        .unwrap();
}

// ---------------------------------------------------------------------------
// HTTP-03 tests
// ---------------------------------------------------------------------------

/// 10 000 events sent as NDJSON across 10 logical chunks (each 1000 lines).
/// The handler flushes at CHUNK_SIZE=1000, so chunks >= 10.
#[tokio::test]
async fn ndjson_streams_10k_events_in_10_chunks() {
    let state = build_test_state(false);
    register_events_stream(&state);
    let app = build_router(state);

    // Build a 10 000-line NDJSON body. Each event has a distinct user key and
    // a monotonically increasing _event_time (ms since epoch, near-now).
    let base_ms: u64 = 1_700_000_000_000; // 2023-11-14
    let mut lines = String::with_capacity(10_000 * 50);
    for i in 0u64..10_000 {
        lines.push_str(&format!(
            "{{\"user\":\"u{i}\",\"amount\":1.0,\"_event_time\":{}}}\n",
            base_ms + i * 1000
        ));
    }

    let mut req = Request::builder()
        .method("POST")
        .uri("/push/events/ndjson")
        .header("content-type", "application/x-ndjson")
        .body(Body::from(lines))
        .unwrap();
    inject_loopback(&mut req);
    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["ok"], true);
    assert_eq!(
        v["data"]["accepted"], 10_000,
        "expected 10000 accepted: {v}"
    );
    assert_eq!(v["data"]["rejected"], 0, "expected 0 rejected: {v}");
    assert!(
        v["data"]["chunks"].as_u64().unwrap_or(0) >= 10,
        "expected >=10 chunks (flush every 1000): {v}"
    );
}

/// Single-chunk 3-event NDJSON body — response must have exactly the four
/// `data` keys: accepted, rejected, chunks, first_error.
#[tokio::test]
async fn ndjson_summary_response_shape() {
    let state = build_test_state(false);
    register_events_stream(&state);
    let app = build_router(state);

    let ndjson = concat!(
        "{\"user\":\"a\",\"amount\":1.0}\n",
        "{\"user\":\"b\",\"amount\":2.0}\n",
        "{\"user\":\"c\",\"amount\":3.0}\n",
    );

    let mut req = Request::builder()
        .method("POST")
        .uri("/push/events/ndjson")
        .header("content-type", "application/x-ndjson")
        .body(Body::from(ndjson))
        .unwrap();
    inject_loopback(&mut req);
    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["ok"], true);

    let data = v["data"].as_object().expect("data must be an object");
    assert!(data.contains_key("accepted"), "missing 'accepted' key");
    assert!(data.contains_key("rejected"), "missing 'rejected' key");
    assert!(data.contains_key("chunks"), "missing 'chunks' key");
    assert!(
        data.contains_key("first_error"),
        "missing 'first_error' key"
    );

    assert_eq!(v["data"]["accepted"], 3);
    assert_eq!(v["data"]["rejected"], 0);
    assert_eq!(v["data"]["first_error"], serde_json::Value::Null);
}

/// A malformed line must be counted as rejected without aborting the stream.
/// Lines before and after the bad line must still be accepted.
#[tokio::test]
async fn ndjson_malformed_line_counted_rejected_not_aborted() {
    let state = build_test_state(false);
    register_events_stream(&state);
    let app = build_router(state);

    // line 1: valid, line 2: invalid JSON, line 3: valid
    let ndjson = concat!(
        "{\"user\":\"x\",\"amount\":1.0}\n",
        "{bad json\n",
        "{\"user\":\"y\",\"amount\":2.0}\n",
    );

    let mut req = Request::builder()
        .method("POST")
        .uri("/push/events/ndjson")
        .header("content-type", "application/x-ndjson")
        .body(Body::from(ndjson))
        .unwrap();
    inject_loopback(&mut req);
    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["ok"], true);
    assert_eq!(v["data"]["accepted"], 2, "expected 2 accepted: {v}");
    assert_eq!(v["data"]["rejected"], 1, "expected 1 rejected: {v}");
    assert_eq!(
        v["data"]["first_error"]["code"], "schema_error",
        "expected schema_error: {v}"
    );
}
