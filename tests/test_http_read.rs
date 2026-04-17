//! Phase 45 — HTTP-04 + HTTP-05: feature read and stream list tests.

mod http_common;

use std::time::SystemTime;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;

use beava::engine::pipeline::{FeatureDef, PipelineEngine, StreamDefinition};
use beava::server::http::build_router;
use beava::server::tcp::{make_concurrent_state_full, BackfillTracker};
use beava::state::store::StateStore;
use http_common::{build_test_state, inject_loopback, TEST_ADMIN_TOKEN};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a SharedState with a registered stream "txn_events" (key_field=user_id,
/// one Count feature "txn_count") and one event pushed for key "u1".
fn seeded_state() -> beava::server::tcp::SharedState {
    use std::time::Duration;

    let mut engine = PipelineEngine::new();
    let stream = StreamDefinition {
        name: "txn_events".to_string(),
        key_field: Some("user_id".to_string()),
        features: vec![(
            "txn_count".to_string(),
            FeatureDef::Count {
                window: Duration::from_secs(3600),
                bucket: Duration::from_secs(60),
                where_expr: None,
                backfill: false,
            },
        )],
        ..Default::default()
    };
    engine.register(stream).unwrap();

    let store = StateStore::new();
    let payload = serde_json::json!({ "user_id": "u1", "amount": 10.0 });
    engine
        .push("txn_events", &payload, &store, SystemTime::now())
        .unwrap();

    make_concurrent_state_full(
        engine,
        store,
        None,
        std::path::PathBuf::from("/tmp/beava-test-http-read.snapshot"),
        Arc::new(BackfillTracker::default()),
        false,
        false,
        Some(TEST_ADMIN_TOKEN.to_string()),
        false, // public_mode = false: reads stay on admin router
    )
}

// ---------------------------------------------------------------------------
// HTTP-04: GET /features/{key}
// ---------------------------------------------------------------------------

#[tokio::test]
async fn features_by_key_all_tables() {
    let app = build_router(seeded_state());

    let mut req = Request::builder()
        .method("GET")
        .uri("/features/u1")
        .header("Authorization", format!("Bearer {}", TEST_ADMIN_TOKEN))
        .body(Body::empty())
        .unwrap();
    inject_loopback(&mut req);

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["ok"], Value::Bool(true));
    assert_eq!(json["data"]["key"], Value::String("u1".to_string()));
    let tables = &json["data"]["tables"];
    assert!(tables.is_object(), "tables must be an object");
    assert!(
        tables.as_object().unwrap().len() >= 1,
        "tables must have at least one entry after seeding"
    );
}

#[tokio::test]
async fn features_filtered_by_table() {
    // Build two Router instances from two identical states so we can oneshot
    // each request independently (oneshot consumes the router).
    let app_a = build_router(seeded_state());
    let app_b = build_router(seeded_state());

    // --- request 1: filter to a table that doesn't exist ---
    let mut req1 = Request::builder()
        .method("GET")
        .uri("/features/u1?table=nonexistent_table")
        .header("Authorization", format!("Bearer {}", TEST_ADMIN_TOKEN))
        .body(Body::empty())
        .unwrap();
    inject_loopback(&mut req1);

    let resp1 = app_a.oneshot(req1).await.unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);
    let body1 = axum::body::to_bytes(resp1.into_body(), usize::MAX)
        .await
        .unwrap();
    let json1: Value = serde_json::from_slice(&body1).unwrap();
    assert_eq!(json1["ok"], Value::Bool(true));
    assert_eq!(
        json1["data"]["tables"],
        serde_json::json!({}),
        "filtering by nonexistent table must return empty tables"
    );

    // --- request 2: filter to the known table ---
    // Features without a dot land under a table named after the full feature.
    // After pushing one event, "txn_count" is the feature name → table = "txn_count".
    let mut req2 = Request::builder()
        .method("GET")
        .uri("/features/u1?table=txn_count")
        .header("Authorization", format!("Bearer {}", TEST_ADMIN_TOKEN))
        .body(Body::empty())
        .unwrap();
    inject_loopback(&mut req2);

    let resp2 = app_b.oneshot(req2).await.unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);
    let body2 = axum::body::to_bytes(resp2.into_body(), usize::MAX)
        .await
        .unwrap();
    let json2: Value = serde_json::from_slice(&body2).unwrap();
    assert_eq!(json2["ok"], Value::Bool(true));
    let table_keys: Vec<String> = json2["data"]["tables"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    assert_eq!(table_keys, vec!["txn_count"], "must return exactly the filtered table");
}

#[tokio::test]
async fn features_404_for_unknown_key() {
    let app = build_router(build_test_state(false));

    let mut req = Request::builder()
        .method("GET")
        .uri("/features/zzzunknown")
        .header("Authorization", format!("Bearer {}", TEST_ADMIN_TOKEN))
        .body(Body::empty())
        .unwrap();
    inject_loopback(&mut req);

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["ok"], Value::Bool(false));
    assert_eq!(
        json["error"]["code"],
        Value::String("key_not_found".to_string())
    );
}

// ---------------------------------------------------------------------------
// HTTP-05: GET /streams + GET /streams/{name}
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_streams_returns_watermark() {
    let app = build_router(seeded_state());

    let mut req = Request::builder()
        .method("GET")
        .uri("/streams")
        .header("Authorization", format!("Bearer {}", TEST_ADMIN_TOKEN))
        .body(Body::empty())
        .unwrap();
    inject_loopback(&mut req);

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["ok"], Value::Bool(true));
    let streams = json["data"]["streams"].as_array().expect("streams must be an array");
    assert!(streams.len() >= 1, "must have at least one registered stream");

    // Every entry must have a "name" field.
    for s in streams {
        assert!(s["name"].is_string(), "stream entry must have a string name");
        // watermark_ms is either a u64 number or null.
        assert!(
            s["watermark_ms"].is_null() || s["watermark_ms"].is_number(),
            "watermark_ms must be null or a number"
        );
    }

    // Our seeded stream must appear.
    let names: Vec<&str> = streams
        .iter()
        .filter_map(|s| s["name"].as_str())
        .collect();
    assert!(names.contains(&"txn_events"), "txn_events must be listed");
}

#[tokio::test]
async fn stream_detail_returns_schema() {
    let app = build_router(seeded_state());

    let mut req = Request::builder()
        .method("GET")
        .uri("/streams/txn_events")
        .header("Authorization", format!("Bearer {}", TEST_ADMIN_TOKEN))
        .body(Body::empty())
        .unwrap();
    inject_loopback(&mut req);

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["ok"], Value::Bool(true));
    assert_eq!(json["data"]["name"], Value::String("txn_events".to_string()));
    assert!(
        json["data"]["watermark_ms"].is_null() || json["data"]["watermark_ms"].is_number(),
        "watermark_ms must be null or number"
    );
    let features = json["data"]["features"]
        .as_array()
        .expect("features must be an array");
    assert_eq!(features.len(), 1, "txn_events has exactly one feature");
    assert_eq!(features[0]["name"], Value::String("txn_count".to_string()));
    assert!(features[0]["type"].is_string(), "feature type must be a string");
}

#[tokio::test]
async fn stream_detail_404_when_unknown() {
    let app = build_router(build_test_state(false));

    let mut req = Request::builder()
        .method("GET")
        .uri("/streams/zzzunknown")
        .header("Authorization", format!("Bearer {}", TEST_ADMIN_TOKEN))
        .body(Body::empty())
        .unwrap();
    inject_loopback(&mut req);

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["ok"], Value::Bool(false));
    assert_eq!(
        json["error"]["code"],
        Value::String("stream_not_found".to_string())
    );
}
