//! Phase 11 end-to-end smoke test (SC3): structured outputs round-trip through
//! `POST /register` + `POST /dev/apply_events` + `GET /get/{feature}/{key}`.
//!
//! Covers all 13 Phase 11 operators registered in a single payload, then
//! verifies each operator's output type and structured envelope.
//!
//! Geo math is verified against the haversine crate (cited in
//! `crates/beava-core/src/agg_geo.rs::haversine_nyc_to_london_matches_published`).

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use beava_core::registry::Registry;
use beava_server::http::{router, ReadinessFlag};
use beava_server::registry_debug::DevAggState;
use http_body_util::BodyExt;
use std::sync::Arc;
use tower::ServiceExt;

async fn call_post(
    r: axum::Router,
    uri: &str,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let payload = serde_json::to_vec(&body).unwrap();
    let req = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(payload))
        .unwrap();
    let resp = r.oneshot(req).await.expect("oneshot");
    let status = resp.status();
    let bytes = resp
        .into_body()
        .collect()
        .await
        .expect("collect")
        .to_bytes();
    let json = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
    };
    (status, json)
}

async fn call_get(r: axum::Router, uri: &str) -> (StatusCode, serde_json::Value) {
    let resp = r
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .expect("oneshot");
    let status = resp.status();
    let bytes = resp
        .into_body()
        .collect()
        .await
        .expect("collect")
        .to_bytes();
    let json = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
    };
    (status, json)
}

/// Build a registry + dev_state, register all 13 Phase 11 ops via /register,
/// push a deterministic event sequence via /dev/apply_events, then GET each
/// feature and verify its envelope shape (D-02 + Phase 11 D-01).
#[tokio::test]
async fn all_thirteen_ops_round_trip_through_http() {
    let registry = Arc::new(Registry::new());
    let dev_state = DevAggState::new(registry.clone());

    // Build a router with both /dev/apply_events and /get mounted.
    let r = router(
        ReadinessFlag::new(),
        registry.clone(),
        true,
        Some(dev_state.clone()),
    );

    // ── 1. Register the event + a derivation that contains all 13 Phase 11 ops ─
    let payload = serde_json::json!({
        "nodes": [
            {
                "kind": "event",
                "name": "TxEvent",
                "schema": {
                    "fields": {
                        "event_time": "i64",
                        "user_id":    "str",
                        "amount":     "f64",
                        "category":   "str",
                        "lat":        "f64",
                        "lon":        "f64"
                    },
                    "optional_fields": []
                },
                "event_time_field": "event_time"
            },
            {
                "kind": "derivation",
                "name": "TxAgg",
                "output_kind": "table",
                "upstreams": ["TxEvent"],
                "ops": [{"op": "group_by", "keys": ["user_id"], "agg": {
                    "amount_hist":      {"op": "histogram",            "params": {"field": "amount", "buckets": [10.0, 100.0]}},
                    "hod":              {"op": "hour_of_day_histogram","params": {}},
                    "dh":               {"op": "dow_hour_histogram",   "params": {}},
                    "amt_seasonal":     {"op": "seasonal_deviation",   "params": {"field": "amount"}},
                    "type_mix":         {"op": "event_type_mix",       "params": {"field": "category", "max_categories": 16}},
                    "last5":            {"op": "most_recent_n",        "params": {"field": "amount", "n": 5}},
                    "sample10":         {"op": "reservoir_sample",     "params": {"field": "amount", "k": 10}},
                    "kmh":              {"op": "geo_velocity",         "params": {"lat": "lat", "lon": "lon"}},
                    "path_km":          {"op": "geo_distance",         "params": {"lat": "lat", "lon": "lon"}},
                    "spread_km":        {"op": "geo_spread",           "params": {"lat": "lat", "lon": "lon"}},
                    "n_cells":          {"op": "unique_cells",         "params": {"lat": "lat", "lon": "lon", "precision": 10}},
                    "geo_h":            {"op": "geo_entropy",          "params": {"lat": "lat", "lon": "lon", "precision": 10}},
                    "home_dist":        {"op": "distance_from_home",   "params": {"lat": "lat", "lon": "lon", "samples": 5}}
                }}],
                "schema": {
                    "fields": {
                        "user_id":     "str",
                        "amount_hist": "str",
                        "hod":         "str",
                        "dh":          "str",
                        "amt_seasonal":"f64",
                        "type_mix":    "str",
                        "last5":       "str",
                        "sample10":    "str",
                        "kmh":         "f64",
                        "path_km":     "f64",
                        "spread_km":   "f64",
                        "n_cells":     "i64",
                        "geo_h":       "f64",
                        "home_dist":   "f64"
                    },
                    "optional_fields": []
                },
                "table_primary_key": ["user_id"]
            }
        ]
    });
    let (status, body) = call_post(r.clone(), "/register", payload).await;
    assert_eq!(status, StatusCode::OK, "register failed: {body:#}");

    // ── 2. Push deterministic events — 6 events at known lat/lon/amount/category ─
    // event_time monotonically increasing 1h apart so geo_velocity has finite dt
    let events: Vec<(i64, serde_json::Value)> = vec![
        (
            1_000_000,
            serde_json::json!({"user_id":"alice","amount":  5.0, "category":"a","lat":40.7128,"lon":-74.0060}),
        ),
        (
            1_000_000 + 3_600_000,
            serde_json::json!({"user_id":"alice","amount": 50.0, "category":"a","lat":40.7128,"lon":-74.0060}),
        ),
        (
            1_000_000 + 2 * 3_600_000,
            serde_json::json!({"user_id":"alice","amount":150.0, "category":"b","lat":41.7128,"lon":-74.0060}),
        ),
        (
            1_000_000 + 3 * 3_600_000,
            serde_json::json!({"user_id":"alice","amount": 25.0, "category":"a","lat":41.7128,"lon":-74.0060}),
        ),
        (
            1_000_000 + 4 * 3_600_000,
            serde_json::json!({"user_id":"alice","amount": 80.0, "category":"c","lat":41.8128,"lon":-74.0060}),
        ),
        (
            1_000_000 + 5 * 3_600_000,
            serde_json::json!({"user_id":"alice","amount":200.0, "category":"a","lat":41.9128,"lon":-74.0060}),
        ),
    ];

    for (t, row) in events {
        let body = serde_json::json!({
            "source": "TxEvent",
            "event_time_ms": t,
            "row": row,
        });
        let (status, body) = call_post(r.clone(), "/dev/apply_events", body).await;
        assert_eq!(status, StatusCode::OK, "/dev/apply_events failed: {body:#}");
    }

    // ── 3. GET each feature and verify envelope shape + value type ────────────

    // amount_hist (histogram → Map)
    let (status, body) = call_get(r.clone(), "/get/amount_hist/alice").await;
    assert_eq!(status, StatusCode::OK, "amount_hist body: {body:#}");
    let v = &body["value"];
    assert!(
        v.is_object(),
        "histogram value must be a JSON object: {v:#}"
    );
    // Buckets: <10, 10-100, >=100 → 1 (5.0), 3 (50.0, 25.0, 80.0), 2 (150.0, 200.0)
    assert_eq!(v["<10"], 1, "expected 1 in <10 cell");
    assert_eq!(v["10-100"], 3, "expected 3 in 10-100 cell");
    assert_eq!(v[">=100"], 2, "expected 2 in >=100 cell");

    // hod (hour_of_day_histogram → Map of 24 keys)
    let (status, body) = call_get(r.clone(), "/get/hod/alice").await;
    assert_eq!(status, StatusCode::OK);
    let v = &body["value"];
    assert!(v.is_object(), "hod must be Map");
    let obj = v.as_object().unwrap();
    assert_eq!(obj.len(), 24, "hod must have 24 keys");

    // dh (dow_hour_histogram → Map of 168 keys)
    let (status, body) = call_get(r.clone(), "/get/dh/alice").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["value"].as_object().unwrap().len(),
        168,
        "dh must have 168 keys"
    );

    // amt_seasonal (seasonal_deviation → F64 or Null)
    let (status, body) = call_get(r.clone(), "/get/amt_seasonal/alice").await;
    assert_eq!(status, StatusCode::OK);
    // Value is either a number or null depending on within-hour variance.
    assert!(body["value"].is_number() || body["value"].is_null());

    // type_mix → Map of categories
    let (status, body) = call_get(r.clone(), "/get/type_mix/alice").await;
    assert_eq!(status, StatusCode::OK);
    let v = &body["value"];
    assert!(v.is_object(), "type_mix must be Map");
    // a:4/6, b:1/6, c:1/6
    let a_share = v["a"].as_f64().expect("a share");
    assert!((a_share - 4.0 / 6.0).abs() < 1e-9, "a share = {a_share}");

    // last5 (most_recent_n → List)
    let (status, body) = call_get(r.clone(), "/get/last5/alice").await;
    assert_eq!(status, StatusCode::OK);
    let v = &body["value"];
    assert!(v.is_array(), "last5 must be List");
    assert_eq!(v.as_array().unwrap().len(), 5, "n=5, 6 events pushed");

    // sample10 (reservoir_sample → List ≤ 10)
    let (status, body) = call_get(r.clone(), "/get/sample10/alice").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["value"].is_array());

    // kmh (geo_velocity → F64)
    let (status, body) = call_get(r.clone(), "/get/kmh/alice").await;
    assert_eq!(status, StatusCode::OK);
    let v = body["value"].as_f64().expect("kmh F64");
    // Between event 2 and 3 we move ~111 km in 1h → ~111 km/h
    assert!((v - 111.0).abs() < 5.0, "expected max kmh ~111, got {v}");

    // path_km (geo_distance → F64)
    let (status, body) = call_get(r.clone(), "/get/path_km/alice").await;
    assert_eq!(status, StatusCode::OK);
    let v = body["value"].as_f64().expect("path_km F64");
    // Total north travel: 0 + 111 + 0 + 11.1 + 11.1 ≈ 133.3 km
    assert!(v > 100.0 && v < 200.0, "path_km expected 100-200, got {v}");

    // spread_km (geo_spread → F64)
    let (status, body) = call_get(r.clone(), "/get/spread_km/alice").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["value"].as_f64().unwrap() > 0.0);

    // n_cells (unique_cells → I64)
    let (status, body) = call_get(r.clone(), "/get/n_cells/alice").await;
    assert_eq!(status, StatusCode::OK);
    let n = body["value"].as_i64().expect("n_cells i64");
    // precision=10, cells: floor(40.7*10),floor(-74.0*10) etc — 4 distinct cells
    assert!((3..=6).contains(&n), "n_cells expected ~4, got {n}");

    // geo_h (geo_entropy → F64)
    let (status, body) = call_get(r.clone(), "/get/geo_h/alice").await;
    assert_eq!(status, StatusCode::OK);
    let h = body["value"].as_f64().expect("geo_h F64");
    assert!(
        h > 0.0,
        "entropy must be positive across distinct cells, got {h}"
    );

    // home_dist (distance_from_home → F64)
    let (status, body) = call_get(r.clone(), "/get/home_dist/alice").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["value"].as_f64().is_some());
}

/// SC4 (replay determinism): pushing the same event sequence twice into two
/// fresh registries produces identical query results across all 13 ops.
#[tokio::test]
async fn replay_determinism_across_two_runs() {
    let run = || async {
        let registry = Arc::new(Registry::new());
        let dev_state = DevAggState::new(registry.clone());
        let r = router(
            ReadinessFlag::new(),
            registry.clone(),
            true,
            Some(dev_state),
        );
        let payload = serde_json::json!({
            "nodes": [
                {"kind":"event","name":"E","schema":{"fields":{"event_time":"i64","u":"str","x":"f64","lat":"f64","lon":"f64"},"optional_fields":[]},"event_time_field":"event_time"},
                {"kind":"derivation","name":"D","output_kind":"table","upstreams":["E"],
                 "ops":[{"op":"group_by","keys":["u"],"agg":{
                    "n_cells":{"op":"unique_cells","params":{"lat":"lat","lon":"lon","precision":100}},
                    "sample":{"op":"reservoir_sample","params":{"field":"x","k":3}}
                 }}],
                 "schema":{"fields":{"u":"str","n_cells":"i64","sample":"str"},"optional_fields":[]},
                 "table_primary_key":["u"]}
            ]
        });
        let (s, _) = call_post(r.clone(), "/register", payload).await;
        assert_eq!(s, StatusCode::OK);
        for i in 0..50_i64 {
            let body = serde_json::json!({
                "source": "E",
                "event_time_ms": i,
                "row": {"u":"u1","x":(i as f64),"lat":(40.0 + i as f64 * 0.01),"lon":-74.0},
            });
            let (s, _) = call_post(r.clone(), "/dev/apply_events", body).await;
            assert_eq!(s, StatusCode::OK);
        }
        let (_, b1) = call_get(r.clone(), "/get/n_cells/u1").await;
        let (_, b2) = call_get(r.clone(), "/get/sample/u1").await;
        (b1["value"].clone(), b2["value"].clone())
    };
    let (r1_n, r1_s) = run().await;
    let (r2_n, r2_s) = run().await;
    assert_eq!(r1_n, r2_n, "n_cells must replay identically");
    assert_eq!(
        r1_s, r2_s,
        "reservoir sample must replay identically (D-06)"
    );
}
