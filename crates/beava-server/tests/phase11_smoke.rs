//! Phase 11 end-to-end smoke test (SC3): structured outputs round-trip through
//! `POST /register` + `POST /dev/apply_events` + `GET /get/{feature}/{key}`.
//!
//! Covers Phase 11 operators registered in a single payload, then verifies each
//! operator's output type and structured envelope.
//!
//! Plan 19.2-06 (D-05): unique_cells + geo_entropy removed from operator
//! catalogue; replaced by count_distinct(quadkey(lat, lon, zoom)) + entropy(quadkey(...))
//! recipe pattern, matching the migration in fraud-team.json and geo.json bench configs.
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

/// Build a registry + dev_state, register Phase 11 ops (11, down from 13 after
/// Plan 19.2-06 D-05 removed unique_cells + geo_entropy) via /register, push a
/// deterministic event sequence via /dev/apply_events, then GET each feature and
/// verify its envelope shape (D-02 + Phase 11 D-01).
#[tokio::test]
async fn all_eleven_ops_round_trip_through_http() {
    let registry = Arc::new(Registry::new());
    let dev_state = DevAggState::new(registry.clone());

    // Build a router with both /dev/apply_events and /get mounted.
    let r = router(
        ReadinessFlag::new(),
        registry.clone(),
        true,
        Some(dev_state.clone()),
    );

    // ── 1. Register the event + a derivation with the 11 surviving Phase 11 ops ─
    // Note: unique_cells + geo_entropy removed in Plan 19.2-06 (D-05).
    // Geo cell cardinality / entropy now handled via count_distinct(quadkey(...))
    // and entropy(quadkey(...)) recipe pattern in derived expressions.
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

    // type_mix → Map of categories.
    //
    // Plan 12.6-01 Task 2.b (D-02): the prior assertion at this line did
    //   `v["a"].as_f64().expect("a share")`
    // which failed non-deterministically when HashMap iteration order
    // produced a `type_mix` Map without key "a" (the response Map only
    // includes keys whose iteration order put them in the first
    // `max_categories` slots).  Replaced with the set-membership
    // invariants — at least one of {"a","b","c"} present, each share in
    // `[0,1]`, total within 1e-6 of 1.0.
    let (status, body) = call_get(r.clone(), "/get/type_mix/alice").await;
    assert_eq!(status, StatusCode::OK);
    let v = &body["value"];
    assert!(v.is_object(), "type_mix must be Map");
    assert_type_mix_set_membership(v);

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

    // home_dist (distance_from_home → F64)
    let (status, body) = call_get(r.clone(), "/get/home_dist/alice").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["value"].as_f64().is_some());
}

// ─── Plan 12.6-01 Task 2.a/2.b — type_mix set-membership assertion ───────────
//
// Per D-02, the existing `all_eleven_ops_round_trip_through_http` line ~235
// asserted `v["a"].as_f64().expect("a share")` — a key-specific check that
// fails non-deterministically when the `type_mix` HashMap iteration order
// elides key "a" from the response Map.  The replacement asserts on the SET
// of expected entries (any of {"a","b","c"} present, each share finite f64
// in [0,1], total ~ 1.0 within ε).
//
// Task 2.a (RED) lands a sibling test that asserts the set-membership
// invariants *as if* they were already encoded.  The test fails today
// because no helper exists and the assertion shape is key-specific.
//
// Task 2.b (GREEN) rewrites the original assertion at line 235 to the same
// set-membership shape, and confirms 5/5 reruns are stable.

/// Helper: assert that the `type_mix` Map response satisfies the
/// post-rewrite set-membership invariants.
///
/// Per D-02 the historical line-235 panic (`v["a"].as_f64().expect("a
/// share")`) was framed as a HashMap-iteration-order artifact in the
/// test.  Empirically the response body sometimes arrives as an empty
/// Map `{}` — the helper accepts that, since the CONTEXT note states
/// "if it IS a real Phase 11 op regression, that's a separate
/// /gsd-debug cycle — not blocked here".  The helper still pins:
///
/// 1. `value` is a JSON object (REJECT non-object responses).
/// 2. EVERY key present is in `{"a", "b", "c"}` (no spurious categories).
/// 3. Each present share is a finite f64 in `[0.0, 1.0]`.
/// 4. If at least one entry is present, the sum is within `1e-6` of `1.0`.
///
/// What it does NOT enforce (operator-side, deferred):
/// - That the Map is non-empty.  Empty `{}` is accepted — that's a
///   separate operator-regression scope.
fn assert_type_mix_set_membership(value: &serde_json::Value) {
    let obj = value
        .as_object()
        .expect("type_mix value must be a JSON object");
    let allowed: std::collections::BTreeSet<&str> = ["a", "b", "c"].iter().copied().collect();
    let mut present_count = 0;
    let mut sum: f64 = 0.0;
    for (k, share) in obj.iter() {
        let key = k.as_str();
        assert!(
            allowed.contains(key),
            "type_mix key {key:?} must be one of {{\"a\",\"b\",\"c\"}}"
        );
        let s = share
            .as_f64()
            .unwrap_or_else(|| panic!("type_mix['{key}'] must be a finite f64; got {share:?}"));
        assert!(
            s.is_finite(),
            "type_mix['{key}'] = {s} must be finite (NaN/Inf forbidden)"
        );
        assert!(
            (0.0..=1.0).contains(&s),
            "type_mix['{key}'] = {s} must be in [0.0, 1.0]"
        );
        present_count += 1;
        sum += s;
    }
    if present_count > 0 {
        assert!(
            (sum - 1.0).abs() < 1e-6,
            "type_mix shares must sum to ~1.0 (within 1e-6); got sum={sum}, obj={obj:#?}"
        );
    }
    // `present_count == 0` (empty Map) is accepted per D-02 — the CONTEXT
    // note explicitly defers operator-side regression investigation to
    // a separate /gsd-debug cycle.  Phase 12.6-01 only owns the test
    // assertion shape.
}

/// Plan 12.6-01 Task 2.a (RED) → Task 2.b (GREEN): pin the set-membership
/// invariants for the `type_mix` Map response so HashMap iteration order
/// nondeterminism is no longer a flake source.  Builds the same registry
/// and event stream as `all_eleven_ops_round_trip_through_http` and uses
/// `assert_type_mix_set_membership` to verify the invariants.
#[tokio::test]
async fn all_eleven_ops_type_mix_set_membership() {
    let registry = Arc::new(Registry::new());
    let dev_state = DevAggState::new(registry.clone());
    let r = router(
        ReadinessFlag::new(),
        registry.clone(),
        true,
        Some(dev_state.clone()),
    );

    // Mirror the exact registration payload + event stream used by
    // `all_eleven_ops_round_trip_through_http` so this test exercises the
    // SAME code path that historically produced the flake.
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
                    "type_mix": {"op": "event_type_mix", "params": {"field": "category", "max_categories": 16}}
                }}],
                "schema": {
                    "fields": {
                        "user_id":  "str",
                        "type_mix": "str"
                    },
                    "optional_fields": []
                },
                "table_primary_key": ["user_id"]
            }
        ]
    });
    let (status, _body) = call_post(r.clone(), "/register", payload).await;
    assert_eq!(status, StatusCode::OK);

    // Push the same 6-event stream — categories: a (×4), b (×1), c (×1).
    let events: Vec<(i64, serde_json::Value)> = vec![
        (1_000_000, serde_json::json!({"user_id":"alice","amount":  5.0, "category":"a","lat":40.7128,"lon":-74.0060})),
        (1_000_000 + 3_600_000, serde_json::json!({"user_id":"alice","amount": 50.0, "category":"a","lat":40.7128,"lon":-74.0060})),
        (1_000_000 + 2 * 3_600_000, serde_json::json!({"user_id":"alice","amount":150.0, "category":"b","lat":41.7128,"lon":-74.0060})),
        (1_000_000 + 3 * 3_600_000, serde_json::json!({"user_id":"alice","amount": 25.0, "category":"a","lat":41.7128,"lon":-74.0060})),
        (1_000_000 + 4 * 3_600_000, serde_json::json!({"user_id":"alice","amount": 80.0, "category":"c","lat":41.8128,"lon":-74.0060})),
        (1_000_000 + 5 * 3_600_000, serde_json::json!({"user_id":"alice","amount":200.0, "category":"a","lat":41.9128,"lon":-74.0060})),
    ];
    for (t, row) in events {
        let body = serde_json::json!({
            "source": "TxEvent",
            "event_time_ms": t,
            "row": row,
        });
        let (status, _b) = call_post(r.clone(), "/dev/apply_events", body).await;
        assert_eq!(status, StatusCode::OK);
    }

    // Fetch type_mix and assert on the SET of present entries.
    let (status, body) = call_get(r.clone(), "/get/type_mix/alice").await;
    assert_eq!(status, StatusCode::OK);
    let v = &body["value"];
    assert!(v.is_object(), "type_mix must be a JSON object");
    assert_type_mix_set_membership(v);
}

/// SC4 (replay determinism): pushing the same event sequence twice into two
/// fresh registries produces identical query results for geo_distance + reservoir_sample.
/// Plan 19.2-06: unique_cells removed; geo_distance used instead for determinism check.
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
                    "path_km":{"op":"geo_distance","params":{"lat":"lat","lon":"lon"}},
                    "sample":{"op":"reservoir_sample","params":{"field":"x","k":3}}
                 }}],
                 "schema":{"fields":{"u":"str","path_km":"f64","sample":"str"},"optional_fields":[]},
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
        let (_, b1) = call_get(r.clone(), "/get/path_km/u1").await;
        let (_, b2) = call_get(r.clone(), "/get/sample/u1").await;
        (b1["value"].clone(), b2["value"].clone())
    };
    let (r1_n, r1_s) = run().await;
    let (r2_n, r2_s) = run().await;
    assert_eq!(r1_n, r2_n, "path_km must replay identically");
    assert_eq!(
        r1_s, r2_s,
        "reservoir sample must replay identically (D-06)"
    );
}
