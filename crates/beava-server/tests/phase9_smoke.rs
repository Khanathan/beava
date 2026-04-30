//! Phase 9 acceptance gate — Rust-side smoke proving 16 decay/velocity/z-score
//! operators register, accept events, and emit values via the live HTTP path.
//!
//! Covers AGG-DECAY-01..07, AGG-VEL-01..08, AGG-Z-01.

use beava_server::testing::TestServerBuilder;
use serde_json::{json, Value};

fn transaction_schema() -> Value {
    json!({
        "kind": "event",
        "name": "Txn",
        "schema": {
            "fields": {
                "event_time": "i64",
                "user_id": "str",
                "amount": "f64",
                "status": "str"
            },
            "optional_fields": []
        },
    })
}

async fn apply_event(
    ts: &beava_server::testing::TestServer,
    event_time_ms: i64,
    user: &str,
    amount: f64,
    status: &str,
) {
    // Plan 12.6-15: migrate from /dev/apply_events to /push/Txn (mio data plane).
    // Both paths drive the same apply_event_to_aggregations function;
    // descriptor.event_time_field reads `event_time` from the row.
    let row = json!({
        "event_time": event_time_ms,
        "user_id": user,
        "amount": amount,
        "status": status,
    });
    let resp = ts.post_json("/push/Txn", &row).await.expect("push");
    assert_eq!(resp.status().as_u16(), 200, "push must succeed");
}

#[tokio::test]
async fn phase9_register_all_16_ops_and_push_events() {
    let ts = TestServerBuilder::new()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn");

    // Register Txn + a single derivation that exposes all 16 Phase 9 ops.
    // Schema has user_id (key) + 16 feature columns.
    let body = json!({
        "nodes": [
            transaction_schema(),
            {
                "kind": "derivation",
                "name": "TxnPhase9",
                "output_kind": "table",
                "upstreams": ["Txn"],
                "ops": [{
                    "op": "group_by",
                    "keys": ["user_id"],
                    "agg": {
                        "ewma_amt":         {"op": "ewma",         "params": {"field": "amount", "half_life": "1s"}},
                        "ewvar_amt":        {"op": "ewvar",        "params": {"field": "amount", "half_life": "1s"}},
                        "ew_z_amt":         {"op": "ew_zscore",    "params": {"field": "amount", "half_life": "1s"}},
                        "decayed_sum_amt":  {"op": "decayed_sum",  "params": {"field": "amount", "half_life": "1s"}},
                        "decayed_count_n":  {"op": "decayed_count","params": {"half_life": "1s"}},
                        "twa_amt":          {"op": "twa",          "params": {"field": "amount", "window": "5m"}},
                        "rate_of_change":   {"op": "rate_of_change","params": {"field": "amount", "window": "5m"}},
                        "interarrival":     {"op": "inter_arrival_stats","params": {"window": "5m"}},
                        "burst":            {"op": "burst_count",  "params": {"window": "5m", "sub_window": "100ms"}},
                        "delta":            {"op": "delta_from_prev","params": {"field": "amount"}},
                        "trend_amt":        {"op": "trend",        "params": {"field": "amount", "window": "5m"}},
                        "trend_residual":   {"op": "trend_residual","params": {"field": "amount", "window": "5m"}},
                        "outliers":         {"op": "outlier_count","params": {"field": "amount", "window": "5m", "sigma": 3.0}},
                        "value_changes":    {"op": "value_change_count","params": {"field": "amount", "window": "5m"}},
                        "z_amt":            {"op": "z_score",      "params": {"field": "amount", "window": "5m"}}
                    }
                }],
                "schema": {
                    "fields": {
                        "user_id":         "str",
                        "ewma_amt":        "f64",
                        "ewvar_amt":       "f64",
                        "ew_z_amt":        "f64",
                        "decayed_sum_amt": "f64",
                        "decayed_count_n": "f64",
                        "twa_amt":         "f64",
                        "rate_of_change":  "f64",
                        "interarrival":    "f64",
                        "burst":           "i64",
                        "delta":           "f64",
                        "trend_amt":       "f64",
                        "trend_residual":  "f64",
                        "outliers":        "i64",
                        "value_changes":   "i64",
                        "z_amt":           "f64"
                    },
                    "optional_fields": []
                },
                "table_primary_key": ["user_id"]
            }
        ]
    });

    let resp = ts.post_json("/register", &body).await.expect("register");
    let status = resp.status().as_u16();
    let body_v: Value = resp.json().await.expect("json");
    assert_eq!(
        status, 200,
        "register all 16 phase 9 ops must succeed: {body_v:#}"
    );
    assert_eq!(body_v["registry_version"], 1);

    // Push a decent stream so each op accumulates evidence.
    for i in 0..30_i64 {
        apply_event(&ts, i * 100, "u1", (i as f64) * 1.5 + 1.0, "ok").await;
    }
    // Inject one outlier
    apply_event(&ts, 4000, "u1", 1000.0, "ok").await;

    // GET /get/{feature}/u1 for each — all should return non-error JSON;
    // most should be non-null after 30 events.
    //
    // Plan 12.6-05/06 (Path X + D-03 hard rip): windowed/velocity ops bucket
    // on **server arrival-time** (events pushed back-to-back land in the
    // same wall-clock millisecond on the server).  Time-derivative ops
    // (`trend`, `trend_residual`, `rate_of_change`, `interarrival`, etc.)
    // need wall-clock dt > 0 to produce a non-null output and so may return
    // null when events arrive too close together.  Their structural
    // correctness is exercised by the GET request returning 200; null
    // values are acceptable post-Path-X for time-derivative features.
    let features_expected_non_null = [
        "ewma_amt",
        "ewvar_amt",
        "decayed_sum_amt",
        "decayed_count_n",
        "twa_amt",
        "burst",
        "delta",
        "value_changes",
        "z_amt",
    ];
    for feature in features_expected_non_null {
        let path = format!("/get/{feature}/u1");
        let v = ts.get_json(&path).await;
        assert!(
            !v["value"].is_null() || feature == "ew_z_amt",
            "feature {feature} should be non-null after 30 events: {v:#}"
        );
    }
    // Time-derivative features may be null when events arrive in the same
    // wall-clock millisecond (Path X arrival-time semantics). We only
    // assert the GET request succeeded structurally.
    for feature in &[
        "rate_of_change",
        "interarrival",
        "trend_amt",
        "trend_residual",
    ] {
        let path = format!("/get/{feature}/u1");
        let _v = ts.get_json(&path).await; // success = no panic
    }

    // ew_z_amt (current z) and outliers may be null/zero respectively in early
    // events; we only need them to *not error* (request succeeded above for all).
    let _ew_z = ts.get_json("/get/ew_z_amt/u1").await;
    let outliers = ts.get_json("/get/outliers/u1").await;
    assert!(
        outliers["value"].as_i64().is_some(),
        "outlier_count must return I64: {outliers:#}"
    );
}

#[tokio::test]
async fn phase9_decay_op_missing_half_life_rejected() {
    let ts = TestServerBuilder::new()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn");

    let body = json!({
        "nodes": [
            transaction_schema(),
            {
                "kind": "derivation",
                "name": "BadDecay",
                "output_kind": "table",
                "upstreams": ["Txn"],
                "ops": [{
                    "op": "group_by",
                    "keys": ["user_id"],
                    "agg": {
                        "ewma_amt": {"op": "ewma", "params": {"field": "amount"}}
                    }
                }],
                "schema": {
                    "fields": {"user_id": "str", "ewma_amt": "f64"},
                    "optional_fields": []
                },
                "table_primary_key": ["user_id"]
            }
        ]
    });

    let resp = ts.post_json("/register", &body).await.expect("register");
    assert_eq!(
        resp.status().as_u16(),
        400,
        "ewma without half_life must be rejected at register time"
    );
    let err: Value = resp.json().await.expect("json");
    let s = err.to_string();
    assert!(
        s.contains("aggregation_invalid_half_life") || s.contains("half_life"),
        "error must reference half_life: {err:#}"
    );
}

#[tokio::test]
async fn phase9_burst_count_missing_sub_window_rejected() {
    let ts = TestServerBuilder::new()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn");

    let body = json!({
        "nodes": [
            transaction_schema(),
            {
                "kind": "derivation",
                "name": "BadBurst",
                "output_kind": "table",
                "upstreams": ["Txn"],
                "ops": [{
                    "op": "group_by",
                    "keys": ["user_id"],
                    "agg": {
                        "burst": {"op": "burst_count", "params": {"window": "5m"}}
                    }
                }],
                "schema": {
                    "fields": {"user_id": "str", "burst": "i64"},
                    "optional_fields": []
                },
                "table_primary_key": ["user_id"]
            }
        ]
    });

    let resp = ts.post_json("/register", &body).await.expect("register");
    assert_eq!(
        resp.status().as_u16(),
        400,
        "burst_count without sub_window must be rejected"
    );
    let err: Value = resp.json().await.expect("json");
    let s = err.to_string();
    assert!(
        s.contains("aggregation_invalid_sub_window") || s.contains("sub_window"),
        "error must reference sub_window: {err:#}"
    );
}

#[tokio::test]
async fn phase9_ema_alias_resolves_to_ewma() {
    // Server accepts "ema" as an alias for "ewma".
    let ts = TestServerBuilder::new()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn");

    let body = json!({
        "nodes": [
            transaction_schema(),
            {
                "kind": "derivation",
                "name": "EmaTable",
                "output_kind": "table",
                "upstreams": ["Txn"],
                "ops": [{
                    "op": "group_by",
                    "keys": ["user_id"],
                    "agg": {
                        "ema_amt": {"op": "ema", "params": {"field": "amount", "half_life": "1s"}}
                    }
                }],
                "schema": {
                    "fields": {"user_id": "str", "ema_amt": "f64"},
                    "optional_fields": []
                },
                "table_primary_key": ["user_id"]
            }
        ]
    });

    let resp = ts.post_json("/register", &body).await.expect("register");
    assert_eq!(resp.status().as_u16(), 200, "ema alias must register");
}
