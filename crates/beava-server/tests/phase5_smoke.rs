//! Phase 5 acceptance gate — Rust-side smoke proving ROADMAP SC1..SC6 end-to-end.
//!
//! All tests run against a live TestServer via HTTP, with BEAVA_DEV_ENDPOINTS=1
//! (mounted by TestServerBuilder::new().dev_endpoints(true)).  No Python dependency.
//!
//! SC1: group_by().agg() produces a Table; GET /registry shows correct output_kind.
//! SC2: push via /dev/apply_events updates the aggregation; GET /get returns the value.
//! SC3: all 8 core operators pass table-driven correctness tests end-to-end.
//! SC4: identical event stream to two fresh servers → byte-identical GET /get responses.
//! SC5: windowless count and ratio work (window omitted).
//! SC6: unknown field in op.field → 400 with aggregation_unknown_field;
//!      aggregation on a Table source → 400 with aggregation_on_table_not_supported.

use beava_server::testing::TestServerBuilder;
use serde_json::json;

// ─── Register helpers ─────────────────────────────────────────────────────────

/// Register a Transaction event with {event_time: i64, user_id: str, amount: f64, status: str}.
fn transaction_schema() -> serde_json::Value {
    json!({
        "kind": "event",
        "name": "Transaction",
        "schema": {
            "fields": {
                "event_time": "i64",
                "user_id": "str",
                "amount": "f64",
                "status": "str"
            },
            "optional_fields": []
        },
        "event_time_field": "event_time"
    })
}

/// Push a single event via /dev/apply_events.
///
/// Returns the response body as serde_json::Value.
async fn apply_event(
    ts: &beava_server::testing::TestServer,
    source: &str,
    event_time_ms: i64,
    row: serde_json::Value,
) -> serde_json::Value {
    let resp = ts
        .post_json(
            "/dev/apply_events",
            &json!({
                "source": source,
                "event_time_ms": event_time_ms,
                "row": row
            }),
        )
        .await
        .expect("apply_events post");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "apply_events must succeed: source={source}"
    );
    resp.json().await.expect("json")
}

// ─── SC1 ──────────────────────────────────────────────────────────────────────

/// SC1: POST /register with Transaction event + group_by derivation produces a Table.
/// Checks: 200 response, registry_version bumped, GET /registry shows output_kind=table,
/// table_primary_key=["user_id"], schema contains {user_id: str, cnt: i64}.
#[tokio::test]
async fn sc1_register_groupby_produces_table() {
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
                "name": "TxCount5m",
                "output_kind": "table",
                "upstreams": ["Transaction"],
                "ops": [{
                    "op": "group_by",
                    "keys": ["user_id"],
                    "agg": {
                        "cnt": {"op": "count", "params": {"window": "5m"}}
                    }
                }],
                "schema": {
                    "fields": {"user_id": "str", "cnt": "i64"},
                    "optional_fields": []
                },
                "table_primary_key": ["user_id"]
            }
        ]
    });

    let resp = ts.post_json("/register", &body).await.expect("register");
    assert_eq!(resp.status().as_u16(), 200, "register must succeed");
    let reg_body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(
        reg_body["registry_version"], 1,
        "version must bump to 1: {reg_body:#}"
    );

    // GET /registry → TxCount5m must have output_kind=table.
    let registry_dump = ts.get_json("/registry").await;
    let deriv = &registry_dump["derivations"]["TxCount5m"];
    assert_eq!(
        deriv["output_kind"], "table",
        "SC1: output_kind must be 'table': {deriv:#}"
    );
    // table_primary_key must be ["user_id"]
    let pk = &deriv["table_primary_key"];
    assert!(
        pk.as_array()
            .map(|a| a.len() == 1 && a[0] == "user_id")
            .unwrap_or(false),
        "SC1: table_primary_key must be [user_id]: {deriv:#}"
    );
    // Schema must contain user_id and cnt.
    let schema_fields = &deriv["schema"]["fields"];
    assert!(
        schema_fields.get("user_id").is_some(),
        "SC1: schema must contain user_id: {schema_fields:#}"
    );
    assert!(
        schema_fields.get("cnt").is_some(),
        "SC1: schema must contain cnt: {schema_fields:#}"
    );

    ts.shutdown().await.expect("shutdown");
}

// ─── SC2 ──────────────────────────────────────────────────────────────────────

/// SC2a: Push 10 events via /dev/apply_events; GET /get/cnt/alice → {"value": 10}.
#[tokio::test]
async fn sc2_push_then_get_returns_count() {
    let ts = TestServerBuilder::new()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn");

    // Register Transaction + count-5m aggregation.
    let body = json!({
        "nodes": [
            transaction_schema(),
            {
                "kind": "derivation",
                "name": "TxCount5m",
                "output_kind": "table",
                "upstreams": ["Transaction"],
                "ops": [{
                    "op": "group_by",
                    "keys": ["user_id"],
                    "agg": {
                        "cnt": {"op": "count", "params": {"window": "5m"}}
                    }
                }],
                "schema": {"fields": {"user_id": "str", "cnt": "i64"}, "optional_fields": []},
                "table_primary_key": ["user_id"]
            }
        ]
    });
    let resp = ts.post_json("/register", &body).await.expect("register");
    assert_eq!(resp.status().as_u16(), 200);

    // Push 10 events for alice, all within the 5m window.
    for i in 0..10i64 {
        apply_event(
            &ts,
            "Transaction",
            1_000_000 + i * 1000,
            json!({"user_id": "alice", "amount": 50.0, "status": "ok"}),
        )
        .await;
    }

    // GET /get/cnt/alice → {"value": 10}
    let resp = ts
        .post_json("/get", &json!({"keys": ["alice"], "features": ["cnt"]}))
        .await
        .expect("post /get");
    assert_eq!(resp.status().as_u16(), 200, "GET must succeed");
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(
        body["result"]["alice"]["cnt"], 10,
        "SC2: 10 pushes must yield count=10: {body:#}"
    );

    ts.shutdown().await.expect("shutdown");
}

/// SC2b: Push events with where-predicate filtering (7 ok, 3 failed); GET → 7.
#[tokio::test]
async fn sc2_push_with_where_filters() {
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
                "name": "TxCountOk",
                "output_kind": "table",
                "upstreams": ["Transaction"],
                "ops": [{
                    "op": "group_by",
                    "keys": ["user_id"],
                    "agg": {
                        "cnt_ok": {"op": "count", "params": {
                            "window": "5m",
                            "where": "(status == 'ok')"
                        }}
                    }
                }],
                "schema": {"fields": {"user_id": "str", "cnt_ok": "i64"}, "optional_fields": []},
                "table_primary_key": ["user_id"]
            }
        ]
    });
    let resp = ts.post_json("/register", &body).await.expect("register");
    assert_eq!(resp.status().as_u16(), 200);

    let base_time = 1_000_000i64;
    // 7 ok events
    for i in 0..7i64 {
        apply_event(
            &ts,
            "Transaction",
            base_time + i * 1000,
            json!({"user_id": "alice", "amount": 10.0, "status": "ok"}),
        )
        .await;
    }
    // 3 failed events
    for i in 7..10i64 {
        apply_event(
            &ts,
            "Transaction",
            base_time + i * 1000,
            json!({"user_id": "alice", "amount": 10.0, "status": "failed"}),
        )
        .await;
    }

    // GET cnt_ok for alice → 7 (only ok events counted)
    let get_resp = ts
        .post_json("/get", &json!({"keys": ["alice"], "features": ["cnt_ok"]}))
        .await
        .expect("post /get");
    assert_eq!(get_resp.status().as_u16(), 200);
    let result: serde_json::Value = get_resp.json().await.expect("json");
    assert_eq!(
        result["result"]["alice"]["cnt_ok"], 7,
        "SC2 where-filter: only 7 ok events should be counted: {result:#}"
    );

    ts.shutdown().await.expect("shutdown");
}

// ─── SC3 ──────────────────────────────────────────────────────────────────────

/// SC3: All 8 core operators pass table-driven E2E correctness tests.
///
/// Fixed event stream: 5 events with amount=[10,20,30,40,50], status=ok.
/// Plus 1 extra event with status="bad" for ratio testing (6 total, 5 ok → ratio 5/6).
///
/// Expected values (sample variance of [10,20,30,40,50]):
///   count=5, sum=150.0, avg=30.0, min=10.0, max=50.0, variance=250.0,
///   stddev=sqrt(250.0)≈15.8113883..., ratio(ok)=5/6≈0.8333...
#[tokio::test]
async fn sc3_all_8_operators_e2e() {
    let ts = TestServerBuilder::new()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn");

    // Register 8 separate aggregations.
    let body = json!({
        "nodes": [
            transaction_schema(),
            {
                "kind": "derivation",
                "name": "AggAll8",
                "output_kind": "table",
                "upstreams": ["Transaction"],
                "ops": [{
                    "op": "group_by",
                    "keys": ["user_id"],
                    "agg": {
                        "cnt":      {"op": "count",    "params": {"window": "1h"}},
                        "total":    {"op": "sum",      "params": {"field": "amount", "window": "1h"}},
                        "avg_amt":  {"op": "avg",      "params": {"field": "amount", "window": "1h"}},
                        "min_amt":  {"op": "min",      "params": {"field": "amount", "window": "1h"}},
                        "max_amt":  {"op": "max",      "params": {"field": "amount", "window": "1h"}},
                        "var_amt":  {"op": "variance", "params": {"field": "amount", "window": "1h"}},
                        "std_amt":  {"op": "stddev",   "params": {"field": "amount", "window": "1h"}},
                        "ratio_ok": {"op": "ratio",    "params": {"where": "(status == 'ok')", "window": "1h"}}
                    }
                }],
                "schema": {
                    "fields": {
                        "user_id": "str",
                        "cnt": "i64",
                        "total": "f64",
                        "avg_amt": "f64",
                        "min_amt": "f64",
                        "max_amt": "f64",
                        "var_amt": "f64",
                        "std_amt": "f64",
                        "ratio_ok": "f64"
                    },
                    "optional_fields": []
                },
                "table_primary_key": ["user_id"]
            }
        ]
    });
    let resp = ts.post_json("/register", &body).await.expect("register");
    assert_eq!(resp.status().as_u16(), 200, "SC3 register must succeed");

    let base_time = 1_000_000i64;
    let amounts = [10.0f64, 20.0, 30.0, 40.0, 50.0];
    // 5 ok events
    for (i, &amt) in amounts.iter().enumerate() {
        apply_event(
            &ts,
            "Transaction",
            base_time + i as i64 * 1000,
            json!({"user_id": "alice", "amount": amt, "status": "ok"}),
        )
        .await;
    }
    // 1 bad event (for ratio: 5 ok / 6 total)
    apply_event(
        &ts,
        "Transaction",
        base_time + 5 * 1000,
        json!({"user_id": "alice", "amount": 99.0, "status": "bad"}),
    )
    .await;

    // Query all features for alice.
    let features = vec![
        "cnt", "total", "avg_amt", "min_amt", "max_amt", "var_amt", "std_amt", "ratio_ok",
    ];
    let get_resp = ts
        .post_json("/get", &json!({"keys": ["alice"], "features": features}))
        .await
        .expect("post /get");
    assert_eq!(get_resp.status().as_u16(), 200);
    let result: serde_json::Value = get_resp.json().await.expect("json");
    let alice = &result["result"]["alice"];

    const TOL: f64 = 1e-9;

    // count: 6 (5 ok + 1 bad)
    assert_eq!(alice["cnt"], 6, "SC3 count: expected 6: {alice:#}");

    // sum: 10+20+30+40+50+99 = 249.0
    let total = alice["total"].as_f64().expect("total must be f64");
    assert!(
        (total - 249.0).abs() < TOL,
        "SC3 sum: expected 249.0, got {total}: {alice:#}"
    );

    // avg: (10+20+30+40+50+99)/6 = 249/6 = 41.5
    let avg = alice["avg_amt"].as_f64().expect("avg must be f64");
    assert!(
        (avg - 41.5).abs() < TOL,
        "SC3 avg: expected 41.5, got {avg}: {alice:#}"
    );

    // min: 10.0
    let min = alice["min_amt"].as_f64().expect("min must be f64");
    assert!(
        (min - 10.0).abs() < TOL,
        "SC3 min: expected 10.0, got {min}: {alice:#}"
    );

    // max: 99.0 (includes the bad event)
    let max = alice["max_amt"].as_f64().expect("max must be f64");
    assert!(
        (max - 99.0).abs() < TOL,
        "SC3 max: expected 99.0, got {max}: {alice:#}"
    );

    // variance: sample variance of [10,20,30,40,50,99].
    // mean=41.5; deviations: -31.5,-21.5,-11.5,-1.5,8.5,57.5
    // sum_sq = 992.25+462.25+132.25+2.25+72.25+3306.25 = 4967.5
    // sample variance = 4967.5/5 = 993.5
    let var = alice["var_amt"].as_f64().expect("var must be f64");
    assert!(
        (var - 993.5).abs() < 1e-6,
        "SC3 variance: expected 993.5, got {var}: {alice:#}"
    );

    // stddev: sqrt(993.5)
    let expected_std = 993.5f64.sqrt();
    let std = alice["std_amt"].as_f64().expect("std must be f64");
    assert!(
        (std - expected_std).abs() < 1e-6,
        "SC3 stddev: expected {expected_std}, got {std}: {alice:#}"
    );

    // ratio: 5 ok / 6 total = 5/6 ≈ 0.8333...
    let ratio = alice["ratio_ok"].as_f64().expect("ratio must be f64");
    let expected_ratio = 5.0 / 6.0;
    assert!(
        (ratio - expected_ratio).abs() < 1e-9,
        "SC3 ratio: expected {expected_ratio}, got {ratio}: {alice:#}"
    );

    ts.shutdown().await.expect("shutdown");
}

// ─── SC4 ──────────────────────────────────────────────────────────────────────

/// SC4 (INTEGRATION-layer gate for observable output).
///
/// SC4 layered coverage:
///   - Plan 05-01's `windowed_replay_determinism` proves byte-identical INTERNAL state
///     at the WindowedOp struct level (format!("{:?}", state) equality after 1000-event
///     stream applied twice). This is the UNIT-level gate.
///   - This test proves byte-identical OBSERVABLE output through the full apply-loop
///     + registry + GET wire path after the same 100-event stream applied to two fresh
///     TestServer instances.
/// Together: internal-state equality + faithful wire projection ⟹ byte-identical state
///           visible at every layer.
///
/// Input: 100 events with deterministic content.
///   user_id = "u{i % 3}", amount = i as f64, event_time_ms = 1000 * i
///   (no RNG; fixed formula so the test is always deterministic).
///
/// Comparison: raw response bodies from GET /get/cnt/u0, /get/cnt/u1, /get/cnt/u2 are
/// asserted byte-identical across the two runs (assert_eq! on String, not parsed values).
#[tokio::test]
async fn sc4_replay_determinism() {
    async fn run_instance(events: &[(i64, serde_json::Value)]) -> Vec<String> {
        let ts = TestServerBuilder::new()
            .dev_endpoints(true)
            .spawn()
            .await
            .expect("spawn replay instance");

        // Register Transaction + count-5m aggregation.
        let body = json!({
            "nodes": [
                {
                    "kind": "event",
                    "name": "Transaction",
                    "schema": {
                        "fields": {
                            "event_time": "i64",
                            "user_id": "str",
                            "amount": "f64",
                            "status": "str"
                        },
                        "optional_fields": []
                    },
                    "event_time_field": "event_time"
                },
                {
                    "kind": "derivation",
                    "name": "TxCount5m",
                    "output_kind": "table",
                    "upstreams": ["Transaction"],
                    "ops": [{
                        "op": "group_by",
                        "keys": ["user_id"],
                        "agg": {
                            "cnt": {"op": "count", "params": {"window": "5m"}}
                        }
                    }],
                    "schema": {"fields": {"user_id": "str", "cnt": "i64"}, "optional_fields": []},
                    "table_primary_key": ["user_id"]
                }
            ]
        });
        let resp = ts.post_json("/register", &body).await.expect("register");
        assert_eq!(resp.status().as_u16(), 200);

        // Apply events.
        for (event_time_ms, row) in events {
            let resp = ts
                .post_json(
                    "/dev/apply_events",
                    &json!({
                        "source": "Transaction",
                        "event_time_ms": event_time_ms,
                        "row": row
                    }),
                )
                .await
                .expect("apply_events");
            assert_eq!(resp.status().as_u16(), 200);
        }

        // Collect raw response bodies for u0, u1, u2.
        let mut bodies = Vec::new();
        for key in ["u0", "u1", "u2"] {
            let resp = ts
                .post_json("/get", &json!({"keys": [key], "features": ["cnt"]}))
                .await
                .expect("post /get");
            assert_eq!(resp.status().as_u16(), 200);
            let raw = resp.text().await.expect("text");
            bodies.push(raw);
        }

        ts.shutdown().await.expect("shutdown");
        bodies
    }

    // Build the deterministic 100-event stream.
    let events: Vec<(i64, serde_json::Value)> = (0..100i64)
        .map(|i| {
            let user_id = format!("u{}", i % 3);
            let event_time_ms = 1000 * i;
            let row = json!({
                "user_id": user_id,
                "amount": i as f64,
                "status": "ok"
            });
            (event_time_ms, row)
        })
        .collect();

    // Run the same event stream against two independent server instances.
    let bodies_a = run_instance(&events).await;
    let bodies_b = run_instance(&events).await;

    // Assert byte-identical observable output for each entity key.
    for (i, key) in ["u0", "u1", "u2"].iter().enumerate() {
        assert_eq!(
            bodies_a[i], bodies_b[i],
            "SC4 replay-determinism FAILED for key={key}: run A={:?} run B={:?}",
            bodies_a[i], bodies_b[i]
        );
    }
}

// ─── SC5 ──────────────────────────────────────────────────────────────────────

/// SC5a: Windowless (lifetime) count — window= omitted; all 100 events are counted.
#[tokio::test]
async fn sc5_lifetime_count_works() {
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
                "name": "TxCountLifetime",
                "output_kind": "table",
                "upstreams": ["Transaction"],
                "ops": [{
                    "op": "group_by",
                    "keys": ["user_id"],
                    "agg": {
                        "cnt_lifetime": {"op": "count", "params": {}}
                    }
                }],
                "schema": {"fields": {"user_id": "str", "cnt_lifetime": "i64"}, "optional_fields": []},
                "table_primary_key": ["user_id"]
            }
        ]
    });
    let resp = ts.post_json("/register", &body).await.expect("register");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "SC5 lifetime count register must succeed"
    );

    // Push 100 events across wide time range (no window to expire them).
    for i in 0..100i64 {
        apply_event(
            &ts,
            "Transaction",
            // Spread over several days to ensure no window would contain all of them
            i * 86_400_000, // 1 day apart in ms
            json!({"user_id": "alice", "amount": 1.0, "status": "ok"}),
        )
        .await;
    }

    let get_resp = ts
        .post_json(
            "/get",
            &json!({"keys": ["alice"], "features": ["cnt_lifetime"]}),
        )
        .await
        .expect("post /get");
    assert_eq!(get_resp.status().as_u16(), 200);
    let result: serde_json::Value = get_resp.json().await.expect("json");
    assert_eq!(
        result["result"]["alice"]["cnt_lifetime"], 100,
        "SC5 lifetime count: expected 100, got: {result:#}"
    );

    ts.shutdown().await.expect("shutdown");
}

/// SC5b: Windowless (lifetime) ratio — 10 events (3 ok, 7 bad); ratio = 3/10 = 0.3.
#[tokio::test]
async fn sc5_lifetime_ratio_works() {
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
                "name": "TxRatioLifetime",
                "output_kind": "table",
                "upstreams": ["Transaction"],
                "ops": [{
                    "op": "group_by",
                    "keys": ["user_id"],
                    "agg": {
                        "ratio_ok": {"op": "ratio", "params": {"where": "(status == 'ok')"}}
                    }
                }],
                "schema": {"fields": {"user_id": "str", "ratio_ok": "f64"}, "optional_fields": []},
                "table_primary_key": ["user_id"]
            }
        ]
    });
    let resp = ts.post_json("/register", &body).await.expect("register");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "SC5 lifetime ratio register must succeed"
    );

    // 3 ok events
    for i in 0..3i64 {
        apply_event(
            &ts,
            "Transaction",
            1_000_000 + i * 1000,
            json!({"user_id": "alice", "amount": 10.0, "status": "ok"}),
        )
        .await;
    }
    // 7 bad events
    for i in 3..10i64 {
        apply_event(
            &ts,
            "Transaction",
            1_000_000 + i * 1000,
            json!({"user_id": "alice", "amount": 10.0, "status": "bad"}),
        )
        .await;
    }

    let get_resp = ts
        .post_json(
            "/get",
            &json!({"keys": ["alice"], "features": ["ratio_ok"]}),
        )
        .await
        .expect("post /get");
    assert_eq!(get_resp.status().as_u16(), 200);
    let result: serde_json::Value = get_resp.json().await.expect("json");
    let ratio = result["result"]["alice"]["ratio_ok"]
        .as_f64()
        .expect("ratio must be f64");
    assert!(
        (ratio - 0.3).abs() < 1e-9,
        "SC5 lifetime ratio: expected 0.3, got {ratio}: {result:#}"
    );

    ts.shutdown().await.expect("shutdown");
}

// ─── SC6 ──────────────────────────────────────────────────────────────────────

/// SC6a: POST /register with sum(field="nonexistent") → 400 with aggregation_unknown_field.
#[tokio::test]
async fn sc6_unknown_field_rejected() {
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
                "name": "BadAgg",
                "output_kind": "table",
                "upstreams": ["Transaction"],
                "ops": [{
                    "op": "group_by",
                    "keys": ["user_id"],
                    "agg": {
                        "bad_sum": {"op": "sum", "params": {"field": "nonexistent", "window": "5m"}}
                    }
                }],
                "schema": {"fields": {"user_id": "str", "bad_sum": "f64"}, "optional_fields": []},
                "table_primary_key": ["user_id"]
            }
        ]
    });

    let resp = ts.post_json("/register", &body).await.expect("register");
    assert_eq!(
        resp.status().as_u16(),
        400,
        "SC6: unknown field must return 400"
    );
    let err_body: serde_json::Value = resp.json().await.expect("json");
    let code = err_body["error"]["code"].as_str().unwrap_or("");
    assert_eq!(
        code, "aggregation_unknown_field",
        "SC6: error code must be aggregation_unknown_field: {err_body:#}"
    );
    // The reason must reference the unknown field name.
    let reason = err_body["error"]["reason"].as_str().unwrap_or("");
    assert!(
        reason.contains("nonexistent"),
        "SC6: error reason must reference 'nonexistent': {err_body:#}"
    );

    ts.shutdown().await.expect("shutdown");
}

/// SC6b: POST /register with aggregation on a Table source → 400 with
/// aggregation_on_table_not_supported.
#[tokio::test]
async fn sc6_aggregation_on_table_rejected() {
    let ts = TestServerBuilder::new()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn");

    // First register a Table derivation.
    let body = json!({
        "nodes": [
            transaction_schema(),
            {
                "kind": "derivation",
                "name": "TxTable",
                "output_kind": "table",
                "upstreams": ["Transaction"],
                "ops": [{
                    "op": "group_by",
                    "keys": ["user_id"],
                    "agg": {
                        "cnt": {"op": "count", "params": {"window": "5m"}}
                    }
                }],
                "schema": {"fields": {"user_id": "str", "cnt": "i64"}, "optional_fields": []},
                "table_primary_key": ["user_id"]
            }
        ]
    });
    let resp = ts
        .post_json("/register", &body)
        .await
        .expect("register TxTable");
    assert_eq!(resp.status().as_u16(), 200);

    // Now attempt to aggregate on the Table (should fail).
    let bad_body = json!({
        "nodes": [{
            "kind": "derivation",
            "name": "BadNestedAgg",
            "output_kind": "table",
            "upstreams": ["TxTable"],
            "ops": [{
                "op": "group_by",
                "keys": ["user_id"],
                "agg": {
                    "cnt2": {"op": "count", "params": {"window": "1h"}}
                }
            }],
            "schema": {"fields": {"user_id": "str", "cnt2": "i64"}, "optional_fields": []},
            "table_primary_key": ["user_id"]
        }]
    });

    let resp = ts
        .post_json("/register", &bad_body)
        .await
        .expect("register bad nested agg");
    assert_eq!(
        resp.status().as_u16(),
        400,
        "SC6: aggregation on Table must return 400"
    );
    let err_body: serde_json::Value = resp.json().await.expect("json");
    let code = err_body["error"]["code"].as_str().unwrap_or("");
    assert_eq!(
        code, "aggregation_on_table_not_supported",
        "SC6: error code must be aggregation_on_table_not_supported: {err_body:#}"
    );

    ts.shutdown().await.expect("shutdown");
}

// ─── Envelope shape (D-02) ───────────────────────────────────────────────────

/// D-02: GET /get response has exactly one top-level key, "value".
/// No "meta", no "updated_at", no "value_and_meta".
#[tokio::test]
async fn envelope_shape_is_value_only() {
    let ts = TestServerBuilder::new()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn");

    // Register + push one event.
    let body = json!({
        "nodes": [
            transaction_schema(),
            {
                "kind": "derivation",
                "name": "TxCount5m",
                "output_kind": "table",
                "upstreams": ["Transaction"],
                "ops": [{
                    "op": "group_by",
                    "keys": ["user_id"],
                    "agg": {
                        "cnt": {"op": "count", "params": {"window": "5m"}}
                    }
                }],
                "schema": {"fields": {"user_id": "str", "cnt": "i64"}, "optional_fields": []},
                "table_primary_key": ["user_id"]
            }
        ]
    });
    let resp = ts.post_json("/register", &body).await.expect("register");
    assert_eq!(resp.status().as_u16(), 200);

    apply_event(
        &ts,
        "Transaction",
        1_000_000,
        json!({"user_id": "alice", "amount": 10.0, "status": "ok"}),
    )
    .await;

    // Use GET /get/{feature}/{key} — this returns the single-feature envelope.
    let resp = ts.get_raw("/get/cnt/alice").await;
    assert_eq!(resp.status().as_u16(), 200);
    let body: serde_json::Value = resp.json().await.expect("json");
    let obj = body.as_object().expect("response must be a JSON object");
    assert_eq!(
        obj.len(),
        1,
        "D-02: response must have exactly 1 key, got: {:?}",
        obj.keys().collect::<Vec<_>>()
    );
    assert!(
        obj.contains_key("value"),
        "D-02: response key must be 'value', got: {:?}",
        obj.keys().collect::<Vec<_>>()
    );
    assert!(
        !obj.contains_key("meta"),
        "D-02: response must NOT contain 'meta'"
    );
    assert!(
        !obj.contains_key("updated_at"),
        "D-02: response must NOT contain 'updated_at'"
    );

    ts.shutdown().await.expect("shutdown");
}
