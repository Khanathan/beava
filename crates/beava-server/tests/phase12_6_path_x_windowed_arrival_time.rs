//! Phase 12.6-05 Path X: windowed-op time-source swap — integration smoke.
//!
//! Asserts that windowed-op bucketing uses **server-side wall-clock at apply
//! time** (`SystemTime::now()`), NOT the `event_time` field read from the row
//! body. Per `project_redis_shaped_no_event_time_ever` and CONTEXT D-03 (hard
//! rip — no event_time semantics anywhere).
//!
//! Pre-Path-X behaviour (RED state): `apply_shard.rs::dispatch_push_sync` reads
//! `event_time` from the row's body via `descriptor.event_time_field`. Two
//! events whose body `event_time` values differ by years land in completely
//! different windowed buckets, even though both arrived in the same wall-clock
//! millisecond at the server. A 60s rolling count near the most-recent arrival
//! returns 1 (the older event's bucket has aged out by years).
//!
//! Post-Path-X (GREEN state): the apply path computes
//! `let now_ms = SystemTime::now()...as_millis() as i64;` and threads it to
//! `apply_event_to_aggregations`, ignoring whatever `event_time` value the
//! caller put in the body. Both events land in the same arrival-time bucket
//! and the rolling count returns the full event count.

#![cfg(feature = "testing")]

use beava_server::testing::TestServer;
use serde_json::json;

/// Path X invariant: windowed bucketing keys on **server arrival time**, not
/// on `event_time` read from the row body.
///
/// Test shape:
/// 1. Register a `Transaction` event with `event_time_field: "event_time"`
///    (so the legacy code would honour the body's `event_time`).
/// 2. Register a `TxCount60s` derivation with a 60s rolling count.
/// 3. Push three events for the same `user_id` whose body `event_time` values
///    span years apart (year 1970 vs year 33658 vs year 1970+1ms).
/// 4. Query `cnt[user_id]`.
/// 5. Pre-Path-X: returns 1 (only the most-recent body-event_time bucket is
///    "active" relative to the GET's query_time_ms).
/// 6. Post-Path-X: returns 3 (all three arrived within the same wall-clock
///    second; all three buckets are active).
#[tokio::test]
async fn path_x_bucketing_is_on_arrival_not_event_time() {
    let ts = TestServer::builder()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn TestServer");

    // Register Transaction (with event_time_field set — legacy honours it
    // pre-Path-X) and a 60s rolling count derivation.
    let registry_payload = json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Transaction",
                "schema": {
                    "fields": {
                        "user_id": "str",
                        "event_time": "i64"
                    },
                    "optional_fields": []
                },
                "event_time_field": "event_time"
            },
            {
                "kind": "derivation",
                "name": "TxCount60s",
                "output_kind": "table",
                "upstreams": ["Transaction"],
                "ops": [{
                    "op": "group_by",
                    "keys": ["user_id"],
                    "agg": {
                        "cnt": {"op": "count", "params": {"window": "60s"}}
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
    let resp = ts
        .post_json("/register", &registry_payload)
        .await
        .expect("register");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "register must succeed: {}",
        resp.text().await.unwrap_or_default()
    );

    // Push three events for the same user with VERY different body
    // `event_time` values, but consecutively (within a single wall-clock
    // second on the server). Pre-Path-X the server would bucket each event
    // by its body `event_time` — and three buckets centuries apart cannot
    // simultaneously be active in any 60s window. Post-Path-X the server
    // ignores the body field entirely and buckets all three on the
    // arrival-time `now_ms`.
    let push_one = |body: serde_json::Value| {
        let ts = &ts;
        async move {
            let r = ts
                .post_json("/push/Transaction", &body)
                .await
                .expect("push");
            assert_eq!(
                r.status().as_u16(),
                200,
                "push must succeed: {}",
                r.text().await.unwrap_or_default()
            );
        }
    };

    // event_time=1ms (1970-01-01 00:00:00.001 UTC).
    push_one(json!({"user_id": "u1", "event_time": 1_i64})).await;
    // event_time = year 33658 (massively in the future — 999_999_999_999 ms).
    push_one(json!({"user_id": "u1", "event_time": 999_999_999_999_i64})).await;
    // event_time=2ms (also 1970, just one ms after the first).
    push_one(json!({"user_id": "u1", "event_time": 2_i64})).await;

    // Query the rolling 60s count for u1.
    let resp = ts
        .post_json(
            "/get",
            &json!({"keys": ["u1"], "features": ["cnt"]}),
        )
        .await
        .expect("post /get");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "GET must succeed: {}",
        resp.text().await.unwrap_or_default()
    );
    let body: serde_json::Value = resp.json().await.expect("get response json");

    // Path X assertion: all three events should be in the same arrival-time
    // 60s bucket. Pre-Path-X returns 1 (most-recent body-event_time wins
    // bucket-by-bucket; only one bucket is active at the GET query_time).
    let cnt = body["result"]["u1"]["cnt"].as_i64();
    assert_eq!(
        cnt,
        Some(3),
        "Path X invariant: all three events arrived in the same wall-clock \
         second so the 60s rolling count must be 3. Got {body:#}. \
         If this returns 1 the apply path is still bucketing on body \
         `event_time` instead of server `now_ms()` — see CONTEXT D-03 \
         and `project_redis_shaped_no_event_time_ever`."
    );

    ts.shutdown().await.expect("shutdown");
}
