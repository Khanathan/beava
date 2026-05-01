//! Phase 11.5 — Temporal tables + retraction primitive integration smoke.
//!
//! Mirrors ROADMAP success criteria #2-#5 against a real `TestServer`.
//! See `.planning/phases/11.5-temporal-tables-retraction-primitive/11.5-CONTEXT.md`
//! decisions D-04 (tombstone retraction), D-07 (as_of semantics), D-08
//! (non-temporal as_of → 400), D-17 (retract API error shapes).

#![cfg(feature = "testing")]

use beava_server::testing::TestServer;
use serde_json::json;

fn temporal_table_node(name: &str, retention_ms: u64) -> serde_json::Value {
    json!({
        "kind": "table",
        "name": name,
        "primary_key": ["k"],
        "schema": {
            "fields": {"k": "str", "v": "i64"},
            "optional_fields": []
        },
        "mode": "upsert",
        "temporal": true,
        "retention_ms": retention_ms
    })
}

fn non_temporal_table_node(name: &str) -> serde_json::Value {
    json!({
        "kind": "table",
        "name": name,
        "primary_key": ["k"],
        "schema": {
            "fields": {"k": "str", "v": "i64"},
            "optional_fields": []
        },
        "mode": "upsert"
    })
}

// Phase 12.7 Plan 01: this test registers a `kind: "table"` payload, which is
// now rejected at register-time by `pre_check_unsupported_node_kind` per
// `project_v0_events_only_scope` (locked 2026-04-30). The full file is slated
// for deletion in Phase 12.7's later waves (CONTEXT.md §"Test sweep — DELETE").
// Ignored until then so the workspace stays green.
#[ignore = "Phase 12.7-01: registers kind=table; file slated for deletion in 12.7 later waves"]
#[tokio::test]
async fn registry_reports_temporal_flag() {
    // SC #2: GET /registry surfaces temporal: true|false per table.
    let ts = TestServer::builder()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn");

    let body = json!({
        "nodes": [
            temporal_table_node("t_yes", 60_000),
            non_temporal_table_node("t_no"),
        ]
    });
    let resp = ts.post_json("/register", &body).await.expect("register");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "register: {}",
        resp.text().await.unwrap()
    );

    let reg = ts.get_json("/registry").await;
    let tables = reg["tables"].as_object().expect("tables map");
    let yes = &tables["t_yes"];
    let no = &tables["t_no"];
    assert_eq!(yes["temporal"], true, "t_yes.temporal: {yes:?}");
    assert_eq!(yes["retention_ms"], 60_000);
    // Non-temporal: either omitted (None serialization) or false.
    assert!(
        no.get("temporal").is_none() || no["temporal"] == false,
        "t_no should be non-temporal: {no:?}"
    );

    ts.shutdown().await.expect("shutdown");
}

// Phase 12.7 Plan 01: registers kind=table — now rejected by the shim.
// File slated for deletion in 12.7 later waves.
#[ignore = "Phase 12.7-01: registers kind=table; file slated for deletion in 12.7 later waves"]
#[tokio::test]
async fn temporal_table_upsert_retract_returns_prior_value() {
    // SC #5: register temporal table, upsert at t=0, upsert at t=1, retract t=1,
    // assert GET returns t=0 value AND GET as_of=t=0 returns t=0 regardless.
    let ts = TestServer::builder()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn");

    let reg = json!({"nodes": [temporal_table_node("merch", 3_600_000)]});
    let r = ts.post_json("/register", &reg).await.expect("register");
    assert_eq!(
        r.status().as_u16(),
        200,
        "register: {}",
        r.text().await.unwrap()
    );

    // Upsert at t=0.
    let r0 = ts
        .post_json("/upsert/merch", &json!({"k": "m1", "v": 100}))
        .await
        .expect("push 1");
    assert_eq!(
        r0.status().as_u16(),
        200,
        "push1: {}",
        r0.text().await.unwrap()
    );
    let ack0: serde_json::Value = r0.json().await.expect("ack 1");
    let lsn0 = ack0["ack_lsn"].as_u64().expect("ack_lsn");

    // Upsert at t=1.
    let r1 = ts
        .post_json("/upsert/merch", &json!({"k": "m1", "v": 200}))
        .await
        .expect("push 2");
    assert_eq!(r1.status().as_u16(), 200);
    let ack1: serde_json::Value = r1.json().await.expect("ack 2");
    let lsn1 = ack1["ack_lsn"].as_u64().expect("ack_lsn");
    assert!(lsn1 > lsn0);

    // GET current → v=200.
    let g_now = ts.get_json("/table/merch?key=m1").await;
    assert_eq!(g_now["row"]["v"], 200, "current row: {g_now:?}");

    // GET as_of=lsn0 → v=100.
    let g_past = ts
        .get_json(&format!("/table/merch?key=m1&as_of={lsn0}"))
        .await;
    assert_eq!(g_past["row"]["v"], 100, "as_of past: {g_past:?}");

    // POST /retract event_id=lsn1.
    let r_retract = ts
        .post_json("/retract", &json!({"event_id": lsn1}))
        .await
        .expect("retract");
    assert_eq!(
        r_retract.status().as_u16(),
        200,
        "retract: {}",
        r_retract.text().await.unwrap()
    );

    // GET current → v=100 (lsn0 restored).
    let g_after = ts.get_json("/table/merch?key=m1").await;
    assert_eq!(
        g_after["row"]["v"], 100,
        "after retract, current: {g_after:?}"
    );

    // GET as_of=lsn0 → still v=100 (history before retraction is preserved).
    let g_past2 = ts
        .get_json(&format!("/table/merch?key=m1&as_of={lsn0}"))
        .await;
    assert_eq!(
        g_past2["row"]["v"], 100,
        "as_of=lsn0 after retract: {g_past2:?}"
    );

    ts.shutdown().await.expect("shutdown");
}

// Phase 12.7-03: /retract route deleted; the deleted route falls through to
// mio's default 404 (`{"error": {"code": "not_found", "path": "/retract"}}`)
// per CONTEXT D-02. Old assertion (`body["error"] == "event_id_not_found"`)
// no longer holds. File slated for deletion in 12.7-06.
#[ignore = "Phase 12.7-03: /retract route deleted; file slated for deletion in 12.7-06"]
#[tokio::test]
async fn retract_unknown_event_id_returns_404() {
    let ts = TestServer::builder()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn");
    let resp = ts
        .post_json("/retract", &json!({"event_id": 999_999}))
        .await
        .expect("retract");
    assert_eq!(resp.status().as_u16(), 404, "unknown event_id");
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(body["error"], "event_id_not_found");
    ts.shutdown().await.expect("shutdown");
}

// Phase 12.7 Plan 01: registers kind=table — now rejected by the shim.
// File slated for deletion in 12.7 later waves.
#[ignore = "Phase 12.7-01: registers kind=table; file slated for deletion in 12.7 later waves"]
#[tokio::test]
async fn retract_on_non_temporal_table_returns_400() {
    let ts = TestServer::builder()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn");
    let reg = json!({"nodes": [non_temporal_table_node("u")]});
    ts.post_json("/register", &reg).await.expect("register");
    let r = ts
        .post_json("/upsert/u", &json!({"k": "u1", "v": 1}))
        .await
        .expect("push");
    assert_eq!(r.status().as_u16(), 200);
    let ack: serde_json::Value = r.json().await.unwrap();
    let lsn = ack["ack_lsn"].as_u64().unwrap();

    let rr = ts
        .post_json("/retract", &json!({"event_id": lsn}))
        .await
        .expect("retract");
    assert_eq!(rr.status().as_u16(), 400);
    let body: serde_json::Value = rr.json().await.expect("json");
    assert_eq!(body["error"], "table_not_temporal");
    ts.shutdown().await.expect("shutdown");
}

// Phase 12.7-03: /retract route deleted; the deleted route falls through to
// mio's default 404 (instead of the temporal_http 501 stream-retraction shape).
// Old assertion (`status == 501; body["error"] == "stream_retraction_unimplemented"`)
// no longer holds. File slated for deletion in 12.7-06.
#[ignore = "Phase 12.7-03: /retract route deleted; file slated for deletion in 12.7-06"]
#[tokio::test]
async fn retract_on_stream_event_returns_501() {
    // SC #4: stream retraction explicitly rejected with the documented shape.
    let ts = TestServer::builder()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn");

    let reg = json!({
        "nodes": [{
            "kind": "event",
            "name": "click",
            "schema": {"fields": {"u": "str"}, "optional_fields": []}
        }]
    });
    ts.post_json("/register", &reg).await.expect("register");

    let r = ts
        .post_json("/push/click", &json!({"u": "u1"}))
        .await
        .expect("push");
    assert_eq!(r.status().as_u16(), 200);
    let ack: serde_json::Value = r.json().await.unwrap();
    let lsn = ack["ack_lsn"].as_u64().unwrap();

    let rr = ts
        .post_json("/retract", &json!({"event_id": lsn}))
        .await
        .expect("retract");
    assert_eq!(rr.status().as_u16(), 501, "stream retract → 501");
    let body: serde_json::Value = rr.json().await.expect("json");
    assert_eq!(body["error"], "stream_retraction_unimplemented");
    assert!(
        body["see"].as_str().is_some(),
        "501 body must include 'see' breadcrumb: {body:?}"
    );
    ts.shutdown().await.expect("shutdown");
}

// Phase 12.7 Plan 01: registers kind=table — now rejected by the shim.
// File slated for deletion in 12.7 later waves.
#[ignore = "Phase 12.7-01: registers kind=table; file slated for deletion in 12.7 later waves"]
#[tokio::test]
async fn as_of_on_non_temporal_table_returns_400() {
    // SC #2: as_of against a non-temporal table is a 400 with a clear error code.
    let ts = TestServer::builder()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn");

    let reg = json!({"nodes": [non_temporal_table_node("u")]});
    ts.post_json("/register", &reg).await.expect("register");

    let resp = ts.get_raw("/table/u?key=x&as_of=10").await;
    assert_eq!(resp.status().as_u16(), 400);
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(body["error"], "as_of_requires_temporal");

    ts.shutdown().await.expect("shutdown");
}
