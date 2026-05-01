//! Phase 12.7 Plan 03 — table routes return plain 404 (mio default).
//!
//! Per `project_v0_events_only_scope` (locked 2026-04-30) v0 ships
//! events-only.  Plan 12.7-03 deletes the `Route::Upsert` /
//! `Route::Delete` / `Route::Retract` / `Route::TableGet` enum variants
//! and the matching `WireRequest::HttpUpsert` / `HttpDelete` /
//! `HttpRetract` / `HttpTableGet` variants, plus the dispatch arms in
//! `apply_shard.rs` that consumed them.
//!
//! Per CONTEXT.md D-02 framing the deleted routes return **plain 404**
//! from mio (its default for unknown routes — `Route::NotFound ->
//! WireRequest::HttpNotFound -> 404`). NO special deny-handler;
//! NO `feature_removed_no_tables_v0` retrospective error code; just
//! "endpoint not found", same as any other unknown route.  v0 is the
//! FIRST public release; users never knew tables existed in v0, so a
//! retrospective code framing would confuse fresh users.
//!
//! ## Test set
//!
//! - **Tests 1-4 (RED → GREEN by Plan 12.7-03):** POST `/upsert/{table}`,
//!   POST `/delete/{table}`, POST `/retract`, GET `/table/{table}` all
//!   return HTTP 404.  RED at HEAD because the routes still resolve to
//!   the temporal_http handlers and respond 200/4xx (NOT 404).  GREEN
//!   once Plan 03 deletes the routes — mio's `Route::NotFound`
//!   fallback kicks in.
//!
//! - **Test 5 (sanity — surviving routes):** POST `/register` with a
//!   valid event payload returns 2xx.  Confirms the deletion in Plan 03
//!   didn't accidentally break the surviving HTTP surface.

#![cfg(feature = "testing")]

use beava_server::testing::TestServer;
use serde_json::json;

/// Helper — assert that a 404 response carries mio's default
/// "route not found" body shape (`{"error": {"code": "not_found",
/// "path": ...}}`) rather than a structured handler-specific 404 body
/// (`{"error": "table_not_found"}`, etc., which is what
/// `temporal_http::*_via_mio` emits pre-deletion).
///
/// Pre-Plan-12.7-03 the temporal_http handlers emit string-valued
/// `error` bodies (e.g. `{"error": "table_not_found"}`); mio's default
/// `Route::NotFound` emits an object-valued `error` body
/// (`{"error": {"code": "not_found", "path": "/upsert/X"}}`).  That
/// distinction is the discriminator Plan 03 needs:
/// - **RED** (HEAD): handler emits string-valued `error` — assert
///   `error.code == "not_found"` FAILS because `error` is a string.
/// - **GREEN** (post-Plan-03): mio default emits object-valued `error`
///   with `code == "not_found"` — assertion passes.
fn assert_plain_404_route_not_found(status: u16, body_text: &str, route: &str) {
    assert_eq!(
        status, 404,
        "{route} must return HTTP 404 after Plan 12.7-03 deletes the route (got {status})"
    );
    let body: serde_json::Value = serde_json::from_str(body_text)
        .unwrap_or_else(|_| panic!("{route} 404 body is not JSON: {body_text}"));
    let code = body["error"]["code"].as_str();
    assert_eq!(
        code,
        Some("not_found"),
        "{route} 404 body must be mio's default route-not-found shape \
         (`error.code == \"not_found\"`) per CONTEXT D-02 (no special deny-handler); \
         got body={body}"
    );
}

/// Test 1 — POST `/upsert/{table}` returns plain 404 from mio.
///
/// Pre-Plan-12.7-03: `Route::Upsert` resolves to
/// `WireRequest::HttpUpsert` → `temporal_http::upsert_via_mio` which
/// returns a 404 with `{"error": "table_not_found"}` (string-valued)
/// when the table isn't registered.  This RED test asserts the body
/// MUST be mio's default route-not-found shape (`error.code ==
/// "not_found"`); the temporal_http body fails that assertion.
///
/// Post-Plan-12.7-03: `/upsert/UserProfile` falls through to
/// `Route::NotFound` (no parse arm matches) → `WireRequest::HttpNotFound`
/// → mio's default 404 with `error.code == "not_found"`.  Same UX as
/// any other unknown route per D-02.
#[tokio::test]
async fn post_upsert_returns_404() {
    let ts = TestServer::spawn().await.expect("spawn");
    let resp = ts
        .post_json("/upsert/UserProfile", &json!({"k": "a", "v": 1}))
        .await
        .expect("upsert");
    let status = resp.status().as_u16();
    let body_text = resp.text().await.expect("body text");
    assert_plain_404_route_not_found(status, &body_text, "POST /upsert/UserProfile");
    ts.shutdown().await.ok();
}

/// Test 2 — POST `/delete/{table}` returns plain 404 from mio.
///
/// Pre-Plan-12.7-03: `Route::Delete` resolves to
/// `WireRequest::HttpDelete` → `temporal_http::delete_via_mio` (returns
/// 400 for malformed JSON, 404 for unknown table, etc., but never
/// mio's default 404 body shape).
///
/// Post-Plan-12.7-03: `/delete/UserProfile` falls through to
/// `Route::NotFound` → mio's default 404.
#[tokio::test]
async fn post_delete_returns_404() {
    let ts = TestServer::spawn().await.expect("spawn");
    let resp = ts
        .post_json("/delete/UserProfile", &json!({"k": "a"}))
        .await
        .expect("delete");
    let status = resp.status().as_u16();
    let body_text = resp.text().await.expect("body text");
    assert_plain_404_route_not_found(status, &body_text, "POST /delete/UserProfile");
    ts.shutdown().await.ok();
}

/// Test 3 — POST `/retract` returns plain 404 from mio.
///
/// Pre-Plan-12.7-03: `Route::Retract` resolves to
/// `WireRequest::HttpRetract` → `temporal_http::retract_via_mio`
/// (which used to return 501 / 400 / 404 for retract paths per D-12 / D-17).
///
/// Post-Plan-12.7-03: `/retract` falls through to `Route::NotFound` →
/// mio's default 404.
#[tokio::test]
async fn post_retract_returns_404() {
    let ts = TestServer::spawn().await.expect("spawn");
    let resp = ts
        .post_json("/retract", &json!({"event_id": 1}))
        .await
        .expect("retract");
    let status = resp.status().as_u16();
    let body_text = resp.text().await.expect("body text");
    assert_plain_404_route_not_found(status, &body_text, "POST /retract");
    ts.shutdown().await.ok();
}

/// Test 4 — GET `/table/{table}?key=...` returns plain 404 from mio.
///
/// Pre-Plan-12.7-03: `Route::TableGet` resolves to
/// `WireRequest::HttpTableGet` → `temporal_http::table_get_via_mio`
/// (404 for unknown table, but with a structured body — not mio's
/// default route-not-found shape).
///
/// Post-Plan-12.7-03: `/table/UserProfile` falls through to
/// `Route::NotFound` → mio's default 404.
#[tokio::test]
async fn get_table_returns_404() {
    let ts = TestServer::spawn().await.expect("spawn");
    let resp = ts.get_raw("/table/UserProfile?key=foo").await;
    let status = resp.status().as_u16();
    let body_text = resp.text().await.expect("body text");
    assert_plain_404_route_not_found(status, &body_text, "GET /table/UserProfile");
    ts.shutdown().await.ok();
}

/// Test 5 — surviving routes still work (sanity for non-table HTTP surface).
///
/// Confirms the route-deletion in Plan 03 didn't accidentally break
/// `/register` (or any other surviving route).  Post-Plan-12.7-03
/// register payloads with `kind: "event"` should still return 200.
///
/// This test is GREEN at HEAD (independent of Plan 03's deletion);
/// it acts as a tripwire for the surviving HTTP surface.  If Plan 03's
/// surgery accidentally drops `Route::Register` or its parse arm, this
/// test catches it.
#[tokio::test]
async fn surviving_routes_still_work() {
    let ts = TestServer::spawn().await.expect("spawn");
    let payload = json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Transaction",
                "schema": {
                    "fields": {"amount": "f64"},
                    "optional_fields": []
                }
            }
        ]
    });
    let resp = ts
        .post_json("/register", &payload)
        .await
        .expect("register");
    assert!(
        (200..300).contains(&resp.status().as_u16()),
        "POST /register with valid event payload must return 2xx (got {})",
        resp.status().as_u16()
    );
    ts.shutdown().await.ok();
}
