//! Phase 13.4 Plan 04 — verb-style HTTP routes (router gate).
//!
//! Adds POST /ping, POST /push (event in body), POST /push-sync (event in body)
//! per CONTEXT scope item #4. Legacy path-segment routes stay alive for
//! backward compat per A-07 in `SCRATCH-PLANNER-NOTES.md` — Phase 13.5/13.6/13.7
//! SDKs use the new verb-style routes; legacy callers don't break.
//!
//! TDD: this is the RED gate — `Route::Ping`, `Route::PushVerb`, and
//! `Route::PushSyncVerb` do not exist yet, so the test file fails to compile
//! until Task 4.b adds the variants and route arms.

use beava_runtime_core::router::{Route, Router};

/// Test 1 — `POST /ping` resolves to the new `Route::Ping` variant
/// (verb-style liveness probe; HTTP mirror of TCP `OP_PING (0x0000)`).
#[test]
fn route_post_ping_returns_route_ping() {
    assert_eq!(Router::route("POST", "/ping"), Route::Ping);
}

/// Test 2 — wrong-method on `/ping` returns `Route::MethodNotAllowed`
/// (consistent with the rest of the verb-style POST table).
#[test]
fn route_get_ping_returns_method_not_allowed() {
    assert_eq!(Router::route("GET", "/ping"), Route::MethodNotAllowed);
}

/// Test 3 — `POST /push` (no path segment) resolves to the new
/// `Route::PushVerb` variant. Event name lives in the JSON body
/// (`{"event":"Tx","data":{...}}`), parsed by Task 4.d's
/// `http_listener::parse_verb_push` helper.
#[test]
fn route_post_push_verb_returns_pushverb() {
    assert_eq!(Router::route("POST", "/push"), Route::PushVerb);
}

/// Test 4 — legacy `POST /push/:event_name` still resolves to the existing
/// `Route::Push { event_name }` variant. A-07 backward-compat: ~20 in-tree
/// tests rely on this URL shape and must keep passing during the migration.
#[test]
fn route_post_push_with_event_name_still_returns_legacy_push() {
    assert_eq!(
        Router::route("POST", "/push/Tx"),
        Route::Push {
            event_name: "Tx".to_owned()
        }
    );
}

/// Test 5 — `POST /push-sync` (no path segment) resolves to the new
/// `Route::PushSyncVerb` variant. Event name in body, awaits fsync.
#[test]
fn route_post_push_sync_verb_returns_pushsyncverb() {
    assert_eq!(Router::route("POST", "/push-sync"), Route::PushSyncVerb);
}

/// Test 6 — legacy `POST /push-sync/:event_name` still resolves to
/// `Route::PushSync { event_name }` (A-07 backward-compat).
#[test]
fn route_post_push_sync_with_event_name_still_legacy() {
    assert_eq!(
        Router::route("POST", "/push-sync/Tx"),
        Route::PushSync {
            event_name: "Tx".to_owned()
        }
    );
}

/// Test 7 — unknown path still returns `Route::NotFound` (regression guard
/// — adding the new exact-match arms must not steal traffic from the
/// catch-all).
#[test]
fn route_unknown_path_returns_notfound() {
    assert_eq!(Router::route("POST", "/whatever"), Route::NotFound);
}
