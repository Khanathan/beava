//! Phase 56 SC-3 — `register()` accepts mismatched shard_key joins with a
//! logged `CrossShardJoinWarning` (TPC-CORR-04 relaxation).
//!
//! Contract (D-B4 / D-C1 / D-C2 / D-C3):
//! - Pre-Phase-56: `register()` returned
//!   `Err(BeavaError::Protocol("join operator between L and R: shard_key mismatch..."))`.
//! - Phase 56: `register()` returns `Ok(_)`. A warning line with content
//!   "CrossShardJoinWarning" + the join field + both shard keys is emitted
//!   (via `eprintln!` — this codebase does not pull in the `tracing` crate).
//!   The warning is surfaced via `GET /debug/warnings` under the top-level
//!   `cross_shard_joins: [{join_id, left_shard_key, right_shard_key,
//!   on_field, perf_note}]` array AND as a `Category::Safety` /
//!   `Severity::Warning` signal in the unified `warnings` feed. Counter
//!   `beava_crossshard_joins_registered_total{join_id}` increments on each
//!   relaxation event.
//!
//! Co-located case (both sides `shard_key=join.on`) does NOT emit the warning.
//!
//! Wave 3 (plan 56-03) relaxes `validate_shard_keys` in
//! `src/engine/register.rs` + `src/engine/join_validator.rs` and extends
//! `/debug/warnings` with the `cross_shard_joins` field. Passes at Wave 3.
//!
//! Wave 3 deviation — `warnings` top-level shape is kept as a flat array
//! (Phase 51 back-compat: tests/test_debug_warnings_endpoint.rs +
//! tests/test_warnings_feed.rs assert `body["warnings"].as_array()`).
//! The structured cross-shard-join surface lands under
//! `body["cross_shard_joins"]` instead of `body["warnings"]["cross_shard_joins"]`.
//!
//! Run:
//!   cargo test --release --test register_crossshard_join_warning

#![cfg(not(feature = "state-inmem"))]

use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use beava::engine::join_validator::{
    validate_shard_keys, CrossShardJoinWarning, ShardKeySpec,
};
use beava::engine::pipeline::{FeatureDef, JoinType, PipelineEngine, StreamDefinition};
use beava::server::http::build_router;
use beava::server::signals::{emit_cross_shard_join_warning, SignalRegistry};
use beava::server::tcp::{make_concurrent_state_full, BackfillTracker, SharedState};

// ---------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------

fn make_stream_def(name: &str, shard_key: Option<ShardKeySpec>) -> StreamDefinition {
    StreamDefinition {
        name: name.into(),
        key_field: Some("user_id".into()),
        group_by_keys: None,
        features: Vec::new(),
        depends_on: None,
        filter: None,
        entity_ttl: None,
        history_ttl: None,
        projection: None,
        ephemeral: None,
        pipeline_ttl: None,
        max_keys: None,
        watermark_lateness: None,
        shard_key,
    }
}

fn make_join_def(
    name: &str,
    shard_key: Option<ShardKeySpec>,
    left: &str,
    right: &str,
    on: &str,
) -> StreamDefinition {
    StreamDefinition {
        name: name.into(),
        key_field: None,
        group_by_keys: None,
        features: vec![(
            "ssj".into(),
            FeatureDef::StreamStreamJoin {
                left_stream: left.into(),
                right_stream: right.into(),
                on: vec![on.into()],
                within_ms: 60_000,
                join_type: JoinType::Inner,
                left_fields: vec![],
                right_fields: vec![],
            },
        )],
        depends_on: Some(vec![left.into(), right.into()]),
        filter: None,
        entity_ttl: None,
        history_ttl: None,
        projection: None,
        ephemeral: None,
        pipeline_ttl: None,
        max_keys: None,
        watermark_lateness: None,
        shard_key,
    }
}

fn test_state() -> SharedState {
    make_concurrent_state_full(
        PipelineEngine::new(),
        None,
        std::path::PathBuf::from("/tmp/beava-test-crossshard-warning.snapshot"),
        Arc::new(BackfillTracker::default()),
        true,
        false,
        None,
        false,
        1,
    )
}

fn loopback_request(uri: &str) -> Request<Body> {
    let addr: SocketAddr = "127.0.0.1:54321".parse().unwrap();
    let mut req = Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    req.extensions_mut().insert(ConnectInfo(addr));
    req
}

// ---------------------------------------------------------------------------
// SC-3 primary — validate_shard_keys produces a CrossShardJoinWarning and
// register() succeeds (does NOT return Err). Counter-and-tracing side
// effects are driven through the unit-level validator; the end-to-end
// signal flow is covered in the HTTP test below.
// ---------------------------------------------------------------------------

#[test]
fn register_emits_crossshard_warning_not_error() {
    // Build the three stream defs.
    let l = make_stream_def("L", Some(ShardKeySpec::Single("user_id".into())));
    let r = make_stream_def("R", Some(ShardKeySpec::Single("session_id".into())));
    let join = make_join_def(
        "LR_Join",
        Some(ShardKeySpec::Single("user_id".into())),
        "L",
        "R",
        "user_id",
    );

    // Drive register() via a PipelineEngine. Third register call MUST
    // return Ok (previously returned Err(BeavaError::Protocol(...)) on
    // the shard_key mismatch between peer R [session_id] and join L
    // [user_id]).
    let mut engine = PipelineEngine::new();
    engine.register(l.clone()).expect("L registers");
    engine.register(r.clone()).expect("R registers");
    engine
        .register(join.clone())
        .expect("Join registers (no longer errors after Wave 3 relaxation)");

    // validate_shard_keys returns the structured warning list directly,
    // so we can assert the warning contents without a running server.
    //
    // The join stream is "LR_Join" with shard_key=user_id. Its peers are
    // L [user_id] (match) and R [session_id] (mismatch). Expect exactly
    // one warning naming R.
    let mut streams_map = ahash::AHashMap::new();
    streams_map.insert("L".to_string(), l);
    streams_map.insert("R".to_string(), r);
    let warnings: Vec<CrossShardJoinWarning> = validate_shard_keys(&streams_map, &join);
    assert_eq!(
        warnings.len(),
        1,
        "expected exactly one warning for join peer R; got {:?}",
        warnings
    );
    let w = &warnings[0];
    assert_eq!(w.stream_a, "LR_Join");
    assert_eq!(w.stream_b, "R");
    assert_eq!(w.left_shard_key, "user_id");
    assert_eq!(w.right_shard_key, "session_id");
    assert_eq!(w.on_field, "user_id");
    assert!(
        w.message.contains("CrossShardJoinWarning"),
        "message must contain 'CrossShardJoinWarning': {}",
        w.message
    );
    assert!(
        w.message.contains("user_id") && w.message.contains("session_id"),
        "message must name both shard keys: {}",
        w.message
    );
    assert!(
        w.perf_note.contains("+1 inbox hop"),
        "perf_note must mention '+1 inbox hop': {}",
        w.perf_note
    );
    assert_eq!(
        w.join_id, "LR_Join_x_R_on_user_id",
        "stable synthetic join_id"
    );
}

// ---------------------------------------------------------------------------
// SC-3 co-located case — both sides declare shard_key=user_id. NO warning
// is emitted. This guards against false-positive warnings on perfectly-
// sharded pipelines.
// ---------------------------------------------------------------------------

#[test]
fn register_colocated_join_emits_no_warning() {
    let l = make_stream_def("L", Some(ShardKeySpec::Single("user_id".into())));
    let r = make_stream_def("R", Some(ShardKeySpec::Single("user_id".into())));
    let join = make_join_def(
        "LR_Join",
        Some(ShardKeySpec::Single("user_id".into())),
        "L",
        "R",
        "user_id",
    );

    // Every register call returns Ok.
    let mut engine = PipelineEngine::new();
    engine.register(l.clone()).expect("L registers");
    engine.register(r.clone()).expect("R registers");
    engine.register(join.clone()).expect("Join registers");

    // validate_shard_keys returns empty — co-location (D-B5) is silent.
    let mut streams_map = ahash::AHashMap::new();
    streams_map.insert("L".to_string(), l);
    streams_map.insert("R".to_string(), r);
    let warnings = validate_shard_keys(&streams_map, &join);
    assert!(
        warnings.is_empty(),
        "co-located join MUST NOT emit warnings; got {:?}",
        warnings
    );
}

// ---------------------------------------------------------------------------
// SC-3 HTTP surface — `GET /debug/warnings` includes the new
// top-level `cross_shard_joins: [...]` field populated via
// `emit_cross_shard_join_warning`. Back-compat note: we keep the flat
// `warnings` array shape that Phase 51 tests assert on, and ADD
// `cross_shard_joins` as a sibling field at the response root.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn debug_warnings_endpoint_lists_cross_shard_joins() {
    let state = test_state();

    // Register a mismatched-shard join and wire the resulting warning
    // into the shared SignalRegistry via the same emitter that
    // `register()` uses in production.
    let l = make_stream_def("L", Some(ShardKeySpec::Single("user_id".into())));
    let r = make_stream_def("R", Some(ShardKeySpec::Single("session_id".into())));
    let join = make_join_def(
        "LR_Join",
        Some(ShardKeySpec::Single("user_id".into())),
        "L",
        "R",
        "user_id",
    );
    let mut streams_map = ahash::AHashMap::new();
    streams_map.insert("L".to_string(), l);
    streams_map.insert("R".to_string(), r);
    let warnings = validate_shard_keys(&streams_map, &join);
    assert_eq!(warnings.len(), 1);
    let w = &warnings[0];
    emit_cross_shard_join_warning(&state.signals, w);

    // Hit the endpoint.
    let app = build_router(state);
    let resp = app
        .oneshot(loopback_request("/debug/warnings"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

    // cross_shard_joins is the new structured array; length == 1.
    let arr = body["cross_shard_joins"]
        .as_array()
        .expect("body.cross_shard_joins is an array");
    assert_eq!(
        arr.len(),
        1,
        "expected exactly one cross_shard_joins entry; got {:?}",
        arr
    );
    let entry = &arr[0];
    assert_eq!(entry["join_id"], "LR_Join_x_R_on_user_id");
    assert_eq!(entry["left_shard_key"], "user_id");
    assert_eq!(entry["right_shard_key"], "session_id");
    assert_eq!(entry["on_field"], "user_id");
    assert!(
        entry["perf_note"]
            .as_str()
            .unwrap_or("")
            .contains("+1 inbox hop"),
        "perf_note must contain '+1 inbox hop': {}",
        entry["perf_note"]
    );

    // Back-compat: the unified `warnings` feed MUST still be an array
    // and MUST contain a matching signal entry (category=safety,
    // severity=warning).
    let warnings_arr = body["warnings"]
        .as_array()
        .expect("body.warnings remains a flat array (Phase 51 contract)");
    let matching = warnings_arr
        .iter()
        .find(|w| {
            w["id"].as_str() == Some("crossshard_join.LR_Join_x_R_on_user_id")
        })
        .expect("matching signal id present in unified warnings feed");
    assert_eq!(matching["category"], "safety");
    assert_eq!(matching["severity"], "warning");
}

// ---------------------------------------------------------------------------
// SignalRegistry dedupe test — T-56-03-01 mitigation. Pushing the same
// warning twice MUST produce exactly one entry.
// ---------------------------------------------------------------------------

#[test]
fn signal_registry_dedupes_cross_shard_joins_by_join_id() {
    let registry = SignalRegistry::new_default().into_shared();
    let w = CrossShardJoinWarning::new("A", "B", "ka", "kb", "on_f");
    emit_cross_shard_join_warning(&registry, &w);
    emit_cross_shard_join_warning(&registry, &w);
    let snap = registry.read().cross_shard_joins_snapshot();
    assert_eq!(snap.len(), 1, "dedupe by join_id; got {:?}", snap);
}
