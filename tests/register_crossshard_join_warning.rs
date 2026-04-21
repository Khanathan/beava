//! Phase 56 SC-3 — `register()` accepts mismatched shard_key joins with a
//! logged `CrossShardJoinWarning` (TPC-CORR-04 relaxation).
//!
//! Contract (D-B4 / D-C1 / D-C2 / D-C3):
//! - Pre-Phase-56: `register()` returned
//!   `Err(BeavaError::Protocol("join operator between L and R: shard_key mismatch..."))`.
//! - Phase 56: `register()` returns `Ok(_)`. A `tracing::warn!` line with
//!   content "CrossShardJoinWarning" + the join field + both shard keys is
//!   emitted. The warning is surfaced via `GET /debug/warnings` under
//!   `cross_shard_joins: [{join_id, left_shard_key, right_shard_key,
//!   on_field, perf_note}]`. Counter `beava_crossshard_joins_registered_total{join_id}`
//!   increments on each relaxation event.
//!
//! Co-located case (both sides `shard_key=join.on`) does NOT emit the warning.
//!
//! RED until Wave 3 (plan 56-03) relaxes `validate_shard_keys` in
//! `src/engine/register.rs` + `src/engine/join_validator.rs` and extends
//! `/debug/warnings` with the `cross_shard_joins` field. Passes at Wave 3.
//!
//! Run:
//!   cargo test --release --test register_crossshard_join_warning -- --ignored --test-threads=1

#![cfg(not(feature = "state-inmem"))]

/// SC-3 primary — `register()` no longer rejects mismatched shard_keys; it
/// emits a `CrossShardJoinWarning` via `tracing::warn!` that names both
/// streams, both shard_keys, and the join field.
///
/// Wave 3 acceptance:
///   - Install a `tracing::subscriber::fmt` layer that captures emitted
///     lines into a `Vec<String>`.
///   - Register L(shard_key=user_id), R(shard_key=session_id) joined on
///     `user_id`.
///   - `register()` returns `Ok(_)`.
///   - Captured lines contain substring "CrossShardJoinWarning".
///   - Captured lines contain both "user_id" and "session_id".
///   - The join field `"user_id"` appears at least once.
///   - Counter `beava_crossshard_joins_registered_total{join_id=...}` ≥ 1.
#[test]
#[ignore = "56-W3"]
fn register_emits_crossshard_warning_not_error() {
    // Wave 3 wiring:
    //   1. Initialize a tracing subscriber with a Vec<String>-collecting
    //      custom Layer. The existing tests/test_warnings_feed.rs has the
    //      canonical pattern (Writer that pushes each line into
    //      Arc<Mutex<Vec<String>>>).
    //   2. Build PipelineEngine.
    //   3. engine.register(stream_def_L with shard_key=user_id).
    //   4. engine.register(stream_def_R with shard_key=session_id).
    //   5. engine.register(join_def with on=user_id).
    //   6. Assert the third register call returns Ok, not Err.
    //   7. Assert the captured logs contain:
    //      - "CrossShardJoinWarning" substring
    //      - "user_id" (join field + left shard_key)
    //      - "session_id" (right shard_key)
    //   8. Assert `beava_crossshard_joins_registered_total` counter ≥ 1.
    todo!(
        "56-W3: wire register() relaxation test. Today register() returns \
         Err(BeavaError::Protocol) on mismatched shard_keys; post-Wave-3 it \
         returns Ok with a logged CrossShardJoinWarning."
    );
}

/// SC-3 co-located case — when both L and R declare `shard_key=user_id`
/// (matching the join field), NO warning is emitted. This guards against
/// false-positive warnings on perfectly-sharded pipelines.
///
/// Wave 3 assertion hooks:
///   - Captured logs do NOT contain "CrossShardJoinWarning".
///   - `beava_crossshard_joins_registered_total` is unchanged.
#[test]
#[ignore = "56-W3"]
fn register_colocated_join_emits_no_warning() {
    // Wave 3: same subscriber setup, but register both sides with
    // shard_key=user_id. Assert zero warnings for co-located joins.
    todo!("56-W3: co-located join quiet-path check.");
}

/// SC-3 HTTP surface — `GET /debug/warnings` JSON response includes
/// `warnings.cross_shard_joins: [...]` with one entry per registered
/// mismatched join.
///
/// Wave 3 assertion hooks:
///   - HTTP GET /debug/warnings returns 200.
///   - `body.warnings.cross_shard_joins` is an array of length 1.
///   - The single entry has fields: `join_id`, `left_shard_key="user_id"`,
///     `right_shard_key="session_id"`, `on_field="user_id"`,
///     `perf_note` containing "+1 inbox hop".
#[tokio::test]
#[ignore = "56-W3"]
async fn debug_warnings_endpoint_lists_cross_shard_joins() {
    // Wave 3 wiring:
    //   1. Spawn full HTTP server via beava::server::http::build_router
    //      (pattern in tests/test_debug_warnings_endpoint.rs).
    //   2. Register cross-shard join as in the test above.
    //   3. GET /debug/warnings via tower::ServiceExt::oneshot.
    //   4. Parse JSON body.
    //   5. Assert warnings.cross_shard_joins is an array with one entry.
    //   6. Assert entry.left_shard_key == "user_id".
    //   7. Assert entry.right_shard_key == "session_id".
    //   8. Assert entry.on_field == "user_id".
    //   9. Assert entry.perf_note contains "+1 inbox hop".
    todo!(
        "56-W3: /debug/warnings extended with cross_shard_joins field. \
         Verifies D-C1 + D-C2 wire surface."
    );
}
