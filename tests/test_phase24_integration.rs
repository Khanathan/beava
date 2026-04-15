//! Phase 24-05 Task 1 — Multi-shape DAG integration tests.
//!
//! These five tests are the Phase 24 integration gate: each builds a small but
//! realistic DAG that exercises storage + watermarks + cascade together, then
//! drives events end-to-end and asserts the observable outputs.
//!
//! DAG (built by `build_full_dag`):
//!
//!   Purchases (Stream, user_id, _event_time)            OP_PUSH
//!       ↓   (aggregation over user_id)
//!   PurchasesAgg (Table, user_id)                       tx_count_1h, tx_sum_1h
//!
//!   UserProfile (Table, user_id)                        OP_PUSH_TABLE / OP_DELETE_TABLE
//!   RiskScore   (Table, user_id)                        OP_PUSH_TABLE / OP_DELETE_TABLE
//!           ↘        ↙
//!    UserRisk = UserProfile.tt_join(RiskScore, inner)   TT-cascade
//!
//! The Purchases → PurchasesAgg branch exercises:
//!   - `_event_time` parsing on every event
//!   - per-stream watermark tracking + late-event drop gate
//!   - event-time bucket routing in the aggregation's RingBuffer
//!   - γ propagation (attach_to_table: Purchases.wm → PurchasesAgg.wm)
//!
//! The UserProfile + RiskScore → UserRisk branch exercises:
//!   - per-Table row storage (plan 01)
//!   - OP_PUSH_TABLE / OP_DELETE_TABLE wiring through the store APIs (plan 02)
//!   - TT-cascade reading real table_rows and writing merged output (plan 03)
//!   - 7d tombstone grace + gc_tombstones
//!
//! Drivers are the in-process `PipelineEngine` + `StateStore` APIs — the same
//! substrate the TCP handlers call into. The watermark gate is applied
//! exactly as `src/server/tcp.rs::handle_sync_command::Command::Push` does.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ahash::AHashMap;
use tally::engine::event_time::WATERMARK_LATENESS;
use tally::engine::pipeline::PipelineEngine;
use tally::engine::register::{
    v0_aggregation_to_stream_def, v0_join_to_stream_def, v0_source_to_stream_def,
    V0RegisterPayload,
};
use tally::state::store::{StateStore, TableRowState, TOMBSTONE_GRACE};
use tally::types::FeatureValue;

fn parse(json: &str) -> V0RegisterPayload {
    V0RegisterPayload::parse(json.as_bytes()).expect("parse register JSON")
}

/// Build the canonical Phase 24 DAG (Purchases stream → PurchasesAgg table;
/// UserProfile + RiskScore → UserRisk via TT-join) and return a fresh
/// engine + store.
fn build_full_dag() -> (PipelineEngine, StateStore) {
    let mut engine = PipelineEngine::new();

    // --- Purchases (stream, keyed user_id; carries _event_time) --------
    let purchases = r#"{
        "name":"Purchases",
        "kind":"stream",
        "key_field":"user_id",
        "fields":{
            "user_id":{"type":"str","optional":false},
            "amount":{"type":"f64","optional":false},
            "_event_time":{"type":"i64","optional":true}
        }
    }"#;
    let purchases_val: serde_json::Value = serde_json::from_str(purchases).unwrap();
    let purchases_def = match parse(purchases) {
        V0RegisterPayload::Source(d) => v0_source_to_stream_def(&d).unwrap(),
        _ => panic!("expected Source"),
    };
    engine.register(purchases_def).unwrap();
    engine.store_raw_register_json("Purchases", purchases_val);

    // --- UserProfile (source Table, keyed user_id) ---------------------
    let up = r#"{
        "name":"UserProfile",
        "kind":"table",
        "mode":"overwrite",
        "key_field":"user_id",
        "fields":{
            "user_id":{"type":"str","optional":false},
            "country":{"type":"str","optional":false},
            "tier":{"type":"str","optional":true}
        }
    }"#;
    let up_val: serde_json::Value = serde_json::from_str(up).unwrap();
    let up_def = match parse(up) {
        V0RegisterPayload::Source(d) => v0_source_to_stream_def(&d).unwrap(),
        _ => panic!(),
    };
    engine.register(up_def).unwrap();
    engine.store_raw_register_json("UserProfile", up_val);

    // --- RiskScore (source Table, keyed user_id) -----------------------
    let rs = r#"{
        "name":"RiskScore",
        "kind":"table",
        "mode":"overwrite",
        "key_field":"user_id",
        "fields":{
            "user_id":{"type":"str","optional":false},
            "score":{"type":"i64","optional":false}
        }
    }"#;
    let rs_val: serde_json::Value = serde_json::from_str(rs).unwrap();
    let rs_def = match parse(rs) {
        V0RegisterPayload::Source(d) => v0_source_to_stream_def(&d).unwrap(),
        _ => panic!(),
    };
    engine.register(rs_def).unwrap();
    engine.store_raw_register_json("RiskScore", rs_val);

    // --- PurchasesAgg = Purchases.group_by(user_id).agg(count, sum) ----
    let agg = r#"{
        "name":"PurchasesAgg",
        "kind":"table",
        "key_field":"user_id",
        "mode":"overwrite",
        "fields":{},
        "aggregation":{
            "source":"Purchases",
            "keys":["user_id"],
            "features":[
                {"name":"tx_count_1h","type":"count","supports_retraction":true,"window":"1h"},
                {"name":"tx_sum_1h","type":"sum","supports_retraction":true,"window":"1h","field":"amount"}
            ]
        },
        "depends_on":["Purchases"]
    }"#;
    let agg_def = match parse(agg) {
        V0RegisterPayload::Aggregation(d) => v0_aggregation_to_stream_def(&d).unwrap(),
        _ => panic!(),
    };
    engine.register(agg_def).unwrap();

    // --- UserRisk = UserProfile.tt_join(RiskScore, inner) --------------
    let ur = r#"{
        "name":"UserRisk",
        "kind":"table",
        "mode":"overwrite",
        "key_field":"user_id",
        "fields":{
            "user_id":{"type":"str","optional":false},
            "country":{"type":"str","optional":false},
            "tier":{"type":"str","optional":true},
            "score":{"type":"i64","optional":false}
        },
        "join":{
            "op":"join",
            "left":"UserProfile",
            "right":"RiskScore",
            "on":["user_id"],
            "type":"inner",
            "shape":"table_table"
        },
        "depends_on":["UserProfile","RiskScore"]
    }"#;
    let ur_val: serde_json::Value = serde_json::from_str(ur).unwrap();
    let ur_desc = match parse(ur) {
        V0RegisterPayload::Join(d) => d,
        _ => panic!(),
    };
    let fl = |name: &str| -> Option<Vec<String>> {
        match name {
            "UserProfile" => Some(vec![
                "user_id".into(),
                "country".into(),
                "tier".into(),
            ]),
            "RiskScore" => Some(vec!["user_id".into(), "score".into()]),
            _ => None,
        }
    };
    let ur_def = v0_join_to_stream_def(&ur_desc, Some(&fl)).unwrap();
    engine.register(ur_def).unwrap();
    engine.store_raw_register_json("UserRisk", ur_val);

    (engine, StateStore::new())
}

/// Push a Purchases event through the full wm-gate + cascade. Mirrors the
/// TCP handler's sync push flow.
fn push_purchase(
    engine: &PipelineEngine,
    store: &StateStore,
    user_id: &str,
    amount: f64,
    event_time: SystemTime,
) {
    let et_ms = event_time
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let wm_opt = engine.watermarks.watermark("Purchases");
    if let Some(wm) = wm_opt {
        if event_time < wm {
            engine.late_drops.increment("Purchases");
            return;
        }
    }
    engine.watermarks.observe("Purchases", event_time);

    let event = serde_json::json!({
        "user_id": user_id,
        "amount": amount,
        "_event_time": et_ms,
    });
    engine
        .push_with_cascade("Purchases", &event, store, event_time)
        .expect("push_with_cascade");
}

fn upsert_profile(
    engine: &PipelineEngine,
    store: &StateStore,
    user_id: &str,
    country: &str,
    tier: Option<&str>,
    now: SystemTime,
) {
    let mut fields = AHashMap::new();
    fields.insert(
        "country".into(),
        FeatureValue::String(country.into()),
    );
    if let Some(t) = tier {
        fields.insert("tier".into(), FeatureValue::String(t.into()));
    }
    store.upsert_table_row(user_id, "UserProfile", fields, now);
    engine
        .cascade_tt_after_upsert("UserProfile", user_id, store, now)
        .expect("cascade upsert UserProfile");
}

fn upsert_risk(
    engine: &PipelineEngine,
    store: &StateStore,
    user_id: &str,
    score: i64,
    now: SystemTime,
) {
    let mut fields = AHashMap::new();
    fields.insert("score".into(), FeatureValue::Int(score));
    store.upsert_table_row(user_id, "RiskScore", fields, now);
    engine
        .cascade_tt_after_upsert("RiskScore", user_id, store, now)
        .expect("cascade upsert RiskScore");
}

fn tombstone_profile(
    engine: &PipelineEngine,
    store: &StateStore,
    user_id: &str,
    now: SystemTime,
) {
    store.tombstone_table_row(user_id, "UserProfile", now);
    engine
        .cascade_tt_after_delete("UserProfile", user_id, store, now)
        .expect("cascade delete UserProfile");
}

fn epoch_plus_secs(s: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(s)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// (1) Happy path: in-order events; both Table sources present.
///
/// Asserts:
///   - PurchasesAgg picks up count=3, sum=60 (via merged GET).
///   - UserRisk row is Live with country/tier/score merged.
///   - Purchases watermark = max(event_time) − 5s.
///   - No late drops.
#[test]
fn integ_full_dag_in_order_happy_path() {
    let (engine, store) = build_full_dag();

    let t0 = epoch_plus_secs(1_700_000_000);
    upsert_profile(&engine, &store, "u1", "US", Some("gold"), t0);
    upsert_risk(&engine, &store, "u1", 42, t0);

    // UserRisk must be Live now — both TT-join inputs are Live.
    let ur = store
        .get_table_row("u1", "UserRisk")
        .expect("UserRisk row");
    assert!(
        matches!(ur.state, TableRowState::Live),
        "UserRisk should be Live after both TT inputs upserted, got {:?}",
        ur.state
    );
    assert_eq!(
        ur.fields.get("country"),
        Some(&FeatureValue::String("US".into()))
    );
    assert_eq!(
        ur.fields.get("tier"),
        Some(&FeatureValue::String("gold".into()))
    );
    assert_eq!(ur.fields.get("score"), Some(&FeatureValue::Int(42)));

    // Three in-order purchase events.
    for (i, amt) in [(0u64, 10.0_f64), (1, 20.0), (2, 30.0)].iter() {
        push_purchase(
            &engine,
            &store,
            "u1",
            *amt,
            t0 + Duration::from_secs(100 + *i),
        );
    }

    // PurchasesAgg is an aggregation table — its values surface through the
    // merged feature view (stream-operator state), not as a TableRow.
    let merged = store.collect_merged_features("u1", t0 + Duration::from_secs(200));
    assert_eq!(
        merged.get("tx_count_1h"),
        Some(&FeatureValue::Int(3)),
        "PurchasesAgg.tx_count_1h after 3 pushes"
    );
    assert_eq!(
        merged.get("tx_sum_1h"),
        Some(&FeatureValue::Float(60.0)),
        "PurchasesAgg.tx_sum_1h after 3 pushes"
    );

    // Watermark = max(event_time) − 5s. max = t0+102.
    let wm = engine.watermarks.watermark("Purchases").unwrap();
    assert_eq!(wm, t0 + Duration::from_secs(102) - WATERMARK_LATENESS);

    // No late drops.
    assert_eq!(engine.late_drops.get("Purchases"), 0);

    // γ propagation: PurchasesAgg.wm attached from Purchases.wm.
    let agg_wm = engine.watermarks.watermark("PurchasesAgg");
    assert_eq!(
        agg_wm, Some(wm),
        "γ: aggregation Table inherits source Stream watermark"
    );
}

/// (2) Out-of-order within 5s: event lands in correct bucket; counter unaffected.
///
/// Pushes three events at t+100 (wm becomes 95), then a straggler at t+97
/// (>= wm=95 → accepted). The RingBuffer's event-time routing must place it
/// in its historical bucket so it counts toward the 1h window aggregate.
#[test]
fn integ_out_of_order_within_5s_lands_in_bucket() {
    let (engine, store) = build_full_dag();

    let t0 = epoch_plus_secs(1_700_000_000);
    upsert_profile(&engine, &store, "u1", "UK", None, t0);
    upsert_risk(&engine, &store, "u1", 7, t0);

    for amt in [10.0, 20.0, 30.0] {
        push_purchase(
            &engine,
            &store,
            "u1",
            amt,
            t0 + Duration::from_secs(100),
        );
    }

    // Straggler at t+97 — wm = 100 − 5 = 95, so 97 ≥ 95 → accepted.
    push_purchase(
        &engine,
        &store,
        "u1",
        40.0,
        t0 + Duration::from_secs(97),
    );

    assert_eq!(
        engine.late_drops.get("Purchases"),
        0,
        "within-5s straggler must NOT be late-dropped"
    );

    let merged = store.collect_merged_features("u1", t0 + Duration::from_secs(200));
    assert_eq!(
        merged.get("tx_count_1h"),
        Some(&FeatureValue::Int(4)),
        "all 4 events (3 in-order + 1 straggler) counted"
    );
    assert_eq!(
        merged.get("tx_sum_1h"),
        Some(&FeatureValue::Float(100.0)),
        "sum includes the straggler's amount"
    );

    // observed_max stays at t+100.
    assert_eq!(
        engine.watermarks.observed_max("Purchases"),
        Some(t0 + Duration::from_secs(100))
    );
}

/// (3) Late event past 5s: dropped, downstream unaffected, counter increments.
#[test]
fn integ_late_event_past_5s_dropped_and_downstream_unaffected() {
    let (engine, store) = build_full_dag();

    let t0 = epoch_plus_secs(1_700_000_000);
    upsert_profile(&engine, &store, "u1", "DE", None, t0);
    upsert_risk(&engine, &store, "u1", 11, t0);

    // First event at t+100 → wm=95.
    push_purchase(&engine, &store, "u1", 10.0, t0 + Duration::from_secs(100));

    // Late event at t+94 (< wm=95) → dropped.
    push_purchase(&engine, &store, "u1", 999.0, t0 + Duration::from_secs(94));

    assert_eq!(
        engine.late_drops.get("Purchases"),
        1,
        "one late-drop expected"
    );

    // Downstream PurchasesAgg must reflect only the first event.
    let merged = store.collect_merged_features("u1", t0 + Duration::from_secs(200));
    assert_eq!(merged.get("tx_count_1h"), Some(&FeatureValue::Int(1)));
    assert_eq!(
        merged.get("tx_sum_1h"),
        Some(&FeatureValue::Float(10.0)),
        "dropped event's amount must not affect the sum"
    );

    // UserRisk still Live — the late drop didn't corrupt TT state.
    let ur = store.get_table_row("u1", "UserRisk").unwrap();
    assert!(matches!(ur.state, TableRowState::Live));
}

/// (4) Table tombstone cascades through TT-join. Tombstone UserProfile →
///     UserRisk retracts; merged GET view excludes both UserProfile.* and
///     UserRisk.* fields.
#[test]
fn integ_table_tombstone_cascades_through_aggregation_and_tt_join() {
    let (engine, store) = build_full_dag();

    let t0 = epoch_plus_secs(1_700_000_000);
    upsert_profile(&engine, &store, "u1", "FR", Some("silver"), t0);
    upsert_risk(&engine, &store, "u1", 50, t0);

    push_purchase(&engine, &store, "u1", 50.0, t0 + Duration::from_secs(100));
    push_purchase(&engine, &store, "u1", 25.0, t0 + Duration::from_secs(101));

    // Pre-tombstone: UserRisk Live with both sides present.
    let ur = store
        .get_table_row("u1", "UserRisk")
        .expect("UserRisk live");
    assert!(matches!(ur.state, TableRowState::Live));
    assert_eq!(
        ur.fields.get("country"),
        Some(&FeatureValue::String("FR".into()))
    );
    assert_eq!(ur.fields.get("score"), Some(&FeatureValue::Int(50)));

    // Delete UserProfile → UserRisk must Tombstone.
    let t_delete = t0 + Duration::from_secs(200);
    tombstone_profile(&engine, &store, "u1", t_delete);

    let ur_after = store
        .get_table_row("u1", "UserRisk")
        .expect("UserRisk row retained during grace");
    assert!(
        matches!(ur_after.state, TableRowState::Tombstoned { .. }),
        "UserRisk must be Tombstoned after UserProfile delete; got {:?}",
        ur_after.state
    );

    // Merged GET view: UserRisk.* + UserProfile.* must NOT appear.
    let merged = store.collect_merged_features("u1", t0 + Duration::from_secs(300));
    assert!(
        !merged.contains_key("UserRisk.country"),
        "tombstoned UserRisk.country must not appear in merged view"
    );
    assert!(
        !merged.contains_key("UserRisk.score"),
        "tombstoned UserRisk.score must not appear in merged view"
    );
    assert!(
        !merged.contains_key("UserProfile.country"),
        "tombstoned UserProfile.country must not appear in merged view"
    );

    // RiskScore is still Live → its fields still surface.
    assert_eq!(
        merged.get("RiskScore.score"),
        Some(&FeatureValue::Int(50)),
        "RiskScore is still Live"
    );
    // Stream-operator features survive unaffected.
    assert_eq!(merged.get("tx_count_1h"), Some(&FeatureValue::Int(2)));
}

/// (5) GC after 7d grace: tombstoned rows removed; live rows untouched.
#[test]
fn integ_gc_tombstones_after_7d_grace_removes_rows() {
    let (engine, store) = build_full_dag();

    let t0 = epoch_plus_secs(1_700_000_000);
    upsert_profile(&engine, &store, "u_tomb", "JP", None, t0);
    upsert_risk(&engine, &store, "u_tomb", 90, t0);
    upsert_profile(&engine, &store, "u_live", "CA", None, t0);
    upsert_risk(&engine, &store, "u_live", 10, t0);

    push_purchase(
        &engine, &store, "u_tomb", 10.0, t0 + Duration::from_secs(100),
    );
    push_purchase(
        &engine, &store, "u_live", 20.0, t0 + Duration::from_secs(101),
    );

    // Tombstone u_tomb's UserProfile → UserRisk tombstones via cascade.
    let t_delete = t0 + Duration::from_secs(200);
    tombstone_profile(&engine, &store, "u_tomb", t_delete);

    let pre = store
        .get_table_row("u_tomb", "UserProfile")
        .expect("retained during grace");
    assert!(matches!(pre.state, TableRowState::Tombstoned { .. }));
    let pre_live = store.get_table_row("u_live", "UserProfile").unwrap();
    assert!(matches!(pre_live.state, TableRowState::Live));

    // Advance past grace.
    let gc_now = t_delete + TOMBSTONE_GRACE + Duration::from_secs(1);
    let removed = store.gc_tombstones(gc_now);
    assert!(
        removed >= 2,
        "at least u_tomb/UserProfile + u_tomb/UserRisk must be GC'd; removed={}",
        removed
    );

    // Tombstoned rows past grace → gone.
    assert!(
        store.get_table_row("u_tomb", "UserProfile").is_none(),
        "tombstoned UserProfile past grace must be GC'd"
    );
    assert!(
        store.get_table_row("u_tomb", "UserRisk").is_none(),
        "cascade-tombstoned UserRisk past grace must be GC'd"
    );

    // Live rows untouched.
    let still_live = store
        .get_table_row("u_live", "UserProfile")
        .expect("live row retained");
    assert!(matches!(still_live.state, TableRowState::Live));
    let still_live_risk = store
        .get_table_row("u_live", "RiskScore")
        .expect("live RiskScore retained");
    assert!(matches!(still_live_risk.state, TableRowState::Live));
    let still_live_ur = store
        .get_table_row("u_live", "UserRisk")
        .expect("live UserRisk retained");
    assert!(matches!(still_live_ur.state, TableRowState::Live));

    // u_tomb's RiskScore was never tombstoned — it should still be Live.
    let tomb_risk = store
        .get_table_row("u_tomb", "RiskScore")
        .expect("u_tomb RiskScore never tombstoned → still Live");
    assert!(matches!(tomb_risk.state, TableRowState::Live));
}
