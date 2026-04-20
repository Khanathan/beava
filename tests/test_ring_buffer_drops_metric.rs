// OBS-01/02: beava_ring_buffer_drops_total{stream, operator_kind, reason}
// counter with bounded labels and mutual-exclusivity invariant.
//
// D-05: counter name = beava_ring_buffer_drops_total; reason ∈ {too_old,
//       too_new, pre_epoch} hard enum.
// D-06: cache Counter handle at operator registration; .inc() only on hot path.
// D-07: counter values exposed via engine.ring_buffer_drops.snapshot().
// D-08: at most one of beava_late_events_dropped_total or
//       beava_ring_buffer_drops_total fires per event.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use beava::engine::event_time::{DropReason, WATERMARK_LATENESS};
use beava::engine::pipeline::PipelineEngine;
use beava::engine::register::{v0_aggregation_to_stream_def, V0RegisterPayload};
use beava::state::store::StateStore;

fn parse(json: &str) -> V0RegisterPayload {
    V0RegisterPayload::parse(json.as_bytes()).expect("parse register JSON")
}

/// Build a single stream ("Sales") with two ring-buffer operators (count + sum)
/// and return a fresh engine + store. Window is narrow (10 s / 1 s bucket) so
/// we can drive TooOld drops with small time deltas.
fn build_sales_engine() -> (PipelineEngine, StateStore) {
    let mut engine = PipelineEngine::new();

    // Sales source stream — keyed on user_id.
    let sales_src = r#"{
        "name":"Sales",
        "kind":"stream",
        "key_field":"user_id",
        "fields":{
            "user_id":{"type":"str","optional":false},
            "amount":{"type":"f64","optional":false},
            "_event_time":{"type":"i64","optional":true}
        }
    }"#;
    let sales_src_val: serde_json::Value = serde_json::from_str(sales_src).unwrap();
    let sales_def = match parse(sales_src) {
        V0RegisterPayload::Source(d) => {
            beava::engine::register::v0_source_to_stream_def(&d).expect("sales source def")
        }
        _ => panic!("expected Source"),
    };
    engine.register(sales_def).unwrap();
    engine.store_raw_register_json("Sales", sales_src_val);

    // SalesAgg aggregation table — count + sum over 10 s window / 1 s bucket.
    let agg = r#"{
        "name":"SalesAgg",
        "kind":"table",
        "key_field":"user_id",
        "mode":"overwrite",
        "fields":{},
        "aggregation":{
            "source":"Sales",
            "keys":["user_id"],
            "features":[
                {"name":"cnt_10s","type":"count","supports_retraction":true,"window":"10s"},
                {"name":"sum_10s","type":"sum","supports_retraction":true,"window":"10s","field":"amount"}
            ]
        },
        "depends_on":["Sales"]
    }"#;
    let agg_def = match parse(agg) {
        V0RegisterPayload::Aggregation(d) => {
            v0_aggregation_to_stream_def(&d).expect("aggregation def")
        }
        _ => panic!("expected Aggregation"),
    };
    engine.register(agg_def).unwrap();

    (engine, StateStore::new())
}

fn epoch_plus_secs(s: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(s)
}

/// Push a Sales event, applying the watermark gate the same way tcp.rs does.
/// Returns true if the event was accepted (not late-dropped at the gate).
fn push_sales(
    engine: &PipelineEngine,
    store: &StateStore,
    user_id: &str,
    amount: f64,
    event_time: SystemTime,
) -> bool {
    // Phase 54-04 Pass B: legacy push/cascade helper deleted. Body stubbed
    // pending Pass C on_shard rewrite.
    unimplemented!("54-04 Pass B: legacy helper deleted; rewrite via on_shard path in Pass C")
}

// ---------------------------------------------------------------------------
// OBS-01 / D-05 / D-06: bounded label cardinality
// ---------------------------------------------------------------------------

/// Verify that:
/// 1. Ring-buffer drop counters increment for TooOld events.
/// 2. Label cardinality never exceeds (streams × operator_kinds × 3 reasons).
///    i.e. pushing N events does NOT create N label combinations.
/// 3. The snapshot contains only the three pre-registered (stream, kind, reason)
///    label combinations — not an unbounded set.
#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn bounded_labels() {
    // Phase 54-04 Pass B: legacy push/cascade helper deleted. Body stubbed
    // pending Pass C on_shard rewrite.
    unimplemented!("54-04 Pass B: legacy helper deleted; rewrite via on_shard path in Pass C")
}

// ---------------------------------------------------------------------------
// OBS-02 / D-08: mutual exclusivity
// ---------------------------------------------------------------------------

/// For each dropped event, exactly ONE of the two counters increments:
/// - `beava_late_events_dropped_total` (gate in tcp.rs, pre-ring-buffer)
/// - `beava_ring_buffer_drops_total`   (ring buffer bucket router)
///
/// They cannot both fire for the same event because the tcp.rs gate fires
/// `continue` (or `return`) before the event reaches push_with_cascade.
#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn counters_mutually_exclusive() {
    // Phase 54-04 Pass B: legacy push/cascade helper deleted. Body stubbed
    // pending Pass C on_shard rewrite.
    unimplemented!("54-04 Pass B: legacy helper deleted; rewrite via on_shard path in Pass C")
}
