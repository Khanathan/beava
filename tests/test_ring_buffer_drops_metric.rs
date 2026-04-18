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
    let et_ms = event_time.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;

    // Watermark gate — mirrors tcp.rs late-drop logic (OBS-02).
    let wm_opt = engine.wm_watermark("Sales");
    if let Some(wm) = wm_opt {
        if event_time < wm {
            engine.late_drops.increment("Sales");
            return false;
        }
    }
    engine.wm_observe("Sales", event_time);

    let event = serde_json::json!({
        "user_id": user_id,
        "amount": amount,
        "_event_time": et_ms,
    });
    engine
        .push_with_cascade("Sales", &event, store, event_time)
        .expect("push_with_cascade");
    true
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
#[test]
fn bounded_labels() {
    let (engine, store) = build_sales_engine();

    let t0 = epoch_plus_secs(1_700_000_000);

    // 1. Push one in-order event to establish a watermark.
    //    After this: wm = t0 + 100 − WATERMARK_LATENESS = t0 + 95.
    push_sales(&engine, &store, "u1", 10.0, t0 + Duration::from_secs(100));
    assert_eq!(engine.late_drops.get("Sales"), 0);

    // 2. Push an event 200 s in the future to advance the watermark far.
    //    wm = t0 + 300 − WATERMARK_LATENESS = t0 + 295.
    push_sales(&engine, &store, "u1", 5.0, t0 + Duration::from_secs(300));

    // 3. Push an event at t0 + 200 (>= wm 295? No — 200 < 295, so this is late
    //    at the watermark gate, not a ring-buffer drop).
    //    Instead we need an event that PASSES the watermark gate but lands
    //    outside the ring-buffer window. The ring-buffer window is 10 s.
    //    Current wm = 295. Push at t0 + 296 (passes gate). The ring buffer
    //    for user "u2" has never seen an event — its internal epoch is the
    //    push time, so this event should land fine.
    //
    //    To trigger TooOld: establish a ring-buffer epoch for u3 with an event
    //    at t0+300, then push an event at t0+296 for u3 (passes gate since
    //    296 >= 295). The ring buffer's window is 10 s wide; the epoch of the
    //    u3 ring buffer is t0+300. t0+296 is 4 s before t0+300, but 10 s
    //    window means buckets span [t0+290, t0+300]. 296 is inside → not TooOld.
    //
    //    To reliably get TooOld: push for u4 at t0+300 (epoch = t0+300),
    //    then advance watermark to t0+311 via u4 at t0+311 (wm = t0+306),
    //    then push u4 at t0+296 — passes gate (296 >= 306? No: 296 < 306 → late).
    //
    //    Watermark gate and ring-buffer TooOld are structurally disjoint:
    //    an event passes the gate (et >= wm) but can still be TooOld in the
    //    ring buffer when wm has advanced less than the ring-buffer window.
    //    Use a different user key so the ring buffer has its own epoch.
    //
    //    Set up: u5 gets an event at t0+300 (ring-buffer epoch ≈ t0+300).
    //    Advance wm with u5 at t0+309 → wm = t0+304.
    //    Push u5 at t0+298 (passes gate: 298 >= 304? No → late-dropped).
    //
    //    The issue is the watermark applies to the whole stream, not per-key.
    //    We need the event to pass the gate AND be too old for the ring buffer.
    //
    //    Strategy: Use a stream with a large lateness (WATERMARK_LATENESS = 5s)
    //    and a very small ring-buffer window. Our window is 10 s. If the ring
    //    buffer epoch for a key is at t+1000, and we push t+985 — that's 15 s
    //    before the epoch, which is outside the 10 s window → TooOld.
    //    The gate wm = max_observed − 5s. If max_observed = t+1000, wm = t+995.
    //    t+985 < t+995 → late-dropped at the gate, not ring-buffer TooOld.
    //
    //    Correct strategy: The key is PER-KEY ring buffers but a GLOBAL watermark.
    //    For a key that has NEVER been pushed to: its ring buffer has no epoch
    //    and will accept ANY event. For a key that has been pushed far in the
    //    future, its ring buffer epoch is advanced.
    //
    //    Use two keys:
    //      key "anchor" always gets current-time events to keep the watermark low.
    //      key "target"  gets an early event to set its ring-buffer epoch at T0,
    //                    then a later "anchor" event advances wm to T0+5 (<=gate),
    //                    then push "target" at T0 + window + 1 (future event),
    //                    then push "target" at T0 - 1  (too old for ring buffer,
    //                    but >= wm if wm <= T0-1).
    //
    //    Simplest approach: push "target" at T0 to set epoch. Then push
    //    "target" at T0 + 500 (far future) to advance its ring buffer epoch
    //    to T0+500. Now the ring buffer for "target" has window [T0+490, T0+500].
    //    The watermark at this point is T0+500 − 5s = T0+495. Push "target"
    //    at T0+489 → passes gate (489 >= 495? No: 489 < 495 → late-drop).
    //
    //    The fundamental challenge: the watermark gate and ring-buffer TooOld
    //    cover adjacent regions. Let's look at what WATERMARK_LATENESS equals.

    let _wm_lateness = WATERMARK_LATENESS; // 5 seconds

    // With a 10 s ring-buffer window and 5 s watermark lateness:
    // - Gate drops events older than (max_observed − 5s).
    // - Ring buffer TooOld drops events older than (max_event_time_seen_in_buffer − window).
    // - If max_observed == max_ring_event, then gate_threshold = max − 5s,
    //   ring_threshold = max − 10s.
    // - Events in [max−10s, max−5s) pass the gate but hit ring-buffer TooOld.
    // This is the correct wedge.

    let (engine2, store2) = build_sales_engine();
    let base = epoch_plus_secs(1_700_100_000);

    // Step 1: push event at base+100s to set ring-buffer epoch for "target".
    push_sales(
        &engine2,
        &store2,
        "target",
        1.0,
        base + Duration::from_secs(100),
    );
    // wm = base+100 − 5s = base+95.

    // Step 2: push event at base+110s to advance wm (base+110 − 5s = base+105).
    push_sales(
        &engine2,
        &store2,
        "target",
        1.0,
        base + Duration::from_secs(110),
    );
    // wm = base+105. Ring buffer for "target" now has epoch around base+110.

    // Step 3: push at base+102. Gate check: 102 >= 105? No → late-drop at gate.
    // Still not in the wedge. We need ring_window > lateness.
    //
    // With window=10s and lateness=5s, the wedge is 5s wide (max−10 to max−5).
    // Push at base+100 (ring buffer epoch ≈ base+110, window=10s, so ring
    // accepts [base+100, base+110]). Gate wm = base+105. base+100 >= base+105? No.
    //
    // Ring buffer epoch moves with each push: after pushing at base+110, the
    // ring buffer's "most_recent" bucket anchor is at base+110. Window=[base+100, base+110].
    // An event at base+99 is TooOld for the ring buffer AND < wm=base+105 → gate drops it.
    //
    // The wedge only exists when an event passes the gate but is old relative to
    // the ring buffer. This requires the ring buffer to be advanced BEYOND the
    // watermark-guarded maximum. That can happen for a new key: if key "new_key"
    // pushes at base+200 as its FIRST event, its ring buffer epoch starts fresh
    // at base+200. The watermark (set by earlier events) is at base+105. An event
    // for "new_key" at base+192 passes the gate (192 >= 105) and is within the
    // ring buffer window [base+190, base+200] → not TooOld.
    //
    // An event for "new_key" at base+188 passes the gate (188 >= 105) but
    // is outside [base+190, base+200] (8s before epoch, window=10s → 190 is
    // the oldest bucket start). Wait: delta = 200−188 = 12s, window=10s → TooOld!
    //
    // Let's verify: base+200 = epoch for "new_key". Window=10s. Event at base+188:
    // delta = 200−188 = 12 buckets (assuming 1s buckets). 12 >= 10 → TooOld.
    // Gate: wm = base+105. 188 >= 105 → PASSES. Ring buffer → TooOld.

    let (engine3, store3) = build_sales_engine();
    let t = epoch_plus_secs(1_700_200_000);

    // Establish watermark with an early event so wm stays low.
    push_sales(
        &engine3,
        &store3,
        "anchor",
        1.0,
        t + Duration::from_secs(10),
    );
    // wm = t+10 − 5s = t+5.

    // Push "new_key" at t+200 to set its ring-buffer epoch far in the future.
    push_sales(
        &engine3,
        &store3,
        "new_key",
        1.0,
        t + Duration::from_secs(200),
    );
    // wm = t+200 − 5s = t+195. Ring buffer for "new_key" epoch ≈ t+200.

    // Push "new_key" at t+187: delta=200-187=13s, window=10s → TooOld.
    // Gate check: 187 >= 195? No → still late-dropped.
    // We need wm <= 187. That means max_observed − 5 <= 187, i.e. max_observed <= 192.
    // But we just pushed at t+200, so max_observed = t+200.

    // The only way to get a ring-buffer TooOld without a late-drop is if the
    // ring buffer epoch for a key is ahead of the global watermark. This happens
    // naturally with per-key ring buffers when one key gets a future-timestamped
    // event and no subsequent events advance the watermark past the TooOld range.

    // Reset: use engine4. Push "anchor" and "target" interleaved so the
    // ring buffer epoch for target is at t+20 but wm is at t+5 (from anchor).
    let (engine4, store4) = build_sales_engine();
    let t4 = epoch_plus_secs(1_700_300_000);

    // Anchor advances wm to t4+5.
    push_sales(
        &engine4,
        &store4,
        "anchor",
        1.0,
        t4 + Duration::from_secs(10),
    );
    // wm = t4+5.

    // Target: push at t4+20. Target ring-buffer epoch = t4+20. wm = t4+15.
    push_sales(
        &engine4,
        &store4,
        "target",
        1.0,
        t4 + Duration::from_secs(20),
    );
    // wm = t4+15.

    // Target: push at t4+7. Gate: 7 >= 15? No → late-drop. Still not right.
    // wm=15 covers too much. We need wm < t4+10 (= t4+20 − window).

    // KEY INSIGHT: WATERMARK_LATENESS = 5s, ring-buffer window = 10s.
    // For TooOld to occur without a gate drop, we need:
    //   event_time >= wm  AND  event_time < ring_epoch − window
    //   i.e. max_observed − 5 <= event_time < ring_epoch − 10
    //
    // For ring_epoch > max_observed + 5:
    //   This happens when the ring buffer's most-recent event is > 5s past wm.
    //   Since ring_epoch ≈ most_recent_push_for_that_key and wm = max_over_all_keys − 5,
    //   ring_epoch > max_observed + 5 requires that key's most-recent event >
    //   max_over_all_keys + 10, which is impossible (it IS an event, so it must
    //   equal max_over_all_keys).
    //
    // CONCLUSION: With WATERMARK_LATENESS (5s) < ring_buffer_window (10s), the
    // TooOld region [epoch−window, epoch−lateness) is fully within the late-drop
    // zone. In this configuration, TooOld drops are always preceded by a late-drop.
    //
    // HOWEVER: the ring buffer's epoch per key is set by that key's own events,
    // not the global max. If key "target" has its own epoch at t+200, and the
    // global wm is only t+5 (because we only pushed one anchor event at t+10),
    // then an event at t+188 for "target" passes the gate (188 >= 5) but is
    // 12s before target's ring epoch (200−188=12 > 10=window) → TooOld!

    let (engine5, store5) = build_sales_engine();
    let t5 = epoch_plus_secs(1_700_400_000);

    // Establish anchor at t5+10 → wm = t5+5.
    push_sales(
        &engine5,
        &store5,
        "anchor",
        1.0,
        t5 + Duration::from_secs(10),
    );
    assert_eq!(engine5.late_drops.get("Sales"), 0);

    // Push "target" at t5+200 → target ring-buffer epoch = t5+200. wm = t5+195.
    // This also advances wm to t5+195.
    push_sales(
        &engine5,
        &store5,
        "target",
        1.0,
        t5 + Duration::from_secs(200),
    );

    // Now we have: wm = t5+195, target ring-buffer epoch ≈ t5+200.
    // Event at t5+188: gate check 188 >= 195? No → late-drop.
    // Still blocked. The single push at t5+200 sets wm to t5+195.

    // TWO-STEP approach: keep wm low, then push target far into future.
    // After wm is low, push target at t_far, wm jumps to t_far−5.
    // So wm = t_far − 5. TooOld region is [t_far−window, t_far−5).
    // window=10, lateness=5 → region is [t_far−10, t_far−5), width 5s.
    // Events at t_far−7 pass gate (>= t_far−5? No: t_far−7 < t_far−5).
    // Hmm still blocked.

    // The issue: every push to "target" at t_far ALSO advances the global wm
    // to t_far − 5. So the TooOld wedge [t_far−10, t_far−5) is entirely within
    // the late-drop zone [0, t_far−5).

    // TRUE ring-buffer TooOld (distinct from late-drop) only fires when
    // window > lateness AND events for a key have been accumulating at rates
    // that push the ring epoch ahead of (wm + window − lateness).
    // OR: for the PreEpoch reason (event_time < UNIX_EPOCH) which is always
    // distinct from the late-drop gate (which only triggers when event_time < wm,
    // and wm is always >= UNIX_EPOCH for any observed event).

    // -----------------------------------------------------------------------
    // ADJUSTED STRATEGY: test TooOld directly via the engine's push_internal,
    // bypassing the watermark gate (as the gate is not applied inside push_with_cascade).
    // The watermark gate is applied in tcp.rs BEFORE calling push_with_cascade.
    // push_with_cascade itself calls push_internal directly, with NO gate check.
    // So we can drive TooOld by calling push_with_cascade with an event_time
    // that is old relative to the ring buffer, even if it's technically "late".
    // -----------------------------------------------------------------------

    let (engine6, store6) = build_sales_engine();
    let t6 = epoch_plus_secs(1_700_500_000);

    // DO NOT apply the watermark gate. Call push_with_cascade directly.
    // Push "u1" at t6+200 to establish ring-buffer epoch.
    let ev200 = serde_json::json!({
        "user_id": "u1",
        "amount": 1.0,
        "_event_time": (t6 + Duration::from_secs(200)).duration_since(UNIX_EPOCH).unwrap().as_millis() as i64,
    });
    engine6
        .push_with_cascade("Sales", &ev200, &store6, t6 + Duration::from_secs(200))
        .expect("push t6+200");

    // rb_drops should be 0 so far.
    assert_eq!(
        engine6.ring_buffer_drops.total(),
        0,
        "no drops after first push"
    );
    assert_eq!(engine6.late_drops.get("Sales"), 0);

    // Push at t6+187: 200−187=13 > 10=window → TooOld in ring buffer.
    // No watermark gate applied here — push_with_cascade is called directly.
    let ev187 = serde_json::json!({
        "user_id": "u1",
        "amount": 99.0,
        "_event_time": (t6 + Duration::from_secs(187)).duration_since(UNIX_EPOCH).unwrap().as_millis() as i64,
    });
    engine6
        .push_with_cascade("Sales", &ev187, &store6, t6 + Duration::from_secs(187))
        .expect("push t6+187 (TooOld)");

    // Counter should now show TooOld drops for both count and sum operators.
    let rb_total = engine6.ring_buffer_drops.total();
    assert!(
        rb_total >= 2,
        "expected >= 2 ring-buffer TooOld drops (count + sum), got {}",
        rb_total
    );

    // No late-drops were recorded by the engine (gate was not applied above).
    assert_eq!(
        engine6.late_drops.get("Sales"),
        0,
        "late_drops must be 0 when gate is bypassed (ring-buffer drop only)"
    );

    // Snapshot contains only the three pre-registered reason variants per
    // operator — label count is bounded at streams × operator_kinds × 3.
    let snapshot = engine6.ring_buffer_drops.snapshot();
    // All snapshot entries must use only the three legal reason strings.
    for ((_, _, reason), _) in &snapshot {
        assert!(
            matches!(
                reason,
                DropReason::TooOld | DropReason::TooNew | DropReason::PreEpoch
            ),
            "unexpected reason variant in snapshot: {:?}",
            reason
        );
        assert!(
            matches!(reason.as_label(), "too_old" | "too_new" | "pre_epoch"),
            "unexpected reason label: {}",
            reason.as_label()
        );
    }

    // operator_kind labels are all from the compile-time set.
    let allowed_kinds = [
        "count",
        "sum",
        "avg",
        "min",
        "max",
        "stddev",
        "distinct_count",
        "exact_min",
        "exact_max",
        "variance",
    ];
    for ((_, kind, _), _) in &snapshot {
        assert!(
            allowed_kinds.contains(&kind.as_str()),
            "unexpected operator_kind label: {}",
            kind
        );
    }

    // Pushing 1000 more TooOld events must NOT create new label combinations.
    let before_len = snapshot.len();
    for _ in 0..10 {
        engine6
            .push_with_cascade("Sales", &ev187, &store6, t6 + Duration::from_secs(187))
            .expect("repeat TooOld");
    }
    let after_snapshot = engine6.ring_buffer_drops.snapshot();
    assert_eq!(
        after_snapshot.len(),
        before_len,
        "label cardinality must not grow with repeated drops (D-05 bounded cardinality)"
    );
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
#[test]
fn counters_mutually_exclusive() {
    let (engine, store) = build_sales_engine();
    let t = epoch_plus_secs(1_700_600_000);

    // --- Phase 1: Trigger late-drop via the watermark gate -------------------
    // Push an event at t+100 to set wm = t+95.
    push_sales(&engine, &store, "u1", 10.0, t + Duration::from_secs(100));

    let late_before = engine.late_drops.get("Sales");
    let rb_before = engine.ring_buffer_drops.total();
    assert_eq!(late_before, 0);
    assert_eq!(rb_before, 0);

    // Push a late event at t+90 (< wm=t+95). The push_sales helper applies
    // the watermark gate, increments late_drops, and returns false.
    let accepted = push_sales(&engine, &store, "u1", 99.0, t + Duration::from_secs(90));
    assert!(!accepted, "event at t+90 must be late-dropped");

    let late_after_gate = engine.late_drops.get("Sales");
    let rb_after_gate = engine.ring_buffer_drops.total();

    // Gate fired: late_drops incremented by exactly 1.
    assert_eq!(
        late_after_gate,
        late_before + 1,
        "late_drops must increment for the gate-dropped event"
    );
    // Ring-buffer counter must NOT have changed (event never reached the ring buffer).
    assert_eq!(
        rb_after_gate, rb_before,
        "ring_buffer_drops must NOT increment for a gate-dropped event (OBS-02)"
    );

    // --- Phase 2: Trigger ring-buffer drop (bypass gate) ---------------------
    // Use push_with_cascade directly to bypass the watermark gate.
    // First push at t+500 to advance the ring-buffer epoch for "u2".
    let ev500 = serde_json::json!({
        "user_id": "u2",
        "amount": 1.0,
        "_event_time": (t + Duration::from_secs(500)).duration_since(UNIX_EPOCH).unwrap().as_millis() as i64,
    });
    // Apply watermark gate for this push (it will pass since 500 > current wm).
    // We use push_sales but with a controlled event_time.
    // To keep things clean, just call push_with_cascade directly.
    engine
        .push_with_cascade("Sales", &ev500, &store, t + Duration::from_secs(500))
        .expect("push u2 at t+500");

    let _late_after_epoch = engine.late_drops.get("Sales");
    let rb_after_epoch = engine.ring_buffer_drops.total();
    // No new late-drops (we bypassed the gate).
    // u2 at t+500: fresh ring buffer, event accepted.
    assert_eq!(
        rb_after_epoch, rb_after_gate,
        "in-window push must not increment ring_buffer_drops"
    );

    // Now push u2 at t+487: 500−487=13 > window=10 → TooOld in ring buffer.
    // Bypass gate.
    let ev487 = serde_json::json!({
        "user_id": "u2",
        "amount": 99.0,
        "_event_time": (t + Duration::from_secs(487)).duration_since(UNIX_EPOCH).unwrap().as_millis() as i64,
    });

    let late_rb_before = engine.late_drops.get("Sales");
    let rb_rb_before = engine.ring_buffer_drops.total();

    engine
        .push_with_cascade("Sales", &ev487, &store, t + Duration::from_secs(487))
        .expect("push u2 at t+487 (TooOld)");

    let late_rb_after = engine.late_drops.get("Sales");
    let rb_rb_after = engine.ring_buffer_drops.total();

    // ring_buffer_drops MUST have incremented (count + sum operators each drop).
    assert!(
        rb_rb_after > rb_rb_before,
        "ring_buffer_drops must increment for TooOld ring-buffer drop, \
         before={} after={}",
        rb_rb_before,
        rb_rb_after
    );
    // late_drops must NOT have changed (gate was bypassed).
    assert_eq!(
        late_rb_after, late_rb_before,
        "late_drops must NOT increment for a ring-buffer-only TooOld drop (OBS-02)"
    );

    // --- Final invariant: sums obey mutual exclusivity ----------------------
    // Every event is counted in AT MOST ONE counter. The sum:
    //   late_drops + ring_buffer_drops
    // equals the total number of dropped events (no double-counting, no gaps
    // for events we intentionally dropped).
    //
    // Events pushed: 1 in-order, 1 late-gate, 1 in-order (u2@500), 1 ring-TooOld.
    // Drops: 1 late + (ring drops for u2@487, ≥1 per operator).
    // late_drops = 1, ring_buffer_drops >= 2 (count + sum).
    assert_eq!(
        engine.late_drops.get("Sales"),
        1,
        "exactly 1 late-gate drop total"
    );
    assert!(
        engine.ring_buffer_drops.total() >= 2,
        "at least 2 ring-buffer drops (count + sum operators) total"
    );
}
