//! Per-entity memory size_of dump + Phase 12.9 regression gate.
//!
//! Originally an investigation tool (post-Phase 12.8); promoted to a
//! permanent CI-enforced size_of regression gate by Phase 12.9.
//!
//! Asserts `size_of::<AggOp>() <= AGGOP_SIZE_CAP_BYTES` (80 bytes post-12.9).
//! Future operator additions that exceed the cap force a deliberate review
//! decision: either Box the new variant (preferred — preserves the cap) or
//! explicitly raise the cap with a documented rationale.
//!
//! Also dumps `std::mem::size_of` for every AggOp variant's state struct +
//! the AggOp enum + WindowedOp overhead + a per-derivation projection
//! against fraud-team.json's actual feature counts.
//!
//! Run with:
//!   cargo test -p beava-core --test per_entity_size_dump -- --nocapture

use std::mem::size_of;

use beava_core::agg_buffer::{
    DowHourHistogramState, EventTypeMixState, HistogramState, HourOfDayHistogramState,
    MostRecentNState, ReservoirSampleState, SeasonalDeviationState,
};
use beava_core::agg_geo::{
    DistanceFromHomeState, GeoDistanceState, GeoSpreadState, GeoVelocityState,
};
use beava_core::agg_op::{AggKind, AggOp};
use beava_core::agg_state::{
    AvgState, CountState, FirstNState, FirstSeenInWindowState, FirstState, LagState, LastNState,
    LastState, MaxState, MinState, NegativeStreakState, RatioState, SeenState, StreakState,
    SumState, TimeSinceLastNState, VarianceState,
};
use beava_core::agg_state::{
    BloomMemberStateWrap, CountDistinctStateWrap, EntropyStateWrap, PercentileStateWrap,
    TopKStateWrap,
};
use beava_core::agg_state_decay::{
    DecayedCountState, DecayedSumState, EwVarState, EwZScoreState, EwmaState, TwaState,
};
use beava_core::agg_state_velocity::{
    BurstCountState, DeltaFromPrevState, InterArrivalStatsState, OutlierCountState,
    RateOfChangeState, TrendResidualState, TrendState, ValueChangeCountState, ZScoreState,
};
use beava_core::agg_windowed::WindowedOp;

/// Phase 12.9 regression gate: AggOp must stay ≤ 80 bytes inline.
///
/// Phase 11 D-08 originally accepted ~600 B AggOp slots (driven by
/// `SeasonalDeviationState`'s 24-hour bucket array) to avoid per-event
/// box indirection. Phase 12.9 reversed this 2026-05-03 after the r8g
/// maxcard bench measured fraud-team at ~22 KB/entity (3× the CLAUDE.md
/// 7 KB budget).
///
/// 80 B = `TrendResidualState` (72 B; the largest unboxed variant
/// post-12.9) plus discriminant + alignment headroom. Raising this cap
/// requires either boxing a new variant or surfacing the trade-off in
/// review.
const AGGOP_SIZE_CAP_BYTES: usize = 80;

#[test]
fn aggop_size_within_cap() {
    let actual = size_of::<AggOp>();
    assert!(
        actual <= AGGOP_SIZE_CAP_BYTES,
        "size_of::<AggOp>() = {actual} bytes — exceeds Phase 12.9 cap of {AGGOP_SIZE_CAP_BYTES} bytes. \
         A new fat-payload variant was likely added unboxed. Either Box the variant in agg_op.rs \
         (same pattern as CountDistinct/Percentile/Windowed) or raise AGGOP_SIZE_CAP_BYTES with \
         a documented rationale. See .planning/phases/12.9-aggop-memory-boxing/12.9-CONTEXT.md."
    );
}

#[test]
fn dump_per_entity_sizes() {
    println!("\n=== AggOp enum + descriptor sizes ===");
    println!(
        "size_of::<AggKind>()        = {} bytes",
        size_of::<AggKind>()
    );
    println!(
        "size_of::<AggOp>()          = {} bytes  (enum payload, the per-feature footprint)",
        size_of::<AggOp>()
    );
    println!(
        "size_of::<Option<AggOp>>()  = {} bytes",
        size_of::<Option<AggOp>>()
    );

    println!("\n=== Phase 5: core stats (8 ops, all inline payloads) ===");
    let mut rows: Vec<(&'static str, usize)> = vec![
        ("CountState", size_of::<CountState>()),
        ("SumState", size_of::<SumState>()),
        ("AvgState", size_of::<AvgState>()),
        ("MinState", size_of::<MinState>()),
        ("MaxState", size_of::<MaxState>()),
        ("VarianceState", size_of::<VarianceState>()),
        ("RatioState", size_of::<RatioState>()),
    ];
    print_rows(&rows);

    println!("\n=== Phase 8: point/ordinal + recency (15 ops) ===");
    rows = vec![
        ("FirstState", size_of::<FirstState>()),
        ("LastState", size_of::<LastState>()),
        ("FirstNState", size_of::<FirstNState>()),
        ("LastNState", size_of::<LastNState>()),
        ("LagState", size_of::<LagState>()),
        (
            "SeenState (FirstSeen/LastSeen/Age/HasSeen/TimeSince)",
            size_of::<SeenState>(),
        ),
        ("TimeSinceLastNState", size_of::<TimeSinceLastNState>()),
        ("StreakState (Streak/MaxStreak)", size_of::<StreakState>()),
        ("NegativeStreakState", size_of::<NegativeStreakState>()),
        (
            "FirstSeenInWindowState",
            size_of::<FirstSeenInWindowState>(),
        ),
    ];
    print_rows(&rows);

    println!("\n=== Phase 9: decay + velocity + z-score (15 ops) ===");
    rows = vec![
        ("EwmaState", size_of::<EwmaState>()),
        ("EwVarState", size_of::<EwVarState>()),
        ("EwZScoreState", size_of::<EwZScoreState>()),
        ("DecayedSumState", size_of::<DecayedSumState>()),
        ("DecayedCountState", size_of::<DecayedCountState>()),
        ("TwaState", size_of::<TwaState>()),
        ("RateOfChangeState", size_of::<RateOfChangeState>()),
        (
            "InterArrivalStatsState",
            size_of::<InterArrivalStatsState>(),
        ),
        ("BurstCountState", size_of::<BurstCountState>()),
        ("DeltaFromPrevState", size_of::<DeltaFromPrevState>()),
        ("TrendState", size_of::<TrendState>()),
        ("TrendResidualState", size_of::<TrendResidualState>()),
        ("OutlierCountState", size_of::<OutlierCountState>()),
        ("ValueChangeCountState", size_of::<ValueChangeCountState>()),
        ("ZScoreState", size_of::<ZScoreState>()),
    ];
    print_rows(&rows);

    println!("\n=== Phase 10: sketches (5 ops, all Box<...>) ===");
    println!("In AggOp these are stored as Box<T> — 8B inline + heap state below.");
    rows = vec![
        (
            "CountDistinctStateWrap (HLL hybrid)",
            size_of::<CountDistinctStateWrap>(),
        ),
        (
            "PercentileStateWrap (UDDSketch wrapper)",
            size_of::<PercentileStateWrap>(),
        ),
        (
            "TopKStateWrap (SpaceSaving wrapper)",
            size_of::<TopKStateWrap>(),
        ),
        (
            "BloomMemberStateWrap (Bloom filter wrapper)",
            size_of::<BloomMemberStateWrap>(),
        ),
        (
            "EntropyStateWrap (categorical histogram)",
            size_of::<EntropyStateWrap>(),
        ),
    ];
    print_rows(&rows);

    println!("\n=== Phase 11: bounded buffer + geo (11 ops) ===");
    rows = vec![
        ("HistogramState", size_of::<HistogramState>()),
        (
            "HourOfDayHistogramState",
            size_of::<HourOfDayHistogramState>(),
        ),
        ("DowHourHistogramState", size_of::<DowHourHistogramState>()),
        (
            "SeasonalDeviationState",
            size_of::<SeasonalDeviationState>(),
        ),
        ("EventTypeMixState", size_of::<EventTypeMixState>()),
        ("MostRecentNState", size_of::<MostRecentNState>()),
        ("ReservoirSampleState", size_of::<ReservoirSampleState>()),
        ("GeoVelocityState", size_of::<GeoVelocityState>()),
        ("GeoDistanceState", size_of::<GeoDistanceState>()),
        ("GeoSpreadState", size_of::<GeoSpreadState>()),
        ("DistanceFromHomeState", size_of::<DistanceFromHomeState>()),
    ];
    print_rows(&rows);

    println!("\n=== Windowed wrapper ===");
    println!("In AggOp this is stored as Box<WindowedOp> — 8B inline + heap state below.");
    rows = vec![(
        "WindowedOp (struct itself, no buckets)",
        size_of::<WindowedOp>(),
    )];
    print_rows(&rows);

    println!("\n=== Summary: AggOp variants stored inline vs boxed ===");
    println!("AggOp enum size = max(payload) + discriminant + alignment.");
    let aggop_size = size_of::<AggOp>();
    println!("size_of::<AggOp>() = {aggop_size} bytes — set by the LARGEST inline payload.");
    println!("Every feature in a Vec<AggOp> consumes {aggop_size} bytes of slot memory,");
    println!("regardless of the variant actually stored.");
    println!();
    println!("Largest inline (non-Box) payloads in current AggOp:");
    println!(
        "  SeasonalDeviationState     {} bytes  ← floor-setter",
        size_of::<SeasonalDeviationState>()
    );
    println!("  HourOfDayHistogramState    {} bytes  ← second-largest (would set floor if Seasonal boxed)", size_of::<HourOfDayHistogramState>());
    println!(
        "  EventTypeMixState          {} bytes",
        size_of::<EventTypeMixState>()
    );
    println!(
        "  DistanceFromHomeState      {} bytes",
        size_of::<DistanceFromHomeState>()
    );
    println!(
        "  GeoVelocityState/GeoSpread  {} bytes",
        size_of::<GeoVelocityState>()
    );
    println!(
        "  GeoDistanceState           {} bytes",
        size_of::<GeoDistanceState>()
    );
    println!(
        "  TrendResidualState         {} bytes",
        size_of::<TrendResidualState>()
    );
    println!(
        "  BurstCountState            {} bytes",
        size_of::<BurstCountState>()
    );
    println!("All boxed wrappers (CountDistinctStateWrap/PercentileStateWrap/TopKStateWrap/");
    println!("BloomMemberStateWrap/EntropyStateWrap/WindowedOp) contribute only 8 bytes inline.");

    println!("\n=== Per-entity slot cost — fraud-team.json derivations ===");
    println!(
        "(features-per-entity × size_of::<AggOp>(), inline-slot floor only — heap state extra)"
    );
    let derivs: &[(&str, usize)] = &[
        ("TxnByUser (user_id)", 62),
        ("TxnByCard (card_fp)", 8),
        ("TxnByDevice (device_id)", 6),
        ("TxnByIp (ip_address)", 8),
        ("TxnByMerchant (merchant_id)", 4),
        ("LoginByUser (user_id)", 8),
        ("SignupByIp (ip_address)", 4),
        ("CardAddByDevice (device_id)", 3),
        ("RefundByUser (user_id)", 8),
    ];
    println!(
        "  {:<32}  {:>5}  {:>10}  {:>10}",
        "derivation", "feats", "current", "if_boxed"
    );
    println!(
        "  {:<32}  {:>5}  {:>10}  {:>10}",
        "─".repeat(32),
        "─────",
        "──────────",
        "──────────"
    );
    let if_boxed_floor: usize = 72; // discriminant + next-largest after boxing 7 fat variants → ~64 + alignment
    let mut total_user_id_feats = 0;
    for (name, n) in derivs {
        let cur = n * aggop_size;
        let new = n * if_boxed_floor;
        println!("  {:<32}  {:>5}  {:>7} B  {:>7} B", name, n, cur, new);
        if name.contains("user_id") {
            total_user_id_feats += n;
        }
    }
    let cur_user = total_user_id_feats * aggop_size;
    let new_user = total_user_id_feats * if_boxed_floor;
    println!(
        "  {:<32}  {:>5}  {:>7} B  {:>7} B",
        "─".repeat(32),
        "─────",
        "──────────",
        "──────────"
    );
    println!(
        "  {:<32}  {:>5}  {:>7} B  {:>7} B   ← single user_id entity (3 derivs)",
        "user_id total (TxnByUser+Login+Refund)", total_user_id_feats, cur_user, new_user
    );
    println!();
    println!("=== Boxing-savings projection ===");
    println!("Hypothesis: boxing the 7 fat-payload variants (SeasonalDeviation, HourOfDay,");
    println!("EventTypeMix, GeoVelocity, GeoSpread, GeoDistance, DistanceFromHome — and");
    println!("optionally TrendResidual + BurstCount) would drop size_of::<AggOp>() from");
    println!(
        "{} to ~{} bytes ({}× shrink).",
        aggop_size,
        if_boxed_floor,
        aggop_size / if_boxed_floor
    );
    println!("For a single user_id entity (78 features across 3 derivations):");
    println!(
        "  Current inline-slot cost:  {:>7} B ({:.1} KB)",
        cur_user,
        cur_user as f64 / 1024.0
    );
    println!(
        "  After boxing fat variants: {:>7} B ({:.1} KB)",
        new_user,
        new_user as f64 / 1024.0
    );
    println!(
        "  Savings:                   {:>7} B ({:.1} KB)",
        cur_user - new_user,
        (cur_user - new_user) as f64 / 1024.0
    );
    println!();
    println!("This is the inline-slot floor only. Heap state (HLL, UDDSketch, BTreeMap,");
    println!("WindowedOp buckets) is unaffected by boxing since it's already on the heap.");
    println!();
}

fn print_rows(rows: &[(&'static str, usize)]) {
    let max_name = rows.iter().map(|(n, _)| n.len()).max().unwrap_or(40);
    for (name, size) in rows {
        println!("  {:<width$}  {:>5} bytes", name, size, width = max_name);
    }
}
