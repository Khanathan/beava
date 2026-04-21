// Phase 59.6 SC-4 — CountOp, LastOp, SumOp, AvgOp, MinOp, MaxOp, FirstOp typed
// implementations produce identical entity state to Value path after 100K events.
// Wave 6: DistinctCountOp, PercentileOp, EmaOp, LagOp, StddevOp, VarianceOp,
// TopKOp, FirstNOp, LastNOp typed advanced-agg parity.

#![allow(unused_imports, dead_code)]

const OPS_WAVE_4: &[&str] = &["count", "last", "first", "sum", "avg", "min", "max"];
const OPS_WAVE_6: &[&str] = &[
    "distinct_count", "percentile", "ema", "lag", "stddev", "variance",
    "topk", "firstn", "lastn",
];

#[test]
#[ignore = "59.6-W4"]
fn typed_count_op_parity_100k_events() {
    panic!("SC-4 RED: typed CountOp not yet implemented; expected in Wave 4");
}

#[test]
#[ignore = "59.6-W4"]
fn typed_simple_aggs_parity_100k_events() {
    // Covers: count, last, first, sum, avg, min, max (D-F1 groups 1-4).
    let _ops = OPS_WAVE_4;
    panic!("SC-4 RED: Wave 4 simple aggs not yet implemented");
}

#[test]
#[ignore = "59.6-W6"]
fn typed_advanced_aggs_parity_100k_events() {
    // Covers: distinct_count, percentile, ema, lag, stddev, variance, topk, firstn, lastn.
    let _ops = OPS_WAVE_6;
    panic!("SC-4 RED: Wave 6 advanced aggs not yet implemented");
}
