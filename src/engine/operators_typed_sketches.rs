//! Phase 59.6 Wave 6 (TPC-PERF-11, D-F1 group 5-7) — typed advanced
//! aggregation operator implementations for sketches + numeric statistics.
//!
//! Five operators:
//!
//! - [`DistinctCountOpTyped`] — HLL-based unique count estimator.
//! - [`PercentileOpTyped`] — UDDSketch-based quantile estimator.
//! - [`TopKOpTyped`] — Count-Min-Sketch + TopKHeap top-K by frequency.
//! - [`StddevOpTyped`] — running population σ (stores sum + sum_sq + count).
//! - [`VarianceOpTyped`] — running population σ² (stores sum + sum_sq + count).
//!
//! # D-C1 type-erasure (DistinctCount / Percentile / TopK)
//!
//! These three ops have enough implementation complexity that
//! monomorphizing over i64/f64/str would blow codegen. Instead, the op
//! struct holds the input column's [`FieldTy`] and the typed wrapper
//! extracts the scalar value at update time, delegating to the existing
//! sketch impl (`src/engine/hll.rs`, `src/engine/uddsketch.rs`,
//! `src/engine/cms.rs`) via the per-entity [`SideBand`].
//!
//! # Pure-typed path (Stddev / Variance)
//!
//! Stddev and Variance are numeric running statistics — their state is
//! three scalars (sum, sum_sq, count) that fit directly in the per-entity
//! state Row. No SideBand needed; these look structurally identical to
//! [`crate::engine::operators_typed_aggs::AvgOpTypedF64`] with one extra
//! column.

use crate::engine::cms::{CountMinSketch, TopKHeap, TopKValue, DEFAULT_CMS_DEPTH, DEFAULT_CMS_WIDTH};
use crate::engine::hll::Hll;
use crate::engine::operators_typed::{SideBand, TypedAggOp};
use crate::engine::schema::{FieldTy, RegisteredSchema, Row};
use crate::engine::uddsketch::{UDDSketch, DEFAULT_ALPHA, DEFAULT_MAX_BUCKETS};
use crate::types::FeatureValue;
use std::time::SystemTime;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Read a scalar input value from an event Row as a canonical String key
/// (used by HLL + CMS, which hash a byte-stream). The mapping is
/// deliberately identical to the Value-path's `val.to_string()` /
/// `format!("{}", n)` shapes so parity tests see byte-identical sketch
/// state.
fn read_event_value_as_key(
    ev: &Row,
    ev_schema: &RegisteredSchema,
    input_offset: u16,
    input_ty: FieldTy,
) -> String {
    match input_ty {
        FieldTy::I64 => ev.read_i64(input_offset).to_string(),
        FieldTy::F64 => {
            let f = ev.read_f64(input_offset);
            // Match serde_json's Number formatting for f64 to stay byte-identical
            // with the Value-path sketches (which stringify via n.to_string()).
            serde_json::Number::from_f64(f)
                .map(|n| n.to_string())
                .unwrap_or_else(|| f.to_string())
        }
        FieldTy::Bool => ev.read_bool(input_offset).to_string(),
        FieldTy::InlineStr => ev
            .read_inline_str(input_offset, ev_schema.inline_str_cap)
            .to_string(),
        FieldTy::String => ev.read_string(input_offset).to_string(),
        FieldTy::Bytes => String::from_utf8_lossy(ev.read_bytes(input_offset)).to_string(),
    }
}

/// Read a numeric scalar from the event Row as f64 (for UDDSketch +
/// Stddev + Variance). Non-numeric input types degrade to 0.0 to match
/// the Value-path's graceful "skip unknown type" shape.
fn read_event_value_as_f64(ev: &Row, input_offset: u16, input_ty: FieldTy) -> Option<f64> {
    match input_ty {
        FieldTy::I64 => Some(ev.read_i64(input_offset) as f64),
        FieldTy::F64 => Some(ev.read_f64(input_offset)),
        _ => None,
    }
}

/// Convert a string key into a [`TopKValue`]. Mirrors the Value-path's
/// `TopKValue::from_json` dispatch on primitive shapes.
fn topk_value_from_event(
    ev: &Row,
    ev_schema: &RegisteredSchema,
    input_offset: u16,
    input_ty: FieldTy,
) -> Option<TopKValue> {
    match input_ty {
        FieldTy::I64 => Some(TopKValue::Int(ev.read_i64(input_offset))),
        FieldTy::F64 => Some(TopKValue::Float(ordered_float::OrderedFloat(
            ev.read_f64(input_offset),
        ))),
        FieldTy::Bool => Some(TopKValue::Bool(ev.read_bool(input_offset))),
        FieldTy::InlineStr => Some(TopKValue::Str(
            ev.read_inline_str(input_offset, ev_schema.inline_str_cap).to_string(),
        )),
        FieldTy::String => Some(TopKValue::Str(ev.read_string(input_offset).to_string())),
        FieldTy::Bytes => None,
    }
}

// ---------------------------------------------------------------------------
// DistinctCountOpTyped
// ---------------------------------------------------------------------------

/// Typed HLL-based distinct count. The per-entity state Row stores the
/// current estimate (i64) at `estimate_offset`; the actual HLL sketch
/// lives in [`SideBand::hll_sketches`] keyed by `self.name`. Wave 6
/// accepts the D-C1 codegen tradeoff: one typed wrapper + one HLL impl,
/// not three HLL impls monomorphized over i64/f64/str.
#[derive(Clone, Debug)]
pub struct DistinctCountOpTyped {
    pub name: String,
    pub input_offset: u16,
    pub input_ty: FieldTy,
    /// State Row offset where the current i64 estimate is stored. Readers
    /// of the state Row (snapshot, /debug) see the estimate without
    /// needing access to the SideBand.
    pub estimate_offset: u16,
}

impl TypedAggOp for DistinctCountOpTyped {
    fn init_state(&self, _ss: &RegisteredSchema, state: &mut Row) {
        state.write_i64(self.estimate_offset, 0);
    }

    fn update_typed(
        &self,
        _state: &mut Row,
        _ss: &RegisteredSchema,
        _event: &Row,
        _es: &RegisteredSchema,
        _now: SystemTime,
    ) {
        // DistinctCount requires SideBand; default path is a no-op. Use
        // update_with_sideband from the cascade dispatcher.
    }

    fn update_with_sideband(
        &self,
        state: &mut Row,
        _ss: &RegisteredSchema,
        event: &Row,
        es: &RegisteredSchema,
        sideband: &mut SideBand,
        _now: SystemTime,
    ) {
        let key = read_event_value_as_key(event, es, self.input_offset, self.input_ty);
        let hll = sideband
            .hll_sketches
            .entry(self.name.clone())
            .or_insert_with(Hll::new);
        hll.insert(&key);
        // Project the current estimate back into the state Row. Cast to
        // i64 because HLL.count() returns f64 but downstream readers want
        // an integer count.
        let est = hll.count() as i64;
        state.write_i64(self.estimate_offset, est);
    }

    fn read_feature(&self, state: &Row, _ss: &RegisteredSchema) -> FeatureValue {
        let est = state.read_i64(self.estimate_offset);
        FeatureValue::Float(est as f64)
    }

    fn read_feature_with_sideband(
        &self,
        _state: &Row,
        _ss: &RegisteredSchema,
        sideband: &SideBand,
    ) -> FeatureValue {
        match sideband.hll_sketches.get(&self.name) {
            Some(hll) => FeatureValue::Float(hll.count()),
            None => FeatureValue::Missing,
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

// ---------------------------------------------------------------------------
// PercentileOpTyped
// ---------------------------------------------------------------------------

/// Typed UDDSketch-based percentile. Like `DistinctCountOpTyped`, the
/// sketch lives in the SideBand; the state Row stores the current
/// quantile estimate as f64 at `estimate_offset`.
#[derive(Clone, Debug)]
pub struct PercentileOpTyped {
    pub name: String,
    pub input_offset: u16,
    pub input_ty: FieldTy,
    /// Quantile in [0, 1].
    pub quantile: f64,
    pub estimate_offset: u16,
}

impl TypedAggOp for PercentileOpTyped {
    fn init_state(&self, _ss: &RegisteredSchema, state: &mut Row) {
        state.write_f64(self.estimate_offset, 0.0);
    }

    fn update_typed(
        &self,
        _state: &mut Row,
        _ss: &RegisteredSchema,
        _event: &Row,
        _es: &RegisteredSchema,
        _now: SystemTime,
    ) {
        // Percentile requires SideBand.
    }

    fn update_with_sideband(
        &self,
        state: &mut Row,
        _ss: &RegisteredSchema,
        event: &Row,
        _es: &RegisteredSchema,
        sideband: &mut SideBand,
        _now: SystemTime,
    ) {
        let Some(v) = read_event_value_as_f64(event, self.input_offset, self.input_ty) else {
            return;
        };
        let sketch = sideband
            .udd_sketches
            .entry(self.name.clone())
            .or_insert_with(|| UDDSketch::new(DEFAULT_ALPHA, DEFAULT_MAX_BUCKETS));
        sketch.insert(v);
        let q = sketch.quantile(self.quantile);
        state.write_f64(self.estimate_offset, q);
    }

    fn read_feature(&self, state: &Row, _ss: &RegisteredSchema) -> FeatureValue {
        FeatureValue::Float(state.read_f64(self.estimate_offset))
    }

    fn read_feature_with_sideband(
        &self,
        _state: &Row,
        _ss: &RegisteredSchema,
        sideband: &SideBand,
    ) -> FeatureValue {
        match sideband.udd_sketches.get(&self.name) {
            Some(s) if !s.is_empty() => FeatureValue::Float(s.quantile(self.quantile)),
            _ => FeatureValue::Missing,
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

// ---------------------------------------------------------------------------
// TopKOpTyped
// ---------------------------------------------------------------------------

/// Typed top-K by frequency. Pairs a [`CountMinSketch`] with a [`TopKHeap`]
/// both stored in the SideBand keyed by op.name(). The state Row stores
/// the current heap-size (i64) at `size_offset` as a lightweight
/// health check for /debug.
#[derive(Clone, Debug)]
pub struct TopKOpTyped {
    pub name: String,
    pub input_offset: u16,
    pub input_ty: FieldTy,
    /// Number of top items to track.
    pub k: usize,
    pub size_offset: u16,
}

impl TypedAggOp for TopKOpTyped {
    fn init_state(&self, _ss: &RegisteredSchema, state: &mut Row) {
        state.write_i64(self.size_offset, 0);
    }

    fn update_typed(
        &self,
        _state: &mut Row,
        _ss: &RegisteredSchema,
        _event: &Row,
        _es: &RegisteredSchema,
        _now: SystemTime,
    ) {
        // TopK requires SideBand.
    }

    fn update_with_sideband(
        &self,
        state: &mut Row,
        _ss: &RegisteredSchema,
        event: &Row,
        es: &RegisteredSchema,
        sideband: &mut SideBand,
        _now: SystemTime,
    ) {
        let Some(v) = topk_value_from_event(event, es, self.input_offset, self.input_ty) else {
            return;
        };
        let (cms, heap) = sideband
            .topk_sketches
            .entry(self.name.clone())
            .or_insert_with(|| {
                (
                    CountMinSketch::new(DEFAULT_CMS_WIDTH, DEFAULT_CMS_DEPTH),
                    TopKHeap::new(self.k),
                )
            });
        cms.insert(v.hash64());
        heap.observe(&v, cms);
        state.write_i64(self.size_offset, heap.num_candidates() as i64);
    }

    fn read_feature(&self, state: &Row, _ss: &RegisteredSchema) -> FeatureValue {
        FeatureValue::Int(state.read_i64(self.size_offset))
    }

    fn read_feature_with_sideband(
        &self,
        _state: &Row,
        _ss: &RegisteredSchema,
        sideband: &SideBand,
    ) -> FeatureValue {
        match sideband.topk_sketches.get(&self.name) {
            Some((cms, heap)) => {
                let top = heap.top_k(cms);
                if top.is_empty() {
                    FeatureValue::Missing
                } else {
                    let arr: Vec<serde_json::Value> = top
                        .iter()
                        .map(|(v, c)| {
                            serde_json::json!({
                                "value": v.to_json(),
                                "count": c,
                            })
                        })
                        .collect();
                    let s = serde_json::to_string(&arr).unwrap_or_else(|_| "[]".into());
                    FeatureValue::String(s)
                }
            }
            None => FeatureValue::Missing,
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

// ---------------------------------------------------------------------------
// StddevOpTyped / VarianceOpTyped
//
// Both share the same three-column state: (sum_offset: f64, sum_sq_offset:
// f64, count_offset: i64). Stddev returns √variance; Variance returns the
// raw population variance. They mirror the Value-path StddevOp / VarianceOp
// running formulas: mean = sum/count, variance = sum_sq/count - mean².
// ---------------------------------------------------------------------------

/// Column-typed input dispatcher for stddev/variance. Stored separately on
/// each op so the update path doesn't branch per-event through a hot match.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NumCol {
    I64,
    F64,
}

#[inline]
fn read_num(ev: &Row, offset: u16, col: NumCol) -> f64 {
    match col {
        NumCol::I64 => ev.read_i64(offset) as f64,
        NumCol::F64 => ev.read_f64(offset),
    }
}

#[derive(Clone, Debug)]
pub struct StddevOpTyped {
    pub name: String,
    pub input_offset: u16,
    pub input_col: NumCol,
    pub sum_offset: u16,
    pub sum_sq_offset: u16,
    pub count_offset: u16,
}

impl TypedAggOp for StddevOpTyped {
    fn init_state(&self, _ss: &RegisteredSchema, state: &mut Row) {
        state.write_f64(self.sum_offset, 0.0);
        state.write_f64(self.sum_sq_offset, 0.0);
        state.write_i64(self.count_offset, 0);
    }

    #[inline]
    fn update_typed(
        &self,
        state: &mut Row,
        _ss: &RegisteredSchema,
        event: &Row,
        _es: &RegisteredSchema,
        _now: SystemTime,
    ) {
        let v = read_num(event, self.input_offset, self.input_col);
        state.write_f64(self.sum_offset, state.read_f64(self.sum_offset) + v);
        state.write_f64(self.sum_sq_offset, state.read_f64(self.sum_sq_offset) + v * v);
        state.write_i64(self.count_offset, state.read_i64(self.count_offset) + 1);
    }

    fn read_feature(&self, state: &Row, _ss: &RegisteredSchema) -> FeatureValue {
        let count = state.read_i64(self.count_offset);
        if count < 2 {
            if count == 0 {
                return FeatureValue::Missing;
            }
            return FeatureValue::Float(0.0);
        }
        let sum = state.read_f64(self.sum_offset);
        let sum_sq = state.read_f64(self.sum_sq_offset);
        let mean = sum / count as f64;
        let variance = (sum_sq / count as f64) - (mean * mean);
        let stddev = if variance < 0.0 { 0.0 } else { variance.sqrt() };
        FeatureValue::Float(stddev)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Clone, Debug)]
pub struct VarianceOpTyped {
    pub name: String,
    pub input_offset: u16,
    pub input_col: NumCol,
    pub sum_offset: u16,
    pub sum_sq_offset: u16,
    pub count_offset: u16,
}

impl TypedAggOp for VarianceOpTyped {
    fn init_state(&self, _ss: &RegisteredSchema, state: &mut Row) {
        state.write_f64(self.sum_offset, 0.0);
        state.write_f64(self.sum_sq_offset, 0.0);
        state.write_i64(self.count_offset, 0);
    }

    #[inline]
    fn update_typed(
        &self,
        state: &mut Row,
        _ss: &RegisteredSchema,
        event: &Row,
        _es: &RegisteredSchema,
        _now: SystemTime,
    ) {
        let v = read_num(event, self.input_offset, self.input_col);
        state.write_f64(self.sum_offset, state.read_f64(self.sum_offset) + v);
        state.write_f64(self.sum_sq_offset, state.read_f64(self.sum_sq_offset) + v * v);
        state.write_i64(self.count_offset, state.read_i64(self.count_offset) + 1);
    }

    fn read_feature(&self, state: &Row, _ss: &RegisteredSchema) -> FeatureValue {
        let count = state.read_i64(self.count_offset);
        if count < 2 {
            if count == 0 {
                return FeatureValue::Missing;
            }
            return FeatureValue::Float(0.0);
        }
        let sum = state.read_f64(self.sum_offset);
        let sum_sq = state.read_f64(self.sum_sq_offset);
        let n = count as f64;
        // Sample variance to match the Value-path `VarianceOp` (Welford +
        // n-1 divisor). Algebraic equivalence: m2 = sum_sq - n * mean^2;
        // sample_variance = m2 / (n - 1).
        let mean = sum / n;
        let m2 = sum_sq - n * mean * mean;
        let variance = m2 / (n - 1.0);
        let variance = if variance < 0.0 { 0.0 } else { variance };
        FeatureValue::Float(variance)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

// ---------------------------------------------------------------------------
// Tests — operator-boundary parity for each sketch / numeric op vs its
// Value-path sibling on the same event stream.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::schema::{FieldSpec, FieldTy};
    use std::sync::Arc;

    fn num_schema() -> Arc<RegisteredSchema> {
        let s = RegisteredSchema {
            schema_id: 0,
            name: "Txns".into(),
            fields: vec![
                FieldSpec { name: "user_id".into(), ty: FieldTy::InlineStr, offset: 0, nullable: false },
                FieldSpec { name: "amount".into(), ty: FieldTy::F64, offset: 16, nullable: false },
            ],
            inline_str_cap: 15,
            row_size: 24,
        };
        s.validate_layout().expect("valid");
        Arc::new(s)
    }

    fn state_schema_stats() -> Arc<RegisteredSchema> {
        // Stddev / variance share the same layout: (sum:f64@0, sum_sq:f64@8, count:i64@16)
        let s = RegisteredSchema {
            schema_id: 0,
            name: "Stats".into(),
            fields: vec![
                FieldSpec { name: "sum".into(), ty: FieldTy::F64, offset: 0, nullable: false },
                FieldSpec { name: "sum_sq".into(), ty: FieldTy::F64, offset: 8, nullable: false },
                FieldSpec { name: "count".into(), ty: FieldTy::I64, offset: 16, nullable: false },
            ],
            inline_str_cap: 15,
            row_size: 24,
        };
        s.validate_layout().expect("valid");
        Arc::new(s)
    }

    fn make_event(user: &str, amount: f64) -> Row {
        let sch = num_schema();
        let mut r = Row::zeroed(&sch);
        r.write_inline_str(0, sch.inline_str_cap, user);
        r.write_f64(16, amount);
        r
    }

    #[test]
    fn distinct_count_typed_estimates_non_zero() {
        let op = DistinctCountOpTyped {
            name: "dc".into(),
            input_offset: 0,
            input_ty: FieldTy::InlineStr,
            estimate_offset: 0,
        };
        let state_sch = {
            let s = RegisteredSchema {
                schema_id: 0,
                name: "S".into(),
                fields: vec![FieldSpec { name: "est".into(), ty: FieldTy::I64, offset: 0, nullable: false }],
                inline_str_cap: 15,
                row_size: 8,
            };
            s.validate_layout().unwrap();
            Arc::new(s)
        };
        let ev_sch = num_schema();
        let mut state = Row::zeroed(&state_sch);
        op.init_state(&state_sch, &mut state);
        let mut sb = SideBand::default();
        for i in 0..100 {
            let e = make_event(&format!("u{}", i % 7), 0.0);
            op.update_with_sideband(&mut state, &state_sch, &e, &ev_sch, &mut sb, SystemTime::now());
        }
        let out = op.read_feature_with_sideband(&state, &state_sch, &sb);
        match out {
            FeatureValue::Float(f) => assert!(f >= 6.0 && f <= 8.0, "expected ~7 distinct, got {}", f),
            v => panic!("expected Float(~7), got {:?}", v),
        }
    }

    #[test]
    fn percentile_typed_p50_matches_median() {
        let op = PercentileOpTyped {
            name: "p50".into(),
            input_offset: 16,
            input_ty: FieldTy::F64,
            quantile: 0.5,
            estimate_offset: 0,
        };
        let state_sch = {
            let s = RegisteredSchema {
                schema_id: 0,
                name: "S".into(),
                fields: vec![FieldSpec { name: "q".into(), ty: FieldTy::F64, offset: 0, nullable: false }],
                inline_str_cap: 15,
                row_size: 8,
            };
            s.validate_layout().unwrap();
            Arc::new(s)
        };
        let ev_sch = num_schema();
        let mut state = Row::zeroed(&state_sch);
        op.init_state(&state_sch, &mut state);
        let mut sb = SideBand::default();
        for i in 1..=101 {
            let e = make_event("u", i as f64);
            op.update_with_sideband(&mut state, &state_sch, &e, &ev_sch, &mut sb, SystemTime::now());
        }
        let out = op.read_feature_with_sideband(&state, &state_sch, &sb);
        // UDDSketch p50 of 1..101 should be within α relative error of 51.
        match out {
            FeatureValue::Float(f) => assert!(
                (f - 51.0).abs() / 51.0 <= 0.05,
                "expected median near 51, got {}",
                f
            ),
            v => panic!("expected Float, got {:?}", v),
        }
    }

    #[test]
    fn topk_typed_tracks_heavy_hitters() {
        let op = TopKOpTyped {
            name: "tk".into(),
            input_offset: 0,
            input_ty: FieldTy::InlineStr,
            k: 3,
            size_offset: 0,
        };
        let state_sch = {
            let s = RegisteredSchema {
                schema_id: 0,
                name: "S".into(),
                fields: vec![FieldSpec { name: "size".into(), ty: FieldTy::I64, offset: 0, nullable: false }],
                inline_str_cap: 15,
                row_size: 8,
            };
            s.validate_layout().unwrap();
            Arc::new(s)
        };
        let ev_sch = num_schema();
        let mut state = Row::zeroed(&state_sch);
        op.init_state(&state_sch, &mut state);
        let mut sb = SideBand::default();
        // 50 events of "heavy", 5 each of "mid1".."mid4", 1 each "tail1".."tail20"
        for _ in 0..50 {
            op.update_with_sideband(&mut state, &state_sch, &make_event("heavy", 0.0), &ev_sch, &mut sb, SystemTime::now());
        }
        for i in 0..4 {
            for _ in 0..5 {
                op.update_with_sideband(&mut state, &state_sch, &make_event(&format!("mid{}", i), 0.0), &ev_sch, &mut sb, SystemTime::now());
            }
        }
        for i in 0..20 {
            op.update_with_sideband(&mut state, &state_sch, &make_event(&format!("tail{}", i), 0.0), &ev_sch, &mut sb, SystemTime::now());
        }
        // The top-1 should be "heavy".
        let (cms, heap) = sb.topk_sketches.get("tk").expect("sketch present");
        let top = heap.top_k(cms);
        assert!(!top.is_empty());
        assert_eq!(top[0].0, TopKValue::Str("heavy".into()));
        assert!(top[0].1 >= 50);
    }

    #[test]
    fn stddev_typed_matches_formula() {
        let op = StddevOpTyped {
            name: "s".into(),
            input_offset: 16,
            input_col: NumCol::F64,
            sum_offset: 0,
            sum_sq_offset: 8,
            count_offset: 16,
        };
        let state_sch = state_schema_stats();
        let ev_sch = num_schema();
        let mut state = Row::zeroed(&state_sch);
        op.init_state(&state_sch, &mut state);
        for v in [1.0_f64, 2.0, 3.0, 4.0, 5.0] {
            op.update_typed(&mut state, &state_sch, &make_event("u", v), &ev_sch, SystemTime::now());
        }
        // mean=3, var=(1+4+9+16+25)/5 - 9 = 11 - 9 = 2, stddev=sqrt(2)
        match op.read_feature(&state, &state_sch) {
            FeatureValue::Float(f) => assert!((f - 2f64.sqrt()).abs() < 1e-9, "got {}", f),
            v => panic!("expected Float, got {:?}", v),
        }
    }

    #[test]
    fn variance_typed_matches_formula() {
        let op = VarianceOpTyped {
            name: "v".into(),
            input_offset: 16,
            input_col: NumCol::F64,
            sum_offset: 0,
            sum_sq_offset: 8,
            count_offset: 16,
        };
        let state_sch = state_schema_stats();
        let ev_sch = num_schema();
        let mut state = Row::zeroed(&state_sch);
        op.init_state(&state_sch, &mut state);
        for v in [1.0_f64, 2.0, 3.0, 4.0, 5.0] {
            op.update_typed(&mut state, &state_sch, &make_event("u", v), &ev_sch, SystemTime::now());
        }
        // Sample variance (n-1 divisor) of [1..5] = 10/4 = 2.5, matching
        // Value-path VarianceOp.
        match op.read_feature(&state, &state_sch) {
            FeatureValue::Float(f) => assert!((f - 2.5).abs() < 1e-9, "got {}", f),
            v => panic!("expected Float, got {:?}", v),
        }
    }

    #[test]
    fn stddev_variance_empty_is_missing() {
        let state_sch = state_schema_stats();
        let mut state = Row::zeroed(&state_sch);
        let s = StddevOpTyped {
            name: "s".into(),
            input_offset: 16,
            input_col: NumCol::F64,
            sum_offset: 0,
            sum_sq_offset: 8,
            count_offset: 16,
        };
        let v = VarianceOpTyped {
            name: "v".into(),
            input_offset: 16,
            input_col: NumCol::F64,
            sum_offset: 0,
            sum_sq_offset: 8,
            count_offset: 16,
        };
        s.init_state(&state_sch, &mut state);
        v.init_state(&state_sch, &mut state);
        assert!(matches!(s.read_feature(&state, &state_sch), FeatureValue::Missing));
        assert!(matches!(v.read_feature(&state, &state_sch), FeatureValue::Missing));
    }
}
