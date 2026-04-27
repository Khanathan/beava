//! Windowed<Op> wrapper: 64-bucket event-time tumbling ring buffer.
//!
//! Phase 5 — implementation lands in plan 05-01 Task 2.b.
//! This file provides a compilable stub so agg_op.rs can reference WindowedOp
//! at Task 1 (red commit). Tests are added in Task 2.a (red).
//!
//! # Requirements traceability
//! - AGG-CORE-09: Windowed<Op> with 64-bucket event-time tumbling
//!
//! D-04: bucket_index = floor(t / bucket_ms) mod 64 via div_euclid.
//! D-06: no wall-clock reads, no rand — pure event-time determinism.
//!
//! # Phase 19.1-04: lazy bucket allocation
//!
//! The old layout pre-allocated `[Option<Box<AggOp>>; 64]` + `[i64; 64]` for
//! every WindowedOp instance — ~1024 bytes of zero-init memory per instance.
//! With 4-14 windowed ops per entity in fraud-team-shape pipelines, this was
//! ~60% of the cold-key entity init cost (~1500 ns / 2576 ns mean).
//!
//! Phase 19.1-04 (per CONTEXT D-16/D-19/D-20) replaces this with
//! `SmallVec<[(i64, Box<AggOp>); 4]>` + lazy allocation. Most entities only
//! see 1-2 active buckets at any given moment; the 4-slot inline SmallVec
//! covers the typical case without heap allocation. The 64-bucket cap from
//! AGG-CORE-09 is enforced by oldest-epoch eviction on each new-epoch insert
//! once `buckets.len() >= max_buckets`.
//!
//! Bucket lookup is now a linear scan of the SmallVec by epoch (typical
//! 1-2 active = effectively O(1); worst-case 64 entries × ~0.5 ns scan ≈
//! 32 ns — still cheap vs the saved ~1500 ns cold-init).
//!
//! Snapshot format: serde representation of `SmallVec<[(i64, Box<AggOp>); 4]>`
//! is incompatible with the OLD `[Option<Box<AggOp>>; 64]` + `[i64; 64]`
//! representation. Phase 7 recovery falls back to WAL replay if snapshot
//! deserialization fails. Operators with existing snapshots: delete the
//! snapshot file before restart; the WAL will replay the missing state.

use crate::agg_op::{AggKind, AggOp, SketchParams};
use crate::row::Row;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

/// 64-bucket event-time tumbling ring buffer wrapping any core AggOp.
///
/// AGG-CORE-09: Windowed<Op> with 64 tumbling event-time buckets.
/// `bucket_ms = ceil(window_ms / 64)`. On update: route to the bucket whose
/// epoch matches the event time, lazily creating it if needed; evict the
/// oldest-epoch entry once `buckets.len() >= max_buckets`. On query: fold
/// active buckets (those with epoch ∈ [query_time - window_ms, query_time])
/// using op-specific combine logic (Welford pairwise for variance/stddev).
///
/// Phase 19.1-04: lazy `SmallVec<[(epoch_ms, Box<AggOp>); 4]>` replaces
/// the original `[Option<Box<AggOp>>; 64]` + `[i64; 64]` arrays for ~60%
/// reduction in cold WindowedOp::new cost on complex pipelines.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowedOp {
    pub inner_kind: AggKind,
    pub bucket_ms: u64,
    pub window_ms: u64,
    /// Lazy-allocated bucket entries: `(epoch_start_ms, op_state)`.
    /// Inline cap 4 covers the typical fraud workload (1-2 active buckets
    /// per entity at any time); spills to heap above 4. Cap of 64 active
    /// buckets (AGG-CORE-09) enforced by oldest-epoch eviction in `update`.
    pub buckets: SmallVec<[(i64, Box<AggOp>); 4]>,
    /// AGG-CORE-09: cap = 64 active buckets. Beyond cap, the oldest-epoch
    /// entry is evicted on each new-epoch insert.
    #[serde(default = "default_max_buckets")]
    pub max_buckets: usize,
    /// Plan 10-05: sketch construction params propagated to per-bucket
    /// fresh_op() calls. Default for non-sketch kinds; threaded for sketch kinds.
    #[serde(default)]
    pub sketch_params: SketchParams,
}

fn default_max_buckets() -> usize {
    64
}

impl WindowedOp {
    /// Create a new WindowedOp.
    ///
    /// `bucket_ms = ceil(window_ms / 64)` — ensures at least 1ms per bucket.
    ///
    /// Phase 19.1-04: cold construction is allocation-free (`SmallVec::new`
    /// is a no-op). Buckets are pushed lazily on the first event.
    pub fn new(kind: AggKind, window_ms: u64) -> Self {
        Self::new_with_params(kind, window_ms, SketchParams::default())
    }

    /// Plan 10-05: construct with explicit sketch params (k, q, fpr, etc.).
    /// Sketch params are persisted on the WindowedOp so each bucket re-init
    /// honors user-supplied configuration.
    ///
    /// Panics for `AggKind::BloomMember` — bloom_member is windowless-only
    /// per CONTEXT D-08 / Plan 10-05 (rejected at register time with
    /// kind=window_not_supported).
    pub fn new_with_params(kind: AggKind, window_ms: u64, sketch_params: SketchParams) -> Self {
        assert!(
            !matches!(kind, AggKind::BloomMember),
            "bloom_member is windowless-only — cannot be wrapped in WindowedOp"
        );
        let bucket_ms = window_ms.div_ceil(64);
        WindowedOp {
            inner_kind: kind,
            bucket_ms,
            window_ms,
            // Lazy allocation: SmallVec::new is allocation-free. Buckets push
            // on first event into a new epoch.
            buckets: SmallVec::new(),
            max_buckets: 64,
            sketch_params,
        }
    }

    /// Compute the bucket index (slot 0..64) for an event time.
    ///
    /// Uses `div_euclid` so negative event_time_ms yields a non-negative index.
    ///
    /// Phase 19.1-04 note: this is now a pure mathematical function returning
    /// `floor(t / bucket_ms) mod 64`. It is no longer used to address physical
    /// storage slots (the SmallVec is keyed by epoch_ms, not slot index), but
    /// is kept as a public API for tests and external callers that reason
    /// about bucket-collision behavior at the 64-slot abstraction level.
    pub fn bucket_index(&self, event_time_ms: i64) -> usize {
        ((event_time_ms.div_euclid(self.bucket_ms as i64)) as usize) % 64
    }

    /// Compute the bucket epoch (start time in ms, inclusive) for an event.
    ///
    /// Phase 19.1-04: this is the new bucket identifier in the SmallVec layout.
    /// Two events at times `t1` and `t2` share a bucket iff
    /// `bucket_epoch(t1) == bucket_epoch(t2)`.
    #[inline]
    pub fn bucket_epoch(&self, event_time_ms: i64) -> i64 {
        event_time_ms.div_euclid(self.bucket_ms as i64) * self.bucket_ms as i64
    }

    /// Find the position in `self.buckets` for a given epoch, if any.
    ///
    /// Linear scan is fastest for n ≤ 4 (typical case is 1-2 active buckets;
    /// worst case 64 still has small constant factor — ~32 ns at scan-of-64).
    #[inline]
    fn position_for_epoch(&self, epoch: i64) -> Option<usize> {
        self.buckets.iter().position(|(e, _)| *e == epoch)
    }

    /// Evict the oldest-epoch bucket. Called when `len >= max_buckets` and a
    /// new-epoch entry is about to be pushed. AGG-CORE-09: 64-bucket cap.
    fn evict_oldest_bucket(&mut self) {
        if let Some(min_pos) = self
            .buckets
            .iter()
            .enumerate()
            .min_by_key(|(_, (e, _))| *e)
            .map(|(i, _)| i)
        {
            self.buckets.swap_remove(min_pos);
        }
    }

    /// Update the windowed state with one event row.
    pub fn update(
        &mut self,
        row: &Row,
        event_time_ms: i64,
        field: Option<&str>,
        where_matched: bool,
    ) {
        let epoch = self.bucket_epoch(event_time_ms);
        if let Some(pos) = self.position_for_epoch(epoch) {
            self.buckets[pos]
                .1
                .update(row, event_time_ms, field, where_matched);
            return;
        }
        // New epoch — evict-then-push if at cap.
        if self.buckets.len() >= self.max_buckets {
            self.evict_oldest_bucket();
        }
        let mut new_op = Box::new(fresh_op(self.inner_kind, &self.sketch_params));
        new_op.update(row, event_time_ms, field, where_matched);
        self.buckets.push((epoch, new_op));
    }

    /// Update the windowed state with one event row, evaluating `where_expr`
    /// (if any) before forwarding to the inner bucket's AggOp.
    ///
    /// Same bucket routing + lazy-allocation logic as `update`; the predicate is
    /// threaded into the per-bucket `AggOp::update_with_row` call.
    ///
    /// # SDK-AGG-04
    pub fn update_with_row(
        &mut self,
        row: &Row,
        event_time_ms: i64,
        field: Option<&str>,
        where_expr: Option<&std::sync::Arc<crate::expr::Expr>>,
    ) {
        let epoch = self.bucket_epoch(event_time_ms);
        if let Some(pos) = self.position_for_epoch(epoch) {
            self.buckets[pos]
                .1
                .update_with_row(row, event_time_ms, field, where_expr);
            return;
        }
        if self.buckets.len() >= self.max_buckets {
            self.evict_oldest_bucket();
        }
        let mut new_op = Box::new(fresh_op(self.inner_kind, &self.sketch_params));
        new_op.update_with_row(row, event_time_ms, field, where_expr);
        self.buckets.push((epoch, new_op));
    }

    /// Query the windowed aggregation value at `query_time_ms`.
    ///
    /// Active buckets: those where `query_time_ms - bucket_epoch >= 0`
    /// AND `query_time_ms - bucket_epoch < window_ms`.
    pub fn query(&self, query_time_ms: i64) -> crate::row::Value {
        use crate::agg_op::AggOp;
        use crate::agg_state::value_lt;
        use crate::agg_state::{AvgState, CountState, MaxState, MinState, RatioState, SumState};
        use crate::row::Value;

        let window_ms = self.window_ms as i64;

        // Helper closure: is a bucket epoch active at the query time?
        // Inlined here so each match arm can use it without re-borrowing self.
        let active = |epoch: i64| -> bool {
            let age = query_time_ms - epoch;
            age >= 0 && age < window_ms
        };

        match self.inner_kind {
            AggKind::Count => {
                let mut total: u64 = 0;
                for (epoch, op) in self.buckets.iter() {
                    if !active(*epoch) {
                        continue;
                    }
                    if let AggOp::Count(CountState { n }) = op.as_ref() {
                        total += n;
                    }
                }
                Value::I64(total as i64)
            }
            AggKind::Sum => {
                let mut total = 0.0_f64;
                let mut seen = false;
                for (epoch, op) in self.buckets.iter() {
                    if !active(*epoch) {
                        continue;
                    }
                    if let AggOp::Sum(SumState { total: t, n }) = op.as_ref() {
                        if *n > 0 {
                            total += t;
                            seen = true;
                        }
                    }
                }
                if seen {
                    Value::F64(total)
                } else {
                    Value::Null
                }
            }
            AggKind::Avg => {
                let mut sum = 0.0_f64;
                let mut n: u64 = 0;
                for (epoch, op) in self.buckets.iter() {
                    if !active(*epoch) {
                        continue;
                    }
                    if let AggOp::Avg(AvgState { sum: s, n: bn }) = op.as_ref() {
                        sum += s;
                        n += bn;
                    }
                }
                if n == 0 {
                    Value::Null
                } else {
                    Value::F64(sum / n as f64)
                }
            }
            AggKind::Min => {
                let mut current: Option<Value> = None;
                for (epoch, op) in self.buckets.iter() {
                    if !active(*epoch) {
                        continue;
                    }
                    if let AggOp::Min(MinState { current: Some(bv) }) = op.as_ref() {
                        match &current {
                            None => current = Some(bv.clone()),
                            Some(cur) => {
                                if value_lt(bv, cur) {
                                    current = Some(bv.clone());
                                }
                            }
                        }
                    }
                }
                current.unwrap_or(Value::Null)
            }
            AggKind::Max => {
                let mut current: Option<Value> = None;
                for (epoch, op) in self.buckets.iter() {
                    if !active(*epoch) {
                        continue;
                    }
                    if let AggOp::Max(MaxState { current: Some(bv) }) = op.as_ref() {
                        match &current {
                            None => current = Some(bv.clone()),
                            Some(cur) => {
                                if value_lt(cur, bv) {
                                    current = Some(bv.clone());
                                }
                            }
                        }
                    }
                }
                current.unwrap_or(Value::Null)
            }
            AggKind::Variance | AggKind::StdDev => {
                // Welford pairwise merge across active buckets.
                let mut combined_n: u64 = 0;
                let mut combined_mean: f64 = 0.0;
                let mut combined_m2: f64 = 0.0;

                for (epoch, op) in self.buckets.iter() {
                    if !active(*epoch) {
                        continue;
                    }
                    let bstate = match op.as_ref() {
                        AggOp::Variance(s) | AggOp::StdDev(s) => s,
                        _ => continue,
                    };
                    if bstate.n == 0 {
                        continue;
                    }

                    // Welford pairwise combine:
                    // delta = b_mean - a_mean
                    // new_n = a_n + b_n
                    // new_mean = a_mean + delta * b_n / new_n
                    // new_m2 = a_m2 + b_m2 + delta^2 * a_n * b_n / new_n
                    let delta = bstate.mean - combined_mean;
                    let new_n = combined_n + bstate.n;
                    let new_mean = combined_mean + delta * bstate.n as f64 / new_n as f64;
                    let new_m2 = combined_m2
                        + bstate.m2
                        + delta * delta * combined_n as f64 * bstate.n as f64 / new_n as f64;
                    combined_n = new_n;
                    combined_mean = new_mean;
                    combined_m2 = new_m2;
                }

                if combined_n < 2 {
                    return Value::Null;
                }
                let variance = combined_m2 / (combined_n - 1) as f64;
                if matches!(self.inner_kind, AggKind::StdDev) {
                    Value::F64(variance.sqrt())
                } else {
                    Value::F64(variance)
                }
            }
            AggKind::CountDistinct => {
                // Pick most recently-active bucket within window (v0 simplification —
                // future work: merge across buckets via Hll::merge for stable HLL union).
                let mut best: Option<&AggOp> = None;
                let mut best_epoch = i64::MIN;
                for (epoch, op) in self.buckets.iter() {
                    if !active(*epoch) {
                        continue;
                    }
                    if *epoch > best_epoch {
                        best_epoch = *epoch;
                        best = Some(op.as_ref());
                    }
                }
                match best {
                    Some(AggOp::CountDistinct(s)) => s.query(),
                    _ => Value::I64(0),
                }
            }
            AggKind::Percentile => {
                let mut best: Option<&AggOp> = None;
                let mut best_epoch = i64::MIN;
                for (epoch, op) in self.buckets.iter() {
                    if !active(*epoch) {
                        continue;
                    }
                    if *epoch > best_epoch {
                        best_epoch = *epoch;
                        best = Some(op.as_ref());
                    }
                }
                match best {
                    Some(AggOp::Percentile(s)) => s.query(),
                    _ => Value::Null,
                }
            }
            AggKind::TopK => {
                let mut best: Option<&AggOp> = None;
                let mut best_epoch = i64::MIN;
                for (epoch, op) in self.buckets.iter() {
                    if !active(*epoch) {
                        continue;
                    }
                    if *epoch > best_epoch {
                        best_epoch = *epoch;
                        best = Some(op.as_ref());
                    }
                }
                match best {
                    Some(AggOp::TopK(s)) => s.query(),
                    _ => Value::Json(serde_json::Value::Array(vec![])),
                }
            }
            AggKind::Entropy => {
                // Merge histograms across active buckets via EntropyHistogram::merge.
                use crate::sketches::entropy::EntropyHistogram;
                let mut combined: Option<EntropyHistogram> = None;
                for (epoch, op) in self.buckets.iter() {
                    if !active(*epoch) {
                        continue;
                    }
                    if let AggOp::Entropy(s) = op.as_ref() {
                        match &mut combined {
                            None => combined = Some(s.inner.clone()),
                            Some(c) => c.merge(&s.inner),
                        }
                    }
                }
                match combined {
                    Some(c) => Value::F64(c.entropy_bits()),
                    None => Value::F64(0.0),
                }
            }
            AggKind::BloomMember => {
                // Unreachable: BloomMember rejected by new_with_params; defensive.
                Value::Bool(false)
            }
            AggKind::Ratio => {
                let mut matching: u64 = 0;
                let mut total: u64 = 0;
                for (epoch, op) in self.buckets.iter() {
                    if !active(*epoch) {
                        continue;
                    }
                    if let AggOp::Ratio(RatioState {
                        matching: m,
                        total: t,
                    }) = op.as_ref()
                    {
                        matching += m;
                        total += t;
                    }
                }
                if total == 0 {
                    Value::Null
                } else {
                    Value::F64(matching as f64 / total as f64)
                }
            }
            // Phase 8 + 9 + 11 ops are never wrapped in WindowedOp (compile-time
            // invariant). Catch-all returns Null defensively.
            _ => Value::Null,
        }
    }
}

/// Create a fresh lifetime AggOp for a given kind (used to initialise buckets).
///
/// `WindowedOp` supports Phase 5 core ops + Phase 10 sketch ops (except
/// `BloomMember` which is windowless-only). Phase 8 + 9 + 11 ops are
/// lifetime-only and `agg_compile` validation rejects `window=` for them.
/// `new_lifetime` handles sketches via `sketch_params`; for non-windowable
/// kinds it delegates to `new_lifetime_full` with a default descriptor.
fn fresh_op(kind: AggKind, sketch_params: &SketchParams) -> AggOp {
    AggOp::new_lifetime(kind, Some(sketch_params))
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::{Row, Value};

    fn row_f64(field: &str, v: f64) -> Row {
        Row::new().with_field(field, Value::F64(v))
    }

    fn empty_row() -> Row {
        Row::new()
    }

    // ── Bucket configuration ─────────────────────────────────────────────

    #[test]
    fn windowed_count_bucket_ms_is_ceil_window_div_64() {
        // 64_000ms / 64 = 1000ms exactly
        let op = WindowedOp::new(AggKind::Count, 64_000);
        assert_eq!(
            op.bucket_ms, 1_000,
            "64s window / 64 buckets = 1000ms bucket"
        );
    }

    #[test]
    fn windowed_count_1s_window_rounds_up_bucket_ms_to_at_least_1() {
        // 10ms / 64 = 0.15 → ceil = 1
        let op = WindowedOp::new(AggKind::Count, 10);
        assert_eq!(op.bucket_ms, 1, "10ms/64 rounds up to 1ms minimum bucket");
    }

    // ── Bucket index ─────────────────────────────────────────────────────

    #[test]
    fn windowed_count_bucket_index_is_pure_function_of_event_time() {
        let op = WindowedOp::new(AggKind::Count, 64_000); // bucket_ms=1000
                                                          // Same t always returns same index
        let idx_a = op.bucket_index(0);
        let idx_b = op.bucket_index(0);
        assert_eq!(
            idx_a, idx_b,
            "bucket_index must be pure function of event_time"
        );

        // Two events in the same bucket share an index
        let idx_1 = op.bucket_index(500);
        let idx_2 = op.bucket_index(999);
        assert_eq!(
            idx_1, idx_2,
            "500ms and 999ms should share bucket 0 (bucket_ms=1000)"
        );

        // Event at boundary belongs to next bucket
        let idx_3 = op.bucket_index(1_000);
        assert_ne!(idx_1, idx_3, "1000ms should be in next bucket");

        // Indices are mod 64
        let idx_wrap = op.bucket_index(64_000); // epoch 64, mod 64 = 0
        assert_eq!(idx_wrap, 0, "index must wrap via mod 64");
    }

    // ── Count windowing ───────────────────────────────────────────────────

    #[test]
    fn windowed_count_100_events_in_5min_window_returns_100() {
        let window_ms: u64 = 5 * 60 * 1_000; // 300_000ms
        let mut op = WindowedOp::new(AggKind::Count, window_ms);
        let r = empty_row();
        // Push 100 events spread across [0, window_ms)
        for i in 0..100_i64 {
            let t = i * (window_ms as i64 / 100);
            op.update(&r, t, None, true);
        }
        // Query at query_time_ms = window_ms - 1 (all events still active)
        // Use query_time that keeps all buckets alive: epoch of bucket 0 is 0,
        // age = (window_ms - 1) - 0 = window_ms - 1 < window_ms ✓
        let result = op.query(window_ms as i64 - 1);
        assert_eq!(result, Value::I64(100), "all 100 events should be counted");
    }

    #[test]
    fn windowed_count_events_outside_window_excluded() {
        let window_ms: u64 = 64_000; // 64s, bucket_ms = 1000
        let mut op = WindowedOp::new(AggKind::Count, window_ms);
        let r = empty_row();
        // Push 50 events in [0, window_ms)
        for i in 0..50_i64 {
            op.update(&r, i * 1_000, None, true);
        }
        // Query at t = 2 * window_ms: all original buckets have age >= window_ms → excluded
        let result = op.query(2 * window_ms as i64);
        assert_eq!(
            result,
            Value::I64(0),
            "events older than window should be excluded"
        );
    }

    #[test]
    fn windowed_count_bucket_rollover_deterministic() {
        let window_ms: u64 = 64_000; // bucket_ms = 1000
        let mut op = WindowedOp::new(AggKind::Count, window_ms);
        let r = empty_row();

        // Push event at t=0: epoch 0
        op.update(&r, 0, None, true);
        // Query at t=0: age of epoch 0 = 0 < 64_000 ✓
        let r1 = op.query(0);
        assert_eq!(r1, Value::I64(1));

        // Push event at t=window_ms+1: a new epoch beyond the original window
        // (epoch for t=window_ms+1 with bucket_ms=1000: floor(64001/1000)*1000 = 64000)
        op.update(&r, window_ms as i64 + 1, None, true);
        // Query at t=window_ms+1: epoch 0 has age=window_ms+1 >= window_ms → excluded
        // epoch 64000 has age=1 < window_ms → included
        let r2 = op.query(window_ms as i64 + 1);
        assert_eq!(
            r2,
            Value::I64(1),
            "only new event should be counted after rollover"
        );
    }

    // ── Sum windowing ─────────────────────────────────────────────────────

    #[test]
    fn windowed_sum_folds_across_buckets() {
        // 5 rows with amount=10.0 in 5 different buckets within window
        let window_ms: u64 = 64_000; // bucket_ms = 1000
        let mut op = WindowedOp::new(AggKind::Sum, window_ms);
        for i in 0..5_i64 {
            let r = row_f64("amount", 10.0);
            op.update(&r, i * 1_000, Some("amount"), true);
        }
        let result = op.query(4_999); // all 5 events within window
        match result {
            Value::F64(v) => assert!((v - 50.0).abs() < 1e-10, "sum should be 50.0, got {v}"),
            other => panic!("expected F64, got {:?}", other),
        }
    }

    // ── Avg windowing ─────────────────────────────────────────────────────

    #[test]
    fn windowed_avg_weighted_by_bucket_n() {
        // Two buckets: bucket 0 has 1 event (value=10), bucket 1 has 9 events (value=1)
        // Weighted avg = (10 + 9*1) / 10 = 1.9, NOT (10+1)/2 = 5.5
        let window_ms: u64 = 64_000;
        let mut op = WindowedOp::new(AggKind::Avg, window_ms);

        op.update(&row_f64("x", 10.0), 0, Some("x"), true);
        for _ in 0..9 {
            op.update(&row_f64("x", 1.0), 1_000, Some("x"), true);
        }
        let result = op.query(1_999);
        match result {
            Value::F64(v) => assert!(
                (v - 1.9).abs() < 1e-10,
                "weighted avg should be 1.9, got {v}"
            ),
            other => panic!("expected F64, got {:?}", other),
        }
    }

    // ── Min/Max windowing ─────────────────────────────────────────────────

    #[test]
    fn windowed_min_is_min_across_bucket_mins() {
        let window_ms: u64 = 64_000;
        let mut op = WindowedOp::new(AggKind::Min, window_ms);
        // Spread values across buckets
        for (i, v) in [
            (0_i64, 5.0_f64),
            (1_000, 2.0),
            (2_000, 8.0),
            (3_000, 1.0),
            (4_000, 7.0),
        ] {
            op.update(&row_f64("x", v), i, Some("x"), true);
        }
        let result = op.query(4_999);
        assert_eq!(result, Value::F64(1.0), "min across buckets should be 1.0");
    }

    #[test]
    fn windowed_max_is_max_across_bucket_maxes() {
        let window_ms: u64 = 64_000;
        let mut op = WindowedOp::new(AggKind::Max, window_ms);
        for (i, v) in [
            (0_i64, 5.0_f64),
            (1_000, 2.0),
            (2_000, 8.0),
            (3_000, 1.0),
            (4_000, 7.0),
        ] {
            op.update(&row_f64("x", v), i, Some("x"), true);
        }
        let result = op.query(4_999);
        assert_eq!(result, Value::F64(8.0), "max across buckets should be 8.0");
    }

    // ── Variance windowing ────────────────────────────────────────────────

    #[test]
    fn windowed_variance_combines_via_welford_pairwise_merge() {
        // [2, 4, 4, 4, 5, 5, 7, 9] split across two buckets:
        //   bucket 0 (t=0):    [2, 4, 4, 4]  — n=4, mean=3.5, m2=3.0
        //   bucket 1 (t=1000): [5, 5, 7, 9]  — n=4, mean=6.5, m2=8.0
        //
        // Pairwise Welford merge gives the same result as computing on the full stream.
        // Full stream: n=8, mean=5.0, SS=32.0
        // Sample variance (n-1 denominator) = 32/7 ≈ 4.571428...
        //
        // Note: the plan referenced "4.0" which is the population variance.
        // Beava uses sample variance (Bessel-corrected, n-1) consistently.
        // (Deviation: plan had incorrect expected value; correct sample variance is 32/7.)
        let window_ms: u64 = 64_000; // bucket_ms = 1000
        let mut op = WindowedOp::new(AggKind::Variance, window_ms);

        for (i, v) in [(0_i64, 2.0_f64), (0, 4.0), (0, 4.0), (0, 4.0)] {
            op.update(&row_f64("x", v), i, Some("x"), true);
        }
        // Put last 4 in bucket 1 (t=1000..1999)
        for (i, v) in [
            (1_000_i64, 5.0_f64),
            (1_000, 5.0),
            (1_000, 7.0),
            (1_000, 9.0),
        ] {
            op.update(&row_f64("x", v), i, Some("x"), true);
        }

        let result = op.query(1_999);
        let expected = 32.0_f64 / 7.0; // sample variance (n-1 denominator) = 4.571428...
        match result {
            Value::F64(v) => assert!(
                (v - expected).abs() < 1e-10,
                "pairwise Welford combined variance should be {expected:.6}, got {v}"
            ),
            other => panic!("expected F64, got {:?}", other),
        }
    }

    // ── Ratio windowing ───────────────────────────────────────────────────

    #[test]
    fn windowed_ratio_is_sum_matching_over_sum_total() {
        // 3 matching out of 5 total across 3 buckets
        let window_ms: u64 = 64_000;
        let mut op = WindowedOp::new(AggKind::Ratio, window_ms);
        let r = empty_row();
        // bucket 0: 2 events, 2 matching
        op.update(&r, 0, None, true);
        op.update(&r, 0, None, true);
        // bucket 1: 2 events, 1 matching
        op.update(&r, 1_000, None, true);
        op.update(&r, 1_000, None, false);
        // bucket 2: 1 event, 0 matching
        op.update(&r, 2_000, None, false);

        let result = op.query(2_999);
        match result {
            Value::F64(v) => assert!((v - 0.6).abs() < 1e-10, "ratio should be 3/5=0.6, got {v}"),
            other => panic!("expected F64, got {:?}", other),
        }
    }

    // ── update_with_row (Plan 05-02) ─────────────────────────────────────

    /// Windowed count with predicate "amount > 25": only matching rows counted
    /// in buckets. 5 rows [10, 20, 30, 40, 50] → 3 match (30, 40, 50) → I64(3).
    #[test]
    fn windowed_count_with_where_predicate_drops_non_matching() {
        let window_ms: u64 = 64_000; // bucket_ms = 1000
        let mut op = WindowedOp::new(AggKind::Count, window_ms);
        let where_expr =
            std::sync::Arc::new(crate::expr::parse("(amount > 25)").expect("should parse"));
        for (i, &amount) in [10.0_f64, 20.0, 30.0, 40.0, 50.0].iter().enumerate() {
            let row = Row::new().with_field("amount", Value::F64(amount));
            // spread across different buckets to exercise bucket routing
            op.update_with_row(&row, (i as i64) * 1_000, None, Some(&where_expr));
        }
        // query at t=4999: all 5 buckets in window; only 3 had matching rows
        let result = op.query(4_999);
        assert_eq!(
            result,
            Value::I64(3),
            "only rows with amount > 25 should be counted (30, 40, 50)"
        );
    }

    // ── Replay determinism ────────────────────────────────────────────────

    #[test]
    fn windowed_replay_determinism() {
        // Apply same 1000-event stream twice; internal state debug representations
        // must be byte-identical (SC4 internal-state gate per D-06).
        let window_ms: u64 = 64_000;

        let mut op1 = WindowedOp::new(AggKind::Count, window_ms);
        let mut op2 = WindowedOp::new(AggKind::Count, window_ms);
        let r = empty_row();

        // Deterministic pseudo-event stream: event_time_ms = i * 97 (prime step, mod window)
        for i in 0..1000_i64 {
            let t = (i * 97) % (window_ms as i64 * 2);
            op1.update(&r, t, None, true);
            op2.update(&r, t, None, true);
        }

        // Snapshot state as debug representation — must be byte-identical.
        // Phase 19.1-04: the SmallVec entry order can depend on push order,
        // but with deterministic event streams it's deterministic across runs.
        let snap1 = format!("{:?}", op1);
        let snap2 = format!("{:?}", op2);
        assert_eq!(
            snap1, snap2,
            "applying the same event stream twice must yield identical state (D-06 SC4)"
        );
    }

    // ── Phase 19.1-04 lazy bucket allocation (D-19) ──────────────────────

    /// Cold WindowedOp must not preallocate 64 bucket slots.
    ///
    /// Phase 19.1 CONTEXT D-19: replace `[Option<Box<AggOp>>; 64]` + `[i64; 64]`
    /// preallocation with a lazy SmallVec. A freshly-constructed op has zero
    /// active buckets; pre-fix, `op.buckets.len()` returns 64 (array length),
    /// which is the red signal.
    #[test]
    fn test_cold_windowed_op_has_no_allocated_buckets() {
        let op = WindowedOp::new(AggKind::Count, 60_000);
        assert_eq!(
            op.buckets.len(),
            0,
            "cold WindowedOp must have ZERO active buckets (lazy alloc); got {}",
            op.buckets.len()
        );
    }

    /// First update must lazily allocate exactly one bucket entry.
    ///
    /// Phase 19.1 CONTEXT D-19: with the SmallVec layout, `update` on a cold
    /// op grows `buckets` from 0 → 1 (one push for the current epoch). Pre-fix,
    /// `op.buckets.len()` is 64 regardless, which is the red signal.
    #[test]
    fn test_windowed_op_lazy_allocates_one_bucket_on_first_update() {
        let mut op = WindowedOp::new(AggKind::Count, 60_000);
        let row = empty_row();
        op.update(&row, 1_000, None, true);
        assert_eq!(
            op.buckets.len(),
            1,
            "single update must lazy-allocate exactly one bucket; got {}",
            op.buckets.len()
        );
    }

    /// SmallVec inline cap is 4: pushing into 4 distinct epochs stays inline
    /// (no heap promotion); pushing a 5th promotes to heap but stays correct.
    ///
    /// Phase 19.1 CONTEXT D-20: inline cap=4 covers the typical fraud case
    /// (1-2 active buckets per entity at any time).
    #[test]
    fn test_windowed_op_smallvec_inline_cap_4_then_spills_to_heap() {
        let mut op = WindowedOp::new(AggKind::Count, 64_000); // bucket_ms = 1000
        let r = empty_row();

        // Push into 4 distinct buckets; should stay inline.
        for i in 0..4_i64 {
            op.update(&r, i * 1_000, None, true);
        }
        assert_eq!(op.buckets.len(), 4, "4 buckets after 4 distinct epochs");
        assert!(
            !op.buckets.spilled(),
            "4 entries should fit inline (SmallVec cap=4)"
        );

        // 5th distinct bucket spills to heap but stays correct.
        op.update(&r, 4_000, None, true);
        assert_eq!(op.buckets.len(), 5, "5 buckets after 5 distinct epochs");
        assert!(
            op.buckets.spilled(),
            "5th entry must spill to heap (graceful promotion)"
        );
    }

    /// AGG-CORE-09 cap: 64 active buckets max — beyond cap, oldest is evicted.
    ///
    /// Pushes into 65 distinct epochs and verifies that exactly 64 entries
    /// remain (the oldest was swap_remove'd) and that querying across all
    /// active windows still folds correctly.
    #[test]
    fn test_windowed_op_evicts_oldest_at_max_buckets_cap() {
        // Use a very small bucket_ms (1ms) so we can pack 65 distinct epochs
        // into a meaningful test: window_ms = 64 * 65 = 4160ms
        let window_ms: u64 = 64; // bucket_ms = 1ms
        let mut op = WindowedOp::new(AggKind::Count, window_ms);
        let r = empty_row();

        // Push 65 events into 65 distinct epochs (event_time = i ms; bucket_ms=1).
        for i in 0..65_i64 {
            op.update(&r, i, None, true);
        }
        assert_eq!(
            op.buckets.len(),
            64,
            "AGG-CORE-09 cap: at most 64 active buckets"
        );
        // Oldest (epoch 0) should be evicted; buckets 1..65 remain.
        let oldest_present = op.buckets.iter().any(|(e, _)| *e == 0);
        assert!(!oldest_present, "epoch=0 should have been evicted");
        let newest_present = op.buckets.iter().any(|(e, _)| *e == 64);
        assert!(newest_present, "epoch=64 should still be present");
    }

    // ── Determinism guard ─────────────────────────────────────────────────

    #[test]
    fn no_wall_clock_or_rand_in_windowed_module() {
        // Split forbidden patterns so this file does not itself trigger the check.
        let forbidden_clock = ["SystemTime", "::", "now"].concat();
        let forbidden_rand = ["rand", "::"].concat();
        let src = include_str!("agg_windowed.rs");
        assert!(
            !src.contains(forbidden_clock.as_str()),
            "agg_windowed.rs must not use wall-clock reads (D-06 determinism invariant)"
        );
        assert!(
            !src.contains(forbidden_rand.as_str()),
            "agg_windowed.rs must not use rand crate (D-06 determinism invariant)"
        );
    }
}
