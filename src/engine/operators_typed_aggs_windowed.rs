//! Phase 59.7 Wave 1 (TPC-PERF-11 extension / TPC-CORR-07 extension) —
//! typed windowed aggregation operators + packed time-bucketed ring buffers.
//!
//! See `.planning/phases/59.7-typed-windowed-cascade/59.7-CONTEXT.md` Gap 1
//! for the full design contract. Reference impl is
//! [`crate::engine::window::RingBuffer<T>`] — the semantics of
//! [`TypedRingBufferI64`] / [`TypedRingBufferF64`] / [`TypedRingBufferAvg`]
//! are a line-for-line port of that generic struct, monomorphized per
//! column type (per D-C1) to avoid codegen blow-up.
//!
//! # Why packed per-entity ring buffers (not generic <T>)
//!
//! The Value-path `RingBuffer<T>` is generic. Typed ops run on hot paths
//! that cannot afford the codegen fanout that T=i64 / T=f64 / T=(f64,i64)
//! would create when combined with the 7+ windowed-op kinds the full
//! Wave-1→Wave-2 matrix ships. Monomorphized structs here also keep
//! per-variant heap layout tight (Vec<i64> is 24 bytes + n*8; Vec<(f64,i64)>
//! is 24 bytes + n*16) so per-entity memory is predictable.
//!
//! # State location (Wave 1 scope)
//!
//! Per-entity ring buffers live out-of-band on
//! [`crate::shard::Shard::entity_ringbuffers_typed`] — an `AHashMap<(stream,
//! entity_key, op_index), TypedRingBufferEnum>` — rather than inside the
//! packed state `Row`. This mirrors the SideBand pattern from Phase 59.6
//! Wave 6 D-C1 and avoids packed-row bloat for the common case of ≥ 20
//! windowed ops × ≥ 60 buckets (~10 KB/entity if inlined).
//!
//! # Parity contract (TPC-CORR-07 extension)
//!
//! Every typed windowed op MUST produce a byte-identical `FeatureValue`
//! stream to its Value-path sibling (`CountOp` / `SumOp` / `AvgOp`) on
//! the same event stream at every event-time checkpoint. The integration
//! harness `tests/typed_windowed_aggregation_parity.rs` drives 100K
//! events over a 30s event-time range with `window=5s, bucket=1s` (so
//! events expire mid-stream) and diffs at 20 checkpoints. Wave 1 flips
//! the Count/Sum×2/Avg tests GREEN; Wave 2 flips the Min/Max/Last/First
//! tests GREEN.

use crate::engine::event_time::DropReason;
use crate::engine::operators_typed::TypedAggOp;
use crate::engine::schema::{RegisteredSchema, Row};
use crate::types::FeatureValue;
use std::time::{Duration, SystemTime};

// ---------------------------------------------------------------------------
// TypedRingBufferI64 — port of RingBuffer<i64> semantics, monomorphized.
// ---------------------------------------------------------------------------

/// Packed time-bucketed ring buffer over `i64`. Semantics mirror
/// [`crate::engine::window::RingBuffer<i64>`] exactly — see that module's
/// doc-comment for the full event-time routing / expiry / late-drop
/// contract. This variant is intentionally duplicated (not generic) per
/// D-C1 to keep the typed hot path free of monomorphization fanout.
#[derive(Debug, Clone)]
pub struct TypedRingBufferI64 {
    buckets: Vec<i64>,
    head: u32,
    bucket_duration: Duration,
    #[allow(dead_code)]
    window_duration: Duration,
    current_bucket_start: Option<SystemTime>,
    /// Last drop reason from [`Self::bucket_index_for`] (OBS-01 semantics,
    /// reset on each success). Observability-only; not serialized.
    pub last_drop: Option<DropReason>,
}

impl TypedRingBufferI64 {
    pub fn new(window_duration: Duration, bucket_duration: Duration) -> Self {
        let window_secs = window_duration.as_secs_f64();
        let bucket_secs = bucket_duration.as_secs_f64();
        let num_buckets = (window_secs / bucket_secs).ceil() as usize;
        Self {
            buckets: vec![0i64; num_buckets],
            head: 0,
            bucket_duration,
            window_duration,
            current_bucket_start: None,
            last_drop: None,
        }
    }

    pub fn num_buckets(&self) -> usize {
        self.buckets.len()
    }

    pub fn take_last_drop(&mut self) -> Option<DropReason> {
        self.last_drop.take()
    }

    pub fn sum_all(&self) -> i64 {
        self.buckets.iter().copied().sum()
    }

    /// Byte footprint of this ring buffer (heap + struct). Used for
    /// observability / `estimated_bytes` wiring.
    pub fn allocated_bytes(&self) -> usize {
        self.buckets.capacity() * std::mem::size_of::<i64>() + std::mem::size_of::<Self>()
    }

    pub fn update_at_event_time<F: FnOnce(&mut i64)>(&mut self, f: F, event_time: SystemTime) {
        if let Some(idx) = self.bucket_index_for(event_time) {
            f(&mut self.buckets[idx]);
        }
    }

    pub fn advance_to(&mut self, now: SystemTime) -> usize {
        let start = match self.current_bucket_start {
            Some(s) => s,
            None => {
                let aligned = self.bucket_start_for(now);
                self.current_bucket_start = Some(aligned);
                self.head = 0;
                return 0;
            }
        };
        let elapsed = now.duration_since(start).unwrap_or(Duration::ZERO);
        let bucket_secs = self.bucket_duration.as_secs_f64();
        let buckets_to_advance = (elapsed.as_secs_f64() / bucket_secs) as usize;
        if buckets_to_advance == 0 {
            return self.head as usize;
        }
        let num_buckets = self.buckets.len();
        if buckets_to_advance >= num_buckets {
            for bucket in self.buckets.iter_mut() {
                *bucket = 0;
            }
            self.head = 0;
        } else {
            for i in 1..=buckets_to_advance {
                let idx = (self.head as usize + i) % num_buckets;
                self.buckets[idx] = 0;
            }
            self.head = ((self.head as usize + buckets_to_advance) % num_buckets) as u32;
        }
        self.current_bucket_start = Some(self.bucket_start_for(now));
        self.head as usize
    }

    fn bucket_start_for(&self, time: SystemTime) -> SystemTime {
        let since_epoch = time
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO);
        let bucket_secs = self.bucket_duration.as_secs();
        if bucket_secs == 0 {
            return SystemTime::UNIX_EPOCH + since_epoch;
        }
        let aligned_secs = (since_epoch.as_secs() / bucket_secs) * bucket_secs;
        SystemTime::UNIX_EPOCH + Duration::from_secs(aligned_secs)
    }

    fn bucket_index_for(&mut self, event_time: SystemTime) -> Option<usize> {
        use std::time::UNIX_EPOCH;
        if event_time < UNIX_EPOCH {
            self.last_drop = Some(DropReason::PreEpoch);
            return None;
        }
        let start = match self.current_bucket_start {
            Some(s) => s,
            None => {
                self.last_drop = None;
                self.advance_to(event_time);
                return Some(self.head as usize);
            }
        };
        if event_time >= start {
            self.last_drop = None;
            self.advance_to(event_time);
            return Some(self.head as usize);
        }
        let et_bucket_start = self.bucket_start_for(event_time);
        let delta = match start.duration_since(et_bucket_start) {
            Ok(d) => d,
            Err(_) => {
                self.last_drop = Some(DropReason::TooNew);
                return None;
            }
        };
        let bucket_secs = self.bucket_duration.as_secs();
        if bucket_secs == 0 {
            self.last_drop = None;
            return Some(self.head as usize);
        }
        let delta_buckets = (delta.as_secs() / bucket_secs) as usize;
        let num_buckets = self.buckets.len();
        if delta_buckets >= num_buckets {
            self.last_drop = Some(DropReason::TooOld);
            return None;
        }
        self.last_drop = None;
        let idx = (self.head as usize + num_buckets - delta_buckets) % num_buckets;
        Some(idx)
    }
}

// ---------------------------------------------------------------------------
// TypedRingBufferF64 — port of RingBuffer<f64> semantics, monomorphized.
// ---------------------------------------------------------------------------

/// Packed time-bucketed ring buffer over `f64`. See [`TypedRingBufferI64`]
/// for the semantics contract.
#[derive(Debug, Clone)]
pub struct TypedRingBufferF64 {
    buckets: Vec<f64>,
    head: u32,
    bucket_duration: Duration,
    #[allow(dead_code)]
    window_duration: Duration,
    current_bucket_start: Option<SystemTime>,
    pub last_drop: Option<DropReason>,
}

impl TypedRingBufferF64 {
    pub fn new(window_duration: Duration, bucket_duration: Duration) -> Self {
        let window_secs = window_duration.as_secs_f64();
        let bucket_secs = bucket_duration.as_secs_f64();
        let num_buckets = (window_secs / bucket_secs).ceil() as usize;
        Self {
            buckets: vec![0.0f64; num_buckets],
            head: 0,
            bucket_duration,
            window_duration,
            current_bucket_start: None,
            last_drop: None,
        }
    }

    pub fn num_buckets(&self) -> usize {
        self.buckets.len()
    }

    pub fn take_last_drop(&mut self) -> Option<DropReason> {
        self.last_drop.take()
    }

    pub fn sum_all(&self) -> f64 {
        self.buckets.iter().copied().sum()
    }

    pub fn allocated_bytes(&self) -> usize {
        self.buckets.capacity() * std::mem::size_of::<f64>() + std::mem::size_of::<Self>()
    }

    pub fn update_at_event_time<F: FnOnce(&mut f64)>(&mut self, f: F, event_time: SystemTime) {
        if let Some(idx) = self.bucket_index_for(event_time) {
            f(&mut self.buckets[idx]);
        }
    }

    pub fn advance_to(&mut self, now: SystemTime) -> usize {
        let start = match self.current_bucket_start {
            Some(s) => s,
            None => {
                let aligned = self.bucket_start_for(now);
                self.current_bucket_start = Some(aligned);
                self.head = 0;
                return 0;
            }
        };
        let elapsed = now.duration_since(start).unwrap_or(Duration::ZERO);
        let bucket_secs = self.bucket_duration.as_secs_f64();
        let buckets_to_advance = (elapsed.as_secs_f64() / bucket_secs) as usize;
        if buckets_to_advance == 0 {
            return self.head as usize;
        }
        let num_buckets = self.buckets.len();
        if buckets_to_advance >= num_buckets {
            for bucket in self.buckets.iter_mut() {
                *bucket = 0.0;
            }
            self.head = 0;
        } else {
            for i in 1..=buckets_to_advance {
                let idx = (self.head as usize + i) % num_buckets;
                self.buckets[idx] = 0.0;
            }
            self.head = ((self.head as usize + buckets_to_advance) % num_buckets) as u32;
        }
        self.current_bucket_start = Some(self.bucket_start_for(now));
        self.head as usize
    }

    fn bucket_start_for(&self, time: SystemTime) -> SystemTime {
        let since_epoch = time
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO);
        let bucket_secs = self.bucket_duration.as_secs();
        if bucket_secs == 0 {
            return SystemTime::UNIX_EPOCH + since_epoch;
        }
        let aligned_secs = (since_epoch.as_secs() / bucket_secs) * bucket_secs;
        SystemTime::UNIX_EPOCH + Duration::from_secs(aligned_secs)
    }

    fn bucket_index_for(&mut self, event_time: SystemTime) -> Option<usize> {
        use std::time::UNIX_EPOCH;
        if event_time < UNIX_EPOCH {
            self.last_drop = Some(DropReason::PreEpoch);
            return None;
        }
        let start = match self.current_bucket_start {
            Some(s) => s,
            None => {
                self.last_drop = None;
                self.advance_to(event_time);
                return Some(self.head as usize);
            }
        };
        if event_time >= start {
            self.last_drop = None;
            self.advance_to(event_time);
            return Some(self.head as usize);
        }
        let et_bucket_start = self.bucket_start_for(event_time);
        let delta = match start.duration_since(et_bucket_start) {
            Ok(d) => d,
            Err(_) => {
                self.last_drop = Some(DropReason::TooNew);
                return None;
            }
        };
        let bucket_secs = self.bucket_duration.as_secs();
        if bucket_secs == 0 {
            self.last_drop = None;
            return Some(self.head as usize);
        }
        let delta_buckets = (delta.as_secs() / bucket_secs) as usize;
        let num_buckets = self.buckets.len();
        if delta_buckets >= num_buckets {
            self.last_drop = Some(DropReason::TooOld);
            return None;
        }
        self.last_drop = None;
        let idx = (self.head as usize + num_buckets - delta_buckets) % num_buckets;
        Some(idx)
    }
}

// ---------------------------------------------------------------------------
// TypedRingBufferAvg — packed (sum: f64, count: i64) per bucket for Avg ops.
// ---------------------------------------------------------------------------

/// Packed time-bucketed ring buffer over `(f64, i64)` — used by
/// [`AvgOpTypedWindowedF64`] which needs both sum and count per bucket so
/// `sum_all() / count_all()` yields the windowed average with Value-path
/// semantics (Missing when `count == 0`).
#[derive(Debug, Clone)]
pub struct TypedRingBufferAvg {
    buckets: Vec<(f64, i64)>,
    head: u32,
    bucket_duration: Duration,
    #[allow(dead_code)]
    window_duration: Duration,
    current_bucket_start: Option<SystemTime>,
    pub last_drop: Option<DropReason>,
}

impl TypedRingBufferAvg {
    pub fn new(window_duration: Duration, bucket_duration: Duration) -> Self {
        let window_secs = window_duration.as_secs_f64();
        let bucket_secs = bucket_duration.as_secs_f64();
        let num_buckets = (window_secs / bucket_secs).ceil() as usize;
        Self {
            buckets: vec![(0.0f64, 0i64); num_buckets],
            head: 0,
            bucket_duration,
            window_duration,
            current_bucket_start: None,
            last_drop: None,
        }
    }

    pub fn num_buckets(&self) -> usize {
        self.buckets.len()
    }

    pub fn take_last_drop(&mut self) -> Option<DropReason> {
        self.last_drop.take()
    }

    /// Returns `(sum_total, count_total)` across all buckets in the window.
    pub fn sum_all(&self) -> (f64, i64) {
        let mut s = 0.0f64;
        let mut c = 0i64;
        for (bs, bc) in &self.buckets {
            s += *bs;
            c += *bc;
        }
        (s, c)
    }

    pub fn allocated_bytes(&self) -> usize {
        self.buckets.capacity() * std::mem::size_of::<(f64, i64)>() + std::mem::size_of::<Self>()
    }

    pub fn update_at_event_time<F: FnOnce(&mut (f64, i64))>(
        &mut self,
        f: F,
        event_time: SystemTime,
    ) {
        if let Some(idx) = self.bucket_index_for(event_time) {
            f(&mut self.buckets[idx]);
        }
    }

    pub fn advance_to(&mut self, now: SystemTime) -> usize {
        let start = match self.current_bucket_start {
            Some(s) => s,
            None => {
                let aligned = self.bucket_start_for(now);
                self.current_bucket_start = Some(aligned);
                self.head = 0;
                return 0;
            }
        };
        let elapsed = now.duration_since(start).unwrap_or(Duration::ZERO);
        let bucket_secs = self.bucket_duration.as_secs_f64();
        let buckets_to_advance = (elapsed.as_secs_f64() / bucket_secs) as usize;
        if buckets_to_advance == 0 {
            return self.head as usize;
        }
        let num_buckets = self.buckets.len();
        if buckets_to_advance >= num_buckets {
            for bucket in self.buckets.iter_mut() {
                *bucket = (0.0, 0);
            }
            self.head = 0;
        } else {
            for i in 1..=buckets_to_advance {
                let idx = (self.head as usize + i) % num_buckets;
                self.buckets[idx] = (0.0, 0);
            }
            self.head = ((self.head as usize + buckets_to_advance) % num_buckets) as u32;
        }
        self.current_bucket_start = Some(self.bucket_start_for(now));
        self.head as usize
    }

    fn bucket_start_for(&self, time: SystemTime) -> SystemTime {
        let since_epoch = time
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO);
        let bucket_secs = self.bucket_duration.as_secs();
        if bucket_secs == 0 {
            return SystemTime::UNIX_EPOCH + since_epoch;
        }
        let aligned_secs = (since_epoch.as_secs() / bucket_secs) * bucket_secs;
        SystemTime::UNIX_EPOCH + Duration::from_secs(aligned_secs)
    }

    fn bucket_index_for(&mut self, event_time: SystemTime) -> Option<usize> {
        use std::time::UNIX_EPOCH;
        if event_time < UNIX_EPOCH {
            self.last_drop = Some(DropReason::PreEpoch);
            return None;
        }
        let start = match self.current_bucket_start {
            Some(s) => s,
            None => {
                self.last_drop = None;
                self.advance_to(event_time);
                return Some(self.head as usize);
            }
        };
        if event_time >= start {
            self.last_drop = None;
            self.advance_to(event_time);
            return Some(self.head as usize);
        }
        let et_bucket_start = self.bucket_start_for(event_time);
        let delta = match start.duration_since(et_bucket_start) {
            Ok(d) => d,
            Err(_) => {
                self.last_drop = Some(DropReason::TooNew);
                return None;
            }
        };
        let bucket_secs = self.bucket_duration.as_secs();
        if bucket_secs == 0 {
            self.last_drop = None;
            return Some(self.head as usize);
        }
        let delta_buckets = (delta.as_secs() / bucket_secs) as usize;
        let num_buckets = self.buckets.len();
        if delta_buckets >= num_buckets {
            self.last_drop = Some(DropReason::TooOld);
            return None;
        }
        self.last_drop = None;
        let idx = (self.head as usize + num_buckets - delta_buckets) % num_buckets;
        Some(idx)
    }
}

// ---------------------------------------------------------------------------
// TypedRingBufferEnum — variant dispatch for Shard::entity_ringbuffers_typed.
// ---------------------------------------------------------------------------

/// Hint for which ring-buffer variant an op needs when it calls
/// [`crate::shard::Shard::get_or_init_typed_ringbuffer`]. Kept as a plain
/// enum (not a trait) to stay object-safe and zero-cost.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TypedRingBufferVariantHint {
    I64,
    F64,
    Avg,
}

impl TypedRingBufferVariantHint {
    pub fn construct(self, window: Duration, bucket: Duration) -> TypedRingBufferEnum {
        match self {
            Self::I64 => TypedRingBufferEnum::I64(TypedRingBufferI64::new(window, bucket)),
            Self::F64 => TypedRingBufferEnum::F64(TypedRingBufferF64::new(window, bucket)),
            Self::Avg => TypedRingBufferEnum::Avg(TypedRingBufferAvg::new(window, bucket)),
        }
    }
}

/// Dispatch wrapper stored in
/// [`crate::shard::Shard::entity_ringbuffers_typed`]. Each entry is
/// owned single-threaded by the shard; variant-mismatch access panics
/// (contract: one op-index keys a single variant for its entire lifetime).
#[derive(Debug, Clone)]
pub enum TypedRingBufferEnum {
    I64(TypedRingBufferI64),
    F64(TypedRingBufferF64),
    Avg(TypedRingBufferAvg),
}

impl TypedRingBufferEnum {
    #[inline]
    pub fn as_i64_mut(&mut self) -> &mut TypedRingBufferI64 {
        match self {
            Self::I64(r) => r,
            other => panic!(
                "TypedRingBufferEnum variant mismatch: expected I64, got {:?}",
                std::mem::discriminant(other)
            ),
        }
    }
    #[inline]
    pub fn as_f64_mut(&mut self) -> &mut TypedRingBufferF64 {
        match self {
            Self::F64(r) => r,
            other => panic!(
                "TypedRingBufferEnum variant mismatch: expected F64, got {:?}",
                std::mem::discriminant(other)
            ),
        }
    }
    #[inline]
    pub fn as_avg_mut(&mut self) -> &mut TypedRingBufferAvg {
        match self {
            Self::Avg(r) => r,
            other => panic!(
                "TypedRingBufferEnum variant mismatch: expected Avg, got {:?}",
                std::mem::discriminant(other)
            ),
        }
    }
    #[inline]
    pub fn as_i64(&self) -> &TypedRingBufferI64 {
        match self {
            Self::I64(r) => r,
            _ => panic!("TypedRingBufferEnum variant mismatch: expected I64"),
        }
    }
    #[inline]
    pub fn as_f64(&self) -> &TypedRingBufferF64 {
        match self {
            Self::F64(r) => r,
            _ => panic!("TypedRingBufferEnum variant mismatch: expected F64"),
        }
    }
    #[inline]
    pub fn as_avg(&self) -> &TypedRingBufferAvg {
        match self {
            Self::Avg(r) => r,
            _ => panic!("TypedRingBufferEnum variant mismatch: expected Avg"),
        }
    }

    pub fn allocated_bytes(&self) -> usize {
        match self {
            Self::I64(r) => r.allocated_bytes(),
            Self::F64(r) => r.allocated_bytes(),
            Self::Avg(r) => r.allocated_bytes(),
        }
    }
}

// ---------------------------------------------------------------------------
// Windowed typed agg ops — Wave 1 subset (Count, Sum i64/f64, Avg f64).
//
// Wave 1 scope note: these ops' `update_typed` / `read_feature` are no-ops
// because the real state lives in the Shard's entity_ringbuffers_typed
// side-map; the windowed path is driven by `update_windowed` /
// `read_feature_windowed` (new trait methods with default implementations
// in `src/engine/operators_typed.rs`). Wave 1 keeps these trait defaults
// unused by `run_typed_agg_step` today — the parity harness in
// `tests/typed_windowed_aggregation_parity.rs` drives the windowed
// op/ring pair directly, mirroring how `typed_aggregation_parity.rs`
// drives unwindowed ops. Wave 4 wires the windowed path into the cascade
// walker.
// ---------------------------------------------------------------------------

/// Phase 59.7 W1 — windowed twin of [`CountOpTyped`]. Pushes +1 into the
/// bucket containing `event_time`; reads `ring.sum_all()` as an `Int`
/// feature (Missing when `sum == 0`, matching `CountOp::read`).
#[derive(Clone, Debug)]
pub struct CountOpTypedWindowed {
    pub name: String,
    pub op_idx: u16,
    pub window: Duration,
    pub bucket: Duration,
}

impl CountOpTypedWindowed {
    pub fn variant_hint(&self) -> TypedRingBufferVariantHint {
        TypedRingBufferVariantHint::I64
    }
    /// Drive one event into the provided ring. Mirrors the closure pattern
    /// used by `CountOp::push` → `RingBuffer<u64>::add_to_current`.
    #[inline]
    pub fn apply(&self, ring: &mut TypedRingBufferI64, event_time: SystemTime) {
        ring.update_at_event_time(|b| *b += 1, event_time);
    }
    /// Project the windowed feature. Matches [`crate::engine::operators::CountOp::read`] —
    /// sum == 0 → Missing.
    pub fn read(&self, ring: &TypedRingBufferI64) -> FeatureValue {
        let s = ring.sum_all();
        if s == 0 {
            FeatureValue::Missing
        } else {
            FeatureValue::Int(s)
        }
    }
}

impl TypedAggOp for CountOpTypedWindowed {
    fn init_state(&self, _ss: &RegisteredSchema, _state: &mut Row) {}
    #[inline]
    fn update_typed(
        &self,
        _state: &mut Row,
        _ss: &RegisteredSchema,
        _e: &Row,
        _es: &RegisteredSchema,
        _now: SystemTime,
    ) {
        // State lives on the shard side-map; see `apply` + Wave-4 walker.
    }
    fn read_feature(&self, _state: &Row, _ss: &RegisteredSchema) -> FeatureValue {
        FeatureValue::Missing
    }
    fn name(&self) -> &str {
        &self.name
    }
}

/// Phase 59.7 W1 — windowed twin of [`SumOpTypedI64`].
#[derive(Clone, Debug)]
pub struct SumOpTypedWindowedI64 {
    pub name: String,
    pub op_idx: u16,
    pub window: Duration,
    pub bucket: Duration,
    pub input_offset: u16,
}

impl SumOpTypedWindowedI64 {
    pub fn variant_hint(&self) -> TypedRingBufferVariantHint {
        TypedRingBufferVariantHint::I64
    }
    #[inline]
    pub fn apply(&self, ring: &mut TypedRingBufferI64, event: &Row, event_time: SystemTime) {
        let v = event.read_i64(self.input_offset);
        ring.update_at_event_time(|b| *b = b.wrapping_add(v), event_time);
    }
    /// Sum is always reportable; Value-path `SumOp` emits `Missing` only when
    /// no events were pushed. For the typed windowed path we separately
    /// track "any event" via a companion count ring; callers construct a
    /// SumOp + CountOp pair when they need Missing-vs-zero distinction. For
    /// the parity gate the test harness drives a companion count ring and
    /// decides Missing there — this op's `read` always returns `Int(sum)`.
    pub fn read(&self, ring: &TypedRingBufferI64) -> FeatureValue {
        FeatureValue::Int(ring.sum_all())
    }
}

impl TypedAggOp for SumOpTypedWindowedI64 {
    fn init_state(&self, _ss: &RegisteredSchema, _state: &mut Row) {}
    #[inline]
    fn update_typed(
        &self,
        _state: &mut Row,
        _ss: &RegisteredSchema,
        _e: &Row,
        _es: &RegisteredSchema,
        _now: SystemTime,
    ) {
    }
    fn read_feature(&self, _state: &Row, _ss: &RegisteredSchema) -> FeatureValue {
        FeatureValue::Missing
    }
    fn name(&self) -> &str {
        &self.name
    }
}

/// Phase 59.7 W1 — windowed twin of [`SumOpTypedF64`].
#[derive(Clone, Debug)]
pub struct SumOpTypedWindowedF64 {
    pub name: String,
    pub op_idx: u16,
    pub window: Duration,
    pub bucket: Duration,
    pub input_offset: u16,
}

impl SumOpTypedWindowedF64 {
    pub fn variant_hint(&self) -> TypedRingBufferVariantHint {
        TypedRingBufferVariantHint::F64
    }
    #[inline]
    pub fn apply(&self, ring: &mut TypedRingBufferF64, event: &Row, event_time: SystemTime) {
        let v = event.read_f64(self.input_offset);
        ring.update_at_event_time(|b| *b += v, event_time);
    }
    pub fn read(&self, ring: &TypedRingBufferF64) -> FeatureValue {
        FeatureValue::Float(ring.sum_all())
    }
}

impl TypedAggOp for SumOpTypedWindowedF64 {
    fn init_state(&self, _ss: &RegisteredSchema, _state: &mut Row) {}
    #[inline]
    fn update_typed(
        &self,
        _state: &mut Row,
        _ss: &RegisteredSchema,
        _e: &Row,
        _es: &RegisteredSchema,
        _now: SystemTime,
    ) {
    }
    fn read_feature(&self, _state: &Row, _ss: &RegisteredSchema) -> FeatureValue {
        FeatureValue::Missing
    }
    fn name(&self) -> &str {
        &self.name
    }
}

/// Phase 59.7 W1 — windowed twin of [`AvgOpTypedF64`]. Uses the `(sum,
/// count)` packed ring; `read` returns `Float(sum/count)` or `Missing` if
/// count==0 (mirrors `AvgOp::read`).
#[derive(Clone, Debug)]
pub struct AvgOpTypedWindowedF64 {
    pub name: String,
    pub op_idx: u16,
    pub window: Duration,
    pub bucket: Duration,
    pub input_offset: u16,
}

impl AvgOpTypedWindowedF64 {
    pub fn variant_hint(&self) -> TypedRingBufferVariantHint {
        TypedRingBufferVariantHint::Avg
    }
    #[inline]
    pub fn apply(&self, ring: &mut TypedRingBufferAvg, event: &Row, event_time: SystemTime) {
        let v = event.read_f64(self.input_offset);
        ring.update_at_event_time(
            |b| {
                b.0 += v;
                b.1 += 1;
            },
            event_time,
        );
    }
    pub fn read(&self, ring: &TypedRingBufferAvg) -> FeatureValue {
        let (s, c) = ring.sum_all();
        if c == 0 {
            FeatureValue::Missing
        } else {
            FeatureValue::Float(s / c as f64)
        }
    }
}

impl TypedAggOp for AvgOpTypedWindowedF64 {
    fn init_state(&self, _ss: &RegisteredSchema, _state: &mut Row) {}
    #[inline]
    fn update_typed(
        &self,
        _state: &mut Row,
        _ss: &RegisteredSchema,
        _e: &Row,
        _es: &RegisteredSchema,
        _now: SystemTime,
    ) {
    }
    fn read_feature(&self, _state: &Row, _ss: &RegisteredSchema) -> FeatureValue {
        FeatureValue::Missing
    }
    fn name(&self) -> &str {
        &self.name
    }
}

// ---------------------------------------------------------------------------
// Tests — unit coverage for the ring-buffer port (semantics parity with
// `src/engine/window.rs::RingBuffer<T>`). Integration parity tests that
// drive these ops vs. Value-path `CountOp`/`SumOp`/`AvgOp` live in
// `tests/typed_windowed_aggregation_parity.rs`.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn ts(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    #[test]
    fn test_typed_ring_buffer_i64_steady_state() {
        // window=5s, bucket=1s → 5 buckets
        let mut rb = TypedRingBufferI64::new(Duration::from_secs(5), Duration::from_secs(1));
        // Insert +1 at 5 distinct event_times within a 5s window.
        for s in 0..5u64 {
            rb.update_at_event_time(|b| *b += 1, ts(1000 + s));
        }
        // Should see 5 events (one per bucket).
        assert_eq!(rb.sum_all(), 5);
        // Advance to t=1010 → full window past all prior buckets. They all expire.
        rb.update_at_event_time(|b| *b += 1, ts(1010));
        // Only the most recent +1 should remain.
        assert_eq!(rb.sum_all(), 1);
    }

    #[test]
    fn test_typed_ring_buffer_f64_historical_bucket() {
        // window=5s, bucket=1s → 5 buckets. Establish head at t=1003,
        // then insert at t=1004, then reach back to t=1002 (historical,
        // in-window).
        let mut rb = TypedRingBufferF64::new(Duration::from_secs(5), Duration::from_secs(1));
        rb.update_at_event_time(|b| *b += 1.5, ts(1003));
        rb.update_at_event_time(|b| *b += 1.5, ts(1004));
        rb.update_at_event_time(|b| *b += 1.5, ts(1002));
        // Sum == 4.5 regardless of bucket placement.
        assert!((rb.sum_all() - 4.5).abs() < 1e-9);
    }

    #[test]
    fn test_typed_ring_buffer_i64_too_old_drop() {
        // window=5s, bucket=1s. Advance to t=1010, then try to insert at
        // t=1003 (1010-5=1005 is the window floor; 1003 < 1005 → TooOld).
        let mut rb = TypedRingBufferI64::new(Duration::from_secs(5), Duration::from_secs(1));
        rb.update_at_event_time(|b| *b += 1, ts(1010));
        assert_eq!(rb.sum_all(), 1);
        rb.update_at_event_time(|b| *b += 99, ts(1003));
        // sum unchanged — event was dropped.
        assert_eq!(rb.sum_all(), 1);
        assert_eq!(rb.take_last_drop(), Some(DropReason::TooOld));
    }

    #[test]
    fn test_typed_ring_buffer_avg_packed() {
        let mut rb = TypedRingBufferAvg::new(Duration::from_secs(5), Duration::from_secs(1));
        rb.update_at_event_time(
            |b| {
                b.0 += 5.0;
                b.1 += 1;
            },
            ts(1000),
        );
        rb.update_at_event_time(
            |b| {
                b.0 += 10.0;
                b.1 += 1;
            },
            ts(1001),
        );
        rb.update_at_event_time(
            |b| {
                b.0 += 15.0;
                b.1 += 1;
            },
            ts(1002),
        );
        let (s, c) = rb.sum_all();
        assert!((s - 30.0).abs() < 1e-9);
        assert_eq!(c, 3);
        // avg = 30/3 = 10.0
        assert!((s / c as f64 - 10.0).abs() < 1e-9);
    }

    #[test]
    fn test_allocated_bytes_reports_nonzero() {
        let rb_i64 = TypedRingBufferI64::new(Duration::from_secs(5), Duration::from_secs(1));
        let rb_f64 = TypedRingBufferF64::new(Duration::from_secs(5), Duration::from_secs(1));
        let rb_avg = TypedRingBufferAvg::new(Duration::from_secs(5), Duration::from_secs(1));
        assert!(rb_i64.allocated_bytes() >= 5 * std::mem::size_of::<i64>());
        assert!(rb_f64.allocated_bytes() >= 5 * std::mem::size_of::<f64>());
        assert!(rb_avg.allocated_bytes() >= 5 * std::mem::size_of::<(f64, i64)>());
    }

    #[test]
    fn test_typed_ring_buffer_enum_variant_dispatch() {
        let mut e_i64 =
            TypedRingBufferVariantHint::I64.construct(Duration::from_secs(5), Duration::from_secs(1));
        e_i64.as_i64_mut().update_at_event_time(|b| *b += 7, ts(1000));
        assert_eq!(e_i64.as_i64().sum_all(), 7);
        let mut e_f64 =
            TypedRingBufferVariantHint::F64.construct(Duration::from_secs(5), Duration::from_secs(1));
        e_f64.as_f64_mut().update_at_event_time(|b| *b += 2.5, ts(1000));
        assert!((e_f64.as_f64().sum_all() - 2.5).abs() < 1e-9);
        let mut e_avg =
            TypedRingBufferVariantHint::Avg.construct(Duration::from_secs(5), Duration::from_secs(1));
        e_avg.as_avg_mut().update_at_event_time(
            |b| {
                b.0 += 4.0;
                b.1 += 1;
            },
            ts(1000),
        );
        let (s, c) = e_avg.as_avg().sum_all();
        assert!((s - 4.0).abs() < 1e-9);
        assert_eq!(c, 1);
    }

    #[test]
    #[should_panic(expected = "variant mismatch")]
    fn test_typed_ring_buffer_enum_variant_mismatch_panics() {
        let mut e =
            TypedRingBufferVariantHint::I64.construct(Duration::from_secs(5), Duration::from_secs(1));
        let _ = e.as_f64_mut(); // expected panic
    }
}
