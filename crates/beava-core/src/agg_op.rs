//! AggOp enum dispatch + AggOpDescriptor + output_type_for.
//!
//! This module provides:
//! - `AggKind`: 8-variant enum identifying the aggregation operation kind.
//! - `AggOpDescriptor`: register-time descriptor (kind, field, window_ms).
//! - `AggOp`: live state enum dispatching update/query via match arms (no
//!   Box<dyn>). Per D-01.
//! - `output_type_for`: register-time type inference mapping op kind →
//!   FieldType. Used by Plan 05-03 schema propagator.
//!
//! # Requirements traceability
//! - AGG-CORE-01..08: covered per-op in agg_state.rs
//! - AGG-CORE-09: Windowed<Op> via WindowedOp in agg_windowed.rs
//!
//! D-01: enum + match arm dispatch; no Box<dyn AggOp>.
//! D-06: no wall-clock reads in apply paths (forbidden: SystemTime now, rand).

use crate::agg_buffer::{
    DowHourHistogramState, EventTypeMixState, HistogramState, HourOfDayHistogramState,
    MostRecentNState, ReservoirSampleState, SeasonalDeviationState,
};
use crate::agg_geo::{
    DistanceFromHomeState, GeoDistanceState, GeoEntropyState, GeoSpreadState, GeoVelocityState,
    UniqueCellsState,
};
use crate::agg_state::{
    AvgState, CountState, MaxState, MinState, RatioState, SumState, VarianceState,
};
use crate::row::{Row, Value};
use crate::schema::FieldType;
use crate::schema_propagate::Schema;
use serde::{Deserialize, Serialize};

// Forward declaration — implementation in agg_windowed.rs (Task 2).
// We import it here so AggOp::Windowed can hold it.
use crate::agg_windowed::WindowedOp;

// ─── AggKind ─────────────────────────────────────────────────────────────────

/// Identifies the aggregation operation kind. Copy + Clone for use in
/// descriptors and WindowedOp inner_kind (no heap allocation).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggKind {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Variance,
    StdDev,
    Ratio,
    // ── Phase 11: bounded-buffer + geo ───────────────────────────────────────
    Histogram,
    HourOfDayHistogram,
    DowHourHistogram,
    SeasonalDeviation,
    EventTypeMix,
    MostRecentN,
    ReservoirSample,
    GeoVelocity,
    GeoDistance,
    GeoSpread,
    UniqueCells,
    GeoEntropy,
    DistanceFromHome,
}

// ─── AggOpDescriptor ─────────────────────────────────────────────────────────

/// Register-time descriptor for one aggregation feature.
///
/// `where_expr` gates the apply-path update via `agg_where::evaluate_where_predicate`.
/// Added in Plan 05-02 (predicate threading, SDK-AGG-04).
///
/// `ext` carries Phase 11-family optional params (buckets, n, k, precision,
/// lat_field, lon_field, samples, categories). Default = empty / no extended
/// config so existing core ops stay source-compatible.
#[derive(Debug, Clone, Default)]
pub struct AggOpDescriptor {
    pub kind: AggKind,
    /// Field name for Sum/Avg/Min/Max/Variance/StdDev. None for Count/Ratio.
    pub field: Option<String>,
    /// Tumbling window duration in milliseconds. None = lifetime (windowless).
    pub window_ms: Option<u64>,
    /// Optional where-predicate (Plan 05-02). Evaluated via
    /// `agg_where::evaluate_where_predicate` at apply time.
    /// None = unconditional update (backwards-compatible with Plan 05-01).
    pub where_expr: Option<std::sync::Arc<crate::expr::Expr>>,
    /// Phase 11 extended params (optional; None-valued for core ops).
    pub ext: AggExtParams,
}

/// Extended register-time params for Phase 11 operators.
#[derive(Debug, Clone, Default)]
pub struct AggExtParams {
    /// Histogram bucket split points (strictly increasing).
    pub buckets: Option<Vec<f64>>,
    /// `n` for MostRecentN.
    pub n: Option<usize>,
    /// `k` for ReservoirSample.
    pub k: Option<usize>,
    /// `precision` for UniqueCells / GeoEntropy.
    pub precision: Option<u32>,
    /// Latitude field name for geo ops.
    pub lat_field: Option<String>,
    /// Longitude field name for geo ops.
    pub lon_field: Option<String>,
    /// `samples` for DistanceFromHome.
    pub samples: Option<usize>,
    /// Allowed category allowlist for EventTypeMix.
    pub categories: Option<Vec<String>>,
    /// Max distinct categories tracked by EventTypeMix (default 256).
    pub max_categories: Option<usize>,
}

// Default for AggKind so AggOpDescriptor::default() compiles. Could be
// `#[derive(Default)] + #[default] AggKind::Count` but a manual impl keeps
// the canonical-default rationale visible at the definition site.
#[allow(clippy::derivable_impls)]
impl Default for AggKind {
    fn default() -> Self {
        AggKind::Count
    }
}

// ─── AggTypeError ────────────────────────────────────────────────────────────

/// Error type for `output_type_for` register-time type inference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggTypeError {
    /// A field-based op (Min/Max) requires `desc.field` to be set.
    FieldRequired { kind: AggKind },
    /// The specified field is not present in the upstream schema.
    FieldMissing { field: String },
}

impl std::fmt::Display for AggTypeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggTypeError::FieldRequired { kind } => {
                write!(f, "aggregation {:?} requires a field to be specified", kind)
            }
            AggTypeError::FieldMissing { field } => {
                write!(f, "field '{}' not found in upstream schema", field)
            }
        }
    }
}

// ─── AggOp ───────────────────────────────────────────────────────────────────

/// Live per-(feature, entity) aggregation state. Enum dispatch; no Box<dyn>.
///
/// AGG-CORE-01..09 per D-01.
///
/// Phase 11 (D-08): buffer/geo variants enlarge the discriminant to ~600 B
/// (driven by `SeasonalDeviationState`'s 24-hour bucket array). We accept the
/// size delta intentionally — every aggregation entity allocates exactly one
/// `Vec<AggOp>` for its slots, and per-feature box indirection would dominate
/// the small per-event update path that Phase 13 needs to keep <300 ns. The
/// `Windowed` variant stays boxed because of its 64-bucket inner array.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggOp {
    /// AGG-CORE-01
    Count(CountState),
    /// AGG-CORE-02
    Sum(SumState),
    /// AGG-CORE-03
    Avg(AvgState),
    /// AGG-CORE-04
    Min(MinState),
    /// AGG-CORE-05
    Max(MaxState),
    /// AGG-CORE-06 (query returns sample variance)
    Variance(VarianceState),
    /// AGG-CORE-06 (shares VarianceState; query takes sqrt)
    StdDev(VarianceState),
    /// AGG-CORE-07
    Ratio(RatioState),
    /// AGG-CORE-09: any op wrapped in 64-bucket event-time tumbling
    Windowed(Box<WindowedOp>),
    // ── Phase 11 (D-08 all windowless in v0) ─────────────────────────────────
    Histogram(HistogramState),
    HourOfDayHistogram(HourOfDayHistogramState),
    DowHourHistogram(DowHourHistogramState),
    SeasonalDeviation(SeasonalDeviationState),
    EventTypeMix(EventTypeMixState),
    MostRecentN(MostRecentNState),
    ReservoirSample(ReservoirSampleState),
    GeoVelocity(GeoVelocityState),
    GeoDistance(GeoDistanceState),
    GeoSpread(GeoSpreadState),
    UniqueCells(UniqueCellsState),
    GeoEntropy(GeoEntropyState),
    DistanceFromHome(DistanceFromHomeState),
}

impl AggOp {
    /// Construct the live state for a descriptor.
    ///
    /// If `desc.window_ms.is_some()`, wraps the inner op in `WindowedOp`.
    /// Otherwise returns the lifetime (windowless) variant.
    ///
    /// Phase 11 (D-08): buffer/geo operators are always windowless in v0; the
    /// register-time compiler rejects `window=...` for those op names.
    pub fn new(desc: &AggOpDescriptor) -> Self {
        // Only core ops support Windowed wrap (Phase 5).
        if let Some(window_ms) = desc.window_ms {
            if is_windowable_core(desc.kind) {
                return AggOp::Windowed(Box::new(WindowedOp::new(desc.kind, window_ms)));
            }
            // Fall through — Phase 11 ops treat window_ms as a no-op (compiler
            // should already have rejected; defensive).
        }
        match desc.kind {
            AggKind::Count => AggOp::Count(CountState::default()),
            AggKind::Sum => AggOp::Sum(SumState::default()),
            AggKind::Avg => AggOp::Avg(AvgState::default()),
            AggKind::Min => AggOp::Min(MinState::default()),
            AggKind::Max => AggOp::Max(MaxState::default()),
            AggKind::Variance => AggOp::Variance(VarianceState::default()),
            AggKind::StdDev => AggOp::StdDev(VarianceState::default()),
            AggKind::Ratio => AggOp::Ratio(RatioState::default()),
            AggKind::Histogram => AggOp::Histogram(HistogramState::new(
                desc.ext.buckets.clone().unwrap_or_default(),
            )),
            AggKind::HourOfDayHistogram => {
                AggOp::HourOfDayHistogram(HourOfDayHistogramState::default())
            }
            AggKind::DowHourHistogram => AggOp::DowHourHistogram(DowHourHistogramState::default()),
            AggKind::SeasonalDeviation => {
                AggOp::SeasonalDeviation(SeasonalDeviationState::default())
            }
            AggKind::EventTypeMix => AggOp::EventTypeMix(EventTypeMixState::new(
                desc.ext.max_categories.unwrap_or(256),
                desc.ext.categories.clone(),
            )),
            AggKind::MostRecentN => {
                AggOp::MostRecentN(MostRecentNState::new(desc.ext.n.unwrap_or(10)))
            }
            AggKind::ReservoirSample => {
                AggOp::ReservoirSample(ReservoirSampleState::new(desc.ext.k.unwrap_or(10)))
            }
            AggKind::GeoVelocity => AggOp::GeoVelocity(GeoVelocityState::with_fields(
                desc.ext.lat_field.clone().unwrap_or_else(|| "lat".into()),
                desc.ext.lon_field.clone().unwrap_or_else(|| "lon".into()),
            )),
            AggKind::GeoDistance => AggOp::GeoDistance(GeoDistanceState::with_fields(
                desc.ext.lat_field.clone().unwrap_or_else(|| "lat".into()),
                desc.ext.lon_field.clone().unwrap_or_else(|| "lon".into()),
            )),
            AggKind::GeoSpread => AggOp::GeoSpread(GeoSpreadState::with_fields(
                desc.ext.lat_field.clone().unwrap_or_else(|| "lat".into()),
                desc.ext.lon_field.clone().unwrap_or_else(|| "lon".into()),
            )),
            AggKind::UniqueCells => AggOp::UniqueCells(UniqueCellsState::with_fields(
                desc.ext.lat_field.clone().unwrap_or_else(|| "lat".into()),
                desc.ext.lon_field.clone().unwrap_or_else(|| "lon".into()),
                desc.ext.precision.unwrap_or(10),
            )),
            AggKind::GeoEntropy => AggOp::GeoEntropy(GeoEntropyState::with_fields(
                desc.ext.lat_field.clone().unwrap_or_else(|| "lat".into()),
                desc.ext.lon_field.clone().unwrap_or_else(|| "lon".into()),
                desc.ext.precision.unwrap_or(10),
            )),
            AggKind::DistanceFromHome => {
                AggOp::DistanceFromHome(DistanceFromHomeState::with_fields(
                    desc.ext.lat_field.clone().unwrap_or_else(|| "lat".into()),
                    desc.ext.lon_field.clone().unwrap_or_else(|| "lon".into()),
                    desc.ext.samples.unwrap_or(100),
                ))
            }
        }
    }

    /// Returns true iff the op is a Phase 5 core op that supports the 64-bucket
    /// Windowed wrap. Used by serialization/debugging helpers; keep crate-public.
    #[allow(dead_code)]
    pub(crate) fn is_core(&self) -> bool {
        matches!(
            self,
            AggOp::Count(_)
                | AggOp::Sum(_)
                | AggOp::Avg(_)
                | AggOp::Min(_)
                | AggOp::Max(_)
                | AggOp::Variance(_)
                | AggOp::StdDev(_)
                | AggOp::Ratio(_)
        )
    }

    /// Update state with one event row. Dispatches to the concrete per-op impl.
    ///
    /// - `field`: the field to aggregate over (None for Count/Ratio and for
    ///   buffer/geo ops which carry their own field refs in state)
    /// - `where_matched`: pre-evaluated predicate result (Plan 05-02 wires this
    ///   from an Expr evaluator; here callers set it directly)
    pub fn update(
        &mut self,
        row: &Row,
        event_time_ms: i64,
        field: Option<&str>,
        where_matched: bool,
    ) {
        match self {
            AggOp::Count(s) => s.update(row, event_time_ms, field, where_matched),
            AggOp::Sum(s) => s.update(row, event_time_ms, field, where_matched),
            AggOp::Avg(s) => s.update(row, event_time_ms, field, where_matched),
            AggOp::Min(s) => s.update(row, event_time_ms, field, where_matched),
            AggOp::Max(s) => s.update(row, event_time_ms, field, where_matched),
            AggOp::Variance(s) => s.update(row, event_time_ms, field, where_matched),
            AggOp::StdDev(s) => s.update(row, event_time_ms, field, where_matched),
            AggOp::Ratio(s) => s.update(row, event_time_ms, field, where_matched),
            AggOp::Windowed(w) => w.update(row, event_time_ms, field, where_matched),
            // ── Phase 11 ────────────────────────────────────────────────
            AggOp::Histogram(s) => s.update(row, field, where_matched),
            AggOp::HourOfDayHistogram(s) => s.update(event_time_ms, where_matched),
            AggOp::DowHourHistogram(s) => s.update(event_time_ms, where_matched),
            AggOp::SeasonalDeviation(s) => s.update(row, event_time_ms, field, where_matched),
            AggOp::EventTypeMix(s) => s.update(row, field, where_matched),
            AggOp::MostRecentN(s) => s.update(row, field, where_matched),
            AggOp::ReservoirSample(s) => s.update(row, field, where_matched),
            AggOp::GeoVelocity(s) => s.update(row, event_time_ms, where_matched),
            AggOp::GeoDistance(s) => s.update(row, where_matched),
            AggOp::GeoSpread(s) => s.update(row, where_matched),
            AggOp::UniqueCells(s) => s.update(row, where_matched),
            AggOp::GeoEntropy(s) => s.update(row, where_matched),
            AggOp::DistanceFromHome(s) => s.update(row, where_matched),
        }
    }

    /// Apply-path entry point: evaluates `where_expr` (if any) before
    /// forwarding to the underlying state's update.
    ///
    /// For Ratio: predicate gates the numerator only; total always increments
    /// unconditionally on every seen row (D-03 semantics).
    /// For all other ops: predicate gates the whole update.
    ///
    /// # SDK-AGG-04
    pub fn update_with_row(
        &mut self,
        row: &Row,
        event_time_ms: i64,
        field: Option<&str>,
        where_expr: Option<&std::sync::Arc<crate::expr::Expr>>,
    ) {
        let where_matched = match where_expr {
            Some(e) => crate::agg_where::evaluate_where_predicate(e, row),
            None => true,
        };

        match self {
            AggOp::Windowed(w) => {
                // Windowed delegates bucket routing to WindowedOp::update_with_row,
                // which threads the predicate into each bucket's inner AggOp.
                w.update_with_row(row, event_time_ms, field, where_expr);
            }
            _ => {
                // All other ops (including Ratio): pass where_matched directly.
                // RatioState::update already implements "gate numerator only"
                // semantics — it increments total unconditionally and matching
                // only when where_matched is true.
                self.update(row, event_time_ms, field, where_matched);
            }
        }
    }

    /// Query current aggregation value. Dispatches to the concrete per-op impl.
    ///
    /// `query_time_ms` is the query-time event clock (used by WindowedOp to
    /// determine which buckets are active). Lifetime ops ignore it.
    pub fn query(&self, query_time_ms: i64) -> Value {
        match self {
            AggOp::Count(s) => s.query(),
            AggOp::Sum(s) => s.query(),
            AggOp::Avg(s) => s.query(),
            AggOp::Min(s) => s.query(),
            AggOp::Max(s) => s.query(),
            AggOp::Variance(s) => s.query_variance(),
            AggOp::StdDev(s) => s.query_stddev(),
            AggOp::Ratio(s) => s.query(),
            AggOp::Windowed(w) => w.query(query_time_ms),
            AggOp::Histogram(s) => s.query(),
            AggOp::HourOfDayHistogram(s) => s.query(),
            AggOp::DowHourHistogram(s) => s.query(),
            AggOp::SeasonalDeviation(s) => s.query(),
            AggOp::EventTypeMix(s) => s.query(),
            AggOp::MostRecentN(s) => s.query(),
            AggOp::ReservoirSample(s) => s.query(),
            AggOp::GeoVelocity(s) => s.query(),
            AggOp::GeoDistance(s) => s.query(),
            AggOp::GeoSpread(s) => s.query(),
            AggOp::UniqueCells(s) => s.query(),
            AggOp::GeoEntropy(s) => s.query(),
            AggOp::DistanceFromHome(s) => s.query(),
        }
    }
}

/// Returns true iff the op can be wrapped in the 64-bucket Windowed envelope.
/// Phase 5 core ops are windowable; Phase 11 ops are lifetime-only (D-08).
fn is_windowable_core(kind: AggKind) -> bool {
    matches!(
        kind,
        AggKind::Count
            | AggKind::Sum
            | AggKind::Avg
            | AggKind::Min
            | AggKind::Max
            | AggKind::Variance
            | AggKind::StdDev
            | AggKind::Ratio
    )
}

// ─── output_type_for ─────────────────────────────────────────────────────────

/// Register-time output type inference.
///
/// Used by Plan 05-03 schema propagator to infer the output FieldType of an
/// aggregation feature without running any events.
///
/// Rules:
/// - Count → I64
/// - Sum, Avg, Variance, StdDev, Ratio → F64
/// - Min, Max → inherit the upstream field's FieldType
pub fn output_type_for(
    upstream: &Schema,
    desc: &AggOpDescriptor,
) -> Result<FieldType, AggTypeError> {
    match desc.kind {
        AggKind::Count => Ok(FieldType::I64),
        AggKind::Sum | AggKind::Avg | AggKind::Variance | AggKind::StdDev | AggKind::Ratio => {
            Ok(FieldType::F64)
        }
        AggKind::Min | AggKind::Max => {
            let field = desc
                .field
                .as_deref()
                .ok_or(AggTypeError::FieldRequired { kind: desc.kind })?;
            upstream
                .fields
                .get(field)
                .copied()
                .ok_or_else(|| AggTypeError::FieldMissing {
                    field: field.to_string(),
                })
        }
        // Phase 11: structured outputs have no FieldType representation — they
        // appear only as aggregation feature outputs (Value::List / Value::Map).
        // The schema propagator treats these as FieldType::Str for placeholder
        // naming (no downstream derivation can consume them as scalars in v0).
        AggKind::Histogram
        | AggKind::HourOfDayHistogram
        | AggKind::DowHourHistogram
        | AggKind::EventTypeMix
        | AggKind::MostRecentN
        | AggKind::ReservoirSample => Ok(FieldType::Str),
        // Scalar Phase 11 outputs
        AggKind::SeasonalDeviation
        | AggKind::GeoVelocity
        | AggKind::GeoDistance
        | AggKind::GeoSpread
        | AggKind::GeoEntropy
        | AggKind::DistanceFromHome => Ok(FieldType::F64),
        AggKind::UniqueCells => Ok(FieldType::I64),
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::Row;
    use std::collections::BTreeMap;

    fn desc(kind: AggKind) -> AggOpDescriptor {
        AggOpDescriptor {
            kind,
            field: None,
            window_ms: None,
            where_expr: None,

            ext: Default::default(),
        }
    }

    fn desc_field(kind: AggKind, field: &str) -> AggOpDescriptor {
        AggOpDescriptor {
            kind,
            field: Some(field.to_string()),
            window_ms: None,
            where_expr: None,

            ext: Default::default(),
        }
    }

    fn desc_windowed(kind: AggKind, window_ms: u64) -> AggOpDescriptor {
        AggOpDescriptor {
            kind,
            field: None,
            window_ms: Some(window_ms),
            where_expr: None,

            ext: Default::default(),
        }
    }

    fn schema_with(pairs: &[(&str, FieldType)]) -> Schema {
        let mut fields = BTreeMap::new();
        for (k, v) in pairs {
            fields.insert(k.to_string(), *v);
        }
        Schema {
            fields,
            optional_fields: vec![],
        }
    }

    // ── AggOp::new dispatch ───────────────────────────────────────────────

    #[test]
    fn aggop_new_dispatches_on_kind() {
        // Lifetime variants
        assert!(matches!(AggOp::new(&desc(AggKind::Count)), AggOp::Count(_)));
        assert!(matches!(AggOp::new(&desc(AggKind::Sum)), AggOp::Sum(_)));
        assert!(matches!(AggOp::new(&desc(AggKind::Avg)), AggOp::Avg(_)));
        assert!(matches!(AggOp::new(&desc(AggKind::Min)), AggOp::Min(_)));
        assert!(matches!(AggOp::new(&desc(AggKind::Max)), AggOp::Max(_)));
        assert!(matches!(
            AggOp::new(&desc(AggKind::Variance)),
            AggOp::Variance(_)
        ));
        assert!(matches!(
            AggOp::new(&desc(AggKind::StdDev)),
            AggOp::StdDev(_)
        ));
        assert!(matches!(AggOp::new(&desc(AggKind::Ratio)), AggOp::Ratio(_)));

        // Windowed variant when window_ms is set
        assert!(matches!(
            AggOp::new(&desc_windowed(AggKind::Count, 60_000)),
            AggOp::Windowed(_)
        ));
    }

    // ── AggOp::query dispatch ─────────────────────────────────────────────

    #[test]
    fn aggop_query_dispatches_on_variant() {
        let r = Row::new().with_field("x", Value::F64(5.0));
        let t = 0_i64;

        // Count: 1 event → I64(1)
        let mut count = AggOp::Count(CountState::default());
        count.update(&r, t, None, true);
        assert_eq!(count.query(t), Value::I64(1));

        // Sum: 5.0 → F64(5.0)
        let mut sum = AggOp::Sum(SumState::default());
        sum.update(&r, t, Some("x"), true);
        assert_eq!(sum.query(t), Value::F64(5.0));

        // Avg: 5.0 → F64(5.0)
        let mut avg = AggOp::Avg(AvgState::default());
        avg.update(&r, t, Some("x"), true);
        assert_eq!(avg.query(t), Value::F64(5.0));

        // Min: 5.0 → F64(5.0)
        let mut min_op = AggOp::Min(MinState::default());
        min_op.update(&r, t, Some("x"), true);
        assert_eq!(min_op.query(t), Value::F64(5.0));

        // Max: 5.0 → F64(5.0)
        let mut max_op = AggOp::Max(MaxState::default());
        max_op.update(&r, t, Some("x"), true);
        assert_eq!(max_op.query(t), Value::F64(5.0));

        // Variance: single element → Null
        let mut var = AggOp::Variance(VarianceState::default());
        var.update(&r, t, Some("x"), true);
        assert_eq!(var.query(t), Value::Null);

        // StdDev: single element → Null
        let mut sd = AggOp::StdDev(VarianceState::default());
        sd.update(&r, t, Some("x"), true);
        assert_eq!(sd.query(t), Value::Null);

        // Ratio: 1 matched out of 1 → F64(1.0)
        let mut ratio = AggOp::Ratio(RatioState::default());
        ratio.update(&r, t, None, true);
        assert_eq!(ratio.query(t), Value::F64(1.0));
    }

    // ── output_type_for ───────────────────────────────────────────────────

    #[test]
    fn output_type_for_count_returns_i64() {
        let s = schema_with(&[("amount", FieldType::F64)]);
        assert_eq!(
            output_type_for(&s, &desc(AggKind::Count)),
            Ok(FieldType::I64)
        );
    }

    #[test]
    fn output_type_for_sum_avg_returns_f64() {
        let s = schema_with(&[("amount", FieldType::F64)]);
        assert_eq!(
            output_type_for(&s, &desc_field(AggKind::Sum, "amount")),
            Ok(FieldType::F64)
        );
        assert_eq!(
            output_type_for(&s, &desc_field(AggKind::Avg, "amount")),
            Ok(FieldType::F64)
        );
    }

    #[test]
    fn output_type_for_min_max_inherits_field_type() {
        let s = schema_with(&[("amount", FieldType::F64), ("count", FieldType::I64)]);
        assert_eq!(
            output_type_for(&s, &desc_field(AggKind::Min, "amount")),
            Ok(FieldType::F64)
        );
        assert_eq!(
            output_type_for(&s, &desc_field(AggKind::Max, "count")),
            Ok(FieldType::I64)
        );
    }

    #[test]
    fn output_type_for_min_missing_field_returns_error() {
        let s = schema_with(&[("amount", FieldType::F64)]);
        let result = output_type_for(&s, &desc_field(AggKind::Min, "nonexistent"));
        assert_eq!(
            result,
            Err(AggTypeError::FieldMissing {
                field: "nonexistent".to_string()
            })
        );
    }

    #[test]
    fn output_type_for_min_no_field_returns_field_required_error() {
        let s = schema_with(&[("amount", FieldType::F64)]);
        let result = output_type_for(&s, &desc(AggKind::Min));
        assert_eq!(
            result,
            Err(AggTypeError::FieldRequired { kind: AggKind::Min })
        );
    }

    // ── update_with_row tests (Plan 05-02) ────────────────────────────────

    fn make_where_expr(src: &str) -> std::sync::Arc<crate::expr::Expr> {
        std::sync::Arc::new(crate::expr::parse(src).expect("should parse where expr"))
    }

    fn row_amount(v: f64) -> Row {
        Row::new().with_field("amount", Value::F64(v))
    }

    fn row_status(s: &str) -> Row {
        Row::new().with_field("status", Value::Str(s.to_string()))
    }

    /// 5 rows, 3 match predicate "(amount > 100)" → Count == I64(3).
    #[test]
    fn count_with_where_true_increments() {
        let where_expr = make_where_expr("(amount > 100)");
        let mut op = AggOp::Count(CountState::default());
        let amounts = [50.0, 150.0, 200.0, 80.0, 300.0]; // 3 > 100
        for &a in &amounts {
            op.update_with_row(&row_amount(a), 0, None, Some(&where_expr));
        }
        assert_eq!(op.query(0), Value::I64(3), "only 3 rows match amount > 100");
    }

    /// 5 rows, all fail predicate → Count == I64(0).
    #[test]
    fn count_with_where_false_does_not_increment() {
        let where_expr = make_where_expr("(amount > 1000)");
        let mut op = AggOp::Count(CountState::default());
        for &a in &[10.0_f64, 20.0, 30.0, 40.0, 50.0] {
            op.update_with_row(&row_amount(a), 0, None, Some(&where_expr));
        }
        assert_eq!(op.query(0), Value::I64(0), "no rows match amount > 1000");
    }

    /// 5 rows amounts [10,20,30,40,50] with predicate "(amount > 25)" → sum == 120.0.
    #[test]
    fn sum_with_where_skips_non_matching() {
        let where_expr = make_where_expr("(amount > 25)");
        let mut op = AggOp::Sum(SumState::default());
        for &a in &[10.0_f64, 20.0, 30.0, 40.0, 50.0] {
            op.update_with_row(&row_amount(a), 0, Some("amount"), Some(&where_expr));
        }
        // 30 + 40 + 50 = 120
        match op.query(0) {
            Value::F64(v) => assert!((v - 120.0).abs() < 1e-10, "sum should be 120.0, got {v}"),
            other => panic!("expected F64, got {:?}", other),
        }
    }

    /// Ratio "gate numerator only": predicate "(status == 'ok')", 10 events (3 match)
    /// → ratio == 0.3 (matching=3, total=10). D-03 semantics: total always increments.
    #[test]
    fn ratio_with_where_gates_numerator_only() {
        let where_expr = make_where_expr("(status == 'ok')");
        let mut op = AggOp::Ratio(RatioState::default());
        // 3 "ok" rows, 7 other rows
        for i in 0..10 {
            let row = if i < 3 {
                row_status("ok")
            } else {
                row_status("fail")
            };
            op.update_with_row(&row, 0, None, Some(&where_expr));
        }
        match op.query(0) {
            Value::F64(v) => assert!((v - 0.3).abs() < 1e-10, "ratio should be 3/10=0.3, got {v}"),
            other => panic!("expected F64, got {:?}", other),
        }
    }

    /// where_expr=None → always updates, identical to Plan 05-01 update (regression guard).
    #[test]
    fn update_with_none_where_always_updates() {
        let mut op = AggOp::Count(CountState::default());
        let r = Row::new();
        for _ in 0..5 {
            op.update_with_row(&r, 0, None, None);
        }
        assert_eq!(
            op.query(0),
            Value::I64(5),
            "None where_expr → all 5 rows counted"
        );
    }

    // ── Determinism guard ─────────────────────────────────────────────────

    #[test]
    fn no_systemtime_now_in_apply() {
        // Split forbidden patterns so this file does not itself trigger the check.
        let forbidden_clock = ["SystemTime", "::", "now"].concat();
        let forbidden_rand = ["rand", "::"].concat();
        let src = include_str!("agg_op.rs");
        assert!(
            !src.contains(forbidden_clock.as_str()),
            "agg_op.rs must not use wall-clock reads (D-06 determinism invariant)"
        );
        assert!(
            !src.contains(forbidden_rand.as_str()),
            "agg_op.rs must not use rand crate (D-06 determinism invariant)"
        );
    }
}
