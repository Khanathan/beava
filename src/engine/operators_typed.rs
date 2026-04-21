//! Phase 59.6 Wave 3 (TPC-PERF-11) — typed-row operator implementations.
//!
//! See `.planning/phases/59.6-typed-pipeline-records/59.6-CONTEXT.md`
//! Area C (D-C1, D-C2, D-C5) for the full design contract.
//!
//! # Overview
//!
//! Wave 3 lands the first operator specialization on typed rows:
//! [`EnrichFromTableTyped`]. Given an input [`Row`] plus a right-side
//! source_table row (typed when Wave 5 ships typed source_tables; Value
//! today via [`EnrichFromTableTyped::enrich_from_value`]), it constructs
//! a typed enriched [`Row`] laid out per a pre-derived enriched schema.
//!
//! Wave 4 will add typed aggregation ops (CountOp, LastOp, SumOp, AvgOp,
//! MinOp, MaxOp) — the [`TypedOperator`] trait exists here as a minimal
//! shape for them to implement. Wave 6 adds the remaining ops
//! (DistinctCountOp, PercentileOp, …).
//!
//! # Fallback path
//!
//! Streams without a registered schema continue to execute through the
//! Value-based operators in `src/engine/operators.rs`. The typed path
//! is additive — the parity gate (`TPC-CORR-07`) requires byte-identical
//! output between both paths on the same event stream.
//!
//! # Enriched schema derivation
//!
//! [`derive_enriched_schema`] computes the output schema for an
//! EnrichFromTable feature: input schema prefix + projected right-side
//! fields appended at the tail. The caller supplies each projection's
//! declared type (from the right schema, when available) so the
//! projection dispatch is data-type-correct. Projected fields are
//! marked nullable — missing right-side rows leave them as zero / empty
//! (D-C2 null-safe enrich semantics preserved from the Value path).

use crate::engine::schema::{FieldSpec, FieldTy, RegisteredSchema, Row};
use std::sync::Arc;

/// Phase 59.6 D-C1 — minimal typed-operator trait. Wave 3 implements
/// [`EnrichFromTableTyped`]. Wave 4 adds [`TypedAggOp`] for aggregation
/// operators (see `src/engine/operators_typed_aggs.rs`).
pub trait TypedOperator: Send + Sync {
    /// Operator name, matches the feature name on the registered stream.
    fn name(&self) -> &str;
    /// Schema of the input row consumed by this operator.
    fn input_schema(&self) -> &Arc<RegisteredSchema>;
    /// Schema of the output row produced by this operator.
    fn output_schema(&self) -> &Arc<RegisteredSchema>;
}

/// Phase 59.6 Wave 4 (TPC-PERF-11, D-C1, D-C4) — typed aggregation operator trait.
///
/// Each impl holds pre-resolved field offsets (resolved at register time)
/// so the hot path is offset arithmetic + scalar read/write — no HashMap
/// lookup, no enum dispatch, no allocation after state init.
///
/// Per-entity agg state is stored as a typed [`Row`] owned by
/// [`crate::shard::Shard::entity_state_typed`]. `init_state` is called
/// exactly once when the entity state Row is created (zero-cost identity
/// write); `update_typed` is called for every event; `read_feature`
/// produces a [`crate::types::FeatureValue`] for downstream consumption.
pub trait TypedAggOp: Send + Sync + std::fmt::Debug {
    /// Initialize this op's columns inside the shared per-entity agg
    /// state Row. Called exactly once when the entity's state Row is
    /// first created.
    fn init_state(&self, state_schema: &RegisteredSchema, state: &mut Row);

    /// Update the agg state given an input event Row. MUST mutate state
    /// in place with no allocation (D-C4 invariant).
    fn update_typed(
        &self,
        state: &mut Row,
        state_schema: &RegisteredSchema,
        event: &Row,
        event_schema: &RegisteredSchema,
        now: std::time::SystemTime,
    );

    /// Read the op's current feature value from the per-entity state Row.
    fn read_feature(
        &self,
        state: &Row,
        state_schema: &RegisteredSchema,
    ) -> crate::types::FeatureValue;

    /// Operator / feature name.
    fn name(&self) -> &str;

    /// Phase 59.6 Wave 6 (D-C1 type-erasure): sketch + windowed ops override
    /// this to update their side-channel state. Simple aggs (Count/Sum/Avg/
    /// Min/Max/Last/First/Stddev/Variance) use the default which forwards to
    /// [`Self::update_typed`]. Ops that need state too complex to
    /// monomorphize over the column type (HLL/UDDSketch/CMS/ring-buffers)
    /// override this to read/write into the per-entity [`SideBand`].
    #[allow(unused_variables)]
    fn update_with_sideband(
        &self,
        state: &mut Row,
        state_schema: &RegisteredSchema,
        event: &Row,
        event_schema: &RegisteredSchema,
        sideband: &mut SideBand,
        now: std::time::SystemTime,
    ) {
        self.update_typed(state, state_schema, event, event_schema, now);
    }

    /// Phase 59.6 Wave 6: sketch ops override this to project from their
    /// SideBand state into a FeatureValue. Default delegates to
    /// [`Self::read_feature`].
    #[allow(unused_variables)]
    fn read_feature_with_sideband(
        &self,
        state: &Row,
        state_schema: &RegisteredSchema,
        sideband: &SideBand,
    ) -> crate::types::FeatureValue {
        self.read_feature(state, state_schema)
    }
}

/// Phase 59.6 Wave 6 (TPC-PERF-11, D-C1 type-erasure) — per-entity
/// side-channel state for typed aggregation operators whose internal state
/// does not fit a fixed-layout Row (HLL registers, UDDSketch buckets,
/// CountMinSketch + TopKHeap, windowed ring buffers).
///
/// Each typed op keys into these maps by its **operator name** (the feature
/// name on the registered stream), because one entity's agg-state Row can
/// host multiple ops of the same kind (e.g. two separate `top_k` features
/// on different input fields). The op's `name()` is unique per-stream by
/// the SDK's validator, so `(stream, entity_key, op.name())` uniquely
/// identifies a sketch instance.
///
/// # Memory profile
///
/// All maps are lazy — a SideBand starts empty and only populates maps the
/// entity's ops actually touch. A typed agg-state with only simple ops
/// (Count/Sum/Avg/Min/Max/Last/First/Stddev/Variance) never allocates any
/// map entries; the SideBand is pure overhead for the in-memory
/// `AHashMap<(String,String), SideBand>` stored alongside
/// [`crate::shard::Shard::entity_state_typed`].
#[derive(Debug, Default)]
pub struct SideBand {
    /// `DistinctCountOpTyped` (HLL). Key = op.name().
    pub hll_sketches: ahash::AHashMap<String, crate::engine::hll::Hll>,
    /// `PercentileOpTyped` (UDDSketch). Key = op.name().
    pub udd_sketches: ahash::AHashMap<String, crate::engine::uddsketch::UDDSketch>,
    /// `TopKOpTyped` — CountMinSketch + TopKHeap pair per op name.
    pub topk_sketches:
        ahash::AHashMap<String, (crate::engine::cms::CountMinSketch, crate::engine::cms::TopKHeap)>,
    /// `LagOpTyped` / `FirstNOpTyped` / `LastNOpTyped` — ring buffers of
    /// recent observed values, keyed by op.name().
    pub ring_buffers: ahash::AHashMap<String, std::collections::VecDeque<crate::types::FeatureValue>>,
    /// `EmaOpTyped` last-timestamp per op name (initialized flag encoded
    /// via presence). Stored as `Duration` since `UNIX_EPOCH` for
    /// serialize-friendliness.
    pub ema_last_ts: ahash::AHashMap<String, std::time::SystemTime>,
}

/// Declaration of a single projected field lifted from the right-side
/// source table row into the enriched output row.
#[derive(Clone, Debug)]
pub struct ProjectedField {
    /// Name of the field on the right-side schema. For Value-mode
    /// (right side is untyped) this is the JSON object key.
    pub right_field_name: String,
    /// Byte offset in the enriched schema's payload where this
    /// projected value is written.
    pub dst_offset: u16,
    /// Declared type of the projection in the enriched schema (and, for
    /// typed right sides, must match the right schema's field type).
    pub dst_ty: FieldTy,
}

/// Phase 59.6 D-C2 — typed `EnrichFromTable` operator.
///
/// Given an input [`Row`] and a right-side row (typed or Value), produce
/// an enriched [`Row`] laid out according to `enriched_schema`. The
/// input's fields are copied verbatim into the enriched row's prefix;
/// projected right-side fields are written into the enriched schema's
/// tail at their pre-resolved offsets.
///
/// # Missing right row (D-C2 null-safe semantics)
///
/// If `right` is `None`, only the input prefix is copied — projected
/// fields remain zero (strings empty, numerics zero, bools false). This
/// matches the Value path where `EnrichFromTable` with no right row
/// emits a Value object containing only the input keys.
pub struct EnrichFromTableTyped {
    /// Operator name.
    pub name: String,
    /// Name of the right-side source table (for diagnostics + cross-shard
    /// lookup routing; the actual read happens in
    /// `run_typed_enrich_cascade`).
    pub right_table: Arc<str>,
    /// Pre-resolved field index (in `input_schema`) of the join key
    /// column on the primary side.
    pub right_key_field_in_input: usize,
    /// Projections from the right-side row into the enriched row.
    pub projected: Vec<ProjectedField>,
    /// Input schema — the primary stream's row layout.
    pub input_schema: Arc<RegisteredSchema>,
    /// Output schema — `input_schema` + projected fields.
    pub enriched_schema: Arc<RegisteredSchema>,
    /// Optional right-side schema. `None` → mixed-mode (right side is
    /// untyped Value). Wave 5 makes source_tables typed and this should
    /// always be `Some` for registered pipelines.
    pub right_schema: Option<Arc<RegisteredSchema>>,
}

impl TypedOperator for EnrichFromTableTyped {
    fn name(&self) -> &str {
        &self.name
    }
    fn input_schema(&self) -> &Arc<RegisteredSchema> {
        &self.input_schema
    }
    fn output_schema(&self) -> &Arc<RegisteredSchema> {
        &self.enriched_schema
    }
}

impl EnrichFromTableTyped {
    /// Typed enrich: input Row + right Row → enriched Row.
    ///
    /// Copies the input's payload prefix verbatim into the enriched row,
    /// then writes projected fields from the right row at their
    /// pre-resolved destination offsets. Missing right row preserves
    /// the zero-initialized tail (D-C2).
    pub fn enrich_from_row(&self, input: &Row, right: Option<&Row>) -> Row {
        let mut out = Row::zeroed(&self.enriched_schema);
        let prefix_len = self.input_schema.row_size as usize;
        out.payload[..prefix_len]
            .copy_from_slice(&input.payload[..prefix_len]);
        out.arena.extend_from_slice(&input.arena);
        if let (Some(right_row), Some(right_schema)) = (right, self.right_schema.as_ref()) {
            for pf in &self.projected {
                copy_field_between_rows(
                    right_row,
                    right_schema,
                    &pf.right_field_name,
                    &mut out,
                    pf.dst_offset,
                    &self.enriched_schema,
                );
            }
        }
        out
    }

    /// Mixed-mode enrich: input Row + right Value → enriched Row.
    ///
    /// Used while the right-side source table is still Value (Wave 3-4);
    /// Wave 5 switches to typed source_tables and the `enrich_from_row`
    /// variant becomes the default.
    pub fn enrich_from_value(
        &self,
        input: &Row,
        right_value: Option<&serde_json::Value>,
    ) -> Row {
        let mut out = Row::zeroed(&self.enriched_schema);
        let prefix_len = self.input_schema.row_size as usize;
        out.payload[..prefix_len]
            .copy_from_slice(&input.payload[..prefix_len]);
        out.arena.extend_from_slice(&input.arena);
        if let Some(v) = right_value {
            if let Some(obj) = v.as_object() {
                for pf in &self.projected {
                    write_projected_from_value(
                        obj.get(&pf.right_field_name),
                        &mut out,
                        pf.dst_offset,
                        pf.dst_ty,
                        &self.enriched_schema,
                    );
                }
            }
        }
        out
    }
}

/// Copy a named field from one Row to another using the source + destination schemas.
fn copy_field_between_rows(
    src: &Row,
    src_schema: &RegisteredSchema,
    field_name: &str,
    dst: &mut Row,
    dst_offset: u16,
    dst_schema: &RegisteredSchema,
) {
    let src_idx = match src_schema.field_index(field_name) {
        Some(i) => i,
        None => return,
    };
    let src_field = &src_schema.fields[src_idx];
    match src_field.ty {
        FieldTy::I64 => dst.write_i64(dst_offset, src.read_i64(src_field.offset)),
        FieldTy::F64 => dst.write_f64(dst_offset, src.read_f64(src_field.offset)),
        FieldTy::Bool => dst.write_bool(dst_offset, src.read_bool(src_field.offset)),
        FieldTy::InlineStr => {
            let s = src
                .read_inline_str(src_field.offset, src_schema.inline_str_cap)
                .to_string();
            dst.write_inline_str(dst_offset, dst_schema.inline_str_cap, &s);
        }
        FieldTy::String => {
            let s = src.read_string(src_field.offset).to_string();
            dst.write_string(dst_offset, &s);
        }
        FieldTy::Bytes => {
            let b = src.read_bytes(src_field.offset).to_vec();
            dst.write_bytes(dst_offset, &b);
        }
    }
}

fn write_projected_from_value(
    opt_v: Option<&serde_json::Value>,
    dst: &mut Row,
    dst_offset: u16,
    dst_ty: FieldTy,
    dst_schema: &RegisteredSchema,
) {
    let v = match opt_v {
        Some(v) => v,
        None => return,
    };
    if v.is_null() {
        return;
    }
    match dst_ty {
        FieldTy::I64 => {
            if let Some(n) = v.as_i64() {
                dst.write_i64(dst_offset, n);
            }
        }
        FieldTy::F64 => {
            if let Some(n) = v.as_f64() {
                dst.write_f64(dst_offset, n);
            }
        }
        FieldTy::Bool => {
            if let Some(b) = v.as_bool() {
                dst.write_bool(dst_offset, b);
            }
        }
        FieldTy::InlineStr => {
            if let Some(s) = v.as_str() {
                dst.write_inline_str(dst_offset, dst_schema.inline_str_cap, s);
            }
        }
        FieldTy::String | FieldTy::Bytes => {
            if let Some(s) = v.as_str() {
                dst.write_string(dst_offset, s);
            }
        }
    }
}

/// Phase 59.6 D-C2: derive the enriched output schema from an input
/// schema + a list of projections. Called at register time when an
/// EnrichFromTable FeatureDef is parsed — the derived schema is cached
/// on the cascade plan so the hot path does not rebuild it.
///
/// The enriched schema is `input_schema.fields ++ projections` with
/// offsets packed sequentially starting from `input_schema.row_size`.
/// Projected fields are marked nullable (right-table row may be
/// missing; D-C2 null-safe semantics).
pub fn derive_enriched_schema(
    input: &RegisteredSchema,
    projections: &[(&str, FieldTy)],
    inline_str_cap: u8,
) -> RegisteredSchema {
    let mut fields: Vec<FieldSpec> = input.fields.clone();
    let mut next_offset = input.row_size;
    for (name, ty) in projections {
        fields.push(FieldSpec {
            name: (*name).to_string(),
            ty: *ty,
            offset: next_offset,
            nullable: true,
        });
        next_offset = next_offset.saturating_add(ty.fixed_width(inline_str_cap));
    }
    RegisteredSchema {
        schema_id: 0,
        name: format!("{}_enriched", input.name),
        fields,
        inline_str_cap,
        row_size: next_offset,
    }
}

/// Phase 59.6 Wave 5 (TPC-PERF-11, D-C3) — typed `StreamStreamJoin` operator.
///
/// Symmetric interval join between two typed streams. Left and right
/// events are buffered on the join-owning shard (hash(join_key) % N,
/// per Phase 56 D-B1); when an event of one side arrives, it probes
/// the opposite-side buffer for time-overlap matches and emits
/// joined output rows via [`match_typed`]. Retention on each side is
/// bounded by `within: Duration`.
///
/// The joined schema is derived at register time via
/// [`derive_joined_schema`] — left fields preserve their offsets;
/// right fields append at `left.row_size` with their offsets
/// shifted. Joined row payload = `left_payload ++ right_payload`;
/// joined row arena = `left_arena ++ right_arena` (each side's arena
/// offsets remain valid because they reference indices into the
/// per-side arena slice; readers of the joined row must know which
/// side a field came from — the appended left/right schema bookends
/// encode this).
pub struct StreamStreamJoinTyped {
    pub name: String,
    pub left_schema: Arc<RegisteredSchema>,
    pub right_schema: Arc<RegisteredSchema>,
    pub joined_schema: Arc<RegisteredSchema>,
    /// Name of the field (on both left and right schemas) the join
    /// matches on. Must exist on both sides with identical FieldTy.
    pub on_field: String,
    pub within: std::time::Duration,
}

impl TypedOperator for StreamStreamJoinTyped {
    fn name(&self) -> &str {
        &self.name
    }
    fn input_schema(&self) -> &Arc<RegisteredSchema> {
        &self.left_schema
    }
    fn output_schema(&self) -> &Arc<RegisteredSchema> {
        &self.joined_schema
    }
}

impl StreamStreamJoinTyped {
    /// Emit a joined Row from a pair of side events (D-C3).
    ///
    /// Copies the left payload into `[0..left.row_size]`, the right
    /// payload into `[left.row_size..left.row_size+right.row_size]`,
    /// and concatenates both sides' arenas. The joined schema's
    /// fields correspond to `left.fields ++ right.fields` (right
    /// field offsets shifted by `left.row_size`); readers should
    /// resolve field names via the joined schema's field list.
    pub fn match_typed(&self, left: &Row, right: &Row) -> Row {
        let ls = self.left_schema.row_size as usize;
        let rs = self.right_schema.row_size as usize;
        let total = self.joined_schema.row_size as usize;
        debug_assert_eq!(total, ls + rs, "joined row_size == left + right");
        let mut out = Row::zeroed(&self.joined_schema);
        out.schema_id = self.joined_schema.schema_id;
        // Left payload into [0..ls].
        out.payload[..ls].copy_from_slice(&left.payload[..ls]);
        // Right payload into [ls..ls+rs].
        out.payload[ls..ls + rs].copy_from_slice(&right.payload[..rs]);
        // Concatenate arenas.
        out.arena.extend_from_slice(&left.arena);
        out.arena.extend_from_slice(&right.arena);
        out
    }
}

/// Phase 59.6 Wave 5 (TPC-PERF-11, D-C3) — derive the joined-output
/// schema from a left + right typed schema.
///
/// Schema layout: `left.fields` followed by `right.fields` with the
/// right side's offsets shifted by `left.row_size`. Field names are
/// preserved as-is; duplicate names across left and right resolve
/// via first-match on the left side (caller is responsible for
/// renaming in the SDK if ambiguity matters).
pub fn derive_joined_schema(
    left: &RegisteredSchema,
    right: &RegisteredSchema,
    inline_str_cap: u8,
) -> RegisteredSchema {
    let mut fields: Vec<FieldSpec> = left.fields.clone();
    for rf in &right.fields {
        fields.push(FieldSpec {
            name: rf.name.clone(),
            ty: rf.ty,
            offset: rf.offset + left.row_size,
            nullable: rf.nullable,
        });
    }
    RegisteredSchema {
        schema_id: 0,
        name: format!("{}_join_{}", left.name, right.name),
        fields,
        inline_str_cap,
        row_size: left.row_size + right.row_size,
    }
}

/// Phase 59.6 Wave 5 (TPC-PERF-11, D-C3) — per-shard typed SSJ buffer.
///
/// Holds one side's buffered events keyed by join_key. Each event
/// carries its (Row, timestamp) so the cross-side probe can evict
/// entries older than `within`. Lives on the join-owning shard
/// (`hash(join_key) % N`) — Phase 56 D-B1 routing preserved.
///
/// Wave 5 scope: in-memory per-shard buffer at library level. The
/// cross-shard SPSC dispatch (`ShardOp::SsjInsertTyped` variant) is
/// a future wave — the parity tests drive this buffer directly to
/// cover SC-9's operator-boundary parity without requiring full
/// cross-shard plumbing.
pub struct TypedSsjBuffer {
    /// join_key → Vec<(Row, timestamp)> — newest-appended-last.
    pub left: ahash::AHashMap<String, Vec<(Row, std::time::SystemTime)>>,
    pub right: ahash::AHashMap<String, Vec<(Row, std::time::SystemTime)>>,
}

impl TypedSsjBuffer {
    pub fn new() -> Self {
        Self {
            left: ahash::AHashMap::new(),
            right: ahash::AHashMap::new(),
        }
    }

    /// Insert a left-side event + probe for time-overlap matches on
    /// the right side. Returns any joined Rows (may be empty).
    pub fn insert_left_and_match(
        &mut self,
        op: &StreamStreamJoinTyped,
        join_key: &str,
        row: Row,
        now: std::time::SystemTime,
    ) -> Vec<Row> {
        // Evict expired right entries first so we match only fresh.
        evict_expired(&mut self.right, join_key, op.within, now);
        let mut out = Vec::new();
        if let Some(rights) = self.right.get(join_key) {
            for (r, _ts) in rights {
                out.push(op.match_typed(&row, r));
            }
        }
        // Buffer the newly arrived left event.
        self.left
            .entry(join_key.to_string())
            .or_default()
            .push((row, now));
        out
    }

    /// Insert a right-side event + probe for time-overlap matches on
    /// the left side. Returns any joined Rows (may be empty).
    pub fn insert_right_and_match(
        &mut self,
        op: &StreamStreamJoinTyped,
        join_key: &str,
        row: Row,
        now: std::time::SystemTime,
    ) -> Vec<Row> {
        evict_expired(&mut self.left, join_key, op.within, now);
        let mut out = Vec::new();
        if let Some(lefts) = self.left.get(join_key) {
            for (l, _ts) in lefts {
                out.push(op.match_typed(l, &row));
            }
        }
        self.right
            .entry(join_key.to_string())
            .or_default()
            .push((row, now));
        out
    }
}

impl Default for TypedSsjBuffer {
    fn default() -> Self {
        Self::new()
    }
}

fn evict_expired(
    buf: &mut ahash::AHashMap<String, Vec<(Row, std::time::SystemTime)>>,
    join_key: &str,
    within: std::time::Duration,
    now: std::time::SystemTime,
) {
    if let Some(vec) = buf.get_mut(join_key) {
        vec.retain(|(_, ts)| match now.duration_since(*ts) {
            Ok(age) => age <= within,
            Err(_) => true, // clock skew — keep
        });
        if vec.is_empty() {
            buf.remove(join_key);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn input_schema() -> Arc<RegisteredSchema> {
        let s = RegisteredSchema {
            schema_id: 0,
            name: "Txns".into(),
            fields: vec![
                FieldSpec {
                    name: "user_id".into(),
                    ty: FieldTy::InlineStr,
                    offset: 0,
                    nullable: false,
                },
                FieldSpec {
                    name: "amount".into(),
                    ty: FieldTy::F64,
                    offset: 16,
                    nullable: false,
                },
            ],
            inline_str_cap: 15,
            row_size: 24,
        };
        s.validate_layout().expect("valid");
        Arc::new(s)
    }

    fn right_schema() -> Arc<RegisteredSchema> {
        let s = RegisteredSchema {
            schema_id: 0,
            name: "Countries".into(),
            fields: vec![
                FieldSpec {
                    name: "user_id".into(),
                    ty: FieldTy::InlineStr,
                    offset: 0,
                    nullable: false,
                },
                FieldSpec {
                    name: "country".into(),
                    ty: FieldTy::InlineStr,
                    offset: 16,
                    nullable: false,
                },
                FieldSpec {
                    name: "tier".into(),
                    ty: FieldTy::InlineStr,
                    offset: 32,
                    nullable: false,
                },
                FieldSpec {
                    // A field the projection SKIPS — must not bleed
                    // into the enriched row.
                    name: "secret_field".into(),
                    ty: FieldTy::InlineStr,
                    offset: 48,
                    nullable: false,
                },
            ],
            inline_str_cap: 15,
            row_size: 64,
        };
        s.validate_layout().expect("valid");
        Arc::new(s)
    }

    fn make_enrich() -> EnrichFromTableTyped {
        let input = input_schema();
        let right = right_schema();
        let inline_cap = input.inline_str_cap;
        // Project only country + tier (NOT secret_field).
        let projections: Vec<(&str, FieldTy)> =
            vec![("country", FieldTy::InlineStr), ("tier", FieldTy::InlineStr)];
        let mut enriched = derive_enriched_schema(&input, &projections, inline_cap);
        // Populate a plausible schema_id.
        enriched.schema_id = 1;
        let enriched = Arc::new(enriched);
        let projected = vec![
            ProjectedField {
                right_field_name: "country".into(),
                dst_offset: input.row_size,
                dst_ty: FieldTy::InlineStr,
            },
            ProjectedField {
                right_field_name: "tier".into(),
                dst_offset: input.row_size + FieldTy::InlineStr.fixed_width(inline_cap),
                dst_ty: FieldTy::InlineStr,
            },
        ];
        EnrichFromTableTyped {
            name: "enrich_country".to_string(),
            right_table: Arc::from("Countries"),
            right_key_field_in_input: 0,
            projected,
            input_schema: input,
            enriched_schema: enriched,
            right_schema: Some(right),
        }
    }

    #[test]
    fn enrich_typed_same_shard_populates_right_fields() {
        let op = make_enrich();
        let mut input_row = Row::zeroed(&op.input_schema);
        input_row.write_inline_str(0, op.input_schema.inline_str_cap, "u1");
        input_row.write_f64(16, 1.5);
        let mut right_row = Row::zeroed(op.right_schema.as_ref().unwrap());
        let right_schema = op.right_schema.as_ref().unwrap();
        right_row.write_inline_str(0, right_schema.inline_str_cap, "u1");
        right_row.write_inline_str(16, right_schema.inline_str_cap, "US");
        right_row.write_inline_str(32, right_schema.inline_str_cap, "gold");
        right_row.write_inline_str(48, right_schema.inline_str_cap, "classified");

        let out = op.enrich_from_row(&input_row, Some(&right_row));
        assert_eq!(out.read_inline_str(0, op.enriched_schema.inline_str_cap), "u1");
        assert!((out.read_f64(16) - 1.5).abs() < 1e-9);
        assert_eq!(
            out.read_inline_str(24, op.enriched_schema.inline_str_cap),
            "US",
            "country projected at offset 24"
        );
        assert_eq!(
            out.read_inline_str(40, op.enriched_schema.inline_str_cap),
            "gold",
            "tier projected at offset 40"
        );
    }

    #[test]
    fn enrich_typed_missing_right_row_preserves_input() {
        let op = make_enrich();
        let mut input_row = Row::zeroed(&op.input_schema);
        input_row.write_inline_str(0, op.input_schema.inline_str_cap, "u1");
        input_row.write_f64(16, 9.5);
        let out = op.enrich_from_row(&input_row, None);
        assert_eq!(out.read_inline_str(0, op.enriched_schema.inline_str_cap), "u1");
        assert!((out.read_f64(16) - 9.5).abs() < 1e-9);
        // Projected fields should be empty (D-C2 missing semantics).
        assert_eq!(out.read_inline_str(24, op.enriched_schema.inline_str_cap), "");
        assert_eq!(out.read_inline_str(40, op.enriched_schema.inline_str_cap), "");
    }

    #[test]
    fn enrich_typed_projected_fields_skip_non_projected() {
        // secret_field is on the right schema but NOT projected — it
        // must NOT land in the enriched row.
        let op = make_enrich();
        // enriched_schema has exactly 4 fields: 2 input + 2 projected.
        assert_eq!(op.enriched_schema.fields.len(), 4);
        let names: Vec<&str> = op
            .enriched_schema
            .fields
            .iter()
            .map(|f| f.name.as_str())
            .collect();
        assert_eq!(names, vec!["user_id", "amount", "country", "tier"]);
        assert!(
            !names.contains(&"secret_field"),
            "secret_field must not land in enriched schema"
        );
    }

    #[test]
    fn enriched_schema_row_size_equals_input_plus_projected() {
        let input = input_schema();
        let projections: Vec<(&str, FieldTy)> =
            vec![("country", FieldTy::InlineStr), ("tier", FieldTy::InlineStr)];
        let enriched = derive_enriched_schema(&input, &projections, input.inline_str_cap);
        // input: 24 bytes. Each InlineStr slot at cap=15 is 16 bytes.
        // enriched = 24 + 16 + 16 = 56.
        assert_eq!(enriched.row_size, 56);
        assert_eq!(enriched.fields.len(), 4);
    }

    #[test]
    fn enrich_typed_with_value_right_side_populates_projected_fields() {
        // Mixed-mode path used while source_tables stay Value (Wave 3-4).
        let op = make_enrich();
        let mut input_row = Row::zeroed(&op.input_schema);
        input_row.write_inline_str(0, op.input_schema.inline_str_cap, "u1");
        input_row.write_f64(16, 1.5);
        let right_val = serde_json::json!({"country": "US", "tier": "gold"});
        let out = op.enrich_from_value(&input_row, Some(&right_val));
        assert_eq!(
            out.read_inline_str(24, op.enriched_schema.inline_str_cap),
            "US"
        );
        assert_eq!(
            out.read_inline_str(40, op.enriched_schema.inline_str_cap),
            "gold"
        );
    }

    #[test]
    fn derive_enriched_schema_marks_projected_fields_nullable() {
        let input = input_schema();
        let projections: Vec<(&str, FieldTy)> = vec![("country", FieldTy::InlineStr)];
        let enriched = derive_enriched_schema(&input, &projections, input.inline_str_cap);
        // Input fields preserve their nullable flag; projected field
        // must be nullable (right row may be missing).
        let c = enriched
            .fields
            .iter()
            .find(|f| f.name == "country")
            .expect("present");
        assert!(c.nullable, "projected fields are nullable");
    }
}
