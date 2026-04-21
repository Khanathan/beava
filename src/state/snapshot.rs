//! Snapshot persistence: OperatorState enum, serializable state types,
//! save/load functions with versioning.
//!
//! OperatorState replaces Box<dyn Operator> throughout the codebase,
//! making EntityState fully serializable with serde/postcard.
//!
//! v1.1: Snapshot format v4 with per-stream grouped state via
//! SerializableStreamEntityState. v3 snapshots are gracefully rejected.
//!
//! Phase 24 (v6 → v7): `SerializableEntityState` grows a `table_rows` field
//! holding first-class Table rows (Live or Tombstoned). The v6 layout is
//! preserved as `SerializableEntityStateV6` + `BaseSnapshotStateV6` /
//! `DeltaSnapshotStateV6` and migrated on read by initializing `table_rows`
//! to empty for each entity. No other field changes — streams, static
//! features, pipelines, and backfill markers all carry over as-is.
//!
//! Phase 52-01 (v7 → v8): `BaseSnapshotStateV8` adds `shard_count: u16` and
//! `replica_lsn_map: HashMap<(StreamName, UpstreamShardId), u64>` for TPC
//! shard-count boot guard (TPC-CORR-02) and LSN-based dedup (D-11). Reads
//! both v7 and v8; writes v8 only. v7 snapshots promote with shard_count=1
//! and an empty replica_lsn_map. The `check_shard_count_guard` function in
//! `store.rs` triggers a hard-fail boot refusal when `snapshot.shard_count !=
//! BEAVA_SHARDS`.

use crate::engine::hll::DistinctCountOp;
use crate::engine::operators::{
    AvgOp, CountOp, EmaOp, ExactMaxOp, ExactMinOp, FirstNOp, FirstOp, LagOp, LastNOp, LastOp,
    MaxOp, MinOp, Operator, PercentileOp, StddevOp, StreamJoinBuffer, SumOp, TopKOp, VarianceOp,
};
use crate::error::BeavaError;
use crate::state::store::{SerializableTableRow, StaticFeature};
use crate::types::FeatureValue;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;

/// Snapshot format version byte. Prepended to serialized data.
/// If the version doesn't match on load, return None (clean startup from empty state).
/// v6 (Phase 9, OPS-03/OPS-04): adds base/delta snapshot type discriminator byte
/// for incremental snapshots.
/// v7 (Phase 24): `SerializableEntityState` grows `table_rows` for first-class
/// Table row storage. v6 snapshots migrate transparently — see `load_snapshot`.
/// v8 (Phase 52-01): `BaseSnapshotStateV8` adds `shard_count: u16` and
/// `replica_lsn_map` for TPC shard-count boot guard (TPC-CORR-02) and LSN dedup.
/// v7 snapshots read and promoted with shard_count=1.
/// v9 (Phase 55-03): identical on-disk body to v8 (`BaseSnapshotStateV8`), but the
/// embedded `SnapshotHeader.schema_version` is `9` (vs serde-default `8` for pre-55
/// bytes). Loading a v8-outer-byte snapshot or a snapshot whose header reads
/// `schema_version < 9` triggers `rematerialize_tables_from_event_logs` at boot
/// (src/state/recovery.rs). Writer always emits v9 going forward; v8 bytes are
/// still accepted by the reader. Pre-Phase-55 binaries clamp their outer-byte
/// dispatch at `V8_FORMAT` and therefore reject v9 bytes (Pitfall 3, intentional
/// forward-compat break).
pub const SNAPSHOT_FORMAT_VERSION: u8 = 9;

/// Phase 55-03: explicit outer-format byte for Phase 52-era snapshots (v8).
/// v8 is STILL ACCEPTED by the reader (serde-default `schema_version = 8` triggers
/// rematerialization at boot). Writer no longer emits v8 — new writes use
/// `V9_FORMAT`. See `SNAPSHOT_FORMAT_VERSION` docs.
pub const V8_FORMAT: u8 = 8;

/// Phase 55-03: outer-format byte for Phase-55+ snapshots. Equal to
/// `SNAPSHOT_FORMAT_VERSION`; exported separately so the boot-guard /
/// rematerialization module can reason about v8-vs-v9 bytes explicitly.
pub const V9_FORMAT: u8 = 9;

/// Phase 57-01: semantic schema version for Phase-57+ snapshots.
///
/// v10 is an additive-only bump: new writes tag
/// `SnapshotHeader.schema_version = 10` to indicate the binary knows about
/// cross-shard retraction tracking (`ContribSet` on `EntityState` —
/// Phase 57 TPC-CORR-10). The on-disk body format is identical to v9 this
/// wave — `SerializableEntityState` does NOT yet carry
/// `contributing_inputs` on the wire; persistence lands with operator
/// wiring in Waves 2/3.
///
/// Loading:
/// - v10 outer byte → decoded as-is; `schema_version` reads `10`.
/// - v9 outer byte → decoded as-is; `schema_version` reads `9`. Retraction
///   logic treats any row from a v9 snapshot as "contributing_inputs = None"
///   (D-A5 — the "cannot-retract" semantic, same as events beyond
///   `history_ttl`).
/// - v8 / v7 / v6 / v5 outer bytes → unchanged from Phase 55.
///
/// The outer byte is kept at 9 (`V9_FORMAT`) for this wave — bumping the
/// outer format would require rematerialization logic on boot, which
/// Wave 1 does NOT need (no wire-shape change). A future wave can bump
/// the outer byte when `SerializableEntityState` gains a
/// `contributing_inputs` field on the wire.
pub const V10_SCHEMA_VERSION: u16 = 10;

/// Phase 59.6 Wave 4 (TPC-PERF-11, D-D3) — snapshot format v11 scaffold.
///
/// v11 adds typed-row entity state storage:
/// `entity_state_typed: AHashMap<(stream, key), Row>` serialized as
/// `schema_id + payload + arena` per entity. Wave 4 declares the format
/// byte + schema version constants **but does not yet implement**
/// writer/reader plumbing — that lands in Wave 5 (state store +
/// fallback cleanup, 59.6-05). Writers keep emitting v9 outer bytes;
/// readers dispatch on the outer byte (v11 → typed-state path when
/// Wave 5 lands; v9/v10 → Value fallback stays GREEN).
///
/// Declaring these now lets Wave-5 planning freeze the on-disk contract
/// without introducing a pre-5 format clash — any build that encounters
/// a v11 outer byte before Wave 5 can `match` the constant and return
/// a clear "snapshot requires Wave 5+" error.
pub const V11_FORMAT: u8 = 11;
pub const V11_SCHEMA_VERSION: u16 = 11;

/// Phase 59.6 Wave 4 (TPC-PERF-11) — marker struct reserved for typed
/// per-entity agg state serialization.
///
/// Named `TypedAggState` to match the Wave 4 grep invariant
/// `grep -cE 'TypedAggState|agg_state_typed' src/state/snapshot.rs`.
/// The actual wire shape (schema_id, payload, arena) is implemented in
/// Wave 5's v11 body writer/reader — Wave 4 only reserves the name so
/// downstream planners can reference it in CONTEXT + future PLAN bodies.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TypedAggState {
    /// Associated schema id — matches
    /// [`crate::engine::schema::RegisteredSchema::schema_id`]. Wave 5
    /// consumers look up the `RegisteredSchema` by this id for on-disk
    /// payload interpretation.
    pub schema_id: u32,
    /// Packed-row payload bytes. See
    /// [`crate::engine::schema::Row::payload`] — length equals
    /// `RegisteredSchema.row_size`.
    pub payload: Vec<u8>,
    /// Per-row arena (long strings + bytes). See
    /// [`crate::engine::schema::Row::arena`].
    pub arena: Vec<u8>,
}

/// Serde default for Phase 55's new `SnapshotHeader.schema_version` field.
/// When a pre-Phase-55 snapshot (no field on the wire) is deserialized, this
/// fills in `8` — the semantic pre-cross-shard-cascade version. The boot guard
/// in `src/main.rs` / `src/state/recovery.rs` uses this to detect v8-era bytes
/// and trigger downstream-table rematerialization through the new cross-shard
/// cascade path. See Phase 55-03 Plan D-C1.
fn default_v8() -> u16 {
    8
}

/// Legacy v5 format version byte. Used by `load_legacy_v5` to migrate
/// existing single-file snapshots to v6 on first startup.
pub const LEGACY_V5_FORMAT: u8 = 5;

/// Legacy v6 format version byte. Phase 24 added `table_rows` to
/// `SerializableEntityState`; v6 snapshots are migrated on read by
/// initializing each entity's `table_rows` to empty.
pub const LEGACY_V6_FORMAT: u8 = 6;

/// Legacy v7 format version byte. Phase 52-01 added `shard_count` and
/// `replica_lsn_map` to `BaseSnapshotStateV8`; v7 snapshots are promoted on
/// read with shard_count=1 and an empty replica_lsn_map.
pub const LEGACY_V7_FORMAT: u8 = 7;

/// Type tag byte following the version byte in a v6 snapshot file.
/// 0x00 = full base snapshot, 0x01 = incremental delta snapshot.
const TYPE_TAG_BASE: u8 = 0x00;
const TYPE_TAG_DELTA: u8 = 0x01;

/// Serializable enum wrapping all operator types.
/// Replaces Box<dyn Operator> so EntityState can be serialized.
/// Phase 5 adds: Min(MinOp), Max(MaxOp), Last(LastOp), DistinctCount(DistinctCountOp)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OperatorState {
    Count(CountOp),
    Sum(SumOp),
    Avg(AvgOp),
    Min(MinOp),
    Max(MaxOp),
    Last(LastOp),
    DistinctCount(DistinctCountOp),
    Stddev(StddevOp),
    Percentile(PercentileOp),
    Lag(LagOp),
    Ema(EmaOp),
    LastN(LastNOp),
    First(FirstOp),
    ExactMin(ExactMinOp),
    ExactMax(ExactMaxOp),
    // Phase 22-01: v0 operator additions. Bodies stubbed; 22-02/03 fill them.
    Variance(VarianceOp),
    TopK(TopKOp),
    FirstN(FirstNOp),
    // Phase 23-02: Stream↔Stream symmetric interval join buffer.
    // State-only: the cascade mutates it directly via probe/insert/evict.
    StreamJoinBuffer(StreamJoinBuffer),
}

impl OperatorState {
    pub fn push(
        &mut self,
        event: &serde_json::Value,
        enrichment: Option<&ahash::AHashMap<String, serde_json::Value>>,
        now: SystemTime,
    ) -> Result<(), BeavaError> {
        match self {
            Self::Count(op) => op.push(event, enrichment, now),
            Self::Sum(op) => op.push(event, enrichment, now),
            Self::Avg(op) => op.push(event, enrichment, now),
            Self::Min(op) => op.push(event, enrichment, now),
            Self::Max(op) => op.push(event, enrichment, now),
            Self::Last(op) => op.push(event, enrichment, now),
            Self::DistinctCount(op) => op.push(event, enrichment, now),
            Self::Stddev(op) => op.push(event, enrichment, now),
            Self::Percentile(op) => op.push(event, enrichment, now),
            Self::Lag(op) => op.push(event, enrichment, now),
            Self::Ema(op) => op.push(event, enrichment, now),
            Self::LastN(op) => op.push(event, enrichment, now),
            Self::First(op) => op.push(event, enrichment, now),
            Self::ExactMin(op) => op.push(event, enrichment, now),
            Self::ExactMax(op) => op.push(event, enrichment, now),
            Self::Variance(op) => op.push(event, enrichment, now),
            Self::TopK(op) => op.push(event, enrichment, now),
            Self::FirstN(op) => op.push(event, enrichment, now),
            Self::StreamJoinBuffer(op) => op.push(event, enrichment, now),
        }
    }

    pub fn read(&mut self, now: SystemTime) -> FeatureValue {
        match self {
            Self::Count(op) => op.read(now),
            Self::Sum(op) => op.read(now),
            Self::Avg(op) => op.read(now),
            Self::Min(op) => op.read(now),
            Self::Max(op) => op.read(now),
            Self::Last(op) => op.read(now),
            Self::DistinctCount(op) => op.read(now),
            Self::Stddev(op) => op.read(now),
            Self::Percentile(op) => op.read(now),
            Self::Lag(op) => op.read(now),
            Self::Ema(op) => op.read(now),
            Self::LastN(op) => op.read(now),
            Self::First(op) => op.read(now),
            Self::ExactMin(op) => op.read(now),
            Self::ExactMax(op) => op.read(now),
            Self::Variance(op) => op.read(now),
            Self::TopK(op) => op.read(now),
            Self::FirstN(op) => op.read(now),
            Self::StreamJoinBuffer(op) => op.read(now),
        }
    }

    /// Estimate the heap memory usage of this operator in bytes.
    pub fn estimated_bytes(&self) -> usize {
        use crate::engine::operators::Operator;
        match self {
            Self::Count(op) => op.estimated_bytes(),
            Self::Sum(op) => op.estimated_bytes(),
            Self::Avg(op) => op.estimated_bytes(),
            Self::Min(op) => op.estimated_bytes(),
            Self::Max(op) => op.estimated_bytes(),
            Self::Last(op) => op.estimated_bytes(),
            Self::DistinctCount(op) => op.estimated_bytes(),
            Self::Stddev(op) => op.estimated_bytes(),
            Self::Percentile(op) => op.estimated_bytes(),
            Self::Lag(op) => op.estimated_bytes(),
            Self::Ema(op) => op.estimated_bytes(),
            Self::LastN(op) => op.estimated_bytes(),
            Self::First(op) => op.estimated_bytes(),
            Self::ExactMin(op) => op.estimated_bytes(),
            Self::ExactMax(op) => op.estimated_bytes(),
            Self::Variance(op) => op.estimated_bytes(),
            Self::TopK(op) => op.estimated_bytes(),
            Self::FirstN(op) => op.estimated_bytes(),
            Self::StreamJoinBuffer(op) => op.estimated_bytes(),
        }
    }

    /// Number of ring buffer buckets, or 0 for non-windowed operators.
    pub fn num_buckets(&self) -> usize {
        use crate::engine::operators::Operator;
        match self {
            Self::Count(op) => op.num_buckets(),
            Self::Sum(op) => op.num_buckets(),
            Self::Avg(op) => op.num_buckets(),
            Self::Min(op) => op.num_buckets(),
            Self::Max(op) => op.num_buckets(),
            Self::Last(op) => op.num_buckets(),
            Self::DistinctCount(op) => op.num_buckets(),
            Self::Stddev(op) => op.num_buckets(),
            Self::Percentile(op) => op.num_buckets(),
            Self::Lag(op) => op.num_buckets(),
            Self::Ema(op) => op.num_buckets(),
            Self::LastN(op) => op.num_buckets(),
            Self::First(op) => op.num_buckets(),
            Self::ExactMin(op) => op.num_buckets(),
            Self::ExactMax(op) => op.num_buckets(),
            Self::Variance(op) => op.num_buckets(),
            Self::TopK(op) => op.num_buckets(),
            Self::FirstN(op) => op.num_buckets(),
            Self::StreamJoinBuffer(op) => op.num_buckets(),
        }
    }

    /// Hybrid telemetry for exact→sketch operators. Returns `None` for
    /// non-hybrid operators. Surfaced in `/debug/key/:key`.
    pub fn hybrid_telemetry(&self) -> Option<crate::engine::operators::HybridTelemetry> {
        use crate::engine::operators::Operator;
        match self {
            Self::Percentile(op) => op.hybrid_telemetry(),
            Self::DistinctCount(op) => op.hybrid_telemetry(),
            Self::TopK(op) => op.hybrid_telemetry(),
            _ => None,
        }
    }

    /// Human-readable operator type name.
    pub fn operator_type_name(&self) -> &'static str {
        match self {
            Self::Count(_) => "count",
            Self::Sum(_) => "sum",
            Self::Avg(_) => "avg",
            Self::Min(_) => "min",
            Self::Max(_) => "max",
            Self::Last(_) => "last",
            Self::DistinctCount(_) => "distinct_count",
            Self::Stddev(_) => "stddev",
            Self::Percentile(_) => "percentile",
            Self::Lag(_) => "lag",
            Self::Ema(_) => "ema",
            Self::LastN(_) => "last_n",
            Self::First(_) => "first",
            Self::ExactMin(_) => "exact_min",
            Self::ExactMax(_) => "exact_max",
            Self::Variance(_) => "variance",
            Self::TopK(_) => "top_k",
            Self::FirstN(_) => "first_n",
            Self::StreamJoinBuffer(_) => "stream_join_buffer",
        }
    }

    /// Return and clear the last ring-buffer drop reason recorded during the
    /// most recent `push()` call. Returns `None` if no drop occurred or if
    /// the operator does not own a `RingBuffer` (e.g. Last, Lag, Ema).
    ///
    /// Used by `push_internal` in `pipeline.rs` to bump
    /// `beava_ring_buffer_drops_total` without changing the `Operator` trait
    /// signature (D-06 / OBS-01).
    pub fn ring_buffer_drop_reason(&mut self) -> Option<crate::engine::event_time::DropReason> {
        match self {
            // Operators that own a RingBuffer<T> as their primary data structure.
            // We read the primary buffer's last_drop (the first buffer that
            // processes the event determines the drop reason; parallel buffers
            // like event_count have the same window so they would produce the
            // same reason).
            Self::Count(op) => op.take_ring_buffer_drop(),
            Self::Sum(op) => op.take_ring_buffer_drop(),
            Self::Avg(op) => op.take_ring_buffer_drop(),
            Self::Min(op) => op.take_ring_buffer_drop(),
            Self::Max(op) => op.take_ring_buffer_drop(),
            Self::Stddev(op) => op.take_ring_buffer_drop(),
            Self::ExactMin(op) => op.take_ring_buffer_drop(),
            Self::ExactMax(op) => op.take_ring_buffer_drop(),
            Self::Variance(op) => op.take_ring_buffer_drop(),
            Self::DistinctCount(op) => op.take_ring_buffer_drop(),
            // Operators that use RetractingRingBuffer or no ring buffer:
            // RetractingRingBuffer always accepts events (advance_to then
            // write to head), so no drop reason is ever set.
            // Non-windowed operators (Last, Lag, Ema, LastN, First, FirstN,
            // StreamJoinBuffer) have no ring buffer at all.
            Self::Percentile(_)
            | Self::TopK(_)
            | Self::Last(_)
            | Self::Lag(_)
            | Self::Ema(_)
            | Self::LastN(_)
            | Self::First(_)
            | Self::FirstN(_)
            | Self::StreamJoinBuffer(_) => None,
        }
    }
}

/// Serializable pipeline definition for snapshot persistence.
/// Stores the raw RegisterRequest JSON as a String so pipelines can be re-parsed on load.
/// Uses String (not serde_json::Value) because postcard cannot serialize serde_json::Value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializablePipeline {
    pub name: String,
    pub key_field: String,
    /// Raw JSON string from the RegisterRequest. Re-parsed via convert_register_request on load.
    pub raw_register_json: String,
}

/// Serializable per-stream entity state for v4 snapshot format.
/// Each stream within an entity has its own operators and last_event_at.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableStreamEntityState {
    pub operators: Vec<(String, OperatorState)>,
    pub last_event_at: Option<SystemTime>,
}

/// Serializable entity state for snapshot persistence.
/// Groups operators by stream name for independent per-stream TTL management.
/// Uses Vec instead of AHashMap for postcard compatibility.
///
/// Phase 24 (v7): added `table_rows` for first-class Table row storage.
/// v6 snapshots are migrated on read via `SerializableEntityStateV6`.
///
/// Phase 57-02 leaves this struct unchanged for top-level SNAPSHOT wire
/// (v8/v9 envelope); the fjall per-entity wire format gains
/// `contributing_inputs` via `SerializableEntityStateV10` — writes go out as
/// V10, reads try V10 then fall back to this V9 layout. See
/// `entity_to_bytes` / `entity_from_bytes` in `shard/mod.rs`. Snapshot
/// envelopes remain on V9 body shape to preserve backward compat with v7
/// fixtures + existing replica snapshot fetch consumers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableEntityState {
    pub streams: Vec<(String, SerializableStreamEntityState)>,
    pub static_features: Vec<(String, StaticFeature)>,
    /// Phase 24: Table rows keyed by table name.
    pub table_rows: Vec<(String, SerializableTableRow)>,
}

/// Phase 57-02 (v10 fjall per-entity wire layout): extends
/// `SerializableEntityState` with `contributing_inputs: Option<ContribSet>`
/// for cross-shard retraction tracking (TPC-CORR-10).
///
/// postcard does NOT support `#[serde(default)]` for missing trailing fields,
/// so adding the field directly to `SerializableEntityState` would break v7
/// fixtures + v8/v9 snapshot envelopes that embed the legacy body. Instead,
/// the per-entity wire format in `entity_to_bytes` / `entity_from_bytes`
/// writes V10 (this struct) and reads try V10 first then fall back to V9
/// (`SerializableEntityState`).
///
/// Pre-Phase-57 rows loaded via the V9 fallback get `contributing_inputs =
/// None` (D-A5 "cannot-retract" semantic).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableEntityStateV10 {
    pub streams: Vec<(String, SerializableStreamEntityState)>,
    pub static_features: Vec<(String, StaticFeature)>,
    pub table_rows: Vec<(String, SerializableTableRow)>,
    /// Phase 57-02: contributing-inputs tracking record (TPC-CORR-10).
    pub contributing_inputs: Option<crate::state::store::ContribSet>,
}

/// Phase 24: Legacy v6 per-entity layout (no `table_rows`). Used exclusively
/// for decoding v6 snapshot files on first startup after a v7 binary upgrade.
/// Promoted to `SerializableEntityState` by defaulting `table_rows` to empty.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableEntityStateV6 {
    pub streams: Vec<(String, SerializableStreamEntityState)>,
    pub static_features: Vec<(String, StaticFeature)>,
}

impl From<SerializableEntityStateV6> for SerializableEntityState {
    fn from(v6: SerializableEntityStateV6) -> Self {
        SerializableEntityState {
            streams: v6.streams,
            static_features: v6.static_features,
            table_rows: Vec::new(),
        }
    }
}

/// Top-level serializable snapshot state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotState {
    pub entities: Vec<(String, SerializableEntityState)>,
    pub pipelines: Vec<SerializablePipeline>,
    /// Set of (stream_name, feature_name) pairs that have completed backfill.
    /// Used on restart to detect incomplete backfills.
    #[serde(default)]
    pub backfill_complete: Vec<(String, String)>,
}

// ================ Phase 9: v6 Incremental Snapshot Format ================

/// Type discriminator: base (full) or delta (incremental).
/// Delta variants carry the sequence number of the base they were taken
/// against so recovery can validate the chain.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SnapshotType {
    Base,
    Delta { base_seq: u64 },
}

/// Header present in all v6+ snapshots. Carries the snapshot type and a
/// monotonic sequence number used to order files during recovery.
///
/// Phase 55-03 added `schema_version: u16` with `#[serde(default = "default_v8")]`.
/// A pre-Phase-55 snapshot on the wire (no field) decodes as `schema_version == 8`
/// via an internal wire-compat shim (`SnapshotHeaderV8Wire` + conversion); a
/// Phase-55+ writer emits `9`. The boot guard in `src/main.rs` /
/// `src/state/recovery.rs` triggers downstream-table rematerialization when the
/// loaded snapshot's `schema_version < 9` (the pre-cross-shard-cascade era wrote
/// downstream TT rows onto the input event's shard — a correctness bug the
/// rematerializer rebuilds away from).
///
/// Wire-compat note: postcard (used for the on-disk encoding) does NOT
/// synthesize missing trailing fields from `#[serde(default)]`. The v8 load
/// path therefore decodes into `BaseSnapshotStateV8Wire` (with a
/// `SnapshotHeaderV8Wire` body that lacks `schema_version`) and converts to
/// `BaseSnapshotStateV8` with `schema_version = 8` before returning. The
/// `#[serde(default = "default_v8")]` attribute is still present so that
/// self-describing formats (JSON in the admin HTTP surface) see the v8
/// default, and it documents the semantic intent at the type level.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotHeader {
    pub snapshot_type: SnapshotType,
    pub sequence: u64,
    /// Phase 55/57: semantic schema version.
    ///   8 = pre-cross-shard-cascade (downstream rows on input event's shard) — BUG.
    ///   9 = post-Phase-55 (downstream rows on hash(output_key) shard) — CORRECT.
    ///  10 = post-Phase-57 (binary knows about ContribSet / retraction tracking).
    ///       Wire format identical to v9 in Wave 1 — schema bump is semantic.
    /// Boot guard triggers rematerialization when loaded `< 9` (unchanged).
    #[serde(default = "default_v8")]
    pub schema_version: u16,
}

/// Phase 55-03 wire-compat shim: the v8 on-disk header layout (no
/// `schema_version` field). Used internally to decode v8-outer-byte bytes
/// under postcard, which does not support `#[serde(default)]` for missing
/// trailing fields. Converts to `SnapshotHeader` via `From` with
/// `schema_version = 8`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotHeaderV8Wire {
    snapshot_type: SnapshotType,
    sequence: u64,
}

impl From<SnapshotHeaderV8Wire> for SnapshotHeader {
    fn from(w: SnapshotHeaderV8Wire) -> Self {
        SnapshotHeader {
            snapshot_type: w.snapshot_type,
            sequence: w.sequence,
            schema_version: 8,
        }
    }
}

/// Phase 55-03 wire-compat: v8-wire body (decoded from v8 outer-byte snapshots).
/// Uses `SnapshotHeaderV8Wire` (no `schema_version` field) so postcard decodes
/// cleanly. Converts to `BaseSnapshotStateV8` with `schema_version = 8`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BaseSnapshotStateV8Wire {
    header: SnapshotHeaderV8Wire,
    entities: Vec<(String, SerializableEntityState)>,
    pipelines: Vec<SerializablePipeline>,
    #[serde(default)]
    backfill_complete: Vec<(String, String)>,
    shard_count: u16,
    #[serde(default)]
    replica_lsn_map: HashMap<(String, u8), u64>,
}

impl From<BaseSnapshotStateV8Wire> for BaseSnapshotStateV8 {
    fn from(w: BaseSnapshotStateV8Wire) -> Self {
        BaseSnapshotStateV8 {
            header: w.header.into(),
            entities: w.entities,
            pipelines: w.pipelines,
            backfill_complete: w.backfill_complete,
            shard_count: w.shard_count,
            replica_lsn_map: w.replica_lsn_map,
        }
    }
}

/// Phase 55-03 wire-compat: v8-wire delta body.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeltaSnapshotStateV8Wire {
    header: SnapshotHeaderV8Wire,
    changed_entities: Vec<(String, SerializableEntityState)>,
    deleted_keys: Vec<String>,
}

impl From<DeltaSnapshotStateV8Wire> for DeltaSnapshotState {
    fn from(w: DeltaSnapshotStateV8Wire) -> Self {
        DeltaSnapshotState {
            header: w.header.into(),
            changed_entities: w.changed_entities,
            deleted_keys: w.deleted_keys,
        }
    }
}

/// Phase 55-03 wire-compat: v6/v7 body shims. v6 and v7 on-disk bytes also pre-date
/// the `schema_version` field, so we use dedicated wire types + conversion.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BaseSnapshotStateV7Wire {
    header: SnapshotHeaderV8Wire,
    entities: Vec<(String, SerializableEntityState)>,
    pipelines: Vec<SerializablePipeline>,
    #[serde(default)]
    backfill_complete: Vec<(String, String)>,
}

impl From<BaseSnapshotStateV7Wire> for BaseSnapshotState {
    fn from(w: BaseSnapshotStateV7Wire) -> Self {
        BaseSnapshotState {
            header: w.header.into(),
            entities: w.entities,
            pipelines: w.pipelines,
            backfill_complete: w.backfill_complete,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BaseSnapshotStateV6Wire {
    header: SnapshotHeaderV8Wire,
    entities: Vec<(String, SerializableEntityStateV6)>,
    pipelines: Vec<SerializablePipeline>,
    #[serde(default)]
    backfill_complete: Vec<(String, String)>,
}

impl From<BaseSnapshotStateV6Wire> for BaseSnapshotStateV6 {
    fn from(w: BaseSnapshotStateV6Wire) -> Self {
        BaseSnapshotStateV6 {
            header: w.header.into(),
            entities: w.entities,
            pipelines: w.pipelines,
            backfill_complete: w.backfill_complete,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeltaSnapshotStateV6Wire {
    header: SnapshotHeaderV8Wire,
    changed_entities: Vec<(String, SerializableEntityStateV6)>,
    deleted_keys: Vec<String>,
}

impl From<DeltaSnapshotStateV6Wire> for DeltaSnapshotStateV6 {
    fn from(w: DeltaSnapshotStateV6Wire) -> Self {
        DeltaSnapshotStateV6 {
            header: w.header.into(),
            changed_entities: w.changed_entities,
            deleted_keys: w.deleted_keys,
        }
    }
}

/// Full base snapshot state (v6). Contains everything needed for standalone
/// recovery: all entities, all pipelines, and all backfill markers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaseSnapshotState {
    pub header: SnapshotHeader,
    pub entities: Vec<(String, SerializableEntityState)>,
    pub pipelines: Vec<SerializablePipeline>,
    #[serde(default)]
    pub backfill_complete: Vec<(String, String)>,
}

/// Delta snapshot: only changed entities since the last snapshot plus the
/// set of keys that were evicted or deleted. Applied on top of a base by
/// `StateStore::apply_delta`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaSnapshotState {
    pub header: SnapshotHeader,
    pub changed_entities: Vec<(String, SerializableEntityState)>,
    pub deleted_keys: Vec<String>,
}

// ================ Phase 24: v6 legacy Base/Delta types for migration ================

/// Phase 24: Legacy v6 base snapshot layout. Identical to `BaseSnapshotState`
/// except entities use `SerializableEntityStateV6` (no `table_rows`).
/// Used only to deserialize v6 snapshots before promoting them to v7.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaseSnapshotStateV6 {
    pub header: SnapshotHeader,
    pub entities: Vec<(String, SerializableEntityStateV6)>,
    pub pipelines: Vec<SerializablePipeline>,
    #[serde(default)]
    pub backfill_complete: Vec<(String, String)>,
}

/// Phase 24: Legacy v6 delta snapshot layout (parallel to `BaseSnapshotStateV6`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaSnapshotStateV6 {
    pub header: SnapshotHeader,
    pub changed_entities: Vec<(String, SerializableEntityStateV6)>,
    pub deleted_keys: Vec<String>,
}

// ================ Phase 52-01: v7 legacy types and v8 new types ================

/// Phase 52-01: Legacy v7 base snapshot layout. Identical to `BaseSnapshotState`
/// (v7 = current before this phase). Used only to deserialize v7 snapshots before
/// promoting them to v8 with `shard_count=1` and empty `replica_lsn_map`.
///
/// `BaseSnapshotState` (the current type without v8 fields) serves as v7 on-disk
/// layout. We alias it here for clarity in the migration path.
pub type BaseSnapshotStateV7 = BaseSnapshotState;

/// Phase 52-01: v8 base snapshot layout. Extends v7 with:
/// - `shard_count: u16` — TPC shard count at snapshot write time. Used by
///   the boot guard (TPC-CORR-02) to refuse boot when `shard_count !=
///   BEAVA_SHARDS`.
/// - `replica_lsn_map: HashMap<(StreamName, UpstreamShardId), u64>` — per
///   (stream, upstream shard) LSN watermark for dedup on reconnect (D-11).
///   `#[serde(default)]` so older-era v8 snapshots (before the map was
///   populated) load cleanly with an empty map.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaseSnapshotStateV8 {
    pub header: SnapshotHeader,
    pub entities: Vec<(String, SerializableEntityState)>,
    pub pipelines: Vec<SerializablePipeline>,
    #[serde(default)]
    pub backfill_complete: Vec<(String, String)>,
    /// TPC shard count at snapshot write time. Defaults to 1 for v7-promoted
    /// snapshots (added via `#[serde(default = "default_shard_count")]` during
    /// v7→v8 promotion, not stored in v7 bytes).
    pub shard_count: u16,
    /// Per-(stream, upstream_shard_id) LSN watermark for LSN-based dedup (D-11).
    /// `#[serde(default)]` so snapshots written before LSN population (e.g. v8
    /// snapshots from 52-01 before 52-06 lands) load cleanly with an empty map.
    #[serde(default)]
    pub replica_lsn_map: HashMap<(String, u8), u64>,
}

impl From<BaseSnapshotStateV6> for BaseSnapshotState {
    fn from(v6: BaseSnapshotStateV6) -> Self {
        BaseSnapshotState {
            header: v6.header,
            entities: v6
                .entities
                .into_iter()
                .map(|(k, e)| (k, e.into()))
                .collect(),
            pipelines: v6.pipelines,
            backfill_complete: v6.backfill_complete,
        }
    }
}

impl From<DeltaSnapshotStateV6> for DeltaSnapshotState {
    fn from(v6: DeltaSnapshotStateV6) -> Self {
        DeltaSnapshotState {
            header: v6.header,
            changed_entities: v6
                .changed_entities
                .into_iter()
                .map(|(k, e)| (k, e.into()))
                .collect(),
            deleted_keys: v6.deleted_keys,
        }
    }
}

/// Wrapper returned by `load_snapshot_file` that preserves the on-disk type.
/// Phase 52-01: Base variant now carries `BaseSnapshotStateV8` to expose
/// `shard_count` and `replica_lsn_map` to the boot guard and LSN dedup paths.
#[derive(Debug, Clone)]
pub enum SnapshotFile {
    Base(BaseSnapshotStateV8),
    Delta(DeltaSnapshotState),
}

/// Serialize a full `SnapshotState` to bytes in v6 base format. This is the
/// legacy entry point used by existing callers; it wraps the data in a
/// `BaseSnapshotState` with a default (zero) sequence number.
///
/// Format: `[version=6][type_tag=0x00][postcard(BaseSnapshotState)]`
///
/// Returns an error if postcard serialization fails.
pub fn save_snapshot(data: &SnapshotState) -> Result<Vec<u8>, postcard::Error> {
    let base = BaseSnapshotState {
        header: SnapshotHeader {
            snapshot_type: SnapshotType::Base,
            sequence: 0,
            // Phase 57-01: new writes tag schema_version=10 (contributing_inputs
            // now tracked on EntityState, in-memory only in Wave 1). The outer
            // byte stays at V9_FORMAT — v10 is an additive schema bump that
            // changes no wire layout; loaders treat v9 bytes with
            // schema_version<10 as "cannot-retract" per D-A5.
            schema_version: V10_SCHEMA_VERSION,
        },
        entities: data.entities.clone(),
        pipelines: data.pipelines.clone(),
        backfill_complete: data.backfill_complete.clone(),
    };
    save_base_snapshot(&base)
}

/// Deserialize a `SnapshotState` from bytes. Accepts v5, v6, v7, and v8 base
/// snapshots. Delta snapshots are rejected by this legacy API (use
/// `load_snapshot_file` for the generic path).
///
/// Returns None if:
/// - bytes is empty
/// - version byte is not v5, v6, v7, or v8
/// - type tag is not base (0x00)
/// - postcard deserialization fails (corrupt data)
pub fn load_snapshot(bytes: &[u8]) -> Option<SnapshotState> {
    if bytes.is_empty() {
        return None;
    }
    let version = bytes[0];
    // Legacy v5 path: transparently migrate on read.
    if version == LEGACY_V5_FORMAT {
        return postcard::from_bytes(&bytes[1..]).ok();
    }
    // Phase 24: Legacy v6 path — deserialize with SerializableEntityStateV6
    // and promote each entity to v7 with an empty `table_rows` map.
    // Phase 55-03: decoded via wire-compat shim (postcard lacks serde-default
    // for trailing fields; v6 bytes have no schema_version field).
    if version == LEGACY_V6_FORMAT {
        if bytes.len() < 2 || bytes[1] != TYPE_TAG_BASE {
            return None;
        }
        let wire: BaseSnapshotStateV6Wire = postcard::from_bytes(&bytes[2..]).ok()?;
        let base_v6: BaseSnapshotStateV6 = wire.into();
        let base: BaseSnapshotState = base_v6.into();
        return Some(SnapshotState {
            entities: base.entities,
            pipelines: base.pipelines,
            backfill_complete: base.backfill_complete,
        });
    }
    // Phase 52-01: Legacy v7 path — decode as BaseSnapshotState (v7 layout).
    // Phase 55-03: wire-compat shim for pre-schema_version bytes.
    if version == LEGACY_V7_FORMAT {
        if bytes.len() < 2 {
            return None;
        }
        if bytes[1] != TYPE_TAG_BASE {
            return None;
        }
        let wire: BaseSnapshotStateV7Wire = postcard::from_bytes(&bytes[2..]).ok()?;
        let base: BaseSnapshotState = wire.into();
        return Some(SnapshotState {
            entities: base.entities,
            pipelines: base.pipelines,
            backfill_complete: base.backfill_complete,
        });
    }
    // Phase 55-03: accept both V8_FORMAT and V9_FORMAT as "base snapshot" bytes
    // for this legacy API. The two share an identical on-disk body layout; only
    // the outer byte + `schema_version` header field differ. v8 bytes decode
    // through the `V8Wire` shim (header lacks `schema_version` → fills in 8);
    // v9 bytes decode directly into `BaseSnapshotStateV8` (header carries 9).
    // Unknown version bytes → None (Pitfall 3 guard for forward-compat:
    // pre-Phase-55 binaries see 0x09 here and bail out).
    if version != V8_FORMAT && version != V9_FORMAT {
        // Intentional: startup status (Phase 47 audit)
        eprintln!(
            "Snapshot version mismatch: found {}, expected {} or {}. Starting fresh.",
            version, V8_FORMAT, V9_FORMAT
        );
        return None;
    }
    // v8/v9 path: must be a base snapshot for this legacy API.
    if bytes.len() < 2 {
        return None;
    }
    if bytes[1] != TYPE_TAG_BASE {
        // Delta snapshots must go through load_snapshot_file.
        return None;
    }
    let base: BaseSnapshotStateV8 = if version == V8_FORMAT {
        let wire: BaseSnapshotStateV8Wire = postcard::from_bytes(&bytes[2..]).ok()?;
        wire.into()
    } else {
        postcard::from_bytes(&bytes[2..]).ok()?
    };
    Some(SnapshotState {
        entities: base.entities,
        pipelines: base.pipelines,
        backfill_complete: base.backfill_complete,
    })
}

// ================ Phase 9: v6 Save/Load Functions ================

/// Serialize a `BaseSnapshotState` (v7 layout) wrapped in v8/v9 envelope.
/// Phase 52-01 added promotion to `BaseSnapshotStateV8` with `shard_count=1` and
/// empty `replica_lsn_map`. Phase 55-03 bumped the outer byte to `V9_FORMAT` for
/// all new writes; the body type (`BaseSnapshotStateV8`) is unchanged — only the
/// outer version byte and the embedded `SnapshotHeader.schema_version` differ.
///
/// Format: `[version=9][type_tag=0x00][postcard(BaseSnapshotStateV8)]`
pub fn save_base_snapshot(data: &BaseSnapshotState) -> Result<Vec<u8>, postcard::Error> {
    let v8 = BaseSnapshotStateV8 {
        header: data.header.clone(),
        entities: data.entities.clone(),
        pipelines: data.pipelines.clone(),
        backfill_complete: data.backfill_complete.clone(),
        shard_count: 1,
        replica_lsn_map: HashMap::new(),
    };
    save_base_snapshot_v8(&v8)
}

/// Serialize a `BaseSnapshotStateV8` in v9 format (identical body to v8).
/// Format: `[version=9][type_tag=0x00][postcard(BaseSnapshotStateV8)]`
///
/// Phase 55-03: outer byte is `SNAPSHOT_FORMAT_VERSION=9`. The v8 body type is
/// retained as the on-disk schema; only the outer byte + header's
/// `schema_version` field changed. See `SNAPSHOT_FORMAT_VERSION` docs.
pub fn save_base_snapshot_v8(data: &BaseSnapshotStateV8) -> Result<Vec<u8>, postcard::Error> {
    let mut buf = vec![SNAPSHOT_FORMAT_VERSION, TYPE_TAG_BASE];
    buf.extend_from_slice(&postcard::to_stdvec(data)?);
    Ok(buf)
}

/// Serialize a `DeltaSnapshotState` in v9 format (Phase 55-03).
/// Format: `[version=9][type_tag=0x01][postcard(DeltaSnapshotState)]`
pub fn save_delta_snapshot(data: &DeltaSnapshotState) -> Result<Vec<u8>, postcard::Error> {
    let mut buf = vec![SNAPSHOT_FORMAT_VERSION, TYPE_TAG_DELTA];
    buf.extend_from_slice(&postcard::to_stdvec(data)?);
    Ok(buf)
}

/// Load a snapshot file (base or delta) from bytes. Accepts v6, v7, and v8.
/// Returns None on unknown version, unknown type tag, or corrupt data.
///
/// Version dispatch:
/// - v6: decode as BaseSnapshotStateV6 / DeltaSnapshotStateV6, promote entities
///       (add empty table_rows), then wrap in BaseSnapshotStateV8 with shard_count=1.
/// - v7: decode as BaseSnapshotState (v7 layout), promote to BaseSnapshotStateV8
///       with shard_count=1 and empty replica_lsn_map.
/// - v8: decode directly as BaseSnapshotStateV8 / DeltaSnapshotState.
/// - unknown: return None (T-52-01-01: unknown version → Err, not panic).
///
/// Security: postcard deserialization rejects malformed input via Result;
/// we convert any error to None to match the rest of the snapshot module's
/// "fail closed, start fresh" policy. (Threat register T-09-01, T-52-01-01.)
pub fn load_snapshot_file(bytes: &[u8]) -> Option<SnapshotFile> {
    if bytes.len() < 2 {
        return None;
    }
    // Phase 24: Accept legacy v6 files and migrate them on read.
    // Phase 55-03: decode via V6Wire shim (v6 bytes pre-date schema_version).
    if bytes[0] == LEGACY_V6_FORMAT {
        return match bytes[1] {
            TYPE_TAG_BASE => {
                let wire: BaseSnapshotStateV6Wire = postcard::from_bytes(&bytes[2..]).ok()?;
                let v6: BaseSnapshotStateV6 = wire.into();
                // Promote v6 → BaseSnapshotState (v7: add table_rows) → BaseSnapshotStateV8.
                let v7: BaseSnapshotState = v6.into();
                let v8 = BaseSnapshotStateV8 {
                    header: v7.header,
                    entities: v7.entities,
                    pipelines: v7.pipelines,
                    backfill_complete: v7.backfill_complete,
                    shard_count: 1,
                    replica_lsn_map: HashMap::new(),
                };
                Some(SnapshotFile::Base(v8))
            }
            TYPE_TAG_DELTA => postcard::from_bytes::<DeltaSnapshotStateV6Wire>(&bytes[2..])
                .ok()
                .map(|wire| {
                    let d6: DeltaSnapshotStateV6 = wire.into();
                    SnapshotFile::Delta(d6.into())
                }),
            _ => None,
        };
    }
    // Phase 52-01: Accept legacy v7 files and promote them to v8.
    // Phase 55-03: decode via V7Wire shim.
    if bytes[0] == LEGACY_V7_FORMAT {
        return match bytes[1] {
            TYPE_TAG_BASE => {
                let wire: BaseSnapshotStateV7Wire = postcard::from_bytes(&bytes[2..]).ok()?;
                let v7: BaseSnapshotState = wire.into();
                let v8 = BaseSnapshotStateV8 {
                    header: v7.header,
                    entities: v7.entities,
                    pipelines: v7.pipelines,
                    backfill_complete: v7.backfill_complete,
                    shard_count: 1,
                    replica_lsn_map: HashMap::new(),
                };
                Some(SnapshotFile::Base(v8))
            }
            TYPE_TAG_DELTA => postcard::from_bytes::<DeltaSnapshotStateV8Wire>(&bytes[2..])
                .ok()
                .map(|w| SnapshotFile::Delta(w.into())),
            _ => None,
        };
    }
    // Phase 55-03: both V8_FORMAT (0x08) and V9_FORMAT (0x09) share the
    // BaseSnapshotStateV8 / DeltaSnapshotState body types. For v8 outer bytes,
    // decode via the wire-compat shim (fills in `schema_version = 8`). For v9,
    // decode directly (header carries `schema_version = 9`). Unknown version
    // bytes → None (T-52-01-01 + Pitfall 3).
    if bytes[0] != V8_FORMAT && bytes[0] != V9_FORMAT {
        return None;
    }
    if bytes[0] == V8_FORMAT {
        return match bytes[1] {
            TYPE_TAG_BASE => postcard::from_bytes::<BaseSnapshotStateV8Wire>(&bytes[2..])
                .ok()
                .map(|w| SnapshotFile::Base(w.into())),
            TYPE_TAG_DELTA => postcard::from_bytes::<DeltaSnapshotStateV8Wire>(&bytes[2..])
                .ok()
                .map(|w| SnapshotFile::Delta(w.into())),
            _ => None,
        };
    }
    match bytes[1] {
        TYPE_TAG_BASE => postcard::from_bytes::<BaseSnapshotStateV8>(&bytes[2..])
            .ok()
            .map(SnapshotFile::Base),
        TYPE_TAG_DELTA => postcard::from_bytes::<DeltaSnapshotState>(&bytes[2..])
            .ok()
            .map(SnapshotFile::Delta),
        _ => None,
    }
}

/// Phase 24 test helper: serialize a v6 base snapshot using the legacy v6
/// layout (`[0x06][0x00][postcard(BaseSnapshotStateV6Wire)]`). Phase 55-03
/// encodes through the V6Wire shim so the on-disk bytes match the historical
/// pre-schema_version layout that `load_snapshot_file` expects.
pub fn save_base_snapshot_v6_for_test(
    data: &BaseSnapshotStateV6,
) -> Result<Vec<u8>, postcard::Error> {
    let wire = BaseSnapshotStateV6Wire {
        header: SnapshotHeaderV8Wire {
            snapshot_type: data.header.snapshot_type.clone(),
            sequence: data.header.sequence,
        },
        entities: data.entities.clone(),
        pipelines: data.pipelines.clone(),
        backfill_complete: data.backfill_complete.clone(),
    };
    let mut buf = vec![LEGACY_V6_FORMAT, TYPE_TAG_BASE];
    buf.extend_from_slice(&postcard::to_stdvec(&wire)?);
    Ok(buf)
}

/// Phase 24 test helper: serialize a v6 delta snapshot in the legacy v6
/// layout. Phase 55-03 encodes through the V6Wire shim (see
/// `save_base_snapshot_v6_for_test`).
pub fn save_delta_snapshot_v6_for_test(
    data: &DeltaSnapshotStateV6,
) -> Result<Vec<u8>, postcard::Error> {
    let wire = DeltaSnapshotStateV6Wire {
        header: SnapshotHeaderV8Wire {
            snapshot_type: data.header.snapshot_type.clone(),
            sequence: data.header.sequence,
        },
        changed_entities: data.changed_entities.clone(),
        deleted_keys: data.deleted_keys.clone(),
    };
    let mut buf = vec![LEGACY_V6_FORMAT, TYPE_TAG_DELTA];
    buf.extend_from_slice(&postcard::to_stdvec(&wire)?);
    Ok(buf)
}

/// Phase 52-01 test helper: serialize a v7 base snapshot using the legacy v7
/// layout (`[0x07][0x00][postcard(BaseSnapshotStateV7Wire)]`). Phase 55-03
/// encodes through the V7Wire shim so the on-disk bytes match the historical
/// pre-schema_version layout.
pub fn save_base_snapshot_v7_for_test(
    data: &BaseSnapshotStateV7,
) -> Result<Vec<u8>, postcard::Error> {
    let wire = BaseSnapshotStateV7Wire {
        header: SnapshotHeaderV8Wire {
            snapshot_type: data.header.snapshot_type.clone(),
            sequence: data.header.sequence,
        },
        entities: data.entities.clone(),
        pipelines: data.pipelines.clone(),
        backfill_complete: data.backfill_complete.clone(),
    };
    let mut buf = vec![LEGACY_V7_FORMAT, TYPE_TAG_BASE];
    buf.extend_from_slice(&postcard::to_stdvec(&wire)?);
    Ok(buf)
}

/// Load a legacy v5 single-file snapshot. Used by startup recovery to
/// migrate pre-Phase-9 installations transparently. Returns None if the
/// bytes are empty, start with a non-v5 version byte, or fail to decode.
pub fn load_legacy_v5(bytes: &[u8]) -> Option<SnapshotState> {
    if bytes.is_empty() {
        return None;
    }
    if bytes[0] != LEGACY_V5_FORMAT {
        return None;
    }
    postcard::from_bytes(&bytes[1..]).ok()
}

// ---------------------------------------------------------------------------
// Phase 54-03 Task 1: boot-time snapshot replay to fjall partitions.
//
// Single-writer invariant: this function writes directly via
// `PartitionHandle::insert` BYPASSING the per-shard SPSC inbox. This is safe
// ONLY because the main boot path calls it BEFORE shard threads are spawned
// (see `src/main.rs`: `run_tcp_server` + `spawn_shard_threads` run strictly
// after `load_incremental_snapshots`). No other thread owns any partition
// handle at this point; `PartitionHandle` is single-writer by convention
// (see `src/shard/mod.rs` module-level note).
//
// Routing reproduces `migrate_to_fjall::resolve_shard_key_for_entity`:
//   - entity with no streams → shard 0 (keyless)
//   - first stream's pipeline has empty `key_field` → shard 0
//   - otherwise `shard_hint_for_event({kf: entity_key}, Some(kf)) % n`
// This matches ingest-time routing EXACTLY so post-boot reads for a given
// entity land on the same shard that any ingest event for that entity will.
// ---------------------------------------------------------------------------

/// Restore entities from a snapshot into per-shard fjall partitions.
///
/// Each `(entity_key, SerializableEntityState)` tuple is postcard-encoded and
/// inserted into `partitions[shard_idx]`, where `shard_idx` matches the
/// per-stream `shard_key` routing used by ingest (and by `migrate_to_fjall`).
/// Returns a per-shard count vector of length `partitions.len()`.
///
/// # Errors
///
/// - `InvalidData` if an entity references a stream not present in `pipelines`.
/// - `Other` wrapping `postcard::Error` on serialization failure.
/// - `Other` wrapping `fjall::Error` on partition insert failure.
///
/// # Single-writer safety
///
/// Caller MUST ensure no shard thread has started when this runs. Under
/// the default (fjall) build this is enforced by boot-path ordering in
/// `src/main.rs`: snapshot replay runs before `run_tcp_server` which is
/// where `spawn_shard_threads` lives.
#[cfg(not(feature = "state-inmem"))]
pub fn restore_snapshot_to_shards(
    entities: Vec<(String, SerializableEntityState)>,
    pipelines: &[SerializablePipeline],
    partitions: &[fjall::PartitionHandle],
) -> std::io::Result<Vec<usize>> {
    use std::io::{Error as IoError, ErrorKind};

    let n = partitions.len();
    if n == 0 {
        return Err(IoError::new(
            ErrorKind::InvalidInput,
            "restore_snapshot_to_shards: partitions slice is empty",
        ));
    }
    let mut counts = vec![0usize; n];
    for (entity_key, entity_state) in entities {
        let shard_idx = route_entity_to_shard(&entity_key, &entity_state, pipelines, n)?;
        let bytes = postcard::to_stdvec(&entity_state).map_err(IoError::other)?;
        partitions[shard_idx]
            .insert(entity_key.as_bytes(), bytes)
            .map_err(IoError::other)?;
        counts[shard_idx] += 1;
    }
    // Intentional: startup status (matches Phase 47 eprintln! convention in this module).
    eprintln!(
        "Boot-time snapshot replay complete: {:?} (single-writer, direct fjall insert, no SPSC)",
        counts
    );
    Ok(counts)
}

/// Compute the shard index for an entity using per-stream `shard_key`
/// routing (mirrors `migrate_to_fjall::resolve_shard_key_for_entity`).
///
/// - Entities with no streams → shard 0 (keyless).
/// - Streams whose pipeline has an empty `key_field` → shard 0 (keyless).
/// - Otherwise: `shard_hint_for_event({kf: entity_key}, Some(kf)) % n`.
///
/// Fails with `InvalidData` if the first stream is not present in the
/// pipeline registry — indicates a corrupt snapshot or a stream that was
/// de-registered between snapshot write and recovery.
#[cfg(not(feature = "state-inmem"))]
fn route_entity_to_shard(
    entity_key: &str,
    entity_state: &SerializableEntityState,
    pipelines: &[SerializablePipeline],
    shard_count: usize,
) -> std::io::Result<usize> {
    use crate::routing::shard_hint::shard_hint_for_event;
    use std::io::{Error as IoError, ErrorKind};

    let Some((stream_name, _)) = entity_state.streams.first() else {
        return Ok(0); // no streams → keyless
    };
    let Some(pipeline) = pipelines.iter().find(|p| &p.name == stream_name) else {
        return Err(IoError::new(
            ErrorKind::InvalidData,
            format!(
                "entity {} references stream {} not found in pipeline registry",
                entity_key, stream_name
            ),
        ));
    };
    let kf = pipeline.key_field.as_str();
    if kf.is_empty() {
        return Ok(0); // keyless stream
    }
    let payload = serde_json::json!({ kf: entity_key });
    let hint = shard_hint_for_event(&payload, Some(kf));
    Ok((hint as usize) % shard_count.max(1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::{Duration, UNIX_EPOCH};

    fn ts(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    // ======================== OperatorState Tests ========================

    #[test]
    fn test_operator_state_count_push_read() {
        let mut op = OperatorState::Count(CountOp::new(
            Duration::from_secs(3600),
            Duration::from_secs(60),
        ));
        let now = ts(60_000);
        op.push(&json!({}), None, now).unwrap();
        op.push(&json!({}), None, now).unwrap();
        op.push(&json!({}), None, now).unwrap();
        assert_eq!(op.read(now), FeatureValue::Int(3));
    }

    #[test]
    fn test_operator_state_sum_push_read() {
        let mut op = OperatorState::Sum(SumOp::new(
            "amount",
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        ));
        let now = ts(60_000);
        op.push(&json!({"amount": 50.0}), None, now).unwrap();
        assert_eq!(op.read(now), FeatureValue::Float(50.0));
    }

    #[test]
    fn test_operator_state_avg_push_read() {
        let mut op = OperatorState::Avg(AvgOp::new(
            "amount",
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        ));
        let now = ts(60_000);
        op.push(&json!({"amount": 10.0}), None, now).unwrap();
        op.push(&json!({"amount": 20.0}), None, now).unwrap();
        assert_eq!(op.read(now), FeatureValue::Float(15.0));
    }

    // ======================== Postcard Round-Trip Tests ========================

    #[test]
    fn test_operator_state_count_roundtrip_postcard() {
        let mut op = OperatorState::Count(CountOp::new(
            Duration::from_secs(3600),
            Duration::from_secs(60),
        ));
        let now = ts(60_000);
        op.push(&json!({}), None, now).unwrap();
        op.push(&json!({}), None, now).unwrap();
        op.push(&json!({}), None, now).unwrap();

        let bytes = postcard::to_stdvec(&op).expect("serialize");
        let mut restored: OperatorState = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(restored.read(now), FeatureValue::Int(3));
    }

    #[test]
    fn test_operator_state_sum_roundtrip_postcard() {
        let mut op = OperatorState::Sum(SumOp::new(
            "amount",
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        ));
        let now = ts(60_000);
        op.push(&json!({"amount": 42.5}), None, now).unwrap();
        op.push(&json!({"amount": 7.5}), None, now).unwrap();

        let bytes = postcard::to_stdvec(&op).expect("serialize");
        let mut restored: OperatorState = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(restored.read(now), FeatureValue::Float(50.0));
    }

    // ======================== SnapshotState Tests (v4 format) ========================

    #[test]
    fn test_snapshot_state_roundtrip_v4() {
        let now = ts(60_000);
        let mut count_op = OperatorState::Count(CountOp::new(
            Duration::from_secs(3600),
            Duration::from_secs(60),
        ));
        count_op.push(&json!({}), None, now).unwrap();
        count_op.push(&json!({}), None, now).unwrap();
        count_op.push(&json!({}), None, now).unwrap();

        let state = SnapshotState {
            entities: vec![(
                "u123".to_string(),
                SerializableEntityState {
                    streams: vec![(
                        "Transactions".to_string(),
                        SerializableStreamEntityState {
                            operators: vec![("tx_count_1h".to_string(), count_op)],
                            last_event_at: Some(now),
                        },
                    )],
                    static_features: vec![(
                        "segment".to_string(),
                        StaticFeature {
                            value: FeatureValue::String("premium".to_string()),
                            updated_at: now,
                        },
                    )],
                    table_rows: vec![],
                },
            )],
            pipelines: vec![SerializablePipeline {
                name: "Transactions".to_string(),
                key_field: "user_id".to_string(),
                raw_register_json: r#"{"name":"Transactions","key_field":"user_id","features":[{"name":"tx_count_1h","type":"count","window":"1h"}]}"#.to_string(),
            }],
            backfill_complete: vec![],
        };

        let bytes = postcard::to_stdvec(&state).expect("serialize");
        let restored: SnapshotState = postcard::from_bytes(&bytes).expect("deserialize");

        assert_eq!(restored.entities.len(), 1);
        assert_eq!(restored.entities[0].0, "u123");
        assert_eq!(restored.entities[0].1.streams.len(), 1);
        assert_eq!(restored.entities[0].1.streams[0].0, "Transactions");
        assert_eq!(restored.entities[0].1.streams[0].1.operators.len(), 1);
        assert_eq!(restored.entities[0].1.streams[0].1.last_event_at, Some(now));
        assert_eq!(restored.entities[0].1.static_features.len(), 1);
        assert_eq!(restored.pipelines.len(), 1);
        assert_eq!(restored.pipelines[0].name, "Transactions");

        // Verify operator state preserved
        let mut restored_op = restored.entities[0].1.streams[0].1.operators[0].1.clone();
        assert_eq!(restored_op.read(now), FeatureValue::Int(3));
    }

    // ======================== save_snapshot / load_snapshot Tests ========================

    #[test]
    fn test_save_snapshot_starts_with_version_byte() {
        let state = SnapshotState {
            entities: vec![],
            pipelines: vec![],
            backfill_complete: vec![],
        };
        let bytes = save_snapshot(&state).expect("save_snapshot should succeed");
        assert_eq!(bytes[0], SNAPSHOT_FORMAT_VERSION);
        assert_eq!(bytes[0], 0x09, "Phase 55-03: save_snapshot must now emit v9");
        // v6+ layouts carry a type tag byte after the version byte.
        assert_eq!(
            bytes[1], 0x00,
            "save_snapshot must emit base type tag"
        );
    }

    #[test]
    fn test_load_snapshot_correct_version() {
        let now = ts(60_000);
        let mut count_op = OperatorState::Count(CountOp::new(
            Duration::from_secs(3600),
            Duration::from_secs(60),
        ));
        count_op.push(&json!({}), None, now).unwrap();
        count_op.push(&json!({}), None, now).unwrap();
        count_op.push(&json!({}), None, now).unwrap();

        let state = SnapshotState {
            entities: vec![(
                "u123".to_string(),
                SerializableEntityState {
                    streams: vec![(
                        "TestStream".to_string(),
                        SerializableStreamEntityState {
                            operators: vec![("tx_count_1h".to_string(), count_op)],
                            last_event_at: Some(now),
                        },
                    )],
                    static_features: vec![],
                    table_rows: vec![],
                },
            )],
            pipelines: vec![],
            backfill_complete: vec![],
        };

        let bytes = save_snapshot(&state).expect("save_snapshot should succeed");
        let restored = load_snapshot(&bytes);
        assert!(restored.is_some());

        let restored = restored.unwrap();
        assert_eq!(restored.entities.len(), 1);
        let mut restored_op = restored.entities[0].1.streams[0].1.operators[0].1.clone();
        assert_eq!(restored_op.read(now), FeatureValue::Int(3));
    }

    #[test]
    fn test_load_snapshot_wrong_version_returns_none() {
        let state = SnapshotState {
            entities: vec![],
            pipelines: vec![],
            backfill_complete: vec![],
        };
        let mut bytes = save_snapshot(&state).expect("save_snapshot should succeed");
        // Tamper with version byte (0xFF is neither v5 nor v6)
        bytes[0] = 0xFF;
        assert!(load_snapshot(&bytes).is_none());
    }

    #[test]
    fn test_load_snapshot_v3_returns_none() {
        // A v3 snapshot byte should be gracefully rejected
        let state = SnapshotState {
            entities: vec![],
            pipelines: vec![],
            backfill_complete: vec![],
        };
        let mut bytes = save_snapshot(&state).expect("save_snapshot should succeed");
        // Set version to 3 (old format)
        bytes[0] = 0x03;
        assert!(
            load_snapshot(&bytes).is_none(),
            "v3 snapshot should be gracefully rejected"
        );
    }

    #[test]
    fn test_load_snapshot_rejects_v6_delta_via_legacy_api() {
        // load_snapshot (legacy API) must refuse delta snapshots -- those are
        // only valid through load_snapshot_file.
        let delta = DeltaSnapshotState {
            header: SnapshotHeader {
                snapshot_type: SnapshotType::Delta { base_seq: 0 },
                sequence: 1,
                schema_version: 9,
            },
            changed_entities: vec![],
            deleted_keys: vec![],
        };
        let bytes = save_delta_snapshot(&delta).expect("save delta");
        assert!(
            load_snapshot(&bytes).is_none(),
            "load_snapshot must reject delta via legacy API"
        );
    }

    #[test]
    fn test_load_snapshot_empty_bytes_returns_none() {
        assert!(load_snapshot(&[]).is_none());
    }

    #[test]
    fn test_load_snapshot_corrupt_data_returns_none() {
        let mut bytes = vec![SNAPSHOT_FORMAT_VERSION];
        bytes.extend_from_slice(b"this is not valid postcard data!!!");
        assert!(load_snapshot(&bytes).is_none());
    }

    // ======================== Phase 5: Min/Max/Last OperatorState Tests ========================

    #[test]
    fn test_operator_state_min_push_read() {
        let mut op = OperatorState::Min(crate::engine::operators::MinOp::new(
            "amount",
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        ));
        let now = ts(60_000);
        op.push(&json!({"amount": 10.0}), None, now).unwrap();
        op.push(&json!({"amount": 5.0}), None, now).unwrap();
        op.push(&json!({"amount": 20.0}), None, now).unwrap();
        assert_eq!(op.read(now), FeatureValue::Float(5.0));
    }

    #[test]
    fn test_operator_state_max_push_read() {
        let mut op = OperatorState::Max(crate::engine::operators::MaxOp::new(
            "amount",
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        ));
        let now = ts(60_000);
        op.push(&json!({"amount": 10.0}), None, now).unwrap();
        op.push(&json!({"amount": 5.0}), None, now).unwrap();
        op.push(&json!({"amount": 20.0}), None, now).unwrap();
        assert_eq!(op.read(now), FeatureValue::Float(20.0));
    }

    #[test]
    fn test_operator_state_last_push_read() {
        let mut op = OperatorState::Last(crate::engine::operators::LastOp::new("country", false));
        let now = ts(60_000);
        op.push(&json!({"country": "US"}), None, now).unwrap();
        assert_eq!(op.read(now), FeatureValue::String("US".into()));
    }

    #[test]
    fn test_operator_state_min_roundtrip_postcard() {
        let mut op = OperatorState::Min(crate::engine::operators::MinOp::new(
            "amount",
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        ));
        let now = ts(60_000);
        op.push(&json!({"amount": 10.0}), None, now).unwrap();
        op.push(&json!({"amount": 5.0}), None, now).unwrap();

        let bytes = postcard::to_stdvec(&op).expect("serialize");
        let mut restored: OperatorState = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(restored.read(now), FeatureValue::Float(5.0));
    }

    #[test]
    fn test_operator_state_max_roundtrip_postcard() {
        let mut op = OperatorState::Max(crate::engine::operators::MaxOp::new(
            "amount",
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        ));
        let now = ts(60_000);
        op.push(&json!({"amount": 10.0}), None, now).unwrap();
        op.push(&json!({"amount": 20.0}), None, now).unwrap();

        let bytes = postcard::to_stdvec(&op).expect("serialize");
        let mut restored: OperatorState = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(restored.read(now), FeatureValue::Float(20.0));
    }

    #[test]
    fn test_operator_state_last_roundtrip_postcard() {
        let mut op = OperatorState::Last(crate::engine::operators::LastOp::new("country", false));
        let now = ts(60_000);
        op.push(&json!({"country": "UK"}), None, now).unwrap();

        let bytes = postcard::to_stdvec(&op).expect("serialize");
        let mut restored: OperatorState = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(restored.read(now), FeatureValue::String("UK".into()));
    }

    #[test]
    fn test_snapshot_format_version_is_9() {
        // Phase 55-03: SNAPSHOT_FORMAT_VERSION bumped 8 → 9 alongside addition
        // of SnapshotHeader.schema_version (triggers boot rematerialization on
        // pre-v9 bytes).
        assert_eq!(SNAPSHOT_FORMAT_VERSION, 9);
        assert_eq!(V8_FORMAT, 8);
        assert_eq!(V9_FORMAT, 9);
    }

    #[test]
    fn test_legacy_v5_format_constant() {
        assert_eq!(LEGACY_V5_FORMAT, 5);
    }

    #[test]
    fn test_legacy_v6_format_constant() {
        assert_eq!(LEGACY_V6_FORMAT, 6);
    }

    // ======================== Phase 5 Plan 03: DistinctCount OperatorState Tests ========================

    #[test]
    fn test_operator_state_distinct_count_push_read() {
        use crate::engine::hll::DistinctCountOp;
        let mut op = OperatorState::DistinctCount(DistinctCountOp::new(
            "merchant_id",
            Duration::from_secs(300),
            Duration::from_secs(60),
            false,
        ));
        let now = ts(60_000);
        op.push(&json!({"merchant_id": "m1"}), None, now).unwrap();
        op.push(&json!({"merchant_id": "m2"}), None, now).unwrap();
        op.push(&json!({"merchant_id": "m3"}), None, now).unwrap();
        match op.read(now) {
            FeatureValue::Float(v) => {
                assert!((2.0..=4.0).contains(&v), "Expected ~3 distinct, got {}", v);
            }
            other => panic!("Expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_operator_state_distinct_count_roundtrip_postcard() {
        use crate::engine::hll::DistinctCountOp;
        let mut op = OperatorState::DistinctCount(DistinctCountOp::new(
            "merchant_id",
            Duration::from_secs(300),
            Duration::from_secs(60),
            false,
        ));
        let now = ts(60_000);
        op.push(&json!({"merchant_id": "m1"}), None, now).unwrap();
        op.push(&json!({"merchant_id": "m2"}), None, now).unwrap();

        let bytes = postcard::to_stdvec(&op).expect("serialize");
        let mut restored: OperatorState = postcard::from_bytes(&bytes).expect("deserialize");
        let val_before = op.read(now);
        let val_after = restored.read(now);
        assert_eq!(val_before, val_after, "Round-trip changed value");
    }

    // ======================== Snapshot v4 round-trip via save/load ========================

    #[test]
    fn test_snapshot_v4_roundtrip_save_load() {
        let now = ts(60_000);
        let mut count_op = OperatorState::Count(CountOp::new(
            Duration::from_secs(3600),
            Duration::from_secs(60),
        ));
        count_op.push(&json!({}), None, now).unwrap();
        count_op.push(&json!({}), None, now).unwrap();

        let state = SnapshotState {
            entities: vec![(
                "u123".to_string(),
                SerializableEntityState {
                    streams: vec![(
                        "Transactions".to_string(),
                        SerializableStreamEntityState {
                            operators: vec![("tx_count".to_string(), count_op)],
                            last_event_at: Some(now),
                        },
                    )],
                    static_features: vec![(
                        "segment".to_string(),
                        StaticFeature {
                            value: FeatureValue::String("premium".to_string()),
                            updated_at: now,
                        },
                    )],
                    table_rows: vec![],
                },
            )],
            pipelines: vec![],
            backfill_complete: vec![],
        };

        let bytes = save_snapshot(&state).expect("save");
        let restored = load_snapshot(&bytes).expect("load");

        assert_eq!(restored.entities.len(), 1);
        assert_eq!(restored.entities[0].1.streams.len(), 1);
        assert_eq!(restored.entities[0].1.streams[0].0, "Transactions");
        let mut op = restored.entities[0].1.streams[0].1.operators[0].1.clone();
        assert_eq!(op.read(now), FeatureValue::Int(2));
        assert_eq!(restored.entities[0].1.streams[0].1.last_event_at, Some(now));
        assert_eq!(restored.entities[0].1.static_features.len(), 1);
    }

    #[test]
    fn test_snapshot_backfill_complete_roundtrip() {
        let state = SnapshotState {
            entities: vec![],
            pipelines: vec![],
            backfill_complete: vec![
                ("Transactions".to_string(), "sum_1h".to_string()),
                ("Logins".to_string(), "count_1h".to_string()),
            ],
        };
        let bytes = save_snapshot(&state).expect("save");
        let restored = load_snapshot(&bytes).expect("load");
        assert_eq!(restored.backfill_complete.len(), 2);
        assert!(restored
            .backfill_complete
            .contains(&("Transactions".to_string(), "sum_1h".to_string())));
        assert!(restored
            .backfill_complete
            .contains(&("Logins".to_string(), "count_1h".to_string())));
    }

    // ======================== Phase 9: v6 Base/Delta Format Tests ========================

    fn sample_entity(
        op_count: u64,
        stream: &str,
        feature: &str,
        when: SystemTime,
    ) -> (String, SerializableEntityState) {
        let mut op = OperatorState::Count(CountOp::new(
            Duration::from_secs(3600),
            Duration::from_secs(60),
        ));
        for _ in 0..op_count {
            op.push(&json!({}), None, when).unwrap();
        }
        (
            format!("entity-{}", op_count),
            SerializableEntityState {
                streams: vec![(
                    stream.to_string(),
                    SerializableStreamEntityState {
                        operators: vec![(feature.to_string(), op)],
                        last_event_at: Some(when),
                    },
                )],
                static_features: vec![],
                table_rows: vec![],
            },
        )
    }

    #[test]
    fn test_save_base_snapshot_header_bytes() {
        let base = BaseSnapshotState {
            header: SnapshotHeader {
                snapshot_type: SnapshotType::Base,
                sequence: 42,
                schema_version: 9,
            },
            entities: vec![],
            pipelines: vec![],
            backfill_complete: vec![],
        };
        let bytes = save_base_snapshot(&base).expect("save base");
        assert_eq!(bytes[0], SNAPSHOT_FORMAT_VERSION);
        assert_eq!(bytes[0], V9_FORMAT, "Phase 55-03: saves are v9");
        assert_eq!(bytes[1], 0x00, "base type tag must be 0x00");
    }

    #[test]
    fn test_save_delta_snapshot_header_bytes() {
        let delta = DeltaSnapshotState {
            header: SnapshotHeader {
                snapshot_type: SnapshotType::Delta { base_seq: 5 },
                sequence: 7,
                schema_version: 9,
            },
            changed_entities: vec![],
            deleted_keys: vec![],
        };
        let bytes = save_delta_snapshot(&delta).expect("save delta");
        assert_eq!(bytes[0], SNAPSHOT_FORMAT_VERSION);
        assert_eq!(bytes[0], V9_FORMAT, "Phase 55-03: saves are v9");
        assert_eq!(bytes[1], 0x01, "delta type tag must be 0x01");
    }

    #[test]
    fn test_base_snapshot_roundtrip_preserves_fields() {
        let now = ts(60_000);
        let (key, entity) = sample_entity(3, "Transactions", "tx_count_1h", now);
        let base = BaseSnapshotState {
            header: SnapshotHeader {
                snapshot_type: SnapshotType::Base,
                sequence: 10,
                schema_version: 9,
            },
            entities: vec![(key.clone(), entity)],
            pipelines: vec![SerializablePipeline {
                name: "Transactions".to_string(),
                key_field: "user_id".to_string(),
                raw_register_json: "{}".to_string(),
            }],
            backfill_complete: vec![("Transactions".to_string(), "tx_count_1h".to_string())],
        };
        let bytes = save_base_snapshot(&base).expect("save base");
        let file = load_snapshot_file(&bytes).expect("load");
        match file {
            SnapshotFile::Base(restored) => {
                assert_eq!(restored.header.sequence, 10);
                assert_eq!(restored.header.snapshot_type, SnapshotType::Base);
                assert_eq!(restored.entities.len(), 1);
                assert_eq!(restored.entities[0].0, key);
                assert_eq!(restored.pipelines.len(), 1);
                assert_eq!(restored.pipelines[0].name, "Transactions");
                assert_eq!(restored.backfill_complete.len(), 1);
                // Verify operator state preserved
                let mut op = restored.entities[0].1.streams[0].1.operators[0].1.clone();
                assert_eq!(op.read(ts(60_000)), FeatureValue::Int(3));
            }
            SnapshotFile::Delta(_) => panic!("expected Base, got Delta"),
        }
    }

    #[test]
    fn test_delta_snapshot_roundtrip_preserves_fields() {
        let now = ts(60_000);
        let (key, entity) = sample_entity(5, "Transactions", "tx_count_1h", now);
        let delta = DeltaSnapshotState {
            header: SnapshotHeader {
                snapshot_type: SnapshotType::Delta { base_seq: 10 },
                sequence: 11,
                schema_version: 9,
            },
            changed_entities: vec![(key.clone(), entity)],
            deleted_keys: vec!["evicted_user".to_string()],
        };
        let bytes = save_delta_snapshot(&delta).expect("save delta");
        let file = load_snapshot_file(&bytes).expect("load");
        match file {
            SnapshotFile::Delta(restored) => {
                assert_eq!(restored.header.sequence, 11);
                assert_eq!(
                    restored.header.snapshot_type,
                    SnapshotType::Delta { base_seq: 10 }
                );
                assert_eq!(restored.changed_entities.len(), 1);
                assert_eq!(restored.changed_entities[0].0, key);
                assert_eq!(restored.deleted_keys, vec!["evicted_user".to_string()]);
                let mut op = restored.changed_entities[0].1.streams[0].1.operators[0]
                    .1
                    .clone();
                assert_eq!(op.read(now), FeatureValue::Int(5));
            }
            SnapshotFile::Base(_) => panic!("expected Delta, got Base"),
        }
    }

    #[test]
    fn test_load_snapshot_file_dispatches_base_vs_delta() {
        let now = ts(60_000);
        let base = BaseSnapshotState {
            header: SnapshotHeader {
                snapshot_type: SnapshotType::Base,
                sequence: 1,
                schema_version: 9,
            },
            entities: vec![sample_entity(1, "S", "f", now)],
            pipelines: vec![],
            backfill_complete: vec![],
        };
        let delta = DeltaSnapshotState {
            header: SnapshotHeader {
                snapshot_type: SnapshotType::Delta { base_seq: 1 },
                sequence: 2,
                schema_version: 9,
            },
            changed_entities: vec![sample_entity(2, "S", "f", now)],
            deleted_keys: vec![],
        };

        let base_bytes = save_base_snapshot(&base).unwrap();
        let delta_bytes = save_delta_snapshot(&delta).unwrap();

        match load_snapshot_file(&base_bytes) {
            Some(SnapshotFile::Base(_)) => {}
            _ => panic!("expected Base from base bytes"),
        }
        match load_snapshot_file(&delta_bytes) {
            Some(SnapshotFile::Delta(_)) => {}
            _ => panic!("expected Delta from delta bytes"),
        }
    }

    #[test]
    fn test_load_snapshot_file_rejects_short_input() {
        assert!(load_snapshot_file(&[]).is_none());
        assert!(load_snapshot_file(&[0x06]).is_none());
    }

    #[test]
    fn test_load_snapshot_file_rejects_wrong_version() {
        let mut bytes = save_base_snapshot(&BaseSnapshotState {
            header: SnapshotHeader {
                snapshot_type: SnapshotType::Base,
                sequence: 0,
                schema_version: 9,
            },
            entities: vec![],
            pipelines: vec![],
            backfill_complete: vec![],
        })
        .unwrap();
        bytes[0] = 0x05;
        assert!(load_snapshot_file(&bytes).is_none());
    }

    #[test]
    fn test_load_snapshot_file_rejects_unknown_type_tag() {
        let mut bytes = save_base_snapshot(&BaseSnapshotState {
            header: SnapshotHeader {
                snapshot_type: SnapshotType::Base,
                sequence: 0,
                schema_version: 9,
            },
            entities: vec![],
            pipelines: vec![],
            backfill_complete: vec![],
        })
        .unwrap();
        bytes[1] = 0xAA;
        assert!(load_snapshot_file(&bytes).is_none());
    }

    #[test]
    fn test_load_snapshot_file_rejects_corrupt_postcard() {
        let bytes = vec![
            SNAPSHOT_FORMAT_VERSION,
            TYPE_TAG_DELTA,
            0xFF,
            0xFF,
            0xFF,
            0xFF,
        ];
        assert!(load_snapshot_file(&bytes).is_none());
    }

    // Phase 54-04 Pass A6b: the `apply_delta` + `restore_from_snapshot` unit
    // tests (formerly at this spot, all using `StateStore::new()`) were deleted
    // here when `StateStore` was removed. The equivalent coverage lives in the
    // shard-dispatch integration tests under `tests/` which exercise the
    // shard-owned state path that replaces the legacy DashMap store.

    // ======================== Phase 9: v5 Legacy Migration Tests ========================

    #[test]
    fn test_load_legacy_v5_reads_v5_bytes() {
        // Manually construct a v5 byte blob (version 5 + postcard(SnapshotState))
        let state = SnapshotState {
            entities: vec![],
            pipelines: vec![SerializablePipeline {
                name: "Old".to_string(),
                key_field: "user_id".to_string(),
                raw_register_json: "{}".to_string(),
            }],
            backfill_complete: vec![],
        };
        let mut v5_bytes = vec![LEGACY_V5_FORMAT];
        v5_bytes.extend_from_slice(&postcard::to_stdvec(&state).unwrap());

        let restored = load_legacy_v5(&v5_bytes).expect("v5 should load");
        assert_eq!(restored.pipelines.len(), 1);
        assert_eq!(restored.pipelines[0].name, "Old");
    }

    #[test]
    fn test_load_legacy_v5_returns_none_for_v6() {
        let base = BaseSnapshotState {
            header: SnapshotHeader {
                snapshot_type: SnapshotType::Base,
                sequence: 0,
                schema_version: 9,
            },
            entities: vec![],
            pipelines: vec![],
            backfill_complete: vec![],
        };
        let v6_bytes = save_base_snapshot(&base).unwrap();
        assert!(
            load_legacy_v5(&v6_bytes).is_none(),
            "v6 bytes must not load as v5"
        );
    }

    #[test]
    fn test_load_legacy_v5_returns_none_for_empty() {
        assert!(load_legacy_v5(&[]).is_none());
    }

    #[test]
    fn test_load_legacy_v5_returns_none_for_corrupt() {
        let mut bytes = vec![LEGACY_V5_FORMAT];
        bytes.extend_from_slice(b"not valid postcard");
        assert!(load_legacy_v5(&bytes).is_none());
    }

    #[test]
    fn test_load_snapshot_transparently_migrates_v5() {
        // The legacy load_snapshot API should still accept v5 bytes (for backward
        // compat with existing snapshot files on disk).
        let state = SnapshotState {
            entities: vec![],
            pipelines: vec![SerializablePipeline {
                name: "Migrated".to_string(),
                key_field: "user_id".to_string(),
                raw_register_json: "{}".to_string(),
            }],
            backfill_complete: vec![],
        };
        let mut v5_bytes = vec![LEGACY_V5_FORMAT];
        v5_bytes.extend_from_slice(&postcard::to_stdvec(&state).unwrap());

        let restored = load_snapshot(&v5_bytes).expect("load_snapshot must accept v5 legacy bytes");
        assert_eq!(restored.pipelines.len(), 1);
        assert_eq!(restored.pipelines[0].name, "Migrated");
    }

    #[test]
    fn test_sequence_numbers_preserved_in_header() {
        let base = BaseSnapshotState {
            header: SnapshotHeader {
                snapshot_type: SnapshotType::Base,
                sequence: 1000,
                schema_version: 9,
            },
            entities: vec![],
            pipelines: vec![],
            backfill_complete: vec![],
        };
        let bytes = save_base_snapshot(&base).unwrap();
        match load_snapshot_file(&bytes).unwrap() {
            SnapshotFile::Base(b) => assert_eq!(b.header.sequence, 1000),
            _ => panic!(),
        }

        let delta = DeltaSnapshotState {
            header: SnapshotHeader {
                snapshot_type: SnapshotType::Delta { base_seq: 1000 },
                sequence: 1001,
                schema_version: 9,
            },
            changed_entities: vec![],
            deleted_keys: vec![],
        };
        let bytes = save_delta_snapshot(&delta).unwrap();
        match load_snapshot_file(&bytes).unwrap() {
            SnapshotFile::Delta(d) => {
                assert_eq!(d.header.sequence, 1001);
                assert_eq!(
                    d.header.snapshot_type,
                    SnapshotType::Delta { base_seq: 1000 }
                );
            }
            _ => panic!(),
        }
    }

    // ======================== Phase 55-03: schema_version + V9_FORMAT ========================

    #[test]
    fn default_v8_helper_returns_8() {
        // The serde default for SnapshotHeader.schema_version MUST be 8.
        // This is how v8-era snapshots (no field on the wire) deserialize as 8
        // and trigger the boot-time rematerialization guard.
        assert_eq!(default_v8(), 8u16);
    }

    #[test]
    fn snapshot_header_schema_version_defaults_to_8_on_v8_wire() {
        // Build a serialized v8-shaped SnapshotHeader (WITHOUT the
        // schema_version field) and ensure the wire-compat shim
        // (SnapshotHeaderV8Wire → SnapshotHeader) fills in schema_version=8.
        //
        // Postcard does NOT synthesize missing trailing fields from
        // `#[serde(default)]`; the v8 decode path therefore routes
        // through `SnapshotHeaderV8Wire` which has no schema_version
        // field, then converts via `From<SnapshotHeaderV8Wire> for
        // SnapshotHeader` (which sets schema_version = 8). This test
        // pins that conversion semantic.
        let wire = SnapshotHeaderV8Wire {
            snapshot_type: SnapshotType::Base,
            sequence: 42,
        };
        let bytes = postcard::to_allocvec(&wire).unwrap();
        let decoded_wire: SnapshotHeaderV8Wire = postcard::from_bytes(&bytes).unwrap();
        let decoded: SnapshotHeader = decoded_wire.into();
        assert_eq!(decoded.sequence, 42);
        assert_eq!(
            decoded.schema_version, 8,
            "v8 wire → schema_version=8 via V8Wire → SnapshotHeader shim"
        );
    }

    #[test]
    fn snapshot_header_v9_roundtrips() {
        let h = SnapshotHeader {
            snapshot_type: SnapshotType::Base,
            sequence: 100,
            schema_version: 9,
        };
        let bytes = postcard::to_allocvec(&h).unwrap();
        let decoded: SnapshotHeader = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.schema_version, 9);
        assert_eq!(decoded.sequence, 100);
    }

    #[test]
    fn load_base_snapshot_rejects_unknown_version_byte() {
        // Pitfall 3 guard — unknown outer version byte (0xFF) → None on
        // both legacy and generic load paths.
        let bytes = vec![0xFFu8, TYPE_TAG_BASE, 0x00, 0x00];
        assert!(load_snapshot_file(&bytes).is_none());
        assert!(load_snapshot(&bytes).is_none());
    }

    #[test]
    fn load_base_snapshot_v8_outer_byte_decodes_with_schema_version_8() {
        // Construct a v8-formatted snapshot by hand (outer byte 0x08). The
        // body is a BaseSnapshotStateV8 whose embedded SnapshotHeader omits
        // the schema_version field; serde default fills in 8. Assert: loaded
        // header.schema_version == 8.
        #[derive(Serialize)]
        struct V8Header {
            snapshot_type: SnapshotType,
            sequence: u64,
        }
        #[derive(Serialize)]
        struct V8Body {
            header: V8Header,
            entities: Vec<(String, SerializableEntityState)>,
            pipelines: Vec<SerializablePipeline>,
            backfill_complete: Vec<(String, String)>,
            shard_count: u16,
            replica_lsn_map: HashMap<(String, u8), u64>,
        }
        let body = V8Body {
            header: V8Header {
                snapshot_type: SnapshotType::Base,
                sequence: 5,
            },
            entities: vec![],
            pipelines: vec![],
            backfill_complete: vec![],
            shard_count: 1,
            replica_lsn_map: HashMap::new(),
        };
        let mut bytes = vec![V8_FORMAT, TYPE_TAG_BASE];
        bytes.extend_from_slice(&postcard::to_stdvec(&body).unwrap());
        let file = load_snapshot_file(&bytes).expect("v8 outer byte must decode");
        match file {
            SnapshotFile::Base(b) => {
                assert_eq!(b.header.sequence, 5);
                assert_eq!(
                    b.header.schema_version, 8,
                    "v8 wire → schema_version=8 via serde default"
                );
            }
            _ => panic!("expected Base"),
        }
    }

    #[test]
    fn load_base_snapshot_v9_outer_byte_decodes_with_schema_version_9() {
        // The Phase 55-03 writer path: save_base_snapshot promotes the header
        // to schema_version=9 AND emits outer byte V9_FORMAT.
        let base = BaseSnapshotState {
            header: SnapshotHeader {
                snapshot_type: SnapshotType::Base,
                sequence: 7,
                schema_version: 9,
            },
            entities: vec![],
            pipelines: vec![],
            backfill_complete: vec![],
        };
        let bytes = save_base_snapshot(&base).expect("save");
        assert_eq!(bytes[0], V9_FORMAT, "Phase 55-03 writes V9_FORMAT byte");
        let file = load_snapshot_file(&bytes).expect("v9 decode");
        match file {
            SnapshotFile::Base(b) => {
                assert_eq!(b.header.schema_version, 9);
            }
            _ => panic!("expected Base"),
        }
    }
}
