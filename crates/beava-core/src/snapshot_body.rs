//! Phase 7 Plan 02: SnapshotBody — bincode-serializable bundle of registry
//! descriptors + per-entity aggregation state + scalar counters.
//!
//! # Design
//!
//! - `RegistryDescriptorsOnly` is a projection of `RegistryInner` that drops
//!   the runtime caches (`compiled_chains`, `compiled_aggregations`,
//!   `feature_index`). These caches are reconstructed on load via
//!   `Registry::install_from_descriptors` (Plan 07-03), which rebuilds them
//!   by feeding each descriptor through the register-time compile path.
//! - `SnapshotBody` wraps the full snapshot payload that will be bincode-encoded
//!   and handed to `beava-persistence::SnapshotWriter` in Plan 07-03.
//! - `body_format_version` is a monotonic u16 to detect future format evolution;
//!   `decode` rejects unknown versions so operators see a clean error.
//!
//! Snapshot bodies carry `AggOp` *state*, not `AggOpDescriptor`. Descriptors
//! (which can hold `Arc<Expr>`) live on the registry; state is plain POD.

use crate::agg_op::AggOp;
use crate::agg_state_table::{EntityKey, StateTables};
use crate::registry::{DerivationDescriptor, EventDescriptor, RegistryInner, TableDescriptor};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Monotonic format version for snapshot body evolution.
pub const SNAPSHOT_BODY_FORMAT_VERSION: u16 = 1;

/// Per-aggregation-node serialized state: ordered list of (entity, ops).
pub type SerializedStateTables = BTreeMap<String, Vec<(EntityKey, Vec<AggOp>)>>;

/// A projection of `RegistryInner` that excludes runtime caches. These are
/// rebuilt on load by Plan 07-03's `install_from_descriptors`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RegistryDescriptorsOnly {
    pub version: u64,
    pub events: BTreeMap<String, EventDescriptor>,
    pub tables: BTreeMap<String, TableDescriptor>,
    pub derivations: BTreeMap<String, DerivationDescriptor>,
}

impl From<&RegistryInner> for RegistryDescriptorsOnly {
    fn from(inner: &RegistryInner) -> Self {
        // Plan 18-11 D-6: events live as Arc<EventDescriptor> on the
        // RegistryInner; the snapshot body holds plain EventDescriptor.
        // Unwrap the Arc by cloning the inner — cold path, infrequent.
        let events: BTreeMap<String, EventDescriptor> = inner
            .events
            .iter()
            .map(|(k, v)| (k.clone(), (**v).clone()))
            .collect();
        RegistryDescriptorsOnly {
            version: inner.version,
            events,
            tables: inner.tables.clone(),
            derivations: inner.derivations.clone(),
        }
    }
}

/// Full snapshot body. Encoded with bincode; decoded by `SnapshotBody::decode`.
///
/// Note: no `PartialEq` — `AggOp` has no `PartialEq` impl because its inner
/// states may carry F64 (NaN-aware) fields. Equality is established by round-
/// tripping through bincode: same bytes ⇒ same state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotBody {
    /// Monotonic format version for evolution detection; v0 = 1.
    pub body_format_version: u16,
    pub registry: RegistryDescriptorsOnly,
    /// Per-aggregation node state: map from node name to a `Vec<(EntityKey, Vec<AggOp>)>`
    /// (explicit list for deterministic ordering on serialize).
    pub state_tables: SerializedStateTables,
    /// Scalar counters preserved across restart.
    pub next_event_id: u64,
    pub max_event_time_ms: i64,
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotBodyError {
    #[error("bincode: {0}")]
    Bincode(#[from] Box<bincode::ErrorKind>),
    #[error("unsupported snapshot body_format_version {0}")]
    UnsupportedVersion(u16),
}

impl SnapshotBody {
    /// Build a SnapshotBody from live pointers. No I/O; no locking beyond the
    /// caller's. Caller must hold the state_tables mutex while this runs so
    /// the iteration is consistent.
    pub fn from_live(
        registry: &RegistryInner,
        state_tables: &StateTables,
        next_event_id: u64,
        max_event_time_ms: i64,
    ) -> Self {
        // Plan 18-11 D-8: iter_sorted on each AggStateTable so the snapshot
        // entry order is byte-identical for the same input event sequence,
        // regardless of HashMap insertion order. The outer state_tables map
        // is still BTreeMap → already deterministic.
        let mut serialized_tables: SerializedStateTables = BTreeMap::new();
        for (node_name, table) in state_tables {
            let entries: Vec<(EntityKey, Vec<AggOp>)> = table
                .iter_sorted()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            serialized_tables.insert(node_name.clone(), entries);
        }
        SnapshotBody {
            body_format_version: SNAPSHOT_BODY_FORMAT_VERSION,
            registry: RegistryDescriptorsOnly::from(registry),
            state_tables: serialized_tables,
            next_event_id,
            max_event_time_ms,
        }
    }

    /// Encode with bincode default config.
    pub fn encode(&self) -> Result<Vec<u8>, SnapshotBodyError> {
        bincode::serialize(self).map_err(SnapshotBodyError::from)
    }

    /// Decode from bincode bytes; rejects unknown `body_format_version`.
    pub fn decode(bytes: &[u8]) -> Result<Self, SnapshotBodyError> {
        let body: SnapshotBody = bincode::deserialize(bytes)?;
        if body.body_format_version != SNAPSHOT_BODY_FORMAT_VERSION {
            return Err(SnapshotBodyError::UnsupportedVersion(
                body.body_format_version,
            ));
        }
        Ok(body)
    }

    /// Consume self and return its parts for the recovery loader.
    pub fn into_parts(self) -> (RegistryDescriptorsOnly, SerializedStateTables, u64, i64) {
        (
            self.registry,
            self.state_tables,
            self.next_event_id,
            self.max_event_time_ms,
        )
    }
}
