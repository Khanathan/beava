//! Snapshot persistence: OperatorState enum, serializable state types,
//! save/load functions with versioning.
//!
//! OperatorState replaces Box<dyn Operator> throughout the codebase,
//! making EntityState fully serializable with serde/postcard.
//!
//! v1.1: Snapshot format v4 with per-stream grouped state via
//! SerializableStreamEntityState. v3 snapshots are gracefully rejected.

use serde::{Serialize, Deserialize};
use std::time::SystemTime;
use crate::engine::operators::{CountOp, SumOp, AvgOp, MinOp, MaxOp, LastOp, StddevOp, PercentileOp, Operator};
use crate::engine::hll::DistinctCountOp;
use crate::state::store::StaticFeature;
use crate::types::FeatureValue;
use crate::error::TallyError;

/// Snapshot format version byte. Prepended to serialized data.
/// If the version doesn't match on load, return None (clean startup from empty state).
/// v6 (Phase 9, OPS-03/OPS-04): adds base/delta snapshot type discriminator byte
/// for incremental snapshots.
pub const SNAPSHOT_FORMAT_VERSION: u8 = 6;

/// Legacy v5 format version byte. Used by `load_legacy_v5` to migrate
/// existing single-file snapshots to v6 on first startup.
pub const LEGACY_V5_FORMAT: u8 = 5;

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
}

impl OperatorState {
    pub fn push(&mut self, event: &serde_json::Value, now: SystemTime) -> Result<(), TallyError> {
        match self {
            Self::Count(op) => op.push(event, now),
            Self::Sum(op) => op.push(event, now),
            Self::Avg(op) => op.push(event, now),
            Self::Min(op) => op.push(event, now),
            Self::Max(op) => op.push(event, now),
            Self::Last(op) => op.push(event, now),
            Self::DistinctCount(op) => op.push(event, now),
            Self::Stddev(op) => op.push(event, now),
            Self::Percentile(op) => op.push(event, now),
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

/// Serializable entity state for snapshot persistence (v4 format).
/// Groups operators by stream name for independent per-stream TTL management.
/// Uses Vec instead of AHashMap for postcard compatibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableEntityState {
    pub streams: Vec<(String, SerializableStreamEntityState)>,
    pub static_features: Vec<(String, StaticFeature)>,
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

/// Header present in all v6 snapshots. Carries the snapshot type and a
/// monotonic sequence number used to order files during recovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotHeader {
    pub snapshot_type: SnapshotType,
    pub sequence: u64,
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

/// Wrapper returned by `load_snapshot_file` that preserves the on-disk type.
#[derive(Debug, Clone)]
pub enum SnapshotFile {
    Base(BaseSnapshotState),
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
        },
        entities: data.entities.clone(),
        pipelines: data.pipelines.clone(),
        backfill_complete: data.backfill_complete.clone(),
    };
    save_base_snapshot(&base)
}

/// Deserialize a `SnapshotState` from bytes. Accepts either a legacy v5
/// single-file snapshot or a v6 base snapshot. Delta snapshots are rejected
/// by this legacy API (use `load_snapshot_file` for the generic path).
///
/// Returns None if:
/// - bytes is empty
/// - version byte is not v5 or v6
/// - v6 type tag is not base (0x00)
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
    if version != SNAPSHOT_FORMAT_VERSION {
        eprintln!(
            "Snapshot version mismatch: found {}, expected {}. Starting fresh.",
            version, SNAPSHOT_FORMAT_VERSION
        );
        return None;
    }
    // v6 path: must be a base snapshot for this legacy API.
    if bytes.len() < 2 {
        return None;
    }
    if bytes[1] != TYPE_TAG_BASE {
        // Delta snapshots must go through load_snapshot_file.
        return None;
    }
    let base: BaseSnapshotState = postcard::from_bytes(&bytes[2..]).ok()?;
    Some(SnapshotState {
        entities: base.entities,
        pipelines: base.pipelines,
        backfill_complete: base.backfill_complete,
    })
}

// ================ Phase 9: v6 Save/Load Functions ================

/// Serialize a `BaseSnapshotState` in v6 format.
/// Format: `[version=6][type_tag=0x00][postcard(BaseSnapshotState)]`
pub fn save_base_snapshot(data: &BaseSnapshotState) -> Result<Vec<u8>, postcard::Error> {
    let mut buf = vec![SNAPSHOT_FORMAT_VERSION, TYPE_TAG_BASE];
    buf.extend_from_slice(&postcard::to_stdvec(data)?);
    Ok(buf)
}

/// Serialize a `DeltaSnapshotState` in v6 format.
/// Format: `[version=6][type_tag=0x01][postcard(DeltaSnapshotState)]`
pub fn save_delta_snapshot(data: &DeltaSnapshotState) -> Result<Vec<u8>, postcard::Error> {
    let mut buf = vec![SNAPSHOT_FORMAT_VERSION, TYPE_TAG_DELTA];
    buf.extend_from_slice(&postcard::to_stdvec(data)?);
    Ok(buf)
}

/// Load a v6 snapshot file (base or delta) from bytes. Returns None on
/// version mismatch, unknown type tag, or corrupt data.
///
/// Security: postcard deserialization rejects malformed input via Result;
/// we convert any error to None to match the rest of the snapshot module's
/// "fail closed, start fresh" policy. (Threat register T-09-01.)
pub fn load_snapshot_file(bytes: &[u8]) -> Option<SnapshotFile> {
    if bytes.len() < 2 {
        return None;
    }
    if bytes[0] != SNAPSHOT_FORMAT_VERSION {
        return None;
    }
    match bytes[1] {
        TYPE_TAG_BASE => postcard::from_bytes::<BaseSnapshotState>(&bytes[2..])
            .ok()
            .map(SnapshotFile::Base),
        TYPE_TAG_DELTA => postcard::from_bytes::<DeltaSnapshotState>(&bytes[2..])
            .ok()
            .map(SnapshotFile::Delta),
        _ => None,
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};
    use serde_json::json;

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
        op.push(&json!({}), now).unwrap();
        op.push(&json!({}), now).unwrap();
        op.push(&json!({}), now).unwrap();
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
        op.push(&json!({"amount": 50.0}), now).unwrap();
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
        op.push(&json!({"amount": 10.0}), now).unwrap();
        op.push(&json!({"amount": 20.0}), now).unwrap();
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
        op.push(&json!({}), now).unwrap();
        op.push(&json!({}), now).unwrap();
        op.push(&json!({}), now).unwrap();

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
        op.push(&json!({"amount": 42.5}), now).unwrap();
        op.push(&json!({"amount": 7.5}), now).unwrap();

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
        count_op.push(&json!({}), now).unwrap();
        count_op.push(&json!({}), now).unwrap();
        count_op.push(&json!({}), now).unwrap();

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
        assert_eq!(bytes[0], 0x06);
        // v6 adds a type tag byte after the version byte.
        assert_eq!(bytes[1], 0x00, "legacy save_snapshot must emit base type tag");
    }

    #[test]
    fn test_load_snapshot_correct_version() {
        let now = ts(60_000);
        let mut count_op = OperatorState::Count(CountOp::new(
            Duration::from_secs(3600),
            Duration::from_secs(60),
        ));
        count_op.push(&json!({}), now).unwrap();
        count_op.push(&json!({}), now).unwrap();
        count_op.push(&json!({}), now).unwrap();

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
        assert!(load_snapshot(&bytes).is_none(), "v3 snapshot should be gracefully rejected");
    }

    #[test]
    fn test_load_snapshot_rejects_v6_delta_via_legacy_api() {
        // load_snapshot (legacy API) must refuse delta snapshots -- those are
        // only valid through load_snapshot_file.
        let delta = DeltaSnapshotState {
            header: SnapshotHeader {
                snapshot_type: SnapshotType::Delta { base_seq: 0 },
                sequence: 1,
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
        op.push(&json!({"amount": 10.0}), now).unwrap();
        op.push(&json!({"amount": 5.0}), now).unwrap();
        op.push(&json!({"amount": 20.0}), now).unwrap();
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
        op.push(&json!({"amount": 10.0}), now).unwrap();
        op.push(&json!({"amount": 5.0}), now).unwrap();
        op.push(&json!({"amount": 20.0}), now).unwrap();
        assert_eq!(op.read(now), FeatureValue::Float(20.0));
    }

    #[test]
    fn test_operator_state_last_push_read() {
        let mut op = OperatorState::Last(crate::engine::operators::LastOp::new(
            "country",
            false,
        ));
        let now = ts(60_000);
        op.push(&json!({"country": "US"}), now).unwrap();
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
        op.push(&json!({"amount": 10.0}), now).unwrap();
        op.push(&json!({"amount": 5.0}), now).unwrap();

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
        op.push(&json!({"amount": 10.0}), now).unwrap();
        op.push(&json!({"amount": 20.0}), now).unwrap();

        let bytes = postcard::to_stdvec(&op).expect("serialize");
        let mut restored: OperatorState = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(restored.read(now), FeatureValue::Float(20.0));
    }

    #[test]
    fn test_operator_state_last_roundtrip_postcard() {
        let mut op = OperatorState::Last(crate::engine::operators::LastOp::new(
            "country",
            false,
        ));
        let now = ts(60_000);
        op.push(&json!({"country": "UK"}), now).unwrap();

        let bytes = postcard::to_stdvec(&op).expect("serialize");
        let mut restored: OperatorState = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(restored.read(now), FeatureValue::String("UK".into()));
    }

    #[test]
    fn test_snapshot_format_version_is_6() {
        assert_eq!(SNAPSHOT_FORMAT_VERSION, 6);
    }

    #[test]
    fn test_legacy_v5_format_constant() {
        assert_eq!(LEGACY_V5_FORMAT, 5);
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
        op.push(&json!({"merchant_id": "m1"}), now).unwrap();
        op.push(&json!({"merchant_id": "m2"}), now).unwrap();
        op.push(&json!({"merchant_id": "m3"}), now).unwrap();
        match op.read(now) {
            FeatureValue::Float(v) => {
                assert!(v >= 2.0 && v <= 4.0, "Expected ~3 distinct, got {}", v);
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
        op.push(&json!({"merchant_id": "m1"}), now).unwrap();
        op.push(&json!({"merchant_id": "m2"}), now).unwrap();

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
        count_op.push(&json!({}), now).unwrap();
        count_op.push(&json!({}), now).unwrap();

        let state = SnapshotState {
            entities: vec![(
                "u123".to_string(),
                SerializableEntityState {
                    streams: vec![
                        (
                            "Transactions".to_string(),
                            SerializableStreamEntityState {
                                operators: vec![("tx_count".to_string(), count_op)],
                                last_event_at: Some(now),
                            },
                        ),
                    ],
                    static_features: vec![(
                        "segment".to_string(),
                        StaticFeature {
                            value: FeatureValue::String("premium".to_string()),
                            updated_at: now,
                        },
                    )],
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
        assert!(restored.backfill_complete.contains(&("Transactions".to_string(), "sum_1h".to_string())));
        assert!(restored.backfill_complete.contains(&("Logins".to_string(), "count_1h".to_string())));
    }

    // ======================== Phase 9: v6 Base/Delta Format Tests ========================

    fn sample_entity(op_count: u64, stream: &str, feature: &str, when: SystemTime) -> (String, SerializableEntityState) {
        let mut op = OperatorState::Count(CountOp::new(
            Duration::from_secs(3600),
            Duration::from_secs(60),
        ));
        for _ in 0..op_count {
            op.push(&json!({}), when).unwrap();
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
            },
        )
    }

    #[test]
    fn test_save_base_snapshot_header_bytes() {
        let base = BaseSnapshotState {
            header: SnapshotHeader {
                snapshot_type: SnapshotType::Base,
                sequence: 42,
            },
            entities: vec![],
            pipelines: vec![],
            backfill_complete: vec![],
        };
        let bytes = save_base_snapshot(&base).expect("save base");
        assert_eq!(bytes[0], SNAPSHOT_FORMAT_VERSION);
        assert_eq!(bytes[1], 0x00, "base type tag must be 0x00");
    }

    #[test]
    fn test_save_delta_snapshot_header_bytes() {
        let delta = DeltaSnapshotState {
            header: SnapshotHeader {
                snapshot_type: SnapshotType::Delta { base_seq: 5 },
                sequence: 7,
            },
            changed_entities: vec![],
            deleted_keys: vec![],
        };
        let bytes = save_delta_snapshot(&delta).expect("save delta");
        assert_eq!(bytes[0], SNAPSHOT_FORMAT_VERSION);
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
                let mut op = restored.changed_entities[0].1.streams[0].1.operators[0].1.clone();
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
            },
            entities: vec![sample_entity(1, "S", "f", now)],
            pipelines: vec![],
            backfill_complete: vec![],
        };
        let delta = DeltaSnapshotState {
            header: SnapshotHeader {
                snapshot_type: SnapshotType::Delta { base_seq: 1 },
                sequence: 2,
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
        let bytes = vec![SNAPSHOT_FORMAT_VERSION, TYPE_TAG_DELTA, 0xFF, 0xFF, 0xFF, 0xFF];
        assert!(load_snapshot_file(&bytes).is_none());
    }

    // ======================== Phase 9: apply_delta Tests ========================

    #[test]
    fn test_apply_delta_inserts_changed_entities() {
        use crate::state::store::StateStore;
        let mut store = StateStore::new();
        let now = ts(60_000);

        let (key, entity) = sample_entity(3, "Transactions", "tx_count_1h", now);
        store.apply_delta(vec![(key.clone(), entity)], vec![]);

        assert_eq!(store.entity_count(), 1);
        let restored_entity = store.get_entity(&key).unwrap();
        assert_eq!(restored_entity.streams.len(), 1);
        let stream = restored_entity.streams.get("Transactions").unwrap();
        assert_eq!(stream.operators.len(), 1);
        assert_eq!(stream.operators[0].0, "tx_count_1h");
        assert_eq!(stream.last_event_at, Some(now));
    }

    #[test]
    fn test_apply_delta_overwrites_existing_entities() {
        use crate::state::store::StateStore;
        let mut store = StateStore::new();
        let now = ts(60_000);

        // Existing entity with count = 1
        let (key, existing) = sample_entity(1, "Transactions", "tx_count_1h", now);
        store.apply_delta(vec![(key.clone(), existing)], vec![]);

        // Apply delta with count = 5 for the same key
        let (_, replacement) = sample_entity(5, "Transactions", "tx_count_1h", now);
        store.apply_delta(vec![(key.clone(), replacement)], vec![]);

        assert_eq!(store.entity_count(), 1);
        let mut val = store
            .get_entity_mut(&key)
            .unwrap()
            .streams
            .get_mut("Transactions")
            .unwrap()
            .operators[0]
            .1
            .clone();
        assert_eq!(val.read(now), FeatureValue::Int(5));
    }

    #[test]
    fn test_apply_delta_removes_deleted_keys() {
        use crate::state::store::StateStore;
        let mut store = StateStore::new();
        let now = ts(60_000);

        let (key, entity) = sample_entity(3, "Transactions", "tx_count_1h", now);
        store.apply_delta(vec![(key.clone(), entity)], vec![]);
        assert_eq!(store.entity_count(), 1);

        store.apply_delta(vec![], vec![key.clone()]);
        assert_eq!(store.entity_count(), 0);
        assert!(store.get_entity(&key).is_none());
    }

    #[test]
    fn test_apply_delta_on_empty_store_works() {
        use crate::state::store::StateStore;
        let mut store = StateStore::new();

        // Applying a delta that deletes a key not in the store is a no-op.
        store.apply_delta(vec![], vec!["ghost".to_string()]);
        assert_eq!(store.entity_count(), 0);
    }

    #[test]
    fn test_apply_delta_change_and_delete_in_single_call() {
        use crate::state::store::StateStore;
        let mut store = StateStore::new();
        let now = ts(60_000);

        // Seed with two entities
        let (k1, e1) = sample_entity(1, "S", "f", now);
        let (k2, e2) = sample_entity(2, "S", "f", now);
        store.apply_delta(vec![(k1.clone(), e1), (k2.clone(), e2)], vec![]);
        assert_eq!(store.entity_count(), 2);

        // Delta: update k1, delete k2
        let (_, e1_new) = sample_entity(9, "S", "f", now);
        store.apply_delta(vec![(k1.clone(), e1_new)], vec![k2.clone()]);

        assert_eq!(store.entity_count(), 1);
        assert!(store.get_entity(&k1).is_some());
        assert!(store.get_entity(&k2).is_none());
    }

    // ======================== Phase 9: Incremental Recovery Tests ========================

    #[test]
    fn test_incremental_recovery_base_plus_two_deltas() {
        use crate::state::store::StateStore;
        let now = ts(60_000);

        // Base snapshot: entities u1, u2
        let base = BaseSnapshotState {
            header: SnapshotHeader {
                snapshot_type: SnapshotType::Base,
                sequence: 10,
            },
            entities: vec![
                {
                    let (_, e) = sample_entity(1, "S", "f", now);
                    ("u1".to_string(), e)
                },
                {
                    let (_, e) = sample_entity(2, "S", "f", now);
                    ("u2".to_string(), e)
                },
            ],
            pipelines: vec![],
            backfill_complete: vec![],
        };

        // Delta 1: update u1 to count=5, insert u3
        let delta1 = DeltaSnapshotState {
            header: SnapshotHeader {
                snapshot_type: SnapshotType::Delta { base_seq: 10 },
                sequence: 11,
            },
            changed_entities: vec![
                {
                    let (_, e) = sample_entity(5, "S", "f", now);
                    ("u1".to_string(), e)
                },
                {
                    let (_, e) = sample_entity(3, "S", "f", now);
                    ("u3".to_string(), e)
                },
            ],
            deleted_keys: vec![],
        };

        // Delta 2: update u3 to count=9
        let delta2 = DeltaSnapshotState {
            header: SnapshotHeader {
                snapshot_type: SnapshotType::Delta { base_seq: 10 },
                sequence: 12,
            },
            changed_entities: vec![{
                let (_, e) = sample_entity(9, "S", "f", now);
                ("u3".to_string(), e)
            }],
            deleted_keys: vec![],
        };

        // Round-trip through bytes to simulate real recovery
        let base_bytes = save_base_snapshot(&base).unwrap();
        let delta1_bytes = save_delta_snapshot(&delta1).unwrap();
        let delta2_bytes = save_delta_snapshot(&delta2).unwrap();

        let mut store = StateStore::new();
        // Apply base
        match load_snapshot_file(&base_bytes).unwrap() {
            SnapshotFile::Base(b) => store.restore_from_snapshot(b.entities),
            _ => panic!(),
        }
        // Apply deltas in order (by sequence)
        for bytes in &[&delta1_bytes, &delta2_bytes] {
            match load_snapshot_file(bytes).unwrap() {
                SnapshotFile::Delta(d) => store.apply_delta(d.changed_entities, d.deleted_keys),
                _ => panic!(),
            }
        }

        assert_eq!(store.entity_count(), 3);
        assert_eq!(
            store.get_feature_value("u1", "f", now),
            FeatureValue::Int(5)
        );
        assert_eq!(
            store.get_feature_value("u2", "f", now),
            FeatureValue::Int(2)
        );
        assert_eq!(
            store.get_feature_value("u3", "f", now),
            FeatureValue::Int(9)
        );
    }

    #[test]
    fn test_incremental_recovery_with_deleted_keys() {
        use crate::state::store::StateStore;
        let now = ts(60_000);

        let base = BaseSnapshotState {
            header: SnapshotHeader {
                snapshot_type: SnapshotType::Base,
                sequence: 1,
            },
            entities: vec![
                {
                    let (_, e) = sample_entity(1, "S", "f", now);
                    ("u1".to_string(), e)
                },
                {
                    let (_, e) = sample_entity(2, "S", "f", now);
                    ("u2".to_string(), e)
                },
            ],
            pipelines: vec![],
            backfill_complete: vec![],
        };

        let delta = DeltaSnapshotState {
            header: SnapshotHeader {
                snapshot_type: SnapshotType::Delta { base_seq: 1 },
                sequence: 2,
            },
            changed_entities: vec![],
            deleted_keys: vec!["u2".to_string()],
        };

        let mut store = StateStore::new();
        store.restore_from_snapshot(base.entities);
        store.apply_delta(delta.changed_entities, delta.deleted_keys);

        assert_eq!(store.entity_count(), 1);
        assert!(store.get_entity("u1").is_some());
        assert!(
            store.get_entity("u2").is_none(),
            "u2 should have been removed by delta"
        );
    }

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
            },
            changed_entities: vec![],
            deleted_keys: vec![],
        };
        let bytes = save_delta_snapshot(&delta).unwrap();
        match load_snapshot_file(&bytes).unwrap() {
            SnapshotFile::Delta(d) => {
                assert_eq!(d.header.sequence, 1001);
                assert_eq!(d.header.snapshot_type, SnapshotType::Delta { base_seq: 1000 });
            }
            _ => panic!(),
        }
    }
}
