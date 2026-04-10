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
use crate::engine::operators::{CountOp, SumOp, AvgOp, MinOp, MaxOp, LastOp, Operator};
use crate::engine::hll::DistinctCountOp;
use crate::state::store::StaticFeature;
use crate::types::FeatureValue;
use crate::error::TallyError;

/// Snapshot format version byte. Prepended to serialized data.
/// If the version doesn't match on load, return None (clean startup from empty state).
/// Bumped to 5 for backfill_complete field in SnapshotState (SCHM-03).
const SNAPSHOT_FORMAT_VERSION: u8 = 5;

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

/// Serialize a SnapshotState to bytes with a version prefix.
/// Format: [1 byte version][postcard-encoded SnapshotState]
/// Returns an error if postcard serialization fails (e.g., due to
/// unsupported types or internal limits), instead of panicking.
pub fn save_snapshot(data: &SnapshotState) -> Result<Vec<u8>, postcard::Error> {
    let mut buf = vec![SNAPSHOT_FORMAT_VERSION];
    buf.extend_from_slice(&postcard::to_stdvec(data)?);
    Ok(buf)
}

/// Deserialize a SnapshotState from bytes.
/// Returns None if:
/// - bytes is empty
/// - version byte doesn't match SNAPSHOT_FORMAT_VERSION
/// - postcard deserialization fails (corrupt data)
pub fn load_snapshot(bytes: &[u8]) -> Option<SnapshotState> {
    if bytes.is_empty() {
        return None;
    }
    let version = bytes[0];
    if version != SNAPSHOT_FORMAT_VERSION {
        eprintln!(
            "Snapshot version mismatch: found {}, expected {}. Starting fresh.",
            version, SNAPSHOT_FORMAT_VERSION
        );
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
        assert_eq!(bytes[0], 0x05);
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
        // Tamper with version byte
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
    fn test_snapshot_format_version_is_5() {
        assert_eq!(SNAPSHOT_FORMAT_VERSION, 5);
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
}
