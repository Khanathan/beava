//! Client-side engine embedding. Phase 28 ships the type surface;
//! Phase 28-04 wires the real `FrozenClient` + `run_clone` (historical
//! snapshot bootstrap).
//!
//! This module is UNCONDITIONAL (not feature-gated) so both server and
//! client builds compile it.

pub mod wire;

// `clone` depends on tokio + rand. Both are already on the default-feature
// dep path (server uses tokio heavily), so exposing it unconditionally is
// cheap and makes tests runnable under `cargo test` without feature gymnastics.
pub mod clone;

use crate::state::snapshot::SerializableEntityState;
use crate::state::store::StateStore;
use std::time::SystemTime;

/// Session execution mode.
///
/// Phase 28 ships only `Historical` (bounded replay from S3 log).
/// `Streaming` is added in Phase 31.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionMode {
    Historical,
    // Streaming, // Phase 31 — tails the live log after historical catch-up.
}

/// Client-side session handle.
///
/// Stub from Phase 28-03 retained as a convenience constructor; the real
/// Phase 28-04 queryable handle is `FrozenClient` produced by
/// `client::clone::run_clone`.
#[derive(Debug, Clone)]
pub struct Session {
    pub remote: String,
    pub streams: Vec<String>,
    pub keys: Option<Vec<String>>,
    pub key_prefix: Option<String>,
    pub mode: SessionMode,
    pub token: Option<String>,
}

impl Session {
    pub fn new(remote: impl Into<String>, streams: Vec<String>) -> Self {
        Self {
            remote: remote.into(),
            streams,
            keys: None,
            key_prefix: None,
            mode: SessionMode::Historical,
            token: None,
        }
    }
}

/// Raised by `FrozenClient::get` when a query targets a stream / key outside
/// the declared scope.
#[derive(Debug, thiserror::Error)]
#[error("query out of scope: {0}")]
pub struct OutOfScopeError(pub String);

impl OutOfScopeError {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
}

/// Phase 28-04: queryable, read-only client handle populated by a one-shot
/// historical-clone bootstrap (`client::clone::run_clone`).
///
/// Invariants:
///   - `state` holds the bulk-loaded snapshot entities (never mutated after
///     construction in Phase 28).
///   - `scope` is the exact scope sent on the wire; queries outside it
///     return `OutOfScopeError`.
///   - `snapshot_taken_at` is preserved from the server's header frame for
///     Phase 31 streaming-catchup to use as its `since` cursor.
#[derive(Debug)]
pub struct FrozenClient {
    pub(crate) state: StateStore,
    pub(crate) scope: wire::Scope,
    /// Server-reported snapshot timestamp. Phase 31 will use this as the
    /// `since` cursor for streaming catch-up.
    pub snapshot_taken_at: SystemTime,
}

impl FrozenClient {
    pub fn new(state: StateStore, scope: wire::Scope, snapshot_taken_at: SystemTime) -> Self {
        Self { state, scope, snapshot_taken_at }
    }

    /// Scope-aware lookup of an entity's aggregated state.
    ///
    /// Returns:
    ///   - `Err(OutOfScopeError)` when `stream` is not in `scope.streams`, or
    ///     `key` falls outside `scope.keys` / `scope.key_prefix` (when set).
    ///   - `Ok(Some(state))` when the key is in-scope and present in the store.
    ///   - `Ok(None)` when the key is in-scope but absent (server filtered it out
    ///     or it simply doesn't exist).
    pub fn get(
        &self,
        stream: &str,
        key: &str,
    ) -> Result<Option<SerializableEntityState>, OutOfScopeError> {
        // 1. Stream membership.
        if !self.scope.streams.iter().any(|s| s == stream) {
            return Err(OutOfScopeError::new(format!(
                "stream {:?} not in declared scope {:?}",
                stream, self.scope.streams
            )));
        }
        // 2. Key membership: keys-set XOR key_prefix. When both are None the
        //    scope is stream-only and any key is allowed.
        if let Some(keys) = &self.scope.keys {
            if !keys.iter().any(|k| k == key) {
                return Err(OutOfScopeError::new(format!(
                    "key {:?} not in declared keys set (stream {:?})",
                    key, stream
                )));
            }
        } else if let Some(prefix) = &self.scope.key_prefix {
            if !key.starts_with(prefix.as_str()) {
                return Err(OutOfScopeError::new(format!(
                    "key {:?} does not match declared prefix {:?} (stream {:?})",
                    key, prefix, stream
                )));
            }
        }
        // 3. Store lookup. Materialise the ref into a clone so the returned
        //    `SerializableEntityState` isn't tied to the DashMap guard's lifetime.
        Ok(self
            .state
            .get_entity(key)
            .map(|e| serialize_entity(key, &e)))
    }

    /// Return every (stream, key, aggregated-state) triple in the bulk-loaded
    /// store. Used by `tally_cli clone --dump-json` for test ergonomics.
    ///
    /// Each distinct `(stream_name, entity_key)` pair is emitted once; for an
    /// entity that participates in N streams this yields N rows.
    pub fn iter_entities(&self) -> Vec<(String, String, SerializableEntityState)> {
        let mut out = Vec::new();
        // `clone_for_snapshot` gives us a Vec<(key, SerializableEntityState)> —
        // matches the bulk_load input shape.
        for (key, entity_state) in self.state.clone_for_snapshot() {
            for (stream_name, _stream_state) in &entity_state.streams {
                out.push((stream_name.clone(), key.clone(), entity_state.clone()));
            }
        }
        out
    }

    /// Read-only accessor for the declared scope (used by `--dump-json`).
    pub fn scope(&self) -> &wire::Scope {
        &self.scope
    }
}

/// Helper: build a `SerializableEntityState` from a live `EntityState` DashMap
/// ref. Reuses the per-stream serialization path without borrowing against the
/// lock guard's lifetime.
fn serialize_entity(
    _key: &str,
    entity: &crate::state::store::EntityState,
) -> SerializableEntityState {
    use crate::state::snapshot::SerializableStreamEntityState;
    use crate::state::store::SerializableTableRow;
    SerializableEntityState {
        streams: entity
            .streams
            .iter()
            .map(|(name, s)| {
                (
                    name.clone(),
                    SerializableStreamEntityState {
                        operators: s.operators.clone(),
                        last_event_at: s.last_event_at,
                    },
                )
            })
            .collect(),
        static_features: entity
            .static_features
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        table_rows: entity
            .table_rows
            .iter()
            .map(|(k, v)| (k.clone(), SerializableTableRow::from(v)))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::operators::CountOp;
    use crate::state::snapshot::{
        OperatorState, SerializableEntityState, SerializableStreamEntityState,
    };
    use std::time::{Duration, UNIX_EPOCH};

    fn scope_streams_only(streams: Vec<&str>) -> wire::Scope {
        wire::Scope {
            streams: streams.into_iter().map(|s| s.into()).collect(),
            keys: None,
            key_prefix: None,
            pull: "all".into(),
        }
    }

    fn entity_for(stream: &str) -> SerializableEntityState {
        let now = UNIX_EPOCH + Duration::from_secs(1000);
        let mut op = OperatorState::Count(CountOp::new(
            Duration::from_secs(3600),
            Duration::from_secs(60),
        ));
        op.push(&serde_json::json!({}), None, now).unwrap();
        SerializableEntityState {
            streams: vec![(
                stream.to_string(),
                SerializableStreamEntityState {
                    operators: vec![("count".to_string(), op)],
                    last_event_at: Some(now),
                },
            )],
            static_features: vec![],
            table_rows: vec![],
        }
    }

    fn frozen_with(scope: wire::Scope, entities: Vec<(String, SerializableEntityState)>) -> FrozenClient {
        let store = StateStore::new();
        store.bulk_load(entities);
        FrozenClient::new(store, scope, UNIX_EPOCH)
    }

    #[test]
    fn session_new_defaults() {
        let s = Session::new("host:6400", vec!["Txn".into()]);
        assert_eq!(s.remote, "host:6400");
        assert_eq!(s.streams, vec!["Txn".to_string()]);
        assert!(matches!(s.mode, SessionMode::Historical));
    }

    #[test]
    fn out_of_scope_display() {
        let e = OutOfScopeError::new("foo");
        assert_eq!(format!("{}", e), "query out of scope: foo");
    }

    #[test]
    fn frozen_get_rejects_unlisted_stream() {
        let fc = frozen_with(scope_streams_only(vec!["A"]), vec![]);
        let err = fc.get("Unlisted", "k").unwrap_err();
        assert!(format!("{}", err).contains("Unlisted"));
    }

    #[test]
    fn frozen_get_rejects_key_not_in_keys_set() {
        let scope = wire::Scope {
            streams: vec!["A".into()],
            keys: Some(vec!["k1".into()]),
            key_prefix: None,
            pull: "all".into(),
        };
        let fc = frozen_with(scope, vec![]);
        let err = fc.get("A", "k2").unwrap_err();
        assert!(format!("{}", err).contains("k2"));
    }

    #[test]
    fn frozen_get_accepts_prefix_match_and_rejects_non_prefix() {
        let scope = wire::Scope {
            streams: vec!["A".into()],
            keys: None,
            key_prefix: Some("pre_".into()),
            pull: "all".into(),
        };
        let entities = vec![("pre_match".to_string(), entity_for("A"))];
        let fc = frozen_with(scope, entities);
        // in-scope hit
        let got = fc.get("A", "pre_match").unwrap();
        assert!(got.is_some());
        // in-scope miss (prefix ok, but not loaded)
        let miss = fc.get("A", "pre_absent").unwrap();
        assert!(miss.is_none());
        // out-of-scope: wrong prefix
        let err = fc.get("A", "other_key").unwrap_err();
        assert!(format!("{}", err).contains("pre_"));
    }

    #[test]
    fn frozen_get_stream_only_scope_allows_any_key() {
        let entities = vec![("k_any".to_string(), entity_for("A"))];
        let fc = frozen_with(scope_streams_only(vec!["A"]), entities);
        let got = fc.get("A", "k_any").unwrap();
        assert!(got.is_some());
        let miss = fc.get("A", "k_other").unwrap();
        assert!(miss.is_none());
    }

    #[test]
    fn frozen_snapshot_taken_at_preserved() {
        let t = UNIX_EPOCH + Duration::from_secs(42);
        let store = StateStore::new();
        let fc = FrozenClient::new(store, scope_streams_only(vec!["A"]), t);
        assert_eq!(fc.snapshot_taken_at, t);
    }

    #[test]
    fn iter_entities_walks_all_streams() {
        let entities = vec![
            ("u1".to_string(), entity_for("A")),
            ("u2".to_string(), entity_for("B")),
        ];
        let fc = frozen_with(
            wire::Scope {
                streams: vec!["A".into(), "B".into()],
                keys: None,
                key_prefix: None,
                pull: "all".into(),
            },
            entities,
        );
        let rows = fc.iter_entities();
        assert_eq!(rows.len(), 2);
        let pairs: std::collections::HashSet<(String, String)> =
            rows.iter().map(|(s, k, _)| (s.clone(), k.clone())).collect();
        assert!(pairs.contains(&("A".to_string(), "u1".to_string())));
        assert!(pairs.contains(&("B".to_string(), "u2".to_string())));
    }
}
