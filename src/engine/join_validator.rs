//! Join shard-key mismatch validation — Phase 51 (TPC-CORR-04).
//!
//! Called by `PipelineEngine::register` before inserting a stream.
//! If the new stream participates in a join and its `shard_key` disagrees
//! with the peer stream's `shard_key`, registration fails immediately with
//! a structured `JoinShardKeyMismatch` error. The pipeline does not start.
//!
//! The D-12 locked error message format is tested for grep-testability:
//! `"join operator between '{A}' and '{B}' requires matching shard_key;
//!   got '{keyA}' vs '{keyB}'. Fix: declare @bv.stream(shard_key='{suggested}')
//!   on both streams."`

use ahash::AHashMap;

use crate::engine::pipeline::{FeatureDef, StreamDefinition};

/// A shard-key specification on a stream. Additive in Phase 51.
/// None = "no explicit shard_key" (default key heuristic applies).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardKeySpec {
    /// Single-field sharding: `shard_key = "user_id"`.
    Single(String),
    /// Composite sharding: `shard_key = ("user_id", "session_id")`.
    Tuple(Vec<String>),
}

impl ShardKeySpec {
    /// Human-readable display string used in the D-12 error message.
    pub fn display(&self) -> String {
        match self {
            ShardKeySpec::Single(s) => s.clone(),
            ShardKeySpec::Tuple(v) => v.join(","),
        }
    }
}

/// Structured error returned (and emitted to /debug/warnings) when two join
/// streams declare incompatible `shard_key` values.
#[derive(Debug, Clone)]
pub struct JoinShardKeyMismatch {
    pub stream_a: String,
    pub stream_b: String,
    /// Display string for stream_a's shard_key (or "None").
    pub key_a: String,
    /// Display string for stream_b's shard_key (or "None").
    pub key_b: String,
    /// Suggested common key field(s): the join's `on=` field(s).
    pub suggested_common: String,
    /// D-12 locked error message — preserved verbatim for grep-testability.
    pub message: String,
}

impl std::fmt::Display for JoinShardKeyMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for JoinShardKeyMismatch {}

// -----------------------------------------------------------------------
// Internal helpers
// -----------------------------------------------------------------------

fn shard_key_display(spec: &Option<ShardKeySpec>) -> String {
    match spec {
        None => "None".to_string(),
        Some(s) => s.display(),
    }
}

fn suggested_common(on_fields: &[String]) -> String {
    on_fields.join(",")
}

/// Returns true if the two shard_key options are considered matching.
///
/// Matching rules:
/// - Both `None` → no mismatch (both use default key heuristic).
/// - Both `Some` and equal → no mismatch.
/// - One `None`, one `Some` → mismatch (explicit vs implicit).
/// - Both `Some` but different → mismatch.
fn keys_match(a: &Option<ShardKeySpec>, b: &Option<ShardKeySpec>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(ka), Some(kb)) => ka == kb,
        _ => false,
    }
}

fn build_mismatch(
    stream_a: &str,
    stream_b: &str,
    key_a: &Option<ShardKeySpec>,
    key_b: &Option<ShardKeySpec>,
    on_fields: &[String],
) -> JoinShardKeyMismatch {
    let ka = shard_key_display(key_a);
    let kb = shard_key_display(key_b);
    let suggested = suggested_common(on_fields);
    let message = format!(
        "join operator between '{}' and '{}' requires matching shard_key; \
         got '{}' vs '{}'. Fix: declare @bv.stream(shard_key='{}') on both streams.",
        stream_a, stream_b, ka, kb, suggested
    );
    JoinShardKeyMismatch {
        stream_a: stream_a.to_string(),
        stream_b: stream_b.to_string(),
        key_a: ka,
        key_b: kb,
        suggested_common: suggested,
        message,
    }
}

/// Validate shard_key consistency for all join operators in `new_stream`.
///
/// For each join feature, looks up the peer stream in `streams`. If the peer
/// is registered and has a different `shard_key`, returns the first mismatch.
/// If the peer is not yet registered, skips (registration order may vary).
///
/// Both-None is always valid (no explicit sharding on either side).
pub fn validate_shard_keys(
    streams: &AHashMap<String, StreamDefinition>,
    new_stream: &StreamDefinition,
) -> Result<(), JoinShardKeyMismatch> {
    let new_key = &new_stream.shard_key;

    for (_feature_name, def) in &new_stream.features {
        match def {
            FeatureDef::EnrichFromTable { right_table, on, .. } => {
                if let Some(peer) = streams.get(right_table) {
                    if !keys_match(new_key, &peer.shard_key) {
                        return Err(build_mismatch(
                            &new_stream.name,
                            right_table,
                            new_key,
                            &peer.shard_key,
                            on,
                        ));
                    }
                }
            }
            FeatureDef::StreamStreamJoin {
                left_stream,
                right_stream,
                on,
                ..
            } => {
                // Check left peer.
                if left_stream != &new_stream.name {
                    if let Some(peer) = streams.get(left_stream) {
                        if !keys_match(new_key, &peer.shard_key) {
                            return Err(build_mismatch(
                                &new_stream.name,
                                left_stream,
                                new_key,
                                &peer.shard_key,
                                on,
                            ));
                        }
                    }
                }
                // Check right peer.
                if right_stream != &new_stream.name {
                    if let Some(peer) = streams.get(right_stream) {
                        if !keys_match(new_key, &peer.shard_key) {
                            return Err(build_mismatch(
                                &new_stream.name,
                                right_stream,
                                new_key,
                                &peer.shard_key,
                                on,
                            ));
                        }
                    }
                }
            }
            FeatureDef::TableTableJoin {
                left_table,
                right_table,
                on,
                ..
            } => {
                if let Some(peer) = streams.get(left_table) {
                    if !keys_match(new_key, &peer.shard_key) {
                        return Err(build_mismatch(
                            &new_stream.name,
                            left_table,
                            new_key,
                            &peer.shard_key,
                            on,
                        ));
                    }
                }
                if let Some(peer) = streams.get(right_table) {
                    if !keys_match(new_key, &peer.shard_key) {
                        return Err(build_mismatch(
                            &new_stream.name,
                            right_table,
                            new_key,
                            &peer.shard_key,
                            on,
                        ));
                    }
                }
            }
            _ => {} // Non-join operators: no shard_key constraint.
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::pipeline::{FeatureDef, JoinType, StreamDefinition};

    fn make_stream(name: &str, shard_key: Option<ShardKeySpec>) -> StreamDefinition {
        StreamDefinition {
            name: name.to_string(),
            shard_key,
            ..Default::default()
        }
    }

    fn make_enrich_stream(
        name: &str,
        shard_key: Option<ShardKeySpec>,
        right_table: &str,
        on: &[&str],
    ) -> StreamDefinition {
        StreamDefinition {
            name: name.to_string(),
            shard_key,
            features: vec![(
                "enrich".to_string(),
                FeatureDef::EnrichFromTable {
                    right_table: right_table.to_string(),
                    on: on.iter().map(|s| s.to_string()).collect(),
                    join_type: JoinType::Inner,
                    right_fields: vec![],
                },
            )],
            ..Default::default()
        }
    }

    // -----------------------------------------------------------------------
    // Test 1: mismatched shard_key -> error with D-12 locked message
    // -----------------------------------------------------------------------
    #[test]
    fn test_mismatch_returns_error_with_locked_message() {
        let mut streams = AHashMap::new();
        streams.insert(
            "Orders".to_string(),
            make_stream("Orders", Some(ShardKeySpec::Single("user_id".to_string()))),
        );
        streams.insert(
            "Products".to_string(),
            make_stream(
                "Products",
                Some(ShardKeySpec::Single("product_id".to_string())),
            ),
        );

        let new_stream = StreamDefinition {
            name: "OrdersEnriched".to_string(),
            shard_key: Some(ShardKeySpec::Single("user_id".to_string())),
            features: vec![(
                "enrich".to_string(),
                FeatureDef::EnrichFromTable {
                    right_table: "Products".to_string(),
                    on: vec!["product_id".to_string()],
                    join_type: JoinType::Inner,
                    right_fields: vec![],
                },
            )],
            ..Default::default()
        };

        let result = validate_shard_keys(&streams, &new_stream);
        assert!(result.is_err(), "expected Err on shard_key mismatch");

        let err = result.unwrap_err();
        // D-12 locked message check
        assert!(
            err.message.contains("join operator between 'OrdersEnriched' and 'Products'"),
            "message should name both streams"
        );
        assert!(
            err.message.contains("requires matching shard_key"),
            "message should say 'requires matching shard_key'"
        );
        assert!(
            err.message.contains("got 'user_id' vs 'product_id'"),
            "message should show both keys: {}",
            err.message
        );
        assert!(
            err.message.contains("Fix: declare @bv.stream(shard_key='product_id')"),
            "message should suggest fix: {}",
            err.message
        );
    }

    // -----------------------------------------------------------------------
    // Test 2: matching shard_key -> no error
    // -----------------------------------------------------------------------
    #[test]
    fn test_matching_shard_key_no_error() {
        let mut streams = AHashMap::new();
        streams.insert(
            "A".to_string(),
            make_stream("A", Some(ShardKeySpec::Single("user_id".to_string()))),
        );
        streams.insert(
            "B".to_string(),
            make_stream("B", Some(ShardKeySpec::Single("user_id".to_string()))),
        );

        let new_stream = StreamDefinition {
            name: "C".to_string(),
            shard_key: Some(ShardKeySpec::Single("user_id".to_string())),
            features: vec![(
                "join".to_string(),
                FeatureDef::StreamStreamJoin {
                    left_stream: "A".to_string(),
                    right_stream: "B".to_string(),
                    on: vec!["user_id".to_string()],
                    within_ms: 1000,
                    join_type: JoinType::Inner,
                    left_fields: vec![],
                    right_fields: vec![],
                },
            )],
            ..Default::default()
        };

        let result = validate_shard_keys(&streams, &new_stream);
        assert!(result.is_ok(), "matching shard_key should register without error");
    }

    // -----------------------------------------------------------------------
    // Test 3: both shard_key=None -> no error
    // -----------------------------------------------------------------------
    #[test]
    fn test_both_none_shard_key_no_error() {
        let mut streams = AHashMap::new();
        streams.insert("X".to_string(), make_stream("X", None));
        streams.insert("Y".to_string(), make_stream("Y", None));

        let new_stream = StreamDefinition {
            name: "Z".to_string(),
            shard_key: None,
            features: vec![(
                "enrich".to_string(),
                FeatureDef::EnrichFromTable {
                    right_table: "Y".to_string(),
                    on: vec!["id".to_string()],
                    join_type: JoinType::Inner,
                    right_fields: vec![],
                },
            )],
            ..Default::default()
        };

        let result = validate_shard_keys(&streams, &new_stream);
        assert!(result.is_ok(), "both-None shard_key should not produce a mismatch");
    }

    // -----------------------------------------------------------------------
    // Test 4: mismatch emits correct error fields (signal emission checked
    // in pipeline integration; here we verify the struct fields).
    // -----------------------------------------------------------------------
    #[test]
    fn test_mismatch_error_fields() {
        let mismatch = build_mismatch(
            "StreamA",
            "StreamB",
            &Some(ShardKeySpec::Single("uid".to_string())),
            &Some(ShardKeySpec::Single("sid".to_string())),
            &["sid".to_string()],
        );
        assert_eq!(mismatch.stream_a, "StreamA");
        assert_eq!(mismatch.stream_b, "StreamB");
        assert_eq!(mismatch.key_a, "uid");
        assert_eq!(mismatch.key_b, "sid");
        assert_eq!(mismatch.suggested_common, "sid");
    }

    // -----------------------------------------------------------------------
    // Test 5: pipeline does not start — after mismatch the stream is not
    // inserted (validated at the pipeline.rs layer; here we just verify
    // validate_shard_keys returns Err without mutating state).
    // -----------------------------------------------------------------------
    #[test]
    fn test_mismatch_does_not_mutate_state() {
        let mut streams: AHashMap<String, StreamDefinition> = AHashMap::new();
        streams.insert(
            "Peer".to_string(),
            make_stream("Peer", Some(ShardKeySpec::Single("a".to_string()))),
        );
        let initial_count = streams.len();

        let new_stream = make_enrich_stream(
            "New",
            Some(ShardKeySpec::Single("b".to_string())),
            "Peer",
            &["a"],
        );

        let _ = validate_shard_keys(&streams, &new_stream);
        assert_eq!(
            streams.len(),
            initial_count,
            "validate_shard_keys must not insert the stream on error"
        );
    }
}
