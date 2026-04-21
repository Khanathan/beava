//! Join shard-key mismatch validation — Phase 51 (TPC-CORR-04) / Phase 56
//! (D-B4 + D-C1..D-C3 — register-time relaxation).
//!
//! Called by `PipelineEngine::register` before inserting a stream. If the new
//! stream participates in a join and its `shard_key` disagrees with the peer
//! stream's `shard_key`, registration **no longer errors**. Instead it emits
//! a `CrossShardJoinWarning` per mismatched peer pair (Phase 56 D-B4):
//!
//! - `tracing::warn!` with context (join_id, shard_keys, on_field, perf note).
//! - `beava_crossshard_joins_registered_total{join_id}` counter increments.
//! - Signal registry records the warning; `/debug/warnings` surfaces it.
//!
//! The `JoinShardKeyMismatch` struct + `build_mismatch` helper remain for
//! back-compat with external callers who still match on the type (Phase 56
//! additive-not-destructive rule). They are `#[deprecated]`-marked.
//!
//! The D-12 locked error message format is preserved verbatim inside the
//! `CrossShardJoinWarning.message` for grep-testability:
//! `"join operator between '{A}' and '{B}' requires matching shard_key;
//!   got '{keyA}' vs '{keyB}'. Fix: declare @bv.stream(shard_key='{suggested}')
//!   on both streams."`

use ahash::AHashMap;
use serde::{Deserialize, Serialize};

use crate::engine::pipeline::{FeatureDef, StreamDefinition};

/// A shard-key specification on a stream. Additive in Phase 51.
/// None = "no explicit shard_key" (default key heuristic applies).
///
/// `#[serde(untagged)]` lets a JSON string deserialize as `Single` and
/// a JSON array as `Tuple`, matching `_serialize.py` output (D-07/TPC-DX-01).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
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
///
/// **Phase 56 D-C2:** this type is retained for back-compat (external
/// callers matching on `BeavaError::Protocol(mismatch.message.clone())` from
/// Phase 51), but `register()` **no longer raises** it. The runtime path
/// emits `CrossShardJoinWarning` instead. A handful of Phase 51 internals
/// (`emit_join_shard_key_mismatch`, `build_mismatch`) continue to use this
/// type for their locked-message formatting — hence the `#[deprecated]`
/// marker is advisory rather than a compile error.
#[deprecated(
    since = "56.0",
    note = "Phase 56 D-C2 relaxation: register() no longer rejects mismatched \
            shard_keys. Use `CrossShardJoinWarning` instead; this struct is \
            retained for back-compat with callers who matched on the \
            Phase 51 error variant."
)]
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

/// Phase 56 D-B4 / D-C1 / D-C2 — non-fatal warning emitted by
/// `validate_shard_keys` when a join's left/right shard_keys mismatch.
///
/// Produced instead of `JoinShardKeyMismatch` at register time. The engine
/// proceeds with registration; at runtime, Phase 56 D-B1 + Wave 1's
/// `ssj_insert_at_shard` routes both sides to `hash(join.on) % N` so the
/// join still evaluates correctly. The warning is advisory: it documents
/// the **perf cost** (+1 inbox hop per event) and points the operator at
/// the co-location fix.
///
/// Flows through three surfaces (D-B4 + D-C1):
/// - `tracing::warn!` — structured log line.
/// - `beava_crossshard_joins_registered_total{join_id}` — counter.
/// - `/debug/warnings` (`cross_shard_joins` array) — operator-visible surface.
#[derive(Debug, Clone, Serialize)]
pub struct CrossShardJoinWarning {
    /// Stable synthetic id: `"{stream_a}_x_{stream_b}_on_{on_field}"`.
    pub join_id: String,
    pub stream_a: String,
    pub stream_b: String,
    /// Display string for the NEW (registering) stream's shard_key.
    pub left_shard_key: String,
    /// Display string for the peer stream's shard_key.
    pub right_shard_key: String,
    /// Join's `on=` field(s), comma-joined.
    pub on_field: String,
    /// Perf note — reminder of the +1 inbox hop + co-location fix.
    pub perf_note: String,
    /// Human-readable single-line summary (log-friendly). Preserves the
    /// D-12 locked "requires matching shard_key; got ..." substring for
    /// grep-testability across old and new call sites.
    pub message: String,
}

impl CrossShardJoinWarning {
    /// Build a warning for a mismatched peer pair. `stream_a` is the new
    /// stream being registered; `stream_b` is the already-registered peer.
    pub fn new(
        stream_a: &str,
        stream_b: &str,
        left_shard_key: &str,
        right_shard_key: &str,
        on_field: &str,
    ) -> Self {
        let join_id = format!("{}_x_{}_on_{}", stream_a, stream_b, on_field);
        let perf_note = format!(
            "Both sides shuffled to hash({}) % N; expect +1 inbox hop per event. \
             Co-locate by setting shard_key='{}' on both streams to remove the hop.",
            on_field, on_field
        );
        // Preserve the D-12 locked message substring so anything grepping
        // for "requires matching shard_key" still finds it.
        let message = format!(
            "CrossShardJoinWarning: join '{}' between '{}' and '{}' requires \
             matching shard_key; got '{}' vs '{}' on '{}'. {}",
            join_id, stream_a, stream_b, left_shard_key, right_shard_key, on_field, perf_note
        );
        Self {
            join_id,
            stream_a: stream_a.to_string(),
            stream_b: stream_b.to_string(),
            left_shard_key: left_shard_key.to_string(),
            right_shard_key: right_shard_key.to_string(),
            on_field: on_field.to_string(),
            perf_note,
            message,
        }
    }
}

#[allow(deprecated)]
impl std::fmt::Display for JoinShardKeyMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

#[allow(deprecated)]
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

#[allow(deprecated, dead_code)]
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

/// Phase 56 D-B4 — validate shard_key compatibility for all join operators
/// in `new_stream` and return a (possibly empty) vector of warnings.
///
/// For each join feature:
///   - Look up the peer stream in `streams`.
///   - If the peer is registered and its `shard_key` mismatches the new
///     stream's `shard_key`, push a `CrossShardJoinWarning` describing the
///     pair.
///   - If the peer is not yet registered, skip (registration order may vary
///     and the check re-runs when the other stream registers later).
///   - Both-None (both implicit) is always OK — no warning.
///
/// **Never returns Err.** The Phase 51 hard-reject behaviour is removed by
/// D-C2. Callers handle the returned warnings by:
///   - `tracing::warn!` on each entry.
///   - Incrementing `CROSSSHARD_JOINS_REGISTERED_TOTAL{join_id}`.
///   - Recording into the signal registry / `/debug/warnings`.
pub fn validate_shard_keys(
    streams: &AHashMap<String, StreamDefinition>,
    new_stream: &StreamDefinition,
) -> Vec<CrossShardJoinWarning> {
    let mut out: Vec<CrossShardJoinWarning> = Vec::new();
    let new_key = &new_stream.shard_key;

    let push_warning = |out: &mut Vec<CrossShardJoinWarning>,
                        peer_name: &str,
                        peer_key: &Option<ShardKeySpec>,
                        on_fields: &[String]| {
        let warning = CrossShardJoinWarning::new(
            &new_stream.name,
            peer_name,
            &shard_key_display(new_key),
            &shard_key_display(peer_key),
            &suggested_common(on_fields),
        );
        // Dedupe by join_id inside the returned Vec (multiple peers may
        // produce the same synthetic id if the stream pair + on field
        // match; stay defensive).
        if out.iter().all(|w| w.join_id != warning.join_id) {
            out.push(warning);
        }
    };

    for (_feature_name, def) in &new_stream.features {
        match def {
            FeatureDef::EnrichFromTable {
                right_table, on, ..
            } => {
                if let Some(peer) = streams.get(right_table) {
                    if !keys_match(new_key, &peer.shard_key) {
                        push_warning(&mut out, right_table, &peer.shard_key, on);
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
                            push_warning(&mut out, left_stream, &peer.shard_key, on);
                        }
                    }
                }
                // Check right peer.
                if right_stream != &new_stream.name {
                    if let Some(peer) = streams.get(right_stream) {
                        if !keys_match(new_key, &peer.shard_key) {
                            push_warning(&mut out, right_stream, &peer.shard_key, on);
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
                        push_warning(&mut out, left_table, &peer.shard_key, on);
                    }
                }
                if let Some(peer) = streams.get(right_table) {
                    if !keys_match(new_key, &peer.shard_key) {
                        push_warning(&mut out, right_table, &peer.shard_key, on);
                    }
                }
            }
            _ => {} // Non-join operators: no shard_key constraint.
        }
    }
    out
}

#[cfg(test)]
#[allow(deprecated)]
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
    // Test 1 (Phase 56 D-B4 relaxation): mismatched shard_key -> returns a
    // CrossShardJoinWarning (not an Err). Locked D-12 message text retained
    // inside `warning.message` for grep-testability.
    // -----------------------------------------------------------------------
    #[test]
    fn test_mismatch_returns_warning_with_locked_message() {
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

        let warnings = validate_shard_keys(&streams, &new_stream);
        assert_eq!(warnings.len(), 1, "expected exactly one mismatch warning");
        let w = &warnings[0];
        assert_eq!(w.stream_a, "OrdersEnriched");
        assert_eq!(w.stream_b, "Products");
        assert_eq!(w.left_shard_key, "user_id");
        assert_eq!(w.right_shard_key, "product_id");
        assert_eq!(w.on_field, "product_id");
        // D-12 locked substring retained inside message for grep-testability.
        assert!(
            w.message.contains("requires matching shard_key"),
            "message should say 'requires matching shard_key': {}",
            w.message
        );
        assert!(
            w.message.contains("'user_id' vs 'product_id'"),
            "message should show both keys: {}",
            w.message
        );
        assert!(
            w.perf_note.contains("+1 inbox hop"),
            "perf_note should mention '+1 inbox hop': {}",
            w.perf_note
        );
        // join_id synthesis pattern
        assert_eq!(
            w.join_id, "OrdersEnriched_x_Products_on_product_id",
            "stable synthetic join_id"
        );
    }

    // -----------------------------------------------------------------------
    // Test 2: matching shard_key -> no warnings
    // -----------------------------------------------------------------------
    #[test]
    fn test_matching_shard_key_no_warning() {
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

        let warnings = validate_shard_keys(&streams, &new_stream);
        assert!(
            warnings.is_empty(),
            "matching shard_key should register without warnings"
        );
    }

    // -----------------------------------------------------------------------
    // Test 3: both shard_key=None -> no warnings (D-B5 implicit co-location).
    // -----------------------------------------------------------------------
    #[test]
    fn test_both_none_shard_key_no_warning() {
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

        let warnings = validate_shard_keys(&streams, &new_stream);
        assert!(
            warnings.is_empty(),
            "both-None shard_key should not produce a mismatch"
        );
    }

    // -----------------------------------------------------------------------
    // Test 4: back-compat — JoinShardKeyMismatch + build_mismatch fields.
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
    // Test 5: validate_shard_keys never returns Err — mismatch no longer
    // blocks registration (D-C2). We assert the Vec return type explicitly
    // plus that no state was mutated (it takes `&` references anyway).
    // -----------------------------------------------------------------------
    #[test]
    fn test_mismatch_does_not_mutate_state_and_returns_vec() {
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

        let warnings: Vec<CrossShardJoinWarning> = validate_shard_keys(&streams, &new_stream);
        assert_eq!(warnings.len(), 1, "expected one mismatch warning");
        assert_eq!(
            streams.len(),
            initial_count,
            "validate_shard_keys must not mutate the streams map"
        );
    }
}
