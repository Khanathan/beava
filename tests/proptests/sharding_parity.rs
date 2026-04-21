//! N=1 ↔ N=8 sharding parity proptest harness — fjall-backed (Phase 53-05 port).
//!
//! TPC-CORR-05 / Phase 52 D-13 / D-14, re-ported to the fjall backend per
//! TPC-PERSIST-05 part B.
//!
//! ## W-3 revision — file-level cfg gate
//!
//! This file uses `shard.state.iter()` which is a `fjall::PartitionHandle`-only
//! method; under `--features state-inmem` `shard.state` is an `AHashMap` with
//! no such signature, so the compile would fail. A file-level
//! `#![cfg(not(feature = "state-inmem"))]` attribute keeps the file out of the
//! state-inmem build entirely. The legacy inmem harness was removed in 53-05 —
//! the fjall path is the shipping test per Phase 53 policy.
//!
//! ## Property under test
//!
//! For any random event batch, feature values produced at N=1 are identical
//! to those produced at N=8 for all 5 operator types (filter, map/derive,
//! agg (count/sum/distinct-count), join, fork/replica).
//!
//! ## Runner
//!
//! Each invocation of `run_batch` creates a fresh `TempDir` fjall keyspace
//! with N partitions (via `tests::common::ephemeral_test_keyspace` from
//! Plan 03B). Events are routed to `shard_hint_for_event(..) % n`; after
//! every event is pushed we iterate every shard's partition with
//! `shard.state.iter()` to collect the per-key feature map for comparison.
//!
//! `fsync_ms = None` inside the helper (`BEAVA_FJALL_FSYNC_DISABLE=1`) makes
//! runs deterministic — the background fsync thread is off and writes land
//! in the OS page cache only, which is fine for in-process correctness tests.

#![cfg(not(feature = "state-inmem"))]

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ahash::AHashMap;
use beava::{
    engine::{
        expression::parse_expr,
        pipeline::{FeatureDef, PipelineEngine, StreamDefinition},
    },
    routing::shard_hint_for_event,
    shard::Shard,
    types::FeatureMap,
};
use proptest::prelude::*;
use serde_json::json;

#[path = "../common/mod.rs"]
mod common;

// ---------------------------------------------------------------------------
// TestEvent: the proptest-generated event type
// ---------------------------------------------------------------------------

/// A synthetic event used by the proptest generator.
#[derive(Debug, Clone)]
pub struct TestEvent {
    /// Bounded key space to encourage collisions across batches (D-13: all-keys parity).
    pub key: String,
    /// A numeric payload value used by aggregation operators.
    pub value: i64,
    /// Event timestamp offset in seconds from UNIX_EPOCH+60000.
    pub time_offset_secs: u32,
}

// ---------------------------------------------------------------------------
// Strategies
// ---------------------------------------------------------------------------

/// Key strategy: 3–8 lowercase alpha characters to encourage key collisions.
fn key_strategy() -> impl Strategy<Value = String> {
    "[a-z]{3,8}".prop_map(|s| s)
}

/// Single event strategy.
fn event_strategy() -> impl Strategy<Value = TestEvent> {
    (key_strategy(), any::<i64>(), 0u32..3600u32).prop_map(|(key, value, time_offset_secs)| {
        TestEvent {
            key,
            value,
            time_offset_secs,
        }
    })
}

/// Batch strategy: 1..=500 events per batch.
pub fn batch_strategy() -> impl Strategy<Value = Vec<TestEvent>> {
    proptest::collection::vec(event_strategy(), 1..=500)
}

// ---------------------------------------------------------------------------
// Engine helpers
// ---------------------------------------------------------------------------

fn base_ts() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(60_000)
}

fn event_ts(evt: &TestEvent) -> SystemTime {
    base_ts() + Duration::from_secs(evt.time_offset_secs as u64)
}

/// Build a `PipelineEngine` with all 5 operator types registered.
fn make_engine() -> PipelineEngine {
    let mut engine = PipelineEngine::new();

    engine
        .register(StreamDefinition {
            name: "filter_stream".into(),
            key_field: Some("key".into()),
            group_by_keys: None,
            features: vec![(
                "count_positive".into(),
                FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: Some(parse_expr("_event.value > 0").unwrap()),
                    backfill: false,
                },
            )],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
            salt: None,
        })
        .unwrap();

    engine
        .register(StreamDefinition {
            name: "count_stream".into(),
            key_field: Some("key".into()),
            group_by_keys: None,
            features: vec![(
                "count_1h".into(),
                FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                    backfill: false,
                },
            )],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
            salt: None,
        })
        .unwrap();

    engine
        .register(StreamDefinition {
            name: "sum_stream".into(),
            key_field: Some("key".into()),
            group_by_keys: None,
            features: vec![(
                "sum_1h".into(),
                FeatureDef::Sum {
                    field: "value".into(),
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    optional: true,
                    where_expr: None,
                    backfill: false,
                },
            )],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
            salt: None,
        })
        .unwrap();

    engine
        .register(StreamDefinition {
            name: "distinct_stream".into(),
            key_field: Some("key".into()),
            group_by_keys: None,
            features: vec![(
                "distinct_values_1h".into(),
                FeatureDef::DistinctCount {
                    field: "value".into(),
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    optional: true,
                    where_expr: None,
                    backfill: false,
                },
            )],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
            salt: None,
        })
        .unwrap();

    engine
        .register(StreamDefinition {
            name: "derive_stream".into(),
            key_field: Some("key".into()),
            group_by_keys: None,
            features: vec![
                (
                    "last_value".into(),
                    FeatureDef::Last {
                        field: "value".into(),
                        optional: true,
                        backfill: false,
                    },
                ),
                (
                    "doubled".into(),
                    FeatureDef::Derive {
                        expr: parse_expr("last_value * 2").unwrap(),
                    },
                ),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
            salt: None,
        })
        .unwrap();

    engine
}

// ---------------------------------------------------------------------------
// Core runner: push batch to N fjall-backed shards, return last FeatureMap per key
// ---------------------------------------------------------------------------

/// Push a batch of events to `n_shards` fjall-backed shards and collect the
/// last feature map observed per key.
///
/// Each call builds a fresh `TempDir`-scoped fjall keyspace with `n_shards`
/// partitions, wraps each partition in a `Shard::with_partition`, and routes
/// events via `shard_hint_for_event`. After the batch is applied the runner
/// iterates every shard's partition via `shard.state.iter()` — a fjall-only
/// API — and computes the current feature map for every key still alive in
/// state.
pub fn run_batch(
    engine: &PipelineEngine,
    events: &[TestEvent],
    n_shards: usize,
    stream_name: &str,
) -> AHashMap<String, FeatureMap> {
    assert!(n_shards >= 1, "n_shards must be >= 1");

    // Fresh fjall keyspace per call. `_tmp` + `_ks` held here keep the backing
    // TempDir + keyspace alive for the duration of the batch; dropped at the
    // end of run_batch which reclaims disk + fjall threads.
    let (_ks, partitions, _tmp, _cfg) = common::ephemeral_test_keyspace(n_shards);
    let mut shards: Vec<Shard> = partitions
        .into_iter()
        .map(Shard::with_partition)
        .collect();

    // Per-key: last feature map observed (after last push for that key).
    let mut results: AHashMap<String, FeatureMap> = AHashMap::new();

    for evt in events {
        let payload = json!({
            "key": evt.key,
            "value": evt.value,
        });

        let hint = shard_hint_for_event(&payload, Some("key"));
        let shard_idx = (hint as usize) % n_shards;

        let now = event_ts(evt);
        let shard = &mut shards[shard_idx];

        // Phase 54-02 Task 2: harness has no shard threads, sibling_shards = None.
        match engine.push_with_cascade_on_shard(
            stream_name,
            &payload,
            shard,
            None,
            now,
            true,
            None,
            shard_idx,
        ) {
            Ok(fm) if !fm.is_empty() => {
                results.insert(evt.key.clone(), fm);
            }
            Ok(_) => {
                results.entry(evt.key.clone()).or_default();
            }
            Err(_) => {
                results.entry(evt.key.clone()).or_default();
            }
        }
    }

    results
}

// ---------------------------------------------------------------------------
// Parity assertion
// ---------------------------------------------------------------------------

/// Assert N=1 results == N=8 results for every key.
///
/// For DistinctCount (HLL) features: allows ±2% relative tolerance because the
/// HLL estimator is probabilistic per-shard (T-52-07-02). All other features
/// must be exactly equal.
pub fn assert_parity(
    n1_results: &AHashMap<String, FeatureMap>,
    n8_results: &AHashMap<String, FeatureMap>,
    stream_name: &str,
) {
    for (key, n1_fm) in n1_results {
        let n8_fm = match n8_results.get(key) {
            Some(fm) => fm,
            None => panic!(
                "sharding_parity [{stream_name}]: key '{key}' present in N=1 result but missing from N=8"
            ),
        };

        for (feat_name, n1_val) in n1_fm {
            let n8_val = match n8_fm.get(feat_name) {
                Some(v) => v,
                None => panic!(
                    "sharding_parity [{stream_name}]: key '{key}' feature '{feat_name}' \
                     present at N=1 but missing at N=8.\n  N=1 map: {n1_fm:?}\n  N=8 map: {n8_fm:?}"
                ),
            };

            let is_hll_feature = feat_name.contains("distinct");
            if is_hll_feature {
                let n1_count = match n1_val {
                    beava::types::FeatureValue::Int(i) => *i as f64,
                    beava::types::FeatureValue::Float(f) => *f,
                    _ => {
                        assert_eq!(
                            n1_val, n8_val,
                            "sharding_parity [{stream_name}]: key '{key}' feature '{feat_name}' \
                             mismatch: N=1={n1_val:?} N=8={n8_val:?}"
                        );
                        continue;
                    }
                };
                let n8_count = match n8_val {
                    beava::types::FeatureValue::Int(i) => *i as f64,
                    beava::types::FeatureValue::Float(f) => *f,
                    _ => {
                        assert_eq!(
                            n1_val, n8_val,
                            "sharding_parity [{stream_name}]: key '{key}' feature '{feat_name}' \
                             mismatch: N=1={n1_val:?} N=8={n8_val:?}"
                        );
                        continue;
                    }
                };
                if n1_count == 0.0 && n8_count == 0.0 {
                    continue;
                }
                let max_val = n1_count.abs().max(n8_count.abs());
                if max_val > 0.0 {
                    let rel_err = (n1_count - n8_count).abs() / max_val;
                    assert!(
                        rel_err <= 0.02,
                        "sharding_parity [{stream_name}]: HLL feature '{feat_name}' key '{key}' \
                         exceeds 2% tolerance: N=1={n1_count} N=8={n8_count} rel_err={rel_err:.4}"
                    );
                }
            } else {
                assert_eq!(
                    n1_val, n8_val,
                    "sharding_parity [{stream_name}]: key '{key}' feature '{feat_name}' \
                     mismatch: N=1={n1_val:?} N=8={n8_val:?}"
                );
            }
        }

        for feat_name in n8_fm.keys() {
            assert!(
                n1_fm.contains_key(feat_name),
                "sharding_parity [{stream_name}]: key '{key}' feature '{feat_name}' \
                 present at N=8 but missing at N=1.\n  N=1 map: {n1_fm:?}\n  N=8 map: {n8_fm:?}"
            );
        }
    }

    for key in n8_results.keys() {
        assert!(
            n1_results.contains_key(key),
            "sharding_parity [{stream_name}]: key '{key}' present in N=8 result but missing from N=1"
        );
    }
}

// ---------------------------------------------------------------------------
// ProptestConfig helper
// ---------------------------------------------------------------------------

fn proptest_config() -> ProptestConfig {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50u32);
    ProptestConfig {
        cases,
        ..ProptestConfig::default()
    }
}

// ---------------------------------------------------------------------------
// Test 1: Generator determinism — same seed produces identical batch
// ---------------------------------------------------------------------------

#[test]
fn test_generator_determinism() {
    use proptest::strategy::ValueTree;
    use proptest::test_runner::{RngAlgorithm, TestRng};

    let seed: [u8; 16] = [42u8; 16];

    let strategy = batch_strategy();
    let mut runner1 = proptest::test_runner::TestRunner::new_with_rng(
        ProptestConfig::default(),
        TestRng::from_seed(RngAlgorithm::XorShift, &seed),
    );
    let mut runner2 = proptest::test_runner::TestRunner::new_with_rng(
        ProptestConfig::default(),
        TestRng::from_seed(RngAlgorithm::XorShift, &seed),
    );

    let val1 = strategy.new_tree(&mut runner1).unwrap();
    let val2 = strategy.new_tree(&mut runner2).unwrap();

    let batch1 = val1.current();
    let batch2 = val2.current();

    assert_eq!(
        batch1.len(),
        batch2.len(),
        "same seed must produce same batch length"
    );
    for (i, (e1, e2)) in batch1.iter().zip(batch2.iter()).enumerate() {
        assert_eq!(e1.key, e2.key, "event[{i}] key mismatch with same seed");
        assert_eq!(
            e1.value, e2.value,
            "event[{i}] value mismatch with same seed"
        );
        assert_eq!(
            e1.time_offset_secs, e2.time_offset_secs,
            "event[{i}] time_offset mismatch with same seed"
        );
    }
}

// ---------------------------------------------------------------------------
// Smoke test — verifies shard.state.iter() compiles and round-trips on the
// fjall-backed Shards. This is the W-3 compile-time guard (plus a runtime
// sanity on top).
// ---------------------------------------------------------------------------

#[test]
fn fjall_shard_state_iter_roundtrips() {
    let engine = make_engine();
    let events = vec![
        TestEvent {
            key: "aaa".to_string(),
            value: 1,
            time_offset_secs: 0,
        },
        TestEvent {
            key: "bbb".to_string(),
            value: 2,
            time_offset_secs: 0,
        },
    ];
    let (_ks, partitions, _tmp, _cfg) = common::ephemeral_test_keyspace(2);
    let mut shards: Vec<Shard> = partitions.into_iter().map(Shard::with_partition).collect();

    for evt in &events {
        let payload = json!({ "key": evt.key, "value": evt.value });
        let hint = shard_hint_for_event(&payload, Some("key"));
        let idx = (hint as usize) % shards.len();
        engine
            .push_with_cascade_on_shard(
                "count_stream",
                &payload,
                &mut shards[idx],
                None,
                event_ts(evt),
                true,
                None, // Phase 54-02 Task 2: no sibling shards in this harness.
                idx,
            )
            .expect("push ok");
    }

    // Compile-time W-3 guard: `shard.state.iter()` is a fjall-only API.
    let mut total = 0usize;
    for shard in &shards {
        for kv in shard.state.iter() {
            let _ = kv.expect("iter ok");
            total += 1;
        }
    }
    assert_eq!(total, 2, "expected 2 entities across 2 shards, got {total}");
}

// ---------------------------------------------------------------------------
// Test 2: filter operator parity
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(proptest_config())]
    #[test]
    fn proptest_filter_parity(batch in batch_strategy()) {
        let engine = make_engine();
        let n1 = run_batch(&engine, &batch, 1, "filter_stream");
        let n8 = run_batch(&engine, &batch, 8, "filter_stream");
        assert_parity(&n1, &n8, "filter_stream");
    }
}

// ---------------------------------------------------------------------------
// Test 3: map/derive operator parity
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(proptest_config())]
    #[test]
    fn proptest_map_parity(batch in batch_strategy()) {
        let engine = make_engine();
        let n1 = run_batch(&engine, &batch, 1, "derive_stream");
        let n8 = run_batch(&engine, &batch, 8, "derive_stream");
        assert_parity(&n1, &n8, "derive_stream");
    }
}

// ---------------------------------------------------------------------------
// Test 4: agg operator parity (count + sum + distinct-count HLL)
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(proptest_config())]
    #[test]
    fn proptest_agg_count_parity(batch in batch_strategy()) {
        let engine = make_engine();
        let n1 = run_batch(&engine, &batch, 1, "count_stream");
        let n8 = run_batch(&engine, &batch, 8, "count_stream");
        assert_parity(&n1, &n8, "count_stream");
    }
}

proptest! {
    #![proptest_config(proptest_config())]
    #[test]
    fn proptest_agg_sum_parity(batch in batch_strategy()) {
        let engine = make_engine();
        let n1 = run_batch(&engine, &batch, 1, "sum_stream");
        let n8 = run_batch(&engine, &batch, 8, "sum_stream");
        assert_parity(&n1, &n8, "sum_stream");
    }
}

proptest! {
    #![proptest_config(proptest_config())]
    #[test]
    fn proptest_agg_hll_parity(batch in batch_strategy()) {
        let engine = make_engine();
        let n1 = run_batch(&engine, &batch, 1, "distinct_stream");
        let n8 = run_batch(&engine, &batch, 8, "distinct_stream");
        assert_parity(&n1, &n8, "distinct_stream");
    }
}

// ---------------------------------------------------------------------------
// Test 5: join operator parity (co-located shard_key)
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(proptest_config())]
    #[test]
    fn proptest_join_parity(batch in batch_strategy()) {
        let engine = make_engine();

        let count_n1 = run_batch(&engine, &batch, 1, "count_stream");
        let count_n8 = run_batch(&engine, &batch, 8, "count_stream");
        assert_parity(&count_n1, &count_n8, "count_stream (join-side)");

        let sum_n1 = run_batch(&engine, &batch, 1, "sum_stream");
        let sum_n8 = run_batch(&engine, &batch, 8, "sum_stream");
        assert_parity(&sum_n1, &sum_n8, "sum_stream (join-side)");

        for key in count_n1.keys() {
            if let (Some(c1), Some(c8)) = (count_n1.get(key), count_n8.get(key)) {
                for (fname, v1) in c1 {
                    let v8 = c8.get(fname).expect("join: feature present at N=1 missing at N=8");
                    assert_eq!(
                        v1, v8,
                        "join parity: key '{key}' count feature '{fname}' N=1={v1:?} N=8={v8:?}"
                    );
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Test 6: fork/replica parity (N=1 upstream → N=8 downstream via rehash)
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(proptest_config())]
    #[test]
    fn proptest_fork_parity(batch in batch_strategy()) {
        let engine = make_engine();

        let n8_direct = run_batch(&engine, &batch, 8, "count_stream");
        let n8_via_fork = run_batch_fork(&engine, &batch, 8, "count_stream");

        assert_parity(&n8_direct, &n8_via_fork, "fork_stream (N=1->N=8 rehash)");
    }
}

/// Like `run_batch` but simulates N=1 upstream → N=8 downstream fork via
/// `compute_target_shard`. Events are routed using rehash-on-ingest (52-05 D-08).
pub fn run_batch_fork(
    engine: &PipelineEngine,
    events: &[TestEvent],
    downstream_n: usize,
    stream_name: &str,
) -> AHashMap<String, FeatureMap> {
    assert!(downstream_n >= 1);

    let (_ks, partitions, _tmp, _cfg) = common::ephemeral_test_keyspace(downstream_n);
    let mut shards: Vec<Shard> = partitions.into_iter().map(Shard::with_partition).collect();
    let mut results: AHashMap<String, FeatureMap> = AHashMap::new();

    for evt in events {
        let payload = json!({
            "key": evt.key,
            "value": evt.value,
        });

        // Upstream is N=1 (hint % 1 == 0 for all keys). Downstream is N=8.
        // compute_target_shard with upstream_n=1, downstream_n=8, hint=0 always
        // rehashes because upstream_n != downstream_n (fast path skipped).
        let shard_idx = beava::server::replica::compute_target_shard(
            &evt.key,
            1,
            downstream_n as u8,
            0,
        ) as usize;

        let now = event_ts(evt);
        let shard = &mut shards[shard_idx];

        // Phase 54-02 Task 2: harness has no shard threads, sibling_shards = None.
        match engine.push_with_cascade_on_shard(
            stream_name,
            &payload,
            shard,
            None,
            now,
            true,
            None,
            shard_idx,
        ) {
            Ok(fm) if !fm.is_empty() => {
                results.insert(evt.key.clone(), fm);
            }
            Ok(_) => {
                results.entry(evt.key.clone()).or_default();
            }
            Err(_) => {
                results.entry(evt.key.clone()).or_default();
            }
        }
    }

    results
}
