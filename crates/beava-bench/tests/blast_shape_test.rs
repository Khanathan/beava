//! Tests for `beava_bench::blast_shape` — the Pool=N pre-encoded frame
//! builder shared by Phase 19+ benches.
//!
//! Phase 19 Plan 19-01 — written RED first per CLAUDE.md §TDD Discipline.
//! Each test names a concrete invariant from the plan's `<behavior>` block:
//!
//! 1.  pool_size_matches_n
//! 2.  fixed_shape_produces_identical_frames
//! 3.  uniform_shape_distributes_keys_evenly         (proptest)
//! 4.  zipfian_shape_long_tail                       (proptest)
//! 5.  mixed_shape_rotates_through_events
//! 6.  frames_decode_to_valid_envelopes_json
//! 7.  frames_decode_to_valid_envelopes_msgpack
//! 8.  zipfian_sampler_deterministic
//! 9.  pool_setup_time_measurable
//! 10. mixed_shape_requires_multi_event_pipeline_or_falls_back
//!
//! The body bytes inside each frame are decoded back through serde_json /
//! rmp_serde to confirm `event` + `body` are present — this is the contract
//! the server-side parsers expect.

use beava_bench::blast_shape::{
    build_pool, build_pool_timed, BlastShape, BlastShapeConfig, BlastShapeError, PipelineConfig,
    WireFormat, ZipfianSampler,
};
use beava_core::wire::{decode_frame, CT_JSON, CT_MSGPACK, OP_PUSH};
use bytes::BytesMut;
use serde_json::{json, Value};

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Default pipeline config used in tests, matching `crates/beava-bench/configs/small.json`
/// shape (single event "Txn", key field "user_id", one extra field "amount" of type f64).
fn small_pipeline() -> PipelineConfig {
    let register = json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Txn",
                "schema": {
                    "fields": {"event_time": "i64", "user_id": "str", "amount": "f64"},
                    "optional_fields": []
                },
                "event_time_field": "event_time"
            }
        ]
    });
    let mut extra = serde_json::Map::new();
    extra.insert("amount".to_string(), Value::String("f64".to_string()));
    PipelineConfig {
        name: "small".into(),
        description: "test fixture".into(),
        register,
        event_name: "Txn".into(),
        features: vec!["cnt".into()],
        key_field: "user_id".into(),
        extra_fields: extra,
    }
}

/// Decode every frame in `pool` back into a Vec<(content_type, payload)>.
/// Each frame's full TCP-wire bytes (length-prefix + op + content_type +
/// payload) are present, so `decode_frame` is the right tool — same path the
/// server uses. Returning an owned vector (rather than taking a closure) lets
/// callers use `prop_assert!` in proptest! contexts where closures cannot
/// return `Result<_, TestCaseError>`.
fn decode_pool_payloads(pool: &[bytes::Bytes]) -> Vec<(u8, Vec<u8>)> {
    let mut out = Vec::with_capacity(pool.len());
    for raw in pool {
        let mut buf = BytesMut::from(&raw[..]);
        let frame = decode_frame(&mut buf, 8 * 1024 * 1024)
            .expect("decode_frame must accept builder output")
            .expect("decode_frame must produce one full frame from each pool entry");
        assert_eq!(frame.op, OP_PUSH, "all blast frames are OP_PUSH");
        out.push((frame.content_type, frame.payload.to_vec()));
    }
    out
}

fn extract_user_id_from_json_payload(bytes: &[u8]) -> u64 {
    let v: Value = serde_json::from_slice(bytes).expect("payload is valid JSON");
    let body = v.get("body").expect("payload has body");
    let user_id = body
        .get("user_id")
        .and_then(|x| x.as_str())
        .expect("body has user_id string");
    // user_id is "k%08u"
    user_id
        .strip_prefix('k')
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or_else(|| panic!("malformed user_id: {user_id}"))
}

fn extract_event_name_from_json_payload(bytes: &[u8]) -> String {
    let v: Value = serde_json::from_slice(bytes).expect("payload is valid JSON");
    v.get("event")
        .and_then(|x| x.as_str())
        .expect("payload has event string")
        .to_string()
}

// ─── Test 1: pool_size_matches_n ──────────────────────────────────────────────

#[test]
fn pool_size_matches_n() {
    let pipeline = small_pipeline();
    let cfg = BlastShapeConfig {
        pipeline: &pipeline,
        event_names_for_mixed: &[],
        wire_format: WireFormat::Json,
        seed: 42,
    };
    let pool = build_pool(BlastShape::Fixed, &cfg, 1_000).expect("build_pool fixed n=1000");
    assert_eq!(pool.len(), 1_000, "pool length must equal requested n");
}

// ─── Test 2: fixed_shape_produces_identical_frames ────────────────────────────

#[test]
fn fixed_shape_produces_identical_frames() {
    let pipeline = small_pipeline();
    let cfg = BlastShapeConfig {
        pipeline: &pipeline,
        event_names_for_mixed: &[],
        wire_format: WireFormat::Json,
        seed: 42,
    };
    let pool = build_pool(BlastShape::Fixed, &cfg, 100).expect("build_pool fixed n=100");
    assert_eq!(pool.len(), 100);
    let first = &pool[0];
    for (i, f) in pool.iter().enumerate() {
        assert_eq!(
            f, first,
            "Fixed shape frame {i} must be byte-identical to frame 0"
        );
    }
}

// ─── Test 3: uniform_shape_distributes_keys_evenly (proptest) ─────────────────

use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig { cases: 8, ..ProptestConfig::default() })]

    #[test]
    fn uniform_shape_distributes_keys_evenly(seed in 0u64..1024u64) {
        let pipeline = small_pipeline();
        let cfg = BlastShapeConfig {
            pipeline: &pipeline,
            event_names_for_mixed: &[],
            wire_format: WireFormat::Json,
            seed,
        };
        const K: u64 = 100;
        const N: u64 = 10_000;
        let pool = build_pool(
            BlastShape::Uniform { cardinality: K },
            &cfg,
            N,
        )
        .expect("build_pool uniform");

        let payloads = decode_pool_payloads(&pool);
        let mut counts = vec![0u64; K as usize];
        for (ct, bytes) in &payloads {
            prop_assert_eq!(*ct, CT_JSON);
            let uid = extract_user_id_from_json_payload(bytes);
            prop_assert!(uid < K, "uid {} out of range for K={}", uid, K);
            counts[uid as usize] += 1;
        }

        // Every bucket must be hit (otherwise we are not "uniform over K").
        for (i, c) in counts.iter().enumerate() {
            prop_assert!(*c > 0, "bucket {} got zero hits at seed {}", i, seed);
        }
        // Loose upper bound on the busiest bucket: 2 × n/K.
        let upper = (N / K) * 2;
        let max = counts.iter().copied().max().unwrap();
        prop_assert!(max <= upper, "max bucket {} exceeded loose upper {}", max, upper);
    }
}

// ─── Test 4: zipfian_shape_long_tail (proptest) ───────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig { cases: 4, ..ProptestConfig::default() })]

    #[test]
    fn zipfian_shape_long_tail(seed in 0u64..1024u64) {
        let pipeline = small_pipeline();
        let cfg = BlastShapeConfig {
            pipeline: &pipeline,
            event_names_for_mixed: &[],
            wire_format: WireFormat::Json,
            seed,
        };
        const K: u64 = 1_000;
        const N: u64 = 10_000;
        let pool = build_pool(
            BlastShape::Zipfian { alpha: 1.0, cardinality: K },
            &cfg,
            N,
        )
        .expect("build_pool zipfian");

        let payloads = decode_pool_payloads(&pool);
        let mut counts = vec![0u64; K as usize];
        for (_ct, bytes) in &payloads {
            let uid = extract_user_id_from_json_payload(bytes);
            prop_assert!(uid < K, "uid {} out of range", uid);
            counts[uid as usize] += 1;
        }

        // Top-1 key must take ≥ 5% of N (head dominance).
        let top1 = counts.iter().copied().max().unwrap();
        prop_assert!(
            top1 >= (N / 20),
            "top-1 zipfian key only got {} hits (<5% of {}) at seed {}",
            top1, N, seed
        );

        // Bottom 50% of keys (sorted ascending) must contribute ≤ 30% of N.
        let mut sorted = counts.clone();
        sorted.sort();
        let bottom_half: u64 = sorted[..(K as usize / 2)].iter().sum();
        prop_assert!(
            bottom_half <= (N * 3 / 10),
            "bottom-half zipfian keys took {} (> 30% of {}) at seed {}",
            bottom_half, N, seed
        );
    }
}

// ─── Test 5: mixed_shape_rotates_through_events ───────────────────────────────

#[test]
fn mixed_shape_rotates_through_events() {
    let pipeline = small_pipeline();
    let names: [&str; 3] = ["Login", "Click", "Txn"];
    let cfg = BlastShapeConfig {
        pipeline: &pipeline,
        event_names_for_mixed: &names,
        wire_format: WireFormat::Json,
        seed: 1234,
    };
    const M: usize = 3;
    const N: u64 = 300;
    let pool = build_pool(BlastShape::Mixed { event_count: M }, &cfg, N).expect("build_pool mixed");
    assert_eq!(pool.len(), N as usize);

    let payloads = decode_pool_payloads(&pool);
    let mut counts: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    for (ct, bytes) in &payloads {
        assert_eq!(*ct, CT_JSON);
        let name = extract_event_name_from_json_payload(bytes);
        *counts.entry(name).or_insert(0) += 1;
    }

    // All M event names must appear.
    for n in names.iter() {
        let c = counts.get(*n).copied().unwrap_or(0);
        let floor = ((N as f64 / M as f64) * 0.8) as u64;
        assert!(
            c >= floor,
            "Mixed shape: event '{n}' only appeared {c} times (< {floor} = ⌊n/M × 0.8⌋)"
        );
    }
    assert_eq!(
        counts.len(),
        M,
        "Mixed shape must produce exactly M distinct event names"
    );
}

// ─── Test 6: frames_decode_to_valid_envelopes_json ────────────────────────────

#[test]
fn frames_decode_to_valid_envelopes_json() {
    let pipeline = small_pipeline();
    let cfg = BlastShapeConfig {
        pipeline: &pipeline,
        event_names_for_mixed: &[],
        wire_format: WireFormat::Json,
        seed: 7,
    };
    let pool = build_pool(BlastShape::Uniform { cardinality: 100 }, &cfg, 50).expect("build_pool");
    let payloads = decode_pool_payloads(&pool);
    for (ct, bytes) in &payloads {
        assert_eq!(*ct, CT_JSON, "JSON frames carry CT_JSON");
        let v: Value = serde_json::from_slice(bytes).expect("payload must parse as JSON envelope");
        assert!(v.get("event").is_some(), "envelope missing `event`");
        assert!(v.get("body").is_some(), "envelope missing `body`");
    }
}

// ─── Test 7: frames_decode_to_valid_envelopes_msgpack ─────────────────────────

#[test]
fn frames_decode_to_valid_envelopes_msgpack() {
    let pipeline = small_pipeline();
    let cfg = BlastShapeConfig {
        pipeline: &pipeline,
        event_names_for_mixed: &[],
        wire_format: WireFormat::Msgpack,
        seed: 7,
    };
    let pool = build_pool(BlastShape::Uniform { cardinality: 100 }, &cfg, 50).expect("build_pool");
    let payloads = decode_pool_payloads(&pool);
    for (ct, bytes) in &payloads {
        assert_eq!(*ct, CT_MSGPACK, "Msgpack frames carry CT_MSGPACK");
        let v: Value =
            rmp_serde::from_slice(bytes).expect("payload must parse as msgpack envelope");
        assert!(v.get("event").is_some(), "envelope missing `event`");
        assert!(v.get("body").is_some(), "envelope missing `body`");
    }
}

// ─── Test 8: zipfian_sampler_deterministic ────────────────────────────────────

#[test]
fn zipfian_sampler_deterministic() {
    let mut a = ZipfianSampler::new(1.0, 1_000, 42);
    let mut b = ZipfianSampler::new(1.0, 1_000, 42);
    let seq_a: Vec<u64> = (0..100).map(|_| a.sample()).collect();
    let seq_b: Vec<u64> = (0..100).map(|_| b.sample()).collect();
    assert_eq!(
        seq_a, seq_b,
        "same-seed ZipfianSampler must produce same seq"
    );
    // Sanity: every rank stays inside [0, k).
    for r in &seq_a {
        assert!(*r < 1_000, "rank {} out of range", r);
    }
}

// ─── Test 9: pool_setup_time_measurable ───────────────────────────────────────

#[test]
fn pool_setup_time_measurable() {
    let pipeline = small_pipeline();
    let cfg = BlastShapeConfig {
        pipeline: &pipeline,
        event_names_for_mixed: &[],
        wire_format: WireFormat::Json,
        seed: 99,
    };
    let (pool, dur) = build_pool_timed(BlastShape::Uniform { cardinality: 1_000 }, &cfg, 10_000)
        .expect("build_pool_timed");
    assert_eq!(pool.len(), 10_000, "pool length must match n");
    assert!(
        dur > std::time::Duration::ZERO,
        "setup duration must be a positive Duration"
    );
}

// ─── Test 10: mixed_shape_requires_multi_event_pipeline_or_falls_back ─────────

#[test]
fn mixed_shape_requires_multi_event_pipeline_or_falls_back() {
    let pipeline = small_pipeline();
    let only_one = ["Txn"];
    let cfg = BlastShapeConfig {
        pipeline: &pipeline,
        event_names_for_mixed: &only_one,
        wire_format: WireFormat::Json,
        seed: 1,
    };
    let res = build_pool(BlastShape::Mixed { event_count: 3 }, &cfg, 10);
    assert!(res.is_err(), "Mixed with too few names must error");
    let err = res.err().unwrap();
    matches!(err, BlastShapeError::MixedRequiresMultipleEvents);
}
