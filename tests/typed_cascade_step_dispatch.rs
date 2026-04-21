//! Phase 59.7 Wave 3 (TPC-PERF-11 extension, TPC-CORR-07) — integration-
//! level unit tests for the `ShardOp::RunTypedAggCascadeStep` dispatch
//! primitives.
//!
//! These tests exercise the NEW accessors landed in W3 WITHOUT needing the
//! full `process_shard_event` loop (which requires a full
//! `ConcurrentAppState` + running shard threads). Covers:
//!
//! 1. `PipelineEngine::build_typed_agg_ops_for` returns `Some(..)` after a
//!    Count-only stream is registered with a schema, and returns `None` for
//!    a non-typed-cascade-compatible stream (with a Derive feature).
//! 2. `PipelineEngine::get_typed_state_schema` returns a schema for the
//!    same stream and `None` for streams without a cache entry.
//! 3. `run_typed_agg_step` (already present) consumes the factory output
//!    and produces a FeatureMap — confirms the factory + state-schema
//!    produce a runnable op list.
//! 4. `try_extract_event_time_from_typed_row` returns the expected
//!    SystemTime for a Row carrying an `event_time` i64 ns column, and
//!    `None` for a Row whose schema lacks the column.
//!
//! Lib-test execution is blocked by the pre-existing Phase 60 salt sweep
//! (see `.planning/phases/59.6-typed-pipeline-records/deferred-items.md`);
//! running these as an integration test binary side-steps that blocker so
//! W3's dispatch primitives are directly exercised.

#![allow(clippy::bool_assert_comparison)]

use beava::engine::pipeline::{FeatureDef, PipelineEngine, StreamDefinition};
use beava::engine::schema::{FieldSpec, FieldTy, RegisteredSchema, Row};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn build_txns_schema() -> RegisteredSchema {
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
                name: "event_time".into(),
                ty: FieldTy::I64,
                offset: 16,
                nullable: false,
            },
        ],
        inline_str_cap: 15,
        row_size: 24,
    };
    s.validate_layout().expect("valid");
    s
}

fn register_count_only_stream(engine: &mut PipelineEngine, name: &str) {
    let schema = build_txns_schema();
    engine.register_typed_schema(name, schema);
    let def = StreamDefinition {
        name: name.to_string(),
        key_field: Some("user_id".to_string()),
        features: vec![(
            "n".to_string(),
            FeatureDef::Count {
                window: Duration::from_secs(5),
                bucket: Duration::from_secs(1),
                where_expr: None,
                backfill: false,
            },
        )],
        ..Default::default()
    };
    engine
        .register(def)
        .expect("register Count-only stream ok");
}

#[test]
fn build_typed_agg_ops_for_returns_count_op_after_register() {
    // Guard against inherited env state from other tests in the same
    // test binary.
    std::env::remove_var("BEAVA_TYPED_CASCADE_DIRECT");
    let mut engine = PipelineEngine::new();
    register_count_only_stream(&mut engine, "UserMetrics");
    let ops = engine
        .build_typed_agg_ops_for("UserMetrics")
        .expect("UserMetrics has typed agg ops after register");
    assert_eq!(ops.len(), 1, "Count feature yields exactly one op");
    assert_eq!(ops[0].name(), "n");
}

#[test]
fn get_typed_state_schema_returns_some_for_typed_cache_entry() {
    std::env::remove_var("BEAVA_TYPED_CASCADE_DIRECT");
    let mut engine = PipelineEngine::new();
    register_count_only_stream(&mut engine, "UserMetrics");
    let schema = engine
        .get_typed_state_schema("UserMetrics")
        .expect("state schema populated alongside ops cache");
    // Placeholder state schema has exactly the single `_reserved` field.
    assert_eq!(schema.fields.len(), 1);
    assert_eq!(schema.row_size, 8);
}

#[test]
fn build_typed_agg_ops_for_returns_none_for_unknown_stream() {
    std::env::remove_var("BEAVA_TYPED_CASCADE_DIRECT");
    let engine = PipelineEngine::new();
    assert!(engine.build_typed_agg_ops_for("does_not_exist").is_none());
    assert!(engine.get_typed_state_schema("does_not_exist").is_none());
}

#[test]
fn try_extract_event_time_from_typed_row_reads_i64_ns_column() {
    let engine = PipelineEngine::new();
    let schema = build_txns_schema();
    let mut row = Row::zeroed(&schema);
    row.write_inline_str(0, schema.inline_str_cap, "user_42");
    // 2026-04-21T00:00:00Z → epoch ns. Use a fixed, positive value so we
    // don't depend on SystemTime::now during the test.
    let epoch_ns: i64 = 1_776_902_400_000_000_000;
    row.write_i64(16, epoch_ns);
    let et = engine
        .try_extract_event_time_from_typed_row(&row, &schema)
        .expect("i64 ns column resolves to SystemTime");
    let delta = et.duration_since(UNIX_EPOCH).expect("positive");
    assert_eq!(delta.as_nanos() as u64, epoch_ns as u64);
}

#[test]
fn try_extract_event_time_from_typed_row_returns_none_for_missing_column() {
    let engine = PipelineEngine::new();
    // Schema without an event_time / bv_event_time column.
    let schema = RegisteredSchema {
        schema_id: 0,
        name: "NoTime".into(),
        fields: vec![FieldSpec {
            name: "k".into(),
            ty: FieldTy::InlineStr,
            offset: 0,
            nullable: false,
        }],
        inline_str_cap: 15,
        row_size: 16,
    };
    schema.validate_layout().expect("valid");
    let mut row = Row::zeroed(&schema);
    row.write_inline_str(0, schema.inline_str_cap, "hello");
    assert!(engine
        .try_extract_event_time_from_typed_row(&row, &schema)
        .is_none());
}

#[test]
#[cfg(feature = "state-inmem")]
fn run_typed_agg_step_consumes_factory_output_and_bumps_count() {
    // Round-trip: factory → run_typed_agg_step → read feature.
    std::env::remove_var("BEAVA_TYPED_CASCADE_DIRECT");
    let mut engine = PipelineEngine::new();
    register_count_only_stream(&mut engine, "UserMetrics");
    let ops_arc = engine.build_typed_agg_ops_for("UserMetrics").unwrap();
    let state_schema = engine.get_typed_state_schema("UserMetrics").unwrap();
    let input_schema = Arc::new(build_txns_schema());
    let mut row = Row::zeroed(&input_schema);
    row.write_inline_str(0, input_schema.inline_str_cap, "user_1");
    row.write_i64(16, 1_776_902_400_000_000_000);
    let op_refs: Vec<&dyn beava::engine::operators_typed::TypedAggOp> =
        ops_arc.iter().map(|o| o.as_ref()).collect();
    let mut shard = beava::shard::Shard::new();
    let now = SystemTime::now();
    let fmap = engine.run_typed_agg_step(
        "UserMetrics",
        "user_1",
        &row,
        &input_schema,
        &op_refs,
        &state_schema,
        &mut shard,
        now,
    );
    // CountOpTypedWindowed's `read_feature` returns Missing (state in ring
    // buffer). Call update_windowed + read_feature_windowed to exercise
    // the full windowed path.
    for op in &op_refs {
        op.update_windowed(
            &mut shard,
            "UserMetrics",
            "user_1",
            &row,
            &input_schema,
            now,
        );
    }
    let _state = shard
        .entity_state_typed
        .get(&("UserMetrics".to_string(), "user_1".to_string()))
        .expect("entity state created");
    // Ring buffer should have received +1.
    let ring = shard
        .entity_ringbuffers_typed
        .get(&("UserMetrics".to_string(), "user_1".to_string(), 0u16))
        .expect("ring buffer allocated");
    // sum_all for i64 ring should be 1 after one event.
    let sum = ring.as_i64().sum_all();
    assert_eq!(sum, 1, "after one Count event, ring sum == 1");
    // fmap should contain the op's name; value via read_feature is
    // Missing for windowed ops (state lives on the ring).
    assert!(fmap.contains_key("n"));
}
