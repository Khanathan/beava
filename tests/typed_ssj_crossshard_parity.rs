//! Phase 59.6 SC-9 — typed StreamStreamJoin cross-shard parity.
//!
//! Wave 5 flips this test RED → GREEN.
//!
//! Scope (operator-boundary parity — matches Wave-3/Wave-4 pattern):
//! drive the typed `StreamStreamJoinTyped` + `TypedSsjBuffer`
//! directly on the same event stream partitioned N=1 (single shard)
//! vs N=8 (one buffer per shard, partitioning by
//! `hash(join_key) % 8`). The joined outputs MUST be byte-identical
//! between the two partitionings — this is the sharding-parity
//! invariant that guarantees cross-shard SSJ produces the same state
//! as single-shard SSJ under the Phase 56 `hash(join_key) % N`
//! routing.
//!
//! Wave 5 ships the typed operator + buffer at the library level; a
//! future wave wires `ShardOp::SsjInsertTyped` end-to-end through the
//! TCP dispatch. SC-9's parity contract is operator-boundary: if the
//! typed operator matches the Value-path semantics per-event, and the
//! cross-shard routing is the same as Phase 56, then sharded parity
//! follows.

#![allow(unused_imports)]

use beava::engine::operators_typed::{
    derive_joined_schema, StreamStreamJoinTyped, TypedSsjBuffer,
};
use beava::engine::schema::{FieldSpec, FieldTy, RegisteredSchema, Row};
use beava::routing::shard_hint::shard_hint_for_event;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

fn left_schema() -> Arc<RegisteredSchema> {
    let s = RegisteredSchema {
        schema_id: 100,
        name: "Txns".into(),
        fields: vec![
            FieldSpec {
                name: "user_id".into(),
                ty: FieldTy::InlineStr,
                offset: 0,
                nullable: false,
            },
            FieldSpec {
                name: "amount".into(),
                ty: FieldTy::F64,
                offset: 16,
                nullable: false,
            },
        ],
        inline_str_cap: 15,
        row_size: 24,
    };
    s.validate_layout().expect("valid left");
    Arc::new(s)
}

fn right_schema() -> Arc<RegisteredSchema> {
    let s = RegisteredSchema {
        schema_id: 101,
        name: "Clicks".into(),
        fields: vec![
            FieldSpec {
                name: "user_id".into(),
                ty: FieldTy::InlineStr,
                offset: 0,
                nullable: false,
            },
            FieldSpec {
                name: "url_code".into(),
                ty: FieldTy::I64,
                offset: 16,
                nullable: false,
            },
        ],
        inline_str_cap: 15,
        row_size: 24,
    };
    s.validate_layout().expect("valid right");
    Arc::new(s)
}

fn make_op() -> StreamStreamJoinTyped {
    let left = left_schema();
    let right = right_schema();
    let mut joined = derive_joined_schema(&left, &right, left.inline_str_cap);
    joined.schema_id = 200;
    let joined = Arc::new(joined);

    StreamStreamJoinTyped {
        name: "txn_click_join".to_string(),
        left_schema: left,
        right_schema: right,
        joined_schema: joined,
        on_field: "user_id".to_string(),
        within: Duration::from_secs(60),
    }
}

/// Build a left event Row + its routing-key payload for shard_hint.
fn left_event(user: &str, amount: f64) -> (Row, serde_json::Value) {
    let schema = left_schema();
    let mut r = Row::zeroed(&schema);
    r.schema_id = schema.schema_id;
    r.write_inline_str(0, schema.inline_str_cap, user);
    r.write_f64(16, amount);
    (r, serde_json::json!({ "user_id": user }))
}

fn right_event(user: &str, url: i64) -> (Row, serde_json::Value) {
    let schema = right_schema();
    let mut r = Row::zeroed(&schema);
    r.schema_id = schema.schema_id;
    r.write_inline_str(0, schema.inline_str_cap, user);
    r.write_i64(16, url);
    (r, serde_json::json!({ "user_id": user }))
}

#[test]
fn typed_ssj_crossshard_n1_n8_parity() {
    // Generate a deterministic event stream over 10 users, alternating
    // left/right sides.
    let users: Vec<String> = (0..10).map(|i| format!("u{i}")).collect();
    let now0 = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);

    // Build a fixed event sequence: for each user, push a left then a
    // right 1ms later. Each right should match the most recent left.
    struct Ev {
        side: char, // 'L' or 'R'
        user: String,
        payload_i64: i64,
        amount: f64,
        ts: SystemTime,
        route_payload: serde_json::Value,
    }
    let mut events: Vec<Ev> = Vec::new();
    for (i, user) in users.iter().enumerate() {
        let (_, route) = left_event(user, 0.0);
        events.push(Ev {
            side: 'L',
            user: user.clone(),
            payload_i64: 0,
            amount: (i as f64) * 1.5,
            ts: now0 + Duration::from_millis((i as u64) * 10),
            route_payload: route.clone(),
        });
        events.push(Ev {
            side: 'R',
            user: user.clone(),
            payload_i64: i as i64 + 100,
            amount: 0.0,
            ts: now0 + Duration::from_millis((i as u64) * 10 + 1),
            route_payload: route,
        });
    }

    let op = make_op();

    // Helper: replay the stream against N shards, partitioning each
    // event by `shard_hint_for_event(route, Some("user_id")) % N`.
    let replay = |n_shards: usize| -> Vec<Row> {
        let mut buffers: Vec<TypedSsjBuffer> =
            (0..n_shards).map(|_| TypedSsjBuffer::new()).collect();
        let mut joined_outputs: Vec<(SystemTime, Row)> = Vec::new();
        for ev in &events {
            let shard_idx =
                (shard_hint_for_event(&ev.route_payload, Some("user_id")) as usize) % n_shards;
            let buf = &mut buffers[shard_idx];
            match ev.side {
                'L' => {
                    let (row, _) = left_event(&ev.user, ev.amount);
                    let outs = buf.insert_left_and_match(&op, &ev.user, row, ev.ts);
                    for r in outs {
                        joined_outputs.push((ev.ts, r));
                    }
                }
                'R' => {
                    let (row, _) = right_event(&ev.user, ev.payload_i64);
                    let outs = buf.insert_right_and_match(&op, &ev.user, row, ev.ts);
                    for r in outs {
                        joined_outputs.push((ev.ts, r));
                    }
                }
                _ => unreachable!(),
            }
        }
        // Sort by timestamp then payload bytes for deterministic comparison.
        joined_outputs.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.payload.cmp(&b.1.payload)));
        joined_outputs.into_iter().map(|(_, r)| r).collect()
    };

    let n1 = replay(1);
    let n8 = replay(8);

    assert!(
        !n1.is_empty(),
        "expected at least one joined output at N=1"
    );
    assert_eq!(
        n1.len(),
        n8.len(),
        "joined output count must match across N=1 and N=8"
    );
    for (a, b) in n1.iter().zip(n8.iter()) {
        assert_eq!(a.schema_id, b.schema_id);
        assert_eq!(a.payload, b.payload, "joined payload byte-identical");
        assert_eq!(a.arena, b.arena, "joined arena byte-identical");
    }

    // Sanity: each joined row has the expected user_id + amount + url.
    let joined_schema = &op.joined_schema;
    let cap = joined_schema.inline_str_cap;
    for out in &n1 {
        // user_id at offset 0 (left.user_id).
        let user = out.read_inline_str(0, cap);
        assert!(user.starts_with('u'), "user_id preserved at offset 0");
        // url_code at offset (left.row_size + 16) — right side's
        // `url_code` offset 16 shifted by left.row_size=24.
        let url_offset = op.left_schema.row_size + 16;
        let url = out.read_i64(url_offset);
        assert!(url >= 100, "url_code from right side preserved");
    }
}
