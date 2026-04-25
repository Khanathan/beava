//! Plan 18-10 Task 10.4 — parse envelope microbench.
//!
//! Measures the per-event cost of `parse_msgpack_envelope` and
//! `parse_json_envelope` against a representative ~150-byte 6-field push body.
//!
//! Targets (Apple M4, hw-class Darwin-24.3.0):
//! - parse_msgpack_envelope: ≤80 ns/op (warn at 88 / 10%; block at 100 / 25%)
//! - parse_json_envelope:    ≤150 ns/op (warn at 165 / 10%; block at 188 / 25%)
//!
//! Body-to-Row benches are informational (no fixed target) — recorded for
//! future regression checks once Row::Deserialize evolves.

use beava_runtime_core::tcp_listener::{parse_json_envelope, parse_msgpack_envelope};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use serde::Serialize;

/// Representative 6-field event body (typical fraud workload):
///   {amount: 99.95, ts: 1714234567000, account_id: "acc_123",
///    merchant: "M_ACME", country: "US", method: "card"}
fn build_msgpack_envelope() -> Vec<u8> {
    #[derive(Serialize)]
    struct Body<'a> {
        amount: f64,
        ts: i64,
        account_id: &'a str,
        merchant: &'a str,
        country: &'a str,
        method: &'a str,
    }
    #[derive(Serialize)]
    struct Envelope<'a> {
        event: &'a str,
        body: Body<'a>,
    }
    rmp_serde::to_vec_named(&Envelope {
        event: "Txn",
        body: Body {
            amount: 99.95,
            ts: 1_714_234_567_000,
            account_id: "acc_123",
            merchant: "M_ACME",
            country: "US",
            method: "card",
        },
    })
    .expect("serialise representative msgpack envelope")
}

fn build_json_envelope() -> Vec<u8> {
    let s = r#"{"event":"Txn","body":{"amount":99.95,"ts":1714234567000,"account_id":"acc_123","merchant":"M_ACME","country":"US","method":"card"}}"#;
    s.as_bytes().to_vec()
}

fn bench_parse_msgpack_envelope(c: &mut Criterion) {
    let payload = build_msgpack_envelope();
    c.bench_function("parse_msgpack_envelope", |b| {
        b.iter(|| {
            let (e, body) = parse_msgpack_envelope(black_box(&payload)).expect("parse");
            black_box((e, body));
        });
    });
}

fn bench_parse_json_envelope(c: &mut Criterion) {
    let payload = build_json_envelope();
    c.bench_function("parse_json_envelope", |b| {
        b.iter(|| {
            let (e, body) = parse_json_envelope(black_box(&payload)).expect("parse");
            black_box((e, body));
        });
    });
}

fn bench_msgpack_body_to_row(c: &mut Criterion) {
    // Extract body bytes once; the benchmark measures only the body→Row step.
    let payload = build_msgpack_envelope();
    let (_, body_bytes) = parse_msgpack_envelope(&payload).expect("setup parse");
    let body_owned = body_bytes.to_vec();
    c.bench_function("msgpack_body_to_row", |b| {
        b.iter(|| {
            let row: beava_core::row::Row =
                rmp_serde::from_slice(black_box(&body_owned)).expect("deser");
            black_box(row);
        });
    });
}

fn bench_json_body_to_row(c: &mut Criterion) {
    let payload = build_json_envelope();
    let (_, body_bytes) = parse_json_envelope(&payload).expect("setup parse");
    let body_owned = body_bytes.to_vec();
    c.bench_function("json_body_to_row", |b| {
        b.iter(|| {
            let row: beava_core::row::Row =
                sonic_rs::from_slice(black_box(&body_owned)).expect("deser");
            black_box(row);
        });
    });
}

criterion_group!(
    parse_envelope,
    bench_parse_msgpack_envelope,
    bench_parse_json_envelope,
    bench_msgpack_body_to_row,
    bench_json_body_to_row
);
criterion_main!(parse_envelope);
