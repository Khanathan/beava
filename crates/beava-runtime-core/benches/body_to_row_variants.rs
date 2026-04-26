//! Spike 18-10 — body→Row deserialization variant benchmark.
//!
//! Measures 5 structural variants × 2 wire formats (msgpack + json) = 10 bench
//! functions. Goal: quantify heap-allocation overhead in the current
//! `Row(BTreeMap<String, Value>)` shape and evaluate SSO/inline-storage alternatives
//! before committing to a structural refactor in Plan 18-11.
//!
//! Variant summary:
//!   A — baseline: existing `Row` + `with_field` (re-clones key)
//!   B — BTreeMap<String, Value> + direct insert (skips with_field re-clone)
//!   C — BTreeMap<CompactString, ValueC> (SSO keys + SSO str values)
//!   D — SmallVec<[(CompactString, ValueC); 8]> (inline storage, no BTreeMap nodes)
//!   E — SmallVec<[ValueC; 8]> positional by descriptor (no keys stored at all)
//!
//! All body payloads: 6-field fraud event
//!   {amount: 99.95, ts: 1714234567000, account_id: "acc_123",
//!    merchant: "M_ACME", country: "US", method: "card"}

// Variant row types store values only for the bench measurement — fields are
// constructed by the deserializer and passed to black_box; they are never read
// back. Suppressing dead_code here is correct for a measurement-only spike.
#![allow(dead_code)]

use beava_core::row::{Row, Value};
use compact_str::CompactString;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};
use smallvec::SmallVec;
use std::collections::BTreeMap;

// ─── Body payload builders ────────────────────────────────────────────────────

/// Build just the body bytes (not the full envelope) as msgpack.
fn build_msgpack_body() -> Vec<u8> {
    use serde::Serialize;
    #[derive(Serialize)]
    struct Body<'a> {
        amount: f64,
        ts: i64,
        account_id: &'a str,
        merchant: &'a str,
        country: &'a str,
        method: &'a str,
    }
    rmp_serde::to_vec_named(&Body {
        amount: 99.95,
        ts: 1_714_234_567_000,
        account_id: "acc_123",
        merchant: "M_ACME",
        country: "US",
        method: "card",
    })
    .expect("serialize msgpack body")
}

/// Build just the body bytes as JSON (hardcoded literal).
fn build_json_body() -> Vec<u8> {
    br#"{"amount":99.95,"ts":1714234567000,"account_id":"acc_123","merchant":"M_ACME","country":"US","method":"card"}"#.to_vec()
}

// ─── Shared value visitor (inlined — BeavaValueSeed/Visitor are private in beava-core) ──

/// Local value visitor that produces `beava_core::row::Value`.
/// Mirrors `BeavaValueVisitor` from `crates/beava-core/src/row.rs` exactly.
struct LocalValueSeed;

impl<'de> DeserializeSeed<'de> for LocalValueSeed {
    type Value = Value;
    fn deserialize<D>(self, deserializer: D) -> Result<Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(LocalValueVisitor)
    }
}

struct LocalValueVisitor;

impl<'de> Visitor<'de> for LocalValueVisitor {
    type Value = Value;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a JSON/msgpack scalar, array, or object")
    }

    fn visit_bool<E>(self, v: bool) -> Result<Value, E> {
        Ok(Value::Bool(v))
    }
    fn visit_i64<E>(self, v: i64) -> Result<Value, E> {
        Ok(Value::I64(v))
    }
    fn visit_u64<E>(self, v: u64) -> Result<Value, E> {
        if v <= i64::MAX as u64 {
            Ok(Value::I64(v as i64))
        } else {
            Ok(Value::F64(v as f64))
        }
    }
    fn visit_i128<E>(self, v: i128) -> Result<Value, E> {
        if v >= i64::MIN as i128 && v <= i64::MAX as i128 {
            Ok(Value::I64(v as i64))
        } else {
            Ok(Value::F64(v as f64))
        }
    }
    fn visit_u128<E>(self, v: u128) -> Result<Value, E> {
        if v <= i64::MAX as u128 {
            Ok(Value::I64(v as i64))
        } else {
            Ok(Value::F64(v as f64))
        }
    }
    fn visit_f64<E>(self, v: f64) -> Result<Value, E> {
        Ok(Value::F64(v))
    }
    fn visit_str<E>(self, v: &str) -> Result<Value, E> {
        Ok(Value::Str(v.to_string()))
    }
    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Value, E> {
        Ok(Value::Str(v.to_string()))
    }
    fn visit_string<E>(self, v: String) -> Result<Value, E> {
        Ok(Value::Str(v))
    }
    fn visit_bytes<E>(self, v: &[u8]) -> Result<Value, E> {
        Ok(Value::Bytes(v.to_vec()))
    }
    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Value, E> {
        Ok(Value::Bytes(v))
    }
    fn visit_unit<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }
    fn visit_none<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }
    fn visit_some<D>(self, deserializer: D) -> Result<Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
    fn visit_seq<A>(self, mut seq: A) -> Result<Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut out = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        while let Some(elem) = seq.next_element_seed(LocalValueSeed)? {
            out.push(elem);
        }
        Ok(Value::List(out))
    }
    fn visit_map<A>(self, mut map: A) -> Result<Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut out = BTreeMap::new();
        while let Some(key) = map.next_key::<String>()? {
            let value: Value = map.next_value_seed(LocalValueSeed)?;
            out.insert(key, value);
        }
        Ok(Value::Map(out))
    }
}

// ─── Variant B: BTreeMap<String, Value> — direct insert, no with_field re-clone ──

struct RowB(BTreeMap<String, Value>);

struct RowBVisitor;

impl<'de> Visitor<'de> for RowBVisitor {
    type Value = RowB;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a map of string field names to primitive values")
    }

    fn visit_map<M>(self, mut access: M) -> Result<RowB, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut row = RowB(BTreeMap::new());
        while let Some(key) = access.next_key::<String>()? {
            let value: Value = access.next_value_seed(LocalValueSeed)?;
            // Direct insert — no with_field(&key, value) re-clone.
            row.0.insert(key, value);
        }
        Ok(row)
    }
}

impl<'de> serde::Deserialize<'de> for RowB {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(RowBVisitor)
    }
}

// ─── ValueC: CompactString for Str variant ────────────────────────────────────

/// Variant of Value using CompactString for Str (SSO: strings ≤24 bytes inline).
/// Omits Json/List/Map variants — returns Null if encountered
/// (our 6-field bench payload never uses them).
#[derive(Debug, Clone)]
enum ValueC {
    Null,
    Str(CompactString),
    I64(i64),
    F64(f64),
    Bool(bool),
    Bytes(Vec<u8>),
}

struct ValueCSeed;

impl<'de> DeserializeSeed<'de> for ValueCSeed {
    type Value = ValueC;
    fn deserialize<D>(self, deserializer: D) -> Result<ValueC, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(ValueCVisitor)
    }
}

struct ValueCVisitor;

impl<'de> Visitor<'de> for ValueCVisitor {
    type Value = ValueC;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a JSON/msgpack scalar (no nested objects for bench)")
    }

    fn visit_bool<E>(self, v: bool) -> Result<ValueC, E> {
        Ok(ValueC::Bool(v))
    }
    fn visit_i64<E>(self, v: i64) -> Result<ValueC, E> {
        Ok(ValueC::I64(v))
    }
    fn visit_u64<E>(self, v: u64) -> Result<ValueC, E> {
        if v <= i64::MAX as u64 {
            Ok(ValueC::I64(v as i64))
        } else {
            Ok(ValueC::F64(v as f64))
        }
    }
    fn visit_i128<E>(self, v: i128) -> Result<ValueC, E> {
        if v >= i64::MIN as i128 && v <= i64::MAX as i128 {
            Ok(ValueC::I64(v as i64))
        } else {
            Ok(ValueC::F64(v as f64))
        }
    }
    fn visit_u128<E>(self, v: u128) -> Result<ValueC, E> {
        if v <= i64::MAX as u128 {
            Ok(ValueC::I64(v as i64))
        } else {
            Ok(ValueC::F64(v as f64))
        }
    }
    fn visit_f64<E>(self, v: f64) -> Result<ValueC, E> {
        Ok(ValueC::F64(v))
    }
    fn visit_str<E>(self, v: &str) -> Result<ValueC, E> {
        // CompactString::from(&str) — SSO for strings ≤24 bytes (all our test fields fit)
        Ok(ValueC::Str(CompactString::from(v)))
    }
    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<ValueC, E> {
        Ok(ValueC::Str(CompactString::from(v)))
    }
    fn visit_string<E>(self, v: String) -> Result<ValueC, E> {
        Ok(ValueC::Str(CompactString::from(v)))
    }
    fn visit_bytes<E>(self, v: &[u8]) -> Result<ValueC, E> {
        Ok(ValueC::Bytes(v.to_vec()))
    }
    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<ValueC, E> {
        Ok(ValueC::Bytes(v))
    }
    fn visit_unit<E>(self) -> Result<ValueC, E> {
        Ok(ValueC::Null)
    }
    fn visit_none<E>(self) -> Result<ValueC, E> {
        Ok(ValueC::Null)
    }
    fn visit_some<D>(self, deserializer: D) -> Result<ValueC, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
    fn visit_seq<A>(self, _seq: A) -> Result<ValueC, A::Error>
    where
        A: SeqAccess<'de>,
    {
        // Bench payload has no arrays; return Null as stub
        Ok(ValueC::Null)
    }
    fn visit_map<A>(self, _map: A) -> Result<ValueC, A::Error>
    where
        A: MapAccess<'de>,
    {
        // Bench payload has no nested maps in values; return Null as stub
        Ok(ValueC::Null)
    }
}

// ─── Variant C: BTreeMap<CompactString, ValueC> ───────────────────────────────

struct RowC(BTreeMap<CompactString, ValueC>);

struct RowCVisitor;

impl<'de> Visitor<'de> for RowCVisitor {
    type Value = RowC;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a map with CompactString keys and ValueC values")
    }

    fn visit_map<M>(self, mut access: M) -> Result<RowC, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut row = RowC(BTreeMap::new());
        // Deserialize key as &str then convert to CompactString — avoids String alloc for key.
        while let Some(key) = access.next_key::<CompactString>()? {
            let value: ValueC = access.next_value_seed(ValueCSeed)?;
            row.0.insert(key, value);
        }
        Ok(row)
    }
}

impl<'de> serde::Deserialize<'de> for RowC {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(RowCVisitor)
    }
}

// ─── Variant D: SmallVec<[(CompactString, ValueC); 8]> ────────────────────────

/// Inline storage for ≤8 fields — avoids BTreeMap node heap allocation.
/// 6-field test payload fits entirely inline.
struct RowD(SmallVec<[(CompactString, ValueC); 8]>);

struct RowDVisitor;

impl<'de> Visitor<'de> for RowDVisitor {
    type Value = RowD;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a map deserializing into SmallVec of (CompactString, ValueC) tuples")
    }

    fn visit_map<M>(self, mut access: M) -> Result<RowD, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut row = RowD(SmallVec::new());
        while let Some(key) = access.next_key::<CompactString>()? {
            let value: ValueC = access.next_value_seed(ValueCSeed)?;
            row.0.push((key, value));
        }
        Ok(row)
    }
}

impl<'de> serde::Deserialize<'de> for RowD {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(RowDVisitor)
    }
}

// ─── Variant E: SmallVec<[ValueC; 8]> positional by descriptor ───────────────

/// Descriptor-driven positional storage — no keys stored at all.
/// Column ID is looked up from static FIELD_NAMES during deserialization.
static FIELD_NAMES: &[&str] = &[
    "amount",
    "ts",
    "account_id",
    "merchant",
    "country",
    "method",
];

struct RowE(SmallVec<[ValueC; 8]>);

struct RowEVisitor;

impl<'de> Visitor<'de> for RowEVisitor {
    type Value = RowE;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a map deserializing into positional SmallVec by descriptor")
    }

    fn visit_map<M>(self, mut access: M) -> Result<RowE, M::Error>
    where
        M: MapAccess<'de>,
    {
        // Pre-init 6 slots with Null.
        let mut row = RowE(SmallVec::from_elem(ValueC::Null, FIELD_NAMES.len()));
        while let Some(key) = access.next_key::<CompactString>()? {
            let value: ValueC = access.next_value_seed(ValueCSeed)?;
            // Linear scan over 6 entries — O(1) in practice for this payload size.
            if let Some(col_id) = FIELD_NAMES.iter().position(|f| *f == key.as_str()) {
                row.0[col_id] = value;
            }
            // Unknown keys are silently dropped (no schema match).
        }
        Ok(row)
    }
}

impl<'de> serde::Deserialize<'de> for RowE {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(RowEVisitor)
    }
}

// ─── Bench functions ──────────────────────────────────────────────────────────

// Variant A — baseline: existing `Row` with `with_field` re-clone

fn bench_a_msgpack(c: &mut Criterion) {
    let body = build_msgpack_body();
    c.bench_function("variant_a_btreemap_string_msgpack", |b| {
        b.iter(|| {
            let row: Row = rmp_serde::from_slice(black_box(&body)).expect("deser");
            black_box(row);
        });
    });
}

fn bench_a_json(c: &mut Criterion) {
    let body = build_json_body();
    c.bench_function("variant_a_btreemap_string_json", |b| {
        b.iter(|| {
            let row: Row = sonic_rs::from_slice(black_box(&body)).expect("deser");
            black_box(row);
        });
    });
}

// Variant B — BTreeMap<String, Value> direct insert (skip with_field re-clone)

fn bench_b_msgpack(c: &mut Criterion) {
    let body = build_msgpack_body();
    c.bench_function("variant_b_btreemap_direct_insert_msgpack", |b| {
        b.iter(|| {
            let row: RowB = rmp_serde::from_slice(black_box(&body)).expect("deser");
            black_box(row.0);
        });
    });
}

fn bench_b_json(c: &mut Criterion) {
    let body = build_json_body();
    c.bench_function("variant_b_btreemap_direct_insert_json", |b| {
        b.iter(|| {
            let row: RowB = sonic_rs::from_slice(black_box(&body)).expect("deser");
            black_box(row.0);
        });
    });
}

// Variant C — BTreeMap<CompactString, ValueC>

fn bench_c_msgpack(c: &mut Criterion) {
    let body = build_msgpack_body();
    c.bench_function("variant_c_btreemap_compact_str_msgpack", |b| {
        b.iter(|| {
            let row: RowC = rmp_serde::from_slice(black_box(&body)).expect("deser");
            black_box(row.0);
        });
    });
}

fn bench_c_json(c: &mut Criterion) {
    let body = build_json_body();
    c.bench_function("variant_c_btreemap_compact_str_json", |b| {
        b.iter(|| {
            let row: RowC = sonic_rs::from_slice(black_box(&body)).expect("deser");
            black_box(row.0);
        });
    });
}

// Variant D — SmallVec<[(CompactString, ValueC); 8]>

fn bench_d_msgpack(c: &mut Criterion) {
    let body = build_msgpack_body();
    c.bench_function("variant_d_smallvec_compact_str_msgpack", |b| {
        b.iter(|| {
            let row: RowD = rmp_serde::from_slice(black_box(&body)).expect("deser");
            black_box(row.0);
        });
    });
}

fn bench_d_json(c: &mut Criterion) {
    let body = build_json_body();
    c.bench_function("variant_d_smallvec_compact_str_json", |b| {
        b.iter(|| {
            let row: RowD = sonic_rs::from_slice(black_box(&body)).expect("deser");
            black_box(row.0);
        });
    });
}

// Variant E — SmallVec<[ValueC; 8]> positional by descriptor

fn bench_e_msgpack(c: &mut Criterion) {
    let body = build_msgpack_body();
    c.bench_function("variant_e_positional_smallvec_msgpack", |b| {
        b.iter(|| {
            let row: RowE = rmp_serde::from_slice(black_box(&body)).expect("deser");
            black_box(row.0);
        });
    });
}

fn bench_e_json(c: &mut Criterion) {
    let body = build_json_body();
    c.bench_function("variant_e_positional_smallvec_json", |b| {
        b.iter(|| {
            let row: RowE = sonic_rs::from_slice(black_box(&body)).expect("deser");
            black_box(row.0);
        });
    });
}

criterion_group!(
    body_to_row_variants,
    bench_a_msgpack,
    bench_a_json,
    bench_b_msgpack,
    bench_b_json,
    bench_c_msgpack,
    bench_c_json,
    bench_d_msgpack,
    bench_d_json,
    bench_e_msgpack,
    bench_e_json,
);
criterion_main!(body_to_row_variants);
