// Phase 2.5 frame codec encode/decode throughput benches.
//
// Covers PERF-BENCH-WIRE-01: encode_frame + decode_frame across three
// representative payload sizes.
//
// Bench IDs (6 total):
//   encode/register_small       — ~13 B  (minimal JSON control frame)
//   encode/register_medium      — ~4 KiB (realistic multi-descriptor register)
//   encode/register_near_limit  — 1 MiB  (upper end of realistic bodies)
//   decode/register_small
//   decode/register_medium
//   decode/register_near_limit

use beava_core::wire::{decode_frame, encode_frame, Frame};
use bytes::BytesMut;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

const OP_REGISTER: u16 = 0x0001;
const CT_JSON: u8 = 0x01;
const MAX_FRAME: u32 = 4 * 1024 * 1024;

// ─── Payload fixtures ─────────────────────────────────────────────────────────

fn payload_small() -> Vec<u8> {
    br#"{"nodes":[]}"#.to_vec()
}

fn payload_medium() -> Vec<u8> {
    // ~4 KiB: 128 repeats of a 32-byte descriptor placeholder, valid JSON array.
    let one = br#"{"id":"00000000","kind":"event"}"#;
    let mut buf = Vec::with_capacity(4096 + 16);
    buf.extend_from_slice(b"[");
    for i in 0..128u32 {
        if i > 0 {
            buf.extend_from_slice(b",");
        }
        buf.extend_from_slice(one);
    }
    buf.extend_from_slice(b"]");
    buf
}

fn payload_near_limit() -> Vec<u8> {
    // Exactly 1 MiB of deterministic bytes (well below the 4 MiB cap).
    let mut buf = Vec::with_capacity(1 << 20);
    for i in 0u32..(1 << 20) {
        buf.push((i as u8).wrapping_mul(31));
    }
    buf
}

// ─── Bench helpers ────────────────────────────────────────────────────────────

fn bench_encode(c: &mut Criterion, name: &str, payload: Vec<u8>) {
    let frame = Frame::new(OP_REGISTER, CT_JSON, payload.clone());
    let payload_len = payload.len();
    let mut group = c.benchmark_group("encode");
    group.throughput(Throughput::Bytes(payload_len as u64));
    group.bench_function(name, |b| {
        b.iter(|| {
            let mut out = BytesMut::with_capacity(payload_len + 16);
            encode_frame(black_box(&frame), &mut out);
            black_box(out);
        });
    });
    group.finish();
}

fn bench_decode(c: &mut Criterion, name: &str, payload: Vec<u8>) {
    let frame = Frame::new(OP_REGISTER, CT_JSON, payload.clone());
    let mut encoded = BytesMut::new();
    encode_frame(&frame, &mut encoded);
    // Freeze once — each iteration clones cheaply from the shared backing buf.
    let encoded = encoded.freeze();
    let payload_len = payload.len();

    let mut group = c.benchmark_group("decode");
    group.throughput(Throughput::Bytes(payload_len as u64));
    group.bench_function(name, |b| {
        b.iter(|| {
            // Fresh BytesMut each iteration so decode_frame always sees a full frame.
            let mut buf = BytesMut::from(&encoded[..]);
            let got = decode_frame(black_box(&mut buf), MAX_FRAME)
                .expect("valid frame")
                .expect("complete frame");
            black_box(got);
        });
    });
    group.finish();
}

// ─── Bench entry point ────────────────────────────────────────────────────────

fn all_benches(c: &mut Criterion) {
    bench_encode(c, "register_small", payload_small());
    bench_encode(c, "register_medium", payload_medium());
    bench_encode(c, "register_near_limit", payload_near_limit());
    bench_decode(c, "register_small", payload_small());
    bench_decode(c, "register_medium", payload_medium());
    bench_decode(c, "register_near_limit", payload_near_limit());
}

criterion_group!(phase25_wire, all_benches);
criterion_main!(phase25_wire);

// Published for the red-commit contract test.
// reason: red-commit contract constant referenced only by the unit-test
// below; the criterion bench binary doesn't read it.
#[allow(dead_code)]
pub mod phase25_wire_benches {
    pub const CRITERION_GROUP_COUNT: usize = 1;
}

// ─── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    // reason: glob import covers symbols referenced via fully-qualified
    // paths inside individual tests; per-test imports would be noisier.
    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn bench_has_nontrivial_output() {
        use super::{decode_frame, encode_frame, phase25_wire_benches, Frame, MAX_FRAME};
        use bytes::BytesMut;

        let frame = Frame::new(0x0001, 0x01, &b"{\"nodes\":[]}"[..]);
        let mut out = BytesMut::new();
        encode_frame(&frame, &mut out);
        assert!(!out.is_empty(), "encode_frame must produce bytes");

        let decoded = decode_frame(&mut out.clone(), MAX_FRAME)
            .expect("valid")
            .expect("complete");
        assert_eq!(decoded.op, 0x0001);

        // RED contract symbol is now present — this compiles and passes.
        let _ = phase25_wire_benches::CRITERION_GROUP_COUNT;
    }

    #[test]
    fn payload_sizes_are_representative() {
        use super::{payload_medium, payload_near_limit, payload_small};

        assert!(payload_small().len() < 100, "small payload must be tiny");
        let med = payload_medium();
        assert!(
            med.len() >= 3_000 && med.len() <= 6_000,
            "medium payload should be ~4 KiB, got {} bytes",
            med.len()
        );
        let near = payload_near_limit();
        assert_eq!(
            near.len(),
            1 << 20,
            "near-limit payload must be exactly 1 MiB"
        );
    }

    #[test]
    fn encode_decode_round_trips_all_sizes() {
        use super::{
            decode_frame, encode_frame, payload_medium, payload_near_limit, payload_small, Frame,
            CT_JSON, MAX_FRAME, OP_REGISTER,
        };
        use bytes::BytesMut;

        for payload in [payload_small(), payload_medium(), payload_near_limit()] {
            let frame = Frame::new(OP_REGISTER, CT_JSON, payload.clone());
            let mut out = BytesMut::new();
            encode_frame(&frame, &mut out);
            // Wire format overhead: 4 (len prefix) + 2 (op) + 1 (ct) = 7 bytes
            assert_eq!(out.len(), payload.len() + 7);

            let decoded = decode_frame(&mut out.clone(), MAX_FRAME)
                .expect("no error")
                .expect("complete frame");
            assert_eq!(decoded.op, OP_REGISTER);
            assert_eq!(decoded.content_type, CT_JSON);
            assert_eq!(decoded.payload.as_ref(), payload.as_slice());
        }
    }
}
