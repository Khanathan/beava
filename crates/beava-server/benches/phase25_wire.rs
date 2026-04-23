// Phase 2.5 frame codec bench (SKELETON — RED state).
//
// This file references `phase25_wire_benches::CRITERION_GROUP_COUNT` which does
// not exist yet. That reference is in `main()` so cargo bench --bench phase25_wire
// fails to compile until Task 1.b adds the module. Compile failure == RED.

use beava_core::wire::{decode_frame, encode_frame, Frame};
use bytes::BytesMut;

fn main() {
    // Smoke: the codec must round-trip a minimal register frame.
    let frame = Frame::new(0x0001, 0x01, &b"{\"nodes\":[]}"[..]);
    let mut out = BytesMut::new();
    encode_frame(&frame, &mut out);
    assert!(!out.is_empty(), "encode_frame must produce bytes");

    let decoded = decode_frame(&mut out.clone(), 4 * 1024 * 1024)
        .expect("decode_frame should not error on self-produced bytes")
        .expect("decode_frame should yield a complete frame");
    assert_eq!(decoded.op, 0x0001);

    // RED contract: assert the bench module publishes a criterion_main entry.
    // This symbol does not exist yet; compilation fails. Task 1.b adds it.
    let _ = phase25_wire_benches::CRITERION_GROUP_COUNT;
}
