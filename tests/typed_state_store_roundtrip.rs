// Phase 59.6 SC-8 — inmem + fjall stores typed rows when stream has schema;
// snapshot v11 round-trip.

#![allow(unused_imports)]

#[test]
#[ignore = "59.6-W5"]
fn inmem_store_persists_typed_row() {
    // Wave 5: AHashMap<EntityKey, Row> in-memory path.
    panic!("SC-8 RED: inmem typed row storage not yet implemented; expected in Wave 5");
}

#[test]
#[ignore = "59.6-W5"]
fn fjall_store_persists_typed_row_memcpy() {
    // Wave 5: packed-row memcpy encoding (no serde_json round-trip).
    panic!("SC-8 RED: fjall packed-row encoding not yet implemented; expected in Wave 5");
}
