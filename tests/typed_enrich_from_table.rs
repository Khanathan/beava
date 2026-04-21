// Phase 59.6 SC-3 — cross-shard EnrichFromTable runs end-to-end on typed rows;
// output byte-identical to Value path.
//
// Wave 0: RED. Wave 3 lands the typed EnrichFromTable operator (mirrors
// `tests/cross_shard_enrich_from_table.rs` scenario but on typed rows) and
// flips this to GREEN.

#![allow(unused_imports)]

#[test]
#[ignore = "59.6-W3"]
fn typed_enrich_from_table_byte_identical_to_value_path() {
    // Scenario mirrors tests/cross_shard_enrich_from_table.rs:
    //   - @bv.source_table Countries, shard_key=country_code
    //   - @bv.stream Txns, shard_key=user_id (J≠K on cross-shard pair)
    //   - Typed-path engine and Value-path engine consume the same event stream
    //   - After ingest, emitted enriched rows MUST be byte-identical between paths.
    panic!("SC-3 RED: typed EnrichFromTable not yet implemented; expected in Wave 3");
}
