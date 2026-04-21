//! Phase 60 TPC-PERF-10 — hot-key salting behavioral envelope.
//!
//! Wave 0 (60-00): RED scaffolding. Every test below is tagged
//! `#[ignore = "60-W[1-4]"]` and contains a placeholder body (or stub
//! assertions) that downstream waves replace with real assertions.
//!
//! Naming convention for the wave markers:
//!   - `60-W1`: parser + typing (Plan 60-01).
//!   - `60-W2`: ingest-side salt routing (Plan 60-02).
//!   - `60-W3`: read-side scatter-gather (Plan 60-03).
//!   - `60-W4`: metrics + perf gate (Plan 60-04).

// -----------------------------------------------------------------------
// Wave 1 — parser
// -----------------------------------------------------------------------

#[test]
#[ignore = "60-W1"]
#[allow(dead_code)]
fn parse_salt_suffix_valid_power_of_2() {
    unimplemented!("60-W1: parse_shard_key_with_salt(\"user_id:salt(16)\") == Ok((Single(\"user_id\"), Some(16)))");
}

#[test]
#[ignore = "60-W1"]
#[allow(dead_code)]
fn parse_salt_suffix_rejects_non_power_of_2() {
    unimplemented!("60-W1: parse_shard_key_with_salt(\"user_id:salt(10)\") errors with message containing 'power of 2'");
}

#[test]
#[ignore = "60-W1"]
#[allow(dead_code)]
fn parse_salt_suffix_rejects_out_of_range() {
    unimplemented!("60-W1: parse_shard_key_with_salt(\"user_id:salt(512)\") errors with message containing '[2, 256]'");
}

// -----------------------------------------------------------------------
// Wave 2 — ingest-side routing
// -----------------------------------------------------------------------

#[test]
#[ignore = "60-W2"]
#[allow(dead_code)]
fn ingest_salted_stream_spreads_across_shards() {
    unimplemented!(
        "60-W2: at N=8 with salt(16), 1000 events all user_id=hot land on >= 8 distinct shards"
    );
}

#[test]
#[ignore = "60-W2"]
#[allow(dead_code)]
fn unsalted_stream_has_zero_overhead() {
    unimplemented!(
        "60-W2: salt_cardinality=None routes ONE event through shard_hint_for_event exactly once"
    );
}

#[test]
#[ignore = "60-W2"]
#[allow(dead_code)]
fn shard_hint_salted_preserves_nonsalted_behavior() {
    unimplemented!(
        "60-W2: salt=None produces byte-identical output to shard_hint_for_event"
    );
}

#[test]
#[ignore = "60-W2"]
#[allow(dead_code)]
fn shard_hint_salted_spreads_hot_key() {
    unimplemented!(
        "60-W2: same user_id=hot with 16 distinct primary_event_ids => >=12 distinct salt indices"
    );
}

#[test]
#[ignore = "60-W2"]
#[allow(dead_code)]
fn shard_hint_salted_deterministic() {
    unimplemented!(
        "60-W2: same (event, primary_event_id) produces identical hint across two calls"
    );
}

#[test]
#[ignore = "60-W2"]
#[allow(dead_code)]
fn derive_storage_key_salted_nonzero_length_suffix() {
    unimplemented!(
        "60-W2: derive_storage_key format is exactly 'orig:idx' with idx in range [0, N)"
    );
}

// -----------------------------------------------------------------------
// Wave 3 — read-side scatter-gather
// -----------------------------------------------------------------------

#[test]
#[ignore = "60-W3"]
#[allow(dead_code)]
fn read_scatter_gathers_across_salts() {
    unimplemented!(
        "60-W3: EnrichFromTable right-side lookup against salted table returns row regardless of salt variant"
    );
}

#[test]
#[ignore = "60-W3"]
#[allow(dead_code)]
fn salted_fan_out_metric_increments() {
    unimplemented!(
        "60-W3: beava_salt_fanout_reads_total{{stream,salt_cardinality}} increments per scatter"
    );
}

#[test]
#[ignore = "60-W3"]
#[allow(dead_code)]
fn expand_salt_variants_none_returns_singleton() {
    unimplemented!("60-W3: expand_salt_variants(\"key\", None) => [\"key\"]");
}

#[test]
#[ignore = "60-W3"]
#[allow(dead_code)]
fn expand_salt_variants_16_returns_16_derived_keys() {
    unimplemented!("60-W3: expand_salt_variants(\"key\", Some(16)) => 16 strings 'key:0'..'key:15'");
}

#[test]
#[ignore = "60-W3"]
#[allow(dead_code)]
fn combine_salt_variants_empty_returns_none() {
    unimplemented!("60-W3: combine_salt_variants over all-None => None");
}

#[test]
#[ignore = "60-W3"]
#[allow(dead_code)]
fn combine_salt_variants_sum_aggregates() {
    unimplemented!("60-W3: [Some(1), Some(2), Some(3)] combined with add => Some(6)");
}

#[test]
#[ignore = "60-W3"]
#[allow(dead_code)]
fn combine_salt_variants_last_value_picks_freshest() {
    unimplemented!("60-W3: last-value combine picks row with max event_time");
}

#[test]
#[ignore = "60-W3"]
#[allow(dead_code)]
fn read_same_shard_salt_stays_inline() {
    unimplemented!(
        "60-W3: at N=1 all salt variants hash to shard 0; no cross-shard hop"
    );
}

// -----------------------------------------------------------------------
// Wave 4 — metrics + observability
// -----------------------------------------------------------------------

#[test]
#[ignore = "60-W4"]
#[allow(dead_code)]
fn beava_shard_hot_key_owner_ratio_emits() {
    unimplemented!(
        "60-W4: after 10K Zipf-1.2 writes, GET /metrics shows beava_shard_hot_key_owner_ratio on hot shard > 0.5"
    );
}

#[test]
#[ignore = "60-W4"]
#[allow(dead_code)]
fn salted_aggregate_eps_exceeds_unsalted_by_50pct() {
    unimplemented!(
        "60-W4: Criterion A/B asserts salted aggregate EPS >= 1.5x unsalted baseline"
    );
}
