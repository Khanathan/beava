//! Phase 60 TPC-PERF-10 — register-time parse/reject envelope.
//!
//! Wave 0 (60-00): RED scaffolding. All tests `#[ignore = "60-W[1-2]"]`
//! until downstream waves land the real `register()` integration.

#[test]
#[ignore = "60-W1"]
#[allow(dead_code)]
fn salted_source_table_rejected() {
    unimplemented!(
        "60-W1: @bv.source_table with salt errors at register() citing D-D3"
    );
}

#[test]
#[ignore = "60-W1"]
#[allow(dead_code)]
fn salted_tuple_at_most_one_element() {
    unimplemented!(
        "60-W1: shard_key=(\"a:salt(4)\", \"b:salt(8)\") errors citing D-A5"
    );
}

#[test]
#[ignore = "60-W1"]
#[allow(dead_code)]
fn salted_join_emits_warning_not_reject() {
    unimplemented!(
        "60-W1: registering a join with one side salted, other not, emits SaltedJoinWarning; pipeline starts"
    );
}

#[test]
#[ignore = "60-W2"]
#[allow(dead_code)]
fn colon_in_key_rejects_salt_declaration() {
    unimplemented!(
        "60-W2: register-time sample validation errors when key_field value contains ':' and salt declared (D-G1)"
    );
}
