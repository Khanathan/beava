//! Phase 4 Rust-side acceptance gate — ROADMAP SC1/SC2/SC3/SC5 smoke over HTTP + TCP.
//! Python-side coverage of SC1..SC5 + the SC4 client/server equivalence proptest lives in Plan 04-07.
//!
//! All tests in this file are RED stubs that will panic until Task 1.b fills in the
//! implementation.  The red commit makes `cargo test --test phase4_smoke` exit non-zero.

/// SC1 (HTTP): filter registered over HTTP rejects events failing the predicate.
/// Proven via POST /dev/apply_ops returning {kept: false} for failing rows and
/// {kept: true, row: ...} for passing rows.
#[tokio::test]
async fn sc1_http_filter_rejects_failing_events() {
    panic!("red stub: 04-06 impl pending");
}

/// SC1 (TCP): same as sc1_http but the derivation is registered over TCP.
/// /dev/apply_ops verification still goes over HTTP (dev endpoint is HTTP-only).
#[tokio::test]
async fn sc1_tcp_filter_rejects_failing_events() {
    panic!("red stub: 04-06 impl pending");
}

/// SC2: with_columns adds a derived field visible to downstream nodes.
/// Proven via GET /registry showing the server-propagated schema with the new
/// field AND via a downstream derivation registering successfully against it
/// AND via /dev/apply_ops showing the field in the row.
#[tokio::test]
async fn sc2_with_columns_adds_derived_field_visible_downstream() {
    panic!("red stub: 04-06 impl pending");
}

/// SC3: chained ops filter → select → with_columns → cast compose correctly;
/// schema propagates through every step.
/// Proven via GET /registry showing final schema AND /dev/apply_ops round-trip.
#[tokio::test]
async fn sc3_chained_ops_filter_select_with_columns_cast_schema_propagates() {
    panic!("red stub: 04-06 impl pending");
}

/// SC5 (HTTP): malformed predicate at register returns 400 with
/// `path` pointing to the offending expression and `code == "invalid_expression"`.
#[tokio::test]
async fn sc5_malformed_predicate_returns_400_with_path_http() {
    panic!("red stub: 04-06 impl pending");
}

/// SC5 (TCP): same as sc5_http but the malformed registration is sent over TCP.
/// Expects op=0xFFFF error frame with code="invalid_expression" and path containing
/// "ops[0].expr".
#[tokio::test]
async fn sc5_malformed_predicate_returns_error_frame_tcp() {
    panic!("red stub: 04-06 impl pending");
}

/// Contract test: Registry::compiled_chain returns Some(Arc<OpChain>) after a
/// derivation with ops is registered, and calling chain.apply(row) in-process
/// agrees with what POST /dev/apply_ops returns for the same row.
/// Establishes the contract Phase 5's apply loop will use.
#[tokio::test]
async fn phase4_compiled_chain_is_retrievable_post_register() {
    panic!("red stub: 04-06 impl pending");
}
