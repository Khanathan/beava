//! Phase 45 — HTTP/TCP schema parity test.
//!
//! Wave 0: #[ignore]'d until Wave 1 wires real push handlers.

mod http_common;

#[tokio::test]
#[ignore = "Phase 45 Wave 1: requires real push handler to compare HTTP vs TCP output"]
async fn same_json_through_http_and_tcp_yields_identical_feature_values() {
    panic!("MISSING: Wave 1 must implement HTTP/TCP schema parity test (HTTP-01)");
}
