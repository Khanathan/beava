//! Phase 45 — HTTP-04 + HTTP-05: feature read and stream list tests.
//!
//! Wave 0: all sub-tests are stubbed until Wave 2 wires real handlers.

mod http_common;

// ---------------------------------------------------------------------------
// Wave 2 stubs — filled by 45-03
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "Phase 45 Wave 2: http_get_features handler stub"]
async fn features_by_key_all_tables() {
    panic!("MISSING: Wave 2 must implement GET /features/{{key}} (HTTP-04)");
}

#[tokio::test]
#[ignore = "Phase 45 Wave 2: http_get_features handler stub"]
async fn features_filtered_by_table() {
    panic!("MISSING: Wave 2 must implement GET /features/{{key}}?table=X (HTTP-04)");
}

#[tokio::test]
#[ignore = "Phase 45 Wave 2: http_list_streams handler stub"]
async fn list_streams_returns_watermark() {
    panic!("MISSING: Wave 2 must implement GET /streams (HTTP-05)");
}

#[tokio::test]
#[ignore = "Phase 45 Wave 2: http_get_stream handler stub"]
async fn stream_detail_returns_schema() {
    panic!("MISSING: Wave 2 must implement GET /streams/{{name}} (HTTP-05)");
}
