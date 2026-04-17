//! Phase 45 — HTTP-03: NDJSON streaming ingest tests.
//!
//! Wave 0: all sub-tests are stubbed until Wave 2 wires the NDJSON handler.

mod http_common;

// ---------------------------------------------------------------------------
// Wave 2 stubs — filled by 45-03
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "Phase 45 Wave 2: http_push_ndjson handler stub"]
async fn ndjson_streams_10k_events_in_10_chunks() {
    panic!("MISSING: Wave 2 must implement NDJSON chunked ingest (HTTP-03)");
}

#[tokio::test]
#[ignore = "Phase 45 Wave 2: http_push_ndjson handler stub"]
async fn ndjson_summary_response_shape() {
    panic!("MISSING: Wave 2 must implement NDJSON summary response (HTTP-03)");
}
