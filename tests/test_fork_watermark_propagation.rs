// CORR-08: replica_ingest_batch must call engine.watermarks.observe() per event
// so fork-replica watermarks advance and downstream table cascades are not
// stalled.
// RED until Phase 46 Wave 3 (D-19) adds the observe() call in
// src/server/tcp.rs replica_ingest_batch.
//
// Once Wave 3 lands:
// - Remove the #[ignore] attribute below.
// - Stand up a mock upstream serving OP_SUBSCRIBE, run a local replica in fork
//   mode with a downstream table-cascade pipeline, assert the downstream
//   watermark advances as events flow.

#[test]
#[ignore = "Phase 46 Wave 3 (D-19): replica_ingest_batch missing watermarks.observe() call"]
fn replica_batch_advances_watermark() {
    // Arrange: create a PipelineEngine in fork-replica mode.
    //          Register stream "Upstream" with a table-cascade child "Derived".
    // Act:     call replica_ingest_batch with N events carrying explicit
    //          event_time values.
    // Assert:  engine.watermarks.observed_max("Upstream") >= max event_time
    //          in the ingested batch.
    //          Downstream "Derived" watermark has also advanced (γ-propagation).
    panic!("MISSING: Wave 3 (D-19) must add watermarks.observe() to replica_ingest_batch");
}
