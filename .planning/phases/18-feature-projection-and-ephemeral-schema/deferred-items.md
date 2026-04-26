# Deferred Items -- Phase 18

## get_features projection applies globally across all streams

**Found during:** 18-02 Task 2 (E2E integration tests)
**Severity:** Medium
**Description:** In `PipelineEngine::get_features()` (pipeline.rs ~L1169), per-stream projections apply sequentially to the entire FeatureMap. When multiple streams have projection definitions, each one filters features from ALL streams (not just its own). This causes cross-stream interference: a `Select(["a", "b"])` on stream X will remove features from stream Y even if stream Y has its own projection.
**Impact:** Multiple projected streams on the same server instance interfere with each other's GET responses. Push responses (per-stream) are unaffected.
**Workaround:** Use unique feature name prefixes per stream to minimize collision, or isolate projected streams on separate server instances.
**Fix:** Projection in `get_features` should only filter features owned by the stream that defines the projection. Requires tracking feature-to-stream ownership in the FeatureMap or applying projection selectively.
