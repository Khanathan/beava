//! Metrics crate wiring for Phase 50 (Wave 2).
//!
//! Installs a global PrometheusRecorder via metrics-exporter-prometheus.
//! Hand-rolled /metrics output remains the primary scrape surface through
//! Wave 3; this recorder runs in parallel (D-06 parallel period).
//! Removal of the hand-rolled path lands in Wave 4.

#[allow(missing_docs)]
pub mod prometheus;
pub use prometheus::PrometheusHandle;

use std::sync::OnceLock;

static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Install the global Prometheus recorder. Idempotent — safe to call
/// multiple times (subsequent calls are no-ops). Must be called once
/// at server startup before any metrics! macro invocations.
pub fn install_prometheus_recorder() -> &'static PrometheusHandle {
    HANDLE.get_or_init(|| {
        // install_recorder() sets the global recorder and returns a handle.
        // If a recorder is already set (e.g. in tests), this will return Err;
        // we wrap it so the OnceLock guarantees we only call it once.
        let inner = ::metrics_exporter_prometheus::PrometheusBuilder::new()
            .install_recorder()
            .expect("failed to install PrometheusRecorder");
        PrometheusHandle::new(inner)
    })
}

/// Return a reference to the installed handle, or None if not yet installed.
pub fn handle() -> Option<&'static PrometheusHandle> {
    HANDLE.get()
}
