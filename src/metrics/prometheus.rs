//! PrometheusHandle: thin wrapper around metrics-exporter-prometheus handle.

/// Wrapper around the metrics-exporter-prometheus scrape handle.
pub struct PrometheusHandle {
    inner: ::metrics_exporter_prometheus::PrometheusHandle,
}

impl PrometheusHandle {
    pub(super) fn new(inner: ::metrics_exporter_prometheus::PrometheusHandle) -> Self {
        Self { inner }
    }

    /// Render all registered metrics as Prometheus text exposition format.
    /// Returns empty string if no metrics have been recorded yet.
    pub fn scrape(&self) -> String {
        self.inner.render()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scrape_returns_string_without_panic() {
        // Use build_recorder() to get a fresh recorder+handle pair without
        // touching the global recorder — avoids conflicts across test runs.
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let wrapper = PrometheusHandle::new(handle);
        // Before any metric recorded, scrape returns empty or just comment lines.
        let out = wrapper.scrape();
        // Just verify it doesn't panic and returns a String.
        let _ = out.len();
    }
}
