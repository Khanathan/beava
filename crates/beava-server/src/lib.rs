//! beava-server: the server binary's logic crate.
//!
//! Plan 12.6-07 sweep: legacy axum data plane deleted (push.rs, http.rs,
//! push_and_get.rs). The mio data plane (apply_shard + runtime_core_glue +
//! ServerV18 in server.rs) is now the SOLE data-plane runtime per
//! `project_phase18_no_dual_runtime`. Tokio admin sidecar lives in
//! http_admin.rs and binds on a separate port (cfg.admin_addr).

pub mod apply_shard;
pub mod cli;
pub mod feature_query;
pub mod http_admin;
pub mod idem_cache;
pub mod logging;
pub mod recovery;
pub mod register;
pub mod registry_debug;
pub mod runtime_core_glue;
pub mod server;
pub mod shutdown;
pub mod snapshot_task;
pub mod temporal_http;
pub mod wal_config;

#[cfg(any(feature = "testing", test))]
pub mod testing;

pub use beava_core::config::{self, Config, ConfigError};
pub use server::{ServerError, ServerV18};

use crate::idem_cache::IdemCache;
use crate::registry_debug::DevAggState;
use beava_persistence::WalSink;
use std::sync::Arc;

/// Unified application state introduced in Phase 6 Plan 03. Holds the
/// `DevAggState` (registry + state_tables + event_id counters) alongside the
/// WAL sink handle and idempotency cache. Shared by both HTTP and TCP handlers
/// via an Arc.
#[derive(Clone)]
pub struct AppState {
    pub dev_agg: DevAggState,
    pub wal_sink: WalSink,
    pub idem_cache: Arc<IdemCache>,
    /// Dev endpoints flag — gates `/registry` on the mio data plane
    /// (404 when false). Stored in an Arc<AtomicBool> so
    /// TestServer-builder callers can flip it post-spawn (matches the
    /// `.dev_endpoints(true)` builder method semantics).
    pub dev_endpoints: Arc<std::sync::atomic::AtomicBool>,
}

impl AppState {
    pub fn new(dev_agg: DevAggState, wal_sink: WalSink, idem_cache: Arc<IdemCache>) -> Self {
        Self {
            dev_agg,
            wal_sink,
            idem_cache,
            dev_endpoints: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Return true iff the data-plane `/registry` shim should serve. The
    /// flag is set only via `TestServer.dev_endpoints(true)`; production
    /// data-plane `/registry` is permanently 404. Production observability
    /// flows through the tokio admin sidecar on `cfg.admin_addr`.
    pub fn dev_endpoints_enabled(&self) -> bool {
        self.dev_endpoints
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Semantic version of the server binary.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Human-readable banner used by `main.rs` and logs.
pub fn banner() -> String {
    format!("beava v{} (skeleton)", VERSION)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn banner_includes_version() {
        let b = banner();
        assert!(b.contains(VERSION));
        assert!(b.starts_with("beava v"));
    }
}
