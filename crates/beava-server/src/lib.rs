//! beava-server: the server binary's logic crate.
//!
//! Growth plan — see 01-CONTEXT.md:
//! - Plan 02: `cli` module + CLI wiring; re-exports `beava_core::config::Config`
//! - Plan 03: `logging` module
//! - Plan 04: `http` module + `Server` type + graceful shutdown
//! - Plan 05: `testing::TestServer`
//! - Phase 6 Plan 03: `idem_cache` + `push` + `AppState` WAL wiring

pub mod cli;
pub mod feature_query;
pub mod http;
pub mod http_admin;
pub mod idem_cache;
pub mod logging;
pub mod push;
pub mod recovery;
pub mod register;
pub mod registry_debug;
pub mod runtime_core_glue;
pub mod server;
pub mod shutdown;
pub mod snapshot_task;
pub mod tcp;
pub mod temporal_http;

#[cfg(any(feature = "testing", test))]
pub mod testing;

pub use beava_core::config::{self, Config, ConfigError};
pub use server::{Server, ServerError, ServerV18};

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
}

impl AppState {
    pub fn new(dev_agg: DevAggState, wal_sink: WalSink, idem_cache: Arc<IdemCache>) -> Self {
        Self {
            dev_agg,
            wal_sink,
            idem_cache,
        }
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
