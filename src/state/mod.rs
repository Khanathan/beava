pub mod store;
pub mod snapshot;
pub mod eviction;
pub mod event_log;

#[cfg(feature = "slatedb-backend")]
pub mod slate_backend;

// Phase 14: re-export StreamStore for use in ConcurrentAppState
pub use store::StreamStore;
