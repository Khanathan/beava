pub mod event_log;
pub mod eviction;
pub mod snapshot;
pub mod store;

#[cfg(feature = "slatedb-backend")]
pub mod slate_backend;

// Phase 14: re-export StreamStore for use in ConcurrentAppState
pub use store::StreamStore;
