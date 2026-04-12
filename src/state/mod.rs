pub mod store;
pub mod snapshot;
pub mod eviction;
pub mod event_log;

// Phase 14: re-export StreamStore for use in ConcurrentAppState
pub use store::StreamStore;
