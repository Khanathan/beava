pub mod event_log;
pub mod eviction;
pub mod eviction_tracker;
pub mod s2_archive;
pub mod snapshot;
pub mod store;

// Phase 14: re-export StreamStore for use in ConcurrentAppState
pub use store::StreamStore;
