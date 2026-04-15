//! Phase 31-01: client-side state wrappers for the streaming path.
//!
//! This module sits on top of `crate::state::store::StateStore` and exposes
//! an `Arc<parking_lot::RwLock<StateStore>>` alias used *only* by the
//! streaming client. The historical path (Phase 28-04, `client::clone::run_clone`)
//! continues to own an unlocked `StateStore` directly — this alias is
//! opt-in and is never wired into the historical codepath.
//!
//! Rationale: the streaming apply thread must mutate the store while
//! concurrent `.get()` readers walk it. `StateStore`'s interior DashMap
//! already tolerates concurrent reads + writes, so in principle the
//! `RwLock` is belt-and-suspenders. We keep it anyway because Phase 31-02's
//! `.watch()` generator needs a coarse synchronization point to coordinate
//! "snapshot flipping" (bulk_load under write lock, then event replay) with
//! registered watchers (per 31-01-PLAN locked-decision §D1). The historical
//! path has no such coordination need.

use crate::state::store::StateStore;
use std::sync::Arc;

/// Lock-wrapped `StateStore` used by the streaming client.
///
/// Alias-only — no new methods. Callers use `.read()` / `.write()` from
/// `parking_lot::RwLock`. Phase 28-04's `FrozenClient` does NOT use this
/// alias; it owns `StateStore` directly.
pub type StreamingStore = Arc<parking_lot::RwLock<StateStore>>;

/// Wrap an owned `StateStore` in the streaming `Arc<RwLock<_>>` shell.
///
/// Used by `StreamingClient::connect` after bulk-loading the snapshot.
pub fn into_streaming(store: StateStore) -> StreamingStore {
    Arc::new(parking_lot::RwLock::new(store))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn into_streaming_roundtrip() {
        let s = into_streaming(StateStore::new());
        assert_eq!(Arc::strong_count(&s), 1);
        let _clone = s.clone();
        assert_eq!(Arc::strong_count(&s), 2);
        // Basic read/write smoke.
        {
            let _r = s.read();
        }
        {
            let _w = s.write();
        }
    }

    #[test]
    fn concurrent_read_write_smoke() {
        use std::thread;
        let s = into_streaming(StateStore::new());
        let s2 = s.clone();
        let writer = thread::spawn(move || {
            for _ in 0..500 {
                let _guard = s2.write();
                // Simulate tiny critical section.
            }
        });
        let s3 = s.clone();
        let reader = thread::spawn(move || {
            for _ in 0..500 {
                let _guard = s3.read();
            }
        });
        writer.join().unwrap();
        reader.join().unwrap();
    }
}
