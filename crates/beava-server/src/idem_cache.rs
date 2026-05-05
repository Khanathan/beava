//! Stream-level idempotency cache keyed by `(stream_id, dedupe_key)`.
//!
//! `get` is a read-lock lazy-expiry check (treats expired as a miss). A
//! background sweeper periodically calls `sweep_expired` under a write lock
//! to bound memory; v0 has no LRU cap, so operators must size the box.

use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashMap;

type StreamId = String;
type DedupeKey = String;

#[derive(Debug, Clone)]
pub struct CachedEntry {
    /// The full HTTP response body from the first successful push. Returned
    /// byte-identical on dedupe replay (success criterion #2).
    pub response_bytes: Bytes,
    pub ack_lsn: u64,
    pub inserted_at_ms: u64,
    pub expires_at_ms: u64,
}

#[derive(Debug, Default)]
pub struct IdemCache {
    inner: RwLock<HashMap<(StreamId, DedupeKey), CachedEntry>>,
}

impl IdemCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Lookup a cached entry. Returns None on miss OR expired entry. Does not
    /// remove expired entries — `sweep_expired` handles that under a write
    /// lock so `get` stays on the read path.
    pub fn get(&self, stream: &str, key: &str, now_ms: u64) -> Option<Bytes> {
        let g = self.inner.read();
        let entry = g.get(&(stream.to_string(), key.to_string()))?;
        if entry.expires_at_ms <= now_ms {
            return None;
        }
        Some(entry.response_bytes.clone())
    }

    /// Lookup returning both the cached body and the original push's
    /// `ack_lsn`. The TCP push encoder needs `ack_lsn` to frame the
    /// dedupe-replay body (`{ack_lsn, idempotent_replay: true,
    /// registry_version}`); HTTP already encodes it inside the verbatim
    /// cached body.
    pub fn get_with_ack_lsn(&self, stream: &str, key: &str, now_ms: u64) -> Option<(u64, Bytes)> {
        let g = self.inner.read();
        let entry = g.get(&(stream.to_string(), key.to_string()))?;
        if entry.expires_at_ms <= now_ms {
            return None;
        }
        Some((entry.ack_lsn, entry.response_bytes.clone()))
    }

    pub fn put(&self, stream: String, key: String, entry: CachedEntry) {
        self.inner.write().insert((stream, key), entry);
    }

    /// Remove any entries whose `expires_at_ms <= now_ms`. Returns the number
    /// removed.
    pub fn sweep_expired(&self, now_ms: u64) -> usize {
        let mut g = self.inner.write();
        let before = g.len();
        g.retain(|_, e| e.expires_at_ms > now_ms);
        before - g.len()
    }

    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(body: &str, expires_at_ms: u64) -> CachedEntry {
        CachedEntry {
            response_bytes: Bytes::copy_from_slice(body.as_bytes()),
            ack_lsn: 1,
            inserted_at_ms: 0,
            expires_at_ms,
        }
    }

    #[test]
    fn get_returns_none_on_miss() {
        let c = IdemCache::new();
        assert!(c.get("S", "K", 0).is_none());
    }

    #[test]
    fn put_then_get_returns_bytes() {
        let c = IdemCache::new();
        c.put("S".into(), "K".into(), entry("ok", 100));
        let got = c.get("S", "K", 10).expect("hit");
        assert_eq!(&got[..], b"ok");
    }

    #[test]
    fn expired_entry_is_miss() {
        let c = IdemCache::new();
        c.put("S".into(), "K".into(), entry("ok", 50));
        assert!(c.get("S", "K", 100).is_none());
    }

    #[test]
    fn sweep_removes_expired_only() {
        let c = IdemCache::new();
        c.put("S".into(), "a".into(), entry("a", 50));
        c.put("S".into(), "b".into(), entry("b", 200));
        assert_eq!(c.sweep_expired(100), 1);
        assert_eq!(c.len(), 1);
        assert!(c.get("S", "b", 100).is_some());
    }
}
