//! Plan 12-08 (D-C): per-thread BytesMut pool.
//!
//! Each IO worker thread holds one `BytesMutPool` of pre-sized BytesMut
//! buffers used by `WriteEncoder` closures to build response payloads
//! without per-call `BytesMut::with_capacity(4096)` allocator hits.
//!
//! # Reclamation
//!
//! Wave 4.b wires the pool into `worker_main_loop`'s write_rx drain. Each
//! encoder closure acquires a temporary buffer from the pool, builds the
//! response, extends the per-client `write_buf` from it, then releases the
//! pool buffer back. Eviction: pool grows past `cap` buffers → drop excess
//! on push (ArrayQueue::push returns Err(value) when full; we drop).
//!
//! # Why a fixed-cap ArrayQueue and not a Vec
//!
//! `ArrayQueue::pop` and `push` are lock-free and ~30 ns. The pool is also
//! `Clone` (an `Arc<ArrayQueue<_>>`), so it can be cheaply cloned into
//! encoder closures captured-by-move. Real per-thread isolation is achieved
//! by NOT sharing the same pool clone across worker threads — Wave 4.b
//! constructs ONE pool inside each worker's `worker_main_loop`.
//!
//! # Performance gate
//!
//! `apply_loop/pool_acquire_release` criterion bench (Wave 5.a) measures
//! the round-trip cost. Target: < 100 ns per (acquire + release).

use bytes::BytesMut;
use crossbeam_queue::ArrayQueue;
use std::sync::Arc;

/// Plan 12-08 (D-C) test instrumentation: cumulative count of pool
/// allocations (cache misses — pool was empty when acquire() was called).
static POOL_ALLOC_CALLS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Plan 12-08 (D-C) test instrumentation: cumulative count of pool acquires
/// (hits + misses).
static POOL_ACQUIRE_CALLS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Cumulative number of `BytesMut::with_capacity` allocations (cache misses).
/// Test hook only — Wave 4 verifies that under steady-state push load
/// `pool_alloc_count() < pool.cap` (only initial allocs; no per-response
/// alloc churn).
#[doc(hidden)]
pub fn pool_alloc_count() -> u64 {
    POOL_ALLOC_CALLS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Cumulative number of `BytesMutPool::acquire()` calls (hits + misses).
/// Test hook only.
#[doc(hidden)]
pub fn pool_acquire_count() -> u64 {
    POOL_ACQUIRE_CALLS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Per-thread BytesMut pool with bounded capacity.
///
/// `cap` — max number of buffers retained (excess on release is dropped).
/// `buf_capacity` — capacity each buffer is constructed with on first alloc.
#[derive(Clone)]
pub struct BytesMutPool {
    inner: Arc<ArrayQueue<BytesMut>>,
    buf_capacity: usize,
}

impl BytesMutPool {
    /// Construct a new pool. Decision (locked 2026-04-29): cap=256,
    /// buf_capacity=4096 → 1 MiB per IO worker maximum retained.
    pub fn new(cap: usize, buf_capacity: usize) -> Self {
        Self {
            inner: Arc::new(ArrayQueue::new(cap)),
            buf_capacity,
        }
    }

    /// Number of buffers currently retained in the pool.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether the pool is currently empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Capacity each newly-allocated buffer is constructed with.
    pub fn buf_capacity(&self) -> usize {
        self.buf_capacity
    }

    /// Acquire a cleared `BytesMut` with capacity ≥ `buf_capacity`.
    ///
    /// Pops from the queue if non-empty (already cleared on release). On
    /// queue empty, allocates a fresh `BytesMut::with_capacity(buf_capacity)`.
    pub fn acquire(&self) -> BytesMut {
        POOL_ACQUIRE_CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        match self.inner.pop() {
            Some(b) => b, // already cleared on release
            None => {
                POOL_ALLOC_CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                BytesMut::with_capacity(self.buf_capacity)
            }
        }
    }

    /// Release a `BytesMut` back to the pool.
    ///
    /// Clears the buffer; drops it if the pool is at capacity (eviction).
    /// `ArrayQueue::push` returns `Err(value)` when full → we drop.
    pub fn release(&self, mut buf: BytesMut) {
        buf.clear();
        let _ = self.inner.push(buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serializer: the POOL_ALLOC_CALLS / POOL_ACQUIRE_CALLS counters are
    /// process-wide globals; running these tests concurrently would race on
    /// the deltas. cargo test runs each module's tests in parallel by
    /// default, so we serialize here.
    static SERIALIZER: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn test_pool_acquire_returns_clear_bytes_mut_at_capacity() {
        let _g = SERIALIZER.lock().unwrap_or_else(|e| e.into_inner());
        let pool = BytesMutPool::new(256, 4096);
        let mut buf = pool.acquire();
        buf.extend_from_slice(&[1u8; 100]);
        pool.release(buf);
        let buf2 = pool.acquire();
        assert!(buf2.capacity() >= 4096);
        assert_eq!(buf2.len(), 0);
    }

    #[test]
    fn test_pool_releases_recycle_within_capacity() {
        let _g = SERIALIZER.lock().unwrap_or_else(|e| e.into_inner());
        let pool = BytesMutPool::new(256, 4096);
        let alloc_before = pool_alloc_count();
        let mut bufs = Vec::with_capacity(256);
        for _ in 0..256 {
            bufs.push(pool.acquire());
        }
        for b in bufs.drain(..) {
            pool.release(b);
        }
        let alloc_after_first_round = pool_alloc_count();
        // Second round should recycle — no new allocs.
        for _ in 0..256 {
            bufs.push(pool.acquire());
        }
        let alloc_after_second_round = pool_alloc_count();
        assert_eq!(
            alloc_after_second_round, alloc_after_first_round,
            "recycle should not allocate; got {} new allocs",
            alloc_after_second_round - alloc_after_first_round
        );
        assert!(alloc_after_first_round - alloc_before >= 256);
    }

    #[test]
    fn test_pool_evicts_excess_at_capacity() {
        let _g = SERIALIZER.lock().unwrap_or_else(|e| e.into_inner());
        let pool = BytesMutPool::new(256, 4096);
        let mut bufs = Vec::with_capacity(600);
        for _ in 0..600 {
            bufs.push(pool.acquire());
        }
        for b in bufs.drain(..) {
            pool.release(b);
        }
        assert!(
            pool.len() <= 256,
            "pool should evict to ≤ cap; got {}",
            pool.len()
        );
    }
}
