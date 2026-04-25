//! I/O thread pool (Plan 18-03).
//!
//! `IoPool` manages N `std::thread` workers that execute `WorkItem` closures in
//! parallel, coordinated via per-slot atomic spin-barriers.
//!
//! # Redis correspondence
//!
//! Mirrors `handleClientsWithPendingReadsUsingThreads` (Redis `networking.c`).
//! Translation table entries #5–#8.
//!
//! # Usage
//!
//! ```no_run
//! use beava_runtime_core::io_pool::IoPool;
//!
//! let pool = IoPool::new(4);
//!
//! let items: Vec<Box<dyn FnOnce() + Send + 'static>> = (0..16)
//!     .map(|i| {
//!         Box::new(move || { /* parse work for client i */ }) as Box<dyn FnOnce() + Send + 'static>
//!     })
//!     .collect();
//!
//! pool.publish(items);  // distributes round-robin + unparks workers
//! pool.join_all();       // spin-waits (with backoff) until all workers done
//! ```

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle, Thread};
use std::time::Duration;

use crate::io_thread::io_worker_loop;

/// A single work item: one closure executed by an I/O worker thread.
///
/// `Box<dyn FnOnce() + Send + 'static>` — dynamic dispatch per item is
/// acceptable at tick granularity (hundreds of ticks/sec), not per-event.
pub type WorkItem = Box<dyn FnOnce() + Send + 'static>;

/// Backoff thresholds for the join_all spin-wait (same constants as worker).
const JOIN_SPIN_ITERS: u64 = 1024;
const JOIN_YIELD_ITERS: u64 = 65_536;

/// Per-I/O-thread slot.
///
/// Shared between the main thread (producer) and the worker thread (consumer).
/// Only one producer and one consumer ever access a slot simultaneously.
pub struct IoSlot {
    /// Number of work items currently pending (or 0 if worker is done).
    ///
    /// Main writes `n > 0` with `Release` to publish work;
    /// worker writes `0` with `Release` to signal completion.
    /// Both reads use `Acquire`.
    pub pending: AtomicUsize,

    /// Queue of work items for this slot.
    ///
    /// Protected by Mutex because `publish` and the worker both need to access
    /// it. The Mutex is only contended once-per-tick (not per-event), so it is
    /// not on the hot path.
    pub work: Mutex<Vec<WorkItem>>,

    /// Handle to the worker thread, used by `publish` to `unpark` it when work
    /// arrives.
    ///
    /// Populated after spawn; `Option` because the thread handle is not
    /// available at slot creation time.
    pub worker_thread: Mutex<Option<Thread>>,

    /// Handle to the main thread, used by the worker to `unpark` it when done
    /// so `join_all` wakes faster.
    pub main_parker: Thread,
}

impl IoSlot {
    fn new(main_thread: Thread) -> Self {
        Self {
            pending: AtomicUsize::new(0),
            work: Mutex::new(Vec::new()),
            worker_thread: Mutex::new(None),
            main_parker: main_thread,
        }
    }
}

/// Pool of N I/O worker threads.
///
/// Workers are spawned at construction and run until `Drop` (or until the stop
/// flag is set). The pool is re-usable across multiple event-loop ticks.
pub struct IoPool {
    pub slots: Vec<Arc<IoSlot>>,
    handles: Vec<JoinHandle<()>>,
    stop: Arc<AtomicBool>,
}

impl IoPool {
    /// Create and start a pool of `n` I/O worker threads.
    ///
    /// If `n == 0`, creates a pool with 1 thread (use `IoConfig::is_inline()`
    /// to skip dispatch entirely instead of creating a 0-thread pool).
    pub fn new(n: usize) -> Self {
        let thread_count = n.max(1);
        let stop = Arc::new(AtomicBool::new(false));
        let main_thread = thread::current();

        let mut slots = Vec::with_capacity(thread_count);
        let mut handles = Vec::with_capacity(thread_count);

        for _ in 0..thread_count {
            let slot = Arc::new(IoSlot::new(main_thread.clone()));
            let slot_clone = Arc::clone(&slot);
            let stop_clone = Arc::clone(&stop);

            let handle = thread::Builder::new()
                .name("beava-io-worker".to_string())
                .spawn(move || {
                    io_worker_loop(slot_clone, stop_clone);
                })
                .expect("failed to spawn I/O worker thread");

            // Store the worker's thread handle so publish() can unpark it.
            *slot.worker_thread.lock().unwrap() = Some(handle.thread().clone());

            slots.push(slot);
            handles.push(handle);
        }

        Self {
            slots,
            handles,
            stop,
        }
    }

    /// Distribute `items` round-robin across slots and wake workers.
    ///
    /// Work items are assigned to slots: item `i` → slot `i % n_slots`.
    /// After filling each slot's vec, publishes via `pending.store(n, Release)`
    /// and unparks the worker thread.
    ///
    /// If `items` is empty, this is a no-op (join_all returns immediately).
    pub fn publish(&self, items: Vec<WorkItem>) {
        if items.is_empty() {
            return;
        }

        let n = self.slots.len();

        // Pre-allocate per-slot sub-vecs to avoid repeated lock + push.
        let mut buckets: Vec<Vec<WorkItem>> = (0..n).map(|_| Vec::new()).collect();
        for (idx, item) in items.into_iter().enumerate() {
            buckets[idx % n].push(item);
        }

        // Publish each non-empty bucket to its slot.
        for (slot_idx, bucket) in buckets.into_iter().enumerate() {
            if bucket.is_empty() {
                // No items for this slot this tick; leave pending = 0.
                continue;
            }
            let slot = &self.slots[slot_idx];
            let count = bucket.len();

            {
                let mut guard = slot.work.lock().unwrap();
                *guard = bucket;
            }

            // Release store: makes work vec contents visible to the worker.
            slot.pending.store(count, Ordering::Release);

            // Unpark the worker — it may be sleeping in park_timeout.
            if let Some(worker) = slot.worker_thread.lock().unwrap().as_ref() {
                worker.unpark();
            }
        }
    }

    /// Wait for all workers to finish their current work batch.
    ///
    /// Spins on `slot.pending.load(Acquire) == 0` for every slot that had
    /// work published this tick. Uses the same 3-tier backoff as the worker:
    ///  - 1024 iterations: `std::hint::spin_loop()`
    ///  - 65536 iterations: `thread::yield_now()`
    ///  - Beyond: `thread::park_timeout(50µs)` (unparked by worker when done)
    pub fn join_all(&self) {
        let mut idle: u64 = 0;

        // Collect slots that have (or had) pending work.
        // A slot with pending == 0 at the point we check was already done
        // (either it had no work or it finished before we got here).
        let active_slots: Vec<&Arc<IoSlot>> = self
            .slots
            .iter()
            .filter(|s| s.pending.load(Ordering::Acquire) > 0)
            .collect();

        if active_slots.is_empty() {
            return;
        }

        loop {
            let all_done = active_slots
                .iter()
                .all(|s| s.pending.load(Ordering::Acquire) == 0);

            if all_done {
                break;
            }

            idle += 1;
            if idle < JOIN_SPIN_ITERS {
                std::hint::spin_loop();
            } else if idle < JOIN_YIELD_ITERS {
                thread::yield_now();
            } else {
                // Park for 50µs — workers call main_parker.unpark() when done.
                thread::park_timeout(Duration::from_micros(50));
                idle = JOIN_YIELD_ITERS; // stay at yield threshold after wake
            }
        }
    }

    /// Signal all workers to stop and join their threads.
    ///
    /// Called on `Drop`. Workers will exit after completing any in-flight work.
    fn shutdown(&mut self) {
        self.stop.store(true, Ordering::Release);

        // Wake all workers so they can observe the stop flag.
        for slot in &self.slots {
            if let Some(worker) = slot.worker_thread.lock().unwrap().as_ref() {
                worker.unpark();
            }
        }

        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}

impl Drop for IoPool {
    fn drop(&mut self) {
        self.shutdown();
    }
}
