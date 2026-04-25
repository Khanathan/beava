//! I/O worker thread (Plan 18-03).
//!
//! Mirrors Redis's `IOThreadMain` from `src/networking.c`.
//! Translation table entries #5–#8 (18-rust-translation.md).
//!
//! # Protocol
//!
//! The main (apply) thread:
//!  1. Calls `IoPool::publish(work_items)` — distributes `WorkItem`s round-robin
//!     into per-slot `IoSlot::work` vecs, stores the pending count with
//!     `Release` ordering, then calls `slot.parker.unpark()`.
//!  2. Calls `IoPool::join_all()` — spins on each `slot.pending.load(Acquire) == 0`,
//!     with exponential backoff (spin → yield → park_timeout).
//!
//! Each worker thread:
//!  - Parks (or spins briefly) waiting for `pending > 0`.
//!  - Drains its `slot.work` vec via the `Mutex<Vec<WorkItem>>`.
//!  - Executes each `WorkItem` in order.
//!  - Stores `pending = 0` with `Release` to signal done.
//!
//! # Memory ordering
//!
//! The `IoSlot::pending` atomic acts as the publication barrier:
//!  - Main writes the work vec contents, then writes `pending = n` with `Release`.
//!    This makes the work vec contents visible to any thread that subsequently
//!    reads `pending` with `Acquire`.
//!  - Worker reads `pending` with `Acquire`, executes work (reading the work vec),
//!    then writes `pending = 0` with `Release`.
//!  - Main reads `pending = 0` with `Acquire`. This makes the worker's writes to
//!    per-client parse buffers visible to the main thread (sound for the
//!    ClientRef Send pattern in Task 3.2).

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::io_pool::{IoSlot, WorkItem};

/// Idle iteration thresholds for the 3-tier backoff.
const SPIN_ITERS: u64 = 1024;
const YIELD_ITERS: u64 = 65_536;

/// Worker loop — runs on a dedicated `std::thread` for the lifetime of `IoPool`.
///
/// # Safety
///
/// This function only accesses `slot.work` (behind a `Mutex`) and `slot.pending`
/// (atomic). It never touches per-client state directly — that is the caller's
/// responsibility to ensure exclusive access before publishing.
pub(crate) fn io_worker_loop(slot: Arc<IoSlot>, stop: Arc<std::sync::atomic::AtomicBool>) {
    let mut idle_count: u64 = 0;

    loop {
        // Check stop signal.
        if stop.load(Ordering::Acquire) {
            break;
        }

        // Check if there is work to do (Acquire load pairs with main's Release store).
        let pending = slot.pending.load(Ordering::Acquire);
        if pending == 0 {
            // No work — apply backoff.
            idle_count += 1;
            if idle_count < SPIN_ITERS {
                std::hint::spin_loop();
            } else if idle_count < YIELD_ITERS {
                thread::yield_now();
            } else {
                // Park for 100µs — unparked by IoPool::publish via slot.parker.unpark().
                thread::park_timeout(Duration::from_micros(100));
                idle_count = YIELD_ITERS; // stay at yield threshold after unpark
            }
            continue;
        }

        // Work is available — drain the vec and execute each item.
        idle_count = 0;

        let work: Vec<WorkItem> = {
            let mut guard = slot.work.lock().unwrap();
            std::mem::take(&mut *guard)
        };

        for item in work {
            item();
        }

        // Signal done: Release ordering makes our writes (to parse buffers etc.)
        // visible to main when it reads this with Acquire in join_all().
        slot.pending.store(0, Ordering::Release);

        // Unpark the main thread in case it is parked in join_all() waiting for us.
        slot.main_parker.unpark();
    }
}
