//! 3-buffer state machine for lock-free WAL append.
//!
//! # Architecture
//!
//! Each `WalBuffer` (16 MiB by default) is in exactly one state at any time:
//!
//! ```text
//! ACTIVE  → append bytes (apply thread sole writer)
//! SEALED  → handed to writer thread; apply must not touch
//! FLUSHING → writer thread is calling write(fd)+fsync(fd)
//! FREE    → safe to reuse as the next active buffer
//! ```
//!
//! `WalBufferRing` owns 3 (or N) buffers and maintains one `active_idx` atomic.
//! The sealed-buffer queue is a `Mutex<VecDeque<Arc<WalBuffer>>>` — contended
//! only between the apply thread (producer) and the writer thread (consumer);
//! this lock is NOT on the per-event append hot path.
//!
//! # Per-event append cost (hot path)
//!
//! 1. Load `active_idx` (Acquire) to find the active buffer.
//! 2. `WalBuffer::try_append`: atomic position fetch_add + memcpy.
//! 3. If `try_append` returns `None` (full): auto-seal + swap (rare path).
//!
//! No Mutex is taken on the hot path. The only synchronization is the
//! atomic `pos` CAS inside `try_append`.
//!
//! # Memory ordering (documented per operation)
//!
//! - `pos` fetch_add: `AcqRel` — ensures the bytes written before the
//!   position bump are visible to any thread reading `pos` with `Acquire`.
//! - `state`: `Release` on write, `Acquire` on read — standard publication
//!   pattern ensuring state transitions are observed in order.
//! - `active_idx`: `AcqRel` on compare_exchange (swap), `Acquire` on load.
//!
//! # Backpressure
//!
//! If all buffers are sealed (writer fell 3+ ticks behind), `append` blocks on
//! `free_condvar` until the writer thread calls `return_to_free`. This is the
//! **only** blocking case on the apply hot path and fires only under sustained
//! overload. Documented as a backpressure mechanism; surface via metrics.

use crate::wal_lsn::{Lsn, WalLsn};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};

/// Apply thread is the sole writer.
pub const BUF_STATE_ACTIVE: u8 = 0;
/// Handed off to writer thread; apply must not touch.
pub const BUF_STATE_SEALED: u8 = 1;
/// Writer thread is calling write(fd) + fsync(fd).
pub const BUF_STATE_FLUSHING: u8 = 2;
/// Safe to reuse as the next active buffer.
pub const BUF_STATE_FREE: u8 = 3;

/// A fixed-size byte buffer used for WAL records.
///
/// Single-writer (apply thread) on `ACTIVE` state; single-reader (writer
/// thread) on `SEALED`/`FLUSHING` state. The `pos` atomic is the
/// synchronization primitive separating the two roles.
pub struct WalBuffer {
    /// The backing store. `Box<[u8]>` ensures the bytes stay at a stable
    /// address; the slice length is `capacity`.
    bytes: Box<[u8]>,
    /// Capacity (bytes.len()). Stored separately to avoid an extra indirection.
    capacity: usize,
    /// Current write position (bytes used). Monotonically increasing while
    /// the buffer is ACTIVE; read-only while SEALED/FLUSHING.
    pos: AtomicUsize,
    /// Lowest LSN stored in this buffer (inclusive). Set when the buffer
    /// transitions from FREE to ACTIVE.
    lsn_lo: AtomicU64,
    /// Highest LSN stored in this buffer (inclusive). Set when the buffer
    /// transitions from ACTIVE to SEALED.
    lsn_hi: AtomicU64,
    /// Current lifecycle state (`BUF_STATE_*` constants).
    state: AtomicU8,
}

impl WalBuffer {
    /// Allocate a new buffer with `capacity` bytes in FREE state.
    pub fn new(capacity: usize) -> Self {
        let bytes = vec![0u8; capacity].into_boxed_slice();
        Self {
            bytes,
            capacity,
            pos: AtomicUsize::new(0),
            lsn_lo: AtomicU64::new(0),
            lsn_hi: AtomicU64::new(0),
            state: AtomicU8::new(BUF_STATE_FREE),
        }
    }

    /// Transition this buffer from FREE to ACTIVE and record the opening LSN.
    ///
    /// Resets `pos` to 0 so the buffer is ready to receive new records.
    pub fn activate(&self, lsn_lo: Lsn) {
        self.pos.store(0, Ordering::Release);
        self.lsn_lo.store(lsn_lo, Ordering::Release);
        self.lsn_hi.store(0, Ordering::Release);
        self.state.store(BUF_STATE_ACTIVE, Ordering::Release);
    }

    /// Try to append `data` at the current write position.
    ///
    /// Returns `Some(offset)` (the byte offset at which data was written) if
    /// `data` fits within the remaining capacity, or `None` if the buffer is
    /// full. The hot-path case; no lock is taken.
    ///
    /// # Safety note
    ///
    /// This is a **single-writer** operation. Only the apply thread may call
    /// `try_append` while the buffer is ACTIVE. Concurrent callers would
    /// corrupt the buffer. The design enforces this via `WalBufferRing::append`
    /// which is the sole entry point and is itself called from the single apply
    /// thread.
    pub fn try_append(&self, data: &[u8]) -> Option<usize> {
        let len = data.len();
        if len == 0 {
            return Some(self.pos.load(Ordering::Acquire));
        }

        // Reserve space via fetch_add. This is safe under single-writer
        // invariant: only the apply thread calls this while ACTIVE.
        let old_pos = self.pos.fetch_add(len, Ordering::AcqRel);
        let new_pos = old_pos + len;

        if new_pos > self.capacity {
            // Undo the reservation — we overflowed.
            // Under single-writer invariant this is safe (no one else is
            // concurrently fetch_add-ing).
            self.pos.store(old_pos, Ordering::Release);
            return None;
        }

        // SAFETY: we verified new_pos ≤ capacity; old_pos..new_pos is in bounds.
        let bytes_ptr = self.bytes.as_ptr() as *mut u8;
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), bytes_ptr.add(old_pos), len);
        }

        Some(old_pos)
    }

    /// Seal this buffer: transition ACTIVE → SEALED and record `lsn_hi`.
    ///
    /// After this call the apply thread must not write into this buffer.
    pub fn seal(&self, lsn_hi: Lsn) {
        self.lsn_hi.store(lsn_hi, Ordering::Release);
        self.state.store(BUF_STATE_SEALED, Ordering::Release);
    }

    /// Transition SEALED → FLUSHING (writer thread takes ownership).
    pub fn mark_flushing(&self) {
        self.state.store(BUF_STATE_FLUSHING, Ordering::Release);
    }

    /// Transition FLUSHING → FREE (writer thread returns to pool).
    pub fn mark_free(&self) {
        self.pos.store(0, Ordering::Release);
        self.state.store(BUF_STATE_FREE, Ordering::Release);
    }

    /// Current write position (bytes used). Read-only after SEALED.
    #[inline]
    pub fn pos(&self) -> usize {
        self.pos.load(Ordering::Acquire)
    }

    /// Lowest LSN stored in this buffer.
    #[inline]
    pub fn lsn_lo(&self) -> Lsn {
        self.lsn_lo.load(Ordering::Acquire)
    }

    /// Highest LSN stored in this buffer (set when sealed).
    #[inline]
    pub fn lsn_hi(&self) -> Lsn {
        self.lsn_hi.load(Ordering::Acquire)
    }

    /// Current lifecycle state.
    #[inline]
    pub fn state(&self) -> u8 {
        self.state.load(Ordering::Acquire)
    }

    /// Capacity of the buffer in bytes.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Borrow the written bytes (valid after SEALED, while writer thread owns).
    ///
    /// # Safety
    ///
    /// Caller must ensure the buffer is in SEALED or FLUSHING state and that no
    /// other thread is writing into it.
    pub fn written_bytes(&self) -> &[u8] {
        let end = self.pos.load(Ordering::Acquire);
        &self.bytes[..end]
    }

    /// Fill fraction (0.0 – 1.0). Used by the ring to decide when to auto-seal.
    #[inline]
    pub fn fill_fraction(&self) -> f32 {
        self.pos.load(Ordering::Acquire) as f32 / self.capacity as f32
    }
}

impl std::fmt::Debug for WalBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalBuffer")
            .field("capacity", &self.capacity)
            .field("pos", &self.pos())
            .field("lsn_lo", &self.lsn_lo())
            .field("lsn_hi", &self.lsn_hi())
            .field("state", &self.state())
            .finish()
    }
}

/// 3-buffer (or N-buffer) state machine for lock-free WAL append.
///
/// Owns N `Arc<WalBuffer>` slots. The `active_idx` atomic identifies which
/// buffer is currently receiving new records. Sealed buffers wait in
/// `sealed_queue` for the writer thread to consume them.
///
/// ## Thread ownership
///
/// | Thread        | Operations allowed                              |
/// |---------------|-------------------------------------------------|
/// | Apply thread  | `append`, `seal_active`, `active_pos`          |
/// | Writer thread | `pop_sealed`, `return_to_free`                  |
/// | Any thread    | `buffer_state_counts` (read-only diagnostics)  |
pub struct WalBufferRing {
    /// All buffers in the ring (fixed-size; N == `buf_count` at construction).
    buffers: Vec<Arc<WalBuffer>>,
    /// Index of the currently active buffer.
    active_idx: AtomicUsize,
    /// Queue of sealed buffers waiting for the writer thread.
    /// Mutex is NOT on the per-event hot path; it's only taken on seal/pop.
    sealed_queue: Mutex<VecDeque<Arc<WalBuffer>>>,
    /// Condvar notified when a buffer returns to FREE (backpressure release).
    free_condvar: Condvar,
    /// Mutex paired with `free_condvar`. Guards no real data.
    free_mutex: Mutex<()>,
    /// Shared LSN tracking.
    lsn: Arc<WalLsn>,
    /// Fraction of buffer capacity at which auto-seal triggers (default 0.80).
    /// Used by the writer thread's tick logic to decide when to force-seal
    /// an active buffer that is past the high-water mark.
    // reason: stored at construction for the WAL writer's tick logic; the
    // current writer path doesn't read it but the field is retained as the
    // canonical record of the configured high-water threshold.
    #[allow(dead_code)]
    seal_threshold: f32,
}

impl WalBufferRing {
    /// Create a new ring with `buf_count` buffers of `buf_bytes` each.
    ///
    /// Buffer 0 is activated immediately with `lsn_lo = 0`; the rest are FREE.
    ///
    /// # Panics
    ///
    /// Panics if `buf_count < 2` (minimum needed for ping-pong).
    pub fn new(buf_count: usize, buf_bytes: usize, lsn: Arc<WalLsn>) -> Self {
        assert!(buf_count >= 2, "WalBufferRing requires at least 2 buffers");
        let mut buffers = Vec::with_capacity(buf_count);
        for _ in 0..buf_count {
            buffers.push(Arc::new(WalBuffer::new(buf_bytes)));
        }
        // Activate buffer 0 immediately.
        buffers[0].activate(lsn.committed());

        Self {
            buffers,
            active_idx: AtomicUsize::new(0),
            sealed_queue: Mutex::new(VecDeque::new()),
            free_condvar: Condvar::new(),
            free_mutex: Mutex::new(()),
            lsn,
            seal_threshold: 0.80,
        }
    }

    /// Append `data` to the active buffer.
    ///
    /// Hot path: atomic pos-bump + memcpy. No Mutex taken unless:
    /// 1. The active buffer is full → auto-seal + find next free buffer.
    /// 2. All buffers are sealed (writer fell 3+ ticks behind) → backpressure
    ///    block on `free_condvar`.
    ///
    /// Returns the new high-water committed LSN.
    pub fn append(&self, data: &[u8]) -> Lsn {
        loop {
            let idx = self.active_idx.load(Ordering::Acquire);
            let buf = &self.buffers[idx];

            // Guard: if active_idx points to a non-ACTIVE buffer (e.g., after
            // seal_active() was called with no free buffer available), skip
            // directly to the swap path — do NOT write into a sealed buffer.
            if buf.state() != BUF_STATE_ACTIVE {
                // Wait for a free buffer and set up a new active.
                self.wait_for_free_and_activate(idx);
                continue;
            }

            match buf.try_append(data) {
                Some(_offset) => {
                    // Success: advance committed_lsn.
                    return self.lsn.record(data.len() as u64);
                }
                None => {
                    // Active buffer is full — seal it and find the next free one.
                    // This is the slow path (fires at most once per 16 MiB).
                    self.do_seal_and_swap(idx);
                    // Retry append in the new active buffer.
                }
            }
        }
    }

    /// Current byte position in the active buffer (diagnostic / test helper).
    pub fn active_pos(&self) -> usize {
        let idx = self.active_idx.load(Ordering::Acquire);
        self.buffers[idx].pos()
    }

    /// Force-seal the active buffer regardless of how full it is.
    ///
    /// Called by the writer/fsync thread on each tick (even if active buffer
    /// is not full) to ensure WAL records are flushed within `tick_ms`.
    ///
    /// Returns `Some(sealed_buf)` if the buffer had any data; `None` if it was
    /// empty (no-op seal avoided).
    pub fn seal_active(&self) -> Option<Arc<WalBuffer>> {
        let idx = self.active_idx.load(Ordering::Acquire);
        let buf = &self.buffers[idx];

        if buf.pos() == 0 {
            // Nothing to seal.
            return None;
        }

        // Find a free buffer to become the new active.
        if let Some(new_buf) = self.find_free_buffer() {
            let sealed_lsn = self.lsn.committed();
            buf.seal(sealed_lsn);

            // Enqueue the sealed buffer.
            {
                let mut q = self.sealed_queue.lock().unwrap();
                q.push_back(Arc::clone(buf));
            }

            let new_idx = self.buffer_index(new_buf);
            new_buf.activate(sealed_lsn);
            self.active_idx.store(new_idx, Ordering::Release);

            Some(Arc::clone(buf))
        } else {
            // No free buffer available right now.
            // Still seal + enqueue so the writer thread can flush it and free it.
            let sealed_lsn = self.lsn.committed();
            buf.seal(sealed_lsn);
            {
                let mut q = self.sealed_queue.lock().unwrap();
                q.push_back(Arc::clone(buf));
            }
            // active_idx now points at a sealed buffer; append() will detect
            // state != ACTIVE and call wait_for_free_and_activate before writing.
            Some(Arc::clone(buf))
        }
    }

    /// Pop the next sealed buffer from the queue, or `None` if empty.
    ///
    /// Called by the writer thread to get the next buffer to flush.
    pub fn pop_sealed(&self) -> Option<Arc<WalBuffer>> {
        let mut q = self.sealed_queue.lock().unwrap();
        let buf = q.pop_front()?;
        buf.mark_flushing();
        Some(buf)
    }

    /// Return a buffer to the free pool after the writer thread has flushed it.
    ///
    /// Transitions the buffer from FLUSHING → FREE and notifies any apply
    /// thread waiting on `free_condvar` (backpressure release).
    pub fn return_to_free(&self, buf: Arc<WalBuffer>) {
        buf.mark_free();
        // Notify apply thread (if it's blocked waiting for a free buffer).
        let _guard = self.free_mutex.lock().unwrap();
        self.free_condvar.notify_all();
    }

    /// Count buffers in each state. Returns `(active, free, sealed+flushing)`.
    ///
    /// Not atomic across all buffers — for diagnostics and tests only.
    pub fn buffer_state_counts(&self) -> (usize, usize, usize) {
        let mut active = 0usize;
        let mut free = 0usize;
        let mut sealed = 0usize;
        for buf in &self.buffers {
            match buf.state() {
                BUF_STATE_ACTIVE => active += 1,
                BUF_STATE_FREE => free += 1,
                BUF_STATE_SEALED | BUF_STATE_FLUSHING => sealed += 1,
                _ => {}
            }
        }
        (active, free, sealed)
    }

    /// Seal the current active buffer and swap in a free one. Blocks on
    /// `free_condvar` if no free buffer is available (backpressure).
    fn do_seal_and_swap(&self, current_idx: usize) {
        let buf = &self.buffers[current_idx];
        let sealed_lsn = self.lsn.committed();
        buf.seal(sealed_lsn);
        {
            let mut q = self.sealed_queue.lock().unwrap();
            q.push_back(Arc::clone(buf));
        }

        // Find or wait for a free buffer.
        let new_buf = loop {
            if let Some(b) = self.find_free_buffer() {
                break b;
            }
            // Backpressure: block until writer returns a buffer to free.
            let guard = self.free_mutex.lock().unwrap();
            // Re-check under lock to avoid TOCTOU.
            if let Some(b) = self.find_free_buffer() {
                break b;
            }
            let _guard2 = self.free_condvar.wait(guard).unwrap();
        };

        let new_idx = self.buffer_index(new_buf);
        new_buf.activate(sealed_lsn);
        self.active_idx.store(new_idx, Ordering::Release);
    }

    /// Wait for a free buffer to become available, then activate it as the
    /// new active buffer. Used when `active_idx` points to a sealed/flushing
    /// buffer (backpressure path).
    fn wait_for_free_and_activate(&self, _stale_idx: usize) {
        let current_lsn = self.lsn.committed();
        let new_buf = loop {
            if let Some(b) = self.find_free_buffer() {
                break b;
            }
            let guard = self.free_mutex.lock().unwrap();
            if let Some(b) = self.find_free_buffer() {
                break b;
            }
            let _guard2 = self.free_condvar.wait(guard).unwrap();
        };
        let new_idx = self.buffer_index(new_buf);
        new_buf.activate(current_lsn);
        self.active_idx.store(new_idx, Ordering::Release);
    }

    /// Find the first buffer in FREE state, or `None`.
    fn find_free_buffer(&self) -> Option<&WalBuffer> {
        for buf in &self.buffers {
            if buf.state() == BUF_STATE_FREE {
                return Some(buf.as_ref());
            }
        }
        None
    }

    /// Return the index of `buf` in `self.buffers`. Panics if not found.
    fn buffer_index(&self, buf: &WalBuffer) -> usize {
        for (i, b) in self.buffers.iter().enumerate() {
            if std::ptr::eq(b.as_ref(), buf) {
                return i;
            }
        }
        panic!("WalBufferRing: buffer not found in ring (BUG)");
    }
}

impl std::fmt::Debug for WalBufferRing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (active, free, sealed) = self.buffer_state_counts();
        f.debug_struct("WalBufferRing")
            .field("buf_count", &self.buffers.len())
            .field("active", &active)
            .field("free", &free)
            .field("sealed", &sealed)
            .finish()
    }
}
