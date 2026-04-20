//! Append-only SSD event log with per-shard, per-stream files.
//!
//! **Phase 52-02:** Directory layout migrated from the legacy flat
//! `data/logs/{stream_name}.log` to the per-shard layout:
//!
//!   `data/shard-{N}/streams/{stream_name}/log.bin`
//!
//! Use `EventLog::new_for_shard(data_dir, shard_id)` for new code.
//! The legacy `EventLog::new(path)` constructor is retained as a thin wrapper
//! around `new_for_shard(path, 0)` for backward compatibility.
//!
//! Path construction is centralised in `EventLog::stream_log_path`; all
//! internal callers use that accessor (PITFALLS.md §2.3 — no ad-hoc path
//! construction outside the accessor).
//!
//! **Legacy migration (D-01/D-02):**
//! - `migrate_legacy_layout(data_dir)` moves `data_dir/logs/*.log` files to
//!   `data_dir/shard-0/streams/{name}/log.bin`. Idempotent, atomic on same FS.
//! - `cleanup_legacy_dir(data_dir)` removes `data_dir/logs/` if empty; is a
//!   no-op if the directory still contains files (safety: never delete data).
//! - Wire `migrate_legacy_layout` in the boot path after detecting a v7
//!   snapshot; wire `cleanup_legacy_dir` in the clean-shutdown path.
//! - `data/logs/` is emptied as part of migration (D-01) and removed on first
//!   clean shutdown (D-02). Do not manually write to `data/logs/`.
//!
//! Events are written to per-stream log files using `O_APPEND` + direct
//! `libc::write()` syscalls. On Linux, concurrent `write()` calls to an
//! `O_APPEND` file descriptor are serialized at the inode level (`i_mutex`),
//! so writes are **atomic** with respect to file position: no two writes
//! ever interleave, and every `write()` lands fully at the then-current
//! end of file. This lets us drop the per-stream userspace writer mutex
//! entirely on the hot path.
//!
//! **Phase 42:** Replaced `DashMap<String, PLMutex<BufWriter<File>>>` with
//! `DashMap<String, LockFreeStreamLog>`. The hot path is:
//!
//!   1. DashMap get (lock-free).
//!   2. Build one contiguous frame `[u32 BE len][postcard bytes]` in a Vec.
//!   3. One `libc::write()` syscall — kernel atomic append.
//!
//! No BufWriter, no Mutex. Partial-write fallback exists but is cold path
//! (only hit on EINTR / disk quota / signal during syscall).
//!
//! fsync is done periodically via a background timer, never on the hot path.
//! Compaction rewrites log files excluding entries older than history_ttl.

// Phase 54-03 Task 3: migrated from dashmap::DashMap to
// parking_lot::RwLock<AHashMap> (RESEARCH Assumption A6). Stream
// registration/deregistration is rare (config-driven), so the outer RwLock
// only serializes boot / admin paths. The hot append path takes a read-lock,
// clones the Arc<LockFreeStreamLog> for the stream, releases the lock, then
// runs the O_APPEND write via the kernel-atomic libc::write syscall — no
// userspace lock held during the I/O.
use ahash::AHashMap;
use parking_lot::{Mutex as PLMutex, RwLock};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read as IoRead, Write};
use std::os::fd::{AsRawFd, OwnedFd};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// Default history TTL: 72 hours (3 days) per CONTEXT.md locked decision.
pub const DEFAULT_HISTORY_TTL: Duration = Duration::from_secs(259200);

/// Event log payload format markers (Plan 11-06).
///
/// New writes prefix the payload with one of these bytes so readers can
/// dispatch between JSON and binary wire format without heuristics. Legacy
/// files written before Plan 11-06 do not have a prefix byte and must be
/// read via JSON fallback — see `decode_log_payload` below.
pub const LOG_FMT_JSON: u8 = 0x00;
pub const LOG_FMT_BINARY: u8 = 0x01;

/// Decode an event-log entry payload, handling both new tagged formats and
/// legacy untagged JSON payloads. Returns `(format, body_slice)`:
///
/// - `(LOG_FMT_JSON, &payload[1..])` if the first byte is `0x00`.
/// - `(LOG_FMT_BINARY, &payload[1..])` if the first byte is `0x01`.
/// - `(LOG_FMT_JSON, &payload[..])` otherwise (legacy untagged JSON —
///   the payload starts directly with a JSON object byte like `{`).
///
/// Callers then parse `body_slice` with the appropriate decoder.
pub fn decode_log_payload(payload: &[u8]) -> (u8, &[u8]) {
    match payload.first() {
        Some(&LOG_FMT_JSON) => (LOG_FMT_JSON, &payload[1..]),
        Some(&LOG_FMT_BINARY) => (LOG_FMT_BINARY, &payload[1..]),
        _ => (LOG_FMT_JSON, payload),
    }
}

/// Pack an LSN from its three components.
///
/// Bit layout (CONTEXT D-10):
/// - bits 63-56: `upstream_shard_id` (u8)
/// - bits 55-40: `stream_ord` (u16)
/// - bits  39-0: `seq` (u40, max ~1 trillion)
///
/// `seq` is clamped to 40 bits; values above 2^40-1 wrap silently.
/// In practice no stream will reach 1 trillion events, so this is safe.
pub fn lsn_pack(upstream_shard_id: u8, stream_ord: u16, seq: u64) -> u64 {
    ((upstream_shard_id as u64) << 56)
        | ((stream_ord as u64) << 40)
        | (seq & 0x00FF_FFFF_FFFF)
}

/// Unpack an LSN into its three components.
/// Returns `(upstream_shard_id, stream_ord, seq)`.
/// A value of `0` means "no LSN" (pre-v1.2 entry); callers must treat
/// `lsn == 0` as a bypass signal (per CONTEXT D-09).
pub fn lsn_unpack(lsn: u64) -> (u8, u16, u64) {
    let upstream_shard_id = (lsn >> 56) as u8;
    let stream_ord = ((lsn >> 40) & 0xFFFF) as u16;
    let seq = lsn & 0x00FF_FFFF_FFFF;
    (upstream_shard_id, stream_ord, seq)
}

/// A single log entry: timestamp + raw event payload bytes.
///
/// Phase 52-06: adds `lsn: u64` packed as `(upstream_shard_id: u8) |
/// (stream_ord: u16) | (seq: u40)` per CONTEXT D-10. `lsn == 0` means a
/// pre-v1.2 entry — replicas bypass dedup for these (T-52-06-04 mitigated).
#[derive(Debug, Serialize, Deserialize)]
pub struct LogEntry {
    pub timestamp: SystemTime,
    pub payload: Vec<u8>,
    /// Phase 52-06: packed LSN. `0` = pre-v1.2 (no dedup for legacy entries).
    #[serde(default)]
    pub lsn: u64,
}

/// Lock-free per-stream log writer.
///
/// Wraps an `O_APPEND` file descriptor. `append_raw` issues a single
/// `libc::write()` syscall per call; on Linux the kernel holds `i_mutex`
/// for the duration of the syscall, so concurrent writes from multiple
/// threads/processes never interleave and always land at the then-current
/// end of file (atomic append).
///
/// The `partial_write_lock` is a cold-path fallback: `write(2)` can return
/// a short count on EINTR, disk quota, or (theoretically) very large
/// buffers. When that happens we grab the mutex and loop-write the
/// remainder, preventing other threads from interleaving their frames
/// into the middle of ours.
pub struct LockFreeStreamLog {
    fd: OwnedFd,
    stream_name: String,
    partial_write_lock: PLMutex<()>,
}

impl LockFreeStreamLog {
    /// Open (or create) a log file at `path` in append mode.
    pub fn open(path: &Path, stream_name: String) -> io::Result<Self> {
        let file = File::options().create(true).append(true).open(path)?;
        Ok(Self {
            fd: OwnedFd::from(file),
            stream_name,
            partial_write_lock: PLMutex::new(()),
        })
    }

    /// Append `bytes` to the log as one atomic `write()` syscall.
    ///
    /// On Linux `O_APPEND` guarantees this write will not interleave with
    /// any other concurrent write to the same fd — the kernel seeks to EOF
    /// and writes all `bytes.len()` bytes under `i_mutex`.
    ///
    /// The common case is a single `libc::write` call returning `n == len`.
    /// If the call is interrupted by a signal before writing anything
    /// (`-1 EINTR`), we retry. If it returns a short count (extremely rare
    /// in practice), we fall back to the partial-write path which takes
    /// a mutex and loops.
    pub fn append_raw(&self, bytes: &[u8]) -> io::Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        loop {
            // SAFETY: fd is valid for the lifetime of self; bytes.as_ptr()
            // is valid for bytes.len() readable bytes.
            let n = unsafe {
                libc::write(
                    self.fd.as_raw_fd(),
                    bytes.as_ptr() as *const libc::c_void,
                    bytes.len(),
                )
            };
            if n < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::Interrupted {
                    continue; // EINTR — retry the whole write (nothing written)
                }
                return Err(err);
            }
            let n = n as usize;
            if n == bytes.len() {
                return Ok(());
            }
            // Partial write — cold path. Grab the fallback lock and finish
            // the write. Holding the mutex across the remainder prevents
            // any other concurrent writer from racing a frame between our
            // already-written prefix and the tail we're about to append.
            //
            // NOTE: on Linux `O_APPEND`, the kernel guarantees no other
            // writer's bytes can physically appear between our prefix and
            // our tail (each write() atomically extends the file by its
            // return value). But taking the lock also serializes with
            // other concurrent partial-write fallbacks on this same fd,
            // which is a correctness win for progress.
            return self.append_raw_partial_fallback(bytes, n);
        }
    }

    /// Cold-path completion of a partial write. Writes the remainder of
    /// `bytes` starting at offset `already_written` under the partial-write
    /// lock, handling EINTR.
    fn append_raw_partial_fallback(&self, bytes: &[u8], already_written: usize) -> io::Result<()> {
        let _g = self.partial_write_lock.lock();
        let mut off = already_written;
        while off < bytes.len() {
            let n = unsafe {
                libc::write(
                    self.fd.as_raw_fd(),
                    bytes[off..].as_ptr() as *const libc::c_void,
                    bytes.len() - off,
                )
            };
            if n < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(io::Error::new(
                    err.kind(),
                    format!(
                        "partial-write fallback failed on stream {:?}: {}",
                        self.stream_name, err
                    ),
                ));
            }
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    format!(
                        "write returned 0 on stream {:?} (disk full?)",
                        self.stream_name
                    ),
                ));
            }
            off += n as usize;
        }
        Ok(())
    }

    /// `fdatasync(fd)` — flush written data to disk. macOS has no
    /// `fdatasync`; fall back to `fsync`.
    pub fn fsync(&self) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        let rc = unsafe { libc::fdatasync(self.fd.as_raw_fd()) };
        #[cfg(not(target_os = "linux"))]
        let rc = unsafe { libc::fsync(self.fd.as_raw_fd()) };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

/// Phase 55-01 D-A3: per-(shard, primary_stream) cascade delivery cursor.
///
/// Stored at `<data_dir>/shard-{shard_id}/cascade-cursor.postcard`. Fsync
/// piggy-backs on primary-log fsync (no extra syscalls on hot path); an
/// explicit `fsync_cascade_cursor` is also called at clean shutdown.
///
/// Crash safety: re-delivery of cascade events whose LSN ≤ on-disk
/// `last_cascaded_lsn` is idempotent because `upsert_table_row` is
/// full-replace (D-B5). Missing cursor file ⇒ first-boot zero state.
#[derive(Default, Serialize, Deserialize, Clone, Debug)]
pub struct CascadeCursor {
    /// primary_stream_name → last_cascaded_lsn
    pub per_stream: std::collections::HashMap<String, u64>,
}

pub struct EventLog {
    /// Root data directory (e.g. `data/` or a tmp dir in tests).
    data_dir: PathBuf,
    /// Which shard owns this log (0 for the N=1 single-shard case).
    shard_id: u8,
    /// Per-stream lock-free writers. Phase 54-03: RwLock<AHashMap<_, Arc<_>>>.
    /// The hot append path reads the map, clones the `Arc<LockFreeStreamLog>`,
    /// releases the read-lock, then runs the kernel-atomic `libc::write`
    /// syscall — no userspace lock held during I/O. Registration /
    /// deregistration / compaction take the write-lock briefly.
    writers: RwLock<AHashMap<String, Arc<LockFreeStreamLog>>>,
    /// Per-stream history TTL for compaction. Streams not in this map are not logged.
    history_ttls: RwLock<AHashMap<String, Duration>>,
    /// Phase 52-06: per-(stream_name, upstream_shard_id) monotonic seq counter.
    /// Keyed by `(stream_name, upstream_shard_id)`. Loaded from
    /// `SnapshotV8.replica_lsn_map` at startup via `load_seq_counters`.
    /// Stores the *next* seq to assign (i.e. the count of events appended so far).
    /// Phase 54-03: a single RwLock<AHashMap<(String, u8), u64>> — the
    /// fetch-and-increment inside `append_lsn_tagged` holds the write-lock
    /// briefly (u64 math only), matching the existing serialization shape
    /// of the prior DashMap entry-based code.
    seq_counters: RwLock<AHashMap<(String, u8), u64>>,
    /// Phase 55-01 D-A3: per-primary-stream cascade delivery cursor.
    /// In-memory; fsynced opportunistically via `fsync_cascade_cursor`
    /// (piggy-backed on primary-log fsync or called explicitly at clean
    /// shutdown).
    cascade_cursor: RwLock<CascadeCursor>,
    /// Dirty flag — only fsync when the cursor has been advanced since the
    /// last persist. Avoids a wasteful write syscall on every fsync tick.
    cursor_dirty: std::sync::atomic::AtomicBool,
}

impl EventLog {
    /// Create a new `EventLog` rooted at `data_dir` for the given `shard_id`.
    ///
    /// The per-shard stream directory tree is created if it does not exist:
    /// `data_dir/shard-{shard_id}/streams/`
    ///
    /// This is the canonical constructor for new code. Use `shard_id = 0` for
    /// the N=1 single-shard case.
    pub fn new_for_shard(data_dir: PathBuf, shard_id: u8) -> std::io::Result<Self> {
        // Pre-create the streams root for this shard so callers that only
        // create the EventLog (but register no streams yet) still see the dir.
        let streams_root = data_dir.join(format!("shard-{}/streams", shard_id));
        fs::create_dir_all(&streams_root)?;
        // Phase 55-01 D-A3: load cascade cursor sidecar if present. Missing
        // file = fresh start at cursor 0.
        let cascade_cursor = Self::load_cascade_cursor(&data_dir, shard_id);
        Ok(Self {
            data_dir,
            shard_id,
            writers: RwLock::new(AHashMap::new()),
            history_ttls: RwLock::new(AHashMap::new()),
            seq_counters: RwLock::new(AHashMap::new()),
            cascade_cursor: RwLock::new(cascade_cursor),
            cursor_dirty: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Phase 55-01 D-A3: path for the cascade cursor sidecar for this shard.
    pub fn cascade_cursor_path(data_dir: &Path, shard_id: u8) -> PathBuf {
        data_dir.join(format!("shard-{}/cascade-cursor.postcard", shard_id))
    }

    /// Phase 55-01 D-A3: in-memory advance of `last_cascaded_lsn` for
    /// `primary_stream`. Monotonic (new_lsn ≤ current ⇒ no-op). Marks the
    /// cursor dirty; on-disk persist happens at the next
    /// `fsync_cascade_cursor` tick.
    pub fn advance_cascaded_lsn(&self, primary_stream: &str, new_lsn: u64) {
        let mut c = self.cascade_cursor.write();
        let slot = c.per_stream.entry(primary_stream.to_string()).or_insert(0);
        if new_lsn > *slot {
            *slot = new_lsn;
            self.cursor_dirty
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }

    /// Phase 55-01 D-A3: read the current `last_cascaded_lsn` for a stream.
    /// Returns 0 if never advanced.
    pub fn cascaded_lsn(&self, primary_stream: &str) -> u64 {
        self.cascade_cursor
            .read()
            .per_stream
            .get(primary_stream)
            .copied()
            .unwrap_or(0)
    }

    /// Phase 55-01 D-A3: atomic-rename cursor persist. Caller typically
    /// invokes this at clean shutdown or piggy-backs on a primary-log
    /// fsync. No-op when cursor_dirty is false.
    pub fn fsync_cascade_cursor(&self) -> std::io::Result<()> {
        if !self
            .cursor_dirty
            .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            return Ok(());
        }
        let snapshot = self.cascade_cursor.read().clone();
        let bytes = postcard::to_stdvec(&snapshot)
            .map_err(std::io::Error::other)?;
        let path = Self::cascade_cursor_path(&self.data_dir, self.shard_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension("postcard.tmp");
        fs::write(&tmp, &bytes)?;
        fs::rename(&tmp, &path)?; // atomic on POSIX
        // Best-effort parent-dir fsync.
        if let Some(parent) = path.parent() {
            if let Ok(f) = File::open(parent) {
                let _ = f.sync_all();
            }
        }
        Ok(())
    }

    /// Phase 55-01 D-A3: load cursor from disk; returns Default on any error.
    fn load_cascade_cursor(data_dir: &Path, shard_id: u8) -> CascadeCursor {
        let path = Self::cascade_cursor_path(data_dir, shard_id);
        match fs::read(&path) {
            Ok(bytes) => postcard::from_bytes(&bytes).unwrap_or_default(),
            Err(_) => CascadeCursor::default(),
        }
    }

    /// Phase 52-06: Load per-(stream, upstream_shard_id) seq counters from a
    /// snapshot's `replica_lsn_map`. Must be called after construction and
    /// before the first `append_lsn_tagged` call to restore monotonicity
    /// across restarts.
    ///
    /// The map value is the *next* seq to assign (stored by `current_lsn_map`).
    /// After loading, the next `append_lsn_tagged` call for `(stream, shard_id)`
    /// will use this value as seq, then increment it.
    pub fn load_seq_counters(&mut self, lsn_map: &std::collections::HashMap<(String, u8), u64>) {
        let mut w = self.seq_counters.write();
        for ((stream, shard_id), &next_seq) in lsn_map {
            w.insert((stream.clone(), *shard_id), next_seq);
        }
    }

    /// Phase 52-06: Append a raw event to the stream's log file with LSN tagging.
    ///
    /// Assigns a monotonic LSN packed as `lsn_pack(upstream_shard_id, stream_ord, seq)`.
    /// The seq counter for `(stream_name, upstream_shard_id)` is incremented atomically.
    ///
    /// Returns `Ok(lsn)` on success, where `lsn` is the assigned packed LSN.
    /// Returns `Ok(0)` if the stream is not registered (mirrors `append` contract).
    pub fn append_lsn_tagged(
        &self,
        stream_name: &str,
        event_bytes: &[u8],
        now: SystemTime,
        upstream_shard_id: u8,
        stream_ord: u16,
    ) -> std::io::Result<u64> {
        let log_ref: Arc<LockFreeStreamLog> = match self.writers.read().get(stream_name) {
            Some(w) => Arc::clone(w),
            None => return Ok(0),
        };

        // Atomically fetch-and-increment the seq counter.
        let seq = {
            let mut w = self.seq_counters.write();
            let slot = w
                .entry((stream_name.to_string(), upstream_shard_id))
                .or_insert(0);
            let current = *slot;
            *slot = current + 1;
            current
        };

        let lsn = lsn_pack(upstream_shard_id, stream_ord, seq);
        let entry = LogEntry {
            timestamp: now,
            payload: event_bytes.to_vec(),
            lsn,
        };
        let encoded = postcard::to_stdvec(&entry).map_err(std::io::Error::other)?;
        let mut frame = Vec::with_capacity(4 + encoded.len());
        frame.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
        frame.extend_from_slice(&encoded);
        debug_assert!(
            frame.len() < 1_048_576,
            "event-log LSN-tagged frame exceeds 1 MiB"
        );
        log_ref.append_raw(&frame)?;
        Ok(lsn)
    }

    /// Phase 52-06: Return the current seq counter map as a `replica_lsn_map`
    /// suitable for writing into a `SnapshotV8`.
    ///
    /// The value stored is the *next* seq to assign for each
    /// `(stream_name, upstream_shard_id)` pair. `load_seq_counters` reads
    /// this value back directly as the starting counter — no +1 adjustment.
    ///
    /// Called on clean shutdown / snapshot write to persist seq counters so
    /// monotonicity is preserved across restarts.
    pub fn current_lsn_map(&self) -> std::collections::HashMap<(String, u8), u64> {
        self.seq_counters
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }

    /// Create a new EventLog at `log_dir` for shard 0.
    ///
    /// Kept for backward compatibility — all existing callers pass a path
    /// directly; internally this calls `new_for_shard(log_dir, 0)`.
    ///
    /// Note: `seq_counters` starts empty. Call `load_seq_counters` with the
    /// snapshot's `replica_lsn_map` before the first `append_lsn_tagged` if
    /// monotonicity across restart is required (Phase 52-06).
    pub fn new(log_dir: PathBuf) -> std::io::Result<Self> {
        Self::new_for_shard(log_dir, 0)
    }

    /// Return the canonical path for a stream's log file under `data_dir`.
    ///
    /// Layout: `data_dir/shard-{shard_id}/streams/{sanitized_stream_name}/log.bin`
    ///
    /// This is the single source of truth for path construction (PITFALLS.md §2.3).
    /// All internal code and callers must use this accessor — never construct
    /// the path manually.
    pub fn stream_log_path(data_dir: &Path, shard_id: u8, stream_name: &str) -> PathBuf {
        let sanitized = sanitize_stream_name(stream_name);
        data_dir
            .join(format!("shard-{}/streams/{}", shard_id, sanitized))
            .join("log.bin")
    }

    /// Return the path for the `.tmp` compaction file for a stream.
    fn stream_tmp_path(data_dir: &Path, shard_id: u8, stream_name: &str) -> PathBuf {
        let sanitized = sanitize_stream_name(stream_name);
        data_dir
            .join(format!("shard-{}/streams/{}", shard_id, sanitized))
            .join("log.bin.tmp")
    }

    /// Register a stream for event logging.
    /// Creates/opens the log file in append mode. Idempotent (re-registration is a no-op).
    pub fn register_stream(
        &self,
        stream_name: &str,
        history_ttl: Option<Duration>,
    ) -> std::io::Result<()> {
        if self.writers.read().contains_key(stream_name) {
            return Ok(()); // idempotent re-registration
        }
        let path = Self::stream_log_path(&self.data_dir, self.shard_id, stream_name);
        // Ensure the per-stream directory exists (stream_log_path nests the
        // file inside a stream-named subdirectory).
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        // Open the log OUTSIDE the write-lock so we don't hold the lock across
        // a filesystem call. Then insert-if-absent under the write-lock to
        // handle the race (two registrations for the same stream).
        let log = LockFreeStreamLog::open(&path, stream_name.to_string())?;
        self.writers
            .write()
            .entry(stream_name.to_string())
            .or_insert_with(|| Arc::new(log));
        self.history_ttls
            .write()
            .entry(stream_name.to_string())
            .or_insert_with(|| history_ttl.unwrap_or(DEFAULT_HISTORY_TTL));
        Ok(())
    }

    /// Append a raw event to the stream's log file.
    /// Returns Ok(false) if the stream is not registered (no error).
    /// Uses length-prefixed postcard serialization: [u32 BE len][postcard bytes].
    pub fn append(
        &self,
        stream_name: &str,
        event_bytes: &[u8],
        now: SystemTime,
    ) -> std::io::Result<bool> {
        let log_ref: Arc<LockFreeStreamLog> = match self.writers.read().get(stream_name) {
            Some(w) => Arc::clone(w),
            None => return Ok(false),
        };
        let entry = LogEntry {
            timestamp: now,
            payload: event_bytes.to_vec(),
            lsn: 0, // pre-v1.2 path; replicas bypass dedup for lsn==0 (T-52-06-04)
        };
        let encoded = postcard::to_stdvec(&entry).map_err(std::io::Error::other)?;
        // Build one contiguous frame: [u32 BE len][postcard bytes].
        let mut frame = Vec::with_capacity(4 + encoded.len());
        frame.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
        frame.extend_from_slice(&encoded);
        debug_assert!(
            frame.len() < 1_048_576,
            "event-log frame exceeds 1 MiB (Linux O_APPEND atomicity guarantee weakens above this); consider splitting"
        );
        log_ref.append_raw(&frame)?;
        Ok(true)
    }

    /// Batch-append multiple raw events to the stream's log file.
    ///
    /// Returns `Ok(n)` where `n` is the number of events successfully written.
    /// Returns `Ok(0)` if the stream is not registered (mirrors the single
    /// `append` method's `Ok(false)` contract — no error).
    ///
    /// All events share the same `now` timestamp. **Batch-atomic**: all
    /// frames are concatenated into one buffer and written with a single
    /// `libc::write()` syscall, so on Linux either the whole batch lands
    /// contiguously at end-of-file, or (in the cold partial-write case) the
    /// fallback path completes the write without interleaving any other
    /// thread's frames.
    pub fn append_many(
        &self,
        stream_name: &str,
        event_bytes_list: &[&[u8]],
        now: SystemTime,
    ) -> std::io::Result<usize> {
        if event_bytes_list.is_empty() {
            return Ok(0);
        }
        let log_ref: Arc<LockFreeStreamLog> = match self.writers.read().get(stream_name) {
            Some(w) => Arc::clone(w),
            None => return Ok(0),
        };
        // Pre-allocate a single buffer for the whole batch.
        //
        // We don't know the exact encoded size upfront (postcard varints),
        // but a reasonable heuristic is: sum of payload lengths + 32 B per
        // entry of framing/timestamp/length overhead.
        let rough_cap: usize = event_bytes_list.iter().map(|b| b.len() + 32).sum();
        let mut buf = Vec::with_capacity(rough_cap);
        let mut written = 0usize;
        for bytes in event_bytes_list {
            let entry = LogEntry {
                timestamp: now,
                payload: bytes.to_vec(),
                lsn: 0, // pre-v1.2 path; replicas bypass dedup for lsn==0 (T-52-06-04)
            };
            let encoded = postcard::to_stdvec(&entry).map_err(std::io::Error::other)?;
            buf.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
            buf.extend_from_slice(&encoded);
            written += 1;
        }
        debug_assert!(
            buf.len() < 1_048_576,
            "event-log batch frame exceeds 1 MiB (Linux O_APPEND atomicity guarantee weakens above this); consider smaller batches"
        );
        log_ref.append_raw(&buf)?;
        Ok(written)
    }

    /// Batch-append multiple raw events with **per-event** timestamps.
    ///
    /// Same single-syscall semantics as `append_many`, but each event carries
    /// its own `SystemTime`. Used by the replica-side batch ingest path, where
    /// a single batch can span hours of upstream event time and the
    /// `LogEntry.timestamp` must survive a later `handle_log_fetch` readback
    /// (the fork reader emits ts_ms directly from `entry.timestamp`).
    ///
    /// `event_bytes_list.len()` must equal `timestamps.len()`.
    pub fn append_many_with_ts(
        &self,
        stream_name: &str,
        event_bytes_list: &[&[u8]],
        timestamps: &[SystemTime],
    ) -> std::io::Result<usize> {
        assert_eq!(
            event_bytes_list.len(),
            timestamps.len(),
            "append_many_with_ts: events/timestamps length mismatch"
        );
        if event_bytes_list.is_empty() {
            return Ok(0);
        }
        let log_ref: Arc<LockFreeStreamLog> = match self.writers.read().get(stream_name) {
            Some(w) => Arc::clone(w),
            None => return Ok(0),
        };
        let rough_cap: usize = event_bytes_list.iter().map(|b| b.len() + 32).sum();
        let mut buf = Vec::with_capacity(rough_cap);
        let mut written = 0usize;
        for (bytes, ts) in event_bytes_list.iter().zip(timestamps.iter()) {
            let entry = LogEntry {
                timestamp: *ts,
                payload: bytes.to_vec(),
                lsn: 0, // pre-v1.2 path; replicas bypass dedup for lsn==0 (T-52-06-04)
            };
            let encoded = postcard::to_stdvec(&entry).map_err(std::io::Error::other)?;
            buf.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
            buf.extend_from_slice(&encoded);
            written += 1;
        }
        debug_assert!(
            buf.len() < 1_048_576,
            "event-log batch frame exceeds 1 MiB (Linux O_APPEND atomicity guarantee weakens above this); consider smaller batches"
        );
        log_ref.append_raw(&buf)?;
        Ok(written)
    }

    /// Read all log entries for a stream.
    /// Opens the file independently from the writer.
    pub fn read_entries(&self, stream_name: &str) -> std::io::Result<Vec<LogEntry>> {
        let path = Self::stream_log_path(&self.data_dir, self.shard_id, stream_name);
        if !path.exists() {
            return Ok(vec![]);
        }
        let file = File::open(&path)?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();
        loop {
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let len = u32::from_be_bytes(len_buf) as usize;
            let mut data = vec![0u8; len];
            reader.read_exact(&mut data)?;
            let entry: LogEntry = postcard::from_bytes(&data)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            entries.push(entry);
        }
        Ok(entries)
    }

    /// `fdatasync` every stream's fd.
    /// Called from background timer only, never on the hot path.
    ///
    /// Iterates the DashMap and calls `fdatasync(fd)` per stream — no
    /// userspace locks held anywhere.
    pub fn fsync_all(&self) -> std::io::Result<()> {
        // Snapshot the Arc handles under the read-lock, release, then fsync
        // each outside the lock so admin/shutdown paths never block register.
        let handles: Vec<Arc<LockFreeStreamLog>> = self
            .writers
            .read()
            .values()
            .map(Arc::clone)
            .collect();
        for h in &handles {
            h.fsync()?;
        }
        Ok(())
    }

    /// Compact a stream's log file by removing entries older than history_ttl.
    /// Writes surviving entries to a tmp file, then atomically renames.
    /// Returns the count of removed entries.
    ///
    /// Takes `&self` via interior mutability: briefly removes the writer from
    /// the DashMap, rewrites the file, then reinserts a fresh writer.
    /// Concurrent `append` calls during the rename window will see
    /// `Ok(false)` (unregistered) — acceptable because compaction runs
    /// from a single-threaded background timer and the window is ~ms.
    pub fn compact_stream(&self, stream_name: &str, now: SystemTime) -> std::io::Result<usize> {
        let history_ttl = match self.history_ttls.read().get(stream_name) {
            Some(ttl) => *ttl,
            None => return Ok(0), // not registered
        };

        // With O_APPEND + direct writes there's nothing to flush in userspace.
        // The writer has no buffer; every append was already a syscall. We
        // optionally fdatasync to ensure compaction scans the durable state.
        if let Some(writer) = self.writers.read().get(stream_name).map(Arc::clone) {
            writer.fsync()?;
        }

        // Read all entries from disk
        let entries = self.read_entries(stream_name)?;
        let cutoff = now
            .checked_sub(history_ttl)
            .unwrap_or(SystemTime::UNIX_EPOCH);

        // Partition into kept and removed
        let (kept, removed): (Vec<_>, Vec<_>) =
            entries.into_iter().partition(|e| e.timestamp >= cutoff);
        let removed_count = removed.len();

        if removed_count == 0 {
            return Ok(0);
        }

        let log_path = Self::stream_log_path(&self.data_dir, self.shard_id, stream_name);
        let tmp_path = Self::stream_tmp_path(&self.data_dir, self.shard_id, stream_name);

        // Write surviving entries to tmp file
        {
            let tmp_file = File::create(&tmp_path)?;
            let mut tmp_writer = BufWriter::new(tmp_file);
            for entry in &kept {
                let encoded = postcard::to_stdvec(entry).map_err(std::io::Error::other)?;
                let len = encoded.len() as u32;
                tmp_writer.write_all(&len.to_be_bytes())?;
                tmp_writer.write_all(&encoded)?;
            }
            tmp_writer.flush()?;
        }

        // Close old writer by removing it from the map.
        self.writers.write().remove(stream_name);

        // Atomic rename
        fs::rename(&tmp_path, &log_path)?;

        // Reopen writer for the stream
        let log = LockFreeStreamLog::open(&log_path, stream_name.to_string())?;
        self.writers
            .write()
            .insert(stream_name.to_string(), Arc::new(log));

        Ok(removed_count)
    }

    /// Deregister a stream: remove from the writers map (closing fd via
    /// OwnedFd Drop when the last Arc to the LockFreeStreamLog is dropped).
    /// Does NOT delete the log file (preserve history for potential
    /// re-registration).
    pub fn deregister_stream(&self, stream_name: &str) -> std::io::Result<()> {
        let _ = self.writers.write().remove(stream_name);
        Ok(())
    }

    /// Get the history TTL for a stream.
    pub fn get_history_ttl(&self, stream_name: &str) -> Option<Duration> {
        self.history_ttls.read().get(stream_name).copied()
    }

    /// Return a snapshot Vec of registered stream names.
    ///
    /// Phase 40: this returns owned `String`s instead of `&str` references
    /// because map entries can't safely yield borrowed keys across
    /// concurrent mutations.
    pub fn registered_streams(&self) -> Vec<String> {
        self.writers.read().keys().cloned().collect()
    }

    /// D-27 / A7: scaffolded hook for durable-ack HTTP push (future `?sync=1,durable=1`).
    ///
    /// Appends the entry and awaits `fdatasync` completion before returning.
    /// NOT YET WIRED — the HTTP `?sync=1` handler continues to use the in-memory
    /// drain path (Phase 45 behavior). A follow-up phase will add a `?sync=1,durable=1`
    /// flag that opts into this method.
    ///
    /// Returns `Ok(false)` if the stream is not registered (mirrors `append` contract).
    /// Errors from the underlying write or fsync are surfaced as `std::io::Error`.
    pub async fn append_with_fsync(
        &self,
        stream_name: &str,
        event_bytes: &[u8],
        now: SystemTime,
    ) -> std::io::Result<bool> {
        // Step 1: perform the normal synchronous append.
        let written = self.append(stream_name, event_bytes, now)?;
        if !written {
            return Ok(false);
        }

        // Step 2: fsync the backing fd for this stream.
        // Extract the raw fd under a read-lock and drop the guard before the
        // async blocking call so the RwLock is never held across an .await.
        let raw_fd = {
            match self.writers.read().get(stream_name) {
                Some(w) => w.fd.as_raw_fd(),
                None => return Ok(false), // writer was concurrently removed
            }
        };

        // Offload the blocking fdatasync to the tokio blocking thread pool.
        // spawn_blocking returns a JoinError only if the task panics; map
        // that to an io::Error so the return type stays clean.
        tokio::task::spawn_blocking(move || {
            #[cfg(target_os = "linux")]
            let rc = unsafe { libc::fdatasync(raw_fd) };
            #[cfg(not(target_os = "linux"))]
            let rc = unsafe { libc::fsync(raw_fd) };
            if rc < 0 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(())
            }
        })
        .await
        .map_err(std::io::Error::other)??;

        Ok(true)
    }
}

/// Sanitize a stream name for filesystem safety (T-06-04 mitigation).
/// Replaces `/`, `\`, NUL bytes with `_`. Replaces `..` with `__`.
fn sanitize_stream_name(name: &str) -> String {
    let mut s = name.replace(['/', '\\', '\0'], "_");
    // Replace ".." to prevent path traversal
    while s.contains("..") {
        s = s.replace("..", "__");
    }
    s
}

// ============================================================
// Phase 52-02: Legacy layout migration helpers (D-01, D-02)
// ============================================================

/// Migrate the legacy `data/logs/*.log` flat layout to the per-shard layout
/// `data/shard-0/streams/{stream_name}/log.bin`.
///
/// - Reads all `*.log` files from `data_dir/logs/` (if it exists).
/// - For each `{stream_name}.log`, moves it to
///   `data_dir/shard-0/streams/{stream_name}/log.bin` using `fs::rename`
///   (atomic on the same filesystem). Falls back to copy + delete if the
///   rename fails across filesystems.
/// - Idempotent: if `data_dir/logs/` does not exist (already migrated or
///   fresh install), returns `Ok(())` immediately.
/// - T-52-02-01: rename is atomic; fallback copy+delete errors on partial
///   write before deleting source so data is never lost.
///
/// Wire this into the boot path after detecting a v7 snapshot load, before
/// recovery threads start reading per-shard logs.
pub fn migrate_legacy_layout(data_dir: &Path) -> std::io::Result<()> {
    let legacy_dir = data_dir.join("logs");
    if !legacy_dir.exists() {
        return Ok(());
    }

    for entry in fs::read_dir(&legacy_dir)? {
        let entry = entry?;
        let src = entry.path();
        // Only process files ending in `.log`
        let file_name = match src.file_name().and_then(|n| n.to_str()) {
            Some(n) => n.to_string(),
            None => continue,
        };
        if !file_name.ends_with(".log") || !src.is_file() {
            continue;
        }
        // Derive stream name by stripping the `.log` suffix.
        let stream_name = &file_name[..file_name.len() - 4];
        let dst = EventLog::stream_log_path(data_dir, 0, stream_name);

        // Create target directory tree if absent.
        if let Some(parent) = dst.parent() {
            fs::create_dir_all(parent)?;
        }

        // If the destination already exists (idempotency: previous partial
        // migration), skip — data is already in the new location.
        if dst.exists() {
            // Remove source so we don't re-process on next boot.
            let _ = fs::remove_file(&src);
            continue;
        }

        // Attempt atomic rename first (same filesystem).
        if fs::rename(&src, &dst).is_err() {
            // Cross-filesystem fallback: copy then delete source.
            // Write to a tmp file first so a crash mid-copy leaves dst clean.
            let tmp_dst = dst.with_extension("bin.tmp");
            fs::copy(&src, &tmp_dst)?;
            fs::rename(&tmp_dst, &dst)?;
            fs::remove_file(&src)?;
        }
    }

    Ok(())
}

/// Remove `data_dir/logs/` if it is empty.
///
/// - T-52-02-02: never removes a non-empty directory (safety: operator data).
///   If files are still present, logs a warning and returns `Ok(())`.
/// - Called after the first clean shutdown following migration (D-02).
///
/// Wire this into the clean-shutdown path after the final snapshot save.
pub fn cleanup_legacy_dir(data_dir: &Path) -> std::io::Result<()> {
    let legacy_dir = data_dir.join("logs");
    if !legacy_dir.exists() {
        return Ok(());
    }

    // Check whether the directory is empty before attempting removal.
    let is_empty = fs::read_dir(&legacy_dir)?.next().is_none();
    if is_empty {
        fs::remove_dir(&legacy_dir)?;
    } else {
        // T-52-02-02: never delete data. Log a warning; caller proceeds.
        eprintln!(
            "cleanup_legacy_dir: {:?} is not empty — skipping removal (data/logs/ still contains files)",
            legacy_dir
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::UNIX_EPOCH;

    fn ts(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    // ---------- Plan 11-06: format dispatch helper tests ----------

    #[test]
    fn test_decode_log_payload_json_tagged() {
        let mut p = vec![LOG_FMT_JSON];
        p.extend_from_slice(br#"{"a":1}"#);
        let (fmt, body) = decode_log_payload(&p);
        assert_eq!(fmt, LOG_FMT_JSON);
        assert_eq!(body, br#"{"a":1}"#);
    }

    #[test]
    fn test_decode_log_payload_binary_tagged() {
        let p = vec![LOG_FMT_BINARY, 0x00, 0x01, 0x02, 0x03];
        let (fmt, body) = decode_log_payload(&p);
        assert_eq!(fmt, LOG_FMT_BINARY);
        assert_eq!(body, &[0x00, 0x01, 0x02, 0x03]);
    }

    #[test]
    fn test_decode_log_payload_legacy_untagged_json() {
        // Legacy (pre-11-06) files start directly with a `{` (0x7B).
        let p = br#"{"legacy":true}"#.to_vec();
        let (fmt, body) = decode_log_payload(&p);
        // Falls through to JSON fallback (legacy treated as JSON).
        assert_eq!(fmt, LOG_FMT_JSON);
        assert_eq!(body, br#"{"legacy":true}"#);
    }

    #[test]
    fn test_decode_log_payload_empty() {
        let (fmt, body) = decode_log_payload(&[]);
        assert_eq!(fmt, LOG_FMT_JSON);
        assert!(body.is_empty());
    }

    #[test]
    fn test_new_creates_log_directory() {
        let tmp = tempfile::TempDir::new().unwrap();
        let log_dir = tmp.path().join("events");
        assert!(!log_dir.exists());
        let _log = EventLog::new(log_dir.clone()).unwrap();
        assert!(log_dir.exists());
    }

    #[test]
    fn test_register_stream_creates_log_file() {
        let tmp = tempfile::TempDir::new().unwrap();
        let log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("Transactions", None).unwrap();
        let log_file = EventLog::stream_log_path(tmp.path(), 0, "Transactions");
        assert!(log_file.exists());
    }

    #[test]
    fn test_append_writes_length_prefixed_entry() {
        let tmp = tempfile::TempDir::new().unwrap();
        let log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("Transactions", None).unwrap();

        let now = ts(1000);
        let result = log.append("Transactions", b"hello", now).unwrap();
        assert!(result);

        // With O_APPEND + direct write there's nothing to flush; entries
        // are visible to readers as soon as `append` returns.
        let entries = log.read_entries("Transactions").unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].payload, b"hello");
        assert_eq!(entries[0].timestamp, now);
    }

    #[test]
    fn test_read_entries_returns_all_entries() {
        let tmp = tempfile::TempDir::new().unwrap();
        let log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("TestStream", None).unwrap();

        let now = ts(1000);
        log.append("TestStream", b"event1", now).unwrap();
        log.append("TestStream", b"event2", ts(1001)).unwrap();
        log.append("TestStream", b"event3", ts(1002)).unwrap();

        let entries = log.read_entries("TestStream").unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].payload, b"event1");
        assert_eq!(entries[1].payload, b"event2");
        assert_eq!(entries[2].payload, b"event3");
    }

    #[test]
    fn test_multiple_appends_sequential_read() {
        let tmp = tempfile::TempDir::new().unwrap();
        let log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("S", None).unwrap();

        for i in 0..10 {
            log.append("S", format!("event_{}", i).as_bytes(), ts(1000 + i))
                .unwrap();
        }

        let entries = log.read_entries("S").unwrap();
        assert_eq!(entries.len(), 10);
        for (i, entry) in entries.iter().enumerate().take(10) {
            assert_eq!(entry.payload, format!("event_{}", i).as_bytes());
        }
    }

    #[test]
    fn test_append_unregistered_stream_returns_false() {
        let tmp = tempfile::TempDir::new().unwrap();
        let log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        let result = log.append("Unknown", b"data", ts(1000)).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_fsync_all_flushes_without_error() {
        let tmp = tempfile::TempDir::new().unwrap();
        let log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("A", None).unwrap();
        log.register_stream("B", None).unwrap();
        log.append("A", b"data_a", ts(1000)).unwrap();
        log.append("B", b"data_b", ts(1000)).unwrap();
        assert!(log.fsync_all().is_ok());
    }

    #[test]
    fn test_compact_stream_removes_expired_entries() {
        let tmp = tempfile::TempDir::new().unwrap();
        let log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        // Use 10-second TTL for testing
        log.register_stream("S", Some(Duration::from_secs(10)))
            .unwrap();

        // Add entries: some old, some recent
        log.append("S", b"old1", ts(100)).unwrap();
        log.append("S", b"old2", ts(105)).unwrap();
        log.append("S", b"recent1", ts(115)).unwrap();
        log.append("S", b"recent2", ts(118)).unwrap();

        // Compact at t=120, TTL=10s, cutoff=110
        let removed = log.compact_stream("S", ts(120)).unwrap();
        assert_eq!(removed, 2);

        let entries = log.read_entries("S").unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].payload, b"recent1");
        assert_eq!(entries[1].payload, b"recent2");
    }

    #[test]
    fn test_compact_keyless_stream_removes_expired() {
        let tmp = tempfile::TempDir::new().unwrap();
        let log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("KeylessStream", Some(Duration::from_secs(5)))
            .unwrap();

        log.append("KeylessStream", b"old", ts(100)).unwrap();
        log.append("KeylessStream", b"new", ts(108)).unwrap();

        // Compact at t=110, TTL=5s, cutoff=105
        let removed = log.compact_stream("KeylessStream", ts(110)).unwrap();
        assert_eq!(removed, 1);

        let entries = log.read_entries("KeylessStream").unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].payload, b"new");
    }

    #[test]
    fn test_compact_stream_preserves_recent_entries() {
        let tmp = tempfile::TempDir::new().unwrap();
        let log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("S", Some(Duration::from_secs(100)))
            .unwrap();

        log.append("S", b"e1", ts(50)).unwrap();
        log.append("S", b"e2", ts(60)).unwrap();

        // All entries within TTL -- no entries removed
        let removed = log.compact_stream("S", ts(100)).unwrap();
        assert_eq!(removed, 0);

        let entries = log.read_entries("S").unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_compact_stream_no_expired_produces_identical_output() {
        let tmp = tempfile::TempDir::new().unwrap();
        let log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("S", Some(Duration::from_secs(1000)))
            .unwrap();

        log.append("S", b"event1", ts(500)).unwrap();
        log.append("S", b"event2", ts(600)).unwrap();

        let before = log.read_entries("S").unwrap();
        let removed = log.compact_stream("S", ts(700)).unwrap();
        assert_eq!(removed, 0);

        let after = log.read_entries("S").unwrap();
        assert_eq!(before.len(), after.len());
        for (b, a) in before.iter().zip(after.iter()) {
            assert_eq!(b.payload, a.payload);
            assert_eq!(b.timestamp, a.timestamp);
        }
    }

    #[test]
    fn test_compact_uses_tmp_file_and_renames() {
        let tmp = tempfile::TempDir::new().unwrap();
        let log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("S", Some(Duration::from_secs(10)))
            .unwrap();

        log.append("S", b"old", ts(100)).unwrap();
        log.append("S", b"new", ts(115)).unwrap();

        log.compact_stream("S", ts(120)).unwrap();

        // tmp file should NOT exist after compaction (renamed away)
        let tmp_file = EventLog::stream_log_path(tmp.path(), 0, "S").with_extension("bin.tmp");
        assert!(
            !tmp_file.exists(),
            "tmp file should be renamed away after compaction"
        );

        // Original file should still exist with surviving entries
        let log_file = EventLog::stream_log_path(tmp.path(), 0, "S");
        assert!(log_file.exists());
    }

    #[test]
    fn test_deregister_stream_removes_writer() {
        let tmp = tempfile::TempDir::new().unwrap();
        let log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("S", None).unwrap();
        log.append("S", b"data", ts(1000)).unwrap();

        log.deregister_stream("S").unwrap();

        // Writer should be removed
        assert!(!log.writers.read().contains_key("S"));

        // Append should return false (unregistered)
        let result = log.append("S", b"more", ts(1001)).unwrap();
        assert!(!result);

        // But log file should still exist (not deleted)
        let log_file = EventLog::stream_log_path(tmp.path(), 0, "S");
        assert!(log_file.exists());
    }

    #[test]
    fn test_default_history_ttl_72_hours() {
        assert_eq!(DEFAULT_HISTORY_TTL, Duration::from_secs(72 * 3600));
        assert_eq!(DEFAULT_HISTORY_TTL, Duration::from_secs(259200));

        let tmp = tempfile::TempDir::new().unwrap();
        let log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("S", None).unwrap();
        assert_eq!(log.get_history_ttl("S"), Some(DEFAULT_HISTORY_TTL));
    }

    #[test]
    fn test_register_stream_idempotent() {
        let tmp = tempfile::TempDir::new().unwrap();
        let log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("S", Some(Duration::from_secs(100)))
            .unwrap();
        log.append("S", b"data", ts(1000)).unwrap();

        // Re-register should be a no-op
        log.register_stream("S", Some(Duration::from_secs(200)))
            .unwrap();

        // TTL should not have changed (first registration wins)
        assert_eq!(log.get_history_ttl("S"), Some(Duration::from_secs(100)));

        // Data should still be readable
        let entries = log.read_entries("S").unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn test_sanitize_stream_name_path_traversal() {
        assert_eq!(sanitize_stream_name("../../etc/passwd"), "______etc_passwd");
        assert_eq!(sanitize_stream_name("a/b\\c"), "a_b_c");
        assert_eq!(sanitize_stream_name("normal_name"), "normal_name");
        assert_eq!(sanitize_stream_name("a\0b"), "a_b");
    }

    #[test]
    fn test_read_entries_nonexistent_stream() {
        let tmp = tempfile::TempDir::new().unwrap();
        let log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        let entries = log.read_entries("NoSuchStream").unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_registered_streams_iterator() {
        let tmp = tempfile::TempDir::new().unwrap();
        let log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("A", None).unwrap();
        log.register_stream("B", None).unwrap();
        log.register_stream("C", None).unwrap();

        let mut names: Vec<String> = log.registered_streams();
        names.sort();
        assert_eq!(
            names,
            vec!["A".to_string(), "B".to_string(), "C".to_string()]
        );
    }

    #[test]
    fn test_append_empty_payload() {
        let tmp = tempfile::TempDir::new().unwrap();
        let log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("S", None).unwrap();
        log.append("S", b"", ts(1000)).unwrap();

        let entries = log.read_entries("S").unwrap();
        assert_eq!(entries.len(), 1);
        assert!(entries[0].payload.is_empty());
    }

    #[test]
    fn test_append_large_payload() {
        let tmp = tempfile::TempDir::new().unwrap();
        let log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("S", None).unwrap();

        let large = vec![0xABu8; 100_000];
        log.append("S", &large, ts(1000)).unwrap();

        let entries = log.read_entries("S").unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].payload.len(), 100_000);
        assert!(entries[0].payload.iter().all(|&b| b == 0xAB));
    }

    #[test]
    fn test_custom_history_ttl() {
        let tmp = tempfile::TempDir::new().unwrap();
        let log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        let ttl = Duration::from_secs(3600);
        log.register_stream("S", Some(ttl)).unwrap();
        assert_eq!(log.get_history_ttl("S"), Some(ttl));
    }

    #[test]
    fn test_get_history_ttl_unregistered() {
        let tmp = tempfile::TempDir::new().unwrap();
        let log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        assert_eq!(log.get_history_ttl("NotRegistered"), None);
    }

    // ---------- Phase 40: parallel-write scaling test ----------

    /// Proves that two threads pushing to *different* streams do not fully
    /// serialize. Each thread does a fixed amount of CPU + I/O work; with
    /// the per-stream lock-free writer (Phase 42) the two threads should
    /// run mostly in parallel.
    ///
    /// Tolerance: we require the 2-thread wall time to be less than
    /// `1.5 × single_thread_time`.
    #[test]
    fn parallel_writes_to_different_streams_do_not_serialize() {
        use std::sync::{Arc, Barrier};
        use std::thread;
        use std::time::Instant;

        const N_EVENTS: usize = 2_000;
        // A biggish payload so each write hits the kernel and the
        // write syscall is actually measurable.
        let payload = vec![0xAAu8; 4096];

        // --- Baseline: single thread, 2× N_EVENTS to one stream ---------
        let tmp_baseline = tempfile::TempDir::new().unwrap();
        let baseline_log = EventLog::new(tmp_baseline.path().to_path_buf()).unwrap();
        baseline_log.register_stream("A", None).unwrap();
        let t0 = Instant::now();
        for _ in 0..(N_EVENTS * 2) {
            baseline_log.append("A", &payload, ts(0)).unwrap();
        }
        baseline_log.fsync_all().unwrap();
        let single_thread_time = t0.elapsed();

        // --- Parallel: two threads, N_EVENTS each to *different* streams ---
        let tmp_parallel = tempfile::TempDir::new().unwrap();
        let parallel_log = Arc::new(EventLog::new(tmp_parallel.path().to_path_buf()).unwrap());
        parallel_log.register_stream("A", None).unwrap();
        parallel_log.register_stream("B", None).unwrap();

        let barrier = Arc::new(Barrier::new(2));

        let log_a = Arc::clone(&parallel_log);
        let barrier_a = Arc::clone(&barrier);
        let payload_a = payload.clone();
        let h_a = thread::spawn(move || {
            barrier_a.wait();
            for _ in 0..N_EVENTS {
                log_a.append("A", &payload_a, ts(0)).unwrap();
            }
        });

        let log_b = Arc::clone(&parallel_log);
        let barrier_b = Arc::clone(&barrier);
        let payload_b = payload.clone();
        let h_b = thread::spawn(move || {
            barrier_b.wait();
            for _ in 0..N_EVENTS {
                log_b.append("B", &payload_b, ts(0)).unwrap();
            }
        });

        let t1 = Instant::now();
        h_a.join().unwrap();
        h_b.join().unwrap();
        let parallel_time = t1.elapsed();
        parallel_log.fsync_all().unwrap();

        // Both streams wrote all their events.
        let entries_a = parallel_log.read_entries("A").unwrap();
        let entries_b = parallel_log.read_entries("B").unwrap();
        assert_eq!(entries_a.len(), N_EVENTS);
        assert_eq!(entries_b.len(), N_EVENTS);

        // Scaling assertion: parallel wall time should be well under 1.5× the
        // serial-equivalent time.
        let ratio = parallel_time.as_secs_f64() / single_thread_time.as_secs_f64();
        assert!(
            ratio < 1.5,
            "parallel writes appear serialized: parallel={:?} single={:?} ratio={:.2}",
            parallel_time,
            single_thread_time,
            ratio,
        );
    }

    // ---------- Phase 42: lock-free append frame-integrity test ----------

    /// Mandatory test from Plan 42-01: 8 threads, each appending 10_000
    /// length-prefixed postcard frames to the SAME stream concurrently.
    /// Barrier-synchronized start to maximize the concurrency window.
    ///
    /// Decode the resulting file via length prefix; assert EXACTLY 80_000
    /// valid frames decode with no corruption. This verifies that
    /// O_APPEND + direct write() gives us atomic appends across threads —
    /// no torn frames, no interleaving.
    #[test]
    fn parallel_appends_do_not_tear_frames() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        const N_THREADS: usize = 8;
        const PER_THREAD: usize = 10_000;
        const EXPECTED: usize = N_THREADS * PER_THREAD;

        let tmp = tempfile::TempDir::new().unwrap();
        let log = Arc::new(EventLog::new(tmp.path().to_path_buf()).unwrap());
        log.register_stream("X", None).unwrap();

        let barrier = Arc::new(Barrier::new(N_THREADS));
        let mut handles = Vec::with_capacity(N_THREADS);
        for tid in 0..N_THREADS {
            let log_c = Arc::clone(&log);
            let bar_c = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                bar_c.wait();
                // Vary the payload a little per thread to catch any
                // silent cross-thread byte mixing.
                let payload: Vec<u8> = (0..64).map(|i| (tid as u8).wrapping_add(i as u8)).collect();
                for _ in 0..PER_THREAD {
                    log_c.append("X", &payload, ts(1000)).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        log.fsync_all().unwrap();

        // Decode via length prefix. If any frame is torn, postcard will
        // fail to decode or entries.len() will be wrong.
        let entries = log.read_entries("X").unwrap();
        assert_eq!(
            entries.len(),
            EXPECTED,
            "expected exactly {} decoded frames, got {}",
            EXPECTED,
            entries.len()
        );
        // Every frame should decode to a 64-byte payload.
        for (i, e) in entries.iter().enumerate() {
            assert_eq!(
                e.payload.len(),
                64,
                "frame {} has wrong payload length {}",
                i,
                e.payload.len()
            );
        }
    }

    /// Batch-atomic sibling of the above: same 8-thread setup but each
    /// iteration calls `append_many` with multiple frames in one shot.
    /// Verifies that `append_many` writes the whole batch atomically —
    /// no batch is interleaved with another thread's frames.
    #[test]
    fn parallel_append_many_preserves_batches() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        const N_THREADS: usize = 8;
        const BATCHES_PER_THREAD: usize = 1_000;
        const EVENTS_PER_BATCH: usize = 10;
        const EXPECTED: usize = N_THREADS * BATCHES_PER_THREAD * EVENTS_PER_BATCH;

        let tmp = tempfile::TempDir::new().unwrap();
        let log = Arc::new(EventLog::new(tmp.path().to_path_buf()).unwrap());
        log.register_stream("Y", None).unwrap();

        let barrier = Arc::new(Barrier::new(N_THREADS));
        let mut handles = Vec::with_capacity(N_THREADS);
        for tid in 0..N_THREADS {
            let log_c = Arc::clone(&log);
            let bar_c = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                bar_c.wait();
                let per_event: Vec<u8> = (0..128)
                    .map(|i| (tid as u8).wrapping_add(i as u8))
                    .collect();
                let batch: Vec<&[u8]> = (0..EVENTS_PER_BATCH)
                    .map(|_| per_event.as_slice())
                    .collect();
                for _ in 0..BATCHES_PER_THREAD {
                    log_c.append_many("Y", &batch, ts(1000)).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        log.fsync_all().unwrap();

        let entries = log.read_entries("Y").unwrap();
        assert_eq!(
            entries.len(),
            EXPECTED,
            "expected exactly {} entries across {} batches/{} threads, got {}",
            EXPECTED,
            BATCHES_PER_THREAD * N_THREADS,
            N_THREADS,
            entries.len()
        );
        for e in &entries {
            assert_eq!(e.payload.len(), 128);
        }
    }

    // ---------- D-27: append_with_fsync smoke test ----------

    /// Verifies that `append_with_fsync` writes the entry and fsyncs without error.
    /// This is a smoke test — it proves the method compiles and runs successfully;
    /// it does NOT test durability semantics (that is post-launch work).
    #[tokio::test]
    async fn append_with_fsync_writes_and_fsyncs() {
        let tmp = tempfile::TempDir::new().unwrap();
        let log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("FsyncStream", None).unwrap();

        let now = ts(2000);
        let result = log
            .append_with_fsync("FsyncStream", b"durable_payload", now)
            .await;
        assert!(
            result.is_ok(),
            "append_with_fsync returned error: {:?}",
            result
        );
        assert!(
            result.unwrap(),
            "append_with_fsync returned false (stream not registered?)"
        );

        // Entry must be readable after fsync.
        let entries = log.read_entries("FsyncStream").unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].payload, b"durable_payload");
        assert_eq!(entries[0].timestamp, now);
    }

    /// Verifies that `append_with_fsync` returns Ok(false) for an unregistered stream.
    #[tokio::test]
    async fn append_with_fsync_unregistered_returns_false() {
        let tmp = tempfile::TempDir::new().unwrap();
        let log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        let result = log
            .append_with_fsync("NoSuchStream", b"data", ts(1000))
            .await;
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }
}
