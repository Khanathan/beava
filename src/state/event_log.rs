//! Append-only SSD event log with per-stream files.
//!
//! Events are written to per-stream log files using BufWriter<File>.
//! Writes are buffered (BufWriter::write_all is ~100-300ns memcpy).
//! fsync is done periodically via a background timer, never on the hot path.
//! Compaction rewrites log files excluding entries older than history_ttl.

use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read as IoRead, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use ahash::AHashMap;
use serde::{Serialize, Deserialize};

/// Default history TTL: 72 hours (3 days) per CONTEXT.md locked decision.
pub const DEFAULT_HISTORY_TTL: Duration = Duration::from_secs(259200);

/// A single log entry: timestamp + raw event payload bytes.
#[derive(Debug, Serialize, Deserialize)]
pub struct LogEntry {
    pub timestamp: SystemTime,
    pub payload: Vec<u8>,
}

pub struct EventLog {
    log_dir: PathBuf,
    writers: AHashMap<String, BufWriter<File>>,
    /// Per-stream history TTL for compaction. Streams not in this map are not logged.
    history_ttls: AHashMap<String, Duration>,
}

impl EventLog {
    /// Create a new EventLog, creating the log directory if it does not exist.
    pub fn new(log_dir: PathBuf) -> std::io::Result<Self> {
        fs::create_dir_all(&log_dir)?;
        Ok(Self {
            log_dir,
            writers: AHashMap::new(),
            history_ttls: AHashMap::new(),
        })
    }

    /// Register a stream for event logging.
    /// Creates/opens the log file in append mode. Idempotent (re-registration is a no-op).
    pub fn register_stream(&mut self, stream_name: &str, history_ttl: Option<Duration>) -> std::io::Result<()> {
        let sanitized = sanitize_stream_name(stream_name);
        if self.writers.contains_key(stream_name) {
            return Ok(()); // idempotent re-registration
        }
        let path = self.log_dir.join(format!("{}.log", sanitized));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        self.writers.insert(stream_name.to_string(), BufWriter::new(file));
        self.history_ttls.insert(
            stream_name.to_string(),
            history_ttl.unwrap_or(DEFAULT_HISTORY_TTL),
        );
        Ok(())
    }

    /// Append a raw event to the stream's log file.
    /// Returns Ok(false) if the stream is not registered (no error).
    /// Uses length-prefixed postcard serialization: [u32 BE len][postcard bytes].
    pub fn append(&mut self, stream_name: &str, event_bytes: &[u8], now: SystemTime) -> std::io::Result<bool> {
        let writer = match self.writers.get_mut(stream_name) {
            Some(w) => w,
            None => return Ok(false),
        };
        let entry = LogEntry {
            timestamp: now,
            payload: event_bytes.to_vec(),
        };
        let encoded = postcard::to_stdvec(&entry)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let len = encoded.len() as u32;
        writer.write_all(&len.to_be_bytes())?;
        writer.write_all(&encoded)?;
        Ok(true)
    }

    /// Read all log entries for a stream.
    /// Opens the file independently from the writer.
    pub fn read_entries(&self, stream_name: &str) -> std::io::Result<Vec<LogEntry>> {
        let sanitized = sanitize_stream_name(stream_name);
        let path = self.log_dir.join(format!("{}.log", sanitized));
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

    /// Flush all writers and call fdatasync (sync_data).
    /// Called from background timer only, never on the hot path.
    pub fn fsync_all(&mut self) -> std::io::Result<()> {
        for writer in self.writers.values_mut() {
            writer.flush()?;
            writer.get_ref().sync_data()?;
        }
        Ok(())
    }

    /// Compact a stream's log file by removing entries older than history_ttl.
    /// Writes surviving entries to a tmp file, then atomically renames.
    /// Returns the count of removed entries.
    pub fn compact_stream(&mut self, stream_name: &str, now: SystemTime) -> std::io::Result<usize> {
        let history_ttl = match self.history_ttls.get(stream_name) {
            Some(ttl) => *ttl,
            None => return Ok(0), // not registered
        };

        // Read all entries
        let entries = self.read_entries(stream_name)?;
        let cutoff = now.checked_sub(history_ttl).unwrap_or(SystemTime::UNIX_EPOCH);

        // Partition into kept and removed
        let (kept, removed): (Vec<_>, Vec<_>) = entries
            .into_iter()
            .partition(|e| e.timestamp >= cutoff);
        let removed_count = removed.len();

        if removed_count == 0 {
            return Ok(0);
        }

        let sanitized = sanitize_stream_name(stream_name);
        let log_path = self.log_dir.join(format!("{}.log", sanitized));
        let tmp_path = self.log_dir.join(format!("{}.log.tmp", sanitized));

        // Write surviving entries to tmp file
        {
            let tmp_file = File::create(&tmp_path)?;
            let mut tmp_writer = BufWriter::new(tmp_file);
            for entry in &kept {
                let encoded = postcard::to_stdvec(entry)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
                let len = encoded.len() as u32;
                tmp_writer.write_all(&len.to_be_bytes())?;
                tmp_writer.write_all(&encoded)?;
            }
            tmp_writer.flush()?;
        }

        // Close old writer by removing it
        self.writers.remove(stream_name);

        // Atomic rename
        fs::rename(&tmp_path, &log_path)?;

        // Reopen writer for the stream
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)?;
        self.writers.insert(stream_name.to_string(), BufWriter::new(file));

        Ok(removed_count)
    }

    /// Deregister a stream: flush writer and remove from the writers map.
    /// Does NOT delete the log file (preserve history for potential re-registration).
    pub fn deregister_stream(&mut self, stream_name: &str) -> std::io::Result<()> {
        if let Some(mut writer) = self.writers.remove(stream_name) {
            writer.flush()?;
        }
        Ok(())
    }

    /// Get the history TTL for a stream.
    pub fn get_history_ttl(&self, stream_name: &str) -> Option<Duration> {
        self.history_ttls.get(stream_name).copied()
    }

    /// Return an iterator over registered stream names.
    pub fn registered_streams(&self) -> impl Iterator<Item = &str> {
        self.writers.keys().map(|s| s.as_str())
    }
}

/// Sanitize a stream name for filesystem safety (T-06-04 mitigation).
/// Replaces `/`, `\`, NUL bytes with `_`. Replaces `..` with `__`.
fn sanitize_stream_name(name: &str) -> String {
    let mut s = name.replace('/', "_").replace('\\', "_").replace('\0', "_");
    // Replace ".." to prevent path traversal
    while s.contains("..") {
        s = s.replace("..", "__");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::UNIX_EPOCH;

    fn ts(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
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
        let mut log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("Transactions", None).unwrap();
        let log_file = tmp.path().join("Transactions.log");
        assert!(log_file.exists());
    }

    #[test]
    fn test_append_writes_length_prefixed_entry() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("Transactions", None).unwrap();

        let now = ts(1000);
        let result = log.append("Transactions", b"hello", now).unwrap();
        assert!(result);

        // Flush to ensure data is written
        log.fsync_all().unwrap();

        let entries = log.read_entries("Transactions").unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].payload, b"hello");
        assert_eq!(entries[0].timestamp, now);
    }

    #[test]
    fn test_read_entries_returns_all_entries() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("TestStream", None).unwrap();

        let now = ts(1000);
        log.append("TestStream", b"event1", now).unwrap();
        log.append("TestStream", b"event2", ts(1001)).unwrap();
        log.append("TestStream", b"event3", ts(1002)).unwrap();
        log.fsync_all().unwrap();

        let entries = log.read_entries("TestStream").unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].payload, b"event1");
        assert_eq!(entries[1].payload, b"event2");
        assert_eq!(entries[2].payload, b"event3");
    }

    #[test]
    fn test_multiple_appends_sequential_read() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("S", None).unwrap();

        for i in 0..10 {
            log.append("S", format!("event_{}", i).as_bytes(), ts(1000 + i)).unwrap();
        }
        log.fsync_all().unwrap();

        let entries = log.read_entries("S").unwrap();
        assert_eq!(entries.len(), 10);
        for i in 0..10 {
            assert_eq!(entries[i].payload, format!("event_{}", i).as_bytes());
        }
    }

    #[test]
    fn test_append_unregistered_stream_returns_false() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        let result = log.append("Unknown", b"data", ts(1000)).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_fsync_all_flushes_without_error() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("A", None).unwrap();
        log.register_stream("B", None).unwrap();
        log.append("A", b"data_a", ts(1000)).unwrap();
        log.append("B", b"data_b", ts(1000)).unwrap();
        assert!(log.fsync_all().is_ok());
    }

    #[test]
    fn test_compact_stream_removes_expired_entries() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        // Use 10-second TTL for testing
        log.register_stream("S", Some(Duration::from_secs(10))).unwrap();

        // Add entries: some old, some recent
        log.append("S", b"old1", ts(100)).unwrap();
        log.append("S", b"old2", ts(105)).unwrap();
        log.append("S", b"recent1", ts(115)).unwrap();
        log.append("S", b"recent2", ts(118)).unwrap();
        log.fsync_all().unwrap();

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
        let mut log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("KeylessStream", Some(Duration::from_secs(5))).unwrap();

        log.append("KeylessStream", b"old", ts(100)).unwrap();
        log.append("KeylessStream", b"new", ts(108)).unwrap();
        log.fsync_all().unwrap();

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
        let mut log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("S", Some(Duration::from_secs(100))).unwrap();

        log.append("S", b"e1", ts(50)).unwrap();
        log.append("S", b"e2", ts(60)).unwrap();
        log.fsync_all().unwrap();

        // All entries within TTL -- no entries removed
        let removed = log.compact_stream("S", ts(100)).unwrap();
        assert_eq!(removed, 0);

        let entries = log.read_entries("S").unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_compact_stream_no_expired_produces_identical_output() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("S", Some(Duration::from_secs(1000))).unwrap();

        log.append("S", b"event1", ts(500)).unwrap();
        log.append("S", b"event2", ts(600)).unwrap();
        log.fsync_all().unwrap();

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
        let mut log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("S", Some(Duration::from_secs(10))).unwrap();

        log.append("S", b"old", ts(100)).unwrap();
        log.append("S", b"new", ts(115)).unwrap();
        log.fsync_all().unwrap();

        log.compact_stream("S", ts(120)).unwrap();

        // tmp file should NOT exist after compaction (renamed away)
        let tmp_file = tmp.path().join("S.log.tmp");
        assert!(!tmp_file.exists(), "tmp file should be renamed away after compaction");

        // Original file should still exist with surviving entries
        let log_file = tmp.path().join("S.log");
        assert!(log_file.exists());
    }

    #[test]
    fn test_deregister_stream_removes_writer() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("S", None).unwrap();
        log.append("S", b"data", ts(1000)).unwrap();

        log.deregister_stream("S").unwrap();

        // Writer should be removed
        assert!(!log.writers.contains_key("S"));

        // Append should return false (unregistered)
        let result = log.append("S", b"more", ts(1001)).unwrap();
        assert!(!result);

        // But log file should still exist (not deleted)
        let log_file = tmp.path().join("S.log");
        assert!(log_file.exists());
    }

    #[test]
    fn test_default_history_ttl_72_hours() {
        assert_eq!(DEFAULT_HISTORY_TTL, Duration::from_secs(72 * 3600));
        assert_eq!(DEFAULT_HISTORY_TTL, Duration::from_secs(259200));

        let tmp = tempfile::TempDir::new().unwrap();
        let mut log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("S", None).unwrap();
        assert_eq!(log.get_history_ttl("S"), Some(DEFAULT_HISTORY_TTL));
    }

    #[test]
    fn test_register_stream_idempotent() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("S", Some(Duration::from_secs(100))).unwrap();
        log.append("S", b"data", ts(1000)).unwrap();

        // Re-register should be a no-op
        log.register_stream("S", Some(Duration::from_secs(200))).unwrap();

        // TTL should not have changed (first registration wins)
        assert_eq!(log.get_history_ttl("S"), Some(Duration::from_secs(100)));

        // Data should still be readable
        log.fsync_all().unwrap();
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
        let mut log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("A", None).unwrap();
        log.register_stream("B", None).unwrap();
        log.register_stream("C", None).unwrap();

        let mut names: Vec<&str> = log.registered_streams().collect();
        names.sort();
        assert_eq!(names, vec!["A", "B", "C"]);
    }

    #[test]
    fn test_append_empty_payload() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("S", None).unwrap();
        log.append("S", b"", ts(1000)).unwrap();
        log.fsync_all().unwrap();

        let entries = log.read_entries("S").unwrap();
        assert_eq!(entries.len(), 1);
        assert!(entries[0].payload.is_empty());
    }

    #[test]
    fn test_append_large_payload() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("S", None).unwrap();

        let large = vec![0xABu8; 100_000];
        log.append("S", &large, ts(1000)).unwrap();
        log.fsync_all().unwrap();

        let entries = log.read_entries("S").unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].payload.len(), 100_000);
        assert!(entries[0].payload.iter().all(|&b| b == 0xAB));
    }

    #[test]
    fn test_custom_history_ttl() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut log = EventLog::new(tmp.path().to_path_buf()).unwrap();
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
}
