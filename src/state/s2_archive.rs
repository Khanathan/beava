//! S2 archive backend for aged-out event-log entries (Phase 43 T6).
//!
//! Design:
//!
//! - When `compact_stream` determines a set of entries are past their TTL,
//!   it hands them to `ArchiveBackend::archive` BEFORE rewriting the
//!   survivors. On success, the expired entries are dropped from local
//!   disk. On failure, they are KEPT (stream survives unmodified this
//!   cycle) and an operational signal is emitted. Losing archival
//!   integrity on an S2 outage is worse than transient disk growth.
//!
//! - `S2Archive` POSTs batches to
//!   `https://{basin}.b.s2.dev/v1/streams/{stream}/records` with the
//!   `s2-format: base64` header. Each `LogEntry.payload` is base64-
//!   encoded into one record; the S2 AppendInput is filled to the max
//!   (1000 records **or** ~900 KiB of encoded body, whichever comes
//!   first) so one compaction cycle of N entries issues `ceil(N/1000)`
//!   append calls — matching the S2 hard limit from
//!   https://s2.dev/docs/api/records/append.md.
//!
//! - No retries inside this module. The compaction timer re-runs every
//!   60s; a transient S2 failure is handled by the cycle skipping its
//!   drop and trying again next minute. Backoff for hard 4xx errors
//!   (auth, basin-not-found) is the operator's call via the log signal.

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use serde::Serialize;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use crate::state::event_log::LogEntry;

/// Max records per S2 AppendInput (S2 API hard cap).
pub const MAX_RECORDS_PER_APPEND: usize = 1000;

/// Soft cap on the encoded body bytes in a single AppendInput. The S2
/// metered-bytes limit is 1 MiB; we leave ~120 KiB headroom for the JSON
/// envelope (field names, commas, base64 padding) so the request body is
/// always under the hard limit without needing an exact size accounting.
pub const MAX_BODY_BYTES_PER_APPEND: usize = 900 * 1024;

/// Trait for the "where do expired events go" policy. The default
/// implementation in `compact_stream` is "just drop them"; S2Archive is
/// the first real implementation.
///
/// Implementors are called from `EventLog::compact_stream` which is
/// synchronous and already running on a tokio-background task.
pub trait ArchiveBackend: Send + Sync {
    /// Archive a batch of entries for `stream_name`. Must return Ok only
    /// if every entry was accepted by the backend; partial success must
    /// fail so the caller retains the entries locally for the next cycle.
    fn archive(&self, stream_name: &str, entries: &[LogEntry]) -> io::Result<usize>;
}

/// S2 archive backend.
pub struct S2Archive {
    basin: String,
    token: String,
    endpoint: String,
    agent: ureq::Agent,
}

impl S2Archive {
    /// Build from environment. Returns `None` when either env var is
    /// missing, so the feature is opt-in. `BEAVA_S2_ENDPOINT` overrides
    /// the default basin-endpoint template for testing / private cells.
    pub fn new_from_env() -> Option<Self> {
        let token = std::env::var("BEAVA_S2_TOKEN").ok().filter(|s| !s.is_empty())?;
        let basin = std::env::var("BEAVA_S2_BASIN").ok().filter(|s| !s.is_empty())?;
        let endpoint = std::env::var("BEAVA_S2_ENDPOINT")
            .unwrap_or_else(|_| format!("https://{}.b.s2.dev/v1", basin));
        Some(Self {
            basin,
            token,
            endpoint,
            agent: ureq::AgentBuilder::new()
                .timeout_connect(Duration::from_secs(5))
                .timeout(Duration::from_secs(30))
                .build(),
        })
    }

    /// Construct directly (tests / private-cell endpoints).
    #[cfg(test)]
    pub fn new_for_test(basin: impl Into<String>, token: impl Into<String>, endpoint: impl Into<String>) -> Self {
        Self {
            basin: basin.into(),
            token: token.into(),
            endpoint: endpoint.into(),
            agent: ureq::AgentBuilder::new()
                .timeout(Duration::from_secs(5))
                .build(),
        }
    }

    pub fn basin(&self) -> &str {
        &self.basin
    }

    /// Chunk `entries` into S2-sized batches (max 1000 records or
    /// ~900 KiB encoded, whichever hits first) and POST each. Returns
    /// total records uploaded on success. Returns the first error the
    /// moment any batch fails — the caller must KEEP all entries in that
    /// case because partial archival is not recoverable from local state
    /// alone.
    fn upload_all(&self, stream_name: &str, entries: &[LogEntry]) -> io::Result<usize> {
        let mut uploaded = 0usize;
        for chunk in pack_chunks(entries) {
            self.post_one_batch(stream_name, chunk)?;
            uploaded += chunk.len();
        }
        Ok(uploaded)
    }

    fn post_one_batch(&self, stream_name: &str, entries: &[LogEntry]) -> io::Result<()> {
        let url = format!("{}/streams/{}/records", self.endpoint, stream_name);
        let body = AppendInput {
            records: entries
                .iter()
                .map(|e| AppendRecord {
                    body: B64.encode(&e.payload),
                })
                .collect(),
        };
        let resp = self
            .agent
            .post(&url)
            .set("Authorization", &format!("Bearer {}", self.token))
            .set("s2-format", "base64")
            .set("Content-Type", "application/json")
            .send_json(serde_json::to_value(&body).map_err(io::Error::other)?);
        match resp {
            Ok(r) if r.status() >= 200 && r.status() < 300 => Ok(()),
            Ok(r) => {
                let status = r.status();
                let body_text = r.into_string().unwrap_or_default();
                Err(io::Error::other(format!(
                    "s2 archive: HTTP {} for stream {}: {}",
                    status, stream_name, body_text
                )))
            }
            Err(ureq::Error::Status(code, r)) => {
                let body_text = r.into_string().unwrap_or_default();
                Err(io::Error::other(format!(
                    "s2 archive: HTTP {} for stream {}: {}",
                    code, stream_name, body_text
                )))
            }
            Err(e) => Err(io::Error::other(format!("s2 archive: transport error: {}", e))),
        }
    }
}

impl ArchiveBackend for S2Archive {
    fn archive(&self, stream_name: &str, entries: &[LogEntry]) -> io::Result<usize> {
        if entries.is_empty() {
            return Ok(0);
        }
        self.upload_all(stream_name, entries)
    }
}

/// Split entries into max-sized chunks bounded by record count AND
/// encoded body bytes. Exposed (pub(crate)) so the chunking logic can be
/// tested directly without needing a live S2 basin.
pub(crate) fn pack_chunks(entries: &[LogEntry]) -> Vec<&[LogEntry]> {
    let mut out: Vec<&[LogEntry]> = Vec::new();
    let mut start = 0usize;
    let mut bytes_in_chunk = 0usize;
    for (i, e) in entries.iter().enumerate() {
        // base64 encodes 3 bytes -> 4 chars, so size ~= 4 * ceil(len/3).
        let encoded_len = 4 * e.payload.len().div_ceil(3);
        let records_in_chunk = i - start;
        let would_exceed_records = records_in_chunk + 1 > MAX_RECORDS_PER_APPEND;
        let would_exceed_bytes =
            records_in_chunk > 0 && bytes_in_chunk + encoded_len > MAX_BODY_BYTES_PER_APPEND;
        if would_exceed_records || would_exceed_bytes {
            out.push(&entries[start..i]);
            start = i;
            bytes_in_chunk = 0;
        }
        bytes_in_chunk += encoded_len;
    }
    if start < entries.len() {
        out.push(&entries[start..]);
    }
    out
}

// Type alias so callers can store either "no archive" or an Arc'd backend
// without conditionally wiring two code paths. None = drop expired
// entries in place (current behaviour).
pub type MaybeArchive = Option<Arc<dyn ArchiveBackend>>;

#[derive(Serialize)]
struct AppendInput {
    records: Vec<AppendRecord>,
}

#[derive(Serialize)]
struct AppendRecord {
    body: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

    fn entry(len: usize) -> LogEntry {
        LogEntry {
            timestamp: SystemTime::UNIX_EPOCH,
            payload: vec![0u8; len],
        }
    }

    #[test]
    fn pack_chunks_empty_input_produces_no_chunks() {
        let out = pack_chunks(&[]);
        assert!(out.is_empty());
    }

    #[test]
    fn pack_chunks_respects_max_records() {
        let entries: Vec<LogEntry> = (0..2500).map(|_| entry(10)).collect();
        let out = pack_chunks(&entries);
        assert_eq!(out.len(), 3, "2500 entries -> ceil(2500/1000) = 3 chunks");
        assert_eq!(out[0].len(), MAX_RECORDS_PER_APPEND);
        assert_eq!(out[1].len(), MAX_RECORDS_PER_APPEND);
        assert_eq!(out[2].len(), 500);
    }

    #[test]
    fn pack_chunks_respects_byte_cap_when_entries_are_large() {
        // Entry payload 10_000 bytes -> base64 ~13_336 chars. 900 KiB /
        // 13_336 ≈ 69 records before the byte cap kicks in. Use 150
        // entries so we force a byte-split before the 1000-record cap.
        let entries: Vec<LogEntry> = (0..150).map(|_| entry(10_000)).collect();
        let out = pack_chunks(&entries);
        assert!(out.len() >= 2, "large entries must split into multiple chunks");
        for chunk in &out {
            let body_bytes: usize = chunk.iter().map(|e| 4 * e.payload.len().div_ceil(3)).sum();
            assert!(
                body_bytes <= MAX_BODY_BYTES_PER_APPEND,
                "chunk exceeds MAX_BODY_BYTES_PER_APPEND: {} bytes",
                body_bytes
            );
        }
    }

    #[test]
    fn pack_chunks_exactly_one_full_append_stays_one_chunk() {
        let entries: Vec<LogEntry> = (0..MAX_RECORDS_PER_APPEND).map(|_| entry(10)).collect();
        let out = pack_chunks(&entries);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].len(), MAX_RECORDS_PER_APPEND);
    }

    #[test]
    fn pack_chunks_one_oversized_entry_still_goes_through_alone() {
        // Pathological case: a single entry whose base64-encoded size
        // exceeds the byte cap. We cannot split inside a record, so it
        // must still be yielded in a chunk of its own.
        let entries = vec![entry(MAX_BODY_BYTES_PER_APPEND * 2)];
        let out = pack_chunks(&entries);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].len(), 1);
    }

    #[test]
    fn new_from_env_absent_vars_returns_none() {
        // Snapshot + restore: save whatever the env currently has, wipe,
        // check, then restore. Do NOT run parallel with other tests that
        // also touch these vars.
        let prior_token = std::env::var("BEAVA_S2_TOKEN").ok();
        let prior_basin = std::env::var("BEAVA_S2_BASIN").ok();
        std::env::remove_var("BEAVA_S2_TOKEN");
        std::env::remove_var("BEAVA_S2_BASIN");
        assert!(S2Archive::new_from_env().is_none());
        if let Some(v) = prior_token {
            std::env::set_var("BEAVA_S2_TOKEN", v);
        }
        if let Some(v) = prior_basin {
            std::env::set_var("BEAVA_S2_BASIN", v);
        }
    }
}
