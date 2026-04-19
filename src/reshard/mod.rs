//! Offline reshard migration module (TPC-DX-03, Phase 52-04).
//!
//! # Purpose
//!
//! `reshard_data_dir` reads a source data directory (snapshot + per-shard event
//! logs) and produces a new data directory rehashed for a different shard count
//! `to_k`. The original directory is never modified; the caller may optionally
//! perform an atomic rename swap via `swap_replace`.
//!
//! # Layout
//!
//! Source:  `data_dir/shard-{0..from_n-1}/streams/{name}/log.bin`
//! Source:  `data_dir/snapshot.bin`  (v8, shard_count == from_n)
//! Output:  `out_dir/shard-{0..to_k-1}/streams/{name}/log.bin`
//! Output:  `out_dir/snapshot.bin`   (v8, shard_count == to_k)
//!
//! # Lock safety
//!
//! Before any I/O the function acquires an exclusive `fs2` lock on
//! `data_dir/.beava.lock`. If that file is held by a running server the call
//! returns `Err(WouldBlock)` with a clear message (T-52-04-01).
//!
//! # Usage
//!
//! ```
//! use std::path::Path;
//! use beava::reshard::reshard_data_dir;
//! // reshard_data_dir(from_n, to_k, data_dir, out_dir)?;
//! ```

pub mod rehash;

pub use rehash::rehash_to_shard;

use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read as IoRead, Write};
use std::path::{Path, PathBuf};

use fs2::FileExt;

use crate::state::event_log::LogEntry;
use crate::state::snapshot::{
    load_snapshot_file, save_base_snapshot_v8, BaseSnapshotStateV8, SnapshotFile, SnapshotHeader,
    SnapshotType,
};

// ──────────────────────────────────────────────────────────────────────────────
// Public API
// ──────────────────────────────────────────────────────────────────────────────

/// Rehash an offline data directory from `from_n` shards to `to_k` shards.
///
/// Steps:
/// 1. Acquire exclusive lock on `data_dir/.beava.lock` (refuse if held).
/// 2. Load and validate `data_dir/snapshot.bin` (`shard_count == from_n`).
/// 3. Create output directory tree (`out_dir/shard-{0..to_k-1}/streams/`).
/// 4. For each source shard: scan its streams, read all log entries, route each
///    entry by `rehash_to_shard(key, to_k)`, and append to the matching output
///    stream log.
/// 5. Write `out_dir/snapshot.bin` (v8, `shard_count == to_k`).
///
/// Progress is printed to stdout. The source directory is never modified.
///
/// # Errors
///
/// Returns `Err` on:
/// - Lock contention: `"data-dir is held by a running server"`.
/// - Missing or unreadable snapshot.
/// - Snapshot `shard_count` mismatch (found N != `from_n`).
/// - I/O errors reading source logs or writing output files.
/// - Corrupt log entries (postcard decode failure).
pub fn reshard_data_dir(
    from_n: u8,
    to_k: u8,
    data_dir: &Path,
    out_dir: &Path,
) -> io::Result<()> {
    // ── Step 1: acquire exclusive lock ────────────────────────────────────
    let lock_path = data_dir.join(".beava.lock");
    let lock_file = File::create(&lock_path)?;
    lock_file.try_lock_exclusive().map_err(|_| {
        io::Error::new(
            io::ErrorKind::WouldBlock,
            "data-dir is held by a running server",
        )
    })?;

    // ── Step 2: load and validate snapshot ────────────────────────────────
    let snap_path = data_dir.join("snapshot.bin");
    let snap_bytes = fs::read(&snap_path).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("failed to read {}: {}", snap_path.display(), e),
        )
    })?;
    let base_snap = match load_snapshot_file(&snap_bytes) {
        Some(SnapshotFile::Base(v8)) => v8,
        Some(SnapshotFile::Delta(_)) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot.bin is a delta snapshot; reshard requires a base snapshot",
            ));
        }
        None => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot.bin is unreadable or has unknown format",
            ));
        }
    };

    // Validate shard_count matches from_n (T-52-04-04)
    if base_snap.shard_count != from_n as u16 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "snapshot shard_count={} does not match --from {}; \
                 pass the correct --from value",
                base_snap.shard_count, from_n
            ),
        ));
    }

    // ── Step 3: create output directory tree ──────────────────────────────
    for s in 0..to_k {
        let streams_dir = out_dir.join(format!("shard-{}/streams", s));
        fs::create_dir_all(&streams_dir)?;
    }

    // ── Step 4: rehash per-source-shard log entries ───────────────────────
    // Collect all stream names seen across all source shards so we know which
    // streams to scan. We walk `shard-{s}/streams/` directories.
    for s in 0..from_n {
        println!("Resharding shard {}/{}...", s, from_n);
        let src_streams_dir = data_dir.join(format!("shard-{}/streams", s));
        if !src_streams_dir.exists() {
            continue;
        }
        let stream_entries = fs::read_dir(&src_streams_dir)?;
        for entry in stream_entries.flatten() {
            let stream_name_raw = entry.file_name();
            let stream_name = stream_name_raw.to_string_lossy();
            let log_path = entry.path().join("log.bin");
            if !log_path.exists() {
                continue;
            }

            // Read all entries from this source shard's stream log
            let entries = read_log_entries(&log_path)?;

            // Route each entry to the target shard and append
            for log_entry in entries {
                // Extract the routing key from the JSON payload.
                // If we can't parse a `key` field, use the raw payload string
                // as the routing key (best-effort; keeps entries moving forward).
                let routing_key = extract_routing_key(&log_entry.payload);

                let target_shard = rehash_to_shard(&routing_key, to_k);
                let target_log_dir = out_dir
                    .join(format!("shard-{}/streams/{}", target_shard, stream_name));
                fs::create_dir_all(&target_log_dir)?;
                let target_log_path = target_log_dir.join("log.bin");

                append_log_entry(&target_log_path, &log_entry)?;
            }
        }
    }

    // ── Step 5: write output snapshot ─────────────────────────────────────
    let out_snap = BaseSnapshotStateV8 {
        header: SnapshotHeader {
            snapshot_type: SnapshotType::Base,
            sequence: base_snap.header.sequence,
        },
        entities: base_snap.entities,
        pipelines: base_snap.pipelines,
        backfill_complete: base_snap.backfill_complete,
        shard_count: to_k as u16,
        replica_lsn_map: base_snap.replica_lsn_map,
    };
    let out_snap_bytes = save_base_snapshot_v8(&out_snap).map_err(io::Error::other)?;
    let out_snap_path = out_dir.join("snapshot.bin");
    fs::write(&out_snap_path, &out_snap_bytes)?;

    println!("Done. Output: {}", out_dir.display());
    Ok(())
}

/// Atomically swap `out_dir` into `data_dir` using `fs::rename`.
///
/// After the swap:
/// - `data_dir.bak` contains the original data directory.
/// - `data_dir` is the freshly resharded output.
///
/// Both renames are performed sequentially; on POSIX `rename(2)` is atomic per
/// call. If the second rename fails the backup rename has already happened —
/// callers should check the return value and handle partial failure by
/// manually restoring from `data_dir.bak`.
pub fn swap_replace(data_dir: &Path, out_dir: &Path) -> io::Result<()> {
    let bak_path = PathBuf::from(format!("{}.bak", data_dir.display()));
    fs::rename(data_dir, &bak_path)?;
    fs::rename(out_dir, data_dir)?;
    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────────
// CLI arg parsing (called from main.rs and tests)
// ──────────────────────────────────────────────────────────────────────────────

/// Parsed result of a `tally reshard ...` command line.
pub struct ReshardArgs {
    pub from_n: u8,
    pub to_k: u8,
    pub data_dir: PathBuf,
    pub out_dir: PathBuf,
    pub replace: bool,
}

/// Parse `tally reshard` arguments from an argv slice.
///
/// Expected shape:
/// ```text
/// tally reshard --from N --to K --data-dir PATH --out-dir PATH [--replace]
/// ```
///
/// Returns `Err(String)` with a usage message when any required argument is
/// missing or malformed. The caller should print the message and exit 1.
pub fn parse_reshard_args(args: &[String]) -> Result<ReshardArgs, String> {
    fn get_arg(args: &[String], name: &str) -> Option<String> {
        let long = format!("--{}", name);
        let long_eq = format!("--{}=", name);
        let mut it = args.iter().skip(2); // skip binary + "reshard"
        while let Some(a) = it.next() {
            if a == &long {
                return it.next().cloned();
            }
            if let Some(rest) = a.strip_prefix(&long_eq) {
                return Some(rest.to_string());
            }
        }
        None
    }

    fn has_flag(args: &[String], name: &str) -> bool {
        let long = format!("--{}", name);
        args.iter().skip(2).any(|a| a == &long)
    }

    let from_str = get_arg(args, "from")
        .ok_or_else(|| "tally reshard: --from N required".to_string())?;
    let from_n: u8 = from_str
        .parse()
        .map_err(|_| format!("tally reshard: --from '{}' is not a valid u8", from_str))?;

    let to_str = get_arg(args, "to")
        .ok_or_else(|| "tally reshard: --to K required".to_string())?;
    let to_k: u8 = to_str
        .parse()
        .map_err(|_| format!("tally reshard: --to '{}' is not a valid u8", to_str))?;

    if to_k == 0 {
        return Err("tally reshard: --to must be >= 1".to_string());
    }

    let data_dir_str = get_arg(args, "data-dir")
        .ok_or_else(|| "tally reshard: --data-dir PATH required".to_string())?;
    let data_dir = PathBuf::from(data_dir_str);

    let out_dir_str = get_arg(args, "out-dir")
        .ok_or_else(|| "tally reshard: --out-dir PATH required".to_string())?;
    let out_dir = PathBuf::from(out_dir_str);

    let replace = has_flag(args, "replace");

    Ok(ReshardArgs {
        from_n,
        to_k,
        data_dir,
        out_dir,
        replace,
    })
}

/// Print usage information for `tally reshard` to stderr.
pub fn print_reshard_help() {
    eprintln!(
        "usage: tally reshard --from N --to K --data-dir PATH --out-dir PATH [--replace]\n\
         \n\
         Offline migration tool. Rehashes all per-shard event logs and snapshot\n\
         from M shards to N shards. The source data-dir is never modified unless\n\
         --replace is passed, which performs an atomic rename swap:\n\
           1. data-dir -> data-dir.bak\n\
           2. out-dir  -> data-dir\n\
         \n\
         Required flags:\n\
           --from N        Source shard count (must match snapshot shard_count).\n\
           --to K          Target shard count (K >= 1).\n\
           --data-dir PATH Source data directory (must not be held by a live server).\n\
           --out-dir PATH  Destination directory for the resharded output.\n\
         \n\
         Optional flags:\n\
           --replace       Atomically swap out-dir into data-dir after resharding.\n"
    );
}

/// Detect `tally reshard` (or `beava reshard`) subcommand from argv[1].
pub fn is_reshard_subcommand(args: &[String]) -> bool {
    args.get(1).map(|s| s == "reshard").unwrap_or(false)
}

// ──────────────────────────────────────────────────────────────────────────────
// Private helpers
// ──────────────────────────────────────────────────────────────────────────────

/// Read all length-prefixed postcard `LogEntry` frames from a log file.
///
/// Returns `Err` on I/O or postcard deserialization failure (T-52-04-03).
fn read_log_entries(path: &Path) -> io::Result<Vec<LogEntry>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut entries = Vec::new();
    loop {
        let mut len_buf = [0u8; 4];
        match reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut data = vec![0u8; len];
        reader.read_exact(&mut data).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!(
                    "corrupt log entry in {}: expected {} bytes, got: {}",
                    path.display(),
                    len,
                    e
                ),
            )
        })?;
        let entry: LogEntry = postcard::from_bytes(&data).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("postcard decode failed for entry in {}: {}", path.display(), e),
            )
        })?;
        entries.push(entry);
    }
    Ok(entries)
}

/// Append one `LogEntry` to a log file using length-prefixed postcard framing.
fn append_log_entry(path: &Path, entry: &LogEntry) -> io::Result<()> {
    let file = fs::OpenOptions::new().create(true).append(true).open(path)?;
    let mut writer = BufWriter::new(file);
    let encoded = postcard::to_stdvec(entry).map_err(io::Error::other)?;
    writer.write_all(&(encoded.len() as u32).to_be_bytes())?;
    writer.write_all(&encoded)?;
    writer.flush()?;
    Ok(())
}

/// Extract a routing key from a log entry payload.
///
/// Tries to parse the payload as a JSON object with a `"key"` field. Falls
/// back to the raw payload bytes as a lossy UTF-8 string if JSON parsing fails
/// or the `"key"` field is absent. This ensures every entry gets routed —
/// corrupt / non-JSON entries are not silently dropped.
fn extract_routing_key(payload: &[u8]) -> String {
    // Strip format tag byte if present (LOG_FMT_JSON = 0x00, LOG_FMT_BINARY = 0x01)
    let body = match payload.first() {
        Some(&0x00) | Some(&0x01) => &payload[1..],
        _ => payload,
    };
    if let Ok(doc) = serde_json::from_slice::<serde_json::Value>(body) {
        if let Some(key) = doc.get("key").and_then(|v| v.as_str()) {
            return key.to_string();
        }
    }
    // Fallback: use the raw bytes as the key string
    String::from_utf8_lossy(payload).into_owned()
}
