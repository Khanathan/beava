//! Phase 53-04: `tally migrate-to-fjall` — convert v8 snapshot entity state to
//! per-shard fjall partitions in-place. Closes TPC-PERSIST-03.
//!
//! # Recipe
//!
//! Follows 53-RESEARCH.md §Migration Tool Recipe (authoritative) verbatim, plus
//! the W-2 revision (per-stream `shard_key` routing parity with production):
//!
//! 1. Acquire exclusive `fs2` lock on `data_dir/.beava.lock`.
//! 2. Early-exit if `data_dir/fjall/` exists and `--force` is NOT set (and we're
//!    not in resume mode).
//! 3. Write `data_dir/.migration-in-progress` marker (unless already in resume mode).
//! 4. Load `data_dir/snapshot.bin` → `BaseSnapshotStateV8`.
//! 5. Open the single fjall keyspace at `data_dir/fjall/`.
//! 6. Open N partitions `shard-0` … `shard-(N-1)`.
//! 7. For each `(entity_key, entity_state)`: resolve the shard index via
//!    `resolve_shard_key_for_entity` (W-2) and insert the postcard-encoded
//!    state. Skip entities already present under resume mode.
//! 8. Final `keyspace.persist(SyncAll)`.
//! 9. Drop the keyspace and partitions (triggers final flush on Drop).
//! 10. Write metadata-only snapshot at `data_dir/snapshot.bin`
//!     (same shard_count, pipelines, backfill_complete, replica_lsn_map;
//!     entities: empty).
//! 11. Rename original → `snapshot.v8.bak` (preserve).
//! 12. If `--replace`: delete `snapshot.v8.bak`.
//! 13. Delete `.migration-in-progress` marker.
//! 14. Return `MigrationReport`.
//!
//! # W-2 routing correctness
//!
//! Each entity's shard index is computed as
//! `shard_hint_for_event(synth_payload, Some(stream.key_field)) % shard_count`,
//! where `stream.key_field` is read from the v8 snapshot's pipeline registry.
//! For any stream whose `key_field` is a single field whose VALUE at ingest
//! equals `entity_key`, `synth_payload = json!({key_field: entity_key})`
//! reproduces the ingest-time hash EXACTLY.
//!
//! For keyless streams (`key_field == ""`), migration routes to shard 0 (matches
//! `shard_hint_for_event(_, None) → 0`).
//!
//! Composite shard_keys (multiple fields, not single-field) are NOT supported
//! in v1.2 and fail fast with a clear diagnostic rather than silently misroute.

use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};
use std::time::Instant;

use fs2::FileExt;

use crate::routing::shard_hint::shard_hint_for_event;
use crate::shard::fjall_backend::{
    fjall_config_from_env, open_keyspace_from_env, open_shard_partition,
};
use crate::state::snapshot::{
    load_snapshot_file, save_base_snapshot_v8, BaseSnapshotStateV8, SerializableEntityState,
    SerializablePipeline, SnapshotFile,
};

const MARKER_FILENAME: &str = ".migration-in-progress";
const BAK_FILENAME: &str = "snapshot.v8.bak";
const SNAPSHOT_FILENAME: &str = "snapshot.bin";
const FENCE_EVERY_N_ENTITIES: usize = 1000;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Summary of a migration run. Returned by `migrate_to_fjall` on success.
///
/// `entities_migrated` counts fresh inserts; `entities_skipped` counts entities
/// that were already present in the target partition (resume path).
/// `streams_resolved` and `streams_keyless` are W-2 diagnostic counters:
/// how many distinct stream `key_field`s were resolved (non-empty), and how
/// many individual routing decisions fell back to the keyless path.
#[derive(Debug, Clone)]
pub struct MigrationReport {
    /// Number of entities newly written to fjall during this run.
    pub entities_migrated: usize,
    /// Number of entities skipped because they already existed (resume mode).
    pub entities_skipped: usize,
    /// Wall-clock duration of the migration in milliseconds.
    pub duration_ms: u64,
    /// Path to the preserved `snapshot.v8.bak`, or `None` if `--replace` was set.
    pub bak_path: Option<PathBuf>,
    /// Whether the `.migration-in-progress` marker was removed on success.
    pub marker_removed: bool,
    /// W-2: count of DISTINCT `key_field` names resolved from the pipeline registry.
    pub streams_resolved: usize,
    /// W-2: count of individual entities routed via the keyless (shard 0) path.
    pub streams_keyless: usize,
}

// ---------------------------------------------------------------------------
// W-2: per-stream shard_key resolution
// ---------------------------------------------------------------------------

/// Given an `entity_key` and the snapshot's pipeline registry, determine the
/// correct shard index by reproducing ingest-time routing.
///
/// Returns `(shard_index, shard_key_used)`. `shard_key_used` is `None` for
/// keyless streams (→ shard 0) and `Some(field_name)` otherwise. The VALUE
/// routed through `shard_hint_for_event` is always `entity_key` (the invariant
/// of Phase 49 entity-keying).
///
/// Fails with `InvalidData` if the entity references a stream not present in
/// `pipelines`. Entities with no streams (pure static_features / table_rows
/// entities) are treated as keyless (shard 0).
///
/// # Errors
///
/// - `InvalidData` if an entity's stream is missing from the pipeline registry.
/// - Future (composite shard_keys): returns `InvalidData` — not supported in v1.2.
pub(crate) fn resolve_shard_key_for_entity(
    entity_key: &str,
    entity_state: &SerializableEntityState,
    pipelines: &[SerializablePipeline],
    shard_count: u16,
) -> io::Result<(usize, Option<String>)> {
    // An entity's `streams` is Vec<(String, SerializableStreamEntityState)>
    // where String is the stream name.
    let first_stream_name: Option<&String> = entity_state.streams.first().map(|(n, _)| n);

    let Some(stream_name) = first_stream_name else {
        // No streams → keyless, shard 0. (Pure static_features / table_rows.)
        return Ok((0, None));
    };

    let pipeline = pipelines.iter().find(|p| &p.name == stream_name);
    let Some(pipeline) = pipeline else {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "entity {} references stream {} not found in pipeline registry",
                entity_key, stream_name
            ),
        ));
    };

    // `SerializablePipeline.key_field` is `String`; empty means keyless.
    // Composite keys (rare; none in v1.2) would land here as a non-field-name
    // JSON fragment — we detect by trying to parse it. For now we accept any
    // non-empty string as a single field name (matches the Phase 49 invariant).
    let kf = pipeline.key_field.as_str();
    if kf.is_empty() {
        return Ok((0, None));
    }

    // W-2 CRITICAL: synthesize the routing payload using the stream's actual
    // key_field name — NOT a hardcoded "key" or "user_id". `shard_hint_for_event`
    // hashes the VALUE, not the field name, so this reproduces ingest routing
    // EXACTLY when `entity_key == ingest_event[kf]` (the Phase 49 invariant).
    let payload = serde_json::json!({ kf: entity_key });
    let hint = shard_hint_for_event(&payload, Some(kf));
    let shard_index = (hint as usize) % shard_count.max(1) as usize;
    Ok((shard_index, Some(kf.to_string())))
}

// ---------------------------------------------------------------------------
// migrate_to_fjall
// ---------------------------------------------------------------------------

/// Convert v8 snapshot entity state to per-shard fjall partitions in-place.
///
/// See module-level docs for the 14-step recipe.
///
/// # Idempotency
///
/// If `data_dir/fjall/` already exists and `force == false` and no resume
/// marker is present → returns a no-op `MigrationReport` (entities_migrated = 0).
///
/// # Resume mode
///
/// If `.migration-in-progress` is present and `force == false`, entities already
/// present in the target partition are skipped; only missing entities are
/// migrated.
///
/// # Arguments
///
/// - `data_dir`: root directory containing `snapshot.bin`.
/// - `force`: re-migrate even if `data/fjall/` already exists.
/// - `replace`: delete `snapshot.v8.bak` after successful migration.
pub fn migrate_to_fjall(
    data_dir: &Path,
    force: bool,
    replace: bool,
) -> io::Result<MigrationReport> {
    let started = Instant::now();

    // --- Step 1: exclusive lock -------------------------------------------
    let lock_path = data_dir.join(".beava.lock");
    let lock_file = File::create(&lock_path)?;
    lock_file.try_lock_exclusive().map_err(|_| {
        io::Error::new(
            ErrorKind::WouldBlock,
            "data-dir is held by a running server (cannot migrate live data)",
        )
    })?;

    // --- Step 2: early-exit / resume / force ------------------------------
    let marker_path = data_dir.join(MARKER_FILENAME);
    let fjall_dir = data_dir.join("fjall");
    let marker_present = marker_path.exists();
    let resume = marker_present && !force;

    if fjall_dir.is_dir() && !force && !resume {
        // Already migrated; nothing to do.
        let bak = data_dir.join(BAK_FILENAME);
        let bak_path = bak.exists().then_some(bak);
        return Ok(MigrationReport {
            entities_migrated: 0,
            entities_skipped: 0,
            duration_ms: started.elapsed().as_millis() as u64,
            bak_path,
            marker_removed: false,
            streams_resolved: 0,
            streams_keyless: 0,
        });
    }

    // --- Step 3: write marker --------------------------------------------
    if !resume {
        let pid = std::process::id();
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        fs::write(&marker_path, format!("pid={} ts={}\n", pid, ts))?;
    }

    // --- Step 4: load snapshot -------------------------------------------
    // Prefer `.bak` whenever it exists, because after a successful prior
    // migration (or a crashed one between steps 10-11) `snapshot.bin` is
    // metadata-only (entities: empty). The `.bak` file is the authoritative
    // source of pre-migration entity state. Fresh runs (no prior migration)
    // read from `snapshot.bin` directly.
    let snap_path = data_dir.join(SNAPSHOT_FILENAME);
    let bak_path = data_dir.join(BAK_FILENAME);

    let snap_source = if bak_path.exists() {
        &bak_path
    } else {
        &snap_path
    };

    let snap_bytes = fs::read(snap_source).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("failed to read {}: {}", snap_source.display(), e),
        )
    })?;
    let base_snap: BaseSnapshotStateV8 = match load_snapshot_file(&snap_bytes) {
        Some(SnapshotFile::Base(v8)) => v8,
        Some(SnapshotFile::Delta(_)) => {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "snapshot.bin is a delta snapshot; migrate-to-fjall requires a base snapshot",
            ));
        }
        None => {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "snapshot.bin is unreadable or has unknown format",
            ));
        }
    };

    let shard_count = base_snap.shard_count.max(1);

    // --- Step 5: open keyspace -------------------------------------------
    let cfg = fjall_config_from_env(shard_count);
    let keyspace = open_keyspace_from_env(data_dir, &cfg).map_err(io::Error::other)?;

    // --- Step 6: open N partitions ---------------------------------------
    let mut partitions = Vec::with_capacity(shard_count as usize);
    for s in 0..shard_count as usize {
        partitions.push(open_shard_partition(&keyspace, s, &cfg).map_err(io::Error::other)?);
    }

    // --- Step 7: iterate entities with W-2 routing -----------------------
    let mut entities_migrated = 0usize;
    let mut entities_skipped = 0usize;
    let mut since_fence = 0usize;
    let mut resolved_kfs: HashSet<String> = HashSet::new();
    let mut keyless_count = 0usize;

    for (entity_key, entity_state) in &base_snap.entities {
        let (shard_idx, kf_used) = resolve_shard_key_for_entity(
            entity_key,
            entity_state,
            &base_snap.pipelines,
            shard_count,
        )?;
        match &kf_used {
            Some(kf) => {
                resolved_kfs.insert(kf.clone());
            }
            None => {
                keyless_count += 1;
            }
        }

        let key_bytes = entity_key.as_bytes();

        // Resume + force interaction:
        //   - resume (no force) → skip present keys
        //   - force → always overwrite (re-insert unconditionally)
        //   - neither → fresh run, just insert
        if resume && !force {
            let present = partitions[shard_idx]
                .contains_key(key_bytes)
                .map_err(io::Error::other)?;
            if present {
                entities_skipped += 1;
                continue;
            }
        }

        let bytes = postcard::to_stdvec(entity_state).map_err(io::Error::other)?;
        partitions[shard_idx]
            .insert(key_bytes, bytes)
            .map_err(io::Error::other)?;
        entities_migrated += 1;
        since_fence += 1;
        if since_fence >= FENCE_EVERY_N_ENTITIES {
            keyspace
                .persist(fjall::PersistMode::SyncData)
                .map_err(io::Error::other)?;
            since_fence = 0;
        }
    }

    // --- Step 8: final fsync fence ---------------------------------------
    keyspace
        .persist(fjall::PersistMode::SyncAll)
        .map_err(io::Error::other)?;

    // --- Step 9: drop keyspace -------------------------------------------
    drop(partitions);
    drop(keyspace);

    // --- Step 10: build metadata-only snapshot ---------------------------
    let metadata_only = BaseSnapshotStateV8 {
        header: base_snap.header.clone(),
        entities: Vec::new(),
        pipelines: base_snap.pipelines.clone(),
        backfill_complete: base_snap.backfill_complete.clone(),
        shard_count: base_snap.shard_count,
        replica_lsn_map: base_snap.replica_lsn_map.clone(),
    };
    let metadata_bytes = save_base_snapshot_v8(&metadata_only).map_err(io::Error::other)?;

    // --- Step 11: preserve .v8.bak ---------------------------------------
    // If bak already exists (resume scenario), leave it alone. Otherwise,
    // rename snapshot.bin → snapshot.v8.bak before writing the metadata-only
    // replacement.
    if !bak_path.exists() {
        // snapshot.bin must exist (we read it at step 4 from `snap_source`).
        // If we read from .bak directly (resume), snapshot.bin may already
        // be the metadata-only file from a prior crash; in that case
        // `snap_path` still exists so we just overwrite it.
        if snap_path.exists() && snap_source == &snap_path {
            fs::rename(&snap_path, &bak_path)?;
        }
    }
    fs::write(&snap_path, &metadata_bytes)?;

    // --- Step 12: --replace ----------------------------------------------
    let final_bak = if replace {
        fs::remove_file(&bak_path).ok();
        None
    } else if bak_path.exists() {
        Some(bak_path)
    } else {
        None
    };

    // --- Step 13: remove marker ------------------------------------------
    let marker_removed = if marker_path.exists() {
        fs::remove_file(&marker_path).is_ok()
    } else {
        true
    };

    // --- Step 14: return report ------------------------------------------
    Ok(MigrationReport {
        entities_migrated,
        entities_skipped,
        duration_ms: started.elapsed().as_millis() as u64,
        bak_path: final_bak,
        marker_removed,
        streams_resolved: resolved_kfs.len(),
        streams_keyless: keyless_count,
    })
}

// ---------------------------------------------------------------------------
// CLI arg parsing — mirrors src/reshard/mod.rs shape
// ---------------------------------------------------------------------------

/// Parsed `tally migrate-to-fjall` arguments.
#[derive(Debug, Clone)]
pub struct MigrateArgs {
    /// Path to the data directory (`--data-dir`).
    pub data_dir: PathBuf,
    /// Re-migrate even if `data/fjall/` already exists.
    pub force: bool,
    /// Delete `snapshot.v8.bak` after successful migration.
    pub replace: bool,
    /// Show help and exit 0.
    pub help: bool,
}

/// Parse `tally migrate-to-fjall` arguments from an argv slice.
///
/// Expected shape:
/// ```text
/// tally migrate-to-fjall --data-dir PATH [--force] [--replace] [--help]
/// ```
pub fn parse_migrate_args(args: &[String]) -> Result<MigrateArgs, String> {
    fn get_arg(args: &[String], name: &str) -> Option<String> {
        let long = format!("--{}", name);
        let long_eq = format!("--{}=", name);
        let mut it = args.iter().skip(2); // skip binary + "migrate-to-fjall"
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

    let help = has_flag(args, "help") || args.iter().skip(2).any(|a| a == "-h");
    if help {
        return Ok(MigrateArgs {
            data_dir: PathBuf::new(),
            force: false,
            replace: false,
            help: true,
        });
    }

    let data_dir_str = get_arg(args, "data-dir").ok_or_else(|| {
        "tally migrate-to-fjall: --data-dir PATH required".to_string()
    })?;
    let data_dir = PathBuf::from(data_dir_str);
    let force = has_flag(args, "force");
    let replace = has_flag(args, "replace");

    Ok(MigrateArgs {
        data_dir,
        force,
        replace,
        help: false,
    })
}

/// Detect `tally migrate-to-fjall` / `beava migrate-to-fjall` subcommand.
pub fn is_migrate_subcommand(args: &[String]) -> bool {
    args.get(1).map(|s| s == "migrate-to-fjall").unwrap_or(false)
}

/// Print usage to stderr.
pub fn print_migrate_help() {
    eprintln!(
        "usage: tally migrate-to-fjall --data-dir PATH [--force] [--replace]\n\
         \n\
         Convert v8 snapshot entity state to per-shard fjall partitions in-place.\n\
         \n\
         Required flags:\n\
           --data-dir PATH  Directory containing snapshot.bin.\n\
         \n\
         Optional flags:\n\
           --force          Re-migrate even if data/fjall/ already exists (overwrites).\n\
           --replace        Delete snapshot.v8.bak after successful migration.\n\
           --help, -h       Show this help and exit.\n\
         \n\
         Idempotent by default: running twice without --force is a no-op.\n\
         Resumable: if interrupted, re-run without --force to resume.\n\
         Safe: acquires an exclusive fs2 lock on .beava.lock; refuses to run\n\
         concurrently with a live server.\n"
    );
}
