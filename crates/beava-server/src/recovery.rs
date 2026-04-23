//! Phase 7 Plan 03: recovery on startup.
//!
//! Order of operations:
//! 1. `load_snapshot_if_any(dir, app_state)` — descending-LSN scan; first valid
//!    snapshot wins; install registry descriptors + state tables; return
//!    `snapshot_lsn`. Empty dir or all-corrupt files → 0 (cold start).
//! 2. `replay_wal_from_lsn(wal_dir, snapshot_lsn, app_state)` — read every WAL
//!    record in LSN order; skip `lsn <= snapshot_lsn`; for each remaining
//!    record dispatch by `RecordType`:
//!    - `Event` → decode payload + `apply_event_to_aggregations`.
//!    - `RegistryBump` → `bincode`-decode `RegistryBumpPayload` + apply.
//!
//! Apply-AFTER-fsync still holds on replay because every WAL record is durable
//! by definition; LSN order = apply order.

use crate::register::RegistryBumpPayload;
use crate::registry_debug::DevAggState;
use beava_core::agg_apply::apply_event_to_aggregations;
use beava_core::agg_state_table::AggStateTable;
use beava_core::row::{Row, Value};
use beava_core::snapshot_body::SnapshotBody;
use beava_persistence::{list_snapshots, Lsn, PersistError, RecordType, SnapshotReader, WalReader};
use serde::Deserialize;
use std::path::Path;
use std::sync::atomic::Ordering;

/// Outcome counters reported back from `replay_wal_from_lsn`.
#[derive(Debug, Default)]
pub struct RecoveryOutcome {
    pub installed_from_snapshot: bool,
    pub snapshot_lsn: Lsn,
    pub replay_event_count: u64,
    pub replay_registry_bumps: u64,
    pub last_lsn: Lsn,
}

/// Scan `snapshot_dir` for the highest-LSN valid snapshot; install its
/// registry descriptors + state tables into `app_state`. Returns the
/// snapshot's LSN, or 0 if no valid snapshot exists (cold start).
pub fn load_snapshot_if_any(
    snapshot_dir: &Path,
    dev_agg: &DevAggState,
) -> Result<Lsn, PersistError> {
    let snaps = list_snapshots(snapshot_dir)?;
    for (lsn, path) in snaps {
        match SnapshotReader::open(&path) {
            Ok((header, body)) => match SnapshotBody::decode(&body) {
                Ok(snapshot_body) => {
                    let (registry_only, state_tables, next_event_id, max_event_time_ms) =
                        snapshot_body.into_parts();
                    dev_agg.registry.install_from_descriptors(&registry_only);
                    {
                        let mut tables = dev_agg.state_tables.lock();
                        tables.clear();
                        for (node_name, entries) in state_tables {
                            let mut tbl = AggStateTable::new();
                            for (key, ops) in entries {
                                tbl.entities.insert(key, ops);
                            }
                            tables.insert(node_name, tbl);
                        }
                    }
                    dev_agg
                        .next_event_id
                        .store(next_event_id, Ordering::Relaxed);
                    if max_event_time_ms > 0 {
                        dev_agg
                            .max_event_time_ms
                            .store(max_event_time_ms as u64, Ordering::Relaxed);
                    }
                    tracing::info!(
                        target: "beava.recovery",
                        kind = "recovery.snapshot_loaded",
                        snapshot_lsn = lsn,
                        registry_version = header.registry_version,
                        "loaded snapshot"
                    );
                    return Ok(lsn);
                }
                Err(e) => {
                    tracing::warn!(
                        target: "beava.recovery",
                        kind = "recovery.snapshot_decode_failed",
                        snapshot_lsn = lsn,
                        error = %e,
                        "snapshot body decode failed; trying older snapshot"
                    );
                    continue;
                }
            },
            Err(e) => {
                tracing::warn!(
                    target: "beava.recovery",
                    kind = "recovery.snapshot_open_failed",
                    snapshot_lsn = lsn,
                    error = %e,
                    "snapshot open/verify failed; trying older snapshot"
                );
                continue;
            }
        }
    }
    Ok(0)
}

/// JSON shape of a WAL Event record's payload (matches push.rs encoder).
#[derive(Debug, Deserialize)]
struct WalEventPayload {
    #[allow(dead_code)]
    v: u64,
    #[allow(dead_code)]
    rv: u64,
    s: String,
    et: i64,
    b: serde_json::Value,
}

/// Replay every WAL record in `wal_dir` whose LSN > `from_lsn_exclusive`,
/// applying them to `app_state`. Returns counters + last LSN seen.
///
/// Bumps `next_event_id` and `max_event_time_ms` as records replay so the
/// post-recovery server picks up monotonic counters consistent with the WAL.
pub fn replay_wal_from_lsn(
    wal_dir: &Path,
    from_lsn_exclusive: Lsn,
    dev_agg: &DevAggState,
) -> Result<RecoveryOutcome, PersistError> {
    let mut outcome = RecoveryOutcome {
        snapshot_lsn: from_lsn_exclusive,
        ..Default::default()
    };
    if !wal_dir.exists() {
        return Ok(outcome);
    }
    let records = WalReader::read_all(wal_dir)?;
    for rec in records {
        if rec.lsn <= from_lsn_exclusive {
            continue;
        }
        outcome.last_lsn = outcome.last_lsn.max(rec.lsn);
        match rec.record_type {
            RecordType::Event => {
                let payload: WalEventPayload = match serde_json::from_slice(&rec.payload) {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::warn!(
                            target: "beava.recovery",
                            kind = "recovery.event_decode_failed",
                            lsn = rec.lsn,
                            error = %e,
                            "event payload decode failed; skipping"
                        );
                        continue;
                    }
                };
                let row = json_object_to_row(&payload.b);
                {
                    let mut tables = dev_agg.state_tables.lock();
                    apply_event_to_aggregations(
                        &payload.s,
                        &row,
                        payload.et,
                        rec.lsn,
                        &dev_agg.registry,
                        &mut tables,
                    );
                }
                dev_agg.next_event_id.fetch_max(rec.lsn, Ordering::Relaxed);
                if payload.et > 0 {
                    dev_agg
                        .max_event_time_ms
                        .fetch_max(payload.et as u64, Ordering::Relaxed);
                }
                outcome.replay_event_count += 1;
            }
            RecordType::RegistryBump => match RegistryBumpPayload::decode(&rec.payload) {
                Ok(bump) => match crate::register::apply_registry_bump(&dev_agg.registry, bump) {
                    Ok(()) => {
                        outcome.replay_registry_bumps += 1;
                    }
                    Err(e) => {
                        tracing::warn!(
                            target: "beava.recovery",
                            kind = "recovery.registry_bump_apply_failed",
                            lsn = rec.lsn,
                            error = %e,
                            "RegistryBump apply failed; skipping"
                        );
                    }
                },
                Err(e) => {
                    tracing::warn!(
                        target: "beava.recovery",
                        kind = "recovery.registry_bump_decode_failed",
                        lsn = rec.lsn,
                        error = %e,
                        "RegistryBump payload decode failed; skipping"
                    );
                }
            },
        }
    }
    outcome.installed_from_snapshot = from_lsn_exclusive > 0;
    tracing::info!(
        target: "beava.recovery",
        kind = "recovery.complete",
        snapshot_lsn = outcome.snapshot_lsn,
        events_replayed = outcome.replay_event_count,
        registry_bumps_replayed = outcome.replay_registry_bumps,
        last_lsn = outcome.last_lsn,
        "recovery complete"
    );
    Ok(outcome)
}

fn json_object_to_row(jv: &serde_json::Value) -> Row {
    let mut row = Row::new();
    if let Some(obj) = jv.as_object() {
        for (field, jv) in obj {
            let v = match jv {
                serde_json::Value::Null => Value::Null,
                serde_json::Value::Bool(b) => Value::Bool(*b),
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        Value::I64(i)
                    } else if let Some(f) = n.as_f64() {
                        Value::F64(f)
                    } else {
                        Value::Null
                    }
                }
                serde_json::Value::String(s) => Value::Str(s.clone()),
                _ => Value::Null,
            };
            row = row.with_field(field.as_str(), v);
        }
    }
    row
}
