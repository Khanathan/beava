//! Phase 12.7 Plan 05 — TDD red gate for D-01 RESET (FORMAT_VERSION 2 → 1).
//!
//! Plan 12.6-06 (D-03 hard rip) bumped the WAL `FORMAT_VERSION` constant
//! 1 → 2 alongside the deletion of `event_time` from per-record payloads.
//! Plan 12.7-05 (D-01 hard rip RESET) now resets ALL THREE format-version
//! constants 2 → 1 because v0 ships events-only per
//! `project_v0_events_only_scope` (locked 2026-04-30) — the table /
//! retraction surface that landed in Phase 11.5 is being stripped wholesale,
//! and v0 isn't released, so there is nothing to be backward-compatible
//! with. Reset is the canonical move for an unreleased product.
//!
//! Per CONTEXT D-02 ("not supported in v0", NOT "feature removed"):
//! `RecordType::from_u8(0x03|0x04|0x05)` returns the existing generic
//! `PersistError::UnknownRecordType(b)` variant — no new table-flavored
//! error code in persistence.
//!
//! Pre-12.7 dev WALs (which carried `v=2`) fail at the version-byte check
//! with the existing `SchemaVersionMismatch`/`UnsupportedVersion` error
//! paths; operators clear `.beava/wal` + `.beava/snapshots` before booting
//! the new binary. No migration shim per D-01.

use beava_persistence::{PersistError, RecordType, FORMAT_VERSION};

/// Test 1 — WAL record FORMAT_VERSION reset 2 → 1.
///
/// `crates/beava-persistence/src/record.rs:25` carries `pub const FORMAT_VERSION: u32`.
/// Plan 12.6-06 set it to 2; Plan 12.7-05 RESETS it to 1.
#[test]
fn record_format_version_is_1() {
    assert_eq!(
        FORMAT_VERSION, 1,
        "Plan 12.7-05 (D-01 hard rip RESET): FORMAT_VERSION must be 1 (was 2 in 12.6 \
         Plan 06; v0 isn't released so we reset rather than bump per CONTEXT D-01)"
    );
}

/// Test 2 — Snapshot body format version reset 2 → 1.
///
/// `crates/beava-core/src/snapshot_body.rs:32` carries
/// `pub const SNAPSHOT_BODY_FORMAT_VERSION: u16`. Same RESET rationale.
#[test]
fn snapshot_body_format_version_is_1() {
    use beava_core::snapshot_body::SNAPSHOT_BODY_FORMAT_VERSION;
    assert_eq!(
        SNAPSHOT_BODY_FORMAT_VERSION, 1,
        "Plan 12.7-05 (D-01 hard rip RESET): SNAPSHOT_BODY_FORMAT_VERSION must be 1 \
         (was 2 in 12.6 Plan 06; reset alongside record.rs FORMAT_VERSION reset)"
    );
}

/// Test 3 — Snapshot header format version reset 2 → 1.
///
/// `crates/beava-persistence/src/snapshot_header.rs:29` carries
/// `pub const SNAPSHOT_FORMAT_VERSION: u16`. Same RESET rationale.
#[test]
fn snapshot_format_version_is_1() {
    use beava_persistence::SNAPSHOT_FORMAT_VERSION;
    assert_eq!(
        SNAPSHOT_FORMAT_VERSION, 1,
        "Plan 12.7-05 (D-01 hard rip RESET): SNAPSHOT_FORMAT_VERSION must be 1 \
         (was 2 in 12.6 Plan 06; v0 launches at version=1 uniformly across WAL/snapshot)"
    );
}

/// Test 4 — `from_u8(0x03|0x04|0x05)` falls through to the existing generic
/// `UnknownRecordType` error variant (CONTEXT D-02).
///
/// Pre-12.7 these bytes mapped to `RecordType::TableUpsert` (0x03),
/// `RecordType::TableDelete` (0x04), and `RecordType::Retract` (0x05).
/// Plan 12.7-05 deletes those variants; the generic `UnknownRecordType(b)`
/// arm naturally covers the now-unmapped bytes — no new table-specific
/// error code per CONTEXT D-02 ("not supported in v0", NOT "feature removed").
#[test]
fn from_u8_maps_table_variants_to_unknown() {
    for b in [0x03u8, 0x04, 0x05] {
        let res = RecordType::from_u8(b);
        match res {
            Err(PersistError::UnknownRecordType(got)) => {
                assert_eq!(
                    got, b,
                    "UnknownRecordType byte payload must echo the input byte, got {got:#04x} for input {b:#04x}"
                );
            }
            other => panic!(
                "RecordType::from_u8({b:#04x}) must return PersistError::UnknownRecordType \
                 (CONTEXT D-02 — no new table-specific code; existing generic variant suffices). \
                 Got: {other:?}"
            ),
        }
    }
}

/// Test 5 — Surviving variants still round-trip cleanly.
///
/// Sanity-positive: `Event = 0x01` and `RegistryBump = 0x02` remain after
/// the table-variant strip. Passes at start of Plan 05 (pre-edit) so this
/// test is the canary that the table strip doesn't accidentally hit the
/// surviving event/registry path.
#[test]
fn surviving_record_types_round_trip() {
    assert_eq!(
        RecordType::from_u8(0x01).expect("0x01 must round-trip"),
        RecordType::Event,
        "RecordType::Event = 0x01 must remain after Plan 12.7-05 (v0 events-only path)"
    );
    assert_eq!(
        RecordType::from_u8(0x02).expect("0x02 must round-trip"),
        RecordType::RegistryBump,
        "RecordType::RegistryBump = 0x02 must remain after Plan 12.7-05 (v0 events-only path)"
    );
}
