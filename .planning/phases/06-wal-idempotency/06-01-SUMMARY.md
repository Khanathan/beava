---
phase: 06-wal-idempotency
plan: 01
status: complete
shipped: 2026-04-23
---

# Plan 06-01: beava-persistence crate + WAL record frame + WalWriter/WalReader

## Commits

- `2040a81` — test(06-01): add WAL writer/reader round-trip + CRC + torn-record tests (RED)
- `d34d265` — feat(06-01): implement WAL record frame + WalWriter/WalReader (no fsync) (GREEN)

## Files created

| File | Lines | Role |
|---|---:|---|
| `crates/beava-persistence/Cargo.toml` | 20 | Crate manifest |
| `crates/beava-persistence/src/lib.rs` | 35 | Public API: Lsn, RecordType, WalRecord |
| `crates/beava-persistence/src/error.rs` | 30 | PersistError enum |
| `crates/beava-persistence/src/record.rs` | 135 | Frame encode/decode + CRC32C |
| `crates/beava-persistence/src/writer.rs` | 75 | WalWriter (no fsync) |
| `crates/beava-persistence/src/reader.rs` | 65 | WalReader (CRC-verified, torn-tail safe) |
| `crates/beava-persistence/src/segment.rs` | 55 | Segment header I/O + filename |
| `crates/beava-persistence/tests/writer_reader.rs` | 195 | 7 round-trip + CRC + torn tests |

## Tests

Plan 01 persistence tests: 7/7 pass.

```
test round_trip_single_event ... ok
test round_trip_multiple_events ... ok
test segment_header_magic_mismatch ... ok
test segment_header_bad_version ... ok
test crc_mismatch_mid_stream ... ok
test torn_last_record_is_eof ... ok
test unknown_record_type_errors ... ok
```

Workspace total after Plan 01: 395 tests (no regression; persistence crate adds 7 new).

Gates green:
- `cargo test --workspace`: 395 passed
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: clean
- `cargo fmt --all --check`: clean (picked up a pre-existing fmt nit in `phase5_5_harness_smoke.rs`)

## Decisions honored

- D-01 Crate layout: new `beava-persistence` crate (WASM invariant preserved)
- D-02 WAL format: length + CRC32C + lsn + type + payload, little-endian
- D-03 Segment filename: `wal-<start_lsn_16hex>.log`
- D-04 File header: `BEAVAWAL` magic + u32 format_version=1 + u64 start_lsn + u32 registry_version

## Deviations

None from the plan. Minor additions:
- `RecordType::to_u8()` helper added beside `from_u8()` (symmetric API).
- `BufWriter` flush on `Drop` so tests don't have to call `close()` explicitly to read-after-write; actual durability via `sync_data()` comes in Plan 02.

## Handoff to Plan 02

- `WalWriter::sync_data()` is implemented but not called — Plan 02's fsync worker invokes it.
- `WalWriter::bytes_written()` exposes the size-since-header used by Plan 02's rotation trigger.
- Segment filename convention (`wal-<hex>.log`) is the input for Plan 02's `truncate_up_to` directory scan.
