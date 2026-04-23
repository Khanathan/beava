# Phase 7 Plan 02 — Serde + SnapshotBody — Summary

**Status:** complete
**Commits:** `e0d0183` (test red), `d526e58` (feat green)
**Tests added:** 15 in `crates/beava-core/tests/snapshot_body_roundtrip.rs`

## What shipped

- Serde `Serialize`/`Deserialize` derives across the AggOp family:
  - `Value` (in `row.rs`) — including `Bytes(Vec<u8>)` (serde default byte-seq encoding) and `Datetime(i64)`.
  - `AggKind` enum.
  - `AggOp` enum (all 9 variants including `Windowed(Box<WindowedOp>)`).
  - All 7 *State structs: `CountState`, `SumState`, `AvgState`, `MinState`, `MaxState`, `VarianceState`, `RatioState`.
  - `WindowedOp` — required custom serde adapters for `[Option<Box<AggOp>>; 64]` and `[i64; 64]` since serde's default array impl tops out at 32 elements. Adapters live in `agg_windowed.rs::serde_array_64` / `serde_array_64_i64` modules.
  - `EntityKey` (in `agg_state_table.rs`).

- New `beava_core::snapshot_body` module exposing:
  - `SNAPSHOT_BODY_FORMAT_VERSION: u16 = 1`
  - `RegistryDescriptorsOnly { version, events, tables, derivations }` — projection of `RegistryInner` that drops runtime caches (compiled chains, compiled aggregations, feature index). Caches re-hydrate via `Registry::install_from_descriptors` in Plan 03's recovery path.
  - `From<&RegistryInner> for RegistryDescriptorsOnly`.
  - `SnapshotBody { body_format_version, registry, state_tables, next_event_id, max_event_time_ms }` with `from_live` / `encode` / `decode` / `into_parts`.
  - `SnapshotBodyError` (Bincode + UnsupportedVersion variants).
  - Type alias `SerializedStateTables = BTreeMap<String, Vec<(EntityKey, Vec<AggOp>)>>` for clippy `type_complexity` cleanliness.

- `bincode` promoted from no-dep to runtime dep on beava-core (`encode` is a runtime API, not just dev-tools).

## Test coverage

15 round-trip tests:
- Per-AggOp-variant: count, sum, avg, min, max, variance, stddev, ratio, windowed sum.
- Value round-trip across all 7 variants.
- EntityKey round-trip.
- Empty SnapshotBody round-trip + byte-equivalence on re-encode.
- Version mismatch → `Err(UnsupportedVersion(99))`.
- Registry descriptors preserved (events + tables + derivations).
- Full state-tables round-trip (2 nodes × 3 entities × 2 ops each, query equivalence after decode).

## Notes / deviations

- `AggOp` does NOT derive `PartialEq`. State variants carry F64 fields with NaN-aware semantics that don't satisfy the reflexivity required by PartialEq. Tests that need equality use bincode round-trip + state queries instead of struct equality. `SnapshotBody` therefore also drops PartialEq from its derive list (verified via re-encode byte equivalence).

- `AggOpDescriptor` (which holds `Option<Arc<Expr>>`) is intentionally NOT serde-derived. Snapshots carry per-entity AggOp *state*, not register-time descriptors. Recovery rebuilds compiled chains by re-applying RegistryBump records via `apply_registration` (Plan 03).

## Gates

- `cargo test --package beava-core --test snapshot_body_roundtrip` → 15/15 pass.
- `cargo test --workspace --features beava-server/testing` → 601 → 616 (no regression).
- `cargo clippy -- -D warnings` clean.
- `cargo fmt --all --check` clean.
