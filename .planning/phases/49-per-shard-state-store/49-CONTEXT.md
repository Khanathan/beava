# Phase 49: per-shard-state-store - Context

**Gathered:** 2026-04-18
**Status:** Ready for planning

<domain>
## Phase Boundary

Introduce the `Shard` struct — the sole data path for per-entity state at `N_SHARDS=1`. Each `Shard` owns `AHashMap<EntityId, Row>` state, a plain `HashSet<String>` dirty-set, an in-shard `WatermarkState` (full relocation — no parallel old path), and a per-shard `EventLog` handle. Expose behind a `ShardedStateStore` trait so Waves 2+ can swap the impl (Vec-backed for N>1, fixed-array, future sled/custom). Add `BEAVA_SHARDS` env + `--shards` CLI with the env-wins-over-CLI contract and debug=1 / release=physical-cpus default. Add `@bv.stream(shard_key=...)` Python SDK surface with tuple support and "first dataclass field" fallback; persist the declared shard-key as a field on `StreamDefinition`.

DashMap + ArcSwap remain as StateStore compat shims — they are NOT deleted in this phase. They live alongside the new `Shard`-backed code path and are removed in Wave 4 (Phase 52). Event-log path layout stays at today's `data/logs/{stream}.bin` — the `data/shard-N/…` migration is Wave 4.

Ship-gate: full test suite green at `BEAVA_SHARDS=1`; 9-cell benchmark matrix within −5% of committed baseline at N=1 (migration-compat gate).

</domain>

<decisions>
## Implementation Decisions

### StateStore facade shape

- **D-01:** Introduce `trait ShardedStateStore` (abstract shape, methods like `get(key)`, `set(key, row)`, `shard_for(key) -> impl ShardOps`, `for_each_shard(f)`, `shard_count() -> u16`). Concrete impl `ShardedStateStoreV1` (v1 = Wave 1-5 impl) that wraps `Vec<Arc<Shard>>` + a router. Keeps today's existing `StateStore` type alive as a compat shim that delegates to `ShardedStateStoreV1` at N=1. The trait shape is deliberately flexible — Waves 2-4 can introduce alternate impls (fixed-array `[Shard; N]`, experimental sled-backed) without rewriting callers.
- **D-02:** Router uses the `shard_hint` function from Phase 48 — `shard_hint_for_event(event, key_field) mod shard_count`. At N=1, always shard 0. The trait's `shard_for(key)` method is the single call site that multiplies this out.

### Event log path convention

- **D-03:** Wave 1 keeps the **current `data/logs/{stream}.bin` layout**. The `Shard` struct's `EventLog` handle points to the existing-format log at N=1. Per-shard directory layout (`data/shard-N/streams/{name}/log.bin`), snapshot v8 with `shard_count: u16` header, hard-fail boot guard — **all three deferred to Wave 4 (Phase 52)**. Rationale: keeps Wave 1 diff tractable; preserves the no-flag-day guarantee (N=1 Wave 1 server boots against a v1.0-launch data dir identically); decouples the risky layout-migration from the structural Shard introduction.

### WatermarkTracker migration strategy

- **D-04:** **Full relocation in Wave 1.** The one DashMap-backed `WatermarkTracker` is replaced by per-shard `WatermarkState` living inside `Shard`. At N=1, global watermark for any stream is shard 0's watermark (identity). Wave 3 (Phase 51) then adds the lazy global-publish mechanism atop the per-shard state (TPC-PERF-06) — Wave 3's work becomes purely additive, no unwinding of a Wave 1 shim.
- **D-05:** The existing `observed_max(stream)` API used by TTL eviction (see v1.0-launch CORR-07 in eviction.rs:63) continues to work — it now returns the shard-local value (at N=1 this is identical to the old global). Wave 3 adds an explicit `global_watermark(stream)` API for callers that need the cross-shard min; TTL eviction continues using the shard-local value per design doc §5.
- **D-06:** No parallel path. No facade delegation. `WatermarkTracker` as a standalone type is deleted in Wave 1; all its consumers move to the per-shard API.

### Python SDK `shard_key` server-side storage

- **D-07:** Add field to `StreamDefinition`:
  ```rust
  pub struct StreamDefinition {
      // ...existing fields...
      pub shard_key: Option<ShardKeySpec>,
  }
  pub enum ShardKeySpec {
      Single(String),              // "user_id"
      Tuple(Vec<String>),          // ("region", "user_id")
  }
  ```
  `Option` because pre-Wave-1 streams (persisted in v1.0-launch data dirs) have no declared shard_key; at load time they deserialize as `None`, which at N=1 falls back to the primary-key heuristic harmlessly.
- **D-08:** `#[serde(default)]` on the new field so pre-Wave-1 snapshots deserialize cleanly (no snapshot-format bump needed in Wave 1 — the actual `shard_count: u16` bump is a Wave 4 concern). postcard wire format handles this via absent-field = default.
- **D-09:** Python SDK `@bv.stream(shard_key=...)` accepts `str | tuple[str, ...] | None`. On registration over TCP/HTTP the SDK serializes to `ShardKeySpec` and sends it in the StreamDefinition payload. Fallback heuristic (primary-key field) is computed server-side when `shard_key` is `None`, **only** at registration time — avoids runtime re-computation on every event.

### BEAVA_SHARDS config surface

- **D-10:** Add `BEAVA_SHARDS` env var (u16, 1..=256) read once at startup. Add `tally serve --shards N` CLI flag. Env wins over CLI when both set (matches every other `BEAVA_*` override — see `src/server/shard_probe.rs` for the pattern). On macOS + `cfg(debug_assertions)` default to 1; on release default to `num_cpus::get_physical()`. Log the resolved value at INFO on startup.
- **D-11:** Wave 1 enforces `BEAVA_SHARDS == 1` at startup (log warn-once if the user sets N>1; proceed at N=1 regardless). Wave 2 (Phase 50) is where N>1 begins to be honored. Rationale: Wave 1 lands plumbing only; the first phase that can actually route to multiple shards is Wave 2 (which lands pinned threads + SPSC channels + SO_REUSEPORT).

### Claude's Discretion

- Exact trait method shape of `ShardedStateStore` (methods that are read-only vs mutating; whether `shard_for` returns `&Shard` or `ShardGuard`): planner picks per existing Beava idioms.
- Where `BEAVA_SHARDS` env parsing lives (new `src/config/shards.rs` vs extending existing config module): planner picks.
- How the `Shard` struct names its fields (bikeshedding): planner picks.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Design + research
- `.planning/arch/TPC-SHARD-DESIGN.md` §"Target architecture" (Shard struct shape), §5 "Watermark propagation across shards", §7 "Migration compatibility" (N=1 byte-compat rules), Q1/Q2 resolved (N_SHARDS default, macOS downshift).
- `.planning/arch/TPC-RESEARCH.md` §3 Q1 (physical cpus), Q2 (debug_assertions + warn-once).
- `.planning/research/SUMMARY.md` §"Wave 1: Per-shard state store".
- `.planning/research/ARCHITECTURE.md` §1 (module impact — `src/state/store.rs` as the primary surgery site; compat shims survive through Wave 3), §4 (arc-swap dirty-set survives as compat shim; Shard owns its own HashSet), §5 (WatermarkTracker relocation — Wave 1 vs later).
- `.planning/research/STACK.md` §1 (crate pins: `num_cpus 1.17`, `ahash` existing), §3 (BEAVA_SHARDS config surface pattern).
- `.planning/research/PITFALLS.md` §1.1 (inter-shard ordering — deferred to Wave 3 join ordering + Wave 5 parity test).

### Requirements
- `.planning/REQUIREMENTS.md` — TPC-INFRA-02 (BEAVA_SHARDS config), TPC-PERF-01 (Shard struct), TPC-DX-01 (shard_key Python SDK).

### Existing code
- `src/state/store.rs` — DashMap `entities`, ArcSwap `dirty_keys`, snapshot load/save (primary surgery site).
- `src/engine/event_time.rs` — `WatermarkTracker` (type to relocate), `SharedWatermarks` alias.
- `src/state/eviction.rs:63` — TTL eviction clock (reads from observed_max; post-Wave-1 reads from shard-local watermark).
- `src/engine/pipeline.rs` — PipelineEngine; owns StateStore today.
- `python/beava/__init__.py` (or `python-native/`) — `@bv.stream` decorator surface to extend.
- `src/main.rs` / CLI parsing — where `BEAVA_SHARDS` env + `--shards` flag hook in.

### Phase 48 inherit
- `.planning/phases/48-shard-hint-scaffolding/48-CONTEXT.md` — **D-01: shard_hint is a routing function, not a field.** Router in this phase calls `shard_hint_for_event(...)`, uses the result, discards it.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `ahash::AHasher` — already in tree; consumed by the shard_hint router function from Phase 48.
- `DashMap` + `ArcSwap<DashSet>` (`src/state/store.rs`) — kept alive in Wave 1 as compat shims alongside the new Shard-backed path. They are the primary readers of the existing state; StateStore continues to expose its current API while the new trait surfaces behind it.
- `num_cpus` crate — pin to 1.17 in Cargo.toml; new runtime dep.

### Established Patterns
- `BEAVA_*` env vars: read once at startup via `std::env::var`, log resolved value at INFO, no runtime reconfiguration. Example: `BEAVA_ENTITIES_SHARDS` in `src/state/store.rs:256`. Wave 2 renames or deprecates `BEAVA_ENTITIES_SHARDS` per TPC-INFRA-07 — Wave 1 leaves it alone.
- Trait boundaries in `src/state/` currently expose concrete types. Introducing `trait ShardedStateStore` is a net-new pattern; follow Rust idiom (trait in `src/state/shard/traits.rs`, impls next to it, callers take `&dyn ShardedStateStore` or `impl ShardedStateStore`).
- `cfg(debug_assertions)` gating is used throughout for dev-only asserts (grep `debug_assert!`). Use the same idiom for BEAVA_SHARDS default.

### Integration Points
- `src/state/store.rs` — introduce `Shard` struct alongside existing StateStore. Today's DashMap-backed methods stay working; new Shard-backed path is added.
- `src/engine/pipeline.rs` — PipelineEngine gains a `sharded_store: Arc<dyn ShardedStateStore>` (or the concrete type; planner decides per D-01).
- `src/engine/event_time.rs` — `WatermarkTracker` struct deleted; `WatermarkState` moves into `src/state/shard/watermark.rs` as a field of `Shard`.
- `src/state/eviction.rs` — continues using `observed_max(stream)` but the underlying implementation now reads shard-local state.

</code_context>

<specifics>
## Specific Ideas

- **Wave 1 is plumbing, not routing.** `BEAVA_SHARDS` env is parsed but actively enforced to 1 in this phase (warn-once if user sets higher). Wave 2 is where N>1 becomes meaningful.
- **The trait is the flexibility hedge.** `ShardedStateStore` makes it cheap to experiment in Wave 2+ (fixed-array `[Shard; N]`, sled-backed) without the ecosystem cost of rewriting call sites.
- **WatermarkTracker full relocation is the bigger Wave 1 risk.** Biggest diff in this phase. Test-suite fragility pitfall (PITFALLS.md §2) concentrates here. Property-test: at N=1, every watermark observation + query produces identical results to pre-Wave-1 behavior. Landing TPC-CORR-05 (N=1 ↔ N>1 parity) in Wave 5 is the safety net; Wave 1 should include at minimum an N=1-vs-main-branch golden watermark-sequence test.
- **Python SDK shard_key deserializing as None for old streams** is intentional. Pre-existing pipelines continue working at N=1 via the primary-key fallback. Warning is emitted at N>1 (that's a Wave 2 concern — TPC-DX-02).

</specifics>

<deferred>
## Deferred Ideas

- Event log path rename `data/logs/{stream}.bin` → `data/shard-N/streams/{name}/log.bin` → Wave 4 (Phase 52).
- Snapshot format v8 with `shard_count: u16` header + hard-fail boot guard → Wave 4 (TPC-CORR-02).
- DashMap + ArcSwap deletion from StateStore → Wave 4 when the compat shim is no longer needed.
- `BEAVA_ENTITIES_SHARDS` rename / deprecation → Wave 2 (TPC-INFRA-07).
- `ShardKeyMissingWarning` on `/debug/warnings` at N>1 → Wave 2 (TPC-DX-02).
- Pinned threads, SPSC channels, SO_REUSEPORT, core_affinity → Wave 2.
- `JoinShardKeyMismatch` register-time enforcement → Wave 3 (TPC-CORR-04). Wave 1 accepts whatever `shard_key` the Python SDK sends without cross-stream validation.

</deferred>

---

*Phase: 49-per-shard-state-store*
*Context gathered: 2026-04-18*
