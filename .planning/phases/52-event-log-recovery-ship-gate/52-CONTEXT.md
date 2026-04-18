# Phase 52: event-log-recovery-ship-gate - Context

**Gathered:** 2026-04-18
**Status:** Ready for planning

<domain>
## Phase Boundary

Final v1.2 phase — combines Wave 4 (per-shard event log layout, parallel recovery, fork/replica re-hash, reshard CLI, snapshot v8 + hard-fail boot guard) with Wave 5 (N=1↔N=K proptest parity harness, 1M+ EPS load test, Pareto-workload benchmark cell, architecture docs). Ships the three ship-gate criteria for merging v1.2 to main; after this phase, DashMap + ArcSwap compat shims are deleted from `StateStore`.

Covers 7 REQs: TPC-INFRA-06 · TPC-CORR-02 · TPC-CORR-05 · TPC-CORR-06 · TPC-PERF-07 · TPC-DX-03 · TPC-DX-04.

**Ship-gate (the three merge-to-main criteria):**
1. 9-cell matrix within −5% of baseline at N=1 (migration-compat gate).
2. ≥3× baseline on `complex-c8-x8` at N=CPU_COUNT (architecture gate).
3. `shard_probe` cross_shard_fraction <40% on release workload (architectural-fit gate).

</domain>

<decisions>
## Implementation Decisions

### Event log directory layout

- **D-01:** New layout: `data/shard-{N}/streams/{stream_name}/log.bin`. Wave 1–3 kept the legacy `data/logs/{stream}.bin` path; Wave 4 performs the atomic rename as part of the snapshot-v8 migration. Recovery code reads the layout expected by the current snapshot version.
- **D-02:** Legacy `data/logs/` dir is emptied as part of migration and removed on first clean shutdown after migration. Document in `docs/operations.md` that operators should not manually write there.

### Snapshot v8 backward-compat strategy

- **D-03:** **Read v7 AND v8; write v8 only.** On boot, the snapshot loader inspects the first 16 bytes for a v8 magic/version tag. v7 snapshots (pre-Wave-1, v1.0-launch format) are read with `shard_count = 1` defaulted. First snapshot after boot is written as v8 with the explicit `shard_count: u16` field. Preserves zero-downtime upgrade from v1.0-launch data dirs — operators boot v1.2 against existing data, first snapshot cycle migrates the format.
- **D-04:** Read-side v7 support is maintained through v1.2 and v1.2.x; v1.3 drops v7 read (documented deprecation). Hard-fail boot guard (TPC-CORR-02) triggers when the snapshot is v8 AND `snapshot.shard_count != BEAVA_SHARDS` — not when v7 is seen (v7 implies shard_count=1, which fails or succeeds the guard against BEAVA_SHARDS normally).

### Parallel recovery

- **D-05:** **N recovery threads — one per shard.** Each shard spawns a dedicated recovery task on its own pinned thread; reads its own per-shard log; replays into its own WatermarkState + state store. Main thread coordinates via the same boot-barrier as Phase 50 D-01 (listeners bind only when every shard passes "recovered"). No `BEAVA_RECOVERY_THREADS` env — start simple; add if operators request. IO-bound workloads will be SSD-ceiling-limited regardless.

### Reshard CLI tool (TPC-DX-03)

- **D-06:** Binary: `tally reshard --from N --to K --data-dir /var/lib/beava --output /var/lib/beava-new [--replace]`. Offline-only (refuses to run if a server is holding the data-dir lock). Reads source data-dir's snapshot + per-shard logs; replays every entry through `hash(event.key) mod K` to route to the new shard layout; writes v8 snapshot with `shard_count=K` to output dir. Original data-dir untouched until `--replace` swaps the dirs atomically (via rename). Downtime = tool runtime.
- **D-07:** Rehashing logic lives in a shared `reshard` module used by: the CLI tool AND the in-process reshard path when an operator sets `BEAVA_SHARDS=K` against an existing data dir and chooses the hard-fail-and-run-reshard path.

### Fork/replica re-hash + LSN-based dedup (TPC-CORR-06 scope expansion)

- **D-08:** **Fork/replica ingest always re-hashes on arrival** by `hash(event.key) mod downstream_N`. Upstream `shard_hint` in `OP_LOG_FETCH` metadata is a fast-path optimization hint (skip rehash when `upstream_N == downstream_N` and key-space partition matches). No `--reshard-from` CLI flag.
- **D-09:** **LSN-based dedup** — scope expansion on TPC-CORR-06 accepted by user 2026-04-18. Every log entry gains a monotonic LSN `(stream_ord, upstream_shard_id, seq)` where `seq` increases per (stream, shard) on the upstream. Replica tracks `max_lsn_seen(stream, upstream_shard)` persistently (in snapshot v8 metadata). On `OP_SUBSCRIBE` reconnect, replica discards events whose LSN is ≤ `max_lsn_seen` for its (stream, upstream_shard) pair. Closes the upstream-rolling-restart double-emit window identified in PITFALLS.md §5.2.
- **D-10:** LSN format: `u64` packed as `(upstream_shard_id: u8) | (stream_ord: u16) | (seq: u40)`. 40-bit seq per (stream, upstream_shard) supports ~1 trillion events per pair — sufficient for any realistic stream lifetime. Planner: confirm packing choice fits postcard encoding cleanly.
- **D-11:** Wave 4 snapshot v8 adds `replica_lsn_map: HashMap<(StreamName, UpstreamShardId), u64>` field. `#[serde(default)]` so pre-v8 snapshots load as empty map (no prior dedup state — standard v1.0-launch upgrade is a fresh replica).

### /ready shard-recovery awareness (TPC-INFRA-06)

- **D-12:** `GET /ready` returns 503 until EVERY shard completes recovery replay. `GET /health` stays 200 from process start (process-is-alive semantics). Recovery completion uses the same barrier primitive as Phase 50 D-01 boot-barrier — add a "recovered" sub-state to the existing ready condition. Observable via `/debug/shards.ready` field per Phase 51 D-09.

### N=1 ↔ N=8 proptest parity harness (TPC-CORR-05, pre-ship gate)

- **D-13:** **All operators scope.** Property test in `tests/proptests/sharding_parity.rs` (proptest-driven). Generator produces correlated event streams (deterministic seed) and applies them to:
  - One `N=1` engine instance (golden reference).
  - One `N=8` engine instance (test target).
  After each shrunk batch, assert: for every key in the key space, `features_at(N=1, key) == features_at(N=8, key)` at every event-time bucket, for every operator type — filter, map, agg (all sketches), join, fork. Fork/replay parity test in same harness.
- **D-14:** Runtime budget: ≤10 min in CI nightly job (extends `bench-nightly.yml` from Phase 48). Per-PR runs a quick smoke variant (≤30s, 1000 events) to catch obvious regressions without blowing up PR CI. Hard pre-merge gate.

### Pareto-workload benchmark cell (TPC-PERF-07)

- **D-15:** Add one new cell `pareto-c8-x8` to the 9-cell matrix — same shape as `complex-c8-x8` but with an 80/20 Zipf key distribution (20% of keys receive 80% of events). Measures `shard_probe` cross_shard_fraction on the skewed workload. Ship-gate: cross_shard_fraction <40% on this cell.

### Docs (TPC-DX-04)

- **D-16:** New `docs/architecture-tpc.md` — deep-dive covering shard model, routing, joins, operational posture, reshard workflow, ship-gate rationale. Include the target-architecture diagram from `TPC-SHARD-DESIGN.md` § "Target architecture." Treat as the TPC explainer for new contributors + operators.
- **D-17:** Update `docs/operations.md` with "Shard Sizing & Hot-Shard Diagnosis" section citing `beava_shard_keys_owned`, `shard_probe`, `BEAVA_HOT_SHARD_THRESHOLD`, the reshard workflow, and the three ship-gate criteria as health checks.

### DashMap + ArcSwap removal

- **D-18:** After Wave 4 migrations land and ship-gate passes, DELETE `dashmap` and `arc-swap` crates from `Cargo.toml`. Remove compat-shim code from `src/state/store.rs`. This is the final cleanup step of the phase — run AFTER all tests green, before final verification.

### Claude's Discretion

- Exact LSN packing-bit boundary layout (upstream_shard_id width — 8 vs 16 bits): planner picks based on realistic upstream-shard maximum.
- Proptest generator for correlated event streams (hand-rolled strategy vs `proptest`'s `Strategy` trait chaining): planner picks.
- Reshard CLI output format (JSON progress log vs plain-text): planner picks; match existing `tally` CLI conventions.

</decisions>

<canonical_refs>
## Canonical References

### Design + research
- `.planning/arch/TPC-SHARD-DESIGN.md` §4 "Per-shard event log", §7 "Migration compatibility" (locked 2026-04-18 snapshot-mismatch guard), Q4 (fork re-sharding — user scope-expanded to LSN dedup).
- `.planning/arch/TPC-RESEARCH.md` §7 Still-open #1 (LSN-based dedup rolling-restart window — now closed by D-09).
- `.planning/research/PITFALLS.md` §5.2 (fork/replica double-count window — closed by D-09).
- `.planning/research/SUMMARY.md` §"Wave 4" + §"Wave 5" ship list.
- `.planning/research/ARCHITECTURE.md` §1 Wave 4 row; §3 migration-compat specifics.
- `.planning/research/STACK.md` §4 (snapshot format migration).

### Requirements
- `.planning/REQUIREMENTS.md` — TPC-INFRA-06, TPC-CORR-02, TPC-CORR-05, TPC-CORR-06, TPC-PERF-07, TPC-DX-03, TPC-DX-04.

### Upstream phases
- Phase 48 D-01 (routing function); Phase 49 D-07 (StreamDefinition.shard_key field — snapshot v8 serializes it).
- Phase 50 D-01 (boot-barrier — reused for recovery barrier), D-06/07 (metrics in parallel).
- Phase 51 D-01 (global-watermark atomic storage — snapshot v8 persists last-published values).

### Existing code
- `src/state/store.rs` — snapshot read/write; v7→v8 migration landing here.
- `src/state/event_log.rs` — per-stream log today; extended with per-shard layout + LSN tagging.
- `src/server/replica.rs` (or equivalent OP_LOG_FETCH / OP_SUBSCRIBE handlers) — replica dedup logic lands here.
- `src/main.rs` — `tally reshard` subcommand added.
- `benchmark/` — 9-cell matrix harness extended with `pareto-c8-x8` cell.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- Phase 50's boot-barrier primitive — reused for recovery completion signaling.
- Phase 51's `GlobalWatermarkStore` — persisted in snapshot v8 so replicas see consistent global watermarks after restart.
- `tally` CLI subcommand pattern (`tally serve`, `tally fork`, `tally suggest-config`) — `tally reshard` follows the same clap-based shape.

### Established Patterns
- Snapshot format migrations in Beava have historically used a version tag + `#[serde(default)]` on new fields. v7→v8 follows the same pattern — no breaking wire-format changes.
- Offline tools that read the data-dir acquire a file lock before running (see existing `tally suggest-config`); `tally reshard` follows that pattern, refuses to run against a locked dir.

### Integration Points
- Snapshot loader at `src/state/store.rs` — version-dispatch between v7 read path and v8 read path.
- Replica ingest path (`src/server/replica.rs` or equivalent) — rehash + LSN dedup landing site.
- `/health` and `/ready` handlers in `src/server/http.rs` — update `/ready` to gate on recovery completion.
- `bench-nightly.yml` (from Phase 48) — extended with sharding-parity proptest job + Pareto cell.

</code_context>

<specifics>
## Specific Ideas

- **LSN dedup (D-09/10/11)** is a user-accepted scope expansion beyond the original TPC-CORR-06 wording. Adds ~1 week of work; closes a real correctness window. Document in the Wave 4 SUMMARY as a scope note so future readers see the lineage.
- **Read v7 + v8 strategy** is the highest-leverage upgrade-compat move: every v1.0-launch operator can boot v1.2 against their existing data dir, no migration tool needed for the common N=1→N=1 case. The reshard tool is only needed for N=1→N>1.
- **All-operators parity proptest** (D-13) is the hard merge gate. Budget generously for this in the plan — 1–2 plans dedicated to test scaffolding + generator design alone.
- **DashMap/ArcSwap deletion** runs LAST in the phase. Sequencing: migrations → parity test green → ship-gate benchmarks → DashMap/ArcSwap removal → final 9-cell matrix run → verification.

</specifics>

<deferred>
## Deferred Ideas

- Live (online) reshard without downtime → v1.3 or later. v1.2 ships offline-only.
- v7 snapshot read-support removal → v1.3 (announce deprecation in v1.2 release notes).
- NUMA-aware shard placement → Beava Cloud era.
- compio runtime migration → v1.3 / Beava Cloud.
- Hot-key salting framework support → application-level; surface via metrics only.

</deferred>

---

*Phase: 52-event-log-recovery-ship-gate*
*Context gathered: 2026-04-18*
