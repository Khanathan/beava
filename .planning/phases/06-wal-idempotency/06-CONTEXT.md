# Phase 6: WAL + idempotency - Context

**Gathered:** 2026-04-23 (auto mode)
**Status:** Ready for planning

<domain>
## Phase Boundary

**This phase delivers:** durable event ingest. `/push` (HTTP) and `op=push` (TCP, handler stub may remain reserved) accept an event, write it to a WAL with CRC-checked framing, wait for group-commit fsync, and only then return an ACK `{ack_lsn, idempotent_replay, registry_version}`. Stream-level `dedupe_key`+`dedupe_window` declared at `@bv.event` registration time are enforced: a duplicate request within TTL returns the **byte-identical** cached response and does NOT mutate state or write a new WAL entry.

**In scope:**
- New `beava-persistence` crate (WASM invariant preserved — `beava-core` stays syscall-free)
- Append-only WAL file with length-prefixed + CRC32C-framed records
- Group-commit fsync worker (1–5ms coalesce OR 1MB buffer whichever first)
- `/push/{event_name}` HTTP endpoint wiring: schema validation → dedupe lookup → WAL append → await fsync → apply to in-memory state → ACK
- Idempotency cache: `HashMap<(stream_id, dedupe_key), CachedResponse>` with dedupe_window TTL (lazy expiry + periodic sweep)
- WAL rotation into size-bounded segments (default 128MB); truncation of segments fully covered by latest snapshot LSN (Phase 7 will wire the "latest snapshot LSN" source; Phase 6 uses a test-injected watermark)
- Crash-recovery UAT: kill-before-fsync → event absent on restart; kill-after-ACK → event present after Phase 7 recovery lands (Phase 6 asserts the durability invariant on disk; Phase 7 wires the replay path)
- Criterion microbench: single-writer append+wait_for_fsync + group-commit coalescing (PERF-03 tripwire)

**Out of scope (other phases):**
- Full recovery/replay from WAL → Phase 7
- Snapshotting of in-memory state → Phase 7
- `/push-sync` response with computed features → Phase 12 (apply is in place from Phase 5; /push-sync still needs FeatureResult shape + push_many wiring which belong in Phase 12)
- `/push-batch` end-to-end → Phase 12
- TCP `op=push` handler → Phase 12 (Phase 2.5 reserved the opcode; handler stays `op_not_implemented` until 12)
- Schema evolution across registry bumps → Phase 7 (SRV-RECOV-05)

**Requirements covered:** SRV-DUR-01, SRV-DUR-02, SRV-DUR-03, SRV-DUR-04, SRV-DUR-05, SRV-API-03 (partial — returns `{ack_lsn, idempotent_replay, registry_version}`; features deferred to /push-sync in Phase 12), PERF-03 (tripwire measurement).

</domain>

<decisions>
## Implementation Decisions

### Crate Layout

- **D-01:** Introduce a new crate `crates/beava-persistence` for WAL code. Preserves the `beava-core` WASM-portability invariant (codified 2026-04-23) — all fs/sync code stays outside core. Phase 7 will extend the same crate with snapshot + recovery.
  *Auto-selected: recommended. Alternatives considered: (a) put WAL in `beava-server` — rejected because snapshotter + recovery belong together, (b) put in `beava-core` — rejected, violates syscall-free invariant.*

### WAL File Format

- **D-02:** Record framing: `[u32 length][u32 crc32c][u64 lsn][u8 record_type][payload]`. `record_type` enum: `0x01 Event`, `0x02 RegistryBump` (phase-7-ready, unused in 6). `payload` for Event = serde_json bytes of `{registry_version, stream_id, event_time_ms, entity_key, event_body}`. CRC32C covers `[lsn][type][payload]`.
  *Auto-selected: recommended. Alternatives: raw postcard (rejected: v1 serialization ceiling lesson); bincode (rejected: stability concerns). JSON body keeps Phase 6 simple; MessagePack hot-path migration is a later optimization — baseline fsync bench captures today's cost.*

- **D-03:** Segment file naming: `wal-<start_lsn_16hex>.log`. Default segment size: 128 MiB. New segment opened lazily when current exceeds threshold after fsync boundary.
  *Auto-selected: standard Postgres/Redis-style naming.*

- **D-04:** File header (once per segment): magic `b"BEAVAWAL"` + u32 format_version (=1) + u64 start_lsn + u32 registry_version_at_creation. Checked on open; mismatch = corruption error with operator message.
  *Auto-selected: recommended. Lets Phase 7 detect format drift across upgrades.*

### Group-Commit Strategy

- **D-05:** Single background "fsync worker" task. Push handlers:
  1. serialize record, append to in-memory staging buffer + LSN assignment (atomic counter)
  2. push record bytes onto an MPSC channel to the fsync worker
  3. subscribe to a `tokio::sync::watch<u64>` "durable_lsn" watermark
  4. await until watermark ≥ their assigned LSN
  5. return ACK
  
  Fsync worker loop:
  - drain channel (non-blocking) up to 1 MiB or 5 ms whichever first
  - write batch to file
  - `file.sync_data()` via `spawn_blocking`
  - bump watermark to highest LSN in batch
  
  *Auto-selected: recommended. Matches SRV-DUR-01 (1–5ms OR 1MB). Tokio watch channel is the right primitive for "wait for LSN" fanout.*

- **D-06:** Default fsync coalesce interval: 2ms (midpoint of 1–5ms range; configurable via `BEAVA_WAL_FSYNC_INTERVAL_MS`). Default flush size: 1 MiB (`BEAVA_WAL_FSYNC_BYTES`).
  *Auto-selected: recommended defaults.*

### Idempotency Cache

- **D-07:** Structure: `HashMap<(StreamId, DedupeKey), CachedEntry { response_bytes: Bytes, inserted_at: u64, expires_at: u64 }>`. Keyed on `(registered_stream_id, string-valued dedupe_key extracted from event per @bv.event config)`.
  *Auto-selected: recommended. Simple and correct for v0.*

- **D-08:** Expiry strategy: **lazy on lookup** (check expires_at; treat expired as miss) + **periodic sweep task** every 60s scanning for expired entries. No LRU cap in v0 — operator sizes box; memory grows bounded by `dedupe_window × push_rate`.
  *Auto-selected: recommended. Matches "no SSD overflow, size your box" project stance.*

- **D-09:** Cache lookup happens BEFORE WAL append. On hit: return cached response (same HTTP body bytes); no WAL entry written, no state mutation, response includes `"idempotent_replay": true`. On miss: proceed with apply + WAL, then insert `(key, response_bytes)` atomically.
  *Auto-selected: recommended. Byte-identical requirement forces caching the fully-serialized response.*

- **D-10:** Events without `dedupe_key` configured on their `@bv.event` descriptor skip the cache entirely. Response shape still includes `"idempotent_replay": false` for API consistency.
  *Auto-selected: recommended.*

### `/push` Endpoint

- **D-11:** Introduce `POST /push/{event_name}` in `beava-server/src/push.rs` (new module). Request = JSON event body. Response 200: `{"ack_lsn": u64, "idempotent_replay": bool, "registry_version": u32}`. Response 400: validation error (unknown event, schema mismatch). Response 409: registry_version mismatch (client sent stale x-registry-version header — optional; v0 accepts without header). Response 503: WAL full / fsync unhealthy.
  *Auto-selected: recommended. Satisfies SRV-API-03 partial — features field arrives with /push-sync in Phase 12.*

- **D-12:** Apply order per push: (1) parse + schema-validate against registered `@bv.event`, (2) dedupe lookup, (3) assign LSN + write-to-WAL-stage + enqueue-fsync, (4) apply event to aggregations **BEFORE** awaiting fsync (the in-memory state reflects the event optimistically; if fsync fails, process aborts — no "unbackout" problem because we crash), (5) await durable_lsn, (6) insert into idempotency cache, (7) return ACK.
  *Auto-selected: recommended. Pre-fsync apply minimizes latency and matches Redis-AOF pattern. On fsync failure we panic — operator restarts and the un-fsynced event is gone (correct per success criterion #1).*

### WAL Rotation + Truncation

- **D-13:** Rotation trigger: current segment post-fsync size ≥ 128 MiB → close, open new segment with next start_lsn. Closed segments retained until explicitly truncated.
  *Auto-selected: recommended.*

- **D-14:** Truncation API: `WalWriter::truncate_up_to(snapshot_covered_lsn: u64)` deletes any **closed** segment whose last LSN < `snapshot_covered_lsn`. Current segment never deleted. Phase 6 exposes this API + unit tests it; Phase 7 calls it from the snapshot task.
  *Auto-selected: recommended. Clean hand-off to Phase 7.*

- **D-15:** Phase 6 smoke test injects a fake "snapshot covered LSN" to exercise truncation end-to-end. This is the UAT vehicle for success criterion #4.
  *Auto-selected: recommended.*

### Durability UAT (success criterion #1)

- **D-16:** Crash-before-fsync test uses a subprocess-spawning integration test (similar to `cli_smoke.rs`): parent spawns beava binary with `BEAVA_WAL_FSYNC_INTERVAL_MS=999999` (forces indefinite coalesce) + `BEAVA_WAL_FSYNC_BYTES=99999999`; parent sends one /push; parent SIGKILLs before fsync worker drains. Parent then opens the WAL segment with `WalReader`, asserts the event record is NOT present (tail truncation via CRC check handles the torn last record if fsync partially wrote — which it won't here because we never signaled fsync).
  *Auto-selected: recommended. Phase 7 adds the "restart and replay" half; Phase 6 proves the disk-level durability invariant.*

- **D-17:** Crash-after-ACK test: same harness but let fsync complete before SIGKILL. Parent reopens segment with `WalReader`, asserts event IS present and CRC-valid.
  *Auto-selected: recommended.*

### Perf Microbench (PERF-03 tripwire)

- **D-18:** `crates/beava-persistence/benches/phase6_wal.rs` with criterion. Benches:
  - `wal/append_nofsync` — single-writer append to segment, no fsync (measures serialization + CRC cost)
  - `wal/append_fsync_2ms_coalesce` — single push waits for fsync under default 2ms coalesce (measures P50 fsync overhead)
  - `wal/append_fsync_burst_1k` — 1000 concurrent pushes through group-commit (measures amortized fsync per push under load)
  
  Baselines captured into `.planning/perf-baselines.md` keyed on hw-class. 10% regression = WARNING; 25% = BLOCKER per CLAUDE.md §Performance Discipline.
  *Auto-selected: recommended. Meets Phase 6+ mandatory bench requirement.*

- **D-19:** Success criterion #3 (P50 fsync overhead < 2ms) verified by the `wal/append_fsync_2ms_coalesce` bench. If hardware consistently fails this, gate is a WARNING and we note "hw-class limited"; the 3M EPS/core throughput gate is the real ship gate in Phase 13.
  *Auto-selected: recommended, matches perf-discipline-doc "measurement not optimization".*

### TDD Discipline (mandatory, Phase 3 onward)

- **D-20:** Every plan task splits `Task N.a (red)` writing failing test(s) committed as `test(06-PP): subject`, then `Task N.b (green)` implementing with commit `feat(06-PP): subject` (or `chore/refactor:`). Proptests count as red. `cargo test --workspace`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, `cargo fmt --all --check` must be green per commit.
  *Auto-selected: mandatory convention — not actually a choice.*

### Claude's Discretion

- Exact WAL buffer implementation (e.g., `BytesMut` vs `Vec<u8>`) — planner chooses.
- Whether to use `std::fs::File` behind `spawn_blocking` vs `tokio::fs::File` for the fsync worker — planner benchmarks both in the perf pass. Default leaning: `std::fs::File` + `spawn_blocking` (deterministic fsync semantics, smaller tokio-fs surface).
- CRC library choice: `crc32c` crate vs `crc` crate. Leaning: `crc32c` (hardware-accelerated on x86-64 + ARMv8).
- LSN type: `u64` locked (enough for ~18 quintillion events). Wrap-handling: not in v0.
- Stream ID encoding in WAL record: small `u32` assigned at registration time (already exists conceptually in registry — planner confirms and wires).

### Folded Todos

None matched for Phase 6.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Requirements + Roadmap

- `.planning/REQUIREMENTS.md` §SRV-DUR (01–05) — durability requirements
- `.planning/REQUIREMENTS.md` §SRV-API-03 — /push endpoint contract
- `.planning/REQUIREMENTS.md` §SDK-DEC-09 — `dedupe_key`/`dedupe_window` config on @bv.event
- `.planning/REQUIREMENTS.md` §PERF-03 — WAL group-commit overhead P50 < 2ms
- `.planning/ROADMAP.md` §"Phase 6: WAL + idempotency" — goal, depends, success criteria
- `.planning/PROJECT.md` §Constraints — "single process, single thread, WAL + periodic snapshot"
- `.planning/PROJECT.md` §"Key Decisions" — "beava-core stays WASM-portable (syscall-free invariant)" — mandates new persistence crate

### Conventions + discipline

- `CLAUDE.md` §Conventions → TDD Discipline — red-green-refactor commit pattern (mandatory)
- `CLAUDE.md` §Performance Discipline — Phase 6+ criterion bench required; 10% / 25% regression thresholds
- `.planning/perf-baselines.md` — hw-class baseline table; append Phase 6 rows on bench landing

### Prior phase handoffs

- `.planning/phases/02-sources-registry-version-bumps/02-CONTEXT.md` — registry version-bump semantics (WAL records must stamp `registry_version`)
- `.planning/phases/02.5-tcp-wire-listener/02.5-CONTEXT.md` + `wire.rs` — TCP opcode `push` reserved but NOT wired in Phase 6 (stays `op_not_implemented`)
- `.planning/phases/05-aggregation-framework-core-operators/` — `apply_event_to_aggregations` signature (Phase 6 /push calls this after WAL stage)
- `.planning/phases/05.5-perf-harness-retroactive-baselines/` — criterion harness convention + capture script + hw-class recipe

### Existing code integration points

- `crates/beava-core/src/agg_apply.rs` — `apply_event_to_aggregations(source, row, event_time_ms, event_id, registry, tables)` is the apply target post-WAL-stage
- `crates/beava-core/src/registry.rs` — `Registry` holds `@bv.event` descriptors including (eventually) `dedupe_key`/`dedupe_window` fields; Phase 6 confirms these fields land in the descriptor (they're in the wire spec from Phase 2.5 defaults module)
- `crates/beava-core/src/defaults.rs` — default dedupe_window (24h), tolerate_delay (5s), keep_events_for (7d)
- `crates/beava-server/src/http.rs` — axum router wiring; Phase 6 adds `/push/{event_name}` route
- `crates/beava-server/src/server.rs` — `Server::bind` / startup; Phase 6 spawns fsync worker here and plumbs `WalWriter` handle into state
- `crates/beava-server/src/feature_query.rs` — `DevAggState` already wraps `state_tables` + `registry`; Phase 6 extends it with `WalWriter` handle (or introduces proper `AppState` / `ApplyState` struct)

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets

- **Registry**: `parking_lot::RwLock<RegistryInner>` — already the thread-safe registry; `@bv.event` descriptors carry (or will carry) `dedupe_key`/`dedupe_window` fields from Phase 2.5 devex rename.
- **Apply loop primitive**: `apply_event_to_aggregations()` in `beava-core` — Phase 6 calls this synchronously after WAL stage.
- **TestServer harness**: `crates/beava-server/src/testing.rs` — spawn-real-server pattern already used in `phase2_smoke.rs`, `phase2_5_smoke.rs`, `phase5_smoke.rs`. Extend for WAL path: parameterize WAL dir (tempdir).
- **Subprocess smoke pattern**: `crates/beava-server/tests/cli_smoke.rs` uses spawn + SIGTERM; Phase 6 crash tests need spawn + SIGKILL (already have `libc` dev-dep).
- **Criterion harness**: `.planning/phases/05.5-perf-harness-retroactive-baselines/` captured capture-script pattern — Phase 6 bench reuses it.

### Established Patterns

- **parking_lot over tokio::sync**: "no lock held across `.await`" is a Phase 2 lesson — Phase 6 fsync worker communicates via tokio MPSC + watch, not shared-state mutexes.
- **Validated→Applied split**: Phase 2's `ValidatedPayload` newtype pattern; Phase 6's push handler should have analogous `ParsedPush → StagedRecord → FsyncedRecord` typestate if complexity warrants, otherwise plain functions + doc comments.
- **cargo test / clippy / fmt gates** enforced every commit.
- **Single current_thread tokio runtime**: already in Phase 2.5; fsync worker runs on `spawn_blocking` so it doesn't block the apply thread.

### Integration Points

- **Router**: add `/push/{event_name}` to `crates/beava-server/src/http.rs:router(...)`.
- **AppState-ish**: `DevAggState` holds `registry + state_tables + next_event_id + max_event_time_ms`. Extend with `wal: Arc<WalWriter>` + `idempotency_cache: Arc<RwLock<IdemCache>>` OR promote to a proper `AppState` struct (planner's call; D-C3 leaning: dedicated `AppState`).
- **Config**: `beava-core/src/config.rs` — add `wal_dir: PathBuf`, `wal_fsync_interval_ms: u64`, `wal_fsync_bytes: u64`, `wal_segment_bytes: u64`, `dedupe_sweep_interval_secs: u64`.
- **CLI/env**: `BEAVA_WAL_DIR`, `BEAVA_WAL_FSYNC_INTERVAL_MS`, `BEAVA_WAL_FSYNC_BYTES`, `BEAVA_WAL_SEGMENT_BYTES`.
- **Shutdown**: `shutdown.rs` — graceful shutdown must flush pending fsync batch + close current segment cleanly; tests assert pending-ACK pushes get their ACK (or 503) before shutdown returns.

</code_context>

<specifics>
## Specific Ideas

- Idempotent-replay response **must be byte-identical** (success criterion #2). This drives D-07 / D-09: cache the serialized response, not a re-render.
- "Pre-fsync apply" (D-12) is the Redis-AOF pattern: apply to memory optimistically, then fsync, then ACK. On fsync failure the process panics — no rollback needed because the in-memory state dies with the process and the un-fsynced event is gone on restart.
- CLAUDE.md explicitly mentions "No cross-process coordination, no distributed consensus, no fancy replication" — Phase 6 is strictly single-process WAL.
- Phase 7 will wire the "snapshot covered LSN" watermark into `WalWriter::truncate_up_to`; Phase 6 ships the API with a test-injected value.
- `beava-persistence` crate name chosen to anticipate Phase 7 (snapshot) + future wal-replay code; single crate keeps the durability surface coherent.

</specifics>

<deferred>
## Deferred Ideas

- **TCP `op=push` handler wiring** — reserved opcode in Phase 2.5, handler lands with `/push-sync` + `push_many` in Phase 12.
- **`/push-sync` with computed features response** — Phase 12 (joins+API completion scope).
- **WAL replay on startup** — Phase 7.
- **Schema evolution across registry version bumps during replay** — Phase 7 (SRV-RECOV-05).
- **MessagePack WAL payload (0x02 content-type)** — deferred; Phase 6 ships with serde_json payload bytes. Revisit if bench shows encode cost matters vs fsync cost.
- **LRU cap on idempotency cache** — not in v0; operator sizes box. Revisit if operators report OOM from dedupe cache.
- **Cross-instance idempotency / replication** — commercial tier; out of OSS forever.
- **Recovery RTO bench** — Phase 13 perf gate (SRV-RECOV-03).

### Reviewed Todos (not folded)

None — no pending todos matched Phase 6 scope.

</deferred>

---

*Phase: 06-wal-idempotency*
*Context gathered: 2026-04-23 (auto mode — all gray areas selected, recommended options applied inline)*
</content>
</invoke>