---
phase: 19-1m-bench
gathered: 2026-04-26
status: ready-for-planning
mode: locked-decisions
---

# Phase 19 — 1M-EPS Bench Harness — Context

Saturation bench that pushes a fixed number of events (default 1,000,000) at the
server as fast as possible, isolated from per-event encoding cost on the bench
side, and reports `wall_clock_ms` + `send_drain_ms` + `ack_lag_ms` plus
sustained EPS. Ships with BOTH a Rust harness (`crates/beava-bench`) and a
Python harness (`python/benches/blast.py`) that drives the public Python SDK
(`bv.App + app.push()`) — so the published "Beava processes 1M events in <Xs"
numbers reflect both the curl/Rust path AND the realistic Python-client path
users will hit. Multi-size workload matrix (`small`, `medium`, `large`,
`large_phase9`) × 4 blast shapes × 2 pipelining modes × 2 transports × 2
languages, tabulated under a new `## 1M-event blast` section in
`.planning/throughput-baselines.md`.

The unlocking pre-condition is Phase 18 wrap (SUMMARY + verification): the 1M
ceiling is only meaningful once the hand-rolled hot path is the data-plane
runtime — measuring against the legacy `IoPool` would give a misleadingly low
number.

<domain>
## Phase Boundary

**In scope:**
- Finalize WIP `--total-events N` flag in `crates/beava-bench/src/bin/beava-bench-v18.rs` (cherry-pick from `git stash@{0}`)
- Pre-encoded frame pool of size N built at sender startup (setup time excluded from wall_clock)
- Four blast shapes: `fixed` / `uniform` / `zipfian` / `mixed_events` (see D-01)
- Two pipelining modes per blast: `--continuous-pipeline=true` (Phase 18 default) AND `--continuous-pipeline=false` (burst)
- New Python harness at `python/benches/blast.py` using public `bv.App + app.push()` over multi-process workers
- New `--isolation-mode` flag splitting timing into `wall_clock_ms` / `send_drain_ms` / `ack_lag_ms`
- Append rows to `.planning/throughput-baselines.md` under new `## 1M-event blast` section
- Architectural notes in this CONTEXT (and a section in the SUMMARY) so future bench changes don't accidentally regress measurement honesty

**Not in scope:**
- Linux Xeon coverage (Phase 18.5 / 18.6 prerequisite — Phase 19 baselines on M4 only)
- New operators / new transports / new wire formats
- io_uring exploration (Phase 18.5)
- Async (asyncio) Python harness (would need SDK surface additions)
- Cross-instance throughput (single-instance metric only — sharding is out of scope)

</domain>

<decisions>
## Implementation Decisions

### A. Blast shape (key variance)

- **D-01:** Ship four blast shapes via `--blast-shape={fixed,uniform,zipfian,mixed}` CLI flag. Each shape produces its own ledger row.
  - `fixed` — ONE pre-encoded frame, reused N times. Cache-warm marketing peak.
  - `uniform` — `user_id` rolls evenly over K keys. Cache-pessimistic floor.
  - `zipfian` — Zipfian distribution over K keys. Realistic fraud workload.
  - `mixed` — pool spans M registered events (e.g., login/click/transaction) sampled per push. Multi-stream realism.
- **D-02:** Pre-build a `Pool=N` vector of pre-encoded frames at sender startup, matching the chosen distribution. Sender iterates `0..N` once; no per-iteration RNG cost. Setup time (encoding the pool) is **excluded** from `wall_clock_ms`.
- **D-03:** All four shapes are published **side-by-side** in `.planning/throughput-baselines.md` — there is no single "headline" 1M-EPS number. The blog/README links to the table; users pick the shape that matches their workload.
- **D-04 (Claude's discretion):** Defaults — zipfian alpha=1.0, cardinality K=1M keys, mixed-events M=3 distinct event types per pipeline config. Tunable via `--zipf-alpha`, `--cardinality`, `--mixed-event-count`. Acceptable to leave the exact CLI surface to the planner.

### B. Pipelining mode for the blast

- **D-05:** Ship **both** continuous and burst pipelining modes side-by-side. Per `(size, transport, shape, language)` cell, the ledger gets TWO rows: one with `--continuous-pipeline=true` (Phase 18 default — REAL per-event latency, p50/p95/p99), one with `--continuous-pipeline=false` (burst — amortized `batch_total/N` only, peak EPS).
- **D-06:** Default config: `--parallel=16`, `--pipeline-depth=1024`. Phase 18's best-observed configuration (msgpack continuous reached 483-527k EPS at pd=1024). Same across all shapes for direct comparison. Single TCP connection per worker → 16 connections total; effective inflight cap = 16384 in continuous mode.
- **D-07:** `--isolation-mode` adds **three** columns to the ledger row: `wall_clock_ms` (start → last ack), `send_drain_ms` (start → last byte left bench), `ack_lag_ms = wall_clock - send_drain`. EPS = `N / wall_clock_ms`. Lets users spot bench-bound vs server-bound at a glance.

### C. Python harness

- **D-08:** Python harness lives at `python/benches/blast.py`. Excluded from the pip wheel via `pyproject.toml` `[tool.hatch.build.targets.wheel]` exclude rules. Importable from a clone, not from `pip install beava`.
- **D-09:** Uses the **public Transport API** in a tight loop (no raw socket bypass). Specifically:
  - HTTP transport: `transport._client.post(f"/push/{event}", json=body)` (the same path `app.upsert()` already uses internally — public on the Transport object, not yet wrapped by an `app.push()` since `SDK-APP-04` hasn't landed).
  - TCP transport: `transport.send_push(event_name, body_dict, wire_format="json"|"msgpack")` (public method on `TcpTransport`).
  - **Revised 2026-04-26:** original draft of D-09 said "public `bv.App + app.push()`" but `app.push()` is gated on `SDK-APP-04` and does not exist yet. The plan-checker caught this; user picked "use `transport.send_push()` until `app.push()` lands" — number reflects what an SDK user observes minus the soon-to-land thin `app.push()` wrapper, so it is still SDK-honest (Python encoder, GIL, httpx overhead). Once `SDK-APP-04` lands, a future Phase 19.1 can switch the harness to `app.push()` and re-baseline.
  - **Forbidden:** opening a raw `socket.create_connection(...)` and writing pre-encoded frames directly. That is wire-direct bypass and falsifies the published Python number.
- **D-10:** Concurrency model: `concurrent.futures.ProcessPoolExecutor` with N = `os.cpu_count() - 1` worker processes. Each worker spawns its own `bv.App` and runs an independent tight push loop. Per-worker counters are aggregated via `multiprocessing.Manager` (or atomic-int over `multiprocessing.Value`). Bypasses GIL; closest to "multiple Python service instances pushing concurrently" — the realistic deployment shape.
- **D-11:** Python rows are added to the same `## 1M-event blast` section in `.planning/throughput-baselines.md`. Schema gets a new `language` column (`rust` | `python`). Direct apples-to-apples comparison in one table.

### D. WIP stash + `--total-events` stop signal

- **D-12:** Drop the WIP watcher task entirely. The receiver task already counts acks per worker — when `ack_count >= cap`, the receiver atomically flips `stop` AND calls `sender_sem.close()`. The closed semaphore makes any sender blocked in `acquire_owned().await` return `Err`, which the sender treats as "exit the loop." Zero polling, zero stall, deterministic exit. Applies to **both** the continuous-pipeline sender path and the burst-mode sender path.
- **D-13:** Hard cap in the sender via shared `Arc<AtomicU64>` (`sender_pushes`). Sender does `fetch_add(1, Relaxed) >= cap` check before each TCP write — if it crosses, break before writing. At end of run, the bench reports an invariant tuple `{requested: N, pushed: N, acked: N}` (the run is rejected as a measurement error if these diverge).
- **D-14:** WIP stash fate: **cherry-pick** the useful diff hunks (`--total-events` `Cli` arg + `effective_duration_secs` cap + `prebuilt_frame` in TCP continuous worker) into a fresh commit. Drop the watcher task entirely (`tokio::spawn(async move { loop { ... sleep(1ms) } })`). Re-implement stop via D-12's receiver-flips-and-close-sem pattern. Net: ~50% of stash lines kept, refactored. Stash@{0} can be dropped after the rewrite lands.
- **D-15:** No warm-up phase. `wall_clock_ms` starts at the first frame send. Captures all real overhead — cold caches, first apply-loop tick, JIT-style optimizer warmup, the lot. The honest cold-start number that matches what a user benchmarking "how fast does this server start serving" would see. (Latency histograms still live in the Phase 18 latency code path; warmup-aware percentiles are not added.)

### Claude's Discretion

- Exact CLI surface for shape parameters (`--zipf-alpha`, `--cardinality`, `--mixed-event-count`, `--key-prefix`)
- Exact column ordering in the ledger row (proposed: `Phase | Date | Pipeline | Transport | Shape | Mode | Language | parallel | pd | N | wall_clock_ms | send_drain_ms | ack_lag_ms | EPS | P50 push (µs) | P95 push (µs) | P99 push (µs) | Peak RSS (MB) | Commit | Notes`)
- M4 threshold check: roadmap suggests small ≤ 2 s, medium ≤ 4 s, large ≤ 8 s, large_phase9 ≤ 12 s — apply to the **zipfian + continuous + msgpack + rust** cell as the canonical regression-gate cell. Other cells are informational. Block phase verification only on this canonical cell missing its threshold; capture-only for the rest.
- Whether to make `python/benches/blast.py` an installable console script (e.g., `beava-blast`) or an explicit `python python/benches/blast.py` invocation. Default to the explicit invocation.
- Whether to extract Zipfian / pool-builder into a shared `crates/beava-bench/src/blast_shape.rs` module for reuse by other future benches. Yes — keep modular.

### Folded Scope

- **Sub-goal 5 (Saturation bench architectural notes)** — captured directly in this CONTEXT.md `<specifics>` section below; phase SUMMARY will reproduce the rationale block so future bench changes don't accidentally regress measurement honesty.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### ROADMAP & STATE
- `.planning/ROADMAP.md` § Phase 19 (line 516+) — full goal + 5 sub-goals + 4 success criteria
- `.planning/STATE.md` — current Phase 18 wrap + headline numbers (commit `a809d04`)
- `.planning/REQUIREMENTS.md` § THROUGHPUT (lines 245-252) — REQ-IDs that Phase 19 baseline rows traceback to

### Throughput-baselines ledger
- `.planning/throughput-baselines.md` — append-only ledger format (Phase 7.5 D-09/D-10); add new `## 1M-event blast` section
- `.planning/perf-baselines.md` — sibling format reference (criterion microbenches, do not append to from Phase 19 unless adding a new microbench)

### Phase 7.5 (origin of throughput-bench convention)
- `.planning/phases/07.5-end-to-end-throughput-harness-first-baseline/07.5-CONTEXT.md` — D-01..D-15 (crate location, workload runner pattern, ledger schema, regression-gate convention)
- `.planning/phases/07.5-end-to-end-throughput-harness-first-baseline/07.5-SUMMARY.md` — first baseline + the per-phase "throughput run" task convention

### Phase 18 (the data-plane runtime being measured)
- `.planning/phases/18-redis-hand-roll/18-CONTEXT.md` — D-01..D-16 locked decisions, especially D-04 (continuous pipelining), D-09 (wire parsing), D-15 (crate structure)
- `.planning/phases/18-redis-hand-roll/18-12-SUMMARY.md` — `Arc<str>` event_name / EPS lift analysis (462k/487k at pd=256)
- `.planning/phases/18-redis-hand-roll/18-04.7-SUMMARY.md`, `18-04.8-SUMMARY.md` — IoPool wiring + body→Row migration
- `.planning/phases/18-redis-hand-roll/18-11-SUMMARY.md` — hot-path optimization (SmallVec/CompactString/hashbrown)
- (Optional) `.planning/phases/18-redis-hand-roll/18-redis-research.md` and `18-rust-translation.md` — Redis architecture + rust translation rationale

### Phase 13.3 rejection (key constraint for Phase 19's framing)
- Memory: `project_no_sharded_apply.md` — single-threaded data plane FOREVER; for higher aggregate throughput users run multiple instances (Redis-cluster pattern)
- `.planning/STATE.md` § "Architectural decision LOCKED 2026-04-26" — the single-instance ceiling that Phase 19 measures IS the per-instance final number

### Bench code (the things we're modifying)
- `crates/beava-bench/Cargo.toml` — workspace member layout
- `crates/beava-bench/src/bin/beava-bench-v18.rs` — current TCP+HTTP bench, target of `--total-events` work
- `crates/beava-bench/src/bin/temporal_throughput.rs` — sibling bench (don't break it)
- `crates/beava-bench/configs/{small,medium,large,large_phase9,medium_phase9,phase8,geo}.json` — pipeline shapes; matrix dim
- `git stash@{0}` (`wip: --total-events + pre-encoded-frame bench`) — design source for the cherry-pick (drop watcher, keep flag + pre-encode)

### Python SDK (the API the Python harness drives)
- `python/beava/_app.py` — `bv.App` constructor + URL scheme dispatch
- `python/beava/_transport.py` — `HttpTransport`, `TcpTransport(wire_format="msgpack")`, msgpack via `CT_MSGPACK`
- `python/beava/_events.py` + `python/beava/_agg.py` — feature decorator surface used to register the four bench configs from Python
- `python/pyproject.toml` — wheel build config; need exclude rule for `benches/`

### Convention enforcement
- `CLAUDE.md` § Conventions § Performance Discipline — Phase 19 plan MUST include the throughput-run task that appends to `.planning/throughput-baselines.md`; same 10%/25% gate vs prior phase's simple-fraud (small) cell
- `CLAUDE.md` § Conventions § TDD Discipline — Phase 19 inherits TDD red-green commits per task

### CHANGELOG (will be updated)
- `CHANGELOG.md` — Phase 19 wrap entry
- `README.md` — link to `## 1M-event blast` section once it lands

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets

- **`crates/beava-bench/src/bin/beava-bench-v18.rs`** — the bench binary; already supports HTTP + TCP, json + msgpack, continuous pipelining, parallel workers, RSS sampling, hdr histogram per-task latency capture, ledger row formatting. Phase 19 EXTENDS this binary; it does not write a new one.
- **`run_tcp_continuous_push_worker`** (private fn in same file) — sender/receiver split using `tokio::io::split` + `tokio::sync::Semaphore` + `mpsc<Instant>` for FIFO ack pairing. Phase 19 D-12's "receiver flips stop + closes sem" lands inside this fn AND its burst-mode sibling.
- **`make_event_payload`** (private fn) — builds an `EventBody` per `(pipeline, seq, rng)`. Phase 19 reuses this for the pool builder.
- **`encode_frame`** + `Frame { seq, op, content_type, payload }` — Phase 2.5 wire codec. Reused unchanged.
- **`hdrhistogram::Histogram`** for per-event latency. Reused unchanged in continuous mode; gated off in burst mode (only batch_total/N reported).
- **`bv.App + app.push()`** — Python SDK surface; thread-safe-per-process. Each multiprocess worker creates its own App.
- **`bv.events / bv.tables / bv.col` + `app.register(...)`** — used to register the same 4 pipeline configs from Python that the Rust harness already supports via JSON config files. Plan 02 of Phase 19 will translate the configs to SDK form (one Python module per config).

### Established Patterns

- **Append-only ledger** (`.planning/throughput-baselines.md` per Phase 7.5 D-09/D-10) — never edit historical rows; new sections + new rows only.
- **In-process `TestServer`** (from `beava-server::testing`) — used by every existing throughput bench so the WAL/snapshot are on real disk but the network roundtrip is replaced by an in-process listener. Phase 19 keeps using this for the Rust harness.
- **`hw-class` tagging** — `apple-m4 / darwin-24.3.0 / 10 cores`. Each ledger section is namespaced by hw-class.
- **Per-phase throughput-run convention** (CLAUDE.md) — Phase 19 plan MUST append rows; plan-checker enforces a `files_modified` includes `.planning/throughput-baselines.md`.

### Integration Points

- `crates/beava-bench/src/bin/beava-bench-v18.rs` — primary integration: `--total-events`, `--blast-shape`, `--isolation-mode` flags; pool builder; receiver-flips-stop wiring.
- `crates/beava-bench/src/blast_shape.rs` (NEW) — Zipfian sampler + pool builder + frame mutator. Reusable across future benches.
- `python/benches/blast.py` (NEW) — multiprocess driver; imports `beava` package; reuses pipeline-config translation from `python/benches/_configs.py` (NEW).
- `python/pyproject.toml` — add `benches/` to wheel exclude list.
- `.planning/throughput-baselines.md` — new `## 1M-event blast` section + `language` column.

</code_context>

<specifics>
## Specific Ideas

### Saturation bench architectural notes (folded sub-goal 5)

The future-bench-author rationale, captured here so a bench refactor doesn't accidentally regress measurement honesty:

1. **Why Pool=N (not a sampler):** Pre-encoding ALL N frames at startup eliminates per-iteration RNG cost AND per-iteration encode cost from the bench hot loop. The bench-side floor becomes "as fast as TCP `write_all` can drain" — the server-side ceiling is the only number we're measuring. Pool memory ~500 MB-1 GB for N=1M; budget for it.
2. **Why all 4 shapes side-by-side:** A single "headline" number invites cherry-picking. Publishing fixed/uniform/zipfian/mixed in the same table forces honesty: marketing claim and realistic claim live one row apart.
3. **Why both pipelining modes:** Continuous gives REAL per-event latency that users actually observe; burst gives the upper-bound EPS the apply loop can sustain when the network isn't waiting. Both are useful answers to different questions.
4. **Why receiver-flips-stop (no watcher):** The 1ms-poll watcher in stash@{0} introduces both a stall risk (sender blocked on `acquire_owned().await` after stop flips) and up to 1ms of cap overshoot. Letting the receiver — which already counts acks per FIFO pair — flip stop AND close the semaphore is zero-poll, zero-stall, and the natural place for the cap check to live.
5. **Why no warm-up:** Saturation answers "how fast does this server actually start serving when I hit it cold." Warm-up turns it into a steady-state question, which the existing 60-s `--duration-secs` mode already answers. Two questions, two flags, no overlap.
6. **Why public Python SDK in the Python harness:** A bench that bypasses the SDK to hit the wire directly tests something users don't do. The headline number must reflect what a user observes when they `pip install beava` and call `app.push()`.

### Stash@{0} cherry-pick checklist

Apply these hunks to a fresh commit; drop the watcher task block:

- ✓ `Cli` struct: `total_events: Option<u64>` arg
- ✓ `effective_duration_secs` cap at 3600 when `total_events.is_some()`
- ✓ `prebuilt_frame` build (move to Pool=N builder; the stashed version only builds ONE frame which only matches the `fixed` shape)
- ✗ `total_events_task` (the watcher) — DROP
- ✓ Sender loop: replace `buf.clear(); encode_frame(...); write_all(&buf)` with `write_all(&pool[idx])` where `idx = pushes.fetch_add(1, Relaxed)`
- ✓ Sender break condition: `pushes.fetch_add(...) >= cap` BEFORE the write
- + NEW: Receiver fn flips `stop.store(true)` + `sender_sem.close()` when `acks >= cap`
- + NEW: `--blast-shape` enum + dispatch to pool builder
- + NEW: `--isolation-mode` flag + `send_drain_ms` / `ack_lag_ms` capture
- + NEW: ledger row formatter outputs new columns

### Zipfian sampling reference

For the `zipfian` shape: rejection sampler over rank `r ∈ [0, K)` with `P(r) ∝ 1/(r+1)^alpha`, alpha=1.0 default. The classic [Gray et al. "Quickly generating billion-record synthetic databases"](https://dl.acm.org/doi/10.1145/191843.191886) Zipfian recipe is sufficient; libraries like `rand_distr::Zipf` work too. Pool builder samples K-1M times (with replacement) to fill the N-frame pool — pool length = N, distinct keys ≤ K.

### Threshold goals (M4)

Roadmap-suggested per-size thresholds (apply to the canonical `zipfian + continuous + msgpack + rust` cell as the regression-gate signal):

| Size          | Wall-clock target | Min EPS implied |
|---------------|------------------:|----------------:|
| small         | ≤ 2 s             | ≥ 500k EPS      |
| medium        | ≤ 4 s             | ≥ 250k EPS      |
| large         | ≤ 8 s             | ≥ 125k EPS      |
| large_phase9  | ≤ 12 s            | ≥ 83k EPS       |

Phase 19 verification BLOCKS only on the small + zipfian + continuous + msgpack + rust cell missing 2 s. All other cells are capture-only; users get the full matrix in the ledger.

</specifics>

<deferred>
## Deferred Ideas

### Out of Phase 19 scope

- **Linux Xeon coverage** — Phase 18.5 / 18.6 prerequisite. Phase 19 baselines on M4 only; Linux numbers land in Phase 18.5/18.6 wrap or a Phase 19.1 follow-up.
- **Async (asyncio) Python harness** — would need SDK surface additions (async `app.push()`). Multi-process is enough for v0; revisit when async SDK is on the roadmap.
- **Beyond Zipfian (long-tail-sequential, geometric, hot-key-burst)** — alpha=1.0 is enough for v0; richer distributions belong in a "advanced bench shapes" follow-up phase.
- **Latency-tail steady-state separation** — D-15 keeps wall_clock cold-start honest; a "skip first 5% of pushes from histograms" mode is a v0.0.x polish item.
- **Beyond multiprocess Python** — multi-process is the recommended deployment shape; threaded/coroutine variants belong in future profiling phases.
- **Cross-instance throughput** — Phase 19 measures single-instance only. Aggregate >1 instance throughput is users' job (Redis-cluster pattern, per `project_no_sharded_apply.md`).
- **Connection-loss / partial-ack handling** — if the server hangs mid-blast, Phase 19 reports "incomplete" and exits. Resilience tests are out of scope.
- **`19-test-migration-and-old-api-removal/`** archival — the legacy phase-19 directory at that path is from a stale roadmap. Logistics housekeeping; clean up in a separate `gsd-cleanup` pass.

### Reviewed Todos (not folded)

None — `gsd-tools todo match-phase 19` returned 0 matches.

</deferred>

---

*Phase: 19-1m-bench*
*Context gathered: 2026-04-26*
