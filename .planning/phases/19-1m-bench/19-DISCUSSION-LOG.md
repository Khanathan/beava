# Phase 19: 1M-EPS Bench Harness — Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-04-26
**Phase:** 19-1m-bench
**Areas discussed:** Blast shape, Pipelining mode, Python harness, WIP stash debug

---

## Gray Area Selection

| Option | Description | Selected |
|--------|-------------|----------|
| Blast shape (key variance) | Pre-encode ONE frame vs vary keys vs ship both as modes | ✓ |
| Pipelining mode | Continuous Semaphore-gated vs burst vs both | ✓ |
| Python harness location + API | Where lives + what API used | ✓ |
| WIP stash debug strategy | Resume WIP vs re-implement with new pattern | ✓ |

**User's choice:** All four areas. (Connection topology + threshold/hw-class folded into other areas as follow-ups.)

---

## Blast shape (key variance)

### Q1: What does the blast bench actually push?

| Option | Description | Selected |
|--------|-------------|----------|
| Both modes (--blast-shape flag) | --blast-shape=fixed and =varied | |
| Fixed only (one frame, one key) | Pre-encode ONE, reuse N times | |
| Varied only (rolling key per event) | Per-event re-encode, deterministic counter | |
| Fixed body + varied id field | Pre-encode body, patch user_id bytes in-place | |

**User's choice:** "Other" — *"how about prepare with multiple shape (e.g zipifan multiple streaming event patterns"*
**Notes:** User wanted richer workload patterns — Zipfian distribution + multiple event types. Drove the follow-up Q2 menu.

### Q2: Which blast shapes should ship as built-in workload patterns?

| Option | Description | Selected |
|--------|-------------|----------|
| fixed (1 key, 1 frame) | Cache-warm marketing peak — upper bound | ✓ |
| uniform (rolling keys) | user_id rolls 0..K-1 evenly; cache-pessimistic floor | ✓ |
| zipfian (heavy-tail) | Zipfian alpha~1.0 over K keys; realistic fraud | ✓ |
| mixed events (multi-stream) | Pool of M registered events sampled per push | ✓ |

**User's choice:** All four shapes.

### Q3: How are blast frames pre-built for varied/zipfian/mixed shapes?

| Option | Description | Selected |
|--------|-------------|----------|
| Pool of pre-encoded frames (Recommended) | P=10k-100k pre-encoded frames sampled per iteration | |
| On-the-fly encode per event | Re-encode every event with current key/event mix | |
| Hybrid: pool + cheap mutation | Pre-encode template, patch user_id + event_name bytes | |

**User's choice:** Pool of pre-encoded frames (Recommended)
**Notes:** Followed up to determine pool size and cardinality.

### Q4: Which shape's number is the published headline?

| Option | Description | Selected |
|--------|-------------|----------|
| zipfian (Recommended) | Realistic-fraud number is the honest claim | |
| fixed (cache-warm peak) | Publish absolute upper bound as headline | |
| All four side-by-side | No single headline; table publishes all four | ✓ |
| mixed events | Most realistic for multi-stream tenants | |

**User's choice:** All four side-by-side.

### Q5: Default pool size + cardinality for the varied shapes?

| Option | Description | Selected |
|--------|-------------|----------|
| Pool 100k frames, K=1M keys (Recommended) | ~50-100 MB pool memory, 10% of pool per blast | |
| Pool 10k frames, K=100k keys | Smaller pool, faster startup, less cache pressure | |
| Pool = N (one frame per event) | Pre-encode ALL N=1M frames upfront (~500MB-1GB) | ✓ |
| Pool = K, sample-with-replacement | One frame per distinct key, sampler picks per iteration | |

**User's choice:** Pool = N (one frame per event).
**Notes:** Eliminates per-iteration RNG cost AND per-iteration encode cost — bench-side floor becomes "as fast as TCP write_all can drain."

---

## Pipelining mode for the 1M blast

### Q1: Which pipelining mode(s) does the 1M blast use?

| Option | Description | Selected |
|--------|-------------|----------|
| Both modes side-by-side (Recommended) | Two rows per cell: continuous + burst | ✓ |
| Burst only (no semaphore) | Maximum saturation, amortized latency only | |
| Continuous only (Phase 18 default) | REAL per-event latency, EPS lower than burst | |
| Burst with bounded-pd large window | pd=4096+ blurs the line | |

**User's choice:** Both modes side-by-side.

### Q2: What's the inflight depth (pd) for continuous mode?

| Option | Description | Selected |
|--------|-------------|----------|
| pd=1024 (Recommended) | Phase 18 best-observed config (483-527k EPS msgpack) | ✓ |
| pd=256 (Phase 18 standard) | Slightly more conservative; matches existing rows | |
| pd=4096 (deep window) | Aggressive depth; risk of TCP send buffer + tail | |
| Sweep pd in {64, 256, 1024, 4096} | All four pd per cell; longest runtime | |

**User's choice:** pd=1024.

### Q3: Connection topology — how does the bench fan out the 1M frames?

| Option | Description | Selected |
|--------|-------------|----------|
| parallel=16, pd=1024 per conn (Recommended) | Phase 18 matrix peak; 16 TCP conns × pd=1024 | ✓ |
| parallel=num_cpus()-1, pd=1024 per conn | Auto-scale to platform | |
| Single conn, pd=16384 | One TCP conn with massive inflight window | |
| parallel sweep {1, 4, 16, 64} | Explore connection scaling per cell | |

**User's choice:** parallel=16, pd=1024 per conn.

### Q4: How does --isolation-mode split timing in the published table?

| Option | Description | Selected |
|--------|-------------|----------|
| Three columns: send_drain, ack_lag, total (Recommended) | wall_clock, send_drain_ms, ack_lag_ms in ledger row | ✓ |
| Single EPS column, isolation-mode in notes | Headline only; isolation logged to stderr | |
| Two-row pattern per cell | Bench-bound row + server-bound row per cell | |
| Just total wall_clock (drop isolation) | No isolation mechanic at all | |

**User's choice:** Three columns: send_drain, ack_lag, total.

---

## Python harness location + API

### Q1: Where does the Python harness live?

| Option | Description | Selected |
|--------|-------------|----------|
| python/benches/blast.py (Recommended) | Sits next to SDK; excluded from pip wheel | ✓ |
| crates/beava-bench/python/blast.py | Co-locates with Rust bench; not in installed wheel | |
| tools/python-bench/blast.py | Top-level tools/ standalone | |
| python/beava/_bench.py (importable) | Inside SDK package as hidden module | |

**User's choice:** python/benches/blast.py.

### Q2: What API does the Python harness use?

| Option | Description | Selected |
|--------|-------------|----------|
| Public app.push() in tight loop (Recommended) | bv.App + app.push() exactly as documented | ✓ |
| Raw socket + msgpack hand-rolled | Apples-to-apples vs Rust harness, bypasses SDK | |
| Both modes (--via=sdk\|raw flag) | Two harnesses in one | |
| asyncio + multiple connections | Async SDK loop with N concurrent push() | |

**User's choice:** Public app.push() in tight loop.
**Notes:** Honest end-user-facing number including SDK overhead.

### Q3: Concurrency model for the Python harness?

| Option | Description | Selected |
|--------|-------------|----------|
| Multi-process (concurrent.futures.ProcessPoolExecutor) (Recommended) | N=cpu-1 workers, each with own bv.App | ✓ |
| Single-threaded sync | One process, one App, tight loop | |
| Thread pool | ThreadPoolExecutor over one bv.App | |
| asyncio (single process, many tasks) | Async push() awaitable tasks | |

**User's choice:** Multi-process via ProcessPoolExecutor.

### Q4: How are Python rows tabulated in throughput-baselines.md?

| Option | Description | Selected |
|--------|-------------|----------|
| language column + same '1M-event blast' section (Recommended) | New columns: language, wall_clock, send_drain, ack_lag, EPS | ✓ |
| Separate '1M-event blast (Python)' subsection | Distinct subsections under '1M-event blast' | |
| language column in EXISTING throughput section | No new section; reuse existing per-phase rows | |

**User's choice:** language column + same '1M-event blast' section.

---

## WIP stash debug strategy

### Q1: How do we land --total-events without the stall?

| Option | Description | Selected |
|--------|-------------|----------|
| Drop watcher; receiver flips stop on cap (Recommended) | Receiver flips stop + sem.close() when cap reached | ✓ |
| Resume stash + raise watcher poll to 1µs | Tighter polling; doesn't fix root cause | |
| Drop watcher; sender checks cap on each iter | Sender atomic load before permit acquire | |
| Drop stash entirely; rewrite from scratch | Clean re-implementation with select! | |

**User's choice:** Drop watcher; receiver flips stop on cap.

### Q2: How does the bench guarantee EXACTLY N events were pushed?

| Option | Description | Selected |
|--------|-------------|----------|
| Hard cap in sender; report actual count (Recommended) | fetch_add(1) >= cap; report N=N=N invariant | ✓ |
| Soft cap (allow up to pd overshoot) | In-flight pushes finish; actual count may be N+(pd-1) | |
| Pre-allocate exactly N; iter once and stop | Pool=N, iterate once, return | |

**User's choice:** Hard cap in sender + report actual count.

### Q3: Stash@{0} fate?

| Option | Description | Selected |
|--------|-------------|----------|
| Cherry-pick CLI flag + pre-encode; rewrite stop logic (Recommended) | Keep ~50% of stash diff; drop watcher; refactor | ✓ |
| Apply stash as-is, then patch on top | git stash apply; preserves WIP commit history | |
| Drop stash; rewrite from scratch | Clean diff; throws away stashed work | |
| Keep stash for archive; new branch lands the work | Don't touch stash; new branch references for context | |

**User's choice:** Cherry-pick CLI flag + pre-encode; rewrite stop logic.

### Q4: Warm-up handling — does the bench warm caches before the timed window?

| Option | Description | Selected |
|--------|-------------|----------|
| No warm-up; first push is timed (Recommended) | wall_clock starts at first frame send | ✓ |
| Warm-up N=10k events (untimed), then time N=1M | Warm L1/L2/page caches first | |
| Two-phase report: cold + steady-state | Run both passes per cell | |
| Skip first 5% of pushes from latency histogram only | EPS whole-window; latency steady-state | |

**User's choice:** No warm-up; first push is timed.

---

## Claude's Discretion

- Exact CLI surface for shape parameters (`--zipf-alpha`, `--cardinality`, `--mixed-event-count`, `--key-prefix`)
- Exact column ordering in the ledger row
- Whether to make `python/benches/blast.py` an installable console script or explicit invocation
- Whether to extract Zipfian / pool-builder into shared `crates/beava-bench/src/blast_shape.rs` module
- M4 threshold check: roadmap's per-size thresholds applied to canonical `zipfian + continuous + msgpack + rust` cell as the regression-gate signal; other cells capture-only

## Deferred Ideas

- Linux Xeon coverage (Phase 18.5/18.6 prerequisite)
- Async (asyncio) Python harness
- Beyond-Zipfian distributions (long-tail-sequential, geometric, hot-key-burst)
- Latency-tail steady-state separation
- Beyond-multiprocess Python concurrency variants
- Cross-instance / aggregate throughput
- Connection-loss / partial-ack resilience tests
- Archival of legacy `19-test-migration-and-old-api-removal/` directory (separate gsd-cleanup pass)
