# Learnings from Building Beava — Series Plan (v2, repositioned)

**Voice:** builder-to-builder, Garry Tan adjacent. No em dashes. Short paragraphs. Real file names, real phase numbers, real measurements. No "delve." No hand-waving. Every claim sourced to a commit or a research doc.

**Positioning north star:** The Fennel lane is open (Databricks acquisition, April 2025). Chalk is enterprise and closed. Feast isn't streaming. Nobody has the OSS, single-binary, Python-decorator, streaming-plus-stateful answer. Beava is that thing. The blog series is the proof.

---

## The three posts

### Part 1: The lane Databricks left open

**Target:** 2500w. Positioning + DX. Most accessible. HN front-page play.

**Thesis:** There is a specific, underserved shape of product that Databricks-acquires-Fennel, Chalk-goes-enterprise, and Feast-isn't-streaming all left behind. An OSS, single-binary, Python-decorator-first, streaming-plus-stateful feature engine that you can actually install and run on Monday. That thing should exist. Here it is.

**Beats:**
1. The gap, named. Fennel was acquired April 2025. Chalk is closed and sold top-down at six-figure ACVs. Feast is a serving facade, not streaming. The ML engineer who wants real-time features on their own terms has no OSS answer in 2026.
2. Why the gap exists. Distributed streaming platforms (Kafka, Flink) were built for mid-2010s hardware. A 10-core M-series laptop now does ~315K events per second sustained at a complex 9-cell workload. Most real-time workloads fit in one box. The cluster was the assumption, not the requirement.
3. What the product is. `pip install beava`, `beava serve`, `@bv.stream` decorator, 60 seconds from `curl` to a correct feature. One binary. No JVM. No Zookeeper.
4. The design constraint that ate everything. 60 seconds from GitHub to a correct feature value, from any language. If this fails, no benchmark or blog post rescues it.
5. The boring-but-load-bearing choices. HTTP-first, not TCP. Bundled Rust binary in a pip wheel, not PyO3. PyO3 breaks under Gunicorn multi-worker spawn (multiple workers, isolated state, production-breaking). Bundled binary + localhost TCP keeps state coherent with ~50 µs overhead.
6. What Redis gets right that we copy. `INCR` is still the baseline mental model for real-time counters. Simple, obvious, installable.
7. What Redis gets wrong that we fix. No event time. No sketches. No replay. No correctness under crash.
8. Close. The 60-second bar, the decorator, and the one-binary install are the whole pitch. Part 2 is where the hard engineering lives.

### Part 2: Two microbenches that lied

**Target:** 3000w. Technical banger. HN front-page play in the "debugging done right" genre. r/rust primary channel.

**Thesis:** Two pieces of conventional wisdom in the Rust systems community are wrong in ways that change how you should build a stateful streaming engine. Both found the same way: by running a benchmark that measured a path production never takes, and then by running the integration bench and seeing the real number.

**Finding 1: DashMap's uncontended lock-acquisition is the bottleneck, not contention.**

The common read: "DashMap is fine until you have contention. Under contention, switch to sharded state."

The actual measurement: even on a workload with effectively zero contention (keys distributed across 16 internal DashMap shards, 8 tokio workers, cold and hot keys mixed), `DashMap::_entry` was 60-61% of CPU samples in a pprof capture. The cost is not contention. The cost is the uncontended lock-acquisition path itself. Cache-line fetch, atomic CAS for the shard lock, deref, hash, deref again. Per operation. At 300K+ events per second that dominates.

This is why thread-per-core is not about eliminating contention. It is about eliminating a lock that costs cycles even when no one else wants it.

**Finding 2: LSM-tree durability does not cost 3-5x. It costs ~15%.**

The common read: "LSM-tree backends like fjall or RocksDB are 3-5x slower than in-memory HashMaps because disk."

The spike benchmark: 2,468x slower. `PartitionHandle::insert` per op took 2.3 ms vs AHashMap's 940 ns. The STOP gate fired.

The integration benchmark (Phase 53 production path): ~15% regression. Inside the -15% budget.

The difference: the spike measured `PartitionHandle::insert` synchronously on every op. The production path uses `PersistMode::Buffer`, a write-through AHashMap cache in front of fjall, and background fsync on a timer. The hot set lives in the cache. Fjall sees eviction writes and periodic flushes. The 2.3 ms per-op cost is real, but it happens on a cold path that triggers once per thousand events, not once per event.

Both findings share a meta-lesson: **the microbench measures the path you can instrument. Production measures the path that actually runs. When those differ by three orders of magnitude, the microbench is lying to you, and you have to trust the integration bench.**

**Beats (in order):**

1. Hook: two benchmarks, one that said "this is catastrophic," one that said "this is fine," same code. The spread was three orders of magnitude.
2. Finding 1: the DashMap pprof. Numbers from the Phase 52 baseline investigation: `DashMap::_entry` 60.8-61.2% of samples, `PipelineEngine::push_internal` 66% inclusive. This was supposed to be a throughput measurement. It turned into an architecture verdict.
3. Why uncontended lock-acquisition is not free. The CPU-level explanation: atomic CAS even on an uncontended lock still fetches a cache line, still orders memory, still pays for the privilege of being a lock. On a single hot thread at 1 GHz event rate the budget per event is nanoseconds. A lock burns those nanoseconds regardless of what it is protecting.
4. The TPC answer. Thread-per-core, one pinned thread per shard, state owned by exactly one thread, plain `HashMap` (not `DashMap`, not `RwLock<HashMap>`). No lock on the hot path because there is no sharing on the hot path. ScyllaDB, Redpanda, Apache Iggy did this. We did this.
5. Finding 2: the fjall spike STOP-gate. `benches/fjall_spike.rs` measured 232 ms for 100 read-modify-write ops. The -25% bench budget wanted maybe 120 µs. The 2,468x gap fired the gate. We knew it was real because we read the fjall source: `PartitionHandle::insert` at `partition/mod.rs:980` takes the journal writer mutex, writes the KV pair to the journal file (`write()` syscall, even in buffer mode), persists in buffer mode, inserts to memtable, checks overflow. Every call. Per event. On macOS APFS that is 23 µs just for the buffered journal write plus memtable.
6. Why we overrode the gate. The production path does not call `PartitionHandle::insert` per event. It goes: event arrives, JSON parse, cascade dispatch, operator state update in the write-through AHashMap cache, eviction to fjall on cold-key displacement, flush to disk on timer. Fjall sees a fraction of the event rate. The spike bench was measuring the hot path if you had no cache. We have a cache.
7. The integration bench. Phase 53 Plan 06 ship-gate. 9-cell Criterion matrix plus Pareto workload, fjall backend vs in-memory baseline. Regression budget -15% on the slow cell. Result: inside budget. (Note: Plan 06 was deferred to Phase 54 because the legacy `PipelineEngine::push_internal` path at N=1 was still bypassing the shard dispatch and writing to DashMap, producing DashMap-vs-DashMap noise instead of fjall-vs-AHashMap signal. That bug is its own story and goes in the post as the "we found the wrong thing measured again" beat.)
8. The meta-lesson, stated plainly. A microbench tells you what a path costs. It does not tell you whether production takes that path. When your microbench and your integration bench disagree by orders of magnitude, one of them is measuring the wrong thing. Usually it is the microbench. Always run the integration bench before believing the verdict.
9. What this means for your own streaming engine. Default `DashMap` state is a tax you are paying 60% of the time even when nobody contends. Default "LSM is 3-5x slower" is wisdom from unbuffered, uncached usage. Neither default is wrong for everyone. Both are wrong for a stateful streaming engine on modern hardware.
10. Close. The engine that came out of this: thread-per-core, per-shard plain `HashMap` in a write-through cache, per-shard `fjall` partition as the durable substrate. 315K+ events per second at N=1, 918K+ at N=8 on Linux reference hardware, durable state, crash-safe on SIGKILL, no snapshot replay needed. And yes, -15% vs the in-memory path. That is the price of durability. It is less than anybody told you.

**Sources:**
- `.planning/phases/53-fjall-state-backend/53-01-SPIKE-RESULTS.md`
- `.planning/phases/53-fjall-state-backend/53-VERIFICATION.md` (DashMap pprof discovery)
- Fjall 2.11 source at `src/partition/mod.rs:980`
- Apache Iggy TPC migration post (Feb 2026)
- Phase 52 baseline 314,931 EPS complex-c8-x8

### Part 3: The feature is the plan

**Target:** 2000w. Vision piece. Quotable. Lower-volume traffic, higher signal.

**Thesis:** The ML platform team between the data scientist and production state is going away. Not because teams are bad, but because agents and data scientists will both want to submit compute plans directly against live state, and neither of them files tickets. The primitive that matters is not the streaming engine. It is a durable, replayable, forkable event log with compute plans as first-class artifacts.

**Beats:**

1. Hook. A data scientist should not file a ticket to compute a feature. An agent should not wait three weeks for an ML platform team to deploy its retrieval layer. Today both of them do.
2. The shape that is wrong. ML platform sits between data/agent and prod. Every feature is a PR. Every backfill is a manual job. Chalk's Symbolic Python Interpreter pointed at the handoff problem but stopped at "Python runs in-engine." The engine is still the gatekeeper.
3. What agentic compute actually needs. Durable event log, replayable from any LSN. Compute plan as serializable artifact (not Python source). Incremental re-execution when plan changes. Fork primitive the agent can call itself. Millisecond reads from live state.
4. The primitive we already have: `tally fork`. A data scientist points `tally fork` at a remote replica stream and gets a scoped slice of production events on their laptop. They iterate on operators without touching prod. The agent version of this is the same primitive with different caller.
5. Compute plan as first-class artifact. Today: plan is Python source compiled at register-time. Tomorrow: plan is a serialized graph. Submit over HTTP. Engine validates (shard-key agreement, operator semantics, bounded resource use). Runs against the log. Returns the streaming result. Human-written, agent-written, LLM-generated, same interface.
6. Why separating incremental compute from storage is load-bearing. If the engine owns both the compute graph and the state it produces, then changing the plan means rebuilding state. At 100 GB per key that is a weekend. If the substrate (durable event log with LSN + event-time semantics) is decoupled from the plan, then a new plan can differentially update from a prior plan's checkpoint. Snowflake compute plane / data plane split, applied to streaming.
7. The hard parts, named. Plan validation at submit time. Multi-tenancy (cost accounting, backpressure per plan, isolation). Incremental re-execution semantics (how much of plan v1's state does plan v2 reuse). Replayable randomness when an LLM is in the plan (seed the RNG, cache outputs, version the model weights).
8. What Beava has to ship to get there. Event log as first-class API (replay from LSN X to LSN Y). Distributed KV layer (Part 2's future work). Compute-plan IR that is not Python-specific. Fork API that an agent can call without human intervention. Preserve the 60-second evaluation even for the agent.
9. The bet. The primitive that matters is not the streaming engine. It is the substrate: durable, replayable, forkable event log with incremental compute plans as first-class citizens. Get the substrate right and the engine is a thin layer. Get it wrong and you have built another Flink with better syntax.
10. Close. "I do not know exactly what 2028 looks like. I am pretty sure it looks more like a data scientist and their agent jointly submitting a compute plan against live state than it looks like a ticket queue and a Kafka cluster."

**Sources:**
- v0 phases 21-38 (`tally fork` history)
- Phase 52-06 (LSN tagging, LsnDedupFilter)
- Differential dataflow / Materialize IVM prior art (cite, don't claim novelty)
- Chalk SPI transpile (cite the precedent, explain where we go further)

---

## Publication plan

1. Day 0: Ship Part 1. HN 8am PT Tuesday. Cross-post r/rust and r/MachineLearning.
2. Day 7-10: Ship Part 2. Primary channel r/rust. The technical banger of the series; should get quoted.
3. Day 14-21: Ship Part 3. Primary channel HN + personal Substack + Twitter. The vision piece.
4. After Part 3: Consolidate into one long-form on beava.dev/blog/learnings. Pitch as a Rust Forge or Strange Loop talk.

## Status

DRAFT - v2 repositioned after landscape research (April 2026). Parts 1, 2, 3 full drafts in separate files.
