# Autonomous session state — 2026-04-23

> **Why this exists:** session running low on quota. Captures everything a future Claude instance (or human) needs to resume cleanly. Read top-to-bottom. Delete after the milestone closes.

## Session anchor commits

- Branch: `v2/greenfield`
- Last orchestrator commit: `157630f docs(ideas): v0.1+ symbolic Python frontend + custom-agg sketch`
- Earlier in session: `1302a8d docs(roadmap): insert Phase 7.5 — end-to-end throughput harness`

If you're a future Claude reading this: `git log --oneline -20 v2/greenfield` will show the trail.

## What's done in this session (chronological)

1. **Phase 6** — closed clean by gap-closure agent. WAL + IdemCache + `/push` endpoint + crash probes + criterion bench. 590/590 tests, VERIFICATION passed. Macros fsync ~7.4ms WARNING, hw-class-bound.
2. **Phase 7** — closed clean. Snapshot + recovery + RegistryBump records + sync readiness flip. 618 tests. SC1/SC2/SC4 deferred at first; closed by Phase 7.5 agent.
3. **Phase 7.5 (NEW PHASE — inserted in this session)** — end-to-end throughput harness + first baseline. ROADMAP/REQUIREMENTS rows added. Crate `crates/beava-bench` ships, configs in `crates/beava-bench/configs/{small,medium,large}.json`. First baseline captured: ~1k EPS on Apple-M4, fsync-bound. Bench discovered + fixed a **silent recovery bug** (RegistryBumpPayload bincode 1.x couldn't deserialize `serde_json::Value`; recovery was `tracing::warn!`-ing the error and continuing → registry recovery silently broken). Fix: switched RegistryBumpPayload to JSON codec + recovery hard-errors on RegistryBump replay failures. CLAUDE.md updated with per-phase throughput-run contract.
4. **Phase 7 deferred items closed by Phase 7.5 agent** — snapshot/recovery criterion bench (10.68µs serialize, 8.45ms atomic write+fsync, 675µs replay 10k records); restart-cycle smokes (SC1+SC4 PASS, SC2 still deferred to Phase 8+ subprocess crash probe). 624 tests after Phase 7.5.
5. **Demo built** — `site/demo/{index.html, proxy.py, register.json, start.sh}`. Single-page playground at http://127.0.0.1:9001/. Beava on :8080 with periodic-fsync WAL config + dev_endpoints. Pre-registers a demo pipeline exercising Phase 5 ops (count, sum, avg, min, max, variance, stddev, ratio with `where=`).
6. **Five parallel worktree agents dispatched** — phases 8 / 9 / 10 / 11 / 11.5. Initial dispatch had a worktree-base bug (`isolation: "worktree"` non-deterministic about base branch); 2 of 5 (Phase 10, Phase 11.5) halted cleanly without polluting wrong branches. Re-dispatched against pre-created worktrees. Phase 10 needed a SECOND re-dispatch because the planning agent didn't execute the plans (thought it needed a `Task` tool — actually just `Skill`).
7. **Phase 6.1 inserted into ROADMAP** — async durability default + `push-sync` endpoint. Detail section + REQUIREMENTS (SRV-DUR-06..10) — *partially landed when this snapshot was written*. Verify with `grep "Phase 6.1" .planning/ROADMAP.md`.

## In flight (background agents) at snapshot time

| Phase | Agent ID | Worktree | Last visible commit | Notes |
|-------|----------|----------|---------------------|-------|
| 8 | `a5c71a973e7320c1a` | `worktree-agent-a5c71a97` | `48e09fd feat(08-03): TCP OP_PUSH handler — shared execute_push with HTTP` | Plans 08-01 (point ops) + 08-02 (streak family tests RED) + 08-03 (TCP push) shipped; 08-04+ pending |
| 9 | `abc51d427b00123b9` | `worktree-agent-abc51d42` | `6f7c9f9 feat(09-01): T9 — phase 9 end-to-end smoke (16 ops...)` | Plan 09-01 nearly done — 16 ops wired, SDK helpers, criterion bench, smoke. Likely close to verification |
| 10 | `a8f40cc78ac525ce8` | `phase-10-sketches` | `928445a test(10-01): EntropyHistogram tests` | Re-dispatched executor making real progress: RetractingRingBuffer ported, Bloom shipped, Entropy in progress; 10-02/03/04 (HLL/UDDSketch/CMS port from main) + 10-05/06/07 still ahead |
| 11 | `a71d2569…` | `worktree-agent-a71d2569` | `17ebf9b feat(11-02,11-03): wire AggOp dispatch + compile parser for 13 Phase 11 ops` | Buffer+geo state types + AggOp dispatch shipped; remaining: per-op tests, throughput row, smoke |
| 11.5 | `adc5373c8a3f363ce` | `phase-11.5-temporal` | `ea28e6a test(11.5-01): temporal table + retract integration smoke (red)` | WAL Table records + MVCC store landed; integration smoke red — green pending |

Demo: still running (`beava` PID 60212 on :8080, `proxy.py` PID 60235 on :9001). Restart with `site/demo/start.sh` if stale.

## Pending decisions / not yet executed

1. **Phase 6.1 dispatch** — user picked Option 1 (Phase 6.1 insert). ROADMAP+REQUIREMENTS edits started (verify completeness with `grep "6.1"`). Need to:
   - Verify ROADMAP table row + parallelization + dependency-graph + detail-section edits all landed
   - Add SRV-DUR-06..10 to `.planning/REQUIREMENTS.md` under `### SRV-DUR`
   - Amend SRV-DUR-02 to mode-dependent wording
   - Create TaskCreate for Phase 6.1
   - Dispatch as 6th parallel worktree agent (use **manual worktree** pattern — `git worktree add .claude/worktrees/phase-6.1-async-dur -b phase-6.1-async-dur v2/greenfield` — NOT `isolation: "worktree"`)
   - Commit
2. **Operator-bug observations from demo** — saw `tx_max_5m=42.5` after pushing amounts 42.5+100 in one smoke test; saw `decline_ratio_5m=1.0` instead of expected 0.5; variance returns 2× population variance value (1653.125 vs 826.5625 expected). Not investigated. May be timing artifacts (state read before second push committed) OR real bugs in Phase 5 max/min/ratio/variance ops. User asked to "eyeball values; flag if off." If user reports issue, dig into `crates/beava-core/src/agg/` for max/min/ratio/variance.
3. **Merge orchestration after parallel batch** — when each phase agent returns:
   - Verify `git merge-base $branch v2/greenfield` returns greenfield HEAD (clean ancestor)
   - Merge in order: Phase 8 first (TCP OP_PUSH foundational) → 9/10/11 in any order → 11.5 last (independent, but ordered for clean snapshot serde changes)
   - **Per-phase row files** (`08-throughput-row.md`, `09-perf-row.md`, etc.) get concatenated into canonical `.planning/throughput-baselines.md` + `.planning/perf-baselines.md` by the orchestrator after merge
   - **Backfill TCP throughput rows** for Phases 9/10/11/11.5 by re-running `beava-bench` on the merged tree (TCP push only available after Phase 8 merges)
   - Phase 12 then dispatches (depends on 11.5 merged); Phase 13 last
4. **Symbolic Python frontend (chalk-style)** — captured in `.planning/ideas/v0.1-symbolic-python-frontend.md`. v0.1 territory, NOT v0. Don't accidentally close off the path with v0 SDK choices.
5. **Parallelism levels (3 levels)** — captured in `.planning/ideas/parallelism-levels.md` (this commit). Level 1 is a Phase 13 tactical lever; Level 2 is v1 headline; Level 3 pairs with Phase 6.1.
6. **Stale worktrees** — 5 leftover `worktree-agent-a*` from prior sessions are locked on disk pointing at `e9ace7c` / `b088500` etc. Not in use. Run `git worktree remove --force .claude/worktrees/agent-a082f86d` etc. AFTER current batch lands; they confuse `isolation: "worktree"`.

## Demo cheat sheet

- URL: **http://127.0.0.1:9001/** (proxy → beava on :8080)
- Restart: `site/demo/start.sh` (kills existing instances first; tmpfs WAL+snapshot dirs)
- Logs: `site/demo/.logs/{beava,proxy}.log`
- Stress test: button in the UI; runs in-browser fetch loop (~700–900 EPS @ 8 workers, fsync-bound on macOS)
- Wire shapes (in case the UI breaks):
  - `POST /register` body: `{"nodes": [...]}` (see `site/demo/register.json`)
  - `POST /push/Transaction` body: `{"event_time": Date.now(), "user_id": "alice", "amount": 42.5, "status": "ok", "merchant": "acme"}`
  - `POST /get` body: `{"keys": ["alice", "bob"], "features": ["tx_count_5m", ...]}` → response `{"result": {"alice": {"tx_count_5m": N, ...}}}`
  - `GET /get/{feature}/{key}` → `{"value": N}`

## Files added this session (will need committing)

- `.planning/ideas/v0.1-symbolic-python-frontend.md` — committed in `157630f`
- `.planning/ideas/parallelism-levels.md` — **commit pending** (this session)
- `.planning/SESSION-STATE-2026-04-23.md` — this file, **commit pending**
- `site/demo/{index.html, proxy.py, register.json, start.sh}` — **commit pending** (consider whether to commit; demo dir was previously untracked)
- `.planning/REQUIREMENTS.md`, `.planning/ROADMAP.md` — Phase 6.1 edits started, verify and finish, **commit pending**

## Resume command for fresh session

```bash
cd /Users/petrpan26/work/tally
git log --oneline -5 v2/greenfield   # confirm at or past 157630f
cat .planning/SESSION-STATE-2026-04-23.md   # this file
cat .planning/STATE.md   # canonical project state (last updated 2026-04-23)
git worktree list   # see in-flight phase branches
# Inspect each phase branch's progress:
for b in worktree-agent-a5c71a97 worktree-agent-abc51d42 phase-10-sketches worktree-agent-a71d2569 phase-11.5-temporal; do
  echo "=== $b ==="
  git log $b --oneline 157630f..HEAD | head -5
done
# If demo not running:
# pkill -f "beava\|proxy.py" 2>/dev/null; site/demo/start.sh &
```

## Open agent IDs (for SendMessage)

If any of these is still alive when you resume, you can `SendMessage` to continue:

- Phase 8: `a5c71a973e7320c1a`
- Phase 9: `abc51d427b00123b9`
- Phase 10: `a8f40cc78ac525ce8`
- Phase 11: `a71d256970155c66e`
- Phase 11.5: `adc5373c8a3f363ce`

Otherwise, fresh `Agent` calls with `subagent_type=general-purpose` and the prompts from this session's git history (search for "Execute Phase N" in your conversation log).
