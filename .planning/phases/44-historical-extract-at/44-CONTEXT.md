# Phase 44: Historical extraction at timestamps — one-pass replay - Context

**Gathered:** 2026-04-15
**Status:** Ready for planning
**Mode:** Direct fix — user directive "one replay, accumulate snapshots at each checkpoint"

<domain>
## Phase Boundary

Add the ability for a data scientist to extract feature state at multiple historical timestamps in a single fork invocation. One replay streams events from `since` forward; as the replay passes each declared `extract_at` timestamp, snapshot the current scope-key state. Return all snapshots at the end.

**In scope:**
- Server boot flag `--replica-extract-at T1,T2,...` (comma-separated ISO-8601 or unix millis; server sorts).
- Replica-client loop tracks a sorted cursor; snapshots state just before applying the first event with `event.ts > extract_at[i]`.
- New storage on AppState: `extracted_history: DashMap<u64_millis, DashMap<String, serde_json::Value>>` — keyed by extraction timestamp, inner keyed by entity key, value is the computed feature map at that moment.
- New HTTP endpoint `GET /extracts` returns `{ts: {key: features}}` JSON.
- `tally fork --extract-at T1,T2,...` CLI passthrough.
- Python `tl.fork(..., extract_at=[T1, T2, ...])` + `fork.extract_history() -> dict`.
- E2E integration test.

**Out of scope:**
- Checkpointed resume (replay always starts from `--since`).
- Query-time arbitrary point-in-time (that would need time-travel indexing; deferred).
- Pausing mid-replay for ad-hoc extraction.

</domain>

<decisions>
## Implementation Decisions (LOCKED)

### Guiding principle
One fork = one replay = all extractions. Scientist supplies timestamps at fork time, gets back dict of snapshots after replay completes.

### Wire format (CLI + Python)
- `--replica-extract-at 2026-03-05T10:00:00Z,2026-03-15T10:00:00Z,2026-04-01T10:00:00Z`
- Server parses each entry as ISO-8601 OR unix-millis u64 (same parser as `--replica-since`).
- Empty / absent → no extraction; normal replay completes and transitions to SUBSCRIBE as before.

### Extraction semantics
- Given sorted `extract_at = [T1, T2, ..., Tn]` and a stream of events in timestamp order:
  - Maintain cursor `i = 0`.
  - Before applying event E: while `i < n && E.ts > extract_at[i]`: snapshot state, `i += 1`.
  - Apply E.
- After replay completes (LOG_FETCH END): for any remaining `extract_at[i..n]` that weren't crossed, snapshot state NOW (end of replay).
- Snapshot = iterate scope keys, compute the feature map for each via the existing per-key debug lookup path (reuse `debug_key` handler internals).

### Storage
- New AppState field: `pub extracted_history: DashMap<u64, DashMap<String, serde_json::Value>>`.
  - Outer key: extraction timestamp in unix millis.
  - Inner key: entity key string.
  - Inner value: JSON object of `{feature_name: feature_value}` for the scope-declared pipelines.
- Lock-free on write (two DashMap levels + serde::Value storage).
- Exposed via `GET /extracts` → JSON `{"extracts": {ts_iso: {key: features}}}`.

### Listener gating
- With `--replica-extract-at` set, keep the existing `--replica-block-until-catchup=true` default. Listeners open ONLY after LOG_FETCH END + all extractions captured.
- SUBSCRIBE phase still runs after extraction; live tail is normal.
- Extractions remain queryable via `/extracts` for the server's lifetime.

### Scope
- Extractions are limited to the replica's declared scope (`--replica-keys` or `--replica-key-prefix`). Keys outside scope not snapshotted.
- If scope is keyed-stream-only with no specific keys, snapshot iterates the StateStore's entity set at extraction time (whatever entities have been seen so far).

### CLI
- `tally fork --extract-at T1,T2,...` translates to `--replica-extract-at`.

### Python
- `tl.fork(..., extract_at=[datetime|str|int])` — accepts Python datetime, ISO-8601 string, or unix millis int. Serializes to the CLI format.
- New method `ForkedReplica.extract_history() -> dict`:
  - Blocks until `/debug/ready` returns 200 (already waited in `with` enter).
  - GETs `/extracts`, parses, returns `{datetime: {key: {feature: value}}}`.

### Plan split
One plan, four tasks:
1. T1: Server-side `extract_at` parsing + AppState field + snapshot hook in replica client.
2. T2: `/extracts` HTTP endpoint.
3. T3: CLI flag on `tally fork` + Python `tl.fork()` kwarg + `extract_history()` method.
4. T4: E2E integration test (Python).

</decisions>

<code_context>
- `src/main.rs` — `ReplicaBootConfig` struct, add `extract_at: Vec<u64>`.
- `src/server/replica_client.rs` — existing `run_historical_catchup` loop, add cursor + snapshot-before-apply.
- `src/server/tcp.rs` — `ConcurrentAppState`, add `extracted_history` field.
- `src/server/http.rs` — add `debug_key` equivalent snapshot helper + `/extracts` endpoint.
- `src/main.rs` (fork subcommand) — add `--extract-at` flag, translate to `--replica-extract-at`.
- `python/tally/_fork.py` — add `extract_at` kwarg + `extract_history()` method.

</code_context>

<specifics>
- `extract_at` sort on server side — accept any order from scientist, sort at boot.
- Timestamp parse reuses Phase 36's `parse_replica_since` helper.
- Snapshot function must respect the existing `debug_key` handler's feature lookup logic (which computes live values via the engine's operators). Re-use, don't reimplement.
- Scope keys may not all have events by the time of extraction. For keys with no events, emit `null` or skip the key. Decision: skip; consistent with normal "missing key → None" semantics.
- Memory: N extractions × K scope keys × F features = bounded. For the demo K=2-10 keys, N=3-10 timestamps, F<20 → trivial.

</specifics>

<deferred>
- Checkpointed resume (restart replay from last snapshot instead of `--since`).
- Arbitrary query-time point-in-time.
- Extract during SUBSCRIBE mode (live tail).
- Compression of extracted snapshots.

</deferred>

---

*Phase: 44-historical-extract-at*
*Source: user directive 2026-04-15 — "one replay from the last point should be able to extract all data. We accumulate data as we replay to return it to user"*
