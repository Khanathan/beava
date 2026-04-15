# Phase 36: Replica-mode server boot - Context

**Gathered:** 2026-04-15
**Status:** Ready for planning
**Mode:** Interactive discuss (user directive: "Option M, no snapshot seed — persist CDC and full replay is enough")

<domain>
## Phase Boundary

Add boot-time "replica mode" to the existing Tally server. When launched with `--replica-from HOST:PORT --replica-since <ISO-8601 or ms> --replica-streams X,Y --replica-token T [--replica-keys foo,bar | --replica-key-prefix P]`, the server:

1. Connects to the remote Tally cluster.
2. Calls the new `OP_LOG_FETCH{from_ts_millis, scope}` (Phase 35) to stream all historical events matching scope.
3. Feeds each event into its **own local ingest path** — same path as a local PUSH, which persists to the per-stream log (Phase 6) and runs through any registered pipelines.
4. After LOG_FETCH hits `REPLICA_FRAME_TAG_END`, calls `OP_SUBSCRIBE{scope}` on a new connection and continues tailing, feeding events into local ingest.
5. Opens its own TCP/HTTP listeners ONLY AFTER historical catchup completes — so scientists connecting with `tl.Client` never see a mid-backfill inconsistent state.

No snapshot seed. Pure CDC replay from time T + live tail.

**Out of scope:**
- Snapshot seeding (user dropped from MVP).
- Resume across restarts (v0.2 — just re-fork for now).
- S3 backfill — stretch.
- Reverse compatibility with the older embedded-client `tally_cli clone` path — Phase 38 mothballs that.

</domain>

<decisions>
## Implementation Decisions (LOCKED)

### Guiding principle
**Easiest for demo.** Reuse everything: the server's existing ingest path, the Phase 6 log code, the existing listener stack, the existing REGISTER flow for pipelines. Add the minimum boot glue.

### Flags
- `--replica-from HOST:PORT` — remote cluster endpoint.
- `--replica-since <timestamp>` — either `YYYY-MM-DDTHH:MM:SSZ` or `u64_millis`. Parsed once at boot to `from_ts_millis`.
- `--replica-streams comma,sep,list` — scope streams.
- `--replica-keys k1,k2` OR `--replica-key-prefix P` — scope key filter (optional; default pulls all keys for the scoped streams).
- `--replica-token T` — admin token for the remote. Falls back to `TALLY_REPLICA_TOKEN` env var.
- `--replica-block-until-catchup` — default **true** (gate listeners). Flip to false for advanced users who want concurrent backfill + serve. v0 default = true.
- `--replica-pipeline-file FILE` (optional) — path to a REGISTER JSON to load before catchup starts. Lets scientists pre-seed the replica with their pipelines from a file. If omitted, scientist must REGISTER via HTTP after listener opens.

### Listener gating
- With `--replica-block-until-catchup=true`: bind local TCP+HTTP listeners only after LOG_FETCH + initial SUBSCRIBE handshake succeed. Query endpoints return only consistent state.
- With false: bind immediately; queries during catchup return whatever state-so-far exists. Documented as "may be incomplete".

### Local ingest routing
- Incoming events from LOG_FETCH and SUBSCRIBE flow into the same function the normal PUSH handler uses (e.g., `pipeline.push_internal` or the equivalent top of the ingest path). This means:
  - They get written to the local per-stream log → persistence for free.
  - They run through any registered pipelines → scientist's pipelines compute aggregates.
  - They trigger the subscriber-registry notify hook → if the scientist has their own subscribers locally, those get notified too.
- **Do NOT call the admin-auth gate** on incoming replica events — they come from an authenticated outbound connection. Mark the push as "system origin" via a flag or internal entry point.
- **Do NOT re-emit replica events** out via the server's own OP_SUBSCRIBE as a feedback loop. Easy way to ensure this: gate the subscriber-registry notify by a "not from replica feed" check, OR give the replica path a dedicated `replica_ingest_internal` function that bypasses the subscriber notify. We can revisit if there's a real use case for downstream subscribers, but not in v0.

### Failure policy
- LOG_FETCH connection failure at boot: retry 5 times (exponential 1s→30s ±20%), then exit 1 with a clear error. Scientist re-runs.
- SUBSCRIBE drop after catchup: log + signal, reconnect with `from_ts_millis = last_applied_event_timestamp`. Accept duplicates at boundary. Give up after 10 consecutive failures within 1 minute → log fatal + exit 1 (prevents zombie replicas).
- No automatic LOG_FETCH catchup retry in-flight if the connection dies mid-stream — just restart LOG_FETCH from the last persisted event's timestamp.

### Scientist pipeline seeding
- Simplest: scientist HTTP-POSTs a REGISTER request to the replica's `/register` endpoint after listeners open. Existing Tally machinery.
- Optional convenience: `--replica-pipeline-file` parses a JSON and calls register directly at boot (before LOG_FETCH kicks off, so the catchup already routes events through their pipeline).
- Document the preferred flow in the plan: register before running, via either `--replica-pipeline-file` or a pre-catchup HTTP call (if `--replica-block-until-catchup=false`).

### What the replica DOESN'T do
- Does not accept external PUSH from local clients (for v0). All its events come from remote. Flip via `--replica-allow-push` (future, not MVP). This keeps the replica honest as a read-only fork.
- Actually: let scientists locally PUSH synthetic events too, for pipeline experimentation. Revisit — document as TBD decision in the plan. **Default for MVP: reject local PUSH with a clear error.** Scientists wanting synthetic events use a test-helper flag.

### Plan split
- One plan (36-01) with four tasks:
  1. CLI flag parsing in `src/main.rs` / wherever the server boots.
  2. Replica-client loop module: drives LOG_FETCH then SUBSCRIBE, routes events to local ingest.
  3. Listener-gate wiring: bind ports only after catchup signal fires (when `--replica-block-until-catchup=true`).
  4. Integration test: spin up prod, run replica, verify events flow in + queryable via local `tl.Client`.

</decisions>

<code_context>
- `src/main.rs` — server entry point; add replica-mode branch.
- `src/server/app.rs` (or wherever `AppState` is constructed) — initialize replica client alongside normal server.
- `src/server/tcp.rs` + `src/server/http.rs` — listener startup; gate behind catchup-done signal.
- `src/engine/pipeline.rs` — existing `push_with_cascade_internal` (or similar) = the ingest entry point to reuse.
- `src/client/session.rs` + `src/client/wire.rs` — already have the client-side codec for SNAPSHOT_FETCH; extend for LOG_FETCH + SUBSCRIBE consumption. The Phase 28 `fetch_snapshot` helper and Phase 31-01's SUBSCRIBE dance are both reusable here (we're re-using the "client side of the protocol" machinery, which was built for the embedded-client path but works equally well for server-to-server).
- `src/server/admin.rs` (or wherever the REGISTER handler lives) — read for the `--replica-pipeline-file` pre-seed flow.

</code_context>

<specifics>
- **Timestamp-based resume after SUBSCRIBE drop**: on each applied event, track `last_applied_event_timestamp_ms` (atomic u64). On reconnect, send LOG_FETCH from that value. Accepts duplicate at boundary; scientist's pipeline tolerates via watermarks.
- **Concurrent runtime**: the replica client loop runs on the server's existing tokio runtime. No new thread pool.
- **Read-only vs read-write**: document clearly in the startup log that "replica mode accepts events only from --replica-from; local PUSH is rejected."
- **Logging**: on startup, log the effective flags. On catchup-complete, log "replica caught up to {timestamp}; opening listeners on :{port}". On SUBSCRIBE drop, log with the reconnect attempt count.

</specifics>

<deferred>
- Snapshot seed for faster catchup on large history — revisit if replay time becomes a UX problem.
- Allow local PUSH of synthetic events — behind a test-helper flag.
- Multi-remote replica (aggregate several prod clusters) — v1 feature.
- Resume across replica restarts — v0.2 (persist `last_applied_timestamp` to disk).
- Snapshot takeover during live run (if catchup can't keep up, pull a new snapshot) — v0.2.

</deferred>

---

*Phase: 36-replica-server-boot*
*Source: user directive 2026-04-15 — Option M, no snapshot seed*
