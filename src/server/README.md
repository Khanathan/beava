# `src/server/`

The HTTP and TCP server layer — everything that accepts external connections.
Routing, request handlers, auth middleware, and the split between the public
read surface and the admin write surface all live here. This module owns the
network boundary; once a request is validated and decoded it hands off to the
engine via `EngineHandle`.

## Files

- **`http.rs`** — axum router; binds the HTTP port, wires the auth middleware
  stack, mounts read routes (`/features`, `/public/*`) and admin routes
  (`/push`, `/push-batch`, `/pipelines`, `/snapshot`, `/debug/*`).
- **`http_ingest.rs`** — `POST /push/{stream}`, `POST /push-batch/{stream}`,
  and `POST /push/{stream}/ndjson` handlers (added in Phase 45). Reuses
  `handle_push_core_ex` and `handle_push_batch` from the TCP path so HTTP and
  TCP share one ingest implementation.
- **`tcp.rs`** — binary protocol server; admin listener, `handle_push_core_ex`
  (the shared push entry-point), `replica_ingest_batch` (fork-replica ingest),
  `run_backfill`. This is the largest file in the layer — start reading at the
  top-level `fn accept` loop.
- **`auth.rs`** — `require_loopback_or_token` middleware: rejects unauthenticated
  writes from non-loopback origins, accepts loopback unconditionally, and
  accepts `BEAVA_ADMIN_TOKEN` bearer auth everywhere else. Applied uniformly
  across push and admin routes.
- **`protocol.rs`** — wire format constants and framing helpers for the TCP
  binary protocol.
- **`replica.rs`** / **`replica_client.rs`** — server-side listener and
  client-side connector for fork-replica replication.
- **`latency.rs`** / **`throughput.rs`** — in-process histogram accumulators
  powering the `/debug/latency` and `/debug/throughput` endpoints.
- **`signals.rs`** — OS signal handler (SIGTERM → graceful shutdown).
- **`shard_probe.rs`** — background health probe used by the replica shard
  discovery path.

## Not here

- Pipeline execution and operator logic (see `../engine/`).
- Persistent state, WAL, and snapshots (see `../state/`).
- Python SDK (see `python/beava/` at the repo root).

## Read order

New contributors should read `http.rs` → `http_ingest.rs` → `tcp.rs` →
`auth.rs`. The engine contract is `EngineHandle` — that type is the boundary
between the server layer and everything below it.
