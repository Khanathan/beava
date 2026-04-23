# Phase 1: Foundation - Context

**Gathered:** 2026-04-22
**Status:** Ready for planning
**Mode:** Auto-generated (infrastructure phase — smart discuss shortcut)

<domain>
## Phase Boundary

A `beava` binary that boots from config, exposes an HTTP server with `/health` and `/ready` stubs, writes structured JSON logs, and runs under an integration test harness. Nothing domain-shaped — pure scaffolding that every later phase attaches to.

Out of scope for Phase 1: any streams, any primitives, any state, any durability. `/health` and `/ready` are stubs only (recovery-complete flag is hardcoded for now; wired up in Phase 5).

</domain>

<decisions>
## Implementation Decisions

### Claude's Discretion

All implementation choices are at Claude's discretion — pure infrastructure phase. Guidance from `PROJECT.md` + `DESIGN-V2.md`:

- **Language:** Rust (2021 or 2024 edition at Claude's choice)
- **HTTP framework:** `axum` as the recommended choice (matches phase success criterion "wired to axum or equivalent"); use `hyper` under the hood
- **Runtime:** Tokio with a single `current_thread` runtime, pinning deferred until Phase 3 when the apply loop actually does work
- **CLI:** `clap` with derive macros
- **Config:** YAML via `serde_yaml` or `serde_yml`; env-var overrides on top (`BEAVA_*`)
- **Logging:** `tracing` + `tracing-subscriber` with the `json` feature; structured fields, configurable level via env
- **Graceful shutdown:** `tokio::signal` listening on SIGTERM + SIGINT; drains HTTP server, waits for in-flight requests, exits 0
- **Test harness:** In-process spawn of the server using `tokio::spawn`, port allocated from OS (`0`), readiness via polling `/ready` with backoff; exposed as a `TestServer` struct usable by all downstream phase tests
- **Workspace layout:** Cargo workspace with one binary crate (`beava-server`) and one library crate (`beava-core`) so later phases can split cleanly; keep minimal for Phase 1 (can consolidate if single crate proves easier for the scaffolding alone)
- **Lint/format:** `rustfmt` default config, `clippy -- -D warnings` in CI
- **Error handling:** `anyhow` for application errors, `thiserror` for library errors — pattern established here, enforced in later phases
- **Binary size:** `[profile.release]` with `lto = "thin"`, `codegen-units = 1`, `strip = true` to aim at <200MB (REQ-PKG-04 — a Phase 10 gate, but establish settings now)

</decisions>

<code_context>
## Existing Code Insights

The repository was reset to greenfield on commit `78a3a24` for the v2 branch. Nothing to reuse from the v1 codebase (which lives on `arch/tpc-full-shard`). The only pre-existing code surface is:

- `python/pyproject.toml` — Python SDK package scaffolding (v2 SDK work lands in Phase 10)
- `docs/http-api.md`, `docs/http-api-examples.sh` — reference material from v1's HTTP API (shape is close to v2; authoritative reference is `DESIGN-V2.md` §4 and `REQUIREMENTS.md` API-01..API-09)

No Rust code exists. Phase 1 creates the Cargo workspace from scratch.

</code_context>

<specifics>
## Specific Ideas

- Config file path defaults to `./beava.yaml` if `--config` flag omitted; error with clear message if not found
- `/health` returns `200 {"status":"ok"}` immediately after HTTP listener binds
- `/ready` returns `503 {"status":"starting"}` until the `recovery_complete` atomic flag flips (hardcoded to flip after 100ms in Phase 1; real value wired in Phase 5)
- Graceful shutdown must complete in ≤2s under idle load; tested in the harness
- Test harness interface: `TestServer::spawn()` returns `(server_handle, base_url)`, `server_handle.shutdown().await` triggers graceful shutdown
- Binary must emit one JSON log line per significant event (startup, HTTP bind, shutdown initiated, shutdown complete) so operators and the test harness can grep for them

</specifics>

<deferred>
## Deferred Ideas

- **Prometheus `/metrics` endpoint** — deferred to Phase 9 (Observability + performance). Phase 1 logs only.
- **`trace_id` propagation from `X-Trace-Id`** — deferred to Phase 9. Phase 1 logs have no trace correlation yet.
- **Docker image** — deferred to Phase 10. Phase 1 builds the binary only.
- **Configuration schema validation** — just enough for Phase 1's needs (listen_addr, log_level). Full config schema grows with each phase's needs.

</deferred>
