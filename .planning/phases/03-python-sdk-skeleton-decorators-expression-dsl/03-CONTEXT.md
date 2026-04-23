# Phase 3: Python SDK skeleton + decorators + expression DSL - Context

**Gathered:** 2026-04-23
**Status:** Ready for planning
**Mode:** Interactive discuss under `/gsd-autonomous --interactive`
**Depends on:** Phase 2.5 (TCP wire listener landed)

<domain>
## Phase Boundary

Ship the Python SDK skeleton — `@bv.event` / `@bv.table` decorators, `bv.col` expression DSL, and the sync `bv.App` client with dual-transport `register` + `validate`. Users write Python-native feature definitions, call `app.register(...)`, and the wire contract from Phase 2 (HTTP) + Phase 2.5 (TCP, framed, ct=JSON for register) is what they speak.

In scope (21 REQs: SDK-DEC-01..09, SDK-COL-01..08, SDK-APP-01, 02, 03, 15, SDK-WIRE-01..03):
- `@bv.event` (class form and function form) — schema extraction from type hints, optional `event_time` field, zero-config on all soft knobs (uses server defaults)
- `@bv.table(key=..., ttl=...)` — key required, soft knobs defaulted
- `bv.Optional[T]`, `bv.Field(desc=..., default=...)` — per-field metadata primitives
- `bv.col(...)` — operator-overloaded AST producing canonical parenthesized string via `.to_expr_string()`
- `bv.App(url)` sync client — URL scheme dispatches to HTTP (`httpx`) or TCP (stdlib `socket`)
- `app.register(*descriptors)` — DAG topological sort + cycle detection + local schema validation BEFORE wire call, then POST/frame-send, returns assigned `registry_version`
- `app.validate(*descriptors)` — zero-network-IO, same local validation as register's pre-check, returns `list[ValidationError]`
- `ValidationError` structured: `kind` / `path` / `message`, with `str` repr `[{kind}] {path}: {message}`
- End-to-end smoke: spawn Rust binary with both ports, register a 2-event + 1-table DAG over both transports, compare `GET /registry` output

Out of scope (land in later phases):
- Stateless op chain (`.filter`, `.select`, `.with_columns`, etc.) — Phase 4 (server-side evaluator + SDK surface together)
- Aggregations (`group_by().agg()`) — Phase 5
- Joins + unions — Phase 12
- Push family (`app.push`, `app.push_sync`, `app.push_many`, `app.push(Table, key, dict)`, `app.delete`) — Phase 6 (push + WAL) / Phase 12 (push_sync, push_many, push_table)
- Read family (`app.get`, `app.mget`, `app.get_multi`) — Phase 12
- Direct write (`app.set`, `app.mset`) — Phase 12
- `bv.AsyncApp` (async parallel class) — Phase 6 when first async use case (push) lands
- `bv.fork(...)` — Phase 13 packaging milestone
- Python SDK user-facing docs (`pip install beava` quickstart) — Phase 13
- Perf / load benchmarks — Phase 13 (expanded to cover simple fraud, complex fraud, recommendations — see Deferred)

</domain>

<decisions>
## Implementation Decisions

### D-01: Clean-room implementation referencing v1 SDK for shape

- **Do NOT copy source files** from `main` branch `python/beava/`. v1 has Phase-39 assumptions (binary TCP, RocksDB-era app client, stream→event rename half-done) that aren't worth porting.
- **Read v1 for reference**: `_col.py` (operator-overloaded AST pattern), `_types_core.py` (Optional/Field primitives, MISSING sentinel), `_stream.py` (decorator extraction logic), `_validate_v0.py` (DAG/cycle patterns). Re-implement each in `python/beava/` against v2's wire contract.
- v1 tests are informative for edge cases but not ported wholesale.
- Rationale: Phase 2's wire contract is already locked devex-first; copying v1 code drags v1's naming back in. Clean-room keeps the Python surface honest to the v2 wire.

### D-02: `@bv.event` is the only name; no `@bv.stream` alias

- No `DeprecationWarning` alias. v2 is a new product with a new name; no v1 users migrating.
- Export path: `beava` package top-level has `event = _events.event_decorator` (and `bv.event(...)` works by convention — `import beava as bv`).

### D-03: Transport deps — httpx + stdlib socket

- `httpx` is the ONLY new third-party dep added to `pyproject.toml`. Version pin: `httpx>=0.27,<1`.
- HTTP client uses `httpx.Client` (sync). Connection pooling + retries come for free.
- TCP client uses stdlib `socket` + `struct` for frame codec. ~150 LoC. No binary-protocol deps. Matches Phase 2.5's frame shape `[u32 len][u16 op][u8 ct][payload]`.
- MessagePack is NOT added in Phase 3 (`ct=0x02` is reserved; register uses `ct=0x01` JSON). Phase 6 adds `msgpack` dep when push needs it.

### D-04: URL scheme dispatches transport

- `bv.App('http://localhost:7379')` → httpx HTTP client
- `bv.App('https://...')` → httpx HTTPS client (TLS terminates at reverse proxy per Phase 2.5 D-06)
- `bv.App('tcp://localhost:7380')` → raw socket + frame codec
- Internal: `_transport.py` module has `Transport` abstract with `HttpTransport` and `TcpTransport` implementations. `bv.App.__init__` parses URL and picks.
- Both transports implement: `send_register(payload_json: bytes) -> RegisterResponse`, `send_ping() -> PingResponse`, `close()`. Phase 3 only exercises register + ping.

### D-05: Sync-only `bv.App` in Phase 3; `bv.AsyncApp` arrives with push (Phase 6)

- Follows redis-py pattern: two parallel classes sharing a core module.
- Phase 3: `bv.App` ONLY. Users do `with bv.App('http://...') as app: app.register(...)`.
- Phase 6: add `bv.AsyncApp` (parallel class using `httpx.AsyncClient` + asyncio sockets) when `app.push` needs fire-and-forget. Shared serialization lives in `_wire.py` (JSON builders, frame codec, validation).
- No dual-mode single class. No async-first with sync shim.

### D-06: `app.register(*descriptors)` auto-validates locally before wire call

- Register's pipeline:
  1. Run the same DAG/cycle/schema checks as `app.validate(*descriptors)` in-process
  2. If local validation fails: raise `beava.ValidationError` (first error) WITH the full list attached as `.errors` — NO wire I/O
  3. Otherwise: serialize to wire JSON, send via transport, parse response
  4. If server returns 400 / 409: raise `beava.RegistrationError` with the server's `{code, path, reason, message}` structured payload
  5. On 200: update internal state with `registry_version`, return the response dict
- Rationale: saves a round-trip on common mistakes (cycles, type errors, unknown upstreams). Server stays authoritative for schema-vs-existing-registry checks that the client can't run.
- `app.validate(*descriptors)` runs steps 1 only; returns `list[ValidationError]` (empty = ok). Zero network I/O. Uses `cls.__subclasshook__`-style deep inspection, no server state.

### D-07: Schema extraction from type hints via stdlib only (no pydantic, no attrs)

- `@bv.event`-decorated class is inspected via `inspect.get_annotations()` + `typing.get_type_hints()`.
- Supported field types (map to server's `FieldType` enum):
  - `str` → `"str"`
  - `float` → `"f64"`
  - `int` → `"i64"`
  - `bool` → `"bool"`
  - `bytes` → `"bytes"`
  - `datetime.datetime` → `"datetime"`
- Nullable: `bv.Optional[str]` → field appears in `optional_fields` list (distinct from `typing.Optional[str]` to avoid the Union[str, None] ambiguity)
- Per-field metadata: `field = bv.Field(desc="description", default=...)` assigned as class attribute
- Unsupported types (e.g., `list`, `dict`, `Path`, custom classes) → `TypeError` at decoration time with clear message
- Zero dependency beyond stdlib for schema extraction.

### D-08: `bv.col(...)` expression DSL — clean-room AST that emits the v1 canonical string grammar

- Grammar (from v1 `_col.py`, locked because server-side Phase 4 evaluator parses THIS grammar):
  - Field access: bare identifier (`x`, `Stream.x`)
  - Literals: numbers, single-quoted strings, `true` / `false` / `null`
  - Arithmetic: `+` `-` `*` `/`
  - Comparison: `> >= < <= == !=`
  - Boolean: `and` `or` `not` (emitted from Python `&` `|` `~`)
  - Calls: `cast(x, float)`, `isnull(x)` (and future builtins Phase 4 adds)
  - EVERY binary op is parenthesized: `"(amount + tax)"`, not `"amount + tax"` — lets Phase 4 parser stay simple
- AST nodes: `_Field`, `_Literal`, `_BinOp`, `_UnaryOp`, `_Call` (all private)
- Public: `bv.col(name: str) -> _Field`
- Methods: `.to_expr_string()` (canonical serialization), `.isnull()`, `.cast(type_name)`
- Dunders: `__add__ __radd__ __sub__ __rsub__ __mul__ __rmul__ __truediv__ __rtruediv__ __lt__ __gt__ __le__ __ge__ __eq__ __ne__ __and__ __or__ __invert__`
- Type inference: bv.col is untyped in Phase 3 (schema validation happens at `register`/`validate` time against the decorator-extracted schemas). Phase 4 server-side adds stricter checking.

### D-09: Test strategy — subprocess-fixture spawning the Rust binary

- Pytest session fixture `beava_binary` runs `cargo build --bin beava --quiet` once (cached across tests in a session).
- Pytest function fixture `beava_server` launches `./target/debug/beava --http-port 0 --tcp-port 0`, reads stderr for the two startup log lines to extract the OS-assigned ports, yields `(http_url, tcp_url)`, sends SIGTERM on teardown.
- No PyO3. No maturin. No mocks. Real wire, real server, every test.
- CI: session fixture pays the cargo build once; per-test overhead ~50ms from spawn + ~100ms startup. Acceptable for Phase 3's ~20-test smoke.
- Tests live at `python/tests/` (matches existing `pyproject.toml` `testpaths = ["tests"]`).
- End-to-end smoke file: `python/tests/test_phase3_smoke.py` — covers all 7 ROADMAP success criteria for Phase 3.

### D-10: `bv.App()` with NO URL auto-spawns a local Rust subprocess (embed mode)

Phase 3 ships TWO ways to use `bv.App`:

1. **Explicit URL**: `bv.App('http://host:7379')` or `bv.App('tcp://host:7380')` — talks to a running beava over the wire as already described (D-04)
2. **Embed mode**: `bv.App()` with no URL — auto-spawns a local `beava` binary on ephemeral ports and connects to it over TCP. Context-manager cleanup sends SIGTERM + waits for exit. Perfect for notebooks, unit tests, quickstart.

Embed mode discovery order for the binary path:
1. `BEAVA_BINARY` env var (explicit override)
2. `beava` on PATH (user installed via brew / apt / Docker / bundled wheel)
3. `./target/debug/beava` (dev-loop convenience — only used when repo present)
4. Raise `beava.BinaryNotFoundError` with clear message ("install beava: brew install beava | pip install beava[server] | docker pull beava/beava")

Spawn: `beava --http-port 0 --tcp-port 0 --log-format json`. Python reads stderr line-by-line until it sees `{"kind":"server.tcp_bound","addr":"127.0.0.1:PORT"}` and `{"kind":"server.http_bound","addr":"127.0.0.1:PORT"}`, extracts both ports, connects TCP client to the TCP port. Subprocess stderr piped into a background thread that forwards to Python logger at DEBUG level so embed-mode errors are discoverable.

Cleanup: context-manager `__exit__` sends SIGTERM; waits up to 5s for graceful shutdown; SIGKILL if the process doesn't exit. `bv.App()` MUST be used as a context manager in embed mode (enforced by raising in `__init__` if not entered). Explicit-URL mode doesn't require the context manager (stateless client).

Out of scope for Phase 3 (Phase 13 / packaging will pick up):
- Shipping the beava binary inside the Python wheel (platform-specific wheels, post-install download, or separate install — decided in Phase 13)
- Persistence across embed-mode sessions (each embed session is ephemeral — snapshot writing is deferred)
- Multi-instance embed (one App = one subprocess)

Rationale: the single biggest devex gap in "pip install beava and go" is "now also install and run the server". Auto-embed closes that gap for notebook users and test authors with ~200 LoC in `_embed.py`. Uses the SAME Rust engine everyone else uses (no second truth); just manages its lifecycle.

### D-11: `ValidationError` structure

- `kind: str` — one of `"cycle"`, `"missing_upstream"`, `"schema_mismatch"`, `"bad_return_type"`, `"unknown_field_type"`, `"table_key_invalid"`, `"event_time_field_invalid"`, `"registration_conflict"` (last is server-side only)
- `path: str` — e.g. `"Transaction.event_time"`, `"Checkouts.filter[2]"`
- `message: str` — human readable
- `errors: list[ValidationError]` attribute on `RegistrationError` (when server returns 400 with multiple errors)
- `__str__` returns `[{kind}] {path}: {message}`
- Immutable `@dataclass(frozen=True)`

### Claude's Discretion

- Module layout inside `python/beava/`: recommend `_types.py` (FieldType mapping), `_events.py` (decorator), `_tables.py` (decorator), `_col.py` (expression AST), `_app.py` (App class), `_transport.py` (HTTP + TCP), `_wire.py` (JSON payload shape + frame codec), `_validate.py` (local DAG/cycle), `_errors.py` (ValidationError, RegistrationError). Underscored privately, re-exported from `__init__.py`.
- `httpx.Client` lifetime: recommend keeping alive across multiple `register()` calls within one `bv.App` instance; close on context-manager exit or explicit `app.close()`.
- TCP connection reuse: one socket per App instance across `register()` calls (Phase 2.5 allows pipelining but Phase 3 uses sync single-request-at-a-time). Reconnect on broken-pipe.
- Error retry semantics: no automatic retries in Phase 3 — fail loudly. Retries are a v0.x concern.
- How DAG validation handles same-name duplicates in one `register` call — raise `ValidationError(kind="duplicate_name", path=..., message=...)` pre-wire.

### Folded Todos

None.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Locked wire contracts
- `.planning/phases/02-sources-registry-version-bumps/02-CONTEXT.md` — HTTP `POST /register` JSON body shape, success/error response shapes. **Read the amendment header** for devex-first field names. SDK builds JSON that matches.
- `.planning/phases/02.5-tcp-wire-listener/02.5-CONTEXT.md` — D-01 frame format `[u32 len][u16 op][u8 ct][payload]`, D-02 opcode table (`OP_REGISTER=0x0001`, `OP_PING=0x0000`, `OP_ERROR_RESPONSE=0xFFFF`, `CT_JSON=0x01`), D-03 error-frame payload shape, D-04 Redis-style strict FIFO. Python TCP transport re-implements this codec in stdlib socket+struct.
- `crates/beava-core/src/defaults.rs` — `DEFAULT_TOLERATE_DELAY_MS`, `DEFAULT_KEEP_EVENTS_FOR_MS`, `DEFAULT_DEDUPE_WINDOW_MS`. SDK passes `None` / omitted fields; server materializes these at runtime. SDK does NOT reimplement defaults on the Python side.

### Project-level
- `.planning/PROJECT.md` §Constraints + Key Decisions — devex-first naming (surface is plain English, no `idempotency_*`/`watermark_*`/`history_*`), dual wire, zero-config for events (event_time optional; table key required).
- `.planning/REQUIREMENTS.md` §SDK-DEC, §SDK-COL, §SDK-APP — 21 REQs in Phase 3 scope.
- `.planning/ROADMAP.md` Phase 3 — 7 success criteria (updated from 6 to include TCP dual-transport smoke).
- `CLAUDE.md` line 16 — wire compatibility constraint.

### v1 SDK reference (read-only, do not copy)
- `git show main:python/beava/_col.py` — AST pattern reference for bv.col
- `git show main:python/beava/_types_core.py` — Optional / Field / MISSING sentinel pattern
- `git show main:python/beava/_stream.py` — decorator pattern (rename `stream` → `event` in our impl)
- `git show main:python/beava/_app.py` — App structure (strip out binary TCP / v1 wire)
- `git show main:python/beava/_validate_v0.py` — DAG/cycle validation pattern

### Existing code attach points (this repo, v2/greenfield)
- `python/pyproject.toml` — existing pyproject; Phase 3 adds `httpx>=0.27,<1` to dependencies
- `python/beava/` — empty directory; Phase 3 creates all modules here
- `python/tests/` — empty; Phase 3 creates `test_phase3_smoke.py` + per-module unit tests

### External
- `httpx` docs (https://www.python-httpx.org/) — sync Client, error types, context-manager pattern
- `redis-py` source pattern (https://github.com/redis/redis-py) — sync `redis.Redis` + async `redis.asyncio.Redis` parallel-class model; informs D-05
- Python `inspect.get_annotations()` / `typing.get_type_hints()` docs — schema extraction

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable assets (Rust side)
- `crates/beava-core/src/register_validate.rs` — server validation rules are the authoritative spec for local Python validation. Python mirrors the same 9 rules (minus the registry-merge rules 5+7, which need server state).
- `crates/beava-core/src/defaults.rs` — single source of truth for default numeric values. Python does NOT duplicate these.
- Phase 2.5 `TestServer` harness will be extended (as part of Phase 2.5 plan-05) to bind both ports; Python subprocess-fixture talks to the deployed binary, not the test harness.

### Established patterns
- Python module convention: `_private.py` implementation, re-exported from `__init__.py` under the user-facing name. Matches v1 SDK's layout.
- Test pattern: pytest subprocess fixture spawns Rust binary, extracts ports from startup JSON logs (Phase 1's tracing structured logging makes this reliable).
- JSON shape: matches HTTP exactly; TCP just wraps the SAME JSON bytes in a frame.

### Integration points
- `bv.App` → sends REGISTER payload to server. Server shared logic `execute_register` (Phase 2.5 plan-03 extraction) is the single codepath; HTTP and TCP both hit it.
- `bv.col` → user composes expressions; `.to_expr_string()` output is what server Phase 4 evaluator parses (canonical grammar locked in v1 `_col.py`).
- `@bv.event` / `@bv.table` → emit registration nodes that match server's JSON DAG shape (`{kind: "event"|"table"|"derivation", name, schema, ...}`).

</code_context>

<specifics>
## Specific Ideas

- **v1's `_col.py` parenthesization rule is non-negotiable** — Phase 4 server parser relies on every binary op being wrapped. Python AST's `.to_expr_string()` MUST emit `"(a + b)"`, never `"a + b"`.
- **Schema extraction error messages need examples** — "unsupported type `list[int]`; supported: str, int, float, bool, bytes, datetime" is better than "bad type hint".
- **ValidationError's `path` format** — follow server's pseudo-JSON-pointer: `Transaction.event_time`, `Transaction.ops[2].expr`. Lets users grep their Python source.
- **Subprocess fixture stderr parsing** — match on the structured JSON log `{"kind":"server.bound","addr":"127.0.0.1:PORT"}` (Phase 1 pattern). Extend to pick up TCP bind log too. Fail fast if either line doesn't arrive within 5s.
- **Registry response parsing** — `GET /registry` is dev-only; Phase 3 smoke uses it to verify both transports produced identical state. Python reads it via HTTP only (TCP doesn't have a get-registry opcode in v0 — it's debug-only and HTTP is fine).
- **No bv.Duration class in Phase 3** — duration strings like `"5s"`, `"24h"`, `"7d"` are validated as strings in Phase 3 (just shape check, no parsing). Phase 5 (aggregation + windows) parses them into ms. Matches v1 pattern.

</specifics>

<deferred>
## Deferred Ideas

- **Load tests for actual EPS on simple fraud / complex fraud / recommendation pipelines** — routed to Phase 13. ROADMAP Phase 13 currently says "≥3M EPS on 5-aggregation fraud shape"; user expanded scope 2026-04-23 to three benchmark pipeline shapes. Update Phase 13 roadmap entry when it comes time to plan. New REQs to add: PERF-FRAUD-SIMPLE, PERF-FRAUD-COMPLEX, PERF-RECO.
- **`bv.AsyncApp`** — Phase 6 (when `app.push` lands and async matters)
- **`bv.fork(...)` scoped replica** — Phase 13 packaging
- **Retries / backoff on transport errors** — v0.x; Phase 3 fails loudly
- **Connection pooling tuning on httpx** — defaults fine for v0
- **pip install benchmarks** (`pip install beava && benchmark`) — Phase 13
- **Typed bv.col (`bv.col[int]("x") + 1` works, `bv.col[str]("x") + 1` errors)** — v0.x; Phase 3 is untyped
- **Python doc generation (Sphinx / mkdocs)** — Phase 13 docs milestone
- **SDK-TRACE headers for request IDs across transports** — v0.x

</deferred>

---

*Phase: 03-python-sdk-skeleton-decorators-expression-dsl*
*Context gathered: 2026-04-23*
*Depends on Phase 2.5 (currently planning in background)*
*Discuss mode: interactive (gsd-autonomous --interactive)*
