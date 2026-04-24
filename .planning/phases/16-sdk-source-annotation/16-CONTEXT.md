---
phase: 16-sdk-source-annotation
type: context
created: 2026-04-24
updated: 2026-04-24
status: locked
---

# Phase 16 CONTEXT — SDK surface v0 ergonomics

**Theme:** Stabilize the public Python-SDK surface at v0.0 before the ship-gate.
Make "this is an external push target" explicit via a required `@bv.source`
annotation, promote `app.upsert` / `app.delete` as the named verbs for
writing to tables, rename the HTTP wire paths to match, and add two
throughput-friendly client affordances (lingering + pipeline context
manager). Result: public API that reads the same way the user reasons
about the pipeline — sources are push targets; derivations are computed;
verbs match at every layer (SDK, HTTP, TCP).

**Scope budget (updated 2026-04-24):** ~500 LoC production + tests +
migrations across ~103 existing decorator sites. Python SDK heavy; small
Rust route-rename in server; no new wire format.

Breakdown:
- Plan 01 (source marker + validators): ~180 LoC (was ~180; unchanged)
- Plan 02 (upsert/delete verbs + server 400 + HTTP rename hard break): ~220 LoC (was ~160)
- Plan 03 (mechanical test + docs + website migration + microbench): ~50 LoC core + ~300 LoC mechanical diff (unchanged)
- Plan 04 (client-side lingering): ~150 LoC Python + tests
- Plan 05 (pipeline context manager): ~100 LoC Python + tests
- Plan 06 (VERIFICATION + SUMMARY + throughput run): ~80 LoC docs

**Not in scope (explicitly):**
- Server-side registry validation rework beyond adding the
  `cannot_push_to_derivation` error shape for upsert/delete against
  non-source tables.
- Phase 14 `tolerate_delay_ms` runtime semantics — Phase 16 only enforces where
  that kwarg may be *declared*.
- Phase 14.1 `modifiable=True` — same: Phase 16 only enforces the declaration
  home.
- Server-side batching or coalescing — Plans 04/05 are pure client-side
  wins using existing server wire (no server changes).
- New TCP opcodes — pipeline CM re-uses existing Phase 2.5 framed-TCP
  strict-FIFO; only client composition changes.

---

## Baseline observations (grounded in repo at HEAD `77276e0`)

1. **No `@bv.source` exists in the codebase today.** Single match in
   `.planning/ROADMAP.md` + phase-14 context, both documentary.
2. **Python SDK does NOT yet expose `app.push_table` / `app.delete_table`.**
   `_app.py` only has `register`, `validate`, `ping`, `close`. Phase 12's
   push/get wiring is still pending (ROADMAP marks Phase 12 🟡 PARTIAL).
   **Consequence:** Plan 02 introduces `app.upsert` / `app.delete` as the
   *first* table-write verbs on the Python client — there is no legacy
   `push_table` / `delete_table` client method to deprecate. GA-2
   resolved 2026-04-24: no deprecation aliases.
3. **Server HTTP endpoints `/push-table/{name}` + `/delete-table/{name}`
   exist** (Phase 11.5, `crates/beava-server/src/temporal_http.rs` line
   565). Plan 02 Task 4 renames these to `/upsert/{name}` + `/delete/{name}`
   as a **hard break** (D-09, 2026-04-24): old paths return 404. Pre-v0
   wire; no deprecation window owed.
4. **Class-form vs function-form distinction lives in
   `python/beava/_events.py` `event()` dispatch (`inspect.isclass`) and
   `python/beava/_tables.py` `table()` dispatch.** Decoration outcome is
   `EventSource` / `TableSource` for class-form, `EventDerivation` /
   `TableDerivation` for function-form.
5. **~103 decorator call-sites repo-wide** (26 in `python/tests/`, rest in
   `docs/` + `beava-website/`). Plan 03's migration task is the biggest by
   LoC.
6. **Source-only schema flags today:** `keep_events_for`, `tolerate_delay`,
   `dedupe_key`, `dedupe_window` (on `@bv.event`) and `ttl`, `mode=upsert` (on
   `@bv.table`) — already reach `EventSource` / `TableSource` only by
   construction.
7. **Server upsert/delete callers** to migrate to new path after D-09:
   - `crates/beava-server/tests/phase11_5_temporal_smoke.rs` (3 sites)
   - `crates/beava-bench/src/bin/temporal_throughput.rs` (2 sites)
   - Any test/doc/blog/website curl snippet referencing `/push-table/` or
     `/delete-table/`

---

## Locked decisions

### D-01 — `@bv.source` required on class-form decorations
Class-form `@bv.event` and `@bv.table` decorations MUST be annotated with
`@bv.source`. Example:

    @bv.source
    @bv.event(keep_events_for="7d")
    class Transaction:
        user_id: str
        amount: float

    @bv.source
    @bv.table(key="user_id")
    class UserProfile:
        user_id: str
        tier: str

**Mechanism (commutative marker, D-03 resolution):** `@bv.source` is a
**pure marker** decorator that sets `_beava_is_source = True` on its
target and returns it unchanged. It works **commutatively** — order with
`@bv.event` / `@bv.table` does not matter:

    # Both forms are equivalent:
    @bv.source
    @bv.event(...)
    class A: ...

    @bv.event(...)
    @bv.source
    class B: ...

Achieved by `@bv.source` accepting either a raw class OR a descriptor
instance:
- If target is a raw class (has no `_beava_kind`): stamp
  `target._beava_is_source = True` and return the class. When `@bv.event`
  / `@bv.table` later wraps it, they propagate the stamped flag into the
  returned descriptor.
- If target is a descriptor (`_beava_kind in ('event','table')`): set
  `desc._beava_is_source = True` on the descriptor and return it.
- If target is an `EventDerivation` / `TableDerivation`
  (`_beava_kind == 'derivation'`): raise TypeError
  `BV-E-SOURCE-ON-DERIVATION` at decoration time.
- If target is a plain function: raise TypeError (callable + not
  `type`) with a message telling the user `@bv.source` belongs on
  class-form sources.
- If target is a raw class with neither `@bv.event` nor `@bv.table` ever
  wrapping it: detected at register-time (the class never becomes a
  descriptor), error `BV-E-SOURCE-MISSING` — same code path as a
  bare-class `@bv.event` missing the source marker.

### D-02 — `@bv.source` forbidden on function-form (decoration-time)
Applying `@bv.source` to an `EventDerivation` / `TableDerivation` or to
a plain function raises `TypeError` with code
`BV-E-SOURCE-ON-DERIVATION` at decoration time. Message names the
derivation (or the function) and points at the file:

    @bv.source is only valid on a class-form @bv.event / @bv.table
    (external push targets); {name!r} is a derivation (function-form) and
    must NOT be annotated @bv.source.

### D-03 — Class-form without `@bv.source` → register-time error (RESOLVED)
GA-1 resolved 2026-04-24: **register-time** `BV-E-SOURCE-MISSING` is the
enforcement point. Decoration-time for the *missing* case is
infeasible in Python without `sys.settrace` (the decorator chain has no
look-ahead). The commutative marker design makes decoration-order grey
area (GA-3) also moot.

Calling `app.register(SomeClassFormSource)` where the source's
`_beava_is_source` is still `False` raises `RegistrationError` with code
`source_missing` and message containing `BV-E-SOURCE-MISSING` **before
any network I/O** (via pre-send local validate in `_validate.py`).

Decoration-time checks that DO fire:
- `@bv.source` on a plain function → TypeError `BV-E-SOURCE-ON-DERIVATION`
- `@bv.source` on a derivation descriptor → TypeError
  `BV-E-SOURCE-ON-DERIVATION`
- `@bv.source` alone on a raw class with no paired `@bv.event` /
  `@bv.table` later → detected at register-time with `BV-E-SOURCE-MISSING`
  (raw class never becomes a descriptor, so register rejects it)

### D-04 — `app.upsert` / `app.delete` verbs
`python/beava/_app.py` gains two methods:

    def upsert(self, target, row: dict[str, Any]) -> dict[str, Any]: ...
    def delete(self, target, *, key: dict[str, Any]) -> dict[str, Any]: ...

- `target` is a `TableSource` descriptor. `app.upsert` / `app.delete`
  are table-only (events are pushed via `app.push`, Phase 12).
- Server wire (post-D-09): `POST /upsert/{target._name}` with `row` as
  JSON body; `POST /delete/{target._name}` with `{"key": {...}}`.
- Transport dispatch: `Transport.send_upsert(name, row_bytes)` +
  `send_delete(name, key_bytes)`. Same wire payloads as the old
  push-table/delete-table handlers; only the URL path differs.
- No deprecation aliases (GA-2 resolved).

### D-05 — Upsert against derivation → 400 `cannot_push_to_derivation`
Two layers:

1. **Python SDK:** `app.upsert(target, ...)` checks `target._beava_kind
   == "table"` AND `target._beava_is_source is True`; else raises
   `ValueError` with `BV-E-PUSH-TO-DERIVATION` before any network I/O.
2. **Server:** `temporal_http.rs` upsert_handler / delete_handler
   reject with 400 `cannot_push_to_derivation` when the resolved
   descriptor is a derivation.

Error body shape:
`{"error":{"code":"cannot_push_to_derivation","path":<name>,"reason":"..."}}`

### D-06 — Schema flags live only on `@bv.source @bv.event(...)` / `@bv.source @bv.table(...)`
`tolerate_delay_ms` (Phase 14), future `modifiable=True` /
`modification_log_depth` (Phase 14.1), `keep_events_for`, `dedupe_key`,
`dedupe_window`, `ttl` — all source-only. Passing any to function-form
raises `BV-E-SOURCE-FLAG-ON-DERIVATION` at decoration time (previously
silent-ignored).

### D-07 — Warning codes
For v0 we ship as errors (not warnings). Error codes registered this phase:
- `BV-E-SOURCE-ON-DERIVATION` (TypeError, decoration time)
- `BV-E-SOURCE-MISSING` (RegistrationError, register time)
- `BV-E-PUSH-TO-DERIVATION` (ValueError client-side; 400
  `cannot_push_to_derivation` server-side)
- `BV-E-SOURCE-FLAG-ON-DERIVATION` (TypeError, decoration time)
- `BV-E-PIPELINE-HTTP` (ValueError, raised when `app.pipeline()` is
  invoked on an HTTP-transport App — see D-11)

### D-08 — DAG root-source tracing in `_validate.py`
`validate_descriptors` gains Rule 9 (no_root_source). See original D-08
— unchanged.

### D-09 — HTTP wire rename: hard break, no deprecation aliases (NEW, 2026-04-24)
- `POST /push-table/{table}` → `POST /upsert/{table}` (hard break; old
  path returns 404)
- `POST /delete-table/{table}` → `POST /delete/{table}` (hard break; old
  path returns 404)
- Rationale: pre-v0. The server has never been publicly released; there
  are no external users on the old paths. A clean rename now saves us
  carrying two aliases through the v0 window.
- `crates/beava-server/src/temporal_http.rs` route registration changes
  at line 565 area.
- `crates/beava-server/tests/phase11_5_temporal_smoke.rs` (3 sites) and
  `crates/beava-bench/src/bin/temporal_throughput.rs` (2 sites) must be
  updated. Integration smoke for the hard-break: a test POSTing to
  `/push-table/merch` asserts 404.
- Python SDK `_transport.py` `send_upsert` / `send_delete` target the
  new paths from inception (they land in the same Plan 02 as the route
  rename; no intermediate state where SDK and server disagree).

### D-10 — Client-side lingering (NEW, 2026-04-24)
Opt-in client-side batching for high-throughput producers.

- `bv.App(url, *, linger_ms=0, max_batch=256)` — two new constructor
  kwargs. `linger_ms=0` default → no behavior change for existing
  callers.
- When `linger_ms > 0`: `app.push(event, row)` returns immediately;
  the SDK buffers per-`Event`-descriptor calls in memory and flushes
  when either:
  - `linger_ms` elapses since first buffered call for that stream, OR
  - `max_batch` events are buffered for that stream.
- Flush dispatches a single `push_many`-shaped request (one HTTP/TCP
  call for N events), using the existing wire.
- `with bv.App(...) as app:` context manager flushes on `__exit__`.
- `app.flush()` — explicit flush; returns when all in-flight buffers
  land.
- `app.close()` — flush + release transport.
- Python SDK only; zero server changes. Server already accepts
  `push_many` since Phase 12 wiring (verify at plan start; if
  `push_many` server wire is NOT yet in, Plan 04 either waits on
  Phase 12 or degrades to sequential single-push within the lingering
  buffer — plan checker must resolve this before execution).

### D-11 — Pipeline context manager (NEW, 2026-04-24)
Multi-op request batching over framed-TCP strict-FIFO.

- `with app.pipeline() as pipe:` returns a `Pipeline` helper.
- `pipe.push(Event, row)`, `pipe.get(feature, key)`,
  `pipe.upsert(Table, row)`, `pipe.delete(Table, *, key)` —
  all queue the op; return no value (the ops are in-flight, results
  await context exit).
- Context `__exit__`: flushes all queued ops over the framed-TCP
  connection in order (Phase 2.5 strict-FIFO guarantees response
  order matches request order); awaits responses; returns `list` of
  op results indexed parallel to submission order via
  `pipe.results()` or via the returned context-manager yield
  (implementation detail — pick whichever reads cleaner in tests).
- **TCP-only**: invoked on an HTTP-transport App → immediately
  raises `ValueError` with `BV-E-PIPELINE-HTTP` message directing
  the user to a TCP URL.
- Zero server changes — Phase 2.5 framing + strict-FIFO are the
  primitives; the pipeline CM is a client-side composition only.
- Performance expectation (for test-assertability): with 20 mixed
  ops, observed round-trip time must be < 2 RTTs (ideally ~1 RTT
  if the server is idle); benched against a 20-sequential-ops
  baseline in Plan 05 tests.

---

## Success criteria (SC1–SC9)

1. `@bv.source @bv.event(...)` / `@bv.source @bv.event(...)` class-form
   registration succeeds; descriptor carries `_beava_is_source = True`.
   (Also: reversed order works, per commutative marker design.)
2. `@bv.source` applied to function-form / plain function raises
   `TypeError` (`BV-E-SOURCE-ON-DERIVATION`) at **decoration time** —
   before `app.register`.
3. Class-form without `@bv.source` raises `RegistrationError`
   (`BV-E-SOURCE-MISSING`) at `app.register()` pre-network I/O (GA-1
   resolved: register-time).
4. `app.upsert(SourceTable, {...})` writes to server `POST
   /upsert/{name}`; `app.delete(SourceTable, key={...})` writes to
   `POST /delete/{name}`; both succeed on source tables.
5. `app.upsert(DerivedTable, {...})` raises `ValueError`
   (`BV-E-PUSH-TO-DERIVATION`) client-side before any network I/O; if
   SDK guard is bypassed (curl), server returns 400
   `cannot_push_to_derivation`.
6. All existing Python SDK tests + docs + website samples migrated to
   the new surface; `pytest` green post-migration; docs build cleanly.
7. HTTP paths `/upsert/{table}` and `/delete/{table}` work; old paths
   `/push-table/{table}` and `/delete-table/{table}` return **404**
   (hard break per D-09). Server integration smoke enforces.
8. `bv.App(linger_ms=5, max_batch=256)` buffers `push()` calls and
   flushes as a single `push_many` request; `app.flush()` +
   `app.close()` + context-manager exit all flush pending buffers; 10
   pushes with `linger_ms=5` produce exactly 1 server request
   (assertable via request counter on a test server).
9. `with app.pipeline() as pipe:` over TCP issues N mixed ops
   (push/get/upsert/delete) in a single TCP round-trip (strict-FIFO).
   `app.pipeline()` over HTTP raises `BV-E-PIPELINE-HTTP`.

---

## Plan breakdown (6 plans, ~20 tasks)

- **16-01** (3 tasks, ~180 LoC, wave 1) — `@bv.source` commutative
  marker (D-01) + decoration-time BV-E-SOURCE-ON-DERIVATION (D-02) +
  register-time BV-E-SOURCE-MISSING (D-03) + DAG root-source walker
  (D-08) + source-only schema-flag guard
  (D-06 / `BV-E-SOURCE-FLAG-ON-DERIVATION`). Pure Python-SDK changes to
  `_events.py`, `_tables.py`, `_validate.py`, `_source.py`,
  `__init__.py`.

- **16-02** (4 tasks, ~220 LoC, wave 2, depends on 16-01) —
  `app.upsert` / `app.delete` methods (D-04) + `Transport.send_upsert`
  / `send_delete` (HTTP + TCP) + server-side 400
  `cannot_push_to_derivation` (D-05) + **HTTP rename with hard break
  (D-09)**: route rename + server integration smoke asserts 404 on old
  paths + existing callers (`phase11_5_temporal_smoke.rs`,
  `temporal_throughput.rs`) migrated to new paths.

- **16-03** (3 tasks, ~50 LoC core + migrations, wave 3, depends on
  16-01/16-02) — migrate all ~103 `@bv.event` / `@bv.table` class-form
  occurrences across `python/tests/`, `docs/`, `beava-website/` to
  `@bv.source`; decoration-overhead microbench.

- **16-04** (3 tasks, ~150 LoC, wave 3, depends on 16-01/16-02) —
  client-side lingering (D-10). `linger_ms` + `max_batch` constructor
  kwargs; in-memory per-stream buffer; flush on timer / max_batch /
  explicit `flush()` / `close()` / context exit; single `push_many`
  per flush. Python SDK only; zero server changes.

- **16-05** (3 tasks, ~100 LoC, wave 3, depends on 16-01/16-02) —
  pipeline context manager (D-11). `with app.pipeline() as pipe:`
  queues mixed ops; flushes over framed-TCP strict-FIFO on exit; HTTP
  transport raises `BV-E-PIPELINE-HTTP`.

- **16-06** (1 task, ~80 LoC docs, wave 4, depends on 16-03/16-04/16-05)
  — Phase 16 throughput run + VERIFICATION.md + SUMMARY.md. Throughput
  comparison against prior baseline; no simple-fraud regression; all
  SC1–SC9 traced to test evidence; grey-area dispositions; carry-over
  notes for Phase 12 / 14 / 14.1.

Waves: 1 → 2 → 3 (three plans parallel) → 4.

---

## Grey areas (residual, 2026-04-24)

### GA-1 — "Decoration time" SC3 enforcement
**RESOLVED 2026-04-24.** User accepted register-time enforcement for
`BV-E-SOURCE-MISSING`. Commutative marker (D-01) lets decoration-time
checks fire for the *positive* rejections (derivation, plain
function); the *missing* case is register-time only.

### GA-2 — `app.push_table` / `app.delete_table` deprecation
**RESOLVED 2026-04-24.** No deprecation aliases. `push_table` /
`delete_table` never shipped on the Python client. `app.upsert` /
`app.delete` are the only verbs.

### GA-3 — `@bv.source` decoration order
**RESOLVED 2026-04-24.** Commutative. Either
`@bv.source @bv.event(...)` or `@bv.event(...) @bv.source` works. D-01
codifies the mechanism.

### GA-4 — Phase 12 `app.push` (event push) alignment
**STILL OPEN (for Phase 12).** Phase 12 will wire `app.push(event,
row)` for event sources. Phase 12 plan should adopt the same
`_beava_is_source` check. Not Phase 16 work; flag carried forward.

### GA-5 — Scope budget
**UPDATED 2026-04-24.** ~500 LoC. Migration sites stable at ~103.

### GA-6 — `push_many` wire availability (NEW, 2026-04-24)
Plan 04 (lingering) depends on a `push_many`-shaped server wire for the
flush path. Verify at Plan 04 start:
- If `push_many` HTTP endpoint exists (Phase 12 done) → use it.
- If not → Plan 04 falls back to a sequential flush that still provides
  the user-observable batching + linger semantics (single flush call
  boundary, deterministic flush-on-timer), with a TODO to upgrade to a
  single `push_many` request once Phase 12 lands. Document choice in
  Plan 04 SUMMARY.

---

## Cross-phase ties

- **Phase 12** — `app.push(event, row)` for event sources; adopt
  `_beava_is_source` check (GA-4). Lingering (D-10) shares the same
  buffer machinery once `app.push` lands; if Phase 12 is deferred past
  Phase 16, Plan 04 buffers `upsert` / `delete` only and extends to
  `push` when Phase 12 lands.
- **Phase 14 / 14.1** — schema-flag homes locked by D-06.
- **Phase 13 ship-gate** — Phase 16 ships before Phase 13 tag.

---

## Requirements mapped

New REQ-IDs (14 total, up from 9):

- `SDK-SOURCE-01` — `@bv.source` commutative marker decorator
- `SDK-SOURCE-02` — `BV-E-SOURCE-ON-DERIVATION` decoration-time error
- `SDK-SOURCE-03` — `BV-E-SOURCE-MISSING` register-time error
- `SDK-SOURCE-04` — `BV-E-SOURCE-FLAG-ON-DERIVATION` decoration-time error
- `SDK-SOURCE-05` — DAG root-source walker (Rule 9)
- `SDK-UPSERT-01` — `app.upsert(target, row)` verb
- `SDK-UPSERT-02` — `app.delete(target, key={...})` verb
- `SDK-UPSERT-03` — `BV-E-PUSH-TO-DERIVATION` client-side guard
- `SRV-PUSH-DERIV-01` — server-side 400 `cannot_push_to_derivation`
- `SRV-WIRE-RENAME-01` — **NEW** — server `/upsert` + `/delete` routes;
  old paths return 404 (D-09)
- `SDK-LINGER-01` — **NEW** — `linger_ms` / `max_batch` constructor
  kwargs + buffer + flush machinery (D-10)
- `SDK-LINGER-02` — **NEW** — `app.flush()` + `app.close()` +
  context-manager exit all drain pending buffers (D-10)
- `SDK-PIPELINE-01` — **NEW** — `app.pipeline()` TCP context manager
  batching mixed ops in one round-trip (D-11)
- `SDK-PIPELINE-02` — **NEW** — `BV-E-PIPELINE-HTTP` raised when
  `app.pipeline()` used on HTTP transport (D-11)
