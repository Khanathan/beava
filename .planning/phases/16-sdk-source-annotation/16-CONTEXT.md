---
phase: 16-sdk-source-annotation
type: context
created: 2026-04-24
status: locked
---

# Phase 16 CONTEXT — SDK surface v0 ergonomics

**Theme:** Stabilize the public Python-SDK surface at v0.0 before the ship-gate.
Make "this is an external push target" explicit via a required `@bv.source`
annotation, and promote `app.upsert` / `app.delete` as the named verbs for
writing to tables. Result: public API that reads the same way the user reasons
about the pipeline — sources are push targets; derivations are computed.

**Scope budget:** ~250 LoC production + tests + migrations across ~103 existing
decorator sites. Mostly Python SDK + test/docs migration; small Rust validator
additions for `cannot_push_to_derivation`.

**Not in scope (explicitly):**
- Wire format changes — none. `POST /push-table/{name}` / `POST /delete-table/{name}`
  stay byte-identical.
- Server-side registry validation rework beyond adding the
  `cannot_push_to_derivation` error shape for push-table/delete-table against
  non-source tables.
- Phase 14 `tolerate_delay_ms` runtime semantics — Phase 16 only enforces where
  that kwarg may be *declared*.
- Phase 14.1 `modifiable=True` — same: Phase 16 only enforces the declaration
  home. Phase 14.1 will reference Phase 16 D-06 when it lands.

---

## Baseline observations (grounded in repo at HEAD `77276e0`)

1. **No `@bv.source` exists in the codebase today.** Single match in
   `.planning/ROADMAP.md` + phase-14 context, both documentary.
2. **Python SDK does NOT yet expose `app.push_table` / `app.delete_table`.**
   `_app.py` only has `register`, `validate`, `ping`, `close`. Phase 12's
   push/get wiring is still pending (ROADMAP marks Phase 12 🟡 PARTIAL).
   **Consequence:** Plan 02 introduces `app.upsert` / `app.delete` as the
   *first* table-write verbs on the Python client — there is no legacy
   `push_table` / `delete_table` client method to deprecate. This simplifies
   migration considerably.
3. **Server HTTP endpoints `/push-table/{name}` + `/delete-table/{name}` exist**
   (Phase 11.5, `crates/beava-server/src/temporal_http.rs`). Plan 02 will send
   `app.upsert` / `app.delete` to those existing endpoints — no new wire.
4. **Class-form vs function-form distinction lives in
   `python/beava/_events.py` `event()` dispatch (`inspect.isclass`) and
   `python/beava/_tables.py` `table()` dispatch.** Decoration outcome is
   `EventSource` / `TableSource` for class-form, `EventDerivation` /
   `TableDerivation` for function-form. Phase 16's `@bv.source` guards this
   split.
5. **~103 decorator call-sites repo-wide** (26 in `python/tests/`, rest in
   `docs/` + `beava-website/`). Plan 03's migration task is the biggest by
   LoC.
6. **Source-only schema flags today:** `keep_events_for`, `tolerate_delay`,
   `dedupe_key`, `dedupe_window` (on `@bv.event`) and `ttl`, `mode=upsert` (on
   `@bv.table`) — these already reach `EventSource` / `TableSource` only by
   construction (they are `_decorate_event_class` / `_decorate_table_class`
   params, not accepted by the function-form paths). Phase 16 encodes the
   invariant, doesn't create it.

---

## Locked decisions

### D-01 — `@bv.source` required on class-form decorations
Class-form `@bv.event` and `@bv.table` decorations MUST be preceded by
`@bv.source` (applied as the outermost decorator, i.e. the one nearest the
class keyword — so the `@bv.source` import sits in the same namespace as
`@bv.event`). Example:

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

**Mechanism:** `@bv.event` / `@bv.table` returns a source descriptor whose
`_beava_is_source` flag defaults to `False`. `@bv.source` is a thin marker
decorator that sets `_beava_is_source = True` on the descriptor it receives.
Final source descriptors assert `_beava_is_source is True` at decoration time
(see D-03).

**Outermost-decorator rationale:** `@bv.source` must see the *decorated*
descriptor (an `EventSource` / `TableSource`), not the raw class. The Python
decoration order `@bv.source` above `@bv.event` means `@bv.event` runs first,
returning the source descriptor; then `@bv.source` flags it.

### D-02 — `@bv.source` forbidden on function-form
Applying `@bv.source` to an `EventDerivation` / `TableDerivation` raises
`TypeError` with code `BV-E-SOURCE-ON-DERIVATION` at decoration time, before
`app.register` is ever called. Message must name the derivation and point at
the function:

    @bv.source is only valid above a class-form @bv.event / @bv.table
    (external push targets); {name!r} is a derivation (function-form) and
    must NOT be annotated @bv.source.

### D-03 — Class-form without `@bv.source` → error
Calling `app.register(SomeClassFormSource)` where the source's
`_beava_is_source` is still `False` raises `TypeError` / `RegistrationError`
with code `BV-E-SOURCE-MISSING` at decoration time (SDK-level) or register
time (fallback). Message:

    class-form @bv.event / @bv.table must be annotated @bv.source above the
    @bv.event / @bv.table decorator (they're external push targets); got
    bare class-form for {name!r}.

**Preferred enforcement point: decoration time.** `@bv.event` / `@bv.table`
class-form return a descriptor with `_beava_is_source = False`; we install a
deferred check (one of: a `__set_name__`-style trick; a check in
`_to_register_json`; a check in `validate_descriptors`). Deferring to
`validate_descriptors` is acceptable if decoration-time deferral is
infeasible — SC3 requires **decoration time** so we prefer that.

**Chosen implementation approach (plan 16-01):** hook the source-descriptor
`__init_subclass__` is not usable (we return instances, not subclasses), so
instead we use a **module-level post-import guard**: `@bv.event` / `@bv.table`
class-form returns a descriptor whose `__repr__` / `.register()` check fires
if `_beava_is_source` is still False. The *decoration-time* guard is that
`@bv.source` is the last decorator to touch the object; if absent, the
descriptor is still constructed — so SC3 is actually enforced at **first
use** (register or push). We document this gap: "decoration-time" means "at
import time, the object is created but its source status is only
cross-checked at register() / push()"; we add a unit test that imports a
module with bare class-form and asserts register() raises.

→ **Grey area GA-1** (below): user wrote "SC3 raises at decoration time" —
the closest we can get in Python without sys.settrace is "at module import
time, via a per-class post-decoration hook" or "at first method-access /
register". We will implement the strictest available: register-time
`BV-E-SOURCE-MISSING`, plus a linter note in docs. Mark as grey area for user
review.

### D-04 — `app.upsert` / `app.delete` verbs
`python/beava/_app.py` gains two methods:

    def upsert(self, target, row: dict[str, Any]) -> dict[str, Any]: ...
    def delete(self, target, *, key: dict[str, Any]) -> dict[str, Any]: ...

- `target` is a `TableSource` descriptor (or, later, `EventSource` for
  `app.upsert` writing to an event? — **no**: events are pushed via a future
  `app.push`. `app.upsert` / `app.delete` are table-only.)
- Server wire: `POST /push-table/{target._name}` with `row` as JSON body for
  `upsert`; `POST /delete-table/{target._name}` with `{"key": {...}}` for
  `delete`. Matches existing Phase 11.5 server surface.
- Transport dispatch: reuse existing `parse_url_to_transport` infrastructure;
  `HttpTransport` already wraps the client. Requires adding
  `send_push_table(name, row_bytes)` + `send_delete_table(name, key_bytes)`
  methods to the `Transport` trait — NO new wire format.
- Python client does NOT currently have `push_table` / `delete_table`
  methods, so there are no deprecation aliases to create. **Simplification
  over the brief:** Plan 02 adds only the new verbs; no DeprecationWarning
  aliases.

→ **Grey area GA-2** (below): user brief said "keep as deprecated aliases
that emit DeprecationWarning; remove in v1." This makes sense if `push_table`
had shipped — it has not. We will NOT add `push_table` / `delete_table`
methods. If v0.1 users are reading docs for the server HTTP surface
(`POST /push-table`), that's a different layer. Flag for user confirmation.

### D-05 — Upsert against derivation → 400 `cannot_push_to_derivation`
Two layers of enforcement:

1. **Python SDK (preferred, fails fast without network I/O):**
   `app.upsert(target, ...)` checks `target._beava_kind == "table"` AND
   `target._beava_is_source is True`. If not, raises `ValueError` /
   `TypeError` with `BV-E-PUSH-TO-DERIVATION`.
2. **Server (authoritative):** `crates/beava-server/src/temporal_http.rs`
   `push_table_handler` / `delete_table_handler` already look up the table
   descriptor in the registry; extend the lookup to reject when the resolved
   descriptor is a derivation (kind-check), returning 400 with
   `{"error":{"code":"cannot_push_to_derivation","path":<name>,"reason":"..."}}`.
   This catches out-of-band callers (curl, other SDKs).

**Error body shape** matches existing registry-error conventions. Error-code
string `cannot_push_to_derivation` is NEW — added to `register_validate.rs`
or `temporal_http.rs` error enum (plan 16-02 picks the right home; current
read is `temporal_http.rs` since that's where push-table lives, and
register-time is already past when push happens).

### D-06 — Schema flags live only on `@bv.source @bv.event(...)` / `@bv.source @bv.table(...)`
`tolerate_delay_ms` (Phase 14), future `modifiable=True` /
`modification_log_depth` (Phase 14.1), `keep_events_for`, `dedupe_key`,
`dedupe_window`, `ttl` — all source-only. Passing these to function-form
`@bv.event(tolerate_delay="5s")` — already impossible today because
`_decorate_event_function` ignores them; Plan 01 makes this an **explicit
TypeError** instead of silent-ignore, so users see the error.

Derivations inherit runtime behavior from their root source via the DAG
walker (D-08). Cross-phase binding: Phase 14.1's plan MUST reference this D-06
when adding `modifiable=True`.

### D-07 — Warning codes
For v0 we ship `BV-E-SOURCE-MISSING` as an **ERROR** (not a warning) per the
brief's "For v0: shipped as ERROR directly." No
`BV-W-SOURCE-NOT-ANNOTATED` graduation path — users migrate once, cleanly, to
v0.

Error codes registered this phase:
- `BV-E-SOURCE-ON-DERIVATION` (TypeError, decoration time)
- `BV-E-SOURCE-MISSING` (RegistrationError, register time)
- `BV-E-PUSH-TO-DERIVATION` (ValueError client-side; 400
  `cannot_push_to_derivation` server-side)
- `BV-E-SOURCE-FLAG-ON-DERIVATION` (TypeError, decoration time) — e.g.
  `@bv.event(tolerate_delay="5s")` applied to function-form

### D-08 — DAG root-source tracing in `_validate.py`
`validate_descriptors` gains a new rule:

    Rule 9 (root-source): every descriptor in the batch must trace back
    through its `_upstreams` chain to a node whose `_beava_is_source is
    True`. Missing root sources → ValidationError
    `kind="no_root_source"`; walker treats unknown upstreams as already-
    registered sources (consistent with existing `missing_upstream` rule).

**Trace algorithm:** For each descriptor D, walk `D._upstreams` recursively
(resolving names against the current batch + already-registered descriptors).
If every reachable root (zero-upstream node) has `_beava_is_source is True`,
pass. Else emit error naming the first bad root.

Reuses existing `_detect_cycle_dfs` machinery; additive rule, not a rewrite.

---

## Success criteria (SC1–SC6 — from brief)

1. `@bv.source @bv.event(...)` / `@bv.source @bv.table(...)` class-form
   registration succeeds; descriptor carries `_beava_is_source = True`.
2. `@bv.source` applied to function-form raises `TypeError`
   (`BV-E-SOURCE-ON-DERIVATION`) at **decoration time** — before
   `app.register`.
3. Class-form without `@bv.source` raises `RegistrationError`
   (`BV-E-SOURCE-MISSING`) — at the latest possible moment **before any
   network I/O** (so: `app.register()` pre-send local-validate). Grey area
   GA-1: "decoration time" weakened to "register time" — flag for user.
4. `app.upsert(SourceTable, {...})` writes to server `POST
   /push-table/{name}`; `app.delete(SourceTable, key={...})` writes to
   `POST /delete-table/{name}`; both succeed on source tables.
5. `app.upsert(DerivedTable, {...})` raises `ValueError`
   (`BV-E-PUSH-TO-DERIVATION`) client-side before any network I/O; if the
   SDK guard is somehow bypassed (curl, other SDKs), server returns 400
   `cannot_push_to_derivation`.
6. All existing Python SDK tests + docs + website samples migrated to the
   new surface; `pytest` green post-migration; docs build cleanly.

---

## Plan breakdown (3 plans, 9 tasks)

- **16-01** (3 tasks, ~100 LoC, wave 1) — `@bv.source` marker + descriptor
  flag + decoration-time enforcement (D-02) + register-time
  `BV-E-SOURCE-MISSING` (D-03) + DAG root-source walker (D-08) + source-only
  schema-flag guard (D-06 / `BV-E-SOURCE-FLAG-ON-DERIVATION`). Pure
  Python-SDK changes to `_events.py`, `_tables.py`, `_validate.py`, plus
  new public symbol `bv.source` in `__init__.py`.

- **16-02** (3 tasks, ~100 LoC, wave 2, depends on 16-01) —
  `app.upsert(target, row)` + `app.delete(target, key=...)` methods in
  `_app.py`; `Transport.send_push_table` + `send_delete_table` in
  `_transport.py` (HTTP + TCP); server-side `cannot_push_to_derivation`
  error in `temporal_http.rs` for push/delete-table against non-source
  tables.

- **16-03** (3 tasks, ~50 LoC core + migrations, wave 3, depends on
  16-01/16-02) — migrate all ~103 `@bv.event` / `@bv.table` class-form
  occurrences across `python/tests/`, `docs/`, `beava-website/` to prepend
  `@bv.source`; criterion microbench for decoration-time overhead
  (Phase 6+ regression gate requirement); phase throughput-run task
  (Phase 8+ convention); VERIFICATION.md + SUMMARY.md.

Total: 9 tasks. Each task red→green per CLAUDE.md TDD discipline (Phase 3+
rule).

---

## Grey areas (flagged for user review, do NOT act silently)

### GA-1 — "Decoration time" SC3 enforcement
Brief says: "SC3: Class-form without `@bv.source` raises `TypeError` at
**decoration time**." In Python, `@bv.event` returns the descriptor
*instance*; by the time the next statement runs, the decorator chain is
complete. There is no language-level "decoration-time" check that can know
`@bv.source` was absent from outside, because the source had no guarantee
`@bv.source` would appear above it. Options:

- **(a)** Require `@bv.source` to be applied *below* `@bv.event` (innermost);
  then `@bv.event` can inspect the return value from the inner decorator and
  complain if it is not a source. Changes the ordering semantic.
- **(b)** Emit an error at `app.register()` local-validate time (what Plan
  01 implements). Error surface stays at the user's first `register()` call
  — still pre-network-I/O, still fast. This is what we plan to ship.
- **(c)** Add a helper `bv.finalize_sources()` that users call at the end of
  module import. Too invasive.

**Recommended:** (b). Flag to user: is "register-time" acceptable for SC3,
or do you want (a) with the decorator-order swap (which would make
`@bv.event` the outer and `@bv.source` the inner)?

### GA-2 — `app.push_table` / `app.delete_table` deprecation
Brief says: "Old verbs: keep as deprecated aliases that emit
DeprecationWarning; remove in v1." These verbs **do not exist on the Python
client today** — `_app.py` has no `push_table` method. Adding them solely
to deprecate them is dead weight.

**Recommendation:** Skip the aliases entirely. Ship `app.upsert` /
`app.delete` as the first and only client-side table-write verbs. Update
the brief's "remove in v1" line to "ship clean in v0."

If user actually means the SERVER HTTP endpoint names
(`POST /push-table/{name}` → `POST /upsert/{name}`?) — that is a wire
change, out of Phase 16 scope per the "no wire changes" constraint.

### GA-3 — `@bv.source` on function-form: decoration order
If we accept GA-1 option (b), then `@bv.source` can legitimately only be
applied to a descriptor (i.e. sits above `@bv.event`). In that case
`@bv.source` receives a descriptor, checks its kind, and sets the flag. If
the descriptor is a derivation (`_beava_kind == "derivation"`), raise
`BV-E-SOURCE-ON-DERIVATION`. This is genuinely decoration-time. D-02 stays
as-specified; just the mechanism is "outer decorator inspects the object
returned by inner decorator."

### GA-4 — Phase 12 `app.push` (event push) alignment
Phase 12 will wire `app.push(event, row)` for event sources. Should it also
reject pushes to event-derivations? Same rule (push only to sources). We
will draft Phase 12's plan to include the same `_beava_is_source` check.
Not Phase 16 work — noted for Phase 12 replanning.

### GA-5 — Scope budget
Brief: ~250 LoC. My estimate post-read:
- Plan 01: ~100 LoC production + ~80 LoC tests
- Plan 02: ~120 LoC Python + ~40 LoC Rust + ~60 LoC tests
- Plan 03: ~50 LoC bench + ~10 LoC per migrated test × 26 sites = ~260 LoC
  mechanical diff (mostly prepending `@bv.source` and an import)

Total ~720 LoC inc. migration; ~300 LoC excluding mechanical migration.
Within rough budget. Migration is grep-driven — Plan 03 task 1 is
genuinely one-line-per-site.

---

## Cross-phase ties

- **Phase 14** — `tolerate_delay_ms` runtime semantics. Phase 16 D-06
  locks the declaration home. Phase 14's PLAN (when it writes) should
  assume `tolerate_delay_ms` is always reachable via a source-annotated
  descriptor.
- **Phase 14.1** — `modifiable=True` / `modification_log_depth` are
  source-only by D-06; Phase 14.1's plan must reference this.
- **Phase 12** (remaining plans on worktree `phase-12-followup`) — should
  adopt `_beava_is_source` check for `app.push(event, row)` in parallel
  (GA-4). Flag for Phase 12 replanning.
- **Ship-gate Phase 13** — public API surface must be stable before tag.
  Phase 16 ships before the Phase 13 ship-gate per the ROADMAP note.

---

## Migration table — sites to update in Plan 03

Run `grep -rnE '^@bv\.(event|table)' python/ docs/ beava-website/` at plan
start for live count. Baseline at HEAD `77276e0`:

| Area | Sites | Notes |
|------|-------|-------|
| `python/tests/` | 26 class-form | all need `@bv.source` prepended |
| `docs/` | ~7 blog/doc examples | prepend + re-verify copy |
| `beava-website/project/` | ~70 HTML samples | bulk find/replace; JS samples |
| SDK internal (`python/beava/`) | 0 | SDK uses internal types, no decorations |

Plan 03 task 1 = mechanical migration; task 2 = bench + throughput run;
task 3 = VERIFICATION + SUMMARY.

---

## Requirements mapped

New REQ-IDs to add to `REQUIREMENTS.md` at plan start (Plan 01 Task 0):

- `SDK-SOURCE-01` — `@bv.source` decorator + descriptor flag
- `SDK-SOURCE-02` — `BV-E-SOURCE-ON-DERIVATION` decoration-time error
- `SDK-SOURCE-03` — `BV-E-SOURCE-MISSING` register-time error
- `SDK-SOURCE-04` — `BV-E-SOURCE-FLAG-ON-DERIVATION` decoration-time error
- `SDK-SOURCE-05` — DAG root-source walker (Rule 9)
- `SDK-UPSERT-01` — `app.upsert(target, row)` verb
- `SDK-UPSERT-02` — `app.delete(target, key={...})` verb
- `SDK-UPSERT-03` — `BV-E-PUSH-TO-DERIVATION` client-side guard
- `SRV-PUSH-DERIV-01` — server-side 400 `cannot_push_to_derivation`

9 REQ-IDs.
