# Phase 38: Mothball v0 embedded-client surfaces - Context

**Gathered:** 2026-04-15
**Status:** Ready for planning
**Mode:** Housekeeping — run AFTER Phases 35/36/37 ship green

<domain>
## Phase Boundary

Remove (or clearly deprecate) the v0 embedded-client code that Option M obsoletes. Frees the codebase from dead paths and reduces cognitive load for anyone reading the client code.

**In scope:**
- Delete or `#[deprecated]` the embedded-client modules.
- Delete the `tally_cli clone / query / inspect / sync` subcommands.
- Delete obsoleted tests.
- Either delete the `python-native` PyO3 `Pipeline` class entirely OR keep it as a thin wrapper that just spawns `tally fork` and returns a client handle.
- Update ROADMAP + PROJECT docs to reflect Option M as the canonical architecture.

**Out of scope:**
- Any new functionality.
- Touching Phase 27 (OP_SNAPSHOT_FETCH / OP_SUBSCRIBE) — those stay (SUBSCRIBE is reused by Phase 36).
- Deleting the Phase 6 event log or any server-side infrastructure.

</domain>

<decisions>
## Implementation Decisions (LOCKED)

### What gets deleted
- `src/client/clone.rs` — embedded historical clone. Obsoleted by Phase 36.
- `src/client/streaming.rs` — embedded streaming client. Obsoleted by Phase 36.
- `src/client/state.rs` — StreamingStore wrapper. Dead after above.
- `src/client/session.rs` — keep IF its helpers are reused by Phase 36's replica-client code, delete if not. Plan checks and decides.
- `src/client/wire.rs` — keep. The replica client uses these wire types.
- `src/client/mod.rs` — prune module exports; leave only what's actively reused.
- `src/bin/tally_cli.rs` subcommands: `clone`, `query`, `inspect`, `sync` — all deleted. Leave the binary stub if Phase 37 added `fork` there; otherwise delete the whole binary.
- Test files obsolete after above: `tests/test_client_streaming.rs`, `tests/integration/test_tally_clone.py`, `tests/integration/test_pipeline_e2e.py`. Delete.
- `python-native/src/pipeline.rs` + `python-native/src/errors.rs` + `python-native/python_src/tally/_native.pyi` — delete the Pipeline class AND the exception hierarchy (they stop making sense once the embedded model is gone).

### What stays
- Phase 27 server opcodes — reused.
- `src/client/wire.rs` — reused by replica client.
- `src/client/session.rs` — if the replica client reuses its helpers.
- Phase 35 + 36 + 37 output — the new foundation.
- `python-native/` crate — keep, but strip to just a scaffold (or delete entirely if scientists are happy using `tl.Client` over HTTP without any native extension). **Decision: delete `python-native/` entirely for MVP.** Scientists use the existing pure-Python SDK against the forked replica. Less code, less to maintain.

### What gets `#[deprecated]` but not deleted
- Nothing. We're shipping small — either keep it (alive and used) or delete it (no second-class zombies).

### Doc updates
- `ROADMAP.md` — mark phases 28 (non-01 plans), 30, 31 as SUPERSEDED by Option M. Keep the SUMMARY.md files as historical record; add a banner at the top pointing to Phase 36.
- Phase CONTEXT files for obsoleted phases: prepend a `**SUPERSEDED 2026-04-15 by Option M (Phase 36).**` banner.

### Plan split
- One plan (38-01), single wave.
- Mechanical work. No design decisions.

</decisions>

<code_context>
- After Phase 35/36/37 land, re-check which modules in `src/client/` are still imported anywhere. If grep returns only test files and the modules themselves, they're safe to delete.
- `Cargo.toml` workspace: drop `python-native` member.
- CI: drop `python-native` job if we delete the crate.

</code_context>

<specifics>
- Verify grep-clean of deleted symbols before committing. No dangling imports.
- Run full test suite after deletions; expect test counts to go DOWN (that's fine and healthy).
- Commit message spells out what was removed and why ("superseded by Option M" / "Phase 36 replica-mode subsumes this").

</specifics>

<deferred>
- None. This phase is cleanup.

</deferred>

---

*Phase: 38-mothball-v0-client*
*Source: user directive 2026-04-15*
