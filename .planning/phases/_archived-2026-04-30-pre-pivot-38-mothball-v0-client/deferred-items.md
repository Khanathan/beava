# Deferred Items — Phase 38

## Clippy drift under `-D warnings` (pre-existing, out of scope)

`cargo clippy --all-targets -- -D warnings` reports ~46 lints (all pre-existing,
all in files NOT touched by 38-01). Files affected:

- `src/engine/cms.rs`
- `src/engine/event_time.rs`
- `src/engine/operators.rs`
- `src/engine/pipeline.rs`
- `src/engine/recommend.rs`
- `src/engine/register.rs`
- `src/engine/retracting_ring.rs`
- `src/server/tcp.rs`

CI sets `RUSTFLAGS="-D warnings"`; on a new clippy toolchain the `-D warnings`
gate will fail even though the deleted files in 38-01 are irrelevant. Likely
clippy version drift since the last green CI run.

**Decision:** out of scope for 38-01 (housekeeping / delete-only). Track as
tech-debt ticket; either pin the clippy channel or do a sweep. `cargo test`
and both feature-flavor `cargo build`s remain clean.
