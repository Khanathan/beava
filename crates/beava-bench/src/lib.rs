//! beava-bench library surface (Phase 19+).
//!
//! Currently exports `blast_shape` for use by the binary harnesses in
//! `src/bin/` and the integration tests under `tests/`.
//!
//! See `.planning/phases/19-1m-bench/19-CONTEXT.md` for the rationale behind
//! the four blast shapes and the Pool=N pre-encoded-frame design.
pub mod blast_shape;
