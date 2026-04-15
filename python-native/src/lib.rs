//! Tally native Python extension (`tally._native`).
//!
//! Plan 30-01: exposes `Pipeline` + a typed exception hierarchy to Python. The
//! top-level `tally/__init__.py` re-exports these so users write
//! `from tally import Pipeline, TallyError, OutOfScopeError, ...`.

// pyo3 0.22's `create_exception!` emits `#[cfg(feature = "gil-refs")]` checks
// that trip rustc's check-cfg lint under our workspace-wide `-D warnings`.
// Silence it locally — upstream fix lands in pyo3 0.23; stays here until we
// bump.
#![allow(unexpected_cfgs)]

use pyo3::prelude::*;

mod errors;
mod pipeline;

pub use errors::{
    ClientConnectError, HandshakeError, OutOfScopeError, ReplicaStateError, TallyError,
};
pub use pipeline::Pipeline;

/// Module entrypoint. `#[pymodule]` name MUST match `[lib] name` in Cargo.toml
/// (here: `_native`), and the maturin `module-name` field in pyproject.toml
/// (here: `tally._native`).
#[pymodule]
fn _native(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Pipeline>()?;

    m.add("TallyError", py.get_type_bound::<TallyError>())?;
    m.add("OutOfScopeError", py.get_type_bound::<OutOfScopeError>())?;
    m.add(
        "ClientConnectError",
        py.get_type_bound::<ClientConnectError>(),
    )?;
    m.add("HandshakeError", py.get_type_bound::<HandshakeError>())?;
    m.add(
        "ReplicaStateError",
        py.get_type_bound::<ReplicaStateError>(),
    )?;

    Ok(())
}
