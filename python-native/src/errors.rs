//! Typed Python exception hierarchy for the tally replica client.
//!
//! Hierarchy (all subclass `Exception` via `TallyError`):
//!
//! ```text
//! tally.TallyError                    (base)
//! ├── tally.OutOfScopeError           (scope boundary violation at .get())
//! ├── tally.ClientConnectError        (TCP connect / snapshot fetch exhausted)
//! ├── tally.HandshakeError            (auth / scope rejected by server)
//! └── tally.ReplicaStateError         (protocol / decode invariant violation)
//! ```
//!
//! The mapping from Rust's `tally::client::clone::CloneError` is performed in
//! `map_clone_error`; scope-violation errors from `FrozenClient::get` are
//! mapped in `pipeline.rs` via `OutOfScopeError::new_err`.

use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

create_exception!(
    _native,
    TallyError,
    PyException,
    "Base class for all tally replica-client errors."
);
create_exception!(
    _native,
    OutOfScopeError,
    TallyError,
    "Query against a stream/key outside the declared scope."
);
create_exception!(
    _native,
    ClientConnectError,
    TallyError,
    "TCP connect / snapshot fetch retries exhausted."
);
create_exception!(
    _native,
    HandshakeError,
    TallyError,
    "Authentication or scope handshake rejected by server."
);
create_exception!(
    _native,
    ReplicaStateError,
    TallyError,
    "Protocol, decode, or invariant violation in replica state."
);

/// Map a `tally::client::clone::CloneError` to the matching Python exception.
///
/// Security note (T-30-02): we deliberately never include the auth token in
/// the exception message; only the remote host + failure stage + wire-level
/// diagnostic. `CloneError`'s Display impls follow that contract (they format
/// only the connect/handshake/protocol messages, never the token).
pub fn map_clone_error(err: tally::client::clone::CloneError) -> PyErr {
    use tally::client::clone::CloneError;
    let msg = err.to_string();
    match err {
        CloneError::AuthFailed { .. } => HandshakeError::new_err(msg),
        CloneError::FetchFailed { .. } => ClientConnectError::new_err(msg),
        CloneError::Protocol(_) | CloneError::Decode(_) => ReplicaStateError::new_err(msg),
        CloneError::Io(_) => ClientConnectError::new_err(msg),
        CloneError::StreamingNotSupported => {
            // Streaming is gated at the Python layer before we ever call run_clone,
            // but if the Rust guard fires first we surface it as ReplicaStateError
            // (it is effectively an invariant violation — we should not have reached
            // run_clone with streaming mode).
            ReplicaStateError::new_err(msg)
        }
    }
}
