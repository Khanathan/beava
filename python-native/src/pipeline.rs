//! `tally.Pipeline` PyO3 class — Plan 30-01.
//!
//! Thin Python-facing wrapper around `tally::client::clone::run_clone`. The
//! constructor validates arguments without connecting; `.run()` drives the
//! async snapshot-fetch on a private tokio runtime with the GIL released
//! (`Python::allow_threads`, per D-A1 GIL behaviour); `.get()` and `.inspect()`
//! read the in-memory `FrozenClient` populated by `.run()`.
//!
//! Token resolution (matches the CLI convention from Phase 28): the `token`
//! kwarg takes precedence; when `None`, the `TALLY_TOKEN` env var is read at
//! `.run()` time.

// pyo3 0.22's `#[pymethods]` macro expansion produces some no-op conversions
// that clippy flags as `useless_conversion`. Suppressed file-wide; upstream
// fix lands with the pyo3 0.23 migration.
#![allow(clippy::useless_conversion)]

use std::collections::HashMap;

use pyo3::exceptions::{PyNotImplementedError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use tally::client::clone::{run_clone, CloneArgs};
use tally::client::wire::Scope as WireScope;
use tally::client::{FrozenClient, SessionMode};

use crate::errors::{map_clone_error, OutOfScopeError};

/// Mode validated at construction time. The Rust `SessionMode` enum only
/// carries `Historical` today (Phase 28); we keep a separate Python-level
/// tri-state so we can reject `mode="streaming"` with a specific
/// `NotImplementedError` pointing at Phase 31.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PyMode {
    Historical,
    Streaming,
}

#[derive(Debug, Clone)]
struct PipelineConfig {
    remote: String,
    streams: Vec<String>,
    keys: Option<Vec<String>>,
    key_prefix: Option<String>,
    mode: PyMode,
    token: Option<String>,
    #[allow(dead_code)] // forwarded to run_clone in Phase 31 streaming mode
    since: Option<u64>,
}

#[pyclass(module = "tally._native")]
pub struct Pipeline {
    config: PipelineConfig,
    frozen: Option<FrozenClient>,
}

#[pymethods]
impl Pipeline {
    #[new]
    #[pyo3(signature = (*, remote, streams, keys=None, key_prefix=None, mode="historical", token=None, since=None))]
    fn new(
        remote: String,
        streams: Vec<String>,
        keys: Option<Vec<String>>,
        key_prefix: Option<String>,
        mode: &str,
        token: Option<String>,
        since: Option<u64>,
    ) -> PyResult<Self> {
        if streams.is_empty() {
            return Err(PyValueError::new_err("streams must be non-empty"));
        }
        if keys.is_some() && key_prefix.is_some() {
            return Err(PyValueError::new_err(
                "keys and key_prefix are mutually exclusive",
            ));
        }
        let mode = match mode {
            "historical" => PyMode::Historical,
            "streaming" => PyMode::Streaming,
            other => {
                return Err(PyValueError::new_err(format!(
                    "mode must be 'historical' or 'streaming', got {other:?}"
                )))
            }
        };
        Ok(Self {
            config: PipelineConfig {
                remote,
                streams,
                keys,
                key_prefix,
                mode,
                token,
                since,
            },
            frozen: None,
        })
    }

    /// Test-only helper: surface the token that `.run()` would use, with the
    /// same precedence (explicit arg > `TALLY_TOKEN` env var > None). Does not
    /// connect or mutate state. Kept on the public Python surface to let
    /// `test_pipeline_unit.py` verify env-var fallback without standing up a
    /// fake server.
    #[pyo3(name = "_debug_effective_token")]
    fn debug_effective_token(&self) -> Option<String> {
        self.config
            .token
            .clone()
            .or_else(|| std::env::var("TALLY_TOKEN").ok())
    }

    /// Blocking bootstrap: run the historical snapshot-fetch round-trip, then
    /// stash the resulting `FrozenClient` so `.get()` / `.inspect()` can query
    /// it. Releases the GIL for the duration of the blocking I/O so Ctrl-C +
    /// signal handlers stay responsive (T-30-03).
    fn run(&mut self, py: Python<'_>) -> PyResult<()> {
        if matches!(self.config.mode, PyMode::Streaming) {
            return Err(PyNotImplementedError::new_err(
                "streaming mode ships in Phase 31",
            ));
        }

        // Token resolution: explicit arg > TALLY_TOKEN env var > None.
        let effective_token = self
            .config
            .token
            .clone()
            .or_else(|| std::env::var("TALLY_TOKEN").ok());

        let scope = WireScope {
            streams: self.config.streams.clone(),
            keys: self.config.keys.clone(),
            key_prefix: self.config.key_prefix.clone(),
            // Phase 28 only supports pull="all"; the Plan 30-01 surface doesn't
            // expose the pull knob yet (deferred until Phase 31).
            pull: "all".into(),
        };

        let args = CloneArgs {
            remote: self.config.remote.clone(),
            scope,
            token: effective_token,
            mode: SessionMode::Historical,
            max_attempts: 5,
        };

        // Drop the GIL around the blocking async drive so the Python
        // interpreter can still service signals.
        let result: Result<FrozenClient, tally::client::clone::CloneError> =
            py.allow_threads(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build current-thread tokio runtime");
                rt.block_on(run_clone(&args))
            });

        let frozen = result.map_err(map_clone_error)?;
        self.frozen = Some(frozen);
        Ok(())
    }

    /// Scope-aware lookup. Signature order is `(key, stream)` on the Python
    /// side to match the plan's Pipeline API; internally this forwards to
    /// `FrozenClient::get(stream, key)` which is the Rust-side convention.
    fn get(&self, py: Python<'_>, key: &str, stream: &str) -> PyResult<Option<PyObject>> {
        let frozen = self
            .frozen
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err(".run() must be called before .get()"))?;
        match frozen.get(stream, key) {
            Ok(None) => Ok(None),
            Ok(Some(state)) => {
                let json = serde_json::to_value(&state).map_err(|e| {
                    crate::errors::ReplicaStateError::new_err(format!(
                        "failed to serialise entity state: {e}"
                    ))
                })?;
                let py_obj = pythonize::pythonize(py, &json).map_err(|e| {
                    crate::errors::ReplicaStateError::new_err(format!(
                        "failed to convert entity state to Python: {e}"
                    ))
                })?;
                Ok(Some(py_obj.into()))
            }
            Err(err) => Err(OutOfScopeError::new_err(err.to_string())),
        }
    }

    /// Return `{stream_name: key_count}` for every stream in the bulk-loaded
    /// store. A key that appears in N streams contributes to each of those
    /// streams' counts (matches the `FrozenClient::iter_entities` convention).
    fn inspect(&self) -> PyResult<HashMap<String, usize>> {
        let frozen = self
            .frozen
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err(".run() must be called before .inspect()"))?;
        let mut out: HashMap<String, usize> = HashMap::new();
        for (stream, _key, _state) in frozen.iter_entities() {
            *out.entry(stream).or_insert(0) += 1;
        }
        // Streams in scope but with no keys should still show up as 0.
        for s in frozen.scope().streams.iter() {
            out.entry(s.clone()).or_insert(0);
        }
        Ok(out)
    }
}
