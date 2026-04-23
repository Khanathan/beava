//! Structured JSON logging.
//!
//! Phase 1 scope:
//! - Install a process-global JSON subscriber via `tracing_subscriber`
//! - Level driven by `Config::log_level` (parameter to `init`)
//! - `RUST_LOG` env var (if set) overrides the programmatic level — standard
//!   tracing convention; lets operators debug a running binary without a config change
//!
//! Deferred to Phase 9:
//! - `trace_id` propagation from `X-Trace-Id` header (OBS-04)
//! - Metrics-like counters on log levels
//!
//! The subscriber is installed exactly once per process. Repeat calls to `init`
//! return Ok without re-installing (needed so integration tests that spawn multiple
//! `TestServer` instances in one test process don't double-init).

use once_cell::sync::OnceCell;
use thiserror::Error;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

static INIT: OnceCell<()> = OnceCell::new();

#[derive(Debug, Error)]
pub enum InitError {
    #[error("invalid log level `{0}` (expected trace|debug|info|warn|error)")]
    InvalidLevel(String),
    #[error("subscriber already initialized by a different logger: {0}")]
    AlreadyInitialized(String),
}

/// Install the JSON tracing subscriber globally. Safe to call multiple times — the
/// second call is a no-op.
///
/// Level precedence:
/// 1. `RUST_LOG` env var, if set and parseable
/// 2. The `level` argument passed to this function
pub fn init(level: &str) -> Result<(), InitError> {
    // Validate the caller's level string up front so misconfigurations are loud.
    validate_level(level)?;

    // Return Ok(()) if already initialized — idempotent.
    if INIT.get().is_some() {
        return Ok(());
    }

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(level.to_ascii_lowercase()));

    let fmt_layer = fmt::layer()
        .json()
        .with_current_span(true)
        .with_span_list(false)
        .with_target(true)
        .with_line_number(false)
        .flatten_event(true);

    let result = tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .try_init();

    match result {
        Ok(()) => {
            let _ = INIT.set(());
            Ok(())
        }
        Err(e) => Err(InitError::AlreadyInitialized(e.to_string())),
    }
}

fn validate_level(level: &str) -> Result<(), InitError> {
    match level.to_ascii_lowercase().as_str() {
        "trace" | "debug" | "info" | "warn" | "error" => Ok(()),
        _ => Err(InitError::InvalidLevel(level.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_level_rejected() {
        let err = super::validate_level("nope").unwrap_err();
        assert!(matches!(err, InitError::InvalidLevel(_)));
    }

    #[test]
    fn all_valid_levels_accepted() {
        for lv in ["trace", "debug", "INFO", "Warn", "error"] {
            super::validate_level(lv).unwrap_or_else(|_| panic!("level {lv} should be valid"));
        }
    }

    #[test]
    fn init_is_idempotent() {
        // First call may succeed or be a no-op depending on test ordering; second
        // must succeed regardless.
        let _ = super::init("info");
        super::init("info").expect("second init must be idempotent-Ok");
    }
}
