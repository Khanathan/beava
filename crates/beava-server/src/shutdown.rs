//! Graceful shutdown signal listener.
//!
//! Returns a future that completes when SIGTERM or SIGINT is received. Passed to
//! `axum::serve(...).with_graceful_shutdown(...)` so in-flight requests drain before
//! the listener closes.

use tokio::signal::unix::{signal, SignalKind};

/// Future that completes on the first SIGTERM or SIGINT received by the process.
pub async fn shutdown_signal() {
    let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("install SIGINT handler");

    tokio::select! {
        _ = sigterm.recv() => {
            tracing::info!(target: "beava.shutdown", signal = "SIGTERM", "shutdown initiated");
        }
        _ = sigint.recv() => {
            tracing::info!(target: "beava.shutdown", signal = "SIGINT", "shutdown initiated");
        }
    }
}
