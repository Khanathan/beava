//! Graceful shutdown signal listener.
//!
//! Returns a future that completes when SIGTERM or SIGINT is received. Passed
//! to `ServerV18::serve_with_dirs(...)` and the tokio admin-sidecar router
//! (the latter binds via `BoundAdminServer::bind` internally on the tokio
//! admin port). The mio data plane reads the same future to gate its accept
//! loop.

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
