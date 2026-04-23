//! Server: bind + serve + graceful-shutdown wiring.

use crate::http::{router, ReadinessFlag};
use crate::Config;
use beava_core::registry::Registry;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::net::TcpListener;

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("failed to bind {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid listen address `{0}`: {1}")]
    InvalidAddr(String, String),
    #[error("server error: {0}")]
    Serve(#[source] std::io::Error),
}

/// Bound server ready to serve. `local_addr` is the actual bound address, useful when
/// config specified port 0 and the OS chose.
pub struct Server {
    listener: TcpListener,
    local_addr: SocketAddr,
    readiness: ReadinessFlag,
    registry: Arc<Registry>,
}

impl std::fmt::Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server")
            .field("local_addr", &self.local_addr)
            .finish_non_exhaustive()
    }
}

impl Server {
    /// Resolve config's listen_addr and bind. Also arms the 100ms readiness delay
    /// that flips `/ready` from 503 to 200 — stand-in for Phase 5's real
    /// recovery-complete signal.
    pub async fn bind(cfg: &Config) -> Result<Self, ServerError> {
        let addr: SocketAddr = cfg
            .listen_addr
            .parse()
            .map_err(|e: std::net::AddrParseError| {
                ServerError::InvalidAddr(cfg.listen_addr.clone(), e.to_string())
            })?;
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| ServerError::Bind { addr, source: e })?;
        let local_addr = listener.local_addr().map_err(ServerError::Serve)?;

        tracing::info!(
            target: "beava.server",
            addr = %local_addr,
            "HTTP server bound"
        );

        let readiness = ReadinessFlag::new();
        let registry = Arc::new(Registry::new());

        // Phase 1 placeholder: flip readiness after 100ms. Phase 5 replaces this
        // with "after snapshot loaded + WAL replayed" (RECOV-02).
        let flag_clone = readiness.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            flag_clone.set_ready();
            tracing::info!(target: "beava.server", "readiness flag set (Phase 1 stub)");
        });

        Ok(Self {
            listener,
            local_addr,
            readiness,
            registry,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Run the server until `shutdown` completes. Emits JSON log events on shutdown
    /// initiation (from the signal handler) and shutdown complete (here). Returns
    /// after in-flight requests drain.
    pub async fn serve<F>(self, shutdown: F) -> Result<(), ServerError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let app = router(self.readiness, self.registry);
        let start = Instant::now();
        axum::serve(self.listener, app)
            .with_graceful_shutdown(shutdown)
            .await
            .map_err(ServerError::Serve)?;

        let elapsed_ms = start.elapsed().as_millis() as u64;
        tracing::info!(
            target: "beava.shutdown",
            duration_ms = elapsed_ms,
            "shutdown complete"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Config;

    fn tmp_cfg() -> Config {
        Config {
            listen_addr: "127.0.0.1:0".to_string(), // OS-allocated port
            log_level: "info".to_string(),
        }
    }

    #[tokio::test]
    async fn bind_reports_actual_local_addr() {
        let cfg = tmp_cfg();
        let s = Server::bind(&cfg).await.expect("bind");
        let addr = s.local_addr();
        assert_eq!(addr.ip().to_string(), "127.0.0.1");
        assert_ne!(addr.port(), 0, "OS should have allocated a real port");
    }

    #[tokio::test]
    async fn invalid_addr_returns_structured_error() {
        let cfg = Config {
            listen_addr: "not-an-addr".to_string(),
            log_level: "info".to_string(),
        };
        let err = Server::bind(&cfg).await.unwrap_err();
        assert!(matches!(err, ServerError::InvalidAddr(_, _)));
    }

    #[tokio::test]
    async fn serve_then_shutdown_exits_within_500ms() {
        let cfg = tmp_cfg();
        let s = Server::bind(&cfg).await.expect("bind");
        let addr = s.local_addr();

        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let shutdown = async move {
            let _ = rx.await;
        };

        let join = tokio::spawn(async move { s.serve(shutdown).await });

        // Confirm /health is reachable.
        let url = format!("http://{}/health", addr);
        let resp = reqwest::get(&url).await.expect("health request");
        assert_eq!(resp.status().as_u16(), 200);

        // Fire shutdown.
        let start = std::time::Instant::now();
        let _ = tx.send(());

        tokio::time::timeout(Duration::from_millis(500), join)
            .await
            .expect("server should exit within 500ms")
            .expect("join")
            .expect("serve ok");
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(500),
            "graceful shutdown took {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn readiness_flips_after_100ms() {
        let cfg = tmp_cfg();
        let s = Server::bind(&cfg).await.expect("bind");
        let addr = s.local_addr();

        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let shutdown = async move {
            let _ = rx.await;
        };
        let join = tokio::spawn(async move { s.serve(shutdown).await });

        let url = format!("http://{}/ready", addr);
        let client = reqwest::Client::new();

        // Within the first ~80ms we should see 503. Tolerance-friendly poll.
        let mut saw_starting = false;
        for _ in 0..3 {
            let r = client.get(&url).send().await.expect("req");
            if r.status().as_u16() == 503 {
                saw_starting = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            saw_starting,
            "expected /ready to report 503 during 100ms warm-up"
        );

        // After 300ms total the flag must be set.
        tokio::time::sleep(Duration::from_millis(250)).await;
        let r = client.get(&url).send().await.expect("req");
        assert_eq!(r.status().as_u16(), 200, "ready should flip to 200");

        let _ = tx.send(());
        let _ = tokio::time::timeout(Duration::from_secs(1), join).await;
    }
}
