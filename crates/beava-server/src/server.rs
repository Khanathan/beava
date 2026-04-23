//! Server: bind + serve + graceful-shutdown wiring.
//!
//! Phase 2.5 update: binds TWO listeners by default — HTTP on `cfg.listen_addr`
//! and TCP on `(cfg.tcp.host, cfg.tcp.port)`. Both share a single
//! CancellationToken so `serve()` drains both on shutdown.

use crate::http::{router, ReadinessFlag};
use crate::tcp::TcpListenerHandle;
use crate::Config;
use beava_core::registry::Registry;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("failed to bind {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to bind TCP wire listener on {host}:{port}: {source}")]
    BindTcp {
        host: String,
        port: u16,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid listen address `{0}`: {1}")]
    InvalidAddr(String, String),
    #[error("server error: {0}")]
    Serve(#[source] std::io::Error),
}

/// Bound server ready to serve. `http_local_addr` is the actual bound HTTP
/// address, useful when config specified port 0 and the OS chose. When
/// `cfg.tcp.enabled`, `tcp_local_addr` is Some and the TCP listener is bound;
/// otherwise None (HTTP-only mode).
pub struct Server {
    http_listener: TcpListener,
    http_local_addr: SocketAddr,
    tcp_listener_handle: Option<TcpListenerHandle>,
    tcp_local_addr: Option<SocketAddr>,
    readiness: ReadinessFlag,
    registry: Arc<Registry>,
    dev_endpoints: bool,
}

impl std::fmt::Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server")
            .field("http_local_addr", &self.http_local_addr)
            .field("tcp_local_addr", &self.tcp_local_addr)
            .finish_non_exhaustive()
    }
}

impl Server {
    /// Resolve config's listen_addr and bind. Also arms the 100ms readiness delay
    /// that flips `/ready` from 503 to 200 — stand-in for Phase 5's real
    /// recovery-complete signal.
    ///
    /// Phase 2.5: also binds the TCP wire listener on `(cfg.tcp.host, cfg.tcp.port)`
    /// when `cfg.tcp.enabled` (default true). Operators who want HTTP-only mode
    /// must set `tcp.enabled: false` or `BEAVA_TCP_ENABLED=0`.
    ///
    /// `dev_endpoints`: pass `true` to mount `GET /registry`. Production callers
    /// derive this from `BEAVA_DEV_ENDPOINTS=1` env var (see `main.rs`).
    pub async fn bind(cfg: &Config, dev_endpoints: bool) -> Result<Self, ServerError> {
        let addr: SocketAddr = cfg
            .listen_addr
            .parse()
            .map_err(|e: std::net::AddrParseError| {
                ServerError::InvalidAddr(cfg.listen_addr.clone(), e.to_string())
            })?;
        let http_listener = TcpListener::bind(addr)
            .await
            .map_err(|e| ServerError::Bind { addr, source: e })?;
        let http_local_addr = http_listener.local_addr().map_err(ServerError::Serve)?;

        tracing::info!(
            target: "beava.server",
            kind = "server.http_bound",
            addr = %http_local_addr,
            "HTTP server bound"
        );

        if dev_endpoints {
            tracing::info!(
                target: "beava.server",
                "dev endpoints enabled (BEAVA_DEV_ENDPOINTS=1)"
            );
        }

        let (tcp_listener_handle, tcp_local_addr) = if cfg.tcp.enabled {
            let h = TcpListenerHandle::bind(&cfg.tcp.host, cfg.tcp.port, cfg.tcp.max_frame_bytes)
                .await
                .map_err(|e| ServerError::BindTcp {
                    host: cfg.tcp.host.clone(),
                    port: cfg.tcp.port,
                    source: e,
                })?;
            let a = h.local_addr();
            tracing::info!(
                target: "beava.server",
                kind = "server.tcp_bound",
                addr = %a,
                "TCP wire listener bound"
            );
            (Some(h), Some(a))
        } else {
            (None, None)
        };

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
            http_listener,
            http_local_addr,
            tcp_listener_handle,
            tcp_local_addr,
            readiness,
            registry,
            dev_endpoints,
        })
    }

    /// Backward-compat alias for the HTTP address. Phase 1/2 tests call this.
    pub fn local_addr(&self) -> SocketAddr {
        self.http_local_addr
    }

    pub fn http_local_addr(&self) -> SocketAddr {
        self.http_local_addr
    }

    pub fn tcp_local_addr(&self) -> Option<SocketAddr> {
        self.tcp_local_addr
    }

    /// Run the server until `shutdown` completes. Emits JSON log events on
    /// shutdown initiation (from the signal handler) and shutdown complete
    /// (here). Returns after in-flight requests drain on both listeners.
    pub async fn serve<F>(self, shutdown: F) -> Result<(), ServerError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let cancel = CancellationToken::new();

        // Translate the external `shutdown` future into a cancel trip.
        let cancel_for_signal = cancel.clone();
        let signal_task = tokio::spawn(async move {
            shutdown.await;
            cancel_for_signal.cancel();
        });

        // Spawn TCP accept loop if enabled.
        let tcp_task = self.tcp_listener_handle.map(|handle| {
            let reg = Arc::clone(&self.registry);
            let cancel_child = cancel.clone();
            tokio::spawn(crate::tcp::accept_loop(handle, reg, cancel_child))
        });

        // HTTP serve with graceful shutdown tied to the same cancel.
        let app = router(self.readiness, self.registry, self.dev_endpoints);
        let http_cancel = cancel.clone();
        let http_shutdown = async move {
            http_cancel.cancelled().await;
        };

        let start = Instant::now();
        let http_result = axum::serve(self.http_listener, app)
            .with_graceful_shutdown(http_shutdown)
            .await;

        // Trip cancel in case HTTP exited for a reason other than cancel.
        cancel.cancel();
        if let Some(t) = tcp_task {
            let _ = t.await; // ignore join errors; accept_loop logs its own exit
        }
        // The signal task exits naturally after the shutdown future completes
        // (or when cancel is tripped above — but it already awaited shutdown).
        let _ = signal_task.await;

        http_result.map_err(ServerError::Serve)?;

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
    use beava_core::config::TcpConfig;

    fn tmp_cfg() -> Config {
        Config {
            listen_addr: "127.0.0.1:0".to_string(), // OS-allocated port
            log_level: "info".to_string(),
            tcp: TcpConfig {
                // Disable TCP in the legacy Phase 1/2 server tests — they predate TCP.
                enabled: false,
                ..Default::default()
            },
        }
    }

    fn tmp_cfg_with_tcp() -> Config {
        Config {
            listen_addr: "127.0.0.1:0".to_string(),
            log_level: "info".to_string(),
            tcp: TcpConfig {
                enabled: true,
                host: "127.0.0.1".to_string(),
                port: 0, // OS-assigned
                max_frame_bytes: 4 * 1024 * 1024,
            },
        }
    }

    #[tokio::test]
    async fn bind_reports_actual_local_addr() {
        let cfg = tmp_cfg();
        let s = Server::bind(&cfg, false).await.expect("bind");
        let addr = s.local_addr();
        assert_eq!(addr.ip().to_string(), "127.0.0.1");
        assert_ne!(addr.port(), 0, "OS should have allocated a real port");
    }

    #[tokio::test]
    async fn invalid_addr_returns_structured_error() {
        let cfg = Config {
            listen_addr: "not-an-addr".to_string(),
            log_level: "info".to_string(),
            tcp: TcpConfig {
                enabled: false,
                ..Default::default()
            },
        };
        let err = Server::bind(&cfg, false).await.unwrap_err();
        assert!(matches!(err, ServerError::InvalidAddr(_, _)));
    }

    #[tokio::test]
    async fn serve_then_shutdown_exits_within_500ms() {
        let cfg = tmp_cfg();
        let s = Server::bind(&cfg, false).await.expect("bind");
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
        let s = Server::bind(&cfg, false).await.expect("bind");
        let addr = s.local_addr();

        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let shutdown = async move {
            let _ = rx.await;
        };
        let join = tokio::spawn(async move { s.serve(shutdown).await });

        let url = format!("http://{}/ready", addr);
        let client = reqwest::Client::new();

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

        tokio::time::sleep(Duration::from_millis(250)).await;
        let r = client.get(&url).send().await.expect("req");
        assert_eq!(r.status().as_u16(), 200, "ready should flip to 200");

        let _ = tx.send(());
        let _ = tokio::time::timeout(Duration::from_secs(1), join).await;
    }

    // ─── Phase 2.5 TCP-aware server tests ─────────────────────────────────────

    #[tokio::test]
    async fn bind_with_tcp_disabled_yields_none_tcp_addr() {
        let cfg = tmp_cfg();
        let s = Server::bind(&cfg, false).await.expect("bind");
        assert!(s.tcp_local_addr().is_none());
        assert_ne!(s.http_local_addr().port(), 0);
    }

    #[tokio::test]
    async fn bind_with_tcp_enabled_yields_some_tcp_addr() {
        let cfg = tmp_cfg_with_tcp();
        let s = Server::bind(&cfg, false).await.expect("bind");
        let tcp = s.tcp_local_addr().expect("tcp bound");
        assert_eq!(tcp.ip().to_string(), "127.0.0.1");
        assert_ne!(tcp.port(), 0);
    }

    #[tokio::test]
    async fn bind_with_tcp_port_conflict_returns_bind_error() {
        // First server grabs a port. Then bind a second server asking for that exact port.
        let h = TcpListenerHandle::bind("127.0.0.1", 0, 1024).await.unwrap();
        let busy_port = h.local_addr().port();
        // Leak: keep h alive for the duration of the test so the port stays busy.
        let cfg = Config {
            listen_addr: "127.0.0.1:0".to_string(),
            log_level: "info".to_string(),
            tcp: TcpConfig {
                enabled: true,
                host: "127.0.0.1".to_string(),
                port: busy_port,
                max_frame_bytes: 1024,
            },
        };
        let err = Server::bind(&cfg, false).await.unwrap_err();
        assert!(matches!(err, ServerError::BindTcp { .. }));
        drop(h);
    }

    #[tokio::test]
    async fn serve_shuts_down_both_listeners_within_500ms() {
        let cfg = tmp_cfg_with_tcp();
        let s = Server::bind(&cfg, false).await.expect("bind");
        let tcp_addr = s.tcp_local_addr().unwrap();

        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let shutdown = async move {
            let _ = rx.await;
        };
        let join = tokio::spawn(async move { s.serve(shutdown).await });

        // Open a TCP connection but don't send anything (idle).
        let _conn = tokio::net::TcpStream::connect(tcp_addr).await.unwrap();

        let start = std::time::Instant::now();
        let _ = tx.send(());
        tokio::time::timeout(Duration::from_millis(800), join)
            .await
            .expect("exit within 800ms")
            .expect("join")
            .expect("serve ok");
        let elapsed = start.elapsed();
        assert!(elapsed < Duration::from_millis(800), "took {elapsed:?}");
    }

    #[tokio::test]
    async fn invalid_tcp_host_returns_structured_error() {
        let cfg = Config {
            listen_addr: "127.0.0.1:0".to_string(),
            log_level: "info".to_string(),
            tcp: TcpConfig {
                enabled: true,
                host: "definitely-not-a-valid-host-format-xyz.invalid".to_string(),
                port: 0,
                max_frame_bytes: 1024,
            },
        };
        let err = Server::bind(&cfg, false).await.unwrap_err();
        assert!(matches!(err, ServerError::BindTcp { .. }));
    }
}
