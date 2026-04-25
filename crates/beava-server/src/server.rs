//! Server: bind + serve + graceful-shutdown wiring.
//!
//! Phase 2.5 update: binds TWO listeners by default — HTTP on `cfg.listen_addr`
//! and TCP on `(cfg.tcp.host, cfg.tcp.port)`. Both share a single
//! CancellationToken so `serve()` drains both on shutdown.

use crate::http::{router_with_push, ReadinessFlag};
use crate::idem_cache::IdemCache;
use crate::recovery::{load_snapshot_if_any, replay_wal_from_lsn};
use crate::registry_debug::DevAggState;
use crate::snapshot_task::{spawn_snapshot_task, SnapshotTaskConfig, SnapshotTriggerTx};
use crate::tcp::TcpListenerHandle;
use crate::{AppState, Config};
use beava_core::registry::Registry;
use beava_persistence::{SyncMode, WalSink, WalSinkConfig};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
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
    #[error("failed to spawn WAL sink: {0}")]
    WalSpawn(String),
    #[error("recovery failed: {0}")]
    Recovery(String),
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
    app_state: Arc<AppState>,
    wal_worker: Option<JoinHandle<()>>,
    snapshot_worker: Option<JoinHandle<()>>,
    snapshot_cancel: CancellationToken,
    snapshot_trigger: SnapshotTriggerTx,
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
        let idem_cache = Arc::new(IdemCache::new());
        let dev_agg = DevAggState::new(registry.clone());

        // Phase 7 Plan 03: recovery BEFORE WAL sink spawns.
        // 1. Snapshot install (descriptors + state_tables + counters).
        // 2. WAL replay from snapshot_lsn forward (events + RegistryBumps).
        // Recovery runs against `dev_agg` directly — it doesn't touch the WAL
        // sink. After recovery returns, we spawn the real WAL sink with
        // initial_start_lsn = max(last_lsn, snapshot_lsn) + 1 so new appends
        // land in a fresh segment past anything already on disk.
        let snapshot_lsn = load_snapshot_if_any(&cfg.durability.snapshot_dir, &dev_agg)
            .map_err(|e| ServerError::Recovery(e.to_string()))?;
        let recovery_outcome = replay_wal_from_lsn(&cfg.durability.wal_dir, snapshot_lsn, &dev_agg)
            .map_err(|e| ServerError::Recovery(e.to_string()))?;
        let initial_start_lsn = recovery_outcome.last_lsn.max(snapshot_lsn) + 1;

        // Phase 6: spawn the WAL sink + periodic idem-cache sweeper.
        let (wal_sink, wal_worker) = WalSink::spawn(WalSinkConfig {
            dir: cfg.durability.wal_dir.clone(),
            initial_start_lsn,
            initial_registry_version: 1,
            fsync_interval_ms: cfg.durability.wal_fsync_interval_ms,
            fsync_bytes: cfg.durability.wal_fsync_bytes,
            segment_bytes: cfg.durability.wal_segment_bytes,
            sync_mode: match cfg.durability.wal_sync_mode {
                beava_core::config::WalSyncMode::Periodic => SyncMode::Periodic,
                beava_core::config::WalSyncMode::PerEvent => SyncMode::PerEvent,
            },
        })
        .map_err(|e| ServerError::WalSpawn(e.to_string()))?;

        let app_state = Arc::new(AppState::new(dev_agg, wal_sink.clone(), idem_cache.clone()));

        // Periodic dedupe sweep.
        let sweep_interval_secs = cfg.durability.dedupe_sweep_interval_secs.max(1);
        let sweep_cache = idem_cache.clone();
        tokio::spawn(async move {
            let mut iv = tokio::time::interval(Duration::from_secs(sweep_interval_secs));
            iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                iv.tick().await;
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                let _ = sweep_cache.sweep_expired(now);
            }
        });

        // Phase 7 Plan 03: spawn periodic snapshot task.
        let snapshot_cancel = CancellationToken::new();
        let (snapshot_worker, snapshot_trigger) = spawn_snapshot_task(
            SnapshotTaskConfig {
                interval: Duration::from_millis(cfg.durability.snapshot_interval_ms.max(1)),
                snapshot_dir: cfg.durability.snapshot_dir.clone(),
                retain: cfg.durability.snapshot_retain_count.max(1),
            },
            Arc::clone(&app_state),
            wal_sink.clone(),
            snapshot_cancel.clone(),
        );

        // Recovery complete → flip readiness immediately.
        readiness.set_ready();
        tracing::info!(
            target: "beava.server",
            kind = "server.ready",
            snapshot_lsn,
            replay_event_count = recovery_outcome.replay_event_count,
            replay_registry_bumps = recovery_outcome.replay_registry_bumps,
            initial_start_lsn,
            "recovery complete; readiness flag set"
        );

        Ok(Self {
            http_listener,
            http_local_addr,
            tcp_listener_handle,
            tcp_local_addr,
            readiness,
            registry,
            dev_endpoints,
            app_state,
            wal_worker: Some(wal_worker),
            snapshot_worker: Some(snapshot_worker),
            snapshot_cancel,
            snapshot_trigger,
        })
    }

    /// Phase 7 Plan 03: test hook — force the snapshot task to run NOW.
    /// Available to integration tests / `TestServer::force_snapshot_now`.
    #[doc(hidden)]
    pub async fn force_snapshot_now(&self) -> Result<(), String> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.snapshot_trigger
            .send(tx)
            .await
            .map_err(|_| "snapshot task channel closed".to_string())?;
        rx.await
            .map_err(|_| "snapshot task ack channel dropped".to_string())?
    }

    /// Return the shared AppState Arc — used by the Phase 6 crash probe binary
    /// to register a test event before accepting /push.
    pub fn app_state(&self) -> Arc<AppState> {
        Arc::clone(&self.app_state)
    }

    /// Backward-compat alias for the HTTP address. Phase 1/2 tests call this.
    pub fn local_addr(&self) -> SocketAddr {
        self.http_local_addr
    }

    /// Phase 7 Plan 03: cloneable handle to the snapshot-trigger channel.
    /// `TestServer` holds onto this so `force_snapshot_now()` continues to
    /// work after `serve()` has consumed the `Server`.
    #[doc(hidden)]
    pub fn snapshot_trigger_handle(&self) -> SnapshotTriggerTx {
        self.snapshot_trigger.clone()
    }

    pub fn http_local_addr(&self) -> SocketAddr {
        self.http_local_addr
    }

    pub fn tcp_local_addr(&self) -> Option<SocketAddr> {
        self.tcp_local_addr
    }

    /// Return a clone of the shared registry Arc.  Used by `TestServer::registry()`
    /// to give acceptance tests direct access to compiled chains without HTTP round-trips.
    pub fn registry(&self) -> Arc<Registry> {
        Arc::clone(&self.registry)
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

        // Spawn TCP accept loop if enabled — Phase 8+ uses the AppState-aware
        // loop so OP_PUSH is routed through the shared WAL + idem-cache +
        // apply-loop pipeline (same as the HTTP handler).
        let tcp_task = self.tcp_listener_handle.map(|handle| {
            let app_for_tcp = Arc::clone(&self.app_state);
            let cancel_child = cancel.clone();
            tokio::spawn(crate::tcp::accept_loop_with_app(
                handle,
                app_for_tcp,
                cancel_child,
            ))
        });

        // HTTP serve with graceful shutdown tied to the same cancel.
        let app = router_with_push(
            self.readiness,
            self.registry,
            self.dev_endpoints,
            None,
            Some(Arc::clone(&self.app_state)),
        );
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

        // Stop snapshot task (cancel + await).
        self.snapshot_cancel.cancel();
        if let Some(worker) = self.snapshot_worker {
            let _ = worker.await;
        }

        // Drain WAL sink pending batches before returning.
        let _ = self.app_state.wal_sink.clone().shutdown().await;
        if let Some(worker) = self.wal_worker {
            let _ = worker.await;
        }

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

// ─── Phase 18 Plan 01: hand-rolled runtime entry point ────────────────────────

/// A server bound with the hand-rolled event loop on the data-plane and
/// tokio/axum on the admin plane.  Created by [`ServerV18::bind`].
///
/// The HTTP and TCP event-plane listeners run on the mio-based
/// `beava-runtime-core` `EventLoop`; the admin plane runs on a separate
/// tokio runtime so `/health`, `/metrics`, and `/registry` stay responsive
/// even when the event loop is saturated.
///
/// See Plan 18-01 Task 1.5 and D-01 in 18-CONTEXT.md.
/// Plan 18-07: feature flag removed; ServerV18 is now unconditionally compiled.
///
/// Plan 18-05.1: `serve()` wires the data-plane listeners into an actual
/// dispatch path. The transport is tokio-based in this slice (matching the
/// existing `Server::serve` path) — the pure mio EventLoop dispatch (Plans
/// 18-05/18-06 proper) replaces this once the WAL is converted to sync.
pub struct ServerV18 {
    http_addr: std::net::SocketAddr,
    tcp_addr: std::net::SocketAddr,
    admin: crate::http_admin::BoundAdminServer,
    // Event-plane listeners bound at construction time. serve() converts these
    // to tokio listeners and hands them to the HTTP/TCP accept loops.
    http_listener: std::net::TcpListener,
    tcp_listener: std::net::TcpListener,
}

impl ServerV18 {
    /// Bind HTTP, TCP, and admin listeners.
    ///
    /// - `http_addr` — event-plane HTTP listener (hand-rolled mio loop)
    /// - `tcp_addr`  — event-plane TCP framed listener (hand-rolled mio loop)
    /// - `admin_addr` — admin HTTP listener (tokio/axum)
    ///
    /// All three ports may be `0` for OS assignment.  The actual bound
    /// addresses are available via [`http_addr`], [`tcp_addr`], [`admin_addr`].
    pub async fn bind(
        http_addr: std::net::SocketAddr,
        tcp_addr: std::net::SocketAddr,
        admin_addr: std::net::SocketAddr,
    ) -> Result<Self, ServerError> {
        // Bind event-plane listeners (std::net — they'll be handed to mio later).
        let http_listener =
            std::net::TcpListener::bind(http_addr).map_err(|e| ServerError::Bind {
                addr: http_addr,
                source: e,
            })?;
        http_listener
            .set_nonblocking(true)
            .map_err(|e| ServerError::Bind {
                addr: http_addr,
                source: e,
            })?;
        let http_bound = http_listener.local_addr().map_err(ServerError::Serve)?;

        let tcp_listener =
            std::net::TcpListener::bind(tcp_addr).map_err(|e| ServerError::BindTcp {
                host: tcp_addr.ip().to_string(),
                port: tcp_addr.port(),
                source: e,
            })?;
        tcp_listener
            .set_nonblocking(true)
            .map_err(|e| ServerError::BindTcp {
                host: tcp_addr.ip().to_string(),
                port: tcp_addr.port(),
                source: e,
            })?;
        let tcp_bound = tcp_listener.local_addr().map_err(ServerError::Serve)?;

        // Bind admin (tokio/axum).
        let snapshot = std::sync::Arc::new(std::sync::RwLock::new(
            crate::http_admin::RegistrySnapshot::default(),
        ));
        let admin = crate::http_admin::BoundAdminServer::bind(admin_addr, snapshot)
            .await
            .map_err(|e| ServerError::Bind {
                addr: admin_addr,
                source: e,
            })?;

        Ok(Self {
            http_addr: http_bound,
            tcp_addr: tcp_bound,
            admin,
            http_listener,
            tcp_listener,
        })
    }

    /// The bound HTTP event-plane address.
    pub fn http_addr(&self) -> std::net::SocketAddr {
        self.http_addr
    }

    /// The bound TCP event-plane address.
    pub fn tcp_addr(&self) -> std::net::SocketAddr {
        self.tcp_addr
    }

    /// The bound admin HTTP address.
    pub fn admin_addr(&self) -> std::net::SocketAddr {
        self.admin.local_addr
    }

    /// Run the server until `shutdown` completes.
    ///
    /// Plan 18-05.1: wires the already-bound HTTP and TCP event-plane listeners
    /// into real dispatch using the same tokio/axum + AppState path as the
    /// legacy `Server::serve()`. This gives measured EPS numbers immediately.
    /// The pure mio EventLoop dispatch (Plans 18-05/18-06 proper) replaces this
    /// accept path once the WAL is converted to synchronous `Write`.
    ///
    /// Boots a temporary WAL in `std::env::temp_dir()` for the duration of the
    /// serve call. Callers that need a durable WAL path should use
    /// `serve_with_dirs()` instead (added in Plan 18-06).
    pub async fn serve<F>(self, shutdown: F) -> Result<(), ServerError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // Use temp dirs for WAL + snapshot (bench / test use case).
        let wal_dir = std::env::temp_dir().join(format!(
            "beava-v18-wal-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(0)
        ));
        let snapshot_dir = std::env::temp_dir().join(format!(
            "beava-v18-snap-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(0)
        ));
        self.serve_with_dirs(shutdown, wal_dir, snapshot_dir).await
    }

    /// Run with explicit WAL + snapshot directories. Called by `serve()` with
    /// temp dirs, and by the bench harness with configured paths.
    pub async fn serve_with_dirs<F>(
        self,
        shutdown: F,
        wal_dir: std::path::PathBuf,
        snapshot_dir: std::path::PathBuf,
    ) -> Result<(), ServerError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        use beava_core::config::DurabilityConfig;

        // Build AppState — same as Server::bind but without recovery (v18 is
        // a clean-slate bench path; recovery wired in Plan 18-06).
        let idem_cache = Arc::new(IdemCache::new());
        let readiness = ReadinessFlag::new();
        let registry = Arc::new(beava_core::registry::Registry::new());
        let dev_agg = crate::registry_debug::DevAggState::new(registry.clone());

        let dur = DurabilityConfig {
            wal_dir: wal_dir.clone(),
            snapshot_dir: snapshot_dir.clone(),
            wal_fsync_interval_ms: 2,
            wal_fsync_bytes: 0,
            wal_segment_bytes: 64 * 1024 * 1024,
            wal_sync_mode: beava_core::config::WalSyncMode::Periodic,
            dedupe_sweep_interval_secs: 60,
            snapshot_interval_ms: 60_000,
            snapshot_retain_count: 2,
        };

        let (wal_sink, wal_worker) = beava_persistence::WalSink::spawn(WalSinkConfig {
            dir: wal_dir,
            initial_start_lsn: 1,
            initial_registry_version: 1,
            fsync_interval_ms: dur.wal_fsync_interval_ms,
            fsync_bytes: dur.wal_fsync_bytes,
            segment_bytes: dur.wal_segment_bytes,
            sync_mode: beava_persistence::SyncMode::Periodic,
        })
        .map_err(|e| ServerError::WalSpawn(e.to_string()))?;

        let app_state = Arc::new(AppState::new(dev_agg, wal_sink.clone(), idem_cache));

        let snapshot_cancel = CancellationToken::new();
        let (snapshot_worker, snapshot_trigger) = spawn_snapshot_task(
            SnapshotTaskConfig {
                interval: Duration::from_millis(60_000),
                snapshot_dir,
                retain: 2,
            },
            Arc::clone(&app_state),
            wal_sink.clone(),
            snapshot_cancel.clone(),
        );
        drop(snapshot_trigger); // bench doesn't need manual snapshot trigger

        readiness.set_ready();

        // Convert std::net listeners to tokio (they were set nonblocking in bind()).
        let http_tokio =
            tokio::net::TcpListener::from_std(self.http_listener).map_err(ServerError::Serve)?;
        let tcp_tokio =
            tokio::net::TcpListener::from_std(self.tcp_listener).map_err(ServerError::Serve)?;

        let cancel = CancellationToken::new();

        // Translate the external shutdown future into a cancel trip.
        let cancel_for_signal = cancel.clone();
        let signal_task = tokio::spawn(async move {
            shutdown.await;
            cancel_for_signal.cancel();
        });

        // Wrap the tokio TCP listener in a TcpListenerHandle for accept_loop_with_app.
        // We can't construct TcpListenerHandle directly (private fields), so we
        // drive the accept loop inline using the tokio listener.
        let tcp_app = Arc::clone(&app_state);
        let tcp_cancel = cancel.clone();
        let tcp_task = tokio::spawn(async move {
            use beava_core::wire::{decode_frame, encode_frame, Frame, CT_JSON, OP_ERROR_RESPONSE};
            use bytes::BytesMut;
            use tokio::io::{AsyncReadExt, AsyncWriteExt};

            loop {
                tokio::select! {
                    accept = tcp_tokio.accept() => {
                        match accept {
                            Ok((mut stream, _peer)) => {
                                let app = Arc::clone(&tcp_app);
                                let conn_cancel = tcp_cancel.clone();
                                tokio::spawn(async move {
                                    let mut buf = BytesMut::with_capacity(4096);
                                    loop {
                                        if conn_cancel.is_cancelled() { break; }
                                        let n = match stream.read_buf(&mut buf).await {
                                            Ok(0) | Err(_) => break,
                                            Ok(n) => n,
                                        };
                                        let _ = n;
                                        loop {
                                            match decode_frame(&mut buf, 4 * 1024 * 1024) {
                                                Ok(Some(frame)) => {
                                                    let resp_frame = dispatch_tcp_frame(&app, frame).await;
                                                    let mut out = BytesMut::new();
                                                    encode_frame(&resp_frame, &mut out);
                                                    if stream.write_all(&out).await.is_err() { return; }
                                                }
                                                Ok(None) => break,
                                                Err(_) => {
                                                    let err_frame = Frame::new(OP_ERROR_RESPONSE, CT_JSON,
                                                        bytes::Bytes::from_static(b"{\"code\":\"frame_error\"}"));
                                                    let mut out = BytesMut::new();
                                                    encode_frame(&err_frame, &mut out);
                                                    let _ = stream.write_all(&out).await;
                                                    return;
                                                }
                                            }
                                        }
                                    }
                                });
                            }
                            Err(_) => break,
                        }
                    }
                    _ = tcp_cancel.cancelled() => break,
                }
            }
        });

        // HTTP serve with graceful shutdown.
        let http_app = router_with_push(
            readiness,
            registry,
            true, // dev_endpoints
            None,
            Some(Arc::clone(&app_state)),
        );
        let http_cancel = cancel.clone();
        let http_shutdown = async move { http_cancel.cancelled().await };

        axum::serve(http_tokio, http_app)
            .with_graceful_shutdown(http_shutdown)
            .await
            .map_err(ServerError::Serve)?;

        cancel.cancel();
        let _ = tcp_task.await;
        let _ = signal_task.await;

        snapshot_cancel.cancel();
        if let Some(w) = Some(snapshot_worker) {
            let _ = w.await;
        }

        let _ = app_state.wal_sink.clone().shutdown().await;
        let _ = wal_worker.await;

        self.admin.shutdown().await;
        Ok(())
    }

    /// Gracefully shut down the admin server without running serve().
    /// Use this only when serve() was never called (e.g. bind-only tests).
    pub async fn shutdown(self) {
        self.admin.shutdown().await;
    }
}

// ─── TCP frame dispatch helper (Plan 18-05.1) ─────────────────────────────────

/// Dispatch one decoded wire frame to the apply path and return a response frame.
///
/// Used by the inline TCP accept loop inside `ServerV18::serve()`. Async because
/// `dispatch_wire_request` drives the tokio WAL sink channel; once the WAL is
/// converted to sync `Write` (Plan 18-06) this can become `fn`.
async fn dispatch_tcp_frame(
    app: &Arc<AppState>,
    frame: beava_core::wire::Frame,
) -> beava_core::wire::Frame {
    use beava_core::wire::{OP_PING, OP_PUSH, OP_REGISTER};
    use beava_runtime_core::wire_request::WireRequest;
    use crate::runtime_core_glue::dispatch_wire_request;

    // Map raw frame op+payload → WireRequest.
    let wire_req = match frame.op {
        OP_PING => WireRequest::Ping,
        OP_REGISTER => WireRequest::Register { payload: frame.payload },
        OP_PUSH => {
            #[derive(serde::Deserialize)]
            struct PushEnvelope { event: String, body: serde_json::Value }
            match serde_json::from_slice::<PushEnvelope>(&frame.payload) {
                Ok(env) => {
                    let body = serde_json::to_vec(&env.body)
                        .map(bytes::Bytes::from)
                        .unwrap_or_else(|_| frame.payload.clone());
                    WireRequest::TcpPush { event_name: env.event, body }
                }
                Err(e) => WireRequest::ParseError { reason: e.to_string() },
            }
        }
        op => WireRequest::Unknown { op },
    };

    // Dispatch and serialise the GlueResponse back to a Frame.
    let glue = dispatch_wire_request(app, wire_req).await;
    glue_to_frame(glue)
}

/// Convert a `GlueResponse` into a wire `Frame` for the TCP transport.
fn glue_to_frame(glue: crate::runtime_core_glue::GlueResponse) -> beava_core::wire::Frame {
    use beava_core::wire::{CT_JSON, OP_ERROR_RESPONSE, OP_PING, OP_PUSH};
    use crate::runtime_core_glue::GlueResponse;

    match glue {
        GlueResponse::Pong { .. } => {
            beava_core::wire::Frame::new(OP_PING, CT_JSON, bytes::Bytes::from_static(b"{}"))
        }
        GlueResponse::PushAck { ack_lsn, registry_version } => {
            let body = serde_json::json!({"ack_lsn": ack_lsn, "registry_version": registry_version});
            let b = serde_json::to_vec(&body).unwrap_or_default();
            beava_core::wire::Frame::new(OP_PUSH, CT_JSON, bytes::Bytes::from(b))
        }
        GlueResponse::PushReplay { registry_version } => {
            let body = serde_json::json!({"replay": true, "registry_version": registry_version});
            let b = serde_json::to_vec(&body).unwrap_or_default();
            beava_core::wire::Frame::new(OP_PUSH, CT_JSON, bytes::Bytes::from(b))
        }
        GlueResponse::RegisterOk { version } => {
            let body = serde_json::json!({"version": version});
            let b = serde_json::to_vec(&body).unwrap_or_default();
            beava_core::wire::Frame::new(OP_PUSH, CT_JSON, bytes::Bytes::from(b))
        }
        GlueResponse::RegisterError { code, message } => {
            let body = serde_json::json!({"code": code, "message": message});
            let b = serde_json::to_vec(&body).unwrap_or_default();
            beava_core::wire::Frame::new(OP_ERROR_RESPONSE, CT_JSON, bytes::Bytes::from(b))
        }
        GlueResponse::PushError { code, .. } => {
            let body = serde_json::json!({"code": code, "message": ""});
            let b = serde_json::to_vec(&body).unwrap_or_default();
            beava_core::wire::Frame::new(OP_ERROR_RESPONSE, CT_JSON, bytes::Bytes::from(b))
        }
        _ => {
            beava_core::wire::Frame::new(
                OP_ERROR_RESPONSE, CT_JSON,
                bytes::Bytes::from_static(b"{\"code\":\"unsupported\"}"),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Config;
    use beava_core::config::TcpConfig;

    fn unique_wal_dir() -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static CTR: AtomicU64 = AtomicU64::new(1);
        let n = CTR.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("beava-server-test-wal-{}-{n}", std::process::id()))
    }

    fn unique_snapshot_dir() -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static CTR: AtomicU64 = AtomicU64::new(1);
        let n = CTR.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("beava-server-test-snap-{}-{n}", std::process::id()))
    }

    fn tmp_cfg() -> Config {
        Config {
            listen_addr: "127.0.0.1:0".to_string(), // OS-allocated port
            log_level: "info".to_string(),
            tcp: TcpConfig {
                // Disable TCP in the legacy Phase 1/2 server tests — they predate TCP.
                enabled: false,
                ..Default::default()
            },
            durability: beava_core::config::DurabilityConfig {
                wal_dir: unique_wal_dir(),
                wal_fsync_interval_ms: 1,
                snapshot_dir: unique_snapshot_dir(),
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
            durability: beava_core::config::DurabilityConfig {
                wal_dir: unique_wal_dir(),
                wal_fsync_interval_ms: 1,
                snapshot_dir: unique_snapshot_dir(),
                ..Default::default()
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
            durability: Default::default(),
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

    /// Phase 7 Plan 03: cold start (empty WAL + snapshot dirs) — recovery
    /// returns immediately, readiness flips before serve(). Verify /ready
    /// reports 200 within 200ms.
    #[tokio::test]
    async fn readiness_ready_after_cold_start_recovery() {
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

        // Cold-start recovery is immediate; /ready should be 200 right away.
        let r = client.get(&url).send().await.expect("req");
        assert_eq!(r.status().as_u16(), 200, "post-recovery /ready must be 200");

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
            durability: Default::default(),
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
            durability: Default::default(),
        };
        let err = Server::bind(&cfg, false).await.unwrap_err();
        assert!(matches!(err, ServerError::BindTcp { .. }));
    }
}
