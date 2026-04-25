//! Server: bind + serve + graceful-shutdown wiring.
//!
//! Phase 2.5 update: binds TWO listeners by default — HTTP on `cfg.listen_addr`
//! and TCP on `(cfg.tcp.host, cfg.tcp.port)`. Both share a single
//! CancellationToken so `serve()` drains both on shutdown.

use crate::http::{router_with_push, ReadinessFlag};
use crate::idem_cache::IdemCache;
use crate::recovery::{load_snapshot_if_any, replay_handrolled_wal_dir, replay_wal_from_lsn};
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
    ///
    /// Plan 18-04.6: REPLACES the tokio shim (Plan 18-05.1) with a real mio
    /// EventLoop on a dedicated `std::thread`. Threading model (D-4):
    ///   - 1 apply thread: mio Poll + EventLoop::tick + ApplyShard::dispatch
    ///   - N IoPool workers: parallel read-parse + write-serialize
    ///   - 1 WalWriter thread: drain sealed WAL buffers → write + fsync
    ///   - 1 tokio runtime: admin endpoints only (/metrics /health /ready /registry)
    pub async fn serve_with_dirs<F>(
        self,
        shutdown: F,
        wal_dir: std::path::PathBuf,
        snapshot_dir: std::path::PathBuf,
    ) -> Result<(), ServerError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        use crate::apply_shard::ApplyShard;
        use beava_runtime_core::wal_buffer::WalBufferRing;
        use beava_runtime_core::wal_lsn::WalLsn;
        use beava_runtime_core::wal_writer::WalWriter;
        use std::sync::atomic::{AtomicBool, Ordering as AOrdering};

        // ── Build AppState ────────────────────────────────────────────────────
        let idem_cache = Arc::new(IdemCache::new());
        let registry = Arc::new(beava_core::registry::Registry::new());
        let dev_agg = crate::registry_debug::DevAggState::new(registry.clone());

        // ── Recovery: replay persistence WAL (*.log) then hand-rolled WAL (*.wal) ──
        //
        // Step 1: replay *.log registry bumps from beava-persistence WalSink.
        //   These records carry /register payloads (RegistryBump). Without them,
        //   the second server instance has no event descriptors and cannot replay
        //   data-plane events.
        //
        // Step 2: replay *.wal data-plane events from WalBufferRing + WalWriter.
        //   These records use the v=2 binary format (apply_shard.rs).
        let persistence_lsn = if wal_dir.exists() {
            match replay_wal_from_lsn(&wal_dir, 0, &dev_agg) {
                Ok(outcome) => outcome.last_lsn,
                Err(_) => 0,
            }
        } else {
            0
        };

        // Step 2: replay hand-rolled *.wal data-plane events.
        // lsn_start = persistence_lsn + 1 to keep LSNs monotonic across both WALs.
        let handrolled_lsn_start = persistence_lsn + 1;
        let handrolled_outcome =
            replay_handrolled_wal_dir(&wal_dir, handrolled_lsn_start, &dev_agg).unwrap_or_default();
        let initial_start_lsn = handrolled_outcome.last_lsn.max(persistence_lsn) + 1;

        tracing::info!(
            target: "beava.recovery",
            kind = "recovery.serve_with_dirs",
            persistence_lsn,
            handrolled_events = handrolled_outcome.replay_event_count,
            initial_start_lsn,
            "serve_with_dirs recovery complete"
        );

        // Legacy WalSink: still used for /register cold path (admin endpoint).
        // Data-plane push uses WalBufferRing directly (D-2).
        // initial_start_lsn ensures the new *.log segment doesn't collide with
        // the existing one from the previous server instance.
        let (wal_sink, legacy_wal_worker) = beava_persistence::WalSink::spawn(WalSinkConfig {
            dir: wal_dir.clone(),
            initial_start_lsn,
            initial_registry_version: dev_agg.registry.version() as u32,
            fsync_interval_ms: 2,
            fsync_bytes: 0,
            segment_bytes: 64 * 1024 * 1024,
            sync_mode: beava_persistence::SyncMode::Periodic,
        })
        .map_err(|e| ServerError::WalSpawn(e.to_string()))?;

        let app_state = Arc::new(AppState::new(dev_agg, wal_sink.clone(), idem_cache));

        // ── Hand-rolled WAL stack ────────────────────────────────────────────
        let wal_lsn = Arc::new(WalLsn::new());
        // 3 buffers × 16 MiB each (covers ~50k events per buffer at 300 bytes each).
        let buf_bytes = 16 * 1024 * 1024;
        let wal_ring = Arc::new(WalBufferRing::new(3, buf_bytes, Arc::clone(&wal_lsn)));

        // WAL writer thread: drains sealed buffers, calls write() + fsync().
        let wal_writer_dir = wal_dir.clone();
        let wal_writer = WalWriter::new(
            &wal_writer_dir,
            Arc::clone(&wal_ring),
            Arc::clone(&wal_lsn),
            2, // tick_ms — match legacy fsync_interval_ms
        )
        .map_err(|e| ServerError::WalSpawn(e.to_string()))?;
        let wal_writer_handle = wal_writer.spawn();

        // ── Apply shard ───────────────────────────────────────────────────────
        let apply_shard = ApplyShard::new(
            Arc::clone(&app_state),
            Arc::clone(&wal_ring),
            Arc::clone(&wal_lsn),
        );

        // ── Shutdown flag (shared between tokio + apply thread) ───────────────
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let shutdown_flag_apply = Arc::clone(&shutdown_flag);
        let shutdown_flag_signal = Arc::clone(&shutdown_flag);

        // ── Convert std::net listeners → mio listeners ────────────────────────
        // ServerV18::bind() already set them nonblocking.
        let http_listener_std = self.http_listener;
        let tcp_listener_std = self.tcp_listener;

        // ── Spawn apply thread (mio EventLoop) ───────────────────────────────
        // The apply thread owns mio Poll + client map + IoPool.
        // It does NOT touch tokio.
        let apply_join = std::thread::Builder::new()
            .name("beava-apply".to_owned())
            .spawn(move || {
                run_mio_event_loop(
                    http_listener_std,
                    tcp_listener_std,
                    apply_shard,
                    shutdown_flag_apply,
                );
            })
            .map_err(ServerError::Serve)?;

        // ── Tokio: admin + shutdown signal ────────────────────────────────────
        // Start snapshot task (legacy, admin plane).
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
        drop(snapshot_trigger);

        // Wait for the external shutdown future, then signal the apply thread.
        shutdown.await;
        shutdown_flag_signal.store(true, AOrdering::Release);

        // Wait for the apply thread to drain.
        let _ = apply_join.join();

        // Signal the WalWriter to do a final fsync and exit.
        let _wal_shutdown_flag = Arc::new(AtomicBool::new(true));
        // The WalWriter's shutdown flag is internal; signal via a local AtomicBool.
        // Since we already joined the apply thread (no more appends), we just wait
        // for the wal_writer_handle to complete naturally.
        // The WalWriter loop: sleep tick → seal → drain → check shutdown.
        // Give it 2 ticks (4ms) to drain, then drop.
        std::thread::sleep(Duration::from_millis(6));
        drop(wal_writer_handle);

        // Stop snapshot task.
        snapshot_cancel.cancel();
        let _ = snapshot_worker.await;

        // Drain legacy WalSink (used only for /register cold path).
        let _ = app_state.wal_sink.clone().shutdown().await;
        let _ = legacy_wal_worker.await;

        // Stop admin server.
        self.admin.shutdown().await;

        Ok(())
    }

    /// Gracefully shut down the admin server without running serve().
    /// Use this only when serve() was never called (e.g. bind-only tests).
    pub async fn shutdown(self) {
        self.admin.shutdown().await;
    }
}

// ─── Plan 18-04.6: real mio EventLoop driver ─────────────────────────────────

/// Token assignments for the mio event loop.
const TOKEN_HTTP_LISTENER: mio::Token = mio::Token(0);
const TOKEN_TCP_LISTENER: mio::Token = mio::Token(1);
/// Client connections start at token 2; new ones increment this counter.
const TOKEN_CLIENT_BASE: usize = 2;

/// Per-client connection state for the mio event loop.
struct MioClient {
    stream: mio::net::TcpStream,
    token: mio::Token,
    /// Protocol: HTTP or TCP framed wire.
    proto: MioProto,
    /// Inbound read buffer.
    read_buf: bytes::BytesMut,
    /// Serialized response bytes waiting to be written to the socket.
    write_buf: bytes::BytesMut,
    /// True when the client has been closed / should be removed.
    closed: bool,
}

#[derive(Clone, Copy, PartialEq)]
enum MioProto {
    Http,
    Tcp,
}

/// Run the mio event loop on a dedicated std::thread (Plan 18-04.6 D-4).
///
/// This is the heart of Plan 18-04.6: a Redis-shaped per-tick orchestration:
///   1. `EventLoop::tick` — poll mio for ready events (up to 5ms timeout)
///   2. For each readable client: read bytes into `read_buf`
///   3. For each client with data: parse + apply (ApplyShard::dispatch_wire_request_sync)
///   4. For each client with responses: write bytes from `write_buf`
///   5. Check shutdown flag; break if set
///
/// Note: IoPool parallelism (Plans 18-03/18-04) can be layered on top later.
/// For Plan 18-04.6, the apply is done inline on the apply thread (single-threaded
/// per-tick), which is correct and sufficient for M4 informational measurement.
fn run_mio_event_loop(
    http_listener_std: std::net::TcpListener,
    tcp_listener_std: std::net::TcpListener,
    apply_shard: crate::apply_shard::ApplyShard,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
) {
    use beava_runtime_core::event_loop::EventLoop;
    use beava_runtime_core::http_listener::parse_http_request;
    use beava_runtime_core::http_listener::HttpListener;
    use beava_runtime_core::tcp_listener::parse_wire_request;
    use beava_runtime_core::tcp_listener::TcpListener as MioTcpListener;
    use std::collections::HashMap;
    use std::io::{Read, Write};
    use std::sync::atomic::Ordering as AOrdering;

    let mut event_loop = match EventLoop::new() {
        Ok(el) => el,
        Err(e) => {
            tracing::error!("apply thread: EventLoop::new failed: {e}");
            return;
        }
    };

    // Convert std::net listeners → mio listeners.
    let mut http_listener = match HttpListener::from_std(http_listener_std) {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("apply thread: HttpListener::from_std failed: {e}");
            return;
        }
    };
    let mut tcp_listener = match MioTcpListener::from_std(tcp_listener_std) {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("apply thread: MioTcpListener::from_std failed: {e}");
            return;
        }
    };

    // Register listeners with the event loop.
    if let Err(e) = event_loop.register(
        http_listener.inner_mut(),
        TOKEN_HTTP_LISTENER,
        mio::Interest::READABLE,
    ) {
        tracing::error!("apply thread: register http listener failed: {e}");
        return;
    }
    if let Err(e) = event_loop.register(
        tcp_listener.inner_mut(),
        TOKEN_TCP_LISTENER,
        mio::Interest::READABLE,
    ) {
        tracing::error!("apply thread: register tcp listener failed: {e}");
        return;
    }

    // Client state map: token → MioClient.
    let mut clients: HashMap<usize, MioClient> = HashMap::new();
    let mut next_token: usize = TOKEN_CLIENT_BASE;

    // Read buffer scratch space.
    let mut tmp_buf = [0u8; 16 * 1024];

    tracing::info!(target: "beava.server", "apply thread: mio EventLoop started");

    loop {
        // ── Phase 1: poll ────────────────────────────────────────────────────
        let events = match event_loop.tick(Some(Duration::from_millis(5))) {
            Ok(events) => {
                // Collect tokens from the iterator before dropping the borrow.
                let tokens: Vec<(mio::Token, bool, bool)> = events
                    .map(|e| (e.token(), e.is_readable(), e.is_writable()))
                    .collect();
                tokens
            }
            Err(e) => {
                tracing::warn!("apply thread: poll error: {e}");
                continue;
            }
        };

        // ── Phase 2: accept new connections ──────────────────────────────────
        for (token, readable, _writable) in &events {
            if !readable {
                continue;
            }
            if *token == TOKEN_HTTP_LISTENER {
                loop {
                    match http_listener.accept() {
                        Ok((stream, _peer)) => {
                            let client_token = mio::Token(next_token);
                            next_token += 1;
                            let mut client = MioClient {
                                stream,
                                token: client_token,
                                proto: MioProto::Http,
                                read_buf: bytes::BytesMut::with_capacity(8 * 1024),
                                write_buf: bytes::BytesMut::new(),
                                closed: false,
                            };
                            if let Err(e) = event_loop.register(
                                &mut client.stream,
                                client_token,
                                mio::Interest::READABLE,
                            ) {
                                tracing::warn!("apply thread: register client failed: {e}");
                            } else {
                                clients.insert(client_token.0, client);
                            }
                        }
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                        Err(_) => break,
                    }
                }
            } else if *token == TOKEN_TCP_LISTENER {
                loop {
                    match tcp_listener.accept() {
                        Ok((stream, _peer)) => {
                            let client_token = mio::Token(next_token);
                            next_token += 1;
                            let mut client = MioClient {
                                stream,
                                token: client_token,
                                proto: MioProto::Tcp,
                                read_buf: bytes::BytesMut::with_capacity(8 * 1024),
                                write_buf: bytes::BytesMut::new(),
                                closed: false,
                            };
                            if let Err(e) = event_loop.register(
                                &mut client.stream,
                                client_token,
                                mio::Interest::READABLE,
                            ) {
                                tracing::warn!("apply thread: register tcp client failed: {e}");
                            } else {
                                clients.insert(client_token.0, client);
                            }
                        }
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                        Err(_) => break,
                    }
                }
            }
        }

        // ── Phase 3: read data from readable clients ──────────────────────────
        for (token, readable, _writable) in &events {
            let tok_idx = token.0;
            if tok_idx < TOKEN_CLIENT_BASE || !readable {
                continue;
            }
            if let Some(client) = clients.get_mut(&tok_idx) {
                loop {
                    match client.stream.read(&mut tmp_buf) {
                        Ok(0) => {
                            client.closed = true;
                            break;
                        }
                        Ok(n) => {
                            client.read_buf.extend_from_slice(&tmp_buf[..n]);
                        }
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                        Err(_) => {
                            client.closed = true;
                            break;
                        }
                    }
                }
            }
        }

        // ── Phase 4: apply — parse + dispatch for all clients with data ───────
        let client_keys: Vec<usize> = clients.keys().cloned().collect();
        for tok_idx in client_keys {
            let client = match clients.get_mut(&tok_idx) {
                Some(c) => c,
                None => continue,
            };
            if client.closed || client.read_buf.is_empty() {
                continue;
            }

            match client.proto {
                MioProto::Tcp => {
                    // Parse all complete framed TCP requests.
                    loop {
                        let trace =
                            std::env::var("BEAVA_TRACE_SRV_TIMING").ok().as_deref() == Some("1");
                        let t_start = if trace {
                            Some(std::time::Instant::now())
                        } else {
                            None
                        };
                        match parse_wire_request(&mut client.read_buf, 4 * 1024 * 1024) {
                            Ok(Some(req)) => {
                                let t_parsed = t_start.map(|t| t.elapsed());
                                let responses = apply_shard.dispatch_wire_request_sync(req);
                                let t_dispatched = t_start.map(|t| t.elapsed());
                                for resp in responses {
                                    encode_glue_response_tcp(&resp, &mut client.write_buf);
                                }
                                if let (Some(t0), Some(parsed), Some(dispatched)) =
                                    (t_start, t_parsed, t_dispatched)
                                {
                                    let total = t0.elapsed();
                                    eprintln!(
                                        "TRACE_SRV ns: parse={} dispatch={} encode={} TOTAL={}",
                                        parsed.as_nanos(),
                                        (dispatched - parsed).as_nanos(),
                                        (total - dispatched).as_nanos(),
                                        total.as_nanos()
                                    );
                                }
                            }
                            Ok(None) => break,
                            Err(_) => {
                                // Protocol error — close.
                                use beava_core::wire::{CT_JSON, OP_ERROR_RESPONSE};
                                encode_tcp_frame_bytes(
                                    OP_ERROR_RESPONSE,
                                    CT_JSON,
                                    b"{\"code\":\"frame_error\"}",
                                    &mut client.write_buf,
                                );
                                client.closed = true;
                                break;
                            }
                        }
                    }
                }
                MioProto::Http => {
                    // Parse all complete HTTP/1.1 requests.
                    loop {
                        match parse_http_request(&mut client.read_buf) {
                            Ok(Some((req, _keep_alive))) => {
                                let responses = apply_shard.dispatch_wire_request_sync(req);
                                for resp in responses {
                                    encode_glue_response_http(&resp, &mut client.write_buf);
                                }
                            }
                            Ok(None) => break,
                            Err(_) => {
                                let err = b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n";
                                client.write_buf.extend_from_slice(err);
                                client.closed = true;
                                break;
                            }
                        }
                    }
                    // Re-register for both read and write if we have data to send.
                    if !client.write_buf.is_empty() {
                        let _ = event_loop.reregister(
                            &mut client.stream,
                            client.token,
                            mio::Interest::READABLE | mio::Interest::WRITABLE,
                        );
                    }
                }
            }

            // For TCP: also re-register for write if there are pending responses.
            if client.proto == MioProto::Tcp && !client.write_buf.is_empty() {
                let _ = event_loop.reregister(
                    &mut client.stream,
                    client.token,
                    mio::Interest::READABLE | mio::Interest::WRITABLE,
                );
            }
        }

        // ── Phase 5: write — flush write buffers ─────────────────────────────
        for (token, _readable, _writable) in &events {
            let tok_idx = token.0;
            if tok_idx < TOKEN_CLIENT_BASE {
                continue;
            }
            if let Some(client) = clients.get_mut(&tok_idx) {
                if client.write_buf.is_empty() {
                    continue;
                }
                loop {
                    if client.write_buf.is_empty() {
                        break;
                    }
                    match client.stream.write(&client.write_buf) {
                        Ok(0) => break,
                        Ok(n) => {
                            let _ = client.write_buf.split_to(n);
                        }
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                        Err(_) => {
                            client.closed = true;
                            break;
                        }
                    }
                }
                // If write buf drained, go back to read-only.
                if client.write_buf.is_empty() && !client.closed {
                    let _ = event_loop.reregister(
                        &mut client.stream,
                        client.token,
                        mio::Interest::READABLE,
                    );
                }
            }
        }

        // Also try to flush write buffers for ALL clients that have pending data
        // (not just those with writable events this tick — catches first-tick writes).
        let client_keys2: Vec<usize> = clients.keys().cloned().collect();
        for tok_idx in client_keys2 {
            if let Some(client) = clients.get_mut(&tok_idx) {
                if client.closed || client.write_buf.is_empty() {
                    continue;
                }
                loop {
                    if client.write_buf.is_empty() {
                        break;
                    }
                    match client.stream.write(&client.write_buf) {
                        Ok(0) => break,
                        Ok(n) => {
                            let _ = client.write_buf.split_to(n);
                        }
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                        Err(_) => {
                            client.closed = true;
                            break;
                        }
                    }
                }
                if client.write_buf.is_empty() && !client.closed {
                    let _ = event_loop.reregister(
                        &mut client.stream,
                        client.token,
                        mio::Interest::READABLE,
                    );
                }
            }
        }

        // ── Phase 6: cleanup closed clients ──────────────────────────────────
        let closed: Vec<usize> = clients
            .iter()
            .filter(|(_, c)| c.closed)
            .map(|(k, _)| *k)
            .collect();
        for tok_idx in closed {
            if let Some(mut client) = clients.remove(&tok_idx) {
                let _ = event_loop.deregister(&mut client.stream);
            }
        }

        // ── Phase 7: check shutdown ───────────────────────────────────────────
        if shutdown.load(AOrdering::Acquire) {
            tracing::info!(target: "beava.server", "apply thread: shutdown signal received, draining");
            break;
        }
    }

    tracing::info!(target: "beava.server", "apply thread: exiting");
}

/// Encode a GlueResponse as a TCP framed response into `buf`.
fn encode_glue_response_tcp(
    resp: &crate::runtime_core_glue::GlueResponse,
    buf: &mut bytes::BytesMut,
) {
    use crate::runtime_core_glue::GlueResponse;
    use beava_core::wire::{CT_JSON, OP_ERROR_RESPONSE, OP_PING, OP_PUSH};

    match resp {
        GlueResponse::Pong { .. } => {
            encode_tcp_frame_bytes(OP_PING, CT_JSON, b"{}", buf);
        }
        GlueResponse::PushAck {
            ack_lsn,
            registry_version,
        } => {
            let body =
                serde_json::json!({"ack_lsn": ack_lsn, "registry_version": registry_version});
            let b = serde_json::to_vec(&body).unwrap_or_default();
            encode_tcp_frame_bytes(OP_PUSH, CT_JSON, &b, buf);
        }
        GlueResponse::PushReplay { registry_version } => {
            let body = serde_json::json!({"replay": true, "registry_version": registry_version});
            let b = serde_json::to_vec(&body).unwrap_or_default();
            encode_tcp_frame_bytes(OP_PUSH, CT_JSON, &b, buf);
        }
        GlueResponse::RegisterOk { version } => {
            let body = serde_json::json!({"version": version});
            let b = serde_json::to_vec(&body).unwrap_or_default();
            encode_tcp_frame_bytes(OP_PUSH, CT_JSON, &b, buf);
        }
        GlueResponse::RegisterError { code, message } => {
            let body = serde_json::json!({"code": code, "message": message});
            let b = serde_json::to_vec(&body).unwrap_or_default();
            encode_tcp_frame_bytes(OP_ERROR_RESPONSE, CT_JSON, &b, buf);
        }
        GlueResponse::PushError { code, .. } => {
            let body = serde_json::json!({"code": code});
            let b = serde_json::to_vec(&body).unwrap_or_default();
            encode_tcp_frame_bytes(OP_ERROR_RESPONSE, CT_JSON, &b, buf);
        }
        _ => {
            encode_tcp_frame_bytes(
                OP_ERROR_RESPONSE,
                CT_JSON,
                b"{\"code\":\"unsupported\"}",
                buf,
            );
        }
    }
}

/// Encode a GlueResponse as a full HTTP/1.1 response into `buf`.
fn encode_glue_response_http(
    resp: &crate::runtime_core_glue::GlueResponse,
    buf: &mut bytes::BytesMut,
) {
    use crate::runtime_core_glue::GlueResponse;

    let (status, body_bytes): (u16, Vec<u8>) = match resp {
        GlueResponse::PushAck {
            ack_lsn,
            registry_version,
        } => {
            let body = serde_json::json!({"ack_lsn": ack_lsn, "registry_version": registry_version, "idempotent_replay": false});
            (200, serde_json::to_vec(&body).unwrap_or_default())
        }
        GlueResponse::PushReplay { registry_version } => {
            let body = serde_json::json!({"idempotent_replay": true, "registry_version": registry_version});
            (200, serde_json::to_vec(&body).unwrap_or_default())
        }
        GlueResponse::RegisterOk { version } => {
            let body = serde_json::json!({"version": version});
            (200, serde_json::to_vec(&body).unwrap_or_default())
        }
        GlueResponse::RegisterError { code, message } => {
            let body = serde_json::json!({"error": {"code": code, "message": message}});
            (400, serde_json::to_vec(&body).unwrap_or_default())
        }
        GlueResponse::PushError { code, .. } => {
            let body = serde_json::json!({"error": {"code": code}});
            let status = if *code == "event_not_found" { 404 } else { 400 };
            (status, serde_json::to_vec(&body).unwrap_or_default())
        }
        GlueResponse::QueryResult { body } => (200, body.to_vec()),
        GlueResponse::QueryNotFound { code } => {
            let body = serde_json::json!({"error": {"code": code}});
            (404, serde_json::to_vec(&body).unwrap_or_default())
        }
        GlueResponse::Pong { registry_version } => {
            let body = serde_json::json!({"pong": true, "registry_version": registry_version});
            (200, serde_json::to_vec(&body).unwrap_or_default())
        }
        GlueResponse::InternalError { reason } => {
            let body = serde_json::json!({"error": {"code": "internal_error", "reason": reason}});
            (500, serde_json::to_vec(&body).unwrap_or_default())
        }
        _ => (501, b"{\"error\":{\"code\":\"unsupported\"}}".to_vec()),
    };

    let status_text = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        _ => "OK",
    };

    let header = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nX-Runtime: hand-rolled\r\nConnection: keep-alive\r\n\r\n",
        status,
        status_text,
        body_bytes.len()
    );
    buf.extend_from_slice(header.as_bytes());
    buf.extend_from_slice(&body_bytes);
}

/// Encode a raw TCP framed response into `buf`.
/// Wire format: [u32 length BE][u16 op BE][u8 content_type][payload]
fn encode_tcp_frame_bytes(op: u16, content_type: u8, payload: &[u8], buf: &mut bytes::BytesMut) {
    use bytes::BufMut;
    // Length field = op(2) + content_type(1) + payload.len()
    let frame_len = 2 + 1 + payload.len() as u32;
    buf.put_u32(frame_len);
    buf.put_u16(op);
    buf.put_u8(content_type);
    buf.extend_from_slice(payload);
}

// ─── TCP frame dispatch helper (Plan 18-05.1, kept for compat) ────────────────

/// Dispatch one decoded wire frame to the apply path and return a response frame.
///
/// Used by the inline TCP accept loop inside `ServerV18::serve()`. Async because
/// `dispatch_wire_request` drives the tokio WAL sink channel; once the WAL is
/// converted to sync `Write` (Plan 18-06) this can become `fn`.
#[allow(dead_code)]
async fn dispatch_tcp_frame(
    app: &Arc<AppState>,
    frame: beava_core::wire::Frame,
) -> beava_core::wire::Frame {
    use crate::runtime_core_glue::dispatch_wire_request;
    use beava_core::wire::{OP_PING, OP_PUSH, OP_REGISTER};
    use beava_runtime_core::wire_request::WireRequest;

    // Map raw frame op+payload → WireRequest.
    let wire_req = match frame.op {
        OP_PING => WireRequest::Ping,
        OP_REGISTER => WireRequest::Register {
            payload: frame.payload,
        },
        OP_PUSH => {
            #[derive(serde::Deserialize)]
            struct PushEnvelope {
                event: String,
                body: serde_json::Value,
            }
            match serde_json::from_slice::<PushEnvelope>(&frame.payload) {
                Ok(env) => {
                    let body = serde_json::to_vec(&env.body)
                        .map(bytes::Bytes::from)
                        .unwrap_or_else(|_| frame.payload.clone());
                    WireRequest::TcpPush {
                        event_name: env.event,
                        body,
                        body_format: beava_core::wire::CT_JSON,
                    }
                }
                Err(e) => WireRequest::ParseError {
                    reason: e.to_string(),
                },
            }
        }
        op => WireRequest::Unknown { op },
    };

    // Dispatch and serialise the GlueResponse back to a Frame.
    let glue = dispatch_wire_request(app, wire_req).await;
    glue_to_frame(glue)
}

/// Convert a `GlueResponse` into a wire `Frame` for the TCP transport.
#[allow(dead_code)]
fn glue_to_frame(glue: crate::runtime_core_glue::GlueResponse) -> beava_core::wire::Frame {
    use crate::runtime_core_glue::GlueResponse;
    use beava_core::wire::{CT_JSON, OP_ERROR_RESPONSE, OP_PING, OP_PUSH};

    match glue {
        GlueResponse::Pong { .. } => {
            beava_core::wire::Frame::new(OP_PING, CT_JSON, bytes::Bytes::from_static(b"{}"))
        }
        GlueResponse::PushAck {
            ack_lsn,
            registry_version,
        } => {
            let body =
                serde_json::json!({"ack_lsn": ack_lsn, "registry_version": registry_version});
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
        _ => beava_core::wire::Frame::new(
            OP_ERROR_RESPONSE,
            CT_JSON,
            bytes::Bytes::from_static(b"{\"code\":\"unsupported\"}"),
        ),
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
