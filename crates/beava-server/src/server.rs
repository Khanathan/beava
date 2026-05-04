//! Server: ServerV18 (mio data-plane runtime) + tokio admin sidecar.
//!
//! Plan 12.6-07: legacy axum `Server` deleted; `ServerV18` is the SOLE
//! data-plane runtime per `project_phase18_no_dual_runtime`. Tokio admin
//! sidecar (in `http_admin.rs`) binds on `cfg.admin_addr` (default 8090).

use crate::idem_cache::IdemCache;
use crate::recovery::{replay_handrolled_wal_dir, replay_wal_from_lsn};
use crate::snapshot_task::{spawn_snapshot_task, SnapshotTaskConfig, SnapshotTriggerTx};
use crate::AppState;
use beava_core::registry::Registry;
use beava_persistence::{Persistence, SyncMode, WalSink, WalSinkConfig};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Phase 13.4 Plan 07 (D-02 USER-LOCKED) — server boot configuration.
///
/// Carried via [`ServerV18::bind_with_config`]. Plan 13.4-08 (OP_RESET) extends
/// this struct with `test_mode`; Plan 07 introduces it as a vehicle for the
/// `Persistence` selector.
///
/// `Persistence::Memory` boots the server WITHOUT a WAL writer thread,
/// WITHOUT a snapshot writer, and WITHOUT calling recovery — pure in-RAM
/// state per D-02. `Persistence::Disk { .. }` preserves the existing WAL +
/// snapshot + recovery path verbatim.
///
/// Named `ServerV18Config` (rather than `Config`) to avoid collision with
/// the existing `beava_core::config::Config` re-export at the crate root.
#[derive(Debug, Clone, Default)]
pub struct ServerV18Config {
    /// Persistence mode. `Persistence::Disk { .. }` is the production
    /// default (via `Default::default()`); `Persistence::Memory` is opt-in
    /// for embed mode + tests.
    pub persistence: Persistence,
    /// Plan 13.4-08 hook: gate for `OP_RESET` in production. `false` (the
    /// default) blocks reset; `true` permits it. Plan 07 lands the field
    /// shape; Plan 08 wires the actual reset dispatch.
    pub test_mode: bool,
}

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

// Plan 12.6-07: legacy `pub struct Server` + `impl Server` (axum data-plane)
// deleted; ServerV18 (mio) is the SOLE production data-plane runtime per
// `project_phase18_no_dual_runtime`.

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
    /// Plan 12.6-01: pre-built runtime state populated by `bind_with_state`.
    ///
    /// When `Some`, the `serve_with_dirs` path consumes this instead of
    /// re-building AppState/Registry/WalSink/snapshot trigger from scratch.
    /// Used by `TestServer` (which needs `app_state()` / `registry()` /
    /// `snapshot_trigger_handle()` accessors before `serve()` is called).
    /// Plain `bind()` callers leave this `None` — back-compat preserved.
    prebuilt_state: Option<ServerV18State>,
}

/// Plan 12.6-01: state pre-built by `ServerV18::bind_with_state` so that
/// `TestServer` can grab Arc clones (`app_state`, `registry`,
/// `snapshot_trigger`) BEFORE consuming the server with `serve_with_dirs`.
///
/// All fields are extracted by `serve_with_dirs` and consumed during the
/// run loop; if `prebuilt_state` is `Some`, `serve_with_dirs` skips the
/// equivalent construction in its prefix.
struct ServerV18State {
    app_state: Arc<AppState>,
    registry: Arc<Registry>,
    idem_cache: Arc<IdemCache>,
    wal_sink: WalSink,
    legacy_wal_worker: JoinHandle<()>,
    wal_ring: Arc<beava_runtime_core::wal_buffer::WalBufferRing>,
    wal_lsn: Arc<beava_runtime_core::wal_lsn::WalLsn>,
    wal_writer_handle: std::thread::JoinHandle<()>,
    /// Plan 12.6-15: shutdown flag for the WalWriter loop. Set on server
    /// shutdown to trigger the writer's final seal+drain+fsync block.
    wal_writer_shutdown: Arc<std::sync::atomic::AtomicBool>,
    /// Plan 13.4-07 (D-02): None when `Persistence::Memory` (snapshot task
    /// not spawned); `Some((cancel, worker))` for `Persistence::Disk`.
    snapshot_task: Option<(CancellationToken, JoinHandle<()>)>,
    /// Plan 13.4-07 (D-02): always `Some` — for memory mode we still own the
    /// trigger sender side of a never-served channel so callers that
    /// `force_snapshot_now` get a clean ack channel rather than panicking.
    /// In Disk mode this is the real scheduler trigger.
    snapshot_trigger: SnapshotTriggerTx,
    wal_dir: std::path::PathBuf,
    snapshot_dir: std::path::PathBuf,
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
        // Plan 12-09: emit `server.http_bound` log line (parsed by
        // python/tests/conftest.py beava_server fixture + read_bench.py to
        // discover the OS-assigned port when listen_addr=127.0.0.1:0).
        // ServerV18::bind didn't emit this in Plan 12-07; tests that use the
        // env-var override pattern need it.
        tracing::info!(
            target: "beava.server",
            kind = "server.http_bound",
            addr = %http_bound,
            "HTTP server bound (ServerV18)"
        );

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
        // Plan 12-09: emit `server.tcp_bound` log line (parsed by
        // python/tests/conftest.py beava_server fixture).
        tracing::info!(
            target: "beava.server",
            kind = "server.tcp_bound",
            addr = %tcp_bound,
            "TCP wire listener bound (ServerV18)"
        );

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
            prebuilt_state: None,
        })
    }

    /// Plan 13.4-07 (D-02 USER-LOCKED) — extended constructor accepting a
    /// `ServerV18Config` struct (persistence + test_mode). Memory mode boots
    /// the server WITHOUT a WAL writer thread, WITHOUT a snapshot writer,
    /// and WITHOUT calling recovery — pure in-RAM state. Disk mode preserves
    /// the existing WAL + snapshot + recovery path verbatim.
    ///
    /// Like `bind_with_state`, this constructor builds the full runtime
    /// state at bind-time so `TestServer` can grab Arc clones via
    /// `app_state()` / `registry()` / `snapshot_trigger_handle()` BEFORE
    /// `serve_with_dirs` consumes the server.
    ///
    /// `tcp_addr` is `Option<SocketAddr>` to mirror the planned wire-spec
    /// shape (TCP listener can be skipped in HTTP-only embed mode); when
    /// `None`, the TCP listener still binds on `127.0.0.1:0` so the mio
    /// event loop has a uniform listener set, but the bound TCP addr is
    /// not surfaced through any user-visible interface. Plan 13.4-07
    /// keeps the TCP listener mandatory in the implementation; the
    /// `Option` is for API forward-compatibility.
    pub async fn bind_with_config(
        http_addr: std::net::SocketAddr,
        tcp_addr: Option<std::net::SocketAddr>,
        admin_addr: std::net::SocketAddr,
        cfg: ServerV18Config,
    ) -> Result<Self, ServerError> {
        // The mio event loop expects two listeners (HTTP + TCP); when the
        // caller passes `None` for tcp_addr we bind it to a throwaway
        // ephemeral 127.0.0.1:0. Plan 13.4-04 may revisit this once the
        // verb-style HTTP migration is in place.
        let resolved_tcp_addr = tcp_addr.unwrap_or_else(|| "127.0.0.1:0".parse().unwrap());
        let mut sv18 = Self::bind(http_addr, resolved_tcp_addr, admin_addr).await?;
        // Plan 13.4-08 (D-03 USER-LOCKED): boot-time test_mode resolution.
        // Effective test_mode is the OR of `cfg.test_mode` (programmatic
        // path: `Server::new(Config { test_mode: true, .. })` per A-10 in
        // SCRATCH-PLANNER-NOTES) and `BEAVA_TEST_MODE=1` env var (ops/CI
        // path). Either gate alone enables reset. Per D-03 the env-var
        // check is exactly `== "1"` — no `=true`, no truthy-coercion.
        // Boot-time-only resolution prevents runtime escalation: an
        // operator who controls env at boot is already trusted.
        let effective_test_mode = cfg.test_mode
            || std::env::var("BEAVA_TEST_MODE")
                .map(|v| v == "1")
                .unwrap_or(false);
        // 60s default snapshot interval mirrors the production tuning;
        // memory mode ignores it (snapshot task not spawned).
        let state =
            build_runtime_state_with_persistence(cfg.persistence, 60_000, 2, effective_test_mode)
                .await?;
        sv18.prebuilt_state = Some(state);
        Ok(sv18)
    }

    /// Plan 12.6-01: variant of `bind` that ALSO eagerly builds AppState,
    /// Registry, WalSink, hand-rolled WAL ring/writer, and the snapshot
    /// task — so `TestServer` can return Arc clones via `app_state()`,
    /// `registry()`, `snapshot_trigger_handle()` BEFORE `serve_with_dirs`
    /// is called.
    ///
    /// Per `project_phase18_no_dual_runtime` and Phase 12.6 D-01 this is
    /// the canonical bootstrap shape for the test harness.  Production
    /// callers continue to use `bind()` + `serve()`.
    ///
    /// Arguments mirror `serve_with_dirs` (which `serve_with_dirs` will
    /// consume after this method returns) plus snapshot/fsync intervals
    /// that were previously hard-coded inside `serve_with_dirs`.
    pub async fn bind_with_state(
        http_addr: std::net::SocketAddr,
        tcp_addr: std::net::SocketAddr,
        admin_addr: std::net::SocketAddr,
        wal_dir: std::path::PathBuf,
        snapshot_dir: std::path::PathBuf,
        snapshot_interval_ms: u64,
        wal_fsync_interval_ms: u64,
    ) -> Result<Self, ServerError> {
        let mut sv18 = Self::bind(http_addr, tcp_addr, admin_addr).await?;
        let state = build_runtime_state(
            wal_dir,
            snapshot_dir,
            snapshot_interval_ms,
            wal_fsync_interval_ms,
        )
        .await?;
        sv18.prebuilt_state = Some(state);
        Ok(sv18)
    }

    /// Plan 12.6-01: clone of the shared `Arc<AppState>` populated by
    /// `bind_with_state`.  Panics if called on a server bound via
    /// plain `bind()` (no pre-built state).
    pub fn app_state(&self) -> Arc<AppState> {
        Arc::clone(
            &self
                .prebuilt_state
                .as_ref()
                .expect(
                    "ServerV18::app_state requires bind_with_state() — \
                     plain bind() leaves prebuilt_state None",
                )
                .app_state,
        )
    }

    /// Plan 12.6-01: clone of the shared `Arc<Registry>` populated by
    /// `bind_with_state`.  Panics on plain-`bind` servers.
    pub fn registry(&self) -> Arc<Registry> {
        Arc::clone(
            &self
                .prebuilt_state
                .as_ref()
                .expect(
                    "ServerV18::registry requires bind_with_state() — \
                     plain bind() leaves prebuilt_state None",
                )
                .registry,
        )
    }

    /// Plan 12.6-01: clone of the snapshot-trigger channel populated by
    /// `bind_with_state`.  Panics on plain-`bind` servers.
    ///
    /// `TestServer::force_snapshot_now` holds onto the cloned sender so
    /// it remains valid after `serve_with_dirs` consumes the server.
    pub fn snapshot_trigger_handle(&self) -> SnapshotTriggerTx {
        self.prebuilt_state
            .as_ref()
            .expect(
                "ServerV18::snapshot_trigger_handle requires bind_with_state() — \
                 plain bind() leaves prebuilt_state None",
            )
            .snapshot_trigger
            .clone()
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
    ///
    /// Plan 12.6-01: if `bind_with_state` was used to construct `self`, the
    /// pre-built `prebuilt_state` is consumed here in place of building
    /// fresh state.  Plain `bind()` callers retain the original behavior
    /// (state built inside `serve_with_dirs`).
    pub async fn serve_with_dirs<F>(
        self,
        shutdown: F,
        wal_dir: std::path::PathBuf,
        snapshot_dir: std::path::PathBuf,
    ) -> Result<(), ServerError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // Plan 12.6-01: extract pre-built state (from `bind_with_state`) or
        // fall back to building it here. Both paths funnel into the same
        // run-loop body.
        let state = match self.prebuilt_state {
            Some(state) => state,
            None => build_runtime_state(wal_dir, snapshot_dir, 60_000, 2).await?,
        };

        run_serve_loop(
            self.http_listener,
            self.tcp_listener,
            self.admin,
            state,
            shutdown,
        )
        .await
    }

    /// Gracefully shut down the admin server without running serve().
    /// Use this only when serve() was never called (e.g. bind-only tests).
    pub async fn shutdown(self) {
        self.admin.shutdown().await;
    }
}

// ─── Plan 12.6-01: shared runtime-state builder ─────────────────────────────

/// Build the shared AppState/Registry/WalSink/snapshot stack used by both
/// `serve_with_dirs` (legacy in-line path) and `bind_with_state`
/// (TestServer path). Identical construction sequence — extracted so
/// `TestServer` can grab Arc clones BEFORE `serve_with_dirs` consumes the
/// server. See `serve_with_dirs` for line-by-line history.
async fn build_runtime_state(
    wal_dir: std::path::PathBuf,
    snapshot_dir: std::path::PathBuf,
    snapshot_interval_ms: u64,
    wal_fsync_interval_ms: u64,
) -> Result<ServerV18State, ServerError> {
    // Phase 13.5.1 Plan 05 (Rule 2 — missing critical functionality):
    // honor the `BEAVA_TEST_MODE=1` env var in legacy `bind()` /
    // `bind_with_state()` callers (i.e. the production `main.rs`). Per
    // Phase 13.4 D-03 (USER-LOCKED) the env var is documented as one of
    // the two opt-in paths to enable OP_RESET / test-only opcodes; the
    // pre-existing comment ("paired with bind_with_config in main.rs")
    // documented an intent that was never wired — `main.rs` calls
    // `bind()`, so without this resolution the env var was a dead knob in
    // production. The env-var check is exactly `== "1"` (no truthy
    // coercion, matches `bind_with_config` at line 263).
    let effective_test_mode = std::env::var("BEAVA_TEST_MODE")
        .map(|v| v == "1")
        .unwrap_or(false);
    build_runtime_state_with_persistence(
        Persistence::Disk {
            wal_dir,
            snapshot_dir,
            sync_mode: SyncMode::Periodic,
        },
        snapshot_interval_ms,
        wal_fsync_interval_ms,
        effective_test_mode,
    )
    .await
}

/// Plan 13.4-07 (D-02 USER-LOCKED): build runtime state branched on the
/// `Persistence` variant.
///
/// `Persistence::Disk { .. }` runs the original boot path verbatim — recovery
/// from snapshots + WAL tails, real `WalSink::spawn`, real
/// `WalWriter::new(..).spawn()`, real `spawn_snapshot_task`.
///
/// `Persistence::Memory` skips ALL filesystem interaction:
///   - No `recovery::*` calls (state starts fresh).
///   - `WalSink::spawn_no_op()` instead of `WalSink::spawn(WalSinkConfig {..})`.
///   - A no-op WAL writer thread that drains sealed `WalBufferRing` buffers
///     into the free pool without writing or fsync'ing.
///   - The snapshot task is not spawned at all; only its trigger channel is
///     created so `TestServer::force_snapshot_now` has a sender to talk to.
///
/// `wal_fsync_interval_ms` is honored in Disk mode and ignored in Memory mode
/// (Memory mode uses the env-resolved `wal_cfg.tick_ms` for the no-op
/// writer's drain cadence).
async fn build_runtime_state_with_persistence(
    persistence: Persistence,
    snapshot_interval_ms: u64,
    wal_fsync_interval_ms: u64,
    effective_test_mode: bool,
) -> Result<ServerV18State, ServerError> {
    use beava_runtime_core::wal_buffer::WalBufferRing;
    use beava_runtime_core::wal_lsn::WalLsn;
    use beava_runtime_core::wal_writer::WalWriter;

    // ── Build AppState ────────────────────────────────────────────────────
    let idem_cache = Arc::new(IdemCache::new());
    let registry = Arc::new(beava_core::registry::Registry::new());
    let dev_agg = crate::registry_debug::DevAggState::new(registry.clone());

    // Resolve persistence-mode-specific paths. Memory mode uses placeholder
    // paths that are never read or written.
    let (wal_dir, snapshot_dir, sync_mode, is_memory) = match &persistence {
        Persistence::Memory => (
            // Placeholder paths — never touched in memory mode (no WAL
            // recovery, no WalSink spawn against disk, no snapshot task).
            std::path::PathBuf::from("<memory-mode-no-wal>"),
            std::path::PathBuf::from("<memory-mode-no-snapshots>"),
            SyncMode::Periodic,
            true,
        ),
        Persistence::Disk {
            wal_dir,
            snapshot_dir,
            sync_mode,
        } => (wal_dir.clone(), snapshot_dir.clone(), *sync_mode, false),
    };

    // ── Recovery: snapshot install → *.log replay (post-snapshot) → *.wal replay ──
    //
    // Plan 13.4-07 (D-02): in `Persistence::Memory` mode this whole block is
    // skipped. State starts FRESH — no replay, no recovery — per D-02
    // USER-LOCKED ("On process restart: state is gone; clean slate").
    //
    // Plan 12.6-15: install latest *.bvs snapshot BEFORE replaying WAL tails.
    // The legacy Server::bind path already does this; Plan 12.6-01's
    // build_runtime_state extraction copied only the WAL replay paths and
    // skipped the snapshot loader — losing all events between the snapshot
    // and shutdown after restart (phase7_restart_cycle::sc1_snapshot_then_restart
    // expected 1250, observed 1189; 61 missing = events past the snapshot
    // that snapshot-replay would otherwise re-feed via the snapshot's
    // pre-aggregated state tables).
    let initial_start_lsn = if is_memory {
        // Memory mode — no recovery, start at LSN 1.
        tracing::info!(
            target: "beava.recovery",
            kind = "recovery.skipped_memory_mode",
            "Persistence::Memory: recovery skipped (D-02 USER-LOCKED)"
        );
        1
    } else {
        // Step 1: install snapshot if any (registry descriptors + state tables +
        //   next_event_id + max_event_time_ms). Returns snapshot_lsn for WAL gating.
        //
        // Step 2: replay *.log records with `lsn > snapshot_lsn`
        //   (RegistryBump records that landed after the snapshot, plus any
        //    persistence-WAL events from the legacy WalSink path).
        //
        // Step 3: replay *.wal data-plane events (v=2 binary format from apply_shard).
        let snapshot_lsn = crate::recovery::load_snapshot_if_any(&snapshot_dir, &dev_agg)
            .ok()
            .unwrap_or(0);

        let persistence_lsn = if wal_dir.exists() {
            match replay_wal_from_lsn(&wal_dir, snapshot_lsn, &dev_agg) {
                Ok(outcome) => outcome.last_lsn,
                Err(_) => snapshot_lsn,
            }
        } else {
            snapshot_lsn
        };

        // Step 3: replay hand-rolled *.wal data-plane events.
        // lsn_start = persistence_lsn + 1 to keep LSNs monotonic across all paths.
        let handrolled_lsn_start = persistence_lsn + 1;
        let handrolled_outcome =
            replay_handrolled_wal_dir(&wal_dir, handrolled_lsn_start, &dev_agg).unwrap_or_default();
        let initial = handrolled_outcome
            .last_lsn
            .max(persistence_lsn)
            .max(snapshot_lsn)
            + 1;

        tracing::info!(
            target: "beava.recovery",
            kind = "recovery.serve_with_dirs",
            persistence_lsn,
            handrolled_events = handrolled_outcome.replay_event_count,
            initial_start_lsn = initial,
            "serve_with_dirs recovery complete"
        );

        initial
    };

    // Legacy WalSink: still used for /register cold path (admin endpoint).
    // Data-plane push uses WalBufferRing directly (D-2).
    // initial_start_lsn ensures the new *.log segment doesn't collide with
    // the existing one from the previous server instance.
    //
    // Plan 13.4-07 (D-02): in Memory mode we use `WalSink::spawn_no_op()`
    // which drains append requests with fake LSNs and never touches disk.
    let (wal_sink, legacy_wal_worker) = if is_memory {
        let (sink, worker) = WalSink::spawn_no_op();
        tracing::info!(
            target: "beava.wal",
            kind = "wal.no_op_sink_spawned",
            "Persistence::Memory: WalSink::spawn_no_op (D-02 USER-LOCKED)"
        );
        (sink, worker)
    } else {
        WalSink::spawn(WalSinkConfig {
            dir: wal_dir.clone(),
            initial_start_lsn,
            initial_registry_version: dev_agg.registry.version() as u32,
            fsync_interval_ms: wal_fsync_interval_ms,
            fsync_bytes: 0,
            segment_bytes: 64 * 1024 * 1024,
            sync_mode,
        })
        .map_err(|e| ServerError::WalSpawn(e.to_string()))?
    };

    let mut app_state_inner = AppState::new(dev_agg, wal_sink.clone(), idem_cache.clone());
    // Plan 13.4-08 (D-03 USER-LOCKED): stamp effective_test_mode at boot time.
    // Resolution lives at the bind_with_config / build_runtime_state call
    // site (cfg.test_mode || env::BEAVA_TEST_MODE=="1"); this struct field is
    // read-only after bind so reset cannot be re-enabled at runtime.
    app_state_inner.effective_test_mode = effective_test_mode;
    let app_state = Arc::new(app_state_inner);
    if effective_test_mode {
        tracing::warn!(
            target: "beava.server",
            kind = "server.test_mode_enabled",
            "test_mode is ENABLED: OP_RESET will accept reset requests \
             (D-03 USER-LOCKED). Disable for production deployments."
        );
    }
    // Plan 12.6-07: production data-plane `/registry` is permanently 404. The
    // tokio admin sidecar (cfg.admin_addr; default 127.0.0.1:8090) is the
    // canonical /registry surface. TestServer flips
    // `app_state.dev_endpoints` directly via `.dev_endpoints(true)` builder
    // for white-box tests.

    // ── Hand-rolled WAL stack ────────────────────────────────────────────
    let wal_lsn = Arc::new(WalLsn::new());
    // WAL ring config from env (default 4 × 32 MiB tick=20ms; tunable via
    // BEAVA_WAL_BUFFERS / BEAVA_WAL_BUFFER_SIZE_MB / BEAVA_WAL_TICK_MS).
    // Phase 18 invariants UNCHANGED: lock-free apply, single writer+fsync,
    // O_APPEND, four-watermark discipline. Only buffer count, size, and
    // tick interval are tuned (Phase 19.1-03 D-01..D-04).
    let wal_cfg = crate::wal_config::WalConfig::resolve_from_env();
    tracing::info!(
        target: "beava.wal",
        kind = "wal.config.resolved",
        buffers = wal_cfg.buffers,
        buffer_size_mb = wal_cfg.buffer_size_mb,
        tick_ms = wal_cfg.tick_ms,
        "WAL config resolved (env-tunable: BEAVA_WAL_BUFFERS default=4 range [2,32] / \
         BEAVA_WAL_BUFFER_SIZE_MB default=32 range [4,256] / BEAVA_WAL_TICK_MS default=20 range [1,1000])"
    );
    let buf_bytes = wal_cfg.buffer_size_mb * 1024 * 1024;
    let wal_ring = Arc::new(WalBufferRing::new(
        wal_cfg.buffers,
        buf_bytes,
        Arc::clone(&wal_lsn),
    ));

    // WAL writer thread: in Disk mode drains sealed buffers, calls
    // write() + fsync(); in Memory mode (D-02) drains sealed buffers
    // back to the free pool with NO file I/O so the apply hot path
    // doesn't backpressure-block once buffers fill.
    let (wal_writer_shutdown, wal_writer_handle) = if is_memory {
        spawn_no_op_wal_writer(Arc::clone(&wal_ring), Arc::clone(&wal_lsn), wal_cfg.tick_ms)
    } else {
        let wal_writer = WalWriter::new(
            &wal_dir,
            Arc::clone(&wal_ring),
            Arc::clone(&wal_lsn),
            wal_cfg.tick_ms,
        )
        .map_err(|e| ServerError::WalSpawn(e.to_string()))?;
        // Plan 12.6-15: capture WalWriter's shutdown flag BEFORE spawn() consumes
        // self. Without this we couldn't actually trigger the writer's
        // final-drain-and-fsync block at server shutdown — the writer loop
        // would just keep ticking (sleep → seal → drain → check shutdown
        // (always false) → loop) and the JoinHandle drop would detach the
        // thread mid-tick, losing any active-buffer contents that hadn't
        // been sealed yet. Surfaced by phase7_restart_cycle::sc1 (1216 of
        // 1250 expected post-restart events; 34 lost in the active buffer).
        let shutdown = wal_writer.shutdown_flag();
        let handle = wal_writer.spawn();
        (shutdown, handle)
    };

    // ── Spawn snapshot task (admin plane) ────────────────────────────────
    //
    // Plan 13.4-07 (D-02 USER-LOCKED): in Memory mode the snapshot task is
    // not spawned at all — saves a tokio task and ensures zero file I/O. We
    // still create a (sender, receiver) channel pair so callers that hold a
    // `snapshot_trigger` clone (e.g. `TestServer::force_snapshot_now`) get
    // an ack-channel-closed error rather than panicking on a missing handle.
    let (snapshot_task, snapshot_trigger) = if is_memory {
        let (trigger_tx, _trigger_rx) =
            tokio::sync::mpsc::channel::<tokio::sync::oneshot::Sender<Result<(), String>>>(8);
        // _trigger_rx is dropped immediately; manual-trigger sends will fail
        // closed-channel — caller surface returns a structured error
        // ("snapshot task channel closed") rather than performing I/O.
        tracing::info!(
            target: "beava.snapshot",
            kind = "snapshot.task_skipped_memory_mode",
            "Persistence::Memory: snapshot task not spawned (D-02 USER-LOCKED)"
        );
        (None, trigger_tx)
    } else {
        let snapshot_cancel = CancellationToken::new();
        let (snapshot_worker, snapshot_trigger) = spawn_snapshot_task(
            SnapshotTaskConfig {
                interval: Duration::from_millis(snapshot_interval_ms.max(1)),
                snapshot_dir: snapshot_dir.clone(),
                retain: 2,
            },
            Arc::clone(&app_state),
            wal_sink.clone(),
            snapshot_cancel.clone(),
        );
        (Some((snapshot_cancel, snapshot_worker)), snapshot_trigger)
    };

    Ok(ServerV18State {
        app_state,
        registry,
        idem_cache,
        wal_sink,
        legacy_wal_worker,
        wal_ring,
        wal_lsn,
        wal_writer_handle,
        wal_writer_shutdown,
        snapshot_task,
        snapshot_trigger,
        wal_dir,
        snapshot_dir,
    })
}

/// Plan 13.4-07 (D-02 USER-LOCKED) — no-op WAL writer thread for memory mode.
///
/// Mirrors the structure of `beava_runtime_core::wal_writer::run_writer_loop`
/// (sleep → seal_active → drain sealed buffers → check shutdown) but with
/// the file `write()` and `fsync()` calls REPLACED by direct `return_to_free`
/// — buffers are recycled without any disk I/O, and the four-watermark LSN
/// state is advanced (`mark_written` + `mark_synced`) so any waiters that
/// depend on the durable watermark unblock immediately.
///
/// Returns `(shutdown_flag, join_handle)` matching the shape of
/// `WalWriter::shutdown_flag()` + `WalWriter::spawn()`.
fn spawn_no_op_wal_writer(
    ring: Arc<beava_runtime_core::wal_buffer::WalBufferRing>,
    lsn: Arc<beava_runtime_core::wal_lsn::WalLsn>,
    tick_ms: u64,
) -> (
    Arc<std::sync::atomic::AtomicBool>,
    std::thread::JoinHandle<()>,
) {
    use std::sync::atomic::{AtomicBool, Ordering as AOrdering};

    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_thread = Arc::clone(&shutdown);
    let tick = Duration::from_millis(tick_ms.max(1));

    let handle = std::thread::Builder::new()
        .name("beava-wal-writer-noop".to_owned())
        .spawn(move || loop {
            std::thread::sleep(tick);

            // Force-seal the active buffer so the apply thread doesn't
            // accumulate beyond `tick_ms` worth of data.
            ring.seal_active();

            // Drain sealed buffers without writing to disk. Advance the
            // written + synced watermarks so PerEvent-style waiters
            // (none in memory mode, but the contract still holds) don't
            // stall on a never-incrementing durable LSN.
            while let Some(buf) = ring.pop_sealed() {
                let hi = buf.lsn_hi();
                lsn.mark_written(hi);
                lsn.mark_synced(hi);
                ring.return_to_free(buf);
            }

            if shutdown_thread.load(AOrdering::Acquire) {
                // Final drain on shutdown.
                ring.seal_active();
                while let Some(buf) = ring.pop_sealed() {
                    let hi = buf.lsn_hi();
                    lsn.mark_written(hi);
                    lsn.mark_synced(hi);
                    ring.return_to_free(buf);
                }
                break;
            }
        })
        .expect("failed to spawn no-op WAL writer thread");

    (shutdown, handle)
}

/// Run the apply thread + admin server until `shutdown` resolves, then
/// drain everything cleanly.  Extracted from the original
/// `serve_with_dirs` body so it can be invoked from both the legacy
/// build path and the `bind_with_state` path (Plan 12.6-01).
async fn run_serve_loop<F>(
    http_listener: std::net::TcpListener,
    tcp_listener: std::net::TcpListener,
    admin: crate::http_admin::BoundAdminServer,
    state: ServerV18State,
    shutdown: F,
) -> Result<(), ServerError>
where
    F: Future<Output = ()> + Send + 'static,
{
    use crate::apply_shard::ApplyShard;
    use std::sync::atomic::{AtomicBool, Ordering as AOrdering};

    let ServerV18State {
        app_state,
        registry: _registry,
        idem_cache: _idem_cache,
        wal_sink,
        legacy_wal_worker,
        wal_ring,
        wal_lsn,
        wal_writer_handle,
        wal_writer_shutdown,
        snapshot_task,
        snapshot_trigger,
        wal_dir: _wal_dir,
        snapshot_dir: _snapshot_dir,
    } = state;

    // Snapshot trigger lifecycle: drop our copy.  Any clones held externally
    // (`TestServer`) keep the channel sender count > 0 until they're
    // dropped or used. Without external clones the channel becomes
    // unreachable, which is fine — the snapshot task itself owns the
    // receiver and continues servicing scheduled ticks.
    drop(snapshot_trigger);

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

    // ── Spawn apply thread (mio EventLoop) ───────────────────────────────
    let apply_join = std::thread::Builder::new()
        .name("beava-apply".to_owned())
        .spawn(move || {
            run_mio_event_loop(
                http_listener,
                tcp_listener,
                apply_shard,
                shutdown_flag_apply,
            );
        })
        .map_err(ServerError::Serve)?;

    // Wait for the external shutdown future, then signal the apply thread.
    shutdown.await;
    shutdown_flag_signal.store(true, AOrdering::Release);

    // Wait for the apply thread to drain.
    let _ = apply_join.join();

    // Plan 12.6-15: signal the WalWriter to seal+drain+fsync the active
    // buffer (which may hold ~tick_ms worth of post-snapshot pushes that
    // would otherwise be lost when JoinHandle is dropped) and JOIN the
    // thread so we wait for the final fsync to land BEFORE we let the
    // WAL ring/lsn Arcs drop. Surfaced by phase7_restart_cycle::sc1
    // (~34 events / ~1 tick of pushes lost per restart pre-fix).
    wal_writer_shutdown.store(true, AOrdering::Release);
    let _ = wal_writer_handle.join();

    // Stop snapshot task (Disk mode only — memory mode never spawned one).
    if let Some((snapshot_cancel, snapshot_worker)) = snapshot_task {
        snapshot_cancel.cancel();
        let _ = snapshot_worker.await;
    }

    // Drain legacy WalSink (used only for /register cold path).
    let _ = app_state.wal_sink.clone().shutdown().await;
    let _ = legacy_wal_worker.await;
    // Make sure wal_sink (the explicit clone we held) is dropped before
    // returning so the channel sender count drops to zero.
    drop(wal_sink);

    // Stop admin server.
    admin.shutdown().await;

    Ok(())
}

// ─── Plan 18-04.6: real mio EventLoop driver ─────────────────────────────────

/// Token assignments for the mio event loop.
const TOKEN_HTTP_LISTENER: mio::Token = mio::Token(0);
const TOKEN_TCP_LISTENER: mio::Token = mio::Token(1);
/// Plan 18-06 follow-up: token used by `mio::Waker` registered with the
/// apply thread's listener `EventLoop`. Workers fire this waker after they
/// push `RingItem`s to `read_rx` so apply doesn't sleep in `tick(timeout)`
/// while there's already work waiting in the channel.
const TOKEN_APPLY_WAKER: mio::Token = mio::Token(usize::MAX);
/// Client connections start at token 2; new ones increment this counter.
///
/// **Dead post-Plan 18-05/18-06**: clients are owned by per-worker IoBackends
/// now. Kept for the legacy IoPool helpers below (still compiled but never
/// invoked at runtime).
#[allow(dead_code)]
const TOKEN_CLIENT_BASE: usize = 2;

/// Maximum concurrent clients supported by the legacy per-tick IoPool lifecycle.
///
/// **Dead post-Plan 18-05/18-06**: see TOKEN_CLIENT_BASE.
#[allow(dead_code)]
const MAX_CONCURRENT_CLIENTS: usize = 8192;

/// Per-client connection state for the mio event loop.
///
/// Plan 18-04.7 D-1: parsed_requests and partially-decoded responses live
/// inside the slot so IoPool workers can populate them while the apply
/// thread waits on `IoPool::join_all()`. The apply thread reads them
/// after the join Acquire barrier.
#[allow(dead_code)]
struct MioClient {
    stream: mio::net::TcpStream,
    token: mio::Token,
    /// Protocol: HTTP or TCP framed wire.
    proto: MioProto,
    /// Inbound read buffer.
    read_buf: bytes::BytesMut,
    /// Plan 18-04.7: queue of responses produced by the apply phase, waiting
    /// for the write-phase IoPool worker to serialize into write_buf.
    ///
    /// Plan 18-13: populated directly by the apply thread's drain loop
    /// (`drain_channel_until_workers_done`) as it consumes RingItems from the
    /// crossbeam channel. The prior `parsed_requests` + `parsed_rows` Vec
    /// fields were removed in Plan 18-13 — the channel carries those payloads
    /// per-event instead of accumulating them per-client per-tick.
    output_queue: std::collections::VecDeque<crate::runtime_core_glue::GlueResponse>,
    /// Serialized response bytes waiting to be written to the socket.
    /// Populated by the write-phase IoPool worker (off-apply).
    write_buf: bytes::BytesMut,
    /// Currently-registered mio interest. Tracked so we only reregister
    /// when the desired interest changes (avoids per-tick reregister syscall).
    /// `true` = WRITABLE bit currently set; `false` = READABLE-only.
    interest_writable: bool,
    /// True when the client has been closed / should be removed.
    closed: bool,
}

#[allow(dead_code)]
#[derive(Clone, Copy, PartialEq)]
enum MioProto {
    Http,
    Tcp,
}

// ─── Plan 18-04.7 IoPool observer (test instrumentation) ─────────────────────

/// Observability hooks used by `tests/phase18_04_7_iopool_test.rs` to verify
/// the apply-thread invariant: parse and encode MUST run on IoPool worker
/// threads, never on the apply thread.
///
/// In production the counters are essentially free (single AtomicUsize bump
/// per parse / encode call). Tests reset them before each run and assert
/// that `apply_*_count()` stays at 0 while `off_apply_*_count()` grows.
pub mod iopool_observer {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    /// Apply-thread id, set by `serve_with_dirs` before the IoPool spins up.
    /// Workers compare `std::thread::current().id() == APPLY_TID` to decide
    /// which counter pair to bump.
    ///
    /// Stored as a `Mutex<Option<ThreadId>>` because ThreadId is Copy but
    /// not representable as a plain AtomicUsize across the kernel ABI.
    pub(crate) static APPLY_TID: Mutex<Option<std::thread::ThreadId>> = Mutex::new(None);

    static APPLY_PARSE_COUNT: AtomicUsize = AtomicUsize::new(0);
    static APPLY_ENCODE_COUNT: AtomicUsize = AtomicUsize::new(0);
    static OFF_APPLY_PARSE_COUNT: AtomicUsize = AtomicUsize::new(0);
    static OFF_APPLY_ENCODE_COUNT: AtomicUsize = AtomicUsize::new(0);

    /// Reset all counters to 0 and clear the apply thread id.
    /// Called by tests before each scenario.
    pub fn reset() {
        APPLY_PARSE_COUNT.store(0, Ordering::Release);
        APPLY_ENCODE_COUNT.store(0, Ordering::Release);
        OFF_APPLY_PARSE_COUNT.store(0, Ordering::Release);
        OFF_APPLY_ENCODE_COUNT.store(0, Ordering::Release);
        *APPLY_TID.lock().unwrap() = None;
    }

    /// Record the apply thread's id. Called once by `run_mio_event_loop`
    /// at startup. Subsequent observers compare against this id.
    pub(crate) fn set_apply_tid() {
        *APPLY_TID.lock().unwrap() = Some(std::thread::current().id());
    }

    /// Bump the appropriate parse counter based on whether we're on the
    /// apply thread. Called from inside the parse helper.
    ///
    /// Plan 18-05/18-06: parse now runs inside per-worker IoBackend threads
    /// in `beava-runtime-core`, which can't reach back into this module.
    /// Kept for potential re-instrumentation; tests query the counter via
    /// `parse_calls()` and may currently see 0.
    #[allow(dead_code)]
    pub(crate) fn record_parse() {
        if is_apply_thread() {
            APPLY_PARSE_COUNT.fetch_add(1, Ordering::AcqRel);
        } else {
            OFF_APPLY_PARSE_COUNT.fetch_add(1, Ordering::AcqRel);
        }
    }

    /// Bump the appropriate encode counter based on whether we're on the
    /// apply thread.
    pub(crate) fn record_encode() {
        if is_apply_thread() {
            APPLY_ENCODE_COUNT.fetch_add(1, Ordering::AcqRel);
        } else {
            OFF_APPLY_ENCODE_COUNT.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn is_apply_thread() -> bool {
        let me = std::thread::current().id();
        match *APPLY_TID.lock().unwrap() {
            Some(tid) => tid == me,
            None => false,
        }
    }

    /// Number of parse calls made by the apply thread. MUST be 0 in healthy
    /// IoPool wiring (Plan 18-04.7 invariant 4.7.2).
    pub fn apply_parse_count() -> usize {
        APPLY_PARSE_COUNT.load(Ordering::Acquire)
    }

    /// Number of encode calls made by the apply thread. MUST be 0 in
    /// healthy IoPool wiring.
    pub fn apply_encode_count() -> usize {
        APPLY_ENCODE_COUNT.load(Ordering::Acquire)
    }

    /// Number of parse calls made by IoPool worker threads.
    pub fn off_apply_parse_count() -> usize {
        OFF_APPLY_PARSE_COUNT.load(Ordering::Acquire)
    }

    /// Number of encode calls made by IoPool worker threads.
    pub fn off_apply_encode_count() -> usize {
        OFF_APPLY_ENCODE_COUNT.load(Ordering::Acquire)
    }
}

// ─── Plan 12-08 (D-A) — apply-thread busy-poll instrumentation ───────────────

/// Plan 12-08: cumulative count of `read_rx.recv_timeout(50µs)` calls made
/// by the apply thread when spin-K=10_000 elapses without seeing work.
/// Test hook only — production code never reads this.
static APPLY_RECV_TIMEOUT_CALLS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Plan 12-08: max drain count observed in a single apply-loop iteration
/// (fetch_max'd on every drain; never reset). Test hook for D-D verification.
static APPLY_MAX_DRAIN_PER_ITER: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Plan 12-08: pthread_t of the apply thread, set by `run_mio_event_loop` at
/// startup. Test hook for per-thread CPU accounting (`mach_thread_basic_info`
/// on macOS; `pthread_getcpuclockid` on Linux). Stored as `usize` because
/// `pthread_t` is opaque + Send-unfriendly across an `AtomicUsize`.
static APPLY_PTHREAD_ID: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Plan 12-08 (D-B): cumulative count of `flush_response_batch` calls that
/// flushed a non-empty batch. Test hook only.
static APPLY_RESPONSE_BATCH_FLUSHES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Plan 12-08 test hook. Cumulative count of apply-thread idle fall-through
/// calls into `read_rx.recv_timeout(50µs)` since process start.
#[doc(hidden)]
pub fn apply_recv_timeout_calls() -> u64 {
    APPLY_RECV_TIMEOUT_CALLS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Plan 12-08 test hook. Max number of `RingItem`s drained in a single
/// apply-loop iteration since process start. Used by Wave 2 tests to verify
/// the DRAIN_CAP=1024 cap was removed (drain-until-empty).
#[doc(hidden)]
pub fn apply_max_drain_per_iter() -> u64 {
    APPLY_MAX_DRAIN_PER_ITER.load(std::sync::atomic::Ordering::Relaxed)
}

/// Plan 12-08 (D-B) test hook. Cumulative count of response-batch flushes
/// (a flush fires when the batch reaches BATCH_SIZE_FLUSH=16 OR
/// BATCH_TIME_FLUSH=100µs elapses). Used by Wave 3 tests to verify D-B has
/// landed.
#[doc(hidden)]
pub fn response_batch_flushes() -> u64 {
    APPLY_RESPONSE_BATCH_FLUSHES.load(std::sync::atomic::Ordering::Relaxed)
}

/// Plan 12-08 test hook. Returns the apply thread's pthread_t (as a libc
/// pthread_t value) once the apply thread has booted, or `None` if the apply
/// thread has not yet registered. Used by tests to call
/// `mach_thread_basic_info` / `pthread_getcpuclockid` for per-thread CPU
/// accounting.
#[doc(hidden)]
pub fn apply_pthread_id() -> Option<libc::pthread_t> {
    let raw = APPLY_PTHREAD_ID.load(std::sync::atomic::Ordering::Acquire);
    if raw == 0 {
        None
    } else {
        // SAFETY: we stored a pthread_t (opaque pointer-sized handle) earlier;
        // pthread_t is layout-compatible with usize on linux+macos. We never
        // dereference the value — we only hand it back to libc functions.
        Some(raw as libc::pthread_t)
    }
}

/// Resolve the IoPool thread count from the BEAVA_IO_THREADS env var.
///
/// Default = `max(2, available_parallelism / 4)` — Redis-style ratio that
/// keeps IoPool threads conservative (they spin briefly between ticks and
/// don't burn full cores).
fn default_io_threads() -> usize {
    if let Ok(s) = std::env::var("BEAVA_IO_THREADS") {
        if let Ok(n) = s.parse::<usize>() {
            return n.max(1);
        }
    }
    let p = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    std::cmp::max(2, p / 4)
}

/// Run the mio event loop on a dedicated std::thread (Plan 18-04.6 D-4 +
/// Plan 18-04.7 IoPool integration).
///
/// Per-tick lifecycle (Plan 18-04.7 D-1):
///   1. `EventLoop::tick` — poll mio for ready events (up to 5ms timeout)
///   2. Accept new connections; classify ready clients into read/write sets.
///   3. **Read phase** — `IoPool::publish` parse work items → `join_all`.
///      IoPool workers do `socket.read` + `parse_*_request` on their threads.
///   4. **Apply phase** — single-threaded on this thread. Drain each client's
///      `parsed_requests` → `apply_shard.dispatch_wire_request_sync` → push
///      `GlueResponse`s into the client's `output_queue`.
///   5. **Write phase** — `IoPool::publish` write work items → `join_all`.
///      IoPool workers do `serialize` + `socket.write` on their threads.
///   6. Cleanup closed clients; check shutdown flag.
///
/// `clients: Vec<Option<MioClient>>` is pre-allocated to MAX_CONCURRENT_CLIENTS
/// at startup and never resized — IoPool worker threads hold raw pointers
/// (`as_mut_ptr().add(idx)`) into the Vec for the duration of each
/// publish + join_all cycle. The two phases are strictly serialized by
/// `IoPool::join_all()` Acquire barriers, so the apply thread never touches
/// the same client a worker is touching.
/// Plan 12-08: per-iteration response batch entry.
///
/// `(worker_index, slot_idx, encoder)` — the worker is selected by the apply
/// thread (`slot_idx % n_workers`), and the encoder runs on the IO worker
/// thread once the batch is flushed.
type ResponseBatchEntry = (
    usize,
    u64,
    beava_runtime_core::io_thread_worker::WriteEncoder,
);

/// Plan 12-08 (D-A + D-B): dispatch a single `RingItem` from the apply-thread.
///
/// Extracted into a free helper so both the top-of-loop `try_recv()` drain AND
/// the idle-backoff `recv_timeout(50µs)` Ok arm share the same dispatch shape.
///
/// Wave 3 (D-B): pushes responses into a per-iteration `response_batch`
/// instead of immediately calling `write_txs[w].send`. The caller flushes
/// the batch when it reaches BATCH_SIZE_FLUSH=16 OR BATCH_TIME_FLUSH=100µs
/// elapsed. `batch_started_at` is set on the FIRST push into an empty batch
/// so the timer starts from "first response of this batch", not "last drain
/// pass".
///
/// Wave 4 (D-C): the encoder closure now takes a `&BytesMutPool` from the
/// worker side; it acquires a pool buffer, encodes the framed response into
/// it, extends `client_write_buf` from the pool buffer (reuses the
/// allocation), then releases the pool buffer back.
#[inline]
fn dispatch_one_ring_item(
    item: beava_runtime_core::work_ring::RingItem,
    apply_shard: &crate::apply_shard::ApplyShard,
    n_workers: usize,
    response_batch: &mut smallvec::SmallVec<[ResponseBatchEntry; 16]>,
    batch_started_at: &mut Option<Instant>,
) {
    use beava_runtime_core::io_thread_worker::{WorkerProto, WriteEncoder};
    use beava_runtime_core::work_ring::{ParseErrorKind, RingItem};
    let _ = apply_shard; // referenced via match arm below; silence linter on early returns
    match item {
        RingItem::Request {
            slot_idx,
            keep_alive: _,
            request,
            parsed_row,
        } => {
            let responses = apply_shard.dispatch_wire_request_with_row(request, parsed_row);
            let slot_u64 = slot_idx as u64;
            let w = (slot_u64 as usize) % n_workers;
            for resp in responses {
                let encoder: WriteEncoder = Box::new(move |proto, pool, client_buf| {
                    let mut tmp = pool.acquire();
                    match proto {
                        WorkerProto::Tcp => encode_glue_response_tcp(&resp, &mut tmp),
                        WorkerProto::Http => encode_glue_response_http(&resp, &mut tmp),
                    }
                    client_buf.extend_from_slice(&tmp);
                    pool.release(tmp);
                });
                if batch_started_at.is_none() {
                    *batch_started_at = Some(Instant::now());
                }
                response_batch.push((w, slot_u64, encoder));
            }
        }
        RingItem::ParseError { slot_idx, kind } => {
            use crate::runtime_core_glue::GlueResponse;
            let slot_u64 = slot_idx as u64;
            let w = (slot_u64 as usize) % n_workers;
            let resp = match kind {
                // Plan 12.6-15: oversize frames get the rich `frame_too_large`
                // error frame with `limit` field (criterion 7).
                ParseErrorKind::TcpFrameTooLarge { declared, limit } => GlueResponse::TcpError {
                    code: "frame_too_large",
                    message: format!("declared frame length {declared} exceeds limit {limit}",),
                    extras: serde_json::json!({"limit": limit, "declared": declared}),
                },
                ParseErrorKind::TcpFrame => GlueResponse::PushError {
                    code: "frame_error",
                    registry_version: 0,
                },
                ParseErrorKind::HttpProtocol => GlueResponse::PushError {
                    code: "http_protocol_error",
                    registry_version: 0,
                },
            };
            let encoder: WriteEncoder = Box::new(move |proto, pool, client_buf| {
                let mut tmp = pool.acquire();
                match proto {
                    WorkerProto::Tcp => encode_glue_response_tcp(&resp, &mut tmp),
                    WorkerProto::Http => encode_glue_response_http(&resp, &mut tmp),
                }
                client_buf.extend_from_slice(&tmp);
                pool.release(tmp);
            });
            if batch_started_at.is_none() {
                *batch_started_at = Some(Instant::now());
            }
            response_batch.push((w, slot_u64, encoder));
        }
    }
}

/// Plan 12-08 (D-B): flush the per-iteration response batch.
///
/// Groups by worker index `w`, sends each worker's slice via
/// `WriteRingExt::send_batch` (one channel.send per item; the amortization
/// is in firing the worker waker ONCE per worker per flush instead of once
/// per response). Resets `batch_started_at` to `None`.
///
/// Returns the total number of items flushed.
#[inline]
fn flush_response_batch(
    response_batch: &mut smallvec::SmallVec<[ResponseBatchEntry; 16]>,
    batch_started_at: &mut Option<Instant>,
    write_txs: &[crossbeam_channel::Sender<(
        u64,
        beava_runtime_core::io_thread_worker::WriteEncoder,
    )>],
    worker_wakers: &[Arc<dyn beava_runtime_core::io_backend::WakerHandle>],
    n_workers: usize,
) -> usize {
    use beava_runtime_core::io_thread_worker::WriteEncoder;
    use beava_runtime_core::work_ring::WriteRingExt;
    if response_batch.is_empty() {
        return 0;
    }
    let total = response_batch.len();
    // Group items by worker index. n_workers is small (≤ 32 by
    // default_io_threads()); fixed-size stack array beats a HashMap.
    let mut per_worker: smallvec::SmallVec<[Vec<(u64, WriteEncoder)>; 32]> =
        smallvec::SmallVec::with_capacity(n_workers);
    for _ in 0..n_workers {
        per_worker.push(Vec::new());
    }
    for (w, slot, enc) in response_batch.drain(..) {
        per_worker[w].push((slot, enc));
    }
    let mut affected_workers: u32 = 0;
    for (w, items) in per_worker.iter_mut().enumerate() {
        if items.is_empty() {
            continue;
        }
        let _ = write_txs[w].send_batch(std::mem::take(items));
        affected_workers |= 1u32 << (w & 31);
    }
    if affected_workers != 0 {
        for (w, waker) in worker_wakers.iter().enumerate() {
            if (affected_workers >> (w & 31)) & 1 == 1 {
                let _ = waker.wake();
            }
        }
    }
    *batch_started_at = None;
    APPLY_RESPONSE_BATCH_FLUSHES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    total
}

fn run_mio_event_loop(
    http_listener_std: std::net::TcpListener,
    tcp_listener_std: std::net::TcpListener,
    apply_shard: crate::apply_shard::ApplyShard,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
) {
    // Plan 18-05/18-06 wiring: replaces the prior IoPool + per-tick join_all
    // architecture with the per-worker continuous-loop (Valkey 8) model.
    // Each worker owns its own MioBackend (its own mio::Poll + Waker + a
    // disjoint subset of clients keyed by `slot_idx % n_workers`). Apply
    // thread now owns ONLY the two listeners and the dispatch path:
    //   - polls the listeners (HTTP + TCP) on its own EventLoop
    //   - drains a shared MPSC `read_rx` (workers parse + send RingItems)
    //   - dispatches via apply_shard, encodes responses, sends bytes back
    //     to the owning worker via `write_tx[w]` and wakes the worker
    //   - on accept, hands the new client to a worker via `new_client_tx[w]`
    // No `IoPool::join_all` anywhere; reads and writes flow continuously.
    use beava_runtime_core::event_loop::EventLoop;
    use beava_runtime_core::http_listener::HttpListener;
    use beava_runtime_core::io_backend::MioBackend;
    use beava_runtime_core::io_thread_worker::{
        start_worker, NewClient, WorkerConfig, WorkerHandle, WorkerProto,
    };
    use beava_runtime_core::tcp_listener::TcpListener as MioTcpListener;
    use beava_runtime_core::work_ring::RingItem;
    use std::sync::atomic::Ordering as AOrdering;

    // Plan 18-04.7: record this thread as the apply thread. Test instrumentation
    // (iopool_observer) compares parse/encode call sites against this id.
    iopool_observer::set_apply_tid();

    // Plan 12-08 (D-A): record the apply thread's pthread_t for per-thread CPU
    // accounting. macOS uses `mach_thread_basic_info` via pthread_mach_thread_np;
    // Linux uses `pthread_getcpuclockid`. Process-level rusage is unreliable
    // here because it sums across apply + N IO workers + admin tokio workers.
    {
        let pid: libc::pthread_t = unsafe { libc::pthread_self() };
        APPLY_PTHREAD_ID.store(pid as usize, std::sync::atomic::Ordering::Release);
    }

    // ── Apply-thread EventLoop: just the two listeners + the worker waker ────
    // (Created BEFORE workers so we can register the apply-side waker with this
    // event_loop's mio::Registry and clone it into each worker's config.)
    let mut event_loop = match EventLoop::new() {
        Ok(el) => el,
        Err(e) => {
            tracing::error!("apply thread: EventLoop::new failed: {e}");
            return;
        }
    };

    // Plan 18-06 follow-up: mio::Waker bound to apply's listener event_loop.
    // Workers fire this after pushing RingItems to read_rx so apply doesn't
    // sit in event_loop.tick(timeout) while the channel has work waiting.
    let apply_waker = match mio::Waker::new(event_loop.registry(), TOKEN_APPLY_WAKER) {
        Ok(w) => Arc::new(w),
        Err(e) => {
            tracing::error!("apply thread: mio::Waker::new failed: {e}");
            return;
        }
    };

    // ── Spin up N per-worker continuous loops (Plan 18-05) ───────────────────
    let n_workers = default_io_threads();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Single MPSC for parsed RingItems: every worker clones the sender; apply
    // owns the receiver. Capacity 16384 = old IoPool budget × 4× headroom.
    let (read_tx, read_rx) = crossbeam_channel::bounded::<RingItem>(16_384);

    // Per-worker write_tx (apply → worker, encoder closures), and the
    // corresponding worker handles. Wakers cached in a parallel Vec so the hot
    // dispatch loop doesn't re-Arc-clone per send.
    use beava_runtime_core::io_thread_worker::WriteEncoder;
    let mut write_txs: Vec<crossbeam_channel::Sender<(u64, WriteEncoder)>> =
        Vec::with_capacity(n_workers);
    let mut workers: Vec<WorkerHandle> = Vec::with_capacity(n_workers);
    let mut worker_wakers: Vec<Arc<dyn beava_runtime_core::io_backend::WakerHandle>> =
        Vec::with_capacity(n_workers);

    for w in 0..n_workers {
        let (write_tx, write_rx) = crossbeam_channel::bounded::<(u64, WriteEncoder)>(4_096);
        let (new_client_tx, new_client_rx) = crossbeam_channel::bounded::<NewClient>(256);

        let cfg = WorkerConfig {
            worker_id: w,
            n_workers,
            read_tx: read_tx.clone(),
            write_rx,
            new_client_rx,
            stop: Arc::clone(&stop),
            apply_waker: Some(Arc::clone(&apply_waker)),
        };
        let handle = start_worker::<MioBackend>(cfg, new_client_tx, write_tx.clone());
        worker_wakers.push(handle.waker());
        workers.push(handle);
        write_txs.push(write_tx);
    }
    // Apply only reads from read_rx; drop our spare sender clone so the
    // channel can disconnect cleanly when all workers exit.
    drop(read_tx);

    tracing::info!(
        target: "beava.server",
        kind = "workers.started",
        threads = n_workers,
        "Plan 18-05 per-worker continuous-loop pool started"
    );
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

    // Per-slot proto, used at response-encode time. Workers track their own
    // WorkerClient.proto independently; this map is the apply-side mirror so
    // we know whether to encode TCP framed or HTTP responses for each slot.
    // TODO Plan 18-06+: workers don't currently signal close back to apply,
    // so entries leak slowly until process exit.
    let mut slot_proto: std::collections::HashMap<u64, WorkerProto> =
        std::collections::HashMap::new();
    let mut accept_seq: u64 = 0;

    tracing::info!(target: "beava.server", "apply thread: dispatcher loop started");

    // Plan 12-08 (D-A): adaptive busy-poll on apply.
    //
    // The apply thread tight-spins on `read_rx.try_recv()` for up to
    // `SPIN_BUDGET_K` consecutive empty iterations. After that it falls
    // through to a single blocking `read_rx.recv_timeout(50µs)` — wakes
    // immediately when a worker sends, otherwise returns Err(Timeout) so
    // the apply core doesn't burn 100% CPU at no-load.
    //
    // The listener-poll cadence ALWAYS runs every `LISTENER_POLL_EVERY`
    // iterations as a non-blocking `event_loop.tick(0)` — accept latency
    // stays bounded regardless of read pressure. The 50ms blocking
    // `tick(50ms)` shape that 12-07 shipped is removed; idleness now lives
    // entirely on the channel side via recv_timeout.
    //
    // Plan 12-08 (D-B): per-iteration response_batch. Responses queue into
    // a SmallVec inline-cap=16; flush fires when batch reaches
    // BATCH_SIZE_FLUSH=16 OR BATCH_TIME_FLUSH=100µs elapsed since first
    // push (whichever comes first). The flush groups items by worker and
    // fires worker_wakers[w].wake() ONCE per affected worker — collapses
    // N response wakes into 1 wake per batch.
    const LISTENER_POLL_EVERY: u32 = 1024;
    const SPIN_BUDGET_K: u32 = 10_000;
    const BATCH_SIZE_FLUSH: usize = 16;
    const BATCH_TIME_FLUSH: Duration = Duration::from_micros(100);
    let mut iter_counter: u32 = 0;
    let mut idle_iters: u32 = 0;
    let mut response_batch: smallvec::SmallVec<[ResponseBatchEntry; 16]> =
        smallvec::SmallVec::new();
    let mut batch_started_at: Option<Instant> = None;

    loop {
        // ── 1. Drain read_rx — dispatch + queue into response_batch ──────────
        // Plan 12-08 (D-D): drain-until-empty. The previous DRAIN_CAP=1024
        // forced re-entry into the listener-poll cadence after every 1024
        // items, costing an extra event_loop.tick(0) syscall + idle/iter
        // bookkeeping pass between batches. Apply is single-threaded; we
        // WANT to fully drain the channel and dispatch all queued work in
        // arrival order before checking accepts.
        //
        // Plan 12-08 (D-B): inside the drain we also check the size-flush
        // trigger (batch.len() ≥ 16) — keeps the response_batch from growing
        // unboundedly inside one big drain pass.
        //
        // Safety: read_rx is bounded(16_384), so even under burst load the
        // drain is bounded by channel capacity (workers can't outpace apply
        // forever — they backpressure on the bounded send). The accept
        // cadence still runs every LISTENER_POLL_EVERY=1024 OUTER iterations,
        // not every 1024 items.
        let mut drained: u64 = 0;
        let mut read_rx_disconnected = false;
        loop {
            let item = match read_rx.try_recv() {
                Ok(it) => it,
                Err(crossbeam_channel::TryRecvError::Empty) => break,
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    tracing::info!(
                        target: "beava.server",
                        "apply thread: read_rx disconnected during drain"
                    );
                    read_rx_disconnected = true;
                    break;
                }
            };
            drained += 1;
            dispatch_one_ring_item(
                item,
                &apply_shard,
                n_workers,
                &mut response_batch,
                &mut batch_started_at,
            );
            // Plan 12-08 (D-B): size-flush inside the drain so a long drain
            // pass doesn't grow the batch unboundedly. The Smallvec inline
            // cap is 16; once we exceed it the batch spills to heap, but
            // we'd rather flush + restart accumulation.
            if response_batch.len() >= BATCH_SIZE_FLUSH {
                flush_response_batch(
                    &mut response_batch,
                    &mut batch_started_at,
                    &write_txs,
                    &worker_wakers,
                    n_workers,
                );
            }
        }

        // ── 2. Time-flush trigger ────────────────────────────────────────────
        // Plan 12-08 (D-B): after a drain pass, check if the batch's first
        // entry has been waiting ≥ 100µs. Under sparse load this dominates:
        // the size-16 trigger never fires, so the time floor delivers
        // responses with bounded p99 latency.
        if !response_batch.is_empty()
            && batch_started_at.is_some_and(|t| t.elapsed() >= BATCH_TIME_FLUSH)
        {
            flush_response_batch(
                &mut response_batch,
                &mut batch_started_at,
                &write_txs,
                &worker_wakers,
                n_workers,
            );
        }

        // ── 3. Update idle/iter bookkeeping ──────────────────────────────────
        iter_counter = iter_counter.wrapping_add(1);
        if drained > 0 {
            APPLY_MAX_DRAIN_PER_ITER.fetch_max(drained, std::sync::atomic::Ordering::Relaxed);
            idle_iters = 0;
        } else {
            idle_iters = idle_iters.saturating_add(1);
        }

        // ── 4. Listener poll cadence (non-blocking) ──────────────────────────
        // Plan 12-08 (D-A): listener poll is ALWAYS non-blocking. Idle backoff
        // moved to the recv_timeout branch below. This way listener accepts
        // never block on the channel, and channel recv never blocks on
        // listener events.
        if iter_counter % LISTENER_POLL_EVERY == 0 {
            let tokens: Vec<mio::Token> = match event_loop.tick(Some(Duration::from_millis(0))) {
                Ok(events) => events.map(|e| e.token()).collect(),
                Err(e) => {
                    tracing::warn!("apply thread: poll error: {e}");
                    continue;
                }
            };

            for token in tokens {
                if token == TOKEN_HTTP_LISTENER {
                    accept_clients_to_workers(
                        &mut http_listener,
                        WorkerProto::Http,
                        &workers,
                        &mut slot_proto,
                        &mut accept_seq,
                    );
                } else if token == TOKEN_TCP_LISTENER {
                    accept_clients_to_workers(
                        &mut tcp_listener,
                        WorkerProto::Tcp,
                        &workers,
                        &mut slot_proto,
                        &mut accept_seq,
                    );
                } else if token == TOKEN_APPLY_WAKER {
                    // Worker pushed RingItems to read_rx; loop back to drain.
                    // No work needed here — the next iteration's drain pass
                    // will process the items. (Pre-12-08 this token also
                    // unblocked tick(50ms); post-12-08 the recv_timeout
                    // branch handles the wake directly via the channel.)
                }
                // Client-token events stay on the workers' EventLoops; apply
                // thread should never see them. Defensive: ignore unknown tokens.
            }
        }

        // ── 5. Idle backoff via recv_timeout (D-A) ───────────────────────────
        // After SPIN_BUDGET_K consecutive empty try_recv passes, give up CPU
        // for at most 50µs by blocking on the channel. A worker push wakes
        // us within ~50ns (channel signal is in-process; no kqueue/epoll
        // syscall round-trip). On Timeout we re-enter the tight try_recv
        // loop — listener poll cadence is unchanged.
        //
        // Plan 12-08 (D-B): before blocking, flush any pending response_batch
        // — the recv_timeout might block for the full 50µs and we don't want
        // to delay queued responses by another 50µs while our own apply
        // thread is idle.
        //
        // Listener cross-wake (D-A correctness fix, observed 2026-04-29):
        // Pre-12-08 the apply thread blocked on `event_loop.tick(50ms)` so
        // listener events (accept) and apply_waker fired on the SAME blocking
        // primitive. Post-12-08 it blocks on `recv_timeout` (channel-side),
        // which the listener event_loop can't wake. Without an explicit
        // listener-poll on each idle backoff, accept latency under sparse
        // load grows to LISTENER_POLL_EVERY × recv_timeout duration ≈ 50 ms
        // (1024 × 50 µs). That's a 1000× regression on first-connection
        // latency. Mitigation: do a non-blocking `event_loop.tick(0)` here
        // BEFORE the recv_timeout. Cheap (~1 µs syscall) and bounded.
        if idle_iters >= SPIN_BUDGET_K {
            if !response_batch.is_empty() {
                flush_response_batch(
                    &mut response_batch,
                    &mut batch_started_at,
                    &write_txs,
                    &worker_wakers,
                    n_workers,
                );
            }
            // Drain pending listener events before blocking.
            if let Ok(events) = event_loop.tick(Some(Duration::from_millis(0))) {
                for token in events.map(|e| e.token()) {
                    if token == TOKEN_HTTP_LISTENER {
                        accept_clients_to_workers(
                            &mut http_listener,
                            WorkerProto::Http,
                            &workers,
                            &mut slot_proto,
                            &mut accept_seq,
                        );
                    } else if token == TOKEN_TCP_LISTENER {
                        accept_clients_to_workers(
                            &mut tcp_listener,
                            WorkerProto::Tcp,
                            &workers,
                            &mut slot_proto,
                            &mut accept_seq,
                        );
                    }
                }
            }
            APPLY_RECV_TIMEOUT_CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            match read_rx.recv_timeout(Duration::from_micros(50)) {
                Ok(item) => {
                    dispatch_one_ring_item(
                        item,
                        &apply_shard,
                        n_workers,
                        &mut response_batch,
                        &mut batch_started_at,
                    );
                    // Flush immediately on the recv_timeout Ok arm — we
                    // were just blocked, no point holding the response.
                    flush_response_batch(
                        &mut response_batch,
                        &mut batch_started_at,
                        &write_txs,
                        &worker_wakers,
                        n_workers,
                    );
                    idle_iters = 0;
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    // Idle 50µs elapsed — fall back into the spin loop. Don't
                    // reset idle_iters; we want to immediately re-enter
                    // recv_timeout next iteration if still idle.
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    tracing::info!(
                        target: "beava.server",
                        "apply thread: read_rx disconnected (all workers gone), exiting"
                    );
                    break;
                }
            }
        }

        // ── 6. Shutdown check ────────────────────────────────────────────────
        if shutdown.load(AOrdering::Acquire) {
            tracing::info!(
                target: "beava.server",
                "apply thread: shutdown signal received, stopping workers"
            );
            break;
        }
        // Plan 12-08 (D-D): if the drain pass observed a disconnected channel,
        // exit the loop after honouring the existing shutdown contract.
        if read_rx_disconnected {
            break;
        }
    }

    // Plan 12-08 (D-B): on shutdown, flush any pending responses so we
    // don't lose acks queued in the batch.
    if !response_batch.is_empty() {
        flush_response_batch(
            &mut response_batch,
            &mut batch_started_at,
            &write_txs,
            &worker_wakers,
            n_workers,
        );
    }

    // ── Shutdown sequence: tell workers to stop, then join ───────────────────
    stop.store(true, AOrdering::Release);
    for w in &workers {
        w.stop();
    }
    for w in workers {
        w.join();
    }
    tracing::info!(target: "beava.server", "apply thread: exiting");
}

/// Plan 18-05/18-06: route accepted clients to per-worker IoBackends.
/// Each accepted stream is assigned a monotonic `slot_idx` and dispatched to
/// `worker[slot_idx % n_workers]` via `send_new_client_with_proto`. Apply
/// records the slot's protocol in `slot_proto` so it can encode responses
/// correctly (the worker tracks the same proto independently for parse).
fn accept_clients_to_workers<L>(
    listener: &mut L,
    proto: beava_runtime_core::io_thread_worker::WorkerProto,
    workers: &[beava_runtime_core::io_thread_worker::WorkerHandle],
    slot_proto: &mut std::collections::HashMap<
        u64,
        beava_runtime_core::io_thread_worker::WorkerProto,
    >,
    accept_seq: &mut u64,
) where
    L: AcceptStream,
{
    let n_workers = workers.len();
    if n_workers == 0 {
        return;
    }
    loop {
        match listener.accept_stream() {
            Ok(stream) => {
                let slot_idx = *accept_seq;
                *accept_seq = accept_seq.wrapping_add(1);
                let w = (slot_idx as usize) % n_workers;
                slot_proto.insert(slot_idx, proto);
                if let Err(e) = workers[w].send_new_client_with_proto(stream, slot_idx, proto) {
                    tracing::warn!(
                        target: "beava.server",
                        "apply thread: send_new_client to worker {} failed: {}",
                        w,
                        e
                    );
                    slot_proto.remove(&slot_idx);
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) => {
                tracing::warn!(target: "beava.server", "apply thread: accept failed: {e}");
                break;
            }
        }
    }
}

// ─── Plan 18-04.7 IoPool wiring helpers ──────────────────────────────────────

/// Newtype wrapper that lets us send a raw mut pointer into a Send WorkItem
/// closure without the per-call `unsafe impl Send` boilerplate.
///
/// SAFETY of the Send impl: the pointer always points into a Vec that is
/// pre-allocated and never resized (`MAX_CONCURRENT_CLIENTS`). Synchronization
/// is provided externally by the IoPool's Release/Acquire barrier — only one
/// worker accesses any given slot index per tick, and the apply thread waits
/// at `join_all()` before touching the slot.
#[allow(dead_code)]
#[derive(Clone, Copy)]
struct ClientsPtr(*mut Option<MioClient>);

// SAFETY: see ClientsPtr docs — pointer aliases are bounded by IoPool barriers.
unsafe impl Send for ClientsPtr {}
unsafe impl Sync for ClientsPtr {}

#[allow(dead_code)]
impl ClientsPtr {
    /// Access the slot at `idx`. Method-based access forces the closure to
    /// capture the whole `ClientsPtr` instead of disjointly capturing the
    /// inner `*mut` field (Rust 2021 RFC 2229 closure-capture rules).
    ///
    /// SAFETY: see the struct-level docs.
    #[inline]
    unsafe fn slot_mut(self, idx: usize) -> *mut Option<MioClient> {
        self.0.add(idx)
    }
}

#[allow(dead_code)]
impl MioClient {
    /// True if the client has bytes to write — either un-serialised
    /// `GlueResponse`s in `output_queue` or partially-flushed bytes in
    /// `write_buf`.
    fn has_write_work(&self) -> bool {
        !self.output_queue.is_empty() || !self.write_buf.is_empty()
    }
}

/// Accept all pending connections from `listener` until WouldBlock, allocate
/// a free slot for each, and register with the event loop.
#[allow(dead_code)]
fn accept_clients<L>(
    listener: &mut L,
    proto: MioProto,
    clients: &mut [Option<MioClient>],
    free_slots: &mut Vec<usize>,
    event_loop: &mut beava_runtime_core::event_loop::EventLoop,
) where
    L: AcceptStream,
{
    loop {
        match listener.accept_stream() {
            Ok(stream) => {
                let slot_idx = match free_slots.pop() {
                    Some(i) => i,
                    None => {
                        tracing::warn!(
                            target: "beava.server",
                            "apply thread: no free client slot — dropping connection (>= {} concurrent clients)",
                            MAX_CONCURRENT_CLIENTS
                        );
                        // Drop the stream by letting it leave scope.
                        drop(stream);
                        break;
                    }
                };
                let client_token = mio::Token(slot_idx + TOKEN_CLIENT_BASE);
                let mut client = MioClient {
                    stream,
                    token: client_token,
                    proto,
                    read_buf: bytes::BytesMut::with_capacity(8 * 1024),
                    output_queue: std::collections::VecDeque::new(),
                    write_buf: bytes::BytesMut::new(),
                    interest_writable: false,
                    closed: false,
                };
                if let Err(e) =
                    event_loop.register(&mut client.stream, client_token, mio::Interest::READABLE)
                {
                    tracing::warn!("apply thread: register client failed: {e}");
                    free_slots.push(slot_idx);
                } else {
                    clients[slot_idx] = Some(client);
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(_) => break,
        }
    }
}

/// Trait abstraction over `HttpListener` / `TcpListener` so `accept_clients`
/// can drive both. Each impl just calls its `accept(...)` method and returns
/// the underlying `mio::net::TcpStream`.
trait AcceptStream {
    fn accept_stream(&mut self) -> std::io::Result<mio::net::TcpStream>;
}

impl AcceptStream for beava_runtime_core::http_listener::HttpListener {
    fn accept_stream(&mut self) -> std::io::Result<mio::net::TcpStream> {
        self.accept().map(|(s, _)| s)
    }
}

impl AcceptStream for beava_runtime_core::tcp_listener::TcpListener {
    fn accept_stream(&mut self) -> std::io::Result<mio::net::TcpStream> {
        self.accept().map(|(s, _)| s)
    }
}

/// Plan 18-13: Read-phase variant that pushes parsed requests directly into a
/// crossbeam channel as each frame is decoded, rather than batching them into
/// `client.parsed_requests` + `client.parsed_rows` Vecs. This lets the apply
/// thread start dispatching events the instant a single worker has parsed
/// one — eliminating the per-tick `IoPool::join_all` spin barrier (the
/// dominant source of inter-event gap on macOS, ~218 µs every ~128 events
/// at p=4/pd=64).
///
/// The channel is bounded; if it fills (apply thread is far behind), `send`
/// blocks the worker briefly. In normal operation apply is faster than parse
/// (apply ~0.9 µs vs parse ~4 µs per push), so the channel rarely contends.
///
/// Backward compat: callers that need the legacy batched behavior continue
/// to call `read_and_parse_client` (used by Phase 18-04.6/18-04.8 tests
/// that exercise `dispatch_wire_request_with_row` directly).
#[allow(dead_code)]
fn read_and_parse_client_to_channel(
    client: &mut MioClient,
    slot_idx: u32,
    sender: &crossbeam_channel::Sender<beava_runtime_core::work_ring::RingItem>,
) {
    use beava_core::row::Row;
    use beava_core::wire::CT_MSGPACK;
    use beava_runtime_core::http_listener::parse_http_request;
    use beava_runtime_core::tcp_listener::parse_wire_request;
    use beava_runtime_core::wire_request::WireRequest;
    use beava_runtime_core::work_ring::{ParseErrorKind, RingItem};
    use std::io::Read;

    if client.closed {
        return;
    }

    // Phase A: drain socket → read_buf.
    let mut tmp_buf = [0u8; 16 * 1024];
    loop {
        match client.stream.read(&mut tmp_buf) {
            Ok(0) => {
                client.closed = true;
                return;
            }
            Ok(n) => {
                client.read_buf.extend_from_slice(&tmp_buf[..n]);
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(_) => {
                client.closed = true;
                return;
            }
        }
    }

    if client.read_buf.is_empty() {
        return;
    }

    // Phase B+C: parse each frame, do body→Row inline, push to channel.
    iopool_observer::record_parse();

    // Helper closure: deserialize body→Row for push variants. None for non-push
    // or when deserialization fails (apply will retry / emit invalid_event).
    let body_to_row = |req: &WireRequest| -> Option<Row> {
        match req {
            WireRequest::TcpPush {
                body, body_format, ..
            }
            | WireRequest::HttpPush {
                body, body_format, ..
            }
            | WireRequest::HttpPushSync {
                body, body_format, ..
            }
            | WireRequest::HttpPushBatch {
                body, body_format, ..
            } => {
                if *body_format == CT_MSGPACK {
                    rmp_serde::from_slice::<Row>(body).ok()
                } else {
                    sonic_rs::from_slice::<Row>(body).ok()
                }
            }
            _ => None,
        }
    };

    match client.proto {
        MioProto::Tcp => loop {
            match parse_wire_request(&mut client.read_buf, 4 * 1024 * 1024) {
                Ok(Some(req)) => {
                    let parsed_row = body_to_row(&req);
                    if sender
                        .send(RingItem::Request {
                            slot_idx,
                            keep_alive: false,
                            request: req,
                            parsed_row,
                        })
                        .is_err()
                    {
                        // Receiver dropped — server shutting down.
                        return;
                    }
                }
                Ok(None) => break,
                Err(_) => {
                    let _ = sender.send(RingItem::ParseError {
                        slot_idx,
                        kind: ParseErrorKind::TcpFrame,
                    });
                    break;
                }
            }
        },
        MioProto::Http => loop {
            match parse_http_request(&mut client.read_buf) {
                Ok(Some((req, keep_alive))) => {
                    let parsed_row = body_to_row(&req);
                    if sender
                        .send(RingItem::Request {
                            slot_idx,
                            keep_alive,
                            request: req,
                            parsed_row,
                        })
                        .is_err()
                    {
                        return;
                    }
                }
                Ok(None) => break,
                Err(_) => {
                    let _ = sender.send(RingItem::ParseError {
                        slot_idx,
                        kind: ParseErrorKind::HttpProtocol,
                    });
                    break;
                }
            }
        },
    }
}

/// Plan 18-13: Drain the work-ring receiver concurrently with IoPool workers
/// running. Returns when:
///   1. All IoPool worker threads have signaled `pending = 0` (no more work)
///   2. AND the receiver channel is empty (all parsed events dispatched).
///
/// This replaces the prior `IoPool::join_all + drain_parsed_requests` two-step
/// (which forced the apply thread to wait for ALL workers to finish parsing
/// before processing ANY parsed event). Now apply dispatches events as they
/// arrive — overlap of parse-on-IoPool with apply-on-apply-thread.
///
/// Per-event flow inside the loop:
/// - `try_recv` → `dispatch_wire_request_with_row` → push response to
///   `clients[slot_idx].output_queue`
/// - `ParseError` → push error response, mark client closed
#[allow(dead_code)]
fn drain_channel_until_workers_done(
    io_pool: &beava_runtime_core::io_pool::IoPool,
    receiver: &crossbeam_channel::Receiver<beava_runtime_core::work_ring::RingItem>,
    apply_shard: &crate::apply_shard::ApplyShard,
    clients: &mut [Option<MioClient>],
) {
    use beava_runtime_core::work_ring::{ParseErrorKind, RingItem};
    use std::sync::atomic::Ordering;

    const SPIN_ITERS: u32 = 1024;
    let mut idle_count: u32 = 0;

    loop {
        // Greedy drain — pull as many as we can.
        let mut drained_any = false;
        while let Ok(item) = receiver.try_recv() {
            drained_any = true;
            match item {
                RingItem::Request {
                    slot_idx,
                    keep_alive: _,
                    request,
                    parsed_row,
                } => {
                    let responses = apply_shard.dispatch_wire_request_with_row(request, parsed_row);
                    if let Some(slot) = clients.get_mut(slot_idx as usize) {
                        if let Some(client) = slot.as_mut() {
                            for resp in responses {
                                client.output_queue.push_back(resp);
                            }
                        }
                    }
                }
                RingItem::ParseError { slot_idx, kind } => {
                    if let Some(slot) = clients.get_mut(slot_idx as usize) {
                        if let Some(client) = slot.as_mut() {
                            use crate::runtime_core_glue::GlueResponse;
                            client.output_queue.push_back(match kind {
                                // Plan 12.6-15: rich frame_too_large frame
                                // (criterion 7).
                                ParseErrorKind::TcpFrameTooLarge { declared, limit } => {
                                    GlueResponse::TcpError {
                                        code: "frame_too_large",
                                        message: format!(
                                            "declared frame length {declared} exceeds limit {limit}",
                                        ),
                                        extras: serde_json::json!({
                                            "limit": limit,
                                            "declared": declared,
                                        }),
                                    }
                                }
                                ParseErrorKind::TcpFrame => GlueResponse::PushError {
                                    code: "frame_error",
                                    registry_version: 0,
                                },
                                ParseErrorKind::HttpProtocol => GlueResponse::PushError {
                                    code: "http_protocol_error",
                                    registry_version: 0,
                                },
                            });
                            client.closed = true;
                        }
                    }
                }
            }
        }

        // Termination: all workers done AND channel empty.
        let all_workers_done = io_pool
            .slots
            .iter()
            .all(|s| s.pending.load(Ordering::Acquire) == 0);
        if all_workers_done && receiver.is_empty() {
            break;
        }

        // Apply backoff if we didn't drain anything this iteration. Workers are
        // still busy parsing — give them CPU.
        if !drained_any {
            idle_count = idle_count.saturating_add(1);
            if idle_count < SPIN_ITERS {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
        } else {
            idle_count = 0;
        }
    }
}

/// Write-phase work item body. Runs on an IoPool worker thread.
///
/// 1. Drain `output_queue`, serialize each `GlueResponse` into `write_buf`
///    using the proto-appropriate encoder.
/// 2. Loop on `socket.write` from the head of `write_buf` until `WouldBlock`
///    or `write_buf` is empty.
/// 3. On EOF / error, mark closed.
#[allow(dead_code)]
fn serialize_and_write_client(client: &mut MioClient) {
    use std::io::Write;

    if client.closed {
        return;
    }

    // Phase A: serialize any queued responses.
    if !client.output_queue.is_empty() {
        iopool_observer::record_encode();
        let queue = std::mem::take(&mut client.output_queue);
        for resp in queue {
            match client.proto {
                MioProto::Tcp => encode_glue_response_tcp(&resp, &mut client.write_buf),
                MioProto::Http => encode_glue_response_http(&resp, &mut client.write_buf),
            }
        }
    }

    // Phase B: drain write_buf to the socket.
    while !client.write_buf.is_empty() {
        match client.stream.write(&client.write_buf) {
            Ok(0) => break,
            Ok(n) => {
                let _ = client.write_buf.split_to(n);
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(_) => {
                client.closed = true;
                return;
            }
        }
    }
}

/// Encode a GlueResponse as a TCP framed response into `buf`.
fn encode_glue_response_tcp(
    resp: &crate::runtime_core_glue::GlueResponse,
    buf: &mut bytes::BytesMut,
) {
    use crate::runtime_core_glue::GlueResponse;
    use beava_core::wire::{CT_JSON, OP_ERROR_RESPONSE, OP_GET_RESPONSE, OP_PING, OP_PUSH};

    match resp {
        // Plan 12.6-01: Pong body matches the legacy TCP encoder so
        // `tcp_client::ping` can read `server_version` + `registry_version`
        // back. Pre-rewrite the mio path emitted `{}` here, breaking
        // `testing::tests::tcp_client_connect_and_ping` against ServerV18.
        GlueResponse::Pong { registry_version } => {
            let body = serde_json::json!({
                "server_version": crate::VERSION,
                "registry_version": registry_version,
            });
            let b = serde_json::to_vec(&body).unwrap_or_default();
            encode_tcp_frame_bytes(OP_PING, CT_JSON, &b, buf);
        }
        GlueResponse::PushAck {
            ack_lsn,
            registry_version,
        } => {
            // Plan 12.6-15: include `idempotent_replay: false` flag so phase8
            // TCP push tests can discriminate between fresh ack and dedupe
            // replay. Matches legacy axum TCP encoder shape.
            let body = serde_json::json!({
                "ack_lsn": ack_lsn,
                "idempotent_replay": false,
                "registry_version": registry_version,
            });
            let b = serde_json::to_vec(&body).unwrap_or_default();
            encode_tcp_frame_bytes(OP_PUSH, CT_JSON, &b, buf);
        }
        GlueResponse::PushReplay {
            registry_version,
            ack_lsn,
            cached_body: _,
        } => {
            // Plan 12.6-15: TCP idempotent-replay body. TCP has no
            // replay header (HTTP gets `X-Beava-Idempotent-Replay: 1`);
            // the body flag `idempotent_replay: true` IS the
            // discriminator. Include the cached `ack_lsn` so callers can
            // assert `ack1.ack_lsn == ack2.ack_lsn` (dedupe replay LSN
            // identity per phase8_tcp_push expectations).
            let body = match ack_lsn {
                Some(lsn) => serde_json::json!({
                    "ack_lsn": lsn,
                    "idempotent_replay": true,
                    "registry_version": registry_version,
                }),
                None => serde_json::json!({
                    "idempotent_replay": true,
                    "registry_version": registry_version,
                }),
            };
            let b = serde_json::to_vec(&body).unwrap_or_default();
            encode_tcp_frame_bytes(OP_PUSH, CT_JSON, &b, buf);
        }
        // Plan 12.6-01: register response (success and error paths funnel
        // through here). `body` is pre-serialised by
        // `register::register_outcome_to_glue` to match the legacy axum
        // `map_outcome_to_http` body verbatim — `RegisterSuccess` on
        // success, `RegisterErrorBody` on failure.  `tcp_op` is
        // `OP_REGISTER` on success, `OP_ERROR_RESPONSE` on failure.
        GlueResponse::Register { body, tcp_op, .. } => {
            encode_tcp_frame_bytes(*tcp_op, CT_JSON, body, buf);
        }
        GlueResponse::PushError {
            code,
            registry_version,
        } => {
            // Plan 12.6-15: TCP error-frame body must match legacy axum
            // shape — `{"error": {"code": "..."}}`, not `{"code": "..."}`.
            // phase8_tcp_push tests assert on `body["error"]["code"]`.
            let body = serde_json::json!({
                "error": {"code": code},
                "registry_version": registry_version,
            });
            let b = serde_json::to_vec(&body).unwrap_or_default();
            encode_tcp_frame_bytes(OP_ERROR_RESPONSE, CT_JSON, &b, buf);
        }
        // Plan 12-07 Wave 5 + Plan 12-09 Wave 3: TCP /get response framing.
        // Echo back the (already-serialised) body framed as
        // OP_GET_RESPONSE = 0x0023. FIFO correlation on the connection
        // ties this frame to its originating OP_GET / OP_MGET / OP_GET_MULTI
        // request — Redis-style strict-FIFO ordering, no request_id needed.
        //
        // Plan 12-09: `*format` is the request frame's content_type byte
        // (CT_JSON or CT_MSGPACK) propagated end-to-end via
        // GlueResponse::QueryResult.format — msgpack-in produces msgpack-out
        // (locked decision D-B). The dispatch helpers in runtime_core_glue.rs
        // already encoded the body with the matching codec.
        GlueResponse::QueryResult { body, format } => {
            encode_tcp_frame_bytes(OP_GET_RESPONSE, *format, body, buf);
        }
        GlueResponse::QueryNotFound { code } => {
            let body = serde_json::json!({"error": {"code": code}});
            let b = serde_json::to_vec(&body).unwrap_or_default();
            encode_tcp_frame_bytes(OP_ERROR_RESPONSE, CT_JSON, &b, buf);
        }
        // Plan 13.4-08 (D-03): OP_RESET success — body shape
        // `{"reset": true, "registry_version": N}`. Frame opcode is
        // OP_GET_RESPONSE (0x0023) — the generic JSON success frame
        // (matching the OP_GET / OP_BATCH_GET response opcode reuse).
        GlueResponse::ResetOk { registry_version } => {
            let body = serde_json::json!({
                "reset": true,
                "registry_version": registry_version,
            });
            let b = serde_json::to_vec(&body).unwrap_or_default();
            encode_tcp_frame_bytes(OP_GET_RESPONSE, CT_JSON, &b, buf);
        }
        // Plan 13.4-08 (D-03): OP_RESET rejected — frame opcode is the
        // dedicated 0xFFFF error frame; body matches the HTTP 403 body
        // verbatim.
        GlueResponse::ResetForbidden => {
            let body = reset_forbidden_body();
            let b = serde_json::to_vec(&body).unwrap_or_default();
            encode_tcp_frame_bytes(OP_ERROR_RESPONSE, CT_JSON, &b, buf);
        }
        // Plan 12.6-15: rich TCP error frame (op_not_implemented, unknown_op,
        // unsupported_content_type, frame_too_large). Body shape:
        // `{"error": {"code": <code>, "message": <msg>, ...extras}}` —
        // matches phase2_5_smoke criterion-5/7/bonus_msgpack expectations.
        GlueResponse::TcpError {
            code,
            message,
            extras,
        } => {
            // Merge `extras` into the error object so callers can carry
            // structured fields like `frame_too_large.limit` without
            // proliferating GlueResponse variants.
            let mut error_obj = serde_json::Map::new();
            error_obj.insert("code".to_string(), serde_json::json!(code));
            error_obj.insert("message".to_string(), serde_json::json!(message));
            if let serde_json::Value::Object(extras_obj) = extras {
                for (k, v) in extras_obj {
                    error_obj.insert(k.clone(), v.clone());
                }
            }
            let body = serde_json::json!({"error": serde_json::Value::Object(error_obj)});
            let b = serde_json::to_vec(&body).unwrap_or_default();
            encode_tcp_frame_bytes(OP_ERROR_RESPONSE, CT_JSON, &b, buf);
        }
        // Plan 13.4.1 D-05: legacy request shape rejected. TCP frame is
        // OP_ERROR_RESPONSE (0xFFFF) with the locked body shape that mirrors
        // the HTTP 400 response.
        GlueResponse::UnsupportedRequestShape { hint } => {
            let body = serde_json::json!({
                "error": {
                    "code": "unsupported_request_shape",
                    "message": hint,
                }
            });
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

    // Plan 12.6-15: extra response headers (e.g. `x-beava-idempotent-replay: 1`
    // on PushReplay). Empty when no extras apply; appended verbatim to the
    // standard HTTP response header block below.
    let mut extra_headers = String::new();

    let (status, body_bytes): (u16, Vec<u8>) = match resp {
        GlueResponse::PushAck {
            ack_lsn,
            registry_version,
        } => {
            let body = serde_json::json!({"ack_lsn": ack_lsn, "registry_version": registry_version, "idempotent_replay": false});
            (200, serde_json::to_vec(&body).unwrap_or_default())
        }
        GlueResponse::PushReplay {
            registry_version,
            ack_lsn: _,
            cached_body,
        } => {
            // Plan 12.6-15: byte-identical replay (success criterion #2).
            // The cached `Bytes` IS the verbatim original /push response
            // body (e.g. `{ack_lsn:N, idempotent_replay:false,
            // registry_version:V}`); pass it through unchanged. When
            // `None` (cache miss), synthesise the legacy generic
            // `{idempotent_replay: true, registry_version: V}` shape.
            //
            // Plan 12.6-15: also emit the `x-beava-idempotent-replay: 1`
            // response header (legacy axum push handler did this; the
            // mio path was missing it pre-Plan-15).
            extra_headers.push_str("X-Beava-Idempotent-Replay: 1\r\n");
            if let Some(cached) = cached_body {
                (200, cached.to_vec())
            } else {
                let body = serde_json::json!({"idempotent_replay": true, "registry_version": registry_version});
                (200, serde_json::to_vec(&body).unwrap_or_default())
            }
        }
        // Plan 12.6-01: HTTP /register response — body is pre-serialised
        // by `register::register_outcome_to_glue` to match the legacy
        // axum `map_outcome_to_http` JSON shape exactly (used by ~30
        // phase2/4/5/etc. tests).  Status is 200 on success, 400/409/503
        // on failure.
        GlueResponse::Register {
            http_status, body, ..
        } => (*http_status, body.to_vec()),
        GlueResponse::PushError { code, .. } => {
            let body = serde_json::json!({"error": {"code": code}});
            let status = if *code == "event_not_found" { 404 } else { 400 };
            (status, serde_json::to_vec(&body).unwrap_or_default())
        }
        // Plan 12-09 D-D: HTTP /get is JSON-only. Format byte from the
        // request path is intentionally ignored here — the response header
        // below always sets `Content-Type: application/json` regardless.
        GlueResponse::QueryResult { body, format: _ } => (200, body.to_vec()),
        GlueResponse::QueryNotFound { code } => {
            let body = serde_json::json!({"error": {"code": code}});
            (404, serde_json::to_vec(&body).unwrap_or_default())
        }
        GlueResponse::Pong { registry_version } => {
            let body = serde_json::json!({"pong": true, "registry_version": registry_version});
            (200, serde_json::to_vec(&body).unwrap_or_default())
        }
        // Plan 12-07: /health on mio data-plane port. Always 200.
        GlueResponse::HealthOk => (200, br#"{"status":"ok"}"#.to_vec()),
        // Plan 12.6-01: /ready on mio data-plane port. Always 200 once the
        // listener is up — readiness is gated by the admin sidecar's
        // recovery flag, but the data-plane mirror returns 200
        // unconditionally so test fixtures that poll `base_url + /ready`
        // converge.  Body matches admin's `{"status":"ready"}`.
        GlueResponse::ReadyOk => (200, br#"{"status":"ready"}"#.to_vec()),
        // Plan 12.6-01: /registry on mio data-plane port. Body is the live
        // registry snapshot (matches the legacy axum dev endpoint shape).
        GlueResponse::RegistrySnapshot { body } => (200, body.to_vec()),
        // Plan 12.6-01: 404 Not Found for paths not in the router table.
        GlueResponse::HttpRouteNotFound { path } => {
            let body = serde_json::json!({"error": {"code": "not_found", "path": path}});
            (404, serde_json::to_vec(&body).unwrap_or_default())
        }
        // Plan 12.6-01: 405 Method Not Allowed for path-known but
        // method-mismatched requests.
        GlueResponse::HttpMethodNotAllowed { method, path } => {
            let body = serde_json::json!({
                "error": {"code": "method_not_allowed", "method": method, "path": path},
            });
            (405, serde_json::to_vec(&body).unwrap_or_default())
        }
        // Plan 12.6-14: 415 Unsupported Media Type for POST endpoints.
        // Body matches legacy axum register handler `RegisterErrorBody`
        // shape (`error.code = "unsupported_media_type"`).
        GlueResponse::HttpUnsupportedMediaType { received, path } => {
            let _ = received;
            let body = serde_json::json!({
                "error": {
                    "code": "unsupported_media_type",
                    "path": path,
                    "reason": "expected application/json",
                },
                "registry_version": 0u64,
            });
            (415, serde_json::to_vec(&body).unwrap_or_default())
        }
        GlueResponse::InternalError { reason } => {
            let body = serde_json::json!({"error": {"code": "internal_error", "reason": reason}});
            (500, serde_json::to_vec(&body).unwrap_or_default())
        }
        // Plan 13.4-08 (D-03): OP_RESET success — HTTP 200 + body
        // `{"reset": true, "registry_version": N}`.
        GlueResponse::ResetOk { registry_version } => {
            let body = serde_json::json!({"reset": true, "registry_version": registry_version});
            (200, serde_json::to_vec(&body).unwrap_or_default())
        }
        // Plan 13.4-08 (D-03): OP_RESET rejected — HTTP 403 + structured
        // `reset_disabled_in_production` body. Reason text mentions both
        // opt-in paths (`BEAVA_TEST_MODE` env var, `test_mode` kwarg) per
        // Test 1 in `phase13_4_reset_default_rejected.rs`.
        GlueResponse::ResetForbidden => {
            let body = reset_forbidden_body();
            (403, serde_json::to_vec(&body).unwrap_or_default())
        }
        // Plan 13.4.1 D-05: legacy request shape rejected. HTTP 400 with the
        // locked body shape `{"error":{"code":"unsupported_request_shape",
        // "message":<hint>}}` where `hint` points at the relevant doc anchor.
        GlueResponse::UnsupportedRequestShape { hint } => {
            let body = serde_json::json!({
                "error": {
                    "code": "unsupported_request_shape",
                    "message": hint,
                }
            });
            (400, serde_json::to_vec(&body).unwrap_or_default())
        }
        _ => (501, b"{\"error\":{\"code\":\"unsupported\"}}".to_vec()),
    };

    let status_text = match status {
        200 => "OK",
        400 => "Bad Request",
        // Plan 13.4-08 (D-03): 403 added for `reset_disabled_in_production`.
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        415 => "Unsupported Media Type",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        503 => "Service Unavailable",
        _ => "OK",
    };

    let header = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nX-Runtime: hand-rolled\r\nConnection: keep-alive\r\n{}\r\n",
        status,
        status_text,
        body_bytes.len(),
        extra_headers,
    );
    buf.extend_from_slice(header.as_bytes());
    buf.extend_from_slice(&body_bytes);
}

/// Plan 13.4-08 (D-03 USER-LOCKED): canonical body for the
/// `reset_disabled_in_production` error. Used by both the HTTP encoder
/// (403) and the TCP encoder (OP_ERROR_RESPONSE) so callers see
/// identical body shape regardless of transport — the error.code is the
/// stable contract per `docs/error-codes.md`.
///
/// Reason text mentions BOTH opt-in paths (`BEAVA_TEST_MODE` env var and
/// `test_mode` kwarg) so users see actionable error text — pinned by
/// `phase13_4_reset_default_rejected::default_config_no_env_var_post_reset_returns_403_structured`.
fn reset_forbidden_body() -> serde_json::Value {
    serde_json::json!({
        "error": {
            "code": "reset_disabled_in_production",
            "reason": "OP_RESET requires server test_mode (set \
                       BEAVA_TEST_MODE=1 or pass Config { test_mode: true \
                       } at server construction). See docs/error-codes.md."
        }
    })
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
#[cfg(test)]
mod tests {
    use super::*;

    // ─── Plan 12-07 Wave 5 (RED) — TCP encoder for QueryResult / QueryNotFound ─

    /// Plan 12-07 Task 5.a: encode_glue_response_tcp must emit OP_GET_RESPONSE
    /// for QueryResult so that batched/single TCP /get clients can read back
    /// the JSON body framed under the new opcode.
    #[test]
    fn test_encode_tcp_query_result_emits_op_get_response_frame() {
        use crate::runtime_core_glue::GlueResponse;
        use beava_core::wire::{decode_frame, CT_JSON, OP_GET_RESPONSE};

        let mut buf = bytes::BytesMut::new();
        let resp = GlueResponse::QueryResult {
            body: bytes::Bytes::from_static(br#"{"value":42}"#),
            format: CT_JSON,
        };
        encode_glue_response_tcp(&resp, &mut buf);
        let frame = decode_frame(&mut buf, 4 * 1024 * 1024)
            .expect("decode_frame")
            .expect("complete frame");
        assert_eq!(
            frame.op, OP_GET_RESPONSE,
            "expected OP_GET_RESPONSE (0x0023), got {:#06x}",
            frame.op
        );
        assert_eq!(frame.content_type, CT_JSON);
        assert_eq!(frame.payload.as_ref(), br#"{"value":42}"#);
    }

    /// Plan 12-07 Task 5.a: QueryNotFound emits an OP_ERROR_RESPONSE frame
    /// whose payload carries the error code.
    #[test]
    fn test_encode_tcp_query_not_found_emits_error_response() {
        use crate::runtime_core_glue::GlueResponse;
        use beava_core::wire::{decode_frame, OP_ERROR_RESPONSE};

        let mut buf = bytes::BytesMut::new();
        let resp = GlueResponse::QueryNotFound {
            code: "key_not_found",
        };
        encode_glue_response_tcp(&resp, &mut buf);
        let frame = decode_frame(&mut buf, 4 * 1024 * 1024)
            .expect("decode_frame")
            .expect("complete frame");
        assert_eq!(
            frame.op, OP_ERROR_RESPONSE,
            "expected OP_ERROR_RESPONSE for QueryNotFound, got {:#06x}",
            frame.op
        );
        let payload_str = std::str::from_utf8(&frame.payload).expect("utf8 payload");
        assert!(
            payload_str.contains("key_not_found"),
            "payload must carry error code, got: {payload_str}"
        );
    }
}
