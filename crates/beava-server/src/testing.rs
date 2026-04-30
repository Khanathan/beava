//! In-process integration-test harness for the beava server.
//!
//! Used by Phase 1's `foundation_smoke.rs` and every subsequent phase's HTTP tests.
//! Spawns a real `ServerV18` (mio data plane + tokio admin sidecar) on an
//! OS-allocated port, waits for readiness via the admin `/ready` endpoint,
//! hands back a `TestServer` whose `.base_url()` can be curled, and shuts
//! down gracefully on `.shutdown().await`.
//!
//! Usage:
//! ```no_run
//! # async fn ex() {
//! use beava_server::testing::TestServer;
//! let ts = TestServer::spawn().await.expect("spawn");
//! let url = format!("{}/health", ts.base_url());
//! // issue requests with reqwest / hyper / etc.
//! ts.shutdown().await.expect("shutdown");
//! # }
//! ```
//!
//! Availability: feature-gated behind `testing`. Consumers in Cargo.toml:
//! ```toml
//! [dev-dependencies]
//! beava-server = { path = "...", features = ["testing"] }
//! ```
//!
//! Plan 12.6-01 (D-01): rewritten to wrap `ServerV18` instead of the legacy
//! axum `Server`.  Public builder API preserved so the ~20 test files
//! recompile without functional changes.

#![cfg(any(feature = "testing", test))]

use crate::server::{ServerError, ServerV18};
use crate::Config;
use beava_core::wire::{decode_frame, encode_frame, Frame, CT_JSON, OP_PING, OP_REGISTER};
use bytes::{Bytes, BytesMut};
use serde::Serialize;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

#[derive(Debug, Error)]
pub enum TestServerError {
    #[error(transparent)]
    Server(#[from] ServerError),
    #[error("readiness timed out after {0:?}")]
    ReadinessTimeout(Duration),
    #[error("server task join failed: {0}")]
    Join(String),
}

/// Builder for a TestServer with overrideable config knobs.
pub struct TestServerBuilder {
    cfg: Config,
    readiness_timeout: Duration,
    readiness_poll_interval: Duration,
    dev_endpoints: bool,
}

impl Default for TestServerBuilder {
    fn default() -> Self {
        // TCP: enabled by default (matches production); OS-assigned port so
        // tests don't collide on 7380. Plan 04 Task 3 wires in the TCP bind.
        // WAL: each test overrides `.wal_dir(tempdir)` so the default here is
        // a unique per-process temp path.
        let default_wal_dir = std::env::temp_dir().join(format!(
            "beava-test-wal-{}-{}",
            std::process::id(),
            uniq_id()
        ));
        let default_snapshot_dir = std::env::temp_dir().join(format!(
            "beava-test-snap-{}-{}",
            std::process::id(),
            uniq_id()
        ));
        let cfg = Config {
            listen_addr: "127.0.0.1:0".to_string(), // OS-allocated
            log_level: "info".to_string(),
            tcp: beava_core::config::TcpConfig {
                port: 0,
                ..Default::default()
            },
            durability: beava_core::config::DurabilityConfig {
                wal_dir: default_wal_dir,
                wal_fsync_interval_ms: 1,
                // Tests sweep aggressively to exercise expiry paths.
                dedupe_sweep_interval_secs: 1,
                snapshot_dir: default_snapshot_dir,
                // Tests should not auto-snapshot during normal flow; bump
                // interval to a long value and force-trigger via
                // TestServer::force_snapshot_now where needed.
                snapshot_interval_ms: 60_000,
                snapshot_retain_count: 2,
                ..Default::default()
            },
            // Plan 12-07: admin_addr must be OS-allocated for tests to avoid
            // colliding with the default 127.0.0.1:8090.
            admin_addr: "127.0.0.1:0".to_string(),
        };
        Self {
            cfg,
            readiness_timeout: Duration::from_secs(5),
            readiness_poll_interval: Duration::from_millis(20),
            dev_endpoints: false,
        }
    }
}

fn uniq_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

impl TestServerBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn listen_addr(mut self, addr: impl Into<String>) -> Self {
        self.cfg.listen_addr = addr.into();
        self
    }

    pub fn log_level(mut self, lvl: impl Into<String>) -> Self {
        self.cfg.log_level = lvl.into();
        self
    }

    pub fn readiness_timeout(mut self, t: Duration) -> Self {
        self.readiness_timeout = t;
        self
    }

    /// Enable the GET /registry dev endpoint on the spawned server.
    /// Passes `dev_endpoints=true` directly to `Server::bind` — no env-var
    /// mutation needed, so no lock is held across the await.
    pub fn dev_endpoints(mut self, enabled: bool) -> Self {
        self.dev_endpoints = enabled;
        self
    }

    /// Phase 2.5: enable / disable the TCP wire listener. Default: true.
    pub fn tcp_enabled(mut self, enabled: bool) -> Self {
        self.cfg.tcp.enabled = enabled;
        self
    }

    /// Phase 2.5: override the TCP listen port. Default: 0 (OS-assigned).
    pub fn tcp_port(mut self, port: u16) -> Self {
        self.cfg.tcp.port = port;
        self
    }

    /// Phase 2.5: override the TCP listen host. Default: 127.0.0.1.
    pub fn tcp_host(mut self, host: impl Into<String>) -> Self {
        self.cfg.tcp.host = host.into();
        self
    }

    /// Phase 2.5: override the max frame bytes for the TCP listener.
    /// Default: 4 MiB. Use a small value for oversize-frame smoke tests.
    ///
    /// Plan 12.6-15: also sets `BEAVA_TCP_MAX_FRAME_BYTES` so the mio
    /// `parse_wire_request` worker reads the same limit (the legacy
    /// `cfg.tcp.max_frame_bytes` field is preserved for source-compat
    /// but the active mio path is env-driven). Process-global; tests
    /// shouldn't run two TestServers with different limits in parallel.
    pub fn tcp_max_frame_bytes(mut self, bytes: u32) -> Self {
        self.cfg.tcp.max_frame_bytes = bytes;
        std::env::set_var("BEAVA_TCP_MAX_FRAME_BYTES", bytes.to_string());
        self
    }

    /// Phase 6 Plan 03: override the WAL directory. Tests pass a per-test
    /// `tempfile::tempdir()` path to avoid cross-test pollution.
    pub fn wal_dir(mut self, dir: std::path::PathBuf) -> Self {
        self.cfg.durability.wal_dir = dir;
        self
    }

    /// Phase 7 Plan 03: override the snapshot directory.
    pub fn snapshot_dir(mut self, dir: std::path::PathBuf) -> Self {
        self.cfg.durability.snapshot_dir = dir;
        self
    }

    /// Phase 7 Plan 03: override the periodic snapshot cadence.
    pub fn snapshot_interval_ms(mut self, ms: u64) -> Self {
        self.cfg.durability.snapshot_interval_ms = ms;
        self
    }

    /// Phase 6 Plan 03: override the group-commit coalesce interval in ms.
    /// Default for tests: 1 ms (keeps tests fast without fighting macOS
    /// `F_FULLSYNC` latency).
    pub fn fsync_interval_ms(mut self, ms: u64) -> Self {
        self.cfg.durability.wal_fsync_interval_ms = ms;
        self
    }

    /// Spawn the server, wait for `/ready` to report 200, return the handle.
    ///
    /// Plan 12.6-01 (D-01): boots `ServerV18` (mio data plane + tokio admin
    /// sidecar) instead of the legacy axum `Server`.
    ///
    /// - HTTP event-plane on `cfg.listen_addr` (typically `127.0.0.1:0` for
    ///   tests; mio routes `/health`, `/push`, `/get`, `/register`, etc).
    /// - TCP event-plane on `(cfg.tcp.host, cfg.tcp.port)` when
    ///   `tcp_enabled(true)`.
    /// - Admin endpoints (`/health`, `/ready`, `/metrics`, `/registry`) on
    ///   `cfg.admin_addr` (a separate OS-allocated port). `wait_ready`
    ///   polls the ADMIN port — `/ready` is admin-only on ServerV18.
    pub async fn spawn(self) -> Result<TestServer, TestServerError> {
        // Plan 12.6-14: dev_endpoints is now a real toggle on the mio
        // data plane (not just the admin sidecar). The flag is applied
        // post-bind via `app_state.dev_endpoints.store(...)` once we
        // have the Arc<AppState> handle below.

        // Resolve the configured event-plane HTTP/TCP addresses + admin
        // address, all permitted to be `*:0` (OS-allocated).
        let http_addr: SocketAddr = self.cfg.listen_addr.parse().map_err(|e| {
            TestServerError::Server(ServerError::InvalidAddr(
                self.cfg.listen_addr.clone(),
                format!("{e}"),
            ))
        })?;
        let admin_addr: SocketAddr = self.cfg.admin_addr.parse().map_err(|e| {
            TestServerError::Server(ServerError::InvalidAddr(
                self.cfg.admin_addr.clone(),
                format!("{e}"),
            ))
        })?;
        // The TCP event-plane addr derives from cfg.tcp.host/port. When
        // tcp.enabled=false the listener binds anyway (mio always accepts
        // TCP) but we set tcp_addr=None on TestServer so callers see the
        // legacy "TCP-disabled" UX. Per Plan 12.6 D-01 we preserve the
        // disabled/enabled split via the TestServer wrapper, not the
        // ServerV18 listener.
        let tcp_addr_str = format!("{}:{}", self.cfg.tcp.host, self.cfg.tcp.port);
        let tcp_addr_socket: SocketAddr = tcp_addr_str.parse().map_err(|e| {
            TestServerError::Server(ServerError::InvalidAddr(
                tcp_addr_str.clone(),
                format!("{e}"),
            ))
        })?;

        let sv18 = ServerV18::bind_with_state(
            http_addr,
            tcp_addr_socket,
            admin_addr,
            self.cfg.durability.wal_dir.clone(),
            self.cfg.durability.snapshot_dir.clone(),
            self.cfg.durability.snapshot_interval_ms.max(1),
            self.cfg.durability.wal_fsync_interval_ms.max(1),
        )
        .await?;

        let base_url = format!("http://{}", sv18.http_addr());
        let admin_url = format!("http://{}", sv18.admin_addr());
        let tcp_addr = if self.cfg.tcp.enabled {
            Some(sv18.tcp_addr())
        } else {
            None
        };

        // Grab Arc clones BEFORE `serve_with_dirs` consumes `sv18`.
        let registry = sv18.registry();
        let snapshot_trigger = sv18.snapshot_trigger_handle();
        let app_state = sv18.app_state();
        // Plan 12.6-14: apply the builder's `dev_endpoints` flag to the
        // shared AppState so `/registry` on the mio data plane gates
        // identically to the legacy axum `BEAVA_DEV_ENDPOINTS=1`
        // toggle. (Default false; `.dev_endpoints(true)` flips it.)
        app_state
            .dev_endpoints
            .store(self.dev_endpoints, std::sync::atomic::Ordering::Relaxed);

        let (tx, rx) = oneshot::channel::<()>();
        let shutdown = async move {
            let _ = rx.await;
        };

        // `serve_with_dirs` consumes `sv18`, picking up the pre-built
        // state from `bind_with_state` and running the apply thread until
        // `shutdown` resolves.  The wal/snapshot dirs are passed again
        // for back-compat — the prebuilt path ignores them in favor of
        // what was used at bind-time.
        let wal_dir = self.cfg.durability.wal_dir.clone();
        let snap_dir = self.cfg.durability.snapshot_dir.clone();
        let serve_task: JoinHandle<Result<(), ServerError>> =
            tokio::spawn(async move { sv18.serve_with_dirs(shutdown, wal_dir, snap_dir).await });

        let harness = TestServer {
            base_url,
            admin_url,
            tcp_addr,
            shutdown_tx: Some(tx),
            serve_task: Some(serve_task),
            registry,
            snapshot_trigger,
            app_state,
        };

        // /ready is on the admin port for ServerV18 (data plane has no
        // /ready route — `project_phase18_no_dual_runtime`).
        harness
            .wait_ready(self.readiness_timeout, self.readiness_poll_interval)
            .await?;

        Ok(harness)
    }
}

pub struct TestServer {
    base_url: String,
    /// Plan 12.6-01: URL of the tokio admin sidecar (where `/ready`,
    /// `/metrics`, `/registry`, `/health` live on a port distinct from
    /// `base_url`).  Per `project_phase18_no_dual_runtime` the data plane
    /// (mio) and admin plane (tokio) bind separate ports.
    admin_url: String,
    tcp_addr: Option<SocketAddr>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    serve_task: Option<JoinHandle<Result<(), ServerError>>>,
    /// Shared registry — same Arc the server uses internally.  Phase 4 acceptance
    /// tests call `.registry().compiled_chain(name)` to assert in-process state
    /// without a round-trip through an HTTP endpoint.
    registry: Arc<beava_core::registry::Registry>,
    /// Phase 7 Plan 03: handle to the snapshot task's manual-trigger channel.
    snapshot_trigger: crate::snapshot_task::SnapshotTriggerTx,
    /// Phase 18 Plan 01: shared AppState Arc — used by glue-layer tests that
    /// call `dispatch_wire_request` directly without going through HTTP.
    app_state: Arc<crate::AppState>,
}

impl TestServer {
    pub async fn spawn() -> Result<Self, TestServerError> {
        TestServerBuilder::default().spawn().await
    }

    pub fn builder() -> TestServerBuilder {
        TestServerBuilder::default()
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Plan 12.6-01: URL of the tokio admin sidecar.  `/ready`, `/metrics`,
    /// `/registry`, `/health` all live here on a port distinct from
    /// `base_url`. Returned as `&str` to mirror `base_url`.
    pub fn admin_url(&self) -> &str {
        &self.admin_url
    }

    /// Phase 2.5: TCP listener address, or None when `tcp.enabled = false`
    /// (HTTP-only mode). Populated from `ServerV18::tcp_addr()` at spawn.
    pub fn tcp_addr(&self) -> Option<SocketAddr> {
        self.tcp_addr
    }

    /// Phase 4: Direct reference to the shared registry Arc.
    /// Acceptance tests call `.registry().compiled_chain(name)` for in-process
    /// assertions without an HTTP round-trip.
    pub fn registry(&self) -> &Arc<beava_core::registry::Registry> {
        &self.registry
    }

    /// Phase 18 Plan 01: return the shared AppState Arc.
    /// Glue-layer tests call `dispatch_wire_request(&ts.app_state(), req)` to
    /// exercise the apply path without going through HTTP.
    pub fn app_state(&self) -> Arc<crate::AppState> {
        Arc::clone(&self.app_state)
    }

    /// Phase 7 Plan 03: force the periodic snapshot task to run NOW.
    /// Resolves once the snapshot has been written, WAL truncated, and old
    /// snapshots pruned. Returns an error if the snapshot task has stopped
    /// or the snapshot itself failed.
    pub async fn force_snapshot_now(&self) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.snapshot_trigger
            .send(tx)
            .await
            .map_err(|_| "snapshot task channel closed".to_string())?;
        rx.await
            .map_err(|_| "snapshot task ack channel dropped".to_string())?
    }

    /// Phase 2.5: Connect a `TcpClient` to the TCP listener. Returns an error if
    /// the listener is not enabled (caller should `.tcp_enabled(true)` on the
    /// builder — it defaults to true, so this only fails when explicitly disabled).
    pub async fn tcp_client(&self) -> std::io::Result<TcpClient> {
        let addr = self.tcp_addr.ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "TCP listener is not enabled on this TestServer; call .tcp_enabled(true)",
            )
        })?;
        TcpClient::connect(addr).await
    }

    /// POST arbitrary JSON body to `path`. Returns the raw reqwest Response so
    /// callers can assert on status and parse the body themselves.
    pub async fn post_json<B: Serialize + ?Sized>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let url = format!("{}{}", self.base_url, path);
        reqwest::Client::new()
            .post(&url)
            .header("Content-Type", "application/json")
            .json(body)
            .timeout(Duration::from_secs(5))
            .send()
            .await
    }

    /// GET `path`; parse response body as JSON. Panics if non-2xx OR non-JSON.
    /// For error-path tests use `get_raw` instead.
    pub async fn get_json(&self, path: &str) -> serde_json::Value {
        let url = format!("{}{}", self.base_url, path);
        let resp = reqwest::Client::new()
            .get(&url)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .expect("GET request");
        assert!(
            resp.status().is_success(),
            "GET {} returned {}",
            path,
            resp.status()
        );
        resp.json().await.expect("JSON body")
    }

    /// Raw GET that does NOT assert on status. Use for 404 / 503 / error-path tests.
    pub async fn get_raw(&self, path: &str) -> reqwest::Response {
        let url = format!("{}{}", self.base_url, path);
        reqwest::Client::new()
            .get(&url)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .expect("GET request")
    }

    async fn wait_ready(
        &self,
        timeout: Duration,
        poll_interval: Duration,
    ) -> Result<(), TestServerError> {
        // Plan 12.6-01: poll the admin port — `/ready` lives on the tokio
        // admin sidecar in ServerV18, not the mio data plane (D-01 +
        // `project_phase18_no_dual_runtime`).
        let url = format!("{}/ready", self.admin_url);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(500))
            .build()
            .expect("build reqwest client");

        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(TestServerError::ReadinessTimeout(timeout));
            }
            match client.get(&url).send().await {
                Ok(r) if r.status().as_u16() == 200 => return Ok(()),
                _ => tokio::time::sleep(poll_interval).await,
            }
        }
    }

    /// Trigger graceful shutdown and await the serve task. Idempotent.
    pub async fn shutdown(mut self) -> Result<(), TestServerError> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.serve_task.take() {
            let serve_result = tokio::time::timeout(Duration::from_secs(2), task)
                .await
                .map_err(|_| {
                    TestServerError::Join("serve task did not exit within 2s".to_string())
                })?;
            match serve_result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(TestServerError::Server(e)),
                Err(join_err) => Err(TestServerError::Join(join_err.to_string())),
            }
        } else {
            Ok(())
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        // Fire-and-forget shutdown on drop. The serve task will observe the channel
        // closed (if not yet signalled) — `ServerV18::serve_with_dirs` awaits the
        // shutdown future and exits when the sender is dropped (the receiver errors).
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        // JoinHandle drop detaches the task; it will still exit cleanly because
        // shutdown was signalled above.
    }
}

// ─── Phase 2.5: TcpClient test helper ─────────────────────────────────────────

/// Thin async helper that speaks the Beava TCP wire. Test-only — production
/// SDK clients live in Python (Phase 3). Each method encodes one frame, writes
/// it, and decodes the response frame. Pipelining is supported via
/// `write_frame` + `read_n_frames`.
pub struct TcpClient {
    stream: TcpStream,
    read_buf: BytesMut,
    max_frame_bytes: u32,
}

impl std::fmt::Debug for TcpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpClient")
            .field("max_frame_bytes", &self.max_frame_bytes)
            .field("read_buf_len", &self.read_buf.len())
            .finish_non_exhaustive()
    }
}

impl TcpClient {
    pub async fn connect(addr: SocketAddr) -> std::io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        // Disable Nagle so small frames (pings) go out immediately.
        let _ = stream.set_nodelay(true);
        Ok(Self {
            stream,
            read_buf: BytesMut::with_capacity(8 * 1024),
            // Client-side cap; the server-side max is the authoritative limit.
            max_frame_bytes: 16 * 1024 * 1024,
        })
    }

    /// Low-level: write one frame, read one frame. Strict FIFO.
    pub async fn send_raw(
        &mut self,
        op: u16,
        content_type: u8,
        payload: impl Into<Bytes>,
    ) -> anyhow::Result<Frame> {
        self.write_frame(&Frame {
            op,
            content_type,
            payload: payload.into(),
        })
        .await?;
        self.read_one_frame().await
    }

    /// Write one frame without reading a response. Used by pipelining tests.
    pub async fn write_frame(&mut self, frame: &Frame) -> anyhow::Result<()> {
        let mut buf = BytesMut::new();
        encode_frame(frame, &mut buf);
        self.stream.write_all(&buf).await.map_err(Into::into)
    }

    /// Read exactly one response frame. Returns anyhow error on decode error
    /// or premature EOF.
    pub async fn read_one_frame(&mut self) -> anyhow::Result<Frame> {
        loop {
            if let Some(f) = decode_frame(&mut self.read_buf, self.max_frame_bytes)? {
                return Ok(f);
            }
            let n = self.stream.read_buf(&mut self.read_buf).await?;
            if n == 0 {
                anyhow::bail!("connection closed before complete frame");
            }
        }
    }

    /// Read N frames in strict FIFO order (for pipelining assertions).
    pub async fn read_n_frames(&mut self, n: usize) -> anyhow::Result<Vec<Frame>> {
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(self.read_one_frame().await?);
        }
        Ok(out)
    }

    /// High-level: send OP_PING; parse response body as JSON.
    pub async fn ping(&mut self) -> anyhow::Result<serde_json::Value> {
        let resp = self.send_raw(OP_PING, CT_JSON, Bytes::new()).await?;
        anyhow::ensure!(
            resp.op == OP_PING,
            "expected OP_PING response, got {:#06x}",
            resp.op
        );
        anyhow::ensure!(
            resp.content_type == CT_JSON,
            "expected CT_JSON, got {:#04x}",
            resp.content_type
        );
        Ok(serde_json::from_slice(&resp.payload)?)
    }

    /// High-level: send OP_REGISTER with a JSON DAG. Returns (response_op,
    /// parsed_body) so the test can distinguish OP_REGISTER (success) from
    /// OP_ERROR_RESPONSE (validation / conflict).
    pub async fn register_json(
        &mut self,
        body: serde_json::Value,
    ) -> anyhow::Result<(u16, serde_json::Value)> {
        let payload = serde_json::to_vec(&body)?;
        let resp = self
            .send_raw(OP_REGISTER, CT_JSON, Bytes::from(payload))
            .await?;
        let parsed: serde_json::Value = serde_json::from_slice(&resp.payload)?;
        Ok((resp.op, parsed))
    }

    /// High-level: send OP_PUSH with a JSON envelope `{event, body}` (Phase 8
    /// folded scope: TCP push handler). Returns `(response_op, parsed_body)`.
    pub async fn push_json(
        &mut self,
        event_name: &str,
        body: serde_json::Value,
    ) -> anyhow::Result<(u16, serde_json::Value)> {
        let envelope = serde_json::json!({
            "event": event_name,
            "body": body,
        });
        let payload = serde_json::to_vec(&envelope)?;
        let resp = self
            .send_raw(beava_core::wire::OP_PUSH, CT_JSON, Bytes::from(payload))
            .await?;
        let parsed: serde_json::Value = serde_json::from_slice(&resp.payload)?;
        Ok((resp.op, parsed))
    }

    /// Attempt to read one more frame OR observe EOF within `timeout`.
    /// Returns Ok(Some(frame)) if a frame arrived, Ok(None) if EOF, Err on
    /// timeout. Used to confirm the connection closes after frame_too_large.
    pub async fn read_or_eof(&mut self, timeout: Duration) -> anyhow::Result<Option<Frame>> {
        let fut = async {
            loop {
                if let Some(f) = decode_frame(&mut self.read_buf, self.max_frame_bytes)? {
                    return anyhow::Ok(Some(f));
                }
                let n = self.stream.read_buf(&mut self.read_buf).await?;
                if n == 0 {
                    return anyhow::Ok(None);
                }
            }
        };
        tokio::time::timeout(timeout, fut)
            .await
            .map_err(|_| anyhow::anyhow!("read_or_eof timed out after {:?}", timeout))?
    }

    /// Explicit close. `drop(self)` also closes.
    pub async fn close(mut self) -> std::io::Result<()> {
        self.stream.shutdown().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn spawn_serves_health() {
        let ts = TestServer::spawn().await.expect("spawn");
        let url = format!("{}/health", ts.base_url());
        let resp = reqwest::get(&url).await.expect("health req");
        assert_eq!(resp.status().as_u16(), 200);
        let json: serde_json::Value = resp.json().await.expect("json");
        assert_eq!(json, serde_json::json!({ "status": "ok" }));
        ts.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn two_test_servers_use_different_ports() {
        let a = TestServer::spawn().await.expect("spawn a");
        let b = TestServer::spawn().await.expect("spawn b");
        assert_ne!(a.base_url(), b.base_url(), "expected distinct ports");
        a.shutdown().await.ok();
        b.shutdown().await.ok();
    }

    #[tokio::test]
    async fn shutdown_exits_within_budget() {
        let ts = TestServer::spawn().await.expect("spawn");
        let start = std::time::Instant::now();
        ts.shutdown().await.expect("shutdown");
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(500),
            "shutdown took {elapsed:?}, expected <500ms"
        );
    }

    #[tokio::test]
    async fn readiness_wait_respects_timeout() {
        let ts = TestServer::builder()
            .readiness_timeout(Duration::from_secs(1))
            .spawn()
            .await
            .expect("spawn");
        ts.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn drop_without_explicit_shutdown_does_not_hang() {
        let base_url = {
            let ts = TestServer::spawn().await.expect("spawn");
            ts.base_url().to_string()
            // ts drops here without explicit shutdown
        };
        // Give the background task a beat to see the dropped tx and exit.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let _ = base_url; // keep clippy happy
    }

    #[tokio::test]
    async fn post_json_returns_response() {
        let ts = TestServer::spawn().await.expect("spawn");
        let body = serde_json::json!({"nodes": []});
        let resp = ts.post_json("/register", &body).await.expect("post_json");
        assert_eq!(resp.status().as_u16(), 200);
        let val: serde_json::Value = resp.json().await.expect("json");
        assert_eq!(val["status"], "ok");
        assert_eq!(val["registry_version"], 0);
        ts.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn post_json_404_on_unknown_path() {
        let ts = TestServer::spawn().await.expect("spawn");
        let body = serde_json::json!({});
        let resp = ts.post_json("/nope", &body).await.expect("post_json");
        assert_eq!(resp.status().as_u16(), 404);
        ts.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn get_raw_returns_non_success_without_panicking() {
        let ts = TestServer::spawn().await.expect("spawn");
        let resp = ts.get_raw("/nope").await;
        assert_eq!(
            resp.status().as_u16(),
            404,
            "get_raw should not panic on 404"
        );
        ts.shutdown().await.expect("shutdown");
    }

    // ─── Phase 2.5 TCP harness tests ──────────────────────────────────────────

    fn valid_event_body() -> serde_json::Value {
        serde_json::json!({
            "nodes": [{
                "kind": "event",
                "name": "Transaction",
                "schema": {
                    "fields": {"event_time": "i64", "amount": "f64"},
                    "optional_fields": []
                },
                "event_time_field": "event_time"
            }]
        })
    }

    #[tokio::test]
    async fn tcp_client_connect_and_ping() {
        let ts = TestServer::spawn().await.expect("spawn");
        let addr = ts.tcp_addr().expect("tcp bound");
        let mut c = TcpClient::connect(addr).await.expect("connect");
        let body = c.ping().await.expect("ping");
        assert_eq!(body["server_version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(body["registry_version"], 0);
        ts.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn tcp_client_register_via_helper() {
        let ts = TestServer::spawn().await.expect("spawn");
        let mut c = ts.tcp_client().await.expect("tcp client");
        let (op, body) = c.register_json(valid_event_body()).await.expect("register");
        assert_eq!(op, OP_REGISTER);
        assert_eq!(body["status"], "ok");
        assert_eq!(body["registry_version"], 1);
        ts.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn tcp_client_send_raw_returns_frame() {
        let ts = TestServer::spawn().await.expect("spawn");
        let mut c = ts.tcp_client().await.expect("tcp client");
        let resp = c
            .send_raw(OP_PING, CT_JSON, Bytes::new())
            .await
            .expect("send_raw");
        assert_eq!(resp.op, OP_PING);
        ts.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn tcp_client_read_n_frames_pipelining() {
        let ts = TestServer::spawn().await.expect("spawn");
        let mut c = ts.tcp_client().await.expect("tcp client");
        // Write 2 pings without awaiting
        for _ in 0..2 {
            c.write_frame(&Frame {
                op: OP_PING,
                content_type: CT_JSON,
                payload: Bytes::new(),
            })
            .await
            .expect("write");
        }
        let frames = c.read_n_frames(2).await.expect("read 2");
        assert_eq!(frames[0].op, OP_PING);
        assert_eq!(frames[1].op, OP_PING);
        ts.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn tcp_client_read_or_eof_returns_none_on_close() {
        let ts = TestServer::spawn().await.expect("spawn");
        let mut c = ts.tcp_client().await.expect("tcp client");
        // Trigger shutdown to close the connection.
        ts.shutdown().await.expect("shutdown");
        // After shutdown, next read should see EOF or connection-reset (OS may
        // send RST instead of FIN when closing with no pending data). Both are
        // valid "connection closed" outcomes — the important bit is that it
        // resolves within the timeout (i.e., the server actually closed).
        let result = c.read_or_eof(Duration::from_secs(2)).await;
        match result {
            Ok(None) => { /* clean EOF */ }
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("reset") || msg.contains("closed"),
                    "expected EOF or reset, got: {msg}"
                );
            }
            Ok(Some(f)) => panic!("expected EOF, got frame op={:#06x}", f.op),
        }
    }

    #[tokio::test]
    async fn tcp_client_read_or_eof_returns_frame_when_available() {
        let ts = TestServer::spawn().await.expect("spawn");
        let mut c = ts.tcp_client().await.expect("tcp client");
        c.write_frame(&Frame {
            op: OP_PING,
            content_type: CT_JSON,
            payload: Bytes::new(),
        })
        .await
        .expect("write");
        let f = c
            .read_or_eof(Duration::from_secs(2))
            .await
            .expect("timeout ok");
        assert!(f.is_some());
        assert_eq!(f.unwrap().op, OP_PING);
        ts.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn test_server_tcp_addr_is_some_when_enabled() {
        let ts = TestServer::spawn().await.expect("spawn");
        assert!(ts.tcp_addr().is_some());
        ts.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn test_server_tcp_addr_is_none_when_disabled() {
        let ts = TestServerBuilder::new()
            .tcp_enabled(false)
            .spawn()
            .await
            .expect("spawn");
        assert!(ts.tcp_addr().is_none());
        ts.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn tcp_client_errors_when_tcp_disabled() {
        let ts = TestServerBuilder::new()
            .tcp_enabled(false)
            .spawn()
            .await
            .expect("spawn");
        let err = ts.tcp_client().await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotConnected);
        ts.shutdown().await.expect("shutdown");
    }

    // ─── Plan 12.6-01 Task 1.a/1.b — TestServer must back ServerV18 ──────────
    //
    // Per Phase 12.6 D-01, TestServer's internals are rewritten to wrap
    // ServerV18 (mio data plane) instead of the legacy axum `Server`.  The
    // following three tests pin that contract — added in the RED commit
    // (test(12.6-01): add failing tests …) and turned GREEN by the rewrite
    // commit (feat(12.6-01): rewrite TestServer harness …).

    /// Task 1.a Test 1: data-plane HTTP listener is the mio path.
    ///
    /// Discriminator: GET `/ready` on the data-plane port returns 200 (mio
    /// added a back-compat shim in Plan 12.6-01); the body is
    /// `{"status":"ready"}` — same as the admin sidecar's /ready.  This is
    /// distinct from the legacy axum Server, which returned 200 from the
    /// SAME port for both /ready (event-plane) and /health.  After the
    /// rewrite both ports answer /ready, but `admin_url()` is on a
    /// different port — verified by `test_server_v18_admin_endpoints_separate_port`.
    #[tokio::test]
    async fn test_server_v18_uses_mio_dataplane() {
        let ts = TestServer::spawn().await.expect("spawn");
        // /health on the data-plane MUST return 200 — mio HTTP listener
        // routes /health.
        let hr = ts.get_raw("/health").await;
        assert_eq!(
            hr.status().as_u16(),
            200,
            "GET /health on the data-plane MUST return 200 — \
             mio HTTP listener routes /health"
        );
        // /ready on the data-plane MUST return 200 with body
        // `{"status":"ready"}` (Plan 12.6-01 back-compat shim).
        let resp = ts.get_raw("/ready").await;
        assert_eq!(
            resp.status().as_u16(),
            200,
            "GET /ready on the data-plane MUST return 200 — \
             mio HTTP listener has a back-compat /ready shim"
        );
        let body: serde_json::Value = resp.json().await.expect("json body");
        assert_eq!(
            body,
            serde_json::json!({ "status": "ready" }),
            "GET /ready body must mirror the admin sidecar"
        );
        // An UNKNOWN path on the data-plane MUST return 404 (route lookup
        // fell off the table).  Pre-rewrite the mio path emitted 501
        // Unsupported here (`Route::NotFound -> ParseError`); Plan 12.6-01
        // wires `Route::NotFound -> WireRequest::HttpNotFound -> 404`.
        let nf = ts.get_raw("/does-not-exist").await;
        assert_eq!(
            nf.status().as_u16(),
            404,
            "Unknown HTTP paths on the data-plane MUST return 404"
        );
        ts.shutdown().await.expect("shutdown");
    }

    /// Task 1.a Test 2: TestServer preserves the public builder API surface
    /// per D-01 — every builder method existing pre-rewrite must keep its
    /// signature and produce a running server.  Tests use `admin_url()` as
    /// the post-rewrite accessor that pins ServerV18-shaped output (RED via
    /// compile-error today; GREEN once 1.b adds `admin_url()`).
    #[tokio::test]
    async fn test_server_preserves_builder_api() {
        let wal = tempfile::tempdir().expect("tempdir wal");
        let snap = tempfile::tempdir().expect("tempdir snap");
        let ts = TestServer::builder()
            .tcp_enabled(true)
            .wal_dir(wal.path().to_path_buf())
            .snapshot_dir(snap.path().to_path_buf())
            .fsync_interval_ms(1)
            .snapshot_interval_ms(60_000)
            .listen_addr("127.0.0.1:0")
            .log_level("info")
            .readiness_timeout(Duration::from_secs(5))
            .dev_endpoints(false)
            .spawn()
            .await
            .expect("spawn must succeed with full builder chain");

        assert!(
            ts.tcp_addr().is_some(),
            "tcp_addr must be Some when tcp_enabled(true)"
        );

        // ServerV18-specific accessor: admin URL.  This is the contract
        // signal that `spawn()` ran ServerV18::bind_with_state (legacy
        // Server has no admin port concept).
        let admin_url = ts.admin_url().to_string();
        assert!(
            admin_url.starts_with("http://"),
            "admin_url must be an http URL, got {admin_url}"
        );
        assert_ne!(
            admin_url,
            ts.base_url(),
            "admin URL must be on a different port from the data-plane base_url"
        );

        ts.shutdown().await.expect("shutdown");
    }

    /// Task 1.a Test 3: ServerV18 separates admin endpoints (`/health`,
    /// `/ready`, `/metrics`, `/registry`) onto a port distinct from the
    /// event-plane HTTP listener — `project_phase18_no_dual_runtime`
    /// invariant.
    #[tokio::test]
    async fn test_server_v18_admin_endpoints_separate_port() {
        let ts = TestServer::spawn().await.expect("spawn");
        let admin_url = ts.admin_url().to_string();
        assert_ne!(
            admin_url,
            ts.base_url(),
            "admin URL must differ from event-plane base_url"
        );

        // `/ready` MUST live on the admin port.
        let resp = reqwest::get(format!("{}/ready", admin_url))
            .await
            .expect("admin /ready GET");
        assert_eq!(
            resp.status().as_u16(),
            200,
            "admin /ready must return 200 on the admin port"
        );

        // `/health` MUST live on the admin port too.
        let resp = reqwest::get(format!("{}/health", admin_url))
            .await
            .expect("admin /health GET");
        assert_eq!(
            resp.status().as_u16(),
            200,
            "admin /health must return 200 on the admin port"
        );
        ts.shutdown().await.expect("shutdown");
    }
}
