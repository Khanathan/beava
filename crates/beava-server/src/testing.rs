//! In-process integration-test harness for the beava server.
//!
//! Used by Phase 1's `foundation_smoke.rs` and every subsequent phase's HTTP tests.
//! Spawns a real `Server` on an OS-allocated port, waits for readiness via `/ready`,
//! hands back a `TestServer` whose `.base_url()` can be curled, and shuts down
//! gracefully on `.shutdown().await`.
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

#![cfg(any(feature = "testing", test))]

use crate::server::{Server, ServerError};
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
        let cfg = Config {
            listen_addr: "127.0.0.1:0".to_string(), // OS-allocated
            log_level: "info".to_string(),
            tcp: beava_core::config::TcpConfig {
                port: 0,
                ..Default::default()
            },
        };
        Self {
            cfg,
            readiness_timeout: Duration::from_secs(5),
            readiness_poll_interval: Duration::from_millis(20),
            dev_endpoints: false,
        }
    }
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
    pub fn tcp_max_frame_bytes(mut self, bytes: u32) -> Self {
        self.cfg.tcp.max_frame_bytes = bytes;
        self
    }

    /// Spawn the server, wait for `/ready` to report 200, return the handle.
    pub async fn spawn(self) -> Result<TestServer, TestServerError> {
        let server = Server::bind(&self.cfg, self.dev_endpoints).await?;
        let base_url = format!("http://{}", server.local_addr());
        let tcp_addr = server.tcp_local_addr();
        // Grab the shared registry Arc before `serve()` consumes the Server.
        let registry = server.registry();

        let (tx, rx) = oneshot::channel::<()>();
        let shutdown = async move {
            let _ = rx.await;
        };

        let serve_task: JoinHandle<Result<(), ServerError>> =
            tokio::spawn(async move { server.serve(shutdown).await });

        let harness = TestServer {
            base_url,
            tcp_addr,
            shutdown_tx: Some(tx),
            serve_task: Some(serve_task),
            registry,
        };

        harness
            .wait_ready(self.readiness_timeout, self.readiness_poll_interval)
            .await?;

        Ok(harness)
    }
}

pub struct TestServer {
    base_url: String,
    tcp_addr: Option<SocketAddr>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    serve_task: Option<JoinHandle<Result<(), ServerError>>>,
    /// Shared registry — same Arc the server uses internally.  Phase 4 acceptance
    /// tests call `.registry().compiled_chain(name)` to assert in-process state
    /// without a round-trip through an HTTP endpoint.
    registry: Arc<beava_core::registry::Registry>,
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

    /// Phase 2.5: TCP listener address, or None when `tcp.enabled = false`
    /// (HTTP-only mode). Populated from `Server::tcp_local_addr()` at spawn.
    pub fn tcp_addr(&self) -> Option<SocketAddr> {
        self.tcp_addr
    }

    /// Phase 4: Direct reference to the shared registry Arc.
    /// Acceptance tests call `.registry().compiled_chain(name)` for in-process
    /// assertions without an HTTP round-trip.
    pub fn registry(&self) -> &Arc<beava_core::registry::Registry> {
        &self.registry
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
        let url = format!("{}/ready", self.base_url);
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
        // closed (if not yet signalled) — axum's graceful shutdown future awakens when
        // the channel's sender is dropped and the receiver errors.
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
}
