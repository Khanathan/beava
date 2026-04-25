//! beava-runtime-core — hand-rolled mio-based event loop (Phase 18).
//!
//! Replaces tokio on the data-plane hot path (TCP + HTTP) per Phase 18 plan.
//! Admin endpoints remain on tokio/axum on a separate port (D-01).
//!
//! # Crate structure
//!
//! - `event_loop` — `EventLoop` wrapping `mio::Poll` + `mio::Events`
//! - `client`     — per-connection state machine (`BytesMut` read buf, response queue)
//! - `tcp_listener` — framed TCP listener + parser (Phase 2.5 wire format)
//! - `http_listener` — HTTP/1.1 listener via `httparse`
//! - `router`     — path dispatch for HTTP requests
//! - `response`   — pre-encoded byte-string response templates (hot path, no serde)

pub mod client;
pub mod event_loop;
pub mod http_listener;
pub mod response;
pub mod router;
pub mod tcp_listener;
pub mod wire_request;

pub use client::Client;
pub use event_loop::{EventLoop, EventLoopError};
pub use http_listener::HttpListener;
pub use response::ResponseTemplate;
pub use router::{Route, Router};
pub use tcp_listener::TcpListener;
pub use wire_request::WireRequest;
