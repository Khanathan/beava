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

pub mod bytes_pool;
pub mod client;
pub mod config;
pub mod event_loop;
pub mod http_listener;
pub mod io_backend;
pub mod io_pool;
pub mod io_thread;
pub mod io_thread_worker;
pub mod response;
pub mod router;
pub mod tcp_listener;
pub mod wal_buffer;
pub mod wal_lsn;
pub mod wal_writer;
pub mod wire_request;
pub mod work_ring;

pub use client::{Client, ParseError};
pub use config::IoConfig;
pub use event_loop::{EventLoop, EventLoopError};
pub use http_listener::HttpListener;
pub use io_pool::IoPool;
pub use response::{serialize_into, ResponseTemplate, WireResponse};
pub use router::{Route, Router};
pub use tcp_listener::TcpListener;
pub use wal_buffer::{WalBuffer, WalBufferRing};
pub use wal_lsn::{Lsn, WalLsn};
pub use wal_writer::WalWriter;
pub use wire_request::WireRequest;
