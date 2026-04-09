use std::sync::{Arc, Mutex};

use tally::engine::pipeline::PipelineEngine;
use tally::server::http::run_http_server;
use tally::server::tcp::{AppState, run_tcp_server};
use tally::state::store::StateStore;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let tcp_port = std::env::var("TALLY_TCP_PORT").unwrap_or_else(|_| "6400".into());
    let http_port = std::env::var("TALLY_HTTP_PORT").unwrap_or_else(|_| "6401".into());

    let tcp_addr = format!("0.0.0.0:{}", tcp_port);
    let http_addr = format!("0.0.0.0:{}", http_port);

    let state = Arc::new(Mutex::new(AppState {
        engine: PipelineEngine::new(),
        store: StateStore::new(),
    }));

    let tcp_state = state.clone();
    let tcp_handle = tokio::spawn(async move {
        if let Err(e) = run_tcp_server(&tcp_addr, tcp_state).await {
            eprintln!("TCP server error: {}", e);
        }
    });

    let http_state = state.clone();
    let http_handle = tokio::spawn(async move {
        if let Err(e) = run_http_server(&http_addr, http_state).await {
            eprintln!("HTTP server error: {}", e);
        }
    });

    tokio::select! {
        _ = tcp_handle => {},
        _ = http_handle => {},
    }
}
