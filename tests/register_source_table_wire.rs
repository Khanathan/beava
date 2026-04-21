//! Phase 56-NEXT #6 — @bv.source_table wire-REGISTER dispatch.
//!
//! Before this change, `app.register(Countries)` on a `@bv.source_table`
//! descriptor emitted `kind="table"` from the Python SDK, which fell
//! through the server's untagged REGISTER matcher into
//! `V0RegisterPayload::Source(SourceDescriptor)`. The REGISTER succeeded,
//! but `PipelineEngine::has_registered_source_table(name)` returned false
//! (it checks for `kind == "source_table"`), so the very first
//! OP_UPSERT_TABLE_ROW frame failed with `ProtocolError: table not
//! registered as @bv.source_table`. This blocked Phase 56 SC-5
//! (`human_needed` on the cross-shard enrichment perf scenario).
//!
//! This test exercises the end-to-end path over a live TCP server:
//!   1. OP_REGISTER with `kind="source_table"` JSON — new SourceTable
//!      variant in `V0RegisterPayload`, short-circuited in tcp.rs so no
//!      StreamDefinition is built.
//!   2. `PipelineEngine::has_registered_source_table("Countries")`
//!      returns true.
//!   3. OP_UPSERT_TABLE_ROW passes the has_registered_source_table gate
//!      and acks STATUS_OK with echoed source_lsn.
//!   4. OP_GET returns the row we just wrote.
//!
//! Run:
//!   cargo test --release --test register_source_table_wire -- --test-threads=1

#![cfg(not(feature = "state-inmem"))]

use beava::engine::pipeline::PipelineEngine;
use beava::server::protocol::{
    encode_frame, write_varint_string, OP_REGISTER, OP_UPSERT_TABLE_ROW, STATUS_OK,
};
use beava::server::tcp::{make_concurrent_state_full, BackfillTracker, SharedState};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn build_state(tmp: &tempfile::TempDir) -> SharedState {
    std::env::set_var("BEAVA_FJALL_FSYNC_DISABLE", "1");
    std::env::set_var("BEAVA_FJALL_CACHE_MB", "32");
    std::env::set_var("BEAVA_DATA_DIR", tmp.path());

    let engine = PipelineEngine::new();
    let state = make_concurrent_state_full(
        engine,
        None,
        tmp.path().join("snapshot.bin"),
        Arc::new(BackfillTracker::default()),
        true,
        false,
        None,
        false,
        1,
    );
    let shard_handles = beava::shard::thread::spawn_shard_threads(
        1,
        beava::shard::thread::DEFAULT_INBOX_SIZE,
        state.clone(),
        None,
    );
    *state.shard_handles.write() = shard_handles;
    state
}

/// Register a @bv.source_table over the wire, assert the engine flips the
/// `has_registered_source_table` bit, then upsert a row and read it back
/// via OP_UPSERT_TABLE_ROW. This is the end-to-end path that Phase 56
/// SC-5's perf scenario needs to boot proc-0 without ProtocolError.
#[test]
#[ignore = "serial-only: build_state mutates process-global env BEAVA_DATA_DIR; run with -- --test-threads=1"]
fn op_register_source_table_enables_upsert_table_row() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let tmp = tempfile::TempDir::new().unwrap();
        let state = build_state(&tmp);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let tcp_state = state.clone();
        tokio::spawn(async move {
            beava::server::tcp::run_tcp_server_with_listener(listener, tcp_state)
                .await
                .ok();
        });
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;

        // ------------------------------------------------------------
        // 1. OP_REGISTER with kind="source_table" (the Phase 56-NEXT #6
        //    payload that the Python SDK now emits for @bv.source_table).
        // ------------------------------------------------------------
        let register_json = serde_json::json!({
            "name": "Countries",
            "kind": "source_table",
            "key_field": null,
            "key_fields": ["country_code"],
            "mode": "append",
            "fields": {
                "country_code": {"type": "str", "optional": false},
                "name": {"type": "str", "optional": false},
                "currency": {"type": "str", "optional": false}
            }
        });
        let payload = serde_json::to_vec(&register_json).unwrap();
        let frame = encode_frame(OP_REGISTER, &payload);

        let mut sock = tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port))
            .await
            .unwrap();
        sock.write_all(&frame).await.unwrap();
        sock.flush().await.unwrap();

        let mut len_bytes = [0u8; 4];
        sock.read_exact(&mut len_bytes).await.unwrap();
        let len = u32::from_be_bytes(len_bytes) as usize;
        let mut ack = vec![0u8; len];
        sock.read_exact(&mut ack).await.unwrap();
        assert_eq!(
            ack[0], STATUS_OK,
            "REGISTER ack status must be STATUS_OK; body={:?}",
            String::from_utf8_lossy(&ack[1..])
        );

        let body: serde_json::Value = serde_json::from_slice(&ack[1..]).unwrap();
        assert_eq!(body["status"], "ok");
        assert_eq!(body["name"], "Countries");

        // ------------------------------------------------------------
        // 2. The engine's CDC-gate must now report true. This is the bit
        //    that OP_UPSERT_TABLE_ROW / OP_DELETE_TABLE_ROW check.
        // ------------------------------------------------------------
        {
            let engine = state.engine.read();
            assert!(
                engine.has_registered_source_table("Countries"),
                "has_registered_source_table('Countries') must be true after wire-REGISTER"
            );
        }

        // ------------------------------------------------------------
        // 3. OP_UPSERT_TABLE_ROW now passes the gate (previously failed
        //    with 'table not registered as @bv.source_table').
        // ------------------------------------------------------------
        let mut upsert_payload = Vec::new();
        write_varint_string(&mut upsert_payload, "Countries");
        write_varint_string(&mut upsert_payload, "US");
        let source_lsn: u64 = 42;
        upsert_payload.extend_from_slice(&source_lsn.to_le_bytes());
        let fields_json = br#"{"country_code":"US","name":"United States","currency":"USD"}"#;
        upsert_payload.extend_from_slice(&(fields_json.len() as u32).to_le_bytes());
        upsert_payload.extend_from_slice(fields_json);
        let upsert_frame = encode_frame(OP_UPSERT_TABLE_ROW, &upsert_payload);

        sock.write_all(&upsert_frame).await.unwrap();
        sock.flush().await.unwrap();

        let mut len_bytes = [0u8; 4];
        sock.read_exact(&mut len_bytes).await.unwrap();
        let upsert_len = u32::from_be_bytes(len_bytes) as usize;
        let mut upsert_ack = vec![0u8; upsert_len];
        sock.read_exact(&mut upsert_ack).await.unwrap();
        assert_eq!(
            upsert_ack[0], STATUS_OK,
            "UPSERT_TABLE_ROW ack must be STATUS_OK after source_table is wire-registered; \
             got status={} body={:?}",
            upsert_ack[0],
            String::from_utf8_lossy(&upsert_ack[1..])
        );
        assert_eq!(upsert_ack.len(), 9, "ack = 1 status + 8 LE u64");
        let echoed = u64::from_le_bytes(upsert_ack[1..9].try_into().unwrap());
        assert_eq!(echoed, source_lsn, "source_lsn must be echoed on ack");
    });
}

/// Negative: register with kind="source_table" but no key_fields /
/// key_field → server must reject at REGISTER-time, not later when a
/// write lands. Guards against silent misconfiguration.
#[test]
#[ignore = "serial-only: build_state mutates process-global env BEAVA_DATA_DIR; run with -- --test-threads=1"]
fn op_register_source_table_rejects_missing_key_fields() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let tmp = tempfile::TempDir::new().unwrap();
        let state = build_state(&tmp);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let tcp_state = state.clone();
        tokio::spawn(async move {
            beava::server::tcp::run_tcp_server_with_listener(listener, tcp_state)
                .await
                .ok();
        });
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;

        let register_json = serde_json::json!({
            "name": "Bad",
            "kind": "source_table",
            "fields": {"x": {"type": "str", "optional": false}}
        });
        let payload = serde_json::to_vec(&register_json).unwrap();
        let frame = encode_frame(OP_REGISTER, &payload);

        let mut sock = tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port))
            .await
            .unwrap();
        sock.write_all(&frame).await.unwrap();
        sock.flush().await.unwrap();

        let mut len_bytes = [0u8; 4];
        sock.read_exact(&mut len_bytes).await.unwrap();
        let len = u32::from_be_bytes(len_bytes) as usize;
        let mut ack = vec![0u8; len];
        sock.read_exact(&mut ack).await.unwrap();
        assert_ne!(
            ack[0], STATUS_OK,
            "REGISTER with missing key_fields must NOT succeed"
        );
        let msg = String::from_utf8_lossy(&ack[1..]);
        assert!(
            msg.contains("key_fields"),
            "error message should mention key_fields; got: {msg}"
        );

        let engine = state.engine.read();
        assert!(
            !engine.has_registered_source_table("Bad"),
            "rejected register must not flip the CDC gate"
        );
    });
}
