//! TCP server: listener, connection handler, command dispatch, MSET cooperative yielding.
//!
//! SharedState wraps PipelineEngine + StateStore in Arc<Mutex<AppState>>.
//! Synchronous commands (PUSH, GET, SET, REGISTER) lock, process, unlock with no .await.
//! MSET releases the lock between 1024-key chunks and calls yield_now().

use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};

use crate::engine::pipeline::PipelineEngine;
use crate::error::TallyError;
use crate::server::protocol::{self, Command, STATUS_ERROR, STATUS_OK};
use crate::state::store::StateStore;
use crate::types::{feature_map_to_json, FeatureValue};

/// Application state: engine + store.
pub struct AppState {
    pub engine: PipelineEngine,
    pub store: StateStore,
}

/// Shared state handle for concurrent connection handlers.
pub type SharedState = Arc<Mutex<AppState>>;

/// Start the TCP server on the given address. Loops forever accepting connections.
pub async fn run_tcp_server(addr: &str, state: SharedState) -> Result<(), std::io::Error> {
    let listener = TcpListener::bind(addr).await?;
    run_tcp_server_with_listener(listener, state).await
}

/// Start the TCP server from a pre-bound listener (for tests with random ports).
pub async fn run_tcp_server_with_listener(
    listener: TcpListener,
    state: SharedState,
) -> Result<(), std::io::Error> {
    loop {
        let (stream, _addr) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(_e) = handle_connection(stream, state).await {
                // Connection closed or error -- debug log only
            }
        });
    }
}

/// Public wrapper for handle_connection, for integration tests.
pub async fn handle_connection_public(
    stream: TcpStream,
    state: SharedState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    handle_connection(stream, state).await
}

/// Handle a single persistent TCP connection: read frames in a loop, dispatch commands.
async fn handle_connection(
    stream: TcpStream,
    state: SharedState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (reader, writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut writer = BufWriter::new(writer);

    loop {
        // Read 4-byte length (u32 BE)
        let len = match reader.read_u32().await {
            Ok(len) => len as usize,
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()), // Clean disconnect
            Err(e) => return Err(e.into()),
        };

        if len == 0 || len > 64 * 1024 * 1024 {
            // Zero-length or >64MB frame: send error, close connection
            let resp = protocol::encode_response(STATUS_ERROR, b"invalid frame length");
            writer.write_all(&resp).await?;
            writer.flush().await?;
            return Ok(());
        }

        // Read opcode (1 byte)
        let opcode = reader.read_u8().await?;

        // Read payload (len - 1 bytes, since len includes opcode)
        let payload_len = len - 1;
        let mut payload = vec![0u8; payload_len];
        if payload_len > 0 {
            reader.read_exact(&mut payload).await?;
        }

        // Parse command from opcode + payload
        let cmd = match protocol::parse_command(opcode, &payload) {
            Ok(cmd) => cmd,
            Err(e) => {
                // Malformed frame: send error, close connection
                let resp = protocol::encode_response(STATUS_ERROR, e.to_string().as_bytes());
                writer.write_all(&resp).await?;
                writer.flush().await?;
                return Ok(());
            }
        };

        // Dispatch command
        let response = match cmd {
            Command::Mset { entries } => handle_mset(entries, &state).await,
            other => handle_sync_command(other, &state),
        };

        // Write response
        let resp_bytes = match response {
            Ok(payload) => protocol::encode_response(STATUS_OK, &payload),
            Err(e) => protocol::encode_response(STATUS_ERROR, e.to_string().as_bytes()),
        };
        writer.write_all(&resp_bytes).await?;
        writer.flush().await?;
    }
}

/// Handle synchronous commands: lock, process, unlock. No .await while locked.
fn handle_sync_command(cmd: Command, state: &SharedState) -> Result<Vec<u8>, TallyError> {
    let now = SystemTime::now();
    match cmd {
        Command::Push {
            stream_name,
            payload,
        } => {
            let mut app = state.lock().unwrap_or_else(|e| e.into_inner());
            let AppState {
                ref engine,
                ref mut store,
            } = *app;
            let features = engine.push(&stream_name, &payload, store, now)?;
            Ok(feature_map_to_json(&features))
        }
        Command::Get { key } => {
            let mut app = state.lock().unwrap_or_else(|e| e.into_inner());
            let AppState {
                ref engine,
                ref mut store,
            } = *app;
            let features = engine.get_features(&key, store, now);
            Ok(feature_map_to_json(&features))
        }
        Command::Set { key, payload } => {
            let mut app = state.lock().unwrap_or_else(|e| e.into_inner());
            // payload is a JSON object; iterate its key-value pairs
            if let serde_json::Value::Object(map) = payload {
                for (feat_name, val) in map {
                    let fv = json_to_feature_value(val);
                    app.store.set_static(&key, &feat_name, fv, now);
                }
            } else {
                return Err(TallyError::Protocol(
                    "SET payload must be a JSON object".into(),
                ));
            }
            Ok(vec![])
        }
        Command::Register { payload } => {
            let req: protocol::RegisterRequest = serde_json::from_value(payload)
                .map_err(|e| TallyError::Protocol(format!("invalid register payload: {}", e)))?;
            let stream_def = protocol::convert_register_request(req)?;
            let mut app = state.lock().unwrap_or_else(|e| e.into_inner());
            app.engine.register(stream_def)?;
            Ok(vec![])
        }
        Command::Mset { .. } => unreachable!("MSET handled separately"),
    }
}

/// Convert a serde_json::Value to FeatureValue for SET/MSET writes.
fn json_to_feature_value(v: serde_json::Value) -> FeatureValue {
    match v {
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                FeatureValue::Int(i)
            } else if let Some(f) = n.as_f64() {
                FeatureValue::Float(f)
            } else {
                FeatureValue::Missing
            }
        }
        serde_json::Value::String(s) => FeatureValue::String(s),
        serde_json::Value::Null => FeatureValue::Missing,
        serde_json::Value::Bool(b) => FeatureValue::Int(if b { 1 } else { 0 }),
        _ => FeatureValue::Missing, // Arrays/objects -> Missing
    }
}

/// Handle MSET with cooperative yielding: process 1024-key chunks, yield between.
async fn handle_mset(
    entries: Vec<(String, serde_json::Value)>,
    state: &SharedState,
) -> Result<Vec<u8>, TallyError> {
    let now = SystemTime::now();
    for chunk in entries.chunks(1024) {
        {
            let mut app = state.lock().unwrap_or_else(|e| e.into_inner());
            for (key, payload) in chunk {
                if let serde_json::Value::Object(map) = payload {
                    for (feat_name, val) in map {
                        let fv = json_to_feature_value(val.clone());
                        app.store.set_static(key, feat_name, fv, now);
                    }
                }
                // Skip non-object payloads silently (defensive)
            }
        } // Lock released before yield
        tokio::task::yield_now().await;
    }
    Ok(vec![])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::pipeline::{FeatureDef, StreamDefinition};
    use std::time::Duration;

    /// Helper: create shared state with empty engine + store.
    fn make_shared_state() -> SharedState {
        Arc::new(Mutex::new(AppState {
            engine: PipelineEngine::new(),
            store: StateStore::new(),
        }))
    }

    /// Helper: register a simple stream with count and sum features.
    fn register_tx_stream(state: &SharedState) {
        let stream = StreamDefinition {
            name: "Transactions".into(),
            key_field: "user_id".into(),
            features: vec![
                (
                    "tx_count_1h".into(),
                    FeatureDef::Count {
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                    },
                ),
                (
                    "tx_sum_1h".into(),
                    FeatureDef::Sum {
                        field: "amount".into(),
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                        optional: false,
                    },
                ),
            ],
        };
        let mut app = state.lock().unwrap();
        app.engine.register(stream).unwrap();
    }

    // --- AppState and SharedState type tests ---

    #[test]
    fn test_app_state_wraps_engine_and_store() {
        let state = make_shared_state();
        let app = state.lock().unwrap();
        assert_eq!(app.engine.stream_count(), 0);
        assert_eq!(app.store.entity_count(), 0);
    }

    #[test]
    fn test_shared_state_is_arc_mutex() {
        // Verify SharedState is Arc<Mutex<AppState>> by cloning
        let state: SharedState = make_shared_state();
        let state2 = state.clone();
        drop(state2); // Would fail if not Arc
        let _app = state.lock().unwrap(); // Would fail if not Mutex
    }

    // --- PUSH command tests ---

    #[test]
    fn test_push_registered_stream_returns_features() {
        let state = make_shared_state();
        register_tx_stream(&state);

        let cmd = Command::Push {
            stream_name: "Transactions".into(),
            payload: serde_json::json!({"user_id": "u123", "amount": 50.0}),
        };
        let result = handle_sync_command(cmd, &state);
        assert!(result.is_ok());

        let bytes = result.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["tx_count_1h"], 1);
        assert_eq!(json["tx_sum_1h"], 50.0);
    }

    #[test]
    fn test_push_unregistered_stream_returns_error() {
        let state = make_shared_state();
        let cmd = Command::Push {
            stream_name: "NonExistent".into(),
            payload: serde_json::json!({"user_id": "u123"}),
        };
        let result = handle_sync_command(cmd, &state);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("unknown stream"));
    }

    // --- GET command tests ---

    #[test]
    fn test_get_existing_key_returns_features() {
        let state = make_shared_state();
        register_tx_stream(&state);

        // Push an event first
        let push_cmd = Command::Push {
            stream_name: "Transactions".into(),
            payload: serde_json::json!({"user_id": "u123", "amount": 50.0}),
        };
        handle_sync_command(push_cmd, &state).unwrap();

        // GET should return features
        let get_cmd = Command::Get {
            key: "u123".into(),
        };
        let result = handle_sync_command(get_cmd, &state);
        assert!(result.is_ok());

        let bytes = result.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["tx_count_1h"], 1);
        assert_eq!(json["tx_sum_1h"], 50.0);
    }

    #[test]
    fn test_get_unknown_key_returns_empty_json() {
        let state = make_shared_state();
        let cmd = Command::Get {
            key: "nonexistent".into(),
        };
        let result = handle_sync_command(cmd, &state);
        assert!(result.is_ok());
        let bytes = result.unwrap();
        assert_eq!(bytes, b"{}");
    }

    // --- SET command tests ---

    #[test]
    fn test_set_writes_static_features() {
        let state = make_shared_state();
        let cmd = Command::Set {
            key: "u123".into(),
            payload: serde_json::json!({"lifetime_value": 4500.0, "segment": "high_value"}),
        };
        let result = handle_sync_command(cmd, &state);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty()); // Empty payload on success

        // Verify the features were written
        let app = state.lock().unwrap();
        let entity = app.store.get_entity("u123").unwrap();
        assert_eq!(
            entity.static_features.get("segment").unwrap().value,
            FeatureValue::String("high_value".into())
        );
    }

    #[test]
    fn test_set_non_object_payload_returns_error() {
        let state = make_shared_state();
        let cmd = Command::Set {
            key: "u123".into(),
            payload: serde_json::json!("not an object"),
        };
        let result = handle_sync_command(cmd, &state);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("SET payload must be a JSON object"));
    }

    // --- REGISTER command tests ---

    #[test]
    fn test_register_valid_stream() {
        let state = make_shared_state();
        let cmd = Command::Register {
            payload: serde_json::json!({
                "name": "Logins",
                "key_field": "user_id",
                "features": [
                    {"name": "login_count_1h", "type": "count", "window": "1h"}
                ]
            }),
        };
        let result = handle_sync_command(cmd, &state);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());

        let app = state.lock().unwrap();
        assert_eq!(app.engine.stream_count(), 1);
        assert!(app.engine.get_stream("Logins").is_some());
    }

    #[test]
    fn test_register_invalid_json_returns_error() {
        let state = make_shared_state();
        // Missing required "name" field
        let cmd = Command::Register {
            payload: serde_json::json!({"features": []}),
        };
        let result = handle_sync_command(cmd, &state);
        assert!(result.is_err());
    }

    // --- MSET tests ---

    #[tokio::test]
    async fn test_mset_processes_entries() {
        let state = make_shared_state();
        let entries = vec![
            (
                "u123".to_string(),
                serde_json::json!({"score": 0.95}),
            ),
            (
                "u456".to_string(),
                serde_json::json!({"score": 0.5}),
            ),
        ];
        let result = handle_mset(entries, &state).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());

        let app = state.lock().unwrap();
        assert_eq!(app.store.entity_count(), 2);
    }

    #[tokio::test]
    async fn test_mset_yields_between_chunks() {
        let state = make_shared_state();
        // Create > 1024 entries to ensure chunking happens
        let entries: Vec<(String, serde_json::Value)> = (0..2050)
            .map(|i| (format!("user_{}", i), serde_json::json!({"v": i})))
            .collect();
        let result = handle_mset(entries, &state).await;
        assert!(result.is_ok());

        let app = state.lock().unwrap();
        assert_eq!(app.store.entity_count(), 2050);
    }

    // --- json_to_feature_value tests ---

    #[test]
    fn test_json_to_feature_value_int() {
        let v = json_to_feature_value(serde_json::json!(42));
        assert_eq!(v, FeatureValue::Int(42));
    }

    #[test]
    fn test_json_to_feature_value_float() {
        let v = json_to_feature_value(serde_json::json!(3.14));
        assert_eq!(v, FeatureValue::Float(3.14));
    }

    #[test]
    fn test_json_to_feature_value_string() {
        let v = json_to_feature_value(serde_json::json!("hello"));
        assert_eq!(v, FeatureValue::String("hello".into()));
    }

    #[test]
    fn test_json_to_feature_value_null() {
        let v = json_to_feature_value(serde_json::Value::Null);
        assert_eq!(v, FeatureValue::Missing);
    }

    #[test]
    fn test_json_to_feature_value_bool_true() {
        let v = json_to_feature_value(serde_json::json!(true));
        assert_eq!(v, FeatureValue::Int(1));
    }

    #[test]
    fn test_json_to_feature_value_bool_false() {
        let v = json_to_feature_value(serde_json::json!(false));
        assert_eq!(v, FeatureValue::Int(0));
    }

    #[test]
    fn test_json_to_feature_value_array_becomes_missing() {
        let v = json_to_feature_value(serde_json::json!([1, 2, 3]));
        assert_eq!(v, FeatureValue::Missing);
    }

    // --- Mutex poisoning recovery test ---

    #[test]
    fn test_poisoned_mutex_recovery() {
        let state = make_shared_state();
        // Poison the mutex by panicking inside a lock
        let state2 = state.clone();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _app = state2.lock().unwrap();
            panic!("intentional panic to poison mutex");
        }));
        assert!(result.is_err()); // Panic was caught

        // Should still be able to use the state via unwrap_or_else recovery
        let cmd = Command::Get {
            key: "test".into(),
        };
        let result = handle_sync_command(cmd, &state);
        assert!(result.is_ok());
    }
}
