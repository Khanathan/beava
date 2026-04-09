# Phase 2: TCP Server and Binary Protocol - Context

**Gathered:** 2026-04-09
**Status:** Ready for planning

<domain>
## Phase Boundary

Build the TCP server (port 6400) with the custom binary protocol, wire up all five commands (PUSH, GET, SET, MSET, REGISTER) to the Phase 1 engine, add cooperative yielding for MSET, and expose a minimal HTTP management API (port 6401) with a health endpoint. The result is a running Tally server that accepts persistent TCP connections, processes binary frames, and returns updated features synchronously.

</domain>

<decisions>
## Implementation Decisions

### Protocol & Connection Management
- REGISTER allowed on both TCP (opcode 0x05) and HTTP — per CLAUDE.md spec
- No hard connection limit (OS default) — v1 single-threaded, connections naturally serialize
- Malformed frames: send error response, then close the connection — prevents cascading parse failures from corrupted stream
- TCP keepalive: tokio defaults (no custom keepalive/timeout) — v1 simplicity

### Error Response Wire Format
- Response format: status byte (0x00 = OK, 0x01 = Error) followed by payload. OK payload = feature map JSON bytes; Error payload = UTF-8 error message string
- Plain text error messages only (no structured error codes) — v1 simplicity
- PUSH to unregistered stream: return error "unknown stream: {name}"
- GET for key with no state: return empty JSON object `{}`

### MSET Yielding & Server Architecture
- MSET cooperative yielding: 1024 keys per chunk, yield to event loop between chunks per CLAUDE.md
- Tokio runtime: single-threaded `current_thread` per "single-threaded core (v1)" design principle
- HTTP management API: same binary, same tokio runtime, separate listener on port 6401
- HTTP framework: axum — lightweight, tokio-native. Only health endpoint in Phase 2; full CRUD in Phase 4

### Claude's Discretion
- Internal buffer sizes for frame reading
- Exact task structure within the tokio runtime (single select loop vs separate spawn_local tasks)
- Frame parsing implementation details (nom/winnow vs manual byte parsing)
- Test helper design for TCP client simulation

</decisions>

<code_context>
## Existing Code Insights

### Reusable Assets
- `PipelineEngine` (src/engine/pipeline.rs): register_stream(), push(), get_features() — the core API to wire up
- `StateStore` (src/state/store.rs): get_or_create_entity(), set_static_feature() — for SET/MSET commands
- `TallyError` (src/error.rs): already has Protocol variant for wire-level errors
- `FeatureValue` (src/types.rs): serializable with serde — can convert to JSON for response payloads
- `StreamDefinition` and `FeatureDef` (src/engine/pipeline.rs): the structures that REGISTER will deserialize into

### Established Patterns
- AHashMap (not std HashMap) per locked decision
- SystemTime for timestamps
- thiserror for error types
- serde + serde_json for serialization
- winnow for parsing (used in expression.rs)
- Phase 1 used edition 2021 for compatibility

### Integration Points
- main.rs is currently a stub — needs to become the server entry point with tokio runtime
- lib.rs exports pub mod types, error, engine, state — will add pub mod server
- Pipeline registration needs JSON deserialization into StreamDefinition/FeatureDef (currently only Rust API exists)
- Cargo.toml needs tokio, axum, bytes dependencies

</code_context>

<specifics>
## Specific Ideas

- Wire format per CLAUDE.md: [4 bytes: length u32 BE][1 byte: opcode][payload], strings as [2 bytes: length u16 BE][UTF-8 data]
- Response wrapping: [4 bytes: length u32 BE][1 byte: status (0x00/0x01)][payload bytes]
- MSET interleaving: PUSH/GET requests prioritized over pending MSET chunks via tokio task yielding
- Phase 1 decisions to honor: Redis-strict type errors propagated as error responses, Missing values serialized as JSON null

</specifics>

<deferred>
## Deferred Ideas

- Connection authentication/TLS — post-v1 security hardening
- Protocol versioning in frame header — could add version byte later if needed
- Backpressure/rate limiting — not needed for v1 single-threaded model

</deferred>
