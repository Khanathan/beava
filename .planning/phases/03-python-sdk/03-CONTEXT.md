# Phase 3: Python SDK - Context

**Gathered:** 2026-04-09
**Status:** Ready for planning

<domain>
## Phase Boundary

Build the Python SDK that lets ML engineers define streams using decorators, register them with a running Tally server, push events, and receive typed feature results — all without writing Rust or touching the wire protocol directly. The SDK is a thin client: pipeline definitions are built in Python, serialized to JSON, and sent to the server. Python never touches the hot path.

</domain>

<decisions>
## Implementation Decisions

### Package Identity & Python Ecosystem
- Package name: `tally` with conventional alias `import tally as st` — matches project rename, preserves CLAUDE.md example patterns
- Minimum Python version: 3.10+ — modern type syntax (X | Y unions, match), wide adoption
- Pure Python + stdlib only (socket, struct, json) — zero external dependencies, matches Tally's zero-infrastructure ethos
- Build system: pyproject.toml + hatchling — modern, minimal config

### Connection & Error Handling
- Single persistent TCP connection per `App` instance — v1 simplicity, matches single-threaded server design
- Auto-reconnect on next call after disconnect — transparent, lazy reconnection
- Python exceptions for errors: `TallyError` base, `ConnectionError`, `ProtocolError` subclasses — idiomatic Python
- Single `timeout=5.0` (seconds) constructor kwarg for both connect and read timeouts

### Feature Result Typing & Decorator Ergonomics
- Dynamic `__getattr__` on `FeatureResult` class — `features.tx_count_30m` attribute access per CLAUDE.md examples
- Missing feature values map to Python `None` — Pythonic, clean mapping from FeatureValue::Missing
- Validate operator definitions at class definition time via metaclass — fail fast on typos/bad expressions before register()
- Mixin support via standard Python multiple inheritance — `class Tx(VelocityMixin, AmountMixin):` per CLAUDE.md spec

### Claude's Discretion
- Internal TCP frame buffer management and read loop details
- Test fixture design for mock server or integration tests
- Exact module file organization within python/tally/
- JSON serialization format details for REGISTER payload (must match Rust deserialization)

</decisions>

<code_context>
## Existing Code Insights

### Reusable Assets
- `src/server/protocol.rs` — defines the binary wire format the SDK must speak: frame encoding (4-byte u32 BE length + 1 byte opcode + payload), string encoding (2-byte u16 BE length + UTF-8), opcodes (PUSH=0x01, GET=0x02, SET=0x03, MSET=0x04, REGISTER=0x05)
- `src/server/protocol.rs:convert_register_request` — defines the JSON schema the REGISTER command expects (RegisterRequest → StreamDefinition conversion)
- `src/types.rs:FeatureValue` — Float(f64), Int(i64), String(String), Missing — the SDK must map these to Python types
- `src/engine/pipeline.rs:StreamDefinition` and `FeatureDef` — the domain types that JSON must deserialize into
- `src/engine/expression.rs` — expression syntax the SDK's derive/where strings must conform to

### Established Patterns
- Wire format: length-prefixed binary frames over persistent TCP
- Response format: status byte (0x00=OK, 0x01=Error) + payload bytes
- REGISTER payload: JSON with stream_name, key_field, features array
- Feature JSON: plain values (not wrapped enums) via FeatureValue::to_json_value

### Integration Points
- TCP port 6400 (configurable) for all SDK ↔ server communication
- REGISTER command sends JSON pipeline definition
- PUSH sends stream_name + JSON event payload, receives JSON feature map
- GET sends entity key string, receives JSON feature map
- SET sends key + JSON feature map
- MSET sends count + (key, payload) pairs

</code_context>

<specifics>
## Specific Ideas

No specific requirements beyond CLAUDE.md spec — the SDK API design (decorators, operators, client methods, typed results) is fully specified in the design document.

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope.

</deferred>
