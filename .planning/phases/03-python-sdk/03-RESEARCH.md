# Phase 3: Python SDK - Research

**Researched:** 2026-04-09
**Domain:** Python SDK for binary TCP protocol client + declarative stream definition DSL
**Confidence:** HIGH

## Summary

Phase 3 builds a pure-Python SDK that lets ML engineers define streaming feature pipelines using decorators (`@st.stream`, `@st.view`), register them with a running Tally server, push events, and receive typed feature results. The SDK is a thin client -- pipeline definitions are built in Python, serialized to JSON matching the Rust `RegisterRequest` DTO, and sent over the custom binary TCP protocol. Python never touches the hot path.

The primary technical challenge is twofold: (1) correctly implementing the binary wire protocol (length-prefixed frames, string encoding, opcode dispatch) using only Python stdlib (`socket`, `struct`, `json`), and (2) building a metaclass-based DSL that collects operator descriptors from class bodies, supports mixin inheritance, validates at definition time, and serializes to JSON matching the exact schema the Rust server deserializes via `serde`.

**Primary recommendation:** Build contract-first -- define the REGISTER JSON schema as the canonical interface, write byte-level conformance tests against known-good Rust encodings first, then implement the protocol client and decorator DSL to satisfy those contracts.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Package name: `tally` with conventional alias `import tally as st`
- Minimum Python version: 3.10+ (modern type syntax X | Y unions, match)
- Pure Python + stdlib only (socket, struct, json) -- zero external dependencies
- Build system: pyproject.toml + hatchling
- Single persistent TCP connection per `App` instance (v1 simplicity)
- Auto-reconnect on next call after disconnect (transparent, lazy)
- Python exceptions: `TallyError` base, `ConnectionError`, `ProtocolError` subclasses
- Single `timeout=5.0` (seconds) constructor kwarg for both connect and read timeouts
- Dynamic `__getattr__` on `FeatureResult` class for attribute access
- Missing feature values map to Python `None`
- Validate operator definitions at class definition time via metaclass
- Mixin support via standard Python multiple inheritance

### Claude's Discretion
- Internal TCP frame buffer management and read loop details
- Test fixture design for mock server or integration tests
- Exact module file organization within python/tally/
- JSON serialization format details for REGISTER payload (must match Rust deserialization)

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| SDK-01 | @st.stream decorator defines a stream with key field and feature declarations | Metaclass pattern + operator descriptors (Architecture Patterns section) |
| SDK-02 | @st.view decorator defines cross-stream views with derive expressions | Same metaclass, restricted to derive/lookup operators only |
| SDK-03 | Operator classes serialize to JSON | RegisterRequest DTO schema documented from Rust source (Code Examples section) |
| SDK-04 | TCP client with persistent connection communicates via binary protocol | Protocol encoding patterns from protocol.rs (Code Examples section) |
| SDK-05 | app.push() sends event and returns typed feature results | PUSH frame format + FeatureResult dynamic class |
| SDK-06 | app.get(), app.set(), app.mset() for read/write operations | Full command encoding for all opcodes documented |
| SDK-07 | app.register() sends pipeline definitions to server | RegisterRequest JSON schema + REGISTER opcode encoding |
</phase_requirements>

## Project Constraints (from CLAUDE.md)

- TDD / Contract-First: Define contracts and write tests before implementation in all phases (from user memory)
- Package is named `tally` (project was renamed from streamlet)
- CLAUDE.md still shows `import streamlet as st` in examples but CONTEXT.md locked decision says `import tally as st` -- follow CONTEXT.md
- Zero external dependencies for the SDK
- Python SDK lives in `python/` directory per CLAUDE.md project structure

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| Python stdlib `socket` | 3.10+ built-in | TCP connection | Zero-dep requirement, direct socket control for binary protocol [VERIFIED: stdlib] |
| Python stdlib `struct` | 3.10+ built-in | Binary encoding/decoding (u32 BE, u16 BE) | Exact control over byte layout matching Rust wire format [VERIFIED: stdlib] |
| Python stdlib `json` | 3.10+ built-in | JSON serialization for payloads | Matches serde_json on server side [VERIFIED: stdlib] |
| hatchling | latest | Build backend for pyproject.toml | Locked decision from CONTEXT.md [VERIFIED: locked decision] |

### Supporting (Development/Testing Only)
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| pytest | 8.x | Test framework | All SDK tests [VERIFIED: installed as `python3 -m pytest`] |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Raw socket | asyncio streams | Would add async complexity; v1 is synchronous, single-connection |
| struct module | int.to_bytes/from_bytes | struct.pack is more readable for multi-field encoding |
| Metaclass | `__init_subclass__` | Metaclass gives more control over class body collection; both work |

## Architecture Patterns

### Recommended Project Structure
```
python/
  pyproject.toml           # hatchling build config, package name "tally"
  tally/
    __init__.py            # Public API re-exports: stream, view, count, sum, avg, etc.
    _client.py             # TCP connection, frame send/recv, auto-reconnect
    _protocol.py           # Binary encoding: frames, strings, command payloads
    _stream.py             # @stream decorator, StreamMeta metaclass, operator collection
    _view.py               # @view decorator (thin wrapper over stream with restrictions)
    _operators.py          # Operator descriptor classes: Count, Sum, Avg, Min, Max, etc.
    _types.py              # FeatureResult, TallyError hierarchy, Value type mapping
    _app.py                # App class: register(), push(), get(), set(), mset()
  tests/
    conftest.py            # Shared fixtures (mock server or live server launcher)
    test_protocol.py       # Byte-level conformance tests
    test_operators.py      # Operator serialization to JSON
    test_stream.py         # @stream decorator, metaclass, mixin inheritance
    test_view.py           # @view decorator tests
    test_client.py         # TCP client, reconnection
    test_integration.py    # End-to-end against live Tally server
```

### Pattern 1: Metaclass-Based Stream DSL
**What:** A metaclass (`StreamMeta`) intercepts class body at definition time, collects operator descriptors, validates them, and attaches metadata for later serialization.
**When to use:** For `@st.stream` and `@st.view` decorated classes.
**Example:**
```python
# Source: Python metaclass pattern for DSL collection [ASSUMED]
class StreamMeta(type):
    def __new__(mcs, name, bases, namespace, **kwargs):
        # Collect operator descriptors from class body and all bases (mixins)
        features = {}
        # Walk MRO for mixin support
        for base in reversed(bases):
            for attr_name, attr_val in vars(base).items():
                if isinstance(attr_val, OperatorBase):
                    features[attr_name] = attr_val
        # Class body overrides
        for attr_name, attr_val in namespace.items():
            if isinstance(attr_val, OperatorBase):
                features[attr_name] = attr_val
        
        cls = super().__new__(mcs, name, bases, namespace)
        cls._tally_features = features
        cls._tally_key_field = kwargs.get('key')
        cls._tally_stream_name = name  # Class name = stream name
        return cls
```

### Pattern 2: Operator Descriptors
**What:** Each operator (count, sum, avg, etc.) is a lightweight descriptor class that stores its configuration and knows how to serialize to the JSON format expected by `RegisterRequest`.
**When to use:** All operator definitions.
**Example:**
```python
# Source: Derived from Rust RegisterRequest DTO in protocol.rs [VERIFIED: protocol.rs:229-252]
class Count(OperatorBase):
    def __init__(self, *, window: str, where: str | None = None):
        self.window = window
        self.where_clause = where  # 'where' is Python keyword, store as where_clause
    
    def to_json(self, name: str) -> dict:
        d = {"name": name, "type": "count", "window": self.window}
        if self.where_clause:
            d["where"] = self.where_clause
        return d
```

### Pattern 3: Frame Buffer Management
**What:** TCP recv loop that accumulates bytes until a complete frame (4-byte length header + payload) is available.
**When to use:** All TCP communication.
**Example:**
```python
# Source: Standard length-prefixed protocol pattern [ASSUMED]
def _recv_frame(self) -> tuple[int, bytes]:
    """Read one response frame: [4-byte BE length][status byte][payload]."""
    header = self._recv_exact(4)
    length = struct.unpack('>I', header)[0]
    body = self._recv_exact(length)
    status = body[0]
    payload = body[1:]
    return status, payload

def _recv_exact(self, n: int) -> bytes:
    """Read exactly n bytes, handling partial reads."""
    buf = bytearray()
    while len(buf) < n:
        chunk = self._sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("server closed connection")
        buf.extend(chunk)
    return bytes(buf)
```

### Pattern 4: FeatureResult with Dynamic Attribute Access
**What:** Thin wrapper over a dict that provides attribute-style access to feature values.
**When to use:** Return type from `push()` and `get()`.
**Example:**
```python
# Source: CONTEXT.md locked decision [VERIFIED: CONTEXT.md]
class FeatureResult:
    def __init__(self, data: dict):
        self._data = data
    
    def __getattr__(self, name: str):
        try:
            return self._data[name]
        except KeyError:
            raise AttributeError(f"no feature named '{name}'")
    
    def __getitem__(self, key: str):
        return self._data[key]
    
    def to_dict(self) -> dict:
        return dict(self._data)
```

### Anti-Patterns to Avoid
- **Building async client for v1:** Single persistent connection is synchronous. Async adds complexity for zero benefit with single-threaded server.
- **Parsing expressions in Python:** Expressions are strings passed through to the Rust server. Python should NOT parse/validate expression syntax -- just forward the string.
- **Wrapping JSON values in type enums:** The server's `FeatureValue::to_json_value()` emits plain JSON (1.5, 42, "ok", null). The SDK should map these directly to Python float/int/str/None.
- **Using `__slots__` on FeatureResult:** Breaks `__getattr__` pattern; keep it simple with `_data` dict.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Binary encoding | Custom byte manipulation | `struct.pack('>I', length)` / `struct.pack('>H', str_len)` | struct handles endianness correctly, matches Rust BE encoding [VERIFIED: stdlib] |
| JSON serialization | Custom serializer | `json.dumps()` / `json.loads()` | Must match serde_json exactly; stdlib json module does this [VERIFIED: stdlib] |
| Duration string formatting | Duration parser | Simple string pass-through | Duration strings ("30m", "1h", "24h") are parsed server-side by `parse_duration_str()`. SDK just passes strings. [VERIFIED: protocol.rs:191-220] |
| Expression validation | Expression parser | String pass-through to server | Server parses expressions at REGISTER time. SDK forwards strings, server returns errors. [VERIFIED: protocol.rs:351-364] |

**Key insight:** The SDK is intentionally thin. All heavy lifting (expression parsing, duration parsing, operator instantiation) happens server-side. The SDK's job is serialization and transport.

## Common Pitfalls

### Pitfall 1: Frame Length Off-By-One
**What goes wrong:** Frame length field includes different bytes than expected, causing protocol desync.
**Why it happens:** Rust protocol defines length = opcode (1 byte) + payload bytes. Easy to accidentally include or exclude the length header itself.
**How to avoid:** Frame = `[4-byte length][opcode][payload]` where length = `1 + len(payload)`. Response = `[4-byte length][status][payload]` where length = `1 + len(payload)`. [VERIFIED: protocol.rs:29-37, 62-70]
**Warning signs:** First request works, second request fails (frame boundary misalignment).

### Pitfall 2: String Encoding Mismatch
**What goes wrong:** Python uses u32 for string length instead of u16, or wrong endianness.
**Why it happens:** Protocol uses u16 BE for strings but u32 BE for frame length and MSET json_len.
**How to avoid:** Strings: `struct.pack('>H', len(s))` (u16). Frame length: `struct.pack('>I', length)` (u32). MSET json_len: `struct.pack('>I', json_len)` (u32). [VERIFIED: protocol.rs:75-93, 97-109]
**Warning signs:** Works for short strings, fails for strings > 255 bytes (where u8 vs u16 would differ).

### Pitfall 3: MSET Entry Format
**What goes wrong:** MSET entries encoded as simple JSON instead of the streaming binary format.
**Why it happens:** Other commands use JSON payloads, but MSET has a special format for streaming parse.
**How to avoid:** MSET format: `[u32 count][for each: u16-string key + u32 json_len + json_bytes]`. [VERIFIED: protocol.rs:145-175, test_server.rs:124-134]
**Warning signs:** MSET always returns error while other commands work.

### Pitfall 4: Metaclass MRO and Mixin Order
**What goes wrong:** Features from mixins override or get lost depending on class definition order.
**Why it happens:** Python MRO is C3 linearization; class body always wins over bases, but base order matters for conflicts between mixins.
**How to avoid:** Walk bases in reverse MRO order, then apply class body namespace. Class body always wins. Document that later-listed bases take precedence for name conflicts between mixins. [ASSUMED]
**Warning signs:** Mixin features missing or have wrong configuration after inheritance.

### Pitfall 5: Socket Partial Reads
**What goes wrong:** `socket.recv(n)` returns fewer than n bytes, causing incomplete frame parsing.
**Why it happens:** TCP is a stream protocol; recv may return any number of bytes up to n.
**How to avoid:** Always use a `recv_exact(n)` loop that accumulates bytes until exactly n bytes are received. [ASSUMED]
**Warning signs:** Works on localhost, fails under load or on slow networks.

### Pitfall 6: `where` is a Python Keyword
**What goes wrong:** `st.count(window="30m", where="status == 'failed'")` -- `where` is a soft keyword in Python 3.10+ (used in match statements) but was a hard keyword candidate. Actually, `where` is NOT a Python keyword.
**Correction:** `where` is not a reserved keyword in Python. It can be used as a parameter name. No issue here. [VERIFIED: Python 3.10+ keyword list]

## Code Examples

### REGISTER JSON Schema (Canonical)
```python
# Source: Rust RegisterRequest DTO [VERIFIED: protocol.rs:229-252]
# This is the exact JSON structure the server expects for REGISTER command.
{
    "name": "Transactions",          # stream name (class name)
    "key_field": "user_id",          # from @st.stream(key="user_id")
    "features": [
        {
            "name": "tx_count_1h",   # Python attribute name
            "type": "count",         # operator type string
            "window": "1h",          # duration string (parsed server-side)
            # optional: "bucket": "1m"
        },
        {
            "name": "tx_sum_1h",
            "type": "sum",
            "field": "amount",       # required for sum/avg
            "window": "1h",
            # optional: "optional": true
        },
        {
            "name": "avg_amount_1h",
            "type": "avg",
            "field": "amount",
            "window": "1h"
        },
        {
            "name": "failure_rate",
            "type": "derive",
            "expr": "failed_tx_30m / tx_count_30m"  # expression string
        }
    ]
}
```

**Supported `type` values (server-side):** "count", "sum", "avg", "derive". [VERIFIED: protocol.rs:286-371]

**Not yet supported server-side:** "min", "max", "distinct_count", "last", "lookup" -- these are Phase 5 (OPS-01 through OPS-05, XSTR-01 through XSTR-03). The SDK MUST define these operator classes (SDK-03 requires all operators serialize to JSON), but REGISTER will reject them until Phase 5 adds server support.

### PUSH Command Encoding
```python
# Source: protocol.rs parse_command for OP_PUSH [VERIFIED: protocol.rs:131-135]
def encode_push(stream_name: str, event: dict) -> bytes:
    """Encode PUSH command payload: [u16-string stream_name][JSON event bytes]."""
    payload = bytearray()
    # String encoding: [u16 BE length][UTF-8 bytes]
    name_bytes = stream_name.encode('utf-8')
    payload.extend(struct.pack('>H', len(name_bytes)))
    payload.extend(name_bytes)
    # JSON payload: remaining bytes
    payload.extend(json.dumps(event).encode('utf-8'))
    return bytes(payload)
```

### GET Command Encoding
```python
# Source: protocol.rs parse_command for OP_GET [VERIFIED: protocol.rs:136-139]
def encode_get(key: str) -> bytes:
    """Encode GET command payload: [u16-string key]."""
    key_bytes = key.encode('utf-8')
    payload = struct.pack('>H', len(key_bytes)) + key_bytes
    return payload
```

### SET Command Encoding
```python
# Source: protocol.rs parse_command for OP_SET [VERIFIED: protocol.rs:140-144]
def encode_set(key: str, features: dict) -> bytes:
    """Encode SET command payload: [u16-string key][JSON feature map]."""
    payload = bytearray()
    key_bytes = key.encode('utf-8')
    payload.extend(struct.pack('>H', len(key_bytes)))
    payload.extend(key_bytes)
    payload.extend(json.dumps(features).encode('utf-8'))
    return bytes(payload)
```

### MSET Command Encoding
```python
# Source: protocol.rs parse_command for OP_MSET [VERIFIED: protocol.rs:145-175]
def encode_mset(entries: dict[str, dict]) -> bytes:
    """Encode MSET payload: [u32 count][for each: u16-string key + u32 json_len + json_bytes]."""
    payload = bytearray()
    payload.extend(struct.pack('>I', len(entries)))
    for key, features in entries.items():
        key_bytes = key.encode('utf-8')
        payload.extend(struct.pack('>H', len(key_bytes)))
        payload.extend(key_bytes)
        json_bytes = json.dumps(features).encode('utf-8')
        payload.extend(struct.pack('>I', len(json_bytes)))
        payload.extend(json_bytes)
    return bytes(payload)
```

### REGISTER Command Encoding
```python
# Source: protocol.rs parse_command for OP_REGISTER [VERIFIED: protocol.rs:177-180]
def encode_register(definition: dict) -> bytes:
    """Encode REGISTER payload: entire payload is JSON bytes."""
    return json.dumps(definition).encode('utf-8')
```

### Full Frame Encoding
```python
# Source: protocol.rs encode_frame [VERIFIED: protocol.rs:30-37]
OP_PUSH     = 0x01
OP_GET      = 0x02
OP_SET      = 0x03
OP_MSET     = 0x04
OP_REGISTER = 0x05

STATUS_OK    = 0x00
STATUS_ERROR = 0x01

def encode_frame(opcode: int, payload: bytes) -> bytes:
    """Encode wire frame: [4-byte BE length][opcode][payload]. Length = 1 + len(payload)."""
    length = 1 + len(payload)
    return struct.pack('>I', length) + bytes([opcode]) + payload
```

### Response Value Mapping
```python
# Source: types.rs FeatureValue::to_json_value [VERIFIED: types.rs:41-48]
# Server sends: float -> JSON number, int -> JSON integer, string -> JSON string, Missing -> null
# Python mapping: float -> float, int -> int, str -> str, None -> None
# json.loads() handles this automatically -- no custom deserialization needed.
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| setup.py + setuptools | pyproject.toml + hatchling | PEP 621 (2021) | Use pyproject.toml exclusively, no setup.py [VERIFIED: locked decision] |
| `typing.Union[X, Y]` | `X \| Y` syntax | Python 3.10 (2021) | Cleaner type hints with 3.10+ minimum [VERIFIED: Python docs] |
| `__init_subclass__` hooks | Metaclass for DSL collection | Both current | Metaclass chosen per CONTEXT.md for full class body control [VERIFIED: locked decision] |

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | Metaclass `__new__` can reliably collect descriptors from class body namespace before `type.__new__` | Architecture Patterns | Would need `__init_subclass__` fallback; low risk, well-established Python pattern |
| A2 | `socket.settimeout()` applies to both connect and recv operations | Architecture Patterns | May need separate timeout handling; low risk, documented stdlib behavior |
| A3 | Server-unsupported operator types (min, max, distinct_count, last, lookup) will return a clear error on REGISTER | Common Pitfalls | Could silently ignore or panic; need to test. SDK should still serialize them correctly for SDK-03. |

## Open Questions

1. **Server behavior for unknown feature types in REGISTER**
   - What we know: `convert_register_request()` has a catch-all `unknown =>` arm that returns `TallyError::Protocol` [VERIFIED: protocol.rs:366-371]
   - What's unclear: Whether SDK tests for min/max/distinct_count/last/lookup should expect server rejection or skip server interaction
   - Recommendation: SDK unit tests verify JSON serialization is correct (no server needed). Integration tests for unsupported types should assert clean error message from server.

2. **View registration format**
   - What we know: CONTEXT.md says `@st.view` is in scope. The current server only has `StreamDefinition` (no `ViewDefinition`).
   - What's unclear: Should views use the same REGISTER JSON format with a "type": "view" flag, or a separate format?
   - Recommendation: Since views (XSTR-01) are Phase 5, the SDK should define the `@st.view` decorator and its JSON serialization (SDK-02), but integration testing against the server will fail until Phase 5. Use same `RegisterRequest` format -- views are just streams with only derive/lookup features.

3. **`where` clause in REGISTER JSON**
   - What we know: The `FeatureDefRequest` struct has no `where` field [VERIFIED: protocol.rs:237-252]. OPS-05 (where-clause filtering) is Phase 5.
   - What's unclear: Whether `st.count(window="30m", where="status == 'failed'")` should include a `where` key in JSON now
   - Recommendation: SDK should serialize the `where` field in JSON. Server will currently ignore unknown fields (serde default behavior). Phase 5 will add the `where` field to `FeatureDefRequest`.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Python 3.10+ | SDK runtime | Yes | 3.13.2 | -- |
| pytest | Testing | Yes | via `python3 -m pytest` | -- |
| hatchling | Build system | Needs install | -- | `pip install hatchling` in Wave 0 |
| Running Tally server | Integration tests | Yes (build from source) | -- | Mock server for unit tests |

**Missing dependencies with no fallback:** None

**Missing dependencies with fallback:**
- hatchling: install via pip as part of project setup

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest 8.x (Python 3.13) |
| Config file | `python/pytest.ini` or `python/pyproject.toml [tool.pytest]` -- Wave 0 |
| Quick run command | `cd python && python3 -m pytest tests/ -x -q` |
| Full suite command | `cd python && python3 -m pytest tests/ -v` |

### Phase Requirements to Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| SDK-01 | @st.stream collects features, key field, stream name from decorated class | unit | `python3 -m pytest tests/test_stream.py -x` | Wave 0 |
| SDK-02 | @st.view collects derive expressions, serializes to JSON | unit | `python3 -m pytest tests/test_view.py -x` | Wave 0 |
| SDK-03 | All operator classes serialize to valid JSON dicts | unit | `python3 -m pytest tests/test_operators.py -x` | Wave 0 |
| SDK-04 | TCP client sends/receives binary frames correctly | unit + integration | `python3 -m pytest tests/test_protocol.py tests/test_client.py -x` | Wave 0 |
| SDK-05 | app.push() returns FeatureResult with attribute access | integration | `python3 -m pytest tests/test_integration.py::test_push -x` | Wave 0 |
| SDK-06 | app.get/set/mset work against server | integration | `python3 -m pytest tests/test_integration.py -x` | Wave 0 |
| SDK-07 | app.register() sends valid REGISTER command | integration | `python3 -m pytest tests/test_integration.py::test_register -x` | Wave 0 |

### Byte-Level Conformance Test (Success Criterion 5)
| Behavior | Test Type | Description |
|----------|-----------|-------------|
| Wire format conformance | unit | Encode PUSH/GET/SET/MSET/REGISTER in Python, compare byte-for-byte against known-good outputs derived from Rust `encode_frame`/`write_string` |

### Sampling Rate
- **Per task commit:** `cd /Users/petrpan26/work/tally/python && python3 -m pytest tests/ -x -q`
- **Per wave merge:** `cd /Users/petrpan26/work/tally/python && python3 -m pytest tests/ -v`
- **Phase gate:** Full Python suite green + integration tests against live server

### Wave 0 Gaps
- [ ] `python/pyproject.toml` -- package config with hatchling, pytest config
- [ ] `python/tests/conftest.py` -- shared fixtures (server launcher, mock server)
- [ ] `python/tests/test_protocol.py` -- byte-level conformance tests
- [ ] `python/tests/test_operators.py` -- operator JSON serialization tests
- [ ] `python/tests/test_stream.py` -- decorator/metaclass tests
- [ ] `python/tests/test_view.py` -- view decorator tests
- [ ] `python/tests/test_client.py` -- TCP client tests
- [ ] `python/tests/test_integration.py` -- end-to-end against live server
- [ ] hatchling install: `pip install hatchling`

## Security Domain

Security enforcement is enabled (default). This phase is a client SDK with minimal attack surface.

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | No | TCP protocol has no auth (by design, per CLAUDE.md out-of-scope) |
| V3 Session Management | No | Persistent TCP connections, no session tokens |
| V4 Access Control | No | No authorization model in v1 |
| V5 Input Validation | Yes | Validate operator params at class definition time (metaclass); validate key/stream_name non-empty before sending |
| V6 Cryptography | No | No encryption in v1 TCP protocol |

### Known Threat Patterns for Python TCP Client

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Oversized frame from server | Denial of Service | Cap max response frame size (e.g., 64MB) before allocating buffer |
| Malicious JSON in response | Tampering | json.loads() is safe against code execution; validate expected types |
| Connection string injection | Information Disclosure | Validate host:port format in App constructor |

## Sources

### Primary (HIGH confidence)
- `/Users/petrpan26/work/tally/src/server/protocol.rs` -- Wire format, opcodes, RegisterRequest DTO, string encoding, command parsing
- `/Users/petrpan26/work/tally/src/types.rs` -- FeatureValue variants, JSON serialization
- `/Users/petrpan26/work/tally/src/engine/pipeline.rs` -- StreamDefinition, FeatureDef, push-through flow
- `/Users/petrpan26/work/tally/tests/test_server.rs` -- REGISTER JSON examples, MSET binary encoding examples
- `/Users/petrpan26/work/tally/.planning/phases/03-python-sdk/03-CONTEXT.md` -- Locked decisions

### Secondary (MEDIUM confidence)
- Python 3.10+ stdlib documentation for socket, struct, json modules

### Tertiary (LOW confidence)
- None

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- pure stdlib, no library selection needed, all locked decisions
- Architecture: HIGH -- wire format fully specified in Rust source, metaclass pattern is well-established Python
- Pitfalls: HIGH -- all protocol edge cases verified against Rust source code
- Operator JSON schema: HIGH for count/sum/avg/derive (verified in protocol.rs), MEDIUM for min/max/distinct_count/last/lookup (not yet in server, SDK must define speculatively)

**Research date:** 2026-04-09
**Valid until:** 2026-05-09 (stable -- stdlib-only, protocol unlikely to change)
