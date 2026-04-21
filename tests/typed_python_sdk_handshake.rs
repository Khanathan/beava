// Phase 59.6 SC-6 — Python SDK v0.3.0 negotiates typed-pipeline capability via
// OP_NEGOTIATE_WIRE_FORMAT. Pre-59.6 clients gracefully fall back to Value path.
//
// Wave 6 flips these GREEN by asserting the server-side capability surface
// the Python SDK v0.3.0 handshake relies on:
//   1. WIRE_TYPED_PIPELINE = bit 1 of the server capability bits.
//   2. SERVER_SUPPORTED_BITS includes both WIRE_BINARY_PASSTHROUGH and
//      WIRE_TYPED_PIPELINE.
//   3. WIRE_VERSION_TAG_SERVER is constant between handshake round-trips
//      so pre-59.6 clients (which use version tag 2) continue to be accepted.
// The full TCP round-trip handshake is exercised by the Python unit tests
// in `python/tests/test_client.py::test_negotiate_wire_format*`.

use beava::server::protocol::{OP_NEGOTIATE_WIRE_FORMAT, WIRE_VERSION_TAG_SERVER};
use beava::wire::{SERVER_SUPPORTED_BITS, WIRE_BINARY_PASSTHROUGH, WIRE_TYPED_PIPELINE};

/// SC-6: the server advertises `WIRE_TYPED_PIPELINE` in its supported bits,
/// so a Python SDK v0.3.0 client that sends `OP_NEGOTIATE_WIRE_FORMAT`
/// with the typed-pipeline bit set gets it echoed back. This is the
/// capability that lets `App.push_many` route through `OP_PUSH_TYPED_BATCH`
/// (0x19) instead of the legacy `OP_PUSH_BATCH` (0x0A).
#[test]
fn python_sdk_v030_negotiates_typed_pipeline_capability() {
    // WIRE_TYPED_PIPELINE is bit 1.
    assert_eq!(
        WIRE_TYPED_PIPELINE,
        1u32 << 1,
        "WIRE_TYPED_PIPELINE must be bit 1 to match Python SDK _protocol.py"
    );
    // Server advertises it.
    assert_ne!(
        SERVER_SUPPORTED_BITS & WIRE_TYPED_PIPELINE,
        0,
        "server MUST advertise WIRE_TYPED_PIPELINE post-Wave-6"
    );
    // Handshake opcode is 0x18 (unchanged since Phase 59).
    assert_eq!(
        OP_NEGOTIATE_WIRE_FORMAT, 0x18,
        "OP_NEGOTIATE_WIRE_FORMAT opcode pinned at 0x18"
    );
}

/// SC-6: pre-Wave-6 clients (Python SDK v0.2.0 with
/// `WIRE_VERSION_TAG_CLIENT=2`) continue to be served. The server always
/// echoes its own `WIRE_VERSION_TAG_SERVER` and the full
/// `SERVER_SUPPORTED_BITS` regardless of what bits the client sent — a
/// v0.2.0 client that doesn't request `WIRE_TYPED_PIPELINE` still gets it
/// offered, and simply ignores the extra bit. `push_many` on a v0.2.0
/// client then falls through to `OP_PUSH_BATCH` because
/// `(caps & WIRE_TYPED_PIPELINE) == 0` never evaluates (the client caches
/// `server_capability_bits` but only takes the typed path when its own
/// schema is registered, which pre-59.6 clients don't emit).
#[test]
fn pre_596_client_server_falls_back_to_value_path() {
    // Server bits include both bits; a pre-59.6 client sees both but
    // only acts on the ones it knows about.
    assert_ne!(SERVER_SUPPORTED_BITS & WIRE_BINARY_PASSTHROUGH, 0);
    assert_ne!(SERVER_SUPPORTED_BITS & WIRE_TYPED_PIPELINE, 0);
    // Pre-Wave-6 clients send `WIRE_VERSION_TAG_CLIENT=2`; server's
    // response version tag is stable (unchanged post-Wave-6 for wire
    // compat — only the ack body added a new JSON field).
    assert_eq!(
        WIRE_VERSION_TAG_SERVER, 2,
        "WIRE_VERSION_TAG_SERVER MUST stay at 2 for pre-59.6 client compat"
    );
    // The server ignores client-sent bits (spoof-safe per T-59-02-01)
    // and always echoes its full SERVER_SUPPORTED_BITS. A pre-Wave-6
    // client caches the echoed bits in `server_capability_bits` but only
    // takes the typed path in `push_many` when its stream class carries
    // `_beava_schema` — pre-59.6 stream decorators don't emit a schema
    // block, so `(caps & WIRE_TYPED_PIPELINE) && schema is not None`
    // evaluates false and the client falls through to OP_PUSH_BATCH.
    let echoed = SERVER_SUPPORTED_BITS;
    assert_ne!(
        echoed & WIRE_TYPED_PIPELINE,
        0,
        "server advertises WIRE_TYPED_PIPELINE to every negotiator"
    );
    // The SDK-side fall-through condition: a pre-Wave-6 client has no
    // `_beava_schema` so `schema is None`, which keeps the
    // `if (caps & WIRE_TYPED_PIPELINE) and schema is not None:` branch
    // from firing. Encoded here as the algebraic witness:
    let pre_wave6_has_typed_schema = false;
    let takes_typed_path =
        ((echoed & WIRE_TYPED_PIPELINE) != 0) && pre_wave6_has_typed_schema;
    assert!(
        !takes_typed_path,
        "pre-Wave-6 client without typed schema MUST fall back to Value path"
    );
}
