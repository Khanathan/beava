import { describe, it, expect } from "vitest";
import { encodeFrame, decodeFrame, OP_PUSH, CT_JSON } from "../src/wire.js";

describe("wire frame codec", () => {
  it("round-trips an OP_PUSH JSON frame", () => {
    const payload = new TextEncoder().encode(JSON.stringify({ fields: { a: 1 } }));
    const frame = encodeFrame(OP_PUSH, CT_JSON, payload);
    const decoded = decodeFrame(frame);
    expect(decoded.op).toBe(OP_PUSH);
    expect(decoded.contentType).toBe(CT_JSON);
    expect(new TextDecoder().decode(decoded.payload)).toBe('{"fields":{"a":1}}');
  });

  it("encodes a 3-byte minimum frame (empty payload)", () => {
    const frame = encodeFrame(0x0000, CT_JSON, new Uint8Array(0));
    expect(frame.byteLength).toBe(4 + 3); // 4 bytes length + 2 op + 1 ct
    const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength);
    expect(view.getUint32(0, false)).toBe(3); // length excludes itself
  });
});
