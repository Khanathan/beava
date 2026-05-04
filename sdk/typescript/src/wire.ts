// Wire-spec opcode + content-type constants. See docs/wire-spec.md.
export const OP_PING = 0x0000;
export const OP_REGISTER = 0x0001;
export const OP_PUSH = 0x0010;
export const OP_GET = 0x0020;
export const OP_GET_RESPONSE = 0x0023;
export const OP_BATCH_GET = 0x0024;
export const OP_RESET = 0x0040;
export const OP_ERROR_RESPONSE = 0xffff;

export const CT_JSON = 0x01;

export interface DecodedFrame {
  op: number;
  contentType: number;
  payload: Uint8Array;
}

/** Encode a frame: [u32 length BE][u16 op BE][u8 content_type][payload]. length = 3 + payload. */
export function encodeFrame(op: number, contentType: number, payload: Uint8Array): Uint8Array {
  const len = 3 + payload.byteLength;
  const buf = new Uint8Array(4 + len);
  const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  view.setUint32(0, len, false); // big-endian
  view.setUint16(4, op, false);
  view.setUint8(6, contentType);
  buf.set(payload, 7);
  return buf;
}

/** Decode a single frame from `buf`. Returns the parsed frame; throws on truncation / oversize. */
export function decodeFrame(buf: Uint8Array): DecodedFrame {
  if (buf.byteLength < 7) {
    throw new Error(`frame too short: ${buf.byteLength} bytes (minimum 7)`);
  }
  const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  const len = view.getUint32(0, false);
  if (4 + len !== buf.byteLength) {
    throw new Error(`frame length mismatch: header says ${4 + len}, got ${buf.byteLength}`);
  }
  const op = view.getUint16(4, false);
  const contentType = view.getUint8(6);
  const payload = buf.subarray(7);
  return { op, contentType, payload };
}
