import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { createServer, Server } from "node:net";
import { AddressInfo } from "node:net";
import { TcpTransport } from "../src/transport-tcp.js";
import { OP_PING, OP_GET_RESPONSE, CT_JSON, encodeFrame, decodeFrame } from "../src/wire.js";

describe("TcpTransport", () => {
  let server: Server;
  let port: number;

  beforeAll(async () => {
    server = createServer((sock) => {
      let buf = Buffer.alloc(0);
      let counter = 0;
      sock.on("data", (chunk) => {
        buf = Buffer.concat([buf, chunk]);
        // Process whole frames
        while (buf.byteLength >= 4) {
          const len = buf.readUInt32BE(0);
          const total = 4 + len;
          if (buf.byteLength < total) return;
          const frameBytes = buf.subarray(0, total);
          buf = buf.subarray(total);
          decodeFrame(new Uint8Array(frameBytes.buffer, frameBytes.byteOffset, frameBytes.byteLength));
          counter++;
          const respPayload = new TextEncoder().encode(JSON.stringify({ seq: counter }));
          const out = encodeFrame(OP_GET_RESPONSE, CT_JSON, respPayload);
          sock.write(out);
        }
      });
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()));
    port = (server.address() as AddressInfo).port;
  });

  afterAll(async () => {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  });

  it("preserves FIFO ordering across concurrent sends", async () => {
    const tcp = new TcpTransport("127.0.0.1", port, 5000);
    const results = await Promise.all([
      tcp.send<{ seq: number }>(OP_PING, {}),
      tcp.send<{ seq: number }>(OP_PING, {}),
      tcp.send<{ seq: number }>(OP_PING, {}),
    ]);
    expect(results.map((r) => r.seq)).toEqual([1, 2, 3]);
    await tcp.close();
  });
});
