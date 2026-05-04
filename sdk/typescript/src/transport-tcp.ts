import { Socket, createConnection } from "node:net";
import { encodeFrame, decodeFrame, CT_JSON, OP_ERROR_RESPONSE } from "./wire.js";
import { RegistrationError } from "./index.js";

interface PendingRequest {
  resolve: (body: unknown) => void;
  reject: (e: Error) => void;
}

export class TcpTransport {
  private sock: Socket | null = null;
  private buffer = Buffer.alloc(0);
  private queue: PendingRequest[] = [];
  private connectPromise: Promise<void> | null = null;

  constructor(
    private readonly host: string,
    private readonly port: number,
    private readonly timeoutMs = 30_000,
  ) {}

  private connect(): Promise<void> {
    if (this.sock) return Promise.resolve();
    if (this.connectPromise) return this.connectPromise;
    this.connectPromise = new Promise((resolve, reject) => {
      const s = createConnection({ host: this.host, port: this.port }, () => {
        this.sock = s;
        resolve();
      });
      s.on("data", (chunk: Buffer) => this.onData(chunk));
      s.on("error", (e) => {
        this.failAll(e);
        reject(e);
      });
      s.on("close", () => {
        this.sock = null;
        this.connectPromise = null;
        this.failAll(new Error("tcp connection closed"));
      });
    });
    return this.connectPromise;
  }

  private onData(chunk: Buffer): void {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    while (this.buffer.byteLength >= 4) {
      const len = this.buffer.readUInt32BE(0);
      const total = 4 + len;
      if (this.buffer.byteLength < total) return;
      const frame = this.buffer.subarray(0, total);
      this.buffer = this.buffer.subarray(total);
      const decoded = decodeFrame(
        new Uint8Array(frame.buffer, frame.byteOffset, frame.byteLength),
      );
      const pending = this.queue.shift();
      if (!pending) {
        this.failAll(new Error("unexpected frame (no pending request)"));
        return;
      }
      try {
        const text = new TextDecoder().decode(decoded.payload);
        const body = text.length === 0 ? {} : JSON.parse(text);
        if (decoded.op === OP_ERROR_RESPONSE) {
          const env = body as {
            error?: { code?: string; path?: string; reason?: string; errors?: unknown[] };
          };
          const err = env.error ?? { code: "wire_error", reason: "OP_ERROR_RESPONSE" };
          pending.reject(
            new RegistrationError({
              code: err.code ?? "wire_error",
              path: err.path,
              message: err.reason ?? "wire error",
              errors: err.errors as unknown[] | undefined,
            }),
          );
        } else {
          pending.resolve(body);
        }
      } catch (e) {
        pending.reject(e as Error);
      }
    }
  }

  private failAll(e: Error): void {
    const pending = this.queue.splice(0, this.queue.length);
    for (const p of pending) p.reject(e);
  }

  async send<T = unknown>(opcode: number, body: unknown): Promise<T> {
    await this.connect();
    if (!this.sock) throw new Error("tcp transport not connected");
    const payload = new TextEncoder().encode(body === undefined ? "" : JSON.stringify(body));
    const frame = encodeFrame(opcode, CT_JSON, payload);
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error(`tcp request timed out after ${this.timeoutMs}ms`)),
        this.timeoutMs,
      );
      this.queue.push({
        resolve: (b) => {
          clearTimeout(timer);
          resolve(b as T);
        },
        reject: (e) => {
          clearTimeout(timer);
          reject(e);
        },
      });
      this.sock!.write(frame);
    });
  }

  async close(): Promise<void> {
    this.failAll(new Error("transport closed"));
    if (this.sock) {
      const s = this.sock;
      this.sock = null;
      await new Promise<void>((res) => s.end(() => res()));
    }
  }
}
