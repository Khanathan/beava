import { HttpTransport } from "./transport.js";
import { TcpTransport } from "./transport-tcp.js";
import { spawnEmbeddedServer, teardownProcess, type SpawnedServer } from "./embed.js";
import {
  OP_PING,
  OP_REGISTER,
  OP_PUSH,
  OP_GET,
  OP_BATCH_GET,
  OP_RESET,
} from "./wire.js";
import { RegistrationError } from "./errors.js";
import type {
  AppOptions,
  Descriptor,
  RegisterOptions,
  RegisterResult,
  PushResult,
  PingResult,
  GetRequest,
  FeatureRow,
} from "./types.js";

/**
 * Internal transport adapter — uniform `request(method, path, body)` shape so
 * both HTTP and TCP backends can be swapped behind one interface.
 */
interface AdapterTransport {
  request<T>(method: string, path: string, body?: unknown): Promise<T>;
  close(): Promise<void>;
}

/** HTTP-backed adapter is a 1:1 wrapper. */
class HttpAdapter implements AdapterTransport {
  constructor(private readonly t: HttpTransport) {}
  request<T>(method: string, path: string, body?: unknown): Promise<T> {
    return this.t.request<T>(method, path, body);
  }
  close(): Promise<void> {
    return this.t.close();
  }
}

/** TCP-backed adapter — translates HTTP-style (method, path, body) into wire opcodes. */
class TcpAdapter implements AdapterTransport {
  constructor(private readonly t: TcpTransport) {}

  async request<T>(method: string, path: string, body?: unknown): Promise<T> {
    const op = this.routeToOpcode(method, path);
    let payload: unknown = body ?? {};
    if (op === OP_PUSH) {
      // path is /push/<eventName> — encode the event name into the body for TCP.
      const eventName = decodeURIComponent(path.replace(/^\/push\//, ""));
      payload = { event_name: eventName, ...(body as Record<string, unknown> | undefined) };
    }
    const resp = await this.t.send<T>(op, payload);
    return resp;
  }

  private routeToOpcode(method: string, path: string): number {
    if (method === "GET" && path === "/health") return OP_PING;
    if (method === "POST" && path === "/register") return OP_REGISTER;
    if (method === "POST" && path.startsWith("/push/")) return OP_PUSH;
    if (method === "POST" && path === "/get") return OP_GET;
    if (method === "POST" && path === "/batch-get") return OP_BATCH_GET;
    if (method === "POST" && path === "/reset") return OP_RESET;
    throw new Error(`tcp adapter: no opcode for ${method} ${path}`);
  }

  close(): Promise<void> {
    return this.t.close();
  }
}

/**
 * BeavaApp — communicate-only client.
 *
 * Per Phase 13.6 scope amendment 2026-05-03, this SDK has NO pipeline DSL.
 * Authoring lives in the Python SDK; this client pushes events, registers
 * pre-compiled JSON descriptors, and reads features.
 */
export class BeavaApp {
  readonly url: string;
  private transport: AdapterTransport | null = null;
  private embedHandle: SpawnedServer | null = null;
  private readyPromise: Promise<AdapterTransport> | null = null;
  private closed = false;
  private readonly opts: AppOptions;

  constructor(url?: string, opts: AppOptions = {}) {
    this.opts = opts;
    this.url = url ?? "";
    if (url && (url.startsWith("http://") || url.startsWith("https://"))) {
      this.transport = new HttpAdapter(new HttpTransport(url, opts.timeout ?? 30_000));
    } else if (url && url.startsWith("tcp://")) {
      const u = new URL(url);
      const port = parseInt(u.port, 10);
      this.transport = new TcpAdapter(new TcpTransport(u.hostname, port, opts.timeout ?? 30_000));
    } else if (!url) {
      // Embed mode — defer spawn until first call.
      this.transport = null;
    } else {
      throw new Error(`unsupported URL scheme: ${url}`);
    }
  }

  private async ensureReady(): Promise<AdapterTransport> {
    if (this.closed) {
      throw new Error("BeavaApp has been closed");
    }
    if (this.transport) return this.transport;
    if (this.readyPromise) return this.readyPromise;
    this.readyPromise = (async () => {
      const handle = await spawnEmbeddedServer({ testMode: this.opts.test_mode });
      this.embedHandle = handle;
      const t = new HttpAdapter(new HttpTransport(handle.httpUrl, this.opts.timeout ?? 30_000));
      this.transport = t;
      return t;
    })();
    return this.readyPromise;
  }

  // ─── Wire methods ────────────────────────────────────────────────────────

  async ping(): Promise<PingResult> {
    const t = await this.ensureReady();
    return t.request<PingResult>("GET", "/health");
  }

  async register(
    descriptors: Descriptor[],
    opts: RegisterOptions = {},
  ): Promise<RegisterResult> {
    const t = await this.ensureReady();
    return t.request<RegisterResult>("POST", "/register", {
      nodes: descriptors,
      force: opts.force ?? false,
      dry_run: opts.dry_run ?? false,
    });
  }

  async push(eventName: string, fields: Record<string, unknown>): Promise<PushResult> {
    const t = await this.ensureReady();
    return t.request<PushResult>("POST", `/push/${encodeURIComponent(eventName)}`, { fields });
  }

  /**
   * pushSync — durable-push semantics (acks=all). v0 delegates to push() because
   * OP_PUSH_SYNC is reserved per docs/wire-spec.md; v0.1+ wires the actual opcode
   * without breaking this API surface.
   */
  async pushSync(eventName: string, fields: Record<string, unknown>): Promise<PushResult> {
    return this.push(eventName, fields);
  }

  // Overloaded signatures: 1-arg = global table per ADR-003, 2-arg = per-entity.
  get(table: string): Promise<FeatureRow>;
  get(table: string, key: string | (string | number | boolean)[]): Promise<FeatureRow>;
  async get(
    table: string,
    key?: string | (string | number | boolean)[],
  ): Promise<FeatureRow> {
    const t = await this.ensureReady();
    const body = { table, key: key ?? "" };
    const resp = await t.request<FeatureRow | null>("POST", "/get", body);
    return resp ?? {};
  }

  async batchGet(requests: GetRequest[]): Promise<FeatureRow[]> {
    const t = await this.ensureReady();
    const resp = await t.request<{ results?: FeatureRow[] } | FeatureRow[]>(
      "POST",
      "/batch-get",
      { requests },
    );
    if (Array.isArray(resp)) return resp;
    return resp?.results ?? [];
  }

  async reset(): Promise<void> {
    const t = await this.ensureReady();
    await t.request<unknown>("POST", "/reset", {});
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    if (this.transport) {
      try {
        await this.transport.close();
      } catch {
        /* swallow */
      }
      this.transport = null;
    }
    if (this.embedHandle) {
      try {
        await teardownProcess(this.embedHandle.proc);
      } catch {
        /* swallow */
      }
      this.embedHandle = null;
    }
  }
}

export { RegistrationError };
