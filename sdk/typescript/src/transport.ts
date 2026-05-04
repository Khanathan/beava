import { RegistrationError } from "./errors.js";

export class HttpTransport {
  constructor(
    private readonly baseUrl: string,
    private readonly timeoutMs = 30_000,
  ) {}

  async request<T = unknown>(method: string, path: string, body: unknown): Promise<T> {
    const url = `${this.baseUrl.replace(/\/+$/, "")}${path}`;
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), this.timeoutMs);
    try {
      const init: RequestInit = {
        method,
        headers: { "content-type": "application/json" },
        signal: ctrl.signal,
      };
      if (body !== undefined && method !== "GET") {
        init.body = JSON.stringify(body);
      }
      const resp = await fetch(url, init);
      const text = await resp.text();
      const parsed: unknown = text.length === 0 ? {} : JSON.parse(text);
      if (!resp.ok) {
        const env = parsed as {
          error?: { code?: string; path?: string; reason?: string; errors?: unknown[] };
        };
        const err = env.error ?? { code: "http_error", reason: `http ${resp.status}` };
        throw new RegistrationError({
          code: err.code ?? "http_error",
          path: err.path,
          message: err.reason ?? `http ${resp.status}`,
          errors: err.errors as unknown[] | undefined,
        });
      }
      return parsed as T;
    } finally {
      clearTimeout(timer);
    }
  }

  async close(): Promise<void> {
    // fetch is fire-and-forget; nothing to clean up.
  }
}
