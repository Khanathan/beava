import { describe, it, expect, beforeAll, afterAll, beforeEach } from "vitest";
import { createServer, IncomingMessage, ServerResponse, Server } from "node:http";
import { AddressInfo } from "node:net";
import { BeavaApp, RegistrationError } from "../src/index.js";

interface MockState {
  lastBody: string;
  lastMethod: string;
  lastPath: string;
  reply: { status: number; body: unknown };
}

let state: MockState;
let server: Server;
let baseUrl: string;

function setReply(status: number, body: unknown): void {
  state.reply = { status, body };
}

beforeAll(async () => {
  state = { lastBody: "", lastMethod: "", lastPath: "", reply: { status: 200, body: {} } };
  server = createServer((req: IncomingMessage, res: ServerResponse) => {
    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", () => {
      state.lastBody = body;
      state.lastMethod = req.method ?? "";
      state.lastPath = req.url ?? "";
      res.writeHead(state.reply.status, { "content-type": "application/json" });
      const respBody =
        typeof state.reply.body === "string"
          ? state.reply.body
          : JSON.stringify(state.reply.body);
      res.end(respBody);
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()));
  const addr = server.address() as AddressInfo;
  baseUrl = `http://127.0.0.1:${addr.port}`;
});

afterAll(async () => {
  await new Promise<void>((resolve) => server.close(() => resolve()));
});

beforeEach(() => {
  state.lastBody = "";
  state.lastMethod = "";
  state.lastPath = "";
  state.reply = { status: 200, body: {} };
});

describe("BeavaApp.ping", () => {
  it("returns server_version + registry_version", async () => {
    setReply(200, { server_version: "v0", registry_version: 1 });
    const app = new BeavaApp(baseUrl);
    const ping = await app.ping();
    expect(ping.server_version).toBe("v0");
    expect(ping.registry_version).toBe(1);
    expect(state.lastMethod).toBe("GET");
    expect(state.lastPath).toBe("/health");
    await app.close();
  });
});

describe("BeavaApp.register", () => {
  it("posts {nodes, force, dry_run}", async () => {
    setReply(200, { status: "ok", registry_version: 2 });
    const app = new BeavaApp(baseUrl);
    const result = await app.register(
      [{ kind: "event", name: "Click" } as Record<string, unknown>],
      { force: true, dry_run: false },
    );
    expect(result.status).toBe("ok");
    expect(result.registry_version).toBe(2);
    expect(state.lastMethod).toBe("POST");
    expect(state.lastPath).toBe("/register");
    const parsed = JSON.parse(state.lastBody);
    expect(parsed).toEqual({
      nodes: [{ kind: "event", name: "Click" }],
      force: true,
      dry_run: false,
    });
    await app.close();
  });
});

describe("BeavaApp.push", () => {
  it("posts to /push/<eventName> with {fields}", async () => {
    setReply(200, { ack_lsn: 42, registry_version: 3 });
    const app = new BeavaApp(baseUrl);
    const result = await app.push("Click", { user: "alice", n: 1 });
    expect(result.ack_lsn).toBe(42);
    expect(state.lastMethod).toBe("POST");
    expect(state.lastPath).toBe("/push/Click");
    const parsed = JSON.parse(state.lastBody);
    expect(parsed).toEqual({ fields: { user: "alice", n: 1 } });
    await app.close();
  });

  it("pushSync delegates to push (OP_PUSH_SYNC reserved)", async () => {
    setReply(200, { ack_lsn: 43, registry_version: 3 });
    const app = new BeavaApp(baseUrl);
    const result = await app.pushSync("Click", { x: 1 });
    expect(result.ack_lsn).toBe(43);
    expect(state.lastPath).toBe("/push/Click");
    await app.close();
  });
});

describe("BeavaApp.get", () => {
  it("get(table, key) posts {table, key}", async () => {
    setReply(200, { c: 7 });
    const app = new BeavaApp(baseUrl);
    const row = await app.get("UserCounts", "alice");
    expect(row).toEqual({ c: 7 });
    expect(state.lastPath).toBe("/get");
    expect(JSON.parse(state.lastBody)).toEqual({ table: "UserCounts", key: "alice" });
    await app.close();
  });

  it("get(table) posts {table, key:''} (global per ADR-003)", async () => {
    setReply(200, { total: 99 });
    const app = new BeavaApp(baseUrl);
    const row = await app.get("Total");
    expect(row).toEqual({ total: 99 });
    expect(JSON.parse(state.lastBody)).toEqual({ table: "Total", key: "" });
    await app.close();
  });

  it("returns {} for cold-start", async () => {
    setReply(200, {});
    const app = new BeavaApp(baseUrl);
    const row = await app.get("X", "y");
    expect(row).toEqual({});
    await app.close();
  });

  it("accepts composite key array", async () => {
    setReply(200, {});
    const app = new BeavaApp(baseUrl);
    await app.get("T", ["a", 42, true]);
    expect(JSON.parse(state.lastBody)).toEqual({ table: "T", key: ["a", 42, true] });
    await app.close();
  });
});

describe("BeavaApp.batchGet", () => {
  it("posts {requests} and returns rows in order", async () => {
    setReply(200, [{ a: 1 }, {}, { b: 2 }]);
    const app = new BeavaApp(baseUrl);
    const rows = await app.batchGet([
      { table: "T1", key: "a" },
      { table: "T2", key: "b" },
      { table: "T3", key: "c" },
    ]);
    expect(rows).toEqual([{ a: 1 }, {}, { b: 2 }]);
    expect(state.lastPath).toBe("/batch-get");
    await app.close();
  });

  it("rejects whole batch on per-entry error (no partial success)", async () => {
    setReply(400, {
      error: { code: "unknown_table", path: "requests[1].table", reason: "T2 not registered" },
    });
    const app = new BeavaApp(baseUrl);
    await expect(
      app.batchGet([
        { table: "T1", key: "a" },
        { table: "T2", key: "b" },
      ]),
    ).rejects.toBeInstanceOf(RegistrationError);
    await app.close();
  });
});

describe("BeavaApp.reset", () => {
  it("posts {} to /reset", async () => {
    setReply(200, {});
    const app = new BeavaApp(baseUrl);
    await app.reset();
    expect(state.lastMethod).toBe("POST");
    expect(state.lastPath).toBe("/reset");
    await app.close();
  });

  it("surfaces 403 when not in test_mode", async () => {
    setReply(403, { error: { code: "reset_forbidden", reason: "test_mode required" } });
    const app = new BeavaApp(baseUrl);
    await expect(app.reset()).rejects.toMatchObject({
      name: "RegistrationError",
      code: "reset_forbidden",
    });
    await app.close();
  });
});

describe("BeavaApp.close", () => {
  it("is idempotent", async () => {
    const app = new BeavaApp(baseUrl);
    await app.close();
    await expect(app.close()).resolves.toBeUndefined();
  });
});
