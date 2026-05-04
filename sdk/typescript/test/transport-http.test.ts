import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { createServer, Server } from "node:http";
import { AddressInfo } from "node:net";
import { HttpTransport } from "../src/transport.js";
import { RegistrationError } from "../src/index.js";

describe("HttpTransport", () => {
  let server: Server;
  let baseUrl: string;

  beforeAll(async () => {
    server = createServer((req, res) => {
      let body = "";
      req.on("data", (chunk) => (body += chunk));
      req.on("end", () => {
        if (req.method === "POST" && req.url === "/register") {
          res.writeHead(200, { "content-type": "application/json" });
          res.end(JSON.stringify({ status: "ok", registry_version: 7 }));
          return;
        }
        if (req.method === "POST" && req.url === "/reject") {
          res.writeHead(400, { "content-type": "application/json" });
          res.end(
            JSON.stringify({
              error: {
                code: "unsupported_node_kind",
                path: "nodes[0]",
                reason: "table is not supported in v0",
              },
            }),
          );
          return;
        }
        res.writeHead(404);
        res.end();
      });
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()));
    const addr = server.address() as AddressInfo;
    baseUrl = `http://127.0.0.1:${addr.port}`;
  });

  afterAll(async () => {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  });

  it("parses success body", async () => {
    const tr = new HttpTransport(baseUrl, 5000);
    const out = await tr.request<{ status: string; registry_version: number }>(
      "POST",
      "/register",
      { nodes: [] },
    );
    expect(out.status).toBe("ok");
    expect(out.registry_version).toBe(7);
  });

  it("throws RegistrationError on structured 400", async () => {
    const tr = new HttpTransport(baseUrl, 5000);
    await expect(tr.request("POST", "/reject", { nodes: [{}] })).rejects.toMatchObject({
      name: "RegistrationError",
      code: "unsupported_node_kind",
    });
    try {
      await tr.request("POST", "/reject", { nodes: [{}] });
    } catch (e) {
      expect(e).toBeInstanceOf(RegistrationError);
    }
  });
});
