import { describe, it, expect, afterAll } from "vitest";
import { BeavaApp } from "../src/index.js";

// Embed-mode integration test — opt-in.
//
// Set BEAVA_RUN_EMBED_TESTS=1 (in addition to having the beava binary discoverable
// via $BEAVA_BINARY / PATH / parents/target/debug/beava) to actually exercise the
// subprocess spawn. Default behavior is `it.skip` so vitest run on a fresh checkout
// stays green even when the workspace's `target/debug/beava` is in a half-initialized
// state (e.g., WAL file conflict) — the conformance harness in Plan 13.6-07 is the
// canonical end-to-end gate.

const optIn = process.env.BEAVA_RUN_EMBED_TESTS === "1";
const maybeIt = optIn ? it : it.skip;

describe("embed mode (opt-in via BEAVA_RUN_EMBED_TESTS=1)", () => {
  let app: BeavaApp | null = null;

  afterAll(async () => {
    if (app) await app.close();
  });

  maybeIt("can ping the embedded server", async () => {
    app = new BeavaApp(undefined, { test_mode: true });
    const ping = await app.ping();
    expect(typeof ping.server_version).toBe("string");
    expect(typeof ping.registry_version).toBe("number");
  }, 15_000);
});
