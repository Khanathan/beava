// TS adapter for the cross-SDK conformance harness (Plan 13.6-07).
//
// Reads scenario.json, registers the payload, replays events, queries gets,
// prints {sdk:"typescript", results:[...]} to stdout.
//
// Imports `@beava/sdk` via the in-tree built dist/ to avoid npm-link state
// pollution. Run via:
//   node --experimental-strip-types run_ts.ts <scenario.json>
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Path resolution: this file lives at python/tests/conformance/run_ts.ts;
// the SDK build artifact lives at sdk/typescript/dist/index.js (3 levels up).
const sdkPath = path.resolve(__dirname, "../../../sdk/typescript/dist/index.js");
const sdkUrl = new URL(`file://${sdkPath}`).href;

const sdk = await import(sdkUrl);
const { BeavaApp } = sdk;

const scenarioPath = process.argv[2];
const scenario = JSON.parse(await fs.readFile(scenarioPath, "utf-8"));

const app = new BeavaApp(undefined, { test_mode: true });
try {
  await app.register(scenario.register_payload.nodes, { force: false, dry_run: false });
  for (const ev of scenario.events) {
    await app.push(ev.event_name, ev.fields);
  }
  const results: Record<string, unknown>[] = [];
  for (const g of scenario.gets) {
    if (g.key === "") {
      results.push(await app.get(g.table));
    } else {
      results.push(await app.get(g.table, g.key));
    }
  }
  process.stdout.write(JSON.stringify({ sdk: "typescript", results }));
} finally {
  await app.close();
}
