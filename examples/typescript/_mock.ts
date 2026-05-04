/**
 * Phase 13.0 mock backend for runnable TypeScript demos.
 *
 * In-memory App shim with MINIMAL AGGREGATION LOGIC. Drop-in for the
 * real `BeavaApp` during Phase 13.0 verification. Replaced by the real
 * `@beava/sdk` BeavaApp once Phase 13.6 lands (single-line edit per demo).
 *
 * Per Q2 locked answer + BLOCKER 4 checker fix: this mock COMPUTES
 * features by applying registered descriptors on push. Demos go through
 * the full register -> push -> query flow (no pre-seeding) so contract
 * drift between specs and the real engine surfaces immediately at
 * 13.6 re-verification.
 *
 * Supported ops in this mock (minimum for the 9 vertical demos):
 * - count: increment per matching event
 * - sum: accumulate field value
 * - mean: running sum / count
 * - min, max: comparison
 *
 * Sketches (nUnique, quantile, topK), decays, velocity, and geo ops
 * are NOT computed -- demo files document the no-op fallback inline.
 */

// camelCase API per docs/sdk-api/shared.md
// (Wire JSON keys are snake_case; the SDK transport layer would translate.)

type AggSpec = { op: string; field: string | null };

type Descriptor = {
  name: string;
  kind: "event" | "table";
  source: string | null;
  keyCols: string[];
  ops: Record<string, AggSpec>;
};

export class MockBeavaApp {
  private registered: Descriptor[] = [];
  private tables: Map<string, Map<string, Record<string, unknown>>> = new Map();
  private aggState: Map<string, Record<string, number>> = new Map();
  private registryVersion = 0;

  async register(
    descriptors: Descriptor[],
    opts?: { force?: boolean; dryRun?: boolean },
  ): Promise<{
    status: string;
    registryVersion: number;
    added: string[];
  }> {
    if (opts?.dryRun) {
      return {
        status: "ok",
        registryVersion: this.registryVersion,
        added: [],
      };
    }
    for (const d of descriptors) {
      this.registered.push(d);
    }
    this.registryVersion = this.registered.length;
    return {
      status: "ok",
      registryVersion: this.registryVersion,
      added: this.registered.map((d) => d.name),
    };
  }

  async push(
    eventName: string,
    fields: Record<string, unknown>,
  ): Promise<{ ackLsn: number; registryVersion: number }> {
    for (const desc of this.registered) {
      if (desc.kind !== "table") continue;
      if (desc.source !== eventName) continue;
      const key = this.keyFromEvent(desc.keyCols, fields);
      for (const [featureName, agg] of Object.entries(desc.ops)) {
        this.update(desc.name, key, featureName, agg, fields);
      }
    }
    return { ackLsn: 1, registryVersion: this.registryVersion };
  }

  async get(
    table: string,
    key: string | (string | number | boolean)[],
  ): Promise<Record<string, unknown>> {
    const keyStr = Array.isArray(key) ? key.map(String).join("|") : key;
    return this.tables.get(table)?.get(keyStr) ?? {};
  }

  async batchGet(
    requests: Array<{
      table: string;
      key: string | (string | number | boolean)[];
    }>,
  ): Promise<Record<string, unknown>[]> {
    const out: Record<string, unknown>[] = [];
    for (const r of requests) {
      out.push(await this.get(r.table, r.key));
    }
    return out;
  }

  async reset(): Promise<void> {
    this.tables.clear();
    this.aggState.clear();
  }

  async ping(): Promise<{ serverVersion: string; registryVersion: number }> {
    return {
      serverVersion: "0.0.0-mock",
      registryVersion: this.registryVersion,
    };
  }

  async close(): Promise<void> {
    // no-op
  }

  private keyFromEvent(
    keyCols: string[],
    event: Record<string, unknown>,
  ): string {
    if (keyCols.length === 0) return "_global";
    return keyCols.map((k) => String(event[k] ?? "")).join("|");
  }

  private update(
    table: string,
    key: string,
    feature: string,
    agg: AggSpec,
    event: Record<string, unknown>,
  ): void {
    const stateKey = `${table}|${key}|${feature}`;
    const state = this.aggState.get(stateKey) ?? {};
    const op = agg.op;
    if (op === "count") {
      state.count = (state.count ?? 0) + 1;
      this.setValue(table, key, feature, state.count);
    } else if (op === "sum") {
      const v = agg.field ? Number(event[agg.field] ?? 0) : 0;
      state.sum = (state.sum ?? 0) + v;
      this.setValue(table, key, feature, state.sum);
    } else if (op === "mean") {
      const v = agg.field ? Number(event[agg.field] ?? 0) : 0;
      state.sum = (state.sum ?? 0) + v;
      state.count = (state.count ?? 0) + 1;
      this.setValue(table, key, feature, state.sum / state.count);
    } else if (op === "min") {
      const v = agg.field ? Number(event[agg.field] ?? 0) : 0;
      state.min = state.min === undefined ? v : Math.min(state.min, v);
      this.setValue(table, key, feature, state.min);
    } else if (op === "max") {
      const v = agg.field ? Number(event[agg.field] ?? 0) : 0;
      state.max = state.max === undefined ? v : Math.max(state.max, v);
      this.setValue(table, key, feature, state.max);
    }
    // Unsupported ops in mock (sketches, decays, geo, etc.): no-op.
    this.aggState.set(stateKey, state);
  }

  private setValue(
    table: string,
    key: string,
    feature: string,
    value: unknown,
  ): void {
    if (!this.tables.has(table)) this.tables.set(table, new Map());
    const t = this.tables.get(table)!;
    if (!t.has(key)) t.set(key, {});
    t.get(key)![feature] = value;
  }
}

// Drop-in factory: `const app = new BeavaApp();` -> swap to real `@beava/sdk`
// in Phase 13.6 by replacing this import.
export class BeavaApp extends MockBeavaApp {}

// Demo helpers -- describe descriptors inline, in real Beava these would
// be `bv.event({...})` / `bv.table({...})` builder calls.

export function event(name: string): Descriptor {
  return { name, kind: "event", source: null, keyCols: [], ops: {} };
}

export function table(args: {
  name: string;
  source: string;
  key: string | string[];
  ops: Record<string, [string, string | null]>;
}): Descriptor {
  const keyCols = typeof args.key === "string" ? [args.key] : args.key;
  const ops: Record<string, AggSpec> = {};
  for (const [feat, [op, field]] of Object.entries(args.ops)) {
    ops[feat] = { op, field };
  }
  return {
    name: args.name,
    kind: "table",
    source: args.source,
    keyCols,
    ops,
  };
}
