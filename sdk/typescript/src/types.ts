// Communicate-only types. Per Phase 13.6 scope amendment, the TS SDK has NO
// pipeline DSL — Descriptors are opaque pre-compiled JSON blobs from Python
// authoring or hand-written by the user.

/** A pre-compiled register node (event / table / derivation). Opaque to the SDK. */
export type Descriptor = Record<string, unknown>;

export interface RegisterResult {
  status: string;
  registry_version: number;
  added?: string[];
  removed?: string[];
  changed?: string[];
}

export interface PushResult {
  ack_lsn?: number;
  registry_version: number;
}

export interface PingResult {
  server_version: string;
  registry_version: number;
}

export interface GetRequest {
  table: string;
  key: string | (string | number | boolean)[];
  features?: string[];
}

export type FeatureRow = Record<string, unknown>;

export interface RegisterOptions {
  force?: boolean;
  dry_run?: boolean;
}

export interface AppOptions {
  /** Transport-level I/O timeout in milliseconds (default 30000). */
  timeout?: number;
  /** Test mode (mirrors Python `bv.App(test_mode=True)` per Phase 13.5 D-05; only meaningful in embed mode). */
  test_mode?: boolean;
  /** Override embed-mode binary discovery path. */
  binary_path?: string;
}
