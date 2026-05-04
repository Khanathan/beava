export type {
  Descriptor,
  RegisterResult,
  PushResult,
  PingResult,
  GetRequest,
  FeatureRow,
  RegisterOptions,
  AppOptions,
} from "./types.js";

import type { AppOptions } from "./types.js";

/**
 * BeavaApp — placeholder constructor; wire I/O lands in Plan 13.6-03.
 *
 * Per Phase 13.6 scope amendment 2026-05-03, this SDK is COMMUNICATE-ONLY:
 * push events, register pre-compiled JSON, get/batch_get features. No
 * pipeline DSL (use Python for authoring).
 */
export class BeavaApp {
  readonly url: string;

  constructor(url?: string, _opts?: AppOptions) {
    // Embed mode (no URL) lands in Plan 13.6-03; for now require an explicit URL.
    if (!url) {
      throw new Error("embed mode not yet implemented (lands in Plan 13.6-03)");
    }
    this.url = url;
  }
}

// Errors (full bodies land in Plan 13.6-05 alongside the wire I/O methods).
export class RegistrationError extends Error {
  code: string;
  path?: string;
  errors?: unknown[];
  constructor(opts: { code: string; path?: string; message: string; errors?: unknown[] }) {
    super(opts.message);
    this.name = "RegistrationError";
    this.code = opts.code;
    this.path = opts.path;
    this.errors = opts.errors;
  }
}

export class BinaryNotFoundError extends Error {
  searched: string[];
  constructor(message: string, searched: string[] = []) {
    super(message);
    this.name = "BinaryNotFoundError";
    this.searched = searched;
  }
}
