/** Structured error surfacing the wire-spec error envelope. */
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

/** Embed-mode binary discovery failed. */
export class BinaryNotFoundError extends Error {
  searched: string[];
  constructor(message: string, searched: string[] = []) {
    super(message);
    this.name = "BinaryNotFoundError";
    this.searched = searched;
  }
}
