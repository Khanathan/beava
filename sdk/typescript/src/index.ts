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

export { BeavaApp } from "./app.js";
export { RegistrationError, BinaryNotFoundError } from "./errors.js";

// Wire layer + transports + embed (Plan 13.6-03).
export * from "./wire.js";
export { HttpTransport } from "./transport.js";
export { TcpTransport } from "./transport-tcp.js";
export { spawnEmbeddedServer, teardownProcess, teardownServer, discoverBinary } from "./embed.js";
export type { SpawnedServer, SpawnOptions } from "./embed.js";
