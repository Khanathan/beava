import { spawn, ChildProcess } from "node:child_process";
import { existsSync, statSync, accessSync, constants as fsConstants } from "node:fs";
import { dirname, resolve as pathResolve } from "node:path";
import { createInterface } from "node:readline";
import { BinaryNotFoundError } from "./index.js";

/** 4-step binary discovery, mirroring python/beava/_embed.py. */
export function discoverBinary(): string {
  // Step 1: BEAVA_BINARY env var — explicit override; MUST be valid if set.
  const envVal = process.env.BEAVA_BINARY;
  if (envVal !== undefined && envVal !== "") {
    if (existsSync(envVal) && statSync(envVal).isFile()) {
      try {
        accessSync(envVal, fsConstants.X_OK);
        return envVal;
      } catch {
        /* fall through to error */
      }
    }
    throw new BinaryNotFoundError(
      `BEAVA_BINARY=${JSON.stringify(envVal)} is set but the path is not an executable file. Unset BEAVA_BINARY or fix the path.`,
      [envVal],
    );
  }

  // Step 2: beava on PATH.
  const pathDirs = (process.env.PATH ?? "").split(":").filter(Boolean);
  for (const dir of pathDirs) {
    const candidate = `${dir}/beava`;
    if (existsSync(candidate) && statSync(candidate).isFile()) {
      try {
        accessSync(candidate, fsConstants.X_OK);
        return candidate;
      } catch {
        continue;
      }
    }
  }

  // Step 3: Walk upward from CWD looking for target/debug/beava.
  const searched: string[] = [];
  let dir = process.cwd();
  while (true) {
    const candidate = pathResolve(dir, "target/debug/beava");
    searched.push(candidate);
    if (existsSync(candidate) && statSync(candidate).isFile()) {
      try {
        accessSync(candidate, fsConstants.X_OK);
        return candidate;
      } catch {
        /* not executable */
      }
    }
    const parent = dirname(dir);
    if (parent === dir) break;
    dir = parent;
  }

  // Step 4: Not found.
  throw new BinaryNotFoundError(
    "beava binary not found. Install with one of:\n" +
      "  brew install beava\n" +
      "  pip install beava[server]\n" +
      "  docker pull beava/beava\n" +
      "Or set BEAVA_BINARY=/path/to/beava.",
    searched,
  );
}

export interface SpawnedServer {
  proc: ChildProcess;
  httpUrl: string;
  tcpUrl: string;
}

export interface SpawnOptions {
  startupTimeoutMs?: number;
  testMode?: boolean;
}

/** Spawn a local beava server on ephemeral ports and wait until both bind events appear. */
export async function spawnEmbeddedServer(opts: SpawnOptions = {}): Promise<SpawnedServer> {
  const timeoutMs = opts.startupTimeoutMs ?? 5000;
  const binary = discoverBinary();

  const env: NodeJS.ProcessEnv = {
    ...process.env,
    BEAVA_LISTEN_ADDR: "127.0.0.1:0",
    BEAVA_TCP_PORT: "0",
    BEAVA_DEV_ENDPOINTS: "1",
  };
  if (opts.testMode) {
    env.BEAVA_TEST_MODE = "1";
  }

  const proc = spawn(binary, ["--config", "/dev/null"], {
    env,
    stdio: ["ignore", "pipe", "ignore"],
  });

  let httpAddr: string | null = null;
  let tcpAddr: string | null = null;

  const promise = new Promise<SpawnedServer>((resolve, reject) => {
    const rl = createInterface({ input: proc.stdout! });
    const timer = setTimeout(() => {
      rl.close();
      try {
        proc.kill("SIGKILL");
      } catch {
        /* ignore */
      }
      reject(
        new Error(
          `embed-mode server did not bind within ${timeoutMs}ms (http=${httpAddr ?? "null"}, tcp=${tcpAddr ?? "null"})`,
        ),
      );
    }, timeoutMs);

    rl.on("line", (line) => {
      try {
        const rec = JSON.parse(line) as { kind?: string; addr?: string };
        if (rec.kind === "server.http_bound" && rec.addr) {
          httpAddr = rec.addr;
        } else if (rec.kind === "server.tcp_bound" && rec.addr) {
          tcpAddr = rec.addr;
        }
        if (httpAddr && tcpAddr) {
          clearTimeout(timer);
          resolve({
            proc,
            httpUrl: `http://${httpAddr}`,
            tcpUrl: `tcp://${tcpAddr}`,
          });
        }
      } catch {
        // non-JSON line (banner) — ignore during startup
      }
    });
    proc.on("exit", (code) => {
      if (!httpAddr || !tcpAddr) {
        clearTimeout(timer);
        reject(new Error(`embed beava exited (code=${code}) before binding`));
      }
    });
  });

  return promise;
}

/** Send SIGTERM, wait, SIGKILL on timeout. */
export async function teardownProcess(
  proc: ChildProcess,
  timeoutMs = 5000,
): Promise<void> {
  if (proc.exitCode !== null) return;
  return new Promise<void>((resolve) => {
    const timer = setTimeout(() => {
      try {
        proc.kill("SIGKILL");
      } catch {
        /* ignore */
      }
    }, timeoutMs);
    proc.once("exit", () => {
      clearTimeout(timer);
      resolve();
    });
    try {
      proc.kill("SIGTERM");
    } catch {
      clearTimeout(timer);
      resolve();
    }
  });
}
