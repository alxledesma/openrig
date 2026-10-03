import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { open, realpath } from "node:fs/promises";
import path from "node:path";
const execute = promisify(execFile);
export interface RolloutProbe {
  run(command: string, args: string[]): Promise<string>;
  canonical(file: string): Promise<string>;
  prefix(file: string): Promise<string>;
}
const defaults: RolloutProbe = {
  async run(command, args) { return (await execute(command, args, { timeout: 2000, maxBuffer: 1024 * 1024 })).stdout; },
  canonical: realpath,
  async prefix(file) {
    const handle = await open(file, "r");
    try { const buffer = Buffer.alloc(65536); const { bytesRead } = await handle.read(buffer, 0, buffer.length, 0); return buffer.subarray(0, bytesRead).toString("utf8"); }
    finally { await handle.close(); }
  },
};
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
/** Bind only an open native rollout to its still-live exact PID. No directory scan,
 * newest-file heuristic, command arguments, environment, or conversation logging. */
export async function readOpenCodexRollout(pid: number, io: RolloutProbe = defaults, expectedStartedAt?: string): Promise<string | undefined> {
  if (!Number.isSafeInteger(pid) || pid <= 0) return undefined;
  try {
    const identity = async () => {
      const started = (await io.run("ps", ["-p", String(pid), "-o", "lstart="])).trim();
      const command = (await io.run("ps", ["-p", String(pid), "-o", "comm="])).trim();
      return started && (!expectedStartedAt || started.replace(/\s+/g, " ") === expectedStartedAt.trim().replace(/\s+/g, " ")) && path.basename(command) === "codex" ? `${started}\n${command}` : undefined;
    };
    const before = await identity();
    if (!before) return undefined;
    const cwdNames = (await io.run("lsof", ["-a", "-p", String(pid), "-d", "cwd", "-Fn"])).split("\n").filter(line => line.startsWith("n")).map(line => line.slice(1));
    if (cwdNames.length !== 1) return undefined;
    const cwd = await io.canonical(cwdNames[0]!);
    const names = (await io.run("lsof", ["-a", "-p", String(pid), "-d", "0-999", "-Fn"])).split("\n").filter(line => line.startsWith("n") && /\/rollout-[^/]+\.jsonl$/.test(line.slice(1))).map(line => line.slice(1));
    const files = [...new Set(await Promise.all(names.map(name => io.canonical(name))))];
    if (files.length !== 1) return undefined;
    const first = (await io.prefix(files[0]!)).split("\n", 1)[0];
    if (!first) return undefined;
    const meta = JSON.parse(first);
    if (meta.type !== "session_meta" || !uuid.test(meta.payload?.id ?? "") || typeof meta.payload?.cwd !== "string") return undefined;
    if (!files[0]!.endsWith(`${meta.payload.id}.jsonl`) || await io.canonical(meta.payload.cwd) !== cwd) return undefined;
    const stillOpenNames = (await io.run("lsof", ["-a", "-p", String(pid), "-d", "0-999", "-Fn"])).split("\n").filter(line => line.startsWith("n") && /\/rollout-[^/]+\.jsonl$/.test(line.slice(1))).map(line => line.slice(1));
    const stillOpen = [...new Set(await Promise.all(stillOpenNames.map(name => io.canonical(name))))];
    if (stillOpen.length !== 1 || stillOpen[0] !== files[0] || await identity() !== before) return undefined;
    return meta.payload.id;
  } catch { return undefined; }
}
