import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { lstatSync, realpathSync } from "node:fs";
import { createHash } from "node:crypto";
import { forEachJsonlLine } from "./rotation-native-proof.js";
import { basename } from "node:path";
const exec = promisify(execFile);
type ReadProcess = (command: string, args: string[], options: { maxBuffer: number; timeout: number }) => Promise<{ stdout: string; stderr: string }>;

/** Codex retains completed child rollouts in the same native process. Their
 * first metadata record identifies the child; forked history can subsequently
 * contain an inherited parent metadata record. Unrelated or unfinished threads
 * remain competing work and must hold maintenance. */
function completedChildFingerprints(paths: Set<string>, saved: string): string {
  if (paths.size === 1) return "";
  const hold = (): never => { throw new Error("codex_native_thread_unbound: different, unfinished or changed child rollout"); };
  const inspect = (file: string, parentId?: string) => {
    if (realpathSync(file) !== file) return hold();
    const before = lstatSync(file);
    if (!before.isFile() || before.isSymbolicLink()) return hold();
    let metadata: Record<string, unknown> | undefined;
    let first = true, turn: string | undefined, completed = false, bornAt = NaN;
    const hash = createHash("sha256"), decoder = new TextDecoder("utf-8", { fatal: true });
    forEachJsonlLine(file, bytes => {
      if (!first && parentId === undefined) return;
      hash.update(bytes).update("\n");
      const row = JSON.parse(decoder.decode(bytes));
      if (first) {
        first = false;
        if (row.type !== "session_meta" || !row.payload || typeof row.payload !== "object") return hold();
        metadata = row.payload;
        bornAt = typeof metadata?.timestamp === "string" ? Date.parse(metadata.timestamp) : NaN;
        if (parentId !== undefined && !Number.isFinite(bornAt)) return hold();
        return;
      }
      if (row.type !== "event_msg") return;
      const event = row.payload;
      if (!event || typeof event.type !== "string") return hold();
      if (event.type.startsWith("task_") || event.type === "turn_aborted") {
        const at = typeof row.timestamp === "string" ? Date.parse(row.timestamp) : NaN;
        if (!Number.isFinite(at)) return hold();
        // Inherited parent completions cannot establish that this child ran.
        if (at <= bornAt) return;
      }
      if (event.type === "task_started") {
        turn = typeof event.turn_id === "string" && event.turn_id ? event.turn_id : undefined;
        completed = false;
      } else if (event.type === "task_complete") {
        completed = !!turn && event.turn_id === turn;
      } else if (event.type.startsWith("task_") || event.type === "turn_aborted") {
        completed = false;
      }
    });
    const after = lstatSync(file);
    if (before.dev !== after.dev || before.ino !== after.ino || before.size !== after.size
      || before.mtimeMs !== after.mtimeMs || before.ctimeMs !== after.ctimeMs) return hold();
    const id = metadata?.id;
    if (typeof id !== "string" || !/^[0-9a-f-]{36}$/i.test(id) || !basename(file).endsWith(`-${id}.jsonl`)) return hold();
    if (parentId !== undefined) {
      const source = metadata?.source as { subagent?: { thread_spawn?: { parent_thread_id?: unknown } } } | undefined;
      if (id === parentId || metadata?.parent_thread_id !== parentId || metadata?.thread_source !== "subagent"
        || source?.subagent?.thread_spawn?.parent_thread_id !== parentId || !completed) return hold();
    }
    return { id, fingerprint: [file, after.dev, after.ino, after.size, after.mtimeMs, after.ctimeMs, hash.digest("hex")] };
  };
  try {
    const parent = inspect(saved);
    return JSON.stringify([...paths].filter(file => file !== saved).sort().map(file => inspect(file, parent.id).fingerprint));
  } catch (error) {
    if (error instanceof Error && error.message.startsWith("codex_native_thread_unbound:")) throw error;
    return hold();
  }
}

/** Open rollout ownership is required even for resume argv: TUI /new or /resume
 * can change the live thread without changing argv. Never fall back to token age. */
export async function proveCodexNativeThread(pid: number, savedPath: string, expectedArgv: string,
  read: ReadProcess = exec) {
  const hold = (reason: string): never => { throw new Error(`codex_native_thread_unbound: ${reason}`); };
  let canonical: string;
  try { canonical = realpathSync(savedPath); } catch { return hold("saved rollout unavailable"); }
  if (canonical !== savedPath || !/^rollout-.*\.jsonl$/.test(basename(canonical))) hold("saved rollout path is not canonical");
  const observe = async () => {
    try {
      const start = (await read("/bin/ps", ["-p", String(pid), "-o", "lstart="], { maxBuffer: 4096, timeout: 3000 })).stdout.trim();
      const argv = (await read("/bin/ps", ["-p", String(pid), "-o", "args="], { maxBuffer: 1024 * 1024, timeout: 3000 })).stdout.trim();
      if (!start || !argv || argv !== expectedArgv.trim()) hold("process fingerprint changed");
      const files = (await read("/usr/sbin/lsof", ["-a", "-p", String(pid), "-Fn"], { maxBuffer: 1024 * 1024, timeout: 3000 })).stdout;
      const processIds = files.split("\n").filter(line => /^p\d+$/.test(line));
      if (processIds.length !== 1 || processIds[0] !== `p${pid}`) hold("open-file PID mismatch");
      const rollouts = new Set(files.split("\n").filter(line => line.startsWith("n")
        && /^rollout-.*\.jsonl(?: .*|)$/.test(basename(line.slice(1)))).map(line => line.slice(1)));
      if (!rollouts.has(canonical)) hold("missing, different or competing open rollout");
      const children = completedChildFingerprints(rollouts, canonical);
      const afterStart = (await read("/bin/ps", ["-p", String(pid), "-o", "lstart="], { maxBuffer: 4096, timeout: 3000 })).stdout.trim();
      const afterArgv = (await read("/bin/ps", ["-p", String(pid), "-o", "args="], { maxBuffer: 1024 * 1024, timeout: 3000 })).stdout.trim();
      if (afterStart !== start || afterArgv !== argv) hold("process drift during open-file observation");
      return { start, argv, children };
    } catch (error) {
      if (error instanceof Error && error.message.startsWith("codex_native_thread_unbound:")) throw error;
      return hold("kernel process/open-file observation unavailable");
    }
  };
  const before = await observe();
  return async () => {
    const after = await observe();
    if (after.start !== before.start || after.argv !== before.argv || after.children !== before.children) hold("process start/fingerprint drift");
  };
}
