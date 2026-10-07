import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { realpathSync } from "node:fs";
import { basename } from "node:path";
const exec = promisify(execFile);
type ReadProcess = (command: string, args: string[], options: { maxBuffer: number; timeout: number }) => Promise<{ stdout: string; stderr: string }>;

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
      if (rollouts.size !== 1 || !rollouts.has(canonical)) hold("missing, different or competing open rollout");
      const afterStart = (await read("/bin/ps", ["-p", String(pid), "-o", "lstart="], { maxBuffer: 4096, timeout: 3000 })).stdout.trim();
      const afterArgv = (await read("/bin/ps", ["-p", String(pid), "-o", "args="], { maxBuffer: 1024 * 1024, timeout: 3000 })).stdout.trim();
      if (afterStart !== start || afterArgv !== argv) hold("process drift during open-file observation");
      return { start, argv };
    } catch (error) {
      if (error instanceof Error && error.message.startsWith("codex_native_thread_unbound:")) throw error;
      return hold("kernel process/open-file observation unavailable");
    }
  };
  const before = await observe();
  return async () => {
    const after = await observe();
    if (after.start !== before.start || after.argv !== before.argv) hold("process start/fingerprint drift");
  };
}
