// C1 REAL CLOSE REGRESSION — packages/daemon/test/legacy-pi-native-discovered-close.test.ts
//
// ROOT-LEGACY-CORRECTIONS-INDEPENDENT-REVIEW.json finding C1 required EXECUTED evidence that the
// production makeNativeLegacyPiTransportSource.closeDiscoveredListener really closes a stray
// loopback inspector, not an injected fixture proving itself.
//
// Everything here is genuine:
//   - the stray listener is a REAL disposable Node 22 child this test owns and spawns itself,
//     listening on 127.0.0.1 with a kernel-assigned random port (--inspect=127.0.0.1:0);
//   - ownership and absence are proven by GENUINE pid-scoped lsof and by the process start
//     identity, never by a stub;
//   - the close is performed by the production InspectorClient over a real inspector websocket.
//
// SAFETY: this file only ever signals or attaches to a pid it spawned itself in this process and
// tracks by its own child handle. It never resolves, touches, signals or attaches to any managed
// production seat or any prior pilot agent. Every assertion is scoped to the owned child pid, and
// the child is killed in afterEach by handle, never by name or pattern.
import { spawn, type ChildProcess } from "node:child_process";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { realpathSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { makeNativeLegacyPiTransportSource, readProcessIdentity } from "../src/domain/legacy-pi-native-witness.js";

const execFileAsync = promisify(execFile);
const HERE = path.dirname(realpathSync(fileURLToPath(import.meta.url)));

/** Owned children only. Keyed by pid so nothing can ever be resolved by session name or pattern. */
const owned = new Map<number, ChildProcess>();

afterEach(async () => {
  for (const [pid, child] of owned) {
    // Kill strictly by our own handle, and only if it is still the pid we spawned.
    if (child.pid !== pid) continue;
    try { child.kill("SIGKILL"); } catch { /* already gone */ }
  }
  owned.clear();
});

interface Stray {
  child: ChildProcess;
  pid: number;
  port: number;
  /** Process start identity, captured before the close, used to prove the SAME process survived. */
  startBefore: string;
}

/**
 * Spawn a disposable Node 22 child that opens a stray loopback inspector on a random port and
 * then stays alive holding it open. Returns only after the real "Debugger listening" line proves
 * the listener is up, so no test ever races its own fixture.
 */
async function spawnStrayInspector(): Promise<Stray> {
  const child = spawn(
    process.execPath,
    ["--inspect=127.0.0.1:0", "-e", "setInterval(() => {}, 1000);"],
    { stdio: ["ignore", "ignore", "pipe"], detached: false },
  );
  if (!child.pid) throw new Error("stray child did not report a pid");
  owned.set(child.pid, child);

  const port = await new Promise<number>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("no inspector line within 15s")), 15_000);
    let buffer = "";
    child.stderr?.on("data", (chunk: Buffer) => {
      buffer += chunk.toString();
      // Real Node inspector announcement. Bind loopback only.
      const match = /Debugger listening on ws:\/\/127\.0\.0\.1:(\d+)\//.exec(buffer);
      if (match) { clearTimeout(timer); resolve(Number(match[1])); }
    });
    child.once("error", (error) => { clearTimeout(timer); reject(error); });
    child.once("exit", (code) => { clearTimeout(timer); reject(new Error(`stray exited early with ${code}`)); });
  });

  // Read the start identity with the SAME production reader the close brackets against, so the
  // expectedStartedAt string is byte-exact rather than reconstructed from a different ps format.
  const identity = await readProcessIdentity(child.pid);
  const startBefore = identity?.startedAt ?? "";
  if (!startBefore) throw new Error("could not read the stray's start identity via the production reader");
  return { child, pid: child.pid, port, startBefore };
}

/** Genuine process start identity, so "same pid AND same start" can be proven later. */
async function processStartIdentity(pid: number): Promise<string | null> {
  try {
    const { stdout } = await execFileAsync("ps", ["-o", "lstart=", "-p", String(pid)]);
    return stdout.trim() || null;
  } catch { return null; }
}

/** Genuine pid-scoped lsof: listening TCP ports owned by EXACTLY this pid.
 *  Uses `-t` (terse pid output) and asks for the PID selector combined with the port selector,
 *  so the answer is the pid list itself rather than a parsed table row. */
async function listeningPortsForPid(pid: number): Promise<number[]> {
  const ports = new Set<number>();
  // `-a` ANDs the selectors. Ask lsof for this pid's LISTEN endpoints and read the address column.
  // lsof EXITS NON-ZERO WHEN NOTHING MATCHES, which is exactly the post-close expectation, so a
  // non-zero exit means "this pid has no listening endpoint", not a test failure.
  try {
    const { stdout } = await execFileAsync(
      "lsof", ["-nP", "-a", "-p", String(pid), "-iTCP", "-sTCP:LISTEN", "-F", "n"],
      { maxBuffer: 1024 * 1024 },
    );
    for (const line of stdout.split("\n")) {
      if (!line.startsWith("n")) continue;
      const match = /:(\d+)$/.exec(line.slice(1).trim());
      if (match) ports.add(Number(match[1]));
    }
  } catch {
    return [];
  }
  return [...ports];
}

/** Genuine port-owner check: which pids hold a LISTEN on this exact port.
 *  Mirrors the production listenerPids() selector exactly: `-iTCP:<port> -sTCP:LISTEN -t`.
 *  lsof exits non-zero when nothing matches, which is the normal "port is free" case. */
async function listenerOwners(port: number): Promise<number[]> {
  try {
    const { stdout } = await execFileAsync("lsof", ["-nP", `-iTCP:${port}`, "-sTCP:LISTEN", "-t"]);
    return stdout.split(/\s+/).filter(Boolean).map(Number).filter(value => Number.isInteger(value) && value > 0);
  } catch {
    return [];
  }
}

async function isAlive(pid: number): Promise<boolean> {
  try { process.kill(pid, 0); return true; } catch { return false; }
}

const modules = {
  runnerModuleUrl: path.join(HERE, "..", "src", "domain", "legacy-pi-native-witness.ts"),
  piModuleUrl: path.join(HERE, "..", "src", "domain", "legacy-pi-native-witness.ts"),
};

describe("C1 real close of a discovered stray loopback inspector", () => {
  it("MATCHING start: the production close really closes the stray port, absent by lsof, with the SAME pid and start surviving", async () => {
    const stray = await spawnStrayInspector();
    const source = makeNativeLegacyPiTransportSource(modules);

    // Precondition, proven with real tools: the child owns a loopback listener on that port.
    expect(await isAlive(stray.pid)).toBe(true);
    expect(await listeningPortsForPid(stray.pid)).toContain(stray.port);
    expect(await listenerOwners(stray.port)).toEqual([stray.pid]);
    const before = await source.pidScopedListeners(stray.pid);
    expect(before.ok).toBe(true);
    expect(before.ports.map(Number)).toContain(stray.port);
    expect(before.public).toEqual([]);
    expect(await source.endpointInUse({ host: "127.0.0.1", port: stray.port })).toBe(true);

    // F1: the exact original start IS passed, so the before/after brackets are both live.
    const closed = await source.closeDiscoveredListener!(stray.pid, stray.port, stray.startBefore);
    expect(closed.listenerClosed).toBe(true);

    // Absence proven by the OWNED pid no longer listening, and the port having no owner at all.
    expect(await listeningPortsForPid(stray.pid)).not.toContain(stray.port);
    expect(await listenerOwners(stray.port)).toEqual([]);
    expect(await source.endpointInUse({ host: "127.0.0.1", port: stray.port })).toBe(false);

    // The close must NOT have killed or replaced the process: same pid AND same start identity.
    expect(await isAlive(stray.pid)).toBe(true);
    expect(await processStartIdentity(stray.pid)).toBe(stray.startBefore);
  }, 40_000);

  it("MISMATCHED start: refused, and the real listener survives with the pid and start untouched", async () => {
    const stray = await spawnStrayInspector();
    const source = makeNativeLegacyPiTransportSource(modules);
    expect(await listenerOwners(stray.port)).toEqual([stray.pid]);

    // F1: a start that is not the real one must fail the bracket BEFORE any evaluate.
    const wrongStart = mismatchedStart(stray.startBefore);
    expect(wrongStart).not.toBe(stray.startBefore);

    const closed = await source.closeDiscoveredListener!(stray.pid, stray.port, wrongStart);
    expect(closed.listenerClosed).toBe(false);

    // The refusal must leave the listener OPEN and the process intact: nothing was closed.
    expect(await listeningPortsForPid(stray.pid)).toContain(stray.port);
    expect(await listenerOwners(stray.port)).toEqual([stray.pid]);
    expect(await source.endpointInUse({ host: "127.0.0.1", port: stray.port })).toBe(true);
    expect(await isAlive(stray.pid)).toBe(true);
    expect(await processStartIdentity(stray.pid)).toBe(stray.startBefore);
  }, 40_000);

  it("OMITTED start: refused at runtime rather than skipping the identity bracket", async () => {
    const stray = await spawnStrayInspector();
    const source = makeNativeLegacyPiTransportSource(modules);
    expect(await listenerOwners(stray.port)).toEqual([stray.pid]);

    // F1: an absent expectedStartedAt must be a REFUSAL, never a silent skip of the bracket.
    // Called through a deliberately loose reference: once source29 makes the parameter REQUIRED,
    // a direct two-argument call would be a COMPILE error, and this case must keep exercising the
    // RUNTIME refusal that a JS or any-caller path would actually hit.
    const closeLooselyTyped = source.closeDiscoveredListener as unknown as
      ((pid: number, port: number, expectedStartedAt?: string) => Promise<{ listenerClosed: boolean }>) | undefined;
    const closed = await closeLooselyTyped!(stray.pid, stray.port);
    expect(closed.listenerClosed).toBe(false);

    // Same honest-negative guarantees: the listener survives and the process is unharmed.
    expect(await listeningPortsForPid(stray.pid)).toContain(stray.port);
    expect(await listenerOwners(stray.port)).toEqual([stray.pid]);
    expect(await isAlive(stray.pid)).toBe(true);
    expect(await processStartIdentity(stray.pid)).toBe(stray.startBefore);

    // An EMPTY start is the other bypass shape and must refuse identically.
    const emptyClosed = await closeLooselyTyped!(stray.pid, stray.port, "");
    expect(emptyClosed.listenerClosed).toBe(false);
    expect(await listenerOwners(stray.port)).toEqual([stray.pid]);
    expect(await isAlive(stray.pid)).toBe(true);
  }, 40_000);

  it("WRONG PORT: a port this pid does not own is refused, leaving the real listener open", async () => {
    // F5 HONEST LABEL: this case does NOT bind publicly. It proves the wrong-port refusal.
    // The production `public.length > 0` branch is NOT exercised here and is not claimed to be.
    const stray = await spawnStrayInspector();
    const source = makeNativeLegacyPiTransportSource(modules);

    const wrongPort = await findUnownedLoopbackPort(stray.port);
    expect(wrongPort).not.toBe(stray.port);
    expect(await listenerOwners(wrongPort)).toEqual([]);

    const closed = await source.closeDiscoveredListener!(stray.pid, wrongPort, stray.startBefore);
    expect(closed.listenerClosed).toBe(false);

    // The real listener is untouched and the process survives.
    expect(await listeningPortsForPid(stray.pid)).toContain(stray.port);
    expect(await listenerOwners(stray.port)).toEqual([stray.pid]);
    expect(await isAlive(stray.pid)).toBe(true);
    expect(await processStartIdentity(stray.pid)).toBe(stray.startBefore);
  }, 40_000);
});

/** Deterministically different but same-shaped start identity, so the bracket must reject it. */
function mismatchedStart(real: string): string {
  // Flip the year: keeps the exact "Mon DD HH:MM:SS YYYY" shape the reader produces.
  const yearMatch = /(\d{4})$/.exec(real);
  if (!yearMatch) return `${real}-not-the-same-start`;
  const flipped = String(Number(yearMatch[1]) + 1).padStart(4, "0");
  return real.slice(0, real.length - 4) + flipped;
}

/** Find a loopback TCP port with no LISTEN owner, near the stray's port for speed. */
async function findUnownedLoopbackPort(nearPort: number): Promise<number> {
  for (let candidate = nearPort + 1; candidate < nearPort + 400; candidate += 1) {
    if (candidate > 65535) break;
    if ((await listenerOwners(candidate)).length === 0) return candidate;
  }
  throw new Error("no unowned loopback port found");
}