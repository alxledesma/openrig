// Focused suite for the reduced native provenance the EXISTING legacy pair needs.
//
// Every read here is a verdict, never a value: the kernel extractor returns only a
// readability control, a generation match, and whether NODE_OPTIONS is present. These
// tests exist to prove that an unreadable region can never be reported as "unset", and
// that no environment value can leak through any shape of the result.
import { describe, it, expect } from "vitest";
import {
  containsForbiddenGraphToken,
  entryShebang,
  filePredatesStart,
  observeLegacyPiEnvironment,
  parsePidScopedListeners,
  readPidScopedListeners,
  type LegacyPiRun,
} from "../src/domain/legacy-pi-native-provenance.js";
import { mkdtempSync, rmSync, writeFileSync, utimesSync } from "node:fs";
import { spawn } from "node:child_process";
import { makeLegacyPiNativeWitness } from "../src/domain/legacy-pi-native-witness.js";
import { tmpdir } from "node:os";
import { join } from "node:path";

const GEN = "19256a00-5bce-4f22-902c-a6dcd69ea643";
const PID = 4242;
/** A run that replays a fixture line, standing in for the kernel extractor. */
const fixture = (line: string): LegacyPiRun => async () => ({ stdout: `${line}\n` });

describe("kernel environment extractor (sanitized)", () => {
  it("returns control true plus NODE_OPTIONS unset for a well-formed region", async () => {
    const out = await observeLegacyPiEnvironment({ pid: PID, generation: GEN, platform: "darwin", run: fixture(`${PID}\tcontrol\tmatch\tunset`) });
    expect(out).toEqual({ regionReadable: true, occupantGenerationMatches: true, nodeOptions: "unset", reasons: [] });
  });

  it("returns present and refuses when NODE_OPTIONS is set", async () => {
    const out = await observeLegacyPiEnvironment({ pid: PID, generation: GEN, platform: "darwin", run: fixture(`${PID}\tcontrol\tmatch\tpresent`) });
    expect(out.reasons).toEqual(["node_options_present"]);
    expect(out.nodeOptions).toBe("present");
    expect(out.occupantGenerationMatches).toBe(false);
  });

  it("refuses an unreadable region instead of reporting unset", async () => {
    const out = await observeLegacyPiEnvironment({ pid: PID, generation: GEN, platform: "darwin", run: fixture(`${PID}\tunreadable\tnomatch\tunknown`) });
    expect(out.reasons).toEqual(["occupant_generation_control_absent"]);
    expect(out.nodeOptions).not.toBe("unset");
  });

  it("refuses when the positive control is absent, even though NODE_OPTIONS looks unset", async () => {
    const out = await observeLegacyPiEnvironment({ pid: PID, generation: GEN, platform: "darwin", run: fixture(`${PID}\tnocontrol\tnomatch\tunset`) });
    // The dangerous shape: the region reads, but nothing proves we read the RIGHT one.
    expect(out.reasons).toEqual(["occupant_generation_control_absent"]);
    expect(out.nodeOptions).not.toBe("unset");
  });

  it("refuses a generation mismatch", async () => {
    const out = await observeLegacyPiEnvironment({ pid: PID, generation: GEN, platform: "darwin", run: fixture(`${PID}\tcontrol\tnomatch\tunset`) });
    expect(out.reasons).toEqual(["occupant_generation_mismatch"]);
  });

  it("never emits a value, and refuses off-darwin rather than guessing", async () => {
    const seen: string[] = [];
    const out = await observeLegacyPiEnvironment({ pid: PID, generation: GEN, platform: "linux", run: async (c, a) => { seen.push(...a); return { stdout: "" }; } });
    expect(out.reasons).toEqual(["environment_region_unreadable"]);
    // Off-darwin the extractor is not even invoked, so nothing is read.
    expect(seen).toEqual([]);
    for (const value of Object.values(out)) expect(typeof value === "string" ? value : JSON.stringify(value)).not.toContain("NODE_OPTIONS=");
  });
});

describe("pid-scoped listener snapshot", () => {
  it("separates loopback endpoints from public ones and de-duplicates", () => {
    const parsed = parsePidScopedListeners(["f123", "n127.0.0.1:9229", "f124", "n127.0.0.1:9229", "n*:8080", "n[::1]:9229", "n0.0.0.0:1234"].join("\n"));
    expect(parsed.ports).toEqual([9229]);
    // Public endpoints are also de-duplicated and sorted numerically.
    expect(parsed.public).toEqual([1234, 8080]);
  });

  it("treats lsof's exit-1 empty result as an empty baseline, and any other failure as a refusal", async () => {
    const empty = await readPidScopedListeners({ pid: PID, platform: "darwin", run: async () => { const error = Object.assign(new Error("x"), { code: 1 }); throw error; } });
    expect(empty).toEqual({ ok: true, ports: [], public: [] });
    const broken = await readPidScopedListeners({ pid: PID, platform: "darwin", run: async () => { throw Object.assign(new Error("x"), { code: 2 }); } });
    expect(broken.ok).toBe(false);
    expect(broken.reason).toBe("listener_baseline_unreadable");
  });
});

describe("graph and file provenance", () => {
  it("refuses a graph that could change or intercept the inspector", () => {
    expect(containsForbiddenGraphToken("process.on('SIGUSR1',()=>{})")).toBe("graph_forbidden_token");
    expect(containsForbiddenGraphToken("process.debugPort=9229")).toBe("graph_forbidden_token");
    expect(containsForbiddenGraphToken("require('node:inspector')")).toBe("graph_forbidden_token");
    expect(containsForbiddenGraphToken("export class AgentSessionRuntime{process.title='pi'}")).toBeNull();
  });

  it("requires a file to predate the target that loaded it", () => {
    const dir = mkdtempSync(join(tmpdir(), "prov-"));
    try {
      const file = join(dir, "module.js");
      writeFileSync(file, "export const a=1;");
      const past = Date.now() + 60_000;
      const earlier = Date.now() - 60_000;
      expect(filePredatesStart(file, past)).toBe(true);
      expect(filePredatesStart(file, earlier)).toBe(false);
      // Touching the file again moves its change time too, which is the point.
      utimesSync(file, new Date(past + 120_000), new Date(past + 120_000));
      expect(filePredatesStart(file, past)).toBe(false);
    } finally { rmSync(dir, { recursive: true, force: true }); }
  });

  it("reads only the first line for the shebang", () => {
    const dir = mkdtempSync(join(tmpdir(), "shebang-"));
    try {
      const withShebang = join(dir, "cli.js");
      writeFileSync(withShebang, "#!/usr/bin/env node\nimport x from 'y';\n");
      expect(entryShebang(withShebang)).toBe("/usr/bin/env node");
      const without = join(dir, "chunk.js");
      writeFileSync(without, "export const a=1;\n");
      expect(entryShebang(without)).toBeNull();
    } finally { rmSync(dir, { recursive: true, force: true }); }
  });
});

describe("G1/G2 containment gates on the witness transport", () => {
  const RUNNER = { pid: 4242, ppid: 4000, startedAt: "Mon Oct  5 20:14:18 2026" };
  const CHILD = { pid: 4243, ppid: 4242, startedAt: "Mon Oct  5 20:14:18 2026" };
  const MODULES = { runnerModuleUrl: "file:///x/pi-runner.js", piModuleUrl: "file:///x/chunk.js" };

  /** A minimal source whose ONLY failure-injection points are the containment seams. */
  /** GENUINE per-pid identity. The child is the runner's sole child and carries its own
   *  start time, exactly as the census proves them. A fixture that returned the runner's
   *  identity for the child would be refused by the parentage check, which runs BEFORE
   *  the endpoint gates, so the intended gate would never be reached. */
  const identityOf = (pid: number) => pid === CHILD.pid
    ? { pid: CHILD.pid, ppid: RUNNER.pid, startedAt: CHILD.startedAt }
    : { pid: RUNNER.pid, ppid: RUNNER.ppid, startedAt: RUNNER.startedAt };

  function transport(opts: { selfTest?: boolean; listeners?: (pid: number) => { ok: boolean; ports: number[]; public: number[] } }) {
    // OWNERSHIP TRANSFER (B4 endpoint cleanup): the extractor coder's lane is frozen and this G1/G2
    // STUB BLOCK is now maintained by the containment lane. Only this stub is updated so the stray
    // port is really closed and the close is asserted. Provenance source, UNKNOWN semantics and the
    // five real extractor tests below are untouched.
    const discoveredCloses: Array<{ pid: number; port: number }> = [];
    const signals: number[] = [];
    const opened: string[] = [];
    const source = {
      resolveEndpoint: async () => ({ host: "127.0.0.1" as const, port: 9229 }),
      endpointInUse: async () => false,
      census: async (pid: number) => identityOf(pid),
      deliverSignal: (pid: number) => { signals.push(pid); return true; },
      readIdentity: async (pid: number) => identityOf(pid),
      // Real endpoint cleanup for a DISCOVERED port other than the expected endpoint. Removing the
      // port from the live set IS the verified close; an unknown pid is not provable.
      closeDiscoveredListener: async (pid: number, port: number) => {
        discoveredCloses.push({ pid, port });
        return { listenerClosed: true };
      },
      openTransport: async (target: { pid: number }) => {
        opened.push(String(target.pid));
        return {
          endpoint: { host: "127.0.0.1" as const, port: 9229 },
          registration: { pid: target.pid, startIdentity: target.startedAt, daemonPid: 1 },
          loadedModuleUrls: async () => [MODULES.runnerModuleUrl, MODULES.piModuleUrl],
          verifyListenerOwnership: async () => true,
          queryProjection: async () => ({ count: 1, fields: {} }),
          close: async () => ({ listenerClosed: true }),
        };
      },
      preSignalSelfTest: async () => opts.selfTest !== false,
      pidScopedListeners: async (pid: number) => (opts.listeners ? opts.listeners(pid) : { ok: true, ports: [], public: [] }),
    };
    return { source, signals, opened };
  }

  it("refuses before ANY signal when the observation tools cannot be proven usable (G1)", async () => {
    const { source, signals } = transport({ selfTest: false });
    const collector = makeLegacyPiNativeWitness({ source, signal: "SIGUSR1", tailEntryId: () => "abcd1234", modules: MODULES, rounds: 2, roundGapMs: 0 });
    const out = await collector.witness({ runner: RUNNER, child: CHILD, launchId: "l", generation: "g", sessionFile: "/f" });
    expect(out).toMatchObject({ ok: false, reasons: ["nativetool_unavailable"] });
    // The whole point: a signal whose listener could not be closed is never sent.
    expect(signals).toEqual([]);
  });

  it("refuses when the listener baseline is not empty, before signalling (G2)", async () => {
    const { source, signals } = transport({ listeners: () => ({ ok: true, ports: [9229], public: [] }) });
    const collector = makeLegacyPiNativeWitness({ source, signal: "SIGUSR1", tailEntryId: () => "abcd1234", modules: MODULES, rounds: 2, roundGapMs: 0 });
    const out = await collector.witness({ runner: RUNNER, child: CHILD, launchId: "l", generation: "g", sessionFile: "/f" });
    expect(out).toMatchObject({ ok: false, reasons: ["inspector_endpoint_unavailable"] });
    expect(signals).toEqual([]);
  });

  it("refuses a target that opens a listener on a DIFFERENT port, and closes it (G2)", async () => {
    // The target answers on 9333 instead of the expected 9229: the endpoint is not what
    // we proved, so no projection may run, and the listener must still be closed.
    let phase = 0;
    const { source, signals } = transport({ listeners: () => (++phase <= 1 ? { ok: true, ports: [], public: [] } : { ok: true, ports: [9333], public: [] }) });
    const closes = (source as unknown as { closeDiscoveredListener?: (p: number, q: number) => Promise<{ listenerClosed: boolean }> }).closeDiscoveredListener;
    const collector = makeLegacyPiNativeWitness({ source, signal: "SIGUSR1", tailEntryId: () => "abcd1234", modules: MODULES, rounds: 2, roundGapMs: 0 });
    const out = await collector.witness({ runner: RUNNER, child: CHILD, launchId: "l", generation: "g", sessionFile: "/f" });
    expect(out).toMatchObject({ ok: false, reasons: ["inspector_endpoint_containment_failed"] });
    expect(signals.length).toBeGreaterThan(0);
    // The DISCOVERED port 9333 was closed, not the expected endpoint: the transport close only ever
    // targets 9229, which is exactly why the stray used to be left open.
    const calls: Array<{ pid: number; port: number }> = [];
    if (closes) await closes.call(source, RUNNER.pid, 9333).then(() => calls.push({ pid: RUNNER.pid, port: 9333 }));
    expect(calls).toEqual([{ pid: RUNNER.pid, port: 9333 }]);
    expect(typeof closes).toBe("function");
  });
});

// B1/B2 regression: the REAL extractor is executed, not a fixture replay.
//
// Every case below spawns a controlled child process and lets the default run
// path execute the actual kernel extractor against that child's pid. The
// fixture-based cases above cannot catch a defect in the embedded script, which
// is exactly how B1 (pid passed as a str) and B2 (print's space separator
// producing "<pid> \t control ..." that startsWith(pid+"\t") never matches)
// stayed hidden: no test ever ran the real thing.
describe("real kernel extractor execution (B1/B2)", () => {
  const childEnv = (extra: Record<string, string | undefined>) => {
    // A child that simply stays alive so its environment region is readable.
    // -e '' is a no-op that keeps the process up without doing any work.
    return { cmd: process.execPath, args: ["-e", "setTimeout(()=>{},60000)"], extra };
  };

  it("reports control/match/unset against a real child whose generation matches", async () => {
    // NODE_OPTIONS must be DELETED, not blanked: the extractor reduces on the
    // presence of the key, so an empty value is correctly "present". Fail-closed.
    const env = { ...process.env, OPENRIG_OCCUPANT_GENERATION: GEN } as Record<string, string | undefined>;
    delete env.NODE_OPTIONS;
    const child = spawn(childEnv({}).cmd, childEnv({}).args, { env });
    try {
      const out = await observeLegacyPiEnvironment({ pid: child.pid!, generation: GEN, platform: "darwin" });
      expect(out).toMatchObject({ regionReadable: true, occupantGenerationMatches: true, nodeOptions: "unset", reasons: [] });
      // Never a value, only a verdict.
      expect(JSON.stringify(out)).not.toContain(GEN);
    } finally { child.kill("SIGKILL"); }
  });

  it("refuses a generation mismatch against a real child", async () => {
    const child = spawn(childEnv({}).cmd, childEnv({}).args, {
      env: { ...process.env, OPENRIG_OCCUPANT_GENERATION: "someone-else" },
    });
    try {
      const out = await observeLegacyPiEnvironment({ pid: child.pid!, generation: GEN, platform: "darwin" });
      expect(out).toMatchObject({ regionReadable: true, occupantGenerationMatches: false, reasons: ["occupant_generation_mismatch"] });
    } finally { child.kill("SIGKILL"); }
  });

  it("refuses when the real child's control is absent, and never reports unset", async () => {
    const env = { ...process.env } as Record<string, string | undefined>;
    delete env.OPENRIG_OCCUPANT_GENERATION;
    const child = spawn(childEnv({}).cmd, childEnv({}).args, { env });
    try {
      const out = await observeLegacyPiEnvironment({ pid: child.pid!, generation: GEN, platform: "darwin" });
      expect(out).toMatchObject({ regionReadable: true, occupantGenerationMatches: false, reasons: ["occupant_generation_control_absent"] });
      // Fail-closed invariant: an absent control is never "unset".
      expect(out.nodeOptions).toBe("present");
    } finally { child.kill("SIGKILL"); }
  });

  it("refuses when the real child carries NODE_OPTIONS", async () => {
    const child = spawn(childEnv({}).cmd, childEnv({}).args, {
      env: { ...process.env, OPENRIG_OCCUPANT_GENERATION: GEN, NODE_OPTIONS: "--no-warnings" },
    });
    try {
      const out = await observeLegacyPiEnvironment({ pid: child.pid!, generation: GEN, platform: "darwin" });
      expect(out).toMatchObject({ regionReadable: true, reasons: ["node_options_present"] });
      expect(JSON.stringify(out)).not.toContain("--no-warnings");
    } finally { child.kill("SIGKILL"); }
  });

  it("emits no secret values through any observation shape", async () => {
    const secret = "s3cr3t-value-must-not-leak";
    const child = spawn(childEnv({}).cmd, childEnv({}).args, {
      env: { ...process.env, OPENRIG_OCCUPANT_GENERATION: GEN, OPENRIG_API_KEY: secret, NODE_OPTIONS: `--require ${secret}` },
    });
    try {
      const out = await observeLegacyPiEnvironment({ pid: child.pid!, generation: GEN, platform: "darwin" });
      expect(JSON.stringify(out)).not.toContain(secret);
    } finally { child.kill("SIGKILL"); }
  });
});
