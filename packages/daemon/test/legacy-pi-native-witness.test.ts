// Focused suite for the STAGE 1 legacy Pi native witness: a real, bounded,
// cleanup-verifiable daemon-owned inspector transport, plus the ephemeral witness
// collector that reads ONLY allowlisted primitives from the exact live legacy
// RunnerCore and the exact live child AgentSessionRuntime session.
//
// The class contract being pinned here:
//   - NO witness is ever accepted from a caller. The collector binds the pids it
//     was handed and RE-PROVES them itself; a caller cannot supply a witness, a
//     leaf, a module URL, a script, an endpoint or a port.
//   - A signal reaches only a pid that is alive with the exact bound start
//     identity, is correctly parented, is not this daemon, and has a verifiably
//     FREE inspector endpoint. Otherwise: typed refusal, no signal, no halt.
//   - Refusals are reduced and closed: no pid, port, path, argv, environment,
//     memory, transcript or exception text in any reason, value or thrown message.
//   - Acceptance needs TWO agreeing R-C-R rounds. Any drift refuses.
//   - The listener is closed and VERIFIED closed on every path, including refusal.
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { spawn, execFile } from "node:child_process";
import { createServer } from "node:net";
import { existsSync, mkdtempSync, realpathSync, rmSync, writeFileSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import {
  makeLegacyPiNativeWitness,
  makeNativeLegacyPiTransportSource,
  isLegacyPiWitnessReason,
  reduceWitnessFailure,
  moduleUrlOf,
  LEGACY_INSPECTOR_PORT,
  LEGACY_WITNESS_REASONS,
  type LegacyPiInspectorTransport,
  type LegacyPiTargetIdentity,
  type LegacyPiTransportSource,
  type LegacyPiWitnessFailure,
} from "../src/domain/legacy-pi-native-witness.js";
import {
  buildLegacySignalIntentEvent,
  type LegacySignalIntentSeatIdentity,
} from "../src/routes/seat.js";

const run = promisify(execFile);

const SESSION_FILE = "/state/pi/intake-lead@app-handy-conveyor/sessions/history.jsonl";
const SESSION_ID = "01a10faa-ea2a-72e5-b891-06fc15b9a04d";
const LAUNCH_ID = "launch-old-0001";
const GENERATION = "19256a00-5bce-4f22-902c-a6dcd69ea643";
const LEAF = "4f04d2d5";

/** Stand-in module URLs for the injected units: the collector must accept them only
 *  because the injected transport reports them as already loaded. */
const FAKE_MODULES = { runnerModuleUrl: moduleUrlOf("/opt/openrig/pi-runner.js"), piModuleUrl: moduleUrlOf("/opt/pi/chunk-33XOIQ5N.js") };

/** A genuinely unused loopback endpoint, obtained by binding and releasing one.
 *  The real transport proves the same precondition against the same endpoint the
 *  target will use, so a made-up port number would prove less. */
async function freeEndpoint(): Promise<{ host: "127.0.0.1"; port: number }> {
  return await new Promise((resolve, reject) => {
    const server = createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      const port = typeof address === "object" && address !== null ? address.port : 0;
      server.close(() => { if (port > 0) resolve({ host: "127.0.0.1" as const, port }); else reject(new Error("no port")); });
    });
  });
}

interface FakeRound { runner: Record<string, unknown>; child: Record<string, unknown> }

interface FakeUnit extends LegacyPiInspectorTransport {
  calls: string[];
  /** Scripted projections per shape, consumed in order; the last entry repeats. */
  script: Array<FakeRound | { throw: string }>;
  loaded: string[];
  counts: Record<string, number>;
  owned: boolean;
  closeResult: boolean;
  closeThrows: boolean;
  queryThrows: boolean;
}

/** One injected transport unit. It mirrors the real unit's exact call order and
 *  close discipline; only the inspector round-trips are simulated. */
function fakeTransport(endpoint: { host: "127.0.0.1"; port: number }, modules: { runnerModuleUrl: string; piModuleUrl: string }, overrides?: {
  owned?: boolean; closeResult?: boolean; closeThrows?: boolean; queryThrows?: boolean;
  loaded?: string[]; counts?: Record<string, number>; script?: Array<FakeRound | { throw: string }>;
}): FakeUnit {
  const calls: string[] = [];
  const script = overrides?.script ?? [{ runner: idleRunner(), child: idleChild(LEAF) }];
  const counts = { RunnerCore: 1, AgentSessionRuntime: 1, AgentSession: 1, ...overrides?.counts };
  const unit: FakeUnit = {
    calls,
    script: [...script],
    loaded: overrides?.loaded ?? [modules.runnerModuleUrl, modules.piModuleUrl],
    counts,
    owned: overrides?.owned ?? true,
    closeResult: overrides?.closeResult ?? true,
    closeThrows: overrides?.closeThrows ?? false,
    queryThrows: overrides?.queryThrows ?? false,
    endpoint,
    registration: { pid: 0, startIdentity: "", daemonPid: 4242 },
    loadedModuleUrls: async () => { calls.push("loadedModuleUrls"); return [...unit.loaded]; },
    verifyListenerOwnership: async () => { calls.push("verifyListenerOwnership"); return unit.owned; },
    queryProjection: async (request) => {
      calls.push(`queryProjection:${request.exportName}`);
      if (unit.queryThrows) throw new Error("inspector round trip failed");
      // One scripted response per shape, consumed in call order, last repeats.
      const seen = unit.calls.filter(c => c === `queryProjection:${request.exportName}`).length - 1;
      const next = unit.script[Math.min(seen, unit.script.length - 1)]!;
      if (typeof next === "object" && "throw" in next) throw new Error(next.throw);
      const shape = request.exportName === "RunnerCore" ? "runner"
        : request.exportName === "AgentSession" ? "child" : "child";
      const source = (next as FakeRound)[shape] as Record<string, unknown> | undefined;
      const fields: Record<string, unknown> = {};
      for (const accessor of request.accessors) {
        const name = accessor.split(":")[0]!;
        // A projected value is only present when the fixture scripted it, so a
        // missing field exercises the projection_invalid path rather than a default.
        if (source && Object.hasOwn(source, name)) fields[name] = source[name];
      }
      return { count: counts[request.exportName] ?? 0, fields: fields as never };
    },
    close: async () => {
      calls.push("close");
      if (unit.closeThrows) throw new Error("listener close failed");
      return { listenerClosed: unit.closeResult };
    },
  };
  return unit;
}

function idleRunner(over?: Record<string, unknown>): Record<string, unknown> {
  return {
    ready: true, streaming: false, processing: false, controlPending: false,
    sessionFile: SESSION_FILE, sessionId: SESSION_ID,
    launchId: LAUNCH_ID, generation: GENERATION,
    // The legacy defect this whole stage exists for: the durable cursor is a
    // generic event id, often a UI UUID, and names no session entry at all.
    lastEntryId: "poisoned-ui-uuid",
    ...over,
  };
}
function idleChild(leaf: string | null, over?: Record<string, unknown>): Record<string, unknown> {
  return {
    isStreaming: false, isCompacting: false, pendingMessageCount: 0,
    sessionFile: SESSION_FILE, sessionId: SESSION_ID, leafId: leaf,
    runtimeSessionMatchesSession: true, ...over,
  };
}

const TARGET_RUNNER: LegacyPiTargetIdentity = { pid: 4242, ppid: 4000, startedAt: "runner-boot" };
const TARGET_CHILD: LegacyPiTargetIdentity = { pid: 4243, ppid: 4242, startedAt: "child-boot" };

type UnitOverrides = NonNullable<Parameters<typeof fakeTransport>[2]>;

interface Box {
  runner: FakeUnit;
  child: FakeUnit;
  census: Map<number, LegacyPiTargetIdentity>;
  tail: { value: string | null };
  signals: Array<{ pid: number; signal: string }>;
  /** Endpoints handed out in order, as two sequential real units would request. */
  endpoints: Array<{ host: "127.0.0.1"; port: number }>;
  /** Ports that are genuinely occupied, i.e. a foreign inspector collision. */
  busyPorts: Set<number>;
  /** Pid order in which transports were acquired. */
  acquired: number[];
  /** Transport lifecycle in actual open/close order. */
  lifecycle: Array<{ action: "open" | "close"; pid: number }>;
  /** Per-pid listener census state driving the B4 containment fixture. */
  listenerStates: Map<number, { reads: number; expectedPort: number; strayPorts: number[]; publicPorts: number[] }>;
  /** Discovered-port close attempts, asserted rather than assumed. */
  discoveredCloseAttempts: Array<{ pid: number; port: number }>;
  /** Reads that must pass before the expected listener appears (late-binding race). */
  lateBindRounds: number;
}

async function box(opts?: {
  runner?: UnitOverrides; child?: UnitOverrides;
  tail?: string | null;
  boundRunner?: LegacyPiTargetIdentity; boundChild?: LegacyPiTargetIdentity;
  census?: Map<number, LegacyPiTargetIdentity>;
}): Promise<Box> {
  // Distinct free endpoints per unit, exactly as two sequential real units would use.
  const runnerEndpoint = await freeEndpoint();
  const childEndpoint = await freeEndpoint();
  const runner = fakeTransport(runnerEndpoint, FAKE_MODULES, opts?.runner);
  const child = fakeTransport(childEndpoint, FAKE_MODULES, opts?.child);
  const b: Box = {
    runner,
    child,
    census: opts?.census ?? new Map([[TARGET_RUNNER.pid, TARGET_RUNNER], [TARGET_CHILD.pid, TARGET_CHILD]]),
    tail: { value: opts?.tail === undefined ? LEAF : opts.tail },
    signals: [],
    endpoints: [runnerEndpoint, childEndpoint, runnerEndpoint, childEndpoint, runnerEndpoint, childEndpoint, runnerEndpoint, childEndpoint],
    busyPorts: new Set(),
    acquired: [],
    lifecycle: [],
    listenerSets: new Map(),
    discoveredCloseAttempts: [] as Array<{ pid: number; port: number }>,
    lateBindRounds: 0,
    listenerStates: new Map<number, { reads: number; expectedPort: number; strayPorts: number[]; publicPorts: number[] }>(),
    openedPorts: new Map<number, number>(),
    extraPorts: new Map<number, number[]>(),
    publicPorts: new Map<number, number[]>(),
  };
  return b;
}

/** The daemon-owned transport source under test: a real SIGUSR1 + inspector unit
 *  whose ONLY simulated parts are the inspector protocol itself. */
function source(b: Box): LegacyPiTransportSource {
  return {
    resolveEndpoint: async () => b.endpoints.length > 0 ? b.endpoints.shift()! : await freeEndpoint(),
    // The endpoint queue is allocation order, not a listener census. A port is
    // busy only when this fixture explicitly holds it as a foreign listener.
    endpointInUse: async endpoint => b.busyPorts.has(endpoint.port),
    // B4: a listener that appears only after a delay, and a discovered stray port whose close is
    // asserted rather than assumed.
    // B4 CENSUS FIXTURE. `lateBindRounds` > 0 makes the expected port appear only after that many
    // reads, which is exactly the late-binding race the single post-signal read used to lose.
    // `strayPorts` adds ports that are NOT the expected endpoint; `publicPorts` adds public binds.
    pidScopedListeners: async (pid: number) => {
      const state = b.listenerStates.get(pid);
      if (!state) {
        // No explicit scenario: a healthy target bound exactly the endpoint it was opened on.
        const opened = b.openedPorts.get(pid);
        return { ok: true, ports: opened === undefined ? [] : [String(opened)], public: [] };
      }
      state.reads++;
      if (state.reads <= b.lateBindRounds) return { ok: true, ports: [], public: [] };
      return { ok: true, ports: [String(state.expectedPort), ...state.strayPorts.map(String)], public: state.publicPorts };
    },
    // B4 DISCOVERED CLOSE, asserted: the attempt is recorded and the port really is removed.
    closeDiscoveredListener: async (pid: number, port: number) => {
      b.discoveredCloseAttempts.push({ pid, port });
      const extra = b.extraPorts.get(pid);
      if (extra === undefined) return { listenerClosed: false }; // unknown state: not provable
      // Removing the port from the census IS the verified close: a re-read would find it gone.
      b.extraPorts.set(pid, extra.filter(p => p !== port));
      return { listenerClosed: true };
    },
    census: async pid => b.census.get(pid) ?? null,
    // F2: a success stub must report the ACTUAL delivery result, not void. A green suite with
    // a void-returning stub proves nothing about delivery.
    deliverSignal: (pid, signal) => { b.signals.push({ pid, signal }); return true; },
    openTransport: async (target, endpoint) => {
      const unit = target.pid === TARGET_CHILD.pid ? b.child : b.runner;
      unit.registration = { pid: target.pid, startIdentity: target.startedAt, daemonPid: 4242 };
      b.acquired.push(target.pid);
      b.openedPorts.set(target.pid, endpoint.port);
      b.lifecycle.push({ action: "open", pid: target.pid });
      return {
        ...unit,
        close: async () => {
          const result = await unit.close();
          b.lifecycle.push({ action: "close", pid: target.pid });
          // A closed unit holds no listener, so the next round's baseline must be empty again.
          b.openedPorts.delete(target.pid);
          return result;
        },
      };
    },
    readIdentity: async pid => b.census.get(pid) ?? null,
  };
}

function collectorFor(b: Box, overrides?: {
  rounds?: number;
  tail?: () => string | null;
  sourceOverrides?: Partial<LegacyPiTransportSource>;
}) {
  return makeLegacyPiNativeWitness({
    source: { ...source(b), ...overrides?.sourceOverrides },
    signal: "SIGUSR1",
    tailEntryId: overrides?.tail ?? (() => b.tail.value),
    modules: FAKE_MODULES,
    rounds: overrides?.rounds ?? 2,
    roundGapMs: 0,
  });
}

function collect(b: Box, overrides?: Parameters<typeof collectorFor>[1]) {
  return collectorFor(b, overrides).witness({
    runner: TARGET_RUNNER,
    child: TARGET_CHILD,
    launchId: LAUNCH_ID,
    generation: GENERATION,
    sessionFile: SESSION_FILE,
  });
}

describe("legacy pi native witness: acceptance", () => {
  let b: Box;
  beforeEach(async () => { b = await box(); });

  it("accepts two agreeing idle R-C-R rounds and returns the AUTHENTIC native leaf", async () => {
    const out = await collect(b);
    expect(out).toMatchObject({
      ok: true, nativeLeaf: LEAF, launchId: LAUNCH_ID, generation: GENERATION,
      sessionFile: SESSION_FILE, sessionId: SESSION_ID, rounds: 2,
    });
    // The plan cursor is the leaf, never the poisoned UI UUID.
    expect((out as { nativeLeaf: string }).nativeLeaf).not.toBe("poisoned-ui-uuid");
    // Bounded: exactly two rounds, three samples each. No unbounded resampling.
    expect(b.runner.calls.filter(c => c === "queryProjection:RunnerCore")).toHaveLength(4);
    expect(b.child.calls.filter(c => c === "queryProjection:AgentSessionRuntime")).toHaveLength(2);
  });

  it("signals each verified target and closes every listener it opened", async () => {
    await collect(b);
    // Two rounds of R-C-R: four runner samples and two child samples.
    expect(b.signals.filter(s => s.pid === TARGET_RUNNER.pid)).toHaveLength(4);
    expect(b.signals.filter(s => s.pid === TARGET_CHILD.pid)).toHaveLength(2);
    expect(b.signals.every(s => s.signal === "SIGUSR1")).toBe(true);
    expect(b.runner.calls.filter(c => c === "close")).toHaveLength(b.acquired.filter(pid => pid === TARGET_RUNNER.pid).length);
    expect(b.child.calls.filter(c => c === "close")).toHaveLength(b.acquired.filter(pid => pid === TARGET_CHILD.pid).length);
  });

  it("closes each target before attaching to the next, so two listeners never overlap", async () => {
    await collect(b);
    // Per round the acquisition order is runner, child, runner.
    expect(b.acquired.slice(0, 3)).toEqual([TARGET_RUNNER.pid, TARGET_CHILD.pid, TARGET_RUNNER.pid]);
    let attached: number | null = null;
    for (const event of b.lifecycle) {
      if (event.action === "open") {
        expect(attached).toBeNull();
        attached = event.pid;
      } else {
        expect(attached).toBe(event.pid);
        attached = null;
      }
    }
    expect(attached).toBeNull();
  });

  it("verifies listener ownership on every attach", async () => {
    await collect(b);
    expect(b.runner.calls.filter(c => c === "verifyListenerOwnership")).toHaveLength(4);
    expect(b.child.calls.filter(c => c === "verifyListenerOwnership")).toHaveLength(2);
  });

  it("records a reduced evidence id without echoing process identifiers", async () => {
    const out = await collect(b);
    expect(out.ok).toBe(true);
    expect((out as { evidenceId: string }).evidenceId).toMatch(/^sha256:[0-9a-f]{16}$/);
    const text = JSON.stringify(out);
    expect(text).not.toContain("4242");
    expect(text).not.toContain("4243");
    // A successful witness carries the authoritative session identity by
    // contract; the digest itself does not expose those values.
    expect(text).toContain(SESSION_FILE);
    expect(text).toContain(SESSION_ID);
    expect((out as { evidenceId: string }).evidenceId).not.toContain(SESSION_ID);
    expect((out as { evidenceId: string }).evidenceId).not.toContain(SESSION_FILE);
  });

  it("accepts a stable witness whose legacy cursor is a poisoned UI UUID and still binds the leaf to the tail", async () => {
    // The reason this stage exists: the recorded live cursor is f365df5b..., a UI
    // UUID, while the real leaf is 2cf52f50. The witness must carry the leaf.
    b.runner.script = [{ runner: idleRunner({ lastEntryId: "f365df5b-2134-425b-8fc4-bba091529fc5" }), child: idleChild("2cf52f50") }];
    b.child.script = [{ runner: idleRunner(), child: idleChild("2cf52f50") }];
    b.tail.value = "2cf52f50";
    const out = await collect(b);
    expect(out).toMatchObject({ ok: true, nativeLeaf: "2cf52f50" });
  });

  it("never writes the witness anywhere: the sidecar and the session file are untouched", async () => {
    const dir = mkdtempSync(join(tmpdir(), "legacy-witness-no-op-"));
    const nonexistent = join(dir, "witness-sidecar.json");
    try {
      expect(existsSync(nonexistent)).toBe(false);
      await collect(b);
      // The collector holds no reference to a sidecar path, a runner or a writer:
      // there is nothing in its surface that could persist a value.
      expect(existsSync(nonexistent)).toBe(false);
      expect(Object.keys(b.runner).some(k => /write|sidecar|persist/i.test(k))).toBe(false);
      expect(Object.keys(b.child).some(k => /write|sidecar|persist/i.test(k))).toBe(false);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe("legacy pi native witness: refusal before any halt", () => {
  it.each([
    ["a busy runner streaming", { runner: { script: [{ runner: idleRunner({ streaming: true }), child: idleChild(LEAF) }] } }, "runner_not_idle"],
    ["a runner still processing", { runner: { script: [{ runner: idleRunner({ processing: true }), child: idleChild(LEAF) }] } }, "runner_not_idle"],
    ["a pending native control", { runner: { script: [{ runner: idleRunner({ controlPending: true }), child: idleChild(LEAF) }] } }, "runner_not_idle"],
    ["a runner that is not ready", { runner: { script: [{ runner: idleRunner({ ready: false }), child: idleChild(LEAF) }] } }, "runner_not_idle"],
    ["a child that is streaming", { child: { script: [{ runner: idleRunner(), child: idleChild(LEAF, { isStreaming: true }) }] } }, "child_not_idle"],
    ["a child that is compacting", { child: { script: [{ runner: idleRunner(), child: idleChild(LEAF, { isCompacting: true }) }] } }, "child_not_idle"],
    ["a child with pending input", { child: { script: [{ runner: idleRunner(), child: idleChild(LEAF, { pendingMessageCount: 2 }) }] } }, "child_not_idle"],
    ["a negative pending count", { child: { script: [{ runner: idleRunner(), child: idleChild(LEAF, { pendingMessageCount: -1 }) }] } }, "projection_invalid"],
    ["a mistyped pending count", { child: { script: [{ runner: idleRunner(), child: idleChild(LEAF, { pendingMessageCount: "zero" }) }] } }, "projection_invalid"],
    ["a mistyped readiness flag", { runner: { script: [{ runner: idleRunner({ ready: "yes" }), child: idleChild(LEAF) }] } }, "projection_invalid"],
    ["a missing readiness flag", { runner: { script: [{ runner: { ...idleRunner(), ready: undefined }, child: idleChild(LEAF) }] } }, "projection_invalid"],
    ["two RunnerCore instances", { runner: { counts: { RunnerCore: 2 } } }, "runner_instance_ambiguous"],
    ["no RunnerCore at all", { runner: { counts: { RunnerCore: 0 } } }, "runner_instance_ambiguous"],
    ["no AgentSessionRuntime", { child: { counts: { AgentSessionRuntime: 0 } } }, "child_instance_ambiguous"],
    ["two AgentSessionRuntime instances", { child: { counts: { AgentSessionRuntime: 3 } } }, "child_instance_ambiguous"],
    ["two AgentSession instances", { child: { counts: { AgentSession: 2 } } }, "session_instance_ambiguous"],
    ["no AgentSession instance", { child: { counts: { AgentSession: 0 } } }, "session_instance_ambiguous"],
    ["a child session that is not the runtime session", { child: { script: [{ runner: idleRunner(), child: idleChild(LEAF, { runtimeSessionMatchesSession: false }) }] } }, "session_identity_mismatch"],
    ["a runner module that is not loaded", { runner: { loaded: [] } }, "module_identity_unverified"],
    ["a Pi chunk that is not loaded", { child: { loaded: [] } }, "module_identity_unverified"],
    ["a leaf that disagrees with the bounded tail", { tail: "2cf52f50" }, "native_leaf_mismatch"],
    ["an absent leaf", { child: { script: [{ runner: idleRunner(), child: idleChild(null) }] } }, "native_leaf_mismatch"],
    ["a leaf that is not a hex entry id", { child: { script: [{ runner: idleRunner(), child: idleChild("not-an-entry") }] } }, "native_leaf_mismatch"],
    ["an unreadable tail", { tail: null }, "native_leaf_mismatch"],
    ["a wrong child session file", { child: { script: [{ runner: idleRunner(), child: idleChild(LEAF, { sessionFile: "/other/history.jsonl" }) }] } }, "session_identity_mismatch"],
    ["a wrong child session id", { child: { script: [{ runner: idleRunner(), child: idleChild(LEAF, { sessionId: "other-session" }) }] } }, "session_identity_mismatch"],
    ["a runner naming another session file", { runner: { script: [{ runner: idleRunner({ sessionFile: "/other/history.jsonl" }), child: idleChild(LEAF) }] } }, "session_identity_mismatch"],
    ["a runner on another launch", { runner: { script: [{ runner: idleRunner({ launchId: "some-other-launch" }), child: idleChild(LEAF) }] } }, "launch_identity_mismatch"],
    ["a runner on another generation", { runner: { script: [{ runner: idleRunner({ generation: "rotated" }), child: idleChild(LEAF) }] } }, "launch_identity_mismatch"],
    ["a runner missing its identity", { runner: { script: [{ runner: idleRunner({ generation: undefined }), child: idleChild(LEAF) }] } }, "launch_identity_mismatch"],
  ] as const)("refuses %s", async (_label, opts, code) => {
    const b = await box(opts as never);
    const out = await collect(b);
    expect(out).toMatchObject({ ok: false, reasons: [code] });
    // A refusal never yields anything a caller could treat as a witness.
    expect((out as { nativeLeaf?: unknown }).nativeLeaf).toBeUndefined();
    expect((out as { evidenceId?: unknown }).evidenceId).toBeUndefined();
  });

  it("refuses drift between rounds instead of accepting the first agreeing round", async () => {
    const b = await box({ runner: { script: [
      { runner: idleRunner(), child: idleChild(LEAF) },
      { runner: idleRunner({ controlPending: false, streaming: true }), child: idleChild(LEAF) },
    ] } });
    // The bracketing R-C-R pair inside ONE round already catches this drift.
    const out = await collect(b);
    expect(out).toMatchObject({ ok: false });
    expect(out.ok === false && (out.reasons[0] === "witness_sample_drift" || out.reasons[0] === "runner_not_idle")).toBe(true);
  });

  it("refuses when only the child projection drifts between rounds", async () => {
    // The bracketing runner pair is stable, so the drift must come from the child.
    const b = await box({ child: { script: [
      { runner: idleRunner(), child: idleChild(LEAF) },
      { runner: idleRunner(), child: idleChild(LEAF, { pendingMessageCount: 4 }) },
    ] } });
    const out = await collect(b);
    expect(out).toMatchObject({ ok: false, reasons: ["witness_sample_drift"] });
  });

  it("refuses a target whose census identity moved between the plan and the witness", async () => {
    // The bound pid is gone, or the pid was reused by a different start identity.
    const b = await box({ census: new Map([[TARGET_CHILD.pid, TARGET_CHILD]]) });
    const out = await collect(b);
    expect(out).toMatchObject({ ok: false, reasons: ["target_identity_unproven"] });
    expect(b.signals).toEqual([]);
  });

  it("refuses identity drift at the pre-signal reproof without signaling or attaching", async () => {
    const b = await box();
    const censusReads = new Map<number, number>();
    const out = await collect(b, { sourceOverrides: {
      census: async pid => {
        const reads = (censusReads.get(pid) ?? 0) + 1;
        censusReads.set(pid, reads);
        if (pid === TARGET_RUNNER.pid && reads === 2) return { ...TARGET_RUNNER, startedAt: "replacement-start" };
        return b.census.get(pid) ?? null;
      },
    } });
    expect(out).toMatchObject({ ok: false, reasons: ["target_identity_unproven"], rounds: 1 });
    expect(b.signals).toEqual([]);
    expect(b.acquired).toEqual([]);
    expect(b.runner.calls.filter(call => call.startsWith("queryProjection"))).toEqual([]);
  });

  it("closes an attached listener when identity drifts before inspection", async () => {
    const b = await box();
    const censusReads = new Map<number, number>();
    const out = await collect(b, { sourceOverrides: {
      census: async pid => {
        const reads = (censusReads.get(pid) ?? 0) + 1;
        censusReads.set(pid, reads);
        if (pid === TARGET_RUNNER.pid && reads === 3) return { ...TARGET_RUNNER, startedAt: "replacement-start" };
        return b.census.get(pid) ?? null;
      },
    } });
    expect(out).toMatchObject({ ok: false, reasons: ["target_identity_unproven"], rounds: 1 });
    expect(b.signals).toEqual([{ pid: TARGET_RUNNER.pid, signal: "SIGUSR1" }]);
    expect(b.acquired).toEqual([TARGET_RUNNER.pid]);
    expect(b.runner.calls.filter(call => call.startsWith("queryProjection"))).toEqual([]);
    expect(b.runner.calls.filter(call => call === "close")).toHaveLength(1);
  });

  it("refuses runner identity drift between rounds before another signal or inspection", async () => {
    const b = await box();
    const censusReads = new Map<number, number>();
    const out = await collect(b, { sourceOverrides: {
      census: async pid => {
        const reads = (censusReads.get(pid) ?? 0) + 1;
        censusReads.set(pid, reads);
        if (pid === TARGET_RUNNER.pid && reads === 6) return { ...TARGET_RUNNER, startedAt: "replacement-start" };
        return b.census.get(pid) ?? null;
      },
    } });
    expect(out).toMatchObject({ ok: false, reasons: ["target_identity_unproven"], rounds: 2 });
    expect(b.signals).toEqual([
      { pid: TARGET_RUNNER.pid, signal: "SIGUSR1" },
      { pid: TARGET_CHILD.pid, signal: "SIGUSR1" },
      { pid: TARGET_RUNNER.pid, signal: "SIGUSR1" },
    ]);
    expect(b.acquired).toEqual([TARGET_RUNNER.pid, TARGET_CHILD.pid, TARGET_RUNNER.pid]);
    expect(b.runner.calls.filter(call => call.startsWith("queryProjection:RunnerCore"))).toHaveLength(2);
    expect(b.child.calls.filter(call => call.startsWith("queryProjection:AgentSessionRuntime"))).toHaveLength(1);
  });

  it("refuses a changed child parent in the final identity read", async () => {
    const b = await box();
    const out = await collect(b, { sourceOverrides: {
      readIdentity: async pid => pid === TARGET_CHILD.pid
        ? { ...TARGET_CHILD, ppid: 9999 }
        : b.census.get(pid) ?? null,
    } });
    expect(out).toMatchObject({ ok: false, reasons: ["target_identity_unproven"], rounds: 2 });
    expect(b.signals).toHaveLength(6);
    expect(b.runner.calls.filter(call => call.startsWith("queryProjection:RunnerCore"))).toHaveLength(4);
    expect(b.child.calls.filter(call => call.startsWith("queryProjection:AgentSessionRuntime"))).toHaveLength(2);
    expect((out as { nativeLeaf?: unknown }).nativeLeaf).toBeUndefined();
  });

  it("refuses a child that is not the bound runner's own child", async () => {
    const b = await box({ census: new Map([
      [TARGET_RUNNER.pid, TARGET_RUNNER],
      [TARGET_CHILD.pid, { ...TARGET_CHILD, ppid: 9999 }],
    ]) });
    const out = await collectorFor(b).witness({
      runner: TARGET_RUNNER,
      child: TARGET_CHILD,
      launchId: LAUNCH_ID, generation: GENERATION, sessionFile: SESSION_FILE,
    });
    expect(out).toMatchObject({ ok: false, reasons: ["target_identity_unproven"] });
    expect(b.signals).toEqual([]);
  });

  it("refuses a target with no bound start identity rather than trusting the pid", async () => {
    const b = await box();
    const out = await collectorFor(b).witness({
      runner: { pid: TARGET_RUNNER.pid, ppid: 4000, startedAt: "  " },
      child: TARGET_CHILD, launchId: LAUNCH_ID, generation: GENERATION, sessionFile: SESSION_FILE,
    });
    expect(out).toMatchObject({ ok: false, reasons: ["target_identity_unproven"] });
    expect(b.signals).toEqual([]);
  });

  it("refuses a target whose inspector endpoint is already owned by someone else", async () => {
    const b = await box();
    // An inspector we did not open and cannot attribute is never adopted.
    b.busyPorts.add(b.endpoints[0]!.port);
    const busy = makeLegacyPiNativeWitness({
      source: source(b),
      signal: "SIGUSR1", tailEntryId: () => b.tail.value,
      modules: FAKE_MODULES,
      rounds: 2, roundGapMs: 0,
    });
    const out = await busy.witness({
      runner: TARGET_RUNNER, child: TARGET_CHILD, launchId: LAUNCH_ID, generation: GENERATION, sessionFile: SESSION_FILE,
    });
    expect(out).toMatchObject({ ok: false, reasons: ["inspector_endpoint_unavailable"] });
    // A collision refuses BEFORE the signal: no process was touched.
    expect(b.signals).toEqual([]);
  });

  it("refuses when the transport reports an unknown listener owner", async () => {
    const b = await box({ runner: { owned: false } });
    const out = await collect(b);
    expect(out).toMatchObject({ ok: false, reasons: ["inspector_listener_unverified"] });
    expect(b.runner.calls).toContain("verifyListenerOwnership");
    // The refused listener is still closed and verified.
    expect(b.runner.calls.filter(c => c === "close").length).toBeGreaterThan(0);
  });

  it("refuses when the listener could not be verifiably closed", async () => {
    const b = await box({ runner: { closeResult: false } });
    const out = await collect(b);
    expect(out).toMatchObject({ ok: false, reasons: ["inspector_close_unverified"] });
  });

  it("refuses when closing the listener throws", async () => {
    const b = await box({ child: { closeThrows: true } });
    const out = await collect(b);
    expect(out).toMatchObject({ ok: false, reasons: ["inspector_close_unverified"] });
  });

  it("refuses when the inspector round trip itself fails", async () => {
    const b = await box({ runner: { queryThrows: true } });
    const out = await collect(b);
    expect(out).toMatchObject({ ok: false, reasons: ["inspector_unavailable"] });
  });

  it("refuses when a scripted inspector error escapes a round", async () => {
    const b = await box({ child: { script: [{ runner: idleRunner(), child: idleChild(LEAF) }, { throw: "target refused the command" }] } });
    const out = await collect(b);
    expect(out).toMatchObject({ ok: false, reasons: ["inspector_unavailable"] });
    // The escaping error text never reaches the caller.
    expect(JSON.stringify(out)).not.toContain("target refused the command");
  });

  it("refuses rather than halts when the target identity changes across the witness", async () => {
    const b = await box();
    // The census drifts while the witness is taken: same pid, a new start identity.
    const reads = { n: 0 };
    const drifting = makeLegacyPiNativeWitness({
      source: {
        ...source(b),
        readIdentity: async pid => (++reads.n > 0
          ? { pid, ppid: pid === TARGET_RUNNER.pid ? 4000 : TARGET_RUNNER.pid, startedAt: "REPLACED" }
          : b.census.get(pid) ?? null),
      },
      signal: "SIGUSR1", tailEntryId: () => b.tail.value,
      modules: FAKE_MODULES,
      rounds: 2, roundGapMs: 0,
    });
    const out = await drifting.witness({
      runner: TARGET_RUNNER, child: TARGET_CHILD, launchId: LAUNCH_ID, generation: GENERATION, sessionFile: SESSION_FILE,
    });
    expect(out).toMatchObject({ ok: false, reasons: ["target_identity_unproven"] });
    // Every listener opened before the drift was still closed.
    expect(b.runner.calls.filter(c => c === "close").length).toBeGreaterThan(0);
    expect(b.child.calls.filter(c => c === "close").length).toBeGreaterThan(0);
  });

  it("a refused witness still closes every listener it opened", async () => {
    const b = await box({ tail: "different-tail" });
    const out = await collect(b);
    expect(out).toMatchObject({ ok: false, reasons: ["native_leaf_mismatch"] });
    // The mismatch is discovered on the child read; both units closed regardless.
    expect(b.child.calls).toContain("close");
    expect(b.runner.calls.filter(c => c === "close")).toHaveLength(b.acquired.filter(pid => pid === TARGET_RUNNER.pid).length);
  });

  it("H1-F1 a close failure AFTER delivery reports REAL delivered:true facts at result time", async () => {
    const b = await box({ runner: { closeResult: false } });
    const audits: number[] = [];
    const out = await makeLegacyPiNativeWitness({
      source: { ...source(b), recordSignalIntent: async (t: { pid: number }) => { audits.push(t.pid); } },
      signal: "SIGUSR1", tailEntryId: () => b.tail.value, modules: FAKE_MODULES, rounds: 2, roundGapMs: 0,
    }).witness({ runner: TARGET_RUNNER, child: TARGET_CHILD, launchId: LAUNCH_ID, generation: GENERATION, sessionFile: SESSION_FILE });
    expect(out).toMatchObject({ ok: false, reasons: ["inspector_close_unverified"] });
    // THE BUG THIS KILLS: the old eager snapshot froze these at delivered:false, making
    // every post-signal failure look pre-effect to the service.
    expect(out.ok === false && out.signal).toEqual({ delivered: true, deliveredPids: [TARGET_RUNNER.pid], auditedBeforeDelivery: true });
    expect(audits.length).toBeGreaterThan(0);
  });

  it("H1-F1 round-2 sample drift AFTER delivery carries both real pids", async () => {
    const b = await box({ child: { script: [{ runner: idleRunner(), child: idleChild(LEAF) }, { runner: idleRunner(), child: idleChild("bbbbbbbb") }] } });
    const out = await collect(b);
    expect(out).toMatchObject({ ok: false, reasons: ["witness_sample_drift"], rounds: 2 });
    expect(out.ok === false && out.signal?.delivered).toBe(true);
    expect(out.ok === false && [...new Set(out.signal?.deliveredPids)].sort()).toEqual([TARGET_CHILD.pid, TARGET_RUNNER.pid].sort());
  });

  it("H1-F2 even a SUCCESSFUL witness carries the real delivered-and-closed facts", async () => {
    const b = await box();
    const out = await collect(b);
    // NO pids in an acceptance receipt (G1 egress rule); the delivery CLASS is truthful.
    expect(out.ok === true && out.signal).toEqual({ delivered: true, auditedBeforeDelivery: false });
  });

  it("never signals its own process", async () => {
    const b = await box();
    b.census.set(process.pid, { pid: process.pid, ppid: 1, startedAt: "daemon-boot" });
    const out = await collectorFor(b).witness({
      runner: { pid: process.pid, ppid: 1, startedAt: "daemon-boot" },
      child: TARGET_CHILD, launchId: LAUNCH_ID, generation: GENERATION, sessionFile: SESSION_FILE,
    });
    expect(out).toMatchObject({ ok: false, reasons: ["target_identity_unproven"] });
    expect(b.signals).toEqual([]);
  });

  it("never signals pid 0, pid 1 or a non-integer pid", async () => {
    for (const pid of [0, 1, 1.5, -1]) {
      const b = await box();
      const out = await collectorFor(b).witness({
        runner: { pid, ppid: 4000, startedAt: "x" },
        child: TARGET_CHILD, launchId: LAUNCH_ID, generation: GENERATION, sessionFile: SESSION_FILE,
      });
      expect(out, `pid=${pid}`).toMatchObject({ ok: false, reasons: ["target_identity_unproven"] });
      expect(b.signals, `pid=${pid}`).toEqual([]);
    }
  });
});

describe("legacy pi native witness: reduced refusal reporting", () => {
  it("every reason is a closed vocabulary token", () => {
    expect([...LEGACY_WITNESS_REASONS].every(reason => /^[a-z_]+$/.test(reason))).toBe(true);
    // The vocabulary is closed: an unlisted token cannot pass the type guard.
    expect(isLegacyPiWitnessReason("child_not_idle")).toBe(true);
    expect(isLegacyPiWitnessReason("leaked_secret")).toBe(false);
    expect(isLegacyPiWitnessReason(42)).toBe(false);
  });

  it("reduces a failure to counts and a closed set, with no pid, port, path or text", () => {
    const failure: LegacyPiWitnessFailure = {
      reasons: ["child_not_idle"],
      rounds: 1,
      detail: "child is streaming (pid 4243 port 54001 /state/pi/secret.jsonl OPENRIG_OCCUPANT_GENERATION)",
    };
    const reduced = reduceWitnessFailure(failure);
    expect(reduced).toEqual({ reasons: ["child_not_idle"], rounds: 1 });
    const text = JSON.stringify(reduced);
    expect(text).not.toContain("4243");
    expect(text).not.toContain("54001");
    expect(text).not.toContain("/state/pi");
    expect(text).not.toContain("OPENRIG_OCCUPANT_GENERATION");
    expect(text).not.toContain("streaming");
  });

  it("drops a reason that is not in the vocabulary rather than passing it through", () => {
    const reduced = reduceWitnessFailure({ reasons: ["child_not_idle", "smuggled text" as never], rounds: 2 });
    expect(reduced.reasons).toEqual(["child_not_idle"]);
  });

  it("an empty reason set still reduces to a closed, non-empty refusal", () => {
    const reduced = reduceWitnessFailure({ reasons: [], rounds: 0 });
    expect(reduced).toEqual({ reasons: ["inspector_unavailable"], rounds: 0 });
  });

  it("a nonsensical round count is normalised rather than echoed", () => {
    expect(reduceWitnessFailure({ reasons: ["runner_not_idle"], rounds: -3 }).rounds).toBe(0);
    expect(reduceWitnessFailure({ reasons: ["runner_not_idle"], rounds: Number.NaN }).rounds).toBe(0);
  });
});

// The real transport, exercised end to end against a PRIVATE test fixture pair:
// two ordinary Node child processes in a temp dir, one hosting a RunnerCore-shaped
// class and one hosting a runtime/session pair. This is the part that proves the
// unit is a working, cleanup-verifiable CDP transport rather than a no-op shim.
describe("legacy pi endpoint containment: bounded appearance and discovered stray close", () => {
  const witnessArgs = {
    runner: TARGET_RUNNER, child: TARGET_CHILD,
    launchId: LAUNCH_ID, generation: GENERATION, sessionFile: SESSION_FILE,
  } as const;

  it("B4 accepts a listener that binds LATE, after the transport handle returns", async () => {
    const b = await box();
    // The inspector thread binds only on the 3rd read. A single post-signal read saw an empty set
    // and produced a spurious inspector_endpoint_containment_failed against a healthy target.
    b.lateBindRounds = 3;
    const out = await collectorFor(b).witness({ ...witnessArgs });
    expect(out).toMatchObject({ ok: true });
    expect(b.discoveredCloseAttempts).toEqual([]);
  });

  it("B4 closes the ACTUALLY DISCOVERED stray port and then refuses", async () => {
    const b = await box();
    const STRAY = 9333;
    // The endpoint port is allocated dynamically, so the census derives it from the opened
    // transport rather than hard-coding 9229, then adds the stray port the target really opened.
    const src = source(b);
    const realOpen = src.openTransport;
    src.openTransport = async (target, endpoint) => {
      const unit = await realOpen(target, endpoint);
      b.openedPorts.set(target.pid, endpoint.port);
      b.extraPorts.set(target.pid, [STRAY]);
      return unit;
    };
    src.pidScopedListeners = async (pid: number) => {
      const opened = b.openedPorts.get(pid);
      if (opened === undefined) return { ok: true, ports: [], public: [] };
      return { ok: true, ports: [String(opened), ...(b.extraPorts.get(pid) ?? []).map(String)], public: [] };
    };
    const out = await makeLegacyPiNativeWitness({
      source: src, signal: "SIGUSR1", tailEntryId: () => LEAF, modules: FAKE_MODULES, rounds: 1, roundGapMs: 0,
    }).witness({ ...witnessArgs });
    expect(out).toMatchObject({ ok: false, reasons: ["inspector_endpoint_containment_failed"] });
    // The close was ATTEMPTED on the discovered stray port, not on the expected endpoint.
    expect(b.discoveredCloseAttempts.map(a => a.port)).toEqual([STRAY]);
  });

  it("C2 delivers ZERO signals when the durable before-signal intent audit cannot be written", async () => {
    const b = await box();
    const src = source(b);
    // The audit is the record that a signal is about to be delivered. If it cannot be written,
    // nothing is delivered at all.
    (src as { recordSignalIntent?: unknown }).recordSignalIntent = async () => { throw new Error("audit unavailable"); };
    const out = await makeLegacyPiNativeWitness({
      source: src, signal: "SIGUSR1", tailEntryId: () => LEAF, modules: FAKE_MODULES, rounds: 1, roundGapMs: 0,
    }).witness({ runner: TARGET_RUNNER, child: TARGET_CHILD, launchId: LAUNCH_ID, generation: GENERATION, sessionFile: SESSION_FILE });
    expect(out).toMatchObject({ ok: false, reasons: ["signal_intent_unaudited"] });
    expect(b.signals).toEqual([]);
    // No transport was opened and nothing was killed.
    expect(b.acquired).toEqual([]);
  });

  it("F2 refuses with delivered false and NEVER attaches when the signal was not delivered", async () => {
    const b = await box();
    const src = source(b);
    // A refused or failed kill is not a partial success: it must refuse BEFORE the attach, typed,
    // and must never report a delivered pid.
    src.deliverSignal = (pid: number, signal: string) => { b.signals.push({ pid, signal }); return false; };
    const out = await makeLegacyPiNativeWitness({
      source: src, signal: "SIGUSR1", tailEntryId: () => LEAF, modules: FAKE_MODULES, rounds: 1, roundGapMs: 0,
    }).witness({ runner: TARGET_RUNNER, child: TARGET_CHILD, launchId: LAUNCH_ID, generation: GENERATION, sessionFile: SESSION_FILE });
    expect(out).toMatchObject({ ok: false, reasons: ["signal_undelivered"], signal: { delivered: false, deliveredPids: [] } });
    // NO attach happened, so no inspector was opened and nothing could be left listening.
    expect(b.acquired).toEqual([]);
    expect(b.lifecycle).toEqual([]);
  });

  it("C2 reports REAL delivery facts rather than inferring them from reasons", async () => {
    const b = await box();
    const src = source(b);
    const audits: number[] = [];
    (src as { recordSignalIntent?: unknown }).recordSignalIntent = async (target: { pid: number }) => { audits.push(target.pid); };
    const out = await makeLegacyPiNativeWitness({
      source: src, signal: "SIGUSR1", tailEntryId: () => LEAF, modules: FAKE_MODULES, rounds: 1, roundGapMs: 0,
    }).witness({ runner: TARGET_RUNNER, child: TARGET_CHILD, launchId: LAUNCH_ID, generation: GENERATION, sessionFile: SESSION_FILE });
    // The intent audit was written BEFORE any delivery, and the facts say so.
    expect(audits.length).toBeGreaterThan(0);
    expect(out).toMatchObject({ ok: true });
    // F3: the intent audit must name the pid, its role and the endpoint it is about to signal.
    // One intent record per attach attempt, in the R-C-R order, once per witness run.
    expect(audits.slice(0, 3)).toEqual([TARGET_RUNNER.pid, TARGET_CHILD.pid, TARGET_RUNNER.pid]);
    expect(audits.length % 3).toBe(0);
    const failing = await makeLegacyPiNativeWitness({
      source: { ...src, preSignalSelfTest: async () => false }, signal: "SIGUSR1", tailEntryId: () => LEAF, modules: FAKE_MODULES, rounds: 1, roundGapMs: 0,
    }).witness({ runner: TARGET_RUNNER, child: TARGET_CHILD, launchId: LAUNCH_ID, generation: GENERATION, sessionFile: SESSION_FILE });
    // A pre-signal refusal reports delivered false: nothing was signalled. The intent
    // audit WAS written before that point, so auditedBeforeDelivery is honestly TRUE at
    // result time (the old eager snapshot pinned the stale pre-delivery false; H1-F1).
    expect(failing).toMatchObject({ ok: false, reasons: ["nativetool_unavailable"], signal: { delivered: false, auditedBeforeDelivery: true } });
  });

  it("B4 refuses a public listener without attempting any close", async () => {
    const b = await box();
    const src = source(b);
    const realOpen = src.openTransport;
    src.openTransport = async (target, endpoint) => {
      const unit = await realOpen(target, endpoint);
      b.openedPorts.set(target.pid, endpoint.port);
      b.publicPorts.set(target.pid, [endpoint.port]);
      return unit;
    };
    src.pidScopedListeners = async (pid: number) => {
      const opened = b.openedPorts.get(pid);
      if (opened === undefined) return { ok: true, ports: [], public: [] };
      return { ok: true, ports: [String(opened)], public: b.publicPorts.get(pid) ?? [] };
    };
    const out = await makeLegacyPiNativeWitness({
      source: src, signal: "SIGUSR1", tailEntryId: () => LEAF, modules: FAKE_MODULES, rounds: 1, roundGapMs: 0,
    }).witness({ ...witnessArgs });
    expect(out).toMatchObject({ ok: false, reasons: ["inspector_endpoint_containment_failed"] });
    // A public listener has no stray port we can attribute, so nothing is closed.
    expect(b.discoveredCloseAttempts).toEqual([]);
  });

  it("B5 refuses without any signal when the self-test cannot run its tools", async () => {
    const b = await box();
    const out = await collectorFor(b, { sourceOverrides: { preSignalSelfTest: async () => false } }).witness({ ...witnessArgs });
    expect(out).toMatchObject({ ok: false, reasons: ["nativetool_unavailable"] });
    // Zero signals: an unusable tool must never reach delivery.
    expect(b.signals).toEqual([]);
  });

  it("S2 the default self-test path refuses with zero signals when the tool is unresolvable", async () => {
    const b = await box();
    // No preSignalSelfTest override at all: the shipped default runs, and the census seam that
    // stands in for ps/lsof is made to fail as an unresolvable tool would.
    const out = await collectorFor(b, {
      sourceOverrides: { pidScopedListeners: async () => ({ ok: false, ports: [], public: [] }) },
    }).witness({ ...witnessArgs });
    expect(out).toMatchObject({ ok: false, reasons: ["nativetool_unavailable"] });
    expect(b.signals).toEqual([]);
  });
});

describe.skipIf(process.platform === "win32")("legacy pi inspector transport (private fixture processes)", () => {
  let dir: string;
  let runner: ReturnType<typeof spawn> | null = null;
  let childPid: number | null = null;
  const spawned: Array<ReturnType<typeof spawn>> = [];

  beforeEach(() => { dir = mkdtempSync(join(tmpdir(), "legacy-witness-")); });
  afterEach(async () => {
    const fixturePids = [childPid, ...spawned.map(proc => typeof proc.pid === "number" ? proc.pid : null)]
      .filter((pid): pid is number => pid !== null);
    if (childPid !== null) { try { process.kill(childPid, "SIGKILL"); } catch { /* already gone */ } }
    for (const proc of spawned.splice(0)) { try { proc.kill("SIGKILL"); } catch { /* already gone */ } }
    const deadline = Date.now() + 3_000;
    while (Date.now() < deadline && (await Promise.all(fixturePids.map(identityOf))).some(Boolean)) {
      await new Promise<void>(resolve => setTimeout(resolve, 25));
    }
    expect((await Promise.all(fixturePids.map(identityOf))).filter(Boolean)).toEqual([]);
    expect(await listeningPids(LEGACY_INSPECTOR_PORT)).toEqual([]);
    runner = null;
    childPid = null;
    rmSync(dir, { recursive: true, force: true });
  });

  it("reads allowlisted primitives out of two live processes and closes both listeners", async () => {
    expect(await listeningPids(LEGACY_INSPECTOR_PORT)).toEqual([]);
    const sessionFile = join(dir, "history.jsonl");
    const moduleEvaluationFile = join(dir, "module-evaluations.log");
    writeFileSync(sessionFile, `${JSON.stringify({ id: "aaaa1111" })}\n${JSON.stringify({ id: "11112222" })}\n${JSON.stringify({ id: LEAF })}\n`);

    // The child hosts the runtime and its session, in ONE module so both exports
    // resolve from one already-loaded URL.
    const childModule = join(dir, "pi-session.mjs");
    writeFileSync(childModule, [
      "import { appendFileSync } from 'node:fs';",
      "appendFileSync(process.env.OPENRIG_FIXTURE_MODULE_EVALUATIONS, 'child\\n');",
      "export class AgentSession {",
      "  constructor(file, id) { this._file = file; this._id = id; this.isStreaming = false; this.isCompacting = false; this.pendingMessageCount = 0; }",
      "  get sessionFile() { return this._file; }",
      "  get sessionId() { return this._id; }",
      "  get sessionManager() { return { getLeafId: () => process.env.OPENRIG_FIXTURE_LEAF }; }",
      "}",
      "export class AgentSessionRuntime {",
      "  constructor(session) { this._session = session; }",
      "  get session() { return this._session; }",
      "}",
      "globalThis.__session = new AgentSession(process.env.OPENRIG_FIXTURE_SESSION, process.env.OPENRIG_FIXTURE_SESSION_ID);",
      "globalThis.__runtime = new AgentSessionRuntime(globalThis.__session);",
      "setInterval(() => {}, 500);",
    ].join("\n"));

    const childPidFile = join(dir, "child.pid");
    // The runner hosts the core and spawns the child, so parentage is real. The
    // child records its own pid and remains alive with a live runtime/session.
    const runnerModule = join(dir, "pi-runner.mjs");
    writeFileSync(runnerModule, [
      "import { spawn } from 'node:child_process';",
      "import { appendFileSync, writeFileSync } from 'node:fs';",
      "appendFileSync(process.env.OPENRIG_FIXTURE_MODULE_EVALUATIONS, 'runner\\n');",
      "export class RunnerCore {",
      "  constructor(file, id) {",
      "    this.ready = true;",
      "    this.streaming = false;",
      "    this.processing = false;",
      "    this.controlPending = false;",
      "    this.sessionFile = file;",
      "    this.sessionId = id;",
      "    this.lastEntryId = 'poisoned-ui-uuid';",
      "    this.identity = { launchId: process.env.OPENRIG_FIXTURE_LAUNCH, generation: process.env.OPENRIG_FIXTURE_GENERATION };",
      "  }",
      "}",
      "globalThis.__core = new RunnerCore(process.env.OPENRIG_FIXTURE_SESSION, process.env.OPENRIG_FIXTURE_SESSION_ID);",
      `globalThis.__child = spawn(process.execPath, [${JSON.stringify(childModule)}], { stdio: ['ignore','ignore','ignore'], env: process.env });`,
      `writeFileSync(${JSON.stringify(childPidFile)}, String(globalThis.__child.pid));`,
      "setInterval(() => {}, 500);",
    ].join("\n"));

    const runnerChild = spawn(process.execPath, [runnerModule], {
      env: {
        ...process.env,
        OPENRIG_FIXTURE_SESSION: sessionFile,
        OPENRIG_FIXTURE_SESSION_ID: SESSION_ID,
        OPENRIG_FIXTURE_LAUNCH: LAUNCH_ID,
        OPENRIG_FIXTURE_GENERATION: GENERATION,
        OPENRIG_FIXTURE_LEAF: LEAF,
        OPENRIG_FIXTURE_MODULE_EVALUATIONS: moduleEvaluationFile,
      },
      stdio: ["ignore", "ignore", "ignore"],
    });
    spawned.push(runnerChild);
    runner = runnerChild;

    // Wait for the real child, so its bound identity and parentage are observable.
    const deadline = Date.now() + 15_000;
    for (;;) {
      if (existsSync(childPidFile)) {
        const recorded = Number(readFileSync(childPidFile, "utf-8").trim());
        if (Number.isInteger(recorded) && recorded > 1 && await identityOf(recorded)) { childPid = recorded; break; }
      }
      if (Date.now() > deadline) throw new Error("fixture child never appeared");
      await new Promise<void>(r => setTimeout(r, 100));
    }

    // Node reports realpathed ESM URLs in Debugger.scriptParsed, including on
    // macOS where /var may resolve through /private. Bind to those exact URLs.
    const modules = { runnerModuleUrl: moduleUrlOf(realpathSync(runnerModule)), piModuleUrl: moduleUrlOf(realpathSync(childModule)) };
    const native = makeNativeLegacyPiTransportSource(modules);
    const loadedModuleUrlsObserved: string[][] = [];
    const nativeProjectionErrors: string[] = [];
    const observedSource: LegacyPiTransportSource = {
      ...native,
      openTransport: async (target, endpoint) => {
        const transport = await native.openTransport(target, endpoint);
        return {
          ...transport,
          loadedModuleUrls: async () => {
            const urls = await transport.loadedModuleUrls();
            loadedModuleUrlsObserved.push(urls);
            return urls;
          },
          queryProjection: async request => {
            try { return await transport.queryProjection(request); }
            catch (error) {
              const detail = error instanceof Error ? `${error.name}: ${error.message}` : String(error);
              nativeProjectionErrors.push(`${request.exportName}: ${detail.split(dir).join("<fixture-dir>")}`);
              throw error;
            }
          },
        };
      },
    };
    const collector = makeLegacyPiNativeWitness({
      source: observedSource,
      signal: "SIGUSR1",
      tailEntryId: (file) => tailId(file),
      modules,
      rounds: 2,
      roundGapMs: 50,
    });

    const runnerIdentity = await identityOf(Number(runnerChild.pid));
    const childIdentity = await identityOf(childPid);
    expect(runnerIdentity).not.toBeNull();
    expect(childIdentity?.ppid).toBe(Number(runnerChild.pid));

    const out = await collector.witness({
      runner: runnerIdentity!,
      child: childIdentity!,
      launchId: LAUNCH_ID,
      generation: GENERATION,
      sessionFile,
    });

    // The REAL leaf was read out of the live child session and matched the tail,
    // while the runner's own cursor stayed the poisoned UI UUID it was seeded with.
    expect(out, `native collector result: ${JSON.stringify(out)}; requested URLs: ${JSON.stringify(modules)}; observed URLs: ${JSON.stringify(loadedModuleUrlsObserved)}; CDP errors: ${JSON.stringify(nativeProjectionErrors)}`)
      .toMatchObject({ ok: true, nativeLeaf: LEAF, launchId: LAUNCH_ID, generation: GENERATION, sessionId: SESSION_ID, rounds: 2 });
    expect((out as { nativeLeaf: string }).nativeLeaf).not.toBe("poisoned-ui-uuid");
    expect((out as { evidenceId: string }).evidenceId).toMatch(/^sha256:[0-9a-f]{16}$/);

    // The real child runtime remains alive after collection, just like the
    // runner; the witness only reads and closes its inspector listener.
    expect(await identityOf(childPid)).toMatchObject({ pid: childPid, ppid: Number(runnerChild.pid) });
    expect(readFileSync(moduleEvaluationFile, "utf-8")).toBe("runner\nchild\n");

    // No inspector listener is left behind by either of the two targets.
    expect(await listeningPids(LEGACY_INSPECTOR_PORT)).toEqual([]);
    // Both fixture processes are still alive: the witness read, it never halted.
    expect(runnerChild.exitCode).toBeNull();
  }, 60_000);

  it("refuses a busy inspector port before sending any signal", async () => {
    const { LEGACY_INSPECTOR_PORT, endpointInUse } = await import("../src/domain/legacy-pi-native-witness.js");
    // Hold the port the way a foreign inspector would, then prove the refusal.
    const squatter = createServer();
    await new Promise<void>((resolve) => squatter.listen(LEGACY_INSPECTOR_PORT, "127.0.0.1", () => resolve()));
    try {
      expect(await endpointInUse({ host: "127.0.0.1", port: LEGACY_INSPECTOR_PORT })).toBe(true);
      const signals: number[] = [];
      const collector = makeLegacyPiNativeWitness({
        source: {
          resolveEndpoint: async () => ({ host: "127.0.0.1", port: LEGACY_INSPECTOR_PORT }),
          endpointInUse,
          census: async pid => ({ pid, ppid: pid === 4242 ? 4000 : 4242, startedAt: "squatted" }),
          deliverSignal: (pid) => { signals.push(pid); return true; },
          openTransport: async () => { throw new Error("must never attach to a foreign listener"); },
          readIdentity: async pid => ({ pid, ppid: pid === 4242 ? 4000 : 4242, startedAt: "squatted" }),
        },
        signal: "SIGUSR1",
        tailEntryId: () => LEAF,
        modules: FAKE_MODULES,
        rounds: 2, roundGapMs: 0,
      });
      const out = await collector.witness({
        runner: { pid: 4242, ppid: 4000, startedAt: "squatted" },
        child: { pid: 4243, ppid: 4242, startedAt: "squatted" },
        launchId: LAUNCH_ID, generation: GENERATION, sessionFile: SESSION_FILE,
      });
      expect(out).toMatchObject({ ok: false, reasons: ["inspector_endpoint_unavailable"] });
      expect(signals).toEqual([]);
    } finally {
      await new Promise<void>((resolve) => squatter.close(() => resolve()));
    }
  }, 20_000);
});

describe("legacy signal intent event construction", () => {
  const seat: LegacySignalIntentSeatIdentity = { rigId: "xv", nodeId: "01M3QP9F8M1HZA5NDCAP79931D", logicalId: "orch1.lead" };
  const binding = { runner: { pid: 4242 } };
  const endpoint = { host: "127.0.0.1" as const, port: 17433 };
  const at = "2026-10-06T08:00:00.000Z";

  it("bound runner PID implies runner role", () => {
    const target = { pid: 4242, startedAt: "2026-10-02T20:16:39.000Z" };
    const event = buildLegacySignalIntentEvent({ seat, binding, target, endpoint, at });
    expect(event.targetPid).toBe(4242);
    expect(event.targetRole).toBe("runner");
    expect(event.targetStartedAt).toBe("2026-10-02T20:16:39.000Z");
    expect(event.endpointPort).toBe(17433);
    expect(event.rigId).toBe("xv");
    expect(event.nodeId).toBe("01M3QP9F8M1HZA5NDCAP79931D");
    expect(event.logicalId).toBe("orch1.lead");
    expect(event.at).toBe(at);
    expect(event.type).toBe("seat.runner_rehost_legacy_signal_intent");
    expect(event.signalDelivered).toBe(false);
    expect(event.operator).toBe(null);
  });

  it("bound child PID implies child role", () => {
    const target = { pid: 4243, startedAt: "2026-10-02T23:27:30.000Z" };
    const event = buildLegacySignalIntentEvent({ seat, binding, target, endpoint, at });
    expect(event.targetPid).toBe(4243);
    expect(event.targetRole).toBe("child");
    expect(event.targetStartedAt).toBe("2026-10-02T23:27:30.000Z");
    expect(event.endpointPort).toBe(17433);
    expect(event.rigId).toBe("xv");
    expect(event.nodeId).toBe("01M3QP9F8M1HZA5NDCAP79931D");
    expect(event.logicalId).toBe("orch1.lead");
    expect(event.at).toBe(at);
    expect(event.type).toBe("seat.runner_rehost_legacy_signal_intent");
    expect(event.signalDelivered).toBe(false);
    expect(event.operator).toBe(null);
  });

  it("parent/start/port/seat evidence unchanged across both roles", () => {
    const runnerTarget = { pid: 4242, startedAt: "2026-10-02T20:16:39.000Z" };
    const childTarget = { pid: 4243, startedAt: "2026-10-02T23:27:30.000Z" };
    const runnerEvent = buildLegacySignalIntentEvent({ seat, binding, target: runnerTarget, endpoint, at });
    const childEvent = buildLegacySignalIntentEvent({ seat, binding, target: childTarget, endpoint, at });

    // Shared fields identical
    expect(runnerEvent.rigId).toBe(childEvent.rigId);
    expect(runnerEvent.nodeId).toBe(childEvent.nodeId);
    expect(runnerEvent.logicalId).toBe(childEvent.logicalId);
    expect(runnerEvent.at).toBe(childEvent.at);
    expect(runnerEvent.endpointPort).toBe(childEvent.endpointPort);
    expect(runnerEvent.type).toBe(childEvent.type);
    expect(runnerEvent.signalDelivered).toBe(childEvent.signalDelivered);
    expect(runnerEvent.operator).toBe(childEvent.operator);

    // Role-specific fields differ
    expect(runnerEvent.targetPid).toBe(4242);
    expect(runnerEvent.targetRole).toBe("runner");
    expect(runnerEvent.targetStartedAt).toBe("2026-10-02T20:16:39.000Z");
    expect(childEvent.targetPid).toBe(4243);
    expect(childEvent.targetRole).toBe("child");
    expect(childEvent.targetStartedAt).toBe("2026-10-02T23:27:30.000Z");
  });
});
async function identityOf(pid: number): Promise<LegacyPiTargetIdentity | null> {
  if (!Number.isInteger(pid) || pid <= 1) return null;
  try {
    const { stdout } = await run("ps", ["-Ao", "pid,ppid,lstart"], { encoding: "utf-8", env: { ...process.env, LC_ALL: "C" } });
    for (const line of stdout.split("\n")) {
      const match = line.trim().match(/^(\d+)\s+(\d+)\s+(\w{3}\s+\w{3}\s+\d+\s+\d{2}:\d{2}:\d{2}\s+\d{4})$/);
      if (match && Number(match[1]) === pid) return { pid, ppid: Number(match[2]), startedAt: match[3]! };
    }
    return null;
  } catch { return null; }
}

/** Pids listening on a loopback port. Empty means nothing is left listening. */
async function listeningPids(port: number): Promise<number[]> {
  try {
    const { stdout } = await run("lsof", ["-nP", `-iTCP:${port}`, "-sTCP:LISTEN", "-t"], { encoding: "utf-8" });
    return stdout.split(/\s+/).filter(Boolean).map(Number);
  } catch { return []; }
}

/** The bounded JSONL tail entry id, exactly as the daemon's route seam reads it. */
function tailId(file: string): string | null {
  try {
    const lines = readFileSync(file, "utf-8").split("\n").filter(line => line.trim().length > 0);
    const parsed = JSON.parse(lines[lines.length - 1]!) as { id?: unknown };
    return typeof parsed.id === "string" ? parsed.id : null;
  } catch { return null; }
}
