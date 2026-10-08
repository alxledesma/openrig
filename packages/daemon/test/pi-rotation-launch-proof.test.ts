import { describe, expect, it } from "vitest";
import type { PiRunnerState } from "../src/adapters/pi-runner-protocol.js";
import type { NativeProcessRow } from "../src/domain/native-process-lineage.js";
import { observePiRotationLaunch, type PiKernelLaunchObservation } from "../src/domain/pi-rotation-launch-proof.js";
import type { PiNativeProof } from "../src/domain/coordinator-runtime-availability.js";

const NOW = Date.parse("2026-10-07T23:00:01.000Z");
const input = {
  nodeId: "node-1", sessionName: "lead@app", generation: "generation-1",
  sessionFile: "/seat/pi/lead/sessions/current.jsonl", agentDir: "/seat/pi/lead/agent",
  runnerEntryPath: "/installed/openrig/daemon/pi-runner.js",
};
const proof: PiNativeProof = {
  state: "present", generation: input.generation, launchId: "launch-1", lastEntryId: "entry-9",
  fingerprint: JSON.stringify({ pane: "%46", runner: [101, 100], pi: [102, 101], launchId: "launch-1", sidecarUpdatedAt: "2026-10-07T23:00:00.000Z" }),
  quiescence: { settled: true, observedAt: "2026-10-07T23:00:00.000Z" },
};
const rows: NativeProcessRow[] = [
  { pid: 100, ppid: 1, startedAt: "Wed Oct 7 20:00:00 2026", command: "-zsh", executableName: "zsh" },
  { pid: 101, ppid: 100, startedAt: "Wed Oct 7 22:00:00 2026", command: "node [runner]", executableName: "node" },
  { pid: 102, ppid: 101, startedAt: "Wed Oct 7 22:00:00 2026", command: "pi", executableName: "pi" },
];
const state: PiRunnerState = {
  managedSpawnProof: { version: 1, intent: { trust: "no-approve" } } as PiRunnerState["managedSpawnProof"],
  ready: true, launchId: "launch-1", sessionFile: input.sessionFile, sessionId: "native-session-1",
  lastEntryId: "entry-9", updatedAt: "2026-10-07T23:00:00.000Z",
  rpcSessionFileProof: {
    launchId: "launch-1", generation: input.generation, childPid: 102, sessionFile: input.sessionFile,
    responseId: "pi-runner-get-state", observedAt: "2026-10-07T23:00:00.000Z",
  },
  runtimeReadiness: {
    launchId: "launch-1", generation: input.generation, sessionFile: input.sessionFile,
    model: { provider: "provider", id: "model", contextWindow: 1000 }, thinkingLevel: "high",
    observedAt: "2026-10-07T23:00:00.000Z", failures: [],
  },
  quiescence: {
    launchId: "launch-1", generation: input.generation, sessionFile: input.sessionFile,
    lastEntryId: "entry-9", settled: true, observedAt: "2026-10-07T23:00:00.000Z",
  },
};
const binding = {
  nodeId: input.nodeId, sessionName: input.sessionName, generation: input.generation,
  runtime: "pi", pane: "%46", sessionFile: input.sessionFile,
};
const kernel = (role: "runner" | "child"): PiKernelLaunchObservation => ({
  publicIdentityMatches: true,
  runnerEntryMatches: role === "runner", runnerFlagsMatch: role === "runner",
  rpcChildMatches: role === "child", sessionDirectoryMatches: role === "child",
  agentDirectoryMatches: role === "child", trustFlag: "no-approve",
});

function deps(over: {
  processes?: () => Promise<NativeProcessRow[]>;
  sidecar?: () => Promise<PiRunnerState | null>;
  kernelProcess?: (pid: number, role: "runner" | "child") => Promise<PiKernelLaunchObservation | null>;
} = {}) {
  return {
    tmux: { getPanePid: async () => 100 },
    currentBinding: async () => binding,
    sidecar: over.sidecar ?? (async () => state),
    processes: over.processes ?? (async () => rows),
    kernelProcess: over.kernelProcess ?? (async (_pid, role) => kernel(role)),
    managedLaunch: async () => true,
    now: () => NOW,
  };
}

describe("Pi rotation OS launch proof", () => {
  it("holds when legacy launch has no actual spawn proof", async () => {
    await expect(observePiRotationLaunch({ ...input, proof }, deps({ sidecar: async () => ({ ...state, managedSpawnProof: undefined }) }))).resolves.toBeNull();
  });
  it("accepts erased child flags only with independent managed corroboration", async () => {
    const d = deps({ kernelProcess: async (_pid, role) => role === "runner" ? kernel(role) : { ...kernel(role), rpcChildMatches: false, sessionDirectoryMatches: false, trustFlag: null } });
    await expect(observePiRotationLaunch({ ...input, proof }, d)).resolves.not.toBeNull();
    d.managedLaunch = async () => false;
    await expect(observePiRotationLaunch({ ...input, proof }, d)).resolves.toBeNull();
  });
  it("returns a verified launch only after stable process and child-bound RPC proof", async () => {
    await expect(observePiRotationLaunch({ ...input, proof }, deps())).resolves.toEqual({
      generation: "generation-1", launchId: "launch-1", sessionFile: input.sessionFile,
      pid: 102, trustFlag: "no-approve", startFingerprint: expect.any(String),
    });
  });

  it("holds when the managed RPC response is not bound to the observed child", async () => {
    await expect(observePiRotationLaunch({ ...input, proof }, deps({
      sidecar: async () => ({ ...state, rpcSessionFileProof: { ...state.rpcSessionFileProof!, childPid: 999 } }),
    }))).resolves.toBeNull();
  });

  it("holds when the daemon sidecar changes during the OS observations", async () => {
    let read = 0;
    await expect(observePiRotationLaunch({ ...input, proof }, deps({ sidecar: async () => {
      read += 1;
      return read === 1 ? state : { ...state, lastEntryId: "entry-moved" };
  } }))).resolves.toBeNull();
  });

  it.each([
    ["model", { provider: "provider", id: "changed", contextWindow: 1000 }],
    ["thinking level", "low"],
  ])("holds when effective RPC %s changes during the OS observations", async (field, value) => {
    let read = 0;
    await expect(observePiRotationLaunch({ ...input, proof }, deps({ sidecar: async () => {
      read += 1;
      if (read === 1) return state;
      return { ...state, runtimeReadiness: { ...state.runtimeReadiness!, [field === "model" ? "model" : "thinkingLevel"]: value } };
    } }))).resolves.toBeNull();
  });

  it("holds when the process start changes between independent samples", async () => {
    let sample = 0;
    await expect(observePiRotationLaunch({ ...input, proof }, deps({ processes: async () => {
      sample += 1;
      return sample === 1 ? rows : rows.map(row => row.pid === 102 ? { ...row, startedAt: "Wed Oct 7 22:01:00 2026" } : row);
    } }))).resolves.toBeNull();
  });

  it("holds when the kernel identity or real child trust posture is incomplete", async () => {
    await expect(observePiRotationLaunch({ ...input, proof }, deps({
      kernelProcess: async (_pid, role) => ({ ...kernel(role), publicIdentityMatches: role !== "child", trustFlag: role === "child" ? "approve" : "no-approve" }),
    }))).resolves.toBeNull();
  });

  it("proves launch identity while native readiness is busy and has a current failure", async () => {
    const busyProof: PiNativeProof = {
      ...proof,
      quiescence: { settled: false, observedAt: "2026-10-07T23:00:00.000Z" },
    };
    const busyState: PiRunnerState = {
      ...state,
      runtimeReadiness: {
        ...state.runtimeReadiness!,
        failures: [{ code: "provider_incomplete", observedAt: "2026-10-07T23:00:00.000Z" }],
      },
      quiescence: { ...state.quiescence!, settled: false },
    };
    await expect(observePiRotationLaunch({ ...input, proof: busyProof }, deps({ sidecar: async () => busyState })))
      .resolves.toMatchObject({ generation: input.generation, launchId: "launch-1", sessionFile: input.sessionFile });
  });

  it("holds when the successful child-bound get_state receipt is older than fifteen seconds", async () => {
    const stale: PiRunnerState = {
      ...state,
      rpcSessionFileProof: { ...state.rpcSessionFileProof!, observedAt: "2026-10-07T22:59:45.999Z" },
    };
    await expect(observePiRotationLaunch({ ...input, proof }, deps({ sidecar: async () => stale }))).resolves.toBeNull();
  });
});
