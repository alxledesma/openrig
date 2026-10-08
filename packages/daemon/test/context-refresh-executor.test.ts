import { afterEach, describe, expect, it } from "vitest";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { DEFAULT_CONTEXT_REFRESH_POLICY, type ContextRefreshActor, type ContextRefreshGrant, type ContextRefreshTarget } from "../src/domain/context-refresh-contract.js";
import type { ContextRefreshEnrollment, ContextRefreshStatus } from "../src/domain/context-refresh-integration.js";
import {
  FileContextRefreshJournal, runContextRefreshCycle, type ContextRefreshJournal, type ContextRefreshJournalState,
  type ContextRefreshLoopInput, type ContextRefreshTransport,
} from "../src/adapters/context-refresh-executor.js";

const actor: ContextRefreshActor = { session: "operator-agent@kernel", generation: "operator-g1" };
const launchId = "native-launch-1";
const executor = { ...actor, nodeId: "operator-node", launchId, configurationDigest: "a".repeat(64) };
const target = (nodeId: string): ContextRefreshTarget => ({ nodeId, sessionName: `${nodeId}@rig`, generation: `${nodeId}-g1`,
  runtime: "codex", nativeId: `${nodeId}-native`, configurationDigest: "b".repeat(64) });
const grant = (targets: ContextRefreshTarget[], validUntil = 10_000): ContextRefreshGrant => ({ grantId: "grant-1", kind: "context-refresh",
  executor, targets, policy: { ...DEFAULT_CONTEXT_REFRESH_POLICY }, policyRevision: "policy-r1", validUntil,
  validator: { session: "validator@kernel", generation: "validator-g1" }, recoveryOwner: { session: "recovery@kernel", generation: "recovery-g1" } });
class MemoryJournal implements ContextRefreshJournal {
  state: ContextRefreshJournalState | null = null;
  read() { return this.state && structuredClone(this.state); }
  save(state: ContextRefreshJournalState) { this.state = structuredClone(state); }
}
function status(operationId: string, complete: boolean): ContextRefreshStatus {
  return { grantId: "grant-1", nodeId: "target-a", invocation: complete ? { operationId, state: "completed" } : { operationId, state: "in-flight" }, attempt: null, checkpointRequest: null };
}
function input(overrides: Partial<ContextRefreshLoopInput> & { journal: ContextRefreshJournal; transport: ContextRefreshTransport }): ContextRefreshLoopInput {
  return { actor, launchId, supervisorPid: 1234, journal: overrides.journal, transport: overrides.transport,
    clock: { now: () => 1000, sleep: async () => {} }, live: () => true,
    operationId: () => "operation-1", ...overrides };
}
function reverseKeys<T extends object>(value: T): T {
  return Object.fromEntries(Object.entries(value).reverse()) as T;
}

describe("Operator-bound context refresh executor", () => {
  let temporary: string | undefined;
  afterEach(() => { if (temporary) fs.rmSync(temporary, { recursive: true, force: true }); temporary = undefined; });

  it("accepts a Pi file identity without weakening Codex IDs or canonical path syntax", async () => {
    let nativeTarget: ContextRefreshTarget = { ...target("target-a"), runtime: "pi", nativeId: "/private/session.jsonl" };
    let steps = 0;
    const transport: ContextRefreshTransport = {
      enrollment: async () => ({ state: "ready", pollMs: 120_000, grants: [grant([nativeTarget])] }),
      step: async () => { steps++; },
      reconcile: async (_actor, selection) => status(selection.operationId!, true),
      status: async (_actor, selection) => status(selection.operationId!, true),
    };
    expect((await runContextRefreshCycle(input({ journal: new MemoryJournal(), transport }))).targets[0]?.state).toBe("completed");
    for (const invalid of ["relative.jsonl", "/private/../session.jsonl", "/private/line\nbreak.jsonl"]) {
      nativeTarget = { ...nativeTarget, nativeId: invalid };
      expect((await runContextRefreshCycle(input({ journal: new MemoryJournal(), transport }))).enrollment).toBe("held");
    }
    nativeTarget = { ...nativeTarget, runtime: "codex", nativeId: "/private/session.jsonl" };
    expect((await runContextRefreshCycle(input({ journal: new MemoryJournal(), transport }))).enrollment).toBe("held");
    expect(steps).toBe(1);
  });

  it("does not manufacture or run work without live enrollment, exact executor binding, or live parent", async () => {
    const journal = new MemoryJournal(); let steps = 0;
    const transport: ContextRefreshTransport = {
      enrollment: async () => ({ state: "held", reason: "no matching finite grant" }),
      step: async () => { steps++; throw new Error("must not step"); },
      reconcile: async () => ({ grantId: "grant-1", nodeId: "target-a", invocation: null, attempt: null, checkpointRequest: null }),
      status: async () => ({ grantId: "grant-1", nodeId: "target-a", invocation: null, attempt: null, checkpointRequest: null }),
    };
    expect(await runContextRefreshCycle(input({ journal, transport }))).toEqual({ enrollment: "held", targets: [] });
    expect(await runContextRefreshCycle(input({ journal, transport, live: () => false }))).toEqual({ enrollment: "stopped", targets: [] });
    const holderGrant = { ...grant([target("target-a")]), kind: "holder-continuation" } as unknown as ContextRefreshGrant;
    const holderTransport: ContextRefreshTransport = { ...transport,
      enrollment: async () => ({ state: "ready", pollMs: 120_000, grants: [holderGrant] }) };
    expect(await runContextRefreshCycle(input({ journal, transport: holderTransport }))).toEqual({ enrollment: "held", targets: [] });
    expect(steps).toBe(0); expect(journal.read()).toBeNull();
  });

  it("holds expired grants before a step and refuses an enrollment bound to another launch", async () => {
    let steps = 0;
    const transport: ContextRefreshTransport = {
      enrollment: async (_actor, request): Promise<ContextRefreshEnrollment> => ({ state: "ready", pollMs: 120_000,
        grants: [grant([target("target-a")], request.launchId === launchId ? 999 : 10_000)] }),
      step: async () => { steps++; throw new Error("must not step"); },
      reconcile: async () => ({ grantId: "grant-1", nodeId: "target-a", invocation: null, attempt: null, checkpointRequest: null }),
      status: async () => ({ grantId: "grant-1", nodeId: "target-a", invocation: null, attempt: null, checkpointRequest: null }),
    };
    const expired = await runContextRefreshCycle(input({ journal: new MemoryJournal(), transport }));
    expect(expired.enrollment).toBe("held"); expect(expired.targets).toEqual([]);
    const mismatched = await runContextRefreshCycle(input({ journal: new MemoryJournal(), transport, launchId: "different-launch" }));
    expect(mismatched.enrollment).toBe("held"); expect(mismatched.targets).toEqual([]);
    expect(steps).toBe(0);
  });

  it("accepts semantically identical policy and actor objects with canonicalized key order", async () => {
    const journal = new MemoryJournal(); let steps = 0, completed = false;
    const base = grant([target("target-a")]);
    const wireGrant = { ...base, executor: reverseKeys(base.executor), policy: reverseKeys(base.policy),
      validator: reverseKeys(base.validator), recoveryOwner: reverseKeys(base.recoveryOwner), targets: [reverseKeys(target("target-a"))] } as ContextRefreshGrant;
    const transport: ContextRefreshTransport = {
      enrollment: async () => ({ state: "ready", pollMs: 120_000, grants: [wireGrant] }),
      step: async () => { steps++; },
      reconcile: async (_actor, selection) => ({ grantId: "grant-1", nodeId: "target-a",
        invocation: completed ? { operationId: selection.operationId!, state: "completed" } : { operationId: selection.operationId!, state: "in-flight" },
        attempt: null, checkpointRequest: null }),
      status: async (_actor, selection) => ({ grantId: "grant-1", nodeId: "target-a",
        invocation: completed ? { operationId: selection.operationId!, state: "completed" } : { operationId: selection.operationId!, state: "in-flight" },
        attempt: null, checkpointRequest: null }),
    };
    const reorderedActor = reverseKeys(actor);
    const first = await runContextRefreshCycle(input({ actor: reorderedActor, journal, transport }));
    expect(first.targets[0]).toMatchObject({ operationId: "operation-1", state: "unknown" });
    completed = true;
    const second = await runContextRefreshCycle(input({ actor, journal, transport }));
    expect(second.targets[0]).toMatchObject({ operationId: "operation-1", state: "completed" });
    expect(steps).toBe(1);
  });

  it("persists one operation before POST and reconciles that same UNKNOWN after restart until its exact completion receipt appears", async () => {
    temporary = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), "context-refresh-journal-")); fs.chmodSync(temporary, 0o700);
    let journal: ContextRefreshJournal = new FileContextRefreshJournal(temporary, actor, launchId);
    let stepCalls = 0, reconciles = 0, completed = false, sentOperation = "";
    let enrolled = grant([target("target-a")]);
    const transport: ContextRefreshTransport = {
      enrollment: async () => ({ state: "ready", pollMs: 120_000, grants: [enrolled] }),
      step: async (_actor, selection) => {
        stepCalls++; sentOperation = selection.operationId;
        expect(journal.read()?.entries[0]?.operationId).toBe(selection.operationId);
        throw new Error("response lost after dispatch");
      },
      reconcile: async (_actor, selection) => { reconciles++; return status(selection.operationId, completed); },
      status: async (_actor, selection) => status(selection.operationId, completed),
    };
    const first = await runContextRefreshCycle(input({ journal, transport }));
    expect(first.targets[0]).toMatchObject({ operationId: "operation-1", state: "unknown" });
    expect(stepCalls).toBe(1); expect(fs.statSync(path.join(temporary, "context-refresh-journal.json")).mode & 0o777).toBe(0o600);

    journal = new FileContextRefreshJournal(temporary, actor, launchId); // simulated helper restart
    enrolled = { ...grant([target("target-a")]), grantId: "grant-2" };
    const newGrant = await runContextRefreshCycle(input({ journal, transport, operationId: () => { throw new Error("must not replace unresolved grant"); } }));
    expect(newGrant.targets[0]).toMatchObject({ state: "held", reason: "prior-unresolved-binding" });
    expect(stepCalls).toBe(1);
    enrolled = { ...grant([target("target-a")]), targets: [{ ...target("target-a"), generation: "target-a-g2", nativeId: "target-a-native-g2" }] };
    const newGeneration = await runContextRefreshCycle(input({ journal, transport, operationId: () => { throw new Error("must not replace unresolved generation"); } }));
    expect(newGeneration.targets[0]).toMatchObject({ state: "held", reason: "prior-unresolved-binding" });
    expect(stepCalls).toBe(1);
    enrolled = grant([target("target-a")]);
    const second = await runContextRefreshCycle(input({ journal, transport, operationId: () => { throw new Error("must not mint replacement"); } }));
    expect(second.targets[0]).toMatchObject({ operationId: "operation-1", state: "unknown" });
    expect(stepCalls).toBe(1); expect(reconciles).toBe(2);

    completed = true;
    const third = await runContextRefreshCycle(input({ journal, transport, operationId: () => { throw new Error("receipt reconciliation must not schedule another step"); } }));
    expect(third.targets[0]).toMatchObject({ operationId: "operation-1", state: "completed" });
    expect(journal.read()?.entries[0]).toMatchObject({ operationId: sentOperation, phase: "completed" });
    expect(stepCalls).toBe(1); expect(reconciles).toBe(3);
  });

  it("continues an independent target when another target remains UNKNOWN", async () => {
    const journal = new MemoryJournal(); const seen: string[] = [];
    const twoTargets = grant([target("target-a"), target("target-b")]);
    let serial = 0;
    const transport: ContextRefreshTransport = {
      enrollment: async () => ({ state: "ready", pollMs: 120_000, grants: [twoTargets] }),
      step: async (_actor, selection) => { seen.push(selection.nodeId); },
      reconcile: async (_actor, selection) => ({ grantId: "grant-1", nodeId: selection.nodeId,
        invocation: selection.nodeId === "target-a" ? { operationId: selection.operationId!, state: "in-flight" } : { operationId: selection.operationId!, state: "completed" }, attempt: null, checkpointRequest: null }),
      status: async (_actor, selection) => ({ grantId: "grant-1", nodeId: selection.nodeId,
        invocation: selection.nodeId === "target-a" ? { operationId: selection.operationId!, state: "in-flight" } : { operationId: selection.operationId!, state: "completed" }, attempt: null, checkpointRequest: null }),
    };
    const result = await runContextRefreshCycle(input({ journal, transport, operationId: () => `operation-${++serial}` }));
    expect(seen).toEqual(["target-a", "target-b"]);
    expect(result.targets.map(row => [row.nodeId, row.state])).toEqual([["target-a", "unknown"], ["target-b", "completed"]]);
    expect(journal.read()?.entries.map(row => row.phase)).toEqual(["unknown", "completed"]);
  });
});
