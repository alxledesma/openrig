import { describe, expect, it } from "vitest";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { createHash } from "node:crypto";
import { createDb } from "../src/db/connection.js";
import { migrate } from "../src/db/migrate.js";
import { nativeDutySupervisionSchema } from "../src/db/migrations/107_native_duty_supervision.js";
import { NativeDutySupervisionService, type NativeDutyOperationReceipt } from "../src/domain/native-duty-supervision-service.js";
import { nativeDutySupervisionRoutes } from "../src/routes/native-duty-supervision.js";
import {
  FileDutyJournal, HolderContinuationExecutor, superviseNativeHarness,
  resolveNativeDutyRegistration, NativeDutyObservationError, NativeDutyInvalidObservationError, NativeDutyTemporaryHoldError, waitNativeDutyObservation,
  inheritedNativeDutyTransport,
  type DutyChild, type DutyClock, type DutyProcesses, type HolderObservation,
  type NativeDutyLaunchConfig, type NativeDutyTransport, NATIVE_DUTY_TRANSPORT_BUDGET_MS,
} from "../src/adapters/native-duty-supervisor.js";
import type { NativeDutyResumeRequest, NativeDutyStatus, NativeDutyScope } from "../src/domain/native-duty-contract.js";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(r => { resolve = r; });
  return { promise, resolve };
}
function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value !== null && typeof value === "object") {
    const o = value as Record<string, unknown>;
    return `{${Object.keys(o).sort().map(k => `${JSON.stringify(k)}:${canonical(o[k])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}
const sha = (value: unknown) => createHash("sha256").update(canonical(value)).digest("hex");
describe("launch-bound holder continuation", () => {
  it("uses real authenticated route intent responses and a conservative scope window; false send permission never posts", async () => {
    const db = createDb(); migrate(db, [nativeDutySupervisionSchema]);
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "native-duty-wire-")); fs.chmodSync(dir, 0o700);
    try {
      let now = 10000, posts = 0, readAt = 0, raceAtInFlight = false;
      const actor = { session: "lead@rig", generation: "native-generation" };
      const operator = { session: "operator-agent@kernel", generation: "operator-generation" };
      const scope: NativeDutyScope = { scopeId: "wire-scope", nodeId: "node", sessionName: actor.session,
        generation: actor.generation, runtime: "pi", rigId: "rig", configurationDigest: "config",
        validUntil: now + NATIVE_DUTY_TRANSPORT_BUDGET_MS + 5000, maxLeaseMs: 30000, kind: "holder-continuation" };
      const shown: HolderObservation = { authority: { rig_id: "rig", owner_session: actor.session,
        owner_generation: actor.generation, epoch: 3, lease_until: now + 2000, state: "active", baton_id: "baton" },
        obligationsDigest: "b".repeat(64), obligations: [{ openQueue: [{ qitem_id: "baton", destination_session: actor.session,
          state: "in-progress", claimed_by_generation_uuid: actor.generation }] }] };
      const receipts = new Map<string, NativeDutyOperationReceipt>();
      const service = new NativeDutySupervisionService({ db, now: () => now,
        approvedScope: id => id === scope.scopeId ? scope : null,
        assertCurrentOperator: actual => { expect(actual).toEqual(operator); },
        observeNative: (s, launchId, supervisorPid) => ({ nodeId: s.nodeId, sessionName: actor.session,
          generation: actor.generation, runtime: s.runtime, launchId, supervisorPid,
          configurationDigest: s.configurationDigest, fingerprint: launchId, observedAt: now,
          nativePresent: true, supervisorIsNativeAncestor: true, lifecycleReserved: false }),
        assertResumeAuthority: (s, actual, request) => {
          expect(actual).toEqual(actor); expect(request.rigId).toBe(s.rigId);
          expect(request.expectedEpoch).toBe(shown.authority.epoch);
          expect(request.expectedObligationsDigest).toBe(shown.obligationsDigest);
          expect(shown.authority.lease_until).toBeGreaterThan(now);
          expect(request.leaseMs).toBeLessThanOrEqual(s.validUntil - now);
        }, operationReceipt: (_rig, operationId) => receipts.get(operationId) ?? null,
      });
      const app = nativeDutySupervisionRoutes({ bearerToken: "fixture-auth", service });
      const calls: string[] = [];
      async function call<T>(route: string, body?: unknown, as = actor): Promise<T> {
        calls.push(route); now++; // Real nonzero elapsed time used to reject exact-remaining requests.
        const response = await app.request(route, { method: body === undefined ? "GET" : "POST",
          headers: { Authorization: "Bearer fixture-auth", "X-OpenRig-Session": as.session,
            "X-OpenRig-Occupant-Generation": as.generation, "Content-Type": "application/json" },
          ...(body === undefined ? {} : { body: JSON.stringify(body) }) });
        expect(response.status).toBeLessThan(400);
        return await response.json() as T;
      }
      await call("/grant", scope, operator);
      let activeJournal: FileDutyJournal;
      const transport: NativeDutyTransport = {
        enrollment: async () => ({ state: "ready" }),
        register: request => call("/register", request), status: id => call(`/status/${id}`),
        heartbeat: registrationId => call("/heartbeat", { registrationId }),
        show: async () => { now++; readAt = now; return structuredClone(shown); },
        prepare: async (registrationId, request) => {
          expect(activeJournal.read()?.request).toEqual(request);
          const intent = await call<Awaited<ReturnType<NativeDutyTransport["prepare"]>>>("/prepare", { registrationId, request });
          expect(intent.phase).toBe("prepared"); expect(intent).not.toHaveProperty("scope");
          expect(service.status(registrationId).phase).toBe("watching");
          return intent;
        },
        inFlight: async (registrationId, operationId) => {
          if (raceAtInFlight) await call("/in-flight", { registrationId, operationId });
          return call("/in-flight", { registrationId, operationId });
        },
        resume: async request => {
          now++; posts++;
          expect(request.leaseMs).toBeGreaterThanOrEqual(1000);
          expect(request.leaseMs).toBeLessThan(scope.maxLeaseMs);
          expect(now + request.leaseMs).toBeLessThan(scope.validUntil);
          const receipt = { ...shown.authority, lease_until: now + request.leaseMs, state: "active" };
          shown.authority = receipt;
          receipts.set(request.operationId, { rigId: request.rigId, operationId: request.operationId, kind: "resume-owned",
            requestHash: sha({ actor: actor.session, callerGeneration: actor.generation, input: request }),
            receiptDigest: sha(receipt), receipt });
        },
        reconcile: (registrationId, operationId) => call("/reconcile", { registrationId, operationId }),
        stop: async (registrationId, reason) => { await call("/stop", { registrationId, reason }); },
      };
      const clock: DutyClock = { now: () => now, sleep: async ms => { now += ms; } };
      const register = async (launchId: string) => {
        const privateDir = path.join(dir, launchId); fs.mkdirSync(privateDir, { mode: 0o700 });
        activeJournal = new FileDutyJournal(privateDir);
        return (await transport.register({ scopeId: scope.scopeId, launchId, supervisorPid: 123 })).registrationId;
      };
      const first = await register("first");
      const executor = new HolderContinuationExecutor(transport, activeJournal!, actor, clock, () => true, () => "wire-op");
      expect(await executor.step(first)).toBe("confirmed");
      expect(posts).toBe(1); expect(service.status(first).phase).toBe("watching");
      expect(service.status(first).intent?.phase).toBe("receipt-confirmed");
      const actualRequest = activeJournal!.read()!.request;
      expect(actualRequest.leaseMs).toBe(scope.validUntil - readAt - NATIVE_DUTY_TRANSPORT_BUDGET_MS);
      // New helper reads retained exact intent; no authority POST or replacement.
      const again = new HolderContinuationExecutor(transport, activeJournal!, actor, clock, () => true, () => { throw new Error("no new op"); });
      expect(await again.step(first)).toBe("watching"); expect(posts).toBe(1);

      // Prepare the independent budget fixture BEFORE introducing UNKNOWN
      // node debt. New registration after that debt must remain refused.
      const third = await register("budget-end"), budgetJournal = activeJournal!;
      const second = await register("race"); raceAtInFlight = true;
      shown.authority.lease_until = now + 1000;
      const denied = new HolderContinuationExecutor(transport, activeJournal!, actor, clock, () => true, () => "race-op");
      expect(await denied.step(second)).toBe("held"); expect(posts).toBe(1);
      expect(service.status(second).intent?.phase).toBe("effect-in-flight");
      expect(await denied.step(second)).toBe("held"); expect(posts).toBe(1);
      expect(service.status(second).phase).toBe("held");
      expect(service.status(second).intent?.phase).toBe("uncertainty-held");

      const blocked = await app.request("/register", { method: "POST", headers: {
        Authorization: "Bearer fixture-auth", "X-OpenRig-Session": actor.session,
        "X-OpenRig-Occupant-Generation": actor.generation, "Content-Type": "application/json" },
        body: JSON.stringify({ scopeId: scope.scopeId, launchId: "forbidden-replacement", supervisorPid: 123 }) });
      expect(blocked.status).toBe(409);
      expect((await blocked.json()).error).toBe("native_duty_unresolved_intent");
      activeJournal = budgetJournal;
      now = scope.validUntil - NATIVE_DUTY_TRANSPORT_BUDGET_MS - 999;
      shown.authority.lease_until = now + 1000;
      const tooShort = new HolderContinuationExecutor(transport, activeJournal!, actor, clock, () => true, () => { throw new Error("no sub-minimum lease"); });
      expect(await tooShort.step(third)).toBe("held"); expect(posts).toBe(1);
      expect(service.status(third).intent).toBeNull();
      expect(calls).toEqual(expect.arrayContaining(["/prepare", "/in-flight", "/reconcile", "/heartbeat"]));
    } finally { db.close(); fs.rmSync(dir, { recursive: true, force: true }); }
  });
  it("recovers only the exact receipt after child loss; UNKNOWN/prepared never resend; harness death fences both native stdio shapes", async () => {
    // One lifecycle scenario, injected process/clock/transport. No provider,
    // native seat, DB or runtime effects. The same parent entry hosts either argv.
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "native-duty-test-"));
    fs.chmodSync(dir, 0o700);
    try {
      for (const runtime of ["codex", "pi"] as const) {
        const journalDir = path.join(dir, runtime); fs.mkdirSync(journalDir, { mode: 0o700 });
        const config: NativeDutyLaunchConfig = { scopeId: "scope", launchId: "launch", journalDir,
          harness: runtime === "codex"
            ? { executable: "/paired/node", args: ["/paired/codex.js", "--no-daemon", "-m", "fixture", "resume", "native-thread"], cwd: "/fixture" }
            : { executable: "/paired/node", args: ["/paired/pi-runner.js", "--launch-id", "native-launch", "--session", "/native/history.jsonl"], cwd: "/fixture" }, pollMs: 1000 };
        const actor = { session: "lead@rig", generation: "genuine-generation" };
        let now = 10000, live = true, posts = 0, reconcileCalls = 0, observationCalls = 0;
        let immutableReceipt: NativeDutyResumeRequest | null = null;
        let childCrashed = false, receiptVisible = true;
        const restart = deferred<void>(), secondHelper = deferred<void>(), runningAgain = deferred<void>();
        const clock: DutyClock = { now: () => now, sleep: async (ms, signal) => {
          if (signal?.aborted) throw new Error("stopped");
          if (ms === 500) { await restart.promise; now += ms; }
          else if (ms === 5000) await new Promise<void>(() => {});
          else now += ms;
        } };
        let status: NativeDutyStatus = { registrationId: "registration", scopeDigest: "scope-digest", launchId: "launch",
          scope: { scopeId: "scope", nodeId: "node", sessionName: actor.session, generation: actor.generation,
            runtime, rigId: "rig", configurationDigest: "config", validUntil: 100000, maxLeaseMs: 10000, kind: "holder-continuation" },
          phase: "watching", lastHeartbeatAt: now, observerDeadline: now + 3000, reason: null, intent: null };
        const shown: HolderObservation = { authority: { rig_id: "rig", owner_session: actor.session,
          owner_generation: actor.generation, epoch: 7, lease_until: 14000, state: "active", baton_id: "baton" },
          obligationsDigest: "a".repeat(64), obligations: [{ openQueue: [{ qitem_id: "baton",
            destination_session: actor.session, state: "in-progress", claimed_by_generation_uuid: actor.generation }] }] };
        const clone = () => structuredClone(status);
        const transport: NativeDutyTransport = {
          enrollment: async () => ({ state: "ready", registrationId: "registration" }),
          register: async () => ({ registrationId: status.registrationId }),
          status: async () => clone(), heartbeat: async () => clone(),
          show: async () => { observationCalls++; return structuredClone(shown); },
          prepare: async (id, request) => {
            expect(id).toBe("registration");
            // Proof that private durable producer evidence precedes network.
            expect(new FileDutyJournal(journalDir).read()?.request).toEqual(request);
            status = { ...status, intent: { operationId: request.operationId, request,
              bodyDigest: "server-exact-digest", preparedAt: now, phase: "prepared" } };
            return clone().intent!;
          },
          inFlight: async (_id, op) => {
            expect(status.intent?.operationId).toBe(op);
            expect(new FileDutyJournal(journalDir).read()?.phase).toBe("effect-in-flight");
            status = { ...status, intent: { ...status.intent!, phase: "effect-in-flight" } };
            return { intent: clone().intent!, maySendEffect: true };
          },
          resume: async request => {
            posts++; expect(posts).toBe(1);
            expect(request).toEqual({ rigId: "rig", operationId: "exact-op", leaseMs: 10000,
              expectedEpoch: 7, expectedObligationsDigest: "a".repeat(64) });
            immutableReceipt = structuredClone(request); childCrashed = true;
            throw new Error("private-provider-or-credential-text"); // effect committed; response lost
          },
          reconcile: async (_id, op) => {
            reconcileCalls++;
            if (childCrashed) throw new Error("child lost before receipt read");
            expect(op).toBe(status.intent!.operationId);
            if (!receiptVisible || !immutableReceipt) {
              status = { ...status, phase: "held", intent: { ...status.intent!, phase: "uncertainty-held" } };
              return clone().intent!;
            }
            expect(status.intent!.request).toEqual(immutableReceipt);
            status = { ...status, intent: { ...status.intent!, phase: "receipt-confirmed" } };
            return clone().intent!;
          }, stop: async () => {},
        };
        const harnessExit = deferred<number>();
        const childSignals: string[] = [], spawns: Array<{ executable: string; args: readonly string[]; options: unknown }> = [];
        let helperCount = 0;
        const processes: DutyProcesses = { spawn(executable, args, options): DutyChild {
          spawns.push({ executable, args: [...args], options });
          if (spawns.length === 1) return { exited: harnessExit.promise, stop: signal => { childSignals.push(`harness:${signal}`); } };
          helperCount++;
          const exit = deferred<number>();
          const executor = new HolderContinuationExecutor(transport, new FileDutyJournal(journalDir), actor, clock,
            () => live, () => "exact-op");
          if (helperCount === 1) {
            executor.step("registration").then(() => exit.resolve(0), () => { exit.resolve(1); secondHelper.resolve(); });
          } else {
            childCrashed = false;
            executor.step("registration").then(result => {
              expect(result).toBe("confirmed"); runningAgain.resolve();
            }, () => exit.resolve(1));
          }
          return { exited: exit.promise, stop: signal => { childSignals.push(`helper:${signal}`); exit.resolve(0); } };
        } };
        const parent = superviseNativeHarness(config, { executable: "/paired/node", args: ["/paired/native-duty-supervisor.js", "--helper", "/public/config.json", "123"] }, processes, clock);
        await secondHelper.promise;
        expect(new FileDutyJournal(journalDir).read()?.phase).toBe("effect-in-flight");
        expect(posts).toBe(1); restart.resolve(); await runningAgain.promise;
        expect(new FileDutyJournal(journalDir).read()?.phase).toBe("receipt-confirmed");
        expect(posts).toBe(1); expect(reconcileCalls).toBe(2); expect(observationCalls).toBe(1);
        expect(helperCount).toBe(2);
        expect(spawns[0]).toEqual({ executable: config.harness.executable, args: config.harness.args,
          options: { cwd: config.harness.cwd, stdio: "inherit" } });
        expect(spawns.slice(1).every(s => JSON.stringify(s.options) === '{"stdio":"ignore"}')).toBe(true);
        live = false; harnessExit.resolve(0); expect(await parent).toBe(0);
        expect(childSignals).toContain("helper:SIGTERM"); expect(helperCount).toBe(2);

        // A second interrupted intent with no receipt, including a crash BEFORE
        // POST, is reconciled/held forever, not inferred absent and retried.
        const unknownRequest = { ...immutableReceipt!, operationId: "prepared-never-posted" };
        new FileDutyJournal(journalDir).save({ registrationId: "registration", request: unknownRequest, phase: "prepared" });
        status = { ...status, intent: { ...status.intent!, operationId: unknownRequest.operationId,
          request: unknownRequest, phase: "prepared" } };
        receiptVisible = false; live = true;
        const observer = new HolderContinuationExecutor(transport, new FileDutyJournal(journalDir), actor, clock,
          () => live, () => { throw new Error("must not mint replacement op"); });
        expect(await observer.step("registration")).toBe("held");
        expect(await observer.step("registration")).toBe("held");
        expect(posts).toBe(1); expect(observationCalls).toBe(1);
        expect(new FileDutyJournal(journalDir).read()?.request).toEqual(unknownRequest);
        expect(status.intent?.phase).toBe("uncertainty-held");
        const persisted = fs.readFileSync(path.join(journalDir, "intent.json"), "utf8");
        expect(persisted).not.toMatch(/private-provider|credential|Bearer|Authorization|OPENRIG/);
        expect(fs.statSync(path.join(journalDir, "intent.json")).mode & 0o077).toBe(0);

        // Server remains final proof/authority fence; the producer holds known
        // wrong generation, expired lease and stale canonical claim pre-effect.
        new FileDutyJournal(journalDir).save({ registrationId: "registration", request: unknownRequest, phase: "receipt-confirmed" });
        status = { ...status, phase: "watching", intent: null };
        for (const damage of ["generation", "expiry", "baton"] as const) {
          const saved = structuredClone(shown);
          if (damage === "generation") shown.authority.owner_generation = "other";
          if (damage === "expiry") shown.authority.lease_until = now;
          if (damage === "baton") shown.obligations[0]!.openQueue![0]!.claimed_by_generation_uuid = "other";
          expect(await observer.step("registration")).toBe("held");
          Object.assign(shown, saved); expect(posts).toBe(1);
        }
      }
    } finally { fs.rmSync(dir, { recursive: true, force: true }); }
  });
  it("waits for enrollment, aborts with its parent, and discovers a registration after a lost response", async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "native-duty-enrollment-"));
    fs.chmodSync(dir, 0o700);
    try {
      const journal = new FileDutyJournal(dir);
      const request = { scopeId: "scope", launchId: "launch", supervisorPid: 321 };
      let polls = 0, registers = 0, durableId: string | undefined;
      const transport = {
        enrollment: async () => {
          polls++;
          if (polls === 1) return { state: "waiting" as const };
          return { state: "ready" as const, ...(durableId ? { registrationId: durableId } : {}) };
        },
        register: async () => {
          registers++; durableId = "durable-registration";
          throw new Error("lost-response");
        },
      } as NativeDutyTransport;
      const clock: DutyClock = { now: () => 0, sleep: async () => {} };
      await expect(resolveNativeDutyRegistration(transport, journal, request, clock, 1000, () => true))
        .rejects.toThrow("lost-response");
      // The lost response is observed by the next helper invocation as the
      // exact durable ID before any second register request can be sent.
      expect(await resolveNativeDutyRegistration(transport, journal, request, clock, 1000, () => true))
        .toBe("durable-registration");
      expect(registers).toBe(1);

      let wake!: () => void;
      const sleeping = new Promise<void>(resolve => { wake = resolve; });
      const abort = new AbortController();
      const waitingTransport = { ...transport,
        enrollment: async () => ({ state: "waiting" as const }),
        register: async () => { throw new Error("must-not-register"); },
      } as NativeDutyTransport;
      const waitingJournal = path.join(dir, "waiting"); fs.mkdirSync(waitingJournal, { mode: 0o700 });
      const abortClock: DutyClock = { now: () => 0, sleep: async (_ms, signal) => {
        if (signal?.aborted) throw new Error("native-duty-parent-stopped");
        await sleeping;
        if (signal?.aborted) throw new Error("native-duty-parent-stopped");
      } };
      const waiting = resolveNativeDutyRegistration(waitingTransport, new FileDutyJournal(waitingJournal), request, abortClock, 1000,
        () => !abort.signal.aborted, abort.signal);
      await Promise.resolve();
      abort.abort(); wake();
      await expect(waiting).rejects.toThrow("native-duty-parent-stopped");
      expect(registers).toBe(1);
    } finally { fs.rmSync(dir, { recursive: true, force: true }); }
  });
  it("uses inherited auth or only the exact private OPENRIG_HOME token file without writing credentials", async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "native-duty-auth-"));
    fs.chmodSync(dir, 0o700);
    const home = path.join(dir, "openrig"), fallbackHome = path.join(dir, "fallback", ".openrig");
    fs.mkdirSync(home, { recursive: true, mode: 0o700 }); fs.chmodSync(home, 0o700);
    fs.mkdirSync(fallbackHome, { recursive: true, mode: 0o700 }); fs.chmodSync(fallbackHome, 0o700);
    const tokenPath = path.join(home, "terminal-token"), fallbackToken = path.join(fallbackHome, "terminal-token");
    fs.writeFileSync(tokenPath, "private-file-token\n", { mode: 0o600 }); fs.chmodSync(tokenPath, 0o600);
    fs.writeFileSync(fallbackToken, "must-not-fallback\n", { mode: 0o600 }); fs.chmodSync(fallbackToken, 0o600);
    const keys = ["OPENRIG_SESSION_NAME", "OPENRIG_OCCUPANT_GENERATION", "OPENRIG_TERMINAL_BEARER_TOKEN",
      "OPENRIG_URL", "OPENRIG_HOME", "HOME"] as const;
    const previous = new Map(keys.map(key => [key, process.env[key]]));
    const oldFetch = globalThis.fetch;
    const authorization: Array<string | null> = [];
    globalThis.fetch = (async (_input, init) => {
      authorization.push(new Headers(init?.headers).get("Authorization"));
      return new Response(JSON.stringify({ state: "waiting" }), { status: 200,
        headers: { "Content-Type": "application/json" } });
    }) as typeof fetch;
    const useEnv = (values: Record<string, string | undefined>) => {
      for (const key of keys) {
        const value = values[key];
        if (value === undefined) delete process.env[key]; else process.env[key] = value;
      }
    };
    try {
      const base = { OPENRIG_SESSION_NAME: "native@rig", OPENRIG_OCCUPANT_GENERATION: "genuine-generation",
        OPENRIG_URL: "http://127.0.0.1:12345", HOME: dir };
      const fileBefore = fs.statSync(tokenPath), entriesBefore = fs.readdirSync(home);
      useEnv({ ...base, OPENRIG_HOME: home, OPENRIG_TERMINAL_BEARER_TOKEN: "inherited-token" });
      await inheritedNativeDutyTransport().transport.enrollment({ scopeId: "scope", launchId: "launch", supervisorPid: 1 });
      expect(authorization.at(-1)).toBe("Bearer inherited-token");

      useEnv({ ...base, OPENRIG_HOME: home });
      await inheritedNativeDutyTransport().transport.enrollment({ scopeId: "scope", launchId: "launch", supervisorPid: 1 });
      expect(authorization.at(-1)).toBe("Bearer private-file-token");
      const fileAfter = fs.statSync(tokenPath);
      expect({ ino: fileAfter.ino, size: fileAfter.size, mtimeMs: fileAfter.mtimeMs })
        .toEqual({ ino: fileBefore.ino, size: fileBefore.size, mtimeMs: fileBefore.mtimeMs });
      expect(fs.readdirSync(home)).toEqual(entriesBefore);

      fs.chmodSync(tokenPath, 0o644);
      expect(() => inheritedNativeDutyTransport()).toThrow("native-duty-auth-required");
      fs.chmodSync(tokenPath, 0o600);
      const link = path.join(home, "terminal-token-link"); fs.symlinkSync(tokenPath, link);
      fs.unlinkSync(tokenPath); fs.symlinkSync(fallbackToken, tokenPath);
      expect(() => inheritedNativeDutyTransport()).toThrow("native-duty-auth-required");
      fs.unlinkSync(tokenPath); fs.writeFileSync(tokenPath, "private-file-token\n", { mode: 0o600 }); fs.chmodSync(tokenPath, 0o600);

      useEnv({ ...base, OPENRIG_HOME: path.join(dir, "missing") });
      expect(() => inheritedNativeDutyTransport()).toThrow("native-duty-auth-required");
      expect(authorization).toEqual(["Bearer inherited-token", "Bearer private-file-token"]);
    } finally {
      globalThis.fetch = oldFetch;
      for (const [key, value] of previous) {
        if (value === undefined) delete process.env[key]; else process.env[key] = value;
      }
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

it('keeps one enrollment resolver observational beyond restart budget and requires fresh ready',async()=>{
 let polls=0,posts=0,slept=0;
 const transport={enrollment:async()=>{polls++;if(polls<=3)throw Error('GET unavailable');if(polls<=8)return {state:'held'};return {state:'ready'};},register:async()=>{posts++;return {registrationId:'exact-id'};}} as NativeDutyTransport;
 const journal={read:()=>null,save:()=>{throw Error('no effect journal expected');}};
 const clock={now:()=>slept,sleep:async(ms:number)=>{expect(posts).toBe(0);slept+=ms;}};
 expect(await resolveNativeDutyRegistration(transport,journal,{scopeId:'scope',launchId:'launch',supervisorPid:321},clock,5000,()=>true)).toBe('exact-id');
 expect(posts).toBe(1);expect(polls).toBe(9);expect(slept).toBe(40000);
});
it('held expiry/revocation never authorizes registration and parent stop ends observation',async()=>{
 let live=true,polls=0,posts=0;
 const transport={enrollment:async()=>{polls++;return {state:'held'};},register:async()=>{posts++;throw Error('forbidden');}} as NativeDutyTransport;
 await expect(resolveNativeDutyRegistration(transport,{read:()=>null,save:()=>{}},{scopeId:'scope',launchId:'launch',supervisorPid:321},
 {now:()=>0,sleep:async()=>{if(polls===6)live=false;}},1000,()=>live)).rejects.toThrow('parent-stopped');
 expect(polls).toBe(6);expect(posts).toBe(0);
});
it('malformed enrollment remains terminal and register UNKNOWN is not observational retry',async()=>{
 const request={scopeId:'scope',launchId:'launch',supervisorPid:321},journal={read:()=>null,save:()=>{}},clock={now:()=>0,sleep:async()=>{throw Error('must not retry');}};
 await expect(resolveNativeDutyRegistration({enrollment:async()=>({state:'invalid'})} as unknown as NativeDutyTransport,journal,request,clock,1000,()=>true)).rejects.toThrow('invalid-enrollment');
 let posts=0;
 await expect(resolveNativeDutyRegistration({enrollment:async()=>({state:'ready'}),register:async()=>{posts++;throw Error('UNKNOWN register');}} as NativeDutyTransport,journal,request,clock,1000,()=>true)).rejects.toThrow('UNKNOWN register');
 expect(posts).toBe(1);
});
it('retries typed read-only observation errors, never mutation errors, and obeys parent abort',async()=>{
 let reads=0,sleeps=0;
 const clock={now:()=>0,sleep:async()=>{sleeps++;}};
 expect(await waitNativeDutyObservation(async()=>{if(++reads<=5)throw new NativeDutyObservationError();return 'fresh';},clock,1000,()=>true)).toBe('fresh');
 expect(reads).toBe(6);expect(sleeps).toBe(5);
 let mutations=0;
 await expect(waitNativeDutyObservation(async()=>{mutations++;throw Error('native-duty-transport-unresolved');},clock,1000,()=>true)).rejects.toThrow('transport-unresolved');
 expect(mutations).toBe(1);expect(sleeps).toBe(5);
 const abort=new AbortController();
 await expect(waitNativeDutyObservation(async()=>{throw new NativeDutyObservationError();},{now:()=>0,sleep:async()=>{abort.abort();}},1000,()=>true,abort.signal)).rejects.toThrow('parent-stopped');
});
it('wire classifies failed GET only; failed POST remains mutation UNKNOWN',async()=>{
 const values={OPENRIG_SESSION_NAME:'test@rig',OPENRIG_OCCUPANT_GENERATION:'test-gen',OPENRIG_URL:'http://127.0.0.1:12345',OPENRIG_TERMINAL_BEARER_TOKEN:'test-only-token'};
 const previous=new Map(Object.keys(values).map(k=>[k,process.env[k]]));const oldFetch=globalThis.fetch;const methods:string[]=[];
 try{
 Object.assign(process.env,values);globalThis.fetch=(async(_input,init)=>{methods.push(init?.method??'GET');throw Error('synthetic network failure');}) as typeof fetch;
 const {transport}=inheritedNativeDutyTransport();
 await expect(transport.status('exact-id')).rejects.toBeInstanceOf(NativeDutyObservationError);
 await expect(transport.show('rig')).rejects.toBeInstanceOf(NativeDutyObservationError);
 await expect(transport.register({scopeId:'scope',launchId:'launch',supervisorPid:321})).rejects.not.toBeInstanceOf(NativeDutyObservationError);
 await expect(transport.heartbeat('exact-id')).rejects.not.toBeInstanceOf(NativeDutyObservationError);
 expect(methods).toEqual(['GET','GET','POST','POST']);
 }finally{globalThis.fetch=oldFetch;for(const[k,v]of previous){if(v===undefined)delete process.env[k];else process.env[k]=v;}}
});

it('successful malformed GET JSON is terminal, while POST malformed JSON remains UNKNOWN',async()=>{
 const values={OPENRIG_SESSION_NAME:'test@rig',OPENRIG_OCCUPANT_GENERATION:'test-gen',OPENRIG_URL:'http://127.0.0.1:12345',OPENRIG_TERMINAL_BEARER_TOKEN:'test-only-token'};
 const previous=new Map(Object.keys(values).map(k=>[k,process.env[k]]));const oldFetch=globalThis.fetch;let calls=0;
 try{
 Object.assign(process.env,values);globalThis.fetch=(async()=>{calls++;return new Response('{invalid',{status:200});}) as typeof fetch;
 const {transport}=inheritedNativeDutyTransport();
 await expect(resolveNativeDutyRegistration(transport,{read:()=>null,save:()=>{}},{scopeId:'scope',launchId:'launch',supervisorPid:321},
 {now:()=>0,sleep:async()=>{throw Error('must not poll malformed JSON');}},1000,()=>true)).rejects.toBeInstanceOf(NativeDutyInvalidObservationError);
 expect(calls).toBe(1);
 await expect(waitNativeDutyObservation(()=>transport.status('exact-id'),{now:()=>0,sleep:async()=>{throw Error('must not retry');}},1000,()=>true)).rejects.toBeInstanceOf(NativeDutyInvalidObservationError);
 await expect(transport.register({scopeId:'scope',launchId:'launch',supervisorPid:321})).rejects.toThrow('native-duty-transport-unresolved');
 expect(calls).toBe(3);
 }finally{globalThis.fetch=oldFetch;for(const[k,v]of previous){if(v===undefined)delete process.env[k];else process.env[k]=v;}}
});

it('only exact temporary heartbeat refusal is observational; other POST failures remain UNKNOWN',async()=>{
 const values={OPENRIG_SESSION_NAME:'test@rig',OPENRIG_OCCUPANT_GENERATION:'test-gen',OPENRIG_URL:'http://127.0.0.1:12345',OPENRIG_TERMINAL_BEARER_TOKEN:'test-only-token'};
 const previous=new Map(Object.keys(values).map(k=>[k,process.env[k]])),oldFetch=globalThis.fetch;
 let error='native_duty_temporary_exclusion',status=409;
 try{
  Object.assign(process.env,values);globalThis.fetch=(async()=>new Response(JSON.stringify({error}),{status})) as typeof fetch;
  const {transport}=inheritedNativeDutyTransport();
  for(error of ['native_duty_temporary_exclusion','native_duty_proof_unavailable']){
   status=409;
   await expect(transport.heartbeat('exact-id')).rejects.toBeInstanceOf(NativeDutyTemporaryHoldError);
   await expect(transport.register({scopeId:'scope',launchId:'launch',supervisorPid:321})).rejects.toThrow('native-duty-transport-unresolved');
   await expect(transport.inFlight('exact-id','op')).rejects.toThrow('native-duty-transport-unresolved');
   status=500;await expect(transport.heartbeat('exact-id')).rejects.toThrow('native-duty-transport-unresolved');
  }
  status=409;error='native_duty_proof_mismatch';await expect(transport.heartbeat('exact-id')).rejects.toThrow('native-duty-transport-unresolved');
  for(const body of ['{invalid','null','[]',JSON.stringify('native_duty_proof_unavailable')]){
   globalThis.fetch=(async()=>new Response(body,{status:409})) as typeof fetch;
   await expect(transport.heartbeat('exact-id')).rejects.toThrow('native-duty-transport-unresolved');
  }
 }finally{globalThis.fetch=oldFetch;for(const[k,v]of previous){if(v===undefined)delete process.env[k];else process.env[k]=v;}}
});

// Actual inherited wire classification feeds the existing executor; all effects
// below are injected. Neither an observational hold nor later proof replays debt.
it.each([1,2])('unavailable heartbeat at observation %i holds; fresh same registration continues without unresolved replay',async(holdAt)=>{
 const values={OPENRIG_SESSION_NAME:'lead@rig',OPENRIG_OCCUPANT_GENERATION:'genuine-generation',OPENRIG_URL:'http://127.0.0.1:12345',OPENRIG_TERMINAL_BEARER_TOKEN:'test-only-token'};
 const previous=new Map(Object.keys(values).map(k=>[k,process.env[k]])),oldFetch=globalThis.fetch;
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'native-duty-observation-'));fs.chmodSync(dir,0o700);
 const actor={session:values.OPENRIG_SESSION_NAME,generation:values.OPENRIG_OCCUPANT_GENERATION};
 let status:NativeDutyStatus={registrationId:'registration',scopeDigest:'scope-digest',launchId:'launch',
  scope:{scopeId:'scope',nodeId:'node',sessionName:actor.session,generation:actor.generation,runtime:'codex',rigId:'rig',configurationDigest:'config',validUntil:100000,maxLeaseMs:10000,kind:'holder-continuation'},
  phase:'watching',lastHeartbeatAt:10000,observerDeadline:13000,reason:null,intent:null};
 let heartbeats=0,resumes=0,prepares=0,reconciles=0;
 const journal=new FileDutyJournal(dir);
 try{
  Object.assign(process.env,values);
  globalThis.fetch=(async(_input,init)=>{
   expect(JSON.parse(String(init?.body))).toEqual({registrationId:'registration'});
   if(++heartbeats===holdAt)return new Response(JSON.stringify({error:'native_duty_proof_unavailable'}),{status:409});
   return new Response(JSON.stringify(status),{status:200});
  }) as typeof fetch;
  const wire=inheritedNativeDutyTransport();
  const transport:NativeDutyTransport={...wire.transport,
   status:async(id)=>{expect(id).toBe('registration');return structuredClone(status);},
   register:async()=>{throw Error('must not re-register');},
   show:async()=>({authority:{rig_id:'rig',owner_session:actor.session,owner_generation:actor.generation,epoch:7,lease_until:14000,state:'active',baton_id:'baton'},obligationsDigest:'a'.repeat(64),obligations:[{openQueue:[{qitem_id:'baton',destination_session:actor.session,state:'in-progress',claimed_by_generation_uuid:actor.generation}]}]}),
   prepare:async(id,request)=>{prepares++;expect(id).toBe('registration');expect(journal.read()?.request).toEqual(request);status.intent={operationId:request.operationId,request,bodyDigest:'exact-digest',preparedAt:10000,phase:'prepared'};return structuredClone(status.intent);},
   inFlight:async(id,op)=>{expect(id).toBe('registration');expect(op).toBe('exact-op');expect(journal.read()?.phase).toBe('effect-in-flight');status.intent={...status.intent!,phase:'effect-in-flight'};return {intent:structuredClone(status.intent),maySendEffect:true};},
   resume:async()=>{resumes++;},
   reconcile:async(id,op)=>{reconciles++;expect(id).toBe('registration');expect(op).toBe('exact-op');return {...status.intent!,phase:resumes===1?'receipt-confirmed':'uncertainty-held'};},
  };
  const executor=new HolderContinuationExecutor(transport,journal,actor,{now:()=>10000,sleep:async()=>{}},()=>true,()=> 'exact-op');
  expect(await executor.step('registration')).toBe('held');expect(resumes).toBe(0);
  expect(status.phase).toBe('watching');expect(status.scope.validUntil).toBe(100000);
  if(holdAt===1){
   expect(journal.read()).toBeNull();expect(prepares).toBe(0);
   expect(await executor.step('registration')).toBe('confirmed');expect(resumes).toBe(1);expect(prepares).toBe(1);expect(heartbeats).toBe(3);
  }else{
   const held=journal.read();expect(held?.phase).toBe('effect-in-flight');
   expect(await executor.step('registration')).toBe('held');expect(journal.read()).toEqual(held);
   expect(resumes).toBe(0);expect(prepares).toBe(1);expect(heartbeats).toBe(2);expect(reconciles).toBe(1);
   // A fresh helper with no local journal must also reconcile registry debt.
   const freshDir=path.join(dir,'fresh');fs.mkdirSync(freshDir,{mode:0o700});
   const fresh=new HolderContinuationExecutor(transport,new FileDutyJournal(freshDir),actor,{now:()=>10000,sleep:async()=>{}},()=>true,()=> 'must-not-create');
   expect(await fresh.step('registration')).toBe('held');expect(resumes).toBe(0);expect(prepares).toBe(1);expect(heartbeats).toBe(2);expect(reconciles).toBe(2);
  }
 }finally{globalThis.fetch=oldFetch;for(const[k,v]of previous){if(v===undefined)delete process.env[k];else process.env[k]=v;}fs.rmSync(dir,{recursive:true,force:true});}
});
