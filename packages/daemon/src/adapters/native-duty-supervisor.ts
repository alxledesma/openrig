import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { FileContextRefreshJournal, launchContextRefreshLoop,
  type ContextRefreshTransport } from "./context-refresh-executor.js";
import {
  NATIVE_DUTY_API, nativeDutyLeaseMs, nativeDutyUnresolved,
  type NativeDutyActor, type NativeDutyRegisterRequest, type NativeDutyResumeRequest,
  type NativeDutyStatus, type NativeDutyIntent,
} from "../domain/native-duty-contract.js";

/** No environment, bearer, actor override or shell command is accepted in config.
 * Root's launch integration supplies the already composed native argv and opts in.
 * The helper may use inherited auth or the existing explicit OPENRIG_HOME token
 * file; it never discovers a default home or changes an existing seat.
 */
export interface NativeDutyLaunchConfig {
  scopeId: string;
  launchId: string;
  journalDir: string;
  harness: { executable: string; args: string[]; cwd: string };
  pollMs: number;
}
export interface DutyClock {
  now(): number;
  sleep(ms: number, signal?: AbortSignal): Promise<void>;
}
export interface NativeDutyTransport {
  enrollment(request: NativeDutyEnrollmentRequest): Promise<NativeDutyEnrollment>;
  register(request: NativeDutyRegisterRequest): Promise<{ registrationId: string }>;
  status(registrationId: string): Promise<NativeDutyStatus>;
  heartbeat(registrationId: string): Promise<NativeDutyStatus>;
  show(rigId: string): Promise<HolderObservation>;
  prepare(registrationId: string, request: NativeDutyResumeRequest): Promise<NativeDutyIntent>;
  inFlight(registrationId: string, operationId: string): Promise<{ intent: NativeDutyIntent; maySendEffect: boolean }>;
  resume(request: NativeDutyResumeRequest): Promise<void>;
  reconcile(registrationId: string, operationId: string): Promise<NativeDutyIntent>;
  stop(registrationId: string, reason: string): Promise<void>;
}

export interface NativeDutyEnrollmentRequest {
  scopeId: string;
  launchId: string;
  supervisorPid: number;
}
export type NativeDutyEnrollment = { state: "waiting" | "held"; registrationId?: never }
  | { state: "ready"; registrationId?: string };

/** Resolve enrollment through the read-only observer before attempting the
 * durable register. A lost register response is resolved by a fresh observer
 * read on the next helper invocation, never by replaying the POST blindly.
 */
export async function resolveNativeDutyRegistration(transport: NativeDutyTransport,
  journal: DutyJournal, request: NativeDutyEnrollmentRequest, clock: DutyClock,
  pollMs: number, live: () => boolean, signal?: AbortSignal): Promise<string> {
  if (!request.scopeId || !request.launchId || !Number.isSafeInteger(request.supervisorPid)
    || request.supervisorPid < 1 || !Number.isSafeInteger(pollMs) || pollMs < 1000 || pollMs > 60000)
    throw new Error("native-duty-invalid-enrollment-request");
  for (;;) {
    if (!live() || signal?.aborted) throw new Error("native-duty-parent-stopped");
    let state: NativeDutyEnrollment;
    try { state = await transport.enrollment(request); }
    catch (error) {
      if (error instanceof NativeDutyInvalidObservationError) throw error;
      // This catch covers ONLY the read-only enrollment GET, never register.
      if (!live() || signal?.aborted) throw new Error("native-duty-parent-stopped");
      await clock.sleep(pollMs, signal);
      continue;
    }
    if (!state || !["waiting", "held", "ready"].includes(state.state)
      || ((state.state === "waiting" || state.state === "held") && state.registrationId !== undefined))
      throw new Error("native-duty-invalid-enrollment-response");
    if (!live() || signal?.aborted) throw new Error("native-duty-parent-stopped");
    if (state.state === "waiting" || state.state === "held") {
      await clock.sleep(pollMs, signal);
      continue;
    }
    if (state.registrationId !== undefined) {
      if (typeof state.registrationId !== "string" || !state.registrationId) throw new Error("native-duty-invalid-registration");
      const local = journal.read();
      if (local && local.registrationId !== state.registrationId) throw new Error("native-duty-registration-mismatch");
      return state.registrationId;
    }
    // A local intent without its server registration cannot be safely rebound.
    if (journal.read()) throw new Error("native-duty-registration-missing");
    if (!live() || signal?.aborted) throw new Error("native-duty-parent-stopped");
    const enrolled = await transport.register(request);
    if (!enrolled || typeof enrolled.registrationId !== "string" || !enrolled.registrationId)
      throw new Error("native-duty-invalid-registration");
    const local = journal.read();
    if (local && local.registrationId !== enrolled.registrationId) throw new Error("native-duty-registration-mismatch");
    return enrolled.registrationId;
  }
}

/** Local transport classification: only GET failures are observational. */
export class NativeDutyInvalidObservationError extends Error {
  constructor() { super("native-duty-invalid-observation-response"); }
}
/** Only the exact server heartbeat refusal, never an arbitrary POST failure. */
export class NativeDutyTemporaryHoldError extends Error {
  constructor() { super("native-duty-temporary-exclusion"); }
}
export class NativeDutyObservationError extends Error {
  constructor() { super("native-duty-observation-unresolved"); }
}
/** Retry a read, never the executor or any mutation. Parent lifetime bounds it. */
export async function waitNativeDutyObservation<T>(read: () => Promise<T>, clock: DutyClock,
  pollMs: number, live: () => boolean, signal?: AbortSignal): Promise<T> {
  for (;;) {
    if (!live() || signal?.aborted) throw new Error("native-duty-parent-stopped");
    try {
      const result = await read();
      if (!live() || signal?.aborted) throw new Error("native-duty-parent-stopped");
      return result;
    } catch (error) {
      if (!(error instanceof NativeDutyObservationError)) throw error;
      if (!live() || signal?.aborted) throw new Error("native-duty-parent-stopped");
      await clock.sleep(pollMs, signal);
    }
  }
}

// Reserve four bounded transport calls AFTER show: prepare, in-flight,
// current heartbeat and the effect. A lease never uses the exact remaining
// scope window and cannot extend it. Unknown delivery still holds, never retries.
export const NATIVE_DUTY_REQUEST_TIMEOUT_MS = 5000;
export const NATIVE_DUTY_TRANSPORT_BUDGET_MS = 4 * NATIVE_DUTY_REQUEST_TIMEOUT_MS;
export interface HolderObservation {
  authority: { rig_id: string; owner_session: string; owner_generation: string;
    epoch: number; lease_until: number; state: string; baton_id: string };
  obligationsDigest: string;
  obligations: Array<{ openQueue?: Array<{ qitem_id: string; destination_session: string;
    state: string; claimed_by_generation_uuid: string | null }> }>;
}
/** Journal holds only public operation data. Save must be durable before returning.
 * A prepared entry is never permission to submit on a subsequent invocation.
 */
export interface DutyJournalEntry {
  registrationId: string;
  request: NativeDutyResumeRequest;
  phase: "prepared" | "effect-in-flight" | "receipt-confirmed";
}
export interface DutyJournal {
  read(): DutyJournalEntry | null;
  save(entry: DutyJournalEntry): void;
}
function sameRequest(a: NativeDutyResumeRequest, b: NativeDutyResumeRequest): boolean {
  return a.rigId === b.rigId && a.operationId === b.operationId && a.leaseMs === b.leaseMs
    && a.expectedEpoch === b.expectedEpoch && a.expectedObligationsDigest === b.expectedObligationsDigest;
}
function assertRequest(r: NativeDutyResumeRequest): void {
  if (!r || Object.keys(r).sort().join(",") !== "expectedEpoch,expectedObligationsDigest,leaseMs,operationId,rigId"
    || typeof r.rigId !== "string" || !r.rigId || typeof r.operationId !== "string" || !r.operationId
    || r.operationId.length > 160 || !Number.isSafeInteger(r.leaseMs) || r.leaseMs < 1000
    || r.leaseMs > 3600000 || !Number.isSafeInteger(r.expectedEpoch) || r.expectedEpoch < 1
    || typeof r.expectedObligationsDigest !== "string" || !/^[a-f0-9]{64}$/.test(r.expectedObligationsDigest)) {
    throw new Error("native-duty-invalid-request");
  }
}

export class HolderContinuationExecutor {
  constructor(private readonly transport: NativeDutyTransport, private readonly journal: DutyJournal,
    private readonly actor: NativeDutyActor, private readonly clock: DutyClock,
    private readonly live: () => boolean, private readonly operationId: () => string = randomUUID) {}

  private async observeHeartbeat(registrationId: string): Promise<NativeDutyStatus | null> {
    try { return await this.transport.heartbeat(registrationId); }
    catch (error) { if (error instanceof NativeDutyTemporaryHoldError) return null; throw error; }
  }
  private allowed(status: NativeDutyStatus): boolean {
    const s = status.scope;
    return this.live() && s.kind === "holder-continuation" && s.sessionName === this.actor.session
      && s.generation === this.actor.generation && nativeDutyLeaseMs(s, this.clock.now()) >= 1000
      && status.phase === "watching";
  }
  private async reconcile(entry: DutyJournalEntry): Promise<"confirmed" | "held"> {
    // Only the server reads and validates the immutable authority receipt. Neither
    // a missing receipt nor an unrecorded registry intent authorizes another POST.
    const intent = await this.transport.reconcile(entry.registrationId, entry.request.operationId);
    if (intent.phase !== "receipt-confirmed" || intent.operationId !== entry.request.operationId
      || !sameRequest(intent.request, entry.request)) return "held";
    this.journal.save({ ...entry, phase: "receipt-confirmed" });
    return "confirmed";
  }

  /** One scheduling observation and at most one effect. Restart never resends an
   * unresolved local OR registry intent, including a crash before the first POST.
   */
  async step(registrationId: string): Promise<"confirmed" | "watching" | "held" | "stopped"> {
    const local = this.journal.read();
    if (local && local.registrationId !== registrationId) return "held";
    if (local && local.phase !== "receipt-confirmed") return this.reconcile(local);
    let status = await this.transport.status(registrationId);
    if (status.registrationId !== registrationId) return "held";
    if (nativeDutyUnresolved(status.intent)) {
      const entry: DutyJournalEntry = { registrationId, request: status.intent!.request,
        phase: status.intent!.phase === "prepared" ? "prepared" : "effect-in-flight" };
      assertRequest(entry.request);
      this.journal.save(entry);
      return this.reconcile(entry);
    }
    if (!this.live() || status.phase === "stopped") return "stopped";
    // Heartbeat is independent native/config/lifecycle revalidation, not a lease.
    const heartbeat = await this.observeHeartbeat(registrationId);
    if (!heartbeat) return "held";
    status = heartbeat;
    if (!this.allowed(status) || status.registrationId !== registrationId) return "held";
    const shown = await this.transport.show(status.scope.rigId);
    const a = shown.authority, now = this.clock.now();
    const leaseMs = Math.max(0, Math.min(3600000, status.scope.maxLeaseMs,
      nativeDutyLeaseMs({ ...status.scope, maxLeaseMs: Number.MAX_SAFE_INTEGER }, now) - NATIVE_DUTY_TRANSPORT_BUDGET_MS));
    const baton = shown.obligations.flatMap(o => o.openQueue ?? []).find(q => q.qitem_id === a.baton_id);
    if (!this.live() || leaseMs < 1000 || a.rig_id !== status.scope.rigId
      || a.owner_session !== this.actor.session || a.owner_generation !== this.actor.generation
      || !["active", "reconciling"].includes(a.state) || !Number.isSafeInteger(a.lease_until)
      || a.lease_until <= now || !baton || baton.destination_session !== this.actor.session
      || baton.state !== "in-progress" || baton.claimed_by_generation_uuid !== this.actor.generation) return "held";
    // Do not churn authority on every heartbeat. Observe while plenty of its live
    // window remains; expiration is a distinct recovery contract, never renewed.
    if (a.lease_until - now > leaseMs / 2) return "watching";
    const request: NativeDutyResumeRequest = { rigId: a.rig_id, operationId: this.operationId(), leaseMs,
      expectedEpoch: a.epoch, expectedObligationsDigest: shown.obligationsDigest };
    assertRequest(request);
    const entry: DutyJournalEntry = { registrationId, request, phase: "prepared" };
    this.journal.save(entry); // BEFORE registry/network; lost prepare response holds.
    const prepared = await this.transport.prepare(registrationId, request);
    if (!this.matchesIntent(prepared, entry, "prepared") || !this.live()) return "held";
    this.journal.save({ ...entry, phase: "effect-in-flight" });
    const inFlight = await this.transport.inFlight(registrationId, request.operationId);
    if (inFlight.maySendEffect !== true || !this.matchesIntent(inFlight.intent, entry, "effect-in-flight")) return "held";
    // Registration proof and intent phase are separate wire contracts. Never
    // promote an intent into native authorization or replay a false send grant.
    const finalHeartbeat = await this.observeHeartbeat(registrationId);
    if (!finalHeartbeat) return "held";
    status = finalHeartbeat;
    if (!this.allowed(status) || status.registrationId !== registrationId || status.scope.rigId !== request.rigId
      || !status.intent || !this.matchesIntent(status.intent, entry, "effect-in-flight")
      || request.leaseMs > nativeDutyLeaseMs(status.scope, this.clock.now())
      || a.lease_until <= this.clock.now()) return "held";
    // Final backend CAS, native generation and baton fences are authoritative.
    // ALL failures, even a rejection response, reconcile or hold the exact op.
    try { await this.transport.resume(request); } catch { /* outcome may be UNKNOWN */ }
    return this.reconcile(entry);
  }
  private matchesIntent(intent: NativeDutyIntent, entry: DutyJournalEntry,
    phase: "prepared" | "effect-in-flight"): boolean {
    return intent.phase === phase && intent.operationId === entry.request.operationId
      && sameRequest(intent.request, entry.request);
  }
}

export interface DutyChild {
  exited: Promise<number>;
  stop(signal: "SIGTERM" | "SIGKILL"): void;
}
export interface DutyProcesses {
  spawn(executable: string, args: readonly string[], options: { cwd?: string; stdio: "inherit" | "ignore" }): DutyChild;
}
/** Native child remains in the inherited foreground process group (no shell,
 * detached session, or stdio proxy). Pi's own RPC runner is the native child.
 * Helper never receives terminal input or prints credentials/provider output.
 */
export async function superviseNativeHarness(config: NativeDutyLaunchConfig,
  helper: { executable: string; args: string[] }, processes: DutyProcesses, clock: DutyClock,
  signal?: AbortSignal): Promise<number> {
  if (signal?.aborted) return 1;
  const harness = processes.spawn(config.harness.executable, config.harness.args,
    { cwd: config.harness.cwd, stdio: "inherit" });
  const ended = new AbortController();
  let current: DutyChild | undefined;
  let live = true;
  const stop = () => { if (live) harness.stop("SIGTERM"); ended.abort(); current?.stop("SIGTERM"); };
  signal?.addEventListener("abort", stop, { once: true });
  const harnessExit = harness.exited.then(code => { live = false; ended.abort(); current?.stop("SIGTERM"); return code; });
  const duty = (async () => {
    // Three observational restarts, only under this still-live native parent.
    for (let attempt = 0; attempt <= 3 && live && !ended.signal.aborted; attempt++) {
      current = processes.spawn(helper.executable, helper.args, { stdio: "ignore" });
      const child = current;
      await Promise.race([child.exited, harnessExit]);
      if (!live || ended.signal.aborted) {
        child.stop("SIGTERM");
        await Promise.race([child.exited, clock.sleep(5000).then(() => child.stop("SIGKILL"))]);
        return;
      }
      if (attempt < 3) {
        try { await clock.sleep(500 * 2 ** attempt, ended.signal); } catch { return; }
      }
    }
    // No native relaunch or authority recovery when helper budget is spent.
  })().catch(() => { current?.stop("SIGTERM"); });
  try { const code = await harnessExit; await duty; return code; }
  finally { signal?.removeEventListener("abort", stop); }
}

const realClock: DutyClock = {
  now: Date.now,
  sleep(ms, signal) {
    return new Promise((resolve, reject) => {
      if (signal?.aborted) { reject(new Error("native-duty-stopped")); return; }
      const abort = () => { clearTimeout(timer); reject(new Error("native-duty-stopped")); };
      const timer = setTimeout(() => { signal?.removeEventListener("abort", abort); resolve(); }, ms);
      signal?.addEventListener("abort", abort, { once: true });
    });
  },
};
const realProcesses: DutyProcesses = {
  spawn(executable, args, options) {
    // Inherited process.env ONLY. No actor environment reconstructed from config.
    const child = spawn(executable, [...args], { ...options, shell: false, detached: false });
    const exited = new Promise<number>(resolve => {
      child.once("error", () => resolve(1)); child.once("exit", code => resolve(code ?? 1));
    });
    return { exited, stop: signal => { if (child.exitCode === null && child.signalCode === null) child.kill(signal); } };
  },
};

/** Private durable journal. No opaque transport response or environment is ever
 * serialized; only the exact allowlisted public request and phase enter disk.
 */
export class FileDutyJournal implements DutyJournal {
  private readonly file: string;
  constructor(dir: string) {
    const stat = fs.lstatSync(dir);
    if (!stat.isDirectory() || stat.isSymbolicLink() || stat.uid !== process.getuid?.()
      || (stat.mode & 0o077) !== 0) throw new Error("native-duty-private-journal-required");
    this.file = path.join(dir, "intent.json");
  }
  read(): DutyJournalEntry | null {
    let fd: number;
    try { fd = fs.openSync(this.file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW); }
    catch (e) { if ((e as NodeJS.ErrnoException).code === "ENOENT") return null; throw new Error("native-duty-journal-unreadable"); }
    try {
      const stat = fs.fstatSync(fd);
      if (!stat.isFile() || (stat.mode & 0o077) !== 0 || stat.uid !== process.getuid?.()) throw new Error("native-duty-private-journal-required");
      const value = JSON.parse(fs.readFileSync(fd, "utf8")) as DutyJournalEntry;
      assertRequest(value.request);
      if (Object.keys(value).sort().join(",") !== "phase,registrationId,request"
        || typeof value.registrationId !== "string" || !value.registrationId
        || !["prepared", "effect-in-flight", "receipt-confirmed"].includes(value.phase)) throw new Error("native-duty-journal-invalid");
      return value;
    } finally { fs.closeSync(fd); }
  }
  save(entry: DutyJournalEntry): void {
    assertRequest(entry.request);
    const prior = this.read();
    if (prior && (prior.registrationId !== entry.registrationId
      || (prior.phase !== "receipt-confirmed" && !sameRequest(prior.request, entry.request)))) {
      throw new Error("native-duty-unresolved-journal-conflict");
    }
    const temp = `${this.file}.${randomUUID()}.tmp`;
    const fd = fs.openSync(temp, "wx", 0o600);
    try {
      fs.writeFileSync(fd, JSON.stringify({ registrationId: entry.registrationId,
        request: { rigId: entry.request.rigId, operationId: entry.request.operationId, leaseMs: entry.request.leaseMs,
          expectedEpoch: entry.request.expectedEpoch, expectedObligationsDigest: entry.request.expectedObligationsDigest }, phase: entry.phase }) + "\n");
      fs.fsyncSync(fd);
    } finally { fs.closeSync(fd); }
    fs.renameSync(temp, this.file);
    const dir = fs.openSync(path.dirname(this.file), "r");
    try { fs.fsyncSync(dir); } finally { fs.closeSync(dir); }
  }
}

/** Transport is constructed ONLY inside the native helper, from its inherited
 * launch channel. No token fallback, env override, remote forwarding or logging.
 * Root wires the paired Node/installed entry; no naked PATH-selected CLI runs.
 */
export function inheritedNativeDutyTransport(): { actor: NativeDutyActor; transport: NativeDutyTransport;
  contextRefreshTransport: ContextRefreshTransport } {
  const env = process.env;
  const session = env.OPENRIG_SESSION_NAME, generation = env.OPENRIG_OCCUPANT_GENERATION;
  const endpoint = env.OPENRIG_URL;
  if (!session || !generation || !endpoint) throw new Error("native-duty-inherited-auth-required");
  const token = resolveNativeDutyToken(env);
  const url = new URL(endpoint);
  if (!["http:", "https:"].includes(url.protocol) || url.username || url.password || url.search || url.hash)
    throw new Error("native-duty-invalid-endpoint");
  const headers = { Authorization: `Bearer ${token}`, "X-OpenRig-Session": session,
    "X-OpenRig-Occupant-Generation": generation, "Content-Type": "application/json" };
  async function call<T>(route: string, body?: unknown): Promise<T> {
    let res: Response;
    try {
      res = await fetch(`${endpoint!.replace(/\/+$/, "")}${route}`, { method: body === undefined ? "GET" : "POST",
        headers, ...(body === undefined ? {} : { body: JSON.stringify(body) }),
        signal: AbortSignal.timeout(NATIVE_DUTY_REQUEST_TIMEOUT_MS), redirect: "error" });
    } catch { throw body === undefined ? new NativeDutyObservationError() : new Error("native-duty-transport-unresolved"); }
    if (!res.ok) {
      if (route === `${NATIVE_DUTY_API}/heartbeat` && body !== undefined && res.status === 409) {
        let refusal: unknown;
        try { refusal = await res.json(); } catch { /* Unknown response stays a mutation failure. */ }
        if (refusal && typeof refusal === "object" && !Array.isArray(refusal)
          && ((refusal as Record<string, unknown>).error === "native_duty_temporary_exclusion"
            || (refusal as Record<string, unknown>).error === "native_duty_proof_unavailable")) throw new NativeDutyTemporaryHoldError();
      }
      throw body === undefined ? new NativeDutyObservationError() : new Error("native-duty-transport-unresolved");
    }
    // A successful HTTP response with invalid JSON is a contract defect, not a
    // network observation interruption. POST outcome still remains UNKNOWN.
    try { return await res.json() as T; }
    catch { throw body === undefined ? new NativeDutyInvalidObservationError() : new Error("native-duty-transport-unresolved"); }
  }
  const actor = { session, generation };
  const assertInheritedActor = (requested: NativeDutyActor): void => {
    if (requested.session !== session || requested.generation !== generation)
      throw new Error("context-refresh-inherited-actor-mismatch");
  };
  const selectionQuery = (selection: { grantId: string; nodeId: string; operationId?: string }): string =>
    new URLSearchParams({ grantId: selection.grantId, nodeId: selection.nodeId,
      ...(selection.operationId === undefined ? {} : { operationId: selection.operationId }) }).toString();
  const contextRefreshTransport: ContextRefreshTransport = {
    enrollment: (requested, input) => {
      assertInheritedActor(requested);
      return call(`/api/context-refresh/enrollment?${new URLSearchParams({ launchId: input.launchId,
        supervisorPid: String(input.supervisorPid) })}`);
    },
    step: (requested, input) => { assertInheritedActor(requested); return call("/api/context-refresh/step", input); },
    reconcile: (requested, input) => { assertInheritedActor(requested); return call("/api/context-refresh/reconcile", input); },
    status: (requested, input) => {
      assertInheritedActor(requested); return call(`/api/context-refresh/status?${selectionQuery(input)}`);
    },
  };
  return { actor, contextRefreshTransport, transport: {
    enrollment: request => {
      const query = new URLSearchParams({ scopeId: request.scopeId, launchId: request.launchId,
        supervisorPid: String(request.supervisorPid) });
      return call(`${NATIVE_DUTY_API}/enrollment?${query.toString()}`);
    },
    register: request => call(`${NATIVE_DUTY_API}/register`, request),
    status: id => call(`${NATIVE_DUTY_API}/status/${encodeURIComponent(id)}`),
    heartbeat: registrationId => call(`${NATIVE_DUTY_API}/heartbeat`, { registrationId }),
    show: rigId => call(`/api/coordinator/${encodeURIComponent(rigId)}`),
    prepare: (registrationId, request) => call(`${NATIVE_DUTY_API}/prepare`, { registrationId, request }),
    inFlight: (registrationId, operationId) => call(`${NATIVE_DUTY_API}/in-flight`, { registrationId, operationId }),
    resume: async request => { await call("/api/coordinator/resume-owned", request); },
    reconcile: (registrationId, operationId) => call(`${NATIVE_DUTY_API}/reconcile`, { registrationId, operationId }),
    stop: async (registrationId, reason) => { await call(`${NATIVE_DUTY_API}/stop`, { registrationId, reason }); },
  } };
}

function resolveNativeDutyToken(env: NodeJS.ProcessEnv): string {
  const inherited = env.OPENRIG_TERMINAL_BEARER_TOKEN?.trim();
  if (inherited) return inherited;
  const home = env.OPENRIG_HOME;
  if (!home || !path.isAbsolute(home)) throw new Error("native-duty-auth-required");
  const tokenPath = path.join(home, "terminal-token");
  let fd: number | undefined;
  try {
    const before = fs.lstatSync(tokenPath);
    if (!before.isFile() || before.isSymbolicLink() || before.uid !== process.getuid?.()
      || (before.mode & 0o777) !== 0o600) throw new Error("native-duty-auth-file-refused");
    const noFollow = fs.constants.O_NOFOLLOW ?? 0;
    fd = fs.openSync(tokenPath, fs.constants.O_RDONLY | noFollow);
    const opened = fs.fstatSync(fd);
    if (!opened.isFile() || opened.uid !== process.getuid?.() || (opened.mode & 0o777) !== 0o600
      || opened.dev !== before.dev || opened.ino !== before.ino) throw new Error("native-duty-auth-file-refused");
    const token = fs.readFileSync(fd, "utf8").trim();
    if (!token) throw new Error("native-duty-auth-file-refused");
    return token;
  } catch {
    throw new Error("native-duty-auth-required");
  } finally {
    if (fd !== undefined) fs.closeSync(fd);
  }
}

function readConfig(file: string): NativeDutyLaunchConfig {
  const c = JSON.parse(fs.readFileSync(file, "utf8")) as NativeDutyLaunchConfig;
  if (Object.keys(c).sort().join(",") !== "harness,journalDir,launchId,pollMs,scopeId"
    || !c.harness || Object.keys(c.harness).sort().join(",") !== "args,cwd,executable"
    || typeof c.scopeId !== "string" || !c.scopeId || typeof c.launchId !== "string" || !c.launchId
    || !path.isAbsolute(c.journalDir) || !path.isAbsolute(c.harness.cwd) || !path.isAbsolute(c.harness.executable)
    || !Array.isArray(c.harness.args) || !c.harness.args.every(a => typeof a === "string" && !a.includes("\0"))
    || !Number.isSafeInteger(c.pollMs) || c.pollMs < 1000 || c.pollMs > 60000) throw new Error("native-duty-invalid-config");
  return c;
}

/** Explicit executable entry; importing never reads config/auth or spawns. The
 * launch config is a public argv contract, never an environment snapshot.
 */
export async function nativeDutySupervisorEntry(args: string[]): Promise<number> {
  const [mode, file, parent] = args;
  if (!file || !path.isAbsolute(file) || !["--supervise", "--helper"].includes(mode ?? "")) return 1;
  const config = readConfig(file);
  const abort = new AbortController();
  const stop = () => abort.abort();
  process.on("SIGTERM", stop);
  // SIGINT reaches the whole inherited foreground group, including the native
  // harness. Parent stays alive while that harness decides whether to exit.
  const ignoreInterrupt = () => {};
  process.on("SIGINT", ignoreInterrupt);
  try {
    if (mode === "--supervise" && args.length === 2) {
      return await superviseNativeHarness(config, { executable: process.execPath,
        args: [fileURLToPath(import.meta.url), "--helper", file, String(process.pid)] }, realProcesses, realClock, abort.signal);
    }
    if (mode !== "--helper" || args.length !== 3 || Number(parent) !== process.ppid) return 1;
    const { actor, transport, contextRefreshTransport } = inheritedNativeDutyTransport();
    const supervisorPid = Number(parent);
    const live = () => !abort.signal.aborted && process.ppid === supervisorPid;
    // The Kernel Operator is the distinct refresh executor, not a coordinator
    // holder. Its finite refresh enrollment must not wait for a holder grant.
    // All identity/auth remains inherited; the server verifies the actual launch.
    if (actor.session === "operator-agent@kernel") {
      await launchContextRefreshLoop({ actor, transport: contextRefreshTransport,
        journal: new FileContextRefreshJournal(config.journalDir, actor, config.launchId),
        clock: realClock, live, launchId: config.launchId, supervisorPid, signal: abort.signal });
      return 0;
    }
    // Wrap ONLY status/show GETs. POST methods and executor mutation ordering
    // are passed through unchanged, including UNKNOWN response handling.
    const observedTransport: NativeDutyTransport = { ...transport,
      status: id => waitNativeDutyObservation(() => transport.status(id), realClock, config.pollMs, live, abort.signal),
      show: rigId => waitNativeDutyObservation(() => transport.show(rigId), realClock, config.pollMs, live, abort.signal),
    };
    const journal = new FileDutyJournal(config.journalDir);
    const registrationId = await resolveNativeDutyRegistration(transport, journal,
      { scopeId: config.scopeId, launchId: config.launchId, supervisorPid: process.ppid },
      realClock, config.pollMs, live, abort.signal);
    const enrolled = await observedTransport.status(registrationId);
    if (enrolled.scope.scopeId !== config.scopeId || enrolled.launchId !== config.launchId
      || enrolled.scope.nodeId !== process.env.OPENRIG_NODE_ID
      || enrolled.scope.runtime !== process.env.OPENRIG_RUNTIME
      || enrolled.scope.sessionName !== actor.session || enrolled.scope.generation !== actor.generation) return 1;
    const executor = new HolderContinuationExecutor(observedTransport, journal, actor, realClock, live);
    try {
      while (live()) {
        const status = await observedTransport.status(registrationId);
        if (status.phase === "stopped" || status.scope.validUntil <= Date.now()) break;
        await executor.step(registrationId);
        await realClock.sleep(config.pollMs, abort.signal);
      }
    } finally {
      // Stop preserves all prepared/in-flight debt. No failure message is logged.
      if (abort.signal.aborted) { try { await transport.stop(registrationId, "native-parent-stopped"); } catch {} }
    }
    return 0;
  } finally { process.off("SIGTERM", stop); process.off("SIGINT", ignoreInterrupt); }
}
if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  nativeDutySupervisorEntry(process.argv.slice(2)).then(code => { process.exitCode = code; },
    () => { process.exitCode = 1; });
}
