import { randomUUID } from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import { DEFAULT_CONTEXT_REFRESH_POLICY, type ContextRefreshActor, type ContextRefreshGrant, type ContextRefreshTarget } from "../domain/context-refresh-contract.js";
import type {
  ContextRefreshEnrollment, ContextRefreshFacade, ContextRefreshSelection, ContextRefreshStatus,
} from "../domain/context-refresh-integration.js";

export const CONTEXT_REFRESH_POLL_MS = 120_000;
const JOURNAL_SCHEMA = 1;
const MAX_JOURNAL_ENTRIES = 1024;
const canonical = (value: unknown): string => JSON.stringify(value, (_key, item) => item && typeof item === "object" && !Array.isArray(item)
  ? Object.fromEntries(Object.entries(item).sort(([a], [b]) => a.localeCompare(b))) : item);
const same = (a: unknown, b: unknown): boolean => canonical(a) === canonical(b);
const validId = (value: unknown): value is string => typeof value === "string" && /^[A-Za-z0-9][A-Za-z0-9._:@-]{0,159}$/.test(value);
const sha = (value: unknown): value is string => typeof value === "string" && /^[a-f0-9]{64}$/.test(value);
const exactKeys = (value: unknown, keys: string): boolean => !!value && typeof value === "object" && !Array.isArray(value)
  && Object.keys(value).sort().join(",") === keys.split(",").sort().join(",");

export interface ContextRefreshClock {
  now(): number;
  sleep(ms: number, signal?: AbortSignal): Promise<void>;
}
export interface ContextRefreshTransport extends Pick<ContextRefreshFacade, "enrollment" | "step" | "reconcile" | "status"> {}
export type ContextRefreshInvocationPhase = "prepared" | "unknown" | "completed";
export interface ContextRefreshJournalEntry {
  actor: ContextRefreshActor;
  launchId: string;
  grantId: string;
  executorNodeId: string;
  executorConfigurationDigest: string;
  target: ContextRefreshTarget;
  operationId: string;
  phase: ContextRefreshInvocationPhase;
}
export interface ContextRefreshJournalState {
  schema: 1;
  actor: ContextRefreshActor;
  launchId: string;
  entries: ContextRefreshJournalEntry[];
}
export interface ContextRefreshJournal {
  read(): ContextRefreshJournalState | null;
  save(state: ContextRefreshJournalState): void;
}
export interface ContextRefreshLoopInput {
  actor: ContextRefreshActor;
  transport: ContextRefreshTransport;
  journal: ContextRefreshJournal;
  clock: ContextRefreshClock;
  live: () => boolean;
  launchId: string;
  supervisorPid: number;
  signal?: AbortSignal;
  operationId?: () => string;
}
export interface ContextRefreshTargetResult {
  grantId: string;
  nodeId: string;
  operationId: string | null;
  state: "completed" | "watching" | "held" | "unknown";
  reason: string | null;
}
export interface ContextRefreshCycleResult {
  enrollment: "ready" | "held" | "stopped";
  targets: ContextRefreshTargetResult[];
}

function assertActor(actor: ContextRefreshActor): void {
  if (!actor || !validId(actor.session) || !validId(actor.generation)) throw new Error("context-refresh-actor-invalid");
}
function assertJournalState(value: unknown, actor: ContextRefreshActor, launchId: string): ContextRefreshJournalState {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("context-refresh-journal-invalid");
  const state = value as ContextRefreshJournalState;
  if (state.schema !== JOURNAL_SCHEMA || !same(state.actor, actor) || state.launchId !== launchId || !Array.isArray(state.entries)
    || state.entries.length > MAX_JOURNAL_ENTRIES) throw new Error("context-refresh-journal-binding-mismatch");
  const keys = new Set<string>();
  for (const entry of state.entries) {
    if (!entry || !same(entry.actor, actor) || entry.launchId !== launchId || !validId(entry.grantId)
      || !validId(entry.executorNodeId) || !sha(entry.executorConfigurationDigest) || !validId(entry.operationId)
      || !["prepared", "unknown", "completed"].includes(entry.phase)
      || !validTarget(entry.target)) throw new Error("context-refresh-journal-invalid");
    const key = `${entry.grantId}\0${entry.target.nodeId}`;
    if (keys.has(key)) throw new Error("context-refresh-journal-duplicate-target");
    keys.add(key);
  }
  return state;
}
function validTarget(value: unknown): value is ContextRefreshTarget {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const t = value as ContextRefreshTarget;
  return validId(t.nodeId) && validId(t.sessionName) && validId(t.generation) && (t.runtime === "codex" || t.runtime === "pi")
    && (t.runtime === "codex" ? validId(t.nativeId)
      : typeof t.nativeId === "string" && t.nativeId.length <= 4096
        && !/[\x00-\x1f\x7f]/.test(t.nativeId) && path.isAbsolute(t.nativeId)
        && path.normalize(t.nativeId) === t.nativeId) && sha(t.configurationDigest)
    && Object.keys(t).sort().join(",") === "configurationDigest,generation,nativeId,nodeId,runtime,sessionName";
}
function validGrant(value: unknown, actor: ContextRefreshActor, launchId: string, now: number): value is ContextRefreshGrant {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const grant = value as ContextRefreshGrant;
  return exactKeys(grant, "grantId,kind,executor,targets,policy,policyRevision,validUntil,validator,recoveryOwner")
    && exactKeys(grant.executor, "session,generation,nodeId,launchId,configurationDigest")
    && exactKeys(grant.validator, "session,generation") && exactKeys(grant.recoveryOwner, "session,generation")
    && grant.kind === "context-refresh" && validId(grant.grantId)
    && grant.executor?.session === actor.session && grant.executor?.generation === actor.generation
    && validId(grant.executor.nodeId) && grant.executor.launchId === launchId && sha(grant.executor.configurationDigest)
    && Number.isSafeInteger(grant.validUntil) && grant.validUntil > now && Array.isArray(grant.targets) && grant.targets.length > 0
    && same(grant.policy, DEFAULT_CONTEXT_REFRESH_POLICY)
    && validId(grant.policyRevision) && validId(grant.validator.session) && validId(grant.validator.generation)
    && validId(grant.recoveryOwner.session) && validId(grant.recoveryOwner.generation)
    && grant.targets.every(validTarget) && new Set(grant.targets.map(target => target.nodeId)).size === grant.targets.length;
}
function targetSame(a: ContextRefreshTarget, b: ContextRefreshTarget): boolean { return same(a, b); }
function resultFor(grantId: string, nodeId: string, operationId: string | null, state: ContextRefreshTargetResult["state"], reason: string | null = null): ContextRefreshTargetResult {
  return { grantId, nodeId, operationId, state, reason };
}

/** A private, single-launch journal. It stores only public identity bindings and
 * invocation IDs; no checkpoint, prompt, transport body or response is serialized. */
export class FileContextRefreshJournal implements ContextRefreshJournal {
  private readonly file: string;
  private readonly actor: ContextRefreshActor;
  private readonly launchId: string;
  constructor(directory: string, actor: ContextRefreshActor, launchId: string) {
    assertActor(actor);
    if (!path.isAbsolute(directory) || path.normalize(directory) !== directory || !validId(launchId)) throw new Error("context-refresh-journal-path-invalid");
    this.assertPrivateDirectory(directory);
    this.file = path.join(fs.realpathSync(directory), "context-refresh-journal.json");
    this.actor = { ...actor };
    this.launchId = launchId;
  }
  private assertPrivateDirectory(directory: string): void {
    const st = fs.lstatSync(directory);
    if (!st.isDirectory() || st.isSymbolicLink() || fs.realpathSync(directory) !== path.resolve(directory)
      || st.uid !== process.getuid?.() || (st.mode & 0o077) !== 0) throw new Error("context-refresh-private-directory-required");
  }
  read(): ContextRefreshJournalState | null {
    let fd: number;
    try { fd = fs.openSync(this.file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0)); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
      throw new Error("context-refresh-journal-unreadable");
    }
    try {
      const st = fs.fstatSync(fd);
      if (!st.isFile() || st.uid !== process.getuid?.() || (st.mode & 0o077) !== 0 || st.size > 1024 * 1024)
        throw new Error("context-refresh-private-journal-required");
      return assertJournalState(JSON.parse(fs.readFileSync(fd, "utf8")), this.actor, this.launchId);
    } catch {
      throw new Error("context-refresh-journal-invalid");
    } finally { fs.closeSync(fd); }
  }
  save(input: ContextRefreshJournalState): void {
    const state = assertJournalState(input, this.actor, this.launchId);
    const prior = this.read();
    if (prior) {
      for (const old of prior.entries) {
        const next = state.entries.find(entry => entry.grantId === old.grantId && entry.target.nodeId === old.target.nodeId);
        if (!next) throw new Error("context-refresh-journal-entry-removal");
        if (!targetSame(next.target, old.target) || next.executorNodeId !== old.executorNodeId
          || next.executorConfigurationDigest !== old.executorConfigurationDigest
          || (old.phase !== "completed" && (next.operationId !== old.operationId || next.phase === "prepared")))
          throw new Error("context-refresh-journal-operation-conflict");
      }
    }
    const bytes = `${JSON.stringify(state)}\n`;
    const temp = `${this.file}.${randomUUID()}.tmp`;
    let fd: number;
    try { fd = fs.openSync(temp, fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_WRONLY | (fs.constants.O_NOFOLLOW ?? 0), 0o600); }
    catch { throw new Error("context-refresh-journal-write-refused"); }
    try { fs.writeFileSync(fd, bytes, "utf8"); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
    try { fs.renameSync(temp, this.file); }
    catch { try { fs.unlinkSync(temp); } catch { /* retained temp is harmless evidence */ } throw new Error("context-refresh-journal-write-refused"); }
    const dir = fs.openSync(path.dirname(this.file), fs.constants.O_RDONLY);
    try { fs.fsyncSync(dir); } finally { fs.closeSync(dir); }
  }
}

/** One target scheduling turn. An unresolved operation can only be reconciled;
 * every new step receives a durable ID first, and failure to see its exact
 * completed invocation remains UNKNOWN. */
export async function runContextRefreshCycle(input: ContextRefreshLoopInput): Promise<ContextRefreshCycleResult> {
  assertActor(input.actor);
  if (!validId(input.launchId) || !Number.isSafeInteger(input.supervisorPid) || input.supervisorPid < 2)
    throw new Error("context-refresh-enrollment-binding-invalid");
  const stopped = (): boolean => !input.live() || Boolean(input.signal?.aborted);
  if (stopped()) return { enrollment: "stopped", targets: [] };
  let enrollment: ContextRefreshEnrollment;
  try { enrollment = await input.transport.enrollment(input.actor, { launchId: input.launchId, supervisorPid: input.supervisorPid }); }
  catch { return { enrollment: "held", targets: [] }; }
  if (stopped()) return { enrollment: "stopped", targets: [] };
  if (enrollment.state !== "ready") return { enrollment: "held", targets: [] };
  if (!Array.isArray(enrollment.grants) || enrollment.grants.length > 64
    || enrollment.grants.some(grant => !validGrant(grant, input.actor, input.launchId, input.clock.now())))
    return { enrollment: "held", targets: [] };
  let state: ContextRefreshJournalState;
  try { state = input.journal.read() ?? { schema: JOURNAL_SCHEMA, actor: { ...input.actor }, launchId: input.launchId, entries: [] }; }
  catch { return { enrollment: "held", targets: [] }; }
  try { assertJournalState(state, input.actor, input.launchId); }
  catch { return { enrollment: "held", targets: [] }; }

  const all = enrollment.grants.flatMap(grant => grant.targets.map(target => ({ grant, target })));
  const counts = new Map<string, number>();
  for (const { target } of all) counts.set(target.nodeId, (counts.get(target.nodeId) ?? 0) + 1);
  const results: ContextRefreshTargetResult[] = [];
  for (const { grant, target } of all) {
    if (stopped()) { results.push(resultFor(grant.grantId, target.nodeId, null, "held", "parent-stopped")); break; }
    if (counts.get(target.nodeId)! > 1) { results.push(resultFor(grant.grantId, target.nodeId, null, "held", "overlapping-grant-target")); continue; }
    const priorForNode = state.entries.filter(entry => entry.target.nodeId === target.nodeId && entry.phase !== "completed");
    if (priorForNode.some(entry => entry.grantId !== grant.grantId || !targetSame(entry.target, target))) {
      results.push(resultFor(grant.grantId, target.nodeId, null, "held", "prior-unresolved-binding")); continue;
    }
    let entry = state.entries.find(item => item.grantId === grant.grantId && item.target.nodeId === target.nodeId);
    if (entry && (!targetSame(entry.target, target) || entry.executorNodeId !== grant.executor.nodeId
      || entry.executorConfigurationDigest !== grant.executor.configurationDigest)) {
      results.push(resultFor(grant.grantId, target.nodeId, entry.operationId, "held", "target-binding-changed")); continue;
    }
    if (grant.validUntil <= input.clock.now()) {
      results.push(resultFor(grant.grantId, target.nodeId, entry?.operationId ?? null, "held", "scope-ended")); continue;
    }
    if (entry && entry.phase !== "completed") {
      const stateBefore = entry.phase;
      const receipt = await reconcileExact(input, grant.grantId, target.nodeId, entry.operationId);
      if (receipt === "completed") {
        entry = { ...entry, phase: "completed" };
        try { state = replaceEntry(state, entry); input.journal.save(state); results.push(resultFor(grant.grantId, target.nodeId, entry.operationId, "completed")); }
        catch { results.push(resultFor(grant.grantId, target.nodeId, entry.operationId, "held", "journal-write-failed")); }
      } else {
        if (stateBefore === "prepared") {
          entry = { ...entry, phase: "unknown" };
          try { state = replaceEntry(state, entry); input.journal.save(state); }
          catch { results.push(resultFor(grant.grantId, target.nodeId, entry.operationId, "held", "journal-write-failed")); continue; }
        }
        results.push(resultFor(grant.grantId, target.nodeId, entry.operationId, "unknown", "exact-invocation-unresolved"));
      }
      continue;
    }
    if (stopped()) { results.push(resultFor(grant.grantId, target.nodeId, null, "held", "parent-stopped")); break; }
    const operationId = (input.operationId ?? randomUUID)();
    if (!validId(operationId)) { results.push(resultFor(grant.grantId, target.nodeId, null, "held", "operation-id-invalid")); continue; }
    entry = { actor: { ...input.actor }, launchId: input.launchId, grantId: grant.grantId,
      executorNodeId: grant.executor.nodeId, executorConfigurationDigest: grant.executor.configurationDigest,
      target: { ...target }, operationId, phase: "prepared" };
    try { state = replaceEntry(state, entry); input.journal.save(state); }
    catch { results.push(resultFor(grant.grantId, target.nodeId, operationId, "held", "journal-write-failed")); continue; }
    if (stopped() || grant.validUntil <= input.clock.now()) {
      results.push(resultFor(grant.grantId, target.nodeId, operationId, "held", stopped() ? "parent-stopped" : "scope-ended"));
      continue;
    }
    try { await input.transport.step(input.actor, { grantId: grant.grantId, nodeId: target.nodeId, operationId }); }
    catch { /* The step may have reached the facade. Reconcile this exact ID only. */ }
    const outcome = await reconcileExact(input, grant.grantId, target.nodeId, operationId);
    if (outcome === "completed") {
      try { state = replaceEntry(state, { ...entry, phase: "completed" }); input.journal.save(state);
        results.push(resultFor(grant.grantId, target.nodeId, operationId, "completed")); }
      catch { results.push(resultFor(grant.grantId, target.nodeId, operationId, "held", "journal-write-failed")); }
    } else {
      try { state = replaceEntry(state, { ...entry, phase: "unknown" }); input.journal.save(state); }
      catch { results.push(resultFor(grant.grantId, target.nodeId, operationId, "held", "journal-write-failed")); continue; }
      results.push(resultFor(grant.grantId, target.nodeId, operationId, "unknown", "exact-invocation-unresolved"));
    }
  }
  return { enrollment: "ready", targets: results };
}

async function reconcileExact(input: ContextRefreshLoopInput, grantId: string, nodeId: string, operationId: string): Promise<"completed" | "unresolved"> {
  const selection: ContextRefreshSelection = { grantId, nodeId, operationId };
  if (input.signal?.aborted) return "unresolved";
  try { await input.transport.reconcile(input.actor, selection); } catch { /* readback below is authoritative */ }
  if (input.signal?.aborted) return "unresolved";
  let status: ContextRefreshStatus;
  try { status = await input.transport.status(input.actor, selection); } catch { return "unresolved"; }
  return status.invocation?.operationId === operationId && status.invocation.state === "completed" ? "completed" : "unresolved";
}
function replaceEntry(state: ContextRefreshJournalState, entry: ContextRefreshJournalEntry): ContextRefreshJournalState {
  const entries = [...state.entries];
  const index = entries.findIndex(item => item.grantId === entry.grantId && item.target.nodeId === entry.target.nodeId);
  if (index >= 0) entries[index] = entry; else entries.push(entry);
  if (entries.length > MAX_JOURNAL_ENTRIES) throw new Error("context-refresh-journal-capacity-exceeded");
  return { ...state, entries };
}

/** Root's startup wiring can import this without starting work. Each cycle is
 * serial and each facade step is one finite target effect at most. */
export async function launchContextRefreshLoop(input: ContextRefreshLoopInput): Promise<void> {
  while (input.live() && !input.signal?.aborted) {
    await runContextRefreshCycle(input);
    if (!input.live() || input.signal?.aborted) return;
    await input.clock.sleep(CONTEXT_REFRESH_POLL_MS, input.signal);
  }
}
