import { createHash } from "node:crypto";
import type { EventBus } from "./event-bus.js";
import type { PersistedEvent } from "./types.js";
import type { NativeRecoveryCompletion } from "./native-recovery-continuation-contract.js";

export const NATIVE_RECOVERY_COMPLETION_EVENT = "seat.native_recovery_completed";
/** Final proved producer facts. Root must independently bind the current native
 * kernel incarnation/configuration before recording a normalized completion.
 * This callback is a daemon dependency, never actor-supplied JSON. */
export interface NativeRecoveryProducerEvidence {
  producer: NativeRecoveryCompletion["producer"];
  recoveryId: string;
  rigId: string; nodeId: string; sessionId: string; sessionName: string; generation: string;
  runtime: NativeRecoveryCompletion["runtime"];
  nativeIdentityHash: string;
  source: NativeRecoveryCompletion["source"];
  runtimeLaunchId?: string;
  supervisorLaunchId?: string;
  nativeFingerprint?: string;
}
export type NativeRecoveryCompletionPublisher = (evidence: NativeRecoveryProducerEvidence) => Promise<void>;
export const recoveryReceiptDigest = (bytes: string | Buffer): string => createHash("sha256").update(bytes).digest("hex");

function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value && typeof value === "object") return "{" + Object.entries(value).sort(([a], [b]) => a.localeCompare(b)).map(([k,v]) => JSON.stringify(k) + ":" + canonical(v)).join(",") + "}";
  return JSON.stringify(value);
}
function keys(value: unknown, required: string[], optional: string[] = []): asserts value is Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value) || required.some(k => !(k in value)) || Object.keys(value).some(k => !required.includes(k) && !optional.includes(k))) throw new Error("Invalid native recovery completion shape");
}
function text(value: unknown): void { if (typeof value !== "string" || !value.trim() || /[\0\r\n]/.test(value)) throw new Error("Invalid native recovery completion identity"); }
function sha(value: unknown): void { if (typeof value !== "string" || !/^[a-f0-9]{64}$/.test(value)) throw new Error("Invalid native recovery completion digest"); }
export function validateNativeRecoveryCompletion(value: unknown): NativeRecoveryCompletion {
  keys(value, ["schema","recoveryId","producer","rigId","nodeId","sessionId","sessionName","generation","runtime","nativeIdentityHash","configurationDigest","completedAt","source","incarnation","custodyPreserved","generationUnchanged"]);
  if (value.schema !== "native-recovery-completion.v1" || value.custodyPreserved !== true || value.generationUnchanged !== true || !Number.isSafeInteger(value.completedAt) || (value.completedAt as number) < 0) throw new Error("Invalid successful native recovery proof");
  const pi = ["pi-runner-rehost","pi-detached-resume"].includes(value.producer as string);
  if ((!pi && !["codex-rehost","codex-stopped-recovery","codex-detached-resume"].includes(value.producer as string)) || value.runtime !== (pi ? "pi" : "codex")) throw new Error("Native recovery producer/runtime mismatch");
  for (const k of ["recoveryId","rigId","nodeId","sessionId","sessionName","generation"]) text(value[k]);
  sha(value.nativeIdentityHash); sha(value.configurationDigest);
  keys(value.source,["ref","digest"]); text(value.source.ref); sha(value.source.digest);
  keys(value.incarnation,["key","native"],["runtimeLaunchId","supervisorLaunchId","supervisor"]); text(value.incarnation.key);
  for (const k of ["runtimeLaunchId","supervisorLaunchId"]) if (k in value.incarnation) text(value.incarnation[k]);
  const process = (v:unknown) => {keys(v,["pid","startFingerprint"]); if (!Number.isSafeInteger(v.pid) || (v.pid as number)<=1) throw new Error("Native kernel process required"); sha(v.startFingerprint);};
  process(value.incarnation.native);
  if ("supervisor" in value.incarnation) process(value.incarnation.supervisor);
  if (("supervisor" in value.incarnation) !== ("supervisorLaunchId" in value.incarnation)) throw new Error("Incomplete supervisor incarnation");
  if (pi && !("runtimeLaunchId" in value.incarnation)) throw new Error("Exact Pi runner launch required");
  return JSON.parse(canonical(value)) as NativeRecoveryCompletion;
}

/** Uses the retained events log; no second ledger, migration or legacy inference.
 * Same recoveryId is globally immutable, including node/generation changes.
 * Transactions serialize conflicting writers on the shared SQLite connection. */
export class NativeRecoveryCompletionStore {
  constructor(private readonly bus: EventBus) {}
  record(input: NativeRecoveryCompletion): NativeRecoveryCompletion {
    const completion = validateNativeRecoveryCompletion(input);
    let token: PersistedEvent | undefined;
    let registered = false;
    const result = this.bus.db.transaction(() => {
      const rows = this.bus.db.prepare("SELECT payload FROM events WHERE type=? AND json_extract(payload,'$.completion.recoveryId')=? ORDER BY seq").all(NATIVE_RECOVERY_COMPLETION_EVENT,completion.recoveryId) as {payload:string}[];
      if (rows.length) {
        const prior = rows.map(r=>validateNativeRecoveryCompletion(JSON.parse(r.payload).completion));
        if (prior.some(p=>canonical(p)!==canonical(completion))) throw new Error("Conflicting immutable native recovery completion");
        return prior[0]!;
      }
      token = this.bus.persistWithinTransaction({type:NATIVE_RECOVERY_COMPLETION_EVENT,rigId:completion.rigId,nodeId:completion.nodeId,completion} as never);
      registered = this.bus.registerPersistedWithinActiveEnvelope(token);
      return completion;
    }).immediate();
    if (token && !registered && !this.bus.db.inTransaction) this.bus.notifySubscribers(token);
    return result;
  }
  latest(nodeId: string, generation: string): NativeRecoveryCompletion | null {
    const row = this.bus.db.prepare("SELECT payload FROM events WHERE type=? AND node_id=? AND json_extract(payload,'$.completion.generation')=? ORDER BY seq DESC LIMIT 1").get(NATIVE_RECOVERY_COMPLETION_EVENT,nodeId,generation) as {payload:string}|undefined;
    if (!row) return null;
    const completion = validateNativeRecoveryCompletion(JSON.parse(row.payload).completion);
    if (completion.nodeId !== nodeId || completion.generation !== generation) throw new Error("Native recovery event projection mismatch");
    return completion;
  }
}
