import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { readFileSync, realpathSync } from "node:fs";
import { resolve, sep } from "node:path";
import { createHash } from "node:crypto";
import type { DispatchReservation } from "./seat-dispatch-reservation.js";

export interface AttemptEffects {
  preparedGeneration: string;
  discoveredId: string | null;
  nativeId: string | null;
  replacementStarted: boolean;
}
export interface HistoricalFailureProof {
  protocol: "failed-precommit-history-v1";
  reservationId: string; operationId: string; nodeId: string;
  predecessorGeneration: string; predecessorNativeId: string;
  performerSession: string; performerGeneration: string;
  fullSnapshotSha256: string; predecessorRowsSha256: string;
  effects: AttemptEffects;
  failure: { path: string; sha256: string };
  attribution: { path: string; sha256: string };
}

/** Explicit, immutable evidence import. This records reconciliation now, never backdates audit.
 * A bare old HTTP500 has no attempt/effect attribution and is deliberately insufficient. */
export function readHistoricalFailure(root: string, reference: string, proofSha256: string, r: DispatchReservation): HistoricalFailureProof {
  const frozen = realpathSync(resolve(root, "frozen"));
  const read = (path: string, sha?: string) => {
    const absolute = realpathSync(resolve(root, path));
    if (!absolute.startsWith(frozen + sep)) throw new Error("Historical proof outside frozen evidence directory");
    const bytes = readFileSync(absolute);
    if (!sha || !/^[a-f0-9]{64}$/.test(sha) || bytes.length > 1024 * 1024 || createHash("sha256").update(bytes).digest("hex") !== sha) throw new Error("Historical proof hash/size mismatch");
    return JSON.parse(bytes.toString()) as Record<string, unknown>;
  };
  const proof = read(reference,proofSha256) as unknown as HistoricalFailureProof;
  const expected = {reservationId:r.reservation_id,operationId:r.operation_id,nodeId:r.node_id,predecessorGeneration:r.predecessor_generation,predecessorNativeId:r.predecessor_native_id,performerSession:r.performer_session,performerGeneration:r.performer_generation};
  if (proof.protocol !== "failed-precommit-history-v1" || Object.entries(expected).some(([key,value]) => (proof as unknown as Record<string,unknown>)[key] !== value) || !/^[a-f0-9]{64}$/.test(proof.fullSnapshotSha256??"") || !/^[a-f0-9]{64}$/.test(proof.predecessorRowsSha256??"") || !proof.effects?.preparedGeneration || typeof proof.effects.replacementStarted !== "boolean") throw new Error("Historical attempt attribution unavailable");
  const failure = read(proof.failure.path, proof.failure.sha256);
  if ((failure.result as {ok?:boolean;code?:string})?.ok !== false || (failure.result as {code?:string}).code !== "handover_commit_failed") throw new Error("Historical failed commit proof unavailable");
  const attribution = read(proof.attribution.path, proof.attribution.sha256);
  if (Object.entries(expected).some(([key,value]) => attribution[key] !== value) || attribution.fullSnapshotSha256!==proof.fullSnapshotSha256 || attribution.predecessorRowsSha256!==proof.predecessorRowsSha256 || JSON.stringify(attribution.effects) !== JSON.stringify(proof.effects)) throw new Error("Historical effect proof is not attributed to exact attempt");
  return proof;
}

/** Read raw process environments privately; retain only generation census, never argv/secrets.
 * A failed inventory is unknown, not an empty census. The exact prepared generation can
 * escape its original pane, so checking only pane descendants would be insufficient. */
export async function censusAttemptNative(r: DispatchReservation, effects: AttemptEffects): Promise<{ remainingPids: number[]; observedAt: string }> {
  if (!effects.preparedGeneration || effects.preparedGeneration === r.predecessor_generation) throw new Error("Prepared successor generation unavailable");
  let stdout:string;
  try {({stdout}=await promisify(execFile)("/bin/ps", ["eww", "-axo", "pid=,command="], {maxBuffer:32*1024*1024,timeout:5000}));}
  catch {throw new Error("Native effect inventory unavailable; raw process environments withheld");}
  if(!/^\s*\d+\s+\S/m.test(stdout))throw new Error("Native effect inventory empty or malformed");
  const remainingPids: number[] = [];
  for (const line of stdout.split("\n")) {
    const pid = /^\s*(\d+)\s/.exec(line)?.[1];
    if (pid && line.split(/\s+/).includes(`OPENRIG_OCCUPANT_GENERATION=${effects.preparedGeneration}`)) remainingPids.push(Number(pid));
  }
  return {remainingPids,observedAt:new Date().toISOString()};
}
