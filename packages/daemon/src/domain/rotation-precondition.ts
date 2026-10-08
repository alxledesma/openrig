/** Automatic rotation has stronger preconditions than owner-directed handover.
 * This verifier must run under the seat lifecycle lease immediately before launch.
 * Unknown facts refuse; caller-supplied expected values never create evidence.
 */
export interface RotationFacts {
  generation: string | null;
  queue: unknown;
  runtimeContract: unknown;
  activity: string | null;
  observedAt: number;
  checkpointHash: string | null;
}
/** Preserve the actual runtime's permission contract at the fresh launch edge. */
export function assertRotationLaunchPosture(runtime: string | null, value: unknown, posture: string): void {
  const contract = value as { runtime?: string; trust?: string; permissions?: { sandbox?: { type?: string }; approval?: string } } | null;
  let preserved = false;
  if (runtime === "pi" && contract?.runtime === "pi") {
    preserved = (posture === "full_bypass" && contract.trust === "approve")
      || (posture === "floor" && contract.trust === "no-approve");
  } else if (runtime === "codex" && contract?.runtime === "codex") {
    preserved = (posture === "full_bypass" && contract.permissions?.sandbox?.type === "danger-full-access" && contract.permissions.approval === "never")
      || (posture === "floor" && contract.permissions?.sandbox?.type === "workspace-write");
  }
  if (!preserved) throw new RotationPreconditionRefusal("Successor launch posture would change native permissions; no process replaced.");
}
export function assertRotationPrecondition(expected: Record<string, unknown>, actual: RotationFacts, now = Date.now()): void {
  if (expected.protocol !== "generation-queue-runtime-idle-v1") throw new Error("Unsupported rotation protocol");
  if (!actual.generation || actual.generation !== expected.generation) throw new Error("Rotation native generation changed or unavailable");
  if (actual.activity !== "idle-at-prompt" && actual.activity !== "idle") throw new Error("Rotation seat is busy or activity unknown");
  if (!Number.isFinite(actual.observedAt) || now < actual.observedAt || now - actual.observedAt > 5000) throw new Error("Rotation idle observation stale");
  if (!actual.checkpointHash || actual.checkpointHash !== expected.checkpointHash) throw new Error("Rotation checkpoint changed or unavailable");
  for (const field of ["queue", "runtimeContract"] as const) {
    if (actual[field] == null || JSON.stringify(actual[field]) !== JSON.stringify(expected[field])) throw new Error(`Rotation ${field} changed or unavailable`);
  }
}

export function isRotationLoopback(address:string|undefined):boolean {return address==="127.0.0.1" || address==="::1" || address==="::ffff:127.0.0.1";}

export function assertManagedUnattended(policy:Record<string,unknown>, seat:string, receipt:Record<string,unknown>):void {
  if(policy.automatic_cutover_enabled!==true || !Array.isArray(policy.managed_unattended_seats) || !policy.managed_unattended_seats.includes(seat))throw new Error("Seat not explicitly opted into managed unattended rotation");
  const snapshot=receipt.snapshot as {who?:{identity?:{sessionName?:string}}}|undefined;
  const packet=receipt.packet as Record<string,unknown>|undefined;
  const required=["current_work","decisions","memory","constraints","standing_duties","evidence","next_action","outstanding_effects"];
  if(receipt.quiescent!==true || receipt.unattended_eligible!==true || snapshot?.who?.identity?.sessionName!==seat || !packet || required.some(field=>!(field in packet)) || !Array.isArray(packet.outstanding_effects) || packet.outstanding_effects.length!==0)throw new Error("Bound quiescent unattended checkpoint required");
}

export class RotationPreconditionRefusal extends Error {
  getResponse():Response {return Response.json({ok:false,code:"rotation_precondition_failed",refusedBeforeReplacement:true,message:this.message},{status:409});}
}
