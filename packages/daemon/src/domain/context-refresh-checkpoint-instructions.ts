import { createHash } from "node:crypto";

export interface ContextRefreshCheckpointInstructionInput {
  grantId: string;
  nodeId: string;
  generation: string;
}
export interface ContextRefreshCheckpointInstructions {
  qitemId: string;
  body: string;
}
const safeIdentity = (value: unknown): value is string => typeof value === "string"
  && value.trim().length > 0 && value.length <= 160 && !/[\x00-\x1f\x7f]/.test(value);

/** One deterministic preparation identity and fixed human task body shared by
 * request creation and the queue's administrative validator. */
export function contextRefreshCheckpointInstructions(input: ContextRefreshCheckpointInstructionInput): ContextRefreshCheckpointInstructions {
  if (!input || !safeIdentity(input.grantId) || !safeIdentity(input.nodeId) || !safeIdentity(input.generation))
    throw new Error("context-refresh-checkpoint-identity-invalid");
  // The digest input is a tuple of strings, so this is identical to the service's
  // canonical contextRefreshDigest([grantId, nodeId, generation]) encoding.
  const tuple = JSON.stringify([input.grantId, input.nodeId, input.generation]);
  const qitemId = "context-refresh-" + createHash("sha256").update(tuple).digest("hex");
  const body = [
    "Claim the exact checkpoint-preparation task " + qitemId + " for grant " + input.grantId + ", node " + input.nodeId + ", generation " + input.generation + ".",
    "At a safe idle boundary, author exactly these eight truthful packet fields: current_work, decisions, memory, constraints, standing_duties, evidence, next_action, outstanding_effects.",
    "Set outstanding_effects to [] only after actually reconciling all effects and confirming none remain. If any effect is open or unknown, do not submit; describe the real state and leave this task active.",
    "Submit once with POST \u0024{OPENRIG_URL}/api/context-refresh/checkpoint using the inherited authenticated OpenRig connection and current session/generation headers. Send {\"grantId\":\"" + input.grantId + "\",\"nodeId\":\"" + input.nodeId + "\",\"packet\":{...}}. Never put credentials in the packet or logs.",
    "A normal receipt is {\"draftId\":\"…\",\"grantId\":\"" + input.grantId + "\",\"nodeId\":\"" + input.nodeId + "\",\"phase\":\"submitted\"}. If the reply is missing, do not submit again: GET \u0024{OPENRIG_URL}/api/context-refresh/status?grantId=\u0024{encodeURIComponent(grantId)}&nodeId=\u0024{encodeURIComponent(nodeId)} and reconcile checkpointDraft for this exact grant/node; accept only its draftId with phase submitted or frozen. If no matching receipt is visible, preserve the uncertainty and stop.",
    "After a verified submitted/frozen receipt, inspect the exact task with rig queue show " + qitemId + " --full --json, then close that preparation task with rig queue update " + qitemId + " --state done --closure-reason no-follow-on. Finish your turn.",
    "A submitted checkpoint is not rotation approval or cutover authority. The daemon freezes only after its genuine idle, identity, custody and lifecycle checks; ordinary holder-continuation authority does not permit context refresh.",
  ].join("\n");
  return { qitemId, body };
}
