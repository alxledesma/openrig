import type {Policy} from "./types.js";
import type {CoordinatorAuthorityService} from "../coordinator-authority-service.js";
/** Opt-in native observer. Existing engine owns cadence, debounce and delivery receipts. */
export function makeCoordinatorContinuityPolicy(service:CoordinatorAuthorityService):Policy {
 return {name:"coordinator-continuity",async evaluate(job){
  if(job.registeredBySession!=="operator-agent@kernel"||job.target.session!=="operator-agent@kernel"||typeof job.context.rigId!=="string")return {action:"skip",reason:"observer-not-authorized"};
  // Persisted opt-in recovery reconciles real queue transitions, not reminder replies.
  service.coordinationRecovery?.supervise(job.context.rigId,job.jobId);
  await service.runtimeOutcomeAssessment?.drain(job.context.rigId);
  await service.coordinationRecovery?.deliverCommitted();
  await service.refreshRuntimeAvailability(job.context.rigId);
  const recovery=service.observeContinuity(job.context.rigId);
  if(!recovery)return {action:"skip",reason:"no-authoritative-exclusion"};
  return {action:"send",target:job.target,message:`Coordinator recovery intake for ${recovery.rigId}, epoch ${recovery.expectedEpoch}. Preserve workers; no product dispatch or automatic transfer.`,conditionReceipt:recovery.evidenceId,coordinatorRecovery:recovery};
 }};
}
