import { setImmediate as yieldToEventLoop } from 'node:timers/promises';
import type {Policy} from "./types.js";
import type {CoordinatorAuthorityService} from "../coordinator-authority-service.js";
/** Opt-in native observer. Existing engine owns cadence, debounce and delivery receipts. */
export function makeCoordinatorContinuityPolicy(service:CoordinatorAuthorityService,yieldBetweenPhases:()=>Promise<void>=()=>yieldToEventLoop()):Policy {
 return {name:"coordinator-continuity",async evaluate(job){
  if(job.registeredBySession!=="operator-agent@kernel"||job.target.session!=="operator-agent@kernel"||typeof job.context.rigId!=="string")return {action:"skip",reason:"observer-not-authorized"};
  await service.refreshRuntimeAvailability(job.context.rigId);
  await yieldBetweenPhases();
  await service.coordinationRecovery?.refreshActivity(job.context.rigId);
  await yieldBetweenPhases();
  // Persisted opt-in recovery reconciles real queue transitions, not reminder replies.
  const coordination=service.coordinationRecovery?.supervise(job.context.rigId,job.jobId);
  await yieldBetweenPhases();
  await service.resumeAdministrativeDuties?.(job.context.rigId,job.jobId);
  await yieldBetweenPhases();
  const outcomePolicyRecovery=service.runtimeOutcomeAssessment?.stagePolicyBoundary(job.context.rigId);
  await yieldBetweenPhases();
  await service.runtimeOutcomeAssessment?.drain(job.context.rigId);
  await yieldBetweenPhases();
  const outcomeRecoveryBindings=service.runtimeOutcomeAssessment?.stageRecoveryBoundary(job.context.rigId);
  await yieldBetweenPhases();
  await service.coordinationRecovery?.deliverCommitted();
  await yieldBetweenPhases();
  if(coordination?.some(result=>result.key==='coordinator'&&result.queueId))return {action:'skip',reason:'committed-coordinator-recovery-custody',notes:{coordination,...(outcomePolicyRecovery?{outcomePolicyRecovery}: {}),...(outcomeRecoveryBindings?.length?{outcomeRecoveryBindings}: {})}};
  const recovery=service.observeContinuity(job.context.rigId);
  if(!recovery)return {action:"skip",reason:coordination?'coordination-reconciled':"no-authoritative-exclusion",...(coordination?{notes:{coordination,...(outcomePolicyRecovery?{outcomePolicyRecovery}: {}),...(outcomeRecoveryBindings?.length?{outcomeRecoveryBindings}: {})}}:{})};
  return {action:"send",target:job.target,message:`Coordinator recovery intake for ${recovery.rigId}, epoch ${recovery.expectedEpoch}. Preserve workers; no product dispatch or automatic transfer.`,conditionReceipt:recovery.evidenceId,coordinatorRecovery:recovery,...(coordination?{notes:{coordination,...(outcomePolicyRecovery?{outcomePolicyRecovery}: {}),...(outcomeRecoveryBindings?.length?{outcomeRecoveryBindings}: {})}}:{})};
 }};
}
