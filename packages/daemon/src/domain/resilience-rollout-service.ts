import type {QueueRepository} from './queue-repository.js';
import type {WatchdogJobsRepository} from './watchdog-jobs-repository.js';
import type {OutcomePolicy} from './runtime-outcome-assessment.js';
import {digest,CoordinatorFenceError} from './coordinator-authority-service.js';
import {parse as yaml} from 'yaml';
export const STANDARD_RESILIENCE_POLICY=Object.freeze({schema:1,ref:'builtin:standard',desiredMode:'enforce',observerIntervalSeconds:30,recoveryDeadlineMs:60000,allowPaid:false,allowUnqualifiedNegativeAdvice:false,privateHostedRouting:false});
export interface RolloutReceipt {rigId:string;rigName:string;policyRef:'builtin:standard';state:'covered'|'recovery-pending'|'operator-unavailable'|'recovery-error';reasons:string[];operatorGeneration:string|null;queueId?:string;jobId?:string;policyRevision?:string;observedAt:number;defaultAuditState:'active'|'stopped'|'terminal'|'unregistered';error?:string;}
/** Observation-only executable contract staged inside the generic intake for an
 * expired ACTIVE current-generation holder. Staging never changes authority; the
 * supported route re-probes runtime evidence and refuses absent/unknown holders. */
export interface ExpiryRecoveryContract {schema:'expiry-recovery-contract.v1';supportedOperation:'active-expiry-recover';guardSequence:['refreshRuntimeAvailability','active-expiry-recover with recomputed current custody digest'];token:{rigId:string;epoch:number;generation:string};expectedLeaseUntil:number;windowMs:{min:10000;max:900000};obligationsDigest:'recompute reconciliationDigest(rigId) at execution; never reuse this snapshot as proof';observedObligationsDigest:string;executionOrder:'Operator executes supported active-expiry-recover within the finite window; the genuine holder may then acknowledge and renew through the guarded reconciling paths; staging itself changes no authority';}
/** Durable inventory is actual custody, not a synthetic enrollment receipt.
 * No role labels, stale qualifications, provider permissions or model pins are copied. */
export class ResilienceRolloutService {
 constructor(private repo:QueueRepository,private jobs:WatchdogJobsRepository,private now:()=>number=Date.now){}
 private get authority(){return this.repo.coordinatorAuthority;}
 private get db(){return this.authority.db;}
 armDefaultAudit():string {return this.jobs.ensureAutoRegistration({policy:'resilience-rollout',targetSession:'operator-agent@kernel',registeredBySession:'daemon@kernel',intervalSeconds:STANDARD_RESILIENCE_POLICY.observerIntervalSeconds,specYaml:JSON.stringify({policy:'resilience-rollout',target:{session:'operator-agent@kernel'},context:{policyRef:STANDARD_RESILIENCE_POLICY.ref}})}).jobId;}
 private defaultAuditState():RolloutReceipt['defaultAuditState'] {return (this.db.prepare("SELECT state FROM watchdog_jobs WHERE policy='resilience-rollout' AND target_session='operator-agent@kernel' AND registered_by_session='daemon@kernel' ORDER BY registered_at DESC,rowid DESC LIMIT 1").get() as {state:RolloutReceipt['defaultAuditState']}|undefined)?.state??'unregistered';}
 private missing(rigId:string):string[]{try{return this.missingUnsafe(rigId);}catch{return ['malformed-current-control-record: exact Operator recovery required'];}}
 private missingUnsafe(rigId:string):string[]{
  const a=this.authority.get(rigId);if(!a)return ['explicit-enrollment-and-legacy-custody-contract-required'];
  const coordinators=JSON.parse(a.coordinators) as string[];
  const reasons:string[]=[];
  if(!this.authority.coordinatorMembersValid(rigId,a.owner_session,coordinators))reasons.push('actual-current-Lead-and-distinct-Peer-required');
  if(a.state!=='active'||this.authority.generation(a.owner_session)!==a.owner_generation||a.lease_until<=this.now())reasons.push('current-holder-reconciliation-required');
  const p=this.authority.coordinationRecovery?.plan(rigId),op=this.authority.generation('operator-agent@kernel');
  if(!p||p.operatorGeneration!==op)reasons.push('current-Operator-admitted-recovery-plan-required');
  else if(p.tasks.some(t=>t.admission.generation!==this.authority.generation(t.owner)||t.admission.configurationDigest!==this.authority.coordinationRecovery?.configurationDigest(t.owner)||!Number.isFinite(t.admission.validUntil)||t.admission.validUntil<=this.now()))reasons.push('fresh-exact-task-admissions-required');
  const outcome=this.authority.runtimeOutcomeAssessment?.policy(rigId);
  if(!outcome||outcome.mode!=='enforce'||outcome.operatorGeneration!==op||!Number.isFinite(outcome.qualification.validUntil)||outcome.qualification.validUntil<=this.now()||outcome.qualification.providerConfigDigest!==digest(JSON.stringify(outcome.adapterConfig)))reasons.push('current-qualified-outcome-policy-required');
  if(!this.currentJob(rigId,op))reasons.push('current-Operator-observer-registration-required');
  return reasons;
 }
 private currentJob(rigId:string,generation:string|null):string|undefined {
  const rows=this.db.prepare("SELECT job_id,spec_yaml FROM watchdog_jobs WHERE policy='coordinator-continuity' AND state='active' AND target_session='operator-agent@kernel' AND registered_by_session='operator-agent@kernel' AND registered_by_generation_uuid=? AND target_generation_uuid=?").all(generation,generation) as Array<{job_id:string;spec_yaml:string}>;
  return rows.find(row=>{try{return yaml(row.spec_yaml)?.context?.rigId===rigId;}catch{return false;}})?.job_id;
 }
 private expiryContract(rigId:string):ExpiryRecoveryContract|undefined {
  const a=this.authority.get(rigId);
  if(!a||a.state!=='active'||a.lease_until>this.now()||this.authority.generation(a.owner_session)!==a.owner_generation)return undefined;
  let observed:string='unavailable';try{observed=this.authority.reconciliationDigest(rigId);}catch{}
  return {schema:'expiry-recovery-contract.v1',supportedOperation:'active-expiry-recover',guardSequence:['refreshRuntimeAvailability','active-expiry-recover with recomputed current custody digest'],token:{rigId:a.rig_id,epoch:a.epoch,generation:a.owner_generation},expectedLeaseUntil:a.lease_until,windowMs:{min:10000,max:900000},obligationsDigest:'recompute reconciliationDigest(rigId) at execution; never reuse this snapshot as proof',observedObligationsDigest:observed,executionOrder:'Operator executes supported active-expiry-recover within the finite window; the genuine holder may then acknowledge and renew through the guarded reconciling paths; staging itself changes no authority'};
 }
 inventory():RolloutReceipt[]{
  const operatorGeneration=this.authority.generation('operator-agent@kernel');
  return (this.db.prepare("SELECT id,name FROM rigs WHERE archived_at IS NULL AND name!='kernel' ORDER BY id").all() as Array<{id:string;name:string}>).map(r=>{
   const reasons=this.missing(r.id);let policyRevision:string|undefined;try{policyRevision=this.authority.runtimeOutcomeAssessment?.policy(r.id)?.revision;}catch{}
   return {rigId:r.id,rigName:r.name,policyRef:'builtin:standard',state:!operatorGeneration?'operator-unavailable':reasons.length?'recovery-pending':'covered',reasons,operatorGeneration,defaultAuditState:this.defaultAuditState(),observedAt:this.now(),jobId:this.currentJob(r.id,operatorGeneration),policyRevision};
  });
 }
 /** Boot + birth catch-up is additive. An absent Operator is explicit inventory;
  * failure never changes an existing rig into an enrolled or accepted state. */
 reconcile(rigId?:string):RolloutReceipt[]{
  return this.inventory().filter(r=>!rigId||r.rigId===rigId).map(r=>{
   if(r.state==='covered')return r;
   if(!r.operatorGeneration)return {...r,state:'operator-unavailable',reasons:['actual-current-Kernel-Operator-required',...r.reasons]};
   const contract=this.expiryContract(r.rigId);
   // Dedupe binds the lease tuple itself: epoch/generation/expectedLeaseUntil enter
   // the rolloutKey, so an intake staged under any other tuple is never returned as
   // the current executable contract — claimed or unclaimed alike. Old custody rows
   // stay untouched (no silent retirement); the new item carries an explicit
   // stale-contract reference so lineage stays auditable.
   const rolloutKey=digest(r.rigId+':'+r.operatorGeneration+':'+r.reasons.join('|')+(contract?':'+digest(`${contract.token.epoch}:${contract.token.generation}:${contract.expectedLeaseUntil}`):''));
   const previous=this.db.prepare("SELECT qitem_id,state FROM queue_items WHERE destination_session='operator-agent@kernel' AND json_valid(body) AND json_extract(body,'$.rolloutKey')=? ORDER BY rowid DESC LIMIT 1").get(rolloutKey) as {qitem_id:string;state:string}|undefined;
   const open=previous&&['pending','in-progress','blocked'].includes(previous.state);
   const queueId=open?previous!.qitem_id:'qitem-resilience-rollout-'+digest(rolloutKey+':'+(previous?.qitem_id??'initial')).slice(0,24);
   let staleContractQueueId:string|null=null;
   if(contract&&!open){const prior=this.db.prepare("SELECT qitem_id FROM queue_items WHERE destination_session='operator-agent@kernel' AND json_valid(body) AND state IN ('pending','in-progress','blocked') AND json_extract(body,'$.action')='materialize-standard-resilience' AND json_extract(body,'$.rigId')=? ORDER BY rowid DESC LIMIT 1").get(r.rigId) as {qitem_id:string}|undefined;if(prior&&prior.qitem_id!==queueId)staleContractQueueId=prior.qitem_id;}
   try {
    this.authority.assertCurrentOperator('operator-agent@kernel',r.operatorGeneration);
    this.db.transaction(()=>{this.authority.assertCurrentOperator('operator-agent@kernel',r.operatorGeneration!);if(!this.repo.getById(queueId))this.repo.createWithinTransaction({qitemId:queueId,sourceSession:'watchdog@system',destinationSession:'operator-agent@kernel',body:JSON.stringify({action:'materialize-standard-resilience',rolloutKey,previousQueueId:previous?.qitem_id??null,rigId:r.rigId,rigName:r.rigName,policyRef:r.policyRef,reasons:r.reasons,...(contract?{expiryRecoveryContract:contract,...(staleContractQueueId?{staleContractQueueId}:{})}:{}),recipientGeneration:r.operatorGeneration,deadline:this.now()+STANDARD_RESILIENCE_POLICY.recoveryDeadlineMs,required:'Reconcile actual identities, unresolved queue/effect custody, exact legacy enrollment and current admitted dependency/recovery frontier. Use supported APIs to enroll and configure current qualifications/explicit provider permissions, then resilience-materialize. Do not invent Peer, qualification, native pickup or override owner/privacy gates. Continue independent ready work.',returnPath:{holder:this.authority.get(r.rigId)?.owner_session??null,missingHolder:!this.authority.get(r.rigId),queueId,actor:'operator-agent@kernel',generation:r.operatorGeneration,completion:'Use supported queue update on this exact claimed recovery item with exact materialization receipt/evidence; inventory independently rechecks coverage',failure:'Block this same recovery item with concrete protected boundary, next action and deadline; never mark setup covered without actual registration'}}),identityProvenance:'system:operator-authorized-coordination',nudge:true});this.repo.stageWakeIntent(queueId,'watchdog@system','operator-agent@kernel','system:operator-authorized-coordination',true,r.operatorGeneration!);}).immediate();
    return {...r,queueId};
   }catch(error){return {...r,state:'recovery-error',error:typeof(error as {code?:unknown})?.code==='string'?String((error as {code:string}).code):'rollout-recovery-stage-failed'};}
  });
 }
 /** Current native Operator owns activation; all existing admission/legacy gates
  * precede configuration. There is no bare-role enrollment or default provider. */
 materialize(actor:string,generation:string,p:OutcomePolicy):RolloutReceipt {
  this.authority.assertCurrentOperator(actor,generation);
  if(!p||typeof p!=='object'||Array.isArray(p)||typeof p.rigId!=='string')throw new CoordinatorFenceError('resilience_policy_required','Typed actual per-rig OutcomePolicy required');
  if(!this.inventory().some(r=>r.rigId===p.rigId))throw new CoordinatorFenceError('resilience_project_required','Actual nonarchived project rig required');
  if(p.mode!=='enforce')throw new CoordinatorFenceError('resilience_enforcement_required','Standard rollout requires enforce; observation alone is not materialization');
  const critical=this.missing(p.rigId).filter(r=>!['current-qualified-outcome-policy-required','current-Operator-observer-registration-required'].includes(r));
  if(critical.length)return this.reconcile(p.rigId)[0]??(()=>{throw new CoordinatorFenceError('resilience_project_required','Actual project rig required');})();
  this.db.transaction(()=>{
   this.authority.runtimeOutcomeAssessment!.configure(actor,generation,p);
   if(!this.currentJob(p.rigId,generation))this.jobs.register({policy:'coordinator-continuity',targetSession:'operator-agent@kernel',registeredBySession:actor,targetGenerationUuid:generation,intervalSeconds:STANDARD_RESILIENCE_POLICY.observerIntervalSeconds,specYaml:JSON.stringify({policy:'coordinator-continuity',target:{session:'operator-agent@kernel'},context:{rigId:p.rigId},generated_by:'standard-resilience-materializer'})});
  }).immediate();
  return this.inventory().find(r=>r.rigId===p.rigId)!;
 }
 async deliver():Promise<void>{await this.repo.drainPendingWakeIntents();}
}
