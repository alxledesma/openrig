import { resolveGuardTarget } from "./seat-delivery-guard.js";
import { validRuntimeRequirements, type DispatchRuntimeRequirements, type DispatchRuntimeHold } from "./dispatch-runtime-readiness.js";
import { isContainedExpiredAdministrativeHistory } from "./expired-administrative-history.js";
import { rotationLocalAddresses } from "./rotation-local-custody.js";
import type { QueueRepository } from "./queue-repository.js";
import type { PersistedEvent } from "./types.js";
import type { NativeQueueCustodyReceipt, QueueTransition } from "./queue-transition-log.js";
import { CoordinatorFenceError, digest, type CoordinatorToken } from "./coordinator-authority-service.js";
import type { ActivityEvidence, ArbitratedSeatState } from "./activity-taxonomy.js";
import type { NativeRecoveryObservation, NativeRecoveryContinuationRuntime, NativeSettledObservation } from "./native-recovery-continuation-contract.js";
import { claimedRecoveryContinuationOutboxId, type ClaimedRecoveryContinuationProof, ownedOutcomeContinuationOutboxId, type OwnedOutcomeContinuationProof } from "./queue-repository.js";
import { ADMISSION_DUTY_KIND, CONFIRMATION_DUTY_KIND, FrontierPlanning, PLANNING_DUTY_KIND, type FrontierAdmissionReceipt, type FrontierConfirmationReceipt, type FrontierPlanReceipt, type FrontierReopenReceipt, type FrontierSnapshot, type LegacyClassificationInput, type LegacyClassificationReceipt, type LegacyRevocationInput, type LegacyRevocationReceipt, type ScopeSource } from "./frontier-planning.js";

/** The frontier kinds are next-work planning obligations, not administrative refresh:
 *  admission and confirmation deliberately keep the strict effect-debt gate because
 *  the administrative allowlist stays explicitly qualification-refresh only. */
type DutyKind='held-history-authoring'|'held-history-pickup'|'held-history-retirement'|'acceptance'|'recovery'|'materialization'|'outcome-qualification-refresh'|'admission-refresh'|'qualification-assessment'|'qualification-assessment-retirement'|'lifecycle-retirement'|'frontier-planning'|'frontier-admission'|'frontier-confirmation';
const dutyKinds:Record<DutyKind,{binding:'currentHolder'|'currentOperator'|'qualificationWorker'|'recipientOnly';effectClass:'state-changing'|'report-only'}>={
 'held-history-authoring':{binding:'currentHolder',effectClass:'state-changing'},'held-history-pickup':{binding:'currentOperator',effectClass:'state-changing'},'held-history-retirement':{binding:'recipientOnly',effectClass:'report-only'},
 acceptance:{binding:'currentHolder',effectClass:'state-changing'},recovery:{binding:'currentHolder',effectClass:'state-changing'},materialization:{binding:'currentOperator',effectClass:'state-changing'},'outcome-qualification-refresh':{binding:'currentOperator',effectClass:'state-changing'},'admission-refresh':{binding:'currentOperator',effectClass:'state-changing'},'qualification-assessment':{binding:'qualificationWorker',effectClass:'state-changing'},'qualification-assessment-retirement':{binding:'qualificationWorker',effectClass:'report-only'},'lifecycle-retirement':{binding:'recipientOnly',effectClass:'report-only'},
 'frontier-planning':{binding:'currentHolder',effectClass:'state-changing'},'frontier-admission':{binding:'currentOperator',effectClass:'state-changing'},'frontier-confirmation':{binding:'currentOperator',effectClass:'state-changing'}
};
export interface DutyFacts {claim:boolean;send:boolean;act:boolean;complete:boolean;close:boolean;retired:boolean;failedByRecipient:boolean;superseded:boolean;expired:boolean;queueId:string;reason?:string}
export interface CoordinationActivity { generation:string; identityVerified:boolean; identityObservedAt?:string|null; state:ArbitratedSeatState; witness:ActivityEvidence|null }
/** A just-refreshed Worker observation carried across the async-to-sync staging boundary. */
interface PreparedNativeObservation { owner:string; generation:string; configurationDigest:string; identityObservedAt:string; activityObservedAt:string }
interface PreparedDispatchObservation extends PreparedNativeObservation { plan:string; authority:string; recoveryTargets:Record<string,PreparedNativeObservation|null> }
type DispatchScope=ReadonlyMap<string,PreparedDispatchObservation|null>;
export interface QualificationWorkerStageObservation { worker:string; generation:string; configurationDigest:string; identityObservedAt:string; activityObservedAt:string }
/** Exact retained-history form, or a pre-bound product successor naming an immutable admitted package instance. The milestone is resolved later from that exact instance; omission means accepted. */
export type CoordinationPredecessor={queueId:string;dispositionId:string}|{packageKey:string;contractHash:string;queueId:string;milestone?:'returned'|'accepted'};
type BoundPredecessor=Extract<CoordinationPredecessor,{packageKey:string}>;
interface ReturnedPredecessorResolution { dispositionId:string; milestone:'returned'; originalBodyHash:string; worker:string; claimedGeneration:string; returnBodyHash:string; returnDestination:string; contractBodyHash:string; dispositionReceiptHash:string }
const isBoundPredecessor=(p:CoordinationPredecessor):p is BoundPredecessor=>!!p&&typeof p==='object'&&'packageKey' in p;
export interface CoordinationTask {
 key:string; packageKey:string; owner:string; action:string; deadline:number; body:string; recoveryFor?:string;
 predecessors:CoordinationPredecessor[];
 admission:{generation:string;configurationDigest:string;qualificationRef:string;capacityRef:string;effortRef:string;validUntil:number;runtimeRequirements?:DispatchRuntimeRequirements};
 /** Owner boundary affects this slice only. Recovery work is a separate admitted task. */
 boundary?:"owner-access"|"owner-credential"|"owner-material"|"owner-irreversible";
}
export interface CoordinationPlan { rigId:string; revision:string; operatorGeneration:string; stallMs:number; allowIdlePeerTransfer:boolean; allowUnavailablePeerTransfer?:boolean; acknowledgmentWindowMs?:number; refreshDispatchIdentity?:boolean; dispatchRestrictions?:Array<{session:string;generation:string;packageKeys:string[];validUntil:number;evidenceRef:string;checkpointDisposition?:'release-listed-packages'}>; tasks:CoordinationTask[]; /** Goal-defining artifacts the genuine current Operator binds. Refs and digests only: the runtime never reads or interprets their contents, and prepared snapshots are not permission. */
 scopeSources?:ScopeSource[];
 /** Bounded stabilization configuration for the next-work planning obligation. Not a retry budget. */
 frontierPlanning?:{stabilizationObservations?:number;stabilizationMs?:number} }
export interface CoordinationResult { key:string; state:string; queueId?:string; reason?:string; deadline:number; activityEvidence?:Record<string,unknown>; /** Exact accountable intake subject when a hold has no duty row: the reserved package key, its native owner and the frozen obligation identity. */
 subject?:{packageKey:string;owner:string;identity:string} }
const successfulReturn=(state:string,disposition:string|null):boolean=>!!disposition&&['done','handed-off'].includes(state);
/** Only the exact migration091 refusal is normalized, never arbitrary SQL failures. */
/** Held outcomes that must keep an accountable Operator intake, whether it is first
   * staged or renewed. Any other hold stays a bare observation. */
export const FRONTIER_INTAKE_ROUTED_REASONS=['frontier-planning-duty-exhausted','frontier-boundary-blocked','frontier-boundary-declined','frontier-operator-absent'] as readonly string[];
export const LIFECYCLE_INTAKE_RENEWAL_REASONS=['lifecycle-duty-exhausted','lifecycle-duty-expired-unclaimed','lifecycle-recipient-protected','lifecycle-return-contract-drift',...FRONTIER_INTAKE_ROUTED_REASONS] as readonly string[];
/** An expired dispatch scope is an accountable Operator boundary, not a silent hold: the
 *  restriction itself is never renewed or released by the intake it raises. */
export const INTAKE_ROUTED_REASONS=['runtime-not-ready','uncertain-worker-effect','existing-queue-without-assignment','deterministic-queue-conflict','terminal-return-incarnation-changed','terminal-return-contract-drift','terminal-return-duty-exhausted','terminal-return-seat_dispatch_reserved','terminal-return-coordinator_resource_conflict','lifecycle-duty-exhausted','lifecycle-duty-expired-unclaimed','lifecycle-recipient-protected','lifecycle-return-contract-drift','dispatch-scope-expired',...FRONTIER_INTAKE_ROUTED_REASONS] as readonly string[];
function heldDispatchCode(error:unknown):string|undefined {
 const e=error as {code?:string;message?:string};
 if(e.code==='SQLITE_CONSTRAINT_TRIGGER'&&e.message==='seat_dispatch_reserved')return 'seat_dispatch_reserved';
 return ['coordinator_resource_conflict','seat_dispatch_reserved'].includes(e.code??'')?e.code:undefined;
}
const fail=(code:string,message:string):never=>{throw new CoordinatorFenceError(code,message);};
/** Same-observation proof; public display/old hook timestamps cannot manufacture idle.
 * lastSwap records an observed handover, not an initial-generation admission.
 * After restart it is null: independently verified current identity/generation plus
 * fresh deciding evidence remain required. A present swap must match the managed
 * generation UUID and the witness cannot predate its invalidation watermark. */
export function coordinationIdle(sample:CoordinationActivity|null,generation:string,now:number):boolean {
 if(!sample||!sample.identityVerified||sample.generation!==generation||(sample.state.lastSwap!==null&&sample.state.lastSwap.generation!==generation)||sample.state.needsInput.count!==0)return false;
 const w=sample.witness,at=Date.parse(w?.observedAt??"");
 return !!w&&w.sessionName.length>0&&w.seatNodeId===sample.state.seatNodeId&&w.activity==="idle-at-prompt"&&sample.state.activity===w.activity&&sample.state.decidedBy===w.rung&&Number.isFinite(at)&&at<=now&&now-at<=3000&&(!sample.state.lastSwap||at>=Date.parse(sample.state.lastSwap.at));
}
/** Durable plans use the existing append-only operation store, with queue/resource
 * mutations in one SQLite transaction. Reconciliation never manufactures worker claims. */
export class CoordinationRecoveryService {
 constructor(private repo:QueueRepository,private activity:(session:string)=>CoordinationActivity|null,private now:()=>number=Date.now,private refreshIdentity?:(sessions:readonly string[])=>Promise<void>,private refreshWorkerActivity?:(session:string)=>Promise<void>,private runtimeReadiness?:(task:CoordinationTask)=>DispatchRuntimeHold|null,private nativeRecoveryContinuation?:NativeRecoveryContinuationRuntime){}
 authorizeTerminalReturnSuccessor(actor:string,generation:string,input:{rigId:string;intakeQueueId:string;previousControlId:string;previousBodyHash:string;workerGeneration:string;holderGeneration:string;deadline:number;operationId:string}):{queueId:string} {
  return this.db.transaction(()=>{
   if(actor!=='operator-agent@kernel'||!generation||this.authority.generation(actor)!==generation)fail('coordination_operator_required','Current native Operator required');
   const id='native-return-successor:'+input.operationId,requestHash=digest(JSON.stringify({actor,generation,input}));
   if(!input.operationId||input.operationId.length>160)fail('coordination_return_successor_required','Bounded operation ID required');
   const saved=this.db.prepare("SELECT receipt,request_hash FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='native-terminal-return-successor-authorization'").get(input.rigId,id) as {receipt:string;request_hash:string}|undefined;
   if(saved){if(saved.request_hash!==requestHash)fail('coordination_return_successor_conflict','Frozen successor authorization differs');return {queueId:JSON.parse(saved.receipt).queueId};}
   const claimGeneration=(id:string)=>(this.db.prepare('SELECT claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(id) as {claimed_by_generation_uuid:string|null}|undefined)?.claimed_by_generation_uuid;
   const intake=this.repo.getById(input.intakeQueueId),previous=this.repo.getById(input.previousControlId),a=this.authority.get(input.rigId),plan=this.plan(input.rigId);
   const row=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='native-terminal-return-control'").get(input.rigId,input.previousControlId) as {receipt:string}|undefined;
   const r=row?JSON.parse(row.receipt):null,b=previous?JSON.parse(previous.body):null,h=intake?JSON.parse(intake.body):null;
   const creation=this.db.prepare('SELECT state,actor_session,identity_provenance FROM queue_transitions WHERE qitem_id=? ORDER BY transition_id LIMIT 1').get(input.intakeQueueId) as any;
   const expectedIntake=h?'qitem-coordination-task-hold-'+digest(JSON.stringify({rigId:input.rigId,revision:h.planRevision,operatorGeneration:generation,packageKey:h.packageKey,reason:'terminal-return-duty-exhausted',queueId:input.previousControlId})).slice(0,24):null;
   if(!intake||intake.sourceSession!=='watchdog@system'||!creation||creation.state!=='pending'||creation.actor_session!=='watchdog@system'||creation.identity_provenance!=='system:operator-authorized-coordination'||(input.intakeQueueId!==expectedIntake&&!this.refreshedIntakeAuthorized(input.rigId,input.intakeQueueId,input.previousControlId,generation,h?.packageKey)&&!this.lineageIntakeAuthorized(input.rigId,input.intakeQueueId,input.previousControlId,generation,h?.packageKey))||h.recipientGeneration!==generation||h.grantsAuthority!==false||h.deadline!==Date.parse(intake.expiresAt??'')||!this.db.prepare("SELECT 1 FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='coordination-plan' AND json_extract(receipt,'$.operatorGeneration')=?").get(input.rigId,'coordination-plan:'+h.planRevision,generation))fail('coordination_return_successor_required','Exact native exhaustion intake provenance required');
   if(!intake||intake.destinationSession!==actor||claimGeneration(input.intakeQueueId)!==generation||!['in-progress','blocked'].includes(intake.state)||!intake.expiresAt||Date.parse(intake.expiresAt)<=this.now()||h.action!=='resolve-exact-coordination-task-hold'||h.reason!=='terminal-return-duty-exhausted'||h.rigId!==input.rigId||h.retainedQueueId!==input.previousControlId||!r||!previous||!['done','failed','denied','canceled','handed-off'].includes(previous.state)||digest(previous.body)!==input.previousBodyHash||r.bodyHash!==input.previousBodyHash||r.workerGeneration!==input.workerGeneration||claimGeneration(input.previousControlId)!==input.workerGeneration||this.authority.generation(r.worker)!==input.workerGeneration||h.packageKey!==r.packageKey||!a||a.state!=='active'||a.lease_until<=this.now()||a.owner_generation!==input.holderGeneration||this.authority.generation(a.owner_session!)!==input.holderGeneration||!plan||plan.operatorGeneration!==generation||!Number.isSafeInteger(input.deadline)||input.deadline<=this.now()||input.deadline>this.now()+1200000)fail('coordination_return_successor_required','Exact claimed exhaustion intake, immutable prior native duty, current holder and finite authorization required');
   if(this.db.prepare("SELECT 1 FROM coordinator_operations WHERE rig_id=? AND kind='native-terminal-return-successor-authorization' AND json_extract(receipt,'$.previousControlId')=?").get(input.rigId,input.previousControlId))fail('coordination_return_successor_conflict','This prior duty already has its one authorized successor');
   if(this.workerEffectDebt(r.worker)||this.db.prepare("SELECT * FROM outbox_entries WHERE audit_pointer=? AND delivery_state IN ('pending','sending','indeterminate')").all(input.previousControlId).some(row=>!this.noticeOutcomeContained(input.rigId,row)))fail('coordination_return_successor_unknown_effect','Reconcile unknown effects before authorizing a successor');
   const queueId='qitem-coordination-terminal-return-'+digest(input.rigId+':'+b.originalQueueId+':'+input.workerGeneration+':'+id).slice(0,24);
   const receipt={queueId,originalQueueId:b.originalQueueId,packageKey:r.packageKey,workerGeneration:input.workerGeneration,holderGeneration:input.holderGeneration,operatorGeneration:generation,previousControlId:input.previousControlId,previousBodyHash:input.previousBodyHash,intakeQueueId:input.intakeQueueId,intakeBodyHash:digest(intake!.body),expiresAt:input.deadline};
   this.db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run(input.rigId,id,'native-terminal-return-successor-authorization',JSON.stringify(receipt),requestHash);
   this.repo.createNativeTerminalReturnDuty(a!.owner_session!,a!.owner_generation!,input.rigId,{qitemId:queueId,sourceSession:'watchdog@system',destinationSession:r.worker,expiresAt:new Date(input.deadline).toISOString(),body:JSON.stringify({...b,authorizationId:id,previousControlId:input.previousControlId,deadline:input.deadline,disposeContract:{rigId:input.rigId,packageKey:r.packageKey,dispositionId:'<new worker-authored typed-return queue item ID>'},required:'Claim this successor under your genuine identity; preserve prior reports. Create your durable JSON return to the original returnContract.destination with {packageKey,inputDigest,evidence:[{kind,ref}]} covering every original evidenceRequired kind, then invoke supported coordinator dispose with original packageKey and that return queue ID. Only then close this duty. No product writes, reruns, acceptance or historical reopening.'}),identityProvenance:'system:operator-authorized-coordination',nudge:true});
   this.repo.stageWakeIntent(queueId,'watchdog@system',r.worker,'system:operator-authorized-coordination',true,input.workerGeneration);
   return {queueId};
  })();
 }
 /** A correction notice continues the exact claimed control; it creates no queue
  * assignment and cannot change its custody, original return or resource locks. */
 private terminalReturnContinuationContext(rigId:string,controlQueueId:string,bodyHash:string,workerGeneration:string,deadline:number,excludeEffect?:string|string[],retirement=false) {
  const op=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='native-terminal-return-control'").get(rigId,controlQueueId) as {receipt:string}|undefined;
  const control=this.db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(controlQueueId) as any;
  const r=op?JSON.parse(op.receipt):null,b=control?JSON.parse(control.body):null,a=this.authority.get(rigId),plan=this.plan(rigId);
  const original=r?this.db.prepare("SELECT a.destination,a.body_hash,a.disposition_id,q.body,q.state,q.claimed_by_generation_uuid,p.contract FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id JOIN coordinator_packages p ON p.rig_id=a.rig_id AND p.package_key=a.package_key WHERE a.rig_id=? AND a.package_key=? AND a.queue_id=?").get(rigId,r.packageKey,r.originalQueueId) as any:null;
  const contract=original?JSON.parse(original.contract):null;
  if(!r||!control||r.bodyHash!==bodyHash||digest(control.body)!==bodyHash||control.source_session!=='watchdog@system'||control.destination_session!==r.worker||!['in-progress','blocked'].includes(control.state)||!workerGeneration||control.claimed_by_generation_uuid!==workerGeneration||r.workerGeneration!==workerGeneration||this.authority.generation(r.worker)!==workerGeneration||Date.parse(control.expires_at)!==r.expiresAt||!Number.isSafeInteger(deadline)||deadline<=this.now()||(!retirement&&(deadline>r.expiresAt||r.expiresAt<=this.now()))||(retirement&&r.expiresAt>this.now())||b.action!=='record-exact-native-terminal-return'||b.rigId!==rigId||b.originalQueueId!==r.originalQueueId||b.packageKey!==r.packageKey||b.recipientGeneration!==workerGeneration||!original||original.disposition_id||original.destination!==r.worker||original.claimed_by_generation_uuid!==workerGeneration||!['done','failed','denied','canceled','handed-off'].includes(original.state)||digest(original.body)!==original.body_hash||contract.destination!==r.worker||contract.bodyHash!==original.body_hash||b.inputDigest!==contract.inputDigest||JSON.stringify(b.returnContract)!==JSON.stringify(contract.returnContract)||!this.authority.terminalReturnResourcesRetained(rigId,r.packageKey,contract)||!a||a.state!=='active'||a.lease_until<=this.now()||this.authority.generation(a.owner_session)!==a.owner_generation||!plan||this.authority.generation('operator-agent@kernel')!==plan.operatorGeneration)fail('coordination_return_continuation_required','Exact immutable active claimed unexpired native return duty, original claimant and current live authority required; expired duties must use supported exhaustion recovery');
  const restriction=plan!.dispatchRestrictions?.find(scope=>scope.session===r.worker);
  if(restriction&&(restriction.generation!==workerGeneration||restriction.validUntil<=this.now()||!restriction.packageKeys.includes(r.packageKey)))fail('coordination_return_continuation_protected','Checkpoint scope excludes this native return continuation');
  for(const session of [r.worker,a!.owner_session,'operator-agent@kernel'])if(this.db.prepare("SELECT 1 FROM seat_dispatch_reservations WHERE state!='released' AND (session_name=? OR node_id IN (SELECT node_id FROM sessions WHERE session_name=?))").get(session,session))fail('coordination_return_continuation_protected','Native seat reservation excludes this continuation');
  if(this.workerEffectDebt(r.worker,excludeEffect))fail('coordination_return_continuation_unknown_effect','Reconcile unknown effects before continuing native return custody');
  if(retirement){const observation=this.activity(r.worker),at=Date.parse(observation?.identityObservedAt??'');if(!Number.isFinite(at)||at>this.now()||this.now()-at>3000||!coordinationIdle(observation,workerGeneration,this.now()))fail('coordination_return_retirement_idle_required','Retirement requires fresh verified native idle; active or unknown workers remain protected');}
  return {r,control,receiptHash:digest(op!.receipt),a:a!,plan:plan!,contract};
 }
 private validContinuationReturn(queueId:string,worker:string,workerGeneration:string,packageKey:string,contract:any):boolean {
  const row=this.db.prepare('SELECT source_session,destination_session,minting_generation_uuid,body FROM queue_items WHERE qitem_id=?').get(queueId) as any;
  let body:any;try{body=row?JSON.parse(row.body):null;}catch{return false;}
  return !!row&&row.source_session===worker&&row.destination_session===contract.returnContract.destination&&row.minting_generation_uuid===workerGeneration&&body?.packageKey===packageKey&&body.inputDigest===contract.inputDigest&&Array.isArray(body.evidence)&&contract.returnContract.evidenceRequired.every((kind:string)=>body.evidence.some((e:any)=>e&&e.kind===kind&&typeof e.ref==='string'&&e.ref.length>0));
 }
 continueTerminalReturn(actor:string,generation:string,input:{rigId:string;controlQueueId:string;controlBodyHash:string;workerGeneration:string;deadline:number;dispositionId?:string}):{queueId:string;outboxId:string;deadline:number} {
  return this.db.transaction(()=>{
   const a=this.authority.get(input.rigId);
   if(!generation||this.authority.generation(actor)!==generation||!(actor==='operator-agent@kernel'||(a?.owner_session===actor&&a.owner_generation===generation)))fail('coordination_return_continuation_required','Genuine current Operator or live holder required');
   const id='native-return-continuation:'+input.controlQueueId,requestHash=digest(JSON.stringify({actor,generation,input}));
   const prior=this.db.prepare("SELECT receipt,request_hash FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='native-terminal-return-continuation'").get(input.rigId,id) as {receipt:string;request_hash:string}|undefined;
   if(prior){if(prior.request_hash!==requestHash)fail('coordination_return_continuation_conflict','This duty already has its one frozen correction notice');const r=JSON.parse(prior.receipt);return {queueId:r.controlQueueId,outboxId:r.outboxId,deadline:r.deadline};}
   const context=this.terminalReturnContinuationContext(input.rigId,input.controlQueueId,input.controlBodyHash,input.workerGeneration,input.deadline);
   if(input.deadline>this.now()+1200000)fail('coordination_return_continuation_required','Correction notice must expire within twenty minutes and the existing duty expiry');
   const {r,contract}=context;
   let dispositionId=input.dispositionId;
   if(dispositionId&&!this.validContinuationReturn(dispositionId,r.worker,input.workerGeneration,r.packageKey,contract))fail('coordination_return_continuation_required','Referenced return must be the original worker incarnation\'s genuine typed return under its admitted contract');
   if(!dispositionId){const returns=this.db.prepare('SELECT qitem_id FROM queue_items WHERE source_session=? AND destination_session=? AND minting_generation_uuid=? AND json_valid(body) ORDER BY rowid DESC').all(r.worker,contract.returnContract.destination,input.workerGeneration) as Array<{qitem_id:string}>;dispositionId=returns.find(row=>this.validContinuationReturn(row.qitem_id,r.worker,input.workerGeneration,r.packageKey,contract))?.qitem_id;}
   const outboxId=this.repo.stageNativeTerminalReturnContinuation({controlQueueId:input.controlQueueId,worker:r.worker,workerGeneration:input.workerGeneration,proofId:id,body:JSON.stringify({action:'continue-exact-native-terminal-return',controlQueueId:input.controlQueueId,originalQueueId:r.originalQueueId,deadline:input.deadline,grantsAuthority:false,disposeContract:{rigId:input.rigId,packageKey:r.packageKey,dispositionId:dispositionId??'<new worker-authored typed-return queue item ID>'},returnContract:contract.returnContract,required:'Continue the same already-claimed duty; do not claim new work or repeat product work. Reuse your genuine retained typed return if identified here. Otherwise author your JSON {packageKey,inputDigest,evidence:[{kind,ref}]} return to returnContract.destination. Submit exactly {rigId,packageKey,dispositionId} to rig coordinator dispose using that actual new return queue ID. Successful duty closure requires the resulting original disposition. Preserve original evidence, unknown effects, checkpoint scope and locks; report a supported refusal. This correction grants no acceptance or automatic disposal.'})});
   const effect=this.db.prepare('SELECT body FROM outbox_entries WHERE outbox_id=?').get(outboxId) as {body:string};
   const receipt={controlQueueId:input.controlQueueId,controlBodyHash:input.controlBodyHash,controlReceiptHash:context.receiptHash,worker:r.worker,workerGeneration:input.workerGeneration,actor,actorGeneration:generation,holder:context.a.owner_session,holderGeneration:context.a.owner_generation,epoch:context.a.epoch,operatorGeneration:context.plan.operatorGeneration,deadline:input.deadline,outboxId,outboxBodyHash:digest(effect.body),...(dispositionId?{dispositionId}:{})};
   this.db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run(input.rigId,id,'native-terminal-return-continuation',JSON.stringify(receipt),requestHash);
   return {queueId:input.controlQueueId,outboxId,deadline:input.deadline};
  }).immediate();
 }
 private exactAccepted(rigId:string,queueId:string,dispositionId:string):boolean {return !!this.db.prepare("SELECT 1 FROM coordinator_operations WHERE rig_id=? AND kind='coordination-accept' AND json_extract(receipt,'$.queueId')=? AND json_extract(receipt,'$.dispositionId')=?").get(rigId,queueId,dispositionId);}
 /** Issue one finite, non-product assessment assignment before product qualification exists.
  * The immutable artifact contract is evidence only; it is deliberately not an admission. */
 stageQualificationAssessment(actor:string,generation:string,input:{rigId:string;worker:string;workerGeneration:string;configurationDigest:string;deadline:number;contract:{schema:'qualification-assessment-contract.v1';artifactRef:string;artifactSha256:string;taskDigest:string;scope:'qualification-only';productAuthority:false}},prepared?:QualificationWorkerStageObservation):{queueId:string;contractDigest:string;deadline:number} {
  return this.db.transaction(()=>{
   const a=this.authority.get(input.rigId),plan=this.plan(input.rigId),now=this.now();
   if(actor!=='operator-agent@kernel'||!generation||this.authority.generation(actor)!==generation||!a||a.state!=='active'||a.lease_until<=now||a.owner_session===null||a.owner_generation!==this.authority.generation(a.owner_session)||(plan&&plan.operatorGeneration!==generation))fail('qualification_duty_operator_required','Current native Operator and active coordinator holder required');
   if(!a||!a.owner_session)fail('qualification_duty_operator_required','Current native Operator and active coordinator holder required');
   const activeAuthority=a!,activePlan=plan,bootstrap=!activePlan;
   if(!Number.isSafeInteger(input.deadline)||input.deadline<=now||input.deadline>now+1200000)fail('qualification_duty_deadline_invalid','Assessment duty must have a finite deadline within twenty minutes');
   const c=input.contract;
   if(!c||c.schema!=='qualification-assessment-contract.v1'||c.scope!=='qualification-only'||c.productAuthority!==false||typeof c.artifactRef!=='string'||!c.artifactRef.trim()||!/^sha256:[a-f0-9]{64}$/.test(c.artifactSha256)||!/^sha256:[a-f0-9]{64}$/.test(c.taskDigest))fail('qualification_duty_contract_invalid','Immutable qualification-only artifact contract required');
   if(!this.db.prepare('SELECT 1 FROM sessions s JOIN nodes n ON n.id=s.node_id WHERE s.session_name=? AND n.rig_id=?').get(input.worker,input.rigId)||!input.workerGeneration||this.authority.generation(input.worker)!==input.workerGeneration||!input.configurationDigest||this.configurationDigest(input.worker)!==input.configurationDigest)fail('qualification_duty_worker_stale','Exact current Worker generation and configuration digest required');
   const contractDigest=digest(JSON.stringify(c)),semanticKey=digest(JSON.stringify({rigId:input.rigId,worker:input.worker,workerGeneration:input.workerGeneration,configurationDigest:input.configurationDigest,contractDigest})),rootId=this.dutyRootId(input.rigId,'qualification-assessment',semanticKey);
   const priorRow=this.db.prepare("SELECT operation_id,receipt FROM coordinator_operations WHERE rig_id=? AND kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.rootId')=? ORDER BY rowid DESC LIMIT 1").get(input.rigId,rootId) as {operation_id:string;receipt:string}|undefined;
   let previousQueueId:string|undefined,previousRetired=false,previousChainIds:string[]=[],queueId=rootId;
   if(priorRow){const prior=JSON.parse(priorRow.receipt),priorQueue=this.repo.getById(prior.queueId);if(prior.semanticKey!==semanticKey||prior.contractDigest!==contractDigest||prior.queueId!==priorRow.operation_id)fail('qualification_duty_conflict','Frozen qualification duty differs');if(!priorQueue)fail('qualification_duty_conflict','Frozen qualification duty row is missing');this.observeDuty(prior.queueId);const facts=this.dutyFacts(prior.queueId);if(facts.complete||(!facts.expired&&!facts.failedByRecipient))return {queueId:prior.queueId,contractDigest,deadline:prior.deadline};if(!facts.retired||!this.dutyNoticeContained(input.rigId,prior))return {queueId:prior.queueId,contractDigest,deadline:prior.deadline};previousQueueId=prior.queueId;previousChainIds=[prior.queueId,...this.bootstrapContainedAncestors(input.rigId,prior,'qualification-assessment')];previousRetired=true;queueId=this.dutySuccessorId(rootId,prior.queueId,this.dutyBindingVersion('qualification-assessment',input.workerGeneration,activeAuthority.epoch));}
   const existing=this.lifecycleControl(queueId);if(existing){if(existing.receipt.semanticKey!==semanticKey||existing.receipt.contractDigest!==contractDigest)return {queueId,contractDigest,deadline:existing.receipt.deadline};return {queueId,contractDigest,deadline:existing.receipt.deadline};}
   if((this.refreshIdentity||this.refreshWorkerActivity)&&!prepared)fail('qualification_duty_worker_not_quiescent','Synchronous assessment staging requires the fresh native observation prepared by its authenticated route');
   const observed=this.activity(input.worker),observedAt=Date.parse(observed?.identityObservedAt??'');
   if(!this.preparedQualificationObservationMatches(prepared,input.worker,input.workerGeneration,now,observed)||!observed?.identityVerified||observed.generation!==input.workerGeneration||!Number.isFinite(observedAt)||observedAt>now||now-observedAt>3000||!coordinationIdle(observed,input.workerGeneration,now))fail('qualification_duty_worker_not_quiescent','Fresh verified idle Worker identity and activity required');
   const containedCustody=this.containedQualificationCustody(input.rigId,input.worker,input.workerGeneration,input.configurationDigest,previousRetired?previousChainIds:[]);
   const activeCustody=(this.db.prepare("SELECT qitem_id FROM queue_items WHERE destination_session=? AND state IN ('pending','in-progress','blocked')").all(input.worker) as Array<{qitem_id:string}>).some(row=>!containedCustody.has(row.qitem_id));
   if(this.workerEffectDebt(input.worker)||this.db.prepare("SELECT 1 FROM seat_dispatch_reservations WHERE state!='released' AND (session_name=? OR node_id IN (SELECT node_id FROM sessions WHERE session_name=?))").get(input.worker,input.worker)||this.db.prepare('SELECT 1 FROM seat_delivery_guards WHERE (desired=1 OR effective=1) AND node_id IN (SELECT node_id FROM sessions WHERE session_name=?)').get(input.worker)||activeCustody)fail('qualification_duty_worker_protected','Worker has unresolved effects, existing custody, a reservation or a delivery guard');
   // A qualification exercise is not a product package and cannot shadow an admitted one.
   const packageKey='qualification-assessment:'+contractDigest;
   if(this.db.prepare('SELECT 1 FROM coordinator_packages WHERE rig_id=? AND package_key=?').get(input.rigId,packageKey))fail('qualification_duty_product_collision','Assessment key collides with an admitted package');
   const body=JSON.stringify({schema:'qualification-assessment-duty.v1',action:'perform-exact-qualification-only-assessment',rigId:input.rigId,queueId,previousQueueId,chainIds:[queueId,...previousChainIds].filter((id,index,ids)=>ids.indexOf(id)===index),issuer:'operator-agent@kernel',issuerGeneration:generation,holder:activeAuthority.owner_session,holderGeneration:activeAuthority.owner_generation,epoch:activeAuthority.epoch,planRevision:activePlan?.revision??null,...(bootstrap?{bootstrap:true}:{}),worker:input.worker,workerGeneration:input.workerGeneration,configurationDigest:input.configurationDigest,contract:c,contractDigest,deadline:input.deadline,scope:'qualification-only',grantsAuthority:false,required:'Claim this exact finite duty under your current native Worker identity. Perform only the frozen non-product assessment and create a typed return to the current Lead with schema qualification-assessment-return.v1, exact dutyQueueId, contractDigest, workerGeneration, configurationDigest, artifact ref/hash, and evidence refs. This duty grants no product admission, qualification PASS, acceptance, dispatch, resource access or scope waiver. Preserve UNKNOWN, locks, current identity and quiescence.'});
   const receipt={kind:'qualification-assessment',queueId,rootId,semanticKey,previousQueueId,chainIds:[queueId,...previousChainIds].filter((id,index,ids)=>ids.indexOf(id)===index),packageKey,rigId:input.rigId,recipient:input.worker,worker:input.worker,workerGeneration:input.workerGeneration,recipientGeneration:input.workerGeneration,configurationDigest:input.configurationDigest,contract:c,contractDigest,deadline:input.deadline,issuer:'operator-agent@kernel',operatorGeneration:generation,holder:activeAuthority.owner_session,holderGeneration:activeAuthority.owner_generation,epoch:activeAuthority.epoch,planRevision:activePlan?.revision??null,...(bootstrap?{bootstrap:true}:{}),bodyHash:digest(body)};
   this.repo.createQualificationAssessmentDuty({qitemId:queueId,sourceSession:'watchdog@system',destinationSession:input.worker,expiresAt:new Date(input.deadline).toISOString(),body,identityProvenance:'system:operator-authorized-coordination',nudge:false});
   this.repo.stageCoordinatorLifecycleWake(queueId,input.worker,input.workerGeneration);
   const notice=this.db.prepare('SELECT body FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+queueId) as {body:string}|undefined;
   if(!notice||typeof notice.body!=='string')fail('qualification_duty_wake_missing','Durable native duty wake required');
   this.db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run(input.rigId,queueId,'coordinator-lifecycle-control',JSON.stringify({...receipt,noticeBodyHash:digest(notice!.body)}),digest(body));
  return {queueId,contractDigest,deadline:input.deadline};
 }).immediate();
}
/** Explicitly contain expired legacy bootstrap rows whose wake linkage is absent.
 * This does not claim failed delivery or completed execution: it records both as
 * UNKNOWN, terminalizes only the exact stale queue records, and never retries a wake. */
 async recordQualificationAssessmentUncertainty(actor:string,generation:string,input:{rigId:string;rows:Array<{targetQueueId:string;targetBodyHash:string;sweepFindingQueueId:string;sweepFindingBodyHash:string}>;deadline:number}):Promise<{operationId:string;outcome:'unknown-preserved';wakeReplayed:false;custodyTransferred:false}> {
  if(!input||typeof input!=='object'||Array.isArray(input)||typeof (input as any).rigId!=='string'||(input as any).rigId.length<1||(input as any).rigId.length>256)fail('qualification_uncertainty_contract_invalid','A bounded string rigId is required');
  // Resolve only the exact requested targets before refreshing identity. This read
  // does not grant authority or change custody; the transaction below repeats every
  // row, hash, effect, authority and quiescence guard after the asynchronous refresh.
  if(!Array.isArray(input.rows)||input.rows.length<1||input.rows.length>4||input.rows.some((row:any)=>!row||typeof row!=='object'||Array.isArray(row)||typeof row.targetQueueId!=='string'||row.targetQueueId.length<1||row.targetQueueId.length>256||typeof row.sweepFindingQueueId!=='string'||row.sweepFindingQueueId.length<1||row.sweepFindingQueueId.length>256||typeof row.targetBodyHash!=='string'||typeof row.sweepFindingBodyHash!=='string'))fail('qualification_uncertainty_contract_invalid','One to four exact legacy pairs are required');
  const rows=[...input.rows].sort((a,b)=>a.targetQueueId.localeCompare(b.targetQueueId));
  if(new Set(rows.flatMap(r=>[r.targetQueueId,r.sweepFindingQueueId])).size!==rows.length*2||rows.some(r=>r.targetQueueId===r.sweepFindingQueueId||!/^([a-f0-9]{64})$/.test(r.targetBodyHash)||!/^([a-f0-9]{64})$/.test(r.sweepFindingBodyHash)))fail('qualification_uncertainty_contract_invalid','Each legacy target and sweep must have a unique ID and exact SHA-256 body hash');
  const preflightNow=this.now(),preflightAuthority=this.authority.get(input.rigId),preflightPlan=this.plan(input.rigId);
  if(actor!=='operator-agent@kernel'||!generation||this.authority.generation(actor)!==generation||!preflightAuthority||preflightAuthority.state!=='active'||preflightAuthority.lease_until<=preflightNow||!preflightAuthority.owner_session||preflightAuthority.owner_generation!==this.authority.generation(preflightAuthority.owner_session)||preflightPlan)fail('qualification_uncertainty_operator_required','Current native Operator, live authority and planless qualification bootstrap required');
  const requestIdentity={rigId:input.rigId,operatorGeneration:generation,rows,deadline:input.deadline};
  const operationId='qualification-assessment-uncertainty:'+digest(JSON.stringify(requestIdentity)).slice(0,32),requestHash=digest(JSON.stringify(requestIdentity));
  const previous=this.db.prepare("SELECT receipt,request_hash FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='qualification-assessment-uncertainty'").get(input.rigId,operationId) as {receipt:string;request_hash:string}|undefined;
  if(previous){if(previous.request_hash!==requestHash)fail('qualification_uncertainty_conflict','A different uncertainty disposition is already recorded');return {operationId,outcome:'unknown-preserved',wakeReplayed:false,custodyTransferred:false};}
  if(!Number.isSafeInteger(input.deadline))fail('qualification_uncertainty_contract_invalid','Finite disposition deadline required');
  const workers=new Set(rows.map(row=>this.repo.getById(row.targetQueueId)?.destinationSession).filter((value):value is string=>typeof value==='string'));
  if(workers.size!==1||rows.some(row=>!this.repo.getById(row.targetQueueId)))fail('qualification_uncertainty_target_invalid','Exact legacy qualification targets must share one current Worker');
  const worker=[...workers][0]!,workerGenerationBefore=this.authority.generation(worker);
  if(!workerGenerationBefore||!this.db.prepare('SELECT 1 FROM sessions s JOIN nodes n ON n.id=s.node_id WHERE s.session_name=? AND n.rig_id=?').get(worker,input.rigId))fail('qualification_uncertainty_worker_not_quiescent','Current native Worker identity and rig binding required');
  try {
   await Promise.all([this.refreshIdentity?.([worker]),this.refreshWorkerActivity?.(worker)]);
  } catch {
   fail('qualification_uncertainty_worker_not_quiescent','Fresh native Worker identity and idle observation could not be obtained');
  }
  if(this.authority.generation(worker)!==workerGenerationBefore)fail('qualification_uncertainty_worker_not_quiescent','Worker generation changed during the native identity refresh');
  return this.db.transaction(()=>{
   const now=this.now(),authority=this.authority.get(input.rigId),plan=this.plan(input.rigId);
   if(actor!=='operator-agent@kernel'||!generation||this.authority.generation(actor)!==generation||!authority||authority.state!=='active'||authority.lease_until<=now||!authority.owner_session||authority.owner_generation!==this.authority.generation(authority.owner_session)||plan)fail('qualification_uncertainty_operator_required','Current native Operator, live authority and planless qualification bootstrap required');
   if(!Number.isSafeInteger(input.deadline)||input.deadline>now+1200000||!Array.isArray(input.rows)||input.rows.length<1||input.rows.length>4)fail('qualification_uncertainty_contract_invalid','One to four exact legacy pairs and a finite deadline within twenty minutes are required');
   if(input.rows.some((row:any)=>!row||typeof row!=='object'||Array.isArray(row)||typeof row.targetQueueId!=='string'||row.targetQueueId.length<1||row.targetQueueId.length>256||typeof row.sweepFindingQueueId!=='string'||row.sweepFindingQueueId.length<1||row.sweepFindingQueueId.length>256||typeof row.targetBodyHash!=='string'||typeof row.sweepFindingBodyHash!=='string'))fail('qualification_uncertainty_contract_invalid','Each legacy pair must contain bounded queue IDs and string body hashes');
   const rows=[...input.rows].sort((a,b)=>a.targetQueueId.localeCompare(b.targetQueueId));
   if(new Set(rows.flatMap(r=>[r.targetQueueId,r.sweepFindingQueueId])).size!==rows.length*2||rows.some(r=>r.targetQueueId===r.sweepFindingQueueId||!/^([a-f0-9]{64})$/.test(r.targetBodyHash)||!/^([a-f0-9]{64})$/.test(r.sweepFindingBodyHash)))fail('qualification_uncertainty_contract_invalid','Each legacy target and sweep must have a unique ID and exact SHA-256 body hash');
   const committed=this.db.prepare("SELECT receipt,request_hash FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='qualification-assessment-uncertainty'").get(input.rigId,operationId) as {receipt:string;request_hash:string}|undefined;
   if(committed){if(committed.request_hash!==requestHash)fail('qualification_uncertainty_conflict','A different uncertainty disposition is already recorded');return {operationId,outcome:'unknown-preserved' as const,wakeReplayed:false as const,custodyTransferred:false as const};}
   if(input.deadline<=now)fail('qualification_uncertainty_contract_invalid','Finite disposition deadline required');
   const pairs=rows.map(binding=>{
    const target=this.repo.getById(binding.targetQueueId),sweep=this.repo.getById(binding.sweepFindingQueueId);
    if(!target)fail('qualification_uncertainty_target_invalid','Exact legacy qualification target is missing');
    const validTarget=target!;
    const targetExpiresAt=Date.parse(validTarget.expiresAt??'');
    if(validTarget.sourceSession!=='operator-agent@kernel'||validTarget.state!=='pending'||validTarget.claimedAt||!Number.isFinite(targetExpiresAt)||targetExpiresAt>now||digest(validTarget.body)!==binding.targetBodyHash||!validTarget.body.toLowerCase().includes('qualification')||!validTarget.body.toLowerCase().includes('assessment'))fail('qualification_uncertainty_target_invalid','Exact expired, unclaimed legacy qualification row and body hash required');
    if(this.db.prepare('SELECT 1 FROM coordinator_assignments WHERE queue_id=?').get(binding.targetQueueId)||this.db.prepare('SELECT 1 FROM coordinator_packages WHERE rig_id=? AND package_key=?').get(input.rigId,binding.targetQueueId))fail('qualification_uncertainty_target_invalid','Assigned product work remains protected');
    if(!sweep||sweep.sourceSession!=='operator-agent@kernel'||sweep.destinationSession!==validTarget.destinationSession||sweep.state!=='pending'||sweep.claimedAt||digest(sweep.body)!==binding.sweepFindingBodyHash||!sweep.body.includes('STUCK SWEEP FINDING (undelivered-wake)')||!sweep.body.includes('row: '+binding.targetQueueId))fail('qualification_uncertainty_sweep_invalid','Exact unclaimed sweep finding for the same Worker is required');
    const validSweep=sweep!,worker=validTarget.destinationSession,generationNow=this.authority.generation(worker),observation=this.activity(worker),observedAt=Date.parse(observation?.identityObservedAt??'');
    if(this.db.prepare('SELECT 1 FROM coordinator_assignments WHERE queue_id=?').get(binding.sweepFindingQueueId)||this.db.prepare('SELECT 1 FROM coordinator_packages WHERE rig_id=? AND package_key=?').get(input.rigId,binding.sweepFindingQueueId))fail('qualification_uncertainty_sweep_invalid','Assigned sweep work remains protected');
    if(!this.db.prepare('SELECT 1 FROM sessions s JOIN nodes n ON n.id=s.node_id WHERE s.session_name=? AND n.rig_id=?').get(worker,input.rigId)||!generationNow||!observation?.identityVerified||observation.generation!==generationNow||!Number.isFinite(observedAt)||observedAt>now||now-observedAt>3000||!coordinationIdle(observation,generationNow,now)||!this.configurationDigest(worker))fail('qualification_uncertainty_worker_not_quiescent','Fresh current native idle Worker identity and configuration required');
    const linked=this.db.prepare('SELECT outbox_id,delivery_state FROM outbox_entries WHERE audit_pointer IN (?,?) OR outbox_id IN (?,?,?)').all(binding.targetQueueId,binding.sweepFindingQueueId,'wake-intent-'+binding.targetQueueId,'wake-intent-'+binding.sweepFindingQueueId,binding.targetQueueId) as Array<{outbox_id:string;delivery_state:string}>;
    if(linked.length)fail('qualification_uncertainty_effect_linked','A target or sweep wake receipt exists; reconcile its exact delivery state through the existing supported path');
    return {binding,target:validTarget,sweep:validSweep,worker,workerGeneration:generationNow,configurationDigest:this.configurationDigest(worker)!,observationAt:new Date(observedAt).toISOString()};
   });
   const first=pairs[0]!;
   if(pairs.some(pair=>pair.worker!==first.worker||pair.workerGeneration!==first.workerGeneration||pair.configurationDigest!==first.configurationDigest))fail('qualification_uncertainty_worker_mismatch','All exact legacy pairs must retain one current Worker generation and configuration');
   const expectedIds=new Set(pairs.flatMap(pair=>[pair.target.qitemId,pair.sweep.qitemId]));
   const active=this.db.prepare("SELECT qitem_id FROM queue_items WHERE destination_session=? AND state IN ('pending','in-progress','blocked')").all(first.worker) as Array<{qitem_id:string}>;
   if(active.some(row=>!expectedIds.has(row.qitem_id))||this.workerEffectDebt(first.worker)||this.db.prepare("SELECT 1 FROM seat_dispatch_reservations WHERE state!='released' AND (session_name=? OR node_id IN (SELECT node_id FROM sessions WHERE session_name=?))").get(first.worker,first.worker)||this.db.prepare('SELECT 1 FROM seat_delivery_guards WHERE (desired=1 OR effective=1) AND node_id IN (SELECT node_id FROM sessions WHERE session_name=?)').get(first.worker))fail('qualification_uncertainty_worker_protected','Worker has unrelated queue custody, unresolved effects, a reservation or a delivery guard');
   const liveAuthority=authority!;
   const semantic={...requestIdentity,holder:liveAuthority.owner_session,holderGeneration:liveAuthority.owner_generation,epoch:liveAuthority.epoch,worker:first.worker,workerGeneration:first.workerGeneration,configurationDigest:first.configurationDigest,rows:pairs.map(pair=>pair.binding)};
   const receipt={...semantic,outcome:'unknown-preserved',queueDisposition:'terminalized-unresolvable',wakeDelivery:'unknown-not-proven-failed',taskExecution:'unknown',wakeReplayed:false,custodyTransferred:false,recordedAt:now,observationAt:first.observationAt};
   this.db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run(input.rigId,operationId,'qualification-assessment-uncertainty',JSON.stringify(receipt),requestHash);
   for(const pair of pairs){
    const note=`qualification uncertainty disposition ${operationId}: expired unclaimed legacy queue record terminalized as unresolvable; wake delivery UNKNOWN (no failure inferred), task execution UNKNOWN; no wake replay and no custody transfer`;
    this.repo.update({qitemId:pair.target.qitemId,actorSession:actor,actorGeneration:generation,identityProvenance:'transport:v1',state:'failed',transitionNote:note});
    this.repo.update({qitemId:pair.sweep.qitemId,actorSession:actor,actorGeneration:generation,identityProvenance:'transport:v1',state:'failed',transitionNote:note});
   }
   return {operationId,outcome:'unknown-preserved' as const,wakeReplayed:false as const,custodyTransferred:false as const};
  }).immediate();
 }
 qualificationAssessmentUncertaintyAllows(queueId:string,actor:string,generation:string|null|undefined,provenance:string|null|undefined,nextState:string|undefined):boolean {
  if(nextState!=='failed'||actor!=='operator-agent@kernel'||provenance!=='transport:v1'||!generation||this.authority.generation(actor)!==generation)return false;
  const row=this.repo.getById(queueId);if(!row||row.state!=='pending'||row.claimedAt)return false;
  const rows=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id IN (SELECT n.rig_id FROM sessions s JOIN nodes n ON n.id=s.node_id WHERE s.session_name=?) AND kind='qualification-assessment-uncertainty'").all(row.destinationSession) as Array<{receipt:string}>;
  return rows.some(entry=>{try{const r=JSON.parse(entry.receipt);return r.operatorGeneration===generation&&r.outcome==='unknown-preserved'&&r.rows.some((binding:any)=>binding.targetQueueId===queueId&&binding.targetBodyHash===digest(row.body)||binding.sweepFindingQueueId===queueId&&binding.sweepFindingBodyHash===digest(row.body));}catch{return false;}});
 }
 isQualificationAssessmentUncertaintyTarget(queueId:string):boolean {
  const rows=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='qualification-assessment-uncertainty'").all() as Array<{receipt:string}>;
  return rows.some(entry=>{try{const r=JSON.parse(entry.receipt);return r.rows.some((binding:any)=>binding.targetQueueId===queueId||binding.sweepFindingQueueId===queueId);}catch{return false;}});
 }
 private qualificationRetirementAccountabilityMatches(rigId:string,r:any):boolean {
  if(r.evidenceKind!=='operator-accountability'||typeof r.accountabilityControlQueueId!=='string'||typeof r.accountabilityControlBodyHash!=='string')return false;
  const row=this.repo.getById(r.accountabilityControlQueueId),claim=this.db.prepare('SELECT claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(r.accountabilityControlQueueId) as any,claimProof=this.db.prepare("SELECT 1 FROM queue_transitions WHERE qitem_id=? AND actor_session='operator-agent@kernel' AND identity_provenance='transport:v1' AND state='in-progress' LIMIT 1").get(r.accountabilityControlQueueId);
  if(!row||row.qitemId===r.targetQueueId||row.sourceSession!=='watchdog@system'||row.destinationSession!=='operator-agent@kernel'||!row.claimedAt||!['in-progress','blocked','done'].includes(row.state)||claim?.claimed_by_generation_uuid!==r.operatorGeneration||!claimProof||digest(row.body)!==r.accountabilityControlBodyHash)return false;
  let body:any;try{body=JSON.parse(row.body);}catch{return false;}
  const original=body?.original,target=this.repo.getById(r.targetQueueId);
  return body.action==='reconcile-refused-stuck-finding'&&body.kind==='undelivered-wake'&&body.reason==='coordinator_dispatch_required'&&typeof body.stuckSweepRecoveryKey==='string'&&body.stuckSweepRecoveryKey.length>0&&original?.qitemId===r.targetQueueId&&original?.sourceSession==='watchdog@system'&&target?.sourceSession===original.sourceSession&&original?.destinationSession===r.worker&&target?.destinationSession===original.destinationSession&&original?.bodyHash===r.targetBodyHash&&digest(target?.body??'')===r.targetBodyHash&&original?.state==='pending'&&typeof original?.evidenceAt==='string'&&typeof original?.factsHash==='string'&&/^[a-f0-9]{64}$/.test(original.factsHash)&&body.intendedRoute===r.worker&&body.recipientGeneration===r.operatorGeneration;
 }
 private qualificationRetirementWakeFailed(queueId:string):boolean {
  const target=this.repo.getById(queueId),linked=this.db.prepare('SELECT outbox_id,sender_session,destination_session,audit_pointer,delivery_state FROM outbox_entries WHERE audit_pointer=? OR outbox_id=?').all(queueId,'wake-intent-'+queueId) as Array<{outbox_id:string;sender_session:string;destination_session:string;audit_pointer:string;delivery_state:string}>;
  return !!target&&linked.some(effect=>effect.outbox_id==='wake-intent-'+queueId&&effect.sender_session===target.sourceSession&&effect.destination_session===target.destinationSession&&effect.audit_pointer===queueId&&effect.delivery_state==='failed')&&linked.every(effect=>effect.delivery_state==='failed');
 }
 /** Give the current Worker a finite, report-only path to retire an expired legacy
  *  qualification task whose original wake is known failed. No effect is replayed and
  *  the original row and wake history remain append-only. */
 stageQualificationAssessmentRetirement(actor:string,generation:string,input:{rigId:string;targetQueueId:string;targetBodyHash:string;deadline:number;sweepFindingQueueId?:string;sweepFindingBodyHash?:string;evidenceKind?:'operator-accountability';accountabilityControlQueueId?:string;accountabilityControlBodyHash?:string},prepared?:QualificationWorkerStageObservation):{queueId:string;deadline:number;targetQueueId:string} {
  return this.db.transaction(()=>{
   const now=this.now(),authority=this.authority.get(input.rigId),plan=this.plan(input.rigId),target=this.repo.getById(input.targetQueueId),accountability=input.evidenceKind==='operator-accountability',sweep=accountability?null:this.repo.getById(input.sweepFindingQueueId??'');
   const hash=(value:unknown):value is string=>typeof value==='string'&&/^[a-f0-9]{64}$/.test(value),validAccountability=accountability&&typeof input.accountabilityControlQueueId==='string'&&input.accountabilityControlQueueId.length>0&&hash(input.accountabilityControlBodyHash)&&input.sweepFindingQueueId===undefined&&input.sweepFindingBodyHash===undefined,validSweep=input.evidenceKind===undefined&&typeof input.sweepFindingQueueId==='string'&&input.sweepFindingQueueId.length>0&&hash(input.sweepFindingBodyHash)&&input.accountabilityControlQueueId===undefined&&input.accountabilityControlBodyHash===undefined;
   if(!validAccountability&&!validSweep)fail('qualification_retirement_evidence_invalid','Provide exactly one bounded worker-finding or claimed Operator-accountability evidence binding');
   if(actor!=='operator-agent@kernel'||!generation||this.authority.generation(actor)!==generation||!authority||authority.state!=='active'||authority.lease_until<=now||!authority.owner_session||authority.owner_generation!==this.authority.generation(authority.owner_session)||plan)fail('qualification_retirement_operator_required','Current native Operator, live authority and planless qualification bootstrap required');
   const liveAuthority=authority!;
   if(!Number.isSafeInteger(input.deadline)||input.deadline<=now||input.deadline>now+1200000)fail('qualification_duty_deadline_invalid','Retirement duty must have a finite deadline within twenty minutes');
   if(!target||!(target.sourceSession==='operator-agent@kernel'||(accountability&&target.sourceSession==='watchdog@system'))||target.destinationSession===actor||target.state!=='pending'||target.claimedAt||!target.expiresAt||Date.parse(target.expiresAt)>now||digest(target.body)!==input.targetBodyHash||!target.body.toLowerCase().includes('qualification')||!target.body.toLowerCase().includes('assessment'))fail('qualification_retirement_target_invalid','Exact expired, unclaimed legacy qualification assessment row required');
   const validTarget=target!,worker=validTarget.destinationSession;
   if(!this.db.prepare('SELECT 1 FROM sessions s JOIN nodes n ON n.id=s.node_id WHERE s.session_name=? AND n.rig_id=?').get(worker,input.rigId))fail('qualification_retirement_worker_rig_mismatch','Target Worker is not a native member of the requested rig');
   if((this.refreshIdentity||this.refreshWorkerActivity)&&!prepared)fail('qualification_duty_worker_not_quiescent','Synchronous retirement staging requires the fresh native observation prepared by its authenticated route');
   const workerGeneration=this.authority.generation(worker),workerObservation=this.activity(worker),observedAt=Date.parse(workerObservation?.identityObservedAt??'');
   if(!workerGeneration||!this.preparedQualificationObservationMatches(prepared,worker,workerGeneration,now,workerObservation)||!workerObservation?.identityVerified||workerObservation.generation!==workerGeneration||!Number.isFinite(observedAt)||observedAt>now||now-observedAt>3000||!coordinationIdle(workerObservation,workerGeneration,now)||!this.configurationDigest(worker))fail('qualification_duty_worker_not_quiescent','Fresh current native idle Worker identity and configuration required');
   if(accountability){if(!input.accountabilityControlQueueId||!input.accountabilityControlBodyHash||!this.qualificationRetirementAccountabilityMatches(input.rigId,{...input,worker,operatorGeneration:generation}))fail('qualification_retirement_accountability_unproven','Exact claimed Operator accountability control for this failed-before-send target is required');}
   else if(!sweep||sweep.destinationSession!==worker||sweep.sourceSession!=='operator-agent@kernel'||sweep.state!=='pending'||sweep.claimedAt||digest(sweep.body)!==input.sweepFindingBodyHash||!sweep.body.includes('STUCK SWEEP FINDING (undelivered-wake)')||!sweep.body.includes('row: '+input.targetQueueId)||!sweep.body.includes('wake failed')||!sweep.body.includes('nothing retried it'))fail('qualification_retirement_wake_unproven','Exact unclaimed stuck-sweep finding must prove the original wake failed and was not retried');
   if(!this.qualificationRetirementWakeFailed(input.targetQueueId))fail('qualification_retirement_effect_unknown','Original qualification wake is not durably failed; preserve UNKNOWN and do not retire');
   const evidenceKind=accountability?'operator-accountability':'worker-sweep-finding',evidenceQueueId=accountability?input.accountabilityControlQueueId!:input.sweepFindingQueueId!,evidenceBodyHash=accountability?input.accountabilityControlBodyHash!:input.sweepFindingBodyHash!;
   // Preserve the established semantic identity for Worker-finding duties so
   // persisted pre-upgrade duties and their contained successors remain visible.
   const semanticInput=accountability
    ?{rigId:input.rigId,targetQueueId:input.targetQueueId,targetBodyHash:input.targetBodyHash,evidenceKind,evidenceQueueId,evidenceBodyHash,worker,workerGeneration,configurationDigest:this.configurationDigest(worker)}
    :{rigId:input.rigId,targetQueueId:input.targetQueueId,targetBodyHash:input.targetBodyHash,sweepFindingQueueId:input.sweepFindingQueueId,sweepFindingBodyHash:input.sweepFindingBodyHash,worker,workerGeneration,configurationDigest:this.configurationDigest(worker)};
   const semanticKey=digest(JSON.stringify(semanticInput)),rootId=this.dutyRootId(input.rigId,'qualification-assessment-retirement',semanticKey),packageKey='qualification-assessment-retirement:'+input.targetQueueId;
   const prior=this.db.prepare("SELECT operation_id,receipt FROM coordinator_operations WHERE rig_id=? AND kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.rootId')=? ORDER BY rowid DESC LIMIT 1").get(input.rigId,rootId) as {operation_id:string;receipt:string}|undefined;
   let previousQueueId:string|undefined,previousChainIds:string[]=[],queueId=prior?.operation_id??rootId;
   if(prior){const old=this.repo.getById(queueId),receipt=JSON.parse(prior.receipt);if(receipt.semanticKey!==semanticKey||!old)fail('qualification_retirement_conflict','Frozen legacy retirement identity differs');this.observeDuty(queueId);const facts=this.dutyFacts(queueId);if(facts.complete||(!facts.expired&&!facts.failedByRecipient))return {queueId,deadline:receipt.deadline,targetQueueId:input.targetQueueId};if(!facts.retired||!this.dutyNoticeContained(input.rigId,receipt))fail('qualification_retirement_protected','Prior retirement attempt is not terminally contained');previousQueueId=queueId;previousChainIds=[queueId,...this.bootstrapContainedAncestors(input.rigId,receipt,'qualification-assessment-retirement')];queueId=this.dutySuccessorId(rootId,queueId,workerGeneration+':'+liveAuthority.epoch);}
   if(this.lifecycleControl(queueId))return {queueId,deadline:this.lifecycleControl(queueId)!.receipt.deadline,targetQueueId:input.targetQueueId};
   const activeRows=this.db.prepare("SELECT qitem_id,source_session,destination_session,state,claimed_at,expires_at,body FROM queue_items WHERE destination_session=? AND state IN ('pending','in-progress','blocked')").all(worker) as Array<any>;
   const staleTarget=(row:any):boolean=>(row.source_session==='operator-agent@kernel'||(accountability&&row.qitem_id===input.targetQueueId&&row.source_session==='watchdog@system'))&&row.destination_session===worker&&row.state==='pending'&&!row.claimed_at&&!!row.expires_at&&Date.parse(row.expires_at)<=now&&row.body.toLowerCase().includes('qualification')&&row.body.toLowerCase().includes('assessment')&&!this.db.prepare('SELECT 1 FROM coordinator_assignments WHERE queue_id=?').get(row.qitem_id)&&!this.db.prepare('SELECT 1 FROM coordinator_packages WHERE rig_id=? AND package_key=?').get(input.rigId,row.qitem_id);
   for(const row of activeRows){
    if(staleTarget(row))continue;
    if(previousChainIds.includes(row.qitem_id)&&this.lifecycleControl(row.qitem_id)?.receipt.kind==='qualification-assessment-retirement')continue;
    const linked=activeRows.some(candidate=>staleTarget(candidate)&&row.body.includes('STUCK SWEEP FINDING (undelivered-wake)')&&row.body.includes('row: '+candidate.qitem_id)&&row.body.includes('wake failed')&&row.body.includes('nothing retried it')&&row.source_session==='operator-agent@kernel'&&!row.claimed_at);
    if(!linked)fail('qualification_duty_worker_protected','Worker has unrelated pending or active custody; resolve it through its own supported path');
   }
   if(this.workerEffectDebt(worker)||this.db.prepare("SELECT 1 FROM seat_dispatch_reservations WHERE state!='released' AND (session_name=? OR node_id IN (SELECT node_id FROM sessions WHERE session_name=?))").get(worker,worker)||this.db.prepare('SELECT 1 FROM seat_delivery_guards WHERE (desired=1 OR effective=1) AND node_id IN (SELECT node_id FROM sessions WHERE session_name=?)').get(worker))fail('qualification_duty_worker_protected','Worker has unresolved effects, reservation or delivery guard');
   if(this.db.prepare('SELECT 1 FROM coordinator_assignments WHERE queue_id=?').get(input.targetQueueId)||this.db.prepare('SELECT 1 FROM coordinator_packages WHERE rig_id=? AND package_key=?').get(input.rigId,input.targetQueueId)||validTarget.state!=='pending'||validTarget.claimedAt||!validTarget.expiresAt||Date.parse(validTarget.expiresAt)>now)fail('qualification_retirement_target_invalid','Target is assigned product work or no longer qualifies for legacy retirement');
   if(accountability&&activeRows.filter(staleTarget).length!==1)fail('qualification_duty_worker_protected','Accountability evidence can retire only its exact stale target; other Worker custody remains protected');
   const contractDigest=digest(JSON.stringify(accountability?{targetQueueId:input.targetQueueId,targetBodyHash:input.targetBodyHash,evidenceKind,evidenceQueueId,evidenceBodyHash}:{targetQueueId:input.targetQueueId,targetBodyHash:input.targetBodyHash,sweepFindingQueueId:input.sweepFindingQueueId,sweepFindingBodyHash:input.sweepFindingBodyHash}));
   const chainIds=[queueId,...previousChainIds].filter((id,index,ids)=>ids.indexOf(id)===index);
   const required=accountability?'Claim only this finite retirement duty under your current native Worker generation. Re-read the exact expired target and the linked Operator accountability control facts in this immutable duty. If either changed, stop. Do not claim or perform the expired target and do not retry its failed-before-send wake. Mark only the exact old target failed with an expiry disposition through supported queue update, then close this report-only duty with actual terminal references. Preserve the original target body, all transitions, and failed wake history. The accountability control is evidence only; do not claim or complete it. This creates no qualification PASS, product admission, acceptance or dispatch.':'Claim only this finite retirement duty under your current native Worker generation. Re-read the exact expired target and linked failed-wake finding. If either changed, stop. Do not claim or perform the expired target and do not retry its wake. Claim the linked sweep finding under this same Worker generation. Mark only the exact old target failed with an expiry disposition through supported queue update, close the linked sweep finding only after the target is terminal, then close this report-only duty with the actual terminal references. Preserve the original target body, all transitions, and failed wake history. This creates no qualification PASS, product admission, acceptance or dispatch.';
   const evidenceFields=accountability?{evidenceKind,evidenceQueueId,evidenceBodyHash,accountabilityControlQueueId:evidenceQueueId,accountabilityControlBodyHash:evidenceBodyHash}:{sweepFindingQueueId:input.sweepFindingQueueId,sweepFindingBodyHash:input.sweepFindingBodyHash};
   const body=JSON.stringify({schema:'qualification-assessment-retirement-duty.v1',action:'retire-exact-expired-unclaimed-qualification-task',rigId:input.rigId,queueId,previousQueueId,chainIds,issuer:'operator-agent@kernel',issuerGeneration:generation,holder:liveAuthority.owner_session,holderGeneration:liveAuthority.owner_generation,epoch:liveAuthority.epoch,planRevision:null,bootstrapType:'qualification-assessment-retirement',worker,workerGeneration,configurationDigest:this.configurationDigest(worker),targetQueueId:input.targetQueueId,targetBodyHash:input.targetBodyHash,targetDeadline:Date.parse(validTarget.expiresAt!),...evidenceFields,contractDigest,deadline:input.deadline,grantsAuthority:false,productAuthority:false,required});
   const receipt={kind:'qualification-assessment-retirement',queueId,rootId,semanticKey,previousQueueId,chainIds,packageKey,rigId:input.rigId,recipient:worker,worker,workerGeneration,recipientGeneration:workerGeneration,configurationDigest:this.configurationDigest(worker),targetQueueId:input.targetQueueId,targetBodyHash:input.targetBodyHash,targetDeadline:Date.parse(validTarget.expiresAt!),...evidenceFields,contractDigest,deadline:input.deadline,issuer:'operator-agent@kernel',operatorGeneration:generation,holder:liveAuthority.owner_session,holderGeneration:liveAuthority.owner_generation,epoch:liveAuthority.epoch,planRevision:null,bootstrapType:'qualification-assessment-retirement',bodyHash:digest(body)};
   this.repo.createQualificationAssessmentRetirementDuty({qitemId:queueId,sourceSession:'watchdog@system',destinationSession:worker,expiresAt:new Date(input.deadline).toISOString(),body,identityProvenance:'system:operator-authorized-coordination',nudge:false});
   this.repo.stageCoordinatorLifecycleWake(queueId,worker,workerGeneration!);
   const notice=this.db.prepare('SELECT body FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+queueId) as {body:string}|undefined;if(!notice)fail('qualification_retirement_wake_missing','Durable controlled retirement wake required');
   this.db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run(input.rigId,queueId,'coordinator-lifecycle-control',JSON.stringify({...receipt,noticeBodyHash:digest(notice!.body)}),digest(body));
   return {queueId,deadline:input.deadline,targetQueueId:input.targetQueueId};
  }).immediate();
 }
 /** Bind the Worker-authored typed return to the exact still-live assessment duty. */
 recordQualificationAssessmentReturn(actor:string,generation:string,input:{rigId:string;dutyQueueId:string;returnQueueId:string}):void {
  this.db.transaction(()=>{
   const control=this.lifecycleControl(input.dutyQueueId),r=control?.receipt,q=this.repo.getById(input.dutyQueueId),claim=this.db.prepare('SELECT claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(input.dutyQueueId) as any;
   if(!control||control.rigId!==input.rigId||!r||r.kind!=='qualification-assessment'||actor!==r.recipient||generation!==r.recipientGeneration||this.authority.generation(actor)!==generation||r.deadline<=this.now()||!q||q.state!=='in-progress'||claim?.claimed_by_generation_uuid!==generation||this.dutyFacts(input.dutyQueueId).act!==true)fail('qualification_duty_return_required','Exact unexpired native Worker claim and live assessment duty required');
   const returned=this.repo.getById(input.returnQueueId);
   if(!returned)fail('qualification_duty_return_invalid','Typed return queue item is missing');
   const returnedItem=returned!, authored=this.db.prepare('SELECT minting_generation_uuid FROM queue_items WHERE qitem_id=?').get(input.returnQueueId) as any,b=(()=>{try{return JSON.parse(returnedItem.body);}catch{return null;}})();
   if(!r||returnedItem.sourceSession!==actor||returnedItem.destinationSession!==r.holder||authored?.minting_generation_uuid!==generation||!['done','handed-off'].includes(returnedItem.state)||b?.schema!=='qualification-assessment-return.v1'||b.dutyQueueId!==r.queueId||b.contractDigest!==r.contractDigest||b.workerGeneration!==generation||b.configurationDigest!==r.configurationDigest||b.artifact?.ref!==r.contract.artifactRef||b.artifact?.sha256!==r.contract.artifactSha256||!Array.isArray(b.evidence)||b.evidence.length===0||b.evidence.some((e:any)=>!e||typeof e.kind!=='string'||typeof e.ref!=='string'||!e.ref.trim()))fail('qualification_duty_return_invalid','Typed return must match the immutable contract and genuine Worker generation');
   const id='qualification-assessment-return:'+r.queueId,requestHash=digest(JSON.stringify({actor,generation,input,returnBodyHash:digest(returnedItem.body)})),prior=this.db.prepare("SELECT receipt,request_hash FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='qualification-assessment-return'").get(input.rigId,id) as any;
   if(prior){if(prior.request_hash!==requestHash)fail('qualification_duty_return_conflict','A different return is already bound');return;}
   const receipt={dutyQueueId:r.queueId,returnQueueId:input.returnQueueId,returnBodyHash:digest(returnedItem.body),worker:actor,workerGeneration:generation,configurationDigest:r.configurationDigest,contractDigest:r.contractDigest,recordedAt:this.now(),grantsAuthority:false};
   this.db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run(input.rigId,id,'qualification-assessment-return',JSON.stringify(receipt),requestHash);
   this.observeLifecycleCompletion(r.queueId);
  }).immediate();
 }
 /** Independent Lead review records evidence sufficiency only; it never changes admission or qualification. */
 assessQualificationDuty(actor:string,generation:string,input:{rigId:string;dutyQueueId:string;finding:'evidence-sufficient'|'evidence-insufficient'|'inconclusive';evidenceRef:string}):{reviewId:string;grantsAuthority:false} {
  return this.db.transaction(()=>{
   const control=this.lifecycleControl(input.dutyQueueId),r=control?.receipt,a=this.authority.get(input.rigId),plan=this.plan(input.rigId),returned=control?.rigId===input.rigId?this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='qualification-assessment-return'").get(input.rigId,'qualification-assessment-return:'+input.dutyQueueId) as any:undefined;
   const planMatches=r?.bootstrap===true?r.planRevision===null&&!plan:!!plan&&plan.operatorGeneration===r.operatorGeneration&&plan.revision===r.planRevision;
   if(!control||control.rigId!==input.rigId||!r||r.kind!=='qualification-assessment'||!returned||actor!==r.holder||generation!==r.holderGeneration||this.authority.generation(actor)!==generation||this.authority.generation('operator-agent@kernel')!==r.operatorGeneration||!a||a.state!=='active'||a.lease_until<=this.now()||a.owner_session!==actor||a.owner_generation!==generation||a.epoch!==r.epoch||!planMatches||this.authority.generation(r.recipient)!==r.recipientGeneration||this.configurationDigest(r.recipient)!==r.configurationDigest||actor===r.recipient||r.deadline<=this.now())fail('qualification_duty_review_required','Exact current Lead holder and Operator, unexpired duty, and unchanged Worker generation/configuration required');
   if(!['evidence-sufficient','evidence-insufficient','inconclusive'].includes(input.finding)||typeof input.evidenceRef!=='string'||!input.evidenceRef.trim())fail('qualification_duty_review_invalid','Independent evidence finding and reference required');
   const reviewId='qualification-assessment-review:'+r.queueId,requestHash=digest(JSON.stringify({actor,generation,input,returnReceipt:returned.receipt})),prior=this.db.prepare("SELECT request_hash FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='qualification-assessment-review'").get(input.rigId,reviewId) as any;if(prior){if(prior.request_hash!==requestHash)fail('qualification_duty_review_conflict','Review is immutable');return {reviewId,grantsAuthority:false as const};}
   const review={dutyQueueId:r.queueId,returnQueueId:JSON.parse(returned.receipt).returnQueueId,returnBodyHash:JSON.parse(returned.receipt).returnBodyHash,assessor:actor,assessorGeneration:generation,workerGeneration:r.recipientGeneration,configurationDigest:r.configurationDigest,contractDigest:r.contractDigest,finding:input.finding,evidenceRef:input.evidenceRef,reviewedAt:this.now(),grantsAuthority:false};
   this.db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run(input.rigId,reviewId,'qualification-assessment-review',JSON.stringify(review),requestHash);
   this.observeLifecycleCompletion(r.queueId);
   return {reviewId,grantsAuthority:false as const};
  }).immediate();
 }
 private acceptedTaskHistory(rigId:string,t:CoordinationTask):boolean {
  const r=this.db.prepare("SELECT a.queue_id,a.disposition_id,a.body_hash,a.destination,q.body,q.state,p.contract FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id JOIN coordinator_packages p ON p.rig_id=a.rig_id AND p.package_key=a.package_key WHERE a.rig_id=? AND a.package_key=?").get(rigId,t.packageKey) as any;
  const c=r?JSON.parse(r.contract):null;return !!r&&r.destination===t.owner&&r.body_hash===digest(t.body)&&digest(r.body)===r.body_hash&&c.destination===t.owner&&c.bodyHash===r.body_hash&&successfulReturn(r.state,r.disposition_id)&&this.exactAccepted(rigId,r.queue_id,r.disposition_id);
 }
 private dormantRecoveryHistory(rigId:string,t:CoordinationTask,prior:CoordinationPlan):boolean {
  const target=prior.tasks.find(parent=>parent.key===t.recoveryFor);if(!target||!this.acceptedTaskHistory(rigId,target)||this.authority.runtimeOutcomeAssessment?.requiresRecovery(rigId,target.packageKey))return false;
  const id='qitem-coordination-'+digest(rigId+':'+t.packageKey).slice(0,24);
  return !this.db.prepare('SELECT 1 FROM coordinator_assignments WHERE rig_id=? AND package_key=?').get(rigId,t.packageKey)&&!this.db.prepare('SELECT 1 FROM coordinator_stage_assignments WHERE rig_id=? AND package_key=?').get(rigId,t.packageKey)&&!this.db.prepare('SELECT 1 FROM coordinator_resources WHERE rig_id=? AND package_key=?').get(rigId,t.packageKey)&&!this.db.prepare("SELECT 1 FROM queue_items WHERE qitem_id=? AND state IN ('pending','in-progress','blocked')").get(id);
 }
 /** Read-only work boundary for finite native continuation grants. Reuse exact
  * acceptance and dormant recovery semantics instead of treating unused backup
  * packages as perpetual work after their primary has been accepted. */
 continuationTasks(rigId:string):CoordinationTask[] {
  const plan=this.plan(rigId);if(!plan)return [];
  return plan.tasks.filter(t=>!this.acceptedTaskHistory(rigId,t)&&!(t.recoveryFor&&this.dormantRecoveryHistory(rigId,t,plan)));
 }
/** Explicit administrative allowlist for the acknowledgment-contract debt boundary:
   * only the Operator's own qualification refresh (F3) uses it. The pre-existing
   * materialization duty keeps its strict zero-debt gate, and no future kind inherits
   * this relaxation by default. Held-history kinds already used that gate before. */
  private administrativeDuty(kind:string):boolean {return kind==='outcome-qualification-refresh'||kind==='admission-refresh';}
  /** This control plane's own accountable intake is its recovery traffic, not an unknown
   * external effect: a genuine wake to the Operator seat for a retained hold never blocks
   * the administrative duty it announces. Provenance is exact (watchdog sender, audit
   * pointer, creation identity, recorded notice hash) and only an already-unresolved
   * delivery state is exempt; a pending or sending intake wake still counts as debt. */
  private administrativeIntakeNotices(rigId:string,recipient:string):string[] {
   const rows=this.db.prepare("SELECT qitem_id FROM queue_items WHERE destination_session=? AND state IN ('pending','in-progress','blocked') AND json_valid(body) AND json_extract(body,'$.action') IN ('resolve-exact-coordination-task-hold','coordination-task-hold-lineage')").all(recipient) as Array<{qitem_id:string}>,excluded:string[]=[];
   for(const {qitem_id} of rows){
    const q=this.repo.getById(qitem_id);if(!q)continue;
    let h:any;try{h=JSON.parse(q.body);}catch{continue;}
    if(h.rigId!==rigId||h.grantsAuthority!==false||h.recipientGeneration===undefined)continue;
    // Either the deterministic root for this hold, or a successor proven to follow a
    // validated root of the same chain by its recorded, hash-bound chain receipt.
    const chainItem=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='coordination-intake-chain' AND operation_id=?").get('coordination-intake-chain:'+qitem_id) as {receipt:string}|undefined,recorded=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='coordination-task-hold-lineage' AND operation_id=?").get(qitem_id) as {receipt:string}|undefined;
    let expectedBodyHash:string|undefined;
    if(this.validAccountableIntake(rigId,qitem_id,q)){expectedBodyHash=(chainItem?JSON.parse(chainItem.receipt):recorded?JSON.parse(recorded.receipt):undefined)?.noticeBodyHash;}
    else if(chainItem){const r=JSON.parse(chainItem.receipt);if(r.rootQueueId===qitem_id)continue;if(!this.validAccountableIntakeChainItem(rigId,q,r.rootQueueId))continue;expectedBodyHash=r.noticeBodyHash;}
    else if(recorded)expectedBodyHash=JSON.parse(recorded.receipt).noticeBodyHash;
    if(typeof expectedBodyHash!=='string')continue;
    const notice=this.db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+qitem_id) as any;
    if(!notice||notice.sender_session!=='watchdog@system'||notice.destination_session!==recipient||notice.audit_pointer!==qitem_id||!['indeterminate','failed'].includes(notice.delivery_state))continue;
    if(expectedBodyHash!==digest(notice.body))continue;
    excluded.push('wake-intent-'+qitem_id);
   }
   return excluded;
  }
  // Qualification only refreshes exact policy metadata: it cannot replay or
  // dispose historical effects, accept work, or authorize product dispatch.
  // Historical pending/UNKNOWN notices remain intact; active sends still fence
  // this bounded probe. Its own predecessor checks remain in lifecycleDuty.
  private qualificationProbeDebtReady(recipient:string):boolean {
   return !this.db.prepare("SELECT 1 FROM outbox_entries WHERE delivery_state='sending' AND (sender_session=? OR destination_session=?)").get(recipient,recipient);
  }
  private lifecycleRecipientReady(rigId:string,recipient:string,packageKey:string,excludeEffect?:string|string[],administrative=false):boolean {
   const plan=this.plan(rigId),scope=plan?.dispatchRestrictions?.find(r=>r.session===recipient),excluded=administrative?[...(Array.isArray(excludeEffect)?excludeEffect:excludeEffect?[excludeEffect]:[]),...this.administrativeIntakeNotices(rigId,recipient)]:excludeEffect;
   return !!plan&&!this.db.prepare("SELECT 1 FROM seat_delivery_guards WHERE (desired=1 OR effective=1) AND node_id IN (SELECT node_id FROM sessions WHERE session_name=?)").get(recipient)&&(!scope||(scope.generation===this.authority.generation(recipient)&&scope.validUntil>this.now()&&scope.packageKeys.includes(packageKey)))&&!this.db.prepare("SELECT 1 FROM seat_dispatch_reservations WHERE state!='released' AND (session_name=? OR node_id IN (SELECT node_id FROM sessions WHERE session_name=?))").get(recipient,recipient)&&(administrative?this.qualificationProbeDebtReady(recipient):!this.workerEffectDebt(recipient,excludeEffect));
  }
 private stageDutyRetirement(rigId:string,parent:any,targetQueueId=parent.queueId):string|undefined {
  const target=this.repo.getById(targetQueueId);if(!target||!target.expiresAt||Date.parse(target.expiresAt)>this.now()||!['pending','in-progress','blocked'].includes(target.state))return;
  const targetControl=this.lifecycleControl(targetQueueId)?.receipt;
  if(!targetControl&&!(parent.recordQueueId===targetQueueId&&this.heldHistoryRecord(parent)))return;
  const generation=targetControl?.recipientGeneration??parent.operatorGeneration;if(this.authority.generation(target.destinationSession)!==generation)return;
  const prior=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.targetQueueId')=? AND json_extract(receipt,'$.kind') IN ('held-history-retirement','lifecycle-retirement') ORDER BY rowid DESC LIMIT 1").get(rigId,targetQueueId) as any;
let previousQueueId:string|undefined;if(prior){const r=JSON.parse(prior.receipt);this.observeDuty(r.queueId);this.recordHeldHistoryNoticeOutcome(rigId,r);const facts=this.dutyFacts(r.queueId);if(facts.complete||(!facts.expired&&!facts.failedByRecipient))return r.queueId;if(!facts.retired){return this.stageDutyRetirement(rigId,r,r.queueId);}previousQueueId=r.queueId;}
   const queueId='qitem-coordination-lifecycle-'+digest(rigId+':retire:'+targetQueueId+':'+(previousQueueId??'initial')).slice(0,24);if(this.repo.getById(queueId))return queueId;
   const chainIds=[...new Set([parent.queueId,targetQueueId,parent.authoringQueueId,parent.recordQueueId,parent.previousQueueId,...(parent.chainIds??[]),previousQueueId].filter(Boolean))],held=parent.kind.startsWith('held-history-'),plan=this.plan(rigId),owner=this.authority.get(rigId);
   // Issue provenance is always current: a freshly staged duty whose plan revision is
   // stale can never satisfy the send facet, which would leave the recipient's own
   // failure report as the only way the chain could ever advance. The held-history chain
   // keeps its authoring holder, epoch and operator generation, whose gates are read
   // against the authoring receipt rather than this one.
   const r={...parent,kind:held?'held-history-retirement':'lifecycle-retirement',queueId,authoringQueueId:parent.authoringQueueId??parent.queueId,targetQueueId,targetBodyHash:digest(target.body),recipient:target.destinationSession,recipientGeneration:generation,deadline:this.now()+1200000,previousQueueId,chainIds,issuedAt:this.now(),planRevision:plan?.revision??parent.planRevision,...(held?{}:{holder:owner?.owner_session??parent.holder,holderGeneration:owner?.owner_generation??parent.holderGeneration,epoch:owner?.epoch??parent.epoch,operatorGeneration:plan?.operatorGeneration??parent.operatorGeneration})};
  if(!this.dutyBinding(rigId,r)||!this.dutyProtection(rigId,r))return;
  const body=JSON.stringify({action:'report-own-expired-administrative-duty-outcome',rigId,queueId,claimCommand:'rig queue claim '+queueId,targetQueueId,targetBodyHash:r.targetBodyHash,originalDeadline:Date.parse(target.expiresAt),recipientGeneration:generation,deadline:r.deadline,grantsAuthority:false,required:'Read the exact frozen expired target and its retained evidence. Genuinely claim this fresh failure-only duty under your own native identity. Report the actual target failed/canceled through rig queue update '+targetQueueId+' --state failed --note <actual-own-expiry-disposition>. Do not claim or execute expired work, renew its authority, infer delivery, retry an uncertain mutation, accept, bind, dispatch, release resources or alter the original notice. The old target stays unclaimed if it was unclaimed. If the genuine target terminal receipt already exists, claim and close this report referencing it; do not repeat the target mutation.'});
  this.db.transaction(()=>{if(!this.dutyBinding(rigId,r)||!this.dutyProtection(rigId,r)||this.repo.getById(queueId))return;this.repo.createWithinTransaction({qitemId:queueId,sourceSession:'watchdog@system',destinationSession:r.recipient,body,expiresAt:new Date(r.deadline).toISOString(),identityProvenance:'system:operator-authorized-coordination',nudge:false});const id=this.repo.stageHeldHistoryAuthoringWake(queueId,r.recipient,generation),notice=this.db.prepare('SELECT body FROM outbox_entries WHERE outbox_id=?').get(id) as any;this.db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run(rigId,queueId,'coordinator-lifecycle-control',JSON.stringify({...r,bodyHash:digest(body),noticeBodyHash:digest(notice.body)}),digest(body));})();return this.repo.getById(queueId)?queueId:undefined;
 }
 private lifecycleDuty(rigId:string,kind:'acceptance'|'recovery'|'materialization'|'outcome-qualification-refresh'|'admission-refresh'|'frontier-planning'|'frontier-admission'|'frontier-confirmation',packageKey:string,recipient:string,recipientGeneration:string,semanticKey:string,details:Record<string,unknown>):CoordinationResult {
  const a=this.authority.get(rigId)!,plan=this.plan(rigId)!,rootId=this.dutyRootId(rigId,kind,semanticKey);
  const rows=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')=? AND json_extract(receipt,'$.packageKey')=? ORDER BY rowid DESC").all(rigId,kind,packageKey) as any[];
  const prior=rows.map(v=>JSON.parse(v.receipt)).find(r=>r.semanticKey===semanticKey||(r.semanticKey===undefined&&(kind==='materialization'?r.contractHash===details.contractHash:r.originalQueueId===details.originalQueueId&&r.dispositionId===details.dispositionId)));
let queueId=rootId,previousQueueId:string|undefined,expiredUnclaimed=false;
   if(prior){this.observeDuty(prior.queueId);this.recordDutyChainNoticeOutcomes(rigId,prior);const facts=this.dutyFacts(prior.queueId),q=this.repo.getById(prior.queueId)!;
    if(facts.complete)return {key:kind+':'+packageKey,state:kind==='materialization'?'materialized':'owned-recovery',queueId:prior.queueId,deadline:prior.deadline};
    if(!facts.superseded&&['pending','in-progress','blocked'].includes(q.state)&&!facts.expired)return {key:kind+':'+packageKey,state:'pending-native-'+kind,queueId:prior.queueId,deadline:prior.deadline};
    expiredUnclaimed=!facts.complete&&!facts.superseded&&facts.expired&&!q.claimedAt&&['pending','in-progress','blocked'].includes(q.state);
    // An expired unclaimed link keeps a supported retirement and containment route for its
    // own unresolved wake, and an accountable intake instead of a silent dead end.
    if(!facts.superseded&&(!facts.retired||(expiredUnclaimed&&!this.dutyNoticeContained(rigId,prior)))){const retirementQueueId=this.stageDutyRetirement(rigId,prior);return {key:kind+':'+packageKey,state:'held',queueId:prior.queueId,reason:facts.retired?'lifecycle-recipient-protected':'lifecycle-duty-exhausted',deadline:prior.deadline,...(retirementQueueId?{activityEvidence:{retirementQueueId}}:{})};}
    if(!this.dutySubjectReady(rigId,prior))return {key:kind+':'+packageKey,state:'held',queueId:prior.queueId,reason:'lifecycle-return-contract-drift',deadline:prior.deadline};
    previousQueueId=prior.queueId;const bindingVersion=this.dutyBindingVersion(kind,recipientGeneration,a.epoch);queueId=this.dutySuccessorId(rootId,prior.queueId,bindingVersion);
   }
   const existing=this.repo.getById(queueId),deadline=existing?.expiresAt?Date.parse(existing.expiresAt):this.now()+1200000;if(existing)return {key:kind+':'+packageKey,state:'pending-native-'+kind,queueId,deadline};
   if(!this.lifecycleRecipientReady(rigId,recipient,packageKey,undefined,this.administrativeDuty(kind)))return {key:kind+':'+packageKey,state:'held',queueId:prior?.queueId,reason:'lifecycle-recipient-protected',deadline,subject:{packageKey,owner:recipient,identity:semanticKey}};
  let bodyValue:any={action:kind==='acceptance'?'accept-exact-return-or-own-recovery':kind==='recovery'?'own-exact-failed-return-recovery':kind==='outcome-qualification-refresh'?'refresh-exact-expired-outcome-qualification':kind==='admission-refresh'?'refresh-exact-expired-task-admission':'materialize-exact-admitted-frontier',rigId,queueId,packageKey,recipientGeneration,deadline,grantsAuthority:false,...details,required:kind==='acceptance'?'Claim this finite duty under current holder identity. Inspect the exact typed disposed return and required technical evidence. Use supported coordination-accept only when all classifier, qualification and independent review gates actually pass. An incomplete or unverified outcome requires distinct admitted, configured and genuinely picked-up recovery; record its exact active custody through coordination-lifecycle-recovery. Prose is not acceptance or owned recovery. Original acceptance alone releases the existing authorized frontier; never invent work or waive a gate.':kind==='recovery'?'Claim this finite recovery-only duty under the current native holder identity. Preserve the exact failed/denied/canceled original and its genuine typed disposition. Technical acceptance is forbidden for this original failure. Coordinate with the genuine current Operator to materialize a distinct admitted current recovery in the existing plan if absent; do not invent or reopen work. After actual worker pickup, record its exact active custody and evidence through coordination-lifecycle-recovery. Successful duty closure requires that verified distinct owned recovery; prose, a pending ticket and a failed-return acceptance attempt are not completion. Unknown effects, quiescence, current admission and qualifications remain protected.':kind==='outcome-qualification-refresh'?'Claim this finite current-Operator duty under your exact native identity. Revalidate the unchanged policy, provider configuration, existing private-input permission and credential availability without logging credentials. Submit a fresh dated qualification through rig coordinator outcome-qualification-refresh using the exact dutyQueueId in qualificationRefreshContract. Preserve the logical policy revision, provider/privacy/paid/calibration/negative-advice settings, prior decisions, unresolved assessment/recovery/acceptance references, and unknown effects. No provider test, provider retry, automatic renewal, policy rewrite, positive authority or acceptance waiver is authorized.': kind==='admission-refresh'?'Claim this finite current-Operator duty under your exact native identity. Re-assess and re-submit evidence for this ONE expired task admission; the runtime never renews it and never judges the qualification for you. Read the exact task bytes and the LIVE owner identity, then assess qualification, capacity and effort evidence against the LIVE owner generation and configurationDigest named in this notice, never against the prior admission. Record a new coordination-plan revision in which ONLY this task carries a freshly assessed admission, through your existing supported coordination-plan write. A byte-identical re-submission, a TTL-only extension of the stale admission, or any stale generation or configurationDigest is refused and does not complete this duty. This duty grants no qualification, acceptance, dispatch or delivery authority, does not reconcile or retry uncertain effects, and never extends a TTL by itself.':'Claim this finite Operator intake. Inspect the immutable admitted contract, retained accepted predecessor references and current plan. Materialize this exact package into the existing plan with actual current qualification, capacity, effort, native generation/configuration and recovery evidence through coordination-plan. Preserve unchanged accepted history and dormant backup bytes. If scope or a protected gate prevents this, park the concrete boundary; this notice grants no admission, qualification, dispatch or acceptance.'};
  if(kind==='admission-refresh')bodyValue.admissionRefreshContract={rigId,dutyQueueId:queueId,taskKey:details.taskKey,packageKey,owner:details.owner,ownerGeneration:details.ownerGeneration,configurationDigest:details.liveConfigurationDigest,admission:{generation:'<live owner generation>',configurationDigest:'<live configurationDigest>',qualificationRef:'<actual-new-proof-reference>',capacityRef:'<actual-new-capacity-reference>',effortRef:'<actual-new-effort-reference>',validUntil:'<actual-finite-proof-deadline>'}};
  if(kind==='outcome-qualification-refresh')bodyValue.qualificationRefreshContract={rigId,dutyQueueId:queueId,operationId:'<new-exact-operation-id>',policyRevision:details.policyRevision,policyDigest:details.policyDigest,qualifiedAt:'<actual-fresh-proof-time>',qualification:{ref:'<actual-new-proof-reference>',providerConfigDigest:details.providerConfigDigest,validUntil:'<actual-finite-proof-deadline>'}};
  let body=JSON.stringify(bodyValue);
  if(kind==='recovery'){const b=JSON.parse(body);b.required='Claim this finite recovery-only duty under the current native holder identity. Preserve the exact failed/denied/canceled original and its genuine typed disposition. Technical acceptance is forbidden for this original failure. Coordinate with the genuine current Operator to materialize a distinct admitted current recovery in the existing plan if absent; do not invent or reopen work. After actual worker pickup, record its exact active custody and evidence through coordination-lifecycle-recovery. Successful duty closure requires that verified distinct owned recovery; prose, a pending ticket and a failed-return acceptance attempt are not completion. Unknown effects, quiescence, current admission and qualifications remain protected.';body=JSON.stringify(b);}
  if(kind===PLANNING_DUTY_KIND||kind===ADMISSION_DUTY_KIND||kind===CONFIRMATION_DUTY_KIND){const b=JSON.parse(body);b.action=kind===PLANNING_DUTY_KIND?'plan-exact-next-product-frontier':kind===ADMISSION_DUTY_KIND?'admit-or-refuse-exact-scope-cited-frontier-proposal':'confirm-exact-scope-mapped-frontier-completion';b.required=kind===PLANNING_DUTY_KIND?'Claim this finite duty under the current native Lead identity. This project has no remaining authorized product work and silence is not completion. Read the frozen scopeSources snapshot; the runtime never reads or interprets their contents. Record exactly one typed disposition with rig coordinator coordination-frontier-plan carrying this duty frontierDigest. plan-proposal: candidate packages that each cite a frozen scope ref with its matching digest, plus their resources and return contract. frontier-complete: every frozen scope item mapped to an accepted scope-bound product package or an explicit owner-attributed deferral carrying its authorization reference. frontier-blocked: a named accountable boundary and its unblock condition. With no configured scope sources, frontier-complete is refused and scope-source-missing is the only accountable boundary. Prose and an empty frontier are not completion. This grants no package creation, admission, qualification, dispatch or acceptance.':kind===ADMISSION_DUTY_KIND?'Claim this finite duty under the current native Operator identity. You are the independent reviewer: the proposer is the current Lead and you are not the implementing worker. Read the exact frozen scope-cited proposal. Register every cited candidate through the existing supported package admission, or refuse the whole proposal once with an attributed reason; partial admission, a non-product work class and any divergence from the frozen proposal resources or return contract are refused. Registration only hands the work to the existing materialization and qualified dispatch path. This grants no judgement of product correctness, no acceptance, no qualification and no delivery.':'Claim this finite duty under the current native Operator identity. The Lead recorded a frontier-complete mapping every frozen scope item to an accepted scope-bound product package or an authorized deferral on this exact planning duty. Read the exact frozen mapping and its cited scope digests and record your own attributed confirmation with rig coordinator coordination-frontier-confirm. You are the independent reviewer and are not the proposer. Confirm only what the mapping actually shows; otherwise record the reopen disposition that returns this frontier to accountable planning. A confirmation never carries across a reopen. Prose is not confirmation. This grants no acceptance, qualification, dispatch or delivery.';body=JSON.stringify(b);}
this.db.transaction(()=>{this.repo.createWithinTransaction({qitemId:queueId,sourceSession:'watchdog@system',destinationSession:recipient,expiresAt:new Date(deadline).toISOString(),body,identityProvenance:'system:operator-authorized-coordination',nudge:false});
    const receipt={kind,queueId,rootId,semanticKey,previousQueueId,chainIds:[queueId,...(prior?.chainIds??[]),...(previousQueueId?[previousQueueId]:[])],packageKey,recipient,recipientGeneration,bodyHash:digest(body),deadline,issuedAt:this.now(),holder:a.owner_session,holderGeneration:a.owner_generation,epoch:a.epoch,operatorGeneration:plan.operatorGeneration,planRevision:plan.revision,...details};
    const id=this.repo.stageHeldHistoryAuthoringWake(queueId,recipient,recipientGeneration),notice=this.db.prepare('SELECT body FROM outbox_entries WHERE outbox_id=?').get(id) as any;this.db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run(rigId,queueId,'coordinator-lifecycle-control',JSON.stringify({...receipt,noticeBodyHash:digest(notice.body)}),digest(body));
   })();
   // One deduplicated accountable intake per expired-unclaimed link: finite per link,
   // with no invented numerical maximum on the lineage.
   if(expiredUnclaimed)return {key:kind+':'+packageKey,state:'held',queueId:prior!.queueId,reason:'lifecycle-duty-expired-unclaimed',deadline:prior!.deadline,activityEvidence:{successorQueueId:queueId}};
   return {key:kind+':'+packageKey,state:'pending-native-'+kind,queueId,deadline};
  }
 /** IMMUTABLE TASK INTENT: every field the task actually asks for, EXCLUDING the
  *  admission proof. The admission is precisely what this duty exists to refresh, so
  *  folding it into the subject digest would make a genuine refresh unable to complete
  *  (the bytes necessarily change) while a byte-identical task could never present a
  *  newer expiry. Intent is therefore the stable identity of the task request, and the
  *  prior stale admission is bound separately in the receipt. */
 private admissionTaskIntentDigest(t:CoordinationTask):string {
  const {admission:_priorAdmissionProof,...intent}=t;
  return digest(JSON.stringify(intent));
 }
 /** WHY the current admission is stale, from LIVE facts only. Never from the prior
  *  admission itself, which is what makes it evidence rather than a restatement. */
 /** The identity of the admission-refresh duty for this task bytes, owner generation and live configuration. */
 private admissionRefreshSemanticKey(t:CoordinationTask):string|null {
  const ownerGeneration=this.authority.generation(t.owner),liveConfigurationDigest=this.configurationDigest(t.owner);
  if(!ownerGeneration||!liveConfigurationDigest)return null;
  // digest() takes a string in this codebase; the object literal is serialized explicitly.
  return digest(JSON.stringify({packageKey:t.packageKey,taskIntentDigest:this.admissionTaskIntentDigest(t),ownerGeneration,liveConfigurationDigest,priorValidUntil:t.admission.validUntil}));
 }
 private admissionStaleReason(t:CoordinationTask):'expired'|'generation_changed'|'configuration_changed'|null {
  const liveGen=this.authority.generation(t.owner),liveCfg=this.configurationDigest(t.owner),ad=t.admission;
  if(liveGen!==ad.generation)return 'generation_changed';
  if(liveCfg!==ad.configurationDigest)return 'configuration_changed';
  if(!Number.isFinite(ad.validUntil)||ad.validUntil<=this.now())return 'expired';
  return null;
 }
 /** One accountable Operator duty per (packageKey, task bytes, live owner generation,
  *  live configuration, prior expiry). An identical retained task under a new plan
  *  revision therefore keeps the SAME duty; a changed owner generation or configuration
  *  correctly opens a NEW one. */
 private stageAdmissionRefreshDuty(rigId:string,t:CoordinationTask):CoordinationResult|undefined {
  const plan=this.plan(rigId),operatorGeneration=this.authority.generation('operator-agent@kernel');
  if(!plan||!operatorGeneration||plan.operatorGeneration!==operatorGeneration)return undefined;
  const ownerGeneration=this.authority.generation(t.owner),liveConfigurationDigest=this.configurationDigest(t.owner);
  if(!ownerGeneration||!liveConfigurationDigest)return undefined;
  const staleReason=this.admissionStaleReason(t);
  if(!staleReason)return undefined;
  const taskIntentDigest=this.admissionTaskIntentDigest(t);
  // One formula for issuing the duty and for finding it again when its completion commits.
  const semanticKey=this.admissionRefreshSemanticKey(t)!;
  return this.lifecycleDuty(rigId,'admission-refresh',t.packageKey,'operator-agent@kernel',operatorGeneration,semanticKey,{
   taskKey:t.key,packageKey:t.packageKey,owner:t.owner,ownerGeneration,liveConfigurationDigest,
   staleReason,priorAdmission:{validUntil:t.admission.validUntil,generation:t.admission.generation,configurationDigest:t.admission.configurationDigest,qualificationRef:t.admission.qualificationRef,capacityRef:t.admission.capacityRef,effortRef:t.admission.effortRef},
   taskIntentDigest,planRevision:plan.revision,
  });
 }
 /** Postcondition: the CURRENT plan carries this same task, now genuinely admitted
  *  against the LIVE owner generation and configuration, with a strictly newer expiry
  *  than the stale one AND than the duty's issue time. A byte-identical re-submission or
  *  a TTL-only extension therefore cannot complete it. Evidence CONTENT is never read
  *  or judged here. */
 private admissionRefreshCompleted(rigId:string,r:any):boolean {
  const plan=this.plan(rigId),operatorGeneration=this.authority.generation('operator-agent@kernel');
  if(!plan||!operatorGeneration||plan.operatorGeneration!==operatorGeneration||r.operatorGeneration!==operatorGeneration)return false;
  const task=plan.tasks.find(t=>t.key===r.taskKey&&t.packageKey===r.packageKey&&t.owner===r.owner);
  // Subject intent must be UNCHANGED: this duty refreshes an admission, it never changes
  // the work the task asks for.
  if(!task||this.admissionTaskIntentDigest(task)!==r.taskIntentDigest)return false;
  if(task.admission.generation!==r.ownerGeneration||task.admission.configurationDigest!==r.liveConfigurationDigest)return false;
  if(task.admission.generation!==this.authority.generation(task.owner))return false;
  if(task.admission.configurationDigest!==this.configurationDigest(task.owner))return false;
  if(!(task.admission.validUntil>r.priorAdmission.validUntil&&task.admission.validUntil>r.issuedAt))return false;
  // FRESH ASSESSMENT, not a TTL extension. The prior evidence references must all have been
  // re-issued: a re-submission that only moves validUntil, or that reuses the stale
  // qualification/capacity/effort references, cannot complete this duty. Reference VALUES
  // are compared, never read or judged; the runtime still writes no admission.
  if(task.admission.qualificationRef===r.priorAdmission.qualificationRef)return false;
  if(task.admission.capacityRef===r.priorAdmission.capacityRef)return false;
  if(task.admission.effortRef===r.priorAdmission.effortRef)return false;
  return this.admittedNow(task);
 }
 /** Subject readiness while the operator may act: the exact task is still in the plan
  *  with identical bytes and is STILL not admitted. Once it is admitted the duty is
  *  complete and the successor link is not opened. */
 private admissionRefreshSubjectReady(rigId:string,r:any):boolean {
  const plan=this.plan(rigId);
  if(!plan)return false;
  const task=plan.tasks.find(t=>t.key===r.taskKey&&t.packageKey===r.packageKey&&t.owner===r.owner);
  return !!task&&this.admissionTaskIntentDigest(task)===r.taskIntentDigest&&!this.admittedNow(task);
 }
 stageOutcomeQualificationDuty(rigId:string,details:{policyRevision:string;policyDigest:string;providerConfigDigest:string;qualificationRef:string;qualificationValidUntil:number}):string|null {
  const assessment=this.authority.runtimeOutcomeAssessment,plan=this.plan(rigId),generation=this.authority.generation('operator-agent@kernel');
  if(!assessment?.qualificationBoundaryMatches(rigId,details)||!plan||!generation||plan.operatorGeneration!==generation)return null;
  const semanticKey=digest(JSON.stringify(details)),result=this.lifecycleDuty(rigId,'outcome-qualification-refresh','outcome-qualification:'+details.policyRevision,'operator-agent@kernel',generation,semanticKey,details);
  // A blocked administrative duty is accountable to the Operator, never a silent null.
  if(result.state==='held'){const owner=this.authority.get(rigId);if(owner)this.stageTaskHoldIntake(rigId,owner,plan,result,[]);}
  return result.queueId??null;
 }
 heldHistoryAuthoringControl(queueId:string):any|null {const op=this.lifecycleControl(queueId);return ['held-history-authoring','held-history-pickup','held-history-retirement'].includes(op?.receipt.kind)?op!.receipt:null;}
 private heldHistoryAuthoringReady(rigId:string,r:any,excludeEffect?:string|string[]):boolean {
  const a=this.authority.get(rigId),plan=this.plan(rigId),q=this.db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(r.operatorIntakeId) as any,creation=this.db.prepare('SELECT * FROM queue_transitions WHERE qitem_id=? ORDER BY transition_id LIMIT 1').get(r.operatorIntakeId) as any;
  let h;try{h=q?JSON.parse(q.body):null;}catch{return false;}
  if(!a||a.state!=='active'||a.lease_until<=this.now()||a.epoch!==r.epoch||a.owner_session!==r.holder||a.owner_generation!==r.holderGeneration||this.authority.generation(r.holder)!==r.holderGeneration||!plan||plan.revision!==r.planRevision||plan.operatorGeneration!==r.operatorGeneration||this.authority.generation('operator-agent@kernel')!==r.operatorGeneration||!q||q.source_session!=='watchdog@system'||q.destination_session!=='operator-agent@kernel'||!['in-progress','blocked'].includes(q.state)||!q.claimed_at||q.claimed_by_generation_uuid!==r.operatorGeneration||digest(q.body)!==r.operatorIntakeBodyHash||q.claimed_at!==r.operatorClaimedAt||creation?.actor_session!=='watchdog@system'||creation?.identity_provenance!=='system:operator-authorized-coordination'||h?.action!=='restore-current-held-history-binding'||h.rigId!==rigId||h.epoch!==r.epoch||h.recipientGeneration!==r.operatorGeneration||r.deadline<=this.now())return false;
  const claim=this.db.prepare("SELECT 1 FROM queue_transitions WHERE qitem_id=? AND transition_note='claimed' AND actor_session='operator-agent@kernel' AND identity_provenance='transport:v1'").get(r.operatorIntakeId);if(!claim)return false;
  try{this.authority.assertCurrentOwner(r.holder,{rigId,epoch:r.epoch,generation:r.holderGeneration});if(JSON.stringify(this.authority.heldHistoryAuthoringSnapshot(rigId))!==JSON.stringify(r.effects))return false;}catch{return false;}
  for(const session of [r.holder,'operator-agent@kernel']){const scope=plan.dispatchRestrictions?.find(v=>v.session===session);if(scope&&(scope.generation!==this.authority.generation(session)||scope.validUntil<=this.now()||!scope.packageKeys.includes(r.packageKey)))return false;if(this.db.prepare('SELECT 1 FROM seat_delivery_guards WHERE (desired=1 OR effective=1) AND node_id IN (SELECT node_id FROM sessions WHERE session_name=?)').get(session)||this.db.prepare("SELECT 1 FROM seat_dispatch_reservations WHERE state!='released' AND (session_name=? OR node_id IN (SELECT node_id FROM sessions WHERE session_name=?))").get(session,session))return false;}
  return this.repo.heldHistoryAuthoringDebtReady(rigId,r.recipient,excludeEffect);
 }
 private stageHeldHistoryAuthoring(rigId:string,operatorIntakeId:string):void {
  const a=this.authority.get(rigId),plan=this.plan(rigId),intake=this.repo.getById(operatorIntakeId);if(!a||!plan||!intake)return;
  let effects;try{effects=this.authority.heldHistoryAuthoringSnapshot(rigId);}catch(error){if(!(error instanceof CoordinatorFenceError))throw error;return;}
  let queueId='qitem-coordination-lifecycle-'+digest(rigId+':held-history-authoring:'+operatorIntakeId+':'+a.epoch+':'+a.owner_generation+':'+digest(JSON.stringify(effects))).slice(0,24);const previous=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='held-history-authoring' AND json_extract(receipt,'$.operatorIntakeId')=? ORDER BY rowid DESC LIMIT 1").get(rigId,operatorIntakeId) as any;
  let predecessor:string|undefined;if(previous){const prior=JSON.parse(previous.receipt);if(prior.deadline>this.now()){this.stageHeldHistoryPickup(rigId,prior);return;}this.recordHeldHistoryNoticeOutcome(rigId,prior);const priorTask=this.repo.getById(prior.queueId),priorNotice=this.db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+prior.queueId) as any;const failedUnclaimedNotice=priorTask?.state==='pending'&&!priorTask.claimedAt&&digest(priorTask.body)===prior.bodyHash&&priorNotice?.delivery_state==='failed'&&priorNotice.sender_session==='watchdog@system'&&priorNotice.destination_session===prior.recipient&&priorNotice.audit_pointer===prior.queueId&&digest(priorNotice.body)===prior.noticeBodyHash;if(!failedUnclaimedNotice)this.stageHeldHistoryRetirement(rigId,prior);if(this.heldHistoryRecord(prior))this.stageHeldHistoryRetirement(rigId,prior,prior.recordQueueId);const pickup=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='held-history-pickup' AND json_extract(receipt,'$.authoringQueueId')=?").get(prior.queueId) as any;if(pickup){const p=JSON.parse(pickup.receipt);this.stageHeldHistoryRetirement(rigId,prior,p.queueId);this.recordHeldHistoryNoticeOutcome(rigId,p);const q=this.repo.getById(p.queueId),notice=this.db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+p.queueId) as any;this.observeDuty(p.queueId);if(!q||digest(q.body)!==p.bodyHash||!this.dutyFacts(p.queueId).retired||!notice||(!['delivered','failed'].includes(notice.delivery_state)&&!this.heldHistoryNoticeOutcomeContained(rigId,notice))||digest(notice.body)!==p.noticeBodyHash)return;}const retirements=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='held-history-retirement' AND json_extract(receipt,'$.authoringQueueId')=?").all(prior.queueId) as any[];for(const retirement of retirements){const t=JSON.parse(retirement.receipt),q=this.repo.getById(t.queueId);this.recordHeldHistoryNoticeOutcome(rigId,t);this.observeDuty(t.queueId);if(!q||!this.dutyFacts(t.queueId).retired)return;}const record=this.heldHistoryRecord(prior);if(this.repo.getById(prior.recordQueueId)&&!record)return;if(!this.heldNativeTerminal(prior.queueId,prior.recipient,prior.recipientGeneration)||record&&!this.heldNativeTerminal(prior.recordQueueId,'operator-agent@kernel',prior.operatorGeneration))return;const notice=this.db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+prior.queueId) as any;if(!notice||(!['delivered','failed'].includes(notice.delivery_state)&&!this.heldHistoryNoticeOutcomeContained(rigId,notice)))return;predecessor=prior.queueId;}
  if(predecessor)queueId='qitem-coordination-lifecycle-'+digest(queueId+':successor:'+predecessor).slice(0,24);if(this.repo.getById(queueId))return;
  const deadline=this.now()+1200000,recordQueueId='qitem-held-history-record-'+digest(queueId).slice(0,24),bindingOperationId='held-authoring:'+queueId;
  const r={kind:'held-history-authoring',queueId,previousQueueId:predecessor,packageKey:'held-history-authoring',recipient:a.owner_session,recipientGeneration:a.owner_generation,holder:a.owner_session,holderGeneration:a.owner_generation,epoch:a.epoch,operatorGeneration:plan.operatorGeneration,planRevision:plan.revision,deadline,operatorIntakeId,operatorIntakeBodyHash:digest(intake.body),operatorClaimedAt:intake.claimedAt,effects,recordQueueId,bindingOperationId};
  if(!this.heldHistoryAuthoringReady(rigId,r))return;
  const recordBody={kind:'coordinator-held-history-recovery.v1',rigId,operationId:bindingOperationId,owner:'operator-agent@kernel',generation:r.operatorGeneration,lead:r.recipient,leadGeneration:r.recipientGeneration,effects:effects.map(e=>e.outboxId),action:'reconcile-preserved-unknown-history',returnPath:{session:r.recipient,queueId:recordQueueId},deadline};
  const body=JSON.stringify({action:'author-exact-finite-held-history-recovery',rigId,queueId,claimCommand:'rig queue claim '+queueId,operatorIntakeId,recipientGeneration:r.recipientGeneration,deadline,grantsAuthority:false,effects,recordQueueId,recordBody,bindingContract:{rigId,operationId:bindingOperationId,effects,recovery:{queueId:recordQueueId,rowHash:'<actual full queue row hash after genuine Operator claim>'}},required:'Genuinely claim this exact finite authoring duty under your current native Lead identity. Inspect the preserved immutable UNKNOWN cohort. If justified, author recordBody unchanged in a local JSON file and use rig queue create --destination operator-agent@kernel --id '+recordQueueId+' --expires-at '+new Date(deadline).toISOString()+' --body-file <recordBodyFile> --no-nudge --json. This is your own native authorization testimony, never runtime-generated testimony. The current Operator already owns '+operatorIntakeId+'; it must actually read and genuinely claim the new record, derive its actual full queue row hash, then use rig coordinator held-history-recovery-bind <bindingContractFile>. Successful authoring-duty closure requires that exact current claimed record and immutable supported binding receipt. Preserve original delivery UNKNOWN, original claims/locks, all qualifications and scopes. Never resend old notices, execute product work, quarantine/adopt automatically, infer delivery or acceptance, extend an expired authorization, or close from prose.'});
  this.db.transaction(()=>{if(!this.heldHistoryAuthoringReady(rigId,r)||this.repo.getById(queueId))return;this.repo.createWithinTransaction({qitemId:queueId,sourceSession:'watchdog@system',destinationSession:r.recipient,expiresAt:new Date(deadline).toISOString(),body,identityProvenance:'system:operator-authorized-coordination',nudge:false});const noticeId=this.repo.stageHeldHistoryAuthoringWake(queueId,r.recipient,r.recipientGeneration),notice=this.db.prepare('SELECT body FROM outbox_entries WHERE outbox_id=?').get(noticeId) as {body:string};this.db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run(rigId,queueId,'coordinator-lifecycle-control',JSON.stringify({...r,bodyHash:digest(body),noticeBodyHash:digest(notice.body)}),digest(body));})();
 }
 private heldNativeTerminal(queueId:string,actor:string,generation:string):boolean {
  const q=this.db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(queueId) as any,last=this.db.prepare('SELECT * FROM queue_transitions WHERE qitem_id=? ORDER BY transition_id DESC LIMIT 1').get(queueId) as any;
  return !!q&&['failed','canceled'].includes(q.state)&&q.destination_session===actor&&this.authority.generation(actor)===generation&&(!q.claimed_at||q.claimed_by_generation_uuid===generation)&&last?.state===q.state&&last?.actor_session===actor&&last?.identity_provenance==='transport:v1';
 }
 private heldRetirementReady(rigId:string,r:any,excludeEffect?:string):boolean {
  const parent=this.lifecycleControl(r.authoringQueueId)?.receipt;if(!parent||parent.kind!=='held-history-authoring'||parent.deadline>this.now()||r.deadline<=this.now()||!this.heldHistoryAuthoringReady(rigId,{...parent,deadline:r.deadline},['wake-intent-'+parent.queueId,...(r.targetQueueId!==parent.recordQueueId?['wake-intent-'+r.targetQueueId]:[]),...(excludeEffect?[excludeEffect]:[])]))return false;
  const notice=this.db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+parent.queueId) as any;if(!notice||notice.sender_session!=='watchdog@system'||notice.destination_session!==parent.recipient||notice.audit_pointer!==parent.queueId||digest(notice.body)!==parent.noticeBodyHash||!['delivered','failed','indeterminate'].includes(notice.delivery_state))return false;
  const target=this.repo.getById(r.targetQueueId),pickup=this.lifecycleControl(r.targetQueueId)?.receipt;
  if(r.targetQueueId!==parent.queueId&&r.targetQueueId!==parent.recordQueueId&&!(pickup?.kind==='held-history-pickup'&&pickup.authoringQueueId===parent.queueId&&pickup.deadline===parent.deadline&&digest(target?.body??'')===pickup.bodyHash))return false;
  if(r.targetQueueId===parent.recordQueueId&&!this.heldHistoryRecord(parent))return false;if(pickup?.kind==='held-history-pickup'){const n=this.db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+r.targetQueueId) as any;if(!n||n.sender_session!=='watchdog@system'||n.destination_session!==r.recipient||n.audit_pointer!==r.targetQueueId||digest(n.body)!==pickup.noticeBodyHash||!['delivered','failed','indeterminate'].includes(n.delivery_state))return false;}
  return !!target&&['pending','in-progress','blocked'].includes(target.state)&&digest(target.body)===r.targetBodyHash&&target.destinationSession===r.recipient&&this.repo.heldHistoryAuthoringDebtReady(rigId,r.recipient,['wake-intent-'+parent.queueId,...(r.targetQueueId!==parent.recordQueueId?['wake-intent-'+r.targetQueueId]:[]),...(excludeEffect?[excludeEffect]:[])]);
 }
 heldHistoryRetirementAllows(queueId:string,actor:string,generation:string|null|undefined,provenance:string|null|undefined):boolean {
  if(!generation||provenance!=='transport:v1'||this.authority.generation(actor)!==generation)return false;
  const rows=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.targetQueueId')=? AND json_extract(receipt,'$.kind') IN ('held-history-retirement','lifecycle-retirement')").all(queueId) as any[];
  return rows.some(row=>{const r=JSON.parse(row.receipt),q=this.repo.getById(r.queueId),claim=this.db.prepare('SELECT claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(r.queueId) as any;return r.recipient===actor&&r.recipientGeneration===generation&&!!q?.claimedAt&&['in-progress','blocked'].includes(q.state)&&claim?.claimed_by_generation_uuid===generation&&this.dutyFacts(r.queueId).act;});
 }
 /** The only non-claimant terminal update allowed for a legacy qualification row is
  *  the exact failed disposition inside its live, claimed report-only retirement duty. */
 qualificationAssessmentRetirementAllows(queueId:string,actor:string,generation:string|null|undefined,provenance:string|null|undefined,nextState:string|undefined):boolean {
  if(nextState!=='failed'||!generation||provenance!=='transport:v1'||this.authority.generation(actor)!==generation)return false;
  const rows=this.db.prepare("SELECT rig_id,receipt FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='qualification-assessment-retirement' AND json_extract(receipt,'$.targetQueueId')=?").all(queueId) as Array<{rig_id:string;receipt:string}>;
  return rows.some(row=>{
   const r=JSON.parse(row.receipt),target=this.repo.getById(queueId),duty=this.repo.getById(r.queueId),dutyClaim=this.db.prepare('SELECT claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(r.queueId) as any;
   let evidenceValid:boolean;
   if(r.evidenceKind==='operator-accountability')evidenceValid=this.qualificationRetirementAccountabilityMatches(row.rig_id,r);
   else {
    const sweep=this.repo.getById(r.sweepFindingQueueId),sweepClaim=this.db.prepare('SELECT claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(r.sweepFindingQueueId) as any;
    evidenceValid=!!sweep&&sweep.state==='in-progress'&&!!sweep.claimedAt&&sweep.destinationSession===actor&&digest(sweep.body)===r.sweepFindingBodyHash&&sweepClaim?.claimed_by_generation_uuid===generation;
   }
   const linked=this.qualificationRetirementWakeFailed(queueId);
   return actor===r.recipient&&generation===r.recipientGeneration&&actor===r.worker&&generation===r.workerGeneration&&!!target&&target.state==='pending'&&!target.claimedAt&&target.destinationSession===actor&&digest(target.body)===r.targetBodyHash&&!!target.expiresAt&&Date.parse(target.expiresAt)<=this.now()&&!this.db.prepare('SELECT 1 FROM coordinator_assignments WHERE queue_id=?').get(queueId)&&!this.db.prepare('SELECT 1 FROM coordinator_packages WHERE rig_id=? AND package_key=?').get(row.rig_id,queueId)&&!!duty?.claimedAt&&['in-progress','blocked'].includes(duty.state)&&dutyClaim?.claimed_by_generation_uuid===generation&&evidenceValid&&this.dutyFacts(r.queueId).act&&linked;
  });
 }
 isQualificationAssessmentRetirementTarget(queueId:string):boolean {return !!this.db.prepare("SELECT 1 FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='qualification-assessment-retirement' AND json_extract(receipt,'$.targetQueueId')=? LIMIT 1").get(queueId);}
 private stageHeldHistoryRetirement(rigId:string,parent:any,targetQueueId=parent.queueId):void {this.stageDutyRetirement(rigId,parent,targetQueueId);}
 private heldHistoryNoticeOutcomeProof(rigId:string,row:any):boolean {
  const id=String(row.outbox_id??'');if(!id.startsWith('wake-intent-')||row.delivery_state!=='indeterminate')return false;
  const qid=id.slice('wake-intent-'.length),op=this.lifecycleControl(qid);if(!op||op.rigId!==rigId||!dutyKinds[op.receipt.kind as DutyKind])return false;const r=op.receipt,q=this.repo.getById(qid);
  if(!q||digest(q.body)!==r.bodyHash||row.sender_session!=='watchdog@system'||row.destination_session!==r.recipient||row.audit_pointer!==qid||digest(String(row.body))!==r.noticeBodyHash||!((this.heldNativeTerminal(qid,r.recipient,r.recipientGeneration))||(q.state==='done'&&!!q.claimedAt&&this.authority.generation(r.recipient)===r.recipientGeneration&&this.dutyPostcondition(rigId,r))||(dutyKinds[r.kind as DutyKind]?.effectClass==='report-only'&&q.state==='done'&&this.heldNativeTerminal(r.targetQueueId,r.recipient,r.recipientGeneration)&&digest(this.repo.getById(r.targetQueueId)?.body??'')===r.targetBodyHash)))return false;
  if(q.claimedAt){const claim=this.db.prepare('SELECT claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(qid) as any,last=this.db.prepare('SELECT * FROM queue_transitions WHERE qitem_id=? ORDER BY transition_id DESC LIMIT 1').get(qid) as any;return claim?.claimed_by_generation_uuid===r.recipientGeneration&&last?.actor_session===r.recipient&&last?.identity_provenance==='transport:v1';}
  const proof=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind') IN ('held-history-retirement','lifecycle-retirement') AND json_extract(receipt,'$.targetQueueId')=? ORDER BY rowid DESC LIMIT 1").get(rigId,qid) as any;if(!proof)return false;
  const t=JSON.parse(proof.receipt),d=this.repo.getById(t.queueId),claim=this.db.prepare('SELECT claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(t.queueId) as any;return !!d?.claimedAt&&claim?.claimed_by_generation_uuid===r.recipientGeneration;
 }
 /** System administrative actions whose pointed-to task may carry an
  *  outcome-only receipt. Nothing here can grant product authority. */
 private static readonly SYSTEM_COORDINATOR_RECOVERY_ACTIONS=['recover-unavailable-coordinator','recover-expired-idle-transfer','restore-current-held-history-binding','reconcile-current-coordinator-lease','active-expiry-recover'] as readonly string[];
  /** The one lease tuple whose expiry duty may carry an outcome-only receipt. Bound
   *  into the recovery key by the producer, so it is re-derived here, never trusted. */
  private static readonly SYSTEM_ACTIVE_EXPIRY_ACTION='active-expiry-recover';
  private expiryRecoveryContractProof(body:any):string|null {
   const lease=body?.activeExpiry;
   if(!lease||typeof lease!=='object'||Array.isArray(lease))return null;
   if(typeof lease.owner!=='string'||!lease.owner||typeof lease.ownerGeneration!=='string'||!lease.ownerGeneration||!Number.isFinite(lease.expectedLeaseUntil))return null;
   // Advisory fields may be absent on an older body; the exact three key inputs are not.
   return JSON.stringify({owner:lease.owner,ownerGeneration:lease.ownerGeneration,expectedLeaseUntil:lease.expectedLeaseUntil});
  }
 private static readonly SYSTEM_WAKE_ROLLOUT='materialize-standard-resilience';
 private static readonly SYSTEM_WAKE_HOLD='resolve-exact-coordination-task-hold';
 /** A completed detector notice is not original-work completion or delivery. */
 private diagnosticWakeProof(rigId:string,row:any,producerRequired:boolean):{queueId:string;bodyHash:string;generation:string;terminalId:number}|null {
  const id=String(row?.outbox_id??''),qid=id.slice('wake-intent-'.length),recipient='operator-agent@kernel';
  if(!id.startsWith('wake-intent-')||row.delivery_state!=='indeterminate'||!this.systemPointerNotice(row,qid,recipient))return null;
  const q=this.repo.getById(qid);let body:any;try{body=JSON.parse(q?.body??'');}catch{return null;}
  const generation=this.authority.generation(recipient),first=this.db.prepare('SELECT * FROM queue_transitions WHERE qitem_id=? ORDER BY transition_id LIMIT 1').get(qid) as any,last=this.db.prepare('SELECT * FROM queue_transitions WHERE qitem_id=? ORDER BY transition_id DESC LIMIT 1').get(qid) as any;
  if(!q||q.sourceSession!=='watchdog@system'||q.destinationSession!==recipient||q.state!=='done'||!generation||body.recipientGeneration!==generation||first?.actor_session!=='watchdog@system'||first.identity_provenance!=='system:operator-authorized-coordination'||last?.state!=='done'||last.actor_session!==recipient||last.identity_provenance!=='transport:v1'||!this.systemNativeCustody(qid,recipient,generation))return null;
  if(body.action!=='reconcile-refused-stuck-finding'||typeof body.stuckSweepRecoveryKey!=='string'||body.returnPath?.queueId!==qid||body.returnPath?.actor!==recipient||body.returnPath?.generation!==generation||'qitem-stuck-sweep-control-'+digest(body.stuckSweepRecoveryKey+':'+(body.previousQueueId??'initial')).slice(0,24)!==qid)return null;
  const original=this.db.prepare('SELECT a.rig_id,q.body,q.source_session,q.destination_session FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.queue_id=? UNION ALL SELECT a.rig_id,q.body,q.source_session,q.destination_session FROM coordinator_stage_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.queue_id=?').get(body.original?.qitemId,body.original?.qitemId) as any;
  if(!original||original.rig_id!==rigId||digest(original.body)!==body.original.bodyHash||original.source_session!==body.original.sourceSession||original.destination_session!==body.original.destinationSession)return null;
  if(producerRequired){const op=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='diagnostic-wake-producer'").get(rigId,'diagnostic-wake-producer:'+qid) as any;let r:any;try{r=JSON.parse(op?.receipt??'');}catch{return null;}
   if(r.queueId!==qid||r.bodyHash!==digest(q.body)||r.generation!==generation||r.recoveryKey!==body.stuckSweepRecoveryKey||digest(r.sourceFacts)!==body.original.factsHash||digest(JSON.stringify([r.sourceFacts,body.kind,body.original.evidenceAt,body.reason,generation]))!==r.recoveryKey)return null;
  }
  return {queueId:qid,bodyHash:digest(q.body),generation,terminalId:last.transition_id};
 }
 disposeDiagnosticWake(actor:string,generation:string,input:{rigId:string;outboxId:string;noticeSnapshotHash:string;taskBodyHash:string;evidenceRef:string}):void {
  this.db.transaction(()=>{
   this.authority.assertCurrentOperator(actor,generation);
   if(this.plan(input.rigId)?.operatorGeneration!==generation||typeof input.evidenceRef!=='string'||!input.evidenceRef.trim())fail('diagnostic_wake_evidence_required','Current genuine Operator, current plan and actual terminal evidence required');
   const row=this.db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get(input.outboxId) as any,proof=row?this.diagnosticWakeProof(input.rigId,row,false):null;
   if(!proof||input.noticeSnapshotHash!==digest(JSON.stringify(row))||input.taskBodyHash!==proof.bodyHash)fail('diagnostic_wake_evidence_required','Exact completed native diagnostic and unchanged UNKNOWN notice required; no historical producer evidence is invented');
   const receipt={...proof,outboxId:input.outboxId,noticeSnapshotHash:input.noticeSnapshotHash,actor,generation,evidenceRef:input.evidenceRef,deliveryConclusion:'unknown',outcomeOnly:true,grantsAuthority:false,originalMutations:0};
   const id='diagnostic-wake-disposition:'+input.outboxId,prior=this.db.prepare('SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=?').get(input.rigId,id) as any;
   if(prior){if(prior.receipt!==JSON.stringify(receipt))fail('diagnostic_wake_conflict','Existing disposition is immutable');return;}
   this.db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run(input.rigId,id,'diagnostic-wake-disposition',JSON.stringify(receipt),digest(JSON.stringify(receipt)));
  }).immediate();
 }
 private diagnosticWakeContained(row:any):boolean {
  const records=this.db.prepare("SELECT rig_id,receipt FROM coordinator_operations WHERE operation_id=? AND kind='diagnostic-wake-disposition'").all('diagnostic-wake-disposition:'+row.outbox_id) as any[];
  return records.some(op=>{let r:any;try{r=JSON.parse(op.receipt);}catch{return false;}const p=this.diagnosticWakeProof(op.rig_id,row,false);return !!p&&r.actor==='operator-agent@kernel'&&r.generation===p.generation&&r.queueId===p.queueId&&r.bodyHash===p.bodyHash&&r.terminalId===p.terminalId&&r.noticeSnapshotHash===digest(JSON.stringify(row))&&r.deliveryConclusion==='unknown'&&r.outcomeOnly===true&&r.grantsAuthority===false&&r.originalMutations===0;});
 }
 /** The exact pointer-wake template. A wake carrying instructions is never eligible. */
 private systemPointerNotice(row:any,qid:string,recipient:string):boolean {
  if(row.outbox_id!=='wake-intent-'+qid||row.sender_session!=='watchdog@system'||row.destination_session!==recipient||row.audit_pointer!==qid)return false;
  if(this.db.prepare('SELECT 1 FROM outbox_historical_quarantines WHERE outbox_id=?').get(row.outbox_id))return false;
  const parts=String(row.body??'').split('\n---\n');
  if(parts.length!==3)return false;
  if(parts[0]?.startsWith('From: watchdog@system\nTo: '+recipient+'\nSent: ')!==true)return false;
  return parts[1]==='Queue handoff: '+qid+' - check your queue.'&&parts[2]==='↩ Reply: rig send watchdog@system "..."';
 }
 /** Validated origin of the pointed-to task. Rollout items must recompute their
  *  deterministic id from the persisted rolloutKey, rig, Operator generation and
  *  reasons, so a body action alone never qualifies. */
 private systemTaskProof(qid:string,recipient:string):{rigId:string;action:string;body:any;bodyHash:string}|null {
  const q=this.repo.getById(qid);if(!q||q.sourceSession!=='watchdog@system'||q.destinationSession!==recipient)return null;
  const first=this.db.prepare('SELECT actor_session,identity_provenance FROM queue_transitions WHERE qitem_id=? ORDER BY transition_id LIMIT 1').get(qid) as any;
  if(first?.actor_session!=='watchdog@system'||first?.identity_provenance!=='system:operator-authorized-coordination')return null;
  let body:any;try{body=JSON.parse(q.body);}catch{return null;}
  if(!body||typeof body.rigId!=='string'||!body.rigId||typeof body.recipientGeneration!=='string'||!body.recipientGeneration)return null;
  if(body.grantsAuthority!==undefined&&body.grantsAuthority!==false)return null;
  const action=String(body.action??'');
if(action===CoordinationRecoveryService.SYSTEM_WAKE_ROLLOUT){
    if(!Array.isArray(body.reasons)||!body.reasons.length||typeof body.rolloutKey!=='string'||!body.rolloutKey)return null;
    // Mirror the producer's key derivation exactly. An expiry-contract rollout item
    // is keyed by its lease tuple too, so the proof must recompute the same suffix or
    // the item's UNKNOWN wake could never be outcome-contained and would sit forever
    // as Operator-seat debt. An absent contract keeps the original plain derivation.
    const contract=body.expiryRecoveryContract;
    let suffix='';
    if(contract!==undefined){
     if(!contract||typeof contract!=='object'||contract.schema!=='expiry-recovery-contract.v1'||!contract.token||typeof contract.token!=='object')return null;
     const {epoch,generation}=contract.token;
     if(contract.token.rigId!==body.rigId||!Number.isInteger(epoch)||epoch<1||typeof generation!=='string'||!generation||!Number.isFinite(contract.expectedLeaseUntil))return null;
     suffix=':'+digest(`${epoch}:${generation}:${contract.expectedLeaseUntil}`);
    }
    const rolloutKey=digest(body.rigId+':'+body.recipientGeneration+':'+body.reasons.join('|')+suffix);
    if(rolloutKey!==body.rolloutKey)return null;
    if('qitem-resilience-rollout-'+digest(rolloutKey+':'+(body.previousQueueId??'initial')).slice(0,24)!==qid)return null;
   }else if(action===CoordinationRecoveryService.SYSTEM_WAKE_HOLD){
   if(!this.validAccountableIntake(body.rigId,qid,q)&&!(typeof body.rootQueueId==='string'&&this.validAccountableIntakeChainItem(body.rigId,q,body.rootQueueId)))return null;
  }else if(action==='record-exact-native-terminal-return'){
   const saved=this.db.prepare("SELECT rig_id,receipt FROM coordinator_operations WHERE operation_id=? AND kind='native-terminal-return-control'").get(qid) as {rig_id:string;receipt:string}|undefined;
   if(!saved||saved.rig_id!==body.rigId)return null;
   let r:any;try{r=JSON.parse(saved.receipt);}catch{return null;}
   if(r.queueId!==qid||r.bodyHash!==digest(q.body)||r.originalQueueId!==body.originalQueueId||r.packageKey!==body.packageKey||r.worker!==recipient||r.workerGeneration!==body.recipientGeneration||r.expiresAt!==Date.parse(q.expiresAt??'')||body.deadline!==r.expiresAt)return null;
   const claim=this.db.prepare('SELECT claimed_at FROM queue_items WHERE qitem_id=?').get(qid) as {claimed_at:string|null}|undefined;
   if(claim?.claimed_at&&Date.parse(claim.claimed_at)>r.expiresAt)return null;
  }else if(action==='reconcile-refused-stuck-finding'){
   if(!this.diagnosticWakeProof(body.rigId,this.db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+qid),true))return null;
  }else if(action==='reconcile-transferred-baton'){
   if(!Number.isInteger(body.epoch)||body.epoch<1||typeof body.batonId!=='string'||!body.batonId)return null;
   if('qitem-coordination-peer-'+digest(body.rigId+':'+body.epoch).slice(0,24)!==qid)return null;
  }else if(CoordinationRecoveryService.SYSTEM_COORDINATOR_RECOVERY_ACTIONS.includes(action)){
   // Recompute the producer's key and chain id. Native custody contains only the
   // pointer wake; it does not resolve the recovery or confer holder authority.
   if(!Number.isInteger(body.epoch)||body.epoch<1||typeof body.reason!=='string'||!body.reason||typeof body.recoveryKey!=='string')return null;
   if(body.previousQueueId!==null&&typeof body.previousQueueId!=='string')return null;
   // The expiry duty's lease tuple participates in the producer's key, so it must be
   // re-derived here from persisted fields. Authority is deliberately NOT re-read:
   // a tuple that has since drifted must still be containable, exactly like the
   // held-history receipts this branch already carries.
   let lease='';
   if(action===CoordinationRecoveryService.SYSTEM_ACTIVE_EXPIRY_ACTION){
    // Absent or malformed frozen tuple fails closed; the expiry duty has no contract-free form.
    const contract=this.expiryRecoveryContractProof(body);
    if(!contract)return null;
    lease=contract;
   }else if(body.activeExpiry!==undefined)return null;
   const recoveryKey=digest((body.heldHistoryAdmission?JSON.stringify(body.heldHistoryAdmission):'')+lease+body.rigId+':'+body.epoch+':'+body.recipientGeneration+':'+action+':'+body.reason);
   if(recoveryKey!==body.recoveryKey||'qitem-coordination-recovery-'+digest(recoveryKey+':'+(body.previousQueueId??'initial')).slice(0,24)!==qid)return null;
   if(body.returnPath?.queueId!==qid||body.returnPath?.actor!==recipient)return null;
  }else return null;
  return {rigId:body.rigId,action,body,bodyHash:digest(q.body)};
 }
 /** Genuine native custody is the outcome: the exact recipient, its current
  *  generation, a transport:v1 claim, and a non-pending state. */
 private systemNativeCustody(qid:string,recipient:string,expectedGeneration:string):boolean {
  const q=this.repo.getById(qid);if(!q||!['in-progress','blocked','done','failed','canceled'].includes(q.state))return false;
  const claim=this.db.prepare("SELECT * FROM queue_transitions WHERE qitem_id=? AND state='in-progress' AND transition_note='claimed' AND actor_session=? AND identity_provenance='transport:v1' ORDER BY transition_id LIMIT 1").get(qid,recipient) as any;
  if(!claim)return false;
  const row=this.db.prepare('SELECT claimed_by_generation_uuid,claimed_at FROM queue_items WHERE qitem_id=?').get(qid) as any;
  const generation=this.authority.generation(recipient);
  return row?.claimed_by_generation_uuid===generation&&generation===expectedGeneration;
 }
 /** A holder is deliberately not required: rollout items exist precisely when a
  *  rig has none. Without a plan only a genuine current Operator plus recomputed
  *  rollout provenance qualifies, so a pre-plan task is never orphaned. */
 private systemWakeAuthority(rigId:string,recipient:string,generation:string,action:string):boolean {
  const plan=this.plan(rigId);
  if(action==='record-exact-native-terminal-return')return !!plan&&plan.operatorGeneration===this.authority.generation('operator-agent@kernel')&&this.authority.generation(recipient)===generation;
  if(plan)return plan.operatorGeneration===generation;
  return action===CoordinationRecoveryService.SYSTEM_WAKE_ROLLOUT&&recipient==='operator-agent@kernel'&&this.authority.generation(recipient)===generation;
 }
 private systemWakeOutcomeProof(rigId:string,row:any):{task:any;generation:string}|null {
  const id=String(row.outbox_id??'');
  if(!id.startsWith('wake-intent-')||row.delivery_state!=='indeterminate')return null;
  const task=this.systemTaskProof(id.slice('wake-intent-'.length),String(row.destination_session??''));if(!task||task.rigId!==rigId)return null;
  if(!this.systemPointerNotice(row,id.slice('wake-intent-'.length),String(row.destination_session??'')))return null;
  if(task.body.recipientGeneration!==this.systemTaskGeneration(task))return null;
  const generation=this.authority.generation(String(row.destination_session??''));
  if(!generation||!this.systemNativeCustody(id.slice('wake-intent-'.length),String(row.destination_session??''),task.body.recipientGeneration))return null;
  if(task.action==='reconcile-transferred-baton'){
   // A native claim alone is insufficient for a baton-transfer notice. Its
   // exact epoch must have a genuine acknowledge receipt and native done closure.
   // Historical acknowledgment proves consumption, never current lease authority.
   const qid=id.slice('wake-intent-'.length),q=this.repo.getById(qid);
   const terminal=this.db.prepare('SELECT * FROM queue_transitions WHERE qitem_id=? ORDER BY transition_id DESC LIMIT 1').get(qid) as any;
   if(q?.state!=='done'||terminal?.state!=='done'||terminal.actor_session!==row.destination_session||terminal.identity_provenance!=='transport:v1')return null;
   const ack=this.db.prepare("SELECT 1 FROM coordinator_operations WHERE rig_id=? AND kind='acknowledge' AND json_valid(receipt) AND json_extract(receipt,'$.rig_id')=? AND json_extract(receipt,'$.epoch')=? AND json_extract(receipt,'$.baton_id')=? AND json_extract(receipt,'$.owner_session')=? AND json_extract(receipt,'$.owner_generation')=? AND json_extract(receipt,'$.state')='active'").get(rigId,rigId,task.body.epoch,task.body.batonId,row.destination_session,generation);
   if(!ack||!this.plan(rigId))return null;
  }else if(!this.systemWakeAuthority(rigId,String(row.destination_session??''),generation,task.action))return null;
  return {task,generation};
 }
 private systemTaskGeneration(task:any):string {return String(task.body.recipientGeneration??'');}
 /** The rig of a wake row, resolved through its validated system task rather
  *  than the endpoints' node membership. This is what kernel seats need. */
 private systemTaskRig(row:any):string|null {
  const id=String(row.outbox_id??'');if(!id.startsWith('wake-intent-'))return null;
  return this.systemTaskProof(id.slice('wake-intent-'.length),String(row.destination_session??''))?.rigId??null;
 }
 /** Outcome-only containment. The notice row is never written, relabelled or
  *  acknowledged: `deliveryConclusion` stays unknown for ever. */
 private systemWakeOutcomeContained(rigId:string,row:any):boolean {
  const taskRig=this.systemTaskRig(row);if(!taskRig||!this.systemWakeOutcomeProof(taskRig,row))return false;
  const proof=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='system-wake-outcome' AND operation_id=?").get(taskRig,'system-wake-outcome:'+row.outbox_id) as any;
  if(!proof)return false;
  try{const p=JSON.parse(proof.receipt);return p.outboxId===row.outbox_id&&p.noticeSnapshotHash===digest(JSON.stringify(row))&&p.taskBodyHash===digest(this.repo.getById(p.taskQueueId)?.body??'')&&p.deliveryConclusion==='unknown'&&p.originalMutations===0&&p.outcomeOnly===true&&p.grantsAuthority===false;}catch{return false;}
 }
 /** Bind only producer-authored assignment pointers to actual native pickup.
  * Historical holder receipts prove origin, never current authority or delivery. */
 /** Exact native assignment claim, including archived history. The producer's
  * row timestamp and transition timestamp are independent; the immutable after
  * snapshot binds the retained claim tuple, never a clock tolerance. */
 private assignmentNativeClaim(queueId:string,recipient:string,generation:string,source:string,body:string):number|null {
  const q=this.db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(queueId) as any;
  if(!q||!['in-progress','blocked','done','failed','canceled'].includes(q.state)||q.source_session!==source||q.destination_session!==recipient||q.body!==body||!q.claimed_at||q.claimed_by_generation_uuid!==generation)return null;
  const history=this.repo.transitionLog.listForQitem(queueId);
  for(let i=history.length-1;i>=0;i--){
   const t=history[i]!;
   if(t.state!=='in-progress'||t.transitionNote!=='claimed'||t.actorSession!==recipient||t.identityProvenance!=='transport:v1')continue;
   const r=this.nativeCustodyEvidenceTransition(t);if(!r||r.actorGeneration!==generation)continue;
   const before=r.beforeQueue,after=r.afterQueue;
   if(!['pending','blocked'].includes(String(before.state))||after.state!=='in-progress'||
      [before,after].some(snapshot=>snapshot.qitem_id!==queueId||snapshot.source_session!==source||snapshot.destination_session!==recipient||snapshot.body!==body)||
      after.claimed_at!==q.claimed_at||after.claimed_by_generation_uuid!==q.claimed_by_generation_uuid)return null;
   return t.transitionId;
  }
  return null;
 }
 /** Bind only producer-authored assignment pointers to actual native pickup.
   * Historical holder receipts prove origin, never current authority or delivery. */
 private assignmentWakeProof(row:any):{rigId:string;queueId:string;bodyHash:string;assignmentHash:string;recipientGeneration:string;claimTransitionId:number}|null {
  if(row.delivery_state!=='indeterminate'||row.identity_provenance!=='system:operator-authorized-coordination')return null;
  const a=this.db.prepare('SELECT a.*,q.body,q.source_session,p.contract,p.contract_hash FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id JOIN coordinator_packages p ON p.rig_id=a.rig_id AND p.package_key=a.package_key WHERE a.queue_id=?').get(row.audit_pointer) as any;
  if(!a||a.destination!==row.destination_session||a.source_session!==a.owner_session||a.body_hash!==digest(a.body)||digest(a.contract)!==a.contract_hash)return null;
  let contract:any;try{contract=JSON.parse(a.contract);}catch{return null;}
  if(contract.destination!==a.destination||contract.bodyHash!==a.body_hash)return null;
  const creation=this.repo.transitionLog.listForQitem(a.queue_id)[0];
  if(!creation||creation.actorSession!==a.owner_session||creation.identityProvenance!=='system:operator-authorized-coordination')return null;
  let sender=a.owner_session,senderGeneration=a.owner_generation,bareBody='Queue handoff: '+a.queue_id+' - check your queue.';
  let tags:any;try{tags=JSON.parse(row.tags??'[]');}catch{return null;}
  if(!Array.isArray(tags))return null;
  if(row.outbox_id==='wake-intent-'+a.queue_id){
   if(tags.includes('queue:coordinator-resume'))return null;
  }else{
   if(tags.length!==2||tags[0]!=='queue:coordinator-resume')return null;
   let proof:any;try{proof=JSON.parse(tags[1]);}catch{return null;}
   if(!proof||Object.keys(proof).sort().join(',')!=='epoch,generation,recipientGeneration,rigId'||proof.rigId!==a.rig_id||!Number.isSafeInteger(proof.epoch)||proof.epoch<1||typeof proof.generation!=='string'||!proof.generation)return null;
   const prefix='wake-intent-coordinator-'+a.queue_id+'-'+proof.epoch+'-'+proof.generation+'-';
   if(!String(row.outbox_id).startsWith(prefix)||!/^\d+$/.test(String(row.outbox_id).slice(prefix.length)))return null;
   const bucket=Number(String(row.outbox_id).slice(prefix.length));
   if(bucket!==Math.floor(Date.parse(row.ts_dispatched)/30000))return null;
   const authority=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind IN ('acknowledge','resume-owned','enable') AND json_extract(receipt,'$.rig_id')=? AND json_extract(receipt,'$.epoch')=? AND json_extract(receipt,'$.owner_session')=? AND json_extract(receipt,'$.owner_generation')=? ORDER BY rowid DESC LIMIT 1").get(a.rig_id,a.rig_id,proof.epoch,row.sender_session,proof.generation) as {receipt:string}|undefined;
   if(!authority||proof.recipientGeneration!==this.authority.generation(a.destination))return null;
   sender=row.sender_session;senderGeneration=proof.generation;
   bareBody='Resume the existing pending assignment '+a.queue_id+'; verify current coordinator authority and native identity before claiming. This wake creates no new assignment or acceptance.';
  }
  if(row.sender_session!==sender)return null;
  const parts=String(row.body??'').split('\n---\n'),header=parts[0]?.split('\n');
  if(parts.length!==3||header?.length!==3||header[0]!=='From: '+sender||header[1]!=='To: '+a.destination||!/^Sent: \d{2}-\d{2} \d{2}:\d{2}Z · gen /.test(header[2]!)||!header[2]!.endsWith(' · gen '+senderGeneration.slice(0,8))||parts[1]!==bareBody||parts[2]!=='↩ Reply: rig send '+sender+' "..."')return null;
  const generation=this.authority.generation(a.destination);
  if(!generation)return null;
  const claim=this.assignmentNativeClaim(a.queue_id,a.destination,generation,a.source_session,a.body);
  if(claim===null)return null;
  // disposition_id advances through genuine dispose and is not dispatch identity.
  const {disposition_id:_,body,...immutable}=a;
  return {rigId:a.rig_id,queueId:a.queue_id,bodyHash:digest(body),assignmentHash:digest(JSON.stringify(immutable)),recipientGeneration:generation,claimTransitionId:claim};
 }
 private assignmentWakeOutcomeContained(rigId:string,row:any):boolean {
  const proof=this.assignmentWakeProof(row);if(!proof||proof.rigId!==rigId)return false;
  const saved=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='assignment-wake-outcome'").get(rigId,'assignment-wake-outcome:'+row.outbox_id) as {receipt:string}|undefined;
  if(!saved)return false;
  try{const r=JSON.parse(saved.receipt);return r.noticeSnapshotHash===digest(JSON.stringify(row))&&r.assignmentHash===proof.assignmentHash&&r.taskBodyHash===proof.bodyHash&&r.taskQueueId===proof.queueId&&r.recipientGeneration===proof.recipientGeneration&&r.claimTransitionId===proof.claimTransitionId&&r.deliveryConclusion==='unknown'&&r.originalMutations===0&&r.outcomeOnly===true&&r.grantsAuthority===false;}catch{return false;}
 }
 private assignmentWakeCursor=new Map<string,number>();
 private recordAssignmentWakeOutcomes(rigId:string):void {
  let from=this.assignmentWakeCursor.get(rigId)??0,scanned=0;
  while(scanned<2000){
   const rows=this.db.prepare("SELECT o.rowid scan_rowid,o.* FROM coordinator_assignments a JOIN outbox_entries o ON o.audit_pointer=a.queue_id WHERE a.rig_id=? AND o.rowid>? AND o.delivery_state='indeterminate' AND NOT EXISTS(SELECT 1 FROM coordinator_operations c WHERE c.rig_id=a.rig_id AND c.operation_id='assignment-wake-outcome:'||o.outbox_id AND c.kind='assignment-wake-outcome') ORDER BY o.rowid LIMIT 200").all(rigId,from) as any[];
   for(const scannedRow of rows){
    const {scan_rowid:_,...row}=scannedRow,proof=this.assignmentWakeProof(row);if(!proof||proof.rigId!==rigId)continue;
    const receipt={outboxId:row.outbox_id,noticeSnapshotHash:digest(JSON.stringify(row)),taskQueueId:proof.queueId,taskBodyHash:proof.bodyHash,assignmentHash:proof.assignmentHash,recipient:row.destination_session,recipientGeneration:proof.recipientGeneration,claimTransitionId:proof.claimTransitionId,deliveryConclusion:'unknown',originalMutations:0,outcomeOnly:true,grantsAuthority:false};
    this.db.prepare('INSERT OR IGNORE INTO coordinator_operations VALUES (?,?,?,?,?)').run(rigId,'assignment-wake-outcome:'+row.outbox_id,'assignment-wake-outcome',JSON.stringify(receipt),digest(JSON.stringify(receipt)));
   }
   scanned+=rows.length;from=rows.length?Number(rows[rows.length-1].scan_rowid):0;
   if(rows.length<200){from=0;break;}
  }
  this.assignmentWakeCursor.set(rigId,from);
 }
 /** One bounded scan per pass, written by the registered observer path only. */
 private recordSystemWakeOutcomes(rigId:string):void {
  this.recordAssignmentWakeOutcomes(rigId);
  this.recordNativeReturnNoticeOutcomes(rigId);
  // Completed scope may no longer be traversed by task reconciliation. Record only
  // exact terminal lifecycle pointer outcomes; never infer transport delivery.
  const completed=this.db.prepare("SELECT c.receipt FROM coordinator_operations c JOIN queue_items q ON q.qitem_id=json_extract(c.receipt,'$.queueId') JOIN outbox_entries o ON o.outbox_id='wake-intent-'||q.qitem_id WHERE c.rig_id=? AND c.kind='coordinator-lifecycle-control' AND q.state IN ('done','failed','denied','canceled','handed-off') AND o.delivery_state='indeterminate' AND NOT EXISTS (SELECT 1 FROM coordinator_operations p WHERE p.rig_id=c.rig_id AND p.kind='held-history-control-outcome' AND p.operation_id='held-control-outcome:'||o.outbox_id) ORDER BY c.rowid LIMIT 200").all(rigId) as Array<{receipt:string}>;
  for(const row of completed)this.recordHeldHistoryNoticeOutcome(rigId,JSON.parse(row.receipt));
  // Paged, and the rig is resolved per row before anything is written, so a busy
  // project can never starve another project's older notice.
  let from=0,scanned=0;
  while(scanned<2000){
   const rows=this.db.prepare("SELECT o.rowid AS scan_rowid,o.* FROM outbox_entries o JOIN queue_items q ON q.qitem_id=o.audit_pointer WHERE o.rowid>? AND o.delivery_state='indeterminate' AND o.outbox_id='wake-intent-'||q.qitem_id AND q.source_session='watchdog@system' AND q.state IN ('in-progress','blocked','done','failed','canceled') AND json_valid(q.body) AND json_extract(q.body,'$.rigId')=? AND NOT EXISTS (SELECT 1 FROM coordinator_operations c WHERE c.rig_id=? AND c.kind='system-wake-outcome' AND c.operation_id='system-wake-outcome:'||o.outbox_id) ORDER BY o.rowid ASC LIMIT 200").all(from,rigId,rigId) as any[];
   if(!rows.length)break;
   for(const scannedRow of rows){
   const {scan_rowid:_,...row}=scannedRow;
   const proof=this.systemWakeOutcomeProof(rigId,row);if(!proof)continue;
   const qid=String(proof.task.rigId===rigId?row.outbox_id.slice('wake-intent-'.length):'');
   const terminal=this.db.prepare('SELECT * FROM queue_transitions WHERE qitem_id=? ORDER BY transition_id DESC LIMIT 1').get(qid) as any;
   const plan=this.plan(rigId);
   const receipt={outboxId:row.outbox_id,noticeSnapshotHash:digest(JSON.stringify(row)),taskQueueId:qid,taskBodyHash:proof.task.bodyHash,action:proof.task.action,rigId,recipient:row.destination_session,recipientGeneration:proof.generation,claimTransitionId:(this.db.prepare("SELECT transition_id FROM queue_transitions WHERE qitem_id=? AND transition_note='claimed' AND actor_session=? AND identity_provenance='transport:v1' ORDER BY transition_id LIMIT 1").get(qid,row.destination_session) as any)?.transition_id??null,taskStateAtRecord:this.repo.getById(qid)!.state,...(terminal&&['done','failed','canceled'].includes(terminal.state)?{terminal:{transitionId:terminal.transition_id,state:terminal.state}}:{}),authorityBasis:{...(proof.task.action==='reconcile-transferred-baton'?{acknowledgedEpoch:proof.task.body.epoch,recipientGeneration:proof.generation}:{operatorGeneration:proof.generation}),planRevision:plan?.revision??null},deliveryConclusion:'unknown',originalMutations:0,outcomeOnly:true,nonExecutable:true,grantsAuthority:false};
   this.db.prepare('INSERT OR IGNORE INTO coordinator_operations VALUES (?,?,?,?,?)').run(rigId,'system-wake-outcome:'+row.outbox_id,'system-wake-outcome',JSON.stringify(receipt),digest(JSON.stringify(receipt)));
  }
   scanned+=rows.length;from=Number((rows[rows.length-1] as any).scan_rowid);
   if(rows.length<200)break;
  }
 }
 heldHistoryNoticeOutcomeContained(rigId:string,row:any):boolean {
  if(!this.heldHistoryNoticeOutcomeProof(rigId,row))return false;
  const proof=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='held-history-control-outcome'").get(rigId,'held-control-outcome:'+row.outbox_id) as any;
  if(!proof)return false;try{const p=JSON.parse(proof.receipt);return p.outboxId===row.outbox_id&&p.noticeSnapshotHash===digest(JSON.stringify(row))&&p.deliveryConclusion==='unknown'&&p.originalMutations===0&&p.outcomeOnly===true;}catch{return false;}
 }
 /** The one consumption predicate both debt gates call. */
 noticeOutcomeContained(rigId:string,row:any):boolean {
  return this.nativeReturnNoticeOutcomeContained(rigId,row)||this.assignmentWakeOutcomeContained(rigId,row)||this.heldHistoryNoticeOutcomeContained(rigId,row)||this.systemWakeOutcomeContained(rigId,row)||this.diagnosticWakeContained(row);
 }
 /** G15 C1 producer binding: one indeterminate watchdog instruction notice for exactly
  * one native-return continuation/retirement proof of this rig. Pending/sending rows can
  * never bind; nothing is re-sent and no UNKNOWN is rewritten. */
 private static readonly NATIVE_RETURN_NOTICE_KINDS={continuation:'native-terminal-return-continuation',retirement:'native-terminal-return-retirement'} as const;
 private nativeReturnNoticeBinding(rigId:string,row:any):{rigId:string;kind:'continuation'|'retirement';proofId:string;controlQueueId:string;worker:string;workerGeneration:string;noticeRow:any;controlReceipt:any}|null{
  if(!row||row.delivery_state!=='indeterminate'||row.sender_session!=='watchdog@system'||row.identity_provenance!=='system:operator-authorized-coordination')return null;
  const outboxId=String(row.outbox_id??'');if(!outboxId.startsWith('wake-intent-native-return-'))return null;
  const proofId=outboxId.slice('wake-intent-'.length);
  const kind=proofId.startsWith('native-return-continuation:')?'continuation':proofId.startsWith('native-return-retirement:')?'retirement':null;
  if(!kind)return null;const controlQueueId=proofId.slice(('native-return-'+kind+':').length);if(!controlQueueId||controlQueueId.includes(':'))return null;
  const op=this.db.prepare("SELECT rig_id,receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind=?").get(rigId,proofId,CoordinationRecoveryService.NATIVE_RETURN_NOTICE_KINDS[kind]) as {rig_id:string;receipt:string}|undefined;
  if(!op||op.rig_id!==rigId)return null;let n:any;try{n=JSON.parse(op.receipt);}catch{return null;}
  if(!n||n.outboxId!==outboxId||n.controlQueueId!==controlQueueId||digest(row.body)!==n.outboxBodyHash||n.worker!==row.destination_session||(kind==='retirement'?n.retirement!==true:n.retirement!==undefined))return null;
  if(String(row.audit_pointer??'')!==controlQueueId)return null;
  let tags:any;try{tags=JSON.parse(row.tags??'[]');}catch{return null;}
  if(!Array.isArray(tags)||tags.length!==3||tags[0]!=='queue:native-return-continuation'||tags[1]!==proofId||tags[2]!=='queue:recipient-generation:'+n.workerGeneration)return null;
  const saved=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='native-terminal-return-control'").get(rigId,controlQueueId) as {receipt:string}|undefined;
  if(!saved)return null;let c:any;try{c=JSON.parse(saved.receipt);}catch{return null;}
  if(!c||typeof c!=='object'||Array.isArray(c))return null;
  const control=this.repo.getById(controlQueueId);let body:any;try{body=control?JSON.parse(control.body):null;}catch{return null;}
  if(!control||!body||typeof n.worker!=='string'||typeof n.workerGeneration!=='string'||!n.workerGeneration||
     c.queueId!==controlQueueId||c.worker!==n.worker||c.workerGeneration!==n.workerGeneration||control.sourceSession!=='watchdog@system'||control.destinationSession!==c.worker||
     digest(control.body)!==c.bodyHash||n.controlBodyHash!==c.bodyHash||n.controlReceiptHash!==digest(saved.receipt)||
     body.action!=='record-exact-native-terminal-return'||body.rigId!==rigId||body.originalQueueId!==c.originalQueueId||body.packageKey!==c.packageKey||body.recipientGeneration!==c.workerGeneration||body.grantsAuthority!==false)return null;
  return {rigId,kind,proofId,controlQueueId,worker:String(n.worker),workerGeneration:String(n.workerGeneration),noticeRow:row,controlReceipt:c};
 }
 /** Read-only reader for the R7 immutable custody-evidence contract
  * (queue-native-custody.v1, published in 349b4d70). A missing
  * table/row, malformed JSON or absent actorGeneration is unproven and stays held; no
  * generation is ever inferred from mutable claim fields. This service reads evidence; it never manufactures producer receipts. */
 private nativeCustodyEvidenceCache:{schemaVersion:number;exists:boolean}|undefined;
 private nativeCustodyEvidenceTransition(actual:QueueTransition):NativeQueueCustodyReceipt|null {
  const schemaVersion=Number(this.db.pragma('schema_version',{simple:true}));
  if(!this.nativeCustodyEvidenceCache||this.nativeCustodyEvidenceCache.schemaVersion!==schemaVersion)this.nativeCustodyEvidenceCache={schemaVersion,exists:!!this.db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='queue_native_custody_evidence'").get()};
  if(!this.nativeCustodyEvidenceCache.exists)return null;
  const row=this.db.prepare('SELECT receipt FROM queue_native_custody_evidence WHERE transition_id=? AND qitem_id=?').get(actual.transitionId,actual.qitemId) as {receipt:string}|undefined;
  let r:any;try{r=JSON.parse(row?.receipt??'null');}catch{return null;}
  if(!r||r.kind!=='queue-native-custody.v1'||typeof r.actorGeneration!=='string'||!r.actorGeneration||actual.identityProvenance!=='transport:v1')return null;
  const t=r.transition;
  if(!t||Object.keys(t).length!==Object.keys(actual).length||Object.entries(actual).some(([key,value])=>t[key]!==value)||
     !r.beforeQueue||!r.afterQueue||r.beforeQueue.qitem_id!==actual.qitemId||r.afterQueue.qitem_id!==actual.qitemId||r.afterQueue.state!==actual.state)return null;
  return r as NativeQueueCustodyReceipt;
 }
 private nativeCustodySnapshotEqual(a:any,b:any):boolean {
  return !!a&&!!b&&typeof a==='object'&&typeof b==='object'&&!Array.isArray(a)&&!Array.isArray(b)&&
    Object.keys(a).length===Object.keys(b).length&&Object.entries(a).every(([key,value])=>b[key]===value);
 }
 /** A genuine terminal state-change, followed only by exact same-generation native notes, proves
  * retirement. The archive-aware immutable receipts identify each actor; mutable claim fields are
  * consistency checks only. A later note cannot manufacture an earlier terminal actor. */
 private nativeReturnTerminalOutcome(binding:NonNullable<ReturnType<CoordinationRecoveryService['nativeReturnNoticeBinding']>>):number|null {
  const control=this.db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(binding.controlQueueId) as any;
  if(!control||!['done','failed','canceled'].includes(control.state)||control.claimed_by_generation_uuid!==binding.workerGeneration)return null;
  const history=this.repo.transitionLog.listForQitem(binding.controlQueueId);
  let expected=control,scanned=0;
  for(let i=history.length-1;i>=0&&scanned++<2000;i--){
   const t=history[i];if(!t)return null;const r=this.nativeCustodyEvidenceTransition(t);
   if(!r||r.actorGeneration!==binding.workerGeneration||t.actorSession!==binding.worker||t.state!==control.state||
      !this.nativeCustodySnapshotEqual(r.afterQueue,expected)||r.beforeQueue.source_session!=='watchdog@system'||r.beforeQueue.destination_session!==binding.worker||
      r.beforeQueue.body!==control.body||r.beforeQueue.claimed_by_generation_uuid!==binding.workerGeneration||r.beforeQueue.claimed_at!==control.claimed_at)return null;
   if(['in-progress','blocked'].includes(String(r.beforeQueue.state)))
    return Date.parse(t.ts)>=Date.parse(String(binding.noticeRow.ts_dispatched))?t.transitionId:null;
   if(r.beforeQueue.state!==control.state||!this.nativeCustodySnapshotEqual(r.beforeQueue,r.afterQueue))return null;
   expected=r.beforeQueue;
  }
  return null;
 }
 private nativeReturnNoticeOutcomeProof(binding:NonNullable<ReturnType<CoordinationRecoveryService['nativeReturnNoticeBinding']>>,expected?:{outcomeTransitionId?:number;dispositionOperationId?:string}):{outcomeTransitionId?:number;dispositionOperationId?:string}|null {
  if(this.authority.generation(binding.worker)!==binding.workerGeneration)return null;
  if(!expected||expected.outcomeTransitionId!==undefined){
   const terminal=this.nativeReturnTerminalOutcome(binding);
   if(terminal!==null&&(!expected||expected.outcomeTransitionId===terminal))return {outcomeTransitionId:terminal};
  }
  if(binding.kind==='continuation'&&(!expected||expected.dispositionOperationId!==undefined)){
   const a=this.db.prepare("SELECT a.disposition_id,a.destination,a.body_hash,q.body,q.claimed_by_generation_uuid,p.contract FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id JOIN coordinator_packages p ON p.rig_id=a.rig_id AND p.package_key=a.package_key WHERE a.rig_id=? AND a.package_key=? AND a.queue_id=?").get(binding.rigId,binding.controlReceipt.packageKey,binding.controlReceipt.originalQueueId) as any;
   if(a?.disposition_id&&(!expected||expected.dispositionOperationId===a.disposition_id)){
    const d=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='disposition'").get(binding.rigId,a.disposition_id) as {receipt:string}|undefined;
    let dr:any,contract:any;try{dr=JSON.parse(d?.receipt??'null');contract=JSON.parse(a.contract);}catch{return null;}
    if(dr&&dr.actor===binding.worker&&dr.generation===binding.workerGeneration&&dr.packageKey===binding.controlReceipt.packageKey&&
       a.destination===binding.worker&&a.claimed_by_generation_uuid===binding.workerGeneration&&digest(a.body)===a.body_hash&&contract?.destination===binding.worker&&contract.bodyHash===a.body_hash&&typeof contract.returnContract?.destination==='string'&&Array.isArray(contract.returnContract?.evidenceRequired)&&
       this.validContinuationReturn(a.disposition_id,binding.worker,binding.workerGeneration,binding.controlReceipt.packageKey,contract))return {dispositionOperationId:a.disposition_id};
   }
  }
  return null;
 }
 private nativeReturnNoticeOutcomeContained(rigId:string,row:any):boolean{
  const binding=this.nativeReturnNoticeBinding(rigId,row);if(!binding)return false;
  const saved=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='native-return-notice-outcome'").get(rigId,'native-return-notice-outcome:'+row.outbox_id) as {receipt:string}|undefined;
  if(!saved)return false;
  try{const p=JSON.parse(saved.receipt);if(p.outboxId!==row.outbox_id||('outcomeTransitionId' in p)===('dispositionOperationId' in p)||p.noticeSnapshotHash!==digest(JSON.stringify(row))||p.proofId!==binding.proofId||p.controlQueueId!==binding.controlQueueId||p.worker!==binding.worker||p.workerGeneration!==binding.workerGeneration||p.deliveryConclusion!=='unknown'||p.originalMutations!==0||p.outcomeOnly!==true||p.grantsAuthority!==false)return false;
   const proof=this.nativeReturnNoticeOutcomeProof(binding,p);if(!proof)return false;
   return 'outcomeTransitionId' in proof?p.outcomeTransitionId===proof.outcomeTransitionId:p.dispositionOperationId===proof.dispositionOperationId;}catch{return false;}
 }
 private nativeReturnNoticeCursor=new Map<string,number>();
 private recordNativeReturnNoticeOutcomes(rigId:string):void{
  let from=this.nativeReturnNoticeCursor.get(rigId)??0,scanned=0;
  while(scanned<2000){
   const rows=this.db.prepare("SELECT o.rowid scan_rowid,o.* FROM outbox_entries o WHERE o.rowid>? AND o.delivery_state='indeterminate' AND o.sender_session='watchdog@system' AND o.outbox_id LIKE 'wake-intent-native-return-%' AND NOT EXISTS(SELECT 1 FROM coordinator_operations c WHERE c.rig_id=? AND c.operation_id='native-return-notice-outcome:'||o.outbox_id AND c.kind='native-return-notice-outcome') ORDER BY o.rowid LIMIT 200").all(from,rigId) as any[];
   for(const scannedRow of rows){
    const {scan_rowid:_,...row}=scannedRow,binding=this.nativeReturnNoticeBinding(rigId,row);if(!binding)continue;
    const proof=this.nativeReturnNoticeOutcomeProof(binding);if(!proof)continue;
    const receipt={outboxId:row.outbox_id,noticeSnapshotHash:digest(JSON.stringify(row)),proofId:binding.proofId,controlQueueId:binding.controlQueueId,worker:binding.worker,workerGeneration:binding.workerGeneration,...proof,deliveryConclusion:'unknown',originalMutations:0,outcomeOnly:true,grantsAuthority:false};
    this.db.prepare('INSERT OR IGNORE INTO coordinator_operations VALUES (?,?,?,?,?)').run(rigId,'native-return-notice-outcome:'+row.outbox_id,'native-return-notice-outcome',JSON.stringify(receipt),digest(JSON.stringify(receipt)));
   }
   scanned+=rows.length;from=rows.length?Number((rows[rows.length-1] as any).scan_rowid):0;
   if(rows.length<200){from=0;break;}
  }
  this.nativeReturnNoticeCursor.set(rigId,from);
 }
 /** Root-approved C2 lineage-exact failure-only retirement exclusion: only the
  * indeterminate producer-bound instruction notices of THIS original + current worker
  * generation lineage qualify. Pending/sending rows and unrelated effects never do.
  * Successor authorization, non-retirement continuation and every dispatch/duty gate
  * still require full C1 containment. */
 private nativeReturnRetirementChainNotices(rigId:string,originalQueueId:string,worker:string,workerGeneration:string):string[]{
  const ids:string[]=[];
  for(const row of this.db.prepare("SELECT * FROM outbox_entries WHERE delivery_state='indeterminate' AND sender_session='watchdog@system' AND outbox_id LIKE 'wake-intent-native-return-%'").all() as any[]){
   const binding=this.nativeReturnNoticeBinding(rigId,row);if(!binding)continue;
   if(binding.controlReceipt.originalQueueId===originalQueueId&&binding.worker===worker&&binding.workerGeneration===workerGeneration&&this.authority.generation(worker)===workerGeneration)ids.push(String(row.outbox_id));
  }
  return ids;
 }
 private recordHeldHistoryNoticeOutcome(rigId:string,parent:any):void {
  const row=this.db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+parent.queueId) as any;if(!row||!this.heldHistoryNoticeOutcomeProof(rigId,row))return;
  const terminal=this.db.prepare('SELECT * FROM queue_transitions WHERE qitem_id=? ORDER BY transition_id DESC LIMIT 1').get(parent.queueId) as any;
  const receipt={outboxId:row.outbox_id,noticeSnapshotHash:digest(JSON.stringify(row)),queueId:parent.queueId,queueBodyHash:parent.bodyHash,recipient:parent.recipient,recipientGeneration:parent.recipientGeneration,terminalTransitionId:terminal.transition_id,terminalState:terminal.state,deliveryConclusion:'unknown',originalMutations:0,outcomeOnly:true,nonExecutable:true};
  this.db.prepare('INSERT OR IGNORE INTO coordinator_operations VALUES (?,?,?,?,?)').run(rigId,'held-control-outcome:'+row.outbox_id,'held-history-control-outcome',JSON.stringify(receipt),digest(JSON.stringify(receipt)));
 }
 /** A chain link's own UNKNOWN wake stops counting only through its own recorded
   * receipt, and only for the exact owning chain and its retirements. */
 private recordDutyChainNoticeOutcomes(rigId:string,r:any):void {
  for(const c of this.dutyChain(r))this.recordHeldHistoryNoticeOutcome(rigId,c);
  for(const row of this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.targetQueueId')=? AND json_extract(receipt,'$.kind') IN ('held-history-retirement','lifecycle-retirement')").all(rigId,r.queueId) as any[])this.recordHeldHistoryNoticeOutcome(rigId,JSON.parse(row.receipt));
 }
 /** A duty's own wake is not debt only when its delivery is resolved or provably
   * contained; an UNKNOWN notice stays unresolved and therefore blocking. */
 private dutyNoticeContained(rigId:string,r:any):boolean {
  const notice=this.db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+r.queueId) as any;
  if(!notice||!this.dutyNoticeMatches(r,notice))return true;
  if(['delivered','failed','retired'].includes(notice.delivery_state))return true;
  return this.noticeOutcomeContained(rigId,notice);
 }
 private heldHistoryRecord(r:any):any|null {
  const q=this.db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(r.recordQueueId) as any;
  const authored=this.repo.getById(r.queueId);let expected;try{expected=JSON.parse(authored?.body??'').recordBody;}catch{return null;}
  const creation=this.db.prepare('SELECT * FROM queue_transitions WHERE qitem_id=? ORDER BY transition_id LIMIT 1').get(r.recordQueueId) as any;
  if(!q||!expected||q.source_session!==r.holder||q.destination_session!=='operator-agent@kernel'||q.minting_generation_uuid!==r.holderGeneration||q.expires_at!==new Date(r.deadline).toISOString()||q.body!==JSON.stringify(expected)||creation?.actor_session!==r.holder||creation?.identity_provenance!=='transport:v1')return null;
  return q;
 }
 private heldHistoryPickupReady(rigId:string,r:any,excludeEffect?:string):boolean {
  const parent=this.heldHistoryAuthoringControl(r.authoringQueueId),q=parent?this.heldHistoryRecord(parent):null,author=parent?this.repo.getById(parent.queueId):null;
  if(!parent||!q||!author?.claimedAt||!['in-progress','blocked'].includes(author.state)||!['pending','in-progress'].includes(q.state)||digest(q.body)!==r.recordBodyHash||q.expires_at!==new Date(r.deadline).toISOString()||q.claimed_at&&(q.claimed_by_generation_uuid!==r.operatorGeneration)||!this.heldHistoryAuthoringReady(rigId,parent,'wake-intent-'+parent.queueId))return false;
  const claim=this.db.prepare("SELECT 1 FROM queue_transitions WHERE qitem_id=? AND transition_note='claimed' AND actor_session=? AND identity_provenance='transport:v1'").get(parent.queueId,parent.recipient);
  return !!claim&&this.repo.heldHistoryAuthoringDebtReady(rigId,r.recipient,excludeEffect);
 }
 private stageHeldHistoryPickup(rigId:string,parent:any):void {
  const record=this.heldHistoryRecord(parent);if(!record||parent.deadline<=this.now())return;
  const queueId='qitem-coordination-lifecycle-'+digest(parent.queueId+':operator-record-pickup').slice(0,24);if(this.repo.getById(queueId))return;
  const r={...parent,kind:'held-history-pickup',queueId,authoringQueueId:parent.queueId,recipient:'operator-agent@kernel',recipientGeneration:parent.operatorGeneration,recordBodyHash:digest(record.body)};
  if(!this.heldHistoryPickupReady(rigId,r))return;
  const body=JSON.stringify({action:'claim-and-bind-exact-held-history-record',rigId,queueId,claimCommand:'rig queue claim '+queueId,recordQueueId:r.recordQueueId,recordClaimCommand:'rig queue claim '+r.recordQueueId,authoringQueueId:parent.queueId,operatorIntakeId:r.operatorIntakeId,deadline:r.deadline,recipientGeneration:r.recipientGeneration,grantsAuthority:false,bindingContract:{rigId,operationId:r.bindingOperationId,effects:r.effects,recovery:{queueId:r.recordQueueId,rowHash:'<actual full queue row hash after genuine Operator claim>'}},required:'Read the actual current Lead-authored record, genuinely claim this duty and that exact record under your own current native identity. Derive its actual row hash and use rig coordinator held-history-recovery-bind <bindingContractFile>. Completion requires the exact supported binding receipt; prose is insufficient. Do not extend expiry, replay UNKNOWN notices, retire original effects, execute product work or infer acceptance.'});
  this.db.transaction(()=>{if(!this.heldHistoryPickupReady(rigId,r)||this.repo.getById(queueId))return;this.repo.createWithinTransaction({qitemId:queueId,sourceSession:'watchdog@system',destinationSession:r.recipient,expiresAt:new Date(r.deadline).toISOString(),body,identityProvenance:'system:operator-authorized-coordination',nudge:false});const noticeId=this.repo.stageHeldHistoryAuthoringWake(queueId,r.recipient,r.recipientGeneration),notice=this.db.prepare('SELECT body FROM outbox_entries WHERE outbox_id=?').get(noticeId) as any;this.db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run(rigId,queueId,'coordinator-lifecycle-control',JSON.stringify({...r,bodyHash:digest(body),noticeBodyHash:digest(notice.body)}),digest(body));})();
 }
 /** Historical recovery for a state-changing duty whose success committed but whose close was never frozen before its
  *  deadline (complete, expired, claimed, not closable). It does NOT rescue the late success, back-date anything or
  *  reapply the effect: it stages the existing accountable failure-only retirement so the original native claimant can
  *  report its own administrative duty item failed. Targeting is the live claimed lifecycle duty items only (an indexed
  *  range over duty queue ids), never the operations history. */
 private retirementCursor=new Map<string,string>();
 private retireCompleteUnclosableDuties(rigId:string):CoordinationResult[] {
  const kinds=['outcome-qualification-refresh','admission-refresh','acceptance','recovery','materialization','frontier-planning','frontier-admission','frontier-confirmation'],out:CoordinationResult[]=[];
  // Rig scope is part of the indexed query (the duty's own control row, by primary key), so another rig's items can never
  // fill the page; a per-rig cursor walks the live items fairly across passes and wraps when it reaches the end.
  const cursor=this.retirementCursor.get(rigId)??'',limit=100;
  const rows=this.db.prepare("SELECT q.qitem_id FROM queue_items q INDEXED BY idx_queue_items_state JOIN coordinator_operations o ON o.rig_id=? AND o.operation_id=q.qitem_id AND o.kind='coordinator-lifecycle-control' WHERE q.state IN ('in-progress','blocked') AND q.qitem_id>? AND q.qitem_id>='qitem-coordination-lifecycle-' AND q.qitem_id<'qitem-coordination-lifecycle.' AND q.source_session='watchdog@system' AND q.claimed_at IS NOT NULL AND q.expires_at IS NOT NULL AND q.expires_at<=? ORDER BY q.qitem_id LIMIT ?").all(rigId,cursor,new Date(this.now()).toISOString(),limit) as Array<{qitem_id:string}>;
  this.retirementCursor.set(rigId,rows.length<limit?'':rows[rows.length-1]!.qitem_id);
  for(const {qitem_id} of rows){
   const control=this.lifecycleControl(qitem_id);if(!control||control.rigId!==rigId||!kinds.includes(control.receipt.kind))continue;
   const f=this.dutyFacts(qitem_id);if(!(f.complete&&f.expired&&!f.close&&!f.retired&&!f.failedByRecipient))continue;
   const retirement=this.stageDutyRetirement(rigId,control.receipt,qitem_id);
   if(!retirement)out.push({key:'duty-retirement:'+control.receipt.kind+':'+control.receipt.packageKey,state:'held',reason:'lifecycle-recipient-protected',deadline:this.now()+1200000,subject:{packageKey:control.receipt.packageKey,owner:control.receipt.recipient,identity:qitem_id}});
  }
  return out;
 }
 private centralLifecyclePass(rigId:string):CoordinationResult[] {
  const a=this.authority.get(rigId)!,plan=this.plan(rigId)!,result:CoordinationResult[]=[...this.retireCompleteUnclosableDuties(rigId)];
  const returned=this.db.prepare("SELECT a.*,q.body,q.state,q.claimed_by_generation_uuid,p.contract FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id JOIN coordinator_packages p ON p.rig_id=a.rig_id AND p.package_key=a.package_key WHERE a.rig_id=? AND a.disposition_id IS NOT NULL AND q.state IN ('done','handed-off','failed','denied','canceled')").all(rigId) as any[];
  for(const row of returned){
   const kind=successfulReturn(row.state,row.disposition_id)?'acceptance':'recovery';
   if(kind==='acceptance'&&this.exactAccepted(rigId,row.queue_id,row.disposition_id))continue;
   const contract=JSON.parse(row.contract);if(row.body_hash!==digest(row.body)||contract.destination!==row.destination||contract.bodyHash!==row.body_hash||!this.validContinuationReturn(row.disposition_id,row.destination,row.claimed_by_generation_uuid,row.package_key,contract)){result.push({key:kind+':'+row.package_key,state:'held',queueId:row.queue_id,reason:'lifecycle-return-contract-drift',deadline:this.now()+1200000,subject:{packageKey:row.package_key,owner:a.owner_session,identity:digest(JSON.stringify({originalQueueId:row.queue_id,dispositionId:row.disposition_id,contractHash:digest(row.contract),assignmentBodyHash:row.body_hash}))}});continue;}
   const returnedBody=(this.repo.getById(row.disposition_id)!).body;
   result.push(this.lifecycleDuty(rigId,kind,row.package_key,a.owner_session,a.owner_generation,row.queue_id+':'+row.disposition_id,{originalQueueId:row.queue_id,dispositionId:row.disposition_id,returnBodyHash:digest(returnedBody),assignmentBodyHash:row.body_hash,contractHash:digest(row.contract),...(kind==='acceptance'?{acceptContract:{rigId,packageKey:row.package_key,dispositionId:row.disposition_id,evidenceRef:'<actual technical acceptance evidence>'}}:{terminalState:row.state,recoveryContract:{rigId,dutyQueueId:'<this native recovery-only duty queue ID>',recoveryPackageKey:'<distinct admitted configured recovery package>',recoveryQueueId:'<actual currently claimed recovery assignment queue ID>',evidenceRef:'<actual recovery pickup evidence>'}})}));
  }
  const unplanned=this.db.prepare('SELECT package_key,contract,contract_hash FROM coordinator_packages p WHERE p.rig_id=? AND NOT EXISTS (SELECT 1 FROM coordinator_assignments a WHERE a.rig_id=p.rig_id AND a.package_key=p.package_key)').all(rigId) as any[];
  const accepted=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='coordination-accept' ORDER BY operation_id").all(rigId) as Array<{receipt:string}>;
  const acceptedPredecessors=accepted.map(row=>{const r=JSON.parse(row.receipt);return {queueId:r.queueId,dispositionId:r.dispositionId,evidenceRef:r.evidenceRef};});
  for(const row of unplanned){if(plan.tasks.some(t=>t.packageKey===row.package_key))continue;
   result.push(this.lifecycleDuty(rigId,'materialization',row.package_key,'operator-agent@kernel',plan.operatorGeneration,row.package_key+':'+row.contract_hash,{contractHash:row.contract_hash,contract:JSON.parse(row.contract),acceptedPredecessors}));
  }
  result.push(...this.frontierPlanning().pass(rigId));
  return result;
 }
 private lifecycleControl(queueId:string):{rigId:string;receipt:any}|null {const row=this.db.prepare("SELECT rig_id,receipt FROM coordinator_operations WHERE operation_id=? AND kind='coordinator-lifecycle-control'").get(queueId) as {rig_id:string;receipt:string}|undefined;return row?{rigId:row.rig_id,receipt:JSON.parse(row.receipt)}:null;}
 lifecycleControlReceipt(queueId:string):any|null {return this.lifecycleControl(queueId)?.receipt??null;}
 isLifecycleControl(queueId:string):boolean {return this.lifecycleControl(queueId)!==null;}
 private dutyPostcondition(rigId:string,r:any):boolean {
  if(r.kind==='qualification-assessment-retirement'){
   const target=this.repo.getById(r.targetQueueId),targetTransition=this.db.prepare("SELECT transition_id,state,actor_session,identity_provenance FROM queue_transitions WHERE qitem_id=? ORDER BY transition_id DESC LIMIT 1").get(r.targetQueueId) as any;
   if(r.evidenceKind==='operator-accountability')return digest(target?.body??'')===r.targetBodyHash&&this.heldNativeTerminal(r.targetQueueId,r.recipient,r.recipientGeneration)&&targetTransition?.state==='failed'&&targetTransition.actor_session===r.recipient&&targetTransition.identity_provenance==='transport:v1'&&this.qualificationRetirementAccountabilityMatches(rigId,r)&&this.qualificationRetirementWakeFailed(r.targetQueueId);
   const sweep=this.repo.getById(r.sweepFindingQueueId),sweepTransition=this.db.prepare("SELECT transition_id,state,actor_session,identity_provenance FROM queue_transitions WHERE qitem_id=? ORDER BY transition_id DESC LIMIT 1").get(r.sweepFindingQueueId) as any,sweepClaim=this.db.prepare('SELECT claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(r.sweepFindingQueueId) as any;
   return digest(target?.body??'')===r.targetBodyHash&&digest(sweep?.body??'')===r.sweepFindingBodyHash&&this.heldNativeTerminal(r.targetQueueId,r.recipient,r.recipientGeneration)&&!!sweep&&sweep.state==='done'&&!!sweep.claimedAt&&sweepClaim?.claimed_by_generation_uuid===r.recipientGeneration&&sweepTransition?.state==='done'&&sweepTransition.actor_session===r.recipient&&sweepTransition.identity_provenance==='transport:v1'&&targetTransition?.state==='failed'&&targetTransition.actor_session===r.recipient&&targetTransition.identity_provenance==='transport:v1'&&targetTransition.transition_id<sweepTransition.transition_id;
  }
  if(dutyKinds[r.kind as DutyKind]?.effectClass==='report-only')return digest(this.repo.getById(r.targetQueueId)?.body??'')===r.targetBodyHash&&this.heldNativeTerminal(r.targetQueueId,r.recipient,r.recipientGeneration);
  if(r.kind==='held-history-authoring'||r.kind==='held-history-pickup'){
   const parent=r.kind==='held-history-pickup'?this.lifecycleControl(r.authoringQueueId)?.receipt:r;if(!parent)return false;
   const record=this.heldHistoryRecord(parent),op=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='held-history-recovery-binding'").get(rigId,parent.bindingOperationId) as any;
   if(!record||!op)return false;try{const b=JSON.parse(op.receipt);return b.kind==='coordinator-held-history-recovery-binding.v1'&&b.actor==='operator-agent@kernel'&&b.generation===parent.operatorGeneration&&b.originalMutations===0&&JSON.stringify(b.effects)===JSON.stringify(parent.effects.map((e:any)=>e.outboxId))&&b.binding?.queueId===parent.recordQueueId&&b.binding.bodyHash===digest(record.body)&&b.binding.lead===parent.holder&&b.binding.leadGeneration===parent.holderGeneration&&record.claimed_by_generation_uuid===parent.operatorGeneration;}catch{return false;}
  }
  if(r.kind==='outcome-qualification-refresh')return this.authority.runtimeOutcomeAssessment?.qualificationRefreshCompleted(rigId,r)===true;
  if(r.kind==='qualification-assessment')return !!this.db.prepare("SELECT 1 FROM coordinator_operations ret JOIN coordinator_operations rev ON rev.rig_id=ret.rig_id AND rev.operation_id=? AND rev.kind='qualification-assessment-review' AND json_extract(rev.receipt,'$.returnBodyHash')=json_extract(ret.receipt,'$.returnBodyHash') AND json_extract(rev.receipt,'$.contractDigest')=json_extract(ret.receipt,'$.contractDigest') WHERE ret.rig_id=? AND ret.operation_id=? AND ret.kind='qualification-assessment-return' AND json_extract(ret.receipt,'$.contractDigest')=? AND json_extract(ret.receipt,'$.workerGeneration')=? AND json_extract(ret.receipt,'$.configurationDigest')=?").get('qualification-assessment-review:'+r.queueId,rigId,'qualification-assessment-return:'+r.queueId,r.contractDigest,r.recipientGeneration,r.configurationDigest);
  if(r.kind==='admission-refresh')return this.admissionRefreshCompleted(rigId,r);
  if(r.kind==='materialization'){const plan=this.plan(rigId),pkg=this.db.prepare('SELECT contract_hash FROM coordinator_packages WHERE rig_id=? AND package_key=?').get(rigId,r.packageKey) as any;return !!plan&&pkg?.contract_hash===r.contractHash&&plan.tasks.some(t=>t.packageKey===r.packageKey&&this.admittedNow(t));}
  if(r.kind===PLANNING_DUTY_KIND)return this.frontierPlanning().planPostcondition(rigId,r);
  if(r.kind===ADMISSION_DUTY_KIND)return this.frontierPlanning().admissionPostcondition(rigId,r);
  if(r.kind===CONFIRMATION_DUTY_KIND)return this.frontierPlanning().confirmationPostcondition(rigId,r);
  if(r.kind==='acceptance'&&this.exactAccepted(rigId,r.originalQueueId,r.dispositionId))return true;
  const op=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='coordinator-lifecycle-recovery'").get(rigId,'lifecycle-recovery:'+r.queueId) as any;return !!op&&this.validOwnedLifecycleRecovery(rigId,r,JSON.parse(op.receipt));
 }
 private dutyNoticeMatches(r:any,notice:any):boolean {
  if(!notice||notice.outbox_id!=='wake-intent-'+r.queueId||notice.sender_session!=='watchdog@system'||notice.destination_session!==r.recipient||notice.audit_pointer!==r.queueId||this.db.prepare('SELECT 1 FROM outbox_historical_quarantines WHERE outbox_id=?').get(notice.outbox_id))return false;
  if(r.noticeBodyHash)return digest(notice.body)===r.noticeBodyHash;
  // Compatibility for pre-facet central controls whose issued receipt predates
  // noticeBodyHash: only the exact original fixed envelope is recognized.
  if(!['acceptance','recovery','materialization'].includes(r.kind))return false;
  const parts=String(notice.body).split('\n---\n');return parts.length===3&&parts[0]?.startsWith('From: watchdog@system\nTo: '+r.recipient+'\nSent: ')===true&&parts[1]==='Read and genuinely claim exact lifecycle duty '+r.queueId+'; retain all existing acceptance, qualification and scope gates.'&&parts[2]==='↩ Reply: rig send watchdog@system "..."';
 }
 private dutyChain(r:any):any[] {
  const seen=new Set<string>(),todo=[r.queueId,...(r.chainIds??[]),r.previousQueueId,r.authoringQueueId,r.targetQueueId,r.recordQueueId,r.accountabilityControlQueueId].filter(Boolean),controls:any[]=[];
  while(todo.length){const id=todo.pop()!;if(seen.has(id))continue;seen.add(id);const op=this.lifecycleControl(id);if(!op)continue;controls.push(op.receipt);todo.push(...[...(op.receipt.chainIds??[]),op.receipt.previousQueueId,op.receipt.authoringQueueId,op.receipt.targetQueueId,op.receipt.recordQueueId].filter(Boolean));}
  return controls;
 }
 private bootstrapContainedAncestors(rigId:string,r:any,kind:'qualification-assessment'|'qualification-assessment-retirement'):string[] {
  if(!r.previousQueueId)return [];
  const ids:string[]=[],seen=new Set<string>(),todo=[r.previousQueueId,...(r.chainIds??[])].filter((id:string)=>!!id&&id!==r.queueId);
  while(todo.length){const id=todo.shift()!;if(seen.has(id))continue;if(ids.length>=128)return [];seen.add(id);
   const predecessor=this.lifecycleControl(id),q=this.repo.getById(id);if(!predecessor||predecessor.rigId!==rigId||predecessor.receipt.kind!==kind||!q)return [];
   const p=predecessor.receipt;if(p.rootId!==r.rootId||p.semanticKey!==r.semanticKey||p.recipient!==r.recipient||p.recipientGeneration!==r.recipientGeneration||p.configurationDigest!==r.configurationDigest)return [];
   if(kind==='qualification-assessment'&&p.contractDigest!==r.contractDigest)return [];
   if(kind==='qualification-assessment-retirement'&&(p.targetQueueId!==r.targetQueueId||p.targetBodyHash!==r.targetBodyHash||(r.evidenceKind?(p.evidenceKind!==r.evidenceKind||p.evidenceQueueId!==r.evidenceQueueId||p.evidenceBodyHash!==r.evidenceBodyHash):(p.sweepFindingQueueId!==r.sweepFindingQueueId||p.sweepFindingBodyHash!==r.sweepFindingBodyHash))))return [];
   if(!this.dutyFacts(id).retired||!this.dutyNoticeContained(rigId,p))return [];
   ids.push(id);todo.push(...[p.previousQueueId,...(p.chainIds??[])].filter((ancestor:string)=>!!ancestor&&ancestor!==id&&ancestor!==r.queueId));
  }
  return ids;
 }
 /** A completed report-only retirement can retain an expired, unclaimed queue row.
  * Treat that frozen history like its exact contained assessment predecessor at BOTH
  * issuance and use. Never release claimed custody, unrelated work or an UNKNOWN wake. */
 private containedQualificationCustody(rigId:string,worker:string,generation:string,configurationDigest:string,assessmentIds:string[]):Set<string> {
  const allowed=new Set(assessmentIds);if(allowed.size===0)return allowed;
  const rows=this.db.prepare("SELECT qitem_id FROM queue_items WHERE destination_session=? AND state='pending' AND claimed_at IS NULL AND expires_at<=?").all(worker,new Date(this.now()).toISOString()) as Array<{qitem_id:string}>;
  for(const {qitem_id} of rows)if(this.containedQualificationRetirement(qitem_id,rigId,worker,generation,configurationDigest,assessmentIds))allowed.add(qitem_id);
  return allowed;
 }
 /** Shared immutable proof for staging custody and generic watch classification.
  * Never terminalizes the historical queue row or concludes delivery. */
 private containedQualificationRetirement(queueId:string,rigId:string,worker:string,generation:string,configurationDigest:string,assessmentIds?:string[]):boolean {
  const row=this.db.prepare("SELECT state,claimed_at,expires_at FROM queue_items WHERE qitem_id=? AND destination_session=?").get(queueId,worker) as {state:string;claimed_at:string|null;expires_at:string|null}|undefined;
  if(!row||row.state!=='pending'||row.claimed_at!==null||!row.expires_at||row.expires_at>new Date(this.now()).toISOString())return false;
  const control=this.lifecycleControl(queueId),r=control?.receipt;
  if(!control||control.rigId!==rigId||r.kind!=='qualification-assessment-retirement'||r.recipient!==worker||r.recipientGeneration!==generation||r.configurationDigest!==configurationDigest||assessmentIds&&!assessmentIds.includes(r.targetQueueId))return false;
  const target=this.lifecycleControl(r.targetQueueId),q=this.repo.getById(r.targetQueueId);
  if(!target||target.rigId!==rigId||target.receipt.kind!=='qualification-assessment'||target.receipt.recipient!==worker||target.receipt.recipientGeneration!==generation||target.receipt.configurationDigest!==configurationDigest||!q||digest(q.body)!==r.targetBodyHash)return false;
  const facts=this.dutyFacts(queueId);
  return facts.expired&&facts.complete&&facts.retired&&this.dutyNoticeContained(rigId,r);
 }
 /** Pure, opt-out only for positively contained report-only history. Call at watch
  * execution boundaries, never queue projection (dutyFacts reads queue faces). */
 genericWatchActionable(queueId:string,expectedRigId?:string):boolean {
  try {
   if (expectedRigId && isContainedExpiredAdministrativeHistory(this.db, expectedRigId, queueId, this.now())) return false;
   const control=this.lifecycleControl(queueId),r=control?.receipt;
   if(!control||expectedRigId&&control.rigId!==expectedRigId||r.kind!=='qualification-assessment-retirement'||!this.db.prepare('SELECT 1 FROM sessions s JOIN nodes n ON n.id=s.node_id WHERE s.session_name=? AND n.rig_id=?').get(r.recipient,control.rigId)||!r.recipientGeneration||!r.configurationDigest||this.authority.generation(r.recipient)!==r.recipientGeneration||this.configurationDigest(r.recipient)!==r.configurationDigest)return true;
   return !this.containedQualificationRetirement(queueId,control.rigId,r.recipient,r.recipientGeneration,r.configurationDigest);
  }catch(error){if(error instanceof SyntaxError)return true;throw error;}
 }
 private dutyExcludedNotices(r:any):string[] {
  const ids=['wake-intent-'+r.queueId];if(dutyKinds[r.kind as DutyKind]?.effectClass!=='report-only')return ids;
  for(const c of this.dutyChain(r)){const n=this.db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+c.queueId) as any;if(this.dutyNoticeMatches(c,n)&&['pending','sending','delivered','indeterminate','failed'].includes(n.delivery_state))ids.push(n.outbox_id);}return [...new Set(ids)];
 }
 private dutyBinding(rigId:string,r:any):boolean {
  const k=dutyKinds[r.kind as DutyKind],a=this.authority.get(rigId),plan=this.plan(rigId);if(!k||this.authority.generation(r.recipient)!==r.recipientGeneration)return false;
  if(k.binding==='recipientOnly')return true;
  if(k.binding==='currentOperator')return r.recipient==='operator-agent@kernel'&&r.operatorGeneration===r.recipientGeneration&&this.authority.generation('operator-agent@kernel')===r.operatorGeneration;
  if(k.binding==='qualificationWorker'){
   const planMatches=!!plan&&plan.operatorGeneration===r.operatorGeneration&&plan.revision===r.planRevision;
   const bootstrapMatches=!plan&&((r.kind==='qualification-assessment'&&r.bootstrap===true&&r.planRevision===null)||(r.kind==='qualification-assessment-retirement'&&r.bootstrapType==='qualification-assessment-retirement'&&r.planRevision===null));
   return ['qualification-assessment','qualification-assessment-retirement'].includes(r.kind)&&r.recipient===r.worker&&r.recipientGeneration===r.workerGeneration&&r.operatorGeneration===this.authority.generation('operator-agent@kernel')&&r.issuer==='operator-agent@kernel'&&r.operatorGeneration===this.authority.generation(r.issuer)&&this.authority.generation(r.recipient)===r.recipientGeneration&&this.configurationDigest(r.recipient)===r.configurationDigest&&!!a&&a.state==='active'&&a.lease_until>this.now()&&a.epoch===r.epoch&&a.owner_session===r.holder&&a.owner_generation===r.holderGeneration&&this.authority.generation(r.holder)===r.holderGeneration&&(planMatches||bootstrapMatches);
  }
  return !!a&&a.state==='active'&&a.lease_until>this.now()&&a.epoch===r.epoch&&a.owner_session===r.holder&&a.owner_generation===r.holderGeneration&&r.recipient===r.holder&&this.authority.generation(r.holder)===r.holderGeneration&&!!plan&&plan.operatorGeneration===r.operatorGeneration&&this.authority.generation('operator-agent@kernel')===r.operatorGeneration;
 }
private dutyProtection(rigId:string,r:any):boolean {
   const plan=this.plan(rigId),administrative=this.administrativeDuty(r.kind),excluded=[...this.dutyExcludedNotices(r),...(administrative?this.administrativeIntakeNotices(rigId,r.recipient):[])];
  if(!plan&&(r.bootstrap===true||r.bootstrapType==='qualification-assessment-retirement')){
   const a=this.authority.get(rigId),now=this.now();if(!a||a.state!=='active'||a.lease_until<=now||a.epoch!==r.epoch||a.owner_session!==r.holder||a.owner_generation!==r.holderGeneration||this.authority.generation(r.holder)!==r.holderGeneration||this.authority.generation('operator-agent@kernel')!==r.operatorGeneration||r.deadline<=now||this.configurationDigest(r.worker)!==r.configurationDigest)return false;
   const allowed=new Set([r.queueId]);
   if(r.bootstrap===true)for(const id of this.containedQualificationCustody(rigId,r.worker,r.workerGeneration,r.configurationDigest,this.bootstrapContainedAncestors(rigId,r,'qualification-assessment')))allowed.add(id);
   if(r.bootstrapType==='qualification-assessment-retirement')for(const id of this.bootstrapContainedAncestors(rigId,r,'qualification-assessment-retirement'))allowed.add(id);
   const custody=this.db.prepare("SELECT qitem_id,source_session,destination_session,state,claimed_at,expires_at,body FROM queue_items WHERE destination_session=? AND state IN ('pending','in-progress','blocked')").all(r.worker) as Array<any>;
   if(r.bootstrapType==='qualification-assessment-retirement'){
    const stale=custody.filter(row=>(row.source_session==='operator-agent@kernel'||(r.evidenceKind==='operator-accountability'&&row.qitem_id===r.targetQueueId&&row.source_session==='watchdog@system'))&&row.destination_session===r.worker&&row.state==='pending'&&!row.claimed_at&&!!row.expires_at&&Date.parse(row.expires_at)<=now&&row.body.toLowerCase().includes('qualification')&&row.body.toLowerCase().includes('assessment')&&!this.db.prepare('SELECT 1 FROM coordinator_assignments WHERE queue_id=?').get(row.qitem_id)&&!this.db.prepare('SELECT 1 FROM coordinator_packages WHERE rig_id=? AND package_key=?').get(rigId,row.qitem_id));
    const staleIds=new Set(stale.map(row=>row.qitem_id));if(staleIds.size===0)return false;
    if(r.evidenceKind==='operator-accountability'){
     if(stale.length!==1||stale[0]?.qitem_id!==r.targetQueueId||!this.qualificationRetirementAccountabilityMatches(rigId,r)||!this.qualificationRetirementWakeFailed(r.targetQueueId))return false;
     allowed.add(r.targetQueueId);
    }else{
    for(const target of stale){const wakeId='wake-intent-'+target.qitem_id,linked=this.db.prepare('SELECT outbox_id,delivery_state FROM outbox_entries WHERE audit_pointer=? OR outbox_id=?').all(target.qitem_id,wakeId) as Array<{outbox_id:string;delivery_state:string}>;if(!linked.some(effect=>effect.outbox_id===wakeId&&effect.delivery_state==='failed')||linked.some(effect=>effect.delivery_state!=='failed'))return false;}
    const findings=custody.filter(row=>row.source_session==='operator-agent@kernel'&&row.destination_session===r.worker&&row.body.includes('STUCK SWEEP FINDING (undelivered-wake)')&&row.body.includes('wake failed')&&row.body.includes('nothing retried it')&&[...staleIds].some(id=>row.body.includes('row: '+id))&&((row.state==='pending'&&!row.claimed_at)||(row.qitem_id===r.sweepFindingQueueId&&row.state==='in-progress'&&row.claimed_at&&(this.db.prepare('SELECT claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(row.qitem_id) as any)?.claimed_by_generation_uuid===r.workerGeneration)));
    if(findings.length!==stale.length||stale.some(target=>!findings.some(row=>row.body.includes('row: '+target.qitem_id))))return false;
    for(const row of [...stale,...findings])allowed.add(row.qitem_id);
    const requestedTarget=this.repo.getById(r.targetQueueId),requestedSweep=this.repo.getById(r.sweepFindingQueueId),requestedClaim=this.db.prepare('SELECT claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(r.sweepFindingQueueId) as any;
    const requestedTargetFailed=!!requestedTarget&&requestedTarget.state==='failed'&&!requestedTarget.claimedAt&&digest(requestedTarget.body)===r.targetBodyHash&&this.heldNativeTerminal(r.targetQueueId,r.worker,r.workerGeneration);
    const requestedSweepOwned=!!requestedSweep&&requestedSweep.state==='in-progress'&&!!requestedSweep.claimedAt&&requestedClaim?.claimed_by_generation_uuid===r.workerGeneration&&digest(requestedSweep.body)===r.sweepFindingBodyHash;
    const requestedSweepDone=!!requestedSweep&&requestedSweep.state==='done'&&!!requestedSweep.claimedAt&&requestedClaim?.claimed_by_generation_uuid===r.workerGeneration&&digest(requestedSweep.body)===r.sweepFindingBodyHash;
    if(requestedTargetFailed&&(requestedSweepOwned||requestedSweepDone))allowed.add(r.sweepFindingQueueId);
    }
   }
   if(custody.some(row=>!allowed.has(row.qitem_id)))return false;
   const reservations=this.db.prepare("SELECT 1 FROM seat_dispatch_reservations WHERE state!='released' AND (session_name=? OR node_id IN (SELECT node_id FROM sessions WHERE session_name=?))").get(r.worker,r.worker);
   const guards=this.db.prepare('SELECT 1 FROM seat_delivery_guards WHERE (desired=1 OR effective=1) AND node_id IN (SELECT node_id FROM sessions WHERE session_name=?)').get(r.worker);
   const excludedEffects=[...excluded,...(r.bootstrapType==='qualification-assessment-retirement'?custody.filter(row=>allowed.has(row.qitem_id)&&row.source_session==='operator-agent@kernel'&&row.body.toLowerCase().includes('qualification')).map(row=>'wake-intent-'+row.qitem_id):[])];
   return !reservations&&!guards&&!this.workerEffectDebt(r.worker,excludedEffects);
  }
  if(!plan)return false;
  const sessions=r.kind.startsWith('held-history-')?[r.recipient,r.holder,'operator-agent@kernel']:[r.recipient];
  for(const session of new Set(sessions)){const restriction=plan.dispatchRestrictions?.find(t=>t.session===session);if(restriction&&(restriction.generation!==this.authority.generation(session)||restriction.validUntil<=this.now()||!restriction.packageKeys.includes(r.packageKey)))return false;if(this.db.prepare('SELECT 1 FROM seat_delivery_guards WHERE (desired=1 OR effective=1) AND node_id IN (SELECT node_id FROM sessions WHERE session_name=?)').get(session)||this.db.prepare("SELECT 1 FROM seat_dispatch_reservations WHERE state!='released' AND (session_name=? OR node_id IN (SELECT node_id FROM sessions WHERE session_name=?))").get(session,session))return false;}
  return administrative?this.qualificationProbeDebtReady(r.recipient):r.kind.startsWith('held-history-')?this.repo.heldHistoryAuthoringDebtReady(rigId,r.recipient,excluded):!this.workerEffectDebt(r.recipient,excluded);
 }
 private dutySubjectReady(rigId:string,r:any):boolean {
  if(r.kind==='qualification-assessment'){
   const q=this.repo.getById(r.queueId),c=q?(()=>{try{return JSON.parse(q.body);}catch{return null;}})():null;
   const plan=this.plan(rigId),planMatches=r.bootstrap===true?!plan&&r.planRevision===null:!!plan&&plan.operatorGeneration===r.operatorGeneration&&plan.revision===r.planRevision;
   return !!q&&!!c&&q.destinationSession===r.recipient&&digest(q.body)===r.bodyHash&&c.schema==='qualification-assessment-duty.v1'&&c.scope==='qualification-only'&&c.grantsAuthority===false&&c.contractDigest===r.contractDigest&&c.workerGeneration===r.recipientGeneration&&c.configurationDigest===r.configurationDigest&&c.planRevision===r.planRevision&&c.bootstrap===r.bootstrap&&planMatches&&r.deadline>this.now()&&this.authority.generation(r.recipient)===r.recipientGeneration&&this.configurationDigest(r.recipient)===r.configurationDigest&&!this.db.prepare('SELECT 1 FROM coordinator_packages WHERE rig_id=? AND package_key=?').get(rigId,r.packageKey)&&!this.workerEffectDebt(r.recipient,'wake-intent-'+r.queueId);
  }
  if(dutyKinds[r.kind as DutyKind]?.effectClass==='report-only'){
   const q=this.repo.getById(r.targetQueueId);if(!q||q.destinationSession!==r.recipient||digest(q.body)!==r.targetBodyHash||!q.expiresAt||Date.parse(q.expiresAt)>this.now())return false;
   if(r.kind!=='qualification-assessment-retirement')return ['pending','in-progress','blocked'].includes(q.state);
   if(r.evidenceKind==='operator-accountability')return ((q.state==='pending'&&!q.claimedAt)||(q.state==='failed'&&!q.claimedAt&&this.heldNativeTerminal(r.targetQueueId,r.recipient,r.recipientGeneration)))&&this.qualificationRetirementAccountabilityMatches(rigId,r)&&this.qualificationRetirementWakeFailed(r.targetQueueId);
   const sweep=this.repo.getById(r.sweepFindingQueueId),sweepClaim=this.db.prepare('SELECT claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(r.sweepFindingQueueId) as any,wakeId='wake-intent-'+r.targetQueueId,wake=this.db.prepare('SELECT delivery_state FROM outbox_entries WHERE outbox_id=?').get(wakeId) as {delivery_state:string}|undefined;
   const pendingTarget=q.state==='pending'&&!q.claimedAt,failedTarget=q.state==='failed'&&!q.claimedAt&&this.heldNativeTerminal(r.targetQueueId,r.recipient,r.recipientGeneration);
   const pendingSweep=!!sweep&&sweep.state==='pending'&&!sweep.claimedAt,ownedSweep=!!sweep&&sweep.state==='in-progress'&&!!sweep.claimedAt&&sweepClaim?.claimed_by_generation_uuid===r.recipientGeneration;
   return (pendingTarget||failedTarget)&&!!sweep&&sweep.destinationSession===r.recipient&&sweep.sourceSession==='operator-agent@kernel'&&(pendingSweep||ownedSweep)&&digest(sweep.body)===r.sweepFindingBodyHash&&sweep.body.includes('STUCK SWEEP FINDING (undelivered-wake)')&&sweep.body.includes('row: '+r.targetQueueId)&&sweep.body.includes('wake failed')&&sweep.body.includes('nothing retried it')&&wake?.delivery_state==='failed';
  }
  if(r.kind==='held-history-authoring')return this.heldHistoryAuthoringReady(rigId,{...r,planRevision:this.plan(rigId)?.revision},this.dutyExcludedNotices(r));
  if(r.kind==='held-history-pickup'){const p=this.lifecycleControl(r.authoringQueueId)?.receipt;return !!p&&this.heldHistoryRecord(p)!==null&&this.heldHistoryAuthoringReady(rigId,{...p,planRevision:this.plan(rigId)?.revision},['wake-intent-'+p.queueId,...this.dutyExcludedNotices(r)]);}
  if(r.kind==='outcome-qualification-refresh')return this.authority.runtimeOutcomeAssessment?.qualificationBoundaryMatches(rigId,r)===true;
  if(r.kind==='admission-refresh')return this.admissionRefreshSubjectReady(rigId,r);
  if(r.kind==='materialization'){const plan=this.plan(rigId),pkg=this.db.prepare('SELECT contract_hash FROM coordinator_packages WHERE rig_id=? AND package_key=?').get(rigId,r.packageKey) as any;return pkg?.contract_hash===r.contractHash&&!plan?.tasks.some(t=>t.packageKey===r.packageKey)&&r.acceptedPredecessors.every((v:any)=>this.exactAccepted(rigId,v.queueId,v.dispositionId));}
  if(r.kind===PLANNING_DUTY_KIND)return this.frontierPlanning().planActAllowed(rigId,r);
  if(r.kind===ADMISSION_DUTY_KIND)return this.frontierPlanning().admissionActAllowed(rigId,r);
  if(r.kind===CONFIRMATION_DUTY_KIND)return this.frontierPlanning().confirmationActAllowed(rigId,r);
  const original=this.db.prepare('SELECT a.*,q.body,q.state,q.claimed_by_generation_uuid,p.contract FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id JOIN coordinator_packages p ON p.rig_id=a.rig_id AND p.package_key=a.package_key WHERE a.rig_id=? AND a.queue_id=? AND a.package_key=?').get(rigId,r.originalQueueId,r.packageKey) as any,returned=this.repo.getById(r.dispositionId);
  return !!original&&(r.kind==='recovery'?['failed','denied','canceled'].includes(original.state)&&original.state===r.terminalState:successfulReturn(original.state,original.disposition_id))&&original.disposition_id===r.dispositionId&&original.body_hash===r.assignmentBodyHash&&digest(original.body)===r.assignmentBodyHash&&digest(original.contract)===r.contractHash&&!!returned&&digest(returned.body)===r.returnBodyHash&&this.validContinuationReturn(r.dispositionId,original.destination,original.claimed_by_generation_uuid,r.packageKey,JSON.parse(original.contract));
 }
 /** Pure facets never infer transport delivery or perform a duty's mutation. */
 dutyFacts(queueId:string):DutyFacts {
  const op=this.lifecycleControl(queueId),none={queueId,claim:false,send:false,act:false,complete:false,close:false,retired:false,failedByRecipient:false,superseded:false,expired:true};if(!op||!dutyKinds[op.receipt.kind as DutyKind])return none;
  const r=op.receipt,q=this.repo.getById(queueId),notice=this.db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+queueId) as any,creation=this.db.prepare('SELECT * FROM queue_transitions WHERE qitem_id=? ORDER BY transition_id LIMIT 1').get(queueId) as any;
  const frozen=!!q&&q.sourceSession==='watchdog@system'&&q.destinationSession===r.recipient&&digest(q.body)===r.bodyHash&&q.expiresAt===new Date(r.deadline).toISOString()&&creation?.actor_session==='watchdog@system'&&creation?.identity_provenance==='system:operator-authorized-coordination'&&this.dutyNoticeMatches(r,notice);
  const complete=frozen&&this.dutyPostcondition(op.rigId,r),expired=r.deadline<=this.now(),binding=this.dutyBinding(op.rigId,r),protection=frozen&&binding&&this.dutyProtection(op.rigId,r);
  const observation=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='duty-completion-observation'").get(op.rigId,'duty-completion:'+queueId) as any;
  let completedAt:number|undefined;try{const o=observation?JSON.parse(observation.receipt):null;if(o?.bodyHash===r.bodyHash&&o.at<=r.deadline)completedAt=o.at;}catch{}
  const expiryObservation=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='duty-expiry-observation'").get(op.rigId,'duty-expiry-observation:'+queueId) as any;
  let expiredUnclaimedObserved=false;try{const o=expiryObservation?JSON.parse(expiryObservation.receipt):null;expiredUnclaimedObserved=!!o&&o.queueId===queueId&&o.bodyHash===r.bodyHash&&o.at>=r.deadline&&o.claimedAt===null&&o.grantsAuthority===false&&!q?.claimedAt;}catch{}
  const close=complete&&(!expired||completedAt!==undefined),failedByRecipient=frozen&&this.heldNativeTerminal(queueId,r.recipient,r.recipientGeneration),superseded=frozen&&dutyKinds[r.kind as DutyKind].binding!=='recipientOnly'&&!binding;
  const live=!!q&&['pending','in-progress','blocked'].includes(q.state),claim=frozen&&!expired&&protection&&live;
  const claimedBy=this.db.prepare('SELECT claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(queueId) as {claimed_by_generation_uuid:string|null}|undefined;
  const nativeClaim=!!q?.claimedAt&&['in-progress','blocked'].includes(q.state)&&claimedBy?.claimed_by_generation_uuid===r.recipientGeneration&&!!this.db.prepare("SELECT 1 FROM queue_transitions WHERE qitem_id=? AND state='in-progress' AND transition_note='claimed' AND actor_session=? AND identity_provenance='transport:v1'").get(queueId,r.recipient);
  const plan=this.plan(op.rigId),bootstrapPlanMatches=!plan&&(r.bootstrap===true||r.bootstrapType==='qualification-assessment-retirement')&&r.planRevision===null,planMatches=plan?plan.revision===r.planRevision:bootstrapPlanMatches;
  const subjectReady=this.dutySubjectReady(op.rigId,r),act=claim&&nativeClaim&&!complete&&subjectReady,send=claim&&!complete&&subjectReady&&planMatches&&q?.state==='pending'&&!q.claimedAt&&['pending','sending'].includes(notice?.delivery_state);
  const retired=frozen&&(failedByRecipient||(complete&&(!q?.claimedAt||close))||expiredUnclaimedObserved);
  return {queueId,claim,send,act,complete,close,retired,failedByRecipient,superseded,expired,...(!frozen?{reason:'duty-immutable-proof-drift'}:!binding?{reason:'duty-binding-superseded'}:!protection?{reason:'duty-recipient-protected'}:{})};
 }
 /** Freeze that a duty is complete, only while it is complete AND not yet expired, stamped now. Never back-dated. */
 private recordCompletionObservation(op:{rigId:string;receipt:any},queueId:string,f:DutyFacts):void {
  if(f.complete&&!f.expired){const r={queueId,bodyHash:op.receipt.bodyHash,at:this.now(),outcomeOnly:true};this.db.prepare('INSERT OR IGNORE INTO coordinator_operations VALUES (?,?,?,?,?)').run(op.rigId,'duty-completion:'+queueId,'duty-completion-observation',JSON.stringify(r),digest(JSON.stringify(r)));}
 }
 private observeDuty(queueId:string):void {
  const op=this.lifecycleControl(queueId);if(!op)return;const f=this.dutyFacts(queueId),q=this.repo.getById(queueId),notice=this.db.prepare('SELECT delivery_state FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+queueId) as any;
  this.recordCompletionObservation(op,queueId,f);
  if(f.expired&&!q?.claimedAt){const r={queueId,bodyHash:op.receipt.bodyHash,subjectComplete:f.complete,noticeState:notice?.delivery_state??null,claimedAt:null,at:this.now(),grantsAuthority:false};this.db.prepare('INSERT OR IGNORE INTO coordinator_operations VALUES (?,?,?,?,?)').run(op.rigId,'duty-expiry-observation:'+queueId,'duty-expiry-observation',JSON.stringify(r),digest(JSON.stringify(r)));}
 }
 /** Apply-time completion capture. Call ONLY inside the authorized transaction that has just written the fact
  *  completing this duty, after that write and before commit; the caller keeps all authority. It records the
  *  completion branch only (never the expiry branch), exactly one duty, and only while the duty is not expired. */
 observeLifecycleCompletion(queueId:string):void {
  const op=this.lifecycleControl(queueId);if(!op)return;
  this.recordCompletionObservation(op,queueId,this.dutyFacts(queueId));
 }
 private dutyRootId(rigId:string,kind:string,semanticKey:string):string {return 'qitem-coordination-lifecycle-'+digest(rigId+':'+kind+':'+semanticKey).slice(0,24);}
 private dutySuccessorId(rootId:string,priorQueueId:string,bindingVersion:string):string {return 'qitem-coordination-lifecycle-'+digest(rootId+':successor:'+priorQueueId+':'+bindingVersion).slice(0,24);}
 private dutyBindingVersion(kind:string,recipientGeneration:string,epoch:number):string {return kind==='materialization'||kind==='outcome-qualification-refresh'||kind==='admission-refresh'?recipientGeneration:recipientGeneration+':'+epoch;}
 /** The live link of a duty, by indexed exact subject: every link of one subject carries the same deterministic rootId in
  *  its control receipt (idx_coordinator_lifecycle_root), the most recently issued link is the live one, and it is
  *  found whatever generation or epoch each link was bound to. The selected control is validated against the exact
  *  immutable subject (kind, semanticKey, rootId, own queue id) before it is used. */
 private liveDutyLink(rigId:string,kind:string,semanticKey:string):string|null {
  const root=this.dutyRootId(rigId,kind,semanticKey);
  const row=this.db.prepare("SELECT operation_id,receipt FROM coordinator_operations WHERE rig_id=? AND kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.rootId')=? ORDER BY rowid DESC LIMIT 1").get(rigId,root) as {operation_id:string;receipt:string}|undefined;
  if(!row)return null;
  let r:any;try{r=JSON.parse(row.receipt);}catch{return null;}
  return r&&r.kind===kind&&r.semanticKey===semanticKey&&r.rootId===root&&r.queueId===row.operation_id?row.operation_id:null;
 }
 private captureDutyCompletion(rigId:string,kind:string,semanticKey:string):void {
  const id=this.liveDutyLink(rigId,kind,semanticKey);if(!id)return;
  const op=this.lifecycleControl(id);if(!op||op.rigId!==rigId)return;
  const f=this.dutyFacts(id);
  if(f.superseded)return;                  // a link bound to a retired holder or operator is not the live duty
  this.recordCompletionObservation(op,id,f);
 }
 /** Admission-refresh duties are selected by their RECORDED subject (package, task key, owner, task intent and the stale
  *  admission's expiry), newest first, never by re-deriving the issuance-time live generation or configuration, which may
  *  have moved since the duty was issued. Which duty a refresh actually completes is decided by the unchanged postcondition
  *  (it must match the duty's recorded generation and configuration AND the live ones), so no authority is relaxed:
  *  a duty issued under a configuration that has since moved simply stays incomplete and is not captured. */
 private captureAdmissionRefreshCompletion(rigId:string,prior:CoordinationTask):void {
  const intent=this.admissionTaskIntentDigest(prior);
  const rows=this.db.prepare("SELECT operation_id,receipt FROM coordinator_operations WHERE rig_id=? AND kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='admission-refresh' AND json_extract(receipt,'$.packageKey')=? AND json_extract(receipt,'$.taskKey')=? AND json_extract(receipt,'$.owner')=? AND json_extract(receipt,'$.taskIntentDigest')=? AND json_extract(receipt,'$.priorAdmission.validUntil')=? ORDER BY rowid DESC LIMIT 16").all(rigId,prior.packageKey,prior.key,prior.owner,intent,prior.admission.validUntil) as Array<{operation_id:string;receipt:string}>;
  // The exact immutable subject is part of the query, so the page can only contain duties for this subject: other
  // historical subjects of the package can never hide the completing duty. The checks below are defense in depth.
  for(const row of rows){
   let c:any;try{c=JSON.parse(row.receipt);}catch{continue;}
   if(!c||c.queueId!==row.operation_id||c.taskKey!==prior.key||c.owner!==prior.owner||c.taskIntentDigest!==intent||c.priorAdmission?.validUntil!==prior.admission.validUntil)continue;
   const op=this.lifecycleControl(row.operation_id);if(!op||op.rigId!==rigId)continue;
   const f=this.dutyFacts(row.operation_id);if(f.superseded)continue;
   this.recordCompletionObservation(op,row.operation_id,f);
  }
 }
 /** Apply-time capture for held-history authoring and pickup duties: the Operator's recovery-binding operation completes
  *  them. The binding operation id is 'held-authoring:' + the authoring duty id and the pickup duty id is derived from the
  *  authoring id, so both are found by primary key. Only duties naming exactly this binding operation are observed. */
 captureHeldHistoryBinding(rigId:string,bindingOperationId:string):void {
  if(typeof bindingOperationId!=='string'||!bindingOperationId.startsWith('held-authoring:'))return;
  const authoringId=bindingOperationId.slice('held-authoring:'.length),pickupId='qitem-coordination-lifecycle-'+digest(authoringId+':operator-record-pickup').slice(0,24);
  for(const id of [authoringId,pickupId]){
   const op=this.lifecycleControl(id);
   if(op&&op.rigId===rigId&&['held-history-authoring','held-history-pickup'].includes(op.receipt.kind)&&op.receipt.bindingOperationId===bindingOperationId)this.observeLifecycleCompletion(id);
  }
 }
 /** Apply-time capture for report-only retirement duties: the recipient's native failed/canceled report of the TARGET
  *  completes them. Found by the indexed (targetQueueId, kind) lookup the retirement staging already uses (099). */
 captureNativeRetirementTerminal(targetQueueId:string):void {
  const rows=this.db.prepare("SELECT operation_id FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.targetQueueId')=? AND json_extract(receipt,'$.kind') IN ('held-history-retirement','lifecycle-retirement','qualification-assessment-retirement')").all(targetQueueId) as Array<{operation_id:string}>;
  for(const r of rows)this.observeLifecycleCompletion(r.operation_id);
 }
 /** Capture the qualification retirement completion while its finite native
  *  duty is still live. The final required event may be the sweep-row closure. */
 captureQualificationAssessmentRetirementProgress(queueId:string):void {
  const rows=this.db.prepare("SELECT rig_id,operation_id,receipt FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='qualification-assessment-retirement' AND (json_extract(receipt,'$.targetQueueId')=? OR json_extract(receipt,'$.sweepFindingQueueId')=? OR json_extract(receipt,'$.accountabilityControlQueueId')=?)").all(queueId,queueId,queueId) as Array<{rig_id:string;operation_id:string;receipt:string}>;
  for(const row of rows){const r=JSON.parse(row.receipt),a=this.authority.get(row.rig_id),duty=this.repo.getById(row.operation_id),claim=this.db.prepare('SELECT claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(row.operation_id) as any;
   if(!this.dutyPostcondition(row.rig_id,r)||!this.dutyBinding(row.rig_id,r)||r.deadline<=this.now()||!a||a.state!=='active'||a.lease_until<=this.now()||a.epoch!==r.epoch||a.owner_session!==r.holder||a.owner_generation!==r.holderGeneration||this.authority.generation(r.holder)!==r.holderGeneration||this.authority.generation(r.recipient)!==r.recipientGeneration||!duty?.claimedAt||!['in-progress','blocked'].includes(duty.state)||claim?.claimed_by_generation_uuid!==r.recipientGeneration)continue;
   this.observeLifecycleCompletion(row.operation_id);
  }
 }
 /** Targeted capture for the duties a plan revision can complete: a package's materialization duty when its task is
  *  new or its admission changed, and an admission-refresh duty when a task's admission bytes were replaced. */
 private captureConfigureCompletions(plan:CoordinationPlan,prior:CoordinationPlan|null):void {
  const before=new Map((prior?.tasks??[]).map(t=>[t.key,t]));
  for(const t of plan.tasks){
   const old=before.get(t.key);if(old&&JSON.stringify(old.admission)===JSON.stringify(t.admission))continue;
   const pkg=this.db.prepare('SELECT contract_hash FROM coordinator_packages WHERE rig_id=? AND package_key=?').get(plan.rigId,t.packageKey) as {contract_hash:string}|undefined;
   if(pkg)this.captureDutyCompletion(plan.rigId,'materialization',t.packageKey+':'+pkg.contract_hash);
   if(old)this.captureAdmissionRefreshCompletion(plan.rigId,old);
  }
 }
 lifecycleControlCompleted(queueId:string):boolean {return this.dutyFacts(queueId).close;}
 lifecycleControlClaimAllowed(queueId:string,actor:string,generation:string|null|undefined,provenance:string|null|undefined):boolean {const op=this.lifecycleControl(queueId);return !!op&&actor===op.receipt.recipient&&generation===op.receipt.recipientGeneration&&this.authority.generation(actor)===generation&&provenance==='transport:v1'&&this.dutyFacts(queueId).claim;}
 lifecycleControlActAllowed(queueId:string,actor:string,generation:string|null|undefined,provenance:string|null|undefined):boolean {const op=this.lifecycleControl(queueId),q=this.repo.getById(queueId),claimed=this.db.prepare('SELECT claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(queueId) as {claimed_by_generation_uuid:string|null}|undefined;return !!op&&actor===op.receipt.recipient&&generation===op.receipt.recipientGeneration&&this.authority.generation(actor)===generation&&provenance==='transport:v1'&&!!q?.claimedAt&&['in-progress','blocked'].includes(q.state)&&claimed?.claimed_by_generation_uuid===generation&&this.dutyFacts(queueId).act;}
 private validOwnedLifecycleRecovery(rigId:string,duty:any,recovery:any):boolean {
  const plan=this.plan(rigId),target=plan?.tasks.find(t=>t.packageKey===duty.packageKey),task=plan?.tasks.find(t=>t.packageKey===recovery.recoveryPackageKey),a=this.authority.get(rigId);
  const row=this.db.prepare('SELECT a.destination,a.body_hash,q.body,q.state,q.claimed_by_generation_uuid FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? AND a.package_key=? AND a.queue_id=?').get(rigId,recovery.recoveryPackageKey,recovery.recoveryQueueId) as any;
  return !!a&&a.state==='active'&&a.lease_until>this.now()&&a.epoch===recovery.epoch&&a.owner_session===recovery.actor&&a.owner_generation===recovery.generation&&this.authority.generation(recovery.actor)===recovery.generation&&!!plan&&plan.operatorGeneration===this.authority.generation('operator-agent@kernel')&&!!target&&!!task&&task.recoveryFor===target.key&&task.packageKey!==duty.packageKey&&this.admittedNow(task)&&!!row&&row.destination===task.owner&&row.state==='in-progress'&&row.claimed_by_generation_uuid===recovery.workerGeneration&&this.authority.generation(task.owner)===recovery.workerGeneration&&row.body_hash===digest(row.body)&&digest(task.body)===row.body_hash;
 }
 recordLifecycleRecovery(actor:string,generation:string,input:{rigId:string;dutyQueueId:string;recoveryPackageKey:string;recoveryQueueId:string;evidenceRef:string}):void {
  this.db.transaction(()=>{
   const op=this.lifecycleControl(input.dutyQueueId),q=this.db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(input.dutyQueueId) as any;
   const row=this.db.prepare('SELECT q.claimed_by_generation_uuid FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? AND a.package_key=? AND a.queue_id=?').get(input.rigId,input.recoveryPackageKey,input.recoveryQueueId) as any;
   const receipt={actor,generation,epoch:this.authority.get(input.rigId)?.epoch,recoveryPackageKey:input.recoveryPackageKey,recoveryQueueId:input.recoveryQueueId,workerGeneration:row?.claimed_by_generation_uuid,evidenceRef:input.evidenceRef};
   if(!op||op.rigId!==input.rigId||!['acceptance','recovery'].includes(op.receipt.kind)||!this.dutyFacts(input.dutyQueueId).act||!q||digest(q.body)!==op.receipt.bodyHash||q.destination_session!==actor||q.claimed_by_generation_uuid!==generation||!['in-progress','blocked'].includes(q.state)||Date.parse(q.expires_at)<=this.now()||typeof input.evidenceRef!=='string'||!input.evidenceRef.trim()||!this.validOwnedLifecycleRecovery(input.rigId,op.receipt,receipt))fail('coordination_lifecycle_recovery_required','Exact current holder claim and distinct admitted genuinely active recovery required; prose cannot close acceptance duty');
   const id='lifecycle-recovery:'+input.dutyQueueId,prior=this.db.prepare('SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=?').get(input.rigId,id) as {receipt:string}|undefined;
   if(prior){if(prior.receipt!==JSON.stringify(receipt))fail('coordination_lifecycle_recovery_conflict','Frozen recovery disposition differs');return;}
   this.db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run(input.rigId,id,'coordinator-lifecycle-recovery',JSON.stringify(receipt),digest(JSON.stringify(receipt)));
   this.observeLifecycleCompletion(input.dutyQueueId);
  }).immediate();
 }
 /** A genuine recipient claim is its own native reading action, not proof of
  * transport delivery. Only that exact claimant may consume its UNKNOWN notice. */
 heldHistoryAuthoringClaimAllowed(queueId:string,actor:string,generation:string|null|undefined,provenance:string|null|undefined):boolean {return this.lifecycleControlClaimAllowed(queueId,actor,generation,provenance);}
 validLifecycleControlWake(source:string|undefined,destination:string,queueId:string):boolean {const op=this.lifecycleControl(queueId);return source==='watchdog@system'&&!!op&&destination===op.receipt.recipient&&this.dutyFacts(queueId).send;}
 private currentReturnObserver(jobId:string,operatorGeneration:string):boolean {
  return !!this.db.prepare("SELECT 1 FROM watchdog_jobs WHERE job_id=? AND policy='coordinator-continuity' AND state='active' AND target_session='operator-agent@kernel' AND registered_by_session='operator-agent@kernel' AND registered_by_generation_uuid=?").get(jobId,operatorGeneration);
 }
 private intakeLineageReady(r:any,excludeEffect?:string):boolean {
  const a=this.authority.get(r.rigId),plan=this.plan(r.rigId),root=this.repo.getById(r.rootQueueId),previous=this.repo.getById(r.previousQueueId);
  const transition=this.db.prepare('SELECT * FROM queue_transitions WHERE qitem_id=? ORDER BY transition_id DESC LIMIT 1').get(r.previousQueueId) as any;
  const priorNotice=this.db.prepare('SELECT delivery_state FROM outbox_entries WHERE audit_pointer=? AND outbox_id LIKE ?').all(r.previousQueueId,'wake-intent-%') as Array<{delivery_state:string}>;
  const claim=this.db.prepare('SELECT claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(r.previousQueueId) as any;
  // Expiry removes authority, never native custody. Claimed intake succession
  // requires the current original claimant's actual native failure/cancellation.
  if(!previous)return false;
  if(previous.claimedAt){if(claim?.claimed_by_generation_uuid!==r.operatorGeneration||!['failed','canceled'].includes(previous.state)||transition?.state!==previous.state||transition?.actor_session!=='operator-agent@kernel'||transition?.identity_provenance!=='transport:v1')return false;}
  else if(previous.state!=='pending'||!previous.expiresAt||Date.parse(previous.expiresAt)>this.now())return false;
  if(!priorNotice.length||!root||!previous||digest(root.body)!==r.rootBodyHash||digest(previous.body)!==r.previousBodyHash||digest(JSON.stringify(transition))!==r.previousTransitionHash||priorNotice.some(n=>!['delivered','failed','retired'].includes(n.delivery_state))||(['pending','in-progress','blocked'].includes(previous.state)&&(!previous.expiresAt||Date.parse(previous.expiresAt)>this.now()))||!a||a.state!=='active'||a.epoch!==r.epoch||a.owner_session!==r.holder||a.owner_generation!==r.holderGeneration||a.lease_until<=this.now()||this.authority.generation(r.holder)!==r.holderGeneration||!plan||plan.revision!==r.planRevision||plan.operatorGeneration!==r.operatorGeneration||this.authority.generation('operator-agent@kernel')!==r.operatorGeneration||!this.currentReturnObserver(r.jobId,r.operatorGeneration))return false;
  try{this.authority.assertCurrentOwner(r.holder,{rigId:r.rigId,epoch:r.epoch,generation:r.holderGeneration});}catch{return false;}
  const retained=r.retainedQueueId?this.repo.getById(r.retainedQueueId):null;
  if(r.retainedQueueId&&(!retained||digest(retained.body)!==r.retainedBodyHash))return false;
  if(r.taskOwner&&(!this.authority.generation(r.taskOwner)||this.authority.generation(r.taskOwner)!==r.taskOwnerGeneration||this.workerEffectDebt(r.taskOwner,excludeEffect)))return false;
  const control=r.retainedQueueId?this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='native-terminal-return-control'").get(r.rigId,r.retainedQueueId) as {receipt:string}|undefined:undefined;
  if(r.retainedReceiptHash&&!control)return false;
  if(control){if(digest(control.receipt)!==r.retainedReceiptHash)return false;const c=JSON.parse(control.receipt),original=this.db.prepare('SELECT disposition_id FROM coordinator_assignments WHERE rig_id=? AND queue_id=?').get(r.rigId,c.originalQueueId) as any;if(!original||original.disposition_id||this.authority.generation(c.worker)!==c.workerGeneration||this.workerEffectDebt(c.worker,excludeEffect))return false;}
  return this.lifecycleRecipientReady(r.rigId,'operator-agent@kernel',r.packageKey,excludeEffect)&&this.lifecycleRecipientReady(r.rigId,r.holder,r.packageKey,excludeEffect)&&![r.holder,'operator-agent@kernel'].some(session=>this.db.prepare('SELECT 1 FROM seat_delivery_guards WHERE (desired=1 OR effective=1) AND node_id IN (SELECT node_id FROM sessions WHERE session_name=?)').get(session));
 }
 private lineageIntakeAuthorized(rigId:string,queueId:string,controlQueueId:string,operatorGeneration:string,packageKey:string):boolean {
  const op=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='coordination-task-hold-lineage'").get(rigId,queueId) as {receipt:string}|undefined;
  if(!op)return false;const r=JSON.parse(op.receipt),q=this.repo.getById(queueId);
  return !!q&&r.queueId===queueId&&r.retainedQueueId===controlQueueId&&r.operatorGeneration===operatorGeneration&&r.packageKey===packageKey&&r.deadline>this.now()&&q.expiresAt===new Date(r.deadline).toISOString()&&digest(q.body)===r.bodyHash&&this.intakeLineageReady(r,'wake-intent-'+queueId);
 }
 validTaskHoldLineageWake(source:string|undefined,destination:string,queueId:string):boolean {
  if(source!=='watchdog@system'||destination!=='operator-agent@kernel')return false;
  const op=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE operation_id=? AND kind='coordination-task-hold-lineage'").get(queueId) as {receipt:string}|undefined;if(!op)return false;
  const r=JSON.parse(op.receipt),q=this.repo.getById(queueId),notice=this.db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+queueId) as any;
  return !!q&&q.state==='pending'&&!q.claimedAt&&q.sourceSession===source&&q.destinationSession===destination&&!!notice&&['pending','sending'].includes(notice.delivery_state)&&notice.sender_session===source&&notice.destination_session===destination&&notice.audit_pointer===queueId&&digest(notice.body)===r.noticeBodyHash&&this.lineageIntakeAuthorized(r.rigId,queueId,r.retainedQueueId,r.operatorGeneration,r.packageKey);
 }
 private advanceTaskHoldIntakes(rigId:string,jobId:string,result:CoordinationResult[]):void {
  const a=this.authority.get(rigId),plan=this.plan(rigId);if(!a||!plan||!this.currentReturnObserver(jobId,plan.operatorGeneration))return;
  for(const held of result){if(held.state!=='held')continue;
   // Registered-observer lineage is the mechanism for terminal-return exhaustion only;
   // lifecycle and administrative holds are renewed exclusively by the intake chain, so a
   // clean Operator seat can never end up with two live accountable items for one hold.
   if(LIFECYCLE_INTAKE_RENEWAL_REASONS.includes(held.reason??''))continue;
   const subject=this.intakeHoldSubject(rigId,held,this.missingAttributedReturns(rigId),true);if(!subject)continue;
   const rootQueueId='qitem-coordination-task-hold-'+digest(JSON.stringify({rigId,revision:plan.revision,operatorGeneration:plan.operatorGeneration,packageKey:subject.packageKey,reason:held.reason,queueId:held.queueId??null})).slice(0,24);
   const root=this.repo.getById(rootQueueId);if(!root)continue;const h=JSON.parse(root.body),creation=this.db.prepare('SELECT * FROM queue_transitions WHERE qitem_id=? ORDER BY transition_id LIMIT 1').get(rootQueueId) as any;
   if(root.sourceSession!=='watchdog@system'||root.destinationSession!=='operator-agent@kernel'||creation?.state!=='pending'||creation.actor_session!=='watchdog@system'||creation.identity_provenance!=='system:operator-authorized-coordination'||h.grantsAuthority!==false||h.recipientGeneration!==plan.operatorGeneration||h.rigId!==rigId)continue;
   const explicit=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='native-terminal-return-intake-authorization' AND json_extract(receipt,'$.previousIntakeQueueId')=? ORDER BY rowid DESC LIMIT 1").get(rigId,rootQueueId) as {receipt:string}|undefined;if(explicit){const q=this.repo.getById(JSON.parse(explicit.receipt).queueId);if(q&&['pending','in-progress','blocked'].includes(q.state)&&q.expiresAt&&Date.parse(q.expiresAt)>this.now())continue;}
   const latest=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='coordination-task-hold-lineage' AND json_extract(receipt,'$.rootQueueId')=? ORDER BY rowid DESC LIMIT 1").get(rigId,rootQueueId) as {receipt:string}|undefined,previous=this.repo.getById(latest?JSON.parse(latest.receipt).queueId:rootQueueId);if(!previous)continue;
   const queueId='qitem-coordination-task-hold-'+digest(rootQueueId+':'+previous.qitemId).slice(0,24),transition=this.db.prepare('SELECT * FROM queue_transitions WHERE qitem_id=? ORDER BY transition_id DESC LIMIT 1').get(previous.qitemId),retained=h.retainedQueueId?this.repo.getById(h.retainedQueueId):null;
   const r={rigId,jobId,queueId,rootQueueId,rootBodyHash:digest(root.body),previousQueueId:previous.qitemId,previousBodyHash:digest(previous.body),previousTransitionHash:digest(JSON.stringify(transition)),retainedQueueId:h.retainedQueueId,retainedBodyHash:retained?digest(retained.body):null,retainedReceiptHash:(()=>{const c=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='native-terminal-return-control'").get(rigId,h.retainedQueueId) as {receipt:string}|undefined;return c?digest(c.receipt):null;})(),packageKey:h.packageKey,taskOwner:h.taskOwner,taskOwnerGeneration:this.authority.generation(h.taskOwner),planRevision:plan.revision,operatorGeneration:plan.operatorGeneration,holder:a.owner_session,holderGeneration:a.owner_generation,epoch:a.epoch,deadline:this.now()+1200000};
   if(!this.intakeLineageReady(r)||this.repo.getById(queueId))continue;
   const body=JSON.stringify({...h,queueId,rootQueueId,previousQueueId:r.previousQueueId,deadline:r.deadline,claimCommand:'rig queue claim '+queueId,required:h.required+' This is a new finite accountable intake, not renewed authority for any predecessor. Preserve prior claims, effects and notices; genuinely claim this item before supported recovery.'});
   this.db.transaction(()=>{if(!this.intakeLineageReady(r)||this.repo.getById(queueId))return;this.repo.createWithinTransaction({qitemId:queueId,sourceSession:'watchdog@system',destinationSession:'operator-agent@kernel',expiresAt:new Date(r.deadline).toISOString(),body,identityProvenance:'system:operator-authorized-coordination',nudge:false});const noticeId=this.repo.stageTaskHoldLineageWake(queueId,plan.operatorGeneration);const notice=this.db.prepare('SELECT body FROM outbox_entries WHERE outbox_id=?').get(noticeId) as {body:string};this.db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run(rigId,queueId,'coordination-task-hold-lineage',JSON.stringify({...r,bodyHash:digest(body),noticeBodyHash:digest(notice.body)}),digest(JSON.stringify(r)));})();
  }
 }
 /** Assignments completed without an attributed typed disposition still need a return. */
 private missingAttributedReturns(rigId:string):Array<{package_key:string;queue_id:string;destination:string;body_hash:string;body:string;ts_updated:string;claimed_by_generation_uuid:string|null;contract:string}> {
  return this.db.prepare("SELECT a.package_key,a.queue_id,a.destination,a.body_hash,q.body,q.ts_updated,q.claimed_by_generation_uuid,p.contract FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id JOIN coordinator_packages p ON p.rig_id=a.rig_id AND p.package_key=a.package_key WHERE a.rig_id=? AND a.disposition_id IS NULL AND q.state IN ('done','failed','denied','canceled','handed-off')").all(rigId) as Array<{package_key:string;queue_id:string;destination:string;body_hash:string;body:string;ts_updated:string;claimed_by_generation_uuid:string|null;contract:string}>;
 }
 /** The next bounded intake id in one chain, always derived from the item it follows, so
   * every renewal is a distinct finite item and no earlier id is ever reused or extended. */
 private nextIntakeChainId(rootQueueId:string,previousQueueId:string):string {return 'qitem-coordination-task-hold-'+digest('intake-chain:'+rootQueueId+':'+previousQueueId).slice(0,24);}
 /** Liveness of an accountable item: a live or genuinely claimed intake still owns the
   * hold. Only an expired, unclaimed item may be followed by a fresh one. */
 private intakeStillAccounted(item:any):boolean {return ['pending','in-progress','blocked'].includes(item.state)&&(!item.expiresAt||Date.parse(item.expiresAt)>this.now()||!!item.claimedAt);}
 /** One resolver for the whole chain of a held reason: validates the deterministic root,
   * then follows the actual prior items to the latest one. Null means the recorded root is
   * not a genuine accountable intake for this hold, so nothing may be derived from it. */
 private intakeChainTail(rigId:string,plan:CoordinationPlan,held:CoordinationResult,task:{packageKey:string;owner:string}):{rootQueueId:string;tail:any}|null {
  const rootQueueId='qitem-coordination-task-hold-'+digest(JSON.stringify({rigId,revision:plan.revision,operatorGeneration:plan.operatorGeneration,packageKey:task.packageKey,reason:held.reason,queueId:held.queueId??null})).slice(0,24),root=this.repo.getById(rootQueueId);
  if(!root)return {rootQueueId,tail:null};
  if(!this.validAccountableIntake(rigId,rootQueueId,root))return null;
  let tail=root;
  for(;;){const next=this.repo.getById(this.nextIntakeChainId(rootQueueId,tail.qitemId));if(!next||!this.validAccountableIntakeChainItem(rigId,next,rootQueueId))break;tail=next;}
  return {rootQueueId,tail};
 }
 /** An accountable intake must be this control plane's own exact item for this rig, with
   * no authority and no identity claims beyond the current Operator generation. */
 private validAccountableIntake(rigId:string,qitemId:string,q:any):boolean {
  if(q.sourceSession!=='watchdog@system'||q.destinationSession!=='operator-agent@kernel')return false;
  const creation=this.db.prepare('SELECT actor_session,identity_provenance FROM queue_transitions WHERE qitem_id=? ORDER BY transition_id LIMIT 1').get(qitemId) as any;
  if(creation?.actor_session!=='watchdog@system'||creation?.identity_provenance!=='system:operator-authorized-coordination')return false;
  let h:any;try{h=JSON.parse(q.body);}catch{return false;}
  if(h.rigId!==rigId||h.grantsAuthority!==false||typeof h.reason!=='string'||typeof h.packageKey!=='string')return false;
  return qitemId==='qitem-coordination-task-hold-'+digest(JSON.stringify({rigId:h.rigId,revision:h.planRevision,operatorGeneration:h.recipientGeneration,packageKey:h.packageKey,reason:h.reason,queueId:h.retainedQueueId??null})).slice(0,24);
 }
 /** A chain successor is valid only when it follows a genuine member of the same chain
   * and its id is exactly the derived next id, so a spoofed item cannot extend a chain. */
 private validAccountableIntakeChainItem(rigId:string,q:any,rootQueueId:string):boolean {
  const qitemId=q.qitemId;
  if(!qitemId||q.sourceSession!=='watchdog@system'||q.destinationSession!=='operator-agent@kernel')return false;
  const creation=this.db.prepare('SELECT actor_session,identity_provenance FROM queue_transitions WHERE qitem_id=? ORDER BY transition_id LIMIT 1').get(qitemId) as any;
  if(creation?.actor_session!=='watchdog@system'||creation?.identity_provenance!=='system:operator-authorized-coordination')return false;
  let h:any;try{h=JSON.parse(q.body);}catch{return false;}
  if(h.rigId!==rigId||h.grantsAuthority!==false||h.rootQueueId!==rootQueueId||h.renewsIntakeQueueId!==rootQueueId||typeof h.previousIntakeQueueId!=='string'||!this.repo.getById(h.previousIntakeQueueId))return false;
  const recorded=this.db.prepare("SELECT 1 FROM coordinator_operations WHERE rig_id=? AND kind='coordination-intake-chain' AND operation_id=?").get(rigId,'coordination-intake-chain:'+qitemId);
  return !!recorded&&qitemId===this.nextIntakeChainId(rootQueueId,h.previousIntakeQueueId);
 }
 /** The accountable subject of a held result, resolved identically for the first intake
   * and for renewal: a plan task, a missing attributed return, the retained lifecycle
   * control receipt, or the administrative boundary key. Renewal therefore never goes
   * silent just because a lifecycle or administrative key has no plan task. */
 private intakeHoldSubject(rigId:string,held:CoordinationResult,missingReturns:ReadonlyArray<{package_key:string;destination:string}>=[],forRenewal=false):{packageKey:string;owner:string}|null {
  if(held.state!=='held'||!INTAKE_ROUTED_REASONS.includes(held.reason??''))return null;
  // A frontier hold can precede any duty row; its own subject is authoritative.
  if(held.subject)return {packageKey:held.subject.packageKey,owner:held.subject.owner};
  const plan=this.plan(rigId);if(!plan)return null;
  const missing=missingReturns.find(m=>'terminal-return:'+m.package_key===held.key);
  const control=held.queueId?this.lifecycleControl(held.queueId)?.receipt:null;
  // Administrative duty prefixes resolve their subject from the key alone, so a held duty that
  // never staged a queue row (protected recipient, no prior control receipt) is still routable.
  const administrativePrefix=['outcome-qualification-refresh:','admission-refresh:'].find(p=>held.key.startsWith(p));
  const administrative=administrativePrefix?{packageKey:held.key.slice(administrativePrefix.length),owner:'operator-agent@kernel'}:null;
  // Renewal keeps the original package key resolution for a terminal return whose
  // control row is already consumed; first staging still needs a real subject.
  const terminal=forRenewal&&held.key.startsWith('terminal-return:')?{packageKey:held.key.slice('terminal-return:'.length),owner:control?.recipient??'operator-agent@kernel'}:null;
  return plan.tasks.find(t=>t.key===held.key)??(missing?{packageKey:missing.package_key,owner:missing.destination}:control?{packageKey:control.packageKey,owner:control.recipient}:terminal??administrative);
 }
 /** One deterministic accountable intake per held reason and prior link; it is a fresh
   * finite recovery item, never renewed authority for any predecessor. */
 private stageTaskHoldIntake(rigId:string,a:any,plan:CoordinationPlan,held:CoordinationResult,missingReturns:ReadonlyArray<{package_key:string;destination:string}>):void {
  const task=this.intakeHoldSubject(rigId,held,missingReturns);if(!task)return;
  const rootId='qitem-coordination-task-hold-'+digest(JSON.stringify({rigId,revision:plan.revision,operatorGeneration:plan.operatorGeneration,packageKey:task.packageKey,reason:held.reason,queueId:held.queueId??null,...(held.subject?{subjectIdentity:held.subject.identity}:{})})).slice(0,24);
  // One mechanism answers each routed reason exactly once. Every reason gets a single
  // deterministic first intake at the root id. Only lifecycle and administrative holds,
  // whose accountability must outlive any one intake window, renew through the chain;
  // terminal-return exhaustion keeps its registered-observer lineage succession, and every
  // other routed reason keeps its original single deterministic intake, untouched.
  let renewed=false,queueId=rootId,chainRoot=rootId,priorItem:any=null;
  if(LIFECYCLE_INTAKE_RENEWAL_REASONS.includes(held.reason??'')){
   const chain=this.intakeChainTail(rigId,plan,held,task);
   if(!chain)return;
   chainRoot=chain.rootQueueId;priorItem=chain.tail;
   if(priorItem&&this.intakeStillAccounted(priorItem))return;
   renewed=!!priorItem;queueId=renewed?this.nextIntakeChainId(chainRoot,priorItem.qitemId):chainRoot;
  }else if(this.repo.getById(rootId))return;
   try {this.db.transaction(()=>{if(!this.repo.getById(queueId)){
    const deadline=this.now()+1200000;
    this.repo.createWithinTransaction({qitemId:queueId,sourceSession:'watchdog@system',destinationSession:'operator-agent@kernel',expiresAt:new Date(deadline).toISOString(),body:JSON.stringify({action:'resolve-exact-coordination-task-hold',rigId,planRevision:plan.revision,packageKey:task.packageKey,taskOwner:task.owner,reason:held.reason,retainedQueueId:held.queueId??null,recipientGeneration:plan.operatorGeneration,deadline,grantsAuthority:false,returnPath:a.owner_session,...(renewed?{rootQueueId:chainRoot,previousIntakeQueueId:priorItem.qitemId,renewsIntakeQueueId:chainRoot,renewalNote:'The previous finite intake for this same held reason reached its deadline while the hold persisted. This is a new finite accountable intake, not renewed authority; prior claims, effects and notices stand, and no prior deadline is extended.'}:{}),...(held.reason==='terminal-return-duty-exhausted'?{nextAction:'For an expired still-claimed control: claim this intake genuinely; the registered Operator observer can stage finite failure-only retirement when native idle and effects are proven. The original worker records failed/canceled; then use coordination-return-successor. Never extend expiry or cancel on the worker behalf.'}:{}),required:'Claim this bounded recovery and inspect the exact current task, native identity and custody. Repair an expired admission only from current qualified evidence. Reconcile uncertain effects through supported disposition without assuming delivery or retrying unknown effects. For a retained pre-ledger row, preserve it and have the current Lead define a distinct admitted follow-up contract when needed; never forge an assignment or reopen terminal history. Return supported resolution evidence or a named protected boundary. Reconcile eligible independent product frontier afterward; this control item grants no acceptance, product qualification, checkpoint release or dispatch authority.'}),identityProvenance:'system:operator-authorized-coordination',nudge:true});
    this.repo.stageWakeIntent(queueId,'watchdog@system','operator-agent@kernel','system:operator-authorized-coordination',true,plan.operatorGeneration);
    const notice=this.db.prepare('SELECT body FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+queueId) as any;if(!notice?.body)throw new Error('accountable intake wake intent missing');
    // The chain is durable and hash-bound, so any later reader can prove this exact
    // notice belongs to this exact accountable item.
    const chainReceipt={rigId,rootQueueId:chainRoot,queueId,previousIntakeQueueId:renewed?priorItem.qitemId:null,packageKey:task.packageKey,reason:held.reason,retainedQueueId:held.queueId??null,operatorGeneration:plan.operatorGeneration,planRevision:plan.revision,grantsAuthority:false,noticeBodyHash:digest(notice.body)};
    this.db.prepare('INSERT OR IGNORE INTO coordinator_operations VALUES (?,?,?,?,?)').run(rigId,'coordination-intake-chain:'+queueId,'coordination-intake-chain',JSON.stringify(chainReceipt),digest(JSON.stringify(chainReceipt)));
   }})();}catch(error){
    const code=heldDispatchCode(error);if(!code)throw error;
    const receipt={rigId,packageKey:task.packageKey,reason:code,owner:'operator-agent@kernel',action:'Resolve native Operator reservation before exact recovery intake',grantsAuthority:false};
    this.db.prepare('INSERT OR IGNORE INTO coordinator_operations VALUES (?,?,?,?,?)').run(rigId,'coordination-intake-hold:'+digest(queueId+':'+code),'coordination-intake-hold',JSON.stringify(receipt),digest(JSON.stringify(receipt)));
   }
  }
 private exhaustionIntakeProvenance(rigId:string,queueId:string,controlQueueId:string,packageKey:string):boolean {
  const q=this.repo.getById(queueId),h=q?JSON.parse(q.body):null,creation=this.db.prepare('SELECT state,actor_session,identity_provenance FROM queue_transitions WHERE qitem_id=? ORDER BY transition_id LIMIT 1').get(queueId) as any;
  const expected=h?'qitem-coordination-task-hold-'+digest(JSON.stringify({rigId,revision:h.planRevision,operatorGeneration:h.recipientGeneration,packageKey,reason:'terminal-return-duty-exhausted',queueId:controlQueueId})).slice(0,24):null;
  return !!q&&q.sourceSession==='watchdog@system'&&q.destinationSession==='operator-agent@kernel'&&!!creation&&creation.state==='pending'&&creation.actor_session==='watchdog@system'&&creation.identity_provenance==='system:operator-authorized-coordination'&&queueId===expected&&h.action==='resolve-exact-coordination-task-hold'&&h.reason==='terminal-return-duty-exhausted'&&h.rigId===rigId&&h.packageKey===packageKey&&h.retainedQueueId===controlQueueId&&h.grantsAuthority===false&&Number.isSafeInteger(h.deadline)&&h.deadline===Date.parse(q.expiresAt??'')&&!!this.db.prepare("SELECT 1 FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='coordination-plan' AND json_extract(receipt,'$.operatorGeneration')=?").get(rigId,'coordination-plan:'+h.planRevision,h.recipientGeneration);
 }
 private refreshedIntakeAuthorized(rigId:string,queueId:string,controlQueueId:string,operatorGeneration:string,packageKey:string):boolean {
  const q=this.repo.getById(queueId),h=q?JSON.parse(q.body):null;
  const saved=h?.authorizationId?this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='native-terminal-return-intake-authorization'").get(rigId,h.authorizationId) as {receipt:string}|undefined:undefined;
  if(!saved)return false;const r=JSON.parse(saved.receipt),a=this.authority.get(rigId),plan=this.plan(rigId),prior=this.repo.getById(r.previousIntakeQueueId),control=this.repo.getById(controlQueueId);
  const record=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='native-terminal-return-control'").get(rigId,controlQueueId) as {receipt:string}|undefined;
  const original=this.db.prepare('SELECT a.destination,a.body_hash,a.disposition_id,q.body,q.state,q.claimed_by_generation_uuid,p.contract FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id JOIN coordinator_packages p ON p.rig_id=a.rig_id AND p.package_key=a.package_key WHERE a.rig_id=? AND a.queue_id=? AND a.package_key=?').get(rigId,r.originalQueueId,packageKey) as any;
  return !!q&&queueId===r.queueId&&h.authorizationId===r.authorizationId&&h.rigId===rigId&&h.packageKey===packageKey&&h.retainedQueueId===controlQueueId&&h.recipientGeneration===operatorGeneration&&h.grantsAuthority===false&&h.deadline===r.deadline&&digest(q.body)===r.intakeBodyHash&&!!prior&&digest(prior.body)===r.previousIntakeBodyHash&&this.exhaustionIntakeProvenance(rigId,r.previousIntakeQueueId,controlQueueId,packageKey)&&!!control&&digest(control.body)===r.controlBodyHash&&!!record&&digest(record.receipt)===r.controlReceiptHash&&!!original&&!original.disposition_id&&digest(JSON.stringify(original))===r.originalHash&&original.claimed_by_generation_uuid===r.workerGeneration&&this.authority.generation(original.destination)===r.workerGeneration&&!!a&&a.state==='active'&&a.lease_until>this.now()&&a.epoch===r.epoch&&a.owner_session===r.holder&&a.owner_generation===r.holderGeneration&&this.authority.generation(r.holder)===r.holderGeneration&&!!plan&&plan.revision===r.planRevision&&plan.operatorGeneration===operatorGeneration&&this.authority.generation('operator-agent@kernel')===operatorGeneration&&r.operatorGeneration===operatorGeneration&&this.currentReturnObserver(r.observerJobId,operatorGeneration)&&this.lifecycleRecipientReady(rigId,'operator-agent@kernel',packageKey)&&this.lifecycleRecipientReady(rigId,r.holder,packageKey);
 }
 refreshTerminalReturnIntake(actor:string,generation:string,input:{rigId:string;previousIntakeQueueId:string;controlQueueId:string;controlBodyHash:string;workerGeneration:string;holderGeneration:string;observerJobId:string;deadline:number;operationId:string}):{queueId:string;deadline:number} {
  return this.db.transaction(()=>{
   if(actor!=='operator-agent@kernel'||!generation||this.authority.generation(actor)!==generation)fail('coordination_operator_required','Genuine current Operator required for administrative exhaustion intake');
   if(!input.operationId||input.operationId.length>160)fail('coordination_return_intake_required','Bounded operation ID required');
   const id='native-return-intake:'+input.operationId,requestHash=digest(JSON.stringify({actor,generation,input})),saved=this.db.prepare("SELECT receipt,request_hash FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='native-terminal-return-intake-authorization'").get(input.rigId,id) as {receipt:string;request_hash:string}|undefined;
   if(saved){if(saved.request_hash!==requestHash)fail('coordination_return_intake_conflict','Frozen administrative intake authorization differs');const r=JSON.parse(saved.receipt);return {queueId:r.queueId,deadline:r.deadline};}
   if(!Number.isSafeInteger(input.deadline)||input.deadline<=this.now()||input.deadline>this.now()+1200000)fail('coordination_return_intake_required','New administrative intake must expire within twenty minutes');
   const c=this.terminalReturnContinuationContext(input.rigId,input.controlQueueId,input.controlBodyHash,input.workerGeneration,input.deadline,undefined,true),previous=this.repo.getById(input.previousIntakeQueueId);
   const claimed=this.db.prepare('SELECT claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(input.previousIntakeQueueId) as any;
   if(c.a.owner_generation!==input.holderGeneration||!this.currentReturnObserver(input.observerJobId,generation)||!previous||!previous.expiresAt||Date.parse(previous.expiresAt)>this.now()||!this.exhaustionIntakeProvenance(input.rigId,input.previousIntakeQueueId,input.controlQueueId,c.r.packageKey)||claimed?.claimed_by_generation_uuid!==JSON.parse(previous!.body).recipientGeneration)fail('coordination_return_intake_required','Exact expired genuinely claimed runtime intake, registered current Operator observer and live holder required');
   if(this.db.prepare("SELECT 1 FROM coordinator_operations WHERE rig_id=? AND kind='native-terminal-return-intake-authorization' AND json_extract(receipt,'$.previousIntakeQueueId')=?").get(input.rigId,input.previousIntakeQueueId))fail('coordination_return_intake_conflict','Expired intake already has its one finite authorized successor');
   if(this.workerEffectDebt(actor)||this.workerEffectDebt(c.a.owner_session))fail('coordination_return_intake_unknown_effect','Reconcile current Operator and holder unknown effects before administrative intake');
   if(!this.lifecycleRecipientReady(input.rigId,actor,c.r.packageKey)||!this.lifecycleRecipientReady(input.rigId,c.a.owner_session,c.r.packageKey))fail('coordination_return_intake_protected','Operator checkpoint scope excludes administrative intake');
   const queueId='qitem-coordination-task-hold-'+digest(input.rigId+':'+input.previousIntakeQueueId+':'+id).slice(0,24),h=JSON.parse(previous!.body),body=JSON.stringify({...h,authorizationId:id,previousIntakeQueueId:input.previousIntakeQueueId,planRevision:c.plan.revision,recipientGeneration:generation,deadline:input.deadline,nextAction:'Genuinely claim this finite administrative intake. The registered current Operator observer can stage only failure/cancellation of the expired native return duty by its original worker. This grants no product dispatch, disposal, acceptance, lock release, admission or plan rewrite.'});
   const original=this.db.prepare('SELECT a.destination,a.body_hash,a.disposition_id,q.body,q.state,q.claimed_by_generation_uuid,p.contract FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id JOIN coordinator_packages p ON p.rig_id=a.rig_id AND p.package_key=a.package_key WHERE a.rig_id=? AND a.queue_id=? AND a.package_key=?').get(input.rigId,c.r.originalQueueId,c.r.packageKey);
   const receipt={authorizationId:id,queueId,previousIntakeQueueId:input.previousIntakeQueueId,previousIntakeBodyHash:digest(previous!.body),controlQueueId:input.controlQueueId,controlBodyHash:input.controlBodyHash,controlReceiptHash:c.receiptHash,originalQueueId:c.r.originalQueueId,originalHash:digest(JSON.stringify(original)),packageKey:c.r.packageKey,workerGeneration:input.workerGeneration,holder:c.a.owner_session,holderGeneration:c.a.owner_generation,epoch:c.a.epoch,operatorGeneration:generation,observerJobId:input.observerJobId,planRevision:c.plan.revision,deadline:input.deadline,intakeBodyHash:digest(body)};
   this.db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run(input.rigId,id,'native-terminal-return-intake-authorization',JSON.stringify(receipt),requestHash);
   this.repo.createWithinTransaction({qitemId:queueId,sourceSession:'watchdog@system',destinationSession:actor,expiresAt:new Date(input.deadline).toISOString(),body,identityProvenance:'system:operator-authorized-coordination',nudge:false});return {queueId,deadline:input.deadline};
  }).immediate();
 }
 private exactRetirementIntake(rigId:string,intakeQueueId:string,controlQueueId:string,operatorGeneration:string,packageKey:string):string {
  const q=this.db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(intakeQueueId) as any;
  const h=q?JSON.parse(q.body):null,creation=this.db.prepare('SELECT state,actor_session,identity_provenance FROM queue_transitions WHERE qitem_id=? ORDER BY transition_id LIMIT 1').get(intakeQueueId) as any;
  const expected=h?'qitem-coordination-task-hold-'+digest(JSON.stringify({rigId,revision:h.planRevision,operatorGeneration,packageKey,reason:'terminal-return-duty-exhausted',queueId:controlQueueId})).slice(0,24):null;
  if(!q||q.source_session!=='watchdog@system'||q.destination_session!=='operator-agent@kernel'||q.claimed_by_generation_uuid!==operatorGeneration||!['in-progress','blocked'].includes(q.state)||!creation||creation.state!=='pending'||creation.actor_session!=='watchdog@system'||creation.identity_provenance!=='system:operator-authorized-coordination'||(intakeQueueId!==expected&&!this.refreshedIntakeAuthorized(rigId,intakeQueueId,controlQueueId,operatorGeneration,packageKey)&&!this.lineageIntakeAuthorized(rigId,intakeQueueId,controlQueueId,operatorGeneration,packageKey))||h.action!=='resolve-exact-coordination-task-hold'||h.reason!=='terminal-return-duty-exhausted'||h.rigId!==rigId||h.packageKey!==packageKey||h.retainedQueueId!==controlQueueId||h.recipientGeneration!==operatorGeneration||h.grantsAuthority!==false||!Number.isSafeInteger(h.deadline)||h.deadline!==Date.parse(q.expires_at)||h.deadline<=this.now()||!this.db.prepare("SELECT 1 FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='coordination-plan' AND json_extract(receipt,'$.operatorGeneration')=?").get(rigId,'coordination-plan:'+h.planRevision,operatorGeneration))fail('coordination_return_retirement_required','Current Operator must genuinely claim the exact finite runtime exhaustion intake before retirement');
  return digest(q.body);
 }
 retireExpiredTerminalReturn(actor:string,generation:string,input:{rigId:string;intakeQueueId:string;controlQueueId:string;controlBodyHash:string;workerGeneration:string;deadline:number}):{queueId:string;outboxId:string;deadline:number} {
  return this.db.transaction(()=>{
   if(actor!=='operator-agent@kernel'||!generation||this.authority.generation(actor)!==generation)fail('coordination_operator_required','Genuine current Operator authorizes expired-control retirement');
   const id='native-return-retirement:'+input.controlQueueId,requestHash=digest(JSON.stringify({actor,generation,input}));
   const saved=this.db.prepare("SELECT receipt,request_hash FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='native-terminal-return-retirement'").get(input.rigId,id) as {receipt:string;request_hash:string}|undefined;
   if(saved){if(saved.request_hash!==requestHash)fail('coordination_return_retirement_conflict','This expired duty already has its one finite retirement notice');const r=JSON.parse(saved.receipt);return {queueId:r.controlQueueId,outboxId:r.outboxId,deadline:r.deadline};}
   if(input.deadline>this.now()+1200000)fail('coordination_return_retirement_required','Retirement notice must expire within twenty minutes');
  let chainNotices:string[]=[];
  try{const controlOp=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='native-terminal-return-control'").get(input.rigId,input.controlQueueId) as {receipt:string}|undefined;const cr=controlOp?JSON.parse(controlOp.receipt):null;if(cr&&cr.workerGeneration===input.workerGeneration)chainNotices=this.nativeReturnRetirementChainNotices(input.rigId,String(cr.originalQueueId),String(cr.worker),input.workerGeneration);}catch{chainNotices=[];}
  const c=this.terminalReturnContinuationContext(input.rigId,input.controlQueueId,input.controlBodyHash,input.workerGeneration,input.deadline,chainNotices,true);
   const intakeBodyHash=this.exactRetirementIntake(input.rigId,input.intakeQueueId,input.controlQueueId,generation,c.r.packageKey);
   const outboxId=this.repo.stageNativeTerminalReturnContinuation({controlQueueId:input.controlQueueId,worker:c.r.worker,workerGeneration:input.workerGeneration,proofId:id,body:JSON.stringify({action:'retire-expired-native-terminal-return',controlQueueId:input.controlQueueId,originalQueueId:c.r.originalQueueId,deadline:input.deadline,grantsAuthority:false,allowedClosureStates:['failed','canceled'],required:'This is a finite failure-only retirement notice. Your original duty has expired; do not execute expired work, dispose product scope, redo work or release locks. Under your own genuine native identity, record failed or canceled for ONLY controlQueueId through the supported queue update with an honest expiry reason. Preserve the original product claim, all typed returns, evidence and effects. Current Operator can then use the existing exact successor API. No automatic cancellation, acceptance or expiry extension is granted.'})});
   const effect=this.db.prepare('SELECT body FROM outbox_entries WHERE outbox_id=?').get(outboxId) as {body:string};
   const receipt={retirement:true,controlQueueId:input.controlQueueId,controlBodyHash:input.controlBodyHash,controlReceiptHash:c.receiptHash,intakeQueueId:input.intakeQueueId,intakeBodyHash,worker:c.r.worker,workerGeneration:input.workerGeneration,actor,actorGeneration:generation,holder:c.a.owner_session,holderGeneration:c.a.owner_generation,epoch:c.a.epoch,operatorGeneration:c.plan.operatorGeneration,deadline:input.deadline,outboxId,outboxBodyHash:digest(effect.body)};
   this.db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run(input.rigId,id,'native-terminal-return-retirement',JSON.stringify(receipt),requestHash);
   return {queueId:input.controlQueueId,outboxId,deadline:input.deadline};
  }).immediate();
 }
 private retireForAuthorizedObserver(rigId:string,operatorGeneration:string,result:CoordinationResult[]):CoordinationResult[] {
  for(const held of result){
   if(held.reason!=='terminal-return-duty-exhausted'||!held.queueId)continue;
   const control=this.repo.getById(held.queueId);if(!control||!['in-progress','blocked'].includes(control.state)||!control.expiresAt||Date.parse(control.expiresAt)>this.now())continue;
   const record=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='native-terminal-return-control'").get(rigId,held.queueId) as {receipt:string}|undefined;if(!record)continue;const r=JSON.parse(record.receipt);
   const plan=this.plan(rigId)!;let intakeQueueId='qitem-coordination-task-hold-'+digest(JSON.stringify({rigId,revision:plan.revision,operatorGeneration,packageKey:r.packageKey,reason:'terminal-return-duty-exhausted',queueId:held.queueId})).slice(0,24);
   const refreshed=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='native-terminal-return-intake-authorization' AND json_extract(receipt,'$.controlQueueId')=? ORDER BY rowid DESC LIMIT 1").get(rigId,held.queueId) as {receipt:string}|undefined;if(refreshed)intakeQueueId=JSON.parse(refreshed.receipt).queueId;
   const lineage=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='coordination-task-hold-lineage' AND json_extract(receipt,'$.retainedQueueId')=? ORDER BY rowid DESC LIMIT 1").get(rigId,held.queueId) as {receipt:string}|undefined;if(lineage)intakeQueueId=JSON.parse(lineage.receipt).queueId;
   const saved=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='native-terminal-return-retirement'").get(rigId,'native-return-retirement:'+held.queueId) as {receipt:string}|undefined;
   if(saved){const receipt=JSON.parse(saved.receipt);held.reason=receipt.deadline>this.now()?'native-retirement-notice-staged':'native-retirement-notice-exhausted';continue;}
   try{const staged=this.retireExpiredTerminalReturn('operator-agent@kernel',operatorGeneration,{rigId,intakeQueueId,controlQueueId:held.queueId,controlBodyHash:digest(control.body),workerGeneration:r.workerGeneration,deadline:this.now()+1200000});held.reason='native-retirement-notice-staged';held.deadline=staged.deadline;}catch(error){if(!(error instanceof CoordinatorFenceError))throw error;held.reason=error.code;}
  }
  return result;
 }
 /** Read-only proof shared by outbox selection and the final managed-send seam. */
 validTerminalReturnContinuationWake(source:string|undefined,destination:string,proofId:string):boolean {
  if(source!=='watchdog@system')return false;
  const op=this.db.prepare("SELECT rig_id,receipt FROM coordinator_operations WHERE operation_id=? AND kind IN ('native-terminal-return-continuation','native-terminal-return-retirement')").get(proofId) as {rig_id:string;receipt:string}|undefined;
  if(!op)return false;const r=JSON.parse(op.receipt);
  try{
   // Sending the one already-authorized failure-only retirement must revalidate the same C2
   // lineage exception as staging it. Ordinary continuations and all successor/dispatch gates keep debt.
   const controlOp=r.retirement?this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='native-terminal-return-control'").get(op.rig_id,r.controlQueueId) as {receipt:string}|undefined:undefined;
   const controlReceipt=controlOp?JSON.parse(controlOp.receipt):null;
   const excluded=r.retirement&&controlReceipt?[r.outboxId,...this.nativeReturnRetirementChainNotices(op.rig_id,controlReceipt.originalQueueId,r.worker,r.workerGeneration)]:r.outboxId;
   const c=this.terminalReturnContinuationContext(op.rig_id,r.controlQueueId,r.controlBodyHash,r.workerGeneration,r.deadline,excluded,r.retirement===true);
   const effect=this.db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get(r.outboxId) as any;
   if(r.retirement&&this.exactRetirementIntake(op.rig_id,r.intakeQueueId,r.controlQueueId,r.operatorGeneration,c.r.packageKey)!==r.intakeBodyHash)return false;
   return destination===r.worker&&c.receiptHash===r.controlReceiptHash&&c.a.epoch===r.epoch&&c.a.owner_session===r.holder&&c.a.owner_generation===r.holderGeneration&&c.plan.operatorGeneration===r.operatorGeneration&&this.authority.generation(r.actor)===r.actorGeneration&&(r.actor==='operator-agent@kernel'||r.actor===c.a.owner_session)&&!!effect&&effect.sender_session===source&&effect.destination_session===destination&&effect.audit_pointer===r.controlQueueId&&['pending','sending'].includes(effect.delivery_state)&&digest(effect.body)===r.outboxBodyHash&&(!r.dispositionId||this.validContinuationReturn(r.dispositionId,r.worker,r.workerGeneration,c.r.packageKey,c.contract));
  }catch(error){if(error instanceof CoordinatorFenceError)return false;throw error;}
 }
 async probeWorker(actor:string,generation:string,input:{rigId:string;worker:string}):Promise<{worker:string;generation:string;configurationDigest:string|null;observedAt:number;identityVerified:boolean;idle:boolean;observation:CoordinationActivity|null;grantsAuthority:false}> {
  const authorize=()=>{
   const a=this.authority.get(input.rigId),current=this.authority.generation(actor);
   if(!generation||current!==generation||!(actor==='operator-agent@kernel'||(a?.state==='active'&&a.owner_session===actor&&a.owner_generation===generation&&a.lease_until>this.now())))fail('coordination_worker_probe_required','Genuine current Operator or active holder required');
   if(!this.db.prepare('SELECT 1 FROM sessions s JOIN nodes n ON n.id=s.node_id WHERE s.session_name=? AND n.rig_id=?').get(input.worker,input.rigId)||!this.authority.generation(input.worker))fail('coordination_worker_probe_required','Existing native worker in exact rig required');
  };
  authorize();const before=this.authority.generation(input.worker)!;
  await this.refreshIdentity?.([input.worker]);authorize();
  if(this.authority.generation(input.worker)!==before)fail('coordination_worker_probe_changed','Worker changed during observation');
  const observation=this.activity(input.worker),now=this.now(),identityAt=Date.parse(observation?.identityObservedAt??'');
  const identityVerified=observation?.identityVerified===true&&observation.generation===before&&Number.isFinite(identityAt)&&identityAt<=now&&now-identityAt<=3000;
  return {worker:input.worker,generation:before,configurationDigest:this.configurationDigest(input.worker),observedAt:now,identityVerified,idle:identityVerified&&coordinationIdle(observation,before,now),observation,grantsAuthority:false};
 }
 /** Refresh coordinator/terminal-duty observations separately from the product frontier.
  * Product owners are prepared individually by preparedDispatch, never a rig-wide barrier. */
 async refreshActivity(rigId:string):Promise<void> {
  const plan=this.plan(rigId);if(!plan)return;
  const a=this.authority.get(rigId);if(!a)return;
  const coordinators=plan.allowIdlePeerTransfer||plan.allowUnavailablePeerTransfer?JSON.parse(a.coordinators) as string[]:[a.owner_session];
  const recipients=[...new Set([...coordinators,'operator-agent@kernel',...this.missingAttributedReturns(rigId).map(r=>r.destination)])];
  await Promise.allSettled(recipients.map(session=>this.refreshDispatchOwner(session)));
 }
 private async refreshDispatchOwner(owner:string):Promise<boolean> {
  if(!this.refreshIdentity||!this.refreshWorkerActivity)return false;
  // Both real producers start together. No SQLite transaction is held across native I/O.
  const results=await Promise.allSettled([
   Promise.resolve().then(()=>this.refreshIdentity!([owner])),
   Promise.resolve().then(()=>this.refreshWorkerActivity!(owner)),
  ]);
  return results.every(result=>result.status==='fulfilled');
 }
 /** Retained claims are excluded from the new-assignment frontier. Their
  * continuation consumer owns fresh identity/activity preparation separately.
  * This returns observation success only; final custody/readiness still decides
  * held versus invalid immediately before staging or the guarded send CAS. */
 async refreshClaimedContinuationOwner(owner:string):Promise<boolean> {
  return this.refreshDispatchOwner(owner);
 }
 private dispatchAuthoritySnapshot(rigId:string):string {const a=this.authority.get(rigId);return JSON.stringify(a?{epoch:a.epoch,holder:a.owner_session,generation:a.owner_generation,state:a.state,leaseUntil:a.lease_until,batonId:a.baton_id}:null);}
 private preparedNativeObservationMatches(prepared:PreparedNativeObservation|null|undefined):boolean {
  if(!prepared)return false;
  const observation=this.activity(prepared.owner),now=this.now();
  const fresh=(at:string)=>{const value=Date.parse(at);return Number.isFinite(value)&&value<=now&&now-value<=3000;};
  return prepared.generation===this.authority.generation(prepared.owner)&&prepared.configurationDigest===this.configurationDigest(prepared.owner)&&
   prepared.identityObservedAt===observation?.identityObservedAt&&prepared.activityObservedAt===observation?.witness?.observedAt&&
   observation?.identityVerified===true&&observation.generation===prepared.generation&&
   observation.witness?.sessionName===prepared.owner&&observation.witness.seatNodeId===observation.state.seatNodeId&&
   observation.witness.rung===observation.state.decidedBy&&observation.witness.activity===observation.state.activity&&
   fresh(prepared.identityObservedAt)&&fresh(prepared.activityObservedAt)&&
   (!observation.state.lastSwap||(observation.state.lastSwap.generation===prepared.generation&&Date.parse(prepared.activityObservedAt)>=Date.parse(observation.state.lastSwap.at)));
 }
 private dispatchObservationMatches(scope:DispatchScope|undefined,t:CoordinationTask,plan:CoordinationPlan):boolean {
  if(!scope)return true; // Synchronous internal compatibility retains its existing cache fences.
  const prepared=scope.get(t.owner);if(!prepared)return false;
  return prepared.owner===t.owner&&prepared.plan===JSON.stringify(plan)&&prepared.authority===this.dispatchAuthoritySnapshot(plan.rigId)&&
   this.preparedNativeObservationMatches(prepared)&&coordinationIdle(this.activity(t.owner),prepared.generation,this.now());
 }
 private dispatchCandidate(rigId:string,plan:CoordinationPlan,t:CoordinationTask):boolean {
  if(t.deadline<=this.now()||t.boundary||this.dispatchScopeHold(plan,t)||!this.predecessorsReady(rigId,t)||!this.admittedNow(t)||this.workerEffectDebt(t.owner)||t.recoveryFor&&!this.recoveryNeeded(rigId,plan,t))return false;
  // Stale telemetry on an otherwise eligible undispatched primary is not a reason
  // to include its dormant backup in the native observation barrier.
  if(t.recoveryFor){const target=plan.tasks.find(other=>other.key===t.recoveryFor)!;
   const assigned=this.db.prepare('SELECT 1 FROM coordinator_assignments WHERE rig_id=? AND package_key=?').get(rigId,target.packageKey);
   if(!assigned&&target.deadline>this.now()&&this.admittedNow(target)&&!this.workerEffectDebt(target.owner)&&this.authority.generation(target.owner))return false;
  }
  if(this.db.prepare("SELECT 1 FROM seat_dispatch_reservations WHERE state!='released' AND (session_name=? OR node_id IN (SELECT node_id FROM sessions WHERE session_name=?))").get(t.owner,t.owner)||this.db.prepare('SELECT 1 FROM seat_delivery_guards WHERE (desired=1 OR effective=1) AND node_id IN (SELECT node_id FROM sessions WHERE session_name=?)').get(t.owner))return false;
  const assigned=this.db.prepare('SELECT a.disposition_id,q.state,q.claimed_by_generation_uuid FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? AND a.package_key=?').get(rigId,t.packageKey) as {disposition_id:string|null;state:string;claimed_by_generation_uuid:string|null}|undefined;
  if(assigned)return assigned.state==='pending'&&!assigned.claimed_by_generation_uuid&&!assigned.disposition_id;
  if(this.repo.getById(`qitem-coordination-${digest(rigId+':'+t.packageKey).slice(0,24)}`))return false;
  return !(this.db.prepare("SELECT qitem_id FROM queue_items WHERE destination_session IN (?,?) AND state IN ('pending','in-progress','blocked')").all(...rotationLocalAddresses(this.db,t.owner)) as Array<{qitem_id:string}>).some(row=>this.genericWatchActionable(row.qitem_id,rigId));
 }
 private async preparedDispatch(rigId:string,reconcile:(scope:DispatchScope)=>CoordinationResult[]|null):Promise<CoordinationResult[]> {
  const plan=this.plan(rigId);if(!plan)return [];
  const candidates=plan.tasks.filter(t=>this.dispatchCandidate(rigId,plan,t));
  const owners=[...new Set(candidates.map(t=>t.owner))];
  const planSnapshot=JSON.stringify(plan),authoritySnapshot=this.dispatchAuthoritySnapshot(rigId);
  const stages=owners.map(()=>'observation');
  const results=await Promise.allSettled(owners.map(async (owner,index)=>{
   const targets=candidates.filter(t=>t.owner===owner&&t.recoveryFor).map(t=>plan.tasks.find(other=>other.key===t.recoveryFor)!);
   const recipients=[...new Set([owner,...targets.map(t=>t.owner)])];
   const bindings=recipients.map(session=>({owner:session,generation:this.authority.generation(session),configurationDigest:this.configurationDigest(session)}));
   // Co-observe the exact primary so fresh working evidence can suppress recovery.
   // Target observation is not recovery eligibility: unchanged recoveryNeeded owns
   // status/deadline/debt/admission predicates, even when the target probe fails.
   const refreshed=await Promise.all(bindings.map(binding=>this.refreshDispatchOwner(binding.owner)));
   const observations=new Map(bindings.map((binding,i)=>{
    const sample=this.activity(binding.owner);
    const observation:PreparedNativeObservation|null=refreshed[i]&&binding.generation&&binding.configurationDigest?{owner:binding.owner,generation:binding.generation,configurationDigest:binding.configurationDigest,identityObservedAt:sample?.identityObservedAt??'',activityObservedAt:sample?.witness?.observedAt??''}:null;
    return [binding.owner,observation] as const;
   }));
   const native=observations.get(owner),prepared:PreparedDispatchObservation|null=native?{...native,plan:planSnapshot,authority:authoritySnapshot,recoveryTargets:Object.fromEntries(targets.map(target=>[target.key,observations.get(target.owner)??null]))}:null;
   // Each owner's completion immediately reaches the existing synchronous fences.
   // Another owner's scoped failure cannot cancel or leave these passes unawaited.
   stages[index]='scoped-reconcile';reconcile(new Map([[owner,prepared]]));
   stages[index]='committed-delivery';await this.deliverCommitted();
  }));
  const failures:CoordinationResult[]=[];
  results.forEach((result,index)=>{
   if(result.status!=='rejected')return;
   const owner=owners[index]!,reason=result.reason instanceof CoordinatorFenceError?result.reason.code:'coordination_dispatch_failed';
   const held:CoordinationResult={key:'dispatch:'+owner,state:'held',reason,deadline:Math.min(...candidates.filter(t=>t.owner===owner).map(t=>t.deadline)),activityEvidence:{stage:stages[index],owner,planRevision:plan.revision,authoritySnapshot}};
   // Report the failure durably and in the aggregate response, never as successful
   // dispatch or an implied retry. Original queue/outbox effects remain untouched.
   this.db.prepare('INSERT OR IGNORE INTO coordinator_operations VALUES (?,?,?,?,?)').run(rigId,'coordination-dispatch-hold:'+digest(JSON.stringify(held)), 'coordination-dispatch-hold',JSON.stringify(held),digest(JSON.stringify({plan:planSnapshot,authority:authoritySnapshot,owner})));
   failures.push(held);
  });
  return failures;
 }
 /** Transport callers carry no proof. Only this service creates preparation records. */
 async reconcilePrepared(actor:string,generation:string,rigId:string):Promise<CoordinationResult[]> {
  const a=this.authority.get(rigId),plan=this.plan(rigId);
  if(!a||!plan)fail('coordination_plan_required','Explicit current recovery plan required');
  if(this.authority.generation('operator-agent@kernel')!==plan!.operatorGeneration)fail('coordination_operator_retired','Reauthorize plan after Operator generation change');
  if(actor!==a!.owner_session||generation!==a!.owner_generation||this.authority.generation(actor)!==generation||a!.state!=='active'||a!.lease_until<=this.now())fail('coordinator_retired','Only reconciled current holder may dispatch');
  const [,failures]=await Promise.all([this.refreshActivity(rigId),this.preparedDispatch(rigId,scope=>this.reconcileScoped(actor,generation,rigId,scope))]);
  const continuationResults=[...await this.stageRecoveredClaimedContinuations(actor,generation,rigId),...await this.stageOwnedOutcomeContinuations(actor,generation,rigId)];
  // Aggregate administration/status may stage its existing duties, but cannot dispatch
  // another owner from cached evidence or re-use a preparation after asynchronous waits.
  return [...this.reconcileScoped(actor,generation,rigId,new Map()),...failures,...continuationResults];
 }
 /** Acceptance is durable before native preparation. Observer failure cannot turn an
  * accepted return into a rejected/ambiguous reaccept request. */
 async acceptPrepared(actor:string,generation:string,rigId:string,packageKey:string,dispositionId:string,evidenceRef:string):Promise<{ok:true;accepted:true;dispatch?:CoordinationResult[];dispatchError?:{code:string;message:string}}> {
  this.commitAcceptance(actor,generation,rigId,packageKey,dispositionId,evidenceRef);
  try {const dispatch=await this.reconcilePrepared(actor,generation,rigId);await this.deliverCommitted();return {ok:true,accepted:true,dispatch};}
  catch(error){return {ok:true,accepted:true,dispatchError:{code:error instanceof CoordinatorFenceError?error.code:'coordination_dispatch_observation_failed',message:'Acceptance is committed; subsequent dispatch remains held for supported reconciliation.'}};}
 }
 /** Registered supervision uses the same per-owner boundary while separately observing
  * actual coordinator/terminal duties. Its final pass preserves all recovery/peer guards. */
 async supervisePrepared(rigId:string,jobId:string):Promise<CoordinationResult[]|null> {
  const plan=this.plan(rigId),a=this.authority.get(rigId);
  if(!plan||!a)return this.superviseScoped(rigId,jobId,new Map());
  this.assertCoordinationObserver(rigId,jobId,plan);
  const dutyObservations=this.refreshActivity(rigId);
  const dispatch=a.state==='active'&&a.lease_until>this.now()?this.preparedDispatch(rigId,scope=>this.superviseScoped(rigId,jobId,scope)):Promise.resolve([] as CoordinationResult[]);
  const [,failures]=await Promise.all([dutyObservations,dispatch]);
  // The registered observer must continue retained claims as well as stage new
  // assignments. Re-read the holder after native observation; the continuation
  // path independently fences current authority, admission, custody and effects.
  const current=this.authority.get(rigId);
  const continuations=current?[...await this.stageRecoveredClaimedContinuations(current.owner_session,current.owner_generation,rigId),...await this.stageOwnedOutcomeContinuations(current.owner_session,current.owner_generation,rigId)]:[];
  const final=this.superviseScoped(rigId,jobId,new Map());
  return final?[...final,...failures,...continuations]:failures.length||continuations.length?[...failures,...continuations]:null;
 }
 private get authority(){return this.repo.coordinatorAuthority;}
 private get db(){return this.authority.db;}
 private assertQualificationStageOperator(actor:string,generation:string,rigId:string):void {
  const authority=this.authority.get(rigId),now=this.now();
  if(actor!=='operator-agent@kernel'||!generation||this.authority.generation(actor)!==generation||!authority||authority.state!=='active'||authority.lease_until<=now||!authority.owner_session||authority.owner_generation!==this.authority.generation(authority.owner_session))fail('qualification_duty_operator_required','Current native Operator and active coordinator holder required');
 }
 /** Refresh the native identity and activity snapshot immediately before a synchronous
  * qualification-stage transaction. This does not prompt the Worker or grant authority. */
 async prepareQualificationWorkerStageObservation(actor:string,generation:string,rigId:string,worker:string,expectedGeneration?:string,expectedConfigurationDigest?:string):Promise<QualificationWorkerStageObservation> {
  this.assertQualificationStageOperator(actor,generation,rigId);
  const member=!!this.db.prepare('SELECT 1 FROM sessions s JOIN nodes n ON n.id=s.node_id WHERE s.session_name=? AND n.rig_id=?').get(worker,rigId),beforeGeneration=this.authority.generation(worker),beforeConfiguration=this.configurationDigest(worker);
  if(!member||!beforeGeneration||!beforeConfiguration||expectedGeneration&&expectedGeneration!==beforeGeneration||expectedConfigurationDigest&&expectedConfigurationDigest!==beforeConfiguration)fail('qualification_duty_worker_stale','Exact current Worker generation and configuration digest required');
  if(!this.refreshIdentity||!this.refreshWorkerActivity)fail('qualification_duty_worker_not_quiescent','Fresh native Worker identity and activity observers are required');
  const refreshIdentity=this.refreshIdentity!,refreshWorkerActivity=this.refreshWorkerActivity!;
  // Start both native observers together: serial probes can age the first witness beyond the 3s fence.
  try { await Promise.all([refreshIdentity([worker]),refreshWorkerActivity(worker)]); }
  catch { fail('qualification_duty_worker_not_quiescent','Fresh native Worker identity and activity observation could not be obtained'); }
  this.assertQualificationStageOperator(actor,generation,rigId);
  const currentGeneration=this.authority.generation(worker),currentConfiguration=this.configurationDigest(worker);
  if(!currentGeneration||!currentConfiguration||currentGeneration!==beforeGeneration||currentConfiguration!==beforeConfiguration||expectedGeneration&&currentGeneration!==expectedGeneration||expectedConfigurationDigest&&currentConfiguration!==expectedConfigurationDigest)fail('qualification_duty_worker_stale','Worker generation or configuration changed during fresh observation');
  const observation=this.activity(worker),now=this.now(),identityObservedAt=observation?.identityObservedAt??'',activityObservedAt=observation?.witness?.observedAt??'',identityAt=Date.parse(identityObservedAt),activityAt=Date.parse(activityObservedAt);
  if(!observation?.identityVerified||observation.generation!==currentGeneration||!identityObservedAt||!Number.isFinite(identityAt)||identityAt>now||now-identityAt>3000||!activityObservedAt||!Number.isFinite(activityAt)||activityAt>now||now-activityAt>3000||!coordinationIdle(observation,currentGeneration,now))fail('qualification_duty_worker_not_quiescent','Fresh current native idle Worker identity and activity required');
  return {worker,generation:currentGeneration!,configurationDigest:currentConfiguration!,identityObservedAt,activityObservedAt};
 }
 async prepareQualificationRetirementStageObservation(actor:string,generation:string,input:{rigId:string;targetQueueId:string}):Promise<QualificationWorkerStageObservation> {
  const target=this.repo.getById(input.targetQueueId);
  const worker=target?.destinationSession;
  if(!worker)fail('qualification_retirement_target_invalid','Exact legacy qualification assessment row required');
  return this.prepareQualificationWorkerStageObservation(actor,generation,input.rigId,worker!);
 }
 private preparedQualificationObservationMatches(prepared:QualificationWorkerStageObservation|undefined,worker:string,generation:string,now:number,observation:CoordinationActivity|null):boolean {
  if(!prepared)return true; // Direct service tests/legacy internal callers still receive the synchronous fail-closed cache checks below.
  const identityAt=Date.parse(prepared.identityObservedAt),activityAt=Date.parse(prepared.activityObservedAt);
  return prepared.worker===worker&&prepared.generation===generation&&prepared.configurationDigest===this.configurationDigest(worker)&&prepared.identityObservedAt===observation?.identityObservedAt&&prepared.activityObservedAt===observation?.witness?.observedAt&&Number.isFinite(identityAt)&&identityAt<=now&&now-identityAt<=3000&&Number.isFinite(activityAt)&&activityAt<=now&&now-activityAt<=3000;
 }
 configure(actor:string,generation:string,plan:CoordinationPlan):CoordinationPlan {
  return this.db.transaction(()=>{
   if(actor!=="operator-agent@kernel"||this.authority.generation(actor)!==generation||plan.operatorGeneration!==generation)fail("coordination_operator_required","Current genuine Operator configures recovery");
   if(!this.authority.get(plan.rigId))fail("coordinator_not_enabled","Explicit legacy enrollment/admission required");
   const prior=this.plan(plan.rigId);
   const executionShape=(p:CoordinationPlan)=>JSON.stringify({...p,revision:undefined,operatorGeneration:undefined,scopeSources:undefined,frontierPlanning:undefined});
   // Attaching scope is not renewed task admission or checkpoint release.
   const scopeOnly=!!prior&&executionShape(prior)===executionShape(plan)&&JSON.stringify([prior.scopeSources,prior.frontierPlanning])!==JSON.stringify([plan.scopeSources,plan.frontierPlanning]);

   if(!Number.isSafeInteger(plan.stallMs)||plan.stallMs<10000||plan.stallMs>3600000||typeof plan.allowIdlePeerTransfer!=="boolean"||!plan.revision||!plan.tasks.length||new Set(plan.tasks.map(t=>t.key)).size!==plan.tasks.length||new Set(plan.tasks.map(t=>t.packageKey)).size!==plan.tasks.length)fail("coordination_invalid_plan","Unique immutable tasks/packages required");
   if(plan.acknowledgmentWindowMs!==undefined&&(!Number.isSafeInteger(plan.acknowledgmentWindowMs)||plan.acknowledgmentWindowMs<10000||plan.acknowledgmentWindowMs>900000))fail("coordination_invalid_ack_window","Acknowledgment window must be 10 seconds to 15 minutes");
   if(plan.allowUnavailablePeerTransfer!==undefined&&typeof plan.allowUnavailablePeerTransfer!=='boolean')fail('coordination_invalid_unavailable_optin','Unavailable-owner transfer requires strict explicit boolean');
   if(plan.refreshDispatchIdentity!==undefined&&typeof plan.refreshDispatchIdentity!=='boolean')fail('coordination_invalid_identity_refresh','Identity refresh requires strict explicit boolean');
   if(plan.dispatchRestrictions!==undefined&&!scopeOnly){
    if(!Array.isArray(plan.dispatchRestrictions)||new Set(plan.dispatchRestrictions.map(r=>r.session)).size!==plan.dispatchRestrictions.length)fail('coordination_invalid_dispatch_scope','Unique explicit dispatch restrictions required');
    // A restriction reproduced BYTE-IDENTICALLY from the prior plan is retained history,
    // not a renewed scope. It may keep its elapsed validUntil so an admission-only successor
    // can refresh a task's own evidence while the scope stay expired and undispatchable;
    // dispatchScopeHold() still returns dispatch-scope-expired for it, so nothing dispatches.
    // Anything changed, removed or extended is a NEW disposition and keeps the full gate.
    const retainedRestrictions=new Set((prior?.dispatchRestrictions??[]).map(r=>JSON.stringify(r)));
    for(const r of plan.dispatchRestrictions){
     if(r.checkpointDisposition!==undefined&&r.checkpointDisposition!=='release-listed-packages')fail('coordination_invalid_checkpoint_disposition','Explicit listed-package checkpoint disposition required');
     const retained=retainedRestrictions.has(JSON.stringify(r));
     if(!r.session||r.generation!==this.authority.generation(r.session)||!Array.isArray(r.packageKeys)||!r.packageKeys.length||new Set(r.packageKeys).size!==r.packageKeys.length||r.packageKeys.some(key=>!plan.tasks.some(t=>t.owner===r.session&&t.packageKey===key))||!Number.isFinite(r.validUntil)||(!retained&&r.validUntil<=this.now())||typeof r.evidenceRef!=='string'||!r.evidenceRef.trim())fail('coordination_invalid_dispatch_scope','Exact current owner, admitted packages, future expiry and evidence required');
    }
   }
  if(plan.scopeSources!==undefined&&(!Array.isArray(plan.scopeSources)||new Set(plan.scopeSources.map(s=>s?.ref)).size!==plan.scopeSources.length||plan.scopeSources.some(s=>!s||typeof s.ref!=='string'||!s.ref.trim()||typeof s.digest!=='string'||!/^[0-9a-f]{64}$/.test(s.digest))))fail('coordination_invalid_scope_sources','Unique scope refs with exact sha256 digests required');
  if(plan.frontierPlanning!==undefined&&(plan.frontierPlanning===null||typeof plan.frontierPlanning!=='object'||(plan.frontierPlanning.stabilizationObservations!==undefined&&(!Number.isSafeInteger(plan.frontierPlanning.stabilizationObservations)||plan.frontierPlanning.stabilizationObservations<1||plan.frontierPlanning.stabilizationObservations>64))||(plan.frontierPlanning.stabilizationMs!==undefined&&(!Number.isSafeInteger(plan.frontierPlanning.stabilizationMs)||plan.frontierPlanning.stabilizationMs<0||plan.frontierPlanning.stabilizationMs>3600000))))fail('coordination_invalid_frontier_planning','Bounded explicit stabilization configuration required');
   const stable=(t:CoordinationTask)=>JSON.stringify({...t,admission:undefined,deadline:undefined});
   const keys=new Set(plan.tasks.map(t=>t.key));
   for(const t of plan.tasks){
    const old=prior?.tasks.find(previous=>previous.key===t.key),historical=!!old&&(this.acceptedTaskHistory(plan.rigId,old)||this.dormantRecoveryHistory(plan.rigId,old,prior!));
    if(historical&&JSON.stringify(old)!==JSON.stringify(t))fail('coordination_history_rewrite_refused','Accepted task and dormant backup history must retain full task, admission and deadline bytes');
    // A task byte-identical to the same-key task in the IMMEDIATELY prior plan is RETAINED, not
    // re-admitted: it keeps its own admission bytes and this writes nothing. No admission field is
    // changed, no package/queue/claim/authority write and no wake occurs, so no credit is created.
    // reconcile()'s admittedNow() and the held reason 'current-admission-required' remain the sole
    // activation gates, and any later byte change makes it a changed task under the full gate.
    const retainedExact=!!old&&JSON.stringify(old)===JSON.stringify(t);
    if(!t.key||!t.action.trim()||!Number.isFinite(t.deadline)||(t.deadline<=this.now()&&!prior?.tasks.some(old=>old.key===t.key&&stable(old)===stable(t)))||!t.body||!Array.isArray(t.predecessors))fail("coordination_invalid_task","Concrete action, future deadline, predecessors and exact body required");
    const assignedPrior=prior?.tasks.find(previous=>previous.packageKey===t.packageKey);
    if(assignedPrior&&JSON.stringify(assignedPrior.predecessors)!==JSON.stringify(t.predecessors)&&this.db.prepare('SELECT 1 FROM coordinator_assignments WHERE rig_id=? AND package_key=?').get(plan.rigId,t.packageKey))fail('coordination_invalid_predecessor','Predecessor references cannot change after assignment');
    const ad=t.admission;
    if(!validRuntimeRequirements(ad?.runtimeRequirements))fail("coordination_invalid_runtime_requirements","Runtime requirements must bind a native model and/or nonnegative context token floor");
    if(!historical&&!scopeOnly&&!retainedExact&&(!ad||ad.generation!==this.authority.generation(t.owner)||ad.configurationDigest!==this.configurationDigest(t.owner)||!ad.qualificationRef||!ad.capacityRef||!ad.effortRef||!Number.isFinite(ad.validUntil)||ad.validUntil<=this.now()))fail('coordination_current_admission_required','Exact current generation/configuration, qualification/capacity/effort evidence and expiry required');
    if(t.boundary&&!['owner-access','owner-credential','owner-material','owner-irreversible'].includes(t.boundary))fail("coordination_invalid_boundary","Unknown boundary");
    const row=this.db.prepare("SELECT contract FROM coordinator_packages WHERE rig_id=? AND package_key=?").get(plan.rigId,t.packageKey) as {contract:string}|undefined;
    if(!row)fail("coordination_package_not_admitted","Every task including recovery needs explicit admission");
    const c=JSON.parse(row!.contract);
    if(c.destination!==t.owner||c.bodyHash!==digest(t.body)||!c.returnContract?.evidenceRequired?.length)fail("coordination_package_mismatch","Exact owner/body and attributed return contract required");
    for(const pred of t.predecessors){
     if(!pred||typeof pred!=='object'||!pred.queueId)fail("coordination_invalid_predecessor","Exact disposition receipt required");
     if(isBoundPredecessor(pred))this.validateBoundPredecessor(plan,t,pred,old);
     else if(Object.keys(pred).sort().join(',')!=='dispositionId,queueId'||!pred.dispositionId)fail("coordination_invalid_predecessor","Exact disposition receipt required");
    }
    if(t.recoveryFor&&(!keys.has(t.recoveryFor)||t.recoveryFor===t.key))fail("coordination_invalid_recovery","Recovery must name another exact plan task");
    if(!t.recoveryFor&&!t.boundary&&!plan.tasks.some(r=>r.recoveryFor===t.key))fail("coordination_recovery_required","Every ordinary task needs a distinct admitted recovery task with concrete owner/action/deadline");
   }
   const byQueue=new Map(plan.tasks.map(t=>['qitem-coordination-'+digest(plan.rigId+':'+t.packageKey).slice(0,24),t]));
   const byPackage=new Map(plan.tasks.map(t=>[t.packageKey,t]));
   const parentOf=(pred:CoordinationPredecessor)=>isBoundPredecessor(pred)?byPackage.get(pred.packageKey):byQueue.get(pred.queueId);
   const visiting=new Set<string>(),visited=new Set<string>();
   const visit=(t:CoordinationTask):void=>{if(visiting.has(t.key))fail('coordination_dependency_cycle','Recovery/dependency graph cannot cycle');if(visited.has(t.key))return;visiting.add(t.key);for(const pred of t.predecessors){const parent=parentOf(pred);if(parent)visit(parent);}if(t.recoveryFor)visit(plan.tasks.find(other=>other.key===t.recoveryFor)!);visiting.delete(t.key);visited.add(t.key);};
   for(const t of plan.tasks)visit(t);
   const reaches=(t:CoordinationTask,target:string,seen=new Set<string>()):boolean=>{
    if(t.key===target)return true;if(seen.has(t.key))return false;seen.add(t.key);
    return t.predecessors.some(pred=>{const parent=parentOf(pred);return !!parent&&reaches(parent,target,seen);});
   };
   for(const t of plan.tasks)if(t.recoveryFor&&reaches(t,t.recoveryFor))fail('coordination_recovery_deadlock','Recovery cannot depend directly or transitively on its blocked task');
   const id=`coordination-plan:${plan.revision}`;
   const old=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=?").get(plan.rigId,id) as {receipt:string}|undefined;
   if(old){if(old.receipt!==JSON.stringify(plan))fail("coordination_plan_conflict","Frozen revision cannot change");return JSON.parse(old.receipt);}
   // Do not replace unresolved contracts with a new plan and silently orphan work.
   if(prior&&prior.tasks.some(t=>!plan.tasks.some(n=>n.key===t.key&&stable(n)===stable(t))))fail("coordination_plan_obligation_lost","Retain all existing tasks unchanged in successor revision");
   this.db.prepare("INSERT INTO coordinator_operations VALUES (?,?,?,?,?)").run(plan.rigId,id,"coordination-plan",JSON.stringify(plan),digest(JSON.stringify({actor,generation,plan})));
   for(const r of scopeOnly?[]:plan.dispatchRestrictions??[]){
    if(r.checkpointDisposition!=='release-listed-packages')continue;
    const queueId='qitem-coordination-scope-'+digest(plan.rigId+':'+generation+':'+JSON.stringify(r)).slice(0,24);
    if(!this.repo.getById(queueId)){this.repo.createWithinTransaction({qitemId:queueId,sourceSession:actor,destinationSession:r.session,expiresAt:new Date(r.validUntil).toISOString(),body:JSON.stringify({action:'checkpoint-scope-disposition',operator:actor,operatorGeneration:generation,rigId:plan.rigId,recipientGeneration:r.generation,packageKeys:r.packageKeys,evidenceRef:r.evidenceRef,validUntil:r.validUntil,instruction:'Current genuine Operator releases post-checkpoint quiescence ONLY for the listed already-admitted packages under the cited disposition. Rederive actual native identity/generation and read the disposition; claim and close this control notice honestly, then consume the existing matching assignment when present. Preserve quiescence for all other work, rotation and baton takeover. No duplicate assignment, source edit, historical-effect replay or acceptance waiver. This notice is not worker pickup or technical acceptance.'}),identityProvenance:'system:operator-authorized-coordination',nudge:true});
     this.repo.stageWakeIntent(queueId,actor,r.session,'system:operator-authorized-coordination',true,r.generation);
    }
   }
   // Exact keyed capture only. Every completing path now freezes its own duty at apply time, so the rig-wide history scan is gone.
   this.captureConfigureCompletions(plan,prior);
   this.recordProgress(plan.rigId);
   return plan;
  }).immediate();
 }
 plan(rigId:string):CoordinationPlan|null {
  const rows=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='coordination-plan' ORDER BY rowid DESC LIMIT 1").get(rigId) as {receipt:string}|undefined;
  return rows?JSON.parse(rows.receipt):null;
 }
 /** The whole frontier integration surface. Every frontier decision lives in
  * `frontier-planning.ts`; this adapter only binds it to the shared facets. */
 private frontierPlanner?:FrontierPlanning;
 frontierPlanning():FrontierPlanning {
  if(!this.frontierPlanner)this.frontierPlanner=new FrontierPlanning({db:this.db,now:()=>this.now(),generation:s=>this.authority.generation(s),authorityRecord:rigId=>this.authority.get(rigId)??null,plan:rigId=>this.plan(rigId),lifecycleControlCompleted:queueId=>this.lifecycleControlCompleted(queueId),actAllowed:(queueId,actor,generation)=>this.frontierDutyActAllowed(queueId,actor,generation),issueLifecycleDuty:input=>this.lifecycleDuty(input.rigId,input.kind,input.packageKey,input.recipient,input.recipientGeneration,input.semanticKey,input.details),admittedNow:t=>this.admittedNow(t),dispatchScopeHold:(plan,t)=>this.dispatchScopeHold(plan,t),effectDebt:session=>this.workerEffectDebt(session),requiresRecovery:(rigId,packageKey)=>this.authority.runtimeOutcomeAssessment?.requiresRecovery(rigId,packageKey)??false,admitPackage:(actor,generation,rigId,packageKey,contract)=>this.authority.admit(actor,generation,rigId,packageKey,contract),observeCompletion:queueId=>this.observeLifecycleCompletion(queueId)});
  return this.frontierPlanner;
 }
 /** The shared Act facet itself: exact recipient identity and generation, a genuine
  *  transport:v1 claim, and dutyFacts().act. Never the send predicate. */
 private frontierDutyActAllowed(queueId:string,actor:string,generation:string):boolean {
  return this.lifecycleControlActAllowed(queueId,actor,generation,'transport:v1')&&this.dutyFacts(queueId).act;
 }
 /** Genuine current Lead records its one typed planning disposition. */
 recordFrontierPlan(actor:string,generation:string,input:{rigId:string;dutyQueueId:string;frontierDigest:string;disposition:string;proposal?:unknown;mapping?:unknown;boundary?:unknown;unblockCondition?:unknown}):FrontierPlanReceipt {return this.frontierPlanning().recordFrontierPlan(actor,generation,input);}
 /** Genuine current Operator registers the cited proposal or refuses it once. */
 admitFrontierProposal(actor:string,generation:string,input:{rigId:string;dutyQueueId:string;proposalDigest:string;admitted?:unknown;declined?:unknown}):FrontierAdmissionReceipt {return this.frontierPlanning().admitFrontierProposal(actor,generation,input);}
 /** Genuine current Operator independently confirms the Lead's recorded completion. */
 recordFrontierConfirmation(actor:string,generation:string,input:{rigId:string;dutyQueueId:string;completionDigest:string;evidenceRef:string}):FrontierConfirmationReceipt {return this.frontierPlanning().recordFrontierConfirmation(actor,generation,input);}
 /** Genuine current Operator records that a recorded blocked or declined disposition is discharged. */
 recordFrontierReopen(actor:string,generation:string,input:{rigId:string;dutyQueueId:string;dispositionDigest:string;evidenceRef:string}):FrontierReopenReceipt {return this.frontierPlanning().recordFrontierReopen(actor,generation,input);}
 /** Genuine current Operator attests ONE exact legacy package as administrative or inquiry work (S1). */
 recordFrontierLegacyClassification(actor:string,generation:string,input:LegacyClassificationInput):LegacyClassificationReceipt {return this.frontierPlanning().recordFrontierLegacyClassification(actor,generation,input);}
 /** Explicit supersession of an attestation; history is append-only. */
 revokeFrontierLegacyClassification(actor:string,generation:string,input:LegacyRevocationInput):LegacyRevocationReceipt {return this.frontierPlanning().revokeFrontierLegacyClassification(actor,generation,input);}
 /** Read-only frontier census for the current Operator. Never a finding of completeness. */
 frontierProjection(rigId:string):FrontierSnapshot|null {return this.frontierPlanning().frontier(rigId);}
 reconcile(actor:string,generation:string,rigId:string):CoordinationResult[] {return this.reconcileScoped(actor,generation,rigId);}
 private nativeRecoveryObservationMatches(rigId:string,owner:string,generation:string,observation:NativeRecoveryObservation,requireFreshObservation=false):boolean {
  const c=observation?.completion,now=this.now(),observedAt=observation?.observedAt;
  if(!c||c.schema!=='native-recovery-completion.v1'||c.rigId!==rigId||c.sessionName!==owner||c.generation!==generation||!c.sessionId||!c.nodeId||!c.nativeIdentityHash||!c.configurationDigest||!c.source?.ref||!/^([a-f0-9]{64})$/.test(c.source.digest)||!c.incarnation?.key||!Number.isSafeInteger(c.incarnation.native?.pid)||c.incarnation.native.pid<=0||!c.incarnation.native.startFingerprint||c.custodyPreserved!==true||c.generationUnchanged!==true||!Number.isFinite(c.completedAt)||c.completedAt>now||!Number.isFinite(observedAt)||observedAt>now||(requireFreshObservation&&now-observedAt>3000))return false;
  const rows=this.db.prepare('SELECT s.id,s.node_id,s.session_name,n.rig_id FROM sessions s JOIN nodes n ON n.id=s.node_id WHERE s.session_name=?').all(owner) as Array<{id:string;node_id:string;session_name:string;rig_id:string}>;
  // Retained native history can contain several rows for this session name.
  // Bind the completion to its exact session row, then prove the current node,
  // rig, occupant tenure and runtime configuration through live bindings below.
  if(!rows.some(row=>row.id===c.sessionId&&row.node_id===c.nodeId&&row.rig_id===rigId)||this.authority.generation(owner)!==generation||this.configurationDigest(owner)!==c.configurationDigest)return false;
  const target=resolveGuardTarget(this.db,owner);
  return !!target&&target.nodeId===c.nodeId&&target.session===owner&&target.occupant===generation;
 }
 private claimedContinuationReady(input:{rigId:string;task:CoordinationTask;queueId:string;holderSession:string;holderGeneration:string;epoch:number;proof?:ClaimedRecoveryContinuationProof;outboxId?:string}):'ready'|'held'|'invalid' {
  const {rigId,task:t,queueId,holderSession,holderGeneration,epoch,proof,outboxId}=input,a=this.authority.get(rigId),plan=this.plan(rigId);
  if(!a||!plan||plan.operatorGeneration!==this.authority.generation('operator-agent@kernel')||a.owner_session!==holderSession||a.owner_generation!==holderGeneration||a.epoch!==epoch||this.authority.generation(holderSession)!==holderGeneration||this.authority.generation(t.owner)!==t.admission.generation||t.boundary||t.recoveryFor)return 'invalid';
  if(t.admission.configurationDigest!==this.configurationDigest(t.owner))return 'invalid';
  // Admission expiry and temporary plan holds withdraw permission to send; they
  // do not destroy an unattempted notice for an unchanged claim/incarnation.
  if(a.state!=='active'||a.lease_until<=this.now()||t.deadline<=this.now()||this.dispatchScopeHold(plan,t)||!this.predecessorsReady(rigId,t)||!this.admittedNow(t))return 'held';
  if(this.runtimeReadiness?.(t)||this.workerEffectDebt(t.owner,outboxId))return 'held';
  const row=this.db.prepare('SELECT a.queue_id,a.package_key,a.destination,a.body_hash,a.disposition_id,q.body,q.state,q.claimed_at,q.claimed_by_generation_uuid,q.reply_to,p.contract_hash,p.contract FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id JOIN coordinator_packages p ON p.rig_id=a.rig_id AND p.package_key=a.package_key WHERE a.rig_id=? AND a.queue_id=? AND a.package_key=?').get(rigId,queueId,t.packageKey) as {queue_id:string;package_key:string;destination:string;body_hash:string;disposition_id:string|null;body:string;state:string;claimed_at:string|null;claimed_by_generation_uuid:string|null;reply_to:string|null;contract_hash:string;contract:string}|undefined;
  if(!row||row.queue_id!==queueId||row.package_key!==t.packageKey||row.destination!==t.owner||row.state!=='in-progress'||row.claimed_by_generation_uuid!==t.admission.generation||!row.claimed_at||row.reply_to!==null||row.disposition_id||row.body!==t.body||digest(row.body)!==row.body_hash||row.body_hash!==digest(t.body)||JSON.parse(row.contract).destination!==t.owner||JSON.parse(row.contract).bodyHash!==row.body_hash||this.db.prepare('SELECT 1 FROM queue_items WHERE reply_to=? LIMIT 1').get(queueId))return 'invalid';
  if(this.db.prepare("SELECT 1 FROM seat_dispatch_reservations WHERE state!='released' AND (session_name=? OR node_id IN (SELECT node_id FROM sessions WHERE session_name=?))").get(t.owner,t.owner))return 'held';
  const target=resolveGuardTarget(this.db,t.owner);
  if(!target)return 'held';
  if(target.session!==t.owner||target.occupant!==t.admission.generation)return 'invalid';
  if(!this.db.prepare('SELECT 1 FROM seat_delivery_guards WHERE node_id=? AND desired=0 AND effective=0').get(target.nodeId))return 'held';
  if(!coordinationIdle(this.activity(t.owner),t.admission.generation,this.now()))return 'held';
  if(proof){
   // Frozen revision/admission hashes audit staging. Delivery uses the current
   // authorized admission above; immutable scope, dependencies and custody stay pinned.
   if(proof.rigId!==rigId||proof.queueId!==queueId||proof.packageKey!==t.packageKey||proof.contractHash!==row.contract_hash||proof.bodyHash!==row.body_hash||proof.claimantGeneration!==row.claimed_by_generation_uuid||proof.holderSession!==holderSession||proof.holderGeneration!==holderGeneration||proof.epoch!==epoch||proof.predecessorsHash!==digest(JSON.stringify(t.predecessors))||outboxId!==claimedRecoveryContinuationOutboxId(proof))return 'invalid';
   const observation=proof.observation,completion=observation?.completion;
   // This observation is immutable recovery provenance, not a lease on future
   // deliveries. The async native guard supplies fresh current-incarnation proof
   // on every drain; final DB checks here bind the retained completion to state.
   if(!Number.isFinite(observation?.observedAt)||observation.observedAt>this.now())return 'invalid';
   if(!completion||completion.sessionName!==t.owner||completion.generation!==t.admission.generation)return 'invalid';
   if(!this.nativeRecoveryObservationMatches(rigId,t.owner,t.admission.generation,observation))return 'invalid';
  }
  return 'ready';
 }
 /** Called only by the async prepared reconciliation after current identity/activity refresh. */
 private async stageRecoveredClaimedContinuations(actor:string,generation:string,rigId:string):Promise<CoordinationResult[]> {
  const runtime=this.nativeRecoveryContinuation;if(!runtime)return [];
  const authority=this.authority.get(rigId),plan=this.plan(rigId);
  if(!authority||!plan||authority.owner_session!==actor||authority.owner_generation!==generation||authority.state!=='active'||authority.lease_until<=this.now()||plan.operatorGeneration!==this.authority.generation('operator-agent@kernel'))return [];
  const results:CoordinationResult[]=[];
  for(const task of plan.tasks){
   const assigned=this.db.prepare('SELECT queue_id,state,claimed_by_generation_uuid,destination_session FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? AND a.package_key=?').get(rigId,task.packageKey) as {queue_id:string;state:string;claimed_by_generation_uuid:string|null;destination_session:string}|undefined;
   if(!assigned||assigned.state!=='in-progress'||assigned.claimed_by_generation_uuid!==task.admission.generation||assigned.destination_session!==task.owner)continue;
   if(!await this.refreshClaimedContinuationOwner(task.owner))continue;
   const holder=this.authority.get(rigId);if(!holder)continue;
   if(this.claimedContinuationReady({rigId,task,queueId:assigned.queue_id,holderSession:actor,holderGeneration:generation,epoch:holder.epoch})!=='ready')continue;
   let observation:NativeRecoveryObservation|null=null;try{observation=await runtime.observeRecoveredIncarnation(task.owner);}catch{continue;}
   if(!observation||!this.nativeRecoveryObservationMatches(rigId,task.owner,task.admission.generation,observation,true))continue;
   const currentPlan=this.plan(rigId),currentAuthority=this.authority.get(rigId),currentTask=currentPlan?.tasks.find(t=>t.key===task.key);
   if(!currentPlan||!currentAuthority||!currentTask||!this.nativeRecoveryObservationMatches(rigId,currentTask.owner,currentTask.admission.generation,observation,true)||this.claimedContinuationReady({rigId,task:currentTask,queueId:assigned.queue_id,holderSession:actor,holderGeneration:generation,epoch:currentAuthority.epoch})!=='ready')continue;
   const row=this.db.prepare('SELECT p.contract_hash,a.body_hash FROM coordinator_assignments a JOIN coordinator_packages p ON p.rig_id=a.rig_id AND p.package_key=a.package_key WHERE a.rig_id=? AND a.package_key=? AND a.queue_id=?').get(rigId,task.packageKey,assigned.queue_id) as {contract_hash:string;body_hash:string}|undefined;
   if(!row)continue;
   const proof:ClaimedRecoveryContinuationProof={schema:'claimed-native-recovery-continuation.v1',rigId,queueId:assigned.queue_id,packageKey:task.packageKey,contractHash:row.contract_hash,bodyHash:row.body_hash,claimantGeneration:task.admission.generation,holderSession:actor,holderGeneration:generation,epoch:currentAuthority.epoch,planRevision:currentPlan.revision,admissionHash:digest(JSON.stringify({deadline:currentTask.deadline,admission:currentTask.admission,dispatchRestriction:currentPlan.dispatchRestrictions?.find(r=>r.session===currentTask.owner)??null})),predecessorsHash:digest(JSON.stringify(currentTask.predecessors)),observation};
   try{
    const outboxId=this.db.transaction(()=>{
     const latest=this.authority.get(rigId),latestPlan=this.plan(rigId),latestTask=latestPlan?.tasks.find(t=>t.key===task.key);
     if(!latest||!latestPlan||!latestTask||latest.epoch!==proof.epoch||this.claimedContinuationReady({rigId,task:latestTask,queueId:assigned.queue_id,holderSession:actor,holderGeneration:generation,epoch:proof.epoch})!=='ready')return null;
     return this.repo.stageClaimedRecoveryContinuation({proof,recipient:latestTask.owner,body:`Resume your already-claimed assignment ${assigned.queue_id} after verified native recovery. Reconcile your retained work and existing task state before continuing. This notice creates no claim, authority, new assignment, acceptance, or permission to repeat ambiguous work.`});
    }).immediate();
    if(outboxId)results.push({key:task.key,state:'claimed-recovery-continuation-staged',queueId:assigned.queue_id,deadline:task.deadline});
   }catch{/* safe hold: no task custody or existing effect is rewritten */}
  }
  return results;
 }
 /** Final synchronous database/custody gate used by the outbox immediately before pending→sending. */
 claimedRecoveryContinuationWakeReadiness(outboxId:string,queueId:string,holderSession:string,recipient:string,tags:string[]):'ready'|'held'|'invalid' {
  if(!Array.isArray(tags)||tags.length!==3||tags[0]!=='queue:claimed-native-recovery-continuation'||!tags[2]?.startsWith('queue:recipient-generation:'))return 'invalid';
  let proof:ClaimedRecoveryContinuationProof;try{proof=JSON.parse(tags[1]!) as ClaimedRecoveryContinuationProof;}catch{return 'invalid';}
  if(!proof||proof.schema!=='claimed-native-recovery-continuation.v1'||proof.queueId!==queueId||proof.holderSession!==holderSession||proof.observation?.completion?.sessionName!==recipient||tags[2]!==`queue:recipient-generation:${proof.claimantGeneration}`||outboxId!==claimedRecoveryContinuationOutboxId(proof))return 'invalid';
  const plan=this.plan(proof.rigId),task=plan?.tasks.find(t=>t.packageKey===proof.packageKey),authority=this.authority.get(proof.rigId);
  return !!task&&!!authority?this.claimedContinuationReady({rigId:proof.rigId,task,queueId,holderSession,holderGeneration:proof.holderGeneration,epoch:proof.epoch,proof,outboxId}):'invalid';
 }
 /** Post-claim durable activity is a trigger, not semantic completion. The native
  * observer independently proves current quiescence before staging and sending. */
 private ownedOutcomeFacts(queueId:string,owner:string,generation:string):{claimedAt:string;claimTransitionId:number;activitySeq:number;activityEventAt:string}|null {
  const q=this.repo.getById(queueId),target=resolveGuardTarget(this.db,owner);
  if(!q?.claimedAt||!target||target.occupant!==generation)return null;
  const claim=this.db.prepare("SELECT transition_id,ts FROM queue_transitions WHERE qitem_id=? AND state='in-progress' AND actor_session=? AND identity_provenance='transport:v1' ORDER BY transition_id DESC LIMIT 1").get(queueId,owner) as {transition_id:number;ts:string}|undefined;
  const event=this.db.prepare("SELECT seq,payload FROM events WHERE node_id=? AND type='agent.activity' ORDER BY seq DESC LIMIT 1").get(target.nodeId) as {seq:number;payload:string}|undefined;
  if(!claim||!event)return null;
  try {
   const e=JSON.parse(event.payload),at=Date.parse(e.activity?.eventAt??''),claimedAt=Date.parse(q.claimedAt),transitionAt=Date.parse(claim.ts);
   if(e.sessionName!==owner||e.nodeId!==target.nodeId||e.activity?.generation!==generation||e.activity?.state!=='idle'||!Number.isFinite(at)||!Number.isFinite(claimedAt)||!Number.isFinite(transitionAt)||at<=Math.max(claimedAt,transitionAt)||at>this.now())return null;
   return {claimedAt:q.claimedAt,claimTransitionId:claim.transition_id,activitySeq:event.seq,activityEventAt:e.activity.eventAt};
  }catch{return null;}
 }
 private settledObservationMatches(rigId:string,owner:string,generation:string,o:NativeSettledObservation,fresh=false):boolean {
  if(!o||o.schema!=='native-settled-observation.v1'||o.rigId!==rigId||o.sessionName!==owner||o.generation!==generation||!o.sessionId||!o.nodeId||!o.nativeIdentityHash||!o.configurationDigest||!o.lastEntryId||!o.incarnation?.key||!Number.isSafeInteger(o.incarnation.native?.pid)||o.incarnation.native.pid<=0||!o.incarnation.native.startFingerprint||!Number.isFinite(o.observedAt)||o.observedAt>this.now()||!Number.isFinite(o.quiescenceObservedAt)||o.quiescenceObservedAt>o.observedAt||(fresh&&this.now()-o.observedAt>3000))return false;
  const target=resolveGuardTarget(this.db,owner);
  return !!target&&target.nodeId===o.nodeId&&target.occupant===generation&&this.configurationDigest(owner)===o.configurationDigest&&!!this.db.prepare('SELECT 1 FROM sessions s JOIN nodes n ON n.id=s.node_id WHERE s.id=? AND s.node_id=? AND s.session_name=? AND n.rig_id=?').get(o.sessionId,o.nodeId,owner,rigId);
 }
 /** Current authority/admission/effects reuse the retained-claim fence. The proof
  * adds the exact claim transition and durable post-claim activity boundary. */
 ownedOutcomeContinuationWakeReadiness(outboxId:string,queueId:string,holderSession:string,recipient:string,tags:string[],requireReceipt=true,currentObservation?:NativeSettledObservation):'ready'|'held'|'invalid' {
  let p:OwnedOutcomeContinuationProof;
  try{if(tags.length!==3||tags[0]!=='queue:owned-outcome-protocol')return 'invalid';p=JSON.parse(tags[1]!) as OwnedOutcomeContinuationProof;}catch{return 'invalid';}
  if(!p||p.schema!=='owned-outcome-protocol.v1'||p.queueId!==queueId||p.holderSession!==holderSession||p.observation?.sessionName!==recipient||tags[2]!==`queue:recipient-generation:${p.claimantGeneration}`||outboxId!==ownedOutcomeContinuationOutboxId(p))return 'invalid';
  const plan=this.plan(p.rigId),task=plan?.tasks.find(t=>t.packageKey===p.packageKey);
  if(!task||task.owner!==recipient||task.admission.generation!==p.claimantGeneration)return 'invalid';
  const common=this.claimedContinuationReady({rigId:p.rigId,task,queueId,holderSession,holderGeneration:p.holderGeneration,epoch:p.epoch,outboxId});
  if(common!=='ready')return common;
  const row=this.db.prepare('SELECT p.contract_hash,a.body_hash FROM coordinator_assignments a JOIN coordinator_packages p ON p.rig_id=a.rig_id AND p.package_key=a.package_key WHERE a.rig_id=? AND a.package_key=? AND a.queue_id=?').get(p.rigId,p.packageKey,queueId) as {contract_hash:string;body_hash:string}|undefined;
  const facts=this.ownedOutcomeFacts(queueId,recipient,p.claimantGeneration);
  if(!row||row.contract_hash!==p.contractHash||row.body_hash!==p.bodyHash||p.predecessorsHash!==digest(JSON.stringify(task.predecessors))||!this.settledObservationMatches(p.rigId,recipient,p.claimantGeneration,p.observation))return 'invalid';
  // Trigger sequence/cursor are immutable audit evidence, not a lease on a future
  // turn. Keep the obligation pending across intervening work and reobserve it.
  if(!facts)return 'held';
  if(facts.claimedAt!==p.claimedAt||facts.claimTransitionId!==p.claimTransitionId)return 'invalid';
  const current=requireReceipt?currentObservation:p.observation;
  if(!current||!this.settledObservationMatches(p.rigId,recipient,p.claimantGeneration,current,true)||current.quiescenceObservedAt<Date.parse(facts.activityEventAt))return 'held';
  if(current.incarnation.key!==p.observation.incarnation.key||current.nativeIdentityHash!==p.observation.nativeIdentityHash)return 'invalid';
  if(requireReceipt){
   const saved=this.db.prepare("SELECT receipt,request_hash FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='owned-outcome-protocol'").get(p.rigId,outboxId) as {receipt:string;request_hash:string}|undefined;
   if(!saved||saved.receipt!==JSON.stringify(p)||saved.request_hash!==digest(saved.receipt))return 'invalid';
  }
  return 'ready';
 }
 private async stageOwnedOutcomeContinuations(actor:string,generation:string,rigId:string):Promise<CoordinationResult[]> {
  const runtime=this.nativeRecoveryContinuation,plan=this.plan(rigId),authority=this.authority.get(rigId);
  if(!runtime?.observeSettledClaimant||!runtime.withSettledClaimant||!plan||!authority||authority.owner_session!==actor||authority.owner_generation!==generation)return [];
  const results:CoordinationResult[]=[];
  for(const task of plan.tasks){
   const assigned=this.db.prepare('SELECT a.queue_id,p.contract_hash,a.body_hash FROM coordinator_assignments a JOIN coordinator_packages p ON p.rig_id=a.rig_id AND p.package_key=a.package_key WHERE a.rig_id=? AND a.package_key=? AND a.disposition_id IS NULL').get(rigId,task.packageKey) as {queue_id:string;contract_hash:string;body_hash:string}|undefined;
   if(!assigned||task.boundary||task.recoveryFor)continue;
   const facts=this.ownedOutcomeFacts(assigned.queue_id,task.owner,task.admission.generation);if(!facts)continue;
   const obligationId=ownedOutcomeContinuationOutboxId({rigId,queueId:assigned.queue_id,claimantGeneration:task.admission.generation,...facts});
   if(this.db.prepare('SELECT 1 FROM coordinator_operations WHERE rig_id=? AND operation_id=?').get(rigId,obligationId))continue;
   if(!await this.refreshClaimedContinuationOwner(task.owner))continue;
   if(this.claimedContinuationReady({rigId,task,queueId:assigned.queue_id,holderSession:actor,holderGeneration:generation,epoch:authority.epoch})!=='ready')continue;
   let observation:NativeSettledObservation|null;try{observation=await runtime.observeSettledClaimant(task.owner);}catch{continue;}
   if(!observation||!this.settledObservationMatches(rigId,task.owner,task.admission.generation,observation,true))continue;
   const proof:OwnedOutcomeContinuationProof={schema:'owned-outcome-protocol.v1',rigId,queueId:assigned.queue_id,packageKey:task.packageKey,contractHash:assigned.contract_hash,bodyHash:assigned.body_hash,claimantGeneration:task.admission.generation,holderSession:actor,holderGeneration:generation,epoch:authority.epoch,planRevision:plan.revision,admissionHash:digest(JSON.stringify({deadline:task.deadline,admission:task.admission,dispatchRestriction:plan.dispatchRestrictions?.find(r=>r.session===task.owner)??null})),predecessorsHash:digest(JSON.stringify(task.predecessors)),...facts,observation};
   try{
    const outboxId=this.db.transaction(()=>this.repo.stageOwnedOutcomeContinuation({proof,recipient:task.owner,body:`Your native turn settled while you still own assignment ${assigned.queue_id}. This is a bounded outcome-protocol obligation, not a new assignment or permission to repeat product work. Read that exact item with rig queue show ${assigned.queue_id} --full --json and follow its return instructions (returnInstructions) using your genuine identity. Reconcile your retained evidence first. If complete, create a NEW worker-authored typed return with the required evidence references, truthfully close the original using the supported closure contract, then invoke coordinator dispose for the original package and new return. If unfinished or blocked, record that truthful state through supported native commands and report the concrete remaining work or blocker; do not invent completion. Preserve all UNKNOWN effects; never replay uncertain sends. This notice grants no acceptance, recovery or additional product scope.`})).immediate();
    if(outboxId)results.push({key:task.key,state:'owned-outcome-protocol-staged',queueId:assigned.queue_id,deadline:task.deadline});
   }catch{/* safe hold; no original claim or effect is rewritten */}
  }
  return results;
 }
 private reconcileScoped(actor:string,generation:string,rigId:string,scope?:DispatchScope):CoordinationResult[] {
  return this.db.transaction(()=>{
   const a=this.authority.get(rigId),plan=this.plan(rigId);
   if(!a||!plan)fail("coordination_plan_required","Explicit current recovery plan required");
   if(this.authority.generation("operator-agent@kernel")!==plan!.operatorGeneration)fail("coordination_operator_retired","Reauthorize plan after Operator generation change");
   if(actor!==a!.owner_session||generation!==a!.owner_generation||this.authority.generation(actor)!==generation||a!.state!=="active"||a!.lease_until<=this.now())fail("coordinator_retired","Only reconciled current holder may dispatch");
   const token:CoordinatorToken={rigId,epoch:a!.epoch,generation};
   // Native evidence has a strict lifetime. Whole-rig history/administration can
   // take longer than that lifetime and belongs to the final unprepared pass,
   // never between an owner's native observation and its guarded effect.
   const dispatchOnly=!!scope?.size;
   if(!dispatchOnly)this.recordSystemWakeOutcomes(rigId);
   const lifecycle=dispatchOnly?[]:this.centralLifecyclePass(rigId),result:CoordinationResult[]=[];
   if(!dispatchOnly)this.recordProgress(rigId);
   for(const t of plan!.tasks){
    if(dispatchOnly&&!scope!.has(t.owner))continue;
    const dispatchHold=this.dispatchScopeHold(plan!,t);
    const assigned=this.db.prepare("SELECT a.queue_id,a.disposition_id,q.state,q.claimed_by_generation_uuid,q.destination_session FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? AND a.package_key=?").get(rigId,t.packageKey) as {queue_id:string;disposition_id:string|null;state:string;claimed_by_generation_uuid:string|null;destination_session:string}|undefined;
    if(assigned){
     // Retained unfinished work needs the same accountable evidence refresh as
     // undispatched work. This duty changes neither product custody nor admission.
     const currentOwnerGeneration=this.authority.generation(t.owner);
     const unresolvedOwned=!!currentOwnerGeneration&&assigned.destination_session===t.owner&&!assigned.disposition_id
      &&(assigned.state==='pending'&&!assigned.claimed_by_generation_uuid
       ||assigned.state==='in-progress'&&assigned.claimed_by_generation_uuid===currentOwnerGeneration);
     // An unattempted continuation waits for current admission; it must not
     // suppress the administrative duty that supplies that admission. Exclude
     // only this retained claim's pending notice, never sending/UNKNOWN debt.
     const refreshExclusions=assigned.state==='in-progress'&&unresolvedOwned
      ?this.pendingClaimedContinuations(t.owner,assigned.queue_id):[];
     if(unresolvedOwned&&!t.boundary&&this.admissionStaleReason(t)&&!this.workerEffectDebt(t.owner,refreshExclusions)){
      const refresh=this.stageAdmissionRefreshDuty(rigId,t);
      if(refresh?.state==='held')
       result.push({key:refresh.key,state:'held',reason:refresh.reason,deadline:refresh.deadline??t.deadline,...(refresh.queueId?{queueId:refresh.queueId}:{}),...(refresh.activityEvidence?{activityEvidence:refresh.activityEvidence}:{})});
     }
     const runtimeHold=assigned.state==='pending'&&!assigned.claimed_by_generation_uuid&&!assigned.disposition_id?this.runtimeReadiness?.(t):null;
     if(runtimeHold){result.push({key:t.key,state:'held',queueId:assigned.queue_id,reason:runtimeHold.reason,deadline:t.deadline,activityEvidence:{...runtimeHold}});continue;}
     if((!scope||t.deadline>this.now())&&this.dispatchObservationMatches(scope,t,plan!)&&!dispatchHold&&!t.boundary&&this.predecessorsReady(rigId,t)&&assigned.state==='pending'&&!assigned.claimed_by_generation_uuid&&!assigned.disposition_id&&this.admittedNow(t)&&!this.workerEffectDebt(t.owner)&&coordinationIdle(this.activity(t.owner),this.authority.generation(t.owner)??'',this.now()))this.repo.stageCoordinatorAssignmentWake({rigId,epoch:a!.epoch,generation,actor,queueId:assigned.queue_id,recipient:t.owner,recipientGeneration:t.admission.generation,now:this.now()});
     const picked=assigned.state==='in-progress'&&assigned.claimed_by_generation_uuid===this.authority.generation(t.owner)&&assigned.destination_session===t.owner;
     const semanticRecovery=this.authority.runtimeOutcomeAssessment?.requiresRecovery(rigId,t.packageKey)??false;
     const accepted=successfulReturn(assigned.state,assigned.disposition_id)&&!!this.db.prepare("SELECT 1 FROM coordinator_operations WHERE rig_id=? AND kind='coordination-accept' AND json_extract(receipt,'$.queueId')=? AND json_extract(receipt,'$.dispositionId')=?").get(rigId,assigned.queue_id,assigned.disposition_id);
     const state=accepted?'accepted':semanticRecovery?'recovery-required:semantic-incomplete':successfulReturn(assigned.state,assigned.disposition_id)?'returned-awaiting-acceptance':picked?'picked-up':assigned.state==='pending'?'pending-pickup':`recovery-required:${assigned.state}`;
     result.push({key:t.key,state,queueId:assigned.queue_id,deadline:t.deadline,...(!assigned.disposition_id&&this.now()>t.deadline?{reason:'deadline-exceeded: concrete recovery owner/action remains '+t.owner+' / '+t.action}:{})});continue;
    }
    // Dispatch scope is a hard no-assignment gate and stays one: nothing below stages a
     // queue row, package claim or worker wake for a task it holds. Admission refresh is a
     // SEPARATE finite duty about the task's own evidence, so an unassigned task under an
     // expired scope still gets its exact duty instead of a silent hold — the same staging
     // the current-admission-required branch performs below. Worker effect debt still
     // suppresses it: an UNKNOWN effect keeps the owner protected before any new duty.
     if(dispatchHold){
      result.push({key:t.key,state:'held',reason:dispatchHold,deadline:t.deadline});
      if(!this.workerEffectDebt(t.owner)){
       const scoped=this.stageAdmissionRefreshDuty(rigId,t);
       if(scoped?.state==='held')
        result.push({key:scoped.key,state:'held',reason:scoped.reason,deadline:scoped.deadline??t.deadline,...(scoped.queueId?{queueId:scoped.queueId}:{}),...(scoped.activityEvidence?{activityEvidence:scoped.activityEvidence}:{})});
      }
      continue;}
    if(t.recoveryFor){
     const needsRecovery=this.recoveryNeeded(rigId,plan!,t);
     if(!needsRecovery){result.push({key:t.key,state:'held',reason:'recovery-not-needed',deadline:t.deadline});continue;}
    }
    if(t.boundary){result.push({key:t.key,state:'held',reason:t.boundary,deadline:t.deadline});continue;}
    const ready=this.predecessorsReady(rigId,t);
    if(!ready){result.push({key:t.key,state:'held',reason:'predecessor-disposition',deadline:t.deadline});continue;}
    if(this.workerEffectDebt(t.owner)){result.push({key:t.key,state:'held',reason:'uncertain-worker-effect',deadline:t.deadline});continue;}
    if(!this.admittedNow(t)){
     result.push({key:t.key,state:'held',reason:'current-admission-required',deadline:t.deadline});
     // One shared, renewing, accountable admission-refresh duty for the stale task.
     // Staging is read-only apart from the duty's own hash-bound wake, which is excluded
     // from debt via administrativeDuty(). It writes no admission and extends no TTL.
     //
     // The duty's OWN result is propagated, never discarded. A duty held for a protected
     // recipient, an exhausted chain, an unresolved retirement or subject drift is
     // accountable in its own right and must reach the existing intake/retirement chain;
     // dropping it left a silent hold once 'current-admission-required' left the routed
     // list. A duty that staged or completed needs no intake, so a successfully staged duty
     // still never produces a duplicate legacy hold.
     const refresh=this.stageAdmissionRefreshDuty(rigId,t);
     if(refresh?.state==='held')
      result.push({key:refresh.key,state:'held',reason:refresh.reason,deadline:refresh.deadline??t.deadline,...(refresh.queueId?{queueId:refresh.queueId}:{}),...(refresh.activityEvidence?{activityEvidence:refresh.activityEvidence}:{})});
     continue;}
    const runtimeHold=this.runtimeReadiness?.(t);
    if(runtimeHold){result.push({key:t.key,state:'held',reason:runtimeHold.reason,deadline:t.deadline,activityEvidence:{...runtimeHold}});continue;}
    const gen=this.authority.generation(t.owner);
    const sample=this.activity(t.owner),observedNow=this.now();
    if(!gen||(!!scope&&t.deadline<=observedNow)||!this.dispatchObservationMatches(scope,t,plan!)||!coordinationIdle(sample,gen,observedNow)){
     const age=(at:string|null|undefined)=>{const ms=Date.parse(at??'');return Number.isFinite(ms)?observedNow-ms:null;};
     result.push({key:t.key,state:'held',reason:'fresh-activity-required',deadline:t.deadline,activityEvidence:{expectedGeneration:gen,generation:sample?.generation??null,identityVerified:sample?.identityVerified??false,identityAgeMs:age(sample?.identityObservedAt),activity:sample?.state.activity??null,decidedBy:sample?.state.decidedBy??null,needsInputCount:sample?.state.needsInput.count??null,witnessActivity:sample?.witness?.activity??null,witnessRung:sample?.witness?.rung??null,witnessAgeMs:age(sample?.witness?.observedAt),witnessSeatMatches:!!sample?.witness&&sample.witness.seatNodeId===sample.state.seatNodeId,swapGeneration:sample?.state.lastSwap?.generation??null,witnessPredatesSwap:!!sample?.witness&&!!sample.state.lastSwap&&Date.parse(sample.witness.observedAt)<Date.parse(sample.state.lastSwap.at)}});continue;
    }
    // An unrelated queue claim is an exclusive worker obligation, even while idle.
    if((this.db.prepare("SELECT qitem_id FROM queue_items WHERE destination_session IN (?,?) AND state IN ('pending','in-progress','blocked')").all(...rotationLocalAddresses(this.db,t.owner)) as Array<{qitem_id:string}>).some(row=>this.genericWatchActionable(row.qitem_id,rigId))){result.push({key:t.key,state:'held',reason:'existing-worker-custody',deadline:t.deadline});continue;}
    const queueId=`qitem-coordination-${digest(rigId+':'+t.packageKey).slice(0,24)}`;
    // A retained pre-ledger row is history, not a new assignment. Never recreate
    // it or manufacture ownership; keep this slice accountable and continue others.
    const retained=this.repo.getById(queueId);
    if(retained){result.push({key:t.key,state:'held',queueId,reason:retained.destinationSession===t.owner&&digest(retained.body)===digest(t.body)?'existing-queue-without-assignment':'deterministic-queue-conflict',deadline:t.deadline});continue;}
    try {
     this.db.transaction(()=>{
      this.repo.createWithinTransaction({qitemId:queueId,sourceSession:actor,destinationSession:t.owner,body:t.body,dispatch:{token,packageKey:t.packageKey},identityProvenance:'system:operator-authorized-coordination',nudge:true});
      this.freezeBoundPredecessorResolution(rigId,t,plan!.revision);
     })();
    } catch(error) {
     const code=heldDispatchCode(error);
     if(!code)throw error;
     result.push({key:t.key,state:'held',reason:code,deadline:t.deadline});continue;
    }
    result.push({key:t.key,state:'pending-pickup',queueId,deadline:t.deadline});
   }
   if(dispatchOnly)return result;
   // A terminal UI state is not an attributed return. Detect retained scope even
   // when that completed assignment is absent from the latest dispatch plan.
   const missingReturns=this.missingAttributedReturns(rigId);
   for(const missing of missingReturns){
    const current=this.authority.generation(missing.destination),contract=JSON.parse(missing.contract);
    const restriction=plan!.dispatchRestrictions?.find(r=>r.session===missing.destination);
    const scopeHeld=restriction&&(restriction.generation!==current||restriction.validUntil<=this.now()||!restriction.packageKeys.includes(missing.package_key));
    const reason=!current||current!==missing.claimed_by_generation_uuid?'terminal-return-incarnation-changed':digest(missing.body)!==missing.body_hash||contract.destination!==missing.destination||contract.bodyHash!==missing.body_hash||!this.authority.terminalReturnResourcesRetained(rigId,missing.package_key,contract)?'terminal-return-contract-drift':scopeHeld?'checkpoint-quiescence':this.workerEffectDebt(missing.destination,this.nativeReturnRetirementChainNotices(rigId,missing.queue_id,missing.destination,current))?'uncertain-worker-effect':null;
    if(reason){result.push({key:'terminal-return:'+missing.package_key,state:'held',queueId:missing.queue_id,reason,deadline:Date.parse(missing.ts_updated)+1200000});continue;}
    let queueId='qitem-coordination-terminal-return-'+digest(rigId+':'+missing.queue_id+':'+current).slice(0,24);const deadline=this.now()+1200000;
    const latest=this.db.prepare("SELECT q.qitem_id,q.body,o.receipt FROM coordinator_operations o JOIN queue_items q ON q.qitem_id=o.operation_id WHERE o.rig_id=? AND o.kind='native-terminal-return-control' AND json_extract(o.receipt,'$.originalQueueId')=? AND json_extract(o.receipt,'$.workerGeneration')=? ORDER BY q.ts_created DESC,q.rowid DESC LIMIT 1").get(rigId,missing.queue_id,current) as {qitem_id:string;body:string;receipt:string}|undefined;
    if(latest&&JSON.parse(latest.receipt).bodyHash===digest(latest.body))queueId=latest.qitem_id;
    const existing=this.repo.getById(queueId);if(existing&&(!['pending','in-progress','blocked'].includes(existing.state)||!existing.expiresAt||Date.parse(existing.expiresAt)<=this.now())){result.push({key:'terminal-return:'+missing.package_key,state:'held',queueId,reason:'terminal-return-duty-exhausted',deadline:existing.expiresAt?Date.parse(existing.expiresAt):Date.parse(missing.ts_updated)+1200000});continue;}
    if(existing&&['in-progress','blocked'].includes(existing.state)&&!JSON.parse(existing.body).disposeContract&&!this.db.prepare("SELECT 1 FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='native-terminal-return-continuation'").get(rigId,'native-return-continuation:'+queueId)){
     try{this.continueTerminalReturn(actor,generation,{rigId,controlQueueId:queueId,controlBodyHash:digest(existing.body),workerGeneration:current!,deadline:Math.min(Date.parse(existing.expiresAt!),this.now()+1200000)});}catch(error){if(!(error instanceof CoordinatorFenceError))throw error;result.push({key:'terminal-return:'+missing.package_key,state:'held',queueId,reason:error.code,deadline:Date.parse(existing.expiresAt!)});continue;}
    }
    try{
     this.db.transaction(()=>{if(!this.repo.getById(queueId)){this.repo.createNativeTerminalReturnDuty(actor,generation,rigId,{qitemId:queueId,sourceSession:'watchdog@system',destinationSession:missing.destination,expiresAt:new Date(deadline).toISOString(),body:JSON.stringify({action:'record-exact-native-terminal-return',rigId,packageKey:missing.package_key,originalQueueId:missing.queue_id,recipientGeneration:current,inputDigest:contract.inputDigest,returnContract:contract.returnContract,disposeContract:{rigId,packageKey:missing.package_key,dispositionId:'<new worker-authored typed-return queue item ID>'},deadline,grantsAuthority:false,required:'Claim this bounded return duty under your genuine current native identity. Reuse retained evidence from your own original assignment; author its exact typed durable return and use supported coordinator disposition. Close this duty with the actual receipt. Do not reopen or redo work, fabricate evidence, release locks directly, accept work, merge, deploy, or claim another incarnation’s results. Preserve uncertainty and explicit scope limits; report a concrete supported API refusal to the current Lead.'}),identityProvenance:'system:operator-authorized-coordination',nudge:true});this.repo.stageWakeIntent(queueId,'watchdog@system',missing.destination,'system:operator-authorized-coordination',true,current!);}})();
     result.push({key:'terminal-return:'+missing.package_key,state:'pending-native-terminal-return',queueId,deadline:Date.parse(this.repo.getById(queueId)!.expiresAt!)});
    }catch(error){const code=heldDispatchCode(error);if(!code)throw error;result.push({key:'terminal-return:'+missing.package_key,state:'held',queueId:missing.queue_id,reason:'terminal-return-'+code,deadline:Date.parse(missing.ts_updated)+1200000});}
   }
   // Configuration/effect holds need real recovery custody, not only a diagnostic
   // string. Control intake grants no dispatch, acceptance or history authority.
   result.push(...lifecycle);
   for(const held of result)this.stageTaskHoldIntake(rigId,a!,plan!,held,missingReturns);
   result.push(...this.repo.observeOutboxAbandonNotifications(rigId,plan!.operatorGeneration));
   // Observation ages are diagnostics, not new work or a new reconciliation state.
   const stableResult=result.map(({activityEvidence,...state})=>state);
   const operationId=`coordination-reconcile:${digest(JSON.stringify({revision:plan!.revision,epoch:a!.epoch,result:stableResult}))}`;
   this.db.prepare("INSERT OR IGNORE INTO coordinator_operations VALUES (?,?,?,?,?)").run(rigId,operationId,'coordination-reconcile',JSON.stringify(result),digest(JSON.stringify({actor,generation})));
   this.recordProgress(rigId);
   return result;
  }).immediate();
 }
 configurationDigest(session:string,launch?:{nodeId:string;generation:string;runtime:string}):string|null {
  const target=resolveGuardTarget(this.db,session);
  if(!target||target.session!==session||(launch&&target.nodeId!==launch.nodeId))return null;
  const row=this.db.prepare('SELECT n.id,n.runtime,n.model,n.profile,n.codex_config_profile,n.cwd FROM nodes n JOIN sessions s ON s.node_id=n.id WHERE n.id=? AND s.session_name=? ORDER BY s.id DESC LIMIT 1').get(target.nodeId,session) as {id:string;runtime:string;model:string|null;profile:string|null;codex_config_profile:string|null;cwd:string|null}|undefined;
  if(!row)return null;
  if(launch){
   if(row.id!==launch.nodeId)return null;
   const reservation=this.db.prepare("SELECT expected_json,successor_generation FROM seat_dispatch_reservations WHERE node_id=? AND state='started'").get(launch.nodeId) as {expected_json:string;successor_generation:string|null}|undefined;
   if(reservation){
    let prepared:any;try{prepared=JSON.parse(reservation.expected_json);}catch{return null;}
    if(prepared.protocol==='runtime-migration-v1'){
     if(reservation.successor_generation!==launch.generation||prepared.expected?.nodeId!==row.id||prepared.expected?.sessionName!==session
       ||prepared.target?.runtime!==launch.runtime||typeof prepared.target.model!=='string'||typeof prepared.target.codexConfigProfile!=='string')return null;
     // The authenticated one-shot migration commits these exact fields only
     // after native launch. Pin that staged configuration in the launch intent;
     // an effect grant still requires the eventual current committed digest.
     return digest(JSON.stringify({...row,runtime:prepared.target.runtime,model:prepared.target.model,codex_config_profile:prepared.target.codexConfigProfile}));
    }
   }
   if(row.runtime!==launch.runtime)return null;
  }
  return digest(JSON.stringify(row));
 }
 private dispatchScopeHold(plan:CoordinationPlan,t:CoordinationTask):string|null {
  const r=plan.dispatchRestrictions?.find(r=>r.session===t.owner);if(!r)return null;
  if(r.generation!==this.authority.generation(t.owner))return 'dispatch-scope-generation';
  if(r.validUntil<=this.now())return 'dispatch-scope-expired';
  return r.packageKeys.includes(t.packageKey)?null:'checkpoint-quiescence';
 }
 /** Read-only shared containment decision; never resolves or replays an effect. */
 hasUnresolvedWorkerEffects(session:string):boolean {
  return this.workerEffectDebt(session);
 }
 private pendingClaimedContinuations(session:string,queueId:string):string[] {
  return (this.db.prepare(`SELECT outbox_id FROM outbox_entries
   WHERE delivery_state='pending' AND destination_session=? AND audit_pointer=?
    AND CASE WHEN json_valid(tags) THEN json_extract(tags,'$[0]') END IN ('queue:claimed-native-recovery-continuation','queue:owned-outcome-protocol')`)
   .all(session,queueId) as {outbox_id:string}[]).map(row=>row.outbox_id);
 }
 private workerEffectDebt(session:string,excludeEffect?:string|string[]):boolean {
  const addresses=rotationLocalAddresses(this.db,session);
  const target=resolveGuardTarget(this.db,session);
  const rig=target?.session===session?this.db.prepare('SELECT rig_id FROM nodes WHERE id=?').get(target.nodeId) as {rig_id:string}|undefined:undefined;
  const effects=this.db.prepare("SELECT * FROM outbox_entries WHERE delivery_state NOT IN ('delivered','failed','retired') AND (sender_session IN (?,?) OR destination_session IN (?,?))").all(...addresses,...addresses) as Record<string,unknown>[];
  return effects.some(row=>{
   if(Array.isArray(excludeEffect)?excludeEffect.includes(String(row.outbox_id)):row.outbox_id===excludeEffect)return false;
   const outboxId=String(row.outbox_id??''),control=outboxId.startsWith('wake-intent-')?this.lifecycleControl(outboxId.slice('wake-intent-'.length)):null,effectRig=control?.rigId??this.systemTaskRig(row)??rig?.rig_id;
if(!effectRig)return true;
    return !this.authority.isAdoptedHistoryContained(effectRig,row)&&!this.noticeOutcomeContained(effectRig,row);
   });
  }
 private recoveryNeeded(rigId:string,plan:CoordinationPlan,t:CoordinationTask):boolean {
  const target=plan.tasks.find(other=>other.key===t.recoveryFor)!;
  const targetAssignment=this.db.prepare("SELECT q.state,a.disposition_id FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? AND a.package_key=?").get(rigId,target.packageKey) as {state:string;disposition_id:string|null}|undefined;
  const targetGen=this.authority.generation(target.owner);
  // An undispatched dependent is not a failed obligation. Ordinary owner
  // custody/busy work is scheduling, not evidence that this package failed.
  const targetReady=this.predecessorsReady(rigId,target);
  const occupied=!!this.db.prepare("SELECT 1 FROM queue_items WHERE destination_session IN (?,?) AND state IN ('pending','in-progress','blocked')").get(...rotationLocalAddresses(this.db,target.owner));
  const observed=this.activity(target.owner);
  const busyAt=Date.parse(observed?.witness?.observedAt??'');
  const knownBusy=observed?.identityVerified&&observed.generation===targetGen&&observed.state.activity==='working'&&observed.witness?.activity==='working'&&observed.witness.rung===observed.state.decidedBy&&observed.witness.seatNodeId===observed.state.seatNodeId&&Number.isFinite(busyAt)&&busyAt<=this.now()&&this.now()-busyAt<=3000;
  const semanticRecovery=this.authority.runtimeOutcomeAssessment?.requiresRecovery(rigId,target.packageKey)??false;
  return !target.boundary&&(!targetAssignment||!successfulReturn(targetAssignment.state,targetAssignment.disposition_id)||semanticRecovery)&&((semanticRecovery&&!!targetAssignment)||(!!targetAssignment&&(['blocked','failed','denied','canceled'].includes(targetAssignment.state)||this.now()>target.deadline))||(!targetAssignment&&targetReady&&!occupied&&!knownBusy&&(this.now()>target.deadline||this.workerEffectDebt(target.owner)||!this.admittedNow(target)||!targetGen||!coordinationIdle(observed,targetGen,this.now()))));
 }
 private admittedNow(t:CoordinationTask):boolean {
  return t.admission.validUntil>this.now()&&t.admission.generation===this.authority.generation(t.owner)&&t.admission.configurationDigest===this.configurationDigest(t.owner);
 }
 /** Returns require coordinator disposition; worker completion alone is not acceptance. */
 accept(actor:string,generation:string,rigId:string,packageKey:string,dispositionId:string,evidenceRef:string):void {
  this.commitAcceptance(actor,generation,rigId,packageKey,dispositionId,evidenceRef);
  const plan=this.plan(rigId);if(plan&&plan.operatorGeneration===this.authority.generation('operator-agent@kernel'))this.reconcile(actor,generation,rigId);
 }
 /** Acceptance and release of the accepting holder's exact claimed inputs commit
  * together. Never claim an unread return or release another generation's work. */
 private commitAcceptance(actor:string,generation:string,rigId:string,packageKey:string,dispositionId:string,evidenceRef:string):void {
  const events=this.db.transaction(()=>{
   const a=this.authority.get(rigId);
   if(!a||a.owner_session!==actor||a.owner_generation!==generation||a.state!=='active'||this.authority.generation(actor)!==generation||a.lease_until<=this.now())fail('coordinator_retired','Current holder must accept exact return');
   this.authority.runtimeOutcomeAssessment?.assertAcceptance(rigId,packageKey);
   if(!evidenceRef)fail('coordination_acceptance_required','Attributed technical disposition evidence required');
   const row=this.db.prepare("SELECT a.queue_id,a.disposition_id,q.state FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? AND a.package_key=?").get(rigId,packageKey) as {queue_id:string;disposition_id:string|null;state:string}|undefined;
   if(!row||row.disposition_id!==dispositionId||!['done','handed-off'].includes(row.state))fail('coordination_return_required','Exact successful attributed released return required');
   const receipt={queueId:row!.queue_id,dispositionId,actor,generation,evidenceRef};
   const id='coordination-accept:'+packageKey;
   const prior=this.db.prepare('SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=?').get(rigId,id) as {receipt:string}|undefined;
   if(prior){if(JSON.parse(prior.receipt).dispositionId!==dispositionId)fail('coordination_acceptance_conflict','Accepted result cannot change');}
   else this.db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run(rigId,id,'coordination-accept',JSON.stringify(receipt),digest(JSON.stringify(receipt)));
   const subject=row!.queue_id+':'+dispositionId;
   // Capture before closing; expired or superseded duties retain their existing fences.
   this.captureDutyCompletion(rigId,'acceptance',subject);
   const dutyId=this.liveDutyLink(rigId,'acceptance',subject);
   const closeIds=[dispositionId,...(dutyId&&this.dutyFacts(dutyId).close?[dutyId]:[])];
   const events:PersistedEvent[]=[];
   for(const queueId of new Set(closeIds)){
    const q=this.repo.getById(queueId);
    const claim=this.db.prepare('SELECT claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(queueId) as {claimed_by_generation_uuid:string|null}|undefined;
    if(!q||!q.claimedAt||!['in-progress','blocked'].includes(q.state)||claim?.claimed_by_generation_uuid!==generation||!rotationLocalAddresses(this.db,actor).includes(q.destinationSession))continue;
    const result=this.repo.updateWithinTransaction({qitemId:queueId,actorSession:actor,actorGeneration:generation,identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on',transitionNote:'Exact result accepted: '+id});
    events.push(...result.persistedEvents);
   }
   this.recordProgress(rigId);
   return events;
  }).immediate();
  this.repo.notifyCommittedUpdates(events);
 }
 private progressDigest(rigId:string):string {
  // Message text, wake consumption, repeated notes and nudge timestamps do not count.
  const rows=this.db.prepare("SELECT a.package_key,a.queue_id,a.disposition_id,q.state,q.claimed_by_generation_uuid FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? ORDER BY a.package_key").all(rigId);
  const accepted=this.db.prepare("SELECT operation_id,receipt FROM coordinator_operations WHERE rig_id=? AND kind='coordination-accept' ORDER BY operation_id").all(rigId);
  return digest(JSON.stringify({rows,accepted}));
 }
 private recordProgress(rigId:string):{digest:string;at:number} {
  const value=this.progressDigest(rigId);
  const last=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='coordination-progress' ORDER BY rowid DESC LIMIT 1").get(rigId) as {receipt:string}|undefined;
  const prior=last?JSON.parse(last.receipt) as {digest:string;at:number}:null;
  if(prior?.digest===value)return prior;
  const next={digest:value,at:this.now()};
  this.db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run(rigId,'coordination-progress:'+digest(JSON.stringify(next)+JSON.stringify(this.db.prepare('SELECT count(*) n FROM coordinator_operations').get())), 'coordination-progress',JSON.stringify(next),value);
  return next;
 }
 canTransferIdle(rigId:string,recipient:string,recipientGeneration:string,progressDigest:string):boolean {
  const a=this.authority.get(rigId);
  if(this.plan(rigId)?.dispatchRestrictions?.some(r=>r.session===recipient))return false;
  if(!a||this.progressDigest(rigId)!==progressDigest||this.authority.generation(recipient)!==recipientGeneration)return false;
  if(this.db.prepare("SELECT 1 FROM queue_items WHERE destination_session IN (?,?) AND state IN ('pending','in-progress','blocked') AND qitem_id<>?").get(...rotationLocalAddresses(this.db,recipient),a.baton_id))return false;
  return coordinationIdle(this.activity(recipient),recipientGeneration,this.now())&&coordinationIdle(this.activity(a.owner_session),a.owner_generation,this.now());
 }
 canTransferUnavailable(rigId:string,recipient:string,recipientGeneration:string):boolean {
  if(this.plan(rigId)?.dispatchRestrictions?.some(r=>r.session===recipient))return false;
  const a=this.authority.get(rigId),plan=this.plan(rigId);if(!a||!plan||plan.allowUnavailablePeerTransfer!==true||this.authority.generation(recipient)!==recipientGeneration)return false;
  if(plan.tasks.some(t=>t.admission.generation!==this.authority.generation(t.owner)||t.admission.configurationDigest!==this.configurationDigest(t.owner)||!Number.isFinite(t.admission.validUntil)||t.admission.validUntil<=this.now()))return false;
  if(this.db.prepare("SELECT 1 FROM queue_items WHERE destination_session IN (?,?) AND state IN ('pending','in-progress','blocked') AND qitem_id<>?").get(...rotationLocalAddresses(this.db,recipient),a.baton_id))return false;
  return coordinationIdle(this.activity(recipient),recipientGeneration,this.now());
 }
/** Same-current native occupant presence, deliberately not idle: a holder that is
   * alive and busy is exactly the case whose lapsed lease must stay actionable instead
   * of being read as an idle takeover opportunity. */
  private nativeHolderPresent(session:string,generation:string):boolean {
   const sample=this.activity(session),at=Date.parse(sample?.identityObservedAt??'');
   return !!sample&&sample.identityVerified&&sample.generation===generation&&Number.isFinite(at)&&at<=this.now()&&this.now()-at<=3000;
  }
  /** A finite lease episode is one administrative intake. The exact lease tuple is part
   * of the recovery key, so repeated ticks dedupe onto one live item, a later lease is a
   * distinct episode, and a lapsed item is restaged through its own lineage instead of
   * being reused forever. The custody digest is notice only: the supported recovery API
   * re-reads exact current custody and refuses any drift. */
  private stageCoordinatorRecovery(rigId:string,epoch:number,operatorGeneration:string,action:string,reason:string,deadline:number,heldHistoryAdmission?:Record<string,unknown>,activeExpiryLease?:{owner:string;ownerGeneration:string;expectedLeaseUntil:number;custodyDigest:string;recoveryWindowMs:number}):string {
   const finite=!!heldHistoryAdmission||!!activeExpiryLease;
   const recoveryKey=digest((heldHistoryAdmission?JSON.stringify(heldHistoryAdmission):'')+(activeExpiryLease?JSON.stringify({owner:activeExpiryLease.owner,ownerGeneration:activeExpiryLease.ownerGeneration,expectedLeaseUntil:activeExpiryLease.expectedLeaseUntil}):'')+rigId+':'+epoch+':'+operatorGeneration+':'+action+':'+reason);
   const previous=this.db.prepare("SELECT qitem_id,state,expires_at FROM queue_items WHERE destination_session='operator-agent@kernel' AND json_valid(body) AND json_extract(body,'$.recoveryKey')=? ORDER BY rowid DESC LIMIT 1").get(recoveryKey) as {qitem_id:string;state:string;expires_at:string|null}|undefined;
   const queueId=previous&&['pending','in-progress','blocked'].includes(previous.state)&&(!finite||(!!previous.expires_at&&Date.parse(previous.expires_at)>this.now()))?previous.qitem_id:'qitem-coordination-recovery-'+digest(recoveryKey+':'+(previous?.qitem_id??'initial')).slice(0,24);
  if(!this.repo.getById(queueId))this.repo.createWithinTransaction({qitemId:queueId,sourceSession:'watchdog@system',destinationSession:'operator-agent@kernel',expiresAt:finite?new Date(deadline).toISOString():undefined,body:JSON.stringify({...(heldHistoryAdmission?{heldHistoryAdmission}:{}),...(activeExpiryLease?{activeExpiry:activeExpiryLease}:{}),action,reason,recoveryKey,previousQueueId:previous?.qitem_id??null,rigId,epoch,recipientGeneration:operatorGeneration,deadline,nextAction:action==='restore-current-held-history-binding'?'Have the actual current Lead author a finite held-history recovery task; current Operator must genuinely claim it and use supported held-history-recovery-bind with exact retained hashes. Preserve UNKNOWN effects, existing worker custody and all checkpoint limits. This notice is not a recovery admission, binding, takeover or acceptance. Return exact proof or a concrete protected boundary.':action==='active-expiry-recover'?'First re-read exact current authority and its canonical baton. If the baton is blocked, only the genuine original holder at the exact current owner generation may reclaim that baton through the supported queue claim path; the Operator must not claim on the holder’s behalf. This restores queue custody only and does not renew or extend the expired authority. After that claim, the current Operator must genuinely claim this recovery notice, re-read exact current authority and obligations, and invoke supported coordinator active-expiry-recover with the exact rigId, epoch, holder generation and expectedLeaseUntil plus the freshly computed current reconciliation digest. Never reuse this notice’s staged custody digest after a baton claim or other custody change. This opens one bounded reconciliation window for the same holder without moving custody, acknowledging, qualifying, admitting or dispatching product work. During that live window, the same-current native holder must use supported resume-owned to acknowledge and renew atomically; resume-owned cannot recover or renew an expired lease directly. Return a concrete supported refusal for any drift, stale or retired holder, unproven native presence or unresolved effects.':'Revalidate exact current native holder/Peer, plan and baton custody. A living holder may perform supported voluntary transfer; admit fresh idle or positive-absence recovery only when proven. Repair expired admissions or uncertain effects through their existing supported paths. Preserve workers and return a concrete protected boundary when evidence is unknown; do not fabricate extension or acknowledgment.',returnPath:{queueId,actor:'operator-agent@kernel',required:'Claim exact recovery item and return supported evidence or concrete protected boundary. Do not declare pickup/ACK or native absence from a role label.'}}),identityProvenance:'system:operator-authorized-coordination',nudge:true});
  if(heldHistoryAdmission){const opId='held-recovery-notice:'+queueId;if(!this.db.prepare('SELECT 1 FROM coordinator_operations WHERE rig_id=? AND operation_id=?').get(rigId,opId))this.authority.recordHeldRecoveryAdmission(rigId,queueId,digest(this.repo.getById(queueId)!.body),heldHistoryAdmission);}
  this.repo.stageWakeIntent(queueId,'watchdog@system','operator-agent@kernel','system:operator-authorized-coordination',true,operatorGeneration);if(action==='restore-current-held-history-binding')this.stageHeldHistoryAuthoring(rigId,queueId);return queueId;
 }
 /** Separately admitted feedback preserves the exact existing parent custody. */
 continueCustody(actor:string,generation:string,input:{rigId:string;epoch:number;parentPackageKey:string;parentQueueId:string;workerGeneration:string;feedbackPackageKey:string;body:string}):{queueId:string} {
  return this.db.transaction(()=>{

   const a=this.authority.get(input.rigId);
   if(!a||a.owner_session!==actor||a.owner_generation!==generation||a.epoch!==input.epoch||this.authority.generation(actor)!==generation||a.state!=='active'||a.lease_until<=this.now())fail('coordinator_retired','Only active current coordinator may continue custody');
   this.authority.assertCurrentOwner(actor,{rigId:input.rigId,epoch:input.epoch,generation});
   const parent=this.db.prepare("SELECT a.destination,a.body_hash,q.body,q.state,q.claimed_by_generation_uuid FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? AND a.package_key=? AND a.queue_id=?").get(input.rigId,input.parentPackageKey,input.parentQueueId) as {destination:string;body_hash:string;body:string;state:string;claimed_by_generation_uuid:string}|undefined;
   if(!parent||!['blocked','in-progress'].includes(parent.state)||parent.body_hash!==digest(parent.body)||parent.claimed_by_generation_uuid!==input.workerGeneration||this.authority.generation(parent.destination)!==input.workerGeneration)fail('coordination_parent_custody_required','Exact current claimed parent obligation required');
   const row=this.db.prepare('SELECT contract FROM coordinator_packages WHERE rig_id=? AND package_key=?').get(input.rigId,input.feedbackPackageKey) as {contract:string}|undefined;
   const c=row?JSON.parse(row.contract):null;let packet;try{packet=JSON.parse(input.body);}catch{fail('coordination_invalid_feedback','Typed feedback required');}
   if(!c||c.destination!==parent!.destination||c.bodyHash!==digest(input.body)||c.resources.length!==0||c.returnContract.destination!==actor||!c.returnContract.evidenceRequired.length||packet.action!=='reconcile-existing-custody'||packet.parentQueueId!==input.parentQueueId||packet.parentPackageKey!==input.parentPackageKey||typeof packet.instruction!=='string'||!packet.instruction.trim()||Object.keys(packet).some(k=>!['action','parentQueueId','parentPackageKey','instruction'].includes(k)))fail('coordination_feedback_not_admitted','Exact separately Operator-admitted feedback and current return path required');
   const queueId='qitem-coordination-feedback-'+digest(input.rigId+':'+input.feedbackPackageKey).slice(0,24);
   const existing=this.db.prepare('SELECT queue_id FROM coordinator_assignments WHERE rig_id=? AND package_key=?').get(input.rigId,input.feedbackPackageKey) as {queue_id:string}|undefined;
   if(existing)return {queueId:existing.queue_id};
   this.repo.createWithinTransaction({qitemId:queueId,sourceSession:actor,destinationSession:parent!.destination,body:input.body,dispatch:{token:{rigId:input.rigId,epoch:input.epoch,generation},packageKey:input.feedbackPackageKey},identityProvenance:'transport:v1',nudge:true});return {queueId};
  }).immediate();
 }
 /** Read-only final dispatch integration: no authority or custody is granted by this proof. */
 assignmentPredecessorsReady(queueId:string):boolean {
  const assignment=this.db.prepare('SELECT rig_id,package_key FROM coordinator_assignments WHERE queue_id=?').get(queueId) as {rig_id:string;package_key:string}|undefined;
  if(!assignment)return false;
  const task=this.plan(assignment.rig_id)?.tasks.find(t=>t.packageKey===assignment.package_key);
  return !!task&&this.predecessorsReady(assignment.rig_id,task);
 }
 private predecessorsReady(rigId:string,t:CoordinationTask):boolean {
  if(!(t.predecessors.every(p=>isBoundPredecessor(p)?this.boundPredecessorReady(rigId,p):!!this.db.prepare("SELECT 1 FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? AND a.queue_id=? AND a.disposition_id=? AND q.state IN ('done','handed-off') AND EXISTS (SELECT 1 FROM coordinator_operations o WHERE o.rig_id=a.rig_id AND o.kind='coordination-accept' AND json_extract(o.receipt,'$.queueId')=a.queue_id AND json_extract(o.receipt,'$.dispositionId')=a.disposition_id)").get(rigId,p.queueId,p.dispositionId))))return false;
  if(!t.predecessors.some(isBoundPredecessor)||!this.db.prepare('SELECT 1 FROM coordinator_assignments WHERE rig_id=? AND package_key=?').get(rigId,t.packageKey))return true;
  const frozen=this.db.prepare("SELECT receipt,request_hash FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='coordination-predecessor-resolution'").get(rigId,`coordination-predecessor-resolution:${rigId}:${t.packageKey}`) as {receipt:string;request_hash:string}|undefined;
  if(!frozen||digest(frozen.receipt)!==frozen.request_hash)return false;
  try{return JSON.stringify(JSON.parse(frozen.receipt).predecessors)===JSON.stringify(t.predecessors.filter(isBoundPredecessor).map(p=>this.boundPredecessorProof(rigId,p)));}catch{return false;}
 }
/** Keyed, same-rig historical proof; returned is independent of acceptance. */
 private returnedPredecessorResolution(rigId:string,p:BoundPredecessor):ReturnedPredecessorResolution|undefined {
  if(p.queueId!=='qitem-coordination-'+digest(rigId+':'+p.packageKey).slice(0,24))return undefined;
  const row=this.db.prepare("SELECT k.contract,a.destination,a.body_hash,a.disposition_id,q.body,q.claimed_by_generation_uuid FROM coordinator_packages k JOIN coordinator_assignments a ON a.rig_id=k.rig_id AND a.package_key=k.package_key JOIN queue_items q ON q.qitem_id=a.queue_id WHERE k.rig_id=? AND k.package_key=? AND k.contract_hash=? AND a.queue_id=? AND a.disposition_id IS NOT NULL AND q.destination_session=a.destination AND q.state IN ('done','handed-off')").get(rigId,p.packageKey,p.contractHash,p.queueId) as any;
  if(!row||row.disposition_id===p.queueId||!row.claimed_by_generation_uuid||digest(row.body)!==row.body_hash)return undefined;
  let contract:any;try{contract=JSON.parse(row.contract);}catch{return undefined;}
  if(contract.destination!==row.destination||contract.bodyHash!==row.body_hash||!Array.isArray(contract.returnContract?.evidenceRequired)||!contract.returnContract.evidenceRequired.length||!this.validContinuationReturn(row.disposition_id,row.destination,row.claimed_by_generation_uuid,p.packageKey,contract))return undefined;
  const op=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='disposition'").get(rigId,row.disposition_id) as {receipt:string}|undefined;
  let receipt:any;try{receipt=op?JSON.parse(op.receipt):null;}catch{return undefined;}
  if(receipt?.packageKey!==p.packageKey||receipt.actor!==row.destination||receipt.generation!==row.claimed_by_generation_uuid)return undefined;
  const returned=this.db.prepare('SELECT body FROM queue_items WHERE qitem_id=?').get(row.disposition_id) as {body:string};
  return {dispositionId:row.disposition_id,milestone:'returned',originalBodyHash:row.body_hash,worker:row.destination,claimedGeneration:row.claimed_by_generation_uuid,returnBodyHash:digest(returned.body),returnDestination:contract.returnContract.destination,contractBodyHash:digest(row.contract),dispositionReceiptHash:digest(op!.receipt)};
 }
 private boundPredecessorResolution(rigId:string,p:BoundPredecessor):{dispositionId:string}|ReturnedPredecessorResolution|undefined {
  if(p.milestone==='returned')return this.returnedPredecessorResolution(rigId,p);
  const resolved=this.db.prepare("SELECT a.disposition_id dispositionId FROM coordinator_packages k JOIN coordinator_assignments a ON a.rig_id=k.rig_id AND a.package_key=k.package_key JOIN queue_items q ON q.qitem_id=a.queue_id WHERE k.rig_id=? AND k.package_key=? AND k.contract_hash=? AND a.queue_id=? AND a.disposition_id IS NOT NULL AND q.state IN ('done','handed-off') AND EXISTS (SELECT 1 FROM coordinator_operations o WHERE o.rig_id=a.rig_id AND o.operation_id='coordination-accept:'||a.package_key AND o.kind='coordination-accept' AND json_extract(o.receipt,'$.queueId')=a.queue_id AND json_extract(o.receipt,'$.dispositionId')=a.disposition_id)").get(rigId,p.packageKey,p.contractHash,p.queueId) as {dispositionId:string}|undefined;
  if(!resolved)return undefined;
  // Reuse the acceptance contract, including genuinely accepted repair. The
  // durable recovery marker alone cannot veto the original's genuine acceptance.
  try{this.authority.runtimeOutcomeAssessment?.assertAcceptance(rigId,p.packageKey);}
  catch(error){if(error instanceof CoordinatorFenceError&&['runtime_outcome_pending','runtime_outcome_recovery_required'].includes(error.code))return undefined;throw error;}
  return resolved;
 }
 private boundPredecessorReady(rigId:string,p:BoundPredecessor):boolean {return !!this.boundPredecessorResolution(rigId,p);}
 private boundPredecessorProof(rigId:string,p:BoundPredecessor):any|undefined {
  const r=this.boundPredecessorResolution(rigId,p);if(!r)return undefined;
  const binding={packageKey:p.packageKey,contractHash:p.contractHash,queueId:p.queueId};
  if(p.milestone==='returned')return {...binding,...r};
  return {...binding,dispositionId:r.dispositionId,acceptOperationId:'coordination-accept:'+p.packageKey,...(p.milestone==='accepted'?{milestone:'accepted'}:{})};
 }
 /** Inside successor creation: freeze exact milestone proof atomically with the queue item. */
 private freezeBoundPredecessorResolution(rigId:string,t:CoordinationTask,planRevision:string):void {
  const bound=t.predecessors.filter(isBoundPredecessor);if(!bound.length)return;
  const predecessors=bound.map(p=>{const proof=this.boundPredecessorProof(rigId,p);if(!proof)fail('coordination_predecessor_unresolved','Bound predecessor lost its exact milestone before successor creation');return proof;});
  const receipt={predecessors,planRevision};
  this.db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run(rigId,`coordination-predecessor-resolution:${rigId}:${t.packageKey}`,'coordination-predecessor-resolution',JSON.stringify(receipt),digest(JSON.stringify(receipt)));
 }
 private validateBoundPredecessor(plan:CoordinationPlan,t:CoordinationTask,pred:BoundPredecessor,old?:CoordinationTask):void {
  const bad=(reason:string):never=>fail('coordination_invalid_predecessor','Pre-bound successor reference refused: '+reason);
  if(Object.keys(pred).some(key=>!['contractHash','packageKey','queueId','milestone'].includes(key))||(pred.milestone!==undefined&&!['returned','accepted'].includes(pred.milestone))||typeof pred.packageKey!=='string'||!pred.packageKey||typeof pred.contractHash!=='string'||!pred.contractHash||typeof pred.queueId!=='string'||!pred.queueId||'dispositionId' in pred)bad('packageKey, contractHash and queueId plus optional returned/accepted milestone required');
  if(t.recoveryFor)bad('a recovery task cannot use a bound predecessor');
  if(pred.packageKey===t.packageKey)bad('self reference');
  const target=plan.tasks.find(other=>other.packageKey===pred.packageKey);
  if(!target)bad('predecessor must be a task of this plan');
  if(target!.recoveryFor)bad('a recovery task never satisfies a successor');
  const pkg=this.db.prepare('SELECT contract_hash FROM coordinator_packages WHERE rig_id=? AND package_key=?').get(plan.rigId,pred.packageKey) as {contract_hash:string}|undefined;
  if(!pkg||pkg.contract_hash!==pred.contractHash)bad('contract hash differs from the admitted package');
  if(pred.queueId!=='qitem-coordination-'+digest(plan.rigId+':'+pred.packageKey).slice(0,24))bad('queue id is not the deterministic assignment instance');
  const assignment=this.db.prepare('SELECT queue_id,body_hash FROM coordinator_assignments WHERE rig_id=? AND package_key=?').get(plan.rigId,pred.packageKey) as {queue_id:string;body_hash:string}|undefined;
  if(!assignment&&this.repo.getById(pred.queueId))bad('retained pre-ledger queue row requires the exact queue/disposition form');
  if(assignment&&(assignment.queue_id!==pred.queueId||assignment.body_hash!==digest(target!.body)))bad('existing assignment is a different instance');
  if(old&&JSON.stringify(old.predecessors)!==JSON.stringify(t.predecessors)&&this.db.prepare('SELECT 1 FROM coordinator_assignments WHERE rig_id=? AND package_key=?').get(plan.rigId,t.packageKey))bad('predecessor references cannot change after assignment');
 }
 async deliverCommitted():Promise<void>{await this.repo.drainPendingWakeIntents();}
 /** Existing Operator-registered coordinator watchdog is the only unattended actor.
  * It creates real queue intents through the same path, never acknowledgment. */
 private assertCoordinationObserver(rigId:string,jobId:string,plan:CoordinationPlan):void {
  const job=this.db.prepare('SELECT * FROM watchdog_jobs WHERE job_id=?').get(jobId) as {policy:string;state:string;registered_by_session:string;registered_by_generation_uuid:string;target_session:string}|undefined;
  if(!job||job.policy!=='coordinator-continuity'||job.state!=='active'||job.target_session!=='operator-agent@kernel'||job.registered_by_session!=='operator-agent@kernel'||job.registered_by_generation_uuid!==plan.operatorGeneration||this.authority.generation('operator-agent@kernel')!==plan.operatorGeneration)fail('coordination_observer_not_authorized','Current Operator job and plan required');
 }
 supervise(rigId:string,jobId:string):CoordinationResult[]|null {return this.superviseScoped(rigId,jobId);}
 private superviseScoped(rigId:string,jobId:string,scope?:DispatchScope):CoordinationResult[]|null {
  return this.db.transaction(()=>{
   if(scope?.size){
    const plan=this.plan(rigId),a=this.authority.get(rigId);
    if(!plan||!a)fail('coordination_plan_required','Explicit current recovery plan required');
    this.assertCoordinationObserver(rigId,jobId,plan!);
    // Observer authority is checked here; the owner/lease/plan/generation and
    // every native dispatch fence remain checked inside reconcileScoped.
    // Peer transfer, recovery and global intake run once in the final pass.
    return this.reconcileScoped(a!.owner_session,a!.owner_generation,rigId,scope);
   }
   // The registered Operator observer records outcome-only receipts without any
   // plan or holder, which is the only path a pre-plan rollout rig has.
   if(this.db.prepare("SELECT 1 FROM watchdog_jobs WHERE job_id=? AND policy='coordinator-continuity' AND state='active' AND target_session='operator-agent@kernel' AND registered_by_session='operator-agent@kernel' AND registered_by_generation_uuid=?").get(jobId,this.authority.generation('operator-agent@kernel')??''))this.recordSystemWakeOutcomes(rigId);
   const plan=this.plan(rigId),a=this.authority.get(rigId);
   if(!plan||!a)return null;
   this.assertCoordinationObserver(rigId,jobId,plan);
   // Pickup of the existing Operator intake is an independent controller edge;
   // it must not wait for another idle-takeover failure to notify its real Lead.
   const heldIntake=this.db.prepare("SELECT qitem_id FROM queue_items WHERE source_session='watchdog@system' AND destination_session='operator-agent@kernel' AND state IN ('in-progress','blocked') AND claimed_by_generation_uuid=? AND json_valid(body) AND json_extract(body,'$.rigId')=? AND json_extract(body,'$.epoch')=? AND json_extract(body,'$.action')='restore-current-held-history-binding' ORDER BY rowid DESC LIMIT 1").get(plan.operatorGeneration,rigId,a.epoch) as {qitem_id:string}|undefined;
   const progress=this.recordProgress(rigId);
   if((a.state==='active'||a.state==='recovery')&&a.lease_until<=this.now()&&plan.allowUnavailablePeerTransfer===true&&this.authority.hasFreshUnavailableOwner(rigId)){
    const peer=(JSON.parse(a.coordinators) as string[]).find(s=>s!==a.owner_session),peerGeneration=peer?this.authority.generation(peer):null;
    try{
     if(!peer||!peerGeneration)fail('coordinator_ineligible_recipient','Actual current Peer required');
     return this.db.transaction(()=>{const transferred=this.authority.transferObservedUnavailable(jobId,{rigId,expectedEpoch:a.epoch,expectedOwner:a.owner_session,expectedOwnerGeneration:a.owner_generation,planRevision:plan.revision,recipient:peer!,recipientGeneration:peerGeneration!,leaseMs:plan.acknowledgmentWindowMs??300000});
     const queueId='qitem-coordination-peer-'+digest(rigId+':'+transferred.epoch).slice(0,24);
     this.repo.createWithinTransaction({qitemId:queueId,sourceSession:'watchdog@system',destinationSession:peer!,body:JSON.stringify({action:'reconcile-transferred-baton',rigId,epoch:transferred.epoch,batonId:transferred.baton_id,deadline:transferred.lease_until,recipientGeneration:peerGeneration,required:'Read actual obligations and native identity; claim notice, then genuine Peer acknowledgment claims canonical baton. No product dispatch until acknowledgment.'}),identityProvenance:'system:operator-authorized-coordination',nudge:true});
     this.repo.stageWakeIntent(queueId,'watchdog@system',peer!,'system:operator-authorized-coordination',true,peerGeneration!);
     return [{key:'coordinator',state:'pending-peer-acknowledgment',queueId,reason:'fresh-native-unavailable-owner',deadline:transferred.lease_until}];})();
    }catch(error){const code=heldDispatchCode(error)??(error instanceof CoordinatorFenceError?error.code:undefined);if(!code)throw error;const deadline=this.now()+(code==='coordinator_held_history_recovery_required'?Math.min(plan.stallMs,1200000):plan.stallMs);const queueId=this.stageCoordinatorRecovery(rigId,a.epoch,plan.operatorGeneration,'recover-unavailable-coordinator',code,deadline,code==='coordinator_held_history_recovery_required'?this.authority.heldRecoveryAdmission(rigId,jobId):undefined);return [{key:'coordinator',state:'held',queueId,reason:code,deadline}];}
   }
   // A lapsed lease on a still-current, natively present holder is actionable now:
   // the supported bounded expiry recovery exists for exactly this, while the
   // idle-transfer fallback can only ever leave a bare takeover hold behind.
   if(a.state==='active'&&a.lease_until<=this.now()&&!this.authority.hasFreshUnavailableOwner(rigId)&&this.authority.generation(a.owner_session)===a.owner_generation&&this.nativeHolderPresent(a.owner_session,a.owner_generation)){
    // Re-read current authority: never precompute a holder, epoch or lease into a
    // duty. Any drift simply falls through to the existing fences below.
    const current=this.authority.get(rigId);
    if(current&&current.state==='active'&&current.owner_session===a.owner_session&&current.owner_generation===a.owner_generation&&current.epoch===a.epoch&&current.lease_until<=this.now()&&this.authority.generation(current.owner_session)===current.owner_generation){
     const recoveryWindowMs=Math.min(plan.acknowledgmentWindowMs??300000,900000);
     const deadline=this.now()+recoveryWindowMs;
     const queueId=this.stageCoordinatorRecovery(rigId,current.epoch,plan.operatorGeneration,'active-expiry-recover','expired-active-native-present-holder',deadline,undefined,{owner:current.owner_session,ownerGeneration:current.owner_generation,expectedLeaseUntil:current.lease_until,custodyDigest:this.authority.reconciliationDigest(rigId),recoveryWindowMs});
     return [{key:'coordinator',state:'recovery-required',queueId,reason:'expired-active-native-present-holder',deadline}];
    }
   }
   if(a.state==='active'&&this.now()-progress.at>=plan.stallMs&&plan.allowIdlePeerTransfer&&!this.authority.hasFreshUnavailableOwner(rigId)){
    const peers=(JSON.parse(a.coordinators) as string[]).filter(s=>s!==a.owner_session),peer=peers[0],peerGeneration=peer?this.authority.generation(peer):null;
    // No live working process or unknown telemetry is overridden. Transfer keeps
    // old worker claims/resources intact and requires genuine successor pickup.
    if(peer&&peerGeneration&&coordinationIdle(this.activity(peer),peerGeneration,this.now())&&coordinationIdle(this.activity(a.owner_session),a.owner_generation,this.now())) {
     try {
      return this.db.transaction(()=>{
       const transferred=this.authority.transferIdleStalled(jobId,{rigId,expectedEpoch:a.epoch,progressDigest:progress.digest,planRevision:plan.revision,recipient:peer,recipientGeneration:peerGeneration,leaseMs:plan.acknowledgmentWindowMs??300000});
       const queueId='qitem-coordination-peer-'+digest(rigId+':'+transferred.epoch).slice(0,24);
       this.repo.createWithinTransaction({qitemId:queueId,sourceSession:'watchdog@system',destinationSession:peer,body:JSON.stringify({action:'reconcile-transferred-baton',rigId,epoch:transferred.epoch,batonId:transferred.baton_id,deadline:transferred.lease_until,recipientGeneration:peerGeneration,required:'Read exact obligations and native current identity; acknowledge authority through supported coordinator API, then reconcile admitted frontier. This notice is not acknowledgment.'}),identityProvenance:'system:operator-authorized-coordination',nudge:true});
       this.repo.stageWakeIntent(queueId,'watchdog@system',peer,'system:operator-authorized-coordination',true,peerGeneration);
       return [{key:'coordinator',state:'pending-peer-acknowledgment',queueId,deadline:transferred.lease_until}];
      })();
     } catch(error) {
      const code=(error as {code?:string}).code,hold=heldDispatchCode(error)??(['coordination_stall_unproven','coordinator_uncertain_effects','coordinator_held_history_recovery_required'].includes(code??'')?code:undefined);
      if(!hold)throw error;
      // A safe takeover refusal is not a global work gate. Re-read authority before
      // fallback; never use a stale cached holder/epoch or imply successor pickup.
      const current=this.authority.get(rigId);
      if(!current||current.owner_session!==a.owner_session||current.owner_generation!==a.owner_generation||current.epoch!==a.epoch||current.state!=='active'||this.authority.generation(current.owner_session)!==current.owner_generation)throw error;
      if(current.lease_until<=this.now()){const deadline=this.now()+plan.stallMs,queueId=this.stageCoordinatorRecovery(rigId,a.epoch,plan.operatorGeneration,'recover-expired-idle-transfer',hold,deadline);return [{key:'coordinator',state:'held',queueId,reason:hold,deadline}];}
      const receipt={rigId,epoch:a.epoch,peer,reason:hold,owner:'operator-agent@kernel',action:'Reconcile exact Peer custody/transport/lifecycle fence and retry only after eligibility is proven',deadline:this.now()+plan.stallMs};
      this.db.prepare('INSERT OR IGNORE INTO coordinator_operations VALUES (?,?,?,?,?)').run(rigId,'coordination-takeover-hold:'+digest(JSON.stringify({revision:plan.revision,epoch:a.epoch,hold})), 'coordination-takeover-hold',JSON.stringify(receipt),digest(JSON.stringify(receipt)));
      const queueId=hold==='coordinator_held_history_recovery_required'?this.stageCoordinatorRecovery(rigId,current.epoch,plan.operatorGeneration,'restore-current-held-history-binding',hold,receipt.deadline):undefined;
      return [...this.reconcileScoped(current.owner_session,current.owner_generation,rigId,scope),{key:'coordinator',state:'held',...(queueId?{queueId}:{}),reason:hold,deadline:receipt.deadline}];
     }
    }
   }
   if(a.state==='reconciling'&&a.lease_until<=this.now()){
    const queueId='qitem-coordination-ack-recovery-'+digest(rigId+':'+a.epoch).slice(0,24);
    const existing=this.repo.getById(queueId);
    if(!existing)this.repo.createWithinTransaction({qitemId:queueId,sourceSession:'watchdog@system',destinationSession:'operator-agent@kernel',body:JSON.stringify({action:'recover-expired-reconciliation',rigId,epoch:a.epoch,recipient:a.owner_session,recipientGeneration:a.owner_generation,batonId:a.baton_id,required:'Inspect current native recipient and exact obligations; invoke reconciliation-recover once with current token and custody digest, then require genuine recipient acknowledge. If exhausted or identity uncertain, return concrete supported recovery boundary; do not fabricate lease or acknowledgment.'}),identityProvenance:'system:operator-authorized-coordination',nudge:true});
    this.repo.stageWakeIntent(queueId,'watchdog@system','operator-agent@kernel','system:operator-authorized-coordination',true,plan.operatorGeneration);
    return [{key:'coordinator',state:'pending-reconciliation-recovery',queueId,reason:'expired-unacknowledged-transfer',deadline:a.lease_until+(plan.acknowledgmentWindowMs??300000)}];
   }
   if(a.state!=='active'||a.lease_until<=this.now()){const deadline=this.now()+plan.stallMs,reason='current-holder-acknowledgment-or-lease';const queueId=this.stageCoordinatorRecovery(rigId,a.epoch,plan.operatorGeneration,'reconcile-current-coordinator-lease',reason,deadline);return [{key:'coordinator',state:'recovery-required',queueId,reason,deadline}];}
   const result=this.reconcileScoped(a.owner_session,a.owner_generation,rigId,scope);if(heldIntake)this.stageHeldHistoryAuthoring(rigId,heldIntake.qitem_id);this.advanceTaskHoldIntakes(rigId,jobId,result);return this.retireForAuthorizedObserver(rigId,plan.operatorGeneration,result);
  }).immediate();
 }

}
