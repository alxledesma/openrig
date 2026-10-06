import type Database from "better-sqlite3";
import type { PackageContract } from "./coordinator-authority-service.js";
import { CoordinatorFenceError, digest } from "./coordinator-authority-service.js";
import type { CoordinationPlan, CoordinationResult, CoordinationTask } from "./coordination-recovery-service.js";

/**
 * Product-frontier planning lifecycle (review Appendix B).
 *
 * This module owns every frontier-planning decision and none of the shared
 * finite-duty mechanism. It reads durable rows, classifies the frontier, and
 * mints duties exclusively through `seam.issueLifecycleDuty`, so adding a kind
 * here is a kind-table row rather than a second duty engine. It creates no
 * package, edits no plan, accepts nothing, qualifies nothing and dispatches
 * nothing: every product fact it reports was already admitted by a genuine
 * Operator or already returned by a genuine worker.
 */

/** Kind-table rows. `dutyKinds` carries the binding/effect pair; the Complete
 *  and Act facets are the `*Postcondition` / `*ActAllowed` counterparts below. */
export const PLANNING_DUTY_KIND = "frontier-planning";
export const ADMISSION_DUTY_KIND = "frontier-admission";
export const CONFIRMATION_DUTY_KIND = "frontier-confirmation";
/** Reserved package keys. They are never registered products; they only give
 *  the shared duty mechanism a scope key for checkpoint dispatch restrictions. */
export const PLANNING_DUTY_PACKAGE_KEY = "frontier-planning";
export const ADMISSION_DUTY_PACKAGE_KEY = "frontier-admission";
export const CONFIRMATION_DUTY_PACKAGE_KEY = "frontier-confirmation";
export type WorkClass = "product"|"recovery"|"administrative"|"inquiry";
export const WORK_CLASSES:readonly WorkClass[] = ["product","recovery","administrative","inquiry"];
/** The existing owner boundary vocabulary plus the missing-scope boundary. */
export const FRONTIER_BOUNDARIES = ["owner-access","owner-credential","owner-material","owner-irreversible","scope-source-missing"] as const;
export type FrontierBoundary = typeof FRONTIER_BOUNDARIES[number];

export interface ScopeSource { ref:string; digest:string }

/** B2 stabilization floor. This is not a retry budget and not a new duty
 *  window: the duty reuses the shared finite-duty convention unchanged. Both
 *  values are genuine Operator plan configuration (`plan.frontierPlanning`);
 *  these are the review's stated minimums, applied when it is absent. */
export const DEFAULT_STABILIZATION_OBSERVATIONS = 2;
export const DEFAULT_STABILIZATION_MS = 300000;

export type FrontierState = "ACTIVE"|"AWAITING-ACCEPTANCE"|"PROTECTED-HOLD"|"MATERIALIZABLE"|"EXHAUSTED";
export interface FrontierPackageFact { packageKey:string; workClass:WorkClass; status:string; legacyClass:boolean }
/** S1: an old unclassified package the genuine Operator attested as administrative or inquiry work.
 *  It stays visible with its REAL status; only the planning flags ignore it. */
export interface FrontierExcludedFact { packageKey:string; attestedClass:"administrative"|"inquiry"; status:string; queueId:string|null; protectedReason:string; classificationId:string }
/** The exact assignment/claim the Operator observed when attesting. A null assignment is
 *  an explicit observation too. Any change to it voids the attestation. */
export interface LegacyObservation { queueId:string|null; claimedByGeneration:string|null; claimedAt:string|null }
export interface FrontierStabilization { observations:number; since:number; requiredObservations:number; requiredMs:number; ready:boolean }
export interface FrontierSnapshot {
  rigId:string; state:FrontierState; reason:string;
  frontierDigest:string; scopeSources:ScopeSource[]; scopeSourcesDigest:string;
  packages:FrontierPackageFact[]; excluded:FrontierExcludedFact[]; accepted:Array<{queueId:string;dispositionId:string;evidenceRef:string}>;
  stabilization:FrontierStabilization; holder:string; holderGeneration:string; epoch:number; operatorGeneration:string;
}
export type FrontierDisposition = "plan-proposal"|"frontier-complete"|"frontier-blocked";
export interface ProposedPackage { packageKey:string; citations:ScopeSource[]; resources:string[]; returnContract:{destination:string;evidenceRequired:string[]} }
export interface ScopeMapping { ref:string; acceptedPackageKey?:string; deferral?:{reason:string;authorizationRef:string} }
export interface FrontierPlanReceipt {
  dutyQueueId:string; frontierDigest:string; scopeSourcesDigest:string; disposition:FrontierDisposition; actor:string; generation:string;
  proposal?:{packages:ProposedPackage[]; proposalDigest:string};
  mapping?:ScopeMapping[]; boundary?:FrontierBoundary; unblockCondition?:string;
  /** Identifies this exact attributed disposition so a reopen can reference it. */
  dispositionDigest?:string;
  /** frontier-complete only: the digest the current Operator independently confirms. */
  completionDigest?:string;
}
export interface FrontierAdmissionReceipt {
  dutyQueueId:string; proposalDigest:string; actor:string; generation:string;
  admitted:Array<{packageKey:string;contractHash:string;scopeCitations:ScopeSource[]}>; declined?:{reason:string};
}
export interface FrontierConfirmationReceipt { dutyQueueId:string; completionDigest:string; actor:string; generation:string; evidenceRef:string }
export interface FrontierReopenReceipt { dutyQueueId:string; dispositionDigest:string; reopenDigest:string; frontierDigest:string; actor:string; generation:string; evidenceRef:string; boundary?:FrontierBoundary }

export interface LegacyClassificationInput { rigId:string; packageKey:string; contractHash:string; workClass:string; evidenceRef:string; observed:LegacyObservation }
export interface LegacyRevocationInput { rigId:string; packageKey:string; contractHash:string; classificationId:string; evidenceRef:string }
export interface LegacyClassificationReceipt { operationId:string; rigId:string; packageKey:string; contractHash:string; workClass:"administrative"|"inquiry"; evidenceRef:string; observed:LegacyObservation; actor:string; generation:string; at:number; grantsAuthority:false }
export interface LegacyRevocationReceipt { operationId:string; rigId:string; packageKey:string; contractHash:string; classificationId:string; evidenceRef:string; actor:string; generation:string; at:number; grantsAuthority:false }

export type FrontierDutyKind = typeof PLANNING_DUTY_KIND|typeof ADMISSION_DUTY_KIND|typeof CONFIRMATION_DUTY_KIND;
/** The whole D10 integration surface, in one structural type. Everything the
 *  planner needs from the shared service is read-only except `issueLifecycleDuty`
 *  (kind-table rows), `actAllowed` (the shared Act facet, never reimplemented
 *  here) and `admitPackage` (the existing supported Operator admission, never
 *  reached without a genuine Operator caller). */
export interface FrontierPlanningSeam {
  readonly db:Database.Database;
  now():number;
  generation(session:string):string|null;
  authorityRecord(rigId:string):{owner_session:string;owner_generation:string;epoch:number;state:string;lease_until:number}|null;
  plan(rigId:string):CoordinationPlan|null;
  lifecycleControlCompleted(queueId:string):boolean;
  /** The shared duty Act facet for this exact duty and claimant. D10: dutyFacts().act */
  actAllowed(queueId:string,actor:string,generation:string):boolean;
  issueLifecycleDuty(input:{rigId:string;kind:FrontierDutyKind;packageKey:string;recipient:string;recipientGeneration:string;semanticKey:string;details:Record<string,unknown>}):CoordinationResult;
  admittedNow(task:CoordinationTask):boolean;
  dispatchScopeHold(plan:CoordinationPlan,task:CoordinationTask):string|null;
  effectDebt(session:string):boolean;
  requiresRecovery(rigId:string,packageKey:string):boolean;
  admitPackage(actor:string,generation:string,rigId:string,packageKey:string,contract:PackageContract&{workClass?:WorkClass;scopeCitations?:ScopeSource[]}):void;
}

const fail:(code:string,message:string)=>never=(code,message)=>{throw new CoordinatorFenceError(code,message);};
const text=(value:unknown):value is string=>typeof value==="string"&&value.trim().length>0;
const successfulReturn=(state:string,disposition:string|null):boolean=>!!disposition&&['done','handed-off'].includes(state);
/** Each disposition accepts exactly its own payload and nothing smuggled beside it. */
const PAYLOAD_BY_DISPOSITION:Record<string,readonly string[]>={'plan-proposal':['proposal'],'frontier-complete':['mapping'],'frontier-blocked':['boundary','unblockCondition']};
const SHA256=/^[0-9a-f]{64}$/;
function canonical(value:unknown):string {
 if(Array.isArray(value))return `[${value.map(canonical).join(",")}]`;
 if(value!==null&&typeof value==="object")return `{${Object.keys(value as Record<string,unknown>).sort().map(k=>`${JSON.stringify(k)}:${canonical((value as Record<string,unknown>)[k])}`).join(",")}}`;
 return JSON.stringify(value);
}
/** Operator order is presentation, not content: a re-listed source is not drift. */
export const scopeSourcesDigest=(sources:readonly ScopeSource[]):string=>digest(canonical([...sources].map(s=>({ref:s.ref,digest:s.digest})).sort((a,b)=>a.ref<b.ref?-1:a.ref>b.ref?1:0)));

export class FrontierPlanning {
 constructor(private readonly seam:FrontierPlanningSeam) {}

 // ---------------------------------------------------------------- projection

 /** Exact scope sources the genuine current Operator configured on the plan. */
 scopeSources(plan:CoordinationPlan|null):ScopeSource[] {
  return (plan?.scopeSources??[]).map(s=>({ref:s.ref,digest:s.digest}));
 }

 private stabilization(plan:CoordinationPlan|null):{observations:number;requiredObservations:number;requiredMs:number} {
  const configured=plan?.frontierPlanning;
  const observations=configured?.stabilizationObservations??DEFAULT_STABILIZATION_OBSERVATIONS;
  const ms=configured?.stabilizationMs??DEFAULT_STABILIZATION_MS;
  return {observations:Math.max(1,observations),requiredObservations:Math.max(1,observations),requiredMs:Math.max(0,ms)};
 }

 /** B2 classification. Read-only, deterministic, no duty and no row written. */
 frontier(rigId:string):FrontierSnapshot|null {
  const db=this.seam.db,plan=this.seam.plan(rigId),authority=this.seam.authorityRecord(rigId);
  if(!plan||!authority)return null;
  const tasks=new Map(plan.tasks.map(t=>[t.packageKey,t]));
  const acceptedRows=db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='coordination-accept' ORDER BY operation_id").all(rigId) as Array<{receipt:string}>;
  const accepted=acceptedRows.map(row=>{const r=JSON.parse(row.receipt) as {queueId:string;dispositionId:string;evidenceRef:string};return {queueId:r.queueId,dispositionId:r.dispositionId,evidenceRef:r.evidenceRef};});
  const rows=db.prepare(`SELECT p.package_key,p.contract,p.contract_hash,a.queue_id,a.disposition_id,a.destination,q.state,q.claimed_by_generation_uuid,q.claimed_at
   FROM coordinator_packages p
   LEFT JOIN coordinator_assignments a ON a.rig_id=p.rig_id AND a.package_key=p.package_key
   LEFT JOIN queue_items q ON q.qitem_id=a.queue_id
   WHERE p.rig_id=? ORDER BY p.package_key`).all(rigId) as Array<{package_key:string;contract:string;contract_hash:string;queue_id:string|null;disposition_id:string|null;destination:string|null;state:string|null;claimed_by_generation_uuid:string|null;claimed_at:string|null}>;
  const attestations=this.effectiveClassifications(rigId);

  const flags={ACTIVE:false,AWAITING_ACCEPTANCE:false,PROTECTED_HOLD:false,MATERIALIZABLE:false};
  const packages:FrontierPackageFact[]=[],excluded:FrontierExcludedFact[]=[],digestPackages:Array<{packageKey:string;workClass:WorkClass;status:string}>=[],classified:Array<{packageKey:string;workClass:string;classificationId:string}>=[];
  for(const row of rows){
   const task=tasks.get(row.package_key),contract=(JSON.parse(row.contract) as PackageContract&{workClass?:string}).workClass;
   const workClass:WorkClass=WORK_CLASSES.includes(contract as WorkClass)?contract as WorkClass:"product";
   const legacyClass=contract===undefined;
   // Non-product work is administrative. It never supports product classification.
   // S1: only an exact, still-matching Operator attestation for THIS stored contract and THIS
   // observed assignment/claim lifts a legacy package out of the planning flags. Never a name.
   const attested=legacyClass?attestations.get(row.package_key+':'+row.contract_hash):undefined;
   const live=attested&&this.sameObservation(attested.observed,{queueId:row.queue_id,claimedByGeneration:row.claimed_by_generation_uuid,claimedAt:row.claimed_at})?attested:undefined;
   const administrative=!!live||(!legacyClass&&(workClass==="administrative"||workClass==="inquiry"));
   // Recovery backups are dormant, not work: accepted target, no semantic
   // recovery requirement, and no assignment, stage, resource or live queue.
   const dormantBackup=!!task?.recoveryFor&&this.dormantBackup(rigId,task,plan);
   const status=this.packageStatus(rigId,row,task);
   packages.push({packageKey:row.package_key,workClass:live?live.workClass:workClass,status,legacyClass});
   // S4: excluded administrative or dormant work is shown but never part of planning identity,
   // so its status churn cannot mint a duty or reset stabilization.
   if(live){
    classified.push({packageKey:row.package_key,workClass:live.workClass,classificationId:live.operationId});
    excluded.push({packageKey:row.package_key,attestedClass:live.workClass,status,queueId:row.queue_id,classificationId:live.operationId,protectedReason:this.protectedReason(status,row,task)});
   }
   if(!administrative&&!dormantBackup)digestPackages.push({packageKey:row.package_key,workClass,status});
   if(administrative||dormantBackup||status==="accepted")continue;
   if(status==="picked-up"||status==="pending-pickup"){flags.ACTIVE=true;continue;}
   if(status==="awaiting-acceptance"){flags.AWAITING_ACCEPTANCE=true;continue;}
   if(status==="held"){flags.PROTECTED_HOLD=true;continue;}
   if(status==="unplanned"){flags.MATERIALIZABLE=true;continue;}
   // A registered, unplanned, non-backup package with no class keeps its
   // existing materialization duty, but never supports a completeness finding.
   if(legacyClass&&!task)flags.MATERIALIZABLE=true;
  }
  // Product work in the plan without an admitted package row is still product work.
  for(const t of plan.tasks){
   if(rows.some(r=>r.package_key===t.packageKey))continue;
   if(this.dormantBackup(rigId,t,plan))continue;
   flags.PROTECTED_HOLD=true;
  }
  const state:FrontierState=flags.ACTIVE?"ACTIVE":flags.AWAITING_ACCEPTANCE?"AWAITING-ACCEPTANCE":flags.PROTECTED_HOLD?"PROTECTED-HOLD":flags.MATERIALIZABLE?"MATERIALIZABLE":"EXHAUSTED";
  const reason=state==="ACTIVE"?"admitted-product-work-in-flight":state==="AWAITING-ACCEPTANCE"?"typed-return-awaits-acceptance":state==="PROTECTED-HOLD"?"product-work-held-by-protection":state==="MATERIALIZABLE"?"registered-product-package-awaiting-materialization":"no-authorized-product-frontier-remains";
  const sources=this.scopeSources(plan),sourcesDigest=scopeSourcesDigest(sources);
  const frontierDigest=digest(canonical({rigId,packages:digestPackages,...(classified.length?{classified}:{}),accepted:accepted.map(a=>[a.queueId,a.dispositionId]),scopeSourcesDigest:sourcesDigest}));
  const run=this.observationRun(rigId,frontierDigest,authority.epoch,state);
  const limits=this.stabilization(plan);
  return {rigId,state,reason,frontierDigest,scopeSources:sources,scopeSourcesDigest:sourcesDigest,packages,excluded,accepted,
   stabilization:{observations:run.observations,since:run.since,requiredObservations:limits.requiredObservations,requiredMs:limits.requiredMs,ready:run.observations>=limits.requiredObservations&&this.seam.now()-run.since>=limits.requiredMs},
   holder:authority.owner_session,holderGeneration:authority.owner_generation,epoch:authority.epoch,operatorGeneration:plan.operatorGeneration};
 }

 private packageStatus(rigId:string,row:{package_key:string;queue_id:string|null;disposition_id:string|null;destination:string|null;state:string|null;claimed_by_generation_uuid:string|null},task:CoordinationTask|undefined):string {
  if(!row.queue_id)return !task?'unplanned':this.taskProtected(rigId,task)?'held':'pending-pickup';
  if(successfulReturn(row.state!,row.disposition_id))return this.exactAccepted(rigId,row.queue_id,row.disposition_id!)?"accepted":"awaiting-acceptance";
  if(['failed','denied','canceled'].includes(row.state!))return task&&task.boundary?"held":"awaiting-acceptance";
  if(!row.disposition_id&&['done','handed-off','failed','denied','canceled'].includes(row.state!))return this.taskProtected(rigId,task)?"held":"awaiting-acceptance";
  if(['pending','in-progress','blocked'].includes(row.state!)){
   // A blocked assignment that no current plan task owns has no dispatch path and no per-task
   // protection: it is an honest local hold, never a fictitious pending pickup.
   if(!task&&row.state==='blocked')return "held";
   if(this.taskProtected(rigId,task))return "held";
   if(row.state==='in-progress'&&row.claimed_by_generation_uuid&&this.seam.generation(row.destination??'')===row.claimed_by_generation_uuid)return "picked-up";
   if(row.state==='in-progress'&&!row.claimed_by_generation_uuid)return "held";
   return "pending-pickup";
  }
  return "held";
 }

 /** Product work held by admission expiry, effect debt, quiescence, reservation
  * or an owner boundary. This is repair, never planning. */
 private taskProtected(rigId:string,task:CoordinationTask|undefined):boolean {
  if(!task)return false;
  if(task.boundary)return true;
  if(this.seam.dispatchScopeHold(this.seam.plan(rigId)!,task))return true;
  if(!this.seam.admittedNow(task))return true;
  return this.seam.effectDebt(task.owner);
 }

 /** The local reason an excluded package is still protected. Report only; nothing repairs or retires. */
 private protectedReason(status:string,row:{queue_id:string|null;state:string|null},task:CoordinationTask|undefined):string {
  if(row.queue_id&&!task&&row.state==='blocked')return 'protected-no-plan-task';
  if(status==='held')return task?.boundary?'owner-boundary':'held-by-local-protection';
  if(status==='awaiting-acceptance')return 'typed-return-awaits-acceptance';
  if(status==='picked-up'||status==='pending-pickup')return 'assignment-in-flight';
  if(status==='unplanned')return 'registered-without-assignment';
  return status;
 }

 private sameObservation(a:LegacyObservation,b:LegacyObservation):boolean {
  return a.queueId===b.queueId&&a.claimedByGeneration===b.claimedByGeneration&&a.claimedAt===b.claimedAt;
 }

 /** The assignment and claim incarnation as they are right now. A package with no assignment
  *  observes `{null,null,null}`; that is an explicit observation, not an absent one. */
 private currentObservation(rigId:string,packageKey:string):LegacyObservation {
  const r=this.seam.db.prepare("SELECT a.queue_id,q.claimed_by_generation_uuid,q.claimed_at FROM coordinator_assignments a LEFT JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? AND a.package_key=?").get(rigId,packageKey) as {queue_id:string;claimed_by_generation_uuid:string|null;claimed_at:string|null}|undefined;
  return r?{queueId:r.queue_id,claimedByGeneration:r.claimed_by_generation_uuid??null,claimedAt:r.claimed_at??null}:{queueId:null,claimedByGeneration:null,claimedAt:null};
 }

 /** Append-only history for one (package, stored contract hash): classifications in write order and
  *  the revocations that name them. The active attestation is the latest classification that no
  *  revocation names. */
 private classificationHistory(rigId:string,packageKey?:string,contractHash?:string):{classes:LegacyClassificationReceipt[];revoked:Map<string,LegacyRevocationReceipt>} {
  const db=this.seam.db,parse=<T>(rows:Array<{receipt:string}>):T[]=>rows.flatMap(r=>{try{return [JSON.parse(r.receipt) as T];}catch{return [];}});
  const classes=parse<LegacyClassificationReceipt>(db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='frontier-legacy-classification' ORDER BY rowid").all(rigId) as Array<{receipt:string}>).filter(c=>(!packageKey||c.packageKey===packageKey)&&(!contractHash||c.contractHash===contractHash));
  const revoked=new Map<string,LegacyRevocationReceipt>();
  for(const r of parse<LegacyRevocationReceipt>(db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='frontier-legacy-classification-revocation' ORDER BY rowid").all(rigId) as Array<{receipt:string}>))revoked.set(r.classificationId,r);
  return {classes,revoked};
 }

 /** Active attestations keyed `packageKey:contractHash`. Matching the CURRENT observation is the
  *  caller's job (frontier() compares it per row), so a drifted assignment simply stops applying. */
 private effectiveClassifications(rigId:string):Map<string,LegacyClassificationReceipt&{observed:LegacyObservation}> {
  const {classes,revoked}=this.classificationHistory(rigId),active=new Map<string,LegacyClassificationReceipt>();
  for(const c of classes)active.set(c.packageKey+':'+c.contractHash,c);
  for(const [key,c] of [...active])if(revoked.has(c.operationId))active.delete(key);
  return active as Map<string,LegacyClassificationReceipt&{observed:LegacyObservation}>;
 }

 /** `dormantRecoveryHistory` expressed over the same durable rows: an accepted
  * target that needs no semantic recovery and left no live or reserved trace. */
 private dormantBackup(rigId:string,task:CoordinationTask,plan:CoordinationPlan):boolean {
  const target=plan.tasks.find(parent=>parent.key===task.recoveryFor);
  if(!target||!this.acceptedTaskHistory(rigId,target)||this.seam.requiresRecovery(rigId,target.packageKey))return false;
  const id='qitem-coordination-'+digest(rigId+':'+task.packageKey).slice(0,24),db=this.seam.db;
  return !db.prepare("SELECT 1 FROM coordinator_assignments WHERE rig_id=? AND package_key=?").get(rigId,task.packageKey)
   &&!db.prepare("SELECT 1 FROM coordinator_stage_assignments WHERE rig_id=? AND package_key=?").get(rigId,task.packageKey)
   &&!db.prepare("SELECT 1 FROM coordinator_resources WHERE rig_id=? AND package_key=?").get(rigId,task.packageKey)
   &&!db.prepare("SELECT 1 FROM queue_items WHERE qitem_id=? AND state IN ('pending','in-progress','blocked')").get(id);
 }

 private acceptedTaskHistory(rigId:string,task:CoordinationTask):boolean {
  const r=this.seam.db.prepare("SELECT a.queue_id,a.disposition_id,a.body_hash,a.destination,q.body,q.state,p.contract FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id JOIN coordinator_packages p ON p.rig_id=a.rig_id AND p.package_key=a.package_key WHERE a.rig_id=? AND a.package_key=?").get(rigId,task.packageKey) as {queue_id:string;disposition_id:string|null;body_hash:string;destination:string;body:string;state:string;contract:string}|undefined;
  if(!r||r.destination!==task.owner||r.body_hash!==digest(task.body)||digest(r.body)!==r.body_hash)return false;
  const c=JSON.parse(r.contract) as PackageContract;
  if(c.destination!==r.destination||c.bodyHash!==r.body_hash)return false;
  return successfulReturn(r.state,r.disposition_id)&&this.exactAccepted(rigId,r.queue_id,r.disposition_id!);
 }

 private exactAccepted(rigId:string,queueId:string,dispositionId:string):boolean {
  return !!this.seam.db.prepare("SELECT 1 FROM coordinator_operations WHERE rig_id=? AND kind='coordination-accept' AND json_extract(receipt,'$.queueId')=? AND json_extract(receipt,'$.dispositionId')=?").get(rigId,queueId,dispositionId);
 }

 /** An accepted package key is one this rig actually admitted and accepted. */
 private acceptedScopeClasses(rigId:string):Map<string,{workClass:string;scopeCitations:Array<{ref:string;digest:string}>}> {
  const classes=new Map<string,{workClass:string;scopeCitations:Array<{ref:string;digest:string}>}>();
  const rows=this.seam.db.prepare("SELECT a.package_key,a.queue_id,a.disposition_id,p.contract FROM coordinator_assignments a JOIN coordinator_packages p ON p.rig_id=a.rig_id AND p.package_key=a.package_key WHERE a.rig_id=? AND a.disposition_id IS NOT NULL").all(rigId) as Array<{package_key:string;queue_id:string;disposition_id:string;contract:string}>;
  for(const row of rows){
   if(!this.exactAccepted(rigId,row.queue_id,row.disposition_id))continue;
   const c=JSON.parse(row.contract) as PackageContract&{workClass?:string;scopeCitations?:Array<{ref:string;digest:string}>};
   classes.set(row.package_key,{workClass:WORK_CLASSES.includes(c.workClass as WorkClass)?c.workClass as WorkClass:'legacy',scopeCitations:Array.isArray(c.scopeCitations)?c.scopeCitations:[]});
  }
  return classes;
 }

 // -------------------------------------------------------------- observation

 private control(rigId:string,kind:string,queueId:string):any|null {
   const row=this.seam.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='coordinator-lifecycle-control'").get(rigId,queueId) as {receipt:string}|undefined;
   if(!row)return null;
   let receipt:any;try{receipt=JSON.parse(row.receipt);}catch{return null;}
   return receipt.kind===kind?receipt:null;
 }

 /** Trailing run of observations for the exact current (digest, epoch, state).
  *  Any change to one of those three ends the run, so a transient gap between
  *  acceptance and the next reconcile can never accumulate into an obligation.
  *  The query is keyed, never a full-table scan. */
 private observationRun(rigId:string,frontierDigest:string,epoch:number,state:string):{observations:number;since:number} {
  const rows=this.seam.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='frontier-observation' AND json_extract(receipt,'$.frontierDigest')=? AND json_extract(receipt,'$.epoch')=? AND json_extract(receipt,'$.state')=? ORDER BY rowid").all(rigId,frontierDigest,epoch,state) as Array<{receipt:string}>;
  let since=this.seam.now();
  for(let i=rows.length-1;i>=0;i--){let o:unknown;try{o=JSON.parse(rows[i]!.receipt);}catch{break;}if(o&&typeof o==='object'&&'observedAt' in o&&typeof o.observedAt==='number')since=o.observedAt;}
  return {observations:rows.length,since};
 }

 /** Append-only census, bounded by construction. Observations are written only
  *  until the stabilization decision for this exact (digest, epoch, state) is
  *  durable plus one decision row, so a permanently exhausted rig stops growing. */
 private observe(snapshot:FrontierSnapshot):void {
  const db=this.seam.db,now=this.seam.now(),run=this.observationRun(snapshot.rigId,snapshot.frontierDigest,snapshot.epoch,snapshot.state);
  const ceiling=this.stabilization(this.seam.plan(snapshot.rigId)).requiredObservations+1;
  if(run.observations>=ceiling)return;
  if(run.observations&&now===run.since)return;
  const receipt={rigId:snapshot.rigId,state:snapshot.state,frontierDigest:snapshot.frontierDigest,scopeSourcesDigest:snapshot.scopeSourcesDigest,epoch:snapshot.epoch,holder:snapshot.holder,holderGeneration:snapshot.holderGeneration,observedAt:now,reason:snapshot.reason,grantsAuthority:false};
  db.prepare("INSERT OR IGNORE INTO coordinator_operations VALUES (?,?,?,?,?)").run(snapshot.rigId,'frontier-observation:'+digest(snapshot.rigId+':'+snapshot.frontierDigest+':'+snapshot.epoch+':'+snapshot.state+':'+now),'frontier-observation',JSON.stringify(receipt),digest(JSON.stringify(receipt)));
 }


 private planningDutyResult(snapshot:FrontierSnapshot,reopenDigest?:string):CoordinationResult {
  const recipient=snapshot.holder,recipientGeneration=snapshot.holderGeneration;
  const details:Record<string,unknown>={
   frontierDigest:snapshot.frontierDigest,scopeSources:snapshot.scopeSources,scopeSourcesDigest:snapshot.scopeSourcesDigest,
   frontierState:snapshot.state,epoch:snapshot.epoch,grantsAuthority:false,...(reopenDigest?{reopenDigest}:{}),...(snapshot.excluded.length?{excludedProtected:snapshot.excluded}:{}),
   planningContract:{
    dispositions:["plan-proposal","frontier-complete","frontier-blocked"],
    recordOperation:"coordination-frontier-plan",
    body:{rigId:snapshot.rigId,frontierDigest:snapshot.frontierDigest,dutyQueueId:'<this exact duty queue item ID>',disposition:'<plan-proposal|frontier-complete|frontier-blocked>',proposal:'<only for plan-proposal>',mapping:'<only for frontier-complete>',boundary:'<only for frontier-blocked>',unblockCondition:'<only for frontier-blocked>'},
    scopeSources:snapshot.scopeSources.length?snapshot.scopeSources:[]
   }};
  return this.seam.issueLifecycleDuty({rigId:snapshot.rigId,kind:PLANNING_DUTY_KIND,packageKey:PLANNING_DUTY_PACKAGE_KEY,recipient,recipientGeneration,semanticKey:reopenDigest?`${snapshot.frontierDigest}:reopen:${reopenDigest}`:snapshot.frontierDigest,details});
 }

 private admissionDutyResult(snapshot:FrontierSnapshot,receipt:FrontierPlanReceipt):CoordinationResult {
   const operatorGeneration=this.seam.generation('operator-agent@kernel');
   if(!operatorGeneration)return {key:'frontier-admission',state:'held',reason:'frontier-operator-absent',deadline:this.seam.now()};
   return this.seam.issueLifecycleDuty({rigId:snapshot.rigId,kind:ADMISSION_DUTY_KIND,packageKey:ADMISSION_DUTY_PACKAGE_KEY,recipient:'operator-agent@kernel',recipientGeneration:operatorGeneration,semanticKey:receipt.proposal!.proposalDigest,
    details:{proposalDigest:receipt.proposal!.proposalDigest,frontierDigest:receipt.frontierDigest,planningQueueId:receipt.dutyQueueId,proposal:receipt.proposal!.packages,...(snapshot.excluded.length?{excludedProtected:snapshot.excluded}:{}),
     admissionContract:{recordOperation:"coordination-frontier-admit",body:{rigId:snapshot.rigId,proposalDigest:receipt.proposal!.proposalDigest,dutyQueueId:'<this exact duty queue item ID>',admitted:'<every proposed packageKey with its exact admitted contract>',declined:'<only when not admitting>'}}}});
 }

 // ------------------------------------------------------------- central pass

 /** Emitted by `centralLifecyclePass`. One obligation at most, and never a
  *  second one while the first is live. */
 pass(rigId:string):CoordinationResult[] {
  const snapshot=this.frontier(rigId);
  if(!snapshot)return [];
  this.observe(snapshot);
  if(snapshot.state!=="EXHAUSTED")return [];
  const disposition=this.activeDisposition(rigId,snapshot.frontierDigest);
  if(disposition?.disposition==='frontier-complete'){
   const confirmed=this.confirmedCompletion(rigId,disposition);
   if(confirmed)return [{key:'frontier',state:'frontier-complete',queueId:disposition.dutyQueueId,reason:'complete-as-of:'+snapshot.scopeSourcesDigest,deadline:this.seam.now()}];
   return this.confirmationResult(snapshot,disposition);
  }
  if(disposition?.disposition==='frontier-blocked'){
   this.recordBoundaryIntake(rigId,'blocked',disposition.dutyQueueId,disposition.boundary,disposition.unblockCondition??'genuine current Operator records the unblock fact');
   return [{key:'frontier',state:'held',queueId:disposition.dutyQueueId,reason:'frontier-boundary-blocked',deadline:this.seam.now(),activityEvidence:{boundary:disposition.boundary??null,unblockCondition:disposition.unblockCondition??null},subject:this.subjectOf(PLANNING_DUTY_PACKAGE_KEY,snapshot.holder,disposition.dispositionDigest??disposition.frontierDigest)}];
  }
  if(disposition?.disposition==='plan-proposal'){
   const result=this.admissionResult(snapshot,disposition);
   if(result[0]?.reason==='frontier-boundary-declined'){this.recordBoundaryIntake(rigId,'declined',disposition.dutyQueueId,undefined,`attributed refusal: ${String(result[0]!.activityEvidence?.declineReason??'unspecified')}. Genuine current Operator records a fresh proposal disposition.`);result[0]!.subject=this.subjectOf(ADMISSION_DUTY_PACKAGE_KEY,'operator-agent@kernel',disposition.proposal!.proposalDigest);}
   return result;
  }
  // A recorded proposal for this accepted scope that has no admission outcome yet is still THE
  // obligation, even when the frontier digest has genuinely moved since it was recorded. A second
  // planning duty here would invite a second live proposal for the same work.
  const pending=this.openProposals(rigId,snapshot.scopeSourcesDigest)[0];
  if(pending)return this.admissionResult(snapshot,pending);
  if(!snapshot.stabilization.ready)return [{key:'frontier',state:'stabilizing',reason:'frontier-stabilization-pending',deadline:this.seam.now(),activityEvidence:{frontierDigest:snapshot.frontierDigest,observations:snapshot.stabilization.observations,requiredObservations:snapshot.stabilization.requiredObservations,elapsedMs:this.seam.now()-snapshot.stabilization.since,requiredMs:snapshot.stabilization.requiredMs}}];
  const duty=this.planningDutyResult(snapshot,this.reopenByDigest(rigId,snapshot.frontierDigest)?.reopenDigest);
  if(duty.state==='held'&&duty.reason==='lifecycle-duty-exhausted')return [{key:'frontier',state:'held',queueId:duty.queueId,reason:'frontier-planning-duty-exhausted',deadline:duty.deadline,activityEvidence:{accountableBoundary:'operator-agent@kernel',escalation:'frontier-planning-exhausted',frontierDigest:snapshot.frontierDigest},subject:this.subjectOf(PLANNING_DUTY_PACKAGE_KEY,snapshot.holder,snapshot.frontierDigest)}];
  return [{...duty,key:'frontier',subject:this.subjectOf(PLANNING_DUTY_PACKAGE_KEY,snapshot.holder,snapshot.frontierDigest)}];
 }
 /** The accountable intake subject of a frontier hold: the reserved duty package
  *  key, the exact native owner, and the frozen frontier/proposal/completion
  *  identity. The shared task-hold intake stages it; this is never a second queue
  *  engine, and the identity keeps two different obligations from deduplicating. */
 private subjectOf(packageKey:string,owner:string,identity:string):{packageKey:string;owner:string;identity:string} {
  return {packageKey,owner,identity};
 }


 /** A disposition the current Operator has already reopened is no longer terminal. */
 private activeDisposition(rigId:string,frontierDigest:string):FrontierPlanReceipt|null {
  const disposition=this.planningDisposition(rigId,frontierDigest);
  if(!disposition)return null;
  if(this.reopenByDisposition(rigId,disposition.dispositionDigest))return null;
  if(disposition.proposal&&this.reopenByDisposition(rigId,disposition.proposal.proposalDigest))return null;
  return disposition;
 }

 /** The Lead's completion stands only once the genuine current Operator has
  *  independently confirmed that exact completion digest **on that exact planning
  *  duty**. A confirmation never carries across a reopen: a reopen issues a
  *  distinct successor planning duty with a distinct id, so re-recording an
  *  identical mapping still requires a fresh Operator confirmation. */
 private confirmedCompletion(rigId:string,receipt:FrontierPlanReceipt):FrontierConfirmationReceipt|null {
  const row=this.seam.db.prepare("SELECT operation_id FROM coordinator_operations WHERE rig_id=? AND kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='frontier-confirmation' AND json_extract(receipt,'$.completionDigest')=? AND json_extract(receipt,'$.planningQueueId')=? ORDER BY rowid DESC LIMIT 1").get(rigId,receipt.completionDigest,receipt.dutyQueueId) as {operation_id:string}|undefined;
  const recorded=row?this.confirmationDisposition(rigId,row.operation_id):null;
  return recorded&&recorded.completionDigest===receipt.completionDigest?recorded:null;
 }

 private confirmationResult(snapshot:FrontierSnapshot,receipt:FrontierPlanReceipt):CoordinationResult[] {
  const operatorGeneration=this.seam.generation('operator-agent@kernel');
  if(!operatorGeneration)return [{key:'frontier',state:'held',reason:'frontier-operator-absent',deadline:this.seam.now(),subject:this.subjectOf(CONFIRMATION_DUTY_PACKAGE_KEY,'operator-agent@kernel',`${receipt.completionDigest}:${receipt.dutyQueueId}`)}];
  const duty=this.seam.issueLifecycleDuty({rigId:snapshot.rigId,kind:CONFIRMATION_DUTY_KIND,packageKey:CONFIRMATION_DUTY_PACKAGE_KEY,recipient:'operator-agent@kernel',recipientGeneration:operatorGeneration,semanticKey:`${receipt.completionDigest}:${receipt.dutyQueueId}`,
   details:{completionDigest:receipt.completionDigest,frontierDigest:receipt.frontierDigest,planningQueueId:receipt.dutyQueueId,scopeSourcesDigest:receipt.scopeSourcesDigest,mapping:receipt.mapping,grantsAuthority:false,
    confirmationContract:{recordOperation:"coordination-frontier-confirm",body:{rigId:snapshot.rigId,completionDigest:receipt.completionDigest,dutyQueueId:'<this exact duty queue item ID>',evidenceRef:'<actual independent confirmation evidence>'}}}});
  return [{...duty,key:'frontier',subject:this.subjectOf(CONFIRMATION_DUTY_PACKAGE_KEY,'operator-agent@kernel',`${receipt.completionDigest}:${receipt.dutyQueueId}`)}];
 }

 private reopenByDisposition(rigId:string,dispositionDigest:string|undefined):FrontierReopenReceipt|null {
  if(!dispositionDigest)return null;
  const row=this.seam.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='frontier-reopen' AND json_extract(receipt,'$.dispositionDigest')=? ORDER BY rowid DESC LIMIT 1").get(rigId,dispositionDigest) as {receipt:string}|undefined;
  if(!row)return null;try{return JSON.parse(row.receipt) as FrontierReopenReceipt;}catch{return null;}
 }

 private reopenByDigest(rigId:string,frontierDigest:string):FrontierReopenReceipt|null {
  const row=this.seam.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='frontier-reopen' AND json_extract(receipt,'$.frontierDigest')=? ORDER BY rowid DESC LIMIT 1").get(rigId,frontierDigest) as {receipt:string}|undefined;
  if(!row)return null;try{return JSON.parse(row.receipt) as FrontierReopenReceipt;}catch{return null;}
 }

 /** A blocked or declined disposition is never a silent permanent stall: the
  *  accountable boundary, its recorded unblock condition and the owning Operator
  *  are written once, durably. Routing to a queue intake stays D10's change. */
 private recordBoundaryIntake(rigId:string,reason:'blocked'|'declined',dutyQueueId:string,boundary:FrontierBoundary|undefined,condition:string):void {
  const receipt={rigId,reason,dutyQueueId,boundary:boundary??null,unblockCondition:condition,accountableSession:'operator-agent@kernel',grantsAuthority:false};
  this.seam.db.prepare("INSERT OR IGNORE INTO coordinator_operations VALUES (?,?,?,?,?)").run(rigId,'frontier-boundary-intake:'+dutyQueueId,'frontier-boundary-intake',JSON.stringify(receipt),digest(JSON.stringify(receipt)));
 }

 /** The Lead recorded a proposal; hand it to the current Operator through the
  *  independent admission step. No package is created here. */
 private admissionResult(snapshot:FrontierSnapshot,receipt:FrontierPlanReceipt):CoordinationResult[] {
  const duty=this.admissionDutyResult(snapshot,receipt),recorded=this.admissionDisposition(snapshot.rigId,duty.queueId);
  if(recorded)return recorded.declined
  ?[{key:'frontier',state:'held',queueId:duty.queueId,reason:'frontier-boundary-declined',deadline:duty.deadline,activityEvidence:{disposition:'frontier-admission-declined',proposalDigest:receipt.proposal!.proposalDigest,declineReason:recorded.declined.reason},subject:this.subjectOf(ADMISSION_DUTY_PACKAGE_KEY,'operator-agent@kernel',receipt.proposal!.proposalDigest)}]
  :[{key:'frontier',state:'frontier-admission-complete',queueId:duty.queueId,deadline:duty.deadline,activityEvidence:{proposalDigest:receipt.proposal!.proposalDigest}}];
  // Whatever the shared engine actually decided, reported verbatim: a refused issuance is a hold.
  return [{...duty,key:'frontier',subject:this.subjectOf(ADMISSION_DUTY_PACKAGE_KEY,'operator-agent@kernel',receipt.proposal!.proposalDigest)}];
 }

 /** Recorded, unreopened plan proposals whose admission has no recorded outcome, newest first.
  *  Found from durable receipts only; nothing is issued here. */
 private openProposals(rigId:string,scopeSourcesDigestValue?:string):FrontierPlanReceipt[] {
  const rows=this.seam.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='frontier-plan-disposition' ORDER BY rowid DESC").all(rigId) as Array<{receipt:string}>;
  const open:FrontierPlanReceipt[]=[];
  for(const row of rows){
   let r:FrontierPlanReceipt;try{r=JSON.parse(row.receipt) as FrontierPlanReceipt;}catch{continue;}
   if(r.disposition!=='plan-proposal'||!r.proposal)continue;
   if(scopeSourcesDigestValue!==undefined&&r.scopeSourcesDigest!==scopeSourcesDigestValue)continue;
   if(this.reopenByDisposition(rigId,r.dispositionDigest)||this.reopenByDisposition(rigId,r.proposal.proposalDigest))continue;
   if(this.admissionOutcomeRecorded(rigId,r.proposal.proposalDigest))continue;
   open.push(r);
  }
  return open;
 }

 /** Any admission duty for this exact proposal that already has an admitted or declined disposition. */
 private admissionOutcomeRecorded(rigId:string,proposalDigest:string):boolean {
  const duties=this.seam.db.prepare("SELECT operation_id FROM coordinator_operations WHERE rig_id=? AND kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='frontier-admission' AND json_extract(receipt,'$.proposalDigest')=?").all(rigId,proposalDigest) as Array<{operation_id:string}>;
  return duties.some(d=>!!this.admissionDisposition(rigId,d.operation_id));
 }

 /** A new proposal may introduce only keys nobody has registered, planned or already proposed. */
 private proposalKeysAvailable(rigId:string,dutyQueueId:string,packages:ProposedPackage[]):void {
  const db=this.seam.db,plan=this.seam.plan(rigId);
  const pendingKeys=new Set(this.openProposals(rigId).filter(r=>r.dutyQueueId!==dutyQueueId).flatMap(r=>r.proposal!.packages.map(p=>p.packageKey)));
  for(const p of packages){
   if(db.prepare("SELECT 1 FROM coordinator_packages WHERE rig_id=? AND package_key=?").get(rigId,p.packageKey)||plan?.tasks.some(t=>t.packageKey===p.packageKey))fail('frontier_proposal_package_exists','A new proposal may not reuse an already registered or planned package key');
   if(pendingKeys.has(p.packageKey))fail('frontier_proposal_package_pending','Another recorded proposal still awaiting admission already names this package key');
  }
 }

 /** Duties are found by their frozen digest, never by predicting the shared
  *  mechanism's queue id. A successor duty without its own disposition never
  *  hides the disposition recorded on an earlier duty for the same digest. */
 private planningDisposition(rigId:string,frontierDigest:string):FrontierPlanReceipt|null {
  const rows=this.seam.db.prepare("SELECT operation_id FROM coordinator_operations WHERE rig_id=? AND kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='frontier-planning' AND json_extract(receipt,'$.frontierDigest')=? ORDER BY rowid DESC").all(rigId,frontierDigest) as Array<{operation_id:string}>;
  for(const row of rows){const receipt=this.planningReceipt(rigId,row.operation_id);if(receipt)return receipt;}
  return null;
 }

 private admissionDisposition(rigId:string,queueId:string|undefined):FrontierAdmissionReceipt|null {
  if(!queueId)return null;
  const row=this.seam.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='frontier-admission-disposition'").get(rigId,'frontier-admission:'+queueId) as {receipt:string}|undefined;
  if(!row)return null;try{return JSON.parse(row.receipt) as FrontierAdmissionReceipt;}catch{return null;}
 }

 private planningReceipt(rigId:string,queueId:string):FrontierPlanReceipt|null {
  const row=this.seam.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='frontier-plan-disposition'").get(rigId,'frontier-disposition:'+queueId) as {receipt:string}|undefined;
  if(!row)return null;try{return JSON.parse(row.receipt) as FrontierPlanReceipt;}catch{return null;}
 }

 /** Every frozen scope item maps to an accepted **product** package that cites
  *  that exact scope ref, or to an explicit owner-attributed deferral carrying
  *  its authorization reference. Legacy or administrative accepted work can
  *  never support a completeness finding. */
 private completeMappingValid(rigId:string,receipt:FrontierPlanReceipt,scopeSources:ScopeSource[]):boolean {
  const mapping=receipt.mapping;
  if(!Array.isArray(mapping)||!mapping.length)return false;
  const frozen=new Map(scopeSources.map(s=>[s.ref,s.digest]));
  if(mapping.length!==frozen.size||new Set(mapping.map(m=>m.ref)).size!==frozen.size)return false;
  if(mapping.some(m=>!frozen.has(m.ref)))return false;
  const accepted=this.acceptedScopeClasses(rigId);
  return mapping.every(m=>(!!m.acceptedPackageKey&&this.scopeBoundAccepted(accepted,m.acceptedPackageKey,frozen.get(m.ref)!,m.ref))||(!!m.deferral&&text(m.deferral.reason)&&text(m.deferral.authorizationRef)));
 }

 private scopeBoundAccepted(accepted:Map<string,{workClass:string;scopeCitations:Array<{ref:string;digest:string}>}>,packageKey:string,digestValue:string,ref:string):boolean {
  const entry=accepted.get(packageKey);
  return !!entry&&entry.workClass==='product'&&entry.scopeCitations.some(c=>c.ref===ref&&c.digest===digestValue);
 }

 // ------------------------------------------------------------------ facets

 /** Complete facet for the Lead planning duty. */
 /** Complete facet for the Lead planning duty. Monotone: it is evaluated against
  *  the duty's own frozen scope sources, never against the current plan, so a
  *  recorded completion never turns incomplete under a later re-measurement. */
 planPostcondition(rigId:string,duty:any):boolean {
  const receipt=this.planningReceipt(rigId,duty.queueId);
  if(!receipt||receipt.frontierDigest!==duty.frontierDigest||receipt.scopeSourcesDigest!==duty.scopeSourcesDigest)return false;
  const scopeSources=(duty.scopeSources??[]) as ScopeSource[];
  if(receipt.disposition==='plan-proposal')return this.proposalValid(receipt.proposal?.packages??[],scopeSources);
  if(receipt.disposition==='frontier-complete')return scopeSources.length>0&&!!receipt.completionDigest&&this.completeMappingValid(rigId,receipt,scopeSources);
  if(receipt.disposition!=='frontier-blocked')return false;
  return FRONTIER_BOUNDARIES.includes(receipt.boundary as FrontierBoundary)&&text(receipt.unblockCondition);
 }

 /** Complete facet for the Operator admission duty: every proposed package is
  *  registered through supported admission, or an attributed refusal exists. */
 admissionPostcondition(rigId:string,duty:any):boolean {
   const receipt=this.admissionDisposition(rigId,duty.queueId);
   if(!receipt||receipt.proposalDigest!==duty.proposalDigest)return false;
   const proposed:ProposedPackage[]=duty.proposal??[];
   if(receipt.declined)return text(receipt.declined.reason);
   if(receipt.admitted.length!==proposed.length)return false;
   const keys=new Set(proposed.map(p=>p.packageKey));
   return receipt.admitted.every(a=>keys.has(a.packageKey))&&proposed.every(p=>receipt.admitted.some(a=>a.packageKey===p.packageKey&&!!this.seam.db.prepare("SELECT contract_hash FROM coordinator_packages WHERE rig_id=? AND package_key=?").get(rigId,p.packageKey)));
 }

 /** Act facet for the Lead: the duty is claimable and its frozen facts still hold. */
 planActAllowed(rigId:string,duty:any):boolean {
  return !!duty.frontierDigest&&(duty.scopeSourcesDigest??'')===scopeSourcesDigest(this.scopeSources(this.seam.plan(rigId)));
 }

 /** Act facet for the Operator: the cited proposal is still the recorded one. */
 admissionActAllowed(rigId:string,duty:any):boolean {
   const receipt=this.planningReceipt(rigId,duty.planningQueueId);
   return !!receipt&&receipt.disposition==='plan-proposal'&&receipt.proposal?.proposalDigest===duty.proposalDigest;
 }
 /** Complete facet for the Operator confirmation duty: the Lead's exact recorded
  *  completion, independently confirmed by the genuine current Operator. */
 confirmationPostcondition(rigId:string,duty:any):boolean {
  const receipt=this.confirmationDisposition(rigId,duty.queueId);
  return !!receipt&&receipt.completionDigest===duty.completionDigest;
 }

 /** Act facet for the Operator confirmation: the cited completion is still the
  *  disposition recorded on the planning duty this confirmation binds. */
 confirmationActAllowed(rigId:string,duty:any):boolean {
  const recorded=this.planningReceipt(rigId,duty.planningQueueId);
  return recorded?.disposition==='frontier-complete'&&recorded.completionDigest===duty.completionDigest;
 }

 private confirmationDisposition(rigId:string,queueId:string):FrontierConfirmationReceipt|null {
  const row=this.seam.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='frontier-confirmation-disposition'").get(rigId,'frontier-confirmation:'+queueId) as {receipt:string}|undefined;
  if(!row)return null;try{return JSON.parse(row.receipt) as FrontierConfirmationReceipt;}catch{return null;}
 }

 // ------------------------------------------------------------- Lead record

 /** The Lead's one permitted act: record exactly one typed disposition. */
 recordFrontierPlan(actor:string,generation:string,input:{rigId:string;dutyQueueId:string;frontierDigest:string;disposition:string;proposal?:unknown;mapping?:unknown;boundary?:unknown;unblockCondition?:unknown}):FrontierPlanReceipt {
  return this.seam.db.transaction(()=>{
   const db=this.seam.db,duty=this.control(input.rigId,PLANNING_DUTY_KIND,input.dutyQueueId);
   if(!duty)fail('frontier_planning_duty_required','Exact current frontier planning duty required');
   const id='frontier-disposition:'+input.dutyQueueId,prior=db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=?").get(input.rigId,id) as {receipt:string}|undefined;
   const receipt=this.buildPlanReceipt(input.rigId,duty,actor,generation,input);
   // An exact replay is idempotent: it mints nothing and extends no expired authority.
   if(prior){if(prior.receipt!==JSON.stringify(receipt))fail('frontier_disposition_conflict','Frozen planning disposition cannot change');return JSON.parse(prior.receipt) as FrontierPlanReceipt;}
   // Only a NEW disposition is checked: an exact replay above returns its frozen receipt even after
   // its own packages were admitted.
   if(receipt.proposal)this.proposalKeysAvailable(input.rigId,input.dutyQueueId,receipt.proposal.packages);
   this.dutyCustody(input.rigId,input.dutyQueueId,duty,actor,generation);
   if(!this.seam.actAllowed(input.dutyQueueId,actor,generation))fail('frontier_act_not_allowed','Shared duty act facet refuses this disposition');
   db.prepare("INSERT INTO coordinator_operations VALUES (?,?,?,?,?)").run(input.rigId,id,'frontier-plan-disposition',JSON.stringify(receipt),digest(JSON.stringify(receipt)));
   return receipt;
  }).immediate();
 }

 /** Validation only: the exact receipt this request would persist. */
 private buildPlanReceipt(rigId:string,duty:any,actor:string,generation:string,input:{frontierDigest:string;disposition:string;proposal?:unknown;mapping?:unknown;boundary?:unknown;unblockCondition?:unknown}):FrontierPlanReceipt {
  if(input.frontierDigest!==duty.frontierDigest)fail('frontier_digest_drift','Disposition must bind this duty\'s frozen frontier digest');
  const scopeSources=(duty.scopeSources??[]) as ScopeSource[];
  const present=['proposal','mapping','boundary','unblockCondition'].filter(key=>input[key as keyof typeof input]!==undefined),expected=PAYLOAD_BY_DISPOSITION[input.disposition];
  if(!expected)fail('frontier_disposition_required','Disposition must be plan-proposal, frontier-complete or frontier-blocked');
  if(present.length!==expected.length||expected.some(key=>!present.includes(key)))fail('frontier_disposition_conflict','Exactly the disposition payload for this disposition is required');
  let receipt:FrontierPlanReceipt={dutyQueueId:duty.queueId,frontierDigest:duty.frontierDigest,scopeSourcesDigest:duty.scopeSourcesDigest,disposition:input.disposition as FrontierDisposition,actor,generation};
  if(input.disposition==='plan-proposal'){
   const packages=this.typedProposal(input.proposal);
   if(!this.proposalValid(packages,scopeSources))fail('frontier_proposal_uncited','Every candidate must cite a frozen scope ref whose digest matches');
   receipt={...receipt,proposal:{packages,proposalDigest:digest(canonical(packages))}};
  }else if(input.disposition==='frontier-complete'){
   if(!scopeSources.length)fail('frontier_scope_required','Scope sources must be configured by the genuine current Operator before completeness can be asserted');
   const mapping=this.typedMapping(input.mapping,scopeSources);
   receipt={...receipt,mapping,completionDigest:digest(canonical({mapping,frontierDigest:duty.frontierDigest,scopeSourcesDigest:duty.scopeSourcesDigest}))};
   if(!this.completeMappingValid(rigId,receipt,scopeSources))fail('frontier_mapping_incomplete','Every scope item must map to an accepted scope-bound product package or an authorized deferral');
  }else{
   const boundary=input.boundary;
   if(typeof boundary!=='string'||!FRONTIER_BOUNDARIES.includes(boundary as FrontierBoundary))fail('frontier_boundary_required','Named boundary required');
   if(!scopeSources.length&&boundary!=='scope-source-missing')fail('frontier_scope_boundary_required','Without configured scope sources the only accountable boundary is scope-source-missing');
   if(!text(input.unblockCondition))fail('frontier_unblock_condition_required','Unblock condition required');
   receipt={...receipt,boundary:boundary as FrontierBoundary,unblockCondition:String(input.unblockCondition)};
  }
  return {...receipt,dispositionDigest:digest(JSON.stringify(receipt))};
 }

 /** Structural validation only. The runtime judges no product correctness. */
 private typedProposal(value:unknown):ProposedPackage[] {
  if(!Array.isArray(value))fail('frontier_proposal_required','Typed candidate package list required');
  const candidates=value as unknown[];
  if(!candidates.length)fail('frontier_proposal_required','Typed candidate package list required');
  const keys=candidates.map(raw=>(raw as Record<string,unknown>|null)?.packageKey);
  if(new Set(keys.map(k=>String(k))).size!==keys.length)fail('frontier_proposal_duplicate','A proposal may name each package key once');
  return candidates.map(raw=>{
   const p=raw as Record<string,unknown>,rc=p.returnContract as {destination?:unknown;evidenceRequired?:unknown}|undefined;
   if(!text(p.packageKey)||!Array.isArray(p.citations)||!p.citations.length||!Array.isArray(p.resources)||new Set(p.resources).size!==p.resources.length||p.resources.some(r=>!text(r)))fail('frontier_proposal_invalid','Exact package key, scope citations and unique resources required');
   if(!rc||!text(rc.destination)||!Array.isArray(rc.evidenceRequired)||!rc.evidenceRequired.length||rc.evidenceRequired.some((k:unknown)=>!text(k)))fail('frontier_proposal_invalid','Exact destination and non-empty required evidence kinds required');
   if((p.citations as unknown[]).some(c=>{const s=c as Record<string,unknown>|null;return !s||!text(s.ref)||!text(s.digest)||!SHA256.test(String(s.digest));}))fail('frontier_citation_invalid','Every candidate must cite a frozen scope ref with its exact digest');
   const citations=(p.citations as unknown[]).map(c=>{const s=c as Record<string,unknown>;return {ref:String(s.ref),digest:String(s.digest)};}) as ScopeSource[];
   return {packageKey:String(p.packageKey),citations,resources:(p.resources as unknown[]).map(String),returnContract:{destination:String(rc.destination),evidenceRequired:(rc.evidenceRequired as unknown[]).map(String)}};
  });
 }

 /** The mechanical anti-invention check: a candidate must cite at least one
  *  scope ref that is in the duty's frozen snapshot, with a matching digest. */
 private proposalValid(packages:ProposedPackage[],scopeSources:ScopeSource[]):boolean {
  if(!packages.length||!scopeSources.length)return false;
  const frozen=new Map(scopeSources.map(s=>[s.ref,s.digest]));
  return packages.every(p=>p.citations.length>0&&p.citations.every(c=>text(c.ref)&&SHA256.test(c.digest)&&frozen.get(c.ref)===c.digest)&&p.resources.every(r=>text(r))&&text(p.returnContract.destination)&&p.returnContract.evidenceRequired.length>0);
 }

 private typedMapping(value:unknown,scopeSources:ScopeSource[]):ScopeMapping[] {
  if(!Array.isArray(value)||value.length!==scopeSources.length)fail('frontier_mapping_incomplete','Every configured scope item requires exactly one mapping');
  const frozen=new Set(scopeSources.map(s=>s.ref));
  return (value as unknown[]).map(raw=>{
   const m=raw as Record<string,unknown>;
   if(!text(m.ref)||!frozen.has(String(m.ref)))fail('frontier_mapping_unknown_scope','Mapping must name a frozen scope item');
   const hasAccepted=text(m.acceptedPackageKey),hasDeferral=m.deferral!==undefined;
   if(hasAccepted===hasDeferral)fail('frontier_mapping_ambiguous','Map to exactly one accepted package or one authorized deferral');
   if(hasAccepted)return {ref:String(m.ref),acceptedPackageKey:String(m.acceptedPackageKey)};
   const d=m.deferral as Record<string,unknown>;
   if(!text(d.reason)||!text(d.authorizationRef))fail('frontier_deferral_unattributed','Deferral needs an explicit reason and its authorization reference');
   return {ref:String(m.ref),deferral:{reason:String(d.reason),authorizationRef:String(d.authorizationRef)}};
  });
 }

 /** Genuine native custody: the exact recipient's own current-generation
  *  transport:v1 claim on this unexpired duty. Prose cannot close a duty. */
 private dutyCustody(rigId:string,queueId:string,duty:any,actor:string,generation:string):void {
  if(duty.recipient!==actor||duty.recipientGeneration!==generation||this.seam.generation(actor)!==generation)fail('frontier_planning_recipient_required','Only this duty\'s genuine current recipient may record its disposition');
  const q=this.seam.db.prepare('SELECT destination_session,state,claimed_at,claimed_by_generation_uuid,expires_at,body FROM queue_items WHERE qitem_id=?').get(queueId) as {destination_session:string;state:string;claimed_at:string|null;claimed_by_generation_uuid:string|null;expires_at:string|null;body:string}|undefined;
  if(!q)fail('frontier_planning_claim_required','Exact genuine native claim required');
  if(q.destination_session!==actor||!q.claimed_at||q.claimed_by_generation_uuid!==generation||!['in-progress','blocked'].includes(q.state))fail('frontier_planning_claim_required','Exact genuine native claim required');
  if(!this.seam.db.prepare("SELECT 1 FROM queue_transitions WHERE qitem_id=? AND state='in-progress' AND transition_note='claimed' AND actor_session=? AND identity_provenance='transport:v1'").get(queueId,actor))fail('frontier_planning_claim_required','Exact transport:v1 claim transition required');
  if(digest(q.body)!==duty.bodyHash||q.expires_at!==new Date(duty.deadline).toISOString())fail('frontier_planning_duty_drift','Exact frozen duty body and deadline required');
  if(!q.expires_at)fail('frontier_planning_duty_drift','Exact frozen duty expiry required');
  if(Date.parse(q.expires_at)<=this.seam.now())fail('frontier_planning_duty_expired','Expired duty authority is never extended');
 }

 // ---------------------------------------------------------- Operator admit

 /** The Operator's independent qualification step. Registers the Lead's cited
  *  candidates through the existing supported admission, or refuses them with
  *  an attributed reason. It never invents, qualifies or dispatches work. */
 admitFrontierProposal(actor:string,generation:string,input:{rigId:string;dutyQueueId:string;proposalDigest:string;admitted?:unknown;declined?:unknown}):FrontierAdmissionReceipt {
  return this.seam.db.transaction(()=>{
   const db=this.seam.db,duty=this.control(input.rigId,ADMISSION_DUTY_KIND,input.dutyQueueId);
   if(!duty)fail('frontier_admission_duty_required','Exact current frontier admission duty required');
   if(actor!=='operator-agent@kernel'||!generation||this.seam.generation(actor)!==generation||duty.recipient!==actor||duty.recipientGeneration!==generation)fail('frontier_operator_required','Current genuine Operator required');
   const id='frontier-admission:'+input.dutyQueueId,prior=db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=?").get(input.rigId,id) as {receipt:string}|undefined;
   const proposed=(duty.proposal??[]) as ProposedPackage[];
   const decision=this.admissionDecision(input,proposed);
   // An exact replay validates and returns the receipt without registering anything.
   if(prior){const replay=this.admissionReceipt(input.rigId,duty,actor,generation,decision);if(prior.receipt!==JSON.stringify(replay))fail('frontier_admission_conflict','Frozen admission disposition cannot change');return JSON.parse(prior.receipt) as FrontierAdmissionReceipt;}
   this.dutyCustody(input.rigId,input.dutyQueueId,duty,actor,generation);
   if(!this.seam.actAllowed(input.dutyQueueId,actor,generation))fail('frontier_act_not_allowed','Shared duty act facet refuses this admission');
   for(const entry of decision.admitted)this.seam.admitPackage(actor,generation,input.rigId,entry.packageKey,entry.contract as unknown as PackageContract&{workClass?:WorkClass;scopeCitations?:ScopeSource[]});
   const receipt=this.admissionReceipt(input.rigId,duty,actor,generation,decision);
   db.prepare("INSERT INTO coordinator_operations VALUES (?,?,?,?,?)").run(input.rigId,id,'frontier-admission-disposition',JSON.stringify(receipt),digest(JSON.stringify(receipt)));
   return receipt;
  }).immediate();
 }

 /** Validates the Operator's decision and binds the proposal's own scope citations
  *  into the admitted contract, so a later completeness mapping stays traceable. */
 private admissionDecision(input:{proposalDigest:string;admitted?:unknown;declined?:unknown},proposed:ProposedPackage[]):{admitted:Array<{packageKey:string;contract:Record<string,unknown>}>;declined?:{reason:string}} {
  const hasAdmitted=input.admitted!==undefined,hasDeclined=input.declined!==undefined;
  if(hasAdmitted===hasDeclined)fail('frontier_admission_required','Admit every proposed package or record one attributed refusal');
  if(hasDeclined){
   const declined=input.declined as Record<string,unknown>;
   if(!text(declined.reason))fail('frontier_declined_reason_required','Attributed refusal reason required');
   return {admitted:[],declined:{reason:String(declined.reason)}};
  }
  if(!Array.isArray(input.admitted))fail('frontier_admission_partial','Partial admission would silently drop cited candidates');
  const candidates=input.admitted as unknown[];
  if(candidates.length!==proposed.length)fail('frontier_admission_partial','Partial admission would silently drop cited candidates');
  const byKey=new Map(proposed.map(p=>[p.packageKey,p]));
  const admitted=candidates.map(raw=>{
   const a=raw as Record<string,unknown>,key=text(a.packageKey)?String(a.packageKey):'',candidate=byKey.get(key);
   if(!candidate)fail('frontier_admission_unknown_package','Only the cited candidate packages may be admitted');
   const c=a.contract as Record<string,unknown>|undefined;
   if(!c)fail('frontier_admission_contract','Exact supported package contract required');
   const rc=c.returnContract as Record<string,unknown>|undefined;
   if(!text(c.inputDigest)||!text(c.destination)||!text(c.bodyHash)||!Array.isArray(c.resources)||new Set(c.resources).size!==c.resources.length||c.resources.some(r=>!text(r))||!rc||!text(rc.destination)||!Array.isArray(rc.evidenceRequired)||!(rc.evidenceRequired as unknown[]).length)fail('frontier_admission_contract','Exact supported package contract required');
   // Reclassifying a cited product proposal as administrative would silently stop
   // planning, so refusing the whole proposal is the attributed alternative.
   if(c.workClass!==undefined&&c.workClass!=='product')fail('frontier_work_class_invalid','Work class must be product for a cited scope-bound product proposal');
   const resources=(c.resources as unknown[]).map(String),returnDestination=String(rc.destination),evidenceRequired=(rc.evidenceRequired as unknown[]).map(String);
   if(JSON.stringify(resources)!==JSON.stringify(candidate.resources)||returnDestination!==candidate.returnContract.destination||JSON.stringify(evidenceRequired)!==JSON.stringify(candidate.returnContract.evidenceRequired))fail('frontier_admission_contract_drift','Admitted resources and return contract must match the frozen proposal');
   return {packageKey:key,contract:{...c,resources,returnContract:{...rc,destination:returnDestination,evidenceRequired},workClass:'product',scopeCitations:candidate.citations}};
  });
  return {admitted};
 }

 private admissionReceipt(rigId:string,duty:any,actor:string,generation:string,decision:{admitted:Array<{packageKey:string}>;declined?:{reason:string}}):FrontierAdmissionReceipt {
  return {dutyQueueId:duty.queueId,proposalDigest:duty.proposalDigest,actor,generation,
   admitted:decision.admitted.map(entry=>{const row=this.seam.db.prepare("SELECT contract_hash,contract FROM coordinator_packages WHERE rig_id=? AND package_key=?").get(rigId,entry.packageKey) as {contract_hash:string;contract:string}|undefined;if(!row)fail('frontier_admission_contract','Admitted candidate must be registered');const contract=JSON.parse(row.contract) as PackageContract&{scopeCitations?:ScopeSource[]};return {packageKey:entry.packageKey,contractHash:row.contract_hash,scopeCitations:contract.scopeCitations??[]};}),
   ...(decision.declined?{declined:decision.declined}:{})};
 }

 /** The genuine current Operator independently confirms the Lead's completion.
  *  Confirmation is a duty, never a prose note on the proposal. */
 recordFrontierConfirmation(actor:string,generation:string,input:{rigId:string;dutyQueueId:string;completionDigest:string;evidenceRef:string}):FrontierConfirmationReceipt {
  return this.seam.db.transaction(()=>{
   const db=this.seam.db,duty=this.control(input.rigId,CONFIRMATION_DUTY_KIND,input.dutyQueueId);
   if(!duty)fail('frontier_confirmation_duty_required','Exact current frontier confirmation duty required');
   if(actor!=='operator-agent@kernel'||!generation||this.seam.generation(actor)!==generation||duty.recipient!==actor||duty.recipientGeneration!==generation)fail('frontier_operator_required','Current genuine Operator required');
   if(!text(input.evidenceRef))fail('frontier_confirmation_evidence_required','Attributed confirmation evidence required');
   const id='frontier-confirmation:'+input.dutyQueueId,prior=db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=?").get(input.rigId,id) as {receipt:string}|undefined;
   if(prior){const replay={dutyQueueId:input.dutyQueueId,completionDigest:input.completionDigest,actor,generation,evidenceRef:String(input.evidenceRef)} as FrontierConfirmationReceipt;if(prior.receipt!==JSON.stringify(replay))fail('frontier_confirmation_conflict','Frozen confirmation cannot change');return JSON.parse(prior.receipt) as FrontierConfirmationReceipt;}
   this.dutyCustody(input.rigId,input.dutyQueueId,duty,actor,generation);
   if(!this.seam.actAllowed(input.dutyQueueId,actor,generation))fail('frontier_act_not_allowed','Shared duty act facet refuses this confirmation');
   if(input.completionDigest!==duty.completionDigest)fail('frontier_completion_drift','Confirmation must bind this duty\'s frozen completion digest');
   const receipt:FrontierConfirmationReceipt={dutyQueueId:input.dutyQueueId,completionDigest:input.completionDigest,actor,generation,evidenceRef:String(input.evidenceRef)};
   db.prepare("INSERT INTO coordinator_operations VALUES (?,?,?,?,?)").run(input.rigId,id,'frontier-confirmation-disposition',JSON.stringify(receipt),digest(JSON.stringify(receipt)));
   return receipt;
  }).immediate();
 }

 /** The genuine current Operator records that a recorded blocked, completed or
  *  declined disposition is discharged. This is the only way planning reopens,
  *  so a terminal state is never a silent permanent stall. */
 recordFrontierReopen(actor:string,generation:string,input:{rigId:string;dutyQueueId:string;dispositionDigest:string;evidenceRef:string}):FrontierReopenReceipt {
  return this.seam.db.transaction(()=>{
   const db=this.seam.db,duty=this.control(input.rigId,PLANNING_DUTY_KIND,input.dutyQueueId);
   if(!duty)fail('frontier_planning_duty_required','Exact current frontier planning duty required');
   if(actor!=='operator-agent@kernel'||!generation||this.seam.generation(actor)!==generation)fail('frontier_operator_required','Current genuine Operator required');
   if(!text(input.evidenceRef))fail('frontier_reopen_evidence_required','Attributed unblock evidence required');
   const recorded=this.planningReceipt(input.rigId,input.dutyQueueId);
   if(!recorded||(recorded.dispositionDigest!==input.dispositionDigest&&recorded.proposal?.proposalDigest!==input.dispositionDigest))fail('frontier_disposition_unknown','Reopen must reference the exact recorded blocked, completed or declined disposition');
   const receipt:FrontierReopenReceipt={dutyQueueId:input.dutyQueueId,dispositionDigest:input.dispositionDigest,reopenDigest:digest(JSON.stringify({dispositionDigest:input.dispositionDigest,evidenceRef:String(input.evidenceRef)})),frontierDigest:duty.frontierDigest,actor,generation,evidenceRef:String(input.evidenceRef),...(recorded.boundary?{boundary:recorded.boundary}:{})};
   const id='frontier-reopen:'+input.dutyQueueId+':'+input.dispositionDigest,prior=db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=?").get(input.rigId,id) as {receipt:string}|undefined;
   if(prior){if(prior.receipt!==JSON.stringify(receipt))fail('frontier_reopen_conflict','Frozen reopen disposition cannot change');return JSON.parse(prior.receipt) as FrontierReopenReceipt;}
   // A reopen is an Operator-owned administrative disposition about a recorded Lead
   // decision, so it never claims the Lead's duty. Where the referenced duty is
   // itself Operator-owned the shared Act facet still applies unchanged.
   if(duty.recipient===actor&&!this.seam.actAllowed(input.dutyQueueId,actor,generation))fail('frontier_act_not_allowed','Shared duty act facet refuses this reopen');
   db.prepare("INSERT INTO coordinator_operations VALUES (?,?,?,?,?)").run(input.rigId,id,'frontier-reopen',JSON.stringify(receipt),digest(JSON.stringify(receipt)));
   return receipt;
  }).immediate();
 }

 // ------------------------------------------------- S1 legacy classification

 /** The genuine current Operator attests that ONE exact old unclassified package is administrative or
  *  inquiry work. Append-only, evidence-bound, and bound to the exact assignment and claim the
  *  Operator observed. It rewrites no contract and accepts, retires, releases or dispatches nothing;
  *  it only lets the planning flags stop counting that package. Protected facts stay visible. */
 recordFrontierLegacyClassification(actor:string,generation:string,input:LegacyClassificationInput):LegacyClassificationReceipt {
  return this.seam.db.transaction(()=>{
   const db=this.seam.db;
   this.legacyOperator(actor,generation,input.rigId);
   if(!text(input.packageKey)||typeof input.contractHash!=='string'||!SHA256.test(input.contractHash))fail('frontier_classification_invalid','Exact package key and stored contract hash required');
   if(input.workClass!=='administrative'&&input.workClass!=='inquiry')fail('frontier_classification_invalid_class','Only administrative or inquiry may be attested');
   if(!text(input.evidenceRef))fail('frontier_classification_evidence_required','Explicit non-empty evidence reference required');
   const observed=this.typedObservation(input.observed);
   const pkg=db.prepare("SELECT contract_hash,contract FROM coordinator_packages WHERE rig_id=? AND package_key=?").get(input.rigId,input.packageKey) as {contract_hash:string;contract:string}|undefined;
   if(!pkg)fail('frontier_classification_package_unknown','No such registered package');
   if(pkg.contract_hash!==input.contractHash)fail('frontier_classification_hash_mismatch','Contract hash differs from the stored immutable contract');
   if((JSON.parse(pkg.contract) as PackageContract&{workClass?:string}).workClass!==undefined)fail('frontier_classification_not_legacy','Only a package whose frozen contract has no work class is eligible');
   // Compare-and-set against the assignment and claim as they are in this transaction.
   if(!this.sameObservation(observed,this.currentObservation(input.rigId,input.packageKey)))fail('frontier_classification_observation_drift','Observed assignment or claim no longer matches; observe again');
   const {classes,revoked}=this.classificationHistory(input.rigId,input.packageKey,input.contractHash),latest=classes[classes.length-1];
   // An attestation whose observed assignment/claim has since changed is already void (frontier() stops
   // applying it), so a fresh attestation for the NEW observation supersedes nothing that was in force.
   if(latest&&!revoked.has(latest.operationId)&&this.sameObservation(latest.observed,observed)){
    // The attestation in force may be replayed exactly; any other change needs an explicit revocation first.
    if(latest.workClass===input.workClass&&latest.evidenceRef===input.evidenceRef)return latest;
    fail('frontier_classification_conflict','An active attestation exists; revoke it explicitly before attesting differently');
   }
   const operationId=`frontier-legacy-class:${input.packageKey}:${input.contractHash}:${classes.length+1}`;
   const receipt:LegacyClassificationReceipt={operationId,rigId:input.rigId,packageKey:input.packageKey,contractHash:input.contractHash,workClass:input.workClass,evidenceRef:String(input.evidenceRef),observed,actor,generation,at:this.seam.now(),grantsAuthority:false};
   db.prepare("INSERT INTO coordinator_operations VALUES (?,?,?,?,?)").run(input.rigId,operationId,'frontier-legacy-classification',JSON.stringify(receipt),digest(JSON.stringify(receipt)));
   return receipt;
  }).immediate();
 }

 /** Explicit supersession: revoke the active attestation, then (optionally) attest again. History is never overwritten. */
 revokeFrontierLegacyClassification(actor:string,generation:string,input:LegacyRevocationInput):LegacyRevocationReceipt {
  return this.seam.db.transaction(()=>{
   const db=this.seam.db;
   this.legacyOperator(actor,generation,input.rigId);
   if(!text(input.packageKey)||typeof input.contractHash!=='string'||!SHA256.test(input.contractHash))fail('frontier_classification_invalid','Exact package key and stored contract hash required');
   if(!text(input.evidenceRef))fail('frontier_classification_evidence_required','Explicit non-empty evidence reference required');
   if(!text(input.classificationId))fail('frontier_classification_id_required','The exact attestation id to revoke is required');
   const {classes,revoked}=this.classificationHistory(input.rigId,input.packageKey,input.contractHash),target=classes.find(c=>c.operationId===input.classificationId),latest=classes[classes.length-1];
   if(!target)fail('frontier_classification_not_active','No such attestation for this package and contract');
   // The revoke is bound to the exact attestation the Operator named, so a delayed replay can never
   // reach across to a successor written after it.
   const prior=revoked.get(target!.operationId);
   if(prior){
    if(prior.evidenceRef===input.evidenceRef)return prior;
    fail('frontier_classification_conflict','This attestation was already revoked with different evidence');
   }
   if(target!==latest)fail('frontier_classification_stale','A newer attestation supersedes the named one; it cannot be revoked now and no successor is touched');
   const operationId='frontier-legacy-class-revoke:'+target!.operationId;
   const receipt:LegacyRevocationReceipt={operationId,rigId:input.rigId,packageKey:input.packageKey,contractHash:input.contractHash,classificationId:target!.operationId,evidenceRef:String(input.evidenceRef),actor,generation,at:this.seam.now(),grantsAuthority:false};
   db.prepare("INSERT INTO coordinator_operations VALUES (?,?,?,?,?)").run(input.rigId,operationId,'frontier-legacy-classification-revocation',JSON.stringify(receipt),digest(JSON.stringify(receipt)));
   return receipt;
  }).immediate();
 }

 /** The live Operator generation must match the generation the current plan was configured under. */
 private legacyOperator(actor:string,generation:string,rigId:string):void {
  const plan=this.seam.plan(rigId);
  if(actor!=='operator-agent@kernel'||!generation||this.seam.generation(actor)!==generation||!plan||plan.operatorGeneration!==generation||!this.seam.authorityRecord(rigId))fail('frontier_classification_operator_required','Current genuine Operator whose generation matches the configured plan is required');
 }

 private typedObservation(value:unknown):LegacyObservation {
  const o=value as Record<string,unknown>|null|undefined,field=(k:string)=>o&&typeof o==='object'&&k in o&&(o[k]===null||typeof o[k]==='string'&&String(o[k]).length>0);
  if(!field('queueId')||!field('claimedByGeneration')||!field('claimedAt'))fail('frontier_classification_observation_required','Exact observed assignment queueId, claim generation and claimedAt are required; use null for an observed absence');
  return {queueId:o!.queueId as string|null,claimedByGeneration:o!.claimedByGeneration as string|null,claimedAt:o!.claimedAt as string|null};
 }
}
