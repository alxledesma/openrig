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
export interface FrontierStabilization { observations:number; since:number; requiredObservations:number; requiredMs:number; ready:boolean }
export interface FrontierSnapshot {
  rigId:string; state:FrontierState; reason:string;
  frontierDigest:string; scopeSources:ScopeSource[]; scopeSourcesDigest:string;
  packages:FrontierPackageFact[]; accepted:Array<{queueId:string;dispositionId:string;evidenceRef:string}>;
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
  const rows=db.prepare(`SELECT p.package_key,p.contract,a.queue_id,a.disposition_id,a.destination,q.state,q.claimed_by_generation_uuid
   FROM coordinator_packages p
   LEFT JOIN coordinator_assignments a ON a.rig_id=p.rig_id AND a.package_key=p.package_key
   LEFT JOIN queue_items q ON q.qitem_id=a.queue_id
   WHERE p.rig_id=? ORDER BY p.package_key`).all(rigId) as Array<{package_key:string;contract:string;queue_id:string|null;disposition_id:string|null;destination:string|null;state:string|null;claimed_by_generation_uuid:string|null}>;

  const flags={ACTIVE:false,AWAITING_ACCEPTANCE:false,PROTECTED_HOLD:false,MATERIALIZABLE:false};
  const packages:FrontierPackageFact[]=[];
  for(const row of rows){
   const task=tasks.get(row.package_key),contract=(JSON.parse(row.contract) as PackageContract&{workClass?:string}).workClass;
   const workClass:WorkClass=WORK_CLASSES.includes(contract as WorkClass)?contract as WorkClass:"product";
   const legacyClass=contract===undefined;
   // Non-product work is administrative. It never supports product classification.
   const administrative=!legacyClass&&(workClass==="administrative"||workClass==="inquiry");
   // Recovery backups are dormant, not work: accepted target, no semantic
   // recovery requirement, and no assignment, stage, resource or live queue.
   const dormantBackup=!!task?.recoveryFor&&this.dormantBackup(rigId,task,plan);
   const status=this.packageStatus(rigId,row,task);
   packages.push({packageKey:row.package_key,workClass,status,legacyClass});
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
  const frontierDigest=digest(canonical({rigId,packages:packages.map(p=>({packageKey:p.packageKey,workClass:p.workClass,status:p.status})),accepted:accepted.map(a=>[a.queueId,a.dispositionId]),scopeSourcesDigest:sourcesDigest}));
  const run=this.observationRun(rigId,frontierDigest,authority.epoch,state);
  const limits=this.stabilization(plan);
  return {rigId,state,reason,frontierDigest,scopeSources:sources,scopeSourcesDigest:sourcesDigest,packages,accepted,
   stabilization:{observations:run.observations,since:run.since,requiredObservations:limits.requiredObservations,requiredMs:limits.requiredMs,ready:run.observations>=limits.requiredObservations&&this.seam.now()-run.since>=limits.requiredMs},
   holder:authority.owner_session,holderGeneration:authority.owner_generation,epoch:authority.epoch,operatorGeneration:plan.operatorGeneration};
 }

 private packageStatus(rigId:string,row:{package_key:string;queue_id:string|null;disposition_id:string|null;destination:string|null;state:string|null;claimed_by_generation_uuid:string|null},task:CoordinationTask|undefined):string {
  if(!row.queue_id)return !task?'unplanned':this.taskProtected(rigId,task)?'held':'pending-pickup';
  if(successfulReturn(row.state!,row.disposition_id))return this.exactAccepted(rigId,row.queue_id,row.disposition_id!)?"accepted":"awaiting-acceptance";
  if(['failed','denied','canceled'].includes(row.state!))return task&&task.boundary?"held":"awaiting-acceptance";
  if(!row.disposition_id&&['done','handed-off','failed','denied','canceled'].includes(row.state!))return this.taskProtected(rigId,task)?"held":"awaiting-acceptance";
  if(['pending','in-progress','blocked'].includes(row.state!)){
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
   frontierState:snapshot.state,epoch:snapshot.epoch,grantsAuthority:false,...(reopenDigest?{reopenDigest}:{}),
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
    details:{proposalDigest:receipt.proposal!.proposalDigest,frontierDigest:receipt.frontierDigest,planningQueueId:receipt.dutyQueueId,proposal:receipt.proposal!.packages,
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

 /** Duties are found by their frozen digest, never by predicting the shared
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
}