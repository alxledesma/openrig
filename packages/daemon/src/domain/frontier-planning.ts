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
 *  and Act facets are `planPostcondition`/`admissionPostcondition` and their
 *  `*ActAllowed` counterparts below. */
export const PLANNING_DUTY_KIND = "frontier-planning";
export const ADMISSION_DUTY_KIND = "frontier-admission";
/** Reserved package keys. They are never registered products; they only give
 *  the shared duty mechanism a scope key for checkpoint dispatch restrictions. */
export const PLANNING_DUTY_PACKAGE_KEY = "frontier-planning";
export const ADMISSION_DUTY_PACKAGE_KEY = "frontier-admission";

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
}
export interface FrontierAdmissionReceipt {
  dutyQueueId:string; proposalDigest:string; actor:string; generation:string;
  admitted:Array<{packageKey:string;contractHash:string}>; declined?:{reason:string};
}

/** The whole D10 integration surface, in one structural type. Everything the
 *  planner needs from the shared service is read-only except `issueLifecycleDuty`
 *  (one kind-table row) and `admitPackage` (the existing supported Operator
 *  admission, never reached without a genuine Operator caller). */
export interface FrontierPlanningSeam {
  readonly db:Database.Database;
  now():number;
  generation(session:string):string|null;
  authorityRecord(rigId:string):{owner_session:string;owner_generation:string;epoch:number;state:string;lease_until:number}|null;
  plan(rigId:string):CoordinationPlan|null;
  lifecycleControlCompleted(queueId:string):boolean;
  issueLifecycleDuty(input:{rigId:string;kind:typeof PLANNING_DUTY_KIND|typeof ADMISSION_DUTY_KIND;packageKey:string;recipient:string;recipientGeneration:string;semanticKey:string;details:Record<string,unknown>}):CoordinationResult;
  admittedNow(task:CoordinationTask):boolean;
  dispatchScopeHold(plan:CoordinationPlan,task:CoordinationTask):string|null;
  effectDebt(session:string):boolean;
  requiresRecovery(rigId:string,packageKey:string):boolean;
  admitPackage(actor:string,generation:string,rigId:string,packageKey:string,contract:PackageContract&{workClass?:WorkClass}):void;
}

const fail:(code:string,message:string)=>never=(code,message)=>{throw new CoordinatorFenceError(code,message);};
const text=(value:unknown):value is string=>typeof value==="string"&&value.trim().length>0;
const successfulReturn=(state:string,disposition:string|null):boolean=>!!disposition&&['done','handed-off'].includes(state);
/** Each disposition accepts exactly its own payload and nothing smuggled beside it. */
const PAYLOAD_BY_DISPOSITION:Record<string,readonly string[]>={'plan-proposal':['proposal'],'frontier-complete':['mapping'],'frontier-blocked':['boundary','unblockCondition']};
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
 private acceptedPackageKeys(rigId:string):Set<string> {
  const keys=new Set<string>();
  for(const row of this.seam.db.prepare("SELECT a.package_key,a.queue_id,a.disposition_id FROM coordinator_assignments a WHERE a.rig_id=? AND a.disposition_id IS NOT NULL").all(rigId) as Array<{package_key:string;queue_id:string;disposition_id:string}>)
   if(this.exactAccepted(rigId,row.queue_id,row.disposition_id))keys.add(row.package_key);
  return keys;
 }

 // -------------------------------------------------------------- observation

 private control(rigId:string,kind:string,queueId:string):any|null {
   const row=this.seam.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='coordinator-lifecycle-control'").get(rigId,queueId) as {receipt:string}|undefined;
   if(!row)return null;
   let receipt:any;try{receipt=JSON.parse(row.receipt);}catch{return null;}
   return receipt.kind===kind?receipt:null;
 }

 /** Trailing run of observations that agree with the current frontier. Any
  *  different state, digest or epoch ends the run, so a transient gap between
  *  acceptance and the next reconcile can never accumulate into an obligation. */
 private observationRun(rigId:string,frontierDigest:string,epoch:number,state:string):{observations:number;since:number} {
   const rows=this.seam.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='frontier-observation' ORDER BY rowid").all(rigId) as Array<{receipt:string}>;
   let observations=0,since=this.seam.now();
   for(let i=rows.length-1;i>=0;i--){
    let o:any;try{o=JSON.parse(rows[i]!.receipt);}catch{break;}
    if(o.frontierDigest!==frontierDigest||o.epoch!==epoch||o.state!==state)break;
    observations++;since=o.observedAt;
   }
   return {observations,since};
 }

 /** Append-only census. A row is written when the frontier changes, and on every
  *  EXHAUSTED observation because those carry the stabilization count and dwell.
  *  An unchanged non-exhausted frontier writes nothing. */
 private observe(snapshot:FrontierSnapshot):void {
   const db=this.seam.db,now=this.seam.now();
   const latest=db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='frontier-observation' ORDER BY rowid DESC LIMIT 1").get(snapshot.rigId) as {receipt:string}|undefined;
   let prior:any=null;try{prior=latest?JSON.parse(latest.receipt):null;}catch{prior=null;}
   const same=!!prior&&prior.frontierDigest===snapshot.frontierDigest&&prior.epoch===snapshot.epoch&&prior.state===snapshot.state;
   if(same&&snapshot.state!=="EXHAUSTED")return;
   if(same&&prior.observedAt===now)return;
   const receipt={rigId:snapshot.rigId,state:snapshot.state,frontierDigest:snapshot.frontierDigest,scopeSourcesDigest:snapshot.scopeSourcesDigest,epoch:snapshot.epoch,holder:snapshot.holder,holderGeneration:snapshot.holderGeneration,observedAt:now,reason:snapshot.reason,grantsAuthority:false};
   db.prepare("INSERT OR IGNORE INTO coordinator_operations VALUES (?,?,?,?,?)").run(snapshot.rigId,'frontier-observation:'+digest(snapshot.rigId+':'+snapshot.frontierDigest+':'+snapshot.epoch+':'+snapshot.state+':'+now),'frontier-observation',JSON.stringify(receipt),digest(JSON.stringify(receipt)));
 }


 private planningDutyResult(snapshot:FrontierSnapshot):CoordinationResult {
   const recipient=snapshot.holder,recipientGeneration=snapshot.holderGeneration;
   const details:Record<string,unknown>={
    frontierDigest:snapshot.frontierDigest,scopeSources:snapshot.scopeSources,scopeSourcesDigest:snapshot.scopeSourcesDigest,
    frontierState:snapshot.state,epoch:snapshot.epoch,grantsAuthority:false,
    planningContract:{
     dispositions:["plan-proposal","frontier-complete","frontier-blocked"],
     recordOperation:"coordination-frontier-plan",
     body:{rigId:snapshot.rigId,frontierDigest:snapshot.frontierDigest,dutyQueueId:'<this exact duty queue item ID>',disposition:'<plan-proposal|frontier-complete|frontier-blocked>',proposal:'<only for plan-proposal>',mapping:'<only for frontier-complete>',boundary:'<only for frontier-blocked>',unblockCondition:'<only for frontier-blocked>'},
     scopeSources:snapshot.scopeSources.length?snapshot.scopeSources:[]
    }};
   return this.seam.issueLifecycleDuty({rigId:snapshot.rigId,kind:PLANNING_DUTY_KIND,packageKey:PLANNING_DUTY_PACKAGE_KEY,recipient,recipientGeneration,semanticKey:snapshot.frontierDigest,details});
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
  const disposition=this.planningDisposition(rigId,snapshot.frontierDigest);
  if(disposition?.disposition==='frontier-complete'&&this.completeMappingValid(rigId,disposition,snapshot))return [{key:'frontier',state:'frontier-complete',queueId:disposition.dutyQueueId,reason:'complete-as-of:'+snapshot.scopeSourcesDigest,deadline:this.seam.now()}];
  if(disposition?.disposition==='frontier-blocked')return [{key:'frontier',state:'held',queueId:disposition.dutyQueueId,reason:disposition.boundary??'frontier-blocked',deadline:this.seam.now()}];
  if(disposition?.disposition==='plan-proposal')return this.admissionResult(snapshot,disposition);
  if(!snapshot.stabilization.ready)return [{key:'frontier',state:'stabilizing',reason:'frontier-stabilization-pending',deadline:this.seam.now(),activityEvidence:{frontierDigest:snapshot.frontierDigest,observations:snapshot.stabilization.observations,requiredObservations:snapshot.stabilization.requiredObservations,elapsedMs:this.seam.now()-snapshot.stabilization.since,requiredMs:snapshot.stabilization.requiredMs}}];
  const duty=this.planningDutyResult(snapshot);
  if(duty.state==='held'&&duty.reason==='lifecycle-duty-exhausted')return [{key:'frontier',state:'held',queueId:duty.queueId,reason:'frontier-planning-duty-exhausted',deadline:duty.deadline,activityEvidence:{accountableBoundary:'operator-agent@kernel',escalation:'frontier-planning-exhausted',frontierDigest:snapshot.frontierDigest}}];
  return [{...duty,key:'frontier'}];
 }

 /** The Lead recorded a proposal; hand it to the current Operator through the
  *  independent admission step. No package is created here. */
 private admissionResult(snapshot:FrontierSnapshot,receipt:FrontierPlanReceipt):CoordinationResult[] {
  const proposed=receipt.proposal!.packages.map(p=>p.packageKey),duty=this.admissionDutyResult(snapshot,receipt);
  const registered=proposed.filter(key=>!!this.seam.db.prepare("SELECT 1 FROM coordinator_packages WHERE rig_id=? AND package_key=?").get(snapshot.rigId,key));
  const recorded=this.admissionDisposition(snapshot.rigId,duty.queueId);
  return [{key:'frontier',state:recorded?(recorded.declined?'frontier-admission-declined':'frontier-admission-complete'):registered.length?'frontier-admission-incomplete':'pending-native-frontier-admission',queueId:duty.queueId,reason:registered.length?`${registered.length}/${proposed.length} cited packages registered`:undefined,deadline:duty.deadline}];
 }

 /** Duties are found by their frozen digest, never by predicting the shared
  *  mechanism's queue id: that id deliberately excludes the plan revision. */
 private planningDisposition(rigId:string,frontierDigest:string):FrontierPlanReceipt|null {
  const row=this.seam.db.prepare("SELECT operation_id FROM coordinator_operations WHERE rig_id=? AND kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='frontier-planning' AND json_extract(receipt,'$.frontierDigest')=? ORDER BY rowid DESC LIMIT 1").get(rigId,frontierDigest) as {operation_id:string}|undefined;
  return row?this.planningReceipt(rigId,row.operation_id):null;
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

 /** Every frozen scope item maps to an accepted package or to an explicit
  *  owner-attributed deferral carrying its authorization reference. */
 private completeMappingValid(rigId:string,receipt:FrontierPlanReceipt,snapshot:FrontierSnapshot):boolean {
   const mapping=receipt.mapping;
   if(!Array.isArray(mapping)||!mapping.length)return false;
   const frozen=new Set(snapshot.scopeSources.map(s=>s.ref));
   if(mapping.length!==frozen.size||new Set(mapping.map(m=>m.ref)).size!==frozen.size)return false;
   if(mapping.some(m=>!frozen.has(m.ref)))return false;
   const accepted=this.acceptedPackageKeys(rigId);
   return mapping.every(m=>!!m.acceptedPackageKey&&accepted.has(m.acceptedPackageKey)||!!m.deferral&&text(m.deferral.reason)&&text(m.deferral.authorizationRef));
 }

 // ------------------------------------------------------------------ facets

 /** Complete facet for the Lead planning duty. */
 planPostcondition(rigId:string,duty:any):boolean {
   const receipt=this.planningReceipt(rigId,duty.queueId);
   if(!receipt||receipt.frontierDigest!==duty.frontierDigest||receipt.scopeSourcesDigest!==duty.scopeSourcesDigest)return false;
   const snapshot=this.frontier(rigId);
   if(!snapshot)return false;
   if(receipt.disposition==='plan-proposal')return this.proposalValid(receipt.proposal?.packages??[],duty.scopeSources??[]);
   if(receipt.disposition==='frontier-complete')return (duty.scopeSources??[]).length>0&&this.completeMappingValid(rigId,receipt,snapshot);
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

 // ------------------------------------------------------------- Lead record

 /** The Lead's one permitted act: record exactly one typed disposition. */
 recordFrontierPlan(actor:string,generation:string,input:{rigId:string;dutyQueueId:string;frontierDigest:string;disposition:string;proposal?:unknown;mapping?:unknown;boundary?:unknown;unblockCondition?:unknown}):FrontierPlanReceipt {
   return this.seam.db.transaction(()=>{
    const db=this.seam.db,duty=this.control(input.rigId,PLANNING_DUTY_KIND,input.dutyQueueId);
    if(!duty)fail('frontier_planning_duty_required','Exact current frontier planning duty required');
    this.dutyCustody(input.rigId,input.dutyQueueId,duty,actor,generation);
    if(input.frontierDigest!==duty.frontierDigest)fail('frontier_digest_drift','Disposition must bind this duty\'s frozen frontier digest');
    const scopeSources=(duty.scopeSources??[]) as ScopeSource[];
    const present=['proposal','mapping','boundary','unblockCondition'].filter(key=>input[key as keyof typeof input]!==undefined),expected=PAYLOAD_BY_DISPOSITION[input.disposition];
    if(!expected)fail('frontier_disposition_required','Disposition must be plan-proposal, frontier-complete or frontier-blocked');
    if(present.length!==expected.length||expected.some(key=>!present.includes(key)))fail('frontier_disposition_conflict','Exactly the disposition payload for this disposition is required');
    let receipt:FrontierPlanReceipt={dutyQueueId:input.dutyQueueId,frontierDigest:duty.frontierDigest,scopeSourcesDigest:duty.scopeSourcesDigest,disposition:input.disposition as FrontierDisposition,actor,generation};
    if(input.disposition==='plan-proposal'){
     const packages=this.typedProposal(input.proposal);
     if(!this.proposalValid(packages,scopeSources))fail('frontier_proposal_uncited','Every candidate must cite a frozen scope ref whose digest matches');
     receipt={...receipt,proposal:{packages,proposalDigest:digest(canonical(packages))}};
    }else if(input.disposition==='frontier-complete'){
     // A project cannot be declared complete against an empty scope.
     if(!scopeSources.length)fail('frontier_scope_required','Scope sources must be configured by the genuine current Operator before completeness can be asserted');
     const mapping=this.typedMapping(input.mapping,scopeSources);
     const provisional={...receipt,mapping} as FrontierPlanReceipt,snapshot=this.frontier(input.rigId);
     if(!snapshot||!this.completeMappingValid(input.rigId,provisional,snapshot))fail('frontier_mapping_incomplete','Every scope item must map to an accepted package or an authorized deferral');
     receipt=provisional;
    }else{
     const boundary=input.boundary;
     if(typeof boundary!=='string'||!FRONTIER_BOUNDARIES.includes(boundary as FrontierBoundary))fail('frontier_boundary_required','Named boundary required');
     if(!scopeSources.length&&boundary!=='scope-source-missing')fail('frontier_scope_boundary_required','Without configured scope sources the only accountable boundary is scope-source-missing');
     if(!text(input.unblockCondition))fail('frontier_unblock_condition_required','Unblock condition required');
     receipt={...receipt,boundary:boundary as FrontierBoundary,unblockCondition:String(input.unblockCondition)};
    }
    const id='frontier-disposition:'+input.dutyQueueId,prior=db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=?").get(input.rigId,id) as {receipt:string}|undefined;
    if(prior){if(prior.receipt!==JSON.stringify(receipt))fail('frontier_disposition_conflict','Frozen planning disposition cannot change');return JSON.parse(prior.receipt) as FrontierPlanReceipt;}
    db.prepare("INSERT INTO coordinator_operations VALUES (?,?,?,?,?)").run(input.rigId,id,'frontier-plan-disposition',JSON.stringify(receipt),digest(JSON.stringify(receipt)));
    return receipt;
   }).immediate();
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
   const citations=(p.citations as unknown[]).map(c=>{const s=c as Record<string,unknown>;return {ref:s?.ref,digest:s?.digest};}) as ScopeSource[];
   return {packageKey:String(p.packageKey),citations,resources:(p.resources as unknown[]).map(String),returnContract:{destination:String(rc.destination),evidenceRequired:(rc.evidenceRequired as unknown[]).map(String)}};
  });
 }

 /** The mechanical anti-invention check: a candidate must cite at least one
  *  scope ref that is in the duty's frozen snapshot, with a matching digest. */
 private proposalValid(packages:ProposedPackage[],scopeSources:ScopeSource[]):boolean {
   if(!packages.length)return false;
   const frozen=new Map(scopeSources.map(s=>[s.ref,s.digest]));
   return packages.every(p=>p.citations.length>0&&p.citations.every(c=>frozen.get(c.ref)===c.digest)&&p.resources.every(r=>text(r))&&text(p.returnContract.destination)&&p.returnContract.evidenceRequired.length>0);
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
    this.dutyCustody(input.rigId,input.dutyQueueId,duty,actor,generation);
    if(input.proposalDigest!==duty.proposalDigest)fail('frontier_proposal_drift','Admission must bind this duty\'s frozen proposal digest');
    const proposed=(duty.proposal??[]) as ProposedPackage[];
    const hasAdmitted=input.admitted!==undefined,hasDeclined=input.declined!==undefined;
    if(hasAdmitted===hasDeclined)fail('frontier_admission_required','Admit every proposed package or record one attributed refusal');
    const id='frontier-admission:'+input.dutyQueueId;
    if(hasDeclined){
     const declined=input.declined as Record<string,unknown>;
     if(!text(declined.reason))fail('frontier_declined_reason_required','Attributed refusal reason required');
     const receipt:FrontierAdmissionReceipt={dutyQueueId:input.dutyQueueId,proposalDigest:duty.proposalDigest,actor,generation,admitted:[],declined:{reason:String(declined.reason)}};
     const prior=db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=?").get(input.rigId,id) as {receipt:string}|undefined;
     if(prior){if(prior.receipt!==JSON.stringify(receipt))fail('frontier_admission_conflict','Frozen admission disposition cannot change');return JSON.parse(prior.receipt) as FrontierAdmissionReceipt;}
     db.prepare("INSERT INTO coordinator_operations VALUES (?,?,?,?,?)").run(input.rigId,id,'frontier-admission-disposition',JSON.stringify(receipt),digest(JSON.stringify(receipt)));
     return receipt;
    }
    if(!Array.isArray(input.admitted))fail('frontier_admission_partial','Partial admission would silently drop cited candidates');
    const candidates=input.admitted as unknown[];
    if(candidates.length!==proposed.length)fail('frontier_admission_partial','Partial admission would silently drop cited candidates');
    const byKey=new Map(proposed.map(p=>[p.packageKey,p]));
    const admitted=candidates.map(raw=>{
     const a=raw as Record<string,unknown>,key=text(a.packageKey)?String(a.packageKey):'';
     if(!byKey.has(key))fail('frontier_admission_unknown_package','Only the cited candidate packages may be admitted');
     const c=a.contract as Record<string,unknown>|undefined;
     if(!c)fail('frontier_admission_contract','Exact supported package contract required');
     const rc=c.returnContract as Record<string,unknown>|undefined;
     if(!text(c.inputDigest)||!text(c.destination)||!text(c.bodyHash)||!Array.isArray(c.resources)||new Set(c.resources).size!==c.resources.length||c.resources.some(r=>!text(r))||!rc||!text(rc.destination)||!Array.isArray(rc.evidenceRequired)||!(rc.evidenceRequired as unknown[]).length)fail('frontier_admission_contract','Exact supported package contract required');
     if(c.workClass!==undefined&&!WORK_CLASSES.includes(c.workClass as WorkClass))fail('frontier_work_class_invalid','Work class must be product, recovery, administrative or inquiry');
     this.seam.admitPackage(actor,generation,input.rigId,key,c as unknown as PackageContract&{workClass?:WorkClass});
     const row=db.prepare("SELECT contract_hash FROM coordinator_packages WHERE rig_id=? AND package_key=?").get(input.rigId,key) as {contract_hash:string};
     return {packageKey:key,contractHash:row.contract_hash};
    });
    const receipt:FrontierAdmissionReceipt={dutyQueueId:input.dutyQueueId,proposalDigest:duty.proposalDigest,actor,generation,admitted};
    const prior=db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=?").get(input.rigId,id) as {receipt:string}|undefined;
    if(prior){if(prior.receipt!==JSON.stringify(receipt))fail('frontier_admission_conflict','Frozen admission disposition cannot change');return JSON.parse(prior.receipt) as FrontierAdmissionReceipt;}
    db.prepare("INSERT INTO coordinator_operations VALUES (?,?,?,?,?)").run(input.rigId,id,'frontier-admission-disposition',JSON.stringify(receipt),digest(JSON.stringify(receipt)));
    return receipt;
   }).immediate();
 }
}