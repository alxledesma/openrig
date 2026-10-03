import { rotationLocalAddresses } from "./rotation-local-custody.js";
import type { QueueRepository } from "./queue-repository.js";
import { CoordinatorFenceError, digest, type CoordinatorToken } from "./coordinator-authority-service.js";
import type { ActivityEvidence, ArbitratedSeatState } from "./activity-taxonomy.js";

export interface CoordinationActivity { generation:string; identityVerified:boolean; state:ArbitratedSeatState; witness:ActivityEvidence|null }
export interface CoordinationTask {
 key:string; packageKey:string; owner:string; action:string; deadline:number; body:string; recoveryFor?:string;
 predecessors:Array<{queueId:string;dispositionId:string}>;
 admission:{generation:string;configurationDigest:string;qualificationRef:string;capacityRef:string;effortRef:string;validUntil:number};
 /** Owner boundary affects this slice only. Recovery work is a separate admitted task. */
 boundary?:"owner-access"|"owner-credential"|"owner-material"|"owner-irreversible";
}
export interface CoordinationPlan { rigId:string; revision:string; operatorGeneration:string; stallMs:number; allowIdlePeerTransfer:boolean; allowUnavailablePeerTransfer?:boolean; acknowledgmentWindowMs?:number; tasks:CoordinationTask[] }
export interface CoordinationResult { key:string; state:string; queueId?:string; reason?:string; deadline:number }
const successfulReturn=(state:string,disposition:string|null):boolean=>!!disposition&&['done','handed-off'].includes(state);
/** Only the exact migration091 refusal is normalized, never arbitrary SQL failures. */
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
 constructor(private repo:QueueRepository,private activity:(session:string)=>CoordinationActivity|null,private now:()=>number=Date.now){}
 private get authority(){return this.repo.coordinatorAuthority;}
 private get db(){return this.authority.db;}
 configure(actor:string,generation:string,plan:CoordinationPlan):CoordinationPlan {
  return this.db.transaction(()=>{
   if(actor!=="operator-agent@kernel"||this.authority.generation(actor)!==generation||plan.operatorGeneration!==generation)fail("coordination_operator_required","Current genuine Operator configures recovery");
   if(!this.authority.get(plan.rigId))fail("coordinator_not_enabled","Explicit legacy enrollment/admission required");
   if(!Number.isSafeInteger(plan.stallMs)||plan.stallMs<10000||plan.stallMs>3600000||typeof plan.allowIdlePeerTransfer!=="boolean"||!plan.revision||!plan.tasks.length||new Set(plan.tasks.map(t=>t.key)).size!==plan.tasks.length||new Set(plan.tasks.map(t=>t.packageKey)).size!==plan.tasks.length)fail("coordination_invalid_plan","Unique immutable tasks/packages required");
   if(plan.acknowledgmentWindowMs!==undefined&&(!Number.isSafeInteger(plan.acknowledgmentWindowMs)||plan.acknowledgmentWindowMs<10000||plan.acknowledgmentWindowMs>900000))fail("coordination_invalid_ack_window","Acknowledgment window must be 10 seconds to 15 minutes");
   if(plan.allowUnavailablePeerTransfer!==undefined&&typeof plan.allowUnavailablePeerTransfer!=='boolean')fail('coordination_invalid_unavailable_optin','Unavailable-owner transfer requires strict explicit boolean');
   const prior=this.plan(plan.rigId);
   const stable=(t:CoordinationTask)=>JSON.stringify({...t,admission:undefined,deadline:undefined});
   const keys=new Set(plan.tasks.map(t=>t.key));
   for(const t of plan.tasks){
    if(!t.key||!t.action.trim()||!Number.isFinite(t.deadline)||(t.deadline<=this.now()&&!prior?.tasks.some(old=>old.key===t.key&&stable(old)===stable(t)))||!t.body||!Array.isArray(t.predecessors))fail("coordination_invalid_task","Concrete action, future deadline, predecessors and exact body required");
    const ad=t.admission;
    if(!ad||ad.generation!==this.authority.generation(t.owner)||ad.configurationDigest!==this.configurationDigest(t.owner)||!ad.qualificationRef||!ad.capacityRef||!ad.effortRef||!Number.isFinite(ad.validUntil)||ad.validUntil<=this.now())fail('coordination_current_admission_required','Exact current generation/configuration, qualification/capacity/effort evidence and expiry required');
    if(t.boundary&&!['owner-access','owner-credential','owner-material','owner-irreversible'].includes(t.boundary))fail("coordination_invalid_boundary","Unknown boundary");
    const row=this.db.prepare("SELECT contract FROM coordinator_packages WHERE rig_id=? AND package_key=?").get(plan.rigId,t.packageKey) as {contract:string}|undefined;
    if(!row)fail("coordination_package_not_admitted","Every task including recovery needs explicit admission");
    const c=JSON.parse(row!.contract);
    if(c.destination!==t.owner||c.bodyHash!==digest(t.body)||!c.returnContract?.evidenceRequired?.length)fail("coordination_package_mismatch","Exact owner/body and attributed return contract required");
    for(const pred of t.predecessors)if(!pred.queueId||!pred.dispositionId)fail("coordination_invalid_predecessor","Exact disposition receipt required");
    if(t.recoveryFor&&(!keys.has(t.recoveryFor)||t.recoveryFor===t.key))fail("coordination_invalid_recovery","Recovery must name another exact plan task");
    if(!t.recoveryFor&&!t.boundary&&!plan.tasks.some(r=>r.recoveryFor===t.key))fail("coordination_recovery_required","Every ordinary task needs a distinct admitted recovery task with concrete owner/action/deadline");
   }
   const byQueue=new Map(plan.tasks.map(t=>['qitem-coordination-'+digest(plan.rigId+':'+t.packageKey).slice(0,24),t]));
   const visiting=new Set<string>(),visited=new Set<string>();
   const visit=(t:CoordinationTask):void=>{if(visiting.has(t.key))fail('coordination_dependency_cycle','Recovery/dependency graph cannot cycle');if(visited.has(t.key))return;visiting.add(t.key);for(const pred of t.predecessors){const parent=byQueue.get(pred.queueId);if(parent)visit(parent);}if(t.recoveryFor)visit(plan.tasks.find(other=>other.key===t.recoveryFor)!);visiting.delete(t.key);visited.add(t.key);};
   for(const t of plan.tasks)visit(t);
   const reaches=(t:CoordinationTask,target:string,seen=new Set<string>()):boolean=>{
    if(t.key===target)return true;if(seen.has(t.key))return false;seen.add(t.key);
    return t.predecessors.some(pred=>{const parent=byQueue.get(pred.queueId);return !!parent&&reaches(parent,target,seen);});
   };
   for(const t of plan.tasks)if(t.recoveryFor&&reaches(t,t.recoveryFor))fail('coordination_recovery_deadlock','Recovery cannot depend directly or transitively on its blocked task');
   const id=`coordination-plan:${plan.revision}`;
   const old=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=?").get(plan.rigId,id) as {receipt:string}|undefined;
   if(old){if(old.receipt!==JSON.stringify(plan))fail("coordination_plan_conflict","Frozen revision cannot change");return JSON.parse(old.receipt);}
   // Do not replace unresolved contracts with a new plan and silently orphan work.
   if(prior&&prior.tasks.some(t=>!plan.tasks.some(n=>n.key===t.key&&stable(n)===stable(t))))fail("coordination_plan_obligation_lost","Retain all existing tasks unchanged in successor revision");
   this.db.prepare("INSERT INTO coordinator_operations VALUES (?,?,?,?,?)").run(plan.rigId,id,"coordination-plan",JSON.stringify(plan),digest(JSON.stringify({actor,generation,plan})));
   this.recordProgress(plan.rigId);
   return plan;
  }).immediate();
 }
 plan(rigId:string):CoordinationPlan|null {
  const rows=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='coordination-plan' ORDER BY rowid DESC LIMIT 1").get(rigId) as {receipt:string}|undefined;
  return rows?JSON.parse(rows.receipt):null;
 }
 reconcile(actor:string,generation:string,rigId:string):CoordinationResult[] {
  return this.db.transaction(()=>{
   const a=this.authority.get(rigId),plan=this.plan(rigId);
   if(!a||!plan)fail("coordination_plan_required","Explicit current recovery plan required");
   if(this.authority.generation("operator-agent@kernel")!==plan!.operatorGeneration)fail("coordination_operator_retired","Reauthorize plan after Operator generation change");
   if(actor!==a!.owner_session||generation!==a!.owner_generation||this.authority.generation(actor)!==generation||a!.state!=="active"||a!.lease_until<=this.now())fail("coordinator_retired","Only reconciled current holder may dispatch");
   const token:CoordinatorToken={rigId,epoch:a!.epoch,generation};
   const result:CoordinationResult[]=[];
   this.recordProgress(rigId);
   for(const t of plan!.tasks){
    const assigned=this.db.prepare("SELECT a.queue_id,a.disposition_id,q.state,q.claimed_by_generation_uuid,q.destination_session FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? AND a.package_key=?").get(rigId,t.packageKey) as {queue_id:string;disposition_id:string|null;state:string;claimed_by_generation_uuid:string|null;destination_session:string}|undefined;
    if(assigned){
     if(!t.boundary&&this.predecessorsReady(rigId,t)&&assigned.state==='pending'&&!assigned.claimed_by_generation_uuid&&!assigned.disposition_id&&this.admittedNow(t)&&!this.workerEffectDebt(t.owner)&&coordinationIdle(this.activity(t.owner),this.authority.generation(t.owner)??'',this.now()))this.repo.stageCoordinatorAssignmentWake({rigId,epoch:a!.epoch,generation,actor,queueId:assigned.queue_id,recipient:t.owner,recipientGeneration:t.admission.generation,now:this.now()});
     const picked=assigned.state==='in-progress'&&assigned.claimed_by_generation_uuid===this.authority.generation(t.owner)&&assigned.destination_session===t.owner;
     const semanticRecovery=this.authority.runtimeOutcomeAssessment?.requiresRecovery(rigId,t.packageKey)??false;
     const state=semanticRecovery?'recovery-required:semantic-incomplete':successfulReturn(assigned.state,assigned.disposition_id)?'returned-awaiting-acceptance':picked?'picked-up':assigned.state==='pending'?'pending-pickup':`recovery-required:${assigned.state}`;
     result.push({key:t.key,state,queueId:assigned.queue_id,deadline:t.deadline,...(!assigned.disposition_id&&this.now()>t.deadline?{reason:'deadline-exceeded: concrete recovery owner/action remains '+t.owner+' / '+t.action}:{})});continue;
    }
    if(t.recoveryFor){
     const target=plan!.tasks.find(other=>other.key===t.recoveryFor)!;
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
     const needsRecovery=!target.boundary&&(!targetAssignment||!successfulReturn(targetAssignment.state,targetAssignment.disposition_id)||semanticRecovery)&&((semanticRecovery&&!!targetAssignment)||(!!targetAssignment&&(['blocked','failed','denied','canceled'].includes(targetAssignment.state)||this.now()>target.deadline))||(!targetAssignment&&targetReady&&!occupied&&!knownBusy&&(this.now()>target.deadline||this.workerEffectDebt(target.owner)||!this.admittedNow(target)||!targetGen||!coordinationIdle(observed,targetGen,this.now()))));
     if(!needsRecovery){result.push({key:t.key,state:'held',reason:'recovery-not-needed',deadline:t.deadline});continue;}
    }
    if(t.boundary){result.push({key:t.key,state:'held',reason:t.boundary,deadline:t.deadline});continue;}
    const ready=this.predecessorsReady(rigId,t);
    if(!ready){result.push({key:t.key,state:'held',reason:'predecessor-disposition',deadline:t.deadline});continue;}
    if(this.workerEffectDebt(t.owner)){result.push({key:t.key,state:'held',reason:'uncertain-worker-effect',deadline:t.deadline});continue;}
    if(!this.admittedNow(t)){result.push({key:t.key,state:'held',reason:'current-admission-required',deadline:t.deadline});continue;}
    const gen=this.authority.generation(t.owner);
    if(!gen||!coordinationIdle(this.activity(t.owner),gen,this.now())){result.push({key:t.key,state:'held',reason:'fresh-activity-required',deadline:t.deadline});continue;}
    // An unrelated queue claim is an exclusive worker obligation, even while idle.
    if(this.db.prepare("SELECT 1 FROM queue_items WHERE destination_session IN (?,?) AND state IN ('pending','in-progress','blocked')").get(...rotationLocalAddresses(this.db,t.owner))){result.push({key:t.key,state:'held',reason:'existing-worker-custody',deadline:t.deadline});continue;}
    const queueId=`qitem-coordination-${digest(rigId+':'+t.packageKey).slice(0,24)}`;
    try {
     this.db.transaction(()=>this.repo.createWithinTransaction({qitemId:queueId,sourceSession:actor,destinationSession:t.owner,body:t.body,dispatch:{token,packageKey:t.packageKey},identityProvenance:'system:operator-authorized-coordination',nudge:true}))();
    } catch(error) {
     const code=heldDispatchCode(error);
     if(!code)throw error;
     result.push({key:t.key,state:'held',reason:code,deadline:t.deadline});continue;
    }
    result.push({key:t.key,state:'pending-pickup',queueId,deadline:t.deadline});
   }
   const operationId=`coordination-reconcile:${digest(JSON.stringify({revision:plan!.revision,epoch:a!.epoch,result}))}`;
   this.db.prepare("INSERT OR IGNORE INTO coordinator_operations VALUES (?,?,?,?,?)").run(rigId,operationId,'coordination-reconcile',JSON.stringify(result),digest(JSON.stringify({actor,generation})));
   this.recordProgress(rigId);
   return result;
  }).immediate();
 }
 configurationDigest(session:string):string|null {
  const row=this.db.prepare('SELECT n.id,n.runtime,n.model,n.profile,n.codex_config_profile,n.cwd FROM nodes n JOIN sessions s ON s.node_id=n.id WHERE s.session_name=? ORDER BY s.id DESC LIMIT 1').get(session);
  return row?digest(JSON.stringify(row)):null;
 }
 private workerEffectDebt(session:string):boolean {
  const addresses=rotationLocalAddresses(this.db,session);
  return !!this.db.prepare("SELECT 1 FROM outbox_entries WHERE delivery_state NOT IN ('delivered','failed','retired') AND (sender_session IN (?,?) OR destination_session IN (?,?)) LIMIT 1").get(...addresses,...addresses);
 }
 private admittedNow(t:CoordinationTask):boolean {
  return t.admission.validUntil>this.now()&&t.admission.generation===this.authority.generation(t.owner)&&t.admission.configurationDigest===this.configurationDigest(t.owner);
 }
 /** Returns require coordinator disposition; worker completion alone is not acceptance. */
 accept(actor:string,generation:string,rigId:string,packageKey:string,dispositionId:string,evidenceRef:string):void {
  this.db.transaction(()=>{
   const a=this.authority.get(rigId);
   if(!a||a.owner_session!==actor||a.owner_generation!==generation||a.state!=='active'||this.authority.generation(actor)!==generation||a.lease_until<=this.now())fail('coordinator_retired','Current holder must accept exact return');
   this.authority.runtimeOutcomeAssessment?.assertAcceptance(rigId,packageKey);
   if(!evidenceRef)fail('coordination_acceptance_required','Attributed technical disposition evidence required');
   const row=this.db.prepare("SELECT a.queue_id,a.disposition_id,q.state FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? AND a.package_key=?").get(rigId,packageKey) as {queue_id:string;disposition_id:string|null;state:string}|undefined;
   if(!row||row.disposition_id!==dispositionId||!['done','handed-off'].includes(row.state))fail('coordination_return_required','Exact successful attributed released return required');
   const receipt={queueId:row!.queue_id,dispositionId,actor,generation,evidenceRef};
   const id='coordination-accept:'+packageKey;
   const prior=this.db.prepare('SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=?').get(rigId,id) as {receipt:string}|undefined;
   if(prior){if(JSON.parse(prior.receipt).dispositionId!==dispositionId)fail('coordination_acceptance_conflict','Accepted result cannot change');return;}
   this.db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run(rigId,id,'coordination-accept',JSON.stringify(receipt),digest(JSON.stringify(receipt)));
   this.recordProgress(rigId);
  }).immediate();
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
  if(!a||this.progressDigest(rigId)!==progressDigest||this.authority.generation(recipient)!==recipientGeneration)return false;
  if(this.db.prepare("SELECT 1 FROM queue_items WHERE destination_session IN (?,?) AND state IN ('pending','in-progress','blocked') AND qitem_id<>?").get(...rotationLocalAddresses(this.db,recipient),a.baton_id))return false;
  return coordinationIdle(this.activity(recipient),recipientGeneration,this.now())&&coordinationIdle(this.activity(a.owner_session),a.owner_generation,this.now());
 }
 canTransferUnavailable(rigId:string,recipient:string,recipientGeneration:string):boolean {
  const a=this.authority.get(rigId),plan=this.plan(rigId);if(!a||!plan||plan.allowUnavailablePeerTransfer!==true||this.authority.generation(recipient)!==recipientGeneration)return false;
  if(plan.tasks.some(t=>t.admission.generation!==this.authority.generation(t.owner)||t.admission.configurationDigest!==this.configurationDigest(t.owner)||!Number.isFinite(t.admission.validUntil)||t.admission.validUntil<=this.now()))return false;
  if(this.db.prepare("SELECT 1 FROM queue_items WHERE destination_session IN (?,?) AND state IN ('pending','in-progress','blocked') AND qitem_id<>?").get(...rotationLocalAddresses(this.db,recipient),a.baton_id))return false;
  return coordinationIdle(this.activity(recipient),recipientGeneration,this.now());
 }
 private stageCoordinatorRecovery(rigId:string,epoch:number,operatorGeneration:string,action:string,reason:string,deadline:number):string {
  const recoveryKey=digest(rigId+':'+epoch+':'+operatorGeneration+':'+action+':'+reason);
  const previous=this.db.prepare("SELECT qitem_id,state FROM queue_items WHERE destination_session='operator-agent@kernel' AND json_valid(body) AND json_extract(body,'$.recoveryKey')=? ORDER BY rowid DESC LIMIT 1").get(recoveryKey) as {qitem_id:string;state:string}|undefined;
  const queueId=previous&&['pending','in-progress','blocked'].includes(previous.state)?previous.qitem_id:'qitem-coordination-recovery-'+digest(recoveryKey+':'+(previous?.qitem_id??'initial')).slice(0,24);
  if(!this.repo.getById(queueId))this.repo.createWithinTransaction({qitemId:queueId,sourceSession:'watchdog@system',destinationSession:'operator-agent@kernel',body:JSON.stringify({action,reason,recoveryKey,previousQueueId:previous?.qitem_id??null,rigId,epoch,recipientGeneration:operatorGeneration,deadline,nextAction:'Revalidate exact current native holder/Peer, plan and baton custody. A living holder may perform supported voluntary transfer; admit fresh idle or positive-absence recovery only when proven. Repair expired admissions or uncertain effects through their existing supported paths. Preserve workers and return a concrete protected boundary when evidence is unknown; do not fabricate extension or acknowledgment.',returnPath:{queueId,actor:'operator-agent@kernel',required:'Claim exact recovery item and return supported evidence or concrete protected boundary. Do not declare pickup/ACK or native absence from a role label.'}}),identityProvenance:'system:operator-authorized-coordination',nudge:true});
  this.repo.stageWakeIntent(queueId,'watchdog@system','operator-agent@kernel','system:operator-authorized-coordination',true,operatorGeneration);return queueId;
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
 private predecessorsReady(rigId:string,t:CoordinationTask):boolean {return t.predecessors.every(p=>!!this.db.prepare("SELECT 1 FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? AND a.queue_id=? AND a.disposition_id=? AND q.state IN ('done','handed-off') AND EXISTS (SELECT 1 FROM coordinator_operations o WHERE o.rig_id=a.rig_id AND o.kind='coordination-accept' AND json_extract(o.receipt,'$.queueId')=a.queue_id AND json_extract(o.receipt,'$.dispositionId')=a.disposition_id)").get(rigId,p.queueId,p.dispositionId));}
 async deliverCommitted():Promise<void>{await this.repo.drainPendingWakeIntents();}
 /** Existing Operator-registered coordinator watchdog is the only unattended actor.
  * It creates real queue intents through the same path, never acknowledgment. */
 supervise(rigId:string,jobId:string):CoordinationResult[]|null {
  return this.db.transaction(()=>{
   const job=this.db.prepare('SELECT * FROM watchdog_jobs WHERE job_id=?').get(jobId) as {policy:string;state:string;registered_by_session:string;registered_by_generation_uuid:string;target_session:string}|undefined;
   const plan=this.plan(rigId),a=this.authority.get(rigId);
   if(!plan||!a)return null;
   if(!job||job.policy!=='coordinator-continuity'||job.state!=='active'||job.target_session!=='operator-agent@kernel'||job.registered_by_session!=='operator-agent@kernel'||job.registered_by_generation_uuid!==plan.operatorGeneration||this.authority.generation('operator-agent@kernel')!==plan.operatorGeneration)fail('coordination_observer_not_authorized','Current Operator job and plan required');
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
    }catch(error){const code=heldDispatchCode(error)??(error instanceof CoordinatorFenceError?error.code:undefined);if(!code)throw error;const deadline=this.now()+plan.stallMs;const queueId=this.stageCoordinatorRecovery(rigId,a.epoch,plan.operatorGeneration,'recover-unavailable-coordinator',code,deadline);return [{key:'coordinator',state:'held',queueId,reason:code,deadline}];}
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
      const code=(error as {code?:string}).code,hold=heldDispatchCode(error)??(['coordination_stall_unproven','coordinator_uncertain_effects'].includes(code??'')?code:undefined);
      if(!hold)throw error;
      // A safe takeover refusal is not a global work gate. Re-read authority before
      // fallback; never use a stale cached holder/epoch or imply successor pickup.
      const current=this.authority.get(rigId);
      if(!current||current.owner_session!==a.owner_session||current.owner_generation!==a.owner_generation||current.epoch!==a.epoch||current.state!=='active'||this.authority.generation(current.owner_session)!==current.owner_generation)throw error;
      if(current.lease_until<=this.now()){const deadline=this.now()+plan.stallMs,queueId=this.stageCoordinatorRecovery(rigId,a.epoch,plan.operatorGeneration,'recover-expired-idle-transfer',hold,deadline);return [{key:'coordinator',state:'held',queueId,reason:hold,deadline}];}
      const receipt={rigId,epoch:a.epoch,peer,reason:hold,owner:'operator-agent@kernel',action:'Reconcile exact Peer custody/transport/lifecycle fence and retry only after eligibility is proven',deadline:this.now()+plan.stallMs};
      this.db.prepare('INSERT OR IGNORE INTO coordinator_operations VALUES (?,?,?,?,?)').run(rigId,'coordination-takeover-hold:'+digest(JSON.stringify({revision:plan.revision,epoch:a.epoch,hold})), 'coordination-takeover-hold',JSON.stringify(receipt),digest(JSON.stringify(receipt)));
      return [...this.reconcile(current.owner_session,current.owner_generation,rigId),{key:'coordinator',state:'held',reason:hold,deadline:receipt.deadline}];
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
   return this.reconcile(a.owner_session,a.owner_generation,rigId);
  }).immediate();
 }

}
